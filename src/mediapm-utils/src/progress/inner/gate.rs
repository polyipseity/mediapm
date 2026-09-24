//! Write-gate: one-draw-per-frame terminal suppression.
//!
//! [`BufferedTerm`] wraps any [`TermLike`] and suppresses terminal writes
//! while a frame is in progress. [`WriteGate`] is the only way to open
//! and close the write window — the `open`/`suppress` methods are
//! private to this module, so a caller outside this module cannot bypass
//! the gate.
//!
//! [`WriteWindow`] is an RAII guard: construction opens the window
//! (writes go through), drop re-suppresses. It is crate-private and
//! can only be obtained via [`WriteGate::open`].
//!
//! # A suppressed write reports failure, it does not report success
//!
//! Every suppressed write method returns [`Err`] — see
//! [`suppressed_write_error`]. This is load-bearing, not cosmetic:
//! `indicatif`'s `DrawState::draw_to_term` writes back the height it just
//! drew (`*bar_count = real_height + shift`) at the very end of a draw, so
//! a suppressed draw that reported success would leave indicatif believing
//! the terminal holds a frame it never received. The next draw released
//! inside a write window then unwinds that phantom frame
//! (`move_cursor_up(bar_count - 1)` followed by one `clear_line` per
//! phantom row) and erases whatever is really on those rows — the previous
//! screen's committed frame.
//!
//! `draw_to_term` propagates the first failing `TermLike` call with `?`
//! *before* the height is written back (`indicatif-0.17.11/src/draw_target.rs`,
//! the `?` on the `move_cursor_up`/`clear_line`/`write_str`/`flush` calls at
//! lines 504-571 all precede the store at line 572), so reporting `Err`
//! leaves the recorded height exactly as the last *released* draw left it.
//!
//! The `Err` never escapes this crate: the suppression only ever wraps the
//! draw target of a [`ProgressTerminal`](super::terminal::ProgressTerminal),
//! which keeps its `MultiProgress` and every `ProgressBar` private. The two
//! places in indicatif 0.17.11 that unwrap a draw result —
//! `MultiState::suspend` (`multi.rs:391`, reached only via
//! `MultiProgress::suspend`/`ProgressBar::suspend`) and
//! `ProgressBar::set_tab_width` (`progress_bar.rs:167`) — therefore need an
//! indicatif handle that no mediapm caller can obtain. No in-tree caller
//! unwraps a draw result.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use indicatif::TermLike;

// ---- BufferedTerm: suppress terminal writes from property setters ----

/// Build the error a suppressed write returns.
///
/// One constructor keeps the message (and therefore the contract: "this
/// write was discarded, the draw did not happen") identical across every
/// suppressed method, so a caller that does inspect the error can identify
/// the gate without matching on a kind. See the module docs for why a
/// suppressed write must not report success.
fn suppressed_write_error() -> std::io::Error {
    std::io::Error::other(
        "mediapm progress write gate: terminal write discarded while a frame is in progress",
    )
}

/// Wraps any [`TermLike`] to suppress terminal writes while a frame is
/// in progress. The [`buffer_enabled`](Self::buffer_enabled) flag is
/// owned privately — the only way to open/close the write window is
/// through the paired [`WriteGate`].
///
/// When buffering is active, every write/clear/move operation is discarded
/// and reports failure (see [`suppressed_write_error`]);
/// [`width`](Self::width) and [`height`](Self::height) always delegate to
/// the inner terminal, because the draw decision (how many rows the frame
/// needs) must stay accurate even while writes are discarded.
#[derive(Debug)]
pub(crate) struct BufferedTerm {
    inner: Box<dyn TermLike>,
    buffer_enabled: Arc<AtomicBool>,
}

impl BufferedTerm {
    /// Create a new buffered terminal paired with its write gate.
    ///
    /// The gate is the only handle that can open/close the write
    /// window; the term is the only handle that enforces suppression.
    /// A gate without its enforcing term is unrepresentable.
    pub(crate) fn new(inner: Box<dyn TermLike>) -> (Self, WriteGate) {
        let flag = Arc::new(AtomicBool::new(true));
        let gate = WriteGate { flag: Arc::clone(&flag) };
        (Self { inner, buffer_enabled: flag }, gate)
    }
}

impl TermLike for BufferedTerm {
    fn width(&self) -> u16 {
        self.inner.width()
    }

    fn height(&self) -> u16 {
        self.inner.height()
    }

    fn write_line(&self, s: &str) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Err(suppressed_write_error());
        }
        self.inner.write_line(s)
    }

    fn write_str(&self, s: &str) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Err(suppressed_write_error());
        }
        self.inner.write_str(s)
    }

    fn clear_line(&self) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Err(suppressed_write_error());
        }
        self.inner.clear_line()
    }

    fn flush(&self) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Err(suppressed_write_error());
        }
        self.inner.flush()
    }

    fn move_cursor_up(&self, n: usize) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Err(suppressed_write_error());
        }
        self.inner.move_cursor_up(n)
    }

    fn move_cursor_down(&self, n: usize) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Err(suppressed_write_error());
        }
        self.inner.move_cursor_down(n)
    }

    fn move_cursor_left(&self, n: usize) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Err(suppressed_write_error());
        }
        self.inner.move_cursor_left(n)
    }

    fn move_cursor_right(&self, n: usize) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Err(suppressed_write_error());
        }
        self.inner.move_cursor_right(n)
    }
}

// ---- WriteGate: the only way to open/close the write window ----------

/// Controls the write window on a paired [`BufferedTerm`].
///
/// Obtained from [`BufferedTerm::new`] or [`WriteGate::open`]. The
/// `open`/`suppress` methods are **private to this module** — callers
/// outside this module cannot bypass the gate.
///
/// Use [`WriteGate::new_noop`] for test paths where the caller
/// provides their own [`MultiProgress`] without a [`BufferedTerm`].
#[derive(Debug, Clone)]
pub(crate) struct WriteGate {
    flag: Arc<AtomicBool>,
}

impl WriteGate {
    /// Create a gate that never suppresses writes.
    ///
    /// Used by [`ProgressRenderer`] test paths where the caller
    /// provides their own [`MultiProgress`] without a [`BufferedTerm`].
    #[allow(dead_code, reason = "used by renderer unit tests and group.rs with_multi_progress")]
    pub(crate) fn new_noop() -> Self {
        Self { flag: Arc::new(AtomicBool::new(false)) }
    }

    /// Open the write window. Terminal writes go through until the
    /// returned [`WriteWindow`] is dropped.
    ///
    pub(crate) fn open(&self) -> WriteWindow {
        self.flag.store(false, Ordering::Release);
        WriteWindow { flag: Arc::clone(&self.flag) }
    }

    /// Suppress all terminal writes (re-enable buffering).
    ///
    /// Idempotent — safe to call when already suppressed (e.g. on
    /// re-entry after a previous `open`).  The nesting guard lives in
    /// [`ProgressRenderer::run_frame`] via the `in_frame` cell.
    pub(crate) fn suppress(&self) {
        self.flag.store(true, Ordering::Release);
    }

    /// Whether terminal writes are currently suppressed.
    ///
    /// Used by `debug_assert!` in `paint()` to verify the caller is
    /// inside a frame window.
    #[allow(dead_code, reason = "used by debug_assert in suppress() and paint assertions")]
    pub(crate) fn is_suppressed(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
}

// ---- WriteWindow: RAII guard for the open write window ---------------

/// RAII guard that keeps the write window open for its lifetime.
///
/// On creation: writes go through. On drop: writes suppressed again.
/// **Private constructor** — only obtainable via [`WriteGate::open`].
/// Crate code outside `gate.rs` cannot construct one.
#[must_use = "dropping a WriteWindow immediately re-suppresses writes"]
pub(crate) struct WriteWindow {
    flag: Arc<AtomicBool>,
}

impl Drop for WriteWindow {
    fn drop(&mut self) {
        self.flag.store(true, Ordering::Release);
    }
}
