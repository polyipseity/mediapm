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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use indicatif::TermLike;

// ---- BufferedTerm: suppress terminal writes from property setters ----

/// Wraps any [`TermLike`] to suppress terminal writes while a frame is
/// in progress. The [`buffer_enabled`](Self::buffer_enabled) flag is
/// owned privately — the only way to open/close the write window is
/// through the paired [`WriteGate`].
///
/// When buffering is active, all write/clear/move operations are
/// no-ops; [`width`](Self::width) and [`height`](Self::height) always
/// delegate to the inner terminal.
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
            return Ok(());
        }
        self.inner.write_line(s)
    }

    fn write_str(&self, s: &str) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner.write_str(s)
    }

    fn clear_line(&self) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner.clear_line()
    }

    fn flush(&self) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner.flush()
    }

    fn move_cursor_up(&self, n: usize) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner.move_cursor_up(n)
    }

    fn move_cursor_down(&self, n: usize) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner.move_cursor_down(n)
    }

    fn move_cursor_left(&self, n: usize) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner.move_cursor_left(n)
    }

    fn move_cursor_right(&self, n: usize) -> std::io::Result<()> {
        if self.buffer_enabled.load(Ordering::Acquire) {
            return Ok(());
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
    pub(crate) fn suppress(&self) {
        self.flag.store(true, Ordering::Release);
    }

    /// Whether terminal writes are currently suppressed.
    ///
    /// Used by `debug_assert!` in `paint()` to verify the caller is
    /// inside a frame window.
    #[expect(dead_code, reason = "used by debug_assert in paint() after C4")]
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
