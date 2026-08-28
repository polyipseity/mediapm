//! `BufferedTerm` terminal wrapper that suppresses writes from property setters.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use indicatif::TermLike;

// ---- BufferedTerm: suppress terminal writes from property setters ----

/// Wraps [`console::Term`] to suppress terminal writes when `buffer_enabled` is
/// `true`. Used by [`ProgressRenderer`] so the 50 ms daemon ticker is the sole
/// draw authority — property setters (called from [`sync_snapshot_to_bar`])
/// never write to the terminal directly. When buffering is active, all
/// write/clear/move operations are no-ops; [`width`](Self::width) and
/// [`height`](Self::height) always delegate to the inner terminal.
#[derive(Debug)]
pub(crate) struct BufferedTerm {
    pub(crate) inner: console::Term,
    pub(crate) buffer_enabled: Arc<AtomicBool>,
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
