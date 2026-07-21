//! Background task lifecycle management.
//!
//! Provides [`BackgroundMaintenanceGuard`] — an RAII guard that spawns and
//! cancels a background task.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::JoinHandle;

/// RAII guard that cancels a background task on drop.
///
/// When the last clone of this guard is dropped, the background task is
/// cancelled (via `Arc<AtomicBool>` flag) and the tokio handle is aborted.
pub struct BackgroundMaintenanceGuard {
    pub(crate) cancelled: Arc<AtomicBool>,
    pub(crate) handle: Option<JoinHandle<()>>,
}

impl BackgroundMaintenanceGuard {
    /// Returns `true` if the background task has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Cancel the background task immediately. Idempotent.
    pub fn cancel(&mut self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for BackgroundMaintenanceGuard {
    fn drop(&mut self) {
        self.cancel();
    }
}
