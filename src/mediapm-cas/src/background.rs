//! Background task lifecycle management.
//!
//! Provides [`BackgroundMaintenanceGuard`] — an RAII guard that spawns and
//! cancels a background task.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::task::JoinHandle;

/// RAII guard that cancels a background task on drop.
///
/// When the last clone of this guard is dropped, the background task is
/// cancelled (via `Arc<AtomicBool>` flag) and the tokio handle is aborted.
///
/// The handle lives behind a [`Mutex`] rather than a plain `Option` so that
/// [`cancel`](Self::cancel) and [`shutdown`](Self::shutdown) work through a
/// shared `&self`: the owning store reaches its guard through an `Arc` and
/// cannot obtain a `&mut` from it.
pub struct BackgroundMaintenanceGuard {
    /// Cooperative cancellation flag the background loop polls between
    /// cycles. Set by [`cancel`](Self::cancel) and
    /// [`shutdown`](Self::shutdown).
    pub cancelled: Arc<AtomicBool>,

    /// The spawned task, taken by the first [`cancel`](Self::cancel) or
    /// [`shutdown`](Self::shutdown) so neither runs twice.
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl BackgroundMaintenanceGuard {
    /// Wrap a spawned task in a guard.
    ///
    /// `cancelled` is the cooperative flag the task polls; the task is
    /// aborted when the last clone of the returned guard drops.
    #[must_use]
    pub fn new(cancelled: Arc<AtomicBool>, handle: JoinHandle<()>) -> Self {
        Self { cancelled, handle: Mutex::new(Some(handle)) }
    }

    /// Returns `true` if the background task has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Request cancellation of the background task without waiting for it.
    /// Idempotent, and callable through a shared reference.
    ///
    /// The abort is asynchronous by construction: it marks the task cancelled
    /// and the task's future is dropped at its next scheduling point, which
    /// this call does not wait for. Work the task already handed to
    /// `spawn_blocking` runs to completion regardless. Use
    /// [`shutdown`](Self::shutdown) to wait for the task itself.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(handle) = self.take_handle() {
            handle.abort();
        }
    }

    /// Cancel the background task and wait for it to actually stop.
    ///
    /// Sets the cooperative flag, aborts the task, and awaits the handle, so
    /// when this returns the task has been dropped and can no longer start
    /// another maintenance cycle. Idempotent.
    ///
    /// Awaiting the aborted handle settles the *task*, not its file-system
    /// work: an operation the task already dispatched to `spawn_blocking` runs
    /// to completion on the blocking pool. Callers that need the store's
    /// directory to be quiescent — a test removing a temp directory, for
    /// instance — pair this with the store's own mutation gate.
    ///
    /// Concurrent callers are not supported: the first caller takes the handle
    /// and awaits it, and a second caller that finds the handle already taken
    /// returns as soon as the flag is set.
    pub async fn shutdown(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(handle) = self.take_handle() {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Take the task handle, leaving `None` behind so a second caller cannot
    /// abort or await the same task.
    fn take_handle(&self) -> Option<JoinHandle<()>> {
        // The guarded value is a single `Option` moved out at most once; a
        // panic can only leave it `None`, which is the state every later caller
        // already handles. Recovering the guard keeps one panicking caller
        // from making the guard un-cancellable.
        let mut handle = self.handle.lock().unwrap_or_else(PoisonError::into_inner);
        handle.take()
    }
}

impl Drop for BackgroundMaintenanceGuard {
    fn drop(&mut self) {
        self.cancel();
    }
}
