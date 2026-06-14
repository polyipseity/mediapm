//! In-flight operation deduplication.
//!
//! When multiple tasks operate on the same key concurrently, only one
//! performs the underlying work while others wait for its result via
//! a [`Notify`]. This avoids redundant I/O without global locks.

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::Notify;

use crate::error::CasError;
use crate::hash::Hash;

/// A pending operation slot that other tasks can wait on.
struct PendingSlot {
    done: Notify,
    result: std::sync::OnceLock<Result<Option<Bytes>, CasError>>,
}

/// Drop guard that ensures waiters are notified even if the leader panics.
///
/// Uses an explicit `committed` flag instead of [`std::mem::forget`]: on the
/// happy path the leader sets `committed = true` before cleaning up, so the
/// `Drop` is a no-op. If the leader panics, `committed` stays `false` and
/// the `Drop` signals an error to waiters.
struct PendingGuard<'a> {
    hash: Hash,
    ops: &'a PendingOps,
    slot: &'a Arc<PendingSlot>,
    committed: bool,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // On panic (or early return without setting committed = true),
            // store an error result so waiters don't block forever.
            self.slot.result.get_or_init(|| Err(CasError::internal("leader panicked")));
            self.slot.done.notify_waiters();
            self.ops.inner.remove(&self.hash);
        }
    }
}

/// Deduplicates concurrent operations keyed by [`Hash`].
///
/// Lock-free for the fast path — the [`DashMap`] entry is only held during
/// insert/remove. The actual work and waiting both happen outside the map
/// lock.
pub(crate) struct PendingOps {
    inner: DashMap<Hash, Arc<PendingSlot>>,
}

impl PendingOps {
    /// Create an empty tracker.
    pub(crate) fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Execute `work` for `hash`, deduplicating concurrent calls.
    ///
    /// If another task is already executing for the same `hash`, this
    /// waits for their result instead of running `work` again.
    /// Returns `Ok(Some(data))` on success, `Ok(None)` if the hash is
    /// confirmed absent, or `Err(e)` on failure.
    pub(crate) async fn execute<F, Fut>(
        &self,
        hash: Hash,
        work: F,
    ) -> Result<Option<Bytes>, CasError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Option<Bytes>, CasError>>,
    {
        let slot =
            Arc::new(PendingSlot { done: Notify::new(), result: std::sync::OnceLock::new() });

        use dashmap::mapref::entry::Entry;
        match self.inner.entry(hash) {
            Entry::Occupied(e) => {
                // Another task is already working — wait for its result.
                let existing = e.get().clone();
                drop(e);
                existing.done.notified().await;
                existing.result.get().unwrap_or(&Err(CasError::NotFound(hash))).clone()
            }
            Entry::Vacant(e) => {
                // We are the leader — do the work.
                e.insert(slot.clone());
                let mut guard = PendingGuard { hash, ops: self, slot: &slot, committed: false };
                let result = work().await;
                guard.committed = true;
                slot.result.set(result.clone()).ok();
                slot.done.notify_waiters();
                self.inner.remove(&hash);
                // At this point `guard` drops with committed=true → no-op.
                drop(guard);
                result
            }
        }
    }
}
