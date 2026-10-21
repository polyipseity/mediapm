//! In-flight operation deduplication.
//!
//! When multiple tasks operate on the same key concurrently, only one
//! performs the underlying work while others wait for its result via
//! a [`Notify`]. This avoids redundant I/O without global locks.

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
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

/// Deduplicates concurrent operations keyed by [`struct@Hash`].
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

        match self.inner.entry(hash) {
            Entry::Occupied(e) => {
                // Another task is already working — wait for its result.
                let existing = e.get().clone();
                drop(e);

                // Register as a waiter *before* reading the result.
                //
                // `notify_waiters` wakes only the waiters registered at the
                // moment it runs, and it leaves no permit behind. Reading the
                // slot and awaiting afterwards therefore misses a leader that
                // finished in between, and the follower waits forever on a
                // notification that already happened. Enabling the future
                // registers it now, so the check below sees either a result or
                // a notification that is guaranteed to arrive.
                let notified = existing.done.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(result) = existing.result.get() {
                    return result.clone();
                }
                notified.await;
                existing.result.get().unwrap_or(&Err(CasError::NotFound(hash))).clone()
            }
            Entry::Vacant(e) => {
                // We are the leader — do the work.
                e.insert(slot.clone());
                let mut guard = PendingGuard { hash, ops: self, slot: &slot, committed: false };
                let result = work().await;
                guard.committed = true;
                slot.result.set(result.clone()).ok();
                // Remove entry BEFORE notifying so new arrivals see Vacant
                // instead of finding an Occupied entry whose notification
                // already fired.  Otherwise the late-comer would call
                // `notified().await` after the notification and block
                // forever.
                self.inner.remove(&hash);
                slot.done.notify_waiters();
                // At this point `guard` drops with committed=true → no-op.
                drop(guard);
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A follower that reads the slot after its leader already finished returns
    /// the recorded result instead of waiting for a notification that has
    /// already been delivered.
    ///
    /// The window is real: the follower clones the slot, the leader completes
    /// and notifies, and only then does the follower await. `notify_waiters`
    /// wakes only the waiters registered when it runs and leaves no permit
    /// behind, so a follower that awaits after the fact would wait forever.
    /// Inserting the entry with its result already set and its notification
    /// already fired reproduces exactly that window.
    #[tokio::test]
    async fn a_follower_that_arrives_after_the_leader_finished_does_not_wait() {
        let ops = PendingOps::new();
        let hash = Hash::from_content(b"late follower");
        let slot =
            Arc::new(PendingSlot { done: Notify::new(), result: std::sync::OnceLock::new() });
        slot.result.set(Ok(Some(Bytes::from_static(b"leader result")))).expect("first set");
        slot.done.notify_waiters();
        ops.inner.insert(hash, Arc::clone(&slot));

        let outcome = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            ops.execute(hash, || async { panic!("a follower must not perform the work") }),
        )
        .await
        .expect("a follower arriving after completion must not wait");

        assert_eq!(
            outcome.expect("the recorded result"),
            Some(Bytes::from_static(b"leader result"))
        );
    }
}
