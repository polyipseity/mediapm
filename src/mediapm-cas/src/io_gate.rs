//! Store-lifetime gate for file-system mutations.
//!
//! # Why this exists
//!
//! A [`FileSystemCas`](crate::storage::file_system::FileSystemCas) arms a
//! background maintenance task that holds an owned `Arc` to the store, so that
//! task can outlive the caller's handle to the store. `tokio::fs` implements
//! every operation with `spawn_blocking`, and aborting a task does **not**
//! cancel a blocking closure it already dispatched: the closure runs to
//! completion on the blocking pool.
//!
//! The consequence is a write-after-teardown race. A blob write dispatched
//! just before the store was dropped calls `create_dir_all` for its fan-out
//! directory *after* the caller's `TempDir` removed the tree, recreating
//! `<cas>/blobs/v1/blake3/ab/cd/` and leaving exactly one object (or one
//! `metadata-v1.json`, or one `checkpoint`) behind in an otherwise empty
//! `$TMPDIR/mediapm-artifact-*` directory.
//!
//! # The contract
//!
//! [`CasIoGate::close`] blocks until every operation already running under a
//! lease has finished, and refuses every lease requested afterwards. So once
//! `close` has returned, no code path reachable from this store can create
//! another directory entry under the store's directory.
//!
//! Two properties make that usable from `Drop`, where awaiting is impossible:
//!
//! 1. A lease is taken **inside** the blocking closure and released **inside**
//!    it, so a lease is never held across an `.await`. [`CasIoGate::close`]
//!    therefore waits only on blocking-pool threads and can be called from a
//!    tokio worker (including a `current_thread` runtime) without deadlocking.
//! 2. Only operations that can *create* a directory entry take a lease. A
//!    write through an already-open file handle cannot resurrect a removed
//!    tree — the inode is unlinked but still writable — and a read or an
//!    unlink cannot create anything.
//!
//! Streaming ingest ([`BlobStore::write_from_reader`](crate::storage::blob_store::BlobStore::write_from_reader)
//! and `write_stream`) does not take a lease for its whole body: it interleaves
//! async reads with writes and cannot hold a lease across an `.await`. That is
//! sound because streaming ingest borrows the store handle, so it cannot be in
//! flight when the last handle drops and the gate closes. Its *creating* steps
//! (directory creation and file creation) are leased, which is what a leaked
//! tree would require.

use std::io;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use tokio::task::JoinError;

/// Error message returned to a mutation refused because the store is closed.
const CLOSED_MESSAGE: &str = "CAS store is closed: filesystem mutations are refused";

/// Number of leases that are currently running.
type InFlight = u32;

/// Gate state: whether the store is closed and how many leases are running.
#[derive(Default)]
struct GateState {
    /// `true` once [`CasIoGate::close`] has been called. No new lease is
    /// granted after this flips.
    closed: bool,
    /// Leases currently running. Each running lease blocks [`CasIoGate::close`]
    /// from returning.
    in_flight: InFlight,
}

/// Lease-counting gate that separates a store's file-system mutations from the
/// store's own destruction.
///
/// See the [module documentation](self) for the teardown race this prevents
/// and the two properties that make it safe to use from `Drop`.
pub(crate) struct CasIoGate {
    state: Mutex<GateState>,
    /// Signalled when `in_flight` reaches zero while `closed` is set.
    quiesced: Condvar,
}

impl CasIoGate {
    /// Create an open gate. Every lease is granted until [`Self::close`] is
    /// called.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self { state: Mutex::new(GateState::default()), quiesced: Condvar::new() }
    }

    /// Return `true` once the gate has been closed.
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Run `op` on the blocking pool under a mutation lease.
    ///
    /// The lease is taken and released inside the blocking closure, never
    /// across an `.await`, so awaiting this future from `Drop`'s caller cannot
    /// deadlock and [`Self::close`] only ever waits on blocking-pool threads.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::BrokenPipe`] without running `op` when the
    /// gate is already closed, and propagates whatever `op` returns otherwise.
    /// A panicked blocking closure surfaces as [`io::ErrorKind::Other`].
    pub(crate) async fn run<T, F>(self: &Arc<Self>, op: F) -> io::Result<T>
    where
        F: FnOnce() -> io::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let gate = Arc::clone(self);
        match tokio::task::spawn_blocking(move || {
            let Some(lease) = gate.acquire() else {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, CLOSED_MESSAGE));
            };
            let result = op();
            // Explicit release rather than relying on `lease` going out of
            // scope: on the panic path the `Drop` impl releases too, and
            // releasing twice is impossible because the lease is moved.
            drop(lease);
            result
        })
        .await
        {
            Ok(result) => result,
            Err(join_error) => Err(join_error_to_io(&join_error)),
        }
    }

    /// Close the gate and block until every running lease has finished.
    ///
    /// After this returns, no code path reachable from the owning store can
    /// create another directory entry under the store's directory: running
    /// operations have completed, and further ones are refused.
    ///
    /// This blocks the calling thread. It is called from `Drop` on a tokio
    /// worker, which is safe because the leases it waits for are released from
    /// blocking-pool threads rather than from runtime tasks.
    pub(crate) fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        while state.in_flight > 0 {
            state = self.quiesced.wait(state).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Take a lease, or return `None` when the gate is closed.
    fn acquire(&self) -> Option<MutationLease<'_>> {
        let mut state = self.lock();
        if state.closed {
            return None;
        }
        state.in_flight += 1;
        Some(MutationLease { gate: self })
    }

    /// Give back a lease, waking [`Self::close`] when the last one returns.
    fn release(&self) {
        let mut state = self.lock();
        state.in_flight -= 1;
        if state.closed && state.in_flight == 0 {
            self.quiesced.notify_all();
        }
    }

    /// Lock the gate state, recovering from poisoning.
    ///
    /// A panic inside a leased operation unwinds through [`MutationLease`]'s
    /// `Drop`, which releases the lease, so the counter cannot be stranded at a
    /// non-zero value by a panic. The guarded state is a flag and a counter
    /// with no cross-field invariant to violate, so recovering the guard keeps
    /// a single panicked operation from turning every later store operation
    /// into a panic.
    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl std::fmt::Debug for CasIoGate {
    /// Report the gate's observable state: whether it is closed and how many
    /// leases are running.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("CasIoGate")
            .field("closed", &state.closed)
            .field("in_flight", &state.in_flight)
            .finish()
    }
}

/// RAII lease held for the duration of one blocking mutation.
///
/// Dropping the lease returns it to the gate, including while unwinding from a
/// panic inside the mutation.
struct MutationLease<'a> {
    gate: &'a CasIoGate,
}

impl Drop for MutationLease<'_> {
    fn drop(&mut self) {
        self.gate.release();
    }
}

/// Map a blocking-task failure to an I/O error.
///
/// A [`JoinError`] here means the blocking closure panicked (or the runtime
/// shut down before running it); the operation's own error, if any, would have
/// been returned as `Ok`.
fn join_error_to_io(error: &JoinError) -> io::Error {
    if error.is_panic() {
        io::Error::other("file-system mutation task panicked")
    } else {
        io::Error::other("file-system mutation task was cancelled")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// An open gate grants every lease, and `close` on an idle gate returns
    /// without blocking.
    #[tokio::test]
    async fn close_on_idle_gate_returns_immediately() {
        let gate = Arc::new(CasIoGate::new());
        assert!(!gate.is_closed());
        gate.close();
        assert!(gate.is_closed());
    }

    /// A refused lease never runs its operation, and reports the closed store
    /// rather than touching the file system.
    #[tokio::test]
    async fn closed_gate_refuses_without_running_the_operation() {
        let gate = Arc::new(CasIoGate::new());
        gate.close();

        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let result = gate
            .run(move || {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
            .await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert!(!ran.load(std::sync::atomic::Ordering::SeqCst), "operation must not run");
    }

    /// The user-level guarantee: once `close` returns, no operation running
    /// under a lease can still be writing. A lease dispatched before the close
    /// is waited for, so a file it creates is complete and accounted for by the
    /// time the store's directory may be removed.
    #[tokio::test]
    async fn close_waits_for_an_in_flight_lease() {
        let gate = Arc::new(CasIoGate::new());
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");

        let mut tasks = Vec::new();
        for index in 0..4 {
            let gate = Arc::clone(&gate);
            let target = dir.path().join(format!("lease-{index}"));
            tasks.push(tokio::spawn(async move {
                gate.run(move || std::fs::write(target, format!("lease-{index}"))).await
            }));
        }

        // The leases are dispatched but may not have finished; closing must
        // wait for whichever of them is still running.
        gate.close();

        for task in tasks {
            // Every dispatched lease either ran to completion before the close
            // returned, or was refused afterwards. Neither can be in flight now.
            let _ = task.await;
        }

        // Whatever landed is a complete file, never a partially written one.
        let mut landed: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read_dir")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("lease-"))
            .collect();
        landed.sort();
        for name in landed {
            let bytes = std::fs::read(dir.path().join(&name)).expect("read landed file");
            assert_eq!(
                String::from_utf8(bytes).ok(),
                Some(name.clone()),
                "file {name} must hold its own complete lease write"
            );
        }
    }

    /// A lease released by a panicking operation must not strand the counter,
    /// or every later `close` would block forever. The channel turns a stranded
    /// counter into a test failure instead of a hang.
    #[tokio::test]
    async fn panicking_lease_still_releases() {
        let gate = Arc::new(CasIoGate::new());
        let result = gate.run(|| -> io::Result<()> { panic!("mutation exploded") }).await;
        assert!(result.is_err(), "a panicking mutation must surface as an error");

        let closer = Arc::clone(&gate);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            closer.close();
            let _ = tx.send(());
        });

        assert!(
            rx.recv_timeout(Duration::from_secs(30)).is_ok(),
            "close() must return after a panicking lease"
        );
        assert!(gate.is_closed());
    }
}
