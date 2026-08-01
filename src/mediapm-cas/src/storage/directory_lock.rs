//! RAII directory lock guard for exclusive CAS directory access.
//!
//! Provides two-layer locking:
//! - **Intra-process**: a global [`DashMap`] of per-directory
//!   [`tokio::sync::Mutex`]es, acquired via non-blocking
//!   [`try_lock_owned`](tokio::sync::Mutex::try_lock_owned).
//! - **Inter-process**: an `flock`/`LockFileEx` advisory file lock via
//!   [`fs4::AsyncFileExt`], acquired via non-blocking
//!   [`try_lock`](fs4::AsyncFileExt::try_lock).
//!
//! Both layers fail immediately with [`CasError::LockContention`] if the
//! lock is already held. This is by design: the lock is held for the full
//! CAS lifetime, so blocking on contention would hang the caller, and
//! same-process contention is a programming bug (the caller should share
//! the [`FileSystemCas`](crate::storage::file_system::FileSystemCas)
//! instance instead).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::LazyLock;

use dashmap::DashMap;
use fs4::AsyncFileExt;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::error::CasError;

/// Global registry of in-process mutexes keyed by canonical CAS directory
/// path.
///
/// Each entry is an `Arc<Mutex<()>>` so the mutex can be owned by the guard
/// via `try_lock_owned` without borrowing from the [`DashMap`] entry.
static DIR_LOCKS: LazyLock<DashMap<PathBuf, Arc<Mutex<()>>>> = LazyLock::new(DashMap::new);

/// An RAII guard that holds both an in-process mutex and an inter-process
/// advisory file lock on a CAS directory.
///
/// Dropping this guard releases both locks in reverse acquisition order:
/// the flock first (via [`tokio::fs::File::drop`]), then the in-process
/// mutex (via [`OwnedMutexGuard::drop`]).
#[derive(Debug)]
pub(super) struct DirectoryLockGuard {
    // Dropped FIRST (flock released), then `_in_process_guard` (mutex
    // released). This is reverse of acquisition order — correct.
    _file: tokio::fs::File,
    _in_process_guard: OwnedMutexGuard<()>,
}

impl DirectoryLockGuard {
    /// Acquires both locks: in-process mutex first, then inter-process
    /// flock.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::LockContention`] if the in-process mutex or the
    /// inter-process flock is already held for this directory.
    pub(super) async fn lock(dir: &Path) -> Result<Self, CasError> {
        let canonical = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());

        // Layer 1: in-process mutex (non-blocking — fail if held).
        let entry = DIR_LOCKS.entry(canonical).or_insert_with(|| Arc::new(Mutex::new(())));
        let mutex_arc = entry.value().clone();
        drop(entry); // Release DashMap shard lock before fallible call.
        // Fail-fast: no blocking .lock() or .lock_owned() — must return immediately on contention.
        let in_process_guard = mutex_arc
            .try_lock_owned()
            .map_err(|_| CasError::LockContention { path: dir.to_path_buf() })?;

        // Layer 2: inter-process flock (non-blocking — fail if held).
        let lock_path = dir.join("lock");
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&lock_path)
            .await?;
        // Fail-fast: no blocking .lock() or .lock_owned() — must return immediately on contention.
        file.try_lock().map_err(|_| CasError::LockContention { path: lock_path })?;

        Ok(Self { _file: file, _in_process_guard: in_process_guard })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs4::AsyncFileExt;
    use std::time::Duration;
    use tempfile::tempdir;

    /// Acquiring, dropping, then re-acquiring the lock must succeed.
    /// This validates RAII drop semantics (both layers released).
    #[tokio::test]
    async fn directory_lock_new_releases_on_drop() {
        let dir = tempdir().unwrap();
        let guard = DirectoryLockGuard::lock(dir.path()).await.unwrap();
        drop(guard);
        // Must succeed after drop.
        let _guard2 = DirectoryLockGuard::lock(dir.path()).await.unwrap();
    }

    /// Acquiring the lock while it is already held in the same process
    /// must return `LockContention`. After dropping the first guard,
    /// acquiring again must succeed.
    #[tokio::test]
    async fn directory_lock_same_process_contention() {
        let dir = tempdir().unwrap();
        let guard = DirectoryLockGuard::lock(dir.path()).await.unwrap();

        // Second acquire must fail with LockContention (Layer 1).
        let err = DirectoryLockGuard::lock(dir.path()).await.unwrap_err();
        assert!(
            matches!(&err, CasError::LockContention { .. }),
            "expected LockContention, got {err:?}"
        );

        drop(guard);
        // Must succeed after first guard is dropped.
        let _guard2 = DirectoryLockGuard::lock(dir.path()).await.unwrap();
    }

    /// Simulating cross-process contention by pre-locking the flock file:
    /// manually lock `<dir>/lock`, then attempt `DirectoryLockGuard::lock`
    /// — must fail with `LockContention` (Layer 2). Release the manual
    /// lock and retry — must succeed.
    #[tokio::test]
    async fn directory_lock_cross_process_contention() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("lock");

        // Manually acquire the flock on the lock file.
        let manual_file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&lock_path)
            .await
            .unwrap();
        manual_file.try_lock().unwrap();

        // DirectoryLockGuard must detect the held flock and fail.
        let err = DirectoryLockGuard::lock(dir.path()).await.unwrap_err();
        assert!(
            matches!(&err, CasError::LockContention { .. }),
            "expected LockContention for held flock, got {err:?}"
        );

        // Release the manual lock (file drop releases flock).
        drop(manual_file);
        // Must succeed after manual lock is released.
        let _guard = DirectoryLockGuard::lock(dir.path()).await.unwrap();
    }

    /// Verifies that both lock layers are fail-fast (non-blocking):
    /// when the lock is already held, the second acquire attempt must
    /// return `LockContention` immediately, not block or timeout.
    #[tokio::test]
    async fn directory_lock_fail_fast_no_blocking() {
        let dir = tempdir().unwrap();
        let _guard = DirectoryLockGuard::lock(dir.path()).await.unwrap();

        // Spawn a second task that tries to acquire the same lock.
        let dir_path = dir.path().to_path_buf();
        let result = tokio::time::timeout(Duration::from_millis(100), async move {
            DirectoryLockGuard::lock(&dir_path).await
        })
        .await;

        // The timeout must NOT fire — the lock attempt must return
        // immediately with LockContention (proving non-blocking).
        let result = result.expect("lock attempt timed out — blocking lock detected");
        assert!(
            matches!(&result, Err(CasError::LockContention { .. })),
            "expected Err(LockContention), got {result:?}"
        );
    }
}
