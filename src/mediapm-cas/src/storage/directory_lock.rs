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
#[allow(dead_code)]
static DIR_LOCKS: LazyLock<DashMap<PathBuf, Arc<Mutex<()>>>> = LazyLock::new(DashMap::new);

/// An RAII guard that holds both an in-process mutex and an inter-process
/// advisory file lock on a CAS directory.
///
/// Dropping this guard releases both locks in reverse acquisition order:
/// the flock first (via [`tokio::fs::File::drop`]), then the in-process
/// mutex (via [`OwnedMutexGuard::drop`]).
#[allow(dead_code)]
pub(super) struct DirectoryLockGuard {
    // Dropped FIRST (flock released), then `_in_process_guard` (mutex
    // released). This is reverse of acquisition order — correct.
    _file: tokio::fs::File,
    _in_process_guard: OwnedMutexGuard<()>,
}

#[allow(dead_code)]
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
        let in_process_guard = mutex_arc
            .try_lock_owned()
            .map_err(|_| CasError::LockContention { path: dir.to_path_buf() })?;

        // Layer 2: inter-process flock (non-blocking — fail if held).
        let lock_path = dir.join("lock");
        let file = tokio::fs::OpenOptions::new().write(true).create(true).open(&lock_path).await?;
        file.try_lock().map_err(|_| CasError::LockContention { path: lock_path })?;

        Ok(Self { _file: file, _in_process_guard: in_process_guard })
    }
}
