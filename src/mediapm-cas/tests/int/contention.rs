//! Integration tests for `FileSystemCas` directory lock contention.
//!
//! Covers:
//! - Same-process contention (two opens on the same dir).
//! - Contention via manual flock pre-lock (simulating cross-process).
//! - Concurrent clone sharing (shared lock via Arc).
//! - Canonical path unification (symlink -> same dir detected).

use mediapm_cas::api::CasApi;
use mediapm_cas::error::CasError;
use mediapm_cas::storage::file_system::FileSystemCas;

/// Opening two `FileSystemCas` instances on the same directory in the same
/// process must return `LockContention`. Dropping the first and retrying
/// must succeed.
#[tokio::test]
async fn file_system_cas_same_process_contention() {
    let dir = mediapm_utils::temp::artifact_dir().unwrap();
    let cas1 = FileSystemCas::open(dir.path()).await.unwrap();

    // Second open must fail with LockContention.
    let Err(err) = FileSystemCas::open(dir.path()).await else {
        panic!("expected LockContention, got Ok")
    };
    assert!(matches!(err, CasError::LockContention { .. }), "expected LockContention variant");

    drop(cas1);

    // After dropping the first CAS, opening again must succeed.
    let _cas2 = FileSystemCas::open(dir.path()).await.unwrap();
}

/// Simulating cross-process contention: manually lock the `<dir>/lock`
/// file via fs4, then try to open a `FileSystemCas` — must fail with
/// `LockContention`. Release the manual lock and retry — must succeed.
#[tokio::test]
async fn file_system_cas_contention_with_flock_barrier() {
    let dir = mediapm_utils::temp::artifact_dir().unwrap();
    let lock_path = dir.path().join("lock");

    // Manually acquire the flock on the lock file.
    let manual_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&lock_path)
        .await
        .unwrap();
    fs4::AsyncFileExt::try_lock(&manual_file).unwrap();

    // FileSystemCas::open must detect the held flock and fail.
    let Err(err) = FileSystemCas::open(dir.path()).await else {
        panic!("expected LockContention for held flock, got Ok")
    };
    assert!(
        matches!(err, CasError::LockContention { .. }),
        "expected LockContention variant for held flock"
    );

    // Release the manual lock.
    drop(manual_file);

    // Must succeed after manual lock is released.
    let _cas = FileSystemCas::open(dir.path()).await.unwrap();
}

/// Two clones of the same `FileSystemCas` (sharing the `Arc<DirectoryLockGuard>`)
/// must both be able to operate concurrently without contention errors.
#[tokio::test]
async fn file_system_cas_concurrent_clones_no_contention() {
    let dir = mediapm_utils::temp::artifact_dir().unwrap();
    let cas = FileSystemCas::open(dir.path()).await.unwrap();
    let cas_clone = cas.clone();

    let handle1 = tokio::spawn(async move {
        let data = bytes::Bytes::from_static(b"from-clone-a");
        cas.put(data).await.unwrap();
    });

    let handle2 = tokio::spawn(async move {
        let data = bytes::Bytes::from_static(b"from-clone-b");
        cas_clone.put(data).await.unwrap();
    });

    handle1.await.unwrap();
    handle2.await.unwrap();
}

/// Opening `FileSystemCas` through a symlink to the same directory must
/// still detect contention because `DirectoryLockGuard::lock` calls
/// `std::fs::canonicalize()` to unify paths.
#[tokio::test]
#[cfg_attr(target_os = "windows", ignore = "symlink support varies on Windows")]
async fn file_system_cas_contention_with_canonical_symlink() {
    let dir = mediapm_utils::temp::artifact_dir().unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(dir.path(), &link).unwrap();

    // Open through the real path.
    let _cas = FileSystemCas::open(dir.path()).await.unwrap();

    // Open through the symlink — should still detect contention.
    let Err(err) = FileSystemCas::open(&link).await else {
        panic!("expected LockContention via symlink, got Ok")
    };
    assert!(
        matches!(err, CasError::LockContention { .. }),
        "expected LockContention variant via symlink"
    );
}
