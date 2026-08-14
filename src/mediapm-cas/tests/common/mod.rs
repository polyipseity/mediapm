//! Shared test utilities for `mediapm-cas` integration tests.

use std::time::Duration;

use bytes::Bytes;

use mediapm_cas::api::CasApi;

/// Puts static bytes into `cas` and returns the content-addressable hash.
pub(crate) async fn put_static(cas: &impl CasApi, data: &'static [u8]) -> mediapm_cas::Hash {
    cas.put(Bytes::from_static(data)).await.unwrap()
}

/// Creates a fresh artifact tempdir (RAII guard dropped at end of test).
pub(crate) fn artifact_dir() -> tempfile::TempDir {
    mediapm_utils::temp::artifact_dir().unwrap()
}

/// Creates a fresh artifact tempdir and opens a `FileSystemCas` on it.
///
/// Returns the tempdir guard alongside the CAS so the directory outlives
/// every operation performed during the test.
pub(crate) async fn open_file_cas() -> (tempfile::TempDir, mediapm_cas::FileSystemCas) {
    let dir = artifact_dir();
    let cas = mediapm_cas::FileSystemCas::open(dir.path()).await.unwrap();
    (dir, cas)
}

/// Creates a fresh artifact tempdir and opens a `FileSystemCas` whose
/// background WAL consumer runs every `bg_interval`.
pub(crate) async fn open_file_cas_with_background(
    bg_interval: Duration,
) -> (tempfile::TempDir, mediapm_cas::FileSystemCas) {
    let dir = artifact_dir();
    let cas = mediapm_cas::FileSystemCas::open_with_strategies_and_interval(
        dir.path(),
        vec![],
        bg_interval,
    )
    .await
    .unwrap();
    (dir, cas)
}
