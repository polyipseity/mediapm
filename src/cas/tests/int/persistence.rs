//! Integration tests for persistence and reopen behavior.
//!
//! Confirms object bytes and fan-out layout survive process restarts.

use bytes::Bytes;
use mediapm_cas::{CasApi, FileSystemCas};
use tempfile::tempdir;

#[tokio::test]
async fn objects_remain_accessible_after_reopen() {
    // Arrange
    let dir = tempdir().expect("tempdir");
    let hash = {
        let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
        cas.put(Bytes::from_static(b"persist-me")).await.expect("put")
    };

    // Act
    let reopened = FileSystemCas::open_for_tests(dir.path()).await.expect("reopen cas");
    let restored = reopened.get(hash).await.expect("get");

    // Assert
    assert_eq!(restored, Bytes::from_static(b"persist-me"));
}

#[tokio::test]
async fn fanout_path_contains_algorithm_directory() {
    // Arrange
    let dir = tempdir().expect("tempdir");
    let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
    let hash = cas.put(Bytes::from_static(b"fanout")).await.expect("put");

    // Act
    let path = cas.object_path_for_hash(hash);

    // Assert
    assert!(path.exists());
    assert!(path.to_string_lossy().contains("blake3"));
}
