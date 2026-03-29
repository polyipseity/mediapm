//! Integration tests for filesystem object encoding layout.
//!
//! Guards on-disk invariants for full-object and delta-object file placement.

use bytes::Bytes;
use mediapm_cas::OptimizeOptions;
use mediapm_cas::{CasApi, CasMaintenanceApi, Constraint, FileSystemCas};
use std::collections::BTreeSet;
use tempfile::tempdir;

#[tokio::test]
async fn full_objects_are_stored_as_data_only_without_headers() {
    // Arrange
    let dir = tempdir().expect("tempdir");
    let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
    let payload = Bytes::from_static(b"full-data-no-header");

    // Act
    let hash = cas.put(payload.clone()).await.expect("put payload");
    let bytes =
        tokio::fs::read(cas.object_path_for_hash(hash)).await.expect("read full object file");

    // Assert
    assert_eq!(bytes, payload);
    assert!(!cas.diff_path_for_hash(hash).exists());
}

#[tokio::test]
async fn diff_objects_use_dot_diff_extension_and_raw_path_absent() {
    // Arrange
    let dir = tempdir().expect("tempdir");
    let cas = FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas");

    let base = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
        .await
        .expect("put base");
    let target = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
        .await
        .expect("put target");

    cas.set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::from([base]) })
        .await
        .expect("set constraint");

    // Act
    let _ = cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");

    // Assert
    assert!(cas.diff_path_for_hash(target).exists());
    assert!(!cas.object_path_for_hash(target).exists());
}

#[tokio::test]
async fn object_paths_use_digest_hex_fanout_layout() {
    // Arrange
    let dir = tempdir().expect("tempdir");
    let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
    let hash = cas.put(Bytes::from_static(b"fanout-layout")).await.expect("put payload");

    // Act
    let path = cas.object_path_for_hash(hash);
    let digest_hex = hash.to_hex();
    let expected_first = &digest_hex[0..2];
    let expected_second = &digest_hex[2..4];
    let expected_rest = &digest_hex[4..];

    // Assert
    let components = path.iter().map(|part| part.to_string_lossy().to_string()).collect::<Vec<_>>();

    let n = components.len();
    assert!(n >= 5, "path should have at least 5 trailing components: {components:?}");
    assert_eq!(components[n - 5], "v1");
    assert_eq!(components[n - 4], hash.algorithm_name());
    assert_eq!(components[n - 3], expected_first);
    assert_eq!(components[n - 2], expected_second);
    assert_eq!(components[n - 1], expected_rest);
}
