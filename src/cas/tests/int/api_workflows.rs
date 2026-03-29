//! Integration coverage for common CAS API workflows.
//!
//! Exercises store/get/constraint/maintenance round-trips as a user-facing
//! contract for the high-level API.

use bytes::Bytes;
use mediapm_cas::{CasApi, CasMaintenanceApi, Constraint, FileSystemCas, OptimizeOptions};
use std::collections::BTreeSet;
use tempfile::tempdir;

#[tokio::test]
async fn workflow_store_get_constraint_prune_roundtrip() {
    // Arrange
    let dir = tempdir().expect("tempdir");
    let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
    let base = cas.put(Bytes::from_static(b"workflow-base")).await.expect("put base");
    let target = cas.put(Bytes::from_static(b"workflow-target")).await.expect("put target");

    // Act
    cas.set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::from([base]) })
        .await
        .expect("set constraint");

    let optimize = cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");
    let prune = cas.prune_constraints().await.expect("prune");
    let restored = cas.get(target).await.expect("get target");

    // Assert
    assert_eq!(restored, Bytes::from_static(b"workflow-target"));
    assert!(optimize.rewritten_objects <= 1);
    assert_eq!(prune.removed_candidates, 0);
}
