//! End-to-end lifecycle test for optimize + reconstruct flow.
//!
//! Validates that applying constraints and running optimization never changes
//! user-visible bytes for the optimized target object.

use bytes::Bytes;
use mediapm_cas::{CasApi, CasMaintenanceApi, Constraint, FileSystemCas, OptimizeOptions};
use std::collections::BTreeSet;
use tempfile::tempdir;

use super::run_with_15s_timeout;

#[tokio::test]
async fn e2e_optimizer_flow_preserves_reconstructability() {
    run_with_15s_timeout(async {
        // Arrange
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas");

        let base = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
            .await
            .expect("put base");

        let v2 = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
            .await
            .expect("put v2");

        cas.set_constraint(Constraint { target_hash: v2, potential_bases: BTreeSet::from([base]) })
            .await
            .expect("set constraint");

        // Act
        let optimize = cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");
        let restored = cas.get(v2).await.expect("get v2");

        // Assert
        assert_eq!(
            restored,
            Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC")
        );
        assert!(optimize.rewritten_objects <= 1);
    })
    .await;
}
