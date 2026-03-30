//! Property tests for constraint pruning semantics.
//!
//! Verifies prune passes preserve valid candidate sets while maintaining the
//! implicit empty-content base rule.

use bytes::Bytes;
use mediapm_cas::{CasApi, CasMaintenanceApi, Constraint, FileSystemCas, Hash, empty_content_hash};
use proptest::prelude::*;
use std::collections::BTreeSet;
use tempfile::tempdir;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    /// Pruning should preserve all still-valid explicit candidates.
    fn prop_constraint_prune_preserves_existing_candidates(
        target_seed in prop::collection::vec(any::<u8>(), 1..128),
        candidate_seeds in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..64), 0..20)
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime build");

        runtime.block_on(async move {
            let temp = tempdir().expect("tempdir");
            let cas = FileSystemCas::open_for_tests(temp.path()).await.expect("open cas");
            let target = cas.put(Bytes::from(target_seed)).await.expect("put target");

            let mut candidates: BTreeSet<Hash> = BTreeSet::new();
            for seed in candidate_seeds {
                let hash = cas.put(Bytes::from(seed)).await.expect("put candidate");
                candidates.insert(hash);
            }
            let mut expected: BTreeSet<Hash> = candidates
                .into_iter()
                .filter(|hash| *hash != target)
                .collect();
            expected.insert(empty_content_hash());

            let set_result = cas
                .set_constraint(Constraint {
                    target_hash: target,
                    potential_bases: expected.clone(),
                })
                .await;

            if set_result.is_ok() {
                let _ = cas.prune_constraints().await.expect("prune constraints");
                let bases = cas.constraint_bases(target).await.expect("constraint bases");
                assert_eq!(bases, expected.into_iter().collect::<Vec<_>>());
            }
        });
    }
}
