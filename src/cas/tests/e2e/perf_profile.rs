//! End-to-end reconstruction latency profile tests.
//!
//! Provides coarse regression checks that deeper delta chains remain within
//! acceptable latency growth bounds.

use std::collections::BTreeSet;
use std::time::Instant;

use bytes::Bytes;
use mediapm_cas::{CasApi, CasMaintenanceApi, Constraint, FileSystemCas, OptimizeOptions};
use tempfile::tempdir;

use super::run_with_15s_timeout;

/// Builds a constrained chain and returns `(hashes, payload_len)` for leaf checks.
async fn build_chain(cas: &FileSystemCas, depth: usize) -> (Vec<mediapm_cas::Hash>, usize) {
    let mut hashes = Vec::with_capacity(depth);
    let mut previous_payload = vec![b'a'; 512];
    let first = cas.put(Bytes::from(previous_payload.clone())).await.expect("put chain root");
    hashes.push(first);

    for i in 1..depth {
        let idx = i % previous_payload.len();
        let digit = u8::try_from(i % 10).expect("mod 10 must fit in u8");
        previous_payload[idx] = b'0' + digit;
        let hash = cas.put(Bytes::from(previous_payload.clone())).await.expect("put chain node");

        cas.set_constraint(Constraint {
            target_hash: hash,
            potential_bases: BTreeSet::from([hashes[i - 1]]),
        })
        .await
        .expect("set constraint");

        hashes.push(hash);
    }

    let _ = cas.optimize_once(OptimizeOptions::default()).await.expect("optimize chain");
    (hashes, previous_payload.len())
}

#[tokio::test]
/// Asserts retrieval latency growth remains bounded for deeper delta chains.
async fn reconstruction_latency_profiles_chain_lengths_1_5_10() {
    run_with_15s_timeout(async {
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas");

        let mut samples = Vec::new();
        for depth in [1usize, 5usize, 10usize] {
            let (hashes, expected_len) = build_chain(&cas, depth).await;
            let leaf = *hashes.last().expect("leaf hash");
            let start = Instant::now();
            let restored = cas.get(leaf).await.expect("get leaf");
            let elapsed = start.elapsed();

            assert_eq!(restored.len(), expected_len);
            assert!(elapsed.as_secs_f64() < 2.0, "depth={depth} retrieval too slow: {elapsed:?}");
            samples.push((depth, elapsed));
        }

        // Sanity: deeper chains should not be orders of magnitude slower.
        let depth_1 = samples[0].1.as_secs_f64();
        let depth_10 = samples[2].1.as_secs_f64();
        assert!(
            depth_10 <= depth_1 * 20.0 + 0.5,
            "depth-10 retrieval regressed excessively: d1={depth_1:.6}s d10={depth_10:.6}s"
        );
    })
    .await;
}
