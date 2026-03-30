//! End-to-end high-load regression tests for filesystem CAS.
//!
//! These tests assert that bursty write/read and optimize workloads stay
//! reconstructable and complete within practical latency budgets.

use std::collections::BTreeSet;
use std::sync::OnceLock;
use std::time::Instant;

use bytes::Bytes;
use mediapm_cas::{CasApi, CasMaintenanceApi, Constraint, FileSystemCas, OptimizeOptions};
use tempfile::tempdir;
use tokio::sync::Mutex;

use super::run_with_15s_timeout;

/// Global mutex to serialize high-load tests and reduce noisy contention.
fn high_load_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
/// Verifies burst put/get workloads complete within bounded wall-clock time.
async fn high_load_store_and_sample_retrieve_completes_within_reasonable_time() {
    let _guard = high_load_test_lock().lock().await;

    run_with_15s_timeout(async {
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
        let total = 96usize;

        let start = Instant::now();
        let mut hashes = Vec::with_capacity(total);
        for i in 0..total {
            let byte = u8::try_from(i % 251).expect("mod 251 must fit in u8");
            let payload = vec![byte; 256 + (i % 32)];
            let hash = cas.put(Bytes::from(payload)).await.expect("put payload");
            hashes.push(hash);
        }

        for (idx, hash) in hashes.iter().enumerate().step_by(17) {
            let loaded = cas.get(*hash).await.expect("load payload");
            assert_eq!(loaded.len(), 256 + (idx % 32));
        }

        let elapsed = start.elapsed();
        assert!(elapsed.as_secs_f64() <= 15.0, "expected <=15s workload, got {elapsed:?}");
    })
    .await;
}

#[tokio::test]
/// Verifies optimize passes keep heavily constrained targets reconstructable.
async fn high_load_optimizer_pass_preserves_reconstructability() {
    let _guard = high_load_test_lock().lock().await;

    run_with_15s_timeout(async {
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas");

        let base = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
            .await
            .expect("put base");

        let total = 96usize;
        let mut targets = Vec::with_capacity(total);

        for i in 0..total {
            let mut payload = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC".to_vec();
            let digit = u8::try_from(i % 10).expect("mod 10 must fit in u8");
            payload[40 + (i % 8)] = b'0' + digit;
            let hash = cas.put(Bytes::from(payload)).await.expect("put target");
            cas.set_constraint(Constraint {
                target_hash: hash,
                potential_bases: BTreeSet::from([base]),
            })
            .await
            .expect("set constraint");
            targets.push(hash);
        }

        let report = cas.optimize_once(OptimizeOptions::default()).await.expect("optimize once");

        assert!(report.rewritten_objects <= total);
        for hash in targets.iter().step_by(11) {
            let loaded = cas.get(*hash).await.expect("reconstruct target");
            assert_eq!(loaded.len(), 48);
        }
    })
    .await;
}
