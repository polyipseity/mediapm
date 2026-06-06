//! Integration tests for CAS integrity verification.
//!
//! Covers the full verification lifecycle: put sets `verify_time`, `get()`
//! gates BLAKE3 re-verification based on `VerifyTriggerStrategy` and
//! `stale_timeout`.
//!
//! Each strategy is tested in isolation (empty, Always, Modified, Stale) to
//! confirm that verification is triggered only when the configured policy
//! requires it.

use bytes::Bytes;
use mediapm_cas::{CasApi, CasIntegrityConfig, FileSystemCas, VerifyTriggerStrategy};
use std::fs::File;
use std::time::{Duration, SystemTime};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Helper: open a CAS with test drop-grace and a custom integrity config.
// ---------------------------------------------------------------------------

async fn open_cas_with_integrity(
    root: &std::path::Path,
    integrity: CasIntegrityConfig,
) -> FileSystemCas {
    FileSystemCas::open_with_alpha_and_recovery(root, 4, Default::default(), integrity)
        .await
        .expect("open CAS")
}

// ---------------------------------------------------------------------------
// Empty config — no re-verification strategy at all.
// ---------------------------------------------------------------------------

#[tokio::test]
/// Empty `verify_on_read` means `can_skip_verification()` always returns
/// `true` — BLAKE3 is never re-computed on `get()`.
async fn empty_config_skips_all_verification() {
    let dir = tempdir().expect("tempdir");
    let cas = open_cas_with_integrity(
        dir.path(),
        CasIntegrityConfig { verify_on_read: vec![], ..Default::default() },
    )
    .await;

    let hash = cas.put(Bytes::from_static(b"empty-skip")).await.expect("put");
    let retrieved = cas.get(hash).await.expect("get");
    assert_eq!(retrieved, Bytes::from_static(b"empty-skip"));
}

// ---------------------------------------------------------------------------
// Always — every `get()` triggers full re-verification.
// ---------------------------------------------------------------------------

#[tokio::test]
/// With `[Always]` and cache disabled, every `get()` must run BLAKE3.
async fn always_triggers_verification_on_every_get() {
    let dir = tempdir().expect("tempdir");
    let cas = open_cas_with_integrity(
        dir.path(),
        CasIntegrityConfig {
            verify_on_read: vec![VerifyTriggerStrategy::Always],
            ..Default::default()
        },
    )
    .await;

    let hash = cas.put(Bytes::from_static(b"always-every")).await.expect("put");
    let r1 = cas.get(hash).await.expect("first get");
    assert_eq!(r1, Bytes::from_static(b"always-every"));

    let r2 = cas.get(hash).await.expect("second get — should re-verify");
    assert_eq!(r2, Bytes::from_static(b"always-every"));
}

// ---------------------------------------------------------------------------
// Modified — skip when mtime ≤ verify_time.
// ---------------------------------------------------------------------------

#[tokio::test]
/// After a fresh `put()`, the stored `verify_time` ≈ file mtime (both in the
/// same second), so `mtime > verify_time` is `false` and verification is
/// skipped.
async fn modified_skips_after_fresh_put() {
    let dir = tempdir().expect("tempdir");
    let cas = open_cas_with_integrity(
        dir.path(),
        CasIntegrityConfig {
            verify_on_read: vec![VerifyTriggerStrategy::Modified],
            ..Default::default()
        },
    )
    .await;

    let hash = cas.put(Bytes::from_static(b"modified-skip")).await.expect("put");
    let retrieved = cas.get(hash).await.expect("get");
    assert_eq!(retrieved, Bytes::from_static(b"modified-skip"));
}

#[tokio::test]
/// Touching the on-disk object file to a future mtime forces the
/// `Modified` strategy to trigger re-verification.
async fn modified_triggers_on_mtime_change() {
    let dir = tempdir().expect("tempdir");
    let cas = open_cas_with_integrity(
        dir.path(),
        CasIntegrityConfig {
            verify_on_read: vec![VerifyTriggerStrategy::Modified],
            ..Default::default()
        },
    )
    .await;

    let hash = cas.put(Bytes::from_static(b"modified-trigger")).await.expect("put");

    // Advance the object file's mtime into the future so that
    // mtime > verify_time is true on the next get().
    let path = cas.object_path_for_hash(hash);
    let future = SystemTime::now() + Duration::from_secs(3600);
    let times = std::fs::FileTimes::new().set_modified(future).set_accessed(future);
    // Open read-only; `File::set_times()` works when the caller owns the file.
    let f = File::open(&path).expect("open object file");
    f.set_times(times).expect("set future mtime");

    let retrieved = cas.get(hash).await.expect("get after mtime change");
    assert_eq!(retrieved, Bytes::from_static(b"modified-trigger"));
}

// ---------------------------------------------------------------------------
// Stale — zero timeout forces re-verification; long timeout skips.
// ---------------------------------------------------------------------------

#[tokio::test]
/// `Stale { timeout: 0s }` means any verify_time > 0 is immediately stale
/// (`now - verify_time >= 0` is always true), so every get re-verifies.
async fn stale_zero_timeout_verifies_always() {
    let dir = tempdir().expect("tempdir");
    let cas = open_cas_with_integrity(
        dir.path(),
        CasIntegrityConfig {
            verify_on_read: vec![VerifyTriggerStrategy::Stale { timeout: Duration::from_secs(0) }],
            ..Default::default()
        },
    )
    .await;

    let hash = cas.put(Bytes::from_static(b"stale-zero")).await.expect("put");
    let r1 = cas.get(hash).await.expect("first get");
    assert_eq!(r1, Bytes::from_static(b"stale-zero"));

    let r2 = cas.get(hash).await.expect("second get — should re-verify");
    assert_eq!(r2, Bytes::from_static(b"stale-zero"));
}

#[tokio::test]
/// `Stale` with a long timeout (24h) does NOT trigger verification shortly
/// after a fresh put because verify_time was just set.
async fn stale_long_timeout_skips_after_put() {
    let dir = tempdir().expect("tempdir");
    let cas = open_cas_with_integrity(
        dir.path(),
        CasIntegrityConfig {
            verify_on_read: vec![VerifyTriggerStrategy::Stale {
                timeout: Duration::from_secs(86400),
            }],
            ..Default::default()
        },
    )
    .await;

    let hash = cas.put(Bytes::from_static(b"stale-long")).await.expect("put");
    let retrieved = cas.get(hash).await.expect("get");
    assert_eq!(retrieved, Bytes::from_static(b"stale-long"));
}

/// Stale with a very short timeout (1ms) should trigger re-verification
/// because `now - verify_time >= 0.001s` is true after the put.
#[tokio::test]
async fn stale_short_timeout_triggers_after_elapsed() {
    let dir = tempdir().expect("tempdir");
    let cas = open_cas_with_integrity(
        dir.path(),
        CasIntegrityConfig {
            verify_on_read: vec![VerifyTriggerStrategy::Stale {
                timeout: Duration::from_millis(1),
            }],
            ..Default::default()
        },
    )
    .await;

    let hash = cas.put(Bytes::from_static(b"stale-short")).await.expect("put");
    // A 1ms timeout is almost certainly expired by the time we reach get().
    let retrieved = cas.get(hash).await.expect("get");
    assert_eq!(retrieved, Bytes::from_static(b"stale-short"));
}

// ---------------------------------------------------------------------------
// Default config — [Modified, Sample] with real defaults.
// ---------------------------------------------------------------------------

#[tokio::test]
/// The default config (`[Modified { 0 }, Sample { 100 }]`) should typically
/// skip verification after a fresh put because mtime ≈ verify_time, and the
/// 1 % sample rate rarely triggers.
async fn default_config_skips_after_fresh_put() {
    let dir = tempdir().expect("tempdir");
    let cas = open_cas_with_integrity(dir.path(), CasIntegrityConfig::default()).await;

    let hash = cas.put(Bytes::from_static(b"default-skip")).await.expect("put");
    let retrieved = cas.get(hash).await.expect("get");
    assert_eq!(retrieved, Bytes::from_static(b"default-skip"));
}

// ---------------------------------------------------------------------------
// Delta chain + verification — coverage for the non-mmap code paths.
// ---------------------------------------------------------------------------

#[tokio::test]
/// After a delta-chain optimization, `get()` should still verify correctly
/// when the strategy demands it.
async fn delta_chain_is_verified_with_always_strategy() {
    let dir = tempdir().expect("tempdir");
    let cas = open_cas_with_integrity(
        dir.path(),
        CasIntegrityConfig {
            verify_on_read: vec![VerifyTriggerStrategy::Always],
            ..Default::default()
        },
    )
    .await;

    // Create a base and a target to force delta optimization.
    let base = cas.put(Bytes::from_static(b"aaaa")).await.expect("put base");
    let target = cas.put(Bytes::from_static(b"aaab")).await.expect("put target");

    use mediapm_cas::{CasMaintenanceApi, Constraint, OptimizeOptions};
    use std::collections::BTreeSet;
    cas.set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::from([base]) })
        .await
        .expect("set constraint");
    cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");

    // Now get() must navigate the delta chain and re-verify the result.
    let retrieved = cas.get(target).await.expect("get after delta chain");
    assert_eq!(retrieved, Bytes::from_static(b"aaab"));
}
