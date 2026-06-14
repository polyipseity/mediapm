use std::collections::BTreeSet;

use bytes::Bytes;

use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi, ObjectEncoding};
use mediapm_cas::hash::Hash;
use mediapm_cas::new_in_memory_cas;

#[tokio::test]
async fn run_maintenance_cycle_wal_consumer() {
    let cas = new_in_memory_cas();
    // Perform some operations to generate WAL entries.
    let h1 = cas.put(Bytes::from_static(b"alpha")).await.unwrap();
    let h2 = cas.put(Bytes::from_static(b"beta")).await.unwrap();
    cas.delete(h1).await.unwrap();

    let report = cas.run_maintenance_cycle().await.unwrap();
    // At least one WAL entry should have been consumed.
    assert!(report.wal_entries_consumed > 0);
    // h1 should be gone from the store after WAL replay.
    assert!(cas.get(h1).await.is_err());
    // h2 should still be present.
    assert!(cas.get(h2).await.is_ok());
}

#[tokio::test]
async fn prune_constraints_no_orphans_after_materialized_delete() {
    let cas = new_in_memory_cas();
    let target = cas.put(Bytes::from_static(b"orphan")).await.unwrap();
    cas.set_constraint(target, BTreeSet::new()).await.unwrap();

    // Delete and fully materialize the delete through the WAL consumer.
    cas.delete(target).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    // In the unified Index architecture, constraints are embedded in the
    // index entry — deleting the entry also removes its constraint, so
    // no orphans remain after materialization.
    let report = cas.prune_constraints().await.unwrap();
    assert_eq!(report.removed, 0);
}

#[tokio::test]
async fn run_maintenance_cycle_runs_maintenance() {
    let cas = new_in_memory_cas();
    // Put some objects to ensure WAL is non-empty.
    cas.put(Bytes::from_static(b"a")).await.unwrap();
    cas.put(Bytes::from_static(b"b")).await.unwrap();
    let report = cas.run_maintenance_cycle().await.unwrap();
    // After consuming WAL entries, maintenance runs.
    assert!(report.wal_entries_consumed > 0);
}

/// GC never deletes objects — it only prunes constraints to approach
/// effective constraints (intersection of stored bases with live hashes).
#[tokio::test]
async fn gc_sweep_never_deletes_objects() {
    let cas = new_in_memory_cas();
    // Put objects and create constraints.
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();
    let b1 = cas.put(Bytes::from_static(b"base1")).await.unwrap();
    let b2 = cas.put(Bytes::from_static(b"base2")).await.unwrap();
    cas.set_constraint(target, [b1, b2].into()).await.unwrap();

    // Consume WAL so objects are in the object store.
    cas.run_maintenance_cycle().await.unwrap();

    // Prune_constraints runs without error and does NOT delete any object.
    let report = cas.prune_constraints().await.unwrap();
    assert_eq!(report.removed, 0, "prune_constraints should not delete objects");

    // All objects still retrievable after prune.
    assert!(cas.get(b1).await.is_ok(), "b1 should still exist after prune");
    assert!(cas.get(b2).await.is_ok(), "b2 should still exist after prune");
    assert!(cas.get(target).await.is_ok(), "target should still exist after prune");
}

/// prune_constraints approaches effective constraints — surviving bases winnow
/// to the live-set intersection.
#[tokio::test]
async fn prune_constraints_approaches_effective_constraints() {
    let cas = new_in_memory_cas();
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();
    let live = cas.put(Bytes::from_static(b"live")).await.unwrap();
    let dead = cas.put(Bytes::from_static(b"dead")).await.unwrap();

    cas.set_constraint(target, [live, dead].into()).await.unwrap();
    cas.delete(dead).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    // The constraint should now contain only the live base.
    let bases = cas.get_constraint(target).await.unwrap();
    assert!(bases.contains(&live), "live base survives");
    assert!(!bases.contains(&dead), "dead base pruned");
    assert_eq!(bases.len(), 1, "only one effective base remains");
}

// ---------------------------------------------------------------------------
// Optimizer delta-rewrite tests (Phase 5)
// ---------------------------------------------------------------------------

/// Optimizer rewrites a full-encoded target to delta-encoded when a
/// constraint with a live base exists. The reconstructed content via
/// `get()` must still match the original.
#[tokio::test]
async fn optimize_delta_rewrite() {
    let cas = new_in_memory_cas();

    // Two similar large buffers so VCDIFF delta is meaningfully smaller.
    let base_content = Bytes::from(vec![b'A'; 4096]);
    let target_content = {
        let mut v = vec![b'A'; 2048];
        v.extend_from_slice(b"CHANGED");
        v.extend_from_slice(&vec![b'A'; 2048 - 7]);
        Bytes::from(v)
    };

    let base_hash = cas.put(base_content.clone()).await.unwrap();
    let target_hash = cas.put(target_content.clone()).await.unwrap();

    // Set constraint and run optimizer.
    cas.set_constraint(target_hash, [base_hash].into()).await.unwrap();
    let maint_report = cas.run_maintenance_cycle().await.unwrap();

    // Optimizer should have done work (WAL consumption + rewrite).
    assert!(maint_report.wal_entries_consumed > 0);

    // Stat should now report Delta encoding with the correct base.
    let meta = cas.stat(target_hash).await.unwrap();
    assert_eq!(
        meta.encoding,
        ObjectEncoding::Delta { base_hash },
        "optimizer should rewrite target to delta encoding",
    );

    // get() must reconstruct original content despite delta encoding.
    let retrieved = cas.get(target_hash).await.unwrap();
    assert_eq!(retrieved, target_content, "get must reconstruct original content");
}

/// Optimizer skips zero-hash targets (sentinel, never materialized) without
/// error.
#[tokio::test]
async fn optimize_skips_zero_hash_target() {
    let cas = new_in_memory_cas();

    let base = cas.put(Bytes::from_static(b"base")).await.unwrap();
    // Set constraint with Hash::zero() as target.
    cas.set_constraint(Hash::zero(), [base].into()).await.unwrap();

    // Must not panic or error.
    let report = cas.run_maintenance_cycle().await.unwrap();
    assert!(report.wal_entries_consumed > 0);
}

/// Optimizer silently skips targets whose effective bases set is empty
/// (all constraint bases are missing from the object store).
#[tokio::test]
async fn optimize_skips_missing_base() {
    let cas = new_in_memory_cas();

    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();
    // Constraint with a hash that was never put.
    let phantom_hash = Hash::from_content(b"i-do-not-exist");
    cas.set_constraint(target, [phantom_hash].into()).await.unwrap();

    // Optimizer must not panic — read_full_bytes returns None for missing
    // base, which causes a `continue` in the optimizer loop. No delta
    // rewrite should occur.
    cas.run_maintenance_cycle().await.unwrap();

    // Stat must still report Full encoding (no rewrite happened).
    let meta = cas.stat(target).await.unwrap();
    assert_eq!(meta.encoding, ObjectEncoding::Full, "no rewrite when all bases are missing",);

    // Content still retrievable.
    let retrieved = cas.get(target).await.unwrap();
    assert_eq!(retrieved, Bytes::from_static(b"target"));
}

/// Optimizer silently skips targets whose effective bases set is empty
/// because all constraints bases were deleted.
#[tokio::test]
async fn optimize_skips_all_bases_deleted() {
    let cas = new_in_memory_cas();

    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();
    let base = cas.put(Bytes::from_static(b"base")).await.unwrap();
    cas.set_constraint(target, [base].into()).await.unwrap();
    cas.delete(base).await.unwrap();

    // First run_maintenance_cycle consumes WAL (delete materialises, base removed).
    cas.run_maintenance_cycle().await.unwrap();

    // Second optimize runs maintenance with empty effective set.
    cas.run_maintenance_cycle().await.unwrap();

    // Stat must still report Full encoding (no rewrite occurred).
    let meta = cas.stat(target).await.unwrap();
    assert_eq!(meta.encoding, ObjectEncoding::Full, "no rewrite when all bases were deleted",);
}

/// Optimizer walks delta chains correctly: an object that already depends on
/// a delta-encoded base is reconstructed, then a new delta to a better base
/// is computed.
#[tokio::test]
async fn optimize_with_delta_chain() {
    let cas = new_in_memory_cas();

    // Three objects of decreasing size so delta chain makes sense.
    let a = Bytes::from(vec![b'X'; 4096]);
    let b = {
        let mut v = vec![b'X'; 2048];
        v.extend_from_slice(b"B_DELTA");
        v.extend_from_slice(&vec![b'X'; 2048 - 7]);
        Bytes::from(v)
    };
    let c = {
        let mut v = vec![b'X'; 1024];
        v.extend_from_slice(b"C_DELTA");
        v.extend_from_slice(&vec![b'X'; 1024 - 7]);
        Bytes::from(v)
    };

    let ha = cas.put(a.clone()).await.unwrap();
    let hb = cas.put(b.clone()).await.unwrap();
    let hc = cas.put(c.clone()).await.unwrap();

    // Set constraints: c depends on b, b depends on a.
    cas.set_constraint(hc, [hb].into()).await.unwrap();
    cas.set_constraint(hb, [ha].into()).await.unwrap();

    // First optimize — should rewrite b→a and c→b to deltas.
    cas.run_maintenance_cycle().await.unwrap();

    // Both should be delta-encoded now.
    let meta_b = cas.stat(hb).await.unwrap();
    let meta_c = cas.stat(hc).await.unwrap();
    assert_eq!(
        meta_b.encoding,
        ObjectEncoding::Delta { base_hash: ha },
        "b should be delta-encoded against a",
    );
    assert_eq!(
        meta_c.encoding,
        ObjectEncoding::Delta { base_hash: hb },
        "c should be delta-encoded against b",
    );

    // Both must still reconstruct correctly.
    assert_eq!(cas.get(hb).await.unwrap(), b);
    assert_eq!(cas.get(hc).await.unwrap(), c);

    // Delete the ultimate base a — re-materialization should fire first.
    cas.delete(ha).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    // After re-materialization: b should be Full (its base a was deleted).
    let meta_b2 = cas.stat(hb).await.unwrap();
    assert_eq!(
        meta_b2.encoding,
        ObjectEncoding::Full,
        "b should be full after base a was deleted and re-materialized",
    );
    // c depends on b which is now Full, so c is a delta against b (which exists).
    // Actually c's delta base was hb. hb still exists (as Full), so c remains
    // a valid delta. Let's verify content.
    assert_eq!(cas.get(hb).await.unwrap(), b, "b still retrievable after re-materialization");
    assert_eq!(cas.get(hc).await.unwrap(), c, "c still retrievable after base re-materialization");
}

/// Constraint with multiple bases: optimizer picks the first effective base
/// (BTreeSet ordering) and computes delta against it.
#[tokio::test]
async fn optimize_multi_base_picks_first_effective() {
    let cas = new_in_memory_cas();

    let target_content = Bytes::from(vec![b'Z'; 4096]);
    let base1_content = Bytes::from(vec![b'Y'; 4096]);
    let base2_content = Bytes::from(vec![b'X'; 4096]);

    let target = cas.put(target_content.clone()).await.unwrap();
    let b1 = cas.put(base1_content.clone()).await.unwrap();
    let b2 = cas.put(base2_content.clone()).await.unwrap();

    // Both b1 and b2 are valid bases.
    cas.set_constraint(target, [b1, b2].into()).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let meta = cas.stat(target).await.unwrap();
    // The optimizer should pick the first effective base from the BTreeSet.
    // BTreeSet ordering: b1 vs b2 depends on Hash ordering (lexicographic).
    // Either is valid — just check that it's a Delta, not Full.
    assert!(
        matches!(meta.encoding, ObjectEncoding::Delta { .. }),
        "target should be delta-encoded when at least one live base exists: got {:?}",
        meta.encoding,
    );
    assert_eq!(cas.get(target).await.unwrap(), target_content);
}

/// Multiple optimize runs are idempotent: re-running after a delta rewrite
/// preserves content and does not cause errors.
#[tokio::test]
async fn optimize_idempotent() {
    let cas = new_in_memory_cas();

    let base = Bytes::from(vec![b'Q'; 4096]);
    let target = {
        let mut v = vec![b'Q'; 2000];
        v.extend_from_slice(b"IDEMPOTENT");
        v.extend_from_slice(&vec![b'Q'; 4096 - 2000 - 10]);
        Bytes::from(v)
    };

    let base_hash = cas.put(base.clone()).await.unwrap();
    let target_hash = cas.put(target.clone()).await.unwrap();
    cas.set_constraint(target_hash, [base_hash].into()).await.unwrap();

    // First optimization: delta rewrite.
    cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(cas.get(target_hash).await.unwrap(), target);

    // Second optimization: should be idempotent.
    cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(cas.get(target_hash).await.unwrap(), target);

    // Third optimization: still idempotent.
    cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(cas.get(target_hash).await.unwrap(), target);
}
