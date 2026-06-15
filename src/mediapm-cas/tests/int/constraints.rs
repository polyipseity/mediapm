use std::collections::BTreeSet;

use bytes::Bytes;
use tempfile::tempdir;

use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi, ConstraintPatch};
use mediapm_cas::new_in_memory_cas;

#[tokio::test]
async fn set_and_get_constraint() {
    let cas = new_in_memory_cas();
    // Put a base object.
    let base_data = Bytes::from_static(b"base");
    let base_hash = cas.put(base_data).await.unwrap();
    // Put a delta target.
    let target_data = Bytes::from_static(b"target");
    let target_hash = cas.put(target_data).await.unwrap();

    let bases: BTreeSet<_> = [base_hash].into();
    cas.set_constraint(target_hash, bases.clone()).await.unwrap();

    let retrieved = cas.get_constraint(target_hash).await.unwrap();
    assert_eq!(retrieved, bases);
}

#[tokio::test]
async fn get_constraint_missing() {
    let cas = new_in_memory_cas();
    let hash = mediapm_cas::Hash::from_content(b"nothing");
    let retrieved = cas.get_constraint(hash).await.unwrap();
    assert!(retrieved.is_empty());
}

#[tokio::test]
async fn patch_constraint_add() {
    let cas = new_in_memory_cas();
    let b1 = cas.put(Bytes::from_static(b"base1")).await.unwrap();
    let b2 = cas.put(Bytes::from_static(b"base2")).await.unwrap();
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();

    cas.set_constraint(target, [b1].into()).await.unwrap();
    let patch =
        ConstraintPatch { clear: false, add_bases: [b2].into(), remove_bases: BTreeSet::new() };
    cas.patch_constraint(target, patch).await.unwrap();

    let bases = cas.get_constraint(target).await.unwrap();
    assert!(bases.contains(&b1));
    assert!(bases.contains(&b2));
}

#[tokio::test]
async fn patch_constraint_remove() {
    let cas = new_in_memory_cas();
    let b1 = cas.put(Bytes::from_static(b"base1")).await.unwrap();
    let b2 = cas.put(Bytes::from_static(b"base2")).await.unwrap();
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();

    cas.set_constraint(target, [b1, b2].into()).await.unwrap();
    let patch =
        ConstraintPatch { clear: false, add_bases: BTreeSet::new(), remove_bases: [b1].into() };
    cas.patch_constraint(target, patch).await.unwrap();

    let bases = cas.get_constraint(target).await.unwrap();
    assert!(!bases.contains(&b1));
    assert!(bases.contains(&b2));
}

#[tokio::test]
async fn patch_constraint_clear() {
    let cas = new_in_memory_cas();
    let b1 = cas.put(Bytes::from_static(b"base1")).await.unwrap();
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();

    cas.set_constraint(target, [b1].into()).await.unwrap();
    let patch =
        ConstraintPatch { clear: true, add_bases: BTreeSet::new(), remove_bases: BTreeSet::new() };
    cas.patch_constraint(target, patch).await.unwrap();

    let bases = cas.get_constraint(target).await.unwrap();
    assert!(bases.is_empty());
}

#[tokio::test]
async fn set_constraint_rejects_self_base() {
    let cas = new_in_memory_cas();
    let target = cas.put(Bytes::from_static(b"self")).await.unwrap();
    let result = cas.set_constraint(target, [target].into()).await;
    assert!(result.is_err(), "constraint target cannot be its own base");
}

/// Per-base pruning: deleting one base leaves the other bases intact.
#[tokio::test]
async fn prune_one_base_preserves_others() {
    let cas = new_in_memory_cas();
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();
    let b1 = cas.put(Bytes::from_static(b"base1")).await.unwrap();
    let b2 = cas.put(Bytes::from_static(b"base2")).await.unwrap();
    let b3 = cas.put(Bytes::from_static(b"base3")).await.unwrap();

    // Constraint with all three bases.
    cas.set_constraint(target, [b1, b2, b3].into()).await.unwrap();

    // Delete b2.
    cas.delete(b2).await.unwrap();

    // After run_maintenance_cycle (WAL consumer + maintenance), the constraint should
    // still exist with only {b1, b3} — b2 was pruned individually.
    cas.run_maintenance_cycle().await.unwrap();

    let bases = cas.get_constraint(target).await.unwrap();
    assert!(bases.contains(&b1), "b1 should remain");
    assert!(bases.contains(&b3), "b3 should remain");
    assert!(!bases.contains(&b2), "b2 should be pruned");
    assert_eq!(bases.len(), 2, "exactly two bases should remain");
}

/// Per-base pruning: deleting the target removes the entire constraint.
#[tokio::test]
async fn prune_target_removes_entire_entry() {
    let cas = new_in_memory_cas();
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();
    let base = cas.put(Bytes::from_static(b"base")).await.unwrap();

    cas.set_constraint(target, [base].into()).await.unwrap();
    cas.delete(target).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    assert!(
        cas.get_constraint(target).await.unwrap().is_empty(),
        "constraint entry should be removed -> empty set"
    );
}

/// Per-base pruning: deleting all bases leaves constraint with empty bases
/// (no effective constraint = any base or full allowed).
#[tokio::test]
async fn prune_all_bases_leaves_empty_entry() {
    let cas = new_in_memory_cas();
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();
    let base = cas.put(Bytes::from_static(b"base")).await.unwrap();

    cas.set_constraint(target, [base].into()).await.unwrap();
    cas.delete(base).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let bases = cas.get_constraint(target).await.unwrap();
    assert!(bases.is_empty(), "all bases pruned -> empty effective set, constraint entry removed");
}

/// Constraint written to WAL (write-back) is visible via WAL fallback
/// before the WAL consumer materializes it into the index.
///
/// Uses `FileSystemCas` because `FileSystemMetadata` has `SYNC_MATERIALIZE =
/// false`, so constraints are WAL-only until consumed.
#[tokio::test]
async fn get_constraint_wal_fallback_before_consumption() {
    let dir = tempdir().unwrap();
    let cas =
        mediapm_cas::FileSystemCas::open_with_strategies(dir.path(), Vec::new()).await.unwrap();

    let base = cas.put(Bytes::from_static(b"base")).await.unwrap();
    let target = cas.put(Bytes::from_static(b"target")).await.unwrap();

    // set_constraint writes to WAL. With FileSystemMetadata (SYNC_MATERIALIZE =
    // false), it does NOT write to index yet.
    cas.set_constraint(target, [base].into()).await.unwrap();

    // get_constraint must find the constraint through WAL fallback.
    let bases = cas.get_constraint(target).await.unwrap();
    assert_eq!(bases.len(), 1, "WAL fallback should return the constraint");
    assert!(bases.contains(&base), "WAL fallback should contain the base");

    // After maintenance consumes the WAL, the constraint should be
    // materialized in the index and still visible.
    cas.run_maintenance_cycle().await.unwrap();
    let bases = cas.get_constraint(target).await.unwrap();
    assert_eq!(bases.len(), 1, "materialized constraint still visible");
    assert!(bases.contains(&base), "materialized constraint still contains base");
}
