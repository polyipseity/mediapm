use std::collections::BTreeSet;

use bytes::Bytes;

use mediapm_cas::Hash;
use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi, ConstraintPatch};
use mediapm_cas::error::CasError;
use mediapm_cas::new_in_memory_cas;

use crate::common::{open_file_cas, put_static};

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
    let hash = Hash::from_content(b"nothing");
    let retrieved = cas.get_constraint(hash).await.unwrap();
    assert!(retrieved.is_empty());
}

#[tokio::test]
async fn patch_constraint_add() {
    let cas = new_in_memory_cas();
    let b1 = put_static(&cas, b"base1").await;
    let b2 = put_static(&cas, b"base2").await;
    let target = put_static(&cas, b"target").await;

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
    let b1 = put_static(&cas, b"base1").await;
    let b2 = put_static(&cas, b"base2").await;
    let target = put_static(&cas, b"target").await;

    cas.set_constraint(target, [b1, b2].into()).await.unwrap();
    let patch =
        ConstraintPatch { clear: false, add_bases: BTreeSet::new(), remove_bases: [b1].into() };
    cas.patch_constraint(target, patch).await.unwrap();

    let bases = cas.get_constraint(target).await.unwrap();
    assert!(!bases.contains(&b1));
    assert!(bases.contains(&b2));
}

/// set/get constraint round-trip where the constraint value contains
/// `Hash::empty()` (the empty-content sentinel) — kept from the former
/// `put_get_test` file, which duplicated coverage now in `api_workflows`.
#[tokio::test]
async fn set_and_get_empty_constraint() {
    let cas = new_in_memory_cas();

    let data = Bytes::from("test data");
    let hash = cas.put(data.clone()).await.unwrap();

    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved, data);

    cas.set_constraint(hash, BTreeSet::from([Hash::empty()])).await.unwrap();

    let constraint = cas.get_constraint(hash).await.unwrap();
    assert_eq!(constraint, BTreeSet::from([Hash::empty()]));
}

#[tokio::test]
async fn patch_constraint_clear() {
    let cas = new_in_memory_cas();
    let b1 = put_static(&cas, b"base1").await;
    let target = put_static(&cas, b"target").await;

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
    let target = put_static(&cas, b"self").await;
    let result = cas.set_constraint(target, [target].into()).await;
    assert!(result.is_err(), "constraint target cannot be its own base");
}

/// Per-base pruning: deleting one base leaves the other bases intact.
#[tokio::test]
async fn prune_one_base_preserves_others() {
    let cas = new_in_memory_cas();
    let target = put_static(&cas, b"target").await;
    let b1 = put_static(&cas, b"base1").await;
    let b2 = put_static(&cas, b"base2").await;
    let b3 = put_static(&cas, b"base3").await;

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
    let target = put_static(&cas, b"target").await;
    let base = put_static(&cas, b"base").await;

    cas.set_constraint(target, [base].into()).await.unwrap();
    cas.delete(target).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    assert!(
        cas.get_constraint(target).await.unwrap().is_empty(),
        "constraint entry should be removed -> empty set"
    );
}

/// Deleting a target leaves no snapshot behind that brings the constraint back.
///
/// A delete names its own hash, so it removes that hash's facts rather than the
/// directories the store believes are occupied. The harm this rules out is a
/// restart, not the current run: a snapshot still holding the deleted target's
/// constraint would be overlaid on the reloaded store, and a fact about an
/// object nobody holds would reappear.
#[tokio::test]
async fn a_deleted_target_leaves_no_constraint_behind_for_the_next_run() {
    let cas = open_file_cas().await;
    let target = put_static(&*cas, b"target-deleted").await;
    let base = put_static(&*cas, b"base-for-delete").await;

    cas.set_constraint(target, [base].into()).await.unwrap();
    cas.delete(target).await.unwrap();

    let reopened = cas.reopen().await;
    assert!(
        reopened.get_constraint(target).await.unwrap().is_empty(),
        "a deleted target's constraint must not survive a reopen",
    );
}

/// Per-base pruning: deleting all bases leaves constraint with empty bases
/// (no effective constraint = any base or full allowed).
#[tokio::test]
async fn prune_all_bases_leaves_empty_entry() {
    let cas = new_in_memory_cas();
    let target = put_static(&cas, b"target").await;
    let base = put_static(&cas, b"base").await;

    cas.set_constraint(target, [base].into()).await.unwrap();
    cas.delete(base).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let bases = cas.get_constraint(target).await.unwrap();
    assert!(bases.is_empty(), "all bases pruned -> empty effective set, constraint entry removed");
}

/// Constraint written to WAL (write-back) is visible via WAL fallback
/// before the WAL consumer materializes it into the index.
///
/// Uses `FileSystemCas` because `FileSystemMetadataStore` has `SYNC_MATERIALIZE =
/// false`, so constraints are WAL-only until consumed.
#[tokio::test]
async fn get_constraint_wal_fallback_before_consumption() {
    let cas = open_file_cas().await;

    let base = put_static(&*cas, b"base").await;
    let target = put_static(&*cas, b"target").await;

    // set_constraint writes to WAL. With FileSystemMetadataStore (SYNC_MATERIALIZE =
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

/// A constraint is refused for a target the store does not hold.
///
/// A constraint says how to rebuild one object from other objects. Recorded
/// for a hash the store never held, it is a fact with no subject: the prune
/// drops it, and until then a caller reads a recipe for bytes that are not
/// there. The refusal names the hash and the rule rather than storing the fact
/// and leaving the prune to explain it later.
#[tokio::test]
async fn set_constraint_refuses_a_target_the_store_does_not_hold() {
    let cas = new_in_memory_cas();
    let base = put_static(&cas, b"base").await;
    let absent = Hash::from_content(b"never stored");

    let refused = cas.set_constraint(absent, [base].into()).await;

    match refused {
        Err(CasError::InvalidArgument(message)) => {
            assert!(
                message.contains(&absent.to_hex()),
                "the refusal must name the hash it refuses: {message}"
            );
            assert!(
                message.contains("not held"),
                "the refusal must say what is wrong with the target: {message}"
            );
        }
        other => panic!("a constraint on an unheld target must be refused, got {other:?}"),
    }
    assert!(
        cas.get_constraint(absent).await.unwrap().is_empty(),
        "a refused constraint must not be recorded",
    );
}

/// A patch is refused for a target the store does not hold, because it records
/// a constraint by another name.
#[tokio::test]
async fn patch_constraint_refuses_a_target_the_store_does_not_hold() {
    let cas = new_in_memory_cas();
    let base = put_static(&cas, b"base").await;
    let absent = Hash::from_content(b"never stored");
    let patch =
        ConstraintPatch { clear: false, add_bases: [base].into(), remove_bases: BTreeSet::new() };

    let refused = cas.patch_constraint(absent, patch).await;

    match refused {
        Err(CasError::InvalidArgument(message)) => {
            assert!(
                message.contains(&absent.to_hex()),
                "the refusal must name the hash it refuses: {message}"
            );
            assert!(
                message.contains("not held"),
                "the refusal must say what is wrong with the target: {message}"
            );
        }
        other => panic!("a patch on an unheld target must be refused, got {other:?}"),
    }
    assert!(
        cas.get_constraint(absent).await.unwrap().is_empty(),
        "a refused patch must not record a constraint",
    );
}

/// A target that only holds pending WAL bytes counts as held, because the read
/// path answers it.
///
/// The filesystem store materializes metadata asynchronously, so a `put` that
/// returned is not yet in the index. Refusing that target would make the
/// ordinary sequence — put, then record how it was encoded — fail until a
/// maintenance pass ran.
#[tokio::test]
async fn set_constraint_accepts_a_target_pending_in_the_wal() {
    let cas = open_file_cas().await;
    let base = put_static(&*cas, b"base").await;
    let target = put_static(&*cas, b"target").await;

    cas.set_constraint(target, [base].into())
        .await
        .expect("a target whose bytes are pending in the WAL is held");
}

/// A target the guard accepted survives the prune that follows it.
///
/// The guard asks `stat`, whose answer includes a put still pending in the WAL,
/// while both prune paths ask `list_hashes()`. A target accepted through the
/// pending-WAL arm would be dropped by an immediate prune, which would make the
/// acceptance a fact that lasts until the next sweep. This is the test that
/// keeps the two answers about the same target in step.
#[tokio::test]
async fn a_target_accepted_with_pending_bytes_survives_a_prune() {
    let cas = open_file_cas().await;
    let base = put_static(&*cas, b"base").await;
    let target = put_static(&*cas, b"target-for-prune").await;

    cas.set_constraint(target, [base].into())
        .await
        .expect("a target whose bytes are pending in the WAL is held");
    cas.prune_constraints().await.expect("prune constraints");

    let bases = cas.get_constraint(target).await.expect("read the constraint back");
    assert_eq!(
        bases,
        [base].into(),
        "a constraint accepted for a held target must survive the prune that follows it",
    );
}
