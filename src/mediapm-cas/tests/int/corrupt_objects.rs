//! Corruption recording, reporting, and clearing against a filesystem store.

use bytes::Bytes;

use crate::common;
use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi};
use mediapm_cas::hash::Hash;
use std::collections::BTreeSet;

/// Both forms of a target are made unsound, which is the one state maintenance
/// cannot repair. The store must remember it rather than rediscover it.
#[tokio::test]
async fn maintenance_records_a_hash_whose_every_form_is_corrupt() {
    let cas = common::open_file_cas_verifying().await;
    let base = cas.put(Bytes::from(common::similar(b'F', b"CHANGED"))).await.unwrap();
    let target = cas.put(Bytes::from(common::similar(b'F', b"REPLACED"))).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let (full, delta) = common::form_paths(&cas, target);
    // The delta is a committed read-only CAS object; clear the read-only bit
    // before overwriting, same as the common::corrupt helper.
    let mut delta_perms = std::fs::metadata(&delta).expect("delta metadata").permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        delta_perms.set_mode(delta_perms.mode() | 0o200);
    }
    #[cfg(windows)]
    {
        #[expect(
            clippy::permissions_set_readonly_false,
            reason = "clearing the read-only attribute is how a test tampers with a committed blob"
        )]
        delta_perms.set_readonly(false);
    }
    std::fs::set_permissions(&delta, delta_perms).expect("make the delta payload writable");
    std::fs::write(&full, b"neither of these").expect("make the full payload wrong");
    std::fs::write(&delta, b"is the target's bytes").expect("make the delta payload wrong");

    let report = cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(report.corrupted_objects, 1, "the pass must report the hash it recorded");
    assert_eq!(cas.list_corrupted_objects().await.unwrap(), vec![target]);
}

/// A later repair has to clear the record, or the report keeps naming a hash
/// that now reads correctly.
#[tokio::test]
async fn a_repaired_form_clears_the_corruption_record() {
    let cas = common::open_file_cas_verifying().await;
    let base = cas.put(Bytes::from(common::similar(b'G', b"CHANGED"))).await.unwrap();
    let expected = common::similar(b'G', b"REPLACED");
    let target = cas.put(Bytes::from(expected.clone())).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let (full, delta) = common::form_paths(&cas, target);
    common::corrupt(&full);
    common::corrupt(&delta);
    assert_eq!(cas.run_maintenance_cycle().await.unwrap().corrupted_objects, 1);

    std::fs::write(&full, &expected).expect("restore the full payload");
    let report = cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(report.corrupted_objects, 0, "the repaired hash must stop being reported");
    assert!(
        cas.list_corrupted_objects().await.unwrap().is_empty(),
        "a sound form clears the record"
    );
}

/// The record survives a restart, because a hash that is corrupt now is still
/// corrupt after the process that found it exits.
#[tokio::test]
async fn the_corruption_record_survives_a_restart() {
    let dir = common::artifact_dir();
    let target = {
        let cas = common::open_file_cas_verifying_at(dir.path()).await;
        let base = cas.put(Bytes::from(common::similar(b'H', b"CHANGED"))).await.unwrap();
        let target = cas.put(Bytes::from(common::similar(b'H', b"REPLACED"))).await.unwrap();
        cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
        cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
        cas.run_maintenance_cycle().await.unwrap();

        let (full, delta) = common::form_paths(&cas, target);
        common::corrupt(&full);
        common::corrupt(&delta);
        assert_eq!(cas.run_maintenance_cycle().await.unwrap().corrupted_objects, 1);
        target
    };

    // The first store is dropped, which is the restart: a second store over the
    // same root rebuilds from the snapshots the first one flushed.
    let reopened = common::open_file_cas_verifying_at(dir.path()).await;
    assert_eq!(
        reopened.list_corrupted_objects().await.unwrap(),
        vec![target],
        "the snapshot must carry the record across a restart"
    );
}

/// A read that finds no sound physical form must record the hash, so the
/// corruption report reflects reads as well as maintenance passes.
#[tokio::test]
async fn a_sole_corrupt_form_read_records_corruption() {
    let cas = common::open_file_cas_verifying().await;
    let expected = b"sole corrupt form";
    let hash = cas.put(Bytes::from_static(expected)).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let (full, delta) = common::form_paths(&cas, hash);
    assert!(!delta.exists(), "fresh put should be full-only");
    common::corrupt(&full);

    let err = cas.get(hash).await.unwrap_err();
    assert!(
        matches!(err, mediapm_cas::CasError::CorruptObject { .. }),
        "corrupt sole form must fail, got {err:?}"
    );
    assert_eq!(
        cas.list_corrupted_objects().await.unwrap(),
        vec![hash],
        "a sole corrupt form read must record corruption"
    );
}

/// A corrupt indexed form with a sound other form is a read-time repair: the
/// read succeeds from the verified fallback and the corruption record clears.
#[tokio::test]
async fn a_verified_fallback_read_clears_corruption_record() {
    let cas = common::open_file_cas_verifying().await;
    let base = cas.put(Bytes::from(common::similar(b'R', b"CHANGED"))).await.unwrap();
    let expected = common::similar(b'R', b"REPLACED");
    let target = cas.put(Bytes::from(expected.clone())).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let (full, delta) = common::form_paths(&cas, target);
    common::corrupt(&full);
    common::corrupt(&delta);
    let report = cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(report.corrupted_objects, 1, "precondition: hash recorded as corrupt");
    std::fs::write(&full, &expected).expect("restore full payload");

    let read = cas.get(target).await.unwrap();
    assert_eq!(read.as_ref(), expected.as_slice(), "fallback read must return correct bytes");
    assert!(
        cas.list_corrupted_objects().await.unwrap().is_empty(),
        "verified fallback must clear corruption record"
    );
}

/// A missing indexed form with a sound other form is also a verified read-time
/// recovery: the read succeeds and clears a stale corruption record.
#[tokio::test]
async fn a_verified_fallback_from_missing_indexed_form_clears_corruption_record() {
    let cas = common::open_file_cas_verifying().await;
    let base = cas.put(Bytes::from(common::similar(b'M', b"CHANGED"))).await.unwrap();
    let expected = common::similar(b'M', b"REPLACED");
    let target = cas.put(Bytes::from(expected.clone())).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let (full, delta) = common::form_paths(&cas, target);
    let delta_bytes = std::fs::read(&delta).expect("read delta before corruption");
    common::corrupt(&full);
    common::corrupt(&delta);
    let report = cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(report.corrupted_objects, 1, "precondition: hash recorded as corrupt");

    // Remove whichever form is indexed and keep the other sound. If the indexed
    // form was delta, the restored delta read is a plain unverified Ok(Some)
    // and must not clear corruption; in that case remove delta and restore full
    // so the missing-indexed path exercises the verified-fallback clearing.
    std::fs::write(&delta, &delta_bytes).expect("restore delta payload");
    std::fs::remove_file(&full).expect("remove full form");

    let read = cas.get(target).await.unwrap();
    assert_eq!(read.as_ref(), expected.as_slice(), "first recovery must return correct bytes");
    if !cas.list_corrupted_objects().await.unwrap().is_empty() {
        std::fs::write(&full, &expected).expect("restore full payload");
        std::fs::remove_file(&delta).expect("remove delta form");
        let read = cas.get(target).await.unwrap();
        assert_eq!(read.as_ref(), expected.as_slice(), "second recovery must return correct bytes");
    }

    assert!(
        cas.list_corrupted_objects().await.unwrap().is_empty(),
        "verified fallback from missing indexed form must clear corruption record"
    );
}
