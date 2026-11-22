//! The four blob-form rules, asserted against the tree of one file-system store.
//!
//! Neither form means the data does not exist; either form means it does; both
//! forms are legitimate only while a transition is in flight and must converge
//! toward the recorded intent. These are properties of the tree, so the tests
//! look at the tree rather than trusting the index alone.

use std::collections::BTreeSet;

use bytes::Bytes;

use mediapm_cas::Hash;
use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi, ObjectEncoding};

use crate::common;

/// The full-blob path and the delta-blob path for one hash.
fn form_paths(
    cas: &common::FileCasFixture,
    hash: Hash,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let full = cas.object_path_for_hash(hash).expect("a file-system store exposes object paths");
    let mut delta = full.clone();
    delta.set_extension(full.extension().map_or_else(
        || std::ffi::OsString::from("diff"),
        |extension| {
            let mut suffix = extension.to_os_string();
            suffix.push(".diff");
            suffix
        },
    ));
    (full, delta)
}

/// Two similar buffers with one marker replaced, large enough that VCDIFF finds
/// a payload worth storing as a delta.
fn similar(fill: u8, marker: &[u8]) -> Vec<u8> {
    let mut buffer = vec![fill; 4096];
    let start = buffer.len() - marker.len();
    buffer[start..].copy_from_slice(marker);
    buffer
}

/// An object stored once has exactly one form, and that form is full.
#[tokio::test]
async fn a_freshly_stored_object_has_one_form() {
    let cas = common::open_file_cas().await;
    let hash = cas.put(Bytes::from_static(b"only one form")).await.unwrap();
    cas.ensure_blob_materialized(hash).await.unwrap();
    let (full, delta) = form_paths(&cas, hash);
    assert!(full.is_file(), "a fresh object is written full");
    assert!(!delta.is_file(), "a fresh object has no delta payload");
}

/// A pinned-full target must be full-encoded the instant `set_constraint`
/// returns, before any maintenance pass runs. This is the hard half of the
/// constraint contract.
#[tokio::test]
async fn pinning_full_is_effective_without_a_maintenance_pass() {
    let cas = common::open_file_cas().await;
    let base = cas.put(Bytes::from(similar(b'A', b"CHANGED"))).await.unwrap();
    let target = cas.put(Bytes::from(similar(b'A', b"OTHERED"))).await.unwrap();

    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();
    assert!(
        matches!(cas.stat(target).await.unwrap().encoding, ObjectEncoding::Delta { .. }),
        "the setup requires the target to be delta-encoded first"
    );

    cas.set_constraint(target, BTreeSet::from([Hash::empty()])).await.unwrap();

    let (full, delta) = form_paths(&cas, target);
    assert!(full.is_file(), "a pinned-full target must have its full form immediately");
    assert!(!delta.is_file(), "the now-redundant delta payload must be gone");
    assert_eq!(
        cas.stat(target).await.unwrap().encoding,
        ObjectEncoding::Full,
        "the index must agree with the tree without a maintenance pass"
    );
    assert_eq!(cas.get(target).await.unwrap(), Bytes::from(similar(b'A', b"OTHERED")));
}
