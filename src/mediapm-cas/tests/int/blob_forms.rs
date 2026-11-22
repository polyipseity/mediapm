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

/// Overwrite a committed payload with bytes that cannot be the right content.
///
/// Objects are committed read-only, so the write bit has to come back before the
/// file can be made wrong, which is exactly what an external tamper does.
/// `Permissions::set_readonly(false)` is the cross-platform spelling: it clears
/// the Windows attribute and grants the write bits on Unix, and the file it is
/// applied to lives in a temp directory either way.
///
/// A file that is not there is created corrupt: an interrupted promotion can
/// leave the full payload absent, and a test that stages that state must not
/// need a second helper to describe it.
fn corrupt(path: &std::path::Path) {
    match std::fs::metadata(path) {
        Ok(metadata) => {
            let mut permissions = metadata.permissions();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                permissions.set_mode(permissions.mode() | 0o200);
            }
            #[cfg(windows)]
            {
                #[expect(
                    clippy::permissions_set_readonly_false,
                    reason = "clearing the read-only attribute is how a Windows test tampers with a committed blob"
                )]
                permissions.set_readonly(false);
            }
            std::fs::set_permissions(path, permissions).expect("make the payload writable");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("payload metadata: {error}"),
    }
    std::fs::write(path, b"not the bytes this hash names").expect("corrupt the payload");
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

/// The optimizer must leave exactly one form behind: after a delta rewrite the
/// full payload is gone, not kept as a second copy of the same bytes.
#[tokio::test]
async fn a_delta_rewrite_reclaims_the_full_payload() {
    let cas = common::open_file_cas().await;
    let base = cas.put(Bytes::from(similar(b'A', b"CHANGED"))).await.unwrap();
    let target = cas.put(Bytes::from(similar(b'A', b"REPLACED"))).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();

    cas.run_maintenance_cycle().await.unwrap();

    let (full, delta) = form_paths(&cas, target);
    assert!(delta.is_file(), "the delta payload must exist after a rewrite");
    assert!(
        !full.is_file(),
        "the full payload is the copy the delta replaces, so it must be reclaimed"
    );
    assert_eq!(cas.get(target).await.unwrap(), Bytes::from(similar(b'A', b"REPLACED")));
}

/// A second pass over an already-optimal target must report no work and must
/// not rewrite the delta it just wrote. Without this the collapse would reopen
/// the both-forms window on every interval.
#[tokio::test]
async fn a_second_pass_over_a_delta_target_does_no_work() {
    let cas = common::open_file_cas().await;
    let base = cas.put(Bytes::from(similar(b'A', b"CHANGED"))).await.unwrap();
    let target = cas.put(Bytes::from(similar(b'A', b"REPLACED"))).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    assert!(
        !cas.run_maintenance_cycle().await.unwrap().maintenance_done,
        "a pass over an already-encoded target must report that it changed nothing"
    );
    assert!(
        !cas.run_maintenance_cycle().await.unwrap().maintenance_done,
        "and the pass after that, too"
    );
}

/// An intent-Full target that also has a `.diff` is a transition that has
/// already decided: the leftover delta must go.
///
/// The state is built the way a crash builds it. The target is pinned full, so
/// the promotion installs the full payload and removes the delta; writing the
/// saved `.diff` back afterwards reproduces an interrupted promotion, where both
/// files exist and the index still names full. Convergence is the collapse
/// pass's job, which is why this asserts on a maintenance run rather than on
/// the promotion.
#[tokio::test]
async fn an_intent_full_target_loses_its_leftover_delta() {
    let cas = common::open_file_cas().await;
    let base = cas.put(Bytes::from(similar(b'A', b"CHANGED"))).await.unwrap();
    let expected = similar(b'A', b"REPLACED");
    let target = cas.put(Bytes::from(expected.clone())).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let (full, delta) = form_paths(&cas, target);
    let leftover = std::fs::read(&delta).expect("read the delta payload");
    cas.set_constraint(target, BTreeSet::from([Hash::empty()])).await.unwrap();
    // The optimizer promotes an empty-constraint delta target to full; running a
    // cycle here makes the setup independent of Task 4's synchronous promotion.
    cas.run_maintenance_cycle().await.unwrap();
    assert!(full.is_file() && !delta.is_file(), "the setup requires a pinned-full target");
    std::fs::write(&delta, &leftover).expect("restore the leftover delta payload");

    cas.run_maintenance_cycle().await.unwrap();
    assert!(!delta.is_file(), "an intent-Full target must not keep a leftover delta");
    assert_eq!(cas.get(target).await.unwrap(), Bytes::from(expected));
}

/// Pinning full must converge the tree, not only the index: the delta payload
/// is gone and the full payload exists when `set_constraint` returns, and that
/// holds even though no maintenance pass ran.
#[tokio::test]
async fn pinning_full_leaves_no_delta_payload_behind() {
    let cas = common::open_file_cas().await;
    let base = cas.put(Bytes::from(similar(b'B', b"CHANGED"))).await.unwrap();
    let target = cas.put(Bytes::from(similar(b'B', b"REPLACED"))).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();

    let (full, delta) = form_paths(&cas, target);
    assert!(!full.is_file() && delta.is_file(), "the setup requires a delta-only target");

    cas.set_constraint(target, BTreeSet::from([Hash::empty()])).await.unwrap();

    let (full, delta) = form_paths(&cas, target);
    assert!(full.is_file(), "pinning full must materialize the full payload synchronously");
    assert!(!delta.is_file(), "pinning full must remove the delta payload synchronously");
    assert_eq!(cas.stat(target).await.unwrap().encoding, ObjectEncoding::Full);
    assert_eq!(cas.get(target).await.unwrap(), Bytes::from(similar(b'B', b"REPLACED")));
}

/// A soft constraint must not promote anything: only the empty object names
/// full storage, so naming a real base leaves the mode to the optimizer.
#[tokio::test]
async fn a_soft_constraint_does_not_promote_a_delta_target() {
    let cas = common::open_file_cas().await;
    let base = cas.put(Bytes::from(similar(b'C', b"CHANGED"))).await.unwrap();
    let target = cas.put(Bytes::from(similar(b'C', b"REPLACED"))).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();
    assert!(matches!(cas.stat(target).await.unwrap().encoding, ObjectEncoding::Delta { .. }));

    // Re-stating the same soft constraint must change nothing.
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    assert!(
        matches!(cas.stat(target).await.unwrap().encoding, ObjectEncoding::Delta { .. }),
        "a non-empty base set leaves storage mode to the optimizer"
    );
}

/// A sound delta must rescue a corrupt full payload instead of the read
/// failing, because either form means the data exists.
///
/// The store verifies on read here: without a verifying store a corrupt file
/// reads back silently and this test would pass with no fallback at all. The
/// index names `Full`, so this is the indexed form that is unsound and the
/// `.diff` is the only copy left, which is the case the fallback exists for.
#[tokio::test]
async fn a_corrupt_full_payload_is_served_from_the_sound_delta() {
    let cas = common::open_file_cas_verifying().await;
    let base = cas.put(Bytes::from(similar(b'D', b"CHANGED"))).await.unwrap();
    let expected = similar(b'D', b"REPLACED");
    let target = cas.put(Bytes::from(expected.clone())).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();
    assert!(
        matches!(cas.stat(target).await.unwrap().encoding, ObjectEncoding::Delta { .. }),
        "the setup requires a delta-encoded target"
    );

    // A promotion writes the full payload and deletes the delta, so an
    // interrupted promotion that left both on disk is: index=Full, full
    // corrupt, delta sound. Recreate that state by saving the delta, promoting
    // through the public API, restoring the delta, and corrupting the full.
    let (full, delta) = form_paths(&cas, target);
    assert!(delta.is_file(), "the setup requires a delta payload");
    let saved_delta = std::fs::read(&delta).expect("read the delta payload");
    cas.set_constraint(target, BTreeSet::from([Hash::empty()])).await.unwrap();
    assert_eq!(
        cas.stat(target).await.unwrap().encoding,
        ObjectEncoding::Full,
        "the promotion must leave the index naming Full"
    );
    std::fs::write(&delta, &saved_delta).expect("restore the delta payload");
    corrupt(&full);

    assert_eq!(
        cas.get(target).await.unwrap(),
        Bytes::from(expected),
        "the sound delta must serve the read the corrupt full payload cannot"
    );
}

/// A sound full payload must rescue a corrupt delta the same way.
///
/// The index names `Delta`, so the delta is the indexed form and the full
/// payload is the fallback; the collapse then records the form it kept.
#[tokio::test]
async fn a_corrupt_delta_payload_is_served_from_the_sound_full() {
    let cas = common::open_file_cas_verifying().await;
    let base = cas.put(Bytes::from(similar(b'E', b"CHANGED"))).await.unwrap();
    let expected = similar(b'E', b"REPLACED");
    let target = cas.put(Bytes::from(expected.clone())).await.unwrap();
    cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
    cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();
    assert!(
        matches!(cas.stat(target).await.unwrap().encoding, ObjectEncoding::Delta { .. }),
        "the setup requires a delta-encoded target"
    );

    // The collision the staging-name fix in Task 2 also protects against: both
    // forms exist, and the one the index names is the broken one.
    let (full, delta) = form_paths(&cas, target);
    std::fs::write(&full, &expected).expect("restore the full payload");
    assert!(full.is_file(), "the setup requires a full payload");
    corrupt(&delta);

    assert_eq!(
        cas.get(target).await.unwrap(),
        Bytes::from(expected),
        "the sound full payload must serve the read the corrupt delta cannot"
    );

    cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(
        cas.stat(target).await.unwrap().encoding,
        ObjectEncoding::Full,
        "the collapse must keep the form that reads and record it"
    );
    assert!(!delta.is_file(), "the corrupt delta payload must not survive the collapse");
}
