//! The read-back check that runs over a just-written entry when the document asks for it.

use super::*;

/// Bytes that do not name the expected object fail, naming what was expected and
/// what was found.
///
/// The check runs in the write path, between a write and a read of the same
/// file, so nothing in a test can make the two disagree. This drives the
/// comparison itself: a reader of the failure has to be able to tell which entry
/// and which two hashes the run disagreed about.
#[test]
fn a_verification_mismatch_names_the_path_and_both_hashes() {
    let expected = Hash::from_content(b"the bytes the store holds");
    let found = b"the bytes the target holds";

    let error = verify_materialized_hash("artist/song.mkv", &expected, found)
        .expect_err("bytes that name another object must fail verification");
    let MediaPmError::Workflow(detail) = error else {
        panic!("a failed verification is a workflow error, got {error:?}");
    };
    assert!(detail.contains("artist/song.mkv"), "the failure names the entry: {detail}");
    assert!(
        detail.contains(&expected.to_string()),
        "the failure names what was expected: {detail}"
    );
    assert!(
        detail.contains(&Hash::from_content(found).to_string()),
        "the failure names what was found: {detail}"
    );
}

/// Bytes that name the expected object pass.
#[test]
fn a_matching_verification_succeeds() {
    let bytes = b"the bytes the store holds";
    let expected = Hash::from_content(bytes);

    verify_materialized_hash("artist/song.mkv", &expected, bytes)
        .expect("the object's own bytes must verify");
}
