//! What a second `sync_hierarchy` does with a variant the materializer
//! writes out of a ZIP pack.
//!
//! A variant carrying a `zip_member` binding is stored as the archive the
//! workflow produced, and the materializer writes one member extracted out of
//! it. That makes the question "does the target already hold this variant" a
//! different one than it is for a variant written straight from a CAS object:
//! the recorded hash and the length belong to the member, while the variant
//! hash names the pack the member came out of. These tests pin both answers —
//! an unchanged pack must skip, and a changed pack must be written again over
//! the read-only file the previous run left.

use super::tests_common::{
    ZIP_MEMBER_MEDIA_ID, ZIP_MEMBER_PATH, ZIP_MEMBER_SUFFIX, ZIP_MEMBER_VARIANT,
    open_hierarchy_cas, run_sync, zip_member_media_document, zip_payload,
};
use super::*;

/// Builds the pack the fixture's variant hash points at, holding `member`
/// under the member name the fixture's binding extracts.
fn pack_holding(member: &[u8]) -> Vec<u8> {
    zip_payload(&[(ZIP_MEMBER_SUFFIX, member)])
}

/// A second `sync_hierarchy` over an unchanged zip-member variant counts a
/// normal skip and leaves the member's bytes and modification time alone.
///
/// The first run writes the extracted member, so the second run finds a
/// read-only file where a variant with no `zip_member` binding would find a
/// hardlink to its CAS object. Comparing the target against the *pack* hash
/// would read that as a change on every run, re-extract, and fail on the
/// read-only file; comparing it against the member's own hash is what makes
/// the skip reachable at all.
#[tokio::test]
async fn a_zip_member_variant_that_is_unchanged_is_skipped_on_the_next_sync() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let input_hash = cas.put(bytes::Bytes::from_static(b"zip-member-input")).await.unwrap();
    let member = b"WEBVTT\n\nmember body";
    let pack_hash = cas.put(bytes::Bytes::from(pack_holding(member))).await.unwrap();
    let document = zip_member_media_document(&input_hash.to_string(), &pack_hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (first.materialized_paths, first.skipped_paths, first.missing_paths),
        (1, 0, 0),
        "the first run has nothing recorded for the entry, so it must write the extracted \
         member: {first:?}"
    );

    let target = paths.hierarchy_root_dir.join(ZIP_MEMBER_PATH);
    assert_eq!(
        std::fs::read(&target).unwrap(),
        member,
        "the target holds the member extracted out of the pack, not the pack: {}",
        target.display()
    );
    let metadata = std::fs::metadata(&target).unwrap();
    assert!(
        metadata.permissions().readonly(),
        "every managed output is left read-only, which is what the second run has to write over \
         if it decides the entry changed"
    );
    let written_mtime = metadata.modified().unwrap();

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the pack is the one the first run extracted and the member is still on disk, so the \
         second run must count one normal skip and write nothing: {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        member,
        "a skipped entry leaves the member it already wrote in place"
    );
    assert_eq!(
        std::fs::metadata(&target).unwrap().modified().unwrap(),
        written_mtime,
        "a skip writes nothing, so the modification time must not move: {}",
        target.display()
    );

    let expected_hash = Hash::from_content(member).to_string();
    assert_eq!(
        state.managed_files.get(ZIP_MEMBER_PATH).map(|record| record.hash.as_str()),
        Some(expected_hash.as_str()),
        "a skip records the member's hash, the same hash the write records, or the next run \
         would compare the target against a hash it never wrote: {:?}",
        state.managed_files
    );
    assert_eq!(
        state
            .workflow_states
            .get(ZIP_MEMBER_MEDIA_ID)
            .and_then(|workflow| workflow.variant_hashes.get(ZIP_MEMBER_VARIANT)),
        Some(&expected_hash),
        "the variant hash a skip leaves behind must be the one a write leaves behind, or the two \
         runs disagree about what the library holds: {:?}",
        state.workflow_states
    );
}

/// A zip-member variant whose pack changed is extracted again, over the
/// read-only member the previous run wrote.
///
/// The skip above is exact rather than unconditional: the pack's hash moves
/// with its contents, so a new pack reaches the write path. That path has to
/// replace the existing file — a plain write into a read-only managed output
/// fails — which is the same replacement the CAS-object path performs.
#[tokio::test]
async fn a_zip_member_variant_whose_pack_changed_is_written_again() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let input_hash = cas.put(bytes::Bytes::from_static(b"zip-member-input")).await.unwrap();
    let first_member = b"WEBVTT\n\nfirst member body";
    let first_pack = cas.put(bytes::Bytes::from(pack_holding(first_member))).await.unwrap();
    let mut document = zip_member_media_document(&input_hash.to_string(), &first_pack.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run must write: {first:?}");

    let target = paths.hierarchy_root_dir.join(ZIP_MEMBER_PATH);
    assert_eq!(
        std::fs::read(&target).unwrap(),
        first_member,
        "the first run wrote the first pack's member"
    );

    let second_member = b"WEBVTT\n\nsecond member body";
    let second_pack = cas.put(bytes::Bytes::from(pack_holding(second_member))).await.unwrap();
    document
        .media
        .get_mut(ZIP_MEMBER_MEDIA_ID)
        .expect("the fixture declares the media entry the test re-points")
        .variant_hashes
        .insert(ZIP_MEMBER_VARIANT.to_string(), second_pack.to_string());

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the variant hash names a different pack, so the extracted member must be written \
         again: {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        second_member,
        "the target has to hold the new pack's member, over the read-only file the first run \
         left behind: {}",
        target.display()
    );
}

/// A pack whose contents change without touching the member the variant
/// extracts still satisfies the skip.
///
/// This is the case that separates deciding the skip against the extracted
/// member from deciding it against the pack: the pack's hash moves, so a
/// comparison against it would rewrite the file, while the member's own bytes
/// are the ones the target has to hold and they did not. Both wrong answers are
/// excluded at once — comparing against the pack re-extracts and rewrites, and
/// comparing nothing at all would skip the case above.
#[tokio::test]
async fn a_zip_member_variant_is_skipped_when_only_the_rest_of_the_pack_changed() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let input_hash = cas.put(bytes::Bytes::from_static(b"zip-member-input")).await.unwrap();
    let member = b"WEBVTT\n\nmember body that does not move";
    let first_pack = cas.put(bytes::Bytes::from(pack_holding(member))).await.unwrap();
    let mut document = zip_member_media_document(&input_hash.to_string(), &first_pack.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run must write: {first:?}");

    let target = paths.hierarchy_root_dir.join(ZIP_MEMBER_PATH);
    assert_eq!(std::fs::read(&target).unwrap(), member, "the first run wrote the member");
    let written_mtime = std::fs::metadata(&target).unwrap().modified().unwrap();

    let second_pack = cas
        .put(bytes::Bytes::from(pack_holding_with(
            member,
            "unrelated.txt",
            b"another member the variant does not extract",
        )))
        .await
        .unwrap();
    assert_ne!(first_pack, second_pack, "the fixture must stage a pack whose hash moves");
    document
        .media
        .get_mut(ZIP_MEMBER_MEDIA_ID)
        .expect("the fixture declares the media entry the test re-points")
        .variant_hashes
        .insert(ZIP_MEMBER_VARIANT.to_string(), second_pack.to_string());

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the pack changed but the member the target holds is byte-identical, so the entry is \
         already correct and must be skipped rather than rewritten: {second:?}"
    );
    assert_eq!(
        std::fs::metadata(&target).unwrap().modified().unwrap(),
        written_mtime,
        "a skip writes nothing, so the member's modification time must not move: {}",
        target.display()
    );
}

/// Builds a pack holding `member` under the fixture's member name plus one
/// unrelated member, so the archive's own hash moves while the extracted
/// member's bytes stay identical.
fn pack_holding_with(member: &[u8], extra_name: &str, extra: &[u8]) -> Vec<u8> {
    zip_payload(&[(extra_name, extra), (ZIP_MEMBER_SUFFIX, member)])
}
