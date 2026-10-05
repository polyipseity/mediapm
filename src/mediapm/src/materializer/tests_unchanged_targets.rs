//! What a second `sync_hierarchy` does with an entry nothing changed, and
//! what it does with one whose content or file moved underneath it.
//!
//! The check those tests are about establishes different things depending on
//! the method that produced the output, so each relationship between an output
//! and its CAS object gets its own case here rather than sharing one.

use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::tests_common::{
    clear_readonly, open_hierarchy_cas, overall_finish, resolvable_media_document, run_sync,
};

use super::*;
use crate::config::MaterializationMethod;

/// Media id of the single entry these tests re-sync.
const MEDIA_ID: &str = "src1";

/// Hierarchy path of that entry, which is the key `state.managed_files`
/// records it under.
const RELATIVE_PATH: &str = "song";

/// Guards the media id the shared fixture binds its entry to, so a fixture
/// change points here rather than at four unrelated failures.
#[test]
fn the_fixture_entry_is_the_one_these_tests_re_sync() {
    let document = resolvable_media_document("ignored");
    assert_eq!(document.hierarchy[0].media_id.as_deref(), Some(MEDIA_ID));
}

/// A second `sync_hierarchy` over an unchanged entry counts a normal skip,
/// writes nothing, and leaves the overall row a success.
///
/// The three counts are read together because each one on its own is
/// satisfiable by a run that did the wrong thing, and because the two path
/// counters have to stay apart. A run that counted a missing entry as a skip
/// would still show `skipped_paths == 1` here while leaving the library short
/// a file, and the overall row would end green. `materialized_paths` alone
/// could come from a run that rewrote the file. Pinning all three says the
/// entry was visited, declined for the right reason, and did not turn the
/// screen red.
///
/// The overall finish is the part that regressed first: the finish rule used
/// to read any skip as a failure, so a clean re-sync would have ended red.
#[tokio::test]
async fn an_unchanged_entry_is_skipped_and_the_run_stays_clean() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(b"unchanged-entry")).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (first.materialized_paths, first.skipped_paths, first.missing_paths),
        (1, 0, 0),
        "the first run has nothing recorded for the entry, so it must write: {first:?}"
    );

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let second = run_sync(
        &paths,
        &document,
        &mut state,
        &cas,
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the second run resolved the same hash the first wrote and the file is still there, so \
         it must count one normal skip and no writes: {second:?}"
    );

    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishSuccess),
        "a run that declined an already-correct entry has materialized the library, so the \
         overall row must not end as an error; got {ops:?}",
        ops = recording.recorded()
    );
}

/// The entry's record survives the skip, or the stale scan would delete the
/// file it just declined to write.
///
/// This is the failure the short circuit invites. A skip that returned no
/// managed paths would leave the file outside `desired_managed_paths`, and the
/// stale scan runs after the workers, so the library would come out of a clean
/// re-sync missing the one entry it already had.
#[tokio::test]
async fn a_skipped_entry_keeps_its_record_so_the_stale_scan_leaves_it_alone() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(b"kept-record")).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run must write: {first:?}");

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(second.skipped_paths, 1, "the second run must skip: {second:?}");

    assert_eq!(
        state.managed_files.get(RELATIVE_PATH).map(|record| record.hash.as_str()),
        Some(hash.to_string().as_str()),
        "a skipped entry must still record what this run resolved for it: {:?}",
        state.managed_files
    );
    assert!(
        paths.hierarchy_root_dir.join(RELATIVE_PATH).is_file(),
        "the stale scan runs after the workers, so a skip that dropped the record would have \
         removed the file it declined to write"
    );
    assert_eq!(
        second.removed_paths, 0,
        "nothing in the hierarchy changed, so the stale scan must remove nothing: {second:?}"
    );
}

/// A resolved hash the recorded one does not match is written, however present
/// the target is.
///
/// The recorded hash is the only statement about the target that survives
/// between runs, so a comparison that ignored it would skip an entry whose
/// content changed and leave the old bytes under the new document's name.
#[tokio::test]
async fn an_entry_whose_hash_changed_is_written_again() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let first_hash = cas.put(bytes::Bytes::from_static(b"first-content")).await.unwrap();
    let second_hash = cas.put(bytes::Bytes::from_static(b"second-content")).await.unwrap();
    let mut state = MediaPmState::default();

    let first = run_sync(
        &paths,
        &resolvable_media_document(&first_hash.to_string()),
        &mut state,
        &cas,
        None,
        None,
    )
    .await;
    assert_eq!(first.materialized_paths, 1, "the first run must write: {first:?}");

    let second = run_sync(
        &paths,
        &resolvable_media_document(&second_hash.to_string()),
        &mut state,
        &cas,
        None,
        None,
    )
    .await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the document now resolves different content, so the entry must be written again: \
         {second:?}"
    );
    assert_eq!(
        std::fs::read(paths.hierarchy_root_dir.join(RELATIVE_PATH)).unwrap(),
        b"second-content",
        "the entry must hold the bytes the second document names"
    );
}

/// A recorded hash whose file is gone is written, however correct the record
/// still reads.
///
/// A deleted output leaves its record behind, so this is the case where the
/// recorded hash alone would skip the one path that needed writing.
#[tokio::test]
async fn an_entry_whose_file_was_deleted_is_written_again() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(b"deleted-then-restored")).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run must write: {first:?}");

    let target = paths.hierarchy_root_dir.join(RELATIVE_PATH);
    commit::remove_path(&target).expect("a readonly managed output is removable");
    assert!(
        !target.exists(),
        "the deletion has to have happened, or the second run has nothing to restore"
    );

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the record still names this hash but the file is gone, so the entry must be written: \
         {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        b"deleted-then-restored",
        "the entry must be back on disk with the bytes its record names"
    );
}

/// Payload the edit tests write over the library file. Its length differs from
/// every CAS object they resolve, which is what the length comparison sees.
const EDITED_IN_PLACE: &[u8] = b"a different number of bytes than the CAS object holds";

/// Payload whose length [`SAME_LENGTH_EDIT`] matches exactly.
const SAME_LENGTH_PAYLOAD: &[u8] = b"same-number-of-bytes";

/// Replacement for [`SAME_LENGTH_PAYLOAD`]: different bytes, same number of
/// them, which is the case a length comparison alone cannot see.
const SAME_LENGTH_EDIT: &[u8] = b"SAME-NUMBER-OF-BYTES";

/// Writes the entry once, then writes `replacement` over it and re-syncs.
///
/// Returns the workspace holding the run, the second report and the entry's
/// path, so each test can assert on whichever half of the outcome its case is
/// about. The workspace comes back with them because the run's files live
/// inside it, and a caller that dropped it would be reading paths out of a tree
/// that no longer exists.
///
/// The write goes through `remove_path` and a fresh file rather than through
/// the existing inode, so the output stops being the object the materializer
/// linked it to and becomes an independent file, which is the shape a copy
/// leaves. The case where the output keeps the shared inode is
/// [`a_hardlinked_output_is_recognised_while_the_object_behind_it_is_edited`].
async fn sync_edit_over_the_entry(
    payload: &[u8],
    replacement: &[u8],
) -> (tempfile::TempDir, MaterializeReport, PathBuf) {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::copy_from_slice(payload)).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        first.materialized_paths, 1,
        "the first run has nothing recorded, so it writes: {first:?}"
    );

    let target = paths.hierarchy_root_dir.join(RELATIVE_PATH);
    commit::remove_path(&target).expect("a readonly managed output is removable");
    std::fs::write(&target, replacement).expect("the external write lands on the library file");

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    (root, second, target)
}

/// An output something else rewrote with a different number of bytes is
/// written again, and the run counts it as materialized.
///
/// The recorded hash cannot see this. A write into the library does not touch
/// `state.managed_files`, so the record still names the content the document
/// resolved while the file holds something else. The length is what notices,
/// and the counters have to agree with it: a run that rewrote the file and
/// still counted a skip would report a clean library over bytes it just
/// replaced.
#[tokio::test]
async fn an_external_write_that_changes_the_length_is_written_again() {
    let (_workspace, second, target) =
        sync_edit_over_the_entry(SAME_LENGTH_PAYLOAD, EDITED_IN_PLACE).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the library file no longer has the length of the object the document names, so the \
         entry must be written and must not be reported as a skip: {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        SAME_LENGTH_PAYLOAD,
        "the entry must hold the bytes the document resolved, not the bytes the external write \
         left behind"
    );
}

/// A hardlinked output is recognised while the object behind it is edited, so
/// the check reads identity rather than content.
///
/// The materializer's first method is a hardlink, so the library file and the
/// CAS object are one inode and a write to either reaches both. This writes
/// through the object's own path, which leaves the inode untouched and the
/// content different, and the next run still declines the entry.
///
/// That is the case a length comparison cannot survive: the edit changed how
/// many bytes the file holds, so a check that counted them would have rewritten
/// it. The relationship did not move, so the file still is the object and
/// there is nothing to restore.
///
/// The content is left corrupted on purpose, and the doc on
/// `target_already_holds` says why the run does nothing about it. A hardlink
/// output and the object it shares an inode with cannot disagree, so a
/// difference between the file and the document's intent is a corrupted store,
/// which the store is where it belongs rather than here.
#[tokio::test]
async fn a_hardlinked_output_is_recognised_while_the_object_behind_it_is_edited() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(SAME_LENGTH_PAYLOAD)).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run must write: {first:?}");

    let target = paths.hierarchy_root_dir.join(RELATIVE_PATH);
    let object_path = cas
        .object_path_for_hash(hash)
        .expect("the first run linked a hardlink, so the object is a file on disk");
    assert!(
        same_file::is_same_file(&object_path, &target)
            .expect("same_file check after the first run"),
        "the first run's method is a hardlink, so the two paths have to be one inode before this \
         test has the case it is about"
    );

    clear_readonly(&object_path);
    std::fs::write(&object_path, EDITED_IN_PLACE)
        .expect("the edit lands on the object, and the output shares its inode");
    assert_ne!(
        std::fs::read(&target).unwrap().len() as u64,
        cas.stat(hash).await.unwrap().len,
        "the edit reached the output through the shared inode, so the file's length no longer \
         agrees with what the document resolves"
    );

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the output is still the inode the object names, and the check never reads either of \
         them, so the entry must be declined rather than rewritten: {second:?}"
    );
}

/// An output rewritten with the same number of bytes is written again.
///
/// Content can change without changing length, and the length decides first
/// because it costs one stat. Once it matches, the target is hashed, and that
/// is what this case turns on: the file holds the right number of bytes and
/// the wrong ones, so a rule that stopped at the length would take it for the
/// content the document resolves.
///
/// The output here is an independent file rather than a link, because the edit
/// went through a fresh inode. That is the shape a copy leaves, and the shape a
/// reflink leaves.
#[tokio::test]
async fn an_output_edited_to_the_same_length_is_written_again() {
    let (_workspace, second, target) =
        sync_edit_over_the_entry(SAME_LENGTH_PAYLOAD, SAME_LENGTH_EDIT).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the edit holds as many bytes as the object the document names, so the length alone takes \
         it for the content that was resolved, and the hash is what has to notice: {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        SAME_LENGTH_PAYLOAD,
        "the entry must hold the bytes the document resolved, not the ones the edit left behind"
    );
}

/// Payload whose object one library entry is redirected at while another
/// payload of the same length stands in for the content the document resolves.
///
/// Same length on purpose. A decoy that held a different number of bytes would
/// be caught by the length comparison that runs for every method without a
/// relationship, so the test would pass against a build that never read a
/// symlink at all.
const DECOY_PAYLOAD: &[u8] = b"SAME-NUMBER-OF-BYTES";

/// A library entry that is a symlink naming the CAS object is declined.
///
/// The re-sync reads the link and finds the path the document resolved, so
/// there is nothing left to establish and no byte is read. The entry is turned
/// into a symlink by replacing it between two runs, because `sync_hierarchy`
/// writes through hardlink and never produces one itself.
#[cfg(unix)]
#[tokio::test]
async fn a_symlink_naming_the_right_object_skips() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(SAME_LENGTH_PAYLOAD)).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run must write: {first:?}");

    let target = paths.hierarchy_root_dir.join(RELATIVE_PATH);
    let object_path = cas
        .object_path_for_hash(hash)
        .expect("the first run linked a hardlink, so the object is a file on disk");
    commit::remove_path(&target).expect("a readonly managed output is removable");
    std::os::unix::fs::symlink(&object_path, &target).expect("the entry becomes a symlink");

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the entry names the object the document resolves, so it is what the run would have \
         written and must be left alone: {second:?}"
    );
    assert_eq!(
        std::fs::read_link(&target).unwrap(),
        object_path,
        "the entry must still be the symlink the check declined to rewrite"
    );
}

/// A library entry that is a symlink naming a different object is written
/// again.
///
/// A managed output is materialized from the store, so one pointing elsewhere is
/// wrong whatever it happens to resolve to. The decoy holds the same number of
/// bytes as the content the document resolves, so neither the length nor the
/// hash can see this: only the link itself says where the entry went.
#[cfg(unix)]
#[tokio::test]
async fn a_symlink_naming_the_wrong_object_is_written_again() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(SAME_LENGTH_PAYLOAD)).await.unwrap();
    let decoy = cas.put(bytes::Bytes::from_static(DECOY_PAYLOAD)).await.unwrap();
    cas.ensure_blob_materialized(decoy)
        .await
        .expect("a small put stays in the WAL until something asks for the blob as a file");
    assert_eq!(
        decoy_hash_len(&cas, decoy).await,
        SAME_LENGTH_PAYLOAD.len() as u64,
        "the decoy has to hold as many bytes as the content the document resolves, or the length \
         comparison would catch this case and the test would prove nothing about reading a link"
    );
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run must write: {first:?}");

    let target = paths.hierarchy_root_dir.join(RELATIVE_PATH);
    commit::remove_path(&target).expect("a readonly managed output is removable");
    let decoy_path = cas
        .object_path_for_hash(decoy)
        .expect("the decoy was materialized above, so it is a file on disk");
    std::os::unix::fs::symlink(&decoy_path, &target).expect("the entry becomes a symlink");

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the entry resolves to a different object of the same length, so only the link itself \
         says the entry is wrong: {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        SAME_LENGTH_PAYLOAD,
        "the entry must hold the content the document resolves"
    );
}

/// Length the CAS reports for `hash`, which the decoy has to match.
#[cfg(unix)]
async fn decoy_hash_len(cas: &FileSystemCas, hash: Hash) -> u64 {
    cas.stat(hash).await.expect("the decoy is in the store").len
}

/// An untouched output is declined whichever method produced it.
///
/// `sync_hierarchy` writes through hardlink, so the other three have to be
/// materialized here directly to reach the check with them. What each method
/// leaves behind is what the check answers on: an inode for hardlink, a name
/// for symlink, and bytes alone for the two that share no inode with the
/// object.
#[cfg(unix)]
#[tokio::test]
async fn an_untouched_output_skips_whatever_method_produced_it() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(SAME_LENGTH_PAYLOAD)).await.unwrap();

    let mut outputs = Vec::new();
    for method in [
        MaterializationMethod::Hardlink,
        MaterializationMethod::Symlink,
        MaterializationMethod::Reflink,
        MaterializationMethod::Copy,
    ] {
        let relative_path = format!("out-{}", method.as_label());
        let target = root.path().join(&relative_path);
        let mut notices = Vec::new();
        if file_ops::materialize_file_from_cas_with_order(
            &cas,
            hash,
            &target,
            &relative_path,
            std::slice::from_ref(&method),
            &mut notices,
        )
        .await
        .is_err()
        {
            // A filesystem with no copy-on-write clone has no reflink to
            // exercise. Which methods ran is asserted at the end, so a test
            // that silently covered nothing cannot pass.
            continue;
        }
        outputs.push((relative_path, target, method.as_label()));
    }

    let shared = SyncSharedState {
        hierarchy_root: root.path().to_path_buf(),
        cas,
        flattened: Vec::new(),
        verify_materialization: false,
        recorded_hashes: outputs
            .iter()
            .map(|(relative_path, _, _)| (relative_path.clone(), hash.to_string()))
            .collect(),
        verdicts: VerdictCache::open(&paths),
    };
    for (relative_path, target, label) in &outputs {
        assert!(
            shared.target_already_holds(relative_path, &hash, target).await,
            "an output '{label}' left untouched holds the content the document resolves, so the \
             run must decline to rewrite it"
        );
    }
    for method in ["hardlink", "copy"] {
        assert!(
            outputs.iter().any(|(_, _, label)| *label == method),
            "the '{method}' method was not exercised on this platform, so this test says nothing: \
             {outputs:?}"
        );
    }
}
