//! What a second `sync_hierarchy` does with an entry nothing changed, and
//! what it does with one whose content or file moved underneath it.

use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::tests_common::{open_hierarchy_cas, overall_finish, resolvable_media_document};

use super::*;

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
/// them, which is the case the length comparison cannot see.
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
/// the existing inode, because the library file is a hardlink of the CAS object
/// and writing through it would edit the object the run restores from. The
/// case where the write does go through the shared inode is
/// [`an_edit_written_through_the_hardlink_is_written_again`].
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

/// An edit written through the hardlink the materializer created is written
/// again, because its length no longer matches the object.
///
/// The first materialization method is a hardlink, so the library file and the
/// CAS object are one inode and a write to either reaches both. The record
/// still matches and the path is still there, which leaves the length as the
/// only thing that can notice, so this is the case where the check has to work
/// on its own.
///
/// The file's content cannot be asserted afterwards. The external write
/// reached the CAS object through the shared inode, so a rewrite restores the
/// edited bytes rather than the original ones. What the run does about that is
/// a separate question from what it counts, and the counters are what this
/// test is about.
#[tokio::test]
async fn an_edit_written_through_the_hardlink_is_written_again() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(SAME_LENGTH_PAYLOAD)).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run must write: {first:?}");

    let target = paths.hierarchy_root_dir.join(RELATIVE_PATH);
    clear_readonly(&target);
    std::fs::write(&target, EDITED_IN_PLACE).expect("the write lands through the shared inode");

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the edit went through the CAS object, so the recorded hash still matches while the file \
         is the wrong length; the entry must still be written: {second:?}"
    );
}

/// A same-length edit survives the skip, and this test pins that.
///
/// This is the limit of the rule rather than a case it handles. Content can
/// change without changing length, and the run compares lengths because
/// rehashing every output on every run would read the whole library each time.
/// An edit that keeps the number of bytes therefore looks correct to the next
/// sync and is left in place.
///
/// The assertion on the surviving edit is deliberate. A change that starts
/// rehashing the target has to come here and delete it, which is how a reader
/// finds out the guarantee moved.
#[tokio::test]
async fn a_same_length_edit_survives_the_skip_and_the_limit_is_pinned() {
    let (_workspace, second, target) =
        sync_edit_over_the_entry(SAME_LENGTH_PAYLOAD, SAME_LENGTH_EDIT).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the edit holds as many bytes as the object the document names, so the run takes it for \
         the content it resolved and leaves it alone: {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        SAME_LENGTH_EDIT,
        "the limit is that a same-length edit survives. A rule that rehashed the target would \
         restore the resolved bytes here, and this assertion is what makes that change visible"
    );
}

/// Clears the read-only bit the materializer sets on every managed output, so
/// a test can stand in for something outside mediapm writing into the library.
fn clear_readonly(path: &Path) {
    let mut permissions = std::fs::metadata(path).unwrap().permissions();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let mode = permissions.mode();
        let writable_mode = mode | 0o200;
        if writable_mode != mode {
            permissions.set_mode(writable_mode);
        }
    }

    #[cfg(not(unix))]
    {
        #[expect(
            clippy::permissions_set_readonly_false,
            reason = "on non-Unix platforms the readonly flag blocks the external write this helper exists to stage"
        )]
        {
            permissions.set_readonly(false);
        }
    }

    std::fs::set_permissions(path, permissions).expect("the permissions are restored");
}

/// Runs `sync_hierarchy` over `document` with no progress output, or with the
/// tracker's own bars when one is supplied.
async fn run_sync(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    state: &mut MediaPmState,
    cas: &FileSystemCas,
    progress_group: Option<Arc<dyn ProgressScreenApi + Send + Sync>>,
    overall_bar: Option<Arc<dyn ProgressBarApi>>,
) -> MaterializeReport {
    sync_hierarchy(
        paths,
        document,
        state,
        cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        progress_group,
        overall_bar,
    )
    .await
    .expect("a run over one resolvable entry either writes it or skips it")
}
