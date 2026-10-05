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
/// satisfiable by a run that did the wrong thing. `skipped_paths` alone could
/// come from an entry that found nothing to commit, `materialized_paths` alone
/// from a run that rewrote the file, and the row's success finish alone from a
/// run whose skip never reached the finish rule. Pinning all three says the
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
