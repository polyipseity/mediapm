//! How the overall row finishes on a failed run, on a run that left a path
//! unwritten, and on a clean one.

use crate::config::hierarchy_types::HierarchyPath;
use mediapm_utils::progress::recording::{BarId, ProgressOp, RecordingProgressTracker};

use super::tests_common::{
    folder_only_document, open_hierarchy_cas, overall_finish,
    playlist_with_unknown_reference_document, resolvable_media_document, single_media_document,
};

use super::*;

/// Hierarchy path of the single folder entry in [`folder_only_document`],
/// which is the directory the conflict test occupies.
const FOLDER_PATH: &str = "album";

/// Variant name of that folder's single variant, and so the file name it
/// would write inside [`FOLDER_PATH`].
const FOLDER_VARIANT: &str = "default";

/// The overall row finishes `FinishError` when an entry fails and when an
/// entry's output could not be written.
///
/// The overall handle and the per-entry rows share the op vocabulary, so
/// an unfiltered search for a finish finds whichever row emitted it first
/// and a test written that way says nothing about the overall row. Every
/// finish assertion here is therefore keyed on the [`BarId`] of the
/// handle [`RecordingProgressTracker::with_overall`] handed back.
///
/// Each half establishes the entry's outcome from the run itself before
/// reading the overall finish, so the assertion cannot pass against a run
/// that did the opposite: the failing half checks the returned `Err` and
/// the entry row's own `FinishError`, the missing-content half checks
/// `missing_paths` and the entry row's `FinishWarning`.
///
/// The missing-content half also pins that the run still returns `Ok`, so the
/// error row is not bought by throwing the report away. A caller that saw a
/// missing path as a green screen missed a path it was asked to write; a
/// caller that saw it as `Err` would lose the tally of what did land.
#[tokio::test]
async fn the_overall_row_finishes_error_on_a_failed_entry_and_on_a_missing_one() {
    let failed = {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;
        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);

        let result = sync_hierarchy(
            &paths,
            &playlist_with_unknown_reference_document(),
            &mut MediaPmState::default(),
            &cas,
            true,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall.clone())),
        )
        .await;

        assert!(result.is_err(), "the broken playlist must fail the run: {result:?}");
        assert_eq!(
            entry_finishes(&recording),
            vec![ProgressOp::FinishError],
            "the failing entry row must be the one that reports the error; got {recorded:?}",
            recorded = recording.recorded()
        );
        (recording, overall)
    };
    assert_eq!(
        overall_finish(&failed.0, &failed.1),
        Some(ProgressOp::FinishError),
        "a run with a failed entry must end the overall row as an error; got {ops:?}",
        ops = failed.0.recorded()
    );

    let missing = {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;
        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);

        let result = sync_hierarchy(
            &paths,
            &single_media_document("src1", HierarchyPath::simple("test_file")),
            &mut MediaPmState::default(),
            &cas,
            true,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall.clone())),
        )
        .await;

        let report = result.expect(
            "an entry with no content hash is a failure to materialize, not a failed run; the \
                 report must survive so the caller can still read the tallies",
        );
        assert_eq!(
            report.missing_paths, 1,
            "the run must really have found no content for its one entry before the overall \
                 finish is read; got {report:?}"
        );
        assert_eq!(
            report.skipped_paths, 0,
            "an entry with no content is not an already-correct entry, so nothing was skipped; \
                 got {report:?}"
        );
        assert_eq!(
            report.materialized_paths, 0,
            "the report must distinguish the missing entry from a written one; got {report:?}"
        );
        assert_eq!(
            entry_finishes(&recording),
            vec![ProgressOp::FinishWarning],
            "the entry row that found no content must be the one carrying the warning; \
             got {ops:?}",
            ops = recording.ops()
        );
        (recording, overall)
    };
    assert_eq!(
        overall_finish(&missing.0, &missing.1),
        Some(ProgressOp::FinishError),
        "a run whose only entry had no content to write has not materialized the library, so \
             the overall row must end as an error; got {ops:?}",
        ops = missing.0.recorded()
    );
}

/// A media folder whose variant path is already a directory counts as
/// missing, and ends the overall row as an error.
///
/// The arm used to record a notice, move on to the next variant and fall
/// through to the folder's success outcome, so a folder that wrote nothing was
/// tallied as materialized and the run finished green. Both counters are read
/// because either one alone is satisfiable by that shape: a missing count
/// beside a green overall row, or a red row beside a materialized count.
///
/// The files a folder's other variants did write are not in question, because
/// [`folder_only_document`] gives the folder a single variant, so a clean run
/// over this fixture writes exactly the one file the conflict blocks.
#[tokio::test]
async fn the_overall_row_finishes_error_when_a_folder_variant_path_is_a_directory() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(b"blocked-folder-variant")).await.unwrap();

    let occupied = paths.hierarchy_root_dir.join(FOLDER_PATH).join(FOLDER_VARIANT);
    tokio::fs::create_dir_all(&occupied).await.unwrap();
    assert!(
        occupied.is_dir(),
        "the conflict has to be in place, or the folder arm finds a free name and writes"
    );

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let report = sync_hierarchy(
        &paths,
        &folder_only_document(&hash.to_string()),
        &mut MediaPmState::default(),
        &cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await
    .expect("one blocked variant leaves a report to read; it is not a failed run");

    assert_eq!(
        report.missing_paths, 1,
        "the folder wrote nothing, so the library is short of the entry the document asked for; \
         got {report:?}"
    );
    assert_eq!(
        report.materialized_paths, 0,
        "a folder that wrote nothing is not a materialized path, whatever its other variants did; \
         got {report:?}"
    );
    assert_eq!(
        report.skipped_paths, 0,
        "nothing was already correct, so the blocked variant is not a normal skip; got \
         {report:?}"
    );
    assert_eq!(
        entry_finishes(&recording),
        vec![ProgressOp::FinishWarning],
        "the folder row must carry the warning, the way a media entry with no content does; \
         got {ops:?}",
        ops = recording.ops()
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishError),
        "a folder left short a variant has not materialized the library, so the overall row must \
         end as an error; got {ops:?}",
        ops = recording.recorded()
    );
}

/// A run that materializes every entry still ends the overall row as a
/// success.
///
/// The half of the error contract above that a missing path now triggers
/// needs a counterpart: if `missing_paths` were consulted for the success
/// path too, or if the error finish bled into a clean run, this would catch
/// it.
#[tokio::test]
async fn the_overall_row_finishes_success_when_every_entry_is_written() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let payload = b"every-entry-written";
    let hash = cas.put(bytes::Bytes::from_static(payload)).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let report = sync_hierarchy(
        &paths,
        &document,
        &mut MediaPmState::default(),
        &cas,
        true,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await
    .expect("an entry holding a resolvable variant hash should write");

    assert_eq!(
        report.materialized_paths, 1,
        "the entry must really have written before the overall finish is read; got {report:?}"
    );
    assert_eq!(
        report.skipped_paths, 0,
        "nothing was skipped, so the error finish has nothing to rest on; got {report:?}"
    );
    assert_eq!(
        report.missing_paths, 0,
        "every entry had content to write, so nothing is missing; got {report:?}"
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishSuccess),
        "a run that wrote every entry must end the overall row as a success; got {ops:?}",
        ops = recording.recorded()
    );
}

/// Returns the terminal op of every bar the tracker opened other than the
/// overall one, in the order the bars were added.
fn entry_finishes(tracker: &RecordingProgressTracker) -> Vec<ProgressOp> {
    tracker
        .recorded()
        .iter()
        .filter(|entry| entry.bar != BarId::Index(0))
        .filter_map(|entry| match entry.op {
            ProgressOp::FinishSuccess | ProgressOp::FinishWarning | ProgressOp::FinishError => {
                Some(entry.op.clone())
            }
            _ => None,
        })
        .collect()
}
