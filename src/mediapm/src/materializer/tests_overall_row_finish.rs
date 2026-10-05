//! How the overall row finishes on a failed run, a skipped run, and a clean one.

use crate::config::hierarchy_types::{HierarchyNodeKind, HierarchyPath};
use mediapm_utils::progress::recording::{BarId, ProgressOp, RecordingProgressTracker};

use super::tests_common::{
    folder_and_playlist_document, open_hierarchy_cas, playlist_with_unknown_reference_document,
    single_media_document,
};

use super::*;

/// The overall row finishes `FinishError` when an entry fails and when an
/// entry is skipped.
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
/// the entry row's own `FinishError`, the skipping half checks
/// `skipped_paths` and the entry row's `FinishWarning`.
///
/// The skipping half also pins that the run still returns `Ok`, so the
/// error row is not bought by throwing the report away. A caller that saw
/// a skip as a green screen missed a path it was asked to write; a caller
/// that saw it as `Err` would lose the tally of what did land.
#[tokio::test]
async fn the_overall_row_finishes_error_on_a_failed_entry_and_on_a_skipped_one() {
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

    let skipped = {
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
            "a skipped entry is a failure to materialize, not a failed run; the report must \
                 survive so the caller can still read the tallies",
        );
        assert_eq!(
            report.skipped_paths, 1,
            "the run must really have skipped its one entry before the overall finish is read; \
                 got {report:?}"
        );
        assert_eq!(
            report.materialized_paths, 0,
            "the report must distinguish the skipped entry from a written one; got {report:?}"
        );
        assert_eq!(
            entry_finishes(&recording),
            vec![ProgressOp::FinishWarning],
            "the skipped entry row must be the one carrying the warning; got {ops:?}",
            ops = recording.ops()
        );
        (recording, overall)
    };
    assert_eq!(
        overall_finish(&skipped.0, &skipped.1),
        Some(ProgressOp::FinishError),
        "a run whose only entry was skipped has not materialized the library, so the overall \
             row must end as an error; got {ops:?}",
        ops = skipped.0.recorded()
    );
}

/// A run that materializes every entry still ends the overall row as a
/// success.
///
/// The half of the error contract above that a skip now triggers needs a
/// counterpart: if `skipped_paths` were consulted for the success path too,
/// or if the error finish bled into a clean run, this would catch it.
#[tokio::test]
async fn the_overall_row_finishes_success_when_every_entry_is_written() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let payload = b"every-entry-written";
    let hash = cas.put(bytes::Bytes::from_static(payload)).await.unwrap();
    let mut document = folder_and_playlist_document(&hash.to_string());
    document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::Media));

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

/// Returns the terminal op of one handle's own bar, or `None` when it never
/// finished.
fn overall_finish(
    tracker: &RecordingProgressTracker,
    overall: &mediapm_utils::progress::recording::RecordingTrackedHandle,
) -> Option<ProgressOp> {
    let bar = overall.bar();
    tracker.recorded().iter().find_map(|entry| {
        if entry.bar != bar {
            return None;
        }
        match entry.op {
            ProgressOp::FinishSuccess | ProgressOp::FinishWarning | ProgressOp::FinishError => {
                Some(entry.op.clone())
            }
            _ => None,
        }
    })
}
