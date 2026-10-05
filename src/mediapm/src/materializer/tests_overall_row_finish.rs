//! How the overall row finishes on a failed run, on a run that left a path
//! unwritten, and on a clean one.

use crate::config::hierarchy_types::HierarchyPath;
use mediapm_utils::progress::recording::{BarId, ProgressOp, RecordingProgressTracker};

use super::tests_common::{
    BLOCKED_VARIANT, folder_only_document, folder_with_blocked_and_good_variants_document,
    folder_without_variants_document, open_hierarchy_cas, overall_finish,
    playlist_with_unknown_reference_document, resolvable_media_document, single_media_document,
    zip_payload,
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

/// A folder variant whose bytes cannot be resolved counts as missing, and
/// ends the overall row as an error.
///
/// `resolve_variant_source_bytes` errors when the variant names a hash the
/// store does not hold. The arm records a notice, moves on to the next
/// variant, and used to leave the folder on its success outcome on the way
/// out, so a run that wrote none of the folder reported it as materialized
/// and finished green.
#[tokio::test]
async fn the_overall_row_finishes_error_when_a_folder_variant_cannot_be_resolved() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let absent = Hash::from_content(b"never-stored-in-this-cas");

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let report = sync_hierarchy(
        &paths,
        &folder_only_document(&absent.to_string()),
        &mut MediaPmState::default(),
        &cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await
    .expect("one unresolvable variant leaves a report to read; it is not a failed run");

    assert_eq!(
        report.missing_paths, 1,
        "the folder wrote nothing, so the library is short of the entry the document asked for; \
         got {report:?}"
    );
    assert_eq!(
        report.materialized_paths, 0,
        "a folder that wrote nothing is not a materialized path; got {report:?}"
    );
    assert_eq!(
        report.skipped_paths, 0,
        "the variant resolved to no bytes at all, which is not an already-correct output; got \
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

/// A ZIP variant holding nothing to extract counts as missing, and ends the
/// overall row as an error.
///
/// The archive resolves, so the arm takes the ZIP branch and opens its
/// member row, then finds no file in it. It used to record a notice and
/// leave the folder on its success outcome, which counted a folder with an
/// empty directory in it as a materialized path.
///
/// The folder directory is read back as well, because the counters alone
/// cannot tell an archive that was read and found empty from one the arm
/// never opened at all.
#[tokio::test]
async fn the_overall_row_finishes_error_when_a_zip_folder_variant_holds_no_members() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from(zip_payload(&[]))).await.unwrap();

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
    .expect("one empty archive leaves a report to read; it is not a failed run");

    assert_eq!(
        report.missing_paths, 1,
        "the archive held no file, so the folder holds nothing the document asked for; got \
         {report:?}"
    );
    assert_eq!(
        report.materialized_paths, 0,
        "an empty archive is not a materialized path; got {report:?}"
    );
    assert_eq!(
        report.skipped_paths, 0,
        "nothing was already on disk to decline, so this is not a normal skip; got {report:?}"
    );
    let folder = paths.hierarchy_root_dir.join(FOLDER_PATH);
    assert_eq!(
        std::fs::read_dir(&folder).unwrap().count(),
        0,
        "the archive has to have been read and found empty rather than skipped, or the counts \
         above say nothing; {} is not empty",
        folder.display()
    );
    assert_eq!(
        finish_of_bar_opened_as(&recording, &format!("{FOLDER_PATH} [stg]")),
        Some(ProgressOp::FinishWarning),
        "the folder row must carry the warning, the way a folder with a blocked variant does; \
         got {ops:?}",
        ops = recording.recorded()
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishError),
        "a folder left short a variant has not materialized the library, so the overall row must \
         end as an error; got {ops:?}",
        ops = recording.recorded()
    );
}

/// A media folder that resolves to no variants at all counts as missing, and
/// ends the overall row as an error.
///
/// The other arms in the folder loop each leave the loop by setting the flag
/// that marks the entry unwritten. An entry with no variants never reaches
/// them, so the loop runs zero times and the flag was never set, which left
/// the entry on the success outcome with an empty directory behind it.
///
/// The three counts are read together, because any one of them alone is
/// satisfiable by an edit that only moves the entry between two counters.
/// Reading the directory back settles what the counts describe: the folder
/// arm ran and wrote nothing, rather than the entry never having been built.
///
/// Whether a real document can produce a source with no variants is not the
/// question this pins. An entry that wrote nothing is not on disk, whether or
/// not a config can reach it.
#[tokio::test]
async fn the_overall_row_finishes_error_when_a_folder_resolves_to_no_variants() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let report = sync_hierarchy(
        &paths,
        &folder_without_variants_document(),
        &mut MediaPmState::default(),
        &cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await
    .expect("an entry with no variants leaves a report to read; it is not a failed run");

    let folder = paths.hierarchy_root_dir.join(FOLDER_PATH);
    assert_eq!(
        std::fs::read_dir(&folder).unwrap().count(),
        0,
        "the folder arm has to have run and found nothing to write, or the counts below say \
         nothing about it; {} is not an empty directory",
        folder.display()
    );
    assert_eq!(
        report.missing_paths, 1,
        "the folder wrote no variant at all, so the library is short of the entry the document \
         asked for; got {report:?}"
    );
    assert_eq!(
        report.materialized_paths, 0,
        "a folder that wrote nothing is not a materialized path; got {report:?}"
    );
    assert_eq!(
        report.skipped_paths, 0,
        "nothing was on disk to match, so this is not an already-correct entry; got {report:?}"
    );
    assert_eq!(
        entry_finishes(&recording),
        vec![ProgressOp::FinishWarning],
        "the folder row must carry the warning, the way every other unwritten folder variant does; \
         got {ops:?}",
        ops = recording.ops()
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishError),
        "a folder left with no variant has not materialized the library, so the overall row must \
         end as an error; got {ops:?}",
        ops = recording.recorded()
    );
}

/// A folder holding a blocked variant beside a good one stays missing, and the
/// file the good variant wrote is still recorded.
///
/// The single-variant folder the neighbouring tests use cannot answer this.
/// It has nothing to write past the conflict, so a run that dropped the good
/// variant's arm entirely would leave the same counts. Here the good variant
/// is resolvable and the directory read back proves it wrote, while the
/// blocked one beside it keeps the entry in `missing_paths` rather than
/// moving it to `materialized_paths` on the strength of the file beside it.
#[tokio::test]
async fn the_overall_row_finishes_error_when_a_folder_mixes_a_blocked_and_a_good_variant() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(b"good-folder-variant")).await.unwrap();

    let occupied = paths.hierarchy_root_dir.join(FOLDER_PATH).join(BLOCKED_VARIANT);
    tokio::fs::create_dir_all(&occupied).await.unwrap();

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let report = sync_hierarchy(
        &paths,
        &folder_with_blocked_and_good_variants_document(&hash.to_string()),
        &mut MediaPmState::default(),
        &cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await
    .expect("one blocked variant among two leaves a report to read; it is not a failed run");

    let written = paths.hierarchy_root_dir.join(FOLDER_PATH).join(FOLDER_VARIANT);
    assert!(
        written.is_file(),
        "the resolvable variant has to have been written, or the missing count below is also what \
         a run that wrote nothing at all reports"
    );
    assert_eq!(
        report.missing_paths, 1,
        "the blocked variant left the library short of one file inside a folder it did write, so \
         the entry is missing; got {report:?}"
    );
    assert_eq!(
        report.materialized_paths, 0,
        "the file the good variant wrote does not put the folder on disk in full; got {report:?}"
    );
    assert_eq!(
        report.skipped_paths, 0,
        "a folder that refused one variant never matched what this run resolved, so nothing was \
         skipped; got {report:?}"
    );
    assert_eq!(
        entry_finishes(&recording),
        vec![ProgressOp::FinishWarning],
        "the folder row must carry the warning, the way a folder with only a blocked variant does; \
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

/// The terminal op of the bar the tracker opened under `label`, or `None`
/// when that bar never finished.
///
/// A ZIP folder variant opens a member row under the folder's own row, so
/// [`entry_finishes`] sees two finishes there and cannot say which one the
/// folder made. Reading the bar off its `AddBar` op names the row instead of
/// counting rows.
fn finish_of_bar_opened_as(tracker: &RecordingProgressTracker, label: &str) -> Option<ProgressOp> {
    let recorded = tracker.recorded();
    let bar = recorded.iter().find_map(|entry| match &entry.op {
        ProgressOp::AddBar { label: opened, .. } if opened == label => Some(entry.bar),
        _ => None,
    })?;
    recorded.iter().rev().find_map(|entry| {
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
