//! The progress ops one `sync_hierarchy` run records, and the terminal markers it installs.

use crate::config::hierarchy_types::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, PlaylistFormat, SanitizeNamesConfig,
};
use crate::config::source_types::{MediaStep, MediaStepTool};
use crate::config::{GenericOutputVariantConfig, OutputVariantValue};
use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::tests_common::{open_hierarchy_cas, single_media_document};

use super::*;

/// Injected [`RecordingProgressTracker`] with overall bar produces no ops
/// beyond the overall `AddBar` when hierarchy is empty (early return
/// before any per-entry progress bar work).
#[tokio::test]
async fn sync_hierarchy_with_empty_hierarchy_no_progress_ops() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());

    // Create a CAS at the runtime store path (needed for the CAS parameter,
    // though it's unused in the empty-hierarchy fast path).
    let cas_root = paths.runtime_root.join("store");
    tokio::fs::create_dir_all(&cas_root).await.unwrap();
    let cas = FileSystemCas::open(&cas_root).await.unwrap();

    let document = MediaPmDocument::default();
    let mut state = MediaPmState::default();
    let conductor_state = ConductorState::new_empty();
    let generated_doc = NickelDocument::default();

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let result = sync_hierarchy(
        &paths,
        &document,
        &mut state,
        &cas,
        true,
        &conductor_state,
        &generated_doc,
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall)),
    )
    .await;

    assert!(result.is_ok());
    let ops = recording.ops();
    // Only the overall bar AddBar from with_overall(); the early return
    // prevents set_total/set_prefix_components from running.
    assert_eq!(
        ops,
        vec![ProgressOp::AddBar { total: 1, label: "materializing".into() }],
        "empty hierarchy should only produce the overall AddBar, got {ops:?}",
    );
}

/// Single media entry with no CAS content emits the full progress
/// sequence: overall bar → per-entry `[stg]`/`[vrf]` phases →
/// `Advance(1)` + `FinishWarning` (skipped) → overall
/// `Advance(1)` + `FinishError`.
///
/// The order is deterministic because the single spawned task completes
/// (advance + `entry_bar` ops) before the overall bar is finished.
///
/// The overall row ends as an error because the entry was skipped, so this
/// test also pins where that finish falls in the sequence rather than only
/// that it happens.
#[tokio::test]
async fn sync_hierarchy_with_single_media_produces_progress_ops() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());

    let cas_root = paths.runtime_root.join("store");
    tokio::fs::create_dir_all(&cas_root).await.unwrap();
    let cas = FileSystemCas::open(&cas_root).await.unwrap();

    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "src1".into(),
            MediaSourceSpec {
                steps: vec![MediaStep {
                    tool: MediaStepTool::Import,
                    input_variants: vec![],
                    output_variants: BTreeMap::from([(
                        "default".into(),
                        OutputVariantValue::Generic(GenericOutputVariantConfig {
                            kind: "primary".to_string(),
                            ..Default::default()
                        }),
                    )]),
                    options: BTreeMap::new(),
                }],
                ..MediaSourceSpec::default()
            },
        )]),
        hierarchy: vec![HierarchyNode {
            path: HierarchyPath::simple("test_file"),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some("src1".into()),
            variant: Some("default".into()),
            variants: vec![],
            rename_files: vec![],
            format: PlaylistFormat::M3u8,
            ids: vec![],
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
            children: vec![],
        }],
        ..MediaPmDocument::default()
    };
    let mut state = MediaPmState::default();
    let conductor_state = ConductorState::new_empty();
    let generated_doc = NickelDocument::default();

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let result = sync_hierarchy(
        &paths,
        &document,
        &mut state,
        &cas,
        true,
        &conductor_state,
        &generated_doc,
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall)),
    )
    .await;

    assert!(result.is_ok(), "sync_hierarchy should succeed: {result:?}");
    let ops = recording.ops();
    // Exact op sequence: overall bar (AddBar from with_overall,
    // then SetTotal + SetTruncation from sync_hierarchy) → per-entry
    // `[stg]`/`[vrf]` phases → `Advance(1)` + `FinishWarning` (skipped, no
    // CAS content) → overall `Advance(1)` + `FinishError`.
    assert_eq!(
        ops,
        vec![
            // Overall bar created by with_overall(), total set by sync_hierarchy.
            ProgressOp::AddBar { total: 1, label: "materializing".into() },
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::SetTruncation { prefix: "materializing".into(), suffix: String::new() },
            // Per-entry bar: staging.
            ProgressOp::AddBar { total: 1, label: "test_file [stg]".into() },
            ProgressOp::SetTruncation { prefix: "test_file [stg]".into(), suffix: String::new() },
            // Per-entry bar: verify phase (set before hash resolution).
            ProgressOp::SetTruncation { prefix: "test_file [vrf]".into(), suffix: String::new() },
            // Skipped: advance(1) on entry_bar, the `[W]` label that
            // names the skip in the text, then FinishWarning.
            ProgressOp::Advance { delta: 1 },
            ProgressOp::SetTruncation {
                prefix: "[W] test_file [vrf]".into(),
                suffix: String::new(),
            },
            ProgressOp::FinishWarning,
            // Overall bar: advance(1) after entry completes + finish_error,
            // because a skipped path has not been materialized.
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishError,
        ],
        "\nops mismatch — expected the overall bar + [stg]→[vrf] skip path",
    );
}

/// The label installed immediately before a warning finish carries `[W]`.
///
/// `finish_warning` on its own only changes the bar's colour, and the
/// materialization label has no marker, so a skipped entry and a failed
/// one used to be indistinguishable in the text. The phase in the marker
/// label is the one the bar already shows, so the marker adds a fact
/// instead of replacing one.
#[tokio::test]
async fn sync_hierarchy_marks_a_skipped_entry_warning() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;

    let document = single_media_document("src1", HierarchyPath::simple("test_file"));
    let mut state = MediaPmState::default();
    let conductor_state = ConductorState::new_empty();
    let generated_doc = NickelDocument::default();

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    sync_hierarchy(
        &paths,
        &document,
        &mut state,
        &cas,
        true,
        &conductor_state,
        &generated_doc,
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall)),
    )
    .await
    .expect("sync_hierarchy should succeed");

    let ops = recording.ops();
    let finish = ops
        .iter()
        .position(|op| matches!(op, ProgressOp::FinishWarning))
        .expect("a media entry with no content hash finishes with a warning");
    assert_eq!(
        ops.get(finish.saturating_sub(1)),
        Some(&ProgressOp::SetTruncation {
            prefix: "[W] test_file [vrf]".into(),
            suffix: String::new(),
        }),
        "the label installed before the warning finish must carry [W]; got {ops:?}",
    );
}

/// The label installed immediately before a failed entry's finish carries
/// `[F]`, and a failure now reads the same way as a warning.
///
/// The failure is provoked through the folder arm rather than asserted
/// against a mock: a variant name of `..` is refused by the
/// path-component parser before any byte is written, which is the
/// cheapest real `Err` the materializer produces.
#[tokio::test]
async fn sync_hierarchy_marks_a_failed_folder_entry() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;

    let mut document = single_media_document("src1", HierarchyPath::simple("album"));
    document.hierarchy[0].kind = HierarchyNodeKind::MediaFolder;
    let source = document.media.get_mut("src1").unwrap();
    source.steps[0].output_variants = BTreeMap::from([(
        "..".to_string(),
        OutputVariantValue::Generic(GenericOutputVariantConfig {
            kind: "primary".to_string(),
            ..Default::default()
        }),
    )]);
    let mut state = MediaPmState::default();
    let conductor_state = ConductorState::new_empty();
    let generated_doc = NickelDocument::default();

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let result = sync_hierarchy(
        &paths,
        &document,
        &mut state,
        &cas,
        true,
        &conductor_state,
        &generated_doc,
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall)),
    )
    .await;

    assert!(result.is_err(), "a rejected variant name must fail the entry: {result:?}");
    let ops = recording.ops();
    let finish = ops
        .iter()
        .position(|op| matches!(op, ProgressOp::FinishError))
        .expect("a refused variant name finishes the entry bar with an error");
    assert_eq!(
        ops.get(finish.saturating_sub(1)),
        Some(&ProgressOp::SetTruncation {
            prefix: "[F] album [stg]".into(),
            suffix: String::new(),
        }),
        "the label installed before the error finish must carry [F]; got {ops:?}",
    );
}
