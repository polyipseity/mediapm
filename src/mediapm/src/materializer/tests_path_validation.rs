//! Hierarchy path validation, at the sanitizer and through a real `sync_hierarchy`.

use crate::config::MediaMetadataValue;
use crate::config::hierarchy_types::HierarchyPath;
use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::tests_common::{open_hierarchy_cas, single_media_document};

use super::*;

/// Builds a one-media-entry document whose source declares `metadata_key` as
/// `metadata_value`, in addition to the shape [`single_media_document`]
/// builds.
///
/// The metadata-carrying form exists because a media id is no longer a
/// legal route for unvalidated text into a path component:
/// `config::hierarchy_types::validate_media_id` rejects a separator at the
/// config boundary, so `${media.id}` can no longer reach the sanitizing
/// stage. `${media.metadata.<key>}` is the interpolation that still carries
/// text the user does not control, and therefore the one the sanitizing
/// stage must normalize.
fn single_media_document_with_metadata(
    media_id: &str,
    metadata_key: &str,
    metadata_value: &str,
    path: HierarchyPath,
) -> MediaPmDocument {
    let mut document = single_media_document(media_id, path);
    let source = document.media.get_mut(media_id).unwrap();
    source
        .metadata
        .insert(metadata_key.to_string(), MediaMetadataValue::Literal(metadata_value.to_string()));
    document
}

/// End-to-end negative control for hierarchy path validation on the
/// materializer commit path.
///
/// A statically declared `..` component survives the config-level
/// reserved-character check (`.` is not reserved), so it reaches
/// `sync_hierarchy`. Before the validation chain was wired in, the entry
/// was processed and its `hierarchy_root/..` target — one level *above*
/// the library root — was accepted, so this test fails on that tree.
///
/// The guarantee protected here: a rejected component aborts the sync
/// before any worker starts, so no staging bar is created and nothing is
/// written outside the hierarchy root.
#[tokio::test]
async fn regression_sync_hierarchy_rejects_parent_traversal_component() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;

    let document = single_media_document("src1", HierarchyPath::simple(".."));
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

    let Err(error) = result else {
        panic!(
            "sync_hierarchy accepted a '..' path component, which would commit \
                 outside the hierarchy root"
        );
    };
    assert!(
        error.to_string().contains("must not be '.' or '..'"),
        "unexpected rejection reason: {error}"
    );
    assert_eq!(
        recording.ops(),
        vec![ProgressOp::AddBar { total: 1, label: "materializing".into() }],
        "rejection must precede the overall-bar setup and every entry worker, \
             so no per-entry staging bar was ever created"
    );
}

/// End-to-end proof that the *sanitized* component — not the raw
/// interpolated one — is what the commit path uses.
///
/// A metadata value is text the user does not control (a tag, an ffprobe
/// string, an upstream title), and `${media.metadata.<key>}` interpolation
/// is how it first reaches a hierarchy path component. Before the chain was
/// wired in, the per-entry staging bar was labelled with the raw
/// `AC/DC [stg]`, i.e. the separator survived into a path component that
/// `hierarchy_root.join(...)` would split. The effective
/// `SanitizeNamesConfig` for this entry resolves to `Enabled` (the
/// flattening default), so the separator is rewritten to `_` and the commit
/// path sees `AC_DC`.
///
/// The `${media.id}` route this test originally used is no longer a case:
/// `config::hierarchy_types::validate_media_id` rejects a separator in a
/// media id at the config boundary, because an id is the user's own key and
/// splitting it between the state and the disk is an identity split rather
/// than a spelling the materializer may fix.
#[tokio::test]
async fn regression_sync_hierarchy_sanitizes_interpolated_path_component() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;

    let document = single_media_document_with_metadata(
        "src1",
        "artist",
        "AC/DC",
        HierarchyPath::simple("${media.metadata.artist}"),
    );
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

    assert!(result.is_ok(), "sanitization must not fail the sync: {result:?}");
    assert_eq!(
        recording.ops(),
        vec![
            ProgressOp::AddBar { total: 1, label: "materializing".into() },
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::SetTruncation { prefix: "materializing".into(), suffix: String::new() },
            ProgressOp::AddBar { total: 1, label: "AC_DC [stg]".into() },
            ProgressOp::SetTruncation { prefix: "AC_DC [stg]".into(), suffix: String::new() },
            ProgressOp::SetTruncation { prefix: "AC_DC [vrf]".into(), suffix: String::new() },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::SetTruncation { prefix: "[W] AC_DC [vrf]".into(), suffix: String::new() },
            ProgressOp::FinishWarning,
            ProgressOp::Advance { delta: 1 },
            // The entry was skipped, so the overall row ends as an error.
            ProgressOp::FinishError,
        ],
        "the interpolated separator must be sanitized out of the committed path; the \
             overall row ends as an error because the entry resolved no content hash",
    );
}

/// The sanitizer returns parsed components, and the returned entry renders
/// the sanitized spelling rather than the declared one.
///
/// `sanitize_and_validate_hierarchy_paths` is the only producer of
/// `ValidatedHierarchyEntry`, so this is the seam the whole type split
/// stands on: if it returned the declared text, every read site downstream
/// would be joining an unvalidated string. A reserved character is injected
/// into the flattened entry directly, because the config boundary refuses
/// one in a declared component and the end-to-end rewriting of an
/// interpolated one is already covered by
/// `regression_sync_hierarchy_sanitizes_interpolated_path_component`.
#[test]
fn sanitize_and_validate_hierarchy_paths_returns_parsed_components() {
    let document = single_media_document("src1", HierarchyPath::simple("album"));
    let mut flattened = flatten_hierarchy_nodes_for_runtime(&document.hierarchy).unwrap();
    flattened[0].path_components = vec!["AC/DC".to_string()];

    let validated = sanitize_and_validate_hierarchy_paths(flattened).unwrap();
    assert_eq!(validated.len(), 1);
    assert_eq!(validated[0].relative_path_text(), "AC_DC");
}

/// A component the parser refuses fails the sanitizer, and therefore the
/// whole sync, before any worker starts.
///
/// `..` is the case the config boundary deliberately lets through: it is
/// not a reserved character, so `validate_hierarchy_path_component`
/// accepts it, and the materializer is the only stage that refuses it. If
/// the split had let the unvalidated entry through, this sync would commit
/// one level above the library root.
#[test]
fn sanitize_and_validate_hierarchy_paths_rejects_parent_traversal_component() {
    let document = single_media_document("src1", HierarchyPath::simple(".."));
    let flattened = flatten_hierarchy_nodes_for_runtime(&document.hierarchy).unwrap();
    let err = sanitize_and_validate_hierarchy_paths(flattened)
        .expect_err("a '..' component must fail the sanitizer");
    assert!(
        err.to_string().contains("must not be '.' or '..'"),
        "unexpected rejection reason: {err}"
    );
}
