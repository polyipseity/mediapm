//! A user-authored `media` map key is one identity with three consumers.
//!
//! The same string reaches the hierarchy as `${media.id}`, the state as a
//! `workflow_states` key, and every `managed_files` record, so a spelling that
//! one of those consumers rewrites is an identity split: the file lands on disk
//! under `a_b` while the state records `a/b`, and no later join can find it.
//! The config boundary therefore *rejects* such an id instead of sanitizing
//! it, because the key is the user's to spell correctly.
//!
//! The three guarantees pinned here:
//!
//! 1. A media id that cannot survive one spelling is refused with an error that
//!    names both the id and the rule, and nothing is written under the
//!    sanitized spelling either. The same assertion runs for every splitting
//!    shape, because a path separator and a reserved character are the *same*
//!    defect reached by a different character.
//! 2. A media key that **no hierarchy node binds** is refused at the same
//!    boundary. The hierarchy walk only sees a key once a node binds it, so the
//!    all-keys observation lives at the workflow-synthesis boundary, which
//!    iterates the whole `media` map.
//! 3. An accepted media id keeps **one** spelling across the materialized path
//!    and the state keys, which is the invariant the rejection exists to
//!    protect.

use std::collections::BTreeMap;

use crate::common::{seed_cas, service_at, sync_library_with_test_terminal};
use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaPmDocument, MediaSourceSpec,
    PlaylistFormat, SanitizeNamesConfig, load_mediapm_state_document, save_mediapm_document,
};

/// Media id of the accepted source, and the folder its id materializes as.
const MEDIA_ID: &str = "vid1";

/// Every id shape that splits into two spellings, with the rule it is refused
/// under and the on-disk spelling the sanitizer would have produced.
///
/// `a/b` splits by re-splitting into extra path components; `a:b` splits by
/// being rewritten to `a_b` by the reserved-character replacement map. Both end
/// in the same `a_b` on disk with the raw spelling still in the state, so both
/// run through the same assertions — a fix that covers only one of them leaves
/// the defect reachable.
const SPLITTING_MEDIA_IDS: [(&str, &str, &str); 2] =
    [("a/b", "path separator", "a_b"), ("a:b", "reserved character", "a_b")];

/// Hierarchy path template that puts the media id itself in the materialized
/// path, so the disk spelling of the id is observable on disk.
const HIERARCHY_PATH: &str = "${media.id}/track.mp4";

/// The variant the single CAS payload is published under.
const VARIANT: &str = "default";

/// Payload stored in CAS for [`VARIANT`].
const PAYLOAD: &[u8] = b"media identity payload";

/// Builds the document under test: one media source bound to `media_id` at the
/// [`HIERARCHY_PATH`] template, with `sanitize_names` set as given so a
/// reserved character in the id is rewritten rather than refused by the
/// materializer.
fn document_with_media_id(
    media_id: &str,
    variant_hash: &str,
    sanitize_names: SanitizeNamesConfig,
) -> MediaPmDocument {
    MediaPmDocument {
        media: BTreeMap::from([(
            media_id.to_string(),
            MediaSourceSpec {
                variant_hashes: BTreeMap::from([(VARIANT.to_string(), variant_hash.to_string())]),
                steps: Vec::new(),
                ..MediaSourceSpec::default()
            },
        )]),
        hierarchy: vec![HierarchyNode {
            path: HierarchyPath::from(HIERARCHY_PATH),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some(media_id.to_string()),
            variant: Some(VARIANT.to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: Some(sanitize_names),
            children: Vec::new(),
        }],
        ..MediaPmDocument::default()
    }
}

/// Seeds CAS, saves the document for `media_id`, and runs one library sync
/// against a fresh workspace.
///
/// The workspace [`TempDir`] is returned rather than its path so the caller
/// keeps it alive: dropping it deletes the very tree the on-disk spelling
/// assertions inspect, turning a real check into a vacuous one.
async fn sync_media_id(
    media_id: &str,
    sanitize_names: SanitizeNamesConfig,
) -> Result<
    (Result<mediapm::SyncSummary, mediapm::MediaPmError>, tempfile::TempDir),
    mediapm::MediaPmError,
> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let mut service = service_at(root.path(), None).await?;
    let hash = seed_cas(&service, Bytes::from_static(PAYLOAD), "media identity variant").await?;
    let cas = service.conductor().cas().clone();
    cas.ensure_blob_materialized(hash).await.map_err(|source| {
        mediapm::MediaPmError::Workflow(format!("ensure blob materialized: {source}"))
    })?;

    let document = document_with_media_id(media_id, &hash.to_string(), sanitize_names);
    save_mediapm_document(&service.paths().mediapm_ncl, &document)?;
    let outcome = sync_library_with_test_terminal(&mut service, false).await;
    // Release the CAS `store/lock` before the caller inspects the tree.
    drop(service);
    Ok((outcome, root))
}

/// Reads `state.json` written under the workspace `.mediapm/` root.
///
/// The path is rebuilt from the workspace root rather than taken from the
/// service because the service is dropped before the assertions run: its CAS
/// handle holds an exclusive `flock` on the same store.
fn read_synced_state(
    root: &std::path::Path,
) -> Result<mediapm::MediaPmState, mediapm::MediaPmError> {
    load_mediapm_state_document(&root.join(".mediapm").join("state.json"))
}

/// A media id carrying a splitting shape is refused, the error names the id and
/// the rule, and nothing is written under the sanitized spelling either.
///
/// Sanitization is enabled on purpose: that is the configuration in which the
/// pre-fix run wrote `a_b/track.mp4` to disk while the state key stayed `a/b`.
/// The loop covers the path separator and the reserved character because the
/// sanitized spelling is the same string for both, so a single assertion over
/// both inputs is the whole identity-splitting contract.
#[tokio::test]
async fn sync_rejects_media_id_with_splitting_shape_and_writes_nothing()
-> Result<(), mediapm::MediaPmError> {
    for (media_id, rule, sanitized_spelling) in SPLITTING_MEDIA_IDS {
        let (outcome, root) = sync_media_id(media_id, SanitizeNamesConfig::Enabled).await?;

        let error = outcome
            .as_ref()
            .err()
            .unwrap_or_else(|| panic!("media id '{media_id}' must be refused, got {outcome:?}"));
        let rendered = error.to_string();
        assert!(
            rendered.contains(media_id),
            "the error must name the offending media id; got: {rendered}"
        );
        assert!(
            rendered.contains(rule),
            "the error for '{media_id}' must state the violated rule '{rule}'; got: {rendered}"
        );

        let sanitized = root.path().join(sanitized_spelling).join("track.mp4");
        assert!(
            !sanitized.exists(),
            "the refused media id '{media_id}' must not be written under its sanitized \
             spelling '{}': '{}' exists",
            sanitized_spelling,
            sanitized.display()
        );
    }
    Ok(())
}

/// A refused media id must leave no row in the state under either spelling.
///
/// The state half of the identity invariant: before the boundary rejected a
/// splitting id, the sync succeeded with `workflow_states` keyed by the raw
/// `a/b` while the library root held `a_b/track.mp4`. A refusal is only
/// meaningful if the unsplit and the sanitized spelling both leave nothing
/// behind to be joined against later. Parameterized over the same splitting
/// shapes, because the state half is where both of them split.
#[tokio::test]
async fn refused_media_id_leaves_no_state_row_under_either_spelling()
-> Result<(), mediapm::MediaPmError> {
    for (media_id, _rule, sanitized_spelling) in SPLITTING_MEDIA_IDS {
        let (outcome, root) = sync_media_id(media_id, SanitizeNamesConfig::Enabled).await?;
        assert!(
            outcome.is_err(),
            "the splitting media id '{media_id}' must be refused, got {outcome:?}"
        );

        let state = read_synced_state(root.path())?;
        let state_keys: Vec<&String> = state.workflow_states.keys().collect();
        assert!(
            state_keys.is_empty(),
            "a refused media id must leave no workflow_states row, got {state_keys:?}"
        );
        assert!(
            state.managed_files.is_empty(),
            "a refused media id must leave no managed_files row, got {:?}",
            state.managed_files.keys().collect::<Vec<_>>()
        );
        assert!(
            !root.path().join(sanitized_spelling).exists() && !root.path().join(media_id).exists(),
            "a refused media id must leave no materialized tree under either spelling"
        );
    }
    Ok(())
}

/// Media key no hierarchy node binds, carrying a splitting shape.
///
/// The document also carries a valid, *referenced* media id, so the test fails
/// only when the unreferenced key is what the sync chokes on.
const UNREFERENCED_SPLITTING_MEDIA_ID_PLACEHOLDER: &str = "orphan/b";

/// Seeds CAS, saves a document whose hierarchy binds only [`MEDIA_ID`] while
/// `media` also carries `unreferenced_id`, and runs one library sync.
///
/// The unreferenced key never reaches the hierarchy walk, so the only
/// production code that observes it is the workflow-synthesis boundary that
/// iterates the whole `media` map. The workspace [`TempDir`] is returned rather
/// than its path so the caller keeps it alive.
#[allow(dead_code)]
async fn sync_unreferenced_media_id(
    unreferenced_id: &str,
) -> Result<
    (Result<mediapm::SyncSummary, mediapm::MediaPmError>, tempfile::TempDir),
    mediapm::MediaPmError,
> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let mut service = service_at(root.path(), None).await?;
    let hash = seed_cas(&service, Bytes::from_static(PAYLOAD), "media identity variant").await?;
    let cas = service.conductor().cas().clone();
    cas.ensure_blob_materialized(hash).await.map_err(|source| {
        mediapm::MediaPmError::Workflow(format!("ensure blob materialized: {source}"))
    })?;

    let mut document =
        document_with_media_id(MEDIA_ID, &hash.to_string(), SanitizeNamesConfig::Enabled);
    document.media.insert(
        unreferenced_id.to_string(),
        MediaSourceSpec {
            variant_hashes: BTreeMap::from([(VARIANT.to_string(), hash.to_string())]),
            steps: Vec::new(),
            ..MediaSourceSpec::default()
        },
    );
    save_mediapm_document(&service.paths().mediapm_ncl, &document)?;
    let outcome = sync_library_with_test_terminal(&mut service, false).await;
    drop(service);
    Ok((outcome, root))
}

/// A media key **no hierarchy node binds** is still refused.
///
/// The hierarchy walk is not an all-keys boundary: it validates a media id only
/// once a node binds it, so an orphaned key never passed the config boundary
/// even though the workflow synthesizer iterates the whole `media` map and
/// turns every key into a `media/{id}` workflow whose name reaches log lines.
/// This is the test that pins that gap shut.
#[allow(dead_code)]
#[tokio::test]
async fn sync_rejects_unreferenced_media_id_with_splitting_shape()
-> Result<(), mediapm::MediaPmError> {
    let (outcome, root) =
        sync_unreferenced_media_id(UNREFERENCED_SPLITTING_MEDIA_ID_PLACEHOLDER).await?;

    let error = outcome.as_ref().err().unwrap_or_else(|| {
        panic!(
            "an unreferenced media id '{UNREFERENCED_SPLITTING_MEDIA_ID_PLACEHOLDER}' must be \
             refused, got {outcome:?}"
        )
    });
    let rendered = error.to_string();
    assert!(
        rendered.contains(UNREFERENCED_SPLITTING_MEDIA_ID_PLACEHOLDER),
        "the error must name the unreferenced media id; got: {rendered}"
    );
    assert!(
        rendered.contains("path separator"),
        "the error must state the violated rule; got: {rendered}"
    );

    assert!(
        !root.path().join("orphan_b").exists(),
        "the refused unreferenced media id must leave no materialized tree under its sanitized \
         spelling"
    );
    Ok(())
}

/// An ordinary media id is accepted and the sync succeeds.
///
/// Guards the rule against over-rejection: only the shapes that break identity
/// are refused, so the ids the tree already uses keep working.
#[tokio::test]
async fn sync_accepts_plain_media_id() -> Result<(), mediapm::MediaPmError> {
    let (outcome, root) = sync_media_id(MEDIA_ID, SanitizeNamesConfig::Inherit).await?;
    let summary = outcome?;
    assert!(summary.materialized_paths > 0, "the entry must materialize: {summary:?}");
    assert!(
        root.path().join(MEDIA_ID).join("track.mp4").exists(),
        "an accepted media id must materialize under its own spelling"
    );
    Ok(())
}

/// The state key and the materialized path must agree on one spelling of the
/// media id.
///
/// This is the invariant the rejection exists to protect. `workflow_states` is
/// keyed by the raw id the hierarchy declared, `managed_files` by the relative
/// path that reached disk, and the directory on disk by the id after
/// sanitization. The assertion is not "a file exists" but "the state key, the
/// managed-file key prefix, and the on-disk directory are the same string" —
/// which is precisely what a media id that sanitizes to something else breaks.
#[tokio::test]
async fn state_key_and_materialized_path_share_one_media_id_spelling()
-> Result<(), mediapm::MediaPmError> {
    let (outcome, root) = sync_media_id(MEDIA_ID, SanitizeNamesConfig::Enabled).await?;
    let summary = outcome?;
    assert!(summary.materialized_paths > 0, "the entry must materialize: {summary:?}");

    let state = read_synced_state(root.path())?;

    let state_keys: Vec<&String> = state.workflow_states.keys().collect();
    assert_eq!(
        state_keys,
        vec![MEDIA_ID],
        "workflow_states must hold exactly the accepted media id, unsanitized"
    );

    let managed_keys: Vec<&String> = state.managed_files.keys().collect();
    assert_eq!(
        managed_keys.len(),
        1,
        "the entry must record exactly one managed file: {managed_keys:?}"
    );
    let managed_key = managed_keys[0];
    assert_eq!(
        managed_key,
        &format!("{MEDIA_ID}/track.mp4"),
        "the managed_files key must be spelled from the same id as the state key"
    );
    assert_eq!(
        state.managed_files[managed_key].media_id, MEDIA_ID,
        "the managed file's media_id must agree with its own key's spelling"
    );

    let materialized = root.path().join(MEDIA_ID).join("track.mp4");
    assert!(
        materialized.exists(),
        "the materialized path must use the same spelling as the state keys: '{}' is missing",
        materialized.display()
    );
    assert_eq!(
        managed_key.split('/').next(),
        Some(state_keys[0].as_str()),
        "the first path component of a managed file must equal its media id"
    );
    Ok(())
}
