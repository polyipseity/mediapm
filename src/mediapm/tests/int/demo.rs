//! Config-flow guardrails for local media source setup.
//!
//! Tests emulate the `mediapm_demo` example's configuration path (add local
//! source, hierarchy, tools) without requiring filesystem probes or network
//! access.
//!
//! Reads persisted `.ncl` files as raw JSON (no Nickel evaluation) since
//! the Nickel schema/contract files are absent in temp directories.

use mediapm::{MediaHierarchyPreset, MediaPmService, MediaSourceSpec, media_id_from_uri};
use std::fs;
use tempfile::tempdir;
use url::Url;

/// Reads a `MediaPmDocument` from a `.ncl` JSON file without Nickel
/// evaluation.
fn read_doc(path: &std::path::Path) -> mediapm::MediaPmDocument {
    let file = fs::File::open(path).expect("mediapm.ncl should exist");
    serde_json::from_reader(file).expect("mediapm.ncl should be valid JSON")
}

/// Adding a media source with a local URI persists the entry with no default
/// steps.
#[tokio::test]
async fn add_local_source_works() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    let uri = Url::parse("local:demo-fixture").expect("url must parse");
    let media_id = media_id_from_uri(&uri);
    assert_eq!(media_id, "demo-fixture");

    service.add_media_source(&MediaSourceSpec::default(), media_id.clone(), &uri, None, None)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    let source = doc.media.get(&media_id).expect("source exists");
    assert!(source.steps.is_empty(), "default MediaSourceSpec has no steps");
    Ok(())
}

/// Title and description set during add are preserved in the document.
#[tokio::test]
async fn add_local_source_with_explicit_title_and_description() -> Result<(), mediapm::MediaPmError>
{
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    let uri = Url::parse("local:demo-fixture").expect("url must parse");
    let media_id = media_id_from_uri(&uri);

    service.add_media_source(
        &MediaSourceSpec::default(),
        media_id.clone(),
        &uri,
        Some("Demo Fixture"),
        Some("A local test fixture for demo"),
    )?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    let source = doc.media.get(&media_id).expect("source exists");
    assert_eq!(source.title.as_deref(), Some("Demo Fixture"));
    assert_eq!(source.description.as_deref(), Some("A local test fixture for demo"));
    Ok(())
}

/// Adding a `Local` hierarchy preset produces non-empty hierarchy nodes.
#[tokio::test]
async fn add_local_hierarchy_preset_creates_expected_nodes() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    service.add_media_hierarchy_preset(MediaHierarchyPreset::Local)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(!doc.hierarchy.is_empty(), "Local preset should produce hierarchy nodes");
    Ok(())
}

/// One tool requirement for a managed tool (media-tagger) persists.
///
/// Note: only one `add_tool_requirement` call per test is reliable because
/// `ensure_and_load_mediapm_document` uses Nickel evaluation internally,
/// which fails in temp directories without schema files.
#[tokio::test]
async fn add_tool_requirement_persists_single_call() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    service.add_tool_requirement("media-tagger", None, None)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(doc.tools.contains_key("media-tagger"), "media-tagger should be registered");
    // rsgain was not added in this test — only one tool per test to avoid
    // a second `ensure_and_load_mediapm_document` call.
    assert!(!doc.tools.contains_key("rsgain"), "rsgain should not be present");
    // import is a builtin, not a managed catalog tool.
    assert!(!doc.tools.contains_key("import"), "builtins are not in tool catalog");
    Ok(())
}
