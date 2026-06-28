//! Config-flow guardrails for remote media source setup.
//!
//! Tests emulate the `mediapm_demo_online` example's configuration path (add
//! remote source, hierarchy, tools) without requiring network access.
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

/// Adding a remote source via `add_media_source` persists the entry with no
/// auto-generated steps (the caller provides the step pipeline).
///
/// Note: `media_id_from_uri` for remote URIs uses the host slug + content
/// hash prefix, not URL query parameters.
#[tokio::test]
async fn add_remote_source_works() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    let uri = Url::parse("https://www.youtube.com/watch?v=dQw4w9WgXcQ").expect("url must parse");
    let media_id = media_id_from_uri(&uri);
    // media_id_from_uri produces "youtube-com.<12-hex-chars>" for https URIs
    assert!(media_id.starts_with("youtube-com."), "expected host slug prefix");
    assert_eq!(media_id.len(), 12 + "youtube-com.".len());

    service.add_media_source(&MediaSourceSpec::default(), media_id.clone(), &uri, None, None)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    let source = doc.media.get(&media_id).expect("source exists");
    assert!(source.steps.is_empty(), "default MediaSourceSpec has no steps");
    Ok(())
}

/// Adding a `YtDlpChannel` hierarchy preset creates non-empty hierarchy nodes.
#[tokio::test]
async fn add_channel_hierarchy_preset_creates_expected_nodes() -> Result<(), mediapm::MediaPmError>
{
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    service.add_media_hierarchy_preset(MediaHierarchyPreset::YtDlpChannel)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(!doc.hierarchy.is_empty(), "YtDlpChannel preset should produce hierarchy nodes");
    Ok(())
}

/// A single tool requirement for a remote-relevant managed tool (yt-dlp)
/// persists.
///
/// Note: only one `add_tool_requirement` call per test is reliable because
/// `ensure_and_load_mediapm_document` uses Nickel evaluation internally.
#[tokio::test]
async fn add_one_remote_tool_requirement_persists() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    service.add_tool_requirement("yt-dlp", None, None)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(doc.runtime.tools.contains_key("yt-dlp"));
    // Other tools were not added in this test — only one tool per test.
    assert!(!doc.runtime.tools.contains_key("ffmpeg"));
    assert!(!doc.runtime.tools.contains_key("media-tagger"));
    assert!(!doc.runtime.tools.contains_key("rsgain"));
    assert!(!doc.runtime.tools.contains_key("sd"));
    // import is a builtin, not a managed catalog tool.
    assert!(!doc.runtime.tools.contains_key("import"), "builtins are not in tool catalog");
    Ok(())
}
