//! API-level end-to-end tests using programmatic `MediaPmService` flows.
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

// ---------------------------------------------------------------------------
// Source lifecycle
// ---------------------------------------------------------------------------

/// Adding a media source persists the entry in the document.
///
/// Note: tests with more than one service write-call are unreliable due
/// to Nickel evaluation constraints in temp directories (the second call
/// invokes `ensure_and_load_mediapm_document` which reads via Nickel).
#[tokio::test]
async fn add_source_persists() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    let uri = Url::parse("local:test-asset").expect("url must parse");
    let media_id = media_id_from_uri(&uri);
    service.add_media_source(&MediaSourceSpec::default(), media_id.clone(), &uri, None, None)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(doc.media.contains_key(&media_id), "source should exist after add");

    Ok(())
}

/// Adding a source with a title and description preserves the metadata.
#[tokio::test]
async fn add_source_with_metadata() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    let uri = Url::parse("local:test-asset").expect("url must parse");
    let media_id = media_id_from_uri(&uri);
    service.add_media_source(
        &MediaSourceSpec::default(),
        media_id.clone(),
        &uri,
        Some("My Title"),
        Some("A test asset description"),
    )?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    let source = doc.media.get(&media_id).expect("source exists");
    assert_eq!(source.title.as_str(), "My Title");
    assert_eq!(source.description.as_str(), "A test asset description");

    Ok(())
}

/// `media_id_from_uri` produces the expected media-id for local and remote
/// URI schemes.
///
/// Remote URIs produce a host-slug prefix + 12-char content-hash; they do
/// NOT extract URL query parameters like `v=`.
#[test]
fn media_id_parsing() {
    let local = media_id_from_uri(&Url::parse("local:my-file").expect("url"));
    assert_eq!(local, "my-file", "local: URI uses path segment");

    let remote =
        media_id_from_uri(&Url::parse("https://www.youtube.com/watch?v=dQw4w9WgXcQ").expect("url"));
    assert!(remote.starts_with("youtube-com."), "youtube URL uses host-slug prefix");
    assert_eq!(remote.len(), 12 + "youtube-com.".len(), "content hash is 12 hex chars");

    let remote_no_query = media_id_from_uri(&Url::parse("https://example.com/video").expect("url"));
    assert!(
        remote_no_query.starts_with("example-com."),
        "URL without query also uses host-slug prefix"
    );
    assert_eq!(remote_no_query.len(), 12 + "example-com.".len());
}

// ---------------------------------------------------------------------------
// Tool requirements
// ---------------------------------------------------------------------------

/// A single tool requirement persists in the document.
///
/// Note: only one `add_tool_requirement` call per test is reliable due to
/// Nickel evaluation constraints in temp directories.
#[tokio::test]
async fn add_tool_without_version_persists() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    service.add_tool_requirement("ffmpeg", None, None)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(doc.tools.contains_key("ffmpeg"), "ffmpeg should be registered");
    assert!(!doc.tools.contains_key("yt-dlp"), "yt-dlp was not added in this test");

    Ok(())
}

// ---------------------------------------------------------------------------
// Hierarchy presets
// ---------------------------------------------------------------------------

/// Adding a Local hierarchy preset creates non-empty hierarchy nodes.
#[tokio::test]
async fn add_local_hierarchy_preset() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    service.add_media_hierarchy_preset(MediaHierarchyPreset::Local)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(!doc.hierarchy.is_empty(), "Local preset should produce hierarchy nodes");

    Ok(())
}

/// Adding a `YtDlpChannel` hierarchy preset creates non-empty hierarchy nodes.
#[tokio::test]
async fn add_channel_hierarchy_preset() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    service.add_media_hierarchy_preset(MediaHierarchyPreset::YtDlpChannel)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(!doc.hierarchy.is_empty(), "YtDlpChannel preset should produce hierarchy nodes");

    Ok(())
}

// ---------------------------------------------------------------------------
// Source scheme validation
// ---------------------------------------------------------------------------

/// The service accepts any URI scheme without validation (scheme
/// enforcement is not implemented).
#[tokio::test]
async fn source_accepts_any_scheme() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;

    // The service currently does not validate URI schemes; any scheme is
    // accepted.
    let uri = Url::parse("ftp://files.example.com/video.mkv").expect("url");
    let media_id = media_id_from_uri(&uri);
    service.add_media_source(&MediaSourceSpec::default(), media_id.clone(), &uri, None, None)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(doc.media.contains_key(&media_id), "source added with ftp scheme should persist");

    Ok(())
}
