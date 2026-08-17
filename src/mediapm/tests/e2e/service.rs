//! API-level end-to-end tests using programmatic `MediaPmService` flows.

use crate::common::{read_doc, service_in_tempdir};
use mediapm::{AddInsertPosition, MediaHierarchyPreset, MediaSourceSpec, media_id_from_uri};
use url::Url;

// ---------------------------------------------------------------------------
// Source lifecycle
// ---------------------------------------------------------------------------

/// Adding a media source persists the entry in the document.
#[tokio::test]
async fn add_source_persists() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root) = service_in_tempdir().await?;

    let uri = Url::parse("local:test-asset").expect("url must parse");
    let media_id = media_id_from_uri(&uri);
    service.add_media_source(&MediaSourceSpec::default(), media_id.clone(), &uri, None, None)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(doc.media.contains_key(&media_id), "source should exist after add");

    Ok(())
}

/// Adding a local source caches its ffprobe metadata under
/// `<runtime>/cache/mediapm/metadata.cache.json` keyed by `ffprobe:{path}`.
#[tokio::test]
async fn add_local_source_caches_metadata() -> Result<(), mediapm::MediaPmError> {
    // Skip when ffprobe is not installed (e.g., minimal CI images).
    if std::process::Command::new("ffprobe").arg("-version").output().is_err() {
        eprintln!("skipping: ffprobe not available");
        return Ok(());
    }
    let (mut service, root) = service_in_tempdir().await?;

    let media_path = root.path().join("track.wav");
    // Minimal valid 8-bit mono 8 kHz PCM WAV so ffprobe can read metadata.
    let wav: &[u8] = &[
        b'R',
        b'I',
        b'F',
        b'F',
        36u32.to_le_bytes()[0],
        36u32.to_le_bytes()[1],
        36u32.to_le_bytes()[2],
        36u32.to_le_bytes()[3],
        b'W',
        b'A',
        b'V',
        b'E',
        b'f',
        b'm',
        b't',
        b' ',
        16,
        0,
        0,
        0,
        1,
        0,
        1,
        0,
        0x40,
        0x1f,
        0,
        0,
        0x40,
        0x1f,
        0,
        0,
        1,
        0,
        8,
        0,
        b'd',
        b'a',
        b't',
        b'a',
        8,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    ];
    std::fs::write(&media_path, wav).expect("write media file");
    let media_id =
        service.add_local_source(&media_path, "ffprobe", None, AddInsertPosition::End)?;
    assert!(!media_id.is_empty(), "local add must produce a media id");

    // The cache flushes when the service is dropped; drop before asserting.
    drop(service);

    let cache_path =
        root.path().join(".mediapm").join("cache").join("mediapm").join("metadata.cache.json");
    assert!(cache_path.is_file(), "metadata cache must be written after add");
    let cache_content = std::fs::read_to_string(&cache_path).expect("read metadata cache");
    assert!(
        cache_content.contains(&format!("ffprobe:{}", media_path.display())),
        "cache must contain ffprobe:{{path}} key"
    );

    Ok(())
}

/// Adding a source with a title and description preserves the metadata.
#[tokio::test]
async fn add_source_with_metadata() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root) = service_in_tempdir().await?;

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
#[tokio::test]
async fn add_tool_without_version_persists() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root) = service_in_tempdir().await?;

    service.add_tool_requirement("ffmpeg", None)?;

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
    let (mut service, _root) = service_in_tempdir().await?;

    service.add_media_hierarchy_preset(MediaHierarchyPreset::Local)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(!doc.hierarchy.is_empty(), "Local preset should produce hierarchy nodes");

    Ok(())
}

/// Adding a `YtDlpChannel` hierarchy preset creates non-empty hierarchy nodes.
#[tokio::test]
async fn add_channel_hierarchy_preset() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root) = service_in_tempdir().await?;

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
    let (mut service, _root) = service_in_tempdir().await?;

    // The service currently does not validate URI schemes; any scheme is
    // accepted.
    let uri = Url::parse("ftp://files.example.com/video.mkv").expect("url");
    let media_id = media_id_from_uri(&uri);
    service.add_media_source(&MediaSourceSpec::default(), media_id.clone(), &uri, None, None)?;

    let doc = read_doc(&service.paths().mediapm_ncl);
    assert!(doc.media.contains_key(&media_id), "source added with ftp scheme should persist");

    Ok(())
}
