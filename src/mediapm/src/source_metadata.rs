//! Source metadata resolution for online and local media sources.
//!
//! This module provides utilities for fetching and resolving metadata from
//! online sources (via `yt-dlp`) and local files (via `ffprobe`), along with
//! parsing helpers that extract structured metadata from tool outputs.

use std::path::Path;
use std::process::Command;

use serde_json::Value;
use url::Url;

use crate::error::MediaPmError;
use crate::metadata_cache::MetadataCache;
use crate::paths::MediaPmPaths;
use crate::util::first_non_empty_json_string;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Metadata extracted from an online source.
#[allow(dead_code)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct OnlineSourceMetadata {
    /// Human-readable title.
    pub title: String,
    /// Human-readable artist.
    pub artist: String,
    /// Human-readable description.
    pub description: String,
}

/// Resolved metadata for a newly added online source.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ResolvedOnlineSourceMetadata {
    /// Human-readable title.
    pub title: String,
    /// Human-readable description.
    pub description: String,
    /// Human-readable artist.
    pub artist: String,
    /// Non-fatal warning message (if any).
    pub warning: Option<String>,
}

/// Metadata extracted from a local file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct LocalSourceMetadata {
    /// Human-readable title.
    pub title: String,
    /// Human-readable artist.
    pub artist: String,
    /// Human-readable description (may be empty).
    pub description: String,
}

// ---------------------------------------------------------------------------
// Online source metadata
// ---------------------------------------------------------------------------

/// Fetches metadata for an online source URI using `yt-dlp`.
///
/// Returns the raw JSON metadata from `yt-dlp --dump-json` as a
/// `serde_json::Value`.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if `yt-dlp` is unavailable or returns
/// a non-zero exit code.
#[allow(dead_code)]
pub(crate) fn try_fetch_online_source_metadata_with_yt_dlp(
    uri: &Url,
    yt_dlp_command: &str,
) -> Result<Value, MediaPmError> {
    let output = Command::new(yt_dlp_command)
        .args(["--dump-json", "--no-download", "--skip-download"])
        .arg(uri.as_str())
        .output()
        .map_err(|e| {
            MediaPmError::Workflow(format!("failed to execute yt-dlp for metadata fetch: {e}"))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(MediaPmError::Workflow(format!("yt-dlp metadata fetch failed: {stderr}")));
    }

    serde_json::from_slice(&output.stdout)
        .map_err(|e| MediaPmError::Workflow(format!("failed to parse yt-dlp JSON output: {e}")))
}

/// Parses `yt-dlp --dump-json` output into an [`OnlineSourceMetadata`].
#[must_use]
#[allow(dead_code)]
pub(crate) fn parse_online_source_metadata(value: &Value) -> OnlineSourceMetadata {
    let title = first_non_empty_json_string(value, &["title", "fulltitle", "webpage_url"])
        .unwrap_or_else(|| "Untitled".to_string());
    let artist = first_non_empty_json_string(value, &["uploader", "channel", "creator"])
        .unwrap_or_else(|| "Unknown".to_string());
    let description =
        first_non_empty_json_string(value, &["description", "synopsis"]).unwrap_or_default();

    OnlineSourceMetadata { title, artist, description }
}

/// Fetches and resolves online source metadata, returning a
/// [`ResolvedOnlineSourceMetadata`].
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the metadata fetch fails.
#[allow(dead_code)]
pub(crate) fn fetch_online_source_metadata(
    uri: &Url,
    yt_dlp_command: &str,
) -> Result<ResolvedOnlineSourceMetadata, MediaPmError> {
    let raw = try_fetch_online_source_metadata_with_yt_dlp(uri, yt_dlp_command)?;
    Ok(resolve_online_source_metadata_for_add(&raw, None))
}

/// Resolves raw yt-dlp metadata into a clean [`ResolvedOnlineSourceMetadata`].
#[must_use]
#[allow(dead_code)]
pub(crate) fn resolve_online_source_metadata_for_add(
    yt_dlp_metadata: &Value,
    warning: Option<String>,
) -> ResolvedOnlineSourceMetadata {
    let parsed = parse_online_source_metadata(yt_dlp_metadata);
    ResolvedOnlineSourceMetadata {
        title: parsed.title,
        description: parsed.description,
        artist: parsed.artist,
        warning,
    }
}

// ---------------------------------------------------------------------------
// Local source metadata
// ---------------------------------------------------------------------------

/// Fetches metadata for a local file using `ffprobe`.
///
/// Returns the raw JSON metadata from `ffprobe -v quiet -print_format json`.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if `ffprobe` is unavailable or returns
/// a non-zero exit code.
pub(crate) fn try_fetch_local_source_metadata_with_ffprobe(
    path: &Path,
    ffprobe_command: &str,
) -> Result<Value, MediaPmError> {
    let output = Command::new(ffprobe_command)
        .args(["-v", "quiet", "-print_format", "json", "-show_format", "-show_streams"])
        .arg(path)
        .output()
        .map_err(|e| {
            MediaPmError::Workflow(format!("failed to execute ffprobe for metadata fetch: {e}"))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(MediaPmError::Workflow(format!("ffprobe metadata fetch failed: {stderr}")));
    }

    serde_json::from_slice(&output.stdout)
        .map_err(|e| MediaPmError::Workflow(format!("failed to parse ffprobe JSON output: {e}")))
}

/// Parses ffprobe JSON output into a [`LocalSourceMetadata`].
///
/// Extracts title, artist, and description from the format tags.
#[must_use]
pub(crate) fn parse_local_source_metadata_from_ffprobe_json(value: &Value) -> LocalSourceMetadata {
    let tags = parse_local_format_tags(value);

    let title = first_non_empty_json_string(&tags, &["title", "TITLE"])
        .or_else(|| {
            value
                .get("format")
                .and_then(|f| f.get("filename"))
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "Untitled".to_string());

    let artist =
        first_non_empty_json_string(&tags, &["artist", "ARTIST", "album_artist", "ALBUM_ARTIST"])
            .unwrap_or_else(|| "Unknown".to_string());

    let description =
        first_non_empty_json_string(&tags, &["description", "DESCRIPTION", "comment", "COMMENT"])
            .unwrap_or_default();

    LocalSourceMetadata { title, artist, description }
}

/// Extracts format tags from ffprobe JSON output.
#[must_use]
fn parse_local_format_tags(value: &Value) -> Value {
    value
        .get("format")
        .and_then(|f| f.get("tags"))
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()))
}

/// Fetches local source metadata, using an optional cache.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if `ffprobe` fails.
pub(crate) fn fetch_local_source_metadata(
    path: &Path,
    ffprobe_command: &str,
    cache: Option<&MetadataCache>,
) -> Result<LocalSourceMetadata, MediaPmError> {
    let cache_key = format!("ffprobe:{}", path.display());

    // Check cache first
    if let Some(cache) = cache
        && let Some(cached) = cache.get(&cache_key)
        && let Ok(metadata) = serde_json::from_value::<LocalSourceMetadata>(cached)
    {
        return Ok(metadata);
    }

    let raw = try_fetch_local_source_metadata_with_ffprobe(path, ffprobe_command)?;
    let metadata = parse_local_source_metadata_from_ffprobe_json(&raw);

    // Update cache
    if let Some(cache) = cache
        && let Ok(value) = serde_json::to_value(&metadata)
    {
        cache.set(cache_key, value);
    }

    Ok(metadata)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves the conductor CAS store root path.
#[must_use]
pub(crate) fn resolve_conductor_cas_root(paths: &MediaPmPaths) -> std::path::PathBuf {
    paths.runtime_root.join("cas_store")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Ensures parsing valid yt-dlp JSON extracts title, artist, and description.
    #[test]
    fn parse_online_source_metadata_extracts_fields() {
        let input = json!({
            "title": "Test Video",
            "uploader": "Test Channel",
            "description": "A test video description"
        });
        let metadata = parse_online_source_metadata(&input);
        assert_eq!(metadata.title, "Test Video");
        assert_eq!(metadata.artist, "Test Channel");
        assert_eq!(metadata.description, "A test video description");
    }

    /// Ensures fallback to alternative yt-dlp keys works.
    #[test]
    fn parse_online_source_metadata_falls_back_alternative_keys() {
        let input = json!({
            "fulltitle": "Fallback Title",
            "channel": "Channel Name"
        });
        let metadata = parse_online_source_metadata(&input);
        assert_eq!(metadata.title, "Fallback Title");
        assert_eq!(metadata.artist, "Channel Name");
    }

    /// Ensures missing description yields an empty string.
    #[test]
    fn parse_online_source_metadata_missing_description_is_empty() {
        let input = json!({
            "title": "No Description",
            "uploader": "Uploader"
        });
        let metadata = parse_online_source_metadata(&input);
        assert_eq!(metadata.description, "");
    }

    /// Ensures ffprobe JSON with format tags is parsed correctly.
    #[test]
    fn parse_local_source_metadata_from_ffprobe_json_extracts_fields() {
        let input = json!({
            "format": {
                "filename": "/path/to/file.mp3",
                "tags": {
                    "title": "Song Title",
                    "artist": "Song Artist"
                }
            }
        });
        let metadata = parse_local_source_metadata_from_ffprobe_json(&input);
        assert_eq!(metadata.title, "Song Title");
        assert_eq!(metadata.artist, "Song Artist");
    }

    /// Ensures ffprobe metadata falls back to filename when title is absent.
    #[test]
    fn parse_local_source_metadata_falls_back_to_filename() {
        let input = json!({
            "format": {
                "filename": "/path/to/audio.mp3",
                "tags": {}
            }
        });
        let metadata = parse_local_source_metadata_from_ffprobe_json(&input);
        assert_eq!(metadata.title, "/path/to/audio.mp3");
        assert_eq!(metadata.artist, "Unknown");
    }

    /// Ensures `resolve_online_source_metadata_for_add` maps fields correctly.
    #[test]
    fn resolve_online_source_metadata_for_add_maps_fields() {
        let input = json!({
            "title": "Resolved Title",
            "uploader": "Resolved Uploader",
            "description": "Resolved description."
        });
        let resolved = resolve_online_source_metadata_for_add(&input, None);
        assert_eq!(resolved.title, "Resolved Title");
        assert_eq!(resolved.artist, "Resolved Uploader");
        assert_eq!(resolved.description, "Resolved description.");
        assert!(resolved.warning.is_none());
    }
}
