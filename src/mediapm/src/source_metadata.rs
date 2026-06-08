//! Online and local source metadata resolution helpers.

use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use mediapm_conductor::MachineNickelDocument;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::paths::MediaPmPaths;

/// Metadata tuple fetched by downloader-aware online probes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct OnlineSourceMetadata {
    /// Best-effort media title.
    pub(crate) title: Option<String>,
    /// Best-effort artist/uploader label.
    pub(crate) artist: Option<String>,
    /// Best-effort textual description.
    pub(crate) description: Option<String>,
}

/// Remote metadata resolved for the online add flow.
///
/// This structure keeps the add-path-specific title, description, artist, and
/// warning text together so service code can stay small while tests can assert
/// the resolution policy directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedOnlineSourceMetadata {
    /// Resolved title used when adding one online media source.
    pub(crate) title: Option<String>,
    /// Resolved description used when adding one online media source.
    pub(crate) description: Option<String>,
    /// Resolved artist/uploader label used when adding one online media source.
    pub(crate) artist: Option<String>,
    /// Optional warning emitted when yt-dlp metadata cannot be fetched.
    pub(crate) warning: Option<String>,
}

/// Metadata tuple fetched by local-file probes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct LocalSourceMetadata {
    /// Best-effort media title.
    pub(crate) title: Option<String>,
    /// Best-effort artist/uploader label.
    pub(crate) artist: Option<String>,
    /// Best-effort textual description.
    pub(crate) description: Option<String>,
}

/// Resolves online metadata using a managed `yt-dlp` executable.
pub(crate) fn fetch_online_source_metadata(
    uri: &Url,
    yt_dlp_command: &Path,
) -> OnlineSourceMetadata {
    try_fetch_online_source_metadata_with_yt_dlp(uri, yt_dlp_command).unwrap_or_default()
}

/// Resolves add-flow metadata for one remote source.
///
/// When yt-dlp metadata is available, the fetched values are passed through
/// as-is. The warning is carried through so the caller can report why
/// metadata could not be fetched.
pub(crate) fn resolve_online_source_metadata_for_add(
    yt_dlp_metadata: Option<OnlineSourceMetadata>,
    warning: Option<String>,
) -> ResolvedOnlineSourceMetadata {
    let metadata = yt_dlp_metadata.unwrap_or_default();
    ResolvedOnlineSourceMetadata {
        title: metadata.title,
        description: metadata.description,
        artist: metadata.artist,
        warning,
    }
}

/// Resolves local metadata using managed ffprobe when available.
pub(crate) fn fetch_local_source_metadata(
    path: &Path,
    ffprobe_command: Option<&Path>,
    cache: Option<&crate::metadata_cache::MetadataCache>,
) -> LocalSourceMetadata {
    let Some(ffprobe) = ffprobe_command else {
        return LocalSourceMetadata::default();
    };
    try_fetch_local_source_metadata_with_ffprobe(path, ffprobe, cache).unwrap_or_default()
}

/// Fetches online metadata by invoking `yt-dlp` from one explicit executable path.
pub(crate) fn try_fetch_online_source_metadata_with_yt_dlp(
    uri: &Url,
    yt_dlp_command: &Path,
) -> Option<OnlineSourceMetadata> {
    let output = ProcessCommand::new(yt_dlp_command)
        .arg("--dump-single-json")
        .arg("--skip-download")
        .arg("--no-warnings")
        .arg(uri.as_str())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let metadata = parse_online_source_metadata(&value);

    if metadata.title.is_none() && metadata.artist.is_none() && metadata.description.is_none() {
        None
    } else {
        Some(metadata)
    }
}

/// Fetches local metadata by invoking managed `ffprobe`, with optional
/// persistent cache.
pub(crate) fn try_fetch_local_source_metadata_with_ffprobe(
    path: &Path,
    ffprobe_command: &Path,
    cache: Option<&crate::metadata_cache::MetadataCache>,
) -> Option<LocalSourceMetadata> {
    // Compute cache key from canonicalized path for persistent metadata cache.
    let cache_key = {
        let canonical = std::fs::canonicalize(path).ok()?;
        blake3::hash(canonical.to_string_lossy().as_bytes()).to_hex().to_string()
    };

    // Check persistent cache first.
    if let Some(cache) = cache
        && let Some(cached) = cache.get(&cache_key)
        && let Ok(metadata) = serde_json::from_value::<LocalSourceMetadata>(cached)
    {
        return Some(metadata);
    }

    let output = ProcessCommand::new(ffprobe_command)
        .arg("-v")
        .arg("error")
        .arg("-print_format")
        .arg("json")
        .arg("-show_format")
        .arg(path)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let metadata = parse_local_source_metadata_from_ffprobe_json(&value);

    // Store in cache on success, then return.
    if metadata.title.is_some() || metadata.artist.is_some() || metadata.description.is_some() {
        if let Some(cache) = cache
            && let Ok(value) = serde_json::to_value(&metadata)
        {
            cache.set(cache_key, value);
        }
        Some(metadata)
    } else {
        None
    }
}

/// Parses online metadata fields from one downloader JSON payload.
pub(crate) fn parse_online_source_metadata(value: &serde_json::Value) -> OnlineSourceMetadata {
    let title = first_non_empty_json_string(value, &["fulltitle", "title", "track"]);
    let artist = first_non_empty_json_string(
        value,
        &["uploader", "channel", "artist", "creator", "uploader_id"],
    );
    let description = first_non_empty_json_string(value, &["description", "summary"]);

    OnlineSourceMetadata { title, artist, description }
}

/// Parses local metadata fields from one ffprobe JSON payload.
pub(crate) fn parse_local_source_metadata_from_ffprobe_json(
    value: &serde_json::Value,
) -> LocalSourceMetadata {
    let tags = value
        .get("format")
        .and_then(|format| format.get("tags"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let title = first_non_empty_json_string(&tags, &["title", "track"]);
    let artist = first_non_empty_json_string(&tags, &["artist", "album_artist"]);
    let description = first_non_empty_json_string(&tags, &["description", "comment", "synopsis"]);

    LocalSourceMetadata { title, artist, description }
}

/// Returns first non-empty string value from one JSON object key list.
pub(crate) fn first_non_empty_json_string(
    value: &serde_json::Value,
    keys: &[&str],
) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .or_else(|| {
                value.as_object().and_then(|object| {
                    object.iter().find_map(|(candidate, candidate_value)| {
                        if candidate.eq_ignore_ascii_case(key) {
                            Some(candidate_value)
                        } else {
                            None
                        }
                    })
                })
            })
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToString::to_string)
    })
}

/// Resolves conductor CAS root from machine runtime storage with default fallback.
pub(crate) fn resolve_conductor_cas_root(
    paths: &MediaPmPaths,
    machine: &MachineNickelDocument,
) -> PathBuf {
    if let Some(raw) = machine.runtime.cas_store_dir.as_deref() {
        let candidate = PathBuf::from(raw);
        if candidate.is_absolute() { candidate } else { paths.root_dir.join(candidate) }
    } else {
        paths.runtime_root.join("store")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Ensures online metadata parsing extracts title/artist/description when
    /// downloader JSON includes those fields.
    #[test]
    fn parse_online_metadata_reads_title_artist_and_description() {
        let payload = json!({
            "fulltitle": "Demo Song",
            "uploader": "Demo Artist",
            "description": "A short description"
        });

        let metadata = parse_online_source_metadata(&payload);
        assert_eq!(
            metadata,
            OnlineSourceMetadata {
                title: Some("Demo Song".to_string()),
                artist: Some("Demo Artist".to_string()),
                description: Some("A short description".to_string()),
            }
        );
    }

    /// Ensures remote add-flow metadata passes `None` through when yt-dlp is
    /// not configured and emits the warning verbatim.
    #[test]
    fn resolve_online_metadata_for_add_warns_when_yt_dlp_is_missing() {
        let warning = "yt-dlp managed tool is not configured; cannot fetch title, description, or artist metadata for remote source 'https://example.com/demo-video'".to_string();
        let resolved = resolve_online_source_metadata_for_add(None, Some(warning.clone()));

        assert!(resolved.title.is_none());
        assert!(resolved.artist.is_none());
        assert!(resolved.description.is_none());
        assert_eq!(resolved.warning.as_deref(), Some(warning.as_str()));
    }

    /// Ensures remote add-flow metadata passes through yt-dlp-fetched values
    /// verbatim when the tool is configured.
    #[test]
    fn resolve_online_metadata_for_add_prefers_yt_dlp_values_when_configured() {
        let fetched = OnlineSourceMetadata {
            title: Some("Fetched Title".to_string()),
            artist: Some("Fetched Artist".to_string()),
            description: Some("Fetched Description".to_string()),
        };

        let resolved = resolve_online_source_metadata_for_add(Some(fetched), None);

        assert_eq!(resolved.title.as_deref(), Some("Fetched Title"));
        assert_eq!(resolved.artist.as_deref(), Some("Fetched Artist"));
        assert_eq!(resolved.description.as_deref(), Some("Fetched Description"));
        assert!(resolved.warning.is_none());
    }

    /// Ensures local metadata parsing extracts title/description from ffprobe
    /// `format.tags` payloads with case-insensitive key matching.
    #[test]
    fn parse_local_metadata_reads_ffprobe_tags_case_insensitively() {
        let payload = json!({
            "format": {
                "tags": {
                    "TITLE": "Local Demo",
                    "Comment": "Local description"
                }
            }
        });

        let metadata = parse_local_source_metadata_from_ffprobe_json(&payload);
        assert_eq!(
            metadata,
            LocalSourceMetadata {
                title: Some("Local Demo".to_string()),
                artist: None,
                description: Some("Local description".to_string()),
            }
        );
    }
}
