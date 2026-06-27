//! Shared utility helpers for media-tagger metadata normalization and output.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use anyhow::Context;
use ffmetadata::FFMetadata;
use musicbrainz_rs::entity::release::Release;
use serde_json::Value;

use super::{
    FFMPEG_EXECUTABLE, MAX_FLATTENED_METADATA_ENTRIES, MAX_FLATTENED_VALUE_LEN,
    MEDIA_TAGGER_FFMPEG_BIN_ENV,
};

/// Track-position metadata resolved from release media structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TrackPosition {
    /// Human-facing track number from the release tracklist.
    pub(super) track_number: String,
    /// One-based disc index in release media ordering.
    pub(super) disc_number: u32,
    /// Optional total track count for the current disc.
    pub(super) total_tracks: Option<u32>,
    /// Optional total disc count for the release.
    pub(super) total_discs: Option<u32>,
}

/// Resolves track/disc position metadata by matching recording MBID.
pub(super) fn resolve_track_position(
    recording_mbid: &str,
    release: &Release,
) -> Option<TrackPosition> {
    let medias = release.media.as_ref()?;

    for media in medias {
        let tracks = media.tracks.as_ref()?;
        for track in tracks {
            let Some(recording) = track.recording.as_ref() else {
                continue;
            };
            if recording.id != recording_mbid {
                continue;
            }

            return Some(TrackPosition {
                track_number: track.number.clone(),
                disc_number: media.position.unwrap_or(1),
                total_tracks: Some(media.track_count),
                total_discs: media.disc_count,
            });
        }
    }

    None
}

/// Converts artist-credit list into stable display string.
pub(super) fn artist_credit_text(
    artist_credit: Option<&[musicbrainz_rs::entity::artist_credit::ArtistCredit]>,
) -> Option<String> {
    let artist_credit = artist_credit?;
    if artist_credit.is_empty() {
        return None;
    }

    let mut combined = String::new();
    for credit in artist_credit {
        combined.push_str(&credit.name);
        if let Some(join_phrase) = credit.joinphrase.as_deref() {
            combined.push_str(join_phrase);
        }
    }

    if combined.trim().is_empty() { None } else { Some(combined) }
}

/// Joins string values while preserving first-seen order and uniqueness.
pub(super) fn join_unique<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut seen = BTreeSet::new();
    let mut ordered = Vec::new();

    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            ordered.push(trimmed.to_string());
        }
    }

    ordered.join("; ")
}

/// Flattens one arbitrary JSON tree into FFmetadata-safe key/value pairs.
pub(super) fn flatten_entity_json(prefix: &str, value: &Value, out: &mut BTreeMap<String, String>) {
    fn walk(prefix: &str, value: &Value, out: &mut BTreeMap<String, String>) {
        if out.len() >= MAX_FLATTENED_METADATA_ENTRIES {
            return;
        }

        match value {
            Value::Object(map) => {
                let mut entries = map.iter().collect::<Vec<_>>();
                entries.sort_by_key(|(key, _)| *key);
                for (key, nested) in entries {
                    let nested_prefix = format!("{prefix}_{key}");
                    walk(&nested_prefix, nested, out);
                }
            }
            Value::Array(items) => {
                if items.iter().all(is_scalar_json_value) {
                    let joined = items
                        .iter()
                        .filter_map(json_scalar_to_string)
                        .collect::<Vec<_>>()
                        .join("; ");
                    let key = sanitize_metadata_key(prefix);
                    if !joined.is_empty() {
                        out.insert(key, truncate_metadata_value(&joined));
                    }
                } else {
                    for (index, nested) in items.iter().enumerate() {
                        walk(&format!("{prefix}_{index}"), nested, out);
                    }
                }
            }
            _ => {
                if let Some(rendered) = json_scalar_to_string(value)
                    && !rendered.is_empty()
                {
                    out.insert(sanitize_metadata_key(prefix), truncate_metadata_value(&rendered));
                }
            }
        }
    }

    walk(prefix, value, out);
}

/// Returns true when one JSON value can be represented as scalar metadata text.
pub(super) fn is_scalar_json_value(value: &Value) -> bool {
    matches!(value, Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_))
}

/// Converts one scalar JSON value into metadata text.
pub(super) fn json_scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Number(number) => Some(number.to_string()),
        Value::String(text) => normalize_optional_text(Some(text)),
        _ => None,
    }
}

/// Sanitizes one metadata key for broad ffmpeg tag compatibility.
pub(super) fn sanitize_metadata_key(input: &str) -> String {
    let mut key =
        input
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() { character.to_ascii_lowercase() } else { '_' }
            })
            .collect::<String>();

    while key.contains("__") {
        key = key.replace("__", "_");
    }
    key = key.trim_matches('_').to_string();

    if key.is_empty() {
        return "metadata".to_string();
    }
    if key.starts_with(|character: char| character.is_ascii_digit()) {
        return format!("meta_{key}");
    }

    key
}

/// Truncates one metadata value to bounded maximum length.
pub(super) fn truncate_metadata_value(value: &str) -> String {
    if value.len() <= MAX_FLATTENED_VALUE_LEN {
        value.to_string()
    } else {
        value.chars().take(MAX_FLATTENED_VALUE_LEN).collect::<String>()
    }
}

/// Persists one FFmetadata document to requested output path.
pub(super) fn write_ffmetadata_document(
    output_path: &Path,
    metadata_map: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let document = FFMetadata {
        global: metadata_map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>(),
        ..FFMetadata::default()
    };

    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("creating ffmetadata output directory '{}'", parent.display())
        })?;
    }

    fs::write(output_path, document.to_string())
        .with_context(|| format!("writing ffmetadata file '{}'", output_path.display()))?;

    Ok(())
}

/// Returns normalized, non-empty optional string value.
pub(super) fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|value| !value.is_empty()).map(ToString::to_string)
}

/// Resolves ffmpeg executable path for internal tagger operations.
///
/// Managed sync injects a host-specific relative executable path through
/// `MEDIA_TAGGER_FFMPEG_BIN_ENV` so internal launchers can run without relying
/// on ambient PATH state.
pub(super) fn resolve_ffmpeg_executable() -> String {
    resolve_ffmpeg_executable_from_configured_path(
        std::env::var(MEDIA_TAGGER_FFMPEG_BIN_ENV).ok().as_deref(),
    )
}

/// Resolves one ffmpeg executable path from an optional configured override.
///
/// Managed runtime paths are expected to already target the conductor
/// `payload/` layout. The configured value is preserved as-is for diagnostics;
/// no alternate layout rewrite is attempted.
#[must_use]
pub(super) fn resolve_ffmpeg_executable_from_configured_path(
    configured_path: Option<&str>,
) -> String {
    let configured = normalize_optional_text(configured_path)
        .map(|value| value.trim_matches('"').trim_matches('\'').to_string());

    let Some(configured) = configured else {
        return FFMPEG_EXECUTABLE.to_string();
    };

    if Path::new(&configured).is_file() {
        return configured;
    }

    configured
}
