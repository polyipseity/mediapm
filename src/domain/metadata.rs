//! Lightweight metadata probing.
//!
//! This module currently captures baseline filesystem/container information.
//! It is intentionally simple and dependency-light so richer probe/tag adapters
//! can be integrated later without changing core data flow.
//!
//! In other words, this module defines the *shape contract* between probing and
//! sidecar persistence, not a final media-analysis implementation.

use std::path::Path;

use anyhow::Result;
use serde_json::{Value, json};
use tokio::fs;

use crate::domain::model::Blake3Hash;

/// Probe a media file and return `(container, raw_probe, normalized_metadata)`.
///
/// - `container`: inferred from extension.
/// - `raw_probe`: machine-oriented fields captured from the current file state.
/// - `normalized_metadata`: application-oriented shape used by sidecars.
///
/// The split between raw and normalized payloads exists so future adapters can
/// preserve tool-native output (`raw_probe`) while still presenting a stable,
/// planner-friendly schema (`normalized_metadata`).
pub async fn probe_media_file(
    path: &Path,
    variant_hash: Blake3Hash,
) -> Result<(Option<String>, Value, Value)> {
    let metadata = fs::metadata(path).await?;
    let file_size = metadata.len();

    let container =
        path.extension().and_then(|value| value.to_str()).map(|value| value.to_ascii_lowercase());

    let probe = json!({
        "byte_size": file_size,
        "container": container,
        "file_name": path.file_name().and_then(|value| value.to_str()),
        "modified_unix_seconds": metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|value| value.as_secs()),
        "variant_hash": variant_hash,
    });

    let normalized_metadata = json!({
        "tags": {},
        "technical": {
            "container": container,
            "byte_size": file_size,
        }
    });

    Ok((container, probe, normalized_metadata))
}
