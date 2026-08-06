//! Metadata resolution: placeholders, variant metadata, regex transforms.
//!
//! Provides template interpolation for `${media.id}` and
//! `${media.metadata.<key>}` placeholders, variant-file metadata extraction
//! from JSON/ffprobe output, and regex-based metadata string transforms.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{ConductorState, NickelDocument};
use regex::Regex;

use crate::config::source_types::{
    MediaMetadataRegexTransform, MediaMetadataValue, MediaMetadataValueCandidate, MediaSourceSpec,
};
use crate::error::MediaPmError;
use crate::tools::workflows::FfmpegSlotLimits;

// ---------------------------------------------------------------------------
// Lookup context
// ---------------------------------------------------------------------------

/// Per-workflow step output hash table (`step_id -> output_name -> CAS hash`).
pub(super) type StepOutputHashes = BTreeMap<String, BTreeMap<String, Hash>>;

/// Shared lookup context threaded through materialization workers.
#[derive(Clone)]
pub(super) struct MaterializationLookupContext {
    /// CAS store reference for resolving variant byte content.
    pub(super) cas: FileSystemCas,
    /// Conductor runtime state after workflow execution.
    pub(super) conductor_state: Option<ConductorState>,
    /// Generated conductor document carrying managed workflows and tools.
    pub(super) generated_doc: NickelDocument,
    /// Effective ffmpeg slot limits for variant binding resolution.
    pub(super) ffmpeg_slot_limits: FfmpegSlotLimits,
    /// Cache for per-workflow step output hash resolution during one sync pass.
    pub(super) step_output_hashes_cache: Arc<Mutex<BTreeMap<String, Option<StepOutputHashes>>>>,
}

impl MaterializationLookupContext {
    /// Creates a lookup context for one hierarchy sync pass.
    #[must_use]
    pub(super) fn new(
        cas: FileSystemCas,
        conductor_state: Option<ConductorState>,
        generated_doc: NickelDocument,
        ffmpeg_slot_limits: FfmpegSlotLimits,
    ) -> Self {
        Self {
            cas,
            conductor_state,
            generated_doc,
            ffmpeg_slot_limits,
            step_output_hashes_cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

// ---------------------------------------------------------------------------
// Metadata value resolution
// ---------------------------------------------------------------------------

/// Resolves one [`MediaMetadataValue`] to a concrete string.
#[allow(dead_code)]
pub(super) async fn resolve_metadata_value(
    value: &MediaMetadataValue,
    media_id: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
) -> Result<String, MediaPmError> {
    match value {
        MediaMetadataValue::Literal(text) => Ok(text.clone()),
        MediaMetadataValue::Variant(binding) => {
            let resolved = resolve_variant_metadata_key(
                &binding.variant,
                &binding.metadata_key,
                media_id,
                source,
                lookup_context,
            )
            .await?
            .ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' variant '{}' metadata key '{}' did not resolve",
                    binding.variant, binding.metadata_key
                ))
            })?;

            let value = if let Some(transform) = &binding.transform {
                apply_metadata_regex_transform(&resolved, transform)
            } else {
                resolved
            };

            Ok(value)
        }
        MediaMetadataValue::Fallback(candidates) => {
            for candidate in candidates {
                match candidate {
                    MediaMetadataValueCandidate::Literal(text) => {
                        if !text.is_empty() {
                            return Ok(text.clone());
                        }
                    }
                    MediaMetadataValueCandidate::Variant(binding) => {
                        if let Some(value) = resolve_variant_metadata_key(
                            &binding.variant,
                            &binding.metadata_key,
                            media_id,
                            source,
                            lookup_context,
                        )
                        .await?
                        {
                            let value = if let Some(transform) = &binding.transform {
                                apply_metadata_regex_transform(&value, transform)
                            } else {
                                value
                            };
                            if !value.is_empty() {
                                return Ok(value);
                            }
                        }
                    }
                }
            }

            Err(MediaPmError::Workflow(format!(
                "media '{media_id}' metadata had no fallback that resolved to a non-empty value"
            )))
        }
    }
}

/// Interpolates `${media.id}` and `${media.metadata.<key>}` placeholders in a
/// hierarchy path template string.
#[allow(dead_code)]
pub(super) async fn interpolate_path_template(
    template: &str,
    media_id: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
) -> Result<String, MediaPmError> {
    let placeholder_keys = collect_metadata_placeholder_keys(template);
    let mut result = template.to_string();

    result = result.replace("${media.id}", media_id);

    for key in &placeholder_keys {
        let placeholder = format!("${{media.metadata.{key}}}");
        let resolved = if let Some(metadata_value) = source.metadata.get(key) {
            resolve_metadata_value(metadata_value, media_id, source, lookup_context).await?
        } else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' template placeholder \
                 '${{media.metadata.{key}}}' has no matching metadata entry"
            )));
        };
        result = result.replace(&placeholder, &resolved);
    }

    Ok(result)
}

/// Resolves one metadata key from a variant's produced content bytes.
pub(super) async fn resolve_variant_metadata_key(
    variant: &str,
    metadata_key: &str,
    media_id: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
) -> Result<Option<String>, MediaPmError> {
    use super::resolve::resolve_variant_hash;

    let hash = resolve_variant_hash(media_id, variant, source, lookup_context).await?;

    let Some(hash) = hash else {
        return Ok(None);
    };

    let bytes = lookup_context.cas.get(hash).await.map_err(|e| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' variant '{variant}' CAS read failed: {e}"
        ))
    })?;

    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&bytes)
        && let Some(value) = extract_metadata_key_from_json(&json, metadata_key)
    {
        return Ok(Some(value));
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// JSON metadata extraction helpers
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn extract_metadata_key_from_json(json: &serde_json::Value, key: &str) -> Option<String> {
    if let Some(format) = json.get("format").and_then(serde_json::Value::as_object) {
        if let Some(value) = lookup_json_string_key(format, key) {
            return Some(value);
        }
        if let Some(tags) = format.get("tags").and_then(serde_json::Value::as_object)
            && let Some(value) = lookup_json_string_key(tags, key)
        {
            return Some(value);
        }
    }

    if let Some(streams) = json.get("streams").and_then(serde_json::Value::as_array) {
        for stream in streams {
            if let Some(stream_obj) = stream.as_object() {
                if let Some(value) = lookup_json_string_key(stream_obj, key) {
                    return Some(value);
                }
                if let Some(tags) = stream_obj.get("tags").and_then(serde_json::Value::as_object)
                    && let Some(value) = lookup_json_string_key(tags, key)
                {
                    return Some(value);
                }
            }
        }
    }

    if let Some(obj) = json.as_object()
        && let Some(value) = lookup_json_string_key(obj, key)
    {
        return Some(value);
    }

    None
}

fn lookup_json_string_key(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    object
        .iter()
        .find_map(|(candidate_key, candidate_value)| {
            candidate_key.eq_ignore_ascii_case(key).then_some(candidate_value)
        })
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

#[allow(dead_code)]
fn apply_metadata_regex_transform(value: &str, transform: &MediaMetadataRegexTransform) -> String {
    match Regex::new(&transform.pattern) {
        Ok(re) => re.replace_all(value, transform.replacement.as_str()).to_string(),
        Err(_) => value.to_string(),
    }
}

#[allow(dead_code)]
fn collect_metadata_placeholder_keys(template: &str) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    let mut cursor = 0usize;

    while let Some(relative_start) = template[cursor..].find("${") {
        let placeholder_start = cursor + relative_start;
        let after_marker = &template[placeholder_start + 2..];
        let Some(relative_end) = after_marker.find('}') else {
            break;
        };

        let expression = after_marker[..relative_end].trim();

        if expression == "media.id" {
            cursor = placeholder_start + 2 + relative_end + 1;
            continue;
        }

        if let Some(metadata_key) = expression.strip_prefix("media.metadata.") {
            let key = metadata_key.trim();
            if !key.is_empty() {
                keys.insert(key.to_string());
            }
        }

        cursor = placeholder_start + 2 + relative_end + 1;
    }

    keys
}
