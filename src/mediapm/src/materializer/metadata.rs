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

use crate::config::MediaPmDocument;
use crate::config::hierarchy_types::{FlattenedHierarchyEntry, HierarchyFolderRenameRule};
use crate::config::output_types::{GenericOutputVariantConfig, OutputVariantValue};
use crate::config::source_types::{
    MediaMetadataRegexTransform, MediaMetadataValue, MediaMetadataValueCandidate, MediaSourceSpec,
};
use crate::error::MediaPmError;
use crate::source_metadata::try_fetch_local_source_metadata_with_ffprobe;
use crate::tools::workflows::{FfmpegSlotLimits, resolve_media_variant_output_binding_with_limits};

use super::zip::extract_zip_member_bytes;

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

/// Interpolates media placeholders in folder rename-rule replacements.
pub(super) async fn resolve_interpolated_folder_rename_rules(
    rules: &[HierarchyFolderRenameRule],
    media_id: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
) -> Result<Vec<HierarchyFolderRenameRule>, MediaPmError> {
    let mut resolved = Vec::with_capacity(rules.len());
    for rule in rules {
        resolved.push(HierarchyFolderRenameRule {
            pattern: rule.pattern.clone(),
            replacement: interpolate_path_template(
                &rule.replacement,
                media_id,
                source,
                lookup_context,
            )
            .await?,
        });
    }
    Ok(resolved)
}

/// Resolves hierarchy path templates for one flattened entry.
pub(super) async fn resolve_materialized_path_components(
    entry: &FlattenedHierarchyEntry,
    document: &MediaPmDocument,
    lookup_context: &MaterializationLookupContext,
) -> Result<Vec<String>, MediaPmError> {
    if entry.path_components.iter().all(|component| !component.contains("${")) {
        return Ok(entry.path_components.clone());
    }

    let media_id = entry.entry.media_id.trim();
    if media_id.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path '{}' contains template placeholders but entry has no media_id",
            entry.path_str()
        )));
    }

    let source = document.media.get(media_id).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "hierarchy references unknown media id '{media_id}' while resolving path templates"
        ))
    })?;

    let mut resolved_components = Vec::with_capacity(entry.path_components.len());
    for component in &entry.path_components {
        let resolved = if component.contains("${") {
            interpolate_path_template(component, media_id, source, lookup_context).await?
        } else {
            component.clone()
        };
        resolved_components.push(resolved);
    }

    Ok(resolved_components)
}

/// Resolves hierarchy path templates across flattened hierarchy entries.
pub(super) async fn resolve_flattened_entry_paths(
    flattened: &mut [FlattenedHierarchyEntry],
    document: &MediaPmDocument,
    lookup_context: &MaterializationLookupContext,
) -> Result<(), MediaPmError> {
    for entry in flattened {
        if entry.path_components.iter().any(|component| component.contains("${")) {
            entry.path_components =
                resolve_materialized_path_components(entry, document, lookup_context).await?;
        }
    }
    Ok(())
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

    extract_metadata_key_from_variant_hash(
        media_id,
        variant,
        source,
        lookup_context,
        hash,
        metadata_key,
    )
    .await
}

async fn extract_metadata_key_from_variant_hash(
    media_id: &str,
    variant: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
    hash: Hash,
    metadata_key: &str,
) -> Result<Option<String>, MediaPmError> {
    let bytes = lookup_context.cas.get(hash).await.map_err(|error| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' variant '{variant}' CAS read failed: {error}"
        ))
    })?;

    let metadata_bytes = metadata_probe_bytes(source, variant, lookup_context, bytes.as_ref());

    if let Some(value) =
        extract_metadata_key_from_probe_bytes(source, variant, metadata_key, &metadata_bytes)?
    {
        return Ok(Some(value));
    }

    if let Ok(member_bytes) = extract_zip_member_bytes(bytes.as_ref(), "info.json")
        && let Some(value) =
            extract_metadata_key_from_probe_bytes(source, variant, metadata_key, &member_bytes)?
    {
        return Ok(Some(value));
    }

    if let Ok(member_bytes) = extract_zip_member_bytes(bytes.as_ref(), ".info.json")
        && let Some(value) =
            extract_metadata_key_from_probe_bytes(source, variant, metadata_key, &member_bytes)?
    {
        return Ok(Some(value));
    }

    Ok(None)
}

fn extract_metadata_key_from_probe_bytes(
    source: &MediaSourceSpec,
    variant: &str,
    metadata_key: &str,
    probe_bytes: &[u8],
) -> Result<Option<String>, MediaPmError> {
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(probe_bytes)
        && let Some(value) = extract_metadata_key_from_json(&json, metadata_key)
    {
        return Ok(Some(value));
    }

    extract_metadata_key_from_media_bytes_via_ffprobe(source, variant, probe_bytes, metadata_key)
}

fn metadata_probe_bytes(
    source: &MediaSourceSpec,
    variant: &str,
    lookup_context: &MaterializationLookupContext,
    raw_bytes: &[u8],
) -> Vec<u8> {
    if let Ok(Some(binding)) = resolve_media_variant_output_binding_with_limits(
        source,
        variant,
        lookup_context.ffmpeg_slot_limits.max_input_slots,
        lookup_context.ffmpeg_slot_limits.max_output_slots,
    ) && let Some(zip_member) = binding.zip_member.as_deref()
        && let Ok(member_bytes) = extract_zip_member_bytes(raw_bytes, zip_member)
    {
        return member_bytes;
    }

    raw_bytes.to_vec()
}

fn output_variant_extension(source: &MediaSourceSpec, variant: &str) -> String {
    for step in &source.steps {
        if let Some(output) = step.output_variants.get(variant)
            && let OutputVariantValue::Generic(GenericOutputVariantConfig { extension, .. }) =
                output
            && !extension.is_empty()
        {
            return extension.clone();
        }
    }
    "mkv".to_string()
}

fn extract_metadata_key_from_media_bytes_via_ffprobe(
    source: &MediaSourceSpec,
    variant: &str,
    bytes: &[u8],
    metadata_key: &str,
) -> Result<Option<String>, MediaPmError> {
    let extension = output_variant_extension(source, variant);
    let temp_dir = mediapm_utils::temp::artifact_dir().map_err(|error| {
        MediaPmError::Workflow(format!(
            "failed to create temp directory for ffprobe metadata probe of variant '{variant}': {error}"
        ))
    })?;
    let probe_path = temp_dir.path().join(format!("probe.{extension}"));
    std::fs::write(&probe_path, bytes).map_err(|error| {
        MediaPmError::Workflow(format!(
            "failed to write temp media bytes for ffprobe metadata probe of variant '{variant}': {error}"
        ))
    })?;

    let json = try_fetch_local_source_metadata_with_ffprobe(&probe_path, "ffprobe")?;
    Ok(extract_metadata_key_from_json(&json, metadata_key))
}

// ---------------------------------------------------------------------------
// JSON metadata extraction helpers
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn extract_metadata_key_from_json(json: &serde_json::Value, key: &str) -> Option<String> {
    if let Some(value) = extract_metadata_key_from_json_direct(json, key) {
        return Some(value);
    }

    match key {
        "artist" => extract_metadata_key_from_json_direct(json, "uploader")
            .or_else(|| extract_metadata_key_from_json_direct(json, "channel"))
            .or_else(|| extract_metadata_key_from_json_direct(json, "track_artist")),
        "title" => extract_metadata_key_from_json_direct(json, "track"),
        _ => None,
    }
}

fn extract_metadata_key_from_json_direct(json: &serde_json::Value, key: &str) -> Option<String> {
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
