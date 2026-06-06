//! Hierarchy path template resolution, media metadata extraction, and ffprobe helpers.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use regex::Regex;

use mediapm_conductor::{MachineNickelDocument, ToolKindSpec};

use crate::config::MediaPmState;
use crate::config::{
    HierarchyEntry, HierarchyFolderRenameRule, MediaMetadataRegexTransform, MediaMetadataValue,
    MediaMetadataValueCandidate, MediaSourceSpec, hierarchy_metadata_placeholder_keys,
};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;

use super::MaterializationLookupContext;
use super::resolve::resolve_variant_source_bytes;
use super::sanitize_hierarchy_path;

/// Resolves `${media.id}` and `${media.metadata.*}` placeholders for one
/// hierarchy key template.
pub(super) async fn resolve_hierarchy_relative_path(
    relative_path_template: &str,
    entry: &HierarchyEntry,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext,
) -> Result<String, MediaPmError> {
    let context_label = format!("hierarchy path '{relative_path_template}'");
    resolve_media_placeholder_template(
        relative_path_template,
        entry,
        source,
        lookup,
        context_label.as_str(),
    )
    .await
}

/// Resolves supported media placeholder forms in one arbitrary template.
///
/// Supported placeholders:
/// - `${media.id}`
/// - `${media.metadata.<key>}`
async fn resolve_media_placeholder_template(
    template: &str,
    entry: &HierarchyEntry,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext,
    context_label: &str,
) -> Result<String, MediaPmError> {
    let placeholder_keys = hierarchy_metadata_placeholder_keys(template).map_err(|reason| {
        MediaPmError::Workflow(format!(
            "{context_label} has invalid metadata placeholder syntax: {reason}"
        ))
    })?;

    let has_media_id_placeholder = template.contains("${media.id}");

    if placeholder_keys.is_empty() && !has_media_id_placeholder {
        return Ok(template.to_string());
    }

    let mut resolved_values = BTreeMap::new();
    if !placeholder_keys.is_empty() {
        let metadata = source.metadata.as_ref().ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "{context_label} references metadata placeholders but media '{}' does not define metadata",
                entry.media_id
            ))
        })?;

        for metadata_key in placeholder_keys {
            if resolved_values.contains_key(&metadata_key) {
                continue;
            }

            let metadata_value = metadata.get(metadata_key.as_str()).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "{context_label} references undefined metadata key '{}' for media '{}'",
                    metadata_key, entry.media_id
                ))
            })?;

            let resolved = resolve_media_metadata_string_value(
                &entry.media_id,
                metadata_key.as_str(),
                metadata_value,
                source,
                lookup,
            )
            .await?;
            resolved_values.insert(metadata_key, resolved);
        }
    }

    let mut resolved_path = template.to_string();
    for (metadata_key, metadata_value) in resolved_values {
        let placeholder = format!("${{media.metadata.{metadata_key}}}");
        resolved_path = resolved_path.replace(&placeholder, metadata_value.as_str());
    }

    if has_media_id_placeholder {
        resolved_path = resolved_path.replace("${media.id}", entry.media_id.as_str());
    }

    Ok(resolved_path)
}

/// Resolves placeholder templates used by folder rename-rule replacements and
/// applies the effective reserved-character replacement map to each resolved
/// replacement.
pub(super) async fn resolve_hierarchy_folder_rename_rule_replacements(
    rules: &[HierarchyFolderRenameRule],
    hierarchy_path: &str,
    entry: &HierarchyEntry,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext,
    replacements: &BTreeMap<char, char>,
) -> Result<Vec<HierarchyFolderRenameRule>, MediaPmError> {
    let mut resolved_rules = Vec::with_capacity(rules.len());

    for (rule_index, rule) in rules.iter().enumerate() {
        let context_label =
            format!("hierarchy path '{hierarchy_path}' rename_files[{rule_index}] replacement");
        let resolved_replacement = resolve_media_placeholder_template(
            rule.replacement.as_str(),
            entry,
            source,
            lookup,
            context_label.as_str(),
        )
        .await?;

        let sanitized_replacement = sanitize_hierarchy_path(&resolved_replacement, replacements);
        resolved_rules.push(HierarchyFolderRenameRule {
            pattern: rule.pattern.clone(),
            replacement: sanitized_replacement,
        });
    }

    Ok(resolved_rules)
}

/// Resolves one media metadata value into a concrete string.
async fn resolve_media_metadata_string_value(
    media_id: &str,
    metadata_key: &str,
    metadata_value: &MediaMetadataValue,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext,
) -> Result<String, MediaPmError> {
    match metadata_value {
        MediaMetadataValue::Literal(value) => Ok(value.clone()),
        MediaMetadataValue::Variant(binding) => {
            resolve_media_metadata_candidate_value(
                media_id,
                metadata_key,
                &MediaMetadataValueCandidate::Variant(binding.clone()),
                source,
                lookup,
            )
            .await
        }
        MediaMetadataValue::Fallback(candidates) => {
            for candidate in candidates {
                let resolved = resolve_media_metadata_candidate_value(
                    media_id,
                    metadata_key,
                    candidate,
                    source,
                    lookup,
                )
                .await;

                if let Ok(value) = resolved
                    && !value.trim().is_empty()
                {
                    return Ok(value);
                }
            }

            Err(MediaPmError::Workflow(format!(
                "media '{media_id}' metadata '{metadata_key}' fallback list did not resolve any non-empty value"
            )))
        }
    }
}

/// Resolves one metadata fallback candidate to a concrete string.
async fn resolve_media_metadata_candidate_value(
    media_id: &str,
    metadata_key: &str,
    candidate: &MediaMetadataValueCandidate,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext,
) -> Result<String, MediaPmError> {
    match candidate {
        MediaMetadataValueCandidate::Literal(value) => Ok(value.clone()),
        MediaMetadataValueCandidate::Variant(binding) => {
            let variant_source =
                resolve_variant_source_bytes(lookup, media_id, source, binding.variant.as_str())
                    .await?;

            let extracted = extract_metadata_value_from_variant_payload(
                lookup,
                media_id,
                metadata_key,
                binding.variant.as_str(),
                binding.metadata_key.as_str(),
                variant_source.bytes.as_slice(),
            )?;

            apply_metadata_regex_transform(
                media_id,
                metadata_key,
                binding.transform.as_ref(),
                extracted,
            )
        }
    }
}

/// Extracts one metadata value from variant payload bytes.
///
/// Resolution first attempts JSON lookup, then falls back to running ffprobe
/// against the variant bytes when JSON extraction does not produce the key.
fn extract_metadata_value_from_variant_payload(
    lookup: &MaterializationLookupContext,
    media_id: &str,
    metadata_name: &str,
    variant_name: &str,
    metadata_key: &str,
    variant_bytes: &[u8],
) -> Result<String, MediaPmError> {
    if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(variant_bytes)
        && let Some(extracted) = extract_metadata_key_from_json(&parsed, metadata_key)
    {
        return Ok(extracted);
    }

    let ffprobe_path = lookup.managed_ffprobe_path.as_deref().ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' bound to variant '{variant_name}' requires ffprobe lookup for key '{metadata_key}', but active managed ffmpeg is not configured"
        ))
    })?;

    extract_metadata_key_with_ffprobe(
        ffprobe_path,
        media_id,
        metadata_name,
        variant_name,
        metadata_key,
        variant_bytes,
    )
}

/// Applies optional regex transform to one extracted metadata value.
fn apply_metadata_regex_transform(
    media_id: &str,
    metadata_name: &str,
    transform: Option<&MediaMetadataRegexTransform>,
    extracted: String,
) -> Result<String, MediaPmError> {
    let Some(transform) = transform else {
        return Ok(extracted);
    };

    let full_match_pattern = format!("^(?:{})$", transform.pattern);
    let regex = Regex::new(&full_match_pattern).map_err(|error| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' transform.pattern '{}' is invalid regex: {error}",
            transform.pattern
        ))
    })?;

    if !regex.is_match(&extracted) {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' transform.pattern '{}' must fully match extracted value '{}'",
            transform.pattern, extracted
        )));
    }

    Ok(regex.replace(&extracted, transform.replacement.as_str()).into_owned())
}

/// Extracts one metadata key from JSON payloads, including ffprobe nested tag
/// layouts.
#[must_use]
fn extract_metadata_key_from_json(
    payload: &serde_json::Value,
    metadata_key: &str,
) -> Option<String> {
    let object = payload.as_object()?;

    if let Some(value) = lookup_json_string_key(object, metadata_key) {
        return Some(value);
    }

    if let Some(format_object) = object.get("format").and_then(serde_json::Value::as_object) {
        if let Some(value) = lookup_json_string_key(format_object, metadata_key) {
            return Some(value);
        }

        if let Some(tags) = format_object.get("tags").and_then(serde_json::Value::as_object)
            && let Some(value) = lookup_json_string_key(tags, metadata_key)
        {
            return Some(value);
        }
    }

    if let Some(streams) = object.get("streams").and_then(serde_json::Value::as_array) {
        for stream in streams {
            let Some(stream_object) = stream.as_object() else {
                continue;
            };

            if let Some(value) = lookup_json_string_key(stream_object, metadata_key) {
                return Some(value);
            }

            if let Some(tags) = stream_object.get("tags").and_then(serde_json::Value::as_object)
                && let Some(value) = lookup_json_string_key(tags, metadata_key)
            {
                return Some(value);
            }
        }
    }

    None
}

/// Looks up one string value by key with case-insensitive matching.
#[must_use]
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

/// Runs managed ffprobe and extracts one metadata key from probe output JSON.
fn extract_metadata_key_with_ffprobe(
    ffprobe_path: &Path,
    media_id: &str,
    metadata_name: &str,
    variant_name: &str,
    metadata_key: &str,
    variant_bytes: &[u8],
) -> Result<String, MediaPmError> {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let temp_path = std::env::temp_dir()
        .join(format!("mediapm-metadata-probe-{}-{unique}.bin", std::process::id()));

    ensure_managed_ffprobe_executable(ffprobe_path)?;

    fs::write(&temp_path, variant_bytes).map_err(|source| MediaPmError::Io {
        operation: "writing temporary metadata probe payload".to_string(),
        path: temp_path.clone(),
        source,
    })?;

    let output = Command::new(ffprobe_path)
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg(format!(
            "format={metadata_key}:stream={metadata_key}:format_tags={metadata_key}:stream_tags={metadata_key}"
        ))
        .arg("-of")
        .arg("json")
        .arg(&temp_path)
        .output()
        .map_err(|source| {
            let _ = fs::remove_file(&temp_path);
            MediaPmError::Workflow(format!(
                "running managed ffprobe '{}' for media '{media_id}' metadata '{metadata_name}' failed: {source}",
                ffprobe_path.display()
            ))
        })?;

    let _ = fs::remove_file(&temp_path);

    if !output.status.success() {
        return Err(MediaPmError::Workflow(format!(
            "managed ffprobe '{}' failed while resolving media '{media_id}' metadata '{metadata_name}' from variant '{}': {}",
            ffprobe_path.display(),
            variant_name,
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let parsed = serde_json::from_slice::<serde_json::Value>(&output.stdout).map_err(|error| {
        MediaPmError::Workflow(format!(
            "managed ffprobe output for media '{media_id}' metadata '{metadata_name}' could not be decoded as JSON: {error}"
        ))
    })?;

    extract_metadata_key_from_json(&parsed, metadata_key).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' expected key '{metadata_key}' in variant '{variant_name}', but ffprobe reported no matching field or tag"
        ))
    })
}

/// Ensures managed ffprobe binary is executable on current host.
fn ensure_managed_ffprobe_executable(ffprobe_path: &Path) -> Result<(), MediaPmError> {
    #[cfg(unix)]
    {
        let metadata = fs::metadata(ffprobe_path).map_err(|source| {
            MediaPmError::Workflow(format!(
                "reading managed ffprobe '{}' metadata failed: {source}",
                ffprobe_path.display()
            ))
        })?;

        let mode = metadata.permissions().mode();
        if mode & 0o111 == 0 {
            let mut permissions = metadata.permissions();
            permissions.set_mode(mode | 0o111);
            fs::set_permissions(ffprobe_path, permissions).map_err(|source| {
                MediaPmError::Workflow(format!(
                    "setting execute permission for managed ffprobe '{}' failed: {source}",
                    ffprobe_path.display()
                ))
            })?;
        }
    }

    #[cfg(not(unix))]
    {
        let _ = ffprobe_path;
    }

    Ok(())
}

/// Resolves host ffprobe path from active managed ffmpeg executable selector.
///
/// Resolution always projects the selector into the conductor `payload/`
/// layout, which is the only supported runtime location for managed tool
/// content.
#[must_use]
pub(super) fn resolve_managed_ffprobe_path(
    paths: &MediaPmPaths,
    machine: &MachineNickelDocument,
    lock: &MediaPmState,
) -> Option<PathBuf> {
    let ffmpeg_tool_id = lock.active_tools.get("ffmpeg")?;
    let ffmpeg_tool = machine.tools.get(ffmpeg_tool_id)?;
    let ToolKindSpec::Executable { command, .. } = &ffmpeg_tool.kind else {
        return None;
    };

    let selector = command.first()?.trim();
    if selector.is_empty() {
        return None;
    }

    let ffmpeg_selector_path = PathBuf::from(resolve_host_command_selector_path(selector)?);
    let ffmpeg_path = if ffmpeg_selector_path.is_absolute() {
        ffmpeg_selector_path
    } else {
        paths.tools_dir.join(ffmpeg_tool_id).join("payload").join(ffmpeg_selector_path)
    };
    let ffprobe_file_name = if cfg!(windows) { "ffprobe.exe" } else { "ffprobe" };

    let candidate = ffmpeg_path
        .parent()
        .map_or_else(|| PathBuf::from(ffprobe_file_name), |parent| parent.join(ffprobe_file_name));

    candidate.is_file().then_some(candidate)
}

/// Resolves a command selector expression to the host-specific path.
#[must_use]
fn resolve_host_command_selector_path(command_selector: &str) -> Option<String> {
    if command_selector.contains("context.os") {
        let host_os = std::env::consts::OS;
        let regex =
            Regex::new(r#"\$\{context\.os\s*==\s*\"([^\"]+)\"\s*\?\s*([^|}]*)\|\s*[^}]*\}"#)
                .ok()?;

        for captures in regex.captures_iter(command_selector) {
            let selector_os = captures.get(1).map(|value| value.as_str())?;
            if selector_os != host_os {
                continue;
            }

            let branch = captures.get(2).map(|value| value.as_str())?.trim();
            let unquoted = branch
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| branch.strip_prefix('\'').and_then(|value| value.strip_suffix('\'')))
                .unwrap_or(branch)
                .trim();

            if !unquoted.is_empty() {
                return Some(unquoted.to_string());
            }
        }

        return None;
    }

    let trimmed = command_selector.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}
