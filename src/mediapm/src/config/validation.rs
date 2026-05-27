//! Schema validation for mediapm configuration documents.

use regex::Regex;

use crate::error::MediaPmError;
use mediapm_cas::Hash;
use std::path::Path;
use std::str::FromStr;
use url::Url;

use std::collections::BTreeMap;

use super::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS, DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, DecodedOutputVariantConfig,
    HierarchyEntryKind, MediaMetadataValue, MediaMetadataValueCandidate,
    MediaMetadataVariantBinding, MediaPmDocument, MediaRuntimeStorage, MediaSourceSpec, MediaStep,
    MediaStepTool, OutputCaptureKind, OutputSaveConfig, TransformInputValue,
    playlist_format_is_default,
};

use super::{
    decode_output_variant_config, decode_output_variant_policy, expand_variant_selectors,
    flatten_hierarchy_nodes_for_runtime, has_step_option_scalar, normalize_selector_compare_value,
    resolve_step_variant_flow, step_option_scalar,
};

/// Validates that a decoded state document keeps only `version` and `state`.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the document shape is invalid.
pub(super) fn validate_mediapm_state_document_shape(
    state_path: &Path,
    document: &MediaPmDocument,
) -> Result<(), MediaPmError> {
    let has_non_state_fields = document.runtime != MediaRuntimeStorage::default()
        || !document.tools.is_empty()
        || !document.media.is_empty()
        || !document.hierarchy.is_empty();

    if has_non_state_fields {
        return Err(MediaPmError::Workflow(format!(
            "{} must contain only top-level 'version' and 'state' properties",
            state_path.display()
        )));
    }

    Ok(())
}

/// Validates media-source schema invariants that require cross-field checks.
pub(super) fn validate_media_document(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    validate_tool_requirements(document)?;
    validate_runtime_materialization_preference_order(document)?;

    for (media_id, source) in &document.media {
        validate_media_source(media_id, source)?;
    }

    let playlist_media_index = collect_playlist_media_index(document)?;
    validate_hierarchy_entries(document, &playlist_media_index)?;
    Ok(())
}

/// Validates runtime-configured materialization method ordering.
fn validate_runtime_materialization_preference_order(
    document: &MediaPmDocument,
) -> Result<(), MediaPmError> {
    let Some(order) = document.runtime.materialization_preference_order.as_ref() else {
        return Ok(());
    };

    if order.is_empty() {
        return Err(MediaPmError::Workflow(
            "runtime.materialization_preference_order must contain at least one method".to_string(),
        ));
    }

    let mut seen = std::collections::BTreeSet::new();
    for method in order {
        if !seen.insert(*method) {
            return Err(MediaPmError::Workflow(format!(
                "runtime.materialization_preference_order contains duplicate method '{}'",
                method.as_label()
            )));
        }
    }

    Ok(())
}

/// Collects effective hierarchy-id -> media-path mappings for playlist entries.
fn collect_playlist_media_index(
    document: &MediaPmDocument,
) -> Result<BTreeMap<String, String>, MediaPmError> {
    let flattened_hierarchy = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;
    let mut index = BTreeMap::new();

    for flattened_entry in &flattened_hierarchy {
        if !matches!(flattened_entry.entry.kind, HierarchyEntryKind::Media) {
            continue;
        }

        let Some(hierarchy_id) = flattened_entry.hierarchy_id.as_deref() else {
            continue;
        };

        if let Some(previous_path) =
            index.insert(hierarchy_id.to_string(), flattened_entry.path.clone())
            && previous_path != flattened_entry.path
        {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy id '{hierarchy_id}' resolves to multiple media paths ('{previous_path}' and '{}')",
                flattened_entry.path
            )));
        }
    }

    Ok(index)
}

/// Metadata describing one resolved producer for hierarchy-policy validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VariantProducerValidationMeta {
    /// Variant resolves to pre-seeded local CAS hash content.
    LocalHash,
    /// Variant resolves to one step output with explicit persistence policy.
    StepOutput {
        /// Whether this output kind captures ZIP-encoded folder payload.
        is_folder_output: bool,
        /// Effective tri-state save policy.
        save: OutputSaveConfig,
    },
}

/// Returns whether one decoded output variant maps to a folder capture payload.
#[must_use]
fn decoded_output_variant_is_folder_capture(decoded: &DecodedOutputVariantConfig) -> bool {
    match decoded {
        DecodedOutputVariantConfig::Generic(config) => {
            matches!(config.effective_capture_kind(), OutputCaptureKind::Folder)
        }
        DecodedOutputVariantConfig::YtDlp(config) => {
            matches!(config.effective_capture_kind(), OutputCaptureKind::Folder)
        }
    }
}

/// Collects latest producer metadata for every variant defined by one source.
fn collect_variant_producer_validation_meta(
    media_id: &str,
    source: &MediaSourceSpec,
) -> Result<BTreeMap<String, VariantProducerValidationMeta>, MediaPmError> {
    let mut producers = BTreeMap::new();

    for variant in source.variant_hashes.keys() {
        producers.insert(variant.clone(), VariantProducerValidationMeta::LocalHash);
    }

    for (step_index, step) in source.steps.iter().enumerate() {
        for (variant_key, value) in &step.output_variants {
            let decoded =
                decode_output_variant_config(step.tool, variant_key, value).map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{step_index} {reason}"
                    ))
                })?;
            let policy =
                decode_output_variant_policy(step.tool, variant_key, value).map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{step_index} {reason}"
                    ))
                })?;

            producers.insert(
                variant_key.clone(),
                VariantProducerValidationMeta::StepOutput {
                    is_folder_output: decoded_output_variant_is_folder_capture(&decoded),
                    save: policy.save,
                },
            );
        }
    }

    Ok(producers)
}

/// Validates hierarchy entry invariants, including persistence-policy
/// guarantees for referenced workflow-produced variants.
///
/// Policy summary:
/// - all hierarchy-referenced step outputs must keep `save != false`,
/// - `kind = "media"` entries must reference file variants,
/// - `kind = "media_folder"` entries must reference folder variants and may
///   keep
///   default `save = true`,
/// - hierarchy `rename_files` rules are allowed only on `media_folder` entries and
///   must compile as valid regex patterns.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps hierarchy validation invariants in one place so cross-field policy checks remain explicit"
)]
fn validate_hierarchy_entries(
    document: &MediaPmDocument,
    playlist_media_index: &BTreeMap<String, String>,
) -> Result<(), MediaPmError> {
    let flattened_hierarchy = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;

    for flattened_entry in &flattened_hierarchy {
        let hierarchy_path = flattened_entry.path.as_str();
        let entry = &flattened_entry.entry;

        if !matches!(entry.kind, HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder) {
            continue;
        }

        if !entry.ids.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' kind 'media' must not define ids"
            )));
        }

        if !playlist_format_is_default(&entry.format) {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' kind 'media' must not define format"
            )));
        }

        if entry.media_id.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' has empty media_id"
            )));
        }

        let source = document.media.get(&entry.media_id).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' references unknown media '{}'",
                entry.media_id
            ))
        })?;

        let metadata_placeholders =
            hierarchy_metadata_placeholder_keys(hierarchy_path).map_err(|reason| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' has invalid metadata placeholder syntax: {reason}"
                ))
            })?;

        for metadata_key in metadata_placeholders {
            if source
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get(metadata_key.as_str()))
                .is_none()
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' references undefined metadata key '{metadata_key}' for media '{}'",
                    entry.media_id
                )));
            }
        }

        if entry.variants.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' must define at least one variant"
            )));
        }

        let is_media_folder = matches!(entry.kind, HierarchyEntryKind::MediaFolder);
        if !is_media_folder && !entry.rename_files.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy kind 'media' path '{hierarchy_path}' must not define rename_files; rename rules are only supported on kind 'media_folder'"
            )));
        }
        for (rule_index, rule) in entry.rename_files.iter().enumerate() {
            let pattern = rule.pattern.trim();
            if pattern.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] must define a non-empty regex pattern"
                )));
            }
            Regex::new(pattern).map_err(|error| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] pattern '{pattern}' is invalid: {error}"
                ))
            })?;

            let replacement_placeholder_keys =
                hierarchy_metadata_placeholder_keys(rule.replacement.as_str()).map_err(
                    |reason| {
                        MediaPmError::Workflow(format!(
                            "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] replacement has invalid metadata placeholder syntax: {reason}"
                        ))
                    },
                )?;

            if !replacement_placeholder_keys.is_empty() {
                let metadata = source.metadata.as_ref().ok_or_else(|| {
                    MediaPmError::Workflow(format!(
                        "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] replacement references metadata placeholders but media '{}' does not define metadata",
                        entry.media_id
                    ))
                })?;

                for metadata_key in replacement_placeholder_keys {
                    if metadata.get(metadata_key.as_str()).is_none() {
                        return Err(MediaPmError::Workflow(format!(
                            "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] replacement references undefined metadata key '{metadata_key}' for media '{}'",
                            entry.media_id
                        )));
                    }
                }
            }
        }

        let producers = collect_variant_producer_validation_meta(&entry.media_id, source)?;
        let available_variants =
            producers.keys().cloned().collect::<std::collections::BTreeSet<_>>();
        let resolved_variants = expand_variant_selectors(&entry.variants, &available_variants)
            .map_err(|reason| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' {reason} for media '{}'",
                    entry.media_id
                ))
            })?;

        if !is_media_folder && resolved_variants.len() != 1 {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy file path '{hierarchy_path}' must resolve exactly one variant"
            )));
        }

        for resolved_variant in &resolved_variants {
            let producer = producers.get(resolved_variant.as_str()).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' references unknown resolved variant '{resolved_variant}' for media '{}'",
                    entry.media_id
                ))
            })?;

            if is_media_folder
                && matches!(
                    producer,
                    VariantProducerValidationMeta::StepOutput { is_folder_output: false, .. }
                )
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy directory path '{hierarchy_path}' requires folder variants, but resolved variant '{resolved_variant}' for media '{}' is not a folder output",
                    entry.media_id
                )));
            }

            if !is_media_folder
                && matches!(
                    producer,
                    VariantProducerValidationMeta::StepOutput { is_folder_output: true, .. }
                )
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy file path '{hierarchy_path}' requires file variants, but resolved variant '{resolved_variant}' for media '{}' is a folder output",
                    entry.media_id
                )));
            }

            if let VariantProducerValidationMeta::StepOutput { is_folder_output: _, save } =
                *producer
                && !save.should_persist()
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' requires resolved variant '{resolved_variant}' for media '{}' to have save=true or save=\"full\" on its latest producer step",
                    entry.media_id
                )));
            }
        }
    }

    for flattened_entry in &flattened_hierarchy {
        let hierarchy_path = flattened_entry.path.as_str();
        let entry = &flattened_entry.entry;

        if !matches!(entry.kind, HierarchyEntryKind::Playlist) {
            continue;
        }

        if !entry.variants.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' kind 'playlist' must not define variants"
            )));
        }

        if !entry.rename_files.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' kind 'playlist' must not define rename_files"
            )));
        }

        if hierarchy_path.ends_with('/') {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy playlist path '{hierarchy_path}' must be a file path"
            )));
        }

        if hierarchy_path.contains("${") {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy playlist path '{hierarchy_path}' must not contain placeholders"
            )));
        }

        if entry.ids.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy playlist path '{hierarchy_path}' must define at least one playlist id"
            )));
        }

        for (item_index, item) in entry.ids.iter().enumerate() {
            let hierarchy_id = item.id().trim();
            if hierarchy_id.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{hierarchy_path}' ids[{item_index}] has empty id"
                )));
            }

            let media_path = playlist_media_index.get(hierarchy_id).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{hierarchy_path}' ids[{item_index}] references unknown hierarchy id '{hierarchy_id}'"
                ))
            })?;

            if media_path.ends_with('/') || media_path.ends_with('\\') {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{hierarchy_path}' ids[{item_index}] references hierarchy id '{hierarchy_id}' whose target '{media_path}' is not a media file path"
                )));
            }
        }
    }

    Ok(())
}

/// Validates desired tool requirement selector invariants.
fn validate_tool_requirements(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    for (tool_name, requirement) in &document.tools {
        let version = requirement.normalized_version();
        let tag = requirement.normalized_tag();

        // Builtin source-ingest tools (import) are never
        // downloader-provisioned, so they are not required to carry a release
        // selector.
        let requires_selector = !MediaStepTool::is_builtin_source_ingest_name(tool_name.as_str());

        if requires_selector && version.is_none() && tag.is_none() {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must define at least one selector: version or tag"
            )));
        }

        if let (Some(version), Some(tag)) = (&version, &tag)
            && normalize_selector_compare_value(version) != normalize_selector_compare_value(tag)
        {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' defines mismatched version '{version}' and tag '{tag}'; when both are provided they must refer to the same release selector"
            )));
        }

        if requirement
            .dependencies
            .ffmpeg_version
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(MediaPmError::Workflow(format!(
                "tools.{tool_name}.dependencies.ffmpeg_version must be non-empty when provided"
            )));
        }

        if requirement
            .dependencies
            .sd_version
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(MediaPmError::Workflow(format!(
                "tools.{tool_name}.dependencies.sd_version must be non-empty when provided"
            )));
        }

        let has_ffmpeg_dependency = requirement.dependencies.ffmpeg_version.is_some();
        let has_sd_dependency = requirement.dependencies.sd_version.is_some();
        let is_media_tagger = tool_name.eq_ignore_ascii_case("media-tagger");
        let is_yt_dlp = tool_name.eq_ignore_ascii_case("yt-dlp");
        let is_rsgain = tool_name.eq_ignore_ascii_case("rsgain");

        if is_media_tagger || is_yt_dlp {
            if has_sd_dependency {
                return Err(MediaPmError::Workflow(format!(
                    "tool '{tool_name}' must not define tools.{tool_name}.dependencies.sd_version; only tools.rsgain.dependencies.sd_version is supported"
                )));
            }
        } else if is_rsgain {
            // rsgain may define both ffmpeg and sd dependency selectors.
        } else if has_ffmpeg_dependency || has_sd_dependency {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must not define dependency selector overrides; only tools.yt-dlp.dependencies.ffmpeg_version, tools.media-tagger.dependencies.ffmpeg_version, tools.rsgain.dependencies.ffmpeg_version, and tools.rsgain.dependencies.sd_version are supported"
            )));
        }

        if tool_name.eq_ignore_ascii_case("ffmpeg") {
            if requirement.max_input_slots_or_default() == 0 {
                return Err(MediaPmError::Workflow(format!(
                    "tools.ffmpeg.max_input_slots must be at least 1 (default {DEFAULT_FFMPEG_MAX_INPUT_SLOTS})",
                )));
            }

            if requirement.max_output_slots_or_default() == 0 {
                return Err(MediaPmError::Workflow(format!(
                    "tools.ffmpeg.max_output_slots must be at least 1 (default {DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS})",
                )));
            }
        } else if requirement.max_input_slots.is_some() || requirement.max_output_slots.is_some() {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must not define ffmpeg slot settings; only tools.ffmpeg.max_input_slots and tools.ffmpeg.max_output_slots are supported"
            )));
        }
    }

    Ok(())
}

/// Validates one media source entry.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn validate_media_source(media_id: &str, source: &MediaSourceSpec) -> Result<(), MediaPmError> {
    if source.steps.is_empty() && source.variant_hashes.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' must define at least one step or at least one variant_hashes entry"
        )));
    }

    if let Some(workflow_id) = source.workflow_id.as_deref()
        && workflow_id.trim().is_empty()
    {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' defines an empty workflow_id override"
        )));
    }

    if source.id.is_some() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' must not define id; playlist references now resolve through hierarchy node ids"
        )));
    }

    let mut available_variants = source
        .variant_hashes
        .keys()
        .map(ToString::to_string)
        .collect::<std::collections::BTreeSet<_>>();

    for (variant, hash) in &source.variant_hashes {
        if variant.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' has an empty variant name in variant_hashes"
            )));
        }
        if hash.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' variant '{variant}' has an empty CAS hash pointer"
            )));
        }
    }

    for (index, step) in source.steps.iter().enumerate() {
        let mut resolved_step = step.clone();
        if !step.tool.is_source_ingest_tool() {
            resolved_step.input_variants =
                expand_variant_selectors(&step.input_variants, &available_variants).map_err(
                    |reason| {
                        MediaPmError::Workflow(format!("media '{media_id}' step #{index} {reason}"))
                    },
                )?;
        }

        let flow = resolve_step_variant_flow(&resolved_step).map_err(|reason| {
            MediaPmError::Workflow(format!("media '{media_id}' step #{index} {reason}"))
        })?;

        validate_step_output_variant_configs(media_id, index, &resolved_step)?;

        for key in resolved_step.options.keys() {
            if !is_allowed_step_option(resolved_step.tool, key) {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses unsupported option '{key}' for tool '{}'",
                    resolved_step.tool.as_str()
                )));
            }
        }

        if resolved_step.tool.is_online_media_downloader() {
            let uri = step_option_scalar(&resolved_step, "uri").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.uri",
                    resolved_step.tool.as_str()
                ))
            })?;

            let uri = Url::parse(uri).map_err(|err| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has invalid options.uri '{uri}': {err}"
                ))
            })?;
            if !matches!(uri.scheme(), "http" | "https") {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} options.uri must use http/https, observed '{}'",
                    uri.scheme()
                )));
            }
        } else if matches!(resolved_step.tool, MediaStepTool::Import) {
            let kind = step_option_scalar(&resolved_step, "kind").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.kind",
                    resolved_step.tool.as_str()
                ))
            })?;

            if kind != "cas_hash" {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} options.kind must be 'cas_hash' for tool '{}', observed '{kind}'",
                    resolved_step.tool.as_str()
                )));
            }

            let hash_text = step_option_scalar(&resolved_step, "hash").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.hash",
                    resolved_step.tool.as_str()
                ))
            })?;
            Hash::from_str(hash_text).map_err(|_| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has invalid options.hash '{hash_text}'"
                ))
            })?;
        } else if has_step_option_scalar(&resolved_step, "uri") {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{index} uses tool '{}' and must not define options.uri",
                resolved_step.tool.as_str()
            )));
        }

        if matches!(resolved_step.tool, MediaStepTool::Ffmpeg) {
            for input_variant in &resolved_step.input_variants {
                if !available_variants.contains(input_variant.trim()) {
                    return Err(MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{index} references unknown input variant '{input_variant}'"
                    )));
                }
            }
        }

        for mapping in &flow {
            if !resolved_step.tool.is_source_ingest_tool()
                && !available_variants.contains(&mapping.input)
            {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} references unknown input variant '{}'",
                    mapping.input
                )));
            }

            available_variants.insert(mapping.output.clone());
        }

        for (key, value) in &resolved_step.options {
            if key.trim().is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has an empty options key"
                )));
            }

            let TransformInputValue::String(text) = value;
            let _ = text;
        }
    }

    validate_media_metadata_entries(media_id, source)?;

    Ok(())
}

/// Validates strict media-metadata entry semantics for one source.
fn validate_media_metadata_entries(
    media_id: &str,
    source: &MediaSourceSpec,
) -> Result<(), MediaPmError> {
    let Some(metadata) = source.metadata.as_ref() else {
        return Ok(());
    };

    let producers = collect_variant_producer_validation_meta(media_id, source)?;

    for (metadata_name, metadata_value) in metadata {
        if metadata_name.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' defines an empty metadata key"
            )));
        }

        match metadata_value {
            MediaMetadataValue::Literal(_) => {}
            MediaMetadataValue::Variant(binding) => {
                validate_media_metadata_variant_binding(
                    media_id,
                    metadata_name,
                    binding,
                    &producers,
                )?;
            }
            MediaMetadataValue::Fallback(candidates) => {
                if candidates.is_empty() {
                    return Err(MediaPmError::Workflow(format!(
                        "media '{media_id}' metadata '{metadata_name}' fallback list must be non-empty"
                    )));
                }

                for candidate in candidates {
                    if let MediaMetadataValueCandidate::Variant(binding) = candidate {
                        validate_media_metadata_variant_binding(
                            media_id,
                            metadata_name,
                            binding,
                            &producers,
                        )?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Validates one variant-backed metadata entry.
fn validate_media_metadata_variant_binding(
    media_id: &str,
    metadata_name: &str,
    binding: &MediaMetadataVariantBinding,
    producers: &BTreeMap<String, VariantProducerValidationMeta>,
) -> Result<(), MediaPmError> {
    let variant_name = binding.variant.trim();
    if variant_name.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' must define a non-empty variant"
        )));
    }

    let metadata_key = binding.metadata_key.trim();
    if metadata_key.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' must define a non-empty metadata_key"
        )));
    }

    if let Some(transform) = &binding.transform {
        let pattern = transform.pattern.trim();
        if pattern.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' metadata '{metadata_name}' transform.pattern must be non-empty"
            )));
        }

        let full_match_pattern = format!("^(?:{pattern})$");
        Regex::new(&full_match_pattern).map_err(|error| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' metadata '{metadata_name}' transform.pattern is invalid regex '{pattern}': {error}"
            ))
        })?;
    }

    let producer = producers.get(variant_name).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' references unknown variant '{variant_name}'"
        ))
    })?;

    if matches!(producer, VariantProducerValidationMeta::StepOutput { is_folder_output: true, .. })
    {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' references variant '{variant_name}' that resolves to a folder output; metadata bindings require file variants"
        )));
    }

    Ok(())
}

/// Parses supported hierarchy placeholders from one hierarchy key.
///
/// Supported placeholders:
/// - `${media.id}`
/// - `${media.metadata.<key>}`
///
/// Returns each referenced `${media.metadata.<key>}` key in first-seen order.
pub(crate) fn hierarchy_metadata_placeholder_keys(
    hierarchy_path: &str,
) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut cursor = 0usize;

    while let Some(relative_start) = hierarchy_path[cursor..].find("${") {
        let placeholder_start = cursor + relative_start;
        let after_marker = &hierarchy_path[placeholder_start + 2..];
        let Some(relative_end) = after_marker.find('}') else {
            return Err("missing closing '}' for placeholder".to_string());
        };

        let expression = &after_marker[..relative_end];
        let expression = expression.trim();

        if expression == "media.id" {
            cursor = placeholder_start + 2 + relative_end + 1;
            continue;
        }

        let metadata_key = expression
            .strip_prefix("media.metadata.")
            .ok_or_else(|| {
                format!(
                    "unsupported placeholder '${{{expression}}}'; only '${{media.id}}' and '${{media.metadata.<key>}}' are supported"
                )
            })?
            .trim();

        if metadata_key.is_empty() {
            return Err(format!(
                "placeholder '${{{expression}}}' must reference a non-empty metadata key"
            ));
        }

        keys.push(metadata_key.to_string());
        cursor = placeholder_start + 2 + relative_end + 1;
    }

    Ok(keys)
}

/// Validates tool-specific output-variant configuration object schemas.
fn validate_step_output_variant_configs(
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
) -> Result<(), MediaPmError> {
    for (key, value) in &step.output_variants {
        let normalized_key = key.trim();
        let decoded =
            decode_output_variant_config(step.tool, normalized_key, value).map_err(|reason| {
                MediaPmError::Workflow(format!("media '{media_id}' step #{step_index} {reason}"))
            })?;

        if matches!(step.tool, MediaStepTool::Ffmpeg)
            && matches!(decoded, DecodedOutputVariantConfig::Generic(ref config) if config.kind != "primary")
        {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} ffmpeg output variant '{normalized_key}' must use kind 'primary'"
            )));
        }
    }

    Ok(())
}

/// Returns whether one step option key is supported for the given tool.
#[must_use]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn is_allowed_step_option(tool: MediaStepTool, key: &str) -> bool {
    match tool {
        MediaStepTool::YtDlp => matches!(
            key,
            "uri"
                | "leading_args"
                | "trailing_args"
                | "option_args"
                | "format"
                | "format_sort"
                | "extract_audio"
                | "audio_format"
                | "audio_quality"
                | "remux_video"
                | "recode_video"
                | "convert_subs"
                | "convert_thumbnails"
                | "merge_output_format"
                | "embed_thumbnail"
                | "embed_metadata"
                | "embed_subs"
                | "embed_chapters"
                | "embed_info_json"
                | "write_subs"
                | "sub_langs"
                | "sub_format"
                | "write_thumbnail"
                | "write_all_thumbnails"
                | "write_info_json"
                | "clean_info_json"
                | "write_comments"
                | "write_description"
                | "write_annotations"
                | "write_link"
                | "write_url_link"
                | "write_webloc_link"
                | "write_desktop_link"
                | "write_chapters"
                | "split_chapters"
                | "playlist_items"
                | "no_playlist"
                | "skip_download"
                | "retries"
                | "limit_rate"
                | "concurrent_fragments"
                | "proxy"
                | "socket_timeout"
                | "sleep_subtitles"
                | "user_agent"
                | "referer"
                | "add_header"
                | "cookies"
                | "cookies_from_browser"
                | "cache_dir"
                | "js_runtimes"
                | "ffmpeg_location"
                | "paths"
                | "output"
                | "parse_metadata"
                | "replace_in_metadata"
                | "download_sections"
                | "postprocessor_args"
                | "extractor_args"
                | "http_chunk_size"
                | "download_archive"
                | "sponsorblock_mark"
                | "sponsorblock_remove"
        ),
        MediaStepTool::Import => matches!(key, "kind" | "hash"),
        MediaStepTool::Ffmpeg => matches!(
            key,
            "leading_args"
                | "trailing_args"
                // common options
                | "option_args"
                | "audio_codec"
                | "video_codec"
                | "container"
                | "audio_bitrate"
                | "video_bitrate"
                | "audio_quality"
                | "video_quality"
                | "crf"
                | "preset"
                | "threads"
                | "log_level"
                | "progress"
                // less-common but useful options
                | "tune"
                | "profile"
                | "level"
                | "pixel_format"
                | "frame_rate"
                | "sample_rate"
                | "channels"
                | "audio_filters"
                | "video_filters"
                | "filter_complex"
                | "start_time"
                | "duration"
                | "to"
                | "movflags"
                | "map_metadata"
                | "map_chapters"
                | "map"
                | "map_channel"
                | "copy_ts"
                | "start_at_zero"
                | "stats"
                | "no_overwrite"
                | "codec_copy"
                | "faststart"
                | "hwaccel"
                | "sample_format"
                | "channel_layout"
                | "metadata"
                | "timestamp"
                | "disposition"
                | "fps_mode"
                | "force_key_frames"
                | "aspect"
                | "stream_loop"
                | "max_muxing_queue_size"
                | "strict"
                | "maxrate"
                | "bufsize"
                | "bitstream_filter"
                | "shortest"
                | "vn"
                | "an"
                | "sn"
                | "dn"
                | "id3v2_version"
        ),
        MediaStepTool::Rsgain => matches!(
            key,
            "leading_args"
                | "trailing_args"
                | "option_args"
                | "mode"
                | "album"
                | "album_aes77"
                | "skip_existing"
                | "tagmode"
                | "loudness"
                | "target_lufs"
                | "clip_mode"
                | "true_peak"
                | "dual_mono"
                | "album_mode"
                | "max_peak"
                | "lowercase"
                | "id3v2_version"
                | "opus_mode"
                | "jobs"
                | "multithread"
                | "preset"
                | "dry_run"
                | "output"
                | "quiet"
                | "skip_tags"
                | "preserve_mtime"
                | "preserve_mtimes"
                | "input_extension"
        ),
        MediaStepTool::MediaTagger => matches!(
            key,
            "leading_args"
                | "trailing_args"
                | "option_args"
                | "acoustid_endpoint"
                | "musicbrainz_endpoint"
                | "cache_dir"
                | "cache_expiry_seconds"
                | "strict_identification"
                | "write_all_tags"
                | "write_all_images"
                | "recording_mbid"
                | "release_mbid"
                | "output_container"
        ),
    }
}

/// Returns one source URI string for diagnostics/materialization bookkeeping.
#[must_use]
pub(crate) fn media_source_uri(media_id: &str, source: &MediaSourceSpec) -> String {
    source
        .steps
        .iter()
        .find_map(|step| {
            if step.tool.is_online_media_downloader() {
                step_option_scalar(step, "uri").map(ToString::to_string)
            } else {
                None
            }
        })
        .unwrap_or_else(|| format!("local:{media_id}"))
}
