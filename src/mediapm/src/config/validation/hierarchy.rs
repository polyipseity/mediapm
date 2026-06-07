//! Hierarchy and media-metadata validation helpers.

use std::collections::BTreeMap;

use regex::Regex;

use crate::error::MediaPmError;

use super::super::{
    DecodedOutputVariantConfig, HierarchyEntryKind, MediaMetadataValue,
    MediaMetadataValueCandidate, MediaMetadataVariantBinding, MediaPmDocument, MediaSourceSpec,
    OutputCaptureKind, OutputSaveConfig, decode_output_variant_config,
    decode_output_variant_policy, expand_variant_selectors, flatten_hierarchy_nodes_for_runtime,
    playlist_format_is_default,
};

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
pub(super) fn validate_hierarchy_entries(
    document: &MediaPmDocument,
    playlist_media_index: &BTreeMap<String, Vec<String>>,
) -> Result<(), MediaPmError> {
    let flattened_hierarchy = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;

    for flattened_entry in &flattened_hierarchy {
        let hierarchy_path = flattened_entry.path_str();
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
            super::hierarchy_metadata_placeholder_keys(&hierarchy_path).map_err(|reason| {
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
                super::hierarchy_metadata_placeholder_keys(rule.replacement.as_str()).map_err(
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
        let hierarchy_path = flattened_entry.path_str();
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

            let media_path_components = playlist_media_index.get(hierarchy_id).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{hierarchy_path}' ids[{item_index}] references unknown hierarchy id '{hierarchy_id}'"
                ))
            })?;
            let media_path = media_path_components.join("/");

            if media_path.ends_with('/') || media_path.ends_with('\\') {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{hierarchy_path}' ids[{item_index}] references hierarchy id '{hierarchy_id}' whose target '{media_path}' is not a media file path"
                )));
            }
        }
    }

    Ok(())
}

/// Validates strict media-metadata entry semantics for one source.
pub(super) fn validate_media_metadata_entries(
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
