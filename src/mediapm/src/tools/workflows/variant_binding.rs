//! Resolves media variant names to managed workflow step-output bindings.
//!
//! Mirrors variant-producer registration during workflow synthesis so the
//! materializer can locate conductor step outputs without re-running synthesis.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use mediapm_cas::Hash;

use crate::config::hierarchy_types::expand_variant_selectors;
use crate::config::output_types::{OutputVariantValue, ResolvedStepVariantFlow};
use crate::config::{MediaSourceSpec, MediaStep, MediaStepTool, YtDlpOutputKind};
use crate::error::MediaPmError;

use crate::conductor_bridge::constants::OUTPUT_YT_DLP_ARCHIVE_FILE;

use super::ffmpeg::ffmpeg_output_capture_name;
use super::yt_dlp_inputs::resolve_step_output_binding;
use super::{
    FfmpegSlotLimits, VariantProducer, ffmpeg_step_id, media_step_id, resolve_step_variant_flow,
};

/// Resolved workflow output binding that produces one media variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedVariantOutputBinding {
    /// Step id whose output should be materialized for the requested variant.
    pub step_id: String,
    /// Output name captured from the selected step.
    pub output_name: String,
    /// Optional ZIP member selected from folder output artifacts.
    pub zip_member: Option<String>,
    /// Whether the binding fell back to producer variant `default`.
    pub used_default_variant: bool,
}

/// Resolves one variant to the managed workflow step-output producer binding.
#[cfg(test)]
pub(crate) fn resolve_media_variant_output_binding(
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<Option<ResolvedVariantOutputBinding>, MediaPmError> {
    resolve_media_variant_output_binding_with_limits(
        source,
        variant,
        FfmpegSlotLimits::default().max_input_slots,
        FfmpegSlotLimits::default().max_output_slots,
    )
}

/// Resolves one variant with explicit ffmpeg slot limits.
pub(crate) fn resolve_media_variant_output_binding_with_limits(
    source: &MediaSourceSpec,
    variant: &str,
    max_ffmpeg_input_slots: usize,
    max_ffmpeg_output_slots: usize,
) -> Result<Option<ResolvedVariantOutputBinding>, MediaPmError> {
    let ffmpeg_slot_limits = FfmpegSlotLimits {
        max_input_slots: max_ffmpeg_input_slots,
        max_output_slots: max_ffmpeg_output_slots,
    };
    resolve_media_variant_output_binding_with_ffmpeg_limits(source, variant, ffmpeg_slot_limits)
}

fn resolve_media_variant_output_binding_with_ffmpeg_limits(
    source: &MediaSourceSpec,
    variant: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<Option<ResolvedVariantOutputBinding>, MediaPmError> {
    let mut variant_producers = BTreeMap::<String, VariantProducer>::new();

    for (local_variant, hash_text) in &source.variant_hashes {
        let hash = Hash::from_str(hash_text).map_err(|_| {
            MediaPmError::Workflow(format!(
                "local variant '{local_variant}' has invalid CAS hash '{hash_text}'"
            ))
        })?;
        variant_producers.insert(local_variant.clone(), VariantProducer::ExternalData { hash });
    }

    for (step_index, step) in source.steps.iter().enumerate() {
        let producer_snapshot = variant_producers.clone();
        let mut resolved_step = step.clone();
        if !step.tool.is_source_ingest_tool() {
            let available_variants =
                producer_snapshot.keys().cloned().collect::<BTreeSet<String>>();
            resolved_step.input_variants =
                expand_variant_selectors(&step.input_variants, &available_variants).map_err(
                    |reason| MediaPmError::Workflow(format!("step #{step_index} {reason}")),
                )?;
        }

        let mappings = resolve_step_variant_flow(&resolved_step)
            .map_err(|reason| MediaPmError::Workflow(format!("step #{step_index} {reason}")))?;

        match step.tool {
            MediaStepTool::Import => {
                register_import_variant_producers(
                    step_index,
                    step.tool,
                    &mappings,
                    &mut variant_producers,
                )?;
            }
            MediaStepTool::Ffmpeg => {
                register_ffmpeg_variant_producers(
                    step_index,
                    &resolved_step,
                    &mappings,
                    &producer_snapshot,
                    ffmpeg_slot_limits,
                    &mut variant_producers,
                )?;
            }
            MediaStepTool::YtDlp => {
                register_per_mapping_variant_producers(
                    step_index,
                    step.tool,
                    &resolved_step,
                    &mappings,
                    ffmpeg_slot_limits,
                    &mut variant_producers,
                )?;
            }
            MediaStepTool::MediaTagger => {
                register_media_tagger_variant_producers(
                    step_index,
                    step.tool,
                    &mappings,
                    &mut variant_producers,
                )?;
            }
            MediaStepTool::Rsgain => {
                register_per_mapping_variant_producers(
                    step_index,
                    step.tool,
                    &resolved_step,
                    &mappings,
                    FfmpegSlotLimits::default(),
                    &mut variant_producers,
                )?;
            }
        }
    }

    let (producer, used_default_variant) = if let Some(exact) = variant_producers.get(variant) {
        (exact, false)
    } else if let Some(default_variant) = variant_producers.get("default") {
        (default_variant, true)
    } else {
        return Ok(None);
    };

    match producer {
        VariantProducer::StepOutput { step_id, output_name, zip_member } => {
            Ok(Some(ResolvedVariantOutputBinding {
                step_id: step_id.clone(),
                output_name: output_name.clone(),
                zip_member: zip_member.clone(),
                used_default_variant,
            }))
        }
        VariantProducer::ExternalData { .. } => Ok(None),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn register_import_variant_producers(
    step_index: usize,
    tool: MediaStepTool,
    mappings: &[ResolvedStepVariantFlow],
    variant_producers: &mut BTreeMap<String, VariantProducer>,
) -> Result<(), MediaPmError> {
    for (mapping_index, mapping) in mappings.iter().enumerate() {
        let step_id = media_step_id(step_index, mapping_index, tool, mapping);
        variant_producers.insert(
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id,
                output_name: mapping.output.clone(),
                zip_member: None,
            },
        );
    }
    Ok(())
}

fn register_ffmpeg_variant_producers(
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    producer_snapshot: &BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
) -> Result<(), MediaPmError> {
    if step.input_variants.len() > ffmpeg_slot_limits.max_input_slots {
        return Err(MediaPmError::Workflow(format!(
            "step #{step_index} declares {} ffmpeg input variants but maximum supported is {}",
            step.input_variants.len(),
            ffmpeg_slot_limits.max_input_slots,
        )));
    }

    for input_variant in &step.input_variants {
        if !producer_snapshot.contains_key(input_variant) {
            return Err(MediaPmError::Workflow(format!(
                "step #{step_index} references unknown input variant '{input_variant}'"
            )));
        }
    }

    let step_id = ffmpeg_step_id(step_index);
    for mapping in mappings {
        let output_binding = resolve_step_output_binding(
            MediaStepTool::Ffmpeg,
            &step.output_variants,
            &mapping.output,
            ffmpeg_slot_limits,
        )?;
        variant_producers.insert(
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: step_id.clone(),
                output_name: output_binding.output_name,
                zip_member: output_binding.zip_member,
            },
        );
    }

    Ok(())
}

fn register_per_mapping_variant_producers(
    step_index: usize,
    tool: MediaStepTool,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    ffmpeg_slot_limits: FfmpegSlotLimits,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
) -> Result<(), MediaPmError> {
    for (mapping_index, mapping) in mappings.iter().enumerate() {
        if matches!(tool, MediaStepTool::YtDlp)
            && let Some(OutputVariantValue::YtDlp(config)) =
                step.output_variants.get(&mapping.output)
            && matches!(config.kind, YtDlpOutputKind::Archive)
        {
            continue;
        }

        let step_id = media_step_id(step_index, mapping_index, tool, mapping);
        let output_binding = resolve_step_output_binding(
            tool,
            &step.output_variants,
            &mapping.output,
            ffmpeg_slot_limits,
        )?;
        variant_producers.insert(
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: step_id.clone(),
                output_name: output_binding.output_name,
                zip_member: output_binding.zip_member,
            },
        );

        if matches!(tool, MediaStepTool::YtDlp)
            && let Some(OutputVariantValue::YtDlp(config)) =
                step.output_variants.get(&mapping.output)
            && matches!(config.kind, YtDlpOutputKind::Primary)
            && step.output_variants.contains_key("archive")
        {
            variant_producers.insert(
                "archive".to_string(),
                VariantProducer::StepOutput {
                    step_id: step_id.clone(),
                    output_name: OUTPUT_YT_DLP_ARCHIVE_FILE.to_string(),
                    zip_member: None,
                },
            );
        }
    }
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn register_media_tagger_variant_producers(
    step_index: usize,
    tool: MediaStepTool,
    mappings: &[ResolvedStepVariantFlow],
    variant_producers: &mut BTreeMap<String, VariantProducer>,
) -> Result<(), MediaPmError> {
    for (mapping_index, mapping) in mappings.iter().enumerate() {
        let base_step_id = media_step_id(step_index, mapping_index, tool, mapping);
        let apply_step_id = format!("{base_step_id}-apply");
        let apply_output_name = ffmpeg_output_capture_name(0);
        variant_producers.insert(
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: apply_step_id,
                output_name: apply_output_name,
                zip_member: None,
            },
        );
    }
    Ok(())
}
