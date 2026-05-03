//! Managed `media-tagger` workflow synthesis.

use std::collections::BTreeMap;

use mediapm_conductor::{
    InputBinding, MachineNickelDocument, OutputPolicy, WorkflowSpec, WorkflowStepSpec,
};

use crate::builtins::media_tagger::{
    cover_art_slot_flag_member_name, cover_art_slot_image_member_name,
};
use crate::config::{
    DecodedOutputVariantConfig, MediaStep, MediaStepTool, ResolvedStepVariantFlow, ToolRequirement,
    decode_output_variant_config, decode_output_variant_policy,
};
use crate::error::MediaPmError;
use crate::lockfile::MediaLockFile;

use super::{
    FfmpegSlotLimits, INPUT_CONTENT, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS,
    MEDIA_TAGGER_APPLY_STEP_OFFSET, MEDIA_TAGGER_EXPANDED_STEPS_PER_MAPPING, OUTPUT_CONTENT,
    OUTPUT_SANDBOX_ARTIFACTS, VariantProducer, conductor_output_save_mode,
    expanded_step_index_for_mapping, extract_step_list_args, ffmpeg_cover_slot_enabled_input_name,
    ffmpeg_input_content_name, ffmpeg_output_capture_name, ffmpeg_output_path_input_name,
    ffmpeg_output_path_with_extension, media_step_id, normalize_output_extension,
    resolve_input_variant_producer, resolve_selected_dependency_tool_id,
    resolve_step_output_binding, resolve_step_tool_id, resolved_ffmpeg_family_output_extension,
    step_option_input_bindings, step_option_scalar,
};

/// Resolves the effective output extension for one managed media-tagger apply
/// step.
///
/// When the output variant omits an explicit extension, media-tagger preserves
/// the upstream produced extension when known so downstream tools can keep
/// codec-copy behavior on already-supported containers. Otherwise, it falls
/// back to the standard managed ffmpeg-family `.mkv` default.
#[must_use]
fn resolve_media_tagger_output_extension(
    configured_extension: Option<&str>,
    input_producer: &VariantProducer,
) -> Option<String> {
    match configured_extension.map(str::trim) {
        Some("") => None,
        Some(_) => normalize_output_extension(configured_extension),
        None => input_producer
            .output_extension()
            .map(ToString::to_string)
            .or_else(|| resolved_ffmpeg_family_output_extension(None)),
    }
}

/// Resolves the number of cover-art slots that should be wired into the
/// managed media-tagger apply step.
///
/// Explicit `write_all_images = "false"` disables slot wiring entirely.
/// Otherwise, optional `cover_art_slot_count` is honored and clamped to
/// available ffmpeg auxiliary input slots.
fn resolve_cover_art_slot_count(
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<usize, MediaPmError> {
    let max_cover_slots = ffmpeg_slot_limits.max_input_slots.saturating_sub(1);
    let write_all_images_enabled = step_option_scalar(step, "write_all_images")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none_or(|value| value.eq_ignore_ascii_case("true"));

    if !write_all_images_enabled {
        return Ok(0);
    }

    let Some(raw_cover_art_slot_count) = step_option_scalar(step, "cover_art_slot_count")
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(max_cover_slots);
    };

    let parsed_cover_art_slot_count = raw_cover_art_slot_count.parse::<usize>().map_err(|error| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' step #{step_index} options.cover_art_slot_count='{raw_cover_art_slot_count}' is invalid: {error}"
        ))
    })?;

    Ok(parsed_cover_art_slot_count.min(max_cover_slots))
}

/// Expands one `media-tagger` config step into metadata + ffmpeg-apply steps.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "media-tagger synthesis requires explicit dependency and producer state context"
)]
pub(super) fn synthesize_media_tagger_step_pair(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
    media_tagger_requirement: Option<&ToolRequirement>,
    producer_snapshot: &BTreeMap<String, VariantProducer>,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    let media_tagger_tool_id = resolve_step_tool_id(lock, machine, MediaStepTool::MediaTagger)?;
    let ffmpeg_tool_id = resolve_selected_dependency_tool_id(
        "media-tagger",
        "ffmpeg",
        media_tagger_requirement.and_then(ToolRequirement::normalized_ffmpeg_selector),
        lock,
        machine,
    )?;

    for (mapping_index, mapping) in mappings.iter().enumerate() {
        let Some(producer) = resolve_input_variant_producer(&mapping.input, producer_snapshot)
        else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} references unknown input variant '{}'",
                mapping.input
            )));
        };
        let (input_binding, input_dependency) = producer.to_binding();

        let metadata_step_index = expanded_step_index_for_mapping(
            mapping_index,
            MEDIA_TAGGER_EXPANDED_STEPS_PER_MAPPING,
            0,
        );
        let apply_step_index = expanded_step_index_for_mapping(
            mapping_index,
            MEDIA_TAGGER_EXPANDED_STEPS_PER_MAPPING,
            MEDIA_TAGGER_APPLY_STEP_OFFSET,
        );
        let metadata_step_id = format!(
            "{}-metadata",
            media_step_id(step_index, metadata_step_index, step.tool, mapping)
        );
        let apply_step_id =
            format!("{}-apply", media_step_id(step_index, apply_step_index, step.tool, mapping));
        let output_variant_value = step.output_variants.get(&mapping.output).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "missing output variant '{}' while resolving output policy",
                mapping.output
            ))
        })?;
        let output_binding = resolve_step_output_binding(
            step.tool,
            &step.output_variants,
            &mapping.output,
            ffmpeg_slot_limits,
        )?;
        let output_policy =
            decode_output_variant_policy(step.tool, &mapping.output, output_variant_value)
                .map_err(MediaPmError::Workflow)?;
        let DecodedOutputVariantConfig::Generic(output_config) =
            decode_output_variant_config(step.tool, &mapping.output, output_variant_value)
                .map_err(MediaPmError::Workflow)?
        else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} output variant '{}' must decode as generic output config",
                mapping.output
            )));
        };
        let apply_output_extension =
            resolve_media_tagger_output_extension(output_config.extension.as_deref(), producer);
        let apply_output_name = ffmpeg_output_capture_name(0);
        let apply_outputs = BTreeMap::from([(
            apply_output_name.clone(),
            OutputPolicy { save: conductor_output_save_mode(output_policy.save) },
        )]);

        let mut metadata_inputs = BTreeMap::new();
        let mut metadata_depends_on = Vec::new();
        metadata_inputs.insert(INPUT_CONTENT.to_string(), input_binding.clone());
        if let Some(step_dependency) = input_dependency.clone() {
            metadata_depends_on.push(step_dependency);
        }

        let (leading_args, trailing_args) = extract_step_list_args(media_id, step_index, step)?;
        let option_inputs = step_option_input_bindings(step.tool, &step.options)?;
        metadata_inputs
            .insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(leading_args));
        metadata_inputs
            .insert(INPUT_TRAILING_ARGS.to_string(), InputBinding::StringList(trailing_args));
        metadata_inputs.extend(option_inputs);

        workflow.steps.push(WorkflowStepSpec {
            id: metadata_step_id.clone(),
            tool: media_tagger_tool_id.clone(),
            inputs: metadata_inputs,
            depends_on: metadata_depends_on,
            outputs: BTreeMap::from([
                (OUTPUT_CONTENT.to_string(), OutputPolicy::default()),
                (OUTPUT_SANDBOX_ARTIFACTS.to_string(), OutputPolicy::default()),
            ]),
        });

        let mut apply_inputs = BTreeMap::new();
        let mut apply_depends_on = Vec::new();
        apply_inputs.insert(ffmpeg_input_content_name(0), input_binding);
        apply_inputs.insert(
            "ffmetadata_content".to_string(),
            InputBinding::String(format!("${{step_output.{metadata_step_id}.{OUTPUT_CONTENT}}}")),
        );
        apply_inputs.insert(
            ffmpeg_output_path_input_name(0),
            InputBinding::String(ffmpeg_output_path_with_extension(
                0,
                apply_output_extension.as_deref(),
            )),
        );
        apply_inputs.insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(Vec::new()));
        apply_inputs.insert(
            INPUT_TRAILING_ARGS.to_string(),
            InputBinding::StringList(vec!["-map".to_string(), "0".to_string()]),
        );
        apply_inputs.insert("map_metadata".to_string(), InputBinding::String("1".to_string()));
        apply_inputs.insert("movflags".to_string(), InputBinding::String(String::new()));

        let cover_art_slot_count =
            resolve_cover_art_slot_count(media_id, step_index, step, ffmpeg_slot_limits)?;
        for slot_index in 1..=cover_art_slot_count {
            let image_member = cover_art_slot_image_member_name(slot_index);
            let flag_member = cover_art_slot_flag_member_name(slot_index);
            apply_inputs.insert(
                ffmpeg_input_content_name(slot_index),
                InputBinding::String(format!(
                    "${{step_output.{metadata_step_id}.{OUTPUT_SANDBOX_ARTIFACTS}:zip({image_member})}}"
                )),
            );
            apply_inputs.insert(
                ffmpeg_cover_slot_enabled_input_name(slot_index),
                InputBinding::String(format!(
                    "${{step_output.{metadata_step_id}.{OUTPUT_SANDBOX_ARTIFACTS}:zip({flag_member})}}"
                )),
            );
        }

        if let Some(output_container) = step_option_scalar(step, "output_container")
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            apply_inputs.insert(
                "container".to_string(),
                InputBinding::String(output_container.to_string()),
            );
        }

        if let Some(step_dependency) = input_dependency {
            apply_depends_on.push(step_dependency);
        }
        apply_depends_on.push(metadata_step_id.clone());

        workflow.steps.push(WorkflowStepSpec {
            id: apply_step_id.clone(),
            tool: ffmpeg_tool_id.clone(),
            inputs: apply_inputs,
            depends_on: apply_depends_on,
            outputs: apply_outputs,
        });

        variant_producers.insert(
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: apply_step_id,
                output_name: apply_output_name,
                zip_member: output_binding.zip_member,
                extension: apply_output_extension,
            },
        );
    }

    Ok(())
}
