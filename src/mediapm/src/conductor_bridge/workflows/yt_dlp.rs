//! `yt-dlp` workflow-step synthesis.
//!
//! This module keeps downloader-specific fan-in/output-merging logic separate
//! from the shared workflow planner so tool-specific policies stay maintainable.

use std::collections::BTreeMap;

use mediapm_conductor::{InputBinding, OutputPolicy, WorkflowSpec, WorkflowStepSpec};

use crate::config::{
    MediaSourceSpec, MediaStep, ResolvedStepVariantFlow, decode_output_variant_policy,
};
use crate::error::MediaPmError;

use super::{
    FfmpegSlotLimits, INPUT_LEADING_ARGS, INPUT_SOURCE_URL, INPUT_TRAILING_ARGS, VariantProducer,
    conductor_output_save_mode, decode_yt_dlp_output_variant_config, extract_step_list_args,
    media_source_uri, resolve_step_output_binding, step_option_input_bindings, step_option_scalar,
    yt_dlp_inputs_for_output_variants, yt_dlp_step_id,
};

/// Expands one yt-dlp step into one multi-output workflow step.
#[expect(
    clippy::too_many_arguments,
    reason = "yt-dlp synthesis requires explicit workflow, source, mapping, and slot-limit context"
)]
pub(super) fn synthesize_yt_dlp_step(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    source: &MediaSourceSpec,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    tool_id: &str,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    let step_id = yt_dlp_step_id(step_index);
    let source_uri = step_option_scalar(step, "uri")
        .map_or_else(|| media_source_uri(media_id, source), ToString::to_string);
    let (leading_args, trailing_args) = extract_step_list_args(media_id, step_index, step)?;

    let mut inputs = BTreeMap::new();
    inputs.insert(INPUT_SOURCE_URL.to_string(), InputBinding::String(source_uri));
    inputs.insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(leading_args));
    inputs.insert(INPUT_TRAILING_ARGS.to_string(), InputBinding::StringList(trailing_args));
    inputs.extend(step_option_input_bindings(step.tool, &step.options)?);

    let mut outputs = BTreeMap::new();
    let mut pending_variant_updates = Vec::new();
    let mut output_configs = Vec::new();

    for mapping in mappings {
        let output_variant_config =
            decode_yt_dlp_output_variant_config(&mapping.output, &step.output_variants)?;
        output_configs.push(output_variant_config.clone());

        let output_policy = decode_output_variant_policy(
            step.tool,
            &mapping.output,
            step.output_variants.get(&mapping.output).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "missing output variant '{}' while resolving output policy",
                    mapping.output
                ))
            })?,
        )
        .map_err(MediaPmError::Workflow)?;

        let output_binding = resolve_step_output_binding(
            step.tool,
            &step.output_variants,
            &mapping.output,
            ffmpeg_slot_limits,
        )?;

        let output_name = output_binding.output_name;
        let policy = OutputPolicy { save: conductor_output_save_mode(output_policy.save) };

        if let Some(existing_policy) = outputs.get(&output_name)
            && existing_policy != &policy
        {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} maps multiple variants to yt-dlp output '{output_name}' with conflicting save policies"
            )));
        }
        outputs.insert(output_name.clone(), policy);

        pending_variant_updates.push((
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: step_id.clone(),
                output_name,
                zip_member: output_binding.zip_member,
                extension: None,
            },
        ));
    }

    inputs.extend(yt_dlp_inputs_for_output_variants(&output_configs)?);

    workflow.steps.push(WorkflowStepSpec {
        id: step_id,
        tool: tool_id.to_string(),
        inputs,
        depends_on: Vec::new(),
        outputs,
    });

    for (output_variant, producer) in pending_variant_updates {
        variant_producers.insert(output_variant, producer);
    }

    Ok(())
}
