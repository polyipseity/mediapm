//! `yt-dlp` workflow-step synthesis.
//!
//! This module keeps downloader-specific fan-in/output-merging logic separate
//! from the shared workflow planner so tool-specific policies stay maintainable.

use std::collections::BTreeMap;

use mediapm_conductor::{NickelDocument, OutputCaptureSpec, WorkflowSpec, WorkflowStepSpec};

use crate::config::{MediaSourceSpec, MediaStep, ResolvedStepVariantFlow, YtDlpOutputKind};
use crate::error::MediaPmError;

use super::yt_dlp_inputs::{
    decode_yt_dlp_output_variant_config, resolve_step_output_binding,
    yt_dlp_inputs_for_output_variants,
};
use super::{
    FfmpegSlotLimits, INPUT_LEADING_ARGS, INPUT_SOURCE_URL, INPUT_TRAILING_ARGS,
    OUTPUT_YT_DLP_LINK_ARTIFACTS, VariantProducer, extract_step_list_args, media_source_uri,
    resolve_builtin_tool_id, step_option_input_bindings, step_option_scalar, yt_dlp_step_id,
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
    machine: &NickelDocument,
) -> Result<(), MediaPmError> {
    let step_id = yt_dlp_step_id(step_index);
    let source_uri = step_option_scalar(step, "uri")
        .map_or_else(|| media_source_uri(media_id, source), ToString::to_string);
    let (leading_args, trailing_args) = extract_step_list_args(step);

    let mut inputs = BTreeMap::new();
    inputs.insert(INPUT_SOURCE_URL.to_string(), source_uri);
    inputs.insert(INPUT_LEADING_ARGS.to_string(), leading_args.join(" "));
    inputs.insert(INPUT_TRAILING_ARGS.to_string(), trailing_args.join(" "));
    inputs.extend(step_option_input_bindings(step.tool, &step.options));

    let mut outputs = BTreeMap::new();
    let mut pending_variant_updates = Vec::new();
    let mut output_configs = Vec::new();
    let mut link_variants: BTreeMap<String, Option<String>> = BTreeMap::new();

    for mapping in mappings {
        let output_variant_config =
            decode_yt_dlp_output_variant_config(&mapping.output, &step.output_variants)?;
        output_configs.push(output_variant_config.clone());

        let output_binding = resolve_step_output_binding(
            step.tool,
            &step.output_variants,
            &mapping.output,
            ffmpeg_slot_limits,
        )?;

        let output_name = output_binding.output_name;

        if output_variant_config.kind == YtDlpOutputKind::Links {
            link_variants.insert(mapping.output.clone(), output_binding.zip_member.clone());
        }

        outputs.insert(
            output_name.clone(),
            OutputCaptureSpec { name: output_name.clone(), capture: String::new(), save: true },
        );

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

    let step_id_for_link_cleanup = step_id.clone();
    workflow.steps.push(WorkflowStepSpec {
        id: step_id,
        tool: tool_id.to_string(),
        inputs,
        outputs,
        max_retries: 0,
        depends_on: Vec::new(),
    });

    if !link_variants.is_empty() {
        let archive_tool_id = resolve_builtin_tool_id(machine, "archive", "1.0.0")?;
        let transform_step_id = format!("{step_index}-1-archive-links-cleanup");

        let mut transform_inputs = BTreeMap::new();
        transform_inputs.insert(
            "content".to_string(),
            format!(
                "${{step_output.{}.{}}}",
                step_id_for_link_cleanup, OUTPUT_YT_DLP_LINK_ARTIFACTS,
            ),
        );
        transform_inputs.insert("action".to_string(), "transform".to_string());
        transform_inputs.insert("filter".to_string(), "*.desktop".to_string());
        transform_inputs.insert("mode".to_string(), "text".to_string());
        transform_inputs.insert("find_0".to_string(), "downloads/(.+)__mediapm__".to_string());
        transform_inputs.insert("replace_0".to_string(), "$1".to_string());

        let transform_outputs = BTreeMap::from([(
            "result".to_string(),
            OutputCaptureSpec { name: "result".to_string(), capture: String::new(), save: true },
        )]);

        workflow.steps.push(WorkflowStepSpec {
            id: transform_step_id.clone(),
            tool: archive_tool_id,
            inputs: transform_inputs,
            outputs: transform_outputs,
            max_retries: 0,
            depends_on: vec![step_id_for_link_cleanup],
        });

        for (output_variant, producer) in pending_variant_updates {
            if let Some(zip_member) = link_variants.get(&output_variant) {
                variant_producers.insert(
                    output_variant,
                    VariantProducer::StepOutput {
                        step_id: transform_step_id.clone(),
                        output_name: "result".to_string(),
                        zip_member: zip_member.clone(),
                        extension: None,
                    },
                );
            } else {
                variant_producers.insert(output_variant, producer);
            }
        }
    } else {
        for (output_variant, producer) in pending_variant_updates {
            variant_producers.insert(output_variant, producer);
        }
    }

    Ok(())
}
