//! `ffmpeg` workflow-step synthesis.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_conductor::{InputBinding, OutputPolicy, WorkflowSpec, WorkflowStepSpec};

use crate::config::{
    DecodedOutputVariantConfig, MediaStep, MediaStepTool, ResolvedStepVariantFlow,
    decode_output_variant_config, decode_output_variant_policy,
};
use crate::error::MediaPmError;

use super::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS, DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, FfmpegSlotLimits,
    INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS, VariantProducer, conductor_output_save_mode,
    extract_step_list_args, ffmpeg_input_content_name, ffmpeg_output_capture_name,
    ffmpeg_output_path_input_name, ffmpeg_output_path_with_extension, normalize_output_extension,
    resolve_input_variant_producer, resolved_ffmpeg_family_output_extension,
    step_option_input_bindings,
};

/// Canonicalizes one ffmpeg container/muxer selector to a stable value.
///
/// `mediapm` accepts common extension-style aliases in `options.container`
/// and from extension-derived inference. This helper rewrites aliases to
/// ffmpeg muxer names accepted by `-f` so extension-only workflows remain
/// executable without redundant explicit container declarations.
fn canonicalize_ffmpeg_container(container: &str) -> String {
    match container.trim().to_ascii_lowercase().as_str() {
        // Matroska-family extension aliases.
        "mkv" | "mka" | "mks" | "mk3d" => "matroska",
        // ISO BMFF extension aliases that ffmpeg typically muxes via `mp4`.
        "m4a" | "m4v" | "m4b" | "f4v" => "mp4",
        // QuickTime extension alias.
        "qt" => "mov",
        // MPEG-TS extension aliases.
        "ts" | "m2ts" | "mts" => "mpegts",
        // ASF-family extension aliases.
        "wmv" | "wma" => "asf",
        // OGG-family extension aliases.
        "oga" | "ogv" => "ogg",
        // Smooth-streaming extension aliases.
        "ism" | "isma" => "ismv",
        other => other,
    }
    .to_string()
}

/// Expands one ffmpeg step with ordered indexed inputs/outputs.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "ffmpeg synthesis requires explicit workflow and producer state to preserve ordering invariants"
)]
pub(super) fn synthesize_ffmpeg_step(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    tool_id: &str,
    producer_snapshot: &BTreeMap<String, VariantProducer>,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    let step_id = super::ffmpeg_step_id(step_index);
    let mut depends_on = Vec::new();
    let mut seen_depends_on = BTreeSet::new();
    let mut inputs = BTreeMap::new();

    for (input_index, input_variant) in step.input_variants.iter().enumerate() {
        if input_index >= ffmpeg_slot_limits.max_input_slots {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} declares {} ffmpeg input variants but maximum supported is {}; reduce input_variants fan-out or increase tools.ffmpeg.max_input_slots (default {DEFAULT_FFMPEG_MAX_INPUT_SLOTS})",
                step.input_variants.len(),
                ffmpeg_slot_limits.max_input_slots,
            )));
        }

        let Some(producer) = resolve_input_variant_producer(input_variant, producer_snapshot)
        else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} references unknown input variant '{input_variant}'"
            )));
        };

        let (input_binding, dependency) = producer.to_binding();
        inputs.insert(ffmpeg_input_content_name(input_index), input_binding);
        if let Some(step_dependency) = dependency
            && seen_depends_on.insert(step_dependency.clone())
        {
            depends_on.push(step_dependency);
        }
    }

    let (leading_args, trailing_args) = extract_step_list_args(step);
    let option_inputs = step_option_input_bindings(step.tool, &step.options);
    inputs.insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(leading_args));
    inputs.insert(INPUT_TRAILING_ARGS.to_string(), InputBinding::StringList(trailing_args));
    inputs.extend(option_inputs);
    if let Some(InputBinding::String(container)) = inputs.get_mut("container") {
        *container = canonicalize_ffmpeg_container(container);
    }
    if !inputs.contains_key("movflags") {
        inputs.insert("movflags".to_string(), InputBinding::String(String::new()));
    }

    let mut outputs = BTreeMap::new();
    let mut pending_variant_updates = Vec::new();
    let mut seen_output_indexes = BTreeSet::new();
    let mut inferred_primary_container = None::<String>;

    for mapping in mappings {
        let variant_value = step.output_variants.get(&mapping.output).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} is missing output variant '{}'",
                mapping.output
            ))
        })?;

        let decoded =
            decode_output_variant_config(MediaStepTool::Ffmpeg, &mapping.output, variant_value)
                .map_err(MediaPmError::Workflow)?;
        let DecodedOutputVariantConfig::Generic(config) = decoded else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} output variant '{}' must decode as ffmpeg generic output config",
                mapping.output
            )));
        };

        let output_index_u32 = config.idx.ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} output variant '{}' must define ffmpeg idx",
                mapping.output
            ))
        })?;
        let output_index = usize::try_from(output_index_u32).map_err(|_| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} output variant '{}' has unsupported ffmpeg idx '{}': expected a non-negative integer",
                mapping.output, output_index_u32
            ))
        })?;

        if output_index >= ffmpeg_slot_limits.max_output_slots {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} output variant '{}' uses ffmpeg idx '{}' but maximum supported idx is {}; reduce output idx usage or increase tools.ffmpeg.max_output_slots (default {DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS})",
                mapping.output,
                output_index_u32,
                ffmpeg_slot_limits.max_output_slots - 1
            )));
        }

        if !seen_output_indexes.insert(output_index) {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} assigns duplicate ffmpeg idx '{output_index_u32}' across output_variants"
            )));
        }

        let Some(input_producer) =
            resolve_input_variant_producer(&mapping.input, producer_snapshot)
        else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} references unknown input variant '{}'",
                mapping.input
            )));
        };

        let resolved_extension = match config.extension.as_deref().map(str::trim) {
            Some("") => None,
            Some(_) => normalize_output_extension(config.extension.as_deref()),
            None => input_producer
                .output_extension()
                .map(ToString::to_string)
                .or_else(|| resolved_ffmpeg_family_output_extension(None)),
        };

        if output_index == 0
            && inferred_primary_container.is_none()
            && let Some(extension) = resolved_extension.as_deref()
        {
            inferred_primary_container = Some(extension.to_string());
        }

        let output_name = ffmpeg_output_capture_name(output_index);
        inputs.insert(
            ffmpeg_output_path_input_name(output_index),
            InputBinding::String(ffmpeg_output_path_with_extension(
                output_index,
                resolved_extension.as_deref(),
            )),
        );

        let policy =
            decode_output_variant_policy(MediaStepTool::Ffmpeg, &mapping.output, variant_value)
                .map_err(MediaPmError::Workflow)?;
        outputs.insert(
            output_name.clone(),
            OutputPolicy { save: conductor_output_save_mode(policy.save) },
        );

        pending_variant_updates.push((
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: step_id.clone(),
                output_name,
                zip_member: config.zip_member,
                extension: resolved_extension,
            },
        ));
    }

    let should_infer_container = match inputs.get("container") {
        Some(InputBinding::String(container)) => container.trim().is_empty(),
        _ => true,
    };
    if should_infer_container && let Some(inferred_container) = inferred_primary_container {
        inputs.insert(
            "container".to_string(),
            InputBinding::String(canonicalize_ffmpeg_container(&inferred_container)),
        );
    }

    workflow.steps.push(WorkflowStepSpec {
        id: step_id,
        tool: tool_id.to_string(),
        inputs,
        depends_on,
        outputs,
    });

    for (output_variant, producer) in pending_variant_updates {
        variant_producers.insert(output_variant, producer);
    }

    Ok(())
}
