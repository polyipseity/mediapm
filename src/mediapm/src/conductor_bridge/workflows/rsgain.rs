//! Managed `rsgain` chain synthesis.

use std::collections::BTreeMap;

use mediapm_conductor::{
    InputBinding, MachineNickelDocument, OutputPolicy, WorkflowSpec, WorkflowStepSpec,
};

use crate::conductor_bridge::tool_runtime::SUPPORTED_RSGAIN_INPUT_EXTENSIONS;
use crate::config::{
    DecodedOutputVariantConfig, MediaStep, MediaStepTool, ResolvedStepVariantFlow, ToolRequirement,
    decode_output_variant_config, decode_output_variant_policy,
};
use crate::error::MediaPmError;
use crate::lockfile::MediaLockFile;

use super::{
    FfmpegSlotLimits, INPUT_CONTENT, INPUT_FFMETADATA_CONTENT, INPUT_LEADING_ARGS,
    INPUT_SD_PATTERN, INPUT_SD_REPLACEMENT, INPUT_TRAILING_ARGS, OUTPUT_CONTENT,
    RSGAIN_APPLY_STEP_OFFSET, RSGAIN_EXPANDED_STEPS_PER_MAPPING, VariantProducer,
    conductor_output_save_mode, expanded_step_index_for_mapping, extract_step_list_args,
    ffmpeg_input_content_name, ffmpeg_output_capture_name, ffmpeg_output_path_input_name,
    ffmpeg_output_path_with_extension, media_step_id, normalize_output_extension,
    resolve_input_variant_producer, resolve_selected_dependency_tool_id,
    resolve_step_output_binding, resolve_step_tool_id, resolved_ffmpeg_family_output_extension,
    step_option_input_bindings, step_option_scalar,
};

/// Resolves managed rsgain extraction extension.
///
/// Selection order:
/// 1. explicit `options.input_extension` when provided,
/// 2. inferred upstream produced extension when `rsgain 3.7` can tag it,
/// 3. managed default `flac` for broad compatibility.
///
/// Non-FLAC extensions keep extraction in codec-copy mode to avoid unnecessary
/// re-encoding before `ReplayGain` analysis.
fn resolve_rsgain_input_extension(
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    input_producer: &VariantProducer,
) -> Result<String, MediaPmError> {
    let configured = step_option_scalar(step, "input_extension")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);

    let resolved = configured
        .or_else(|| {
            input_producer.output_extension().and_then(|extension| {
                SUPPORTED_RSGAIN_INPUT_EXTENSIONS
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(extension))
                    .then(|| extension.to_string())
            })
        })
        .unwrap_or_else(|| "flac".to_string());
    if SUPPORTED_RSGAIN_INPUT_EXTENSIONS
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(&resolved))
    {
        return Ok(resolved);
    }

    Err(MediaPmError::Workflow(format!(
        "media '{media_id}' step #{step_index} options.input_extension='{resolved}' is unsupported for managed rsgain; supported values are: {}",
        SUPPORTED_RSGAIN_INPUT_EXTENSIONS.join(", "),
    )))
}

/// Resolves the effective output extension for one managed rsgain apply step.
///
/// When the output variant omits an explicit extension, rsgain preserves the
/// upstream produced container extension when known. This keeps audio-only
/// flows on already-supported containers such as `.m4a` or `.mp3` and avoids
/// unnecessary remuxing into generic `.mkv` outputs.
#[must_use]
fn resolve_rsgain_output_extension(
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

/// Expands one `rsgain` config step into a deterministic metadata pipeline.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "rsgain chain synthesis requires explicit lock/machine/dependency and producer state context"
)]
pub(super) fn synthesize_rsgain_step_chain(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
    logical_tool_requirement: Option<&ToolRequirement>,
    producer_snapshot: &BTreeMap<String, VariantProducer>,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    let ffmpeg_tool_id = resolve_selected_dependency_tool_id(
        "rsgain",
        "ffmpeg",
        logical_tool_requirement.and_then(ToolRequirement::normalized_ffmpeg_selector),
        lock,
        machine,
    )?;
    let rsgain_tool_id = resolve_step_tool_id(lock, machine, MediaStepTool::Rsgain)?;
    let sd_tool_id = resolve_selected_dependency_tool_id(
        "rsgain",
        "sd",
        logical_tool_requirement.and_then(ToolRequirement::normalized_sd_selector),
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
        let rsgain_input_extension =
            resolve_rsgain_input_extension(media_id, step_index, step, producer)?;
        let extract_codec_copy = !rsgain_input_extension.eq_ignore_ascii_case("flac");

        let (input_binding, input_dependency) = producer.to_binding();
        let extract_step_id = format!(
            "{}-ffmpeg-extract",
            media_step_id(
                step_index,
                expanded_step_index_for_mapping(
                    mapping_index,
                    RSGAIN_EXPANDED_STEPS_PER_MAPPING,
                    0
                ),
                step.tool,
                mapping,
            )
        );
        let rsgain_step_id = format!(
            "{}-rsgain",
            media_step_id(
                step_index,
                expanded_step_index_for_mapping(
                    mapping_index,
                    RSGAIN_EXPANDED_STEPS_PER_MAPPING,
                    1
                ),
                step.tool,
                mapping,
            )
        );
        let metadata_export_step_id = format!(
            "{}-ffmpeg-export-metadata",
            media_step_id(
                step_index,
                expanded_step_index_for_mapping(
                    mapping_index,
                    RSGAIN_EXPANDED_STEPS_PER_MAPPING,
                    2
                ),
                step.tool,
                mapping,
            )
        );
        let metadata_rewrite_step_id = format!(
            "{}-sd-rewrite-metadata",
            media_step_id(
                step_index,
                expanded_step_index_for_mapping(
                    mapping_index,
                    RSGAIN_EXPANDED_STEPS_PER_MAPPING,
                    3
                ),
                step.tool,
                mapping,
            )
        );
        let metadata_r128_rewrite_step_id = format!(
            "{}-sd-rewrite-r128-metadata",
            media_step_id(
                step_index,
                expanded_step_index_for_mapping(
                    mapping_index,
                    RSGAIN_EXPANDED_STEPS_PER_MAPPING,
                    4
                ),
                step.tool,
                mapping,
            )
        );
        let apply_step_id = format!(
            "{}-ffmpeg-apply",
            media_step_id(
                step_index,
                expanded_step_index_for_mapping(
                    mapping_index,
                    RSGAIN_EXPANDED_STEPS_PER_MAPPING,
                    RSGAIN_APPLY_STEP_OFFSET,
                ),
                step.tool,
                mapping,
            )
        );

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
            resolve_rsgain_output_extension(output_config.extension.as_deref(), producer);

        let mut extract_inputs = BTreeMap::new();
        let mut extract_depends_on = Vec::new();
        extract_inputs.insert(ffmpeg_input_content_name(0), input_binding.clone());
        extract_inputs.insert(
            ffmpeg_output_path_input_name(0),
            InputBinding::String(ffmpeg_output_path_with_extension(
                0,
                Some(&rsgain_input_extension),
            )),
        );
        extract_inputs.insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(Vec::new()));
        extract_inputs.insert(
            INPUT_TRAILING_ARGS.to_string(),
            InputBinding::StringList(vec!["-map".to_string(), "0:a?".to_string()]),
        );
        extract_inputs
            .insert("codec_copy".to_string(), InputBinding::String(extract_codec_copy.to_string()));
        extract_inputs.insert("vn".to_string(), InputBinding::String("true".to_string()));
        extract_inputs.insert("movflags".to_string(), InputBinding::String(String::new()));
        extract_inputs.insert("map_metadata".to_string(), InputBinding::String("-1".to_string()));
        extract_inputs.insert("map_chapters".to_string(), InputBinding::String("-1".to_string()));

        if let Some(step_dependency) = input_dependency.clone() {
            extract_depends_on.push(step_dependency);
        }

        workflow.steps.push(WorkflowStepSpec {
            id: extract_step_id.clone(),
            tool: ffmpeg_tool_id.clone(),
            inputs: extract_inputs,
            depends_on: extract_depends_on,
            outputs: BTreeMap::from([(ffmpeg_output_capture_name(0), OutputPolicy::default())]),
        });

        let (leading_args, trailing_args) = extract_step_list_args(media_id, step_index, step)?;
        let option_inputs = step_option_input_bindings(step.tool, &step.options)?;

        let mut rsgain_inputs = BTreeMap::new();
        rsgain_inputs.insert(
            INPUT_CONTENT.to_string(),
            InputBinding::String(format!(
                "${{step_output.{extract_step_id}.{}}}",
                ffmpeg_output_capture_name(0)
            )),
        );
        rsgain_inputs
            .insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(leading_args));
        rsgain_inputs
            .insert(INPUT_TRAILING_ARGS.to_string(), InputBinding::StringList(trailing_args));
        rsgain_inputs.extend(option_inputs);
        // Keep managed ReplayGain synthesis in single-track mode by default.
        // Callers can still opt into album families explicitly through
        // step-level options.
        rsgain_inputs.insert(
            "input_extension".to_string(),
            InputBinding::String(rsgain_input_extension.clone()),
        );

        workflow.steps.push(WorkflowStepSpec {
            id: rsgain_step_id.clone(),
            tool: rsgain_tool_id.clone(),
            inputs: rsgain_inputs,
            depends_on: vec![extract_step_id.clone()],
            outputs: BTreeMap::from([(OUTPUT_CONTENT.to_string(), OutputPolicy::default())]),
        });

        let mut metadata_export_inputs = BTreeMap::new();
        metadata_export_inputs.insert(
            ffmpeg_input_content_name(0),
            InputBinding::String(format!("${{step_output.{rsgain_step_id}.{OUTPUT_CONTENT}}}")),
        );
        metadata_export_inputs.insert(
            ffmpeg_output_path_input_name(0),
            InputBinding::String(ffmpeg_output_path_with_extension(0, Some("ffmeta"))),
        );
        metadata_export_inputs
            .insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(Vec::new()));
        metadata_export_inputs.insert(
            INPUT_TRAILING_ARGS.to_string(),
            InputBinding::StringList(vec!["-f".to_string(), "ffmetadata".to_string()]),
        );
        metadata_export_inputs
            .insert("codec_copy".to_string(), InputBinding::String("true".to_string()));
        metadata_export_inputs.insert("movflags".to_string(), InputBinding::String(String::new()));

        workflow.steps.push(WorkflowStepSpec {
            id: metadata_export_step_id.clone(),
            tool: ffmpeg_tool_id.clone(),
            inputs: metadata_export_inputs,
            depends_on: vec![rsgain_step_id.clone()],
            outputs: BTreeMap::from([(ffmpeg_output_capture_name(0), OutputPolicy::default())]),
        });

        let mut metadata_rewrite_inputs = BTreeMap::new();
        metadata_rewrite_inputs.insert(
            INPUT_CONTENT.to_string(),
            InputBinding::String(format!(
                "${{step_output.{metadata_export_step_id}.{}}}",
                ffmpeg_output_capture_name(0)
            )),
        );
        metadata_rewrite_inputs
            .insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(Vec::new()));
        metadata_rewrite_inputs
            .insert(INPUT_TRAILING_ARGS.to_string(), InputBinding::StringList(Vec::new()));
        metadata_rewrite_inputs.insert(
            INPUT_SD_PATTERN.to_string(),
            InputBinding::String("(?i)REPLAYGAIN_".to_string()),
        );
        metadata_rewrite_inputs.insert(
            INPUT_SD_REPLACEMENT.to_string(),
            InputBinding::String("replaygain_".to_string()),
        );

        workflow.steps.push(WorkflowStepSpec {
            id: metadata_rewrite_step_id.clone(),
            tool: sd_tool_id.clone(),
            inputs: metadata_rewrite_inputs,
            depends_on: vec![metadata_export_step_id.clone()],
            outputs: BTreeMap::from([(OUTPUT_CONTENT.to_string(), OutputPolicy::default())]),
        });

        let mut metadata_r128_rewrite_inputs = BTreeMap::new();
        metadata_r128_rewrite_inputs.insert(
            INPUT_CONTENT.to_string(),
            InputBinding::String(format!(
                "${{step_output.{metadata_rewrite_step_id}.{OUTPUT_CONTENT}}}"
            )),
        );
        metadata_r128_rewrite_inputs
            .insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(Vec::new()));
        metadata_r128_rewrite_inputs
            .insert(INPUT_TRAILING_ARGS.to_string(), InputBinding::StringList(Vec::new()));
        metadata_r128_rewrite_inputs
            .insert(INPUT_SD_PATTERN.to_string(), InputBinding::String("(?i)R128_".to_string()));
        metadata_r128_rewrite_inputs
            .insert(INPUT_SD_REPLACEMENT.to_string(), InputBinding::String("R128_".to_string()));

        workflow.steps.push(WorkflowStepSpec {
            id: metadata_r128_rewrite_step_id.clone(),
            tool: sd_tool_id.clone(),
            inputs: metadata_r128_rewrite_inputs,
            depends_on: vec![metadata_rewrite_step_id.clone()],
            outputs: BTreeMap::from([(OUTPUT_CONTENT.to_string(), OutputPolicy::default())]),
        });
        let mut apply_inputs = BTreeMap::new();
        let mut apply_depends_on = vec![metadata_r128_rewrite_step_id.clone()];
        apply_inputs.insert(ffmpeg_input_content_name(0), input_binding);
        apply_inputs.insert(
            INPUT_FFMETADATA_CONTENT.to_string(),
            InputBinding::String(format!(
                "${{step_output.{metadata_r128_rewrite_step_id}.{OUTPUT_CONTENT}}}"
            )),
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
            InputBinding::StringList(vec![
                "-map".to_string(),
                "0".to_string(),
                "-map_metadata".to_string(),
                "1".to_string(),
            ]),
        );
        apply_inputs.insert(
            "metadata".to_string(),
            InputBinding::String("replaygain_reference_loudness=89.0 dB".to_string()),
        );
        apply_inputs.insert("map_metadata".to_string(), InputBinding::String("0".to_string()));
        apply_inputs.insert("codec_copy".to_string(), InputBinding::String("true".to_string()));
        apply_inputs.insert("movflags".to_string(), InputBinding::String(String::new()));

        if let Some(step_dependency) = input_dependency {
            apply_depends_on.push(step_dependency);
        }

        workflow.steps.push(WorkflowStepSpec {
            id: apply_step_id.clone(),
            tool: ffmpeg_tool_id.clone(),
            inputs: apply_inputs,
            depends_on: apply_depends_on,
            outputs: BTreeMap::from([(
                ffmpeg_output_capture_name(0),
                OutputPolicy { save: conductor_output_save_mode(output_policy.save) },
            )]),
        });

        variant_producers.insert(
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: apply_step_id,
                output_name: ffmpeg_output_capture_name(0),
                zip_member: output_binding.zip_member,
                extension: apply_output_extension,
            },
        );
    }

    Ok(())
}
