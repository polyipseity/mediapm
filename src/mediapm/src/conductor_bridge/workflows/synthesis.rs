//! Workflow step synthesis for managed media sources.
//!
//! This module expands ordered `media.<id>.steps` declarations into conductor
//! workflow steps, keeping per-step iteration logic separate from the
//! workflow-plan construction in [`super`].

use std::collections::{BTreeMap, BTreeSet};

use mediapm_conductor::{InputBinding, MachineNickelDocument, WorkflowSpec, WorkflowStepSpec};

use crate::config::{
    ManagedWorkflowStepState, MediaSourceSpec, MediaStep, MediaStepTool, ResolvedStepVariantFlow,
    ToolRequirement, expand_variant_selectors, media_source_uri, resolve_step_variant_flow,
};
use crate::error::MediaPmError;
use crate::lockfile::MediaLockFile;

use super::yt_dlp_inputs::{resolve_step_output_binding, step_output_policy_overrides};
use super::{
    FfmpegSlotLimits, IMPORT_KIND_CAS_HASH, INPUT_CONTENT, INPUT_IMPORT_HASH, INPUT_IMPORT_KIND,
    INPUT_LEADING_ARGS, INPUT_SOURCE_URL, INPUT_TRAILING_ARGS, VariantProducer,
    explicit_media_step_config_snapshot, extract_step_list_args, find_matching_step_state_index,
    fresh_impure_timestamp, matched_state_requires_refresh, media_step_id,
    preserve_existing_generated_step_tools, resolve_input_variant_producer,
    resolve_logical_tool_requirement, resolve_step_tool_id, step_option_input_bindings,
    step_option_scalar,
};

/// Creates ordered workflow steps from unified media-step declarations.
///
/// This synthesis pass also updates per-step refresh state in
/// `lock.workflow_states`:
/// - explicit config snapshots are persisted from user-authored step values,
/// - for each step, explicit config matching forward-scans for the first
///   exact match after the last matched state index,
/// - timestamps are checked only for exactly matched states,
/// - unchanged steps with existing timestamps preserve prior immutable tool ids
///   from the existing machine workflow when possible.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "step synthesis requires explicit immutable and mutable workflow context inputs"
)]
pub(super) fn synthesize_media_steps(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    source: &MediaSourceSpec,
    lock: &mut MediaLockFile,
    machine: &MachineNickelDocument,
    tool_requirements: &BTreeMap<String, ToolRequirement>,
    existing_workflow: Option<&WorkflowSpec>,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    let existing_media_states = lock.workflow_states.get(media_id).cloned().unwrap_or_default();
    let mut next_media_states = Vec::with_capacity(source.steps.len());
    let mut next_match_search_start = 0usize;

    for (step_index, step) in source.steps.iter().enumerate() {
        let explicit_step_config = explicit_media_step_config_snapshot(step)?;
        let matched_state_index = find_matching_step_state_index(
            &existing_media_states,
            next_match_search_start,
            &explicit_step_config,
        );
        if let Some(matched_index) = matched_state_index {
            next_match_search_start = matched_index.saturating_add(1);
        }
        let existing_step_state =
            matched_state_index.and_then(|index| existing_media_states.get(index)).cloned();
        let mut requires_refresh = matched_state_requires_refresh(existing_step_state.as_ref());

        let tool_id = resolve_step_tool_id(lock, machine, step.tool)?;
        let producer_snapshot = variant_producers.clone();
        let mut resolved_step = step.clone();
        if !step.tool.is_source_ingest_tool() {
            let available_variants =
                producer_snapshot.keys().cloned().collect::<BTreeSet<String>>();
            resolved_step.input_variants = expand_variant_selectors(
                &step.input_variants,
                &available_variants,
            )
            .map_err(|reason| {
                MediaPmError::Workflow(format!("media '{media_id}' step #{step_index} {reason}"))
            })?;
        }

        let mappings = resolve_step_variant_flow(&resolved_step).map_err(|reason| {
            MediaPmError::Workflow(format!("media '{media_id}' step #{step_index} {reason}"))
        })?;

        let mut duplicate_outputs = BTreeSet::new();
        for mapping in &mappings {
            if !duplicate_outputs.insert(mapping.output.clone()) {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{step_index} maps multiple inputs to output variant '{}'",
                    mapping.output
                )));
            }
        }

        let generated_start = workflow.steps.len();

        if matches!(step.tool, MediaStepTool::MediaTagger) {
            synthesize_media_tagger_step_pair(
                workflow,
                media_id,
                step_index,
                &resolved_step,
                &mappings,
                lock,
                machine,
                resolve_logical_tool_requirement(tool_requirements, "media-tagger"),
                &producer_snapshot,
                variant_producers,
                ffmpeg_slot_limits,
            )?;
        } else if matches!(step.tool, MediaStepTool::Rsgain) {
            synthesize_rsgain_step_chain(
                workflow,
                media_id,
                step_index,
                &resolved_step,
                &mappings,
                lock,
                machine,
                resolve_logical_tool_requirement(tool_requirements, "rsgain"),
                &producer_snapshot,
                variant_producers,
                ffmpeg_slot_limits,
            )?;
        } else if matches!(step.tool, MediaStepTool::YtDlp) {
            synthesize_yt_dlp_step(
                workflow,
                media_id,
                source,
                step_index,
                &resolved_step,
                &mappings,
                &tool_id,
                variant_producers,
                ffmpeg_slot_limits,
            )?;
        } else if matches!(step.tool, MediaStepTool::Ffmpeg) {
            synthesize_ffmpeg_step(
                workflow,
                media_id,
                step_index,
                &resolved_step,
                &mappings,
                &tool_id,
                &producer_snapshot,
                variant_producers,
                ffmpeg_slot_limits,
            )?;
        } else {
            let mut pending_variant_updates = Vec::new();

            for (mapping_index, mapping) in mappings.iter().enumerate() {
                let step_id = media_step_id(step_index, mapping_index, step.tool, mapping);
                let mut depends_on = Vec::new();
                let mut inputs = BTreeMap::new();

                if step.tool.is_online_media_downloader() {
                    let source_uri = step_option_scalar(&resolved_step, "uri")
                        .map_or_else(|| media_source_uri(media_id, source), ToString::to_string);
                    inputs.insert(INPUT_SOURCE_URL.to_string(), InputBinding::String(source_uri));
                } else if matches!(step.tool, MediaStepTool::Import) {
                    let kind = step_option_scalar(&resolved_step, INPUT_IMPORT_KIND)
                        .map_or_else(|| IMPORT_KIND_CAS_HASH.to_string(), ToString::to_string);
                    let hash = step_option_scalar(&resolved_step, INPUT_IMPORT_HASH)
                        .map(ToString::to_string)
                        .ok_or_else(|| {
                            MediaPmError::Workflow(format!(
                                "media '{media_id}' step #{step_index} uses tool '{}' and must define options.hash",
                                step.tool.as_str()
                            ))
                        })?;
                    inputs.insert(INPUT_IMPORT_KIND.to_string(), InputBinding::String(kind));
                    inputs.insert(INPUT_IMPORT_HASH.to_string(), InputBinding::String(hash));
                } else {
                    let Some(producer) =
                        resolve_input_variant_producer(&mapping.input, &producer_snapshot)
                    else {
                        return Err(MediaPmError::Workflow(format!(
                            "media '{media_id}' step #{step_index} references unknown input variant '{}'",
                            mapping.input
                        )));
                    };
                    let (input_binding, dependency) = producer.to_binding();
                    inputs.insert(INPUT_CONTENT.to_string(), input_binding);
                    if let Some(step_dependency) = dependency {
                        depends_on.push(step_dependency);
                    }
                }

                if !matches!(step.tool, MediaStepTool::Import) {
                    let (leading_args, trailing_args) = extract_step_list_args(&resolved_step);

                    let option_inputs =
                        step_option_input_bindings(step.tool, &resolved_step.options);

                    inputs.insert(
                        INPUT_LEADING_ARGS.to_string(),
                        InputBinding::StringList(leading_args),
                    );
                    inputs.insert(
                        INPUT_TRAILING_ARGS.to_string(),
                        InputBinding::StringList(trailing_args),
                    );
                    inputs.extend(option_inputs);
                }

                let outputs = step_output_policy_overrides(
                    step.tool,
                    &resolved_step.output_variants,
                    &mapping.output,
                    ffmpeg_slot_limits,
                )?;

                let output_binding = resolve_step_output_binding(
                    step.tool,
                    &resolved_step.output_variants,
                    &mapping.output,
                    ffmpeg_slot_limits,
                )?;

                workflow.steps.push(WorkflowStepSpec {
                    id: step_id.clone(),
                    tool: tool_id.clone(),
                    inputs,
                    depends_on,
                    outputs,
                });

                pending_variant_updates.push((
                    mapping.output.clone(),
                    VariantProducer::StepOutput {
                        step_id,
                        output_name: output_binding.output_name,
                        zip_member: output_binding.zip_member,
                        extension: None,
                    },
                ));
            }

            for (output_variant, producer) in pending_variant_updates {
                variant_producers.insert(output_variant, producer);
            }
        }

        if !requires_refresh {
            let preserved_all_tools = preserve_existing_generated_step_tools(
                workflow,
                generated_start,
                existing_workflow,
            );
            if !preserved_all_tools {
                requires_refresh = true;
            }
        }

        let impure_timestamp = if requires_refresh {
            Some(fresh_impure_timestamp())
        } else {
            existing_step_state.and_then(|state| state.impure_timestamp)
        };

        next_media_states.push(ManagedWorkflowStepState {
            explicit_config: explicit_step_config,
            impure_timestamp,
        });
    }

    if next_media_states.is_empty() {
        lock.workflow_states.remove(media_id);
    } else {
        lock.workflow_states.insert(media_id.to_string(), next_media_states);
    }

    Ok(())
}

/// Expands one yt-dlp step into one multi-output workflow step.
#[expect(
    clippy::too_many_arguments,
    reason = "yt-dlp synthesis requires explicit workflow, source, mapping, and slot-limit context"
)]
fn synthesize_yt_dlp_step(
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
    super::yt_dlp::synthesize_yt_dlp_step(
        workflow,
        media_id,
        source,
        step_index,
        step,
        mappings,
        tool_id,
        variant_producers,
        ffmpeg_slot_limits,
    )
}

/// Expands one ffmpeg step with ordered indexed inputs/outputs.
#[expect(
    clippy::too_many_arguments,
    reason = "ffmpeg synthesis requires explicit workflow and producer state to preserve ordering invariants"
)]
fn synthesize_ffmpeg_step(
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
    super::ffmpeg::synthesize_ffmpeg_step(
        workflow,
        media_id,
        step_index,
        step,
        mappings,
        tool_id,
        producer_snapshot,
        variant_producers,
        ffmpeg_slot_limits,
    )
}

/// Expands one `rsgain` config step into a deterministic metadata pipeline:
///
/// 1. `ffmpeg` extracts all audio channels from the input media as an
///    extension-selected intermediate (`flac` by default, or caller-specified
///    `options.input_extension`), using codec-copy mode for non-`flac`
///    targets to avoid unnecessary transcoding,
/// 2. `rsgain` computes/writes replaygain tags on the extracted audio,
/// 3. `ffmpeg` exports tagged audio metadata to `ffmetadata`,
/// 4. `sd` normalizes replaygain key prefixes in ffmetadata,
/// 5. `sd` normalizes opus/R128 key prefixes in ffmetadata,
/// 6. `ffmpeg` merges normalized ffmetadata back into the original media.
#[expect(
    clippy::too_many_arguments,
    reason = "rsgain chain synthesis requires explicit lock/machine/dependency and producer state context"
)]
fn synthesize_rsgain_step_chain(
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
    super::rsgain::synthesize_rsgain_step_chain(
        workflow,
        media_id,
        step_index,
        step,
        mappings,
        lock,
        machine,
        logical_tool_requirement,
        producer_snapshot,
        variant_producers,
        ffmpeg_slot_limits,
    )
}

/// Expands one `media-tagger` config step into metadata + ffmpeg-apply steps.
#[expect(
    clippy::too_many_arguments,
    reason = "media-tagger synthesis requires explicit dependency and producer state context"
)]
fn synthesize_media_tagger_step_pair(
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
    super::media_tagger::synthesize_media_tagger_step_pair(
        workflow,
        media_id,
        step_index,
        step,
        mappings,
        lock,
        machine,
        media_tagger_requirement,
        producer_snapshot,
        variant_producers,
        ffmpeg_slot_limits,
    )
}
