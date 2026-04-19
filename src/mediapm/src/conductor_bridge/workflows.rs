//! Per-media workflow synthesis for phase-3 `mediapm` documents.
//!
//! This module translates ordered `media.<id>.steps` declarations into managed
//! conductor workflows so each media id maps to exactly one workflow.
//! Variant-flow dependencies are expressed with explicit `${step_output...}`
//! bindings plus matching `depends_on` edges, allowing independent branches to
//! execute as soon as their producer data is ready.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use mediapm_cas::Hash;
use mediapm_conductor::{
    AddExternalDataOptions, ExternalContentRef, InputBinding, MachineNickelDocument, OutputPolicy,
    ToolKindSpec, WorkflowSpec, WorkflowStepSpec,
};
use serde_json::Value;

use crate::config::{
    DecodedOutputVariantConfig, MediaPmDocument, MediaSourceSpec, MediaStep, MediaStepTool,
    ResolvedStepVariantFlow, TransformInputValue, YtDlpOutputKind, YtDlpOutputVariantConfig,
    decode_output_variant_config, decode_output_variant_policy, media_source_uri,
    normalize_selector_compare_value, resolve_step_variant_flow,
};
use crate::error::MediaPmError;
use crate::lockfile::MediaLockFile;
use crate::paths::MediaPmPaths;

use super::documents::{load_machine_document, save_machine_document};
use super::tool_runtime::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS, DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, FfmpegSlotLimits,
    ffmpeg_input_content_name, ffmpeg_output_capture_name, ffmpeg_output_file_path,
    ffmpeg_output_path_input_name, resolve_ffmpeg_slot_limits,
};

/// Prefix for default `mediapm`-managed workflow ids in machine documents.
const MANAGED_WORKFLOW_PREFIX: &str = "mediapm.media.";
/// Prefix for `mediapm`-managed local-source external-data descriptions.
const MANAGED_EXTERNAL_DESCRIPTION_PREFIX: &str = "managed local variant source for media";

/// Output name exposed by generated executable tool contracts.
const OUTPUT_CONTENT: &str = "output_content";
/// Output name exposing full sandbox artifact bundles.
const OUTPUT_SANDBOX_ARTIFACTS: &str = "sandbox_artifacts";
/// yt-dlp subtitle artifact bundle output.
const OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS: &str = "yt_dlp_subtitle_artifacts";
/// yt-dlp thumbnail artifact bundle output.
const OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS: &str = "yt_dlp_thumbnail_artifacts";
/// yt-dlp description file output.
const OUTPUT_YT_DLP_DESCRIPTION_FILE: &str = "yt_dlp_description_file";
/// yt-dlp annotation artifact bundle output.
const OUTPUT_YT_DLP_ANNOTATION_ARTIFACTS: &str = "yt_dlp_annotation_artifacts";
/// yt-dlp infojson file output.
const OUTPUT_YT_DLP_INFOJSON_FILE: &str = "yt_dlp_infojson_file";
/// yt-dlp comments artifact bundle output.
const OUTPUT_YT_DLP_COMMENTS_ARTIFACTS: &str = "yt_dlp_comments_artifacts";
/// yt-dlp internet-shortcut artifact bundle output.
const OUTPUT_YT_DLP_LINK_ARTIFACTS: &str = "yt_dlp_link_artifacts";
/// yt-dlp split-chapter artifact bundle output.
const OUTPUT_YT_DLP_CHAPTER_ARTIFACTS: &str = "yt_dlp_chapter_artifacts";
/// yt-dlp playlist-video artifact bundle output.
const OUTPUT_YT_DLP_PLAYLIST_VIDEO_ARTIFACTS: &str = "yt_dlp_playlist_video_artifacts";
/// yt-dlp playlist-thumbnail artifact bundle output.
const OUTPUT_YT_DLP_PLAYLIST_THUMBNAIL_ARTIFACTS: &str = "yt_dlp_playlist_thumbnail_artifacts";
/// yt-dlp playlist-description artifact bundle output.
const OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_ARTIFACTS: &str = "yt_dlp_playlist_description_artifacts";
/// yt-dlp playlist-infojson artifact bundle output.
const OUTPUT_YT_DLP_PLAYLIST_INFOJSON_ARTIFACTS: &str = "yt_dlp_playlist_infojson_artifacts";
/// Generated tool input for list args injected right after executable token.
const INPUT_LEADING_ARGS: &str = "leading_args";
/// Generated tool input for list args appended after default operation args.
const INPUT_TRAILING_ARGS: &str = "trailing_args";
/// Generated tool input carrying upstream bytes for non-downloader tools.
const INPUT_CONTENT: &str = "input_content";
/// Generated tool input carrying source URL for online downloader tools.
const INPUT_SOURCE_URL: &str = "source_url";
/// Generated builtin import arg key selecting operation kind.
const INPUT_IMPORT_KIND: &str = "kind";
/// Generated builtin import arg key carrying source CAS hash text.
const INPUT_IMPORT_HASH: &str = "hash";

/// Builtin import kind used by local-source ingest steps.
const IMPORT_KIND_CAS_HASH: &str = "cas_hash";

/// Resolves one step option to scalar string text.
#[must_use]
fn step_option_scalar<'a>(step: &'a MediaStep, key: &str) -> Option<&'a str> {
    match step.options.get(key) {
        Some(TransformInputValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Reconciles one workflow per media source into machine config.
///
/// Managed data policy:
/// - workflows under `mediapm.media.*` are regenerated each sync,
/// - workflows whose ids are explicitly declared via `media.<id>.workflow_id`
///   are also replaced each sync,
/// - local-source external-data records with managed descriptions are
///   regenerated each sync,
/// - user-managed workflows/external-data entries outside those managed ids are
///   preserved untouched.
pub(crate) fn reconcile_media_workflows(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    lock: &MediaLockFile,
) -> Result<(), MediaPmError> {
    let mut machine = load_machine_document(&paths.conductor_machine_ncl)?;
    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(&document.tools)?;
    let plan = build_media_workflow_plan_with_limits(document, lock, &machine, ffmpeg_slot_limits)?;
    let override_ids = explicit_managed_workflow_overrides(document);

    machine.workflows.retain(|workflow_id, _| {
        !workflow_id.starts_with(MANAGED_WORKFLOW_PREFIX) && !override_ids.contains(workflow_id)
    });
    machine.external_data.retain(|_, reference| {
        !reference
            .description
            .as_deref()
            .is_some_and(|description| description.starts_with(MANAGED_EXTERNAL_DESCRIPTION_PREFIX))
    });

    for (workflow_id, workflow) in plan.workflows {
        machine.workflows.insert(workflow_id, workflow);
    }

    for (hash, reference) in plan.external_data {
        machine.add_external_data(
            hash,
            AddExternalDataOptions::new(reference).overwrite_existing(true),
        )?;
    }

    save_machine_document(&paths.conductor_machine_ncl, &machine)
}

/// Returns explicit workflow-id overrides that should be managed by `mediapm`.
fn explicit_managed_workflow_overrides(document: &MediaPmDocument) -> BTreeSet<String> {
    document.media.values().filter_map(|source| source.workflow_id.clone()).collect::<BTreeSet<_>>()
}

/// Planned managed workflow + external-data updates.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct MediaWorkflowPlan {
    /// Desired managed workflows keyed by workflow id.
    workflows: BTreeMap<String, WorkflowSpec>,
    /// Desired managed external-data refs keyed by CAS hash identity.
    external_data: BTreeMap<Hash, ExternalContentRef>,
}

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

/// One variant-source producer binding available to downstream steps.
#[derive(Debug, Clone, PartialEq, Eq)]
enum VariantProducer {
    /// Variant bytes come from one external-data reference.
    ExternalData { hash: Hash },
    /// Variant bytes come from one prior step output.
    StepOutput { step_id: String, output_name: String, zip_member: Option<String> },
}

impl VariantProducer {
    /// Renders this producer into one input binding plus optional dependency.
    fn to_binding(&self) -> Result<(InputBinding, Option<String>), MediaPmError> {
        match self {
            Self::ExternalData { hash } => {
                Ok((InputBinding::String(format!("${{external_data.{hash}}}")), None))
            }
            Self::StepOutput { step_id, output_name, zip_member } => {
                let expression = if let Some(member) = zip_member.as_deref() {
                    format!("${{step_output.{step_id}.{output_name}:zip({member})}}")
                } else {
                    format!("${{step_output.{step_id}.{output_name}}}")
                };

                Ok((InputBinding::String(expression), Some(step_id.clone())))
            }
        }
    }
}

/// Builds the full managed workflow/external-data plan from `mediapm` config.
#[cfg(test)]
fn build_media_workflow_plan(
    document: &MediaPmDocument,
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
) -> Result<MediaWorkflowPlan, MediaPmError> {
    build_media_workflow_plan_with_limits(document, lock, machine, FfmpegSlotLimits::default())
}

/// Builds the full managed workflow/external-data plan from `mediapm` config.
fn build_media_workflow_plan_with_limits(
    document: &MediaPmDocument,
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<MediaWorkflowPlan, MediaPmError> {
    let mut plan = MediaWorkflowPlan::default();

    for (media_id, source) in &document.media {
        let mut workflow = WorkflowSpec {
            name: Some(managed_workflow_name_for_media(media_id)),
            description: source.description.clone(),
            ..WorkflowSpec::default()
        };
        let mut variant_producers = BTreeMap::<String, VariantProducer>::new();

        seed_local_variant_sources(&mut plan, media_id, source, &mut variant_producers)?;
        synthesize_media_steps(
            &mut workflow,
            media_id,
            source,
            lock,
            machine,
            &mut variant_producers,
            ffmpeg_slot_limits,
        )?;

        plan.workflows.insert(managed_workflow_id_for_media(media_id, source), workflow);
    }

    Ok(plan)
}

/// Resolves one variant to the managed workflow step-output producer binding.
///
/// This helper mirrors the variant-flow synthesis logic used during managed
/// workflow generation. It returns `None` when the requested variant is not
/// produced by any workflow step (for example direct local CAS pointers that
/// bypass managed step outputs).
#[cfg(test)]
pub(crate) fn resolve_media_variant_output_binding(
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<Option<ResolvedVariantOutputBinding>, MediaPmError> {
    resolve_media_variant_output_binding_with_ffmpeg_limits(
        source,
        variant,
        FfmpegSlotLimits::default(),
    )
}

/// Resolves one variant to the managed workflow step-output producer binding
/// with explicit ffmpeg slot limits.
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

/// Resolves one variant to the managed workflow step-output producer binding
/// using one internal slot-limit struct.
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
        let mappings = resolve_step_variant_flow(step)
            .map_err(|reason| MediaPmError::Workflow(format!("step #{step_index} {reason}")))?;

        let producer_snapshot = variant_producers.clone();
        let mut pending_variant_updates = Vec::new();

        if matches!(step.tool, MediaStepTool::Ffmpeg) {
            if step.input_variants.len() > ffmpeg_slot_limits.max_input_slots {
                return Err(MediaPmError::Workflow(format!(
                    "step #{step_index} declares {} ffmpeg input variants but maximum supported is {}; reduce input_variants fan-out or increase tools.ffmpeg.max_input_slots (default {DEFAULT_FFMPEG_MAX_INPUT_SLOTS})",
                    step.input_variants.len(),
                    ffmpeg_slot_limits.max_input_slots,
                )));
            }

            for input_variant in &step.input_variants {
                if !producer_snapshot.contains_key(input_variant) {
                    return Err(MediaPmError::Workflow(format!(
                        "step #{step_index} references unknown input variant '{}'",
                        input_variant
                    )));
                }
            }

            let step_id = ffmpeg_step_id(step_index);
            for mapping in &mappings {
                let output_binding = resolve_step_output_binding(
                    step.tool,
                    &step.output_variants,
                    &mapping.output,
                    ffmpeg_slot_limits,
                )?;
                pending_variant_updates.push((
                    mapping.output.clone(),
                    VariantProducer::StepOutput {
                        step_id: step_id.clone(),
                        output_name: output_binding.output_name,
                        zip_member: output_binding.zip_member,
                    },
                ));
            }

            for (output_variant, producer) in pending_variant_updates {
                variant_producers.insert(output_variant, producer);
            }

            continue;
        }

        for (mapping_index, mapping) in mappings.iter().enumerate() {
            if !step.tool.is_source_ingest_tool() && !producer_snapshot.contains_key(&mapping.input)
            {
                return Err(MediaPmError::Workflow(format!(
                    "step #{step_index} references unknown input variant '{}'",
                    mapping.input
                )));
            }

            let step_id = media_step_id(step_index, mapping_index, step.tool, mapping);
            let step_id = if matches!(step.tool, MediaStepTool::MediaTagger) {
                format!("{step_id}-apply")
            } else {
                step_id
            };
            let output_binding = resolve_step_output_binding(
                step.tool,
                &step.output_variants,
                &mapping.output,
                ffmpeg_slot_limits,
            )?;
            let resolved_output_name = if matches!(step.tool, MediaStepTool::MediaTagger) {
                ffmpeg_output_capture_name(0)
            } else {
                output_binding.output_name
            };
            pending_variant_updates.push((
                mapping.output.clone(),
                VariantProducer::StepOutput {
                    step_id,
                    output_name: resolved_output_name,
                    zip_member: output_binding.zip_member,
                },
            ));
        }

        for (output_variant, producer) in pending_variant_updates {
            variant_producers.insert(output_variant, producer);
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

/// Seeds initial variant producers from local-source CAS variant pointers.
fn seed_local_variant_sources(
    plan: &mut MediaWorkflowPlan,
    media_id: &str,
    source: &MediaSourceSpec,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
) -> Result<(), MediaPmError> {
    for (variant, hash_text) in &source.variant_hashes {
        let hash = Hash::from_str(hash_text).map_err(|_| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' local variant '{variant}' has invalid CAS hash '{hash_text}'"
            ))
        })?;

        plan.external_data.insert(
            hash,
            ExternalContentRef {
                description: Some(format!(
                    "managed local variant source for media '{media_id}' variant '{variant}'"
                )),
            },
        );
        variant_producers.insert(variant.clone(), VariantProducer::ExternalData { hash });
    }

    Ok(())
}

/// Resolves one input variant to a producer plus optional ZIP-member selector.
///
/// Resolution priority:
/// 1. exact key match.
fn resolve_input_variant_producer<'a>(
    input_variant: &str,
    producer_snapshot: &'a BTreeMap<String, VariantProducer>,
) -> Result<Option<&'a VariantProducer>, MediaPmError> {
    Ok(producer_snapshot.get(input_variant))
}

/// Creates ordered workflow steps from unified media-step declarations.
fn synthesize_media_steps(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    source: &MediaSourceSpec,
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    for (step_index, step) in source.steps.iter().enumerate() {
        let tool_id = resolve_step_tool_id(lock, machine, step.tool)?;
        let mappings = resolve_step_variant_flow(step).map_err(|reason| {
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

        let producer_snapshot = variant_producers.clone();

        if matches!(step.tool, MediaStepTool::MediaTagger) {
            synthesize_media_tagger_step_pair(
                workflow,
                media_id,
                step_index,
                step,
                &mappings,
                lock,
                machine,
                &producer_snapshot,
                variant_producers,
                ffmpeg_slot_limits,
            )?;
            continue;
        }

        if matches!(step.tool, MediaStepTool::Ffmpeg) {
            synthesize_ffmpeg_step(
                workflow,
                media_id,
                step_index,
                step,
                &mappings,
                &tool_id,
                &producer_snapshot,
                variant_producers,
                ffmpeg_slot_limits,
            )?;
            continue;
        }

        let mut pending_variant_updates = Vec::new();

        for (mapping_index, mapping) in mappings.iter().enumerate() {
            let step_id = media_step_id(step_index, mapping_index, step.tool, mapping);
            let mut depends_on = Vec::new();
            let mut inputs = BTreeMap::new();

            if step.tool.is_online_media_downloader() {
                let source_uri = step_option_scalar(step, "uri")
                    .map(ToString::to_string)
                    .unwrap_or_else(|| media_source_uri(media_id, source));
                inputs.insert(INPUT_SOURCE_URL.to_string(), InputBinding::String(source_uri));
            } else if matches!(step.tool, MediaStepTool::Import | MediaStepTool::ImportOnce) {
                let kind = step_option_scalar(step, INPUT_IMPORT_KIND)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| IMPORT_KIND_CAS_HASH.to_string());
                let hash = step_option_scalar(step, INPUT_IMPORT_HASH)
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
                    resolve_input_variant_producer(&mapping.input, &producer_snapshot)?
                else {
                    return Err(MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{step_index} references unknown input variant '{}'",
                        mapping.input
                    )));
                };
                let (input_binding, dependency) = producer.to_binding()?;
                inputs.insert(INPUT_CONTENT.to_string(), input_binding);
                if let Some(step_dependency) = dependency {
                    depends_on.push(step_dependency);
                }
            }

            if !matches!(step.tool, MediaStepTool::Import | MediaStepTool::ImportOnce) {
                let (leading_args, trailing_args) =
                    extract_step_list_args(media_id, step_index, step)?;

                let mut option_inputs = step_option_input_bindings(step.tool, &step.options)?;
                if matches!(step.tool, MediaStepTool::YtDlp) {
                    let output_variant_config = decode_yt_dlp_output_variant_config(
                        &mapping.output,
                        &step.output_variants,
                    )?;
                    option_inputs.extend(yt_dlp_variant_inputs(&output_variant_config)?);
                }

                inputs
                    .insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(leading_args));
                inputs.insert(
                    INPUT_TRAILING_ARGS.to_string(),
                    InputBinding::StringList(trailing_args),
                );
                inputs.extend(option_inputs);
            }

            let outputs = step_output_policy_overrides(
                step.tool,
                &step.output_variants,
                &mapping.output,
                ffmpeg_slot_limits,
            )?;

            let output_binding = resolve_step_output_binding(
                step.tool,
                &step.output_variants,
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
                },
            ));
        }

        for (output_variant, producer) in pending_variant_updates {
            variant_producers.insert(output_variant, producer);
        }
    }

    Ok(())
}

/// Expands one ffmpeg step with ordered indexed inputs/outputs.
#[allow(clippy::too_many_arguments)]
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
    let step_id = ffmpeg_step_id(step_index);
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

        let Some(producer) = resolve_input_variant_producer(input_variant, producer_snapshot)?
        else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} references unknown input variant '{input_variant}'"
            )));
        };

        let (input_binding, dependency) = producer.to_binding()?;
        inputs.insert(ffmpeg_input_content_name(input_index), input_binding);
        if let Some(step_dependency) = dependency
            && seen_depends_on.insert(step_dependency.clone())
        {
            depends_on.push(step_dependency);
        }
    }

    let (leading_args, trailing_args) = extract_step_list_args(media_id, step_index, step)?;
    let option_inputs = step_option_input_bindings(step.tool, &step.options)?;
    inputs.insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(leading_args));
    inputs.insert(INPUT_TRAILING_ARGS.to_string(), InputBinding::StringList(trailing_args));
    inputs.extend(option_inputs);

    let mut outputs = BTreeMap::new();
    let mut pending_variant_updates = Vec::new();
    let mut seen_output_indexes = BTreeSet::new();

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
                "media '{media_id}' step #{step_index} assigns duplicate ffmpeg idx '{}' across output_variants",
                output_index_u32
            )));
        }

        let output_name = ffmpeg_output_capture_name(output_index);
        inputs.insert(
            ffmpeg_output_path_input_name(output_index),
            InputBinding::String(ffmpeg_output_file_path(output_index)),
        );

        let policy =
            decode_output_variant_policy(MediaStepTool::Ffmpeg, &mapping.output, variant_value)
                .map_err(MediaPmError::Workflow)?;
        outputs.insert(
            output_name.clone(),
            OutputPolicy { save: Some(policy.save), force_full: Some(policy.save_full) },
        );

        pending_variant_updates.push((
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: step_id.clone(),
                output_name,
                zip_member: config.zip_member,
            },
        ));
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

/// Expands one `media-tagger` config step into metadata + ffmpeg-apply steps.
#[allow(clippy::too_many_arguments)]
fn synthesize_media_tagger_step_pair(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
    producer_snapshot: &BTreeMap<String, VariantProducer>,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    let media_tagger_tool_id = resolve_step_tool_id(lock, machine, MediaStepTool::MediaTagger)?;
    let ffmpeg_tool_id = resolve_media_tagger_ffmpeg_tool_id(step, lock, machine)?;
    let ffmpeg_bin_for_metadata = resolve_media_tagger_ffmpeg_bin(step);

    for (mapping_index, mapping) in mappings.iter().enumerate() {
        let Some(producer) = resolve_input_variant_producer(&mapping.input, producer_snapshot)?
        else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} references unknown input variant '{}'",
                mapping.input
            )));
        };
        let (input_binding, input_dependency) = producer.to_binding()?;

        let base_step_id = media_step_id(step_index, mapping_index, step.tool, mapping);
        let metadata_step_id = format!("{base_step_id}-metadata");
        let apply_step_id = format!("{base_step_id}-apply");
        let output_binding = resolve_step_output_binding(
            step.tool,
            &step.output_variants,
            &mapping.output,
            ffmpeg_slot_limits,
        )?;
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
        let apply_output_name = ffmpeg_output_capture_name(0);
        let apply_outputs = BTreeMap::from([(
            apply_output_name.clone(),
            OutputPolicy {
                save: Some(output_policy.save),
                force_full: Some(output_policy.save_full),
            },
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

        metadata_inputs
            .entry("ffmpeg_bin".to_string())
            .or_insert_with(|| InputBinding::String(ffmpeg_bin_for_metadata.clone()));

        workflow.steps.push(WorkflowStepSpec {
            id: metadata_step_id.clone(),
            tool: media_tagger_tool_id.clone(),
            inputs: metadata_inputs,
            depends_on: metadata_depends_on,
            outputs: BTreeMap::from([(
                OUTPUT_CONTENT.to_string(),
                OutputPolicy { save: Some(true), force_full: Some(true) },
            )]),
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
            InputBinding::String(ffmpeg_output_file_path(0)),
        );
        apply_inputs.insert(INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(Vec::new()));
        apply_inputs.insert(INPUT_TRAILING_ARGS.to_string(), InputBinding::StringList(Vec::new()));
        apply_inputs.insert("map_metadata".to_string(), InputBinding::String("1".to_string()));
        apply_inputs.insert("codec_copy".to_string(), InputBinding::String("true".to_string()));
        apply_inputs.insert("vn".to_string(), InputBinding::String("false".to_string()));

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
            },
        );
    }

    Ok(())
}

/// Resolves selected ffmpeg tool id for one `media-tagger` step option set.
fn resolve_media_tagger_ffmpeg_tool_id(
    step: &MediaStep,
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
) -> Result<String, MediaPmError> {
    let requested_version =
        step_option_scalar(step, "ffmpeg_version").map(str::trim).filter(|value| !value.is_empty());

    if requested_version.is_none() || requested_version.is_some_and(|value| value == "global") {
        let active_tool_id = lock.active_tools.get("ffmpeg").cloned().ok_or_else(|| {
            MediaPmError::Workflow(
                "media-tagger step requires active logical tool 'ffmpeg' for metadata apply"
                    .to_string(),
            )
        })?;

        if !machine.tools.contains_key(&active_tool_id) {
            return Err(MediaPmError::Workflow(format!(
                "active ffmpeg tool '{active_tool_id}' is missing from conductor machine config"
            )));
        }

        return Ok(active_tool_id);
    }

    let requested_version = requested_version.expect("checked is_some above");
    let normalized_requested = normalize_selector_compare_value(requested_version);

    let mut candidates = lock
        .tool_registry
        .iter()
        .filter(|(_, record)| record.name.eq_ignore_ascii_case("ffmpeg"))
        .filter_map(|(tool_id, record)| {
            let normalized_record_version = normalize_selector_compare_value(&record.version);
            if normalized_record_version == normalized_requested
                && machine.tools.contains_key(tool_id)
            {
                Some(tool_id.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if let Some(active_tool_id) = lock.active_tools.get("ffmpeg")
        && candidates.iter().any(|candidate| candidate == active_tool_id)
    {
        return Ok(active_tool_id.clone());
    }

    candidates.sort();
    candidates.into_iter().next().ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "media-tagger step requested ffmpeg_version '{requested_version}', but no matching ffmpeg tool is registered in conductor machine config"
        ))
    })
}

/// Resolves ffmpeg executable path passed into metadata-fetch stage.
fn resolve_media_tagger_ffmpeg_bin(step: &MediaStep) -> String {
    if let Some(explicit_bin) =
        step_option_scalar(step, "ffmpeg_bin").map(str::trim).filter(|value| !value.is_empty())
    {
        return explicit_bin.to_string();
    }

    "ffmpeg".to_string()
}

/// Extracts low-level list-option bindings from step `options`.
fn extract_step_list_args(
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
) -> Result<(Vec<String>, Vec<String>), MediaPmError> {
    let leading_args = match step.options.get(INPUT_LEADING_ARGS) {
        Some(TransformInputValue::StringList(items)) => items.clone(),
        Some(TransformInputValue::String(_)) => {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} options['{INPUT_LEADING_ARGS}'] must be a string list"
            )));
        }
        None => Vec::new(),
    };

    let trailing_args = match step.options.get(INPUT_TRAILING_ARGS) {
        Some(TransformInputValue::StringList(items)) => items.clone(),
        Some(TransformInputValue::String(_)) => {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} options['{INPUT_TRAILING_ARGS}'] must be a string list"
            )));
        }
        None => Vec::new(),
    };

    Ok((leading_args, trailing_args))
}

/// Builds deterministic tool input bindings from one step tool/options map.
fn step_option_input_bindings(
    tool: MediaStepTool,
    options: &BTreeMap<String, TransformInputValue>,
) -> Result<BTreeMap<String, InputBinding>, MediaPmError> {
    let mut input_bindings = BTreeMap::new();

    for (key, value) in options {
        if let Some(binding) = map_step_option_input_binding(tool, key, value)? {
            input_bindings.insert(key.clone(), binding);
        }
    }

    Ok(input_bindings)
}

/// Resolved output binding behavior for one step output-variant entry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StepOutputBinding {
    /// Step output name to reference in `${step_output...}` bindings.
    output_name: String,
    /// Optional ZIP member selector applied during downstream input binding.
    zip_member: Option<String>,
}

/// Decodes one yt-dlp output-variant config entry for a specific map key.
fn decode_yt_dlp_output_variant_config(
    variant_key: &str,
    output_variants: &BTreeMap<String, Value>,
) -> Result<YtDlpOutputVariantConfig, MediaPmError> {
    let value = output_variants.get(variant_key).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "missing output variant '{variant_key}' while decoding yt-dlp config"
        ))
    })?;

    match decode_output_variant_config(MediaStepTool::YtDlp, variant_key, value)
        .map_err(MediaPmError::Workflow)?
    {
        DecodedOutputVariantConfig::YtDlp(config) => Ok(config),
        DecodedOutputVariantConfig::Generic(_) => Err(MediaPmError::Workflow(format!(
            "decoded non-yt-dlp output variant config for yt-dlp key '{variant_key}'"
        ))),
    }
}

/// Resolves one output variant to the generated step output binding behavior.
fn resolve_step_output_binding(
    tool: MediaStepTool,
    output_variants: &BTreeMap<String, Value>,
    output_variant: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<StepOutputBinding, MediaPmError> {
    let value = output_variants.get(output_variant).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "missing output variant '{output_variant}' while resolving step output binding"
        ))
    })?;

    let decoded = decode_output_variant_config(tool, output_variant, value)
        .map_err(MediaPmError::Workflow)?;

    Ok(match decoded {
        DecodedOutputVariantConfig::Generic(config) => {
            let output_name = if matches!(tool, MediaStepTool::Ffmpeg) {
                let index = config.idx.ok_or_else(|| {
                    MediaPmError::Workflow(format!(
                        "missing ffmpeg idx for output variant '{output_variant}'"
                    ))
                })?;
                let output_index = usize::try_from(index).map_err(|_| {
                    MediaPmError::Workflow(format!(
                        "invalid ffmpeg idx '{index}' for output variant '{output_variant}'"
                    ))
                })?;
                if output_index >= ffmpeg_slot_limits.max_output_slots {
                    return Err(MediaPmError::Workflow(format!(
                        "output variant '{output_variant}' uses ffmpeg idx '{index}' but tools.ffmpeg.max_output_slots is {}; reduce idx usage or increase tools.ffmpeg.max_output_slots (default {DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS})",
                        ffmpeg_slot_limits.max_output_slots,
                    )));
                }
                ffmpeg_output_capture_name(output_index)
            } else {
                config.kind
            };

            StepOutputBinding { output_name, zip_member: config.zip_member }
        }
        DecodedOutputVariantConfig::YtDlp(config) => StepOutputBinding {
            output_name: yt_dlp_output_name_for_kind(config.kind).to_string(),
            zip_member: config.zip_member,
        },
    })
}

/// Builds yt-dlp option inputs from one value-driven output-variant config.
fn yt_dlp_variant_inputs(
    config: &YtDlpOutputVariantConfig,
) -> Result<BTreeMap<String, InputBinding>, MediaPmError> {
    let mut inputs = BTreeMap::new();
    let true_binding = || InputBinding::String("true".to_string());
    let false_binding = || InputBinding::String("false".to_string());

    if !matches!(config.kind, YtDlpOutputKind::Primary) {
        inputs.insert("skip_download".to_string(), true_binding());
    }

    match config.kind {
        YtDlpOutputKind::Primary => {}
        YtDlpOutputKind::Sandbox => {}
        YtDlpOutputKind::Subtitle => {
            inputs.insert("write_subs".to_string(), true_binding());
            inputs.insert("write_description".to_string(), false_binding());
            inputs.insert("write_info_json".to_string(), false_binding());
        }
        YtDlpOutputKind::AutoSubtitle => {
            inputs.insert("write_auto_subs".to_string(), true_binding());
            inputs.insert("write_description".to_string(), false_binding());
            inputs.insert("write_info_json".to_string(), false_binding());
        }
        YtDlpOutputKind::Thumbnail => {
            inputs.insert("write_thumbnail".to_string(), true_binding());
            inputs.insert("write_description".to_string(), false_binding());
            inputs.insert("write_info_json".to_string(), false_binding());
        }
        YtDlpOutputKind::Description => {
            inputs.insert("write_description".to_string(), true_binding());
            inputs.insert("write_info_json".to_string(), false_binding());
        }
        YtDlpOutputKind::Annotation => {
            inputs.insert("write_description".to_string(), false_binding());
            inputs.insert("write_info_json".to_string(), false_binding());
        }
        YtDlpOutputKind::Infojson => {
            inputs.insert("write_info_json".to_string(), true_binding());
            inputs.insert("write_description".to_string(), false_binding());
        }
        YtDlpOutputKind::Comments => {
            inputs.insert("write_comments".to_string(), true_binding());
            inputs.insert("write_description".to_string(), false_binding());
            inputs.insert("write_info_json".to_string(), false_binding());
        }
        YtDlpOutputKind::Link => {
            inputs.insert("write_link".to_string(), true_binding());
            inputs.insert("write_description".to_string(), false_binding());
            inputs.insert("write_info_json".to_string(), false_binding());
        }
        YtDlpOutputKind::Chapter => {
            inputs.insert("split_chapters".to_string(), true_binding());
            inputs.insert("write_description".to_string(), false_binding());
            inputs.insert("write_info_json".to_string(), false_binding());
        }
        YtDlpOutputKind::PlaylistVideo
        | YtDlpOutputKind::PlaylistThumbnail
        | YtDlpOutputKind::PlaylistDescription
        | YtDlpOutputKind::PlaylistInfojson => {
            inputs.insert("write_description".to_string(), false_binding());
            inputs.insert("write_info_json".to_string(), false_binding());
        }
    }

    if let Some(format_value) = config.format.as_deref() {
        inputs.insert("format".to_string(), InputBinding::String(format_value.to_string()));
    }
    if let Some(langs) = config.langs.as_deref() {
        inputs.insert("sub_langs".to_string(), InputBinding::String(langs.to_string()));
    }
    if let Some(sub_format) = config.sub_format.as_deref() {
        inputs.insert("sub_format".to_string(), InputBinding::String(sub_format.to_string()));
    }
    if let Some(convert) = config.convert.as_deref() {
        inputs.insert(
            yt_dlp_convert_input_name_for_kind(config.kind).to_string(),
            InputBinding::String(convert.to_string()),
        );
    }

    Ok(inputs)
}

/// Resolves yt-dlp input name used for `convert` override semantics.
#[must_use]
fn yt_dlp_convert_input_name_for_kind(kind: YtDlpOutputKind) -> &'static str {
    match kind {
        YtDlpOutputKind::Subtitle | YtDlpOutputKind::AutoSubtitle => "convert_subs",
        YtDlpOutputKind::Thumbnail => "convert_thumbnails",
        _ => "recode_video",
    }
}

/// Maps one tool option key/value pair into deterministic step-input binding.
///
/// Mapping policy:
/// - values in `media.<id>.steps[].options` remain value-centric,
/// - `option_args` remains list-typed to support multi-value forwarding,
/// - all other option inputs are scalar `string` values,
/// - conductor command templates transform those values to argv tokens at
///   runtime.
pub(super) fn map_step_option_input_binding(
    tool: MediaStepTool,
    key: &str,
    value: &TransformInputValue,
) -> Result<Option<InputBinding>, MediaPmError> {
    if matches!(tool, MediaStepTool::YtDlp) && key == "uri" {
        return Ok(None);
    }

    if matches!(tool, MediaStepTool::MediaTagger)
        && matches!(key, "ffmpeg_version" | "output_container")
    {
        return Ok(None);
    }

    if matches!(key, INPUT_LEADING_ARGS | INPUT_TRAILING_ARGS) {
        return Ok(None);
    }

    if key == "option_args" {
        let items = match value {
            TransformInputValue::String(value) => {
                value.split_whitespace().map(ToString::to_string).collect::<Vec<_>>()
            }
            TransformInputValue::StringList(items) => items.clone(),
        };
        return Ok(Some(InputBinding::StringList(items)));
    }

    if matches!(value, TransformInputValue::StringList(_)) {
        return Err(MediaPmError::Workflow(format!(
            "tool '{}' option '{key}' must be a string; string_list is only supported for 'option_args', '{INPUT_LEADING_ARGS}', and '{INPUT_TRAILING_ARGS}'",
            tool.as_str()
        )));
    }

    Ok(Some(match value {
        TransformInputValue::String(value) => InputBinding::String(value.to_string()),
        TransformInputValue::StringList(_) => unreachable!("list values are rejected above"),
    }))
}

/// Resolves active immutable tool id for one logical tool name.
fn resolve_step_tool_id(
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
    step_tool: MediaStepTool,
) -> Result<String, MediaPmError> {
    if matches!(step_tool, MediaStepTool::Import | MediaStepTool::ImportOnce) {
        return resolve_builtin_tool_id(machine, "import", "1.0.0");
    }

    let logical_tool_name = step_tool.as_str();
    lock.active_tools.get(logical_tool_name).cloned().ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "logical tool '{logical_tool_name}' is required but not active; add it under mediapm.ncl tools and run tool sync"
        ))
    })
}

/// Resolves one registered builtin tool id by builtin identity tuple.
fn resolve_builtin_tool_id(
    machine: &MachineNickelDocument,
    builtin_name: &str,
    builtin_version: &str,
) -> Result<String, MediaPmError> {
    machine
        .tools
        .iter()
        .find_map(|(tool_id, spec)| match &spec.kind {
            ToolKindSpec::Builtin { name, version }
                if name == builtin_name && version == builtin_version =>
            {
                Some(tool_id.clone())
            }
            _ => None,
        })
        .ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "builtin tool '{}@{}' is required but not registered in conductor machine config",
                builtin_name, builtin_version
            ))
        })
}

/// Maps one value-driven yt-dlp output kind to generated output capture name.
#[must_use]
fn yt_dlp_output_name_for_kind(kind: YtDlpOutputKind) -> &'static str {
    match kind {
        YtDlpOutputKind::Primary => OUTPUT_CONTENT,
        YtDlpOutputKind::Sandbox => OUTPUT_SANDBOX_ARTIFACTS,
        YtDlpOutputKind::Subtitle | YtDlpOutputKind::AutoSubtitle => {
            OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS
        }
        YtDlpOutputKind::Thumbnail => OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
        YtDlpOutputKind::Description => OUTPUT_YT_DLP_DESCRIPTION_FILE,
        YtDlpOutputKind::Annotation => OUTPUT_YT_DLP_ANNOTATION_ARTIFACTS,
        YtDlpOutputKind::Infojson => OUTPUT_YT_DLP_INFOJSON_FILE,
        YtDlpOutputKind::Comments => OUTPUT_YT_DLP_COMMENTS_ARTIFACTS,
        YtDlpOutputKind::Link => OUTPUT_YT_DLP_LINK_ARTIFACTS,
        YtDlpOutputKind::Chapter => OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
        YtDlpOutputKind::PlaylistVideo => OUTPUT_YT_DLP_PLAYLIST_VIDEO_ARTIFACTS,
        YtDlpOutputKind::PlaylistThumbnail => OUTPUT_YT_DLP_PLAYLIST_THUMBNAIL_ARTIFACTS,
        YtDlpOutputKind::PlaylistDescription => OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_ARTIFACTS,
        YtDlpOutputKind::PlaylistInfojson => OUTPUT_YT_DLP_PLAYLIST_INFOJSON_ARTIFACTS,
    }
}

/// Builds conductor output-policy overrides for one resolved output variant.
fn step_output_policy_overrides(
    tool: MediaStepTool,
    output_variants: &BTreeMap<String, Value>,
    output_variant: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<BTreeMap<String, OutputPolicy>, MediaPmError> {
    let options = output_variants
        .get(output_variant)
        .map(|value| decode_output_variant_policy(tool, output_variant, value))
        .transpose()
        .map_err(MediaPmError::Workflow)?
        .ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "missing output variant '{output_variant}' while resolving output policy"
            ))
        })?;
    let output_binding =
        resolve_step_output_binding(tool, output_variants, output_variant, ffmpeg_slot_limits)?;

    let policy = OutputPolicy { save: Some(options.save), force_full: Some(options.save_full) };

    Ok(BTreeMap::from([(output_binding.output_name, policy)]))
}

/// Builds managed workflow id for one media source.
pub(crate) fn managed_workflow_id_for_media(media_id: &str, source: &MediaSourceSpec) -> String {
    source.workflow_id.clone().unwrap_or_else(|| format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"))
}

/// Returns default managed workflow display name for one media source.
#[must_use]
fn managed_workflow_name_for_media(media_id: &str) -> String {
    media_id.to_string()
}

/// Builds deterministic step id for one variant-flow mapping edge.
fn media_step_id(
    step_index: usize,
    mapping_index: usize,
    tool: MediaStepTool,
    mapping: &ResolvedStepVariantFlow,
) -> String {
    format!(
        "step-{}-{}-{}-{}-to-{}",
        step_index,
        mapping_index,
        tool.as_str(),
        sanitize_identifier(&mapping.input),
        sanitize_identifier(&mapping.output)
    )
}

/// Builds deterministic step id for one aggregated ffmpeg media step.
fn ffmpeg_step_id(step_index: usize) -> String {
    format!("step-{step_index}-ffmpeg")
}

/// Normalizes one identifier segment into lowercase ASCII-safe token.
fn sanitize_identifier(value: &str) -> String {
    let sanitized =
        value
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() { character.to_ascii_lowercase() } else { '_' }
            })
            .collect::<String>();

    if sanitized.is_empty() { "default".to_string() } else { sanitized }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_conductor::{
        InputBinding, MachineNickelDocument, OutputPolicy, ToolKindSpec, ToolSpec,
    };
    use serde_json::{Value, json};

    use crate::config::{
        MediaPmDocument, MediaSourceSpec, MediaStep, MediaStepTool, TransformInputValue,
    };
    use crate::lockfile::MediaLockFile;

    use super::{
        MANAGED_EXTERNAL_DESCRIPTION_PREFIX, MANAGED_WORKFLOW_PREFIX, build_media_workflow_plan,
        resolve_media_variant_output_binding, resolve_media_variant_output_binding_with_limits,
        step_option_input_bindings,
    };

    fn generic_output_variant(kind: &str) -> Value {
        json!({ "kind": kind, "save": true, "save_full": true })
    }

    fn ffmpeg_output_variant(idx: u32) -> Value {
        json!({ "kind": "output_content", "save": true, "save_full": true, "idx": idx })
    }

    fn yt_dlp_output_variant(kind: &str) -> Value {
        json!({ "kind": kind, "save": true, "save_full": true })
    }

    /// Protects one-workflow-per-media synthesis and managed id namespace.
    #[test]
    fn plan_builds_exactly_one_workflow_per_media() {
        let document = MediaPmDocument {
            media: BTreeMap::from([
                (
                    "media-a".to_string(),
                    MediaSourceSpec {
                        description: None,
                        workflow_id: None,
                        metadata: None,
                        variant_hashes: BTreeMap::from([(
                            "default".to_string(),
                            "blake3:0000000000000000000000000000000000000000000000000000000000000000"
                                .to_string(),
                        )]),
                        steps: Vec::new(),
                    },
                ),
                (
                    "media-b".to_string(),
                    MediaSourceSpec {
                        description: Some("custom media description".to_string()),
                        workflow_id: Some("custom.workflow.media-b".to_string()),
                        metadata: None,
                        variant_hashes: BTreeMap::from([(
                            "default".to_string(),
                            "blake3:1111111111111111111111111111111111111111111111111111111111111111"
                                .to_string(),
                        )]),
                        steps: Vec::new(),
                    },
                ),
            ]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile::default();
        let machine = MachineNickelDocument::default();
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");

        assert_eq!(plan.workflows.len(), 2);
        assert!(plan.workflows.contains_key("mediapm.media.media-a"));
        assert!(plan.workflows.contains_key("custom.workflow.media-b"));
        assert_eq!(
            plan.workflows
                .get("mediapm.media.media-a")
                .and_then(|workflow| workflow.name.as_deref()),
            Some("media-a")
        );
        assert_eq!(
            plan.workflows
                .get("custom.workflow.media-b")
                .and_then(|workflow| workflow.name.as_deref()),
            Some("media-b")
        );
        assert_eq!(
            plan.workflows
                .get("custom.workflow.media-b")
                .and_then(|workflow| workflow.description.as_deref()),
            Some("custom media description")
        );
        assert!(plan.external_data.keys().all(|hash| hash.to_string().starts_with("blake3:")));
        assert!(plan.external_data.values().all(|reference| {
            reference.description.as_deref().is_some_and(|description| {
                description.starts_with(MANAGED_EXTERNAL_DESCRIPTION_PREFIX)
            })
        }));
        assert!(
            plan.workflows
                .keys()
                .any(|workflow_id| workflow_id.starts_with(MANAGED_WORKFLOW_PREFIX))
        );
    }

    /// Protects dependency synthesis for ordered variant-flow step chains.
    #[test]
    fn variant_flow_creates_explicit_step_dependencies() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "remote-a".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![
                        MediaStep {
                            tool: MediaStepTool::YtDlp,
                            input_variants: Vec::new(),
                            output_variants: BTreeMap::from([(
                                "default".to_string(),
                                yt_dlp_output_variant("primary"),
                            )]),
                            options: BTreeMap::from([(
                                "uri".to_string(),
                                TransformInputValue::String(
                                    "https://example.com/video".to_string(),
                                ),
                            )]),
                        },
                        MediaStep {
                            tool: MediaStepTool::Ffmpeg,
                            input_variants: vec!["default".to_string()],
                            output_variants: BTreeMap::from([(
                                "aac".to_string(),
                                ffmpeg_output_variant(0),
                            )]),
                            options: BTreeMap::new(),
                        },
                        MediaStep {
                            tool: MediaStepTool::Rsgain,
                            input_variants: vec!["aac".to_string()],
                            output_variants: BTreeMap::from([(
                                "aac".to_string(),
                                generic_output_variant("output_content"),
                            )]),
                            options: BTreeMap::new(),
                        },
                    ],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([
                ("yt-dlp".to_string(), "mediapm.tools.yt-dlp+github-releases@latest".to_string()),
                ("ffmpeg".to_string(), "mediapm.tools.ffmpeg+github-btbn@latest".to_string()),
                ("rsgain".to_string(), "mediapm.tools.rsgain+github-releases@latest".to_string()),
            ]),
            ..MediaLockFile::default()
        };
        let machine = MachineNickelDocument::default();

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.remote-a").expect("managed workflow");

        assert_eq!(workflow.steps.len(), 3);
        let download = &workflow.steps[0];
        let ffmpeg = &workflow.steps[1];
        let rsgain = &workflow.steps[2];

        assert!(download.depends_on.is_empty());
        assert_eq!(ffmpeg.depends_on, vec![download.id.clone()]);
        assert_eq!(rsgain.depends_on, vec![ffmpeg.id.clone()]);
    }

    /// Protects media-tagger synthesis expansion into metadata-fetch and
    /// ffmpeg-apply step pair with deterministic dependency wiring.
    #[test]
    fn media_tagger_step_expands_to_metadata_and_apply_steps() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "tag-a".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    )]),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::MediaTagger,
                        input_variants: vec!["default".to_string()],
                        output_variants: BTreeMap::from([(
                            "tagged".to_string(),
                            generic_output_variant("output_content"),
                        )]),
                        options: BTreeMap::from([
                            (
                                "strict_identification".to_string(),
                                TransformInputValue::String("false".to_string()),
                            ),
                            (
                                "ffmpeg_version".to_string(),
                                TransformInputValue::String("global".to_string()),
                            ),
                            (
                                "output_container".to_string(),
                                TransformInputValue::String("mp4".to_string()),
                            ),
                        ]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([
                (
                    "media-tagger".to_string(),
                    "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
                ),
                ("ffmpeg".to_string(), "mediapm.tools.ffmpeg+github-btbn@latest".to_string()),
            ]),
            ..MediaLockFile::default()
        };
        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(
            "mediapm.tools.ffmpeg+github-btbn@latest".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec!["${context.os == \"windows\" ? windows/ffmpeg.exe | ''}${context.os == \"linux\" ? linux/ffmpeg | ''}${context.os == \"macos\" ? macos/ffmpeg | ''}".to_string()],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            },
        );

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.tag-a").expect("managed workflow");

        assert_eq!(workflow.steps.len(), 2);

        let metadata = &workflow.steps[0];
        let apply = &workflow.steps[1];

        assert_eq!(metadata.tool, "mediapm.tools.media-tagger+mediapm-internal@latest");
        assert_eq!(
            metadata.outputs.get("output_content"),
            Some(&OutputPolicy { save: Some(true), force_full: Some(true) })
        );
        assert_eq!(
            metadata.inputs.get("ffmpeg_bin"),
            Some(&InputBinding::String("ffmpeg".to_string()))
        );
        assert!(!metadata.inputs.contains_key("ffmpeg_version"));
        assert!(!metadata.inputs.contains_key("output_container"));

        assert_eq!(apply.tool, "mediapm.tools.ffmpeg+github-btbn@latest");
        assert!(apply.depends_on.contains(&metadata.id));
        assert_eq!(
            apply.inputs.get("ffmetadata_content"),
            Some(&InputBinding::String(format!("${{step_output.{}.output_content}}", metadata.id)))
        );
        assert_eq!(apply.inputs.get("container"), Some(&InputBinding::String("mp4".to_string())));
        assert_eq!(
            apply.outputs.get("output_content_0"),
            Some(&OutputPolicy { save: Some(true), force_full: Some(true) })
        );

        let binding = resolve_media_variant_output_binding(
            document.media.get("tag-a").expect("tag-a source"),
            "tagged",
        )
        .expect("resolve tagged binding")
        .expect("tagged binding should exist");
        assert_eq!(binding.step_id, apply.id);
        assert_eq!(binding.output_name, "output_content_0");
    }

    /// Protects local import-step synthesis using builtin import output wiring.
    #[test]
    fn import_once_step_synthesizes_builtin_import_binding() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "local-a".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::ImportOnce,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "source".to_string(),
                            generic_output_variant("output_content"),
                        )]),
                        options: BTreeMap::from([
                            (
                                "kind".to_string(),
                                TransformInputValue::String("cas_hash".to_string()),
                            ),
                            (
                                "hash".to_string(),
                                TransformInputValue::String(
                                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                        .to_string(),
                                ),
                            ),
                        ]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile::default();
        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(
            "import@1.0.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: "import".to_string(),
                    version: "1.0.0".to_string(),
                },
                ..ToolSpec::default()
            },
        );

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.local-a").expect("managed workflow");
        assert_eq!(workflow.steps.len(), 1);

        let step = &workflow.steps[0];
        assert_eq!(step.tool, "import@1.0.0");
        assert!(step.depends_on.is_empty());
        assert_eq!(step.inputs.get("kind"), Some(&InputBinding::String("cas_hash".to_string())));
        assert_eq!(step.outputs.get("output_content").and_then(|policy| policy.save), Some(true),);
    }

    /// Protects per-variant output policy mapping from mediapm schema into
    /// generated conductor workflow-step output overrides.
    #[test]
    fn step_output_variant_policy_maps_to_workflow_output_policy() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "policy-a".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "source".to_string(),
                        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    )]),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::Ffmpeg,
                        input_variants: vec!["source".to_string()],
                        output_variants: BTreeMap::from([(
                            "normalized".to_string(),
                            json!({ "kind": "output_content", "save": false, "save_full": true, "idx": 0 }),
                        )]),
                        options: BTreeMap::new(),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([(
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-btbn@latest".to_string(),
            )]),
            ..MediaLockFile::default()
        };
        let machine = MachineNickelDocument::default();

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.policy-a").expect("managed workflow");
        let step = workflow.steps.first().expect("workflow step");

        assert_eq!(
            step.outputs.get("output_content_0"),
            Some(&OutputPolicy { save: Some(false), force_full: Some(true) }),
        );
    }

    /// Protects yt-dlp artifact variants by mapping non-primary outputs to
    /// artifact-bundle capture outputs instead of `output_content`.
    #[test]
    fn yt_dlp_artifact_variant_maps_output_policy_to_artifact_capture() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "policy-ytdlp".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "subtitles".to_string(),
                            json!({
                                "kind": "subtitle",
                                "save": true,
                                "save_full": false
                            }),
                        )]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        )]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases@latest".to_string(),
            )]),
            ..MediaLockFile::default()
        };
        let machine = MachineNickelDocument::default();

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.policy-ytdlp").expect("managed workflow");
        let step = workflow.steps.first().expect("workflow step");

        assert_eq!(
            step.outputs.get("yt_dlp_subtitle_artifacts"),
            Some(&OutputPolicy { save: Some(true), force_full: Some(false) }),
        );
        assert!(!step.outputs.contains_key("output_content"));
    }

    /// Protects sidecar capture routing by forcing an explicit output key even
    /// when per-variant save/force overrides are omitted.
    #[test]
    fn yt_dlp_sidecar_variant_without_policy_still_emits_artifact_output_key() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "policy-ytdlp-default-sidecar".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "thumbnail".to_string(),
                            yt_dlp_output_variant("thumbnail"),
                        )]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        )]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases@latest".to_string(),
            )]),
            ..MediaLockFile::default()
        };
        let machine = MachineNickelDocument::default();

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan
            .workflows
            .get("mediapm.media.policy-ytdlp-default-sidecar")
            .expect("managed workflow");
        let step = workflow.steps.first().expect("workflow step");

        assert!(step.outputs.contains_key("yt_dlp_thumbnail_artifacts"));
        assert!(!step.outputs.contains_key("output_content"));
    }

    /// Protects value-centric option binding policy by keeping non-`option_args`
    /// option values as scalar `string` bindings.
    #[test]
    fn step_option_bindings_keep_non_option_args_values_scalar() {
        let bindings = step_option_input_bindings(
            MediaStepTool::YtDlp,
            &BTreeMap::from([
                ("merge_output_format".to_string(), TransformInputValue::String("mkv".to_string())),
                ("no_playlist".to_string(), TransformInputValue::String("true".to_string())),
            ]),
        )
        .expect("bindings");

        assert!(
            bindings.get("merge_output_format") == Some(&InputBinding::String("mkv".to_string()))
        );
        assert!(bindings.get("no_playlist") == Some(&InputBinding::String("true".to_string())));
    }

    /// Protects `option_args` escape-hatch behavior, which remains `string_list` and
    /// splits scalar input on whitespace.
    #[test]
    fn step_option_bindings_split_option_args_to_string_list() {
        let bindings = step_option_input_bindings(
            MediaStepTool::YtDlp,
            &BTreeMap::from([(
                "option_args".to_string(),
                TransformInputValue::String("--foo --bar=baz".to_string()),
            )]),
        )
        .expect("bindings");

        assert_eq!(
            bindings.get("option_args"),
            Some(&InputBinding::StringList(vec!["--foo".to_string(), "--bar=baz".to_string()])),
        );
    }

    /// Protects scalar-first option typing by rejecting list values for
    /// non-`option_args` option inputs.
    #[test]
    fn step_option_bindings_reject_string_list_for_non_option_args_option() {
        let error = step_option_input_bindings(
            MediaStepTool::YtDlp,
            &BTreeMap::from([(
                "merge_output_format".to_string(),
                TransformInputValue::StringList(vec!["mkv".to_string()]),
            )]),
        )
        .expect_err("non-option_args list option should fail");

        assert!(error.to_string().contains("must be a string"));
        assert!(error.to_string().contains("merge_output_format"));
    }

    /// Protects yt-dlp source URI routing so workflow synthesis does not bind
    /// `options.uri` as a tool option input.
    #[test]
    fn step_option_bindings_skip_yt_dlp_uri_option() {
        let bindings = step_option_input_bindings(
            MediaStepTool::YtDlp,
            &BTreeMap::from([(
                "uri".to_string(),
                TransformInputValue::String("https://example.com/v".to_string()),
            )]),
        )
        .expect("bindings");

        assert!(!bindings.contains_key("uri"));
    }

    /// Protects hierarchy variant resolution so any variant exposed by any
    /// step remains selectable by name.
    #[test]
    fn variant_binding_resolves_non_latest_variant_name_when_still_unique() {
        let source = MediaSourceSpec {
            description: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![
                MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([
                        ("downloaded".to_string(), yt_dlp_output_variant("primary")),
                        ("subtitles".to_string(), yt_dlp_output_variant("subtitle")),
                    ]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String("https://example.com/video".to_string()),
                    )]),
                },
                MediaStep {
                    tool: MediaStepTool::Ffmpeg,
                    input_variants: vec!["downloaded".to_string()],
                    output_variants: BTreeMap::from([(
                        "video_144p".to_string(),
                        ffmpeg_output_variant(0),
                    )]),
                    options: BTreeMap::new(),
                },
            ],
        };

        let binding =
            resolve_media_variant_output_binding(&source, "subtitles").expect("resolve binding");
        let binding = binding.expect("binding should exist for subtitles variant");

        assert_eq!(binding.step_id, "step-0-1-yt-dlp-subtitles-to-subtitles");
        assert_eq!(binding.output_name, "yt_dlp_subtitle_artifacts");
    }

    /// Protects duplicate output-variant semantics by selecting the latest
    /// producer when multiple steps expose the same variant name.
    #[test]
    fn variant_binding_uses_last_producer_for_duplicate_output_variant() {
        let source = MediaSourceSpec {
            description: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![
                MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "downloaded".to_string(),
                        yt_dlp_output_variant("primary"),
                    )]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String("https://example.com/video".to_string()),
                    )]),
                },
                MediaStep {
                    tool: MediaStepTool::Ffmpeg,
                    input_variants: vec!["downloaded".to_string()],
                    output_variants: BTreeMap::from([(
                        "normalized".to_string(),
                        ffmpeg_output_variant(0),
                    )]),
                    options: BTreeMap::new(),
                },
                MediaStep {
                    tool: MediaStepTool::Rsgain,
                    input_variants: vec!["normalized".to_string()],
                    output_variants: BTreeMap::from([(
                        "normalized".to_string(),
                        generic_output_variant("output_content"),
                    )]),
                    options: BTreeMap::new(),
                },
            ],
        };

        let binding =
            resolve_media_variant_output_binding(&source, "normalized").expect("resolve binding");
        let binding = binding.expect("binding should exist for normalized variant");

        assert_eq!(binding.step_id, "step-2-0-rsgain-normalized-to-normalized");
        assert_eq!(binding.output_name, "output_content");
    }

    /// Protects ffmpeg runtime-limit configurability for high-index outputs.
    #[test]
    fn variant_binding_supports_custom_ffmpeg_output_limit() {
        let source = MediaSourceSpec {
            description: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::from([(
                "default".to_string(),
                "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            )]),
            steps: vec![MediaStep {
                tool: MediaStepTool::Ffmpeg,
                input_variants: vec!["default".to_string()],
                output_variants: BTreeMap::from([(
                    "hi".to_string(),
                    serde_json::json!({
                        "kind": "output_content",
                        "save": true,
                        "save_full": true,
                        "idx": 70
                    }),
                )]),
                options: BTreeMap::new(),
            }],
        };

        let default_error = resolve_media_variant_output_binding(&source, "hi")
            .expect_err("default limit should fail");
        assert!(default_error.to_string().contains("tools.ffmpeg.max_output_slots"));

        let binding = resolve_media_variant_output_binding_with_limits(&source, "hi", 128, 128)
            .expect("custom limits should resolve")
            .expect("binding should exist");
        assert_eq!(binding.output_name, "output_content_70");
    }

    /// Protects yt-dlp description sidecar semantics by binding directly to
    /// file captures with no implicit ZIP member selector.
    #[test]
    fn yt_dlp_description_binding_uses_file_capture_without_zip_member() {
        let source = MediaSourceSpec {
            description: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "description".to_string(),
                    serde_json::json!({ "kind": "description", "save": true, "save_full": true }),
                )]),
                options: BTreeMap::from([(
                    "uri".to_string(),
                    TransformInputValue::String("https://example.com/video".to_string()),
                )]),
            }],
        };

        let binding = resolve_media_variant_output_binding(&source, "description")
            .expect("resolve description binding")
            .expect("binding should exist");

        assert_eq!(binding.output_name, "yt_dlp_description_file");
        assert!(binding.zip_member.is_none());
    }

    /// Prevents equivalent-call dedup collisions between description and
    /// infojson sidecar steps by forcing opposite boolean toggles.
    #[test]
    fn yt_dlp_description_and_infojson_steps_set_complementary_flags() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "sidecar-flags".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([
                            (
                                "description".to_string(),
                                json!({ "kind": "description", "save": true, "save_full": true }),
                            ),
                            (
                                "info_json".to_string(),
                                json!({ "kind": "infojson", "save": true, "save_full": true }),
                            ),
                        ]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        )]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases@latest".to_string(),
            )]),
            ..MediaLockFile::default()
        };

        let plan = build_media_workflow_plan(&document, &lock, &MachineNickelDocument::default())
            .expect("plan");
        let workflow = plan.workflows.get("mediapm.media.sidecar-flags").expect("workflow");

        let description_step = workflow
            .steps
            .iter()
            .find(|step| step.id == "step-0-0-yt-dlp-description-to-description")
            .expect("description step");
        let infojson_step = workflow
            .steps
            .iter()
            .find(|step| step.id == "step-0-1-yt-dlp-info_json-to-info_json")
            .expect("infojson step");

        assert_eq!(
            description_step.inputs.get("write_description"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            description_step.inputs.get("write_info_json"),
            Some(&InputBinding::String("false".to_string()))
        );

        assert_eq!(
            infojson_step.inputs.get("write_info_json"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            infojson_step.inputs.get("write_description"),
            Some(&InputBinding::String("false".to_string()))
        );
    }

    /// Prevents thumbnail sidecar steps from leaking description/infojson
    /// sidecars by forcing sidecar-only toggles.
    #[test]
    fn yt_dlp_thumbnail_step_disables_description_and_infojson_sidecars() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "thumbnail-only".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "thumbnails/".to_string(),
                            json!({ "kind": "thumbnail", "save": true, "save_full": true }),
                        )]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        )]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases@latest".to_string(),
            )]),
            ..MediaLockFile::default()
        };

        let plan = build_media_workflow_plan(&document, &lock, &MachineNickelDocument::default())
            .expect("plan");
        let workflow = plan.workflows.get("mediapm.media.thumbnail-only").expect("workflow");
        let step = workflow.steps.first().expect("thumbnail step");

        assert_eq!(
            step.inputs.get("write_thumbnail"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            step.inputs.get("write_description"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(
            step.inputs.get("write_info_json"),
            Some(&InputBinding::String("false".to_string()))
        );
    }

    /// Protects key-agnostic producer resolution by requiring exact producer
    /// matches for scoped input variants.
    #[test]
    fn scoped_input_variant_requires_exact_producer_without_folder_fallback() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "scoped-folder".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![
                        MediaStep {
                            tool: MediaStepTool::YtDlp,
                            input_variants: Vec::new(),
                            output_variants: BTreeMap::from([(
                                "subtitles/".to_string(),
                                yt_dlp_output_variant("subtitle"),
                            )]),
                            options: BTreeMap::from([(
                                "uri".to_string(),
                                TransformInputValue::String(
                                    "https://example.com/video".to_string(),
                                ),
                            )]),
                        },
                        MediaStep {
                            tool: MediaStepTool::Ffmpeg,
                            input_variants: vec!["subtitles/en".to_string()],
                            output_variants: BTreeMap::from([(
                                "normalized".to_string(),
                                ffmpeg_output_variant(0),
                            )]),
                            options: BTreeMap::new(),
                        },
                    ],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([
                ("yt-dlp".to_string(), "mediapm.tools.yt-dlp+github-releases@latest".to_string()),
                ("ffmpeg".to_string(), "mediapm.tools.ffmpeg+github-btbn@latest".to_string()),
            ]),
            ..MediaLockFile::default()
        };

        let error = build_media_workflow_plan(&document, &lock, &MachineNickelDocument::default())
            .expect_err("plan should fail without exact scoped producer");
        assert!(error.to_string().contains("references unknown input variant 'subtitles/en'"));
    }

    /// Protects producer selection precedence so exact scoped outputs resolve
    /// successfully when both scoped and folder-like keys exist.
    #[test]
    fn scoped_input_variant_prefers_exact_output_over_folder_fallback() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "scoped-exact".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![
                        MediaStep {
                            tool: MediaStepTool::YtDlp,
                            input_variants: Vec::new(),
                            output_variants: BTreeMap::from([
                                ("subtitles/".to_string(), yt_dlp_output_variant("subtitle")),
                                (
                                    "subtitles/en".to_string(),
                                    json!({
                                        "kind": "subtitle",
                                        "save": true,
                                        "save_full": true,
                                        "langs": "en"
                                    }),
                                ),
                            ]),
                            options: BTreeMap::from([(
                                "uri".to_string(),
                                TransformInputValue::String(
                                    "https://example.com/video".to_string(),
                                ),
                            )]),
                        },
                        MediaStep {
                            tool: MediaStepTool::Ffmpeg,
                            input_variants: vec!["subtitles/en".to_string()],
                            output_variants: BTreeMap::from([(
                                "normalized".to_string(),
                                ffmpeg_output_variant(0),
                            )]),
                            options: BTreeMap::new(),
                        },
                    ],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([
                ("yt-dlp".to_string(), "mediapm.tools.yt-dlp+github-releases@latest".to_string()),
                ("ffmpeg".to_string(), "mediapm.tools.ffmpeg+github-btbn@latest".to_string()),
            ]),
            ..MediaLockFile::default()
        };

        let plan = build_media_workflow_plan(&document, &lock, &MachineNickelDocument::default())
            .expect("plan");
        let workflow = plan.workflows.get("mediapm.media.scoped-exact").expect("managed workflow");

        let exact_producer_step = &workflow.steps[1];
        let consumer_step = &workflow.steps[2];
        assert_eq!(
            consumer_step.inputs.get("input_content_0"),
            Some(&InputBinding::String(format!(
                "${{step_output.{}.yt_dlp_subtitle_artifacts}}",
                exact_producer_step.id
            ))),
        );
    }

    /// Protects yt-dlp auto-injected inputs derived from scoped subtitle
    /// variants.
    #[test]
    fn yt_dlp_scoped_subtitle_variant_auto_injects_write_and_langs_inputs() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "auto-inputs".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "subtitles/en".to_string(),
                            json!({
                                "kind": "subtitle",
                                "save": true,
                                "save_full": true,
                                "langs": "en"
                            }),
                        )]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        )]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases@latest".to_string(),
            )]),
            ..MediaLockFile::default()
        };

        let plan = build_media_workflow_plan(&document, &lock, &MachineNickelDocument::default())
            .expect("plan");
        let workflow = plan.workflows.get("mediapm.media.auto-inputs").expect("managed workflow");
        let step = workflow.steps.first().expect("yt-dlp step");

        assert_eq!(step.inputs.get("write_subs"), Some(&InputBinding::String("true".to_string())),);
        assert_eq!(step.inputs.get("sub_langs"), Some(&InputBinding::String("en".to_string())),);
        assert_eq!(
            step.inputs.get("skip_download"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert!(!step.inputs.contains_key("output"));
    }

    /// Protects primary yt-dlp variant behavior by keeping download-enabled
    /// defaults for media outputs.
    #[test]
    fn yt_dlp_primary_variant_does_not_auto_inject_skip_download() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "primary-output".to_string(),
                MediaSourceSpec {
                    description: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "video".to_string(),
                            yt_dlp_output_variant("primary"),
                        )]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        )]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases@latest".to_string(),
            )]),
            ..MediaLockFile::default()
        };

        let plan = build_media_workflow_plan(&document, &lock, &MachineNickelDocument::default())
            .expect("plan");
        let workflow =
            plan.workflows.get("mediapm.media.primary-output").expect("managed workflow");
        let step = workflow.steps.first().expect("yt-dlp step");

        assert!(!step.inputs.contains_key("skip_download"));
    }
}
