//! Per-media workflow synthesis for phase-3 `mediapm` documents.
//!
//! This module translates ordered `media.<id>.steps` declarations into managed
//! conductor workflows so each media id maps to exactly one workflow.
//! Variant-flow dependencies are expressed with explicit `${step_output...}`
//! bindings plus matching `depends_on` edges, allowing independent branches to
//! execute as soon as their producer data is ready.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::Hash;
use mediapm_conductor::{
    AddExternalDataOptions, ExternalContentRef, InputBinding, MachineNickelDocument, OutputPolicy,
    OutputSaveMode, ToolKindSpec, WorkflowSpec, WorkflowStepSpec,
};
use serde_json::Value;

use crate::config::{
    DecodedOutputVariantConfig, ManagedWorkflowStepState, MediaPmDocument, MediaPmImpureTimestamp,
    MediaSourceSpec, MediaStep, MediaStepTool, OutputCaptureKind, OutputSaveConfig,
    ResolvedStepVariantFlow, ToolRequirement, TransformInputValue, YtDlpOutputKind,
    YtDlpOutputVariantConfig, decode_output_variant_config, decode_output_variant_policy,
    expand_variant_selectors, media_source_uri, normalize_selector_compare_value,
    resolve_step_variant_flow,
};
use crate::error::MediaPmError;
use crate::lockfile::MediaLockFile;
use crate::paths::MediaPmPaths;

use super::documents::{load_machine_document, save_machine_document};
use super::tool_runtime::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS, DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, FfmpegSlotLimits,
    ffmpeg_cover_slot_enabled_input_name, ffmpeg_input_content_name, ffmpeg_output_capture_name,
    ffmpeg_output_file_path, ffmpeg_output_path_input_name, resolve_ffmpeg_slot_limits,
};

mod ffmpeg;
mod media_tagger;
mod rsgain;
mod yt_dlp;

/// Prefix for default `mediapm`-managed workflow ids in machine documents.
const MANAGED_WORKFLOW_PREFIX: &str = "mediapm.media.";
/// Prefix for persisted per-media step-state keys.
const MANAGED_WORKFLOW_STEP_STATE_KEY_PREFIX: &str = "step-";
/// Prefix for `mediapm`-managed external-data descriptions.
const MANAGED_EXTERNAL_DESCRIPTION_PREFIX: &str = "managed external data:";
/// Legacy prefix used by older local-variant-only managed external-data rows.
const LEGACY_MANAGED_EXTERNAL_DESCRIPTION_PREFIX: &str = "managed local variant source for media";

/// Output name exposed by generated executable tool contracts.
const OUTPUT_CONTENT: &str = "content";
/// Preferred generated output name for primary media payloads.
const OUTPUT_PRIMARY: &str = "primary";
/// Output name exposing full sandbox artifact bundles.
const OUTPUT_SANDBOX_ARTIFACTS: &str = "sandbox_artifacts";
/// yt-dlp subtitle artifact bundle output.
const OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS: &str = "yt_dlp_subtitle_artifacts";
/// yt-dlp thumbnail artifact bundle output.
const OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS: &str = "yt_dlp_thumbnail_artifacts";
/// yt-dlp description file output.
const OUTPUT_YT_DLP_DESCRIPTION_FILE: &str = "yt_dlp_description_file";
/// yt-dlp annotation file output.
const OUTPUT_YT_DLP_ANNOTATION_FILE: &str = "yt_dlp_annotation_file";
/// yt-dlp infojson file output.
const OUTPUT_YT_DLP_INFOJSON_FILE: &str = "yt_dlp_infojson_file";
/// yt-dlp download-archive file output.
const OUTPUT_YT_DLP_ARCHIVE_FILE: &str = "yt_dlp_archive_file";
/// yt-dlp internet-shortcut artifact bundle output.
const OUTPUT_YT_DLP_LINK_ARTIFACTS: &str = "yt_dlp_link_artifacts";
/// yt-dlp split-chapter artifact bundle output.
const OUTPUT_YT_DLP_CHAPTER_ARTIFACTS: &str = "yt_dlp_chapter_artifacts";
/// yt-dlp playlist-description file output.
const OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE: &str = "yt_dlp_playlist_description_file";
/// yt-dlp playlist-infojson file output.
const OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE: &str = "yt_dlp_playlist_infojson_file";
/// Deterministic managed archive path used when archive output is requested.
const YT_DLP_MANAGED_ARCHIVE_FILE: &str = "downloads/archive.txt";
/// Generated tool input for list args injected right after executable token.
const INPUT_LEADING_ARGS: &str = "leading_args";
/// Generated tool input for list args appended after default operation args.
const INPUT_TRAILING_ARGS: &str = "trailing_args";
/// Generated tool input carrying upstream bytes for non-downloader tools.
const INPUT_CONTENT: &str = "input_content";
/// Generated ffmpeg input carrying ffmetadata sidecar bytes.
const INPUT_FFMETADATA_CONTENT: &str = "ffmetadata_content";
/// Generated `sd` input carrying regex pattern text.
const INPUT_SD_PATTERN: &str = "pattern";
/// Generated `sd` input carrying replacement text.
const INPUT_SD_REPLACEMENT: &str = "replacement";
/// Generated tool input carrying source URL for online downloader tools.
const INPUT_SOURCE_URL: &str = "source_url";
/// Generated builtin import arg key selecting operation kind.
const INPUT_IMPORT_KIND: &str = "kind";
/// Generated builtin import arg key carrying source CAS hash text.
const INPUT_IMPORT_HASH: &str = "hash";

/// Builtin import kind used by local-source ingest steps.
const IMPORT_KIND_CAS_HASH: &str = "cas_hash";

/// Number of conductor steps emitted for one `media-tagger` mapping.
pub(super) const MEDIA_TAGGER_EXPANDED_STEPS_PER_MAPPING: usize = 2;
/// Offset of the `media-tagger` ffmpeg-apply step within one mapping expansion.
pub(super) const MEDIA_TAGGER_APPLY_STEP_OFFSET: usize = 1;
/// Number of conductor steps emitted for one `rsgain` mapping chain.
pub(super) const RSGAIN_EXPANDED_STEPS_PER_MAPPING: usize = 6;
/// Offset of the `rsgain` ffmpeg-apply step within one mapping expansion.
pub(super) const RSGAIN_APPLY_STEP_OFFSET: usize = 5;

/// Converts mediapm tri-state save policy into optional conductor persistence.
///
/// Conductor treats omitted `save` as the default `saved` behavior, so this
/// helper intentionally encodes `save = true` as `None`.
#[must_use]
fn conductor_output_save_mode(policy: OutputSaveConfig) -> Option<OutputSaveMode> {
    match policy {
        OutputSaveConfig::Bool(false) => Some(OutputSaveMode::Unsaved),
        OutputSaveConfig::Bool(true) => None,
        OutputSaveConfig::Full => Some(OutputSaveMode::Full),
    }
}

/// Resolves one step option to scalar string text.
#[must_use]
fn step_option_scalar<'a>(step: &'a MediaStep, key: &str) -> Option<&'a str> {
    match step.options.get(key) {
        Some(TransformInputValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Resolves one logical tool requirement by case-insensitive tool name.
#[must_use]
fn resolve_logical_tool_requirement<'a>(
    tool_requirements: &'a BTreeMap<String, ToolRequirement>,
    logical_tool_name: &str,
) -> Option<&'a ToolRequirement> {
    tool_requirements.iter().find_map(|(candidate, requirement)| {
        if candidate.eq_ignore_ascii_case(logical_tool_name) { Some(requirement) } else { None }
    })
}

/// Builds one deterministic ffmpeg output path using optional extension text.
///
/// Extension values may be provided with or without a leading dot. When the
/// extension is omitted, default managed ffmpeg output naming is used.
#[must_use]
fn ffmpeg_output_path_with_extension(index: usize, extension: Option<&str>) -> String {
    let Some(extension) = extension.map(str::trim) else {
        return ffmpeg_output_file_path(index);
    };

    if extension.is_empty() {
        return format!("output-{index}");
    }

    if extension.starts_with('.') {
        format!("output-{index}{extension}")
    } else {
        format!("output-{index}.{extension}")
    }
}

/// Normalizes one optional output extension to lowercase without a leading dot.
#[must_use]
pub(super) fn normalize_output_extension(extension: Option<&str>) -> Option<String> {
    let extension = extension?.trim();
    if extension.is_empty() {
        return None;
    }

    Some(extension.trim_start_matches('.').to_ascii_lowercase())
}

/// Resolves the effective managed ffmpeg-family output extension.
///
/// Generated ffmpeg-family outputs default to `.mkv` when no explicit
/// extension is configured. An explicitly empty extension keeps the output
/// extensionless.
#[must_use]
pub(super) fn resolved_ffmpeg_family_output_extension(extension: Option<&str>) -> Option<String> {
    match extension.map(str::trim) {
        None => Some("mkv".to_string()),
        Some("") => None,
        Some(value) => Some(value.trim_start_matches('.').to_ascii_lowercase()),
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
///
/// Lock side effects:
/// - updates `lock.workflow_step_state` refresh metadata for each
///   `media.<id>.steps[<index>]` row,
/// - prunes stale media/step refresh rows when sources or step indexes are
///   removed.
pub(crate) fn reconcile_media_workflows(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    lock: &mut MediaLockFile,
) -> Result<(), MediaPmError> {
    let mut machine = load_machine_document(&paths.conductor_machine_ncl)?;
    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(&document.tools)?;
    let plan = build_media_workflow_plan_with_limits(document, lock, &machine, ffmpeg_slot_limits)?;
    let override_ids = explicit_managed_workflow_overrides(document);
    let mut managed_external_data = plan.external_data;
    collect_managed_external_data_from_machine_and_lock(
        &machine,
        lock,
        &mut managed_external_data,
    )?;

    machine.workflows.retain(|workflow_id, _| {
        !workflow_id.starts_with(MANAGED_WORKFLOW_PREFIX) && !override_ids.contains(workflow_id)
    });
    machine.external_data.retain(|_, reference| {
        !reference.description.as_deref().is_some_and(is_managed_external_description)
    });

    for (workflow_id, workflow) in plan.workflows {
        machine.workflows.insert(workflow_id, workflow);
    }

    for (hash, reference) in managed_external_data {
        machine.add_external_data(
            hash,
            AddExternalDataOptions::new(reference).overwrite_existing(true),
        )?;
    }

    save_machine_document(&paths.conductor_machine_ncl, &machine)
}

/// Collects managed external-data roots from machine tool content and lock
/// managed files.
///
/// Save-policy merge contract:
/// - tool-content hashes require at least `save = true` (`Saved`),
/// - materialized managed-file hashes require at least `save = "full"`,
/// - duplicate hashes are deduplicated with monotonic escalation (`Full`
///   dominates `Saved`).
fn collect_managed_external_data_from_machine_and_lock(
    machine: &MachineNickelDocument,
    lock: &MediaLockFile,
    managed_external_data: &mut BTreeMap<Hash, ExternalContentRef>,
) -> Result<(), MediaPmError> {
    for (tool_id, tool_config) in &machine.tool_configs {
        let Some(content_map) = tool_config.content_map.as_ref() else {
            continue;
        };

        for (relative_path, hash) in content_map {
            upsert_managed_external_data(
                managed_external_data,
                *hash,
                managed_external_description(format!(
                    "tool content '{tool_id}' path '{relative_path}'"
                )),
                OutputSaveMode::Saved,
            );
        }
    }

    for (managed_path, record) in &lock.managed_files {
        let hash_text = record.hash.trim();
        let hash = Hash::from_str(hash_text).map_err(|source| {
            MediaPmError::Workflow(format!(
                "managed file '{managed_path}' has invalid CAS hash '{}': {source}",
                record.hash
            ))
        })?;

        upsert_managed_external_data(
            managed_external_data,
            hash,
            managed_external_description(format!(
                "materialized output '{managed_path}' (media '{}', variant '{}')",
                record.media_id, record.variant
            )),
            OutputSaveMode::Full,
        );
    }

    Ok(())
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

/// Returns true when one external-data description belongs to `mediapm`-managed rows.
#[must_use]
fn is_managed_external_description(description: &str) -> bool {
    description.starts_with(MANAGED_EXTERNAL_DESCRIPTION_PREFIX)
        || description.starts_with(LEGACY_MANAGED_EXTERNAL_DESCRIPTION_PREFIX)
}

/// Renders one managed external-data description with canonical prefix.
#[must_use]
fn managed_external_description<S>(suffix: S) -> String
where
    S: AsRef<str>,
{
    format!("{} {}", MANAGED_EXTERNAL_DESCRIPTION_PREFIX, suffix.as_ref())
}

/// Normalizes optional external-data save policy into a non-unsaved minimum.
#[must_use]
fn normalized_external_data_save_mode(save: Option<OutputSaveMode>) -> OutputSaveMode {
    match save {
        Some(OutputSaveMode::Full) => OutputSaveMode::Full,
        Some(OutputSaveMode::Saved | OutputSaveMode::Unsaved) | None => OutputSaveMode::Saved,
    }
}

/// Merges two external-data save policies while enforcing monotonic persistence.
#[must_use]
fn merge_external_data_save_mode(
    existing: Option<OutputSaveMode>,
    minimum: OutputSaveMode,
) -> OutputSaveMode {
    let existing = normalized_external_data_save_mode(existing);
    if matches!(existing, OutputSaveMode::Full) || matches!(minimum, OutputSaveMode::Full) {
        OutputSaveMode::Full
    } else {
        OutputSaveMode::Saved
    }
}

/// Upserts one managed external-data row with hash dedupe and save-policy merge.
fn upsert_managed_external_data(
    external_data: &mut BTreeMap<Hash, ExternalContentRef>,
    hash: Hash,
    description: String,
    minimum_save: OutputSaveMode,
) {
    let minimum_save = normalized_external_data_save_mode(Some(minimum_save));

    if let Some(existing) = external_data.get_mut(&hash) {
        existing.save = Some(merge_external_data_save_mode(existing.save, minimum_save));
        if existing.description.is_none() {
            existing.description = Some(description);
        }
        return;
    }

    external_data.insert(
        hash,
        ExternalContentRef { description: Some(description), save: Some(minimum_save) },
    );
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
    StepOutput {
        step_id: String,
        output_name: String,
        zip_member: Option<String>,
        extension: Option<String>,
    },
}

impl VariantProducer {
    /// Renders this producer into one input binding plus optional dependency.
    fn to_binding(&self) -> (InputBinding, Option<String>) {
        match self {
            Self::ExternalData { hash } => {
                (InputBinding::String(format!("${{external_data.{hash}}}")), None)
            }
            Self::StepOutput { step_id, output_name, zip_member, .. } => {
                let expression = if let Some(member) = zip_member.as_deref() {
                    format!("${{step_output.{step_id}.{output_name}:zip({member})}}")
                } else {
                    format!("${{step_output.{step_id}.{output_name}}}")
                };

                (InputBinding::String(expression), Some(step_id.clone()))
            }
        }
    }

    /// Returns the tracked output extension for one produced variant.
    #[must_use]
    fn output_extension(&self) -> Option<&str> {
        match self {
            Self::ExternalData { .. } => None,
            Self::StepOutput { extension, .. } => extension.as_deref(),
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
    let mut working_lock = lock.clone();
    build_media_workflow_plan_with_limits(
        document,
        &mut working_lock,
        machine,
        FfmpegSlotLimits::default(),
    )
}

/// Builds the full managed workflow/external-data plan and updates lock step
/// refresh state (test helper).
#[cfg(test)]
fn build_media_workflow_plan_and_update_state(
    document: &MediaPmDocument,
    lock: &mut MediaLockFile,
    machine: &MachineNickelDocument,
) -> Result<MediaWorkflowPlan, MediaPmError> {
    build_media_workflow_plan_with_limits(document, lock, machine, FfmpegSlotLimits::default())
}

/// Builds the full managed workflow/external-data plan from `mediapm` config.
fn build_media_workflow_plan_with_limits(
    document: &MediaPmDocument,
    lock: &mut MediaLockFile,
    machine: &MachineNickelDocument,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<MediaWorkflowPlan, MediaPmError> {
    let mut plan = MediaWorkflowPlan::default();

    for (media_id, source) in &document.media {
        let workflow_id = managed_workflow_id_for_media(media_id, source);
        let existing_workflow = machine.workflows.get(&workflow_id);
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
            &document.tools,
            existing_workflow,
            &mut variant_producers,
            ffmpeg_slot_limits,
        )?;

        plan.workflows.insert(workflow_id, workflow);
    }

    lock.workflow_step_state
        .retain(|tracked_media_id, _| document.media.contains_key(tracked_media_id));

    Ok(plan)
}

/// Builds the persisted state key for one media step index.
#[must_use]
fn managed_workflow_step_state_key(step_index: usize) -> String {
    format!("{MANAGED_WORKFLOW_STEP_STATE_KEY_PREFIX}{step_index}")
}

/// Serializes one explicit user-facing media step config snapshot.
fn explicit_media_step_config_snapshot(step: &MediaStep) -> Result<Value, MediaPmError> {
    serde_json::to_value(step).map_err(|error| {
        MediaPmError::Serialization(format!(
            "encoding explicit media step config snapshot failed: {error}"
        ))
    })
}

/// Returns true when one media step requires refresh under mediapm policy.
#[must_use]
fn media_step_requires_refresh(
    existing_state: Option<&ManagedWorkflowStepState>,
    explicit_config: &Value,
) -> bool {
    let Some(existing_state) = existing_state else {
        return true;
    };

    existing_state.explicit_config != *explicit_config || existing_state.impure_timestamp.is_none()
}

/// Generates one fresh monotonic impure timestamp for mediapm step refresh.
#[must_use]
fn fresh_impure_timestamp() -> MediaPmImpureTimestamp {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    MediaPmImpureTimestamp { epoch_seconds: now.as_secs(), subsec_nanos: now.subsec_nanos() }
}

/// Rewrites generated step tool ids using an existing workflow snapshot.
///
/// Returns `true` when every generated step id was found in `existing` and
/// therefore successfully pinned to its prior immutable tool id.
fn preserve_existing_generated_step_tools(
    workflow: &mut WorkflowSpec,
    generated_start: usize,
    existing: Option<&WorkflowSpec>,
) -> bool {
    let Some(existing) = existing else {
        return false;
    };

    let mut all_matched = true;
    for generated in workflow.steps.iter_mut().skip(generated_start) {
        if let Some(previous) = existing.steps.iter().find(|candidate| candidate.id == generated.id)
        {
            generated.tool = previous.tool.clone();
        } else {
            all_matched = false;
        }
    }

    all_matched
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
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
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

        let mut pending_variant_updates = Vec::new();

        if matches!(step.tool, MediaStepTool::Ffmpeg) {
            if resolved_step.input_variants.len() > ffmpeg_slot_limits.max_input_slots {
                return Err(MediaPmError::Workflow(format!(
                    "step #{step_index} declares {} ffmpeg input variants but maximum supported is {}; reduce input_variants fan-out or increase tools.ffmpeg.max_input_slots (default {DEFAULT_FFMPEG_MAX_INPUT_SLOTS})",
                    resolved_step.input_variants.len(),
                    ffmpeg_slot_limits.max_input_slots,
                )));
            }

            for input_variant in &resolved_step.input_variants {
                if !producer_snapshot.contains_key(input_variant) {
                    return Err(MediaPmError::Workflow(format!(
                        "step #{step_index} references unknown input variant '{input_variant}'"
                    )));
                }
            }

            let step_id = ffmpeg_step_id(step_index);
            for mapping in &mappings {
                let output_binding = resolve_step_output_binding(
                    step.tool,
                    &resolved_step.output_variants,
                    &mapping.output,
                    ffmpeg_slot_limits,
                )?;
                pending_variant_updates.push((
                    mapping.output.clone(),
                    VariantProducer::StepOutput {
                        step_id: step_id.clone(),
                        output_name: output_binding.output_name,
                        zip_member: output_binding.zip_member,
                        extension: None,
                    },
                ));
            }

            for (output_variant, producer) in pending_variant_updates {
                variant_producers.insert(output_variant, producer);
            }

            continue;
        }

        if matches!(step.tool, MediaStepTool::YtDlp) {
            let step_id = yt_dlp_step_id(step_index);

            for mapping in &mappings {
                let output_binding = resolve_step_output_binding(
                    step.tool,
                    &resolved_step.output_variants,
                    &mapping.output,
                    ffmpeg_slot_limits,
                )?;
                pending_variant_updates.push((
                    mapping.output.clone(),
                    VariantProducer::StepOutput {
                        step_id: step_id.clone(),
                        output_name: output_binding.output_name,
                        zip_member: output_binding.zip_member,
                        extension: None,
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
            let output_binding = resolve_step_output_binding(
                step.tool,
                &resolved_step.output_variants,
                &mapping.output,
                ffmpeg_slot_limits,
            )?;
            let (step_id, resolved_output_name) = if matches!(step.tool, MediaStepTool::MediaTagger)
            {
                let apply_step_index = expanded_step_index_for_mapping(
                    mapping_index,
                    MEDIA_TAGGER_EXPANDED_STEPS_PER_MAPPING,
                    MEDIA_TAGGER_APPLY_STEP_OFFSET,
                );
                (
                    format!(
                        "{}-apply",
                        media_step_id(step_index, apply_step_index, step.tool, mapping)
                    ),
                    ffmpeg_output_capture_name(0),
                )
            } else if matches!(step.tool, MediaStepTool::Rsgain) {
                let apply_step_index = expanded_step_index_for_mapping(
                    mapping_index,
                    RSGAIN_EXPANDED_STEPS_PER_MAPPING,
                    RSGAIN_APPLY_STEP_OFFSET,
                );
                (
                    format!(
                        "{}-ffmpeg-apply",
                        media_step_id(step_index, apply_step_index, step.tool, mapping)
                    ),
                    ffmpeg_output_capture_name(0),
                )
            } else {
                (step_id, output_binding.output_name)
            };
            pending_variant_updates.push((
                mapping.output.clone(),
                VariantProducer::StepOutput {
                    step_id,
                    output_name: resolved_output_name,
                    zip_member: output_binding.zip_member,
                    extension: None,
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
        VariantProducer::StepOutput { step_id, output_name, zip_member, .. } => {
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

        upsert_managed_external_data(
            &mut plan.external_data,
            hash,
            managed_external_description(format!(
                "local variant source for media '{media_id}' variant '{variant}'"
            )),
            OutputSaveMode::Saved,
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
) -> Option<&'a VariantProducer> {
    producer_snapshot.get(input_variant)
}

/// Creates ordered workflow steps from unified media-step declarations.
///
/// This synthesis pass also updates per-step refresh state in
/// `lock.workflow_step_state`:
/// - explicit config snapshots are persisted from user-authored step values,
/// - step impure timestamps are refreshed only when explicit config changes or
///   timestamps are missing,
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
fn synthesize_media_steps(
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
    let mut retained_step_state_keys = BTreeSet::new();

    for (step_index, step) in source.steps.iter().enumerate() {
        let step_state_key = managed_workflow_step_state_key(step_index);
        let _ = retained_step_state_keys.insert(step_state_key.clone());
        let explicit_step_config = explicit_media_step_config_snapshot(step)?;
        let existing_step_state = lock
            .workflow_step_state
            .get(media_id)
            .and_then(|steps| steps.get(&step_state_key))
            .cloned();
        let mut requires_refresh =
            media_step_requires_refresh(existing_step_state.as_ref(), &explicit_step_config);

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
                    let (leading_args, trailing_args) =
                        extract_step_list_args(media_id, step_index, &resolved_step)?;

                    let option_inputs =
                        step_option_input_bindings(step.tool, &resolved_step.options)?;

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

        lock.workflow_step_state.entry(media_id.to_string()).or_default().insert(
            step_state_key,
            ManagedWorkflowStepState { explicit_config: explicit_step_config, impure_timestamp },
        );
    }

    let mut remove_media_step_state = false;
    if let Some(step_state) = lock.workflow_step_state.get_mut(media_id) {
        step_state.retain(|step_key, _| retained_step_state_keys.contains(step_key));
        remove_media_step_state = step_state.is_empty();
    }
    if remove_media_step_state {
        lock.workflow_step_state.remove(media_id);
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
    yt_dlp::synthesize_yt_dlp_step(
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
    ffmpeg::synthesize_ffmpeg_step(
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
    rsgain::synthesize_rsgain_step_chain(
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
    media_tagger::synthesize_media_tagger_step_pair(
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

/// Resolves selected dependency tool id for managed steps that depend on
/// additional logical tools.
///
/// Selector behavior:
/// - omitted / `global` / `inherit`: use active logical dependency tool,
/// - explicit selector text: pick matching registered dependency tool version.
fn resolve_selected_dependency_tool_id(
    logical_tool_name: &str,
    dependency_tool_name: &str,
    requested_selector: Option<String>,
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
) -> Result<String, MediaPmError> {
    if requested_selector.is_none()
        || requested_selector.as_deref().is_some_and(|value| {
            value.eq_ignore_ascii_case("global") || value.eq_ignore_ascii_case("inherit")
        })
    {
        let active_tool_id = lock.active_tools.get(dependency_tool_name).cloned().ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "{logical_tool_name} requires active logical tool '{dependency_tool_name}'; configure tools.{dependency_tool_name} before using tools.{logical_tool_name}.dependencies.{dependency_tool_name}_version='inherit'"
            ))
        })?;

        if !machine.tools.contains_key(&active_tool_id) {
            return Err(MediaPmError::Workflow(format!(
                "active {dependency_tool_name} tool '{active_tool_id}' is missing from conductor machine config"
            )));
        }

        return Ok(active_tool_id);
    }

    let requested_selector = requested_selector.expect("checked is_some above");
    let normalized_requested = normalize_selector_compare_value(&requested_selector);

    let mut candidates = lock
        .tool_registry
        .iter()
        .filter(|(_, record)| record.name.eq_ignore_ascii_case(dependency_tool_name))
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

    if let Some(active_tool_id) = lock.active_tools.get(dependency_tool_name)
        && candidates.iter().any(|candidate| candidate == active_tool_id)
    {
        return Ok(active_tool_id.clone());
    }

    candidates.sort();
    candidates.into_iter().next().ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "tools.{logical_tool_name}.dependencies.{dependency_tool_name}_version '{requested_selector}' did not match any registered {dependency_tool_name} tool in conductor machine config"
        ))
    })
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
        DecodedOutputVariantConfig::YtDlp(config) => {
            let capture_kind = config.effective_capture_kind();
            StepOutputBinding {
                output_name: yt_dlp_output_name_for_kind(config.kind, capture_kind).to_string(),
                zip_member: config.zip_member,
            }
        }
    })
}

/// Returns yt-dlp boolean sidecar toggle input keys used by managed runtime
/// defaults and per-variant overrides.
///
/// Per-variant sidecar workflow steps set all of these toggles to `"false"`
/// first, then selectively enable only the toggle required by the variant
/// kind. This keeps each sidecar family isolated and prevents mixed output
/// artifact directories.
#[must_use]
fn yt_dlp_sidecar_toggle_inputs() -> [&'static str; 12] {
    [
        "write_subs",
        "write_thumbnail",
        "write_all_thumbnails",
        "write_description",
        "write_annotations",
        "write_chapters",
        "write_info_json",
        "write_url_link",
        "write_webloc_link",
        "write_desktop_link",
        "write_comments",
        "write_link",
    ]
}

/// Counts non-empty comma-separated selector values.
/// Builds merged yt-dlp option inputs from requested output-variant kinds.
///
/// This enables the minimum required toggles so one yt-dlp call can produce
/// multiple declared outputs efficiently.
///
/// Language downloader selection remains authoritative in
/// `steps[*].options.sub_langs`; variant-level language hints are used only by
/// capture/materialization behavior.
fn yt_dlp_inputs_for_output_variants(
    output_configs: &[YtDlpOutputVariantConfig],
) -> Result<BTreeMap<String, InputBinding>, MediaPmError> {
    let mut inputs = BTreeMap::new();
    let true_binding = || InputBinding::String("true".to_string());
    let false_binding = || InputBinding::String("false".to_string());

    if output_configs.is_empty() {
        return Ok(inputs);
    }

    for toggle in yt_dlp_sidecar_toggle_inputs() {
        inputs.insert(toggle.to_string(), false_binding());
    }
    inputs.insert("split_chapters".to_string(), false_binding());

    let has_primary_or_sandbox = output_configs
        .iter()
        .any(|config| matches!(config.kind, YtDlpOutputKind::Primary | YtDlpOutputKind::Sandbox));
    if !has_primary_or_sandbox {
        inputs.insert("skip_download".to_string(), true_binding());
    }

    for config in output_configs {
        match config.kind {
            YtDlpOutputKind::Primary => {}
            YtDlpOutputKind::Sandbox => {
                for toggle in yt_dlp_sidecar_toggle_inputs() {
                    inputs.insert(toggle.to_string(), true_binding());
                }
                inputs.insert("embed_chapters".to_string(), true_binding());
                inputs.insert("split_chapters".to_string(), true_binding());
            }
            YtDlpOutputKind::Subtitles => {
                inputs.insert("write_subs".to_string(), true_binding());
            }
            YtDlpOutputKind::Thumbnails => {
                inputs.insert("write_thumbnail".to_string(), true_binding());
            }
            YtDlpOutputKind::Description | YtDlpOutputKind::PlaylistDescription => {
                inputs.insert("write_description".to_string(), true_binding());
            }
            YtDlpOutputKind::Annotation => {
                inputs.insert("write_annotations".to_string(), true_binding());
            }
            YtDlpOutputKind::Infojson | YtDlpOutputKind::PlaylistInfojson => {
                inputs.insert("write_info_json".to_string(), true_binding());
            }
            YtDlpOutputKind::Comment => {
                inputs.insert("write_comments".to_string(), true_binding());
                inputs.insert("write_info_json".to_string(), true_binding());
            }
            YtDlpOutputKind::Archive => {
                merge_yt_dlp_scalar_override(
                    &mut inputs,
                    "download_archive",
                    YT_DLP_MANAGED_ARCHIVE_FILE,
                )?;
            }
            YtDlpOutputKind::Links => {
                inputs.insert("write_link".to_string(), true_binding());
                inputs.insert("write_url_link".to_string(), true_binding());
                inputs.insert("write_webloc_link".to_string(), true_binding());
                inputs.insert("write_desktop_link".to_string(), true_binding());
            }
            YtDlpOutputKind::Chapters => {
                inputs.insert("write_chapters".to_string(), true_binding());
                inputs.insert("split_chapters".to_string(), true_binding());
            }
        }

        if let Some(sub_format) = config.sub_format.as_deref() {
            merge_yt_dlp_scalar_override(&mut inputs, "sub_format", sub_format)?;
        }
        if let Some(convert) = config.convert.as_deref() {
            merge_yt_dlp_scalar_override(
                &mut inputs,
                yt_dlp_convert_input_name_for_kind(config.kind),
                convert,
            )?;
        }
    }

    Ok(inputs)
}

/// Merges one scalar yt-dlp per-variant override while rejecting conflicts.
fn merge_yt_dlp_scalar_override(
    inputs: &mut BTreeMap<String, InputBinding>,
    key: &str,
    value: &str,
) -> Result<(), MediaPmError> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Ok(());
    }

    match inputs.get(key) {
        Some(InputBinding::String(existing)) if existing == normalized => Ok(()),
        Some(InputBinding::String(existing)) => Err(MediaPmError::Workflow(format!(
            "yt-dlp multi-output step has conflicting '{key}' values: '{existing}' vs '{normalized}'"
        ))),
        Some(_) => Err(MediaPmError::Workflow(format!(
            "yt-dlp multi-output step cannot merge non-scalar input override for '{key}'"
        ))),
        None => {
            inputs.insert(key.to_string(), InputBinding::String(normalized.to_string()));
            Ok(())
        }
    }
}

/// Resolves yt-dlp input name used for `convert` override semantics.
#[must_use]
fn yt_dlp_convert_input_name_for_kind(kind: YtDlpOutputKind) -> &'static str {
    match kind {
        YtDlpOutputKind::Subtitles => "convert_subs",
        YtDlpOutputKind::Thumbnails => "convert_thumbnails",
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

    if matches!(tool, MediaStepTool::MediaTagger) && key == "output_container" {
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
        TransformInputValue::String(value) => InputBinding::String(value.clone()),
        TransformInputValue::StringList(_) => unreachable!("list values are rejected above"),
    }))
}

/// Resolves active immutable tool id for one logical tool name.
fn resolve_step_tool_id(
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
    step_tool: MediaStepTool,
) -> Result<String, MediaPmError> {
    if matches!(step_tool, MediaStepTool::Import) {
        return resolve_builtin_tool_id(machine, "import", "1.0.0");
    }

    resolve_active_logical_tool_id(lock, machine, step_tool.as_str())
}

/// Resolves one active immutable tool id for a logical tool name and validates
/// machine-config presence.
fn resolve_active_logical_tool_id(
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
    logical_tool_name: &str,
) -> Result<String, MediaPmError> {
    let tool_id = lock.active_tools.get(logical_tool_name).cloned().ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "logical tool '{logical_tool_name}' is required but not active; add it under mediapm.ncl tools and run tool sync"
        ))
    })?;

    if !machine.tools.contains_key(&tool_id) {
        return Err(MediaPmError::Workflow(format!(
            "logical tool '{logical_tool_name}' resolves to active tool '{tool_id}', but that tool is missing from conductor machine config"
        )));
    }

    Ok(tool_id)
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
                "builtin tool '{builtin_name}@{builtin_version}' is required but not registered in conductor machine config"
            ))
        })
}

/// Maps one value-driven yt-dlp output kind to generated output capture name.
#[must_use]
fn yt_dlp_output_name_for_kind(
    kind: YtDlpOutputKind,
    capture_kind: OutputCaptureKind,
) -> &'static str {
    match kind {
        YtDlpOutputKind::Primary => {
            if matches!(capture_kind, OutputCaptureKind::Folder) {
                OUTPUT_SANDBOX_ARTIFACTS
            } else {
                OUTPUT_PRIMARY
            }
        }
        YtDlpOutputKind::Sandbox => OUTPUT_SANDBOX_ARTIFACTS,
        YtDlpOutputKind::Subtitles => OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
        YtDlpOutputKind::Thumbnails => OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
        YtDlpOutputKind::Description => {
            if matches!(capture_kind, OutputCaptureKind::Folder) {
                OUTPUT_SANDBOX_ARTIFACTS
            } else {
                OUTPUT_YT_DLP_DESCRIPTION_FILE
            }
        }
        YtDlpOutputKind::Annotation => {
            if matches!(capture_kind, OutputCaptureKind::Folder) {
                OUTPUT_SANDBOX_ARTIFACTS
            } else {
                OUTPUT_YT_DLP_ANNOTATION_FILE
            }
        }
        YtDlpOutputKind::Archive => OUTPUT_YT_DLP_ARCHIVE_FILE,
        YtDlpOutputKind::Infojson | YtDlpOutputKind::Comment => {
            if matches!(capture_kind, OutputCaptureKind::Folder) {
                OUTPUT_SANDBOX_ARTIFACTS
            } else {
                OUTPUT_YT_DLP_INFOJSON_FILE
            }
        }
        YtDlpOutputKind::Links => OUTPUT_YT_DLP_LINK_ARTIFACTS,
        YtDlpOutputKind::Chapters => OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
        YtDlpOutputKind::PlaylistDescription => OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE,
        YtDlpOutputKind::PlaylistInfojson => OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE,
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

    let policy = OutputPolicy { save: conductor_output_save_mode(options.save) };

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

/// Computes one expanded conductor-step index within a single mediapm step.
///
/// The expanded index is deterministic by mapping order and per-mapping
/// expansion size:
///
/// - `mapping_index * expanded_steps_per_mapping + step_offset_within_mapping`.
#[must_use]
pub(super) fn expanded_step_index_for_mapping(
    mapping_index: usize,
    expanded_steps_per_mapping: usize,
    step_offset_within_mapping: usize,
) -> usize {
    (mapping_index * expanded_steps_per_mapping) + step_offset_within_mapping
}

/// Builds deterministic step id for one expanded conductor step.
///
/// Id format preserves a stable two-number prefix:
/// `<mediapm_step_index>-<expanded_step_index_within_step>-<tool>`.
///
/// The expanded index already encodes mapping position uniquely when multiple
/// input/output mappings exist within one step, so input/output variant names
/// are omitted to keep ids concise.
fn media_step_id(
    step_index: usize,
    expanded_step_index: usize,
    tool: MediaStepTool,
    _mapping: &ResolvedStepVariantFlow,
) -> String {
    format!("{step_index}-{expanded_step_index}-{}", sanitize_identifier(tool.as_str()))
}

/// Builds deterministic step id for one aggregated ffmpeg media step.
fn ffmpeg_step_id(step_index: usize) -> String {
    format!("{step_index}-0-ffmpeg")
}

/// Builds deterministic step id for one aggregated yt-dlp media step.
fn yt_dlp_step_id(step_index: usize) -> String {
    format!("{step_index}-0-yt-dlp")
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
mod tests;
