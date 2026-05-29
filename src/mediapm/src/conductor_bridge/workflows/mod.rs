//! Per-media workflow synthesis for `mediapm` documents.
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
    AddExternalDataOptions, ExternalContentRef, InputBinding, MachineNickelDocument,
    OutputSaveMode, ToolKindSpec, WorkflowSpec,
};
use serde_json::Value;

use crate::config::{
    ManagedWorkflowStepState, MediaPmDocument, MediaPmImpureTimestamp, MediaSourceSpec, MediaStep,
    MediaStepTool, OutputSaveConfig, ResolvedStepVariantFlow, ToolRequirement, TransformInputValue,
    expand_variant_selectors, media_source_uri, normalize_selector_compare_value,
    resolve_step_variant_flow,
};
use crate::error::MediaPmError;
use crate::lockfile::MediaLockFile;
use crate::paths::MediaPmPaths;
use crate::tools::catalog::tool_catalog_entry;
use crate::tools::downloader::{ProvisionedToolPayload, ResolvedToolIdentity};

use super::documents::{load_machine_document, save_machine_document};
use super::tool_runtime::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS, DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, FfmpegSlotLimits,
    build_tool_spec, default_tool_config_description, ffmpeg_cover_slot_enabled_input_name,
    ffmpeg_input_content_name, ffmpeg_output_capture_name, ffmpeg_output_file_path,
    ffmpeg_output_path_input_name, merge_tool_config_defaults, resolve_ffmpeg_slot_limits,
};

mod ffmpeg;
mod media_tagger;
mod rsgain;
mod synthesis;
mod yt_dlp;
mod yt_dlp_inputs;

use self::synthesis::synthesize_media_steps;
use self::yt_dlp_inputs::resolve_step_output_binding;

/// Prefix for default `mediapm`-managed workflow ids in machine documents.
const MANAGED_WORKFLOW_PREFIX: &str = "mediapm.media.";
/// Prefix for `mediapm`-managed external-data descriptions.
const MANAGED_EXTERNAL_DESCRIPTION_PREFIX: &str = "managed external data:";
/// Legacy prefix used by older local-variant-only managed external-data rows.
const LEGACY_MANAGED_EXTERNAL_DESCRIPTION_PREFIX: &str = "managed local variant source for media";

/// Output name exposed by generated executable tool contracts.
const OUTPUT_CONTENT: &str = "content";
/// Output name exposed by the builtin import tool contract.
const OUTPUT_IMPORT_RESULT: &str = "result";
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
/// - updates `lock.workflow_states` refresh metadata for each
///   `media.<id>.steps[<index>]` row,
/// - prunes stale media refresh rows when sources are removed.
pub(crate) fn reconcile_media_workflows(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    lock: &mut MediaLockFile,
) -> Result<(), MediaPmError> {
    reconcile_media_workflows_with_mode(paths, document, lock, false)
}

/// Reconciles managed media workflows while permitting unresolved-tool
/// placeholders for config-only edit flows.
///
/// This mode is used by source/hierarchy configuration commands that mutate
/// `mediapm.ncl` without running tool provisioning. It keeps generated
/// conductor machine workflows populated for inspection by synthesizing
/// placeholder active tools only when required logical tools are missing.
pub(crate) fn reconcile_media_workflows_for_config_edits(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    lock: &mut MediaLockFile,
) -> Result<(), MediaPmError> {
    reconcile_media_workflows_with_mode(paths, document, lock, true)
}

/// Reconciles one workflow per media source into machine config with optional
/// unresolved-tool placeholder fallback.
fn reconcile_media_workflows_with_mode(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    lock: &mut MediaLockFile,
    allow_unresolved_tool_placeholders: bool,
) -> Result<(), MediaPmError> {
    let mut machine = load_machine_document(&paths.conductor_machine_ncl)?;
    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(&document.tools)?;
    if allow_unresolved_tool_placeholders {
        ensure_active_tool_placeholders_for_media_steps(
            paths,
            document,
            &mut machine,
            lock,
            ffmpeg_slot_limits,
        )?;
    }
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

/// Ensures required logical media tools are represented by active tool ids.
///
/// When add/remove config commands run before explicit tool sync, this helper
/// seeds deterministic unresolved placeholders so workflow synthesis can still
/// build managed workflow rows for examples and config inspection.
fn ensure_active_tool_placeholders_for_media_steps(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    machine: &mut MachineNickelDocument,
    lock: &mut MediaLockFile,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    for logical_tool_name in required_logical_tool_names_for_media_steps(document) {
        ensure_active_tool_placeholder(
            paths,
            machine,
            lock,
            logical_tool_name,
            ffmpeg_slot_limits,
        )?;
    }

    Ok(())
}

/// Returns logical tool names required by configured media-step synthesis.
#[must_use]
fn required_logical_tool_names_for_media_steps(
    document: &MediaPmDocument,
) -> BTreeSet<&'static str> {
    let mut required = BTreeSet::new();

    for source in document.media.values() {
        for step in &source.steps {
            match step.tool {
                MediaStepTool::YtDlp => {
                    required.insert("yt-dlp");
                }
                MediaStepTool::Import => {}
                MediaStepTool::Ffmpeg => {
                    required.insert("ffmpeg");
                }
                MediaStepTool::Rsgain => {
                    required.insert("rsgain");
                    required.insert("ffmpeg");
                    required.insert("sd");
                }
                MediaStepTool::MediaTagger => {
                    required.insert("media-tagger");
                    required.insert("ffmpeg");
                }
            }
        }
    }

    required
}

/// Ensures one logical media tool maps to a machine-visible active tool id.
fn ensure_active_tool_placeholder(
    paths: &MediaPmPaths,
    machine: &mut MachineNickelDocument,
    lock: &mut MediaLockFile,
    logical_tool_name: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    if lock
        .active_tools
        .get(logical_tool_name)
        .is_some_and(|active_tool_id| machine.tools.contains_key(active_tool_id))
    {
        return Ok(());
    }

    let placeholder_tool_id = unresolved_placeholder_tool_id(logical_tool_name);
    let placeholder_payload =
        unresolved_placeholder_payload(logical_tool_name, &placeholder_tool_id)?;
    if !machine.tools.contains_key(&placeholder_tool_id) {
        machine.tools.insert(
            placeholder_tool_id.clone(),
            build_tool_spec(paths, logical_tool_name, &placeholder_payload, ffmpeg_slot_limits),
        );
    }
    machine.tool_configs.insert(
        placeholder_tool_id.clone(),
        merge_tool_config_defaults(
            machine.tool_configs.get(&placeholder_tool_id),
            paths,
            logical_tool_name,
            BTreeMap::new(),
            default_tool_config_description(
                logical_tool_name,
                &placeholder_payload.identity,
                placeholder_payload.catalog.description,
            ),
            ffmpeg_slot_limits,
        ),
    );
    lock.active_tools.insert(logical_tool_name.to_string(), placeholder_tool_id);

    Ok(())
}

/// Builds deterministic placeholder id for one unresolved logical tool name.
#[must_use]
fn unresolved_placeholder_tool_id(logical_tool_name: &str) -> String {
    format!(
        "mediapm.tools.{}+mediapm-unresolved@latest",
        logical_tool_name.trim().to_ascii_lowercase()
    )
}

/// Builds placeholder provisioned payload for unresolved logical tools.
fn unresolved_placeholder_payload(
    logical_tool_name: &str,
    placeholder_tool_id: &str,
) -> Result<ProvisionedToolPayload, MediaPmError> {
    let catalog = tool_catalog_entry(logical_tool_name)?;
    Ok(ProvisionedToolPayload {
        tool_id: placeholder_tool_id.to_string(),
        command_selector: format!("unresolved/{logical_tool_name}"),
        content_entries: BTreeMap::new(),
        identity: ResolvedToolIdentity::default(),
        source_label: "mediapm unresolved placeholder".to_string(),
        source_identifier: "mediapm-unresolved".to_string(),
        catalog,
        warnings: Vec::new(),
    })
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

    lock.workflow_states
        .retain(|tracked_media_id, _| document.media.contains_key(tracked_media_id));

    Ok(plan)
}

/// Serializes one explicit user-facing media step config snapshot.
fn explicit_media_step_config_snapshot(step: &MediaStep) -> Result<Value, MediaPmError> {
    serde_json::to_value(step).map_err(|error| {
        MediaPmError::Serialization(format!(
            "encoding explicit media step config snapshot failed: {error}"
        ))
    })
}

/// Finds the first exact step-state match after `start_index`.
#[must_use]
fn find_matching_step_state_index(
    existing_states: &[ManagedWorkflowStepState],
    start_index: usize,
    explicit_config: &Value,
) -> Option<usize> {
    existing_states
        .iter()
        .enumerate()
        .skip(start_index)
        .find_map(|(index, state)| (state.explicit_config == *explicit_config).then_some(index))
}

/// Returns true when one matched step-state still requires refresh.
#[must_use]
fn matched_state_requires_refresh(existing_state: Option<&ManagedWorkflowStepState>) -> bool {
    existing_state.is_none_or(|state| state.impure_timestamp.is_none())
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

/// Resolves selected dependency tool id for managed steps that depend on
/// additional logical tools.
///
/// Dependency class policy:
/// - cross-step dependency (this helper): resolve one standalone conductor tool
///   id per dependent step, do not inline dependency content-map bytes into the
///   requesting tool, and do not mutate the requesting tool id with dependency
///   selector fragments;
/// - same-step companion dependency: handled in sync/tool-config synthesis where
///   dependency bytes are inlined and requesting tool ids encode dependency
///   selector identity.
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
fn extract_step_list_args(step: &MediaStep) -> (Vec<String>, Vec<String>) {
    let leading_args = match step.options.get(INPUT_LEADING_ARGS) {
        Some(TransformInputValue::String(value)) => split_option_args(value),
        None => Vec::new(),
    };

    let trailing_args = match step.options.get(INPUT_TRAILING_ARGS) {
        Some(TransformInputValue::String(value)) => split_option_args(value),
        None => Vec::new(),
    };

    (leading_args, trailing_args)
}

/// Splits one whitespace-delimited option-arg string into argv items.
#[must_use]
fn split_option_args(value: &str) -> Vec<String> {
    value.split_whitespace().map(ToString::to_string).collect::<Vec<_>>()
}

/// Builds deterministic tool input bindings from one step tool/options map.
fn step_option_input_bindings(
    tool: MediaStepTool,
    options: &BTreeMap<String, TransformInputValue>,
) -> BTreeMap<String, InputBinding> {
    let mut input_bindings = BTreeMap::new();

    for (key, value) in options {
        if let Some(binding) = map_step_option_input_binding(tool, key, value) {
            input_bindings.insert(key.clone(), binding);
        }
    }

    input_bindings
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
) -> Option<InputBinding> {
    if matches!(tool, MediaStepTool::YtDlp) && key == "uri" {
        return None;
    }

    if matches!(tool, MediaStepTool::MediaTagger) && key == "output_container" {
        return None;
    }

    if matches!(key, INPUT_LEADING_ARGS | INPUT_TRAILING_ARGS) {
        return None;
    }

    if key == "option_args" {
        let items = match value {
            TransformInputValue::String(value) => split_option_args(value),
        };
        return Some(InputBinding::StringList(items));
    }

    Some(match value {
        TransformInputValue::String(value) => InputBinding::String(value.clone()),
    })
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
