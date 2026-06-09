//! Per-media workflow synthesis for `mediapm` documents.
//!
//! This module translates ordered `media.<id>.steps` declarations into managed
//! conductor workflows so each media id maps to exactly one workflow.
//! Variant-flow dependencies are expressed with explicit `${step_output...}`
//! bindings plus matching `depends_on` edges, allowing independent branches to
//! execute as soon as their producer data is ready.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::Hash;
use mediapm_conductor::{
    AddExternalDataOptions, ExternalContentRef, InputBinding, MachineNickelDocument,
    OutputSaveMode, ToolKindSpec, WorkflowSpec,
};
use serde_json::Value;

use crate::config::MediaPmState;
use crate::config::{
    ManagedWorkflowStepState, MediaPmDocument, MediaPmImpureTimestamp, MediaSourceSpec, MediaStep,
    MediaStepTool, OutputSaveConfig, ResolvedStepVariantFlow, ToolRequirement, TransformInputValue,
    expand_variant_selectors, media_source_uri, normalize_selector_compare_value,
    resolve_step_variant_flow,
};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::tools::catalog::tool_catalog_entry;
use crate::tools::downloader::{ProvisionedToolPayload, ResolvedToolIdentity};

use super::documents::{
    load_machine_document, register_missing_builtin_tools, save_machine_document,
};
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
    lock: &mut MediaPmState,
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
#[allow(dead_code)]
pub(crate) fn reconcile_media_workflows_for_config_edits(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    lock: &mut MediaPmState,
) -> Result<(), MediaPmError> {
    reconcile_media_workflows_with_mode(paths, document, lock, true)
}

/// Reconciles one workflow per media source into machine config with optional
/// unresolved-tool placeholder fallback.
fn reconcile_media_workflows_with_mode(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    lock: &mut MediaPmState,
    allow_unresolved_tool_placeholders: bool,
) -> Result<(), MediaPmError> {
    let mut machine = load_machine_document(&paths.conductor_machine_ncl)?;
    register_missing_builtin_tools(&mut machine);

    // Propagate instance_ttl_seconds from mediapm document to conductor
    // machine config so the effective value (CLI override > config file > None)
    // reaches the conductor coordinator's GC TTL resolution.
    if let Some(ttl) = document.runtime.instance_ttl_seconds {
        machine.runtime.instance_ttl_seconds = Some(ttl);
    }

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
    lock: &mut MediaPmState,
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
    lock: &mut MediaPmState,
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
    lock: &MediaPmState,
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
    lock: &MediaPmState,
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
    lock: &mut MediaPmState,
    machine: &MachineNickelDocument,
) -> Result<MediaWorkflowPlan, MediaPmError> {
    build_media_workflow_plan_with_limits(document, lock, machine, FfmpegSlotLimits::default())
}

/// Builds the full managed workflow/external-data plan from `mediapm` config.
fn build_media_workflow_plan_with_limits(
    document: &MediaPmDocument,
    lock: &mut MediaPmState,
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

/// Builds a hash map index from existing step states keyed by blake3 hash of explicit config.
#[must_use]
fn build_explicit_config_index(
    existing_states: &[ManagedWorkflowStepState],
) -> HashMap<u64, Vec<usize>> {
    let mut index: HashMap<u64, Vec<usize>> = HashMap::new();
    for (idx, state) in existing_states.iter().enumerate() {
        if let Ok(bytes) = serde_json::to_vec(&state.explicit_config) {
            let hash = blake3::hash(&bytes);
            let key = u64::from_le_bytes(*hash.as_bytes().first_chunk::<8>().unwrap());
            index.entry(key).or_default().push(idx);
        }
    }
    index
}

/// Finds one matching step state using a pre-built config hash index.
///
/// Looks up `explicit_config` by its blake3 hash in the index and removes one
/// matching position, ensuring each existing state is matched at most once.
#[must_use]
fn find_matching_step_state_index(
    index: &mut HashMap<u64, Vec<usize>>,
    explicit_config: &Value,
) -> Option<usize> {
    let Ok(bytes) = serde_json::to_vec(explicit_config) else {
        return None;
    };
    let hash = blake3::hash(&bytes);
    let key = u64::from_le_bytes(*hash.as_bytes().first_chunk::<8>().unwrap());
    let positions = index.get_mut(&key)?;
    if positions.is_empty() {
        return None;
    }
    Some(positions.remove(0))
}

/// Returns true when one matched step-state still requires refresh.
#[must_use]
fn matched_state_requires_refresh(existing_state: Option<&ManagedWorkflowStepState>) -> bool {
    existing_state.is_none_or(|state| state.impure_timestamp.is_none())
}

/// Generates one fresh monotonic impure timestamp for mediapm step refresh.
#[must_use]
pub(crate) fn fresh_impure_timestamp() -> MediaPmImpureTimestamp {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    MediaPmImpureTimestamp { epoch_seconds: now.as_secs(), subsec_nanos: now.subsec_nanos() }
}

/// Rewrites generated step tool ids using an existing workflow snapshot.
///
/// When a generated step's tool id differs from the previous one but the
/// previous tool id is still valid (present and executable in machine config),
/// the previous id is preserved to avoid unnecessary cache invalidation on
/// tool version updates.
///
/// Returns `true` when every generated step id was found in `existing` and
/// pinned to a still-valid prior immutable tool id present in current machine
/// configuration.
fn preserve_existing_generated_step_tools(
    workflow: &mut WorkflowSpec,
    generated_start: usize,
    existing: Option<&WorkflowSpec>,
    machine: &MachineNickelDocument,
) -> bool {
    let Some(existing) = existing else {
        return false;
    };

    let mut all_matched = true;
    for generated in workflow.steps.iter_mut().skip(generated_start) {
        if let Some(previous) = existing.steps.iter().find(|candidate| candidate.id == generated.id)
        {
            if previous.tool == generated.tool {
                if !preserved_step_tool_is_valid(machine, &previous.tool) {
                    all_matched = false;
                }
            } else if preserved_step_tool_is_valid(machine, &previous.tool) {
                // Tool identity changed but the previous tool id is still
                // valid — preserve it to avoid unnecessary cache invalidation
                // when only the tool version has changed.
                generated.tool = previous.tool.clone();
            } else {
                // Tool identity changed and the previous tool id is no longer
                // valid — the generated identity is the correct one for the
                // current configuration.
                all_matched = false;
            }
        } else {
            all_matched = false;
        }
    }

    all_matched
}

/// Returns whether one preserved step tool id is still present in the current
/// machine configuration.
///
/// A tool id is valid if it exists in `machine.tools` and — for executable
/// tools — has a `tool_configs` entry, indicating it was once reconciled and
/// may have cached conductor outputs (the content map may have been cleared
/// since then). Builtin tools are always valid.
#[must_use]
fn preserved_step_tool_is_valid(machine: &MachineNickelDocument, tool_id: &str) -> bool {
    let Some(tool_spec) = machine.tools.get(tool_id) else {
        return false;
    };

    match &tool_spec.kind {
        ToolKindSpec::Builtin { .. } => true,
        ToolKindSpec::Executable { .. } => machine.tool_configs.contains_key(tool_id),
    }
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
                        "{}-ffmpeg-apply",
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
///   selector fragments; each dependent step should reference that resolved
///   dependency tool id directly;
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
    lock: &MediaPmState,
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
    lock: &MediaPmState,
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
    lock: &MediaPmState,
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

    if !is_unresolved_placeholder_tool_id(&tool_id)
        && let Some(tool_spec) = machine.tools.get(&tool_id)
        && matches!(tool_spec.kind, ToolKindSpec::Executable { .. })
        && machine
            .tool_configs
            .get(&tool_id)
            .and_then(|config| config.content_map.as_ref())
            .is_none_or(BTreeMap::is_empty)
    {
        return Err(MediaPmError::Workflow(format!(
            "logical tool '{logical_tool_name}' resolves to active tool '{tool_id}', but that executable tool has no materialized content_map; run mediapm tool sync for '{logical_tool_name}'"
        )));
    }

    Ok(tool_id)
}

#[must_use]
fn is_unresolved_placeholder_tool_id(tool_id: &str) -> bool {
    tool_id.contains("+mediapm-unresolved@")
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
mod tests {
    //! Unit tests for managed workflow synthesis and variant binding behavior.

    use std::collections::BTreeMap;

    use mediapm_cas::Hash;
    use mediapm_conductor::{
        InputBinding, MachineNickelDocument, OutputPolicy, OutputSaveMode, ToolConfigSpec,
        ToolKindSpec, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    };
    use serde_json::{Value, json};

    use crate::config::MediaPmState;
    use crate::config::{
        ManagedFileRecord, ManagedWorkflowStepState, MediaPmDocument, MediaPmImpureTimestamp,
        MediaSourceSpec, MediaStep, MediaStepTool, ToolRequirement, ToolRequirementDependencies,
        TransformInputValue,
    };

    use crate::conductor_bridge::workflows::{
        MANAGED_EXTERNAL_DESCRIPTION_PREFIX, MANAGED_WORKFLOW_PREFIX, build_media_workflow_plan,
        build_media_workflow_plan_and_update_state,
        collect_managed_external_data_from_machine_and_lock, resolve_media_variant_output_binding,
        resolve_media_variant_output_binding_with_limits, step_option_input_bindings,
        upsert_managed_external_data,
    };

    fn generic_output_variant(kind: &str) -> Value {
        json!({ "kind": kind, "save": "full" })
    }

    fn ffmpeg_output_variant(idx: u32) -> Value {
        json!({ "kind": "primary", "save": "full", "idx": idx })
    }

    fn ffmpeg_output_variant_with_extension(idx: u32, extension: &str) -> Value {
        json!({
            "kind": "primary",
            "save": "full",
            "idx": idx,
            "extension": extension,
        })
    }

    fn yt_dlp_output_variant(kind: &str) -> Value {
        json!({ "kind": kind, "save": "full" })
    }

    fn executable_tool_spec(command: &str) -> ToolSpec {
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec![command.to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        }
    }

    fn machine_with_active_tool_specs(lock: &MediaPmState) -> MachineNickelDocument {
        let mut machine = MachineNickelDocument::default();

        for (logical_name, tool_id) in &lock.active_tools {
            let command = match logical_name.as_str() {
                "yt-dlp" => "yt-dlp",
                "ffmpeg" => "ffmpeg",
                "rsgain" => "rsgain",
                "sd" => "sd",
                "media-tagger" => "media-tagger",
                _ => "tool",
            };

            machine.tools.insert(tool_id.clone(), executable_tool_spec(command));
            machine.tool_configs.insert(
                tool_id.clone(),
                ToolConfigSpec {
                    content_map: Some(BTreeMap::from([(
                        format!("linux/{command}"),
                        Hash::from_content(format!("{tool_id}:{command}").as_bytes()),
                    )])),
                    ..ToolConfigSpec::default()
                },
            );
        }

        machine
    }

    fn single_step_yt_dlp_source(output_kind: &str) -> MediaSourceSpec {
        MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "default".to_string(),
                    yt_dlp_output_variant(output_kind),
                )]),
                options: BTreeMap::from([(
                    "uri".to_string(),
                    TransformInputValue::String("https://example.com/video".to_string()),
                )]),
            }],
        }
    }

    /// Protects one-workflow-per-media synthesis and managed id namespace.
    #[test]
    fn plan_builds_exactly_one_workflow_per_media() {
        let document = MediaPmDocument {
        media: BTreeMap::from([
            (
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                    id: None,
                    description: Some("custom media description".to_string()),
                    title: None,
                    artist: None,
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

        let lock = MediaPmState::default();
        let machine = machine_with_active_tool_specs(&lock);
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
            plan.external_data
                .values()
                .all(|reference| { reference.save == Some(OutputSaveMode::Saved) })
        );
        assert!(
            plan.workflows
                .keys()
                .any(|workflow_id| workflow_id.starts_with(MANAGED_WORKFLOW_PREFIX))
        );
    }

    /// Protects mediapm incremental behavior by preserving prior immutable tool
    /// ids when explicit step config is unchanged and the old tool id is still
    /// valid in machine config. This avoids unnecessary cache invalidation on
    /// tool version updates.
    #[test]
    fn unchanged_step_config_preserves_old_tool_when_generated_tool_changes() {
        let old_tool = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@old".to_string();
        let new_tool = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@new".to_string();
        let media_id = "archive-a".to_string();
        let source = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
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
                    "default".to_string(),
                    ffmpeg_output_variant(0),
                )]),
                options: BTreeMap::new(),
            }],
        };
        let explicit_snapshot =
            serde_json::to_value(&source.steps[0]).expect("serialize explicit step config");
        let preserved_timestamp = MediaPmImpureTimestamp { epoch_seconds: 10, subsec_nanos: 20 };

        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.clone(), source)]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaPmState {
            active_tools: BTreeMap::from([("ffmpeg".to_string(), new_tool.clone())]),
            workflow_states: BTreeMap::from([(
                media_id.clone(),
                vec![ManagedWorkflowStepState {
                    explicit_config: explicit_snapshot.clone(),
                    impure_timestamp: Some(preserved_timestamp),
                }],
            )]),
            ..MediaPmState::default()
        };

        let mut machine = machine_with_active_tool_specs(&lock);
        machine.tools.insert(old_tool.clone(), executable_tool_spec("ffmpeg"));
        machine.tool_configs.insert(
            old_tool.clone(),
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([(
                    "linux/ffmpeg".to_string(),
                    Hash::from_content(b"ffmpeg-old"),
                )])),
                ..ToolConfigSpec::default()
            },
        );
        machine.workflows.insert(
            format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "0-0-ffmpeg".to_string(),
                    tool: old_tool.clone(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([("primary".to_string(), OutputPolicy { save: None })]),
                }],
                ..WorkflowSpec::default()
            },
        );

        let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
            .expect("plan should succeed");
        let workflow =
            plan.workflows.get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")).expect("workflow");

        assert_eq!(workflow.steps.len(), 1);
        assert_eq!(workflow.steps[0].tool, old_tool);

        let stored = lock
            .workflow_states
            .get(&media_id)
            .and_then(|steps| steps.first())
            .expect("stored step refresh state");
        assert_eq!(stored.explicit_config, explicit_snapshot);
        assert!(stored.impure_timestamp.is_some());
    }

    /// Protects tool identity preservation semantics by verifying that when
    /// companion suffixes change and the previous tool id is still valid, the
    /// old tool id is preserved to avoid unnecessary cache invalidation.
    #[test]
    fn unchanged_yt_dlp_step_config_preserves_old_tool_when_companion_suffix_changes() {
        let old_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp+ffmpeg-old+deno-old@old"
            .to_string();
        let new_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp+ffmpeg-new+deno-new@new"
            .to_string();
        let media_id = "archive-companion-refresh".to_string();
        let source = single_step_yt_dlp_source("subtitles");
        let explicit_snapshot =
            serde_json::to_value(&source.steps[0]).expect("serialize explicit step config");

        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.clone(), source)]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaPmState {
            active_tools: BTreeMap::from([("yt-dlp".to_string(), new_tool.clone())]),
            workflow_states: BTreeMap::from([(
                media_id.clone(),
                vec![ManagedWorkflowStepState {
                    explicit_config: explicit_snapshot.clone(),
                    impure_timestamp: Some(MediaPmImpureTimestamp {
                        epoch_seconds: 10,
                        subsec_nanos: 20,
                    }),
                }],
            )]),
            ..MediaPmState::default()
        };

        let mut machine = machine_with_active_tool_specs(&lock);
        machine.tools.insert(old_tool.clone(), executable_tool_spec("yt-dlp"));
        machine.tool_configs.insert(
            old_tool.clone(),
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([(
                    "linux/yt-dlp".to_string(),
                    Hash::from_content(b"yt-dlp-old"),
                )])),
                ..ToolConfigSpec::default()
            },
        );
        machine.workflows.insert(
            format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "0-0-yt-dlp".to_string(),
                    tool: old_tool.clone(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([(
                        "yt_dlp_subtitle_artifacts".to_string(),
                        OutputPolicy { save: None },
                    )]),
                }],
                ..WorkflowSpec::default()
            },
        );

        let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
            .expect("plan should succeed");
        let workflow =
            plan.workflows.get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")).expect("workflow");

        assert_eq!(workflow.steps.len(), 1);
        assert_eq!(workflow.steps[0].tool, old_tool);
    }

    /// Protects refresh gating by forcing refresh when explicit user-facing step
    /// config changes.
    #[test]
    fn changed_step_config_forces_refresh_to_active_tool() {
        let old_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@old".to_string();
        let new_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@new".to_string();
        let media_id = "refresh-on-config-change".to_string();

        let old_source = single_step_yt_dlp_source("subtitles");
        let old_snapshot =
            serde_json::to_value(&old_source.steps[0]).expect("serialize old explicit step config");
        let new_source = single_step_yt_dlp_source("primary");
        let new_snapshot =
            serde_json::to_value(&new_source.steps[0]).expect("serialize new explicit step config");
        assert_ne!(old_snapshot, new_snapshot);

        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.clone(), new_source)]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaPmState {
            active_tools: BTreeMap::from([("yt-dlp".to_string(), new_tool.clone())]),
            workflow_states: BTreeMap::from([(
                media_id.clone(),
                vec![ManagedWorkflowStepState {
                    explicit_config: old_snapshot,
                    impure_timestamp: Some(MediaPmImpureTimestamp {
                        epoch_seconds: 1,
                        subsec_nanos: 2,
                    }),
                }],
            )]),
            ..MediaPmState::default()
        };

        let mut machine = machine_with_active_tool_specs(&lock);
        machine.tools.insert(old_tool.clone(), executable_tool_spec("yt-dlp"));
        machine.tool_configs.insert(
            old_tool.clone(),
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([(
                    "linux/yt-dlp".to_string(),
                    Hash::from_content(b"yt-dlp-old-forward-scan"),
                )])),
                ..ToolConfigSpec::default()
            },
        );
        machine.workflows.insert(
            format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "0-0-yt-dlp".to_string(),
                    tool: old_tool,
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([("primary".to_string(), OutputPolicy { save: None })]),
                }],
                ..WorkflowSpec::default()
            },
        );

        let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
            .expect("plan should succeed");
        let workflow =
            plan.workflows.get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")).expect("workflow");

        assert_eq!(workflow.steps.len(), 1);
        assert_eq!(workflow.steps[0].tool, new_tool);

        let stored = lock
            .workflow_states
            .get(&media_id)
            .and_then(|steps| steps.first())
            .expect("stored step refresh state");
        assert_eq!(stored.explicit_config, new_snapshot);
        assert!(stored.impure_timestamp.is_some(), "refresh sets impure_timestamp unconditionally");
    }

    /// Protects refresh gating by forcing refresh when mediapm step impure
    /// timestamp is missing even when explicit step config is unchanged.
    #[test]
    fn missing_step_timestamp_forces_refresh_to_active_tool() {
        let old_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@old".to_string();
        let new_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@new".to_string();
        let media_id = "refresh-on-missing-timestamp".to_string();

        let source = single_step_yt_dlp_source("subtitles");
        let explicit_snapshot =
            serde_json::to_value(&source.steps[0]).expect("serialize explicit step config");
        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.clone(), source)]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaPmState {
            active_tools: BTreeMap::from([("yt-dlp".to_string(), new_tool.clone())]),
            workflow_states: BTreeMap::from([(
                media_id.clone(),
                vec![ManagedWorkflowStepState {
                    explicit_config: explicit_snapshot.clone(),
                    impure_timestamp: None,
                }],
            )]),
            ..MediaPmState::default()
        };

        let mut machine = machine_with_active_tool_specs(&lock);
        machine.tools.insert(old_tool.clone(), executable_tool_spec("yt-dlp"));
        machine.workflows.insert(
            format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "0-0-yt-dlp".to_string(),
                    tool: old_tool,
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([(
                        "yt_dlp_subtitle_artifacts".to_string(),
                        OutputPolicy { save: None },
                    )]),
                }],
                ..WorkflowSpec::default()
            },
        );

        let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
            .expect("plan should succeed");
        let workflow =
            plan.workflows.get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")).expect("workflow");

        assert_eq!(workflow.steps.len(), 1);
        assert_eq!(workflow.steps[0].tool, new_tool);

        let stored = lock
            .workflow_states
            .get(&media_id)
            .and_then(|steps| steps.first())
            .expect("stored step refresh state");
        assert_eq!(stored.explicit_config, explicit_snapshot);
        assert!(stored.impure_timestamp.is_some(), "refresh sets impure_timestamp unconditionally");
    }

    /// Protects unchanged-step reconciliation by refreshing to the current active
    /// tool id when the previously pinned immutable tool no longer has executable
    /// content-map bytes in machine config.
    #[test]
    fn unchanged_step_with_missing_previous_tool_content_refreshes_to_active_tool() {
        let old_tool = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@old".to_string();
        let new_tool = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@new".to_string();
        let media_id = "refresh-on-missing-previous-tool-content".to_string();
        let source = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
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
                    "default".to_string(),
                    ffmpeg_output_variant(0),
                )]),
                options: BTreeMap::new(),
            }],
        };
        let explicit_snapshot =
            serde_json::to_value(&source.steps[0]).expect("serialize explicit step config");

        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.clone(), source)]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaPmState {
            active_tools: BTreeMap::from([("ffmpeg".to_string(), new_tool.clone())]),
            workflow_states: BTreeMap::from([(
                media_id.clone(),
                vec![ManagedWorkflowStepState {
                    explicit_config: explicit_snapshot,
                    impure_timestamp: Some(MediaPmImpureTimestamp {
                        epoch_seconds: 11,
                        subsec_nanos: 22,
                    }),
                }],
            )]),
            ..MediaPmState::default()
        };

        let mut machine = machine_with_active_tool_specs(&lock);
        machine.tools.insert(old_tool.clone(), executable_tool_spec("ffmpeg"));
        machine.workflows.insert(
            format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "0-0-ffmpeg".to_string(),
                    tool: old_tool,
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([("primary".to_string(), OutputPolicy { save: None })]),
                }],
                ..WorkflowSpec::default()
            },
        );

        let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
            .expect("plan should succeed");
        let workflow =
            plan.workflows.get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")).expect("workflow");

        assert_eq!(workflow.steps.len(), 1);
        assert_eq!(workflow.steps[0].tool, new_tool);
    }

    /// Protects forward-scan state matching by ensuring synthesis stays stable
    /// when earlier steps diverge: later steps still match explicit config, but
    /// may refresh tool identity to the current active immutable id.
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "this regression test keeps full setup inline to make matching behavior auditable"
    )]
    fn forward_scan_matching_refreshes_later_step_tool_identity_when_needed() {
        let old_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@old".to_string();
        let new_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@new".to_string();
        let media_id = "forward-scan".to_string();

        let step0_old = MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([("v0".to_string(), yt_dlp_output_variant("primary"))]),
            options: BTreeMap::from([(
                "uri".to_string(),
                TransformInputValue::String("https://example.com/a".to_string()),
            )]),
        };
        let step0_new = MediaStep {
            options: BTreeMap::from([(
                "uri".to_string(),
                TransformInputValue::String("https://example.com/a?v=2".to_string()),
            )]),
            ..step0_old.clone()
        };
        let step1 = MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([("v1".to_string(), yt_dlp_output_variant("primary"))]),
            options: BTreeMap::from([(
                "uri".to_string(),
                TransformInputValue::String("https://example.com/b".to_string()),
            )]),
        };

        let step0_old_snapshot = serde_json::to_value(&step0_old).expect("serialize step0 old");
        let step1_snapshot = serde_json::to_value(&step1).expect("serialize step1");
        let step1_timestamp = MediaPmImpureTimestamp { epoch_seconds: 77, subsec_nanos: 88 };

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                media_id.clone(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![step0_new, step1.clone()],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaPmState {
            active_tools: BTreeMap::from([("yt-dlp".to_string(), new_tool.clone())]),
            workflow_states: BTreeMap::from([(
                media_id.clone(),
                vec![
                    ManagedWorkflowStepState {
                        explicit_config: step0_old_snapshot,
                        impure_timestamp: Some(MediaPmImpureTimestamp {
                            epoch_seconds: 1,
                            subsec_nanos: 2,
                        }),
                    },
                    ManagedWorkflowStepState {
                        explicit_config: step1_snapshot.clone(),
                        impure_timestamp: Some(step1_timestamp),
                    },
                ],
            )]),
            ..MediaPmState::default()
        };

        let mut machine = machine_with_active_tool_specs(&lock);
        machine.tools.insert(old_tool.clone(), executable_tool_spec("yt-dlp"));
        machine.workflows.insert(
            format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
            WorkflowSpec {
                steps: vec![
                    WorkflowStepSpec {
                        id: "0-0-yt-dlp".to_string(),
                        tool: old_tool.clone(),
                        inputs: BTreeMap::new(),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::from([(
                            "primary".to_string(),
                            OutputPolicy { save: None },
                        )]),
                    },
                    WorkflowStepSpec {
                        id: "1-0-yt-dlp".to_string(),
                        tool: old_tool.clone(),
                        inputs: BTreeMap::new(),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::from([(
                            "primary".to_string(),
                            OutputPolicy { save: None },
                        )]),
                    },
                ],
                ..WorkflowSpec::default()
            },
        );

        let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
            .expect("plan should succeed");
        let workflow =
            plan.workflows.get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")).expect("workflow");
        assert_eq!(workflow.steps.len(), 2);
        assert_eq!(workflow.steps[0].tool, new_tool);
        assert_eq!(workflow.steps[1].tool, new_tool);

        let stored_states = lock.workflow_states.get(&media_id).expect("stored workflow states");
        assert_eq!(stored_states.len(), 2);
        assert_eq!(stored_states[1].explicit_config, step1_snapshot);
        // Step 1's tool identity changed from old_tool to new_tool, which
        // triggers a refresh that sets impure_timestamp unconditionally.
        assert!(stored_states[1].impure_timestamp.is_some());
    }

    /// Protects managed external-data dedupe by merging overlapping hash policies
    /// so `full` dominates `saved` when the same hash is rooted from multiple
    /// managed sources.
    #[test]
    fn managed_external_data_dedupe_merges_save_policy_to_full() {
        let hash = Hash::from_content(b"shared-external-hash");
        let mut external_data = BTreeMap::new();

        upsert_managed_external_data(
            &mut external_data,
            hash,
            "managed external data: tool content 'demo-tool' path 'windows/tool.exe'".to_string(),
            OutputSaveMode::Saved,
        );
        upsert_managed_external_data(
        &mut external_data,
        hash,
        "managed external data: materialized output 'library/demo.bin' (media 'demo', variant 'video')".to_string(),
        OutputSaveMode::Full,
    );

        let reference = external_data.get(&hash).expect("merged external-data row");
        assert_eq!(reference.save, Some(OutputSaveMode::Full));
    }

    /// Protects managed-state persistence by rooting managed file CAS hashes in
    /// machine external-data with minimum `save = "full"`.
    #[test]
    fn managed_external_data_collection_roots_lock_managed_file_hashes() {
        let hash = Hash::from_content(b"managed-file-hash");
        let lock = MediaPmState {
            managed_files: BTreeMap::from([(
                "music videos/demo.mkv".to_string(),
                ManagedFileRecord {
                    media_id: "demo-media".to_string(),
                    variant: "video_tagged".to_string(),
                    hash: hash.to_string(),
                    last_synced_unix_millis: 1,
                },
            )]),
            ..MediaPmState::default()
        };
        let machine = MachineNickelDocument::default();
        let mut external_data = BTreeMap::new();

        collect_managed_external_data_from_machine_and_lock(&machine, &lock, &mut external_data)
            .expect("managed external-data collection should succeed");

        let reference = external_data.get(&hash).expect("managed-file hash should be rooted");
        assert_eq!(reference.save, Some(OutputSaveMode::Full));
        assert!(reference.description.as_deref().is_some_and(|description| {
            description.contains("materialized output 'music videos/demo.mkv'")
                && description.contains("media 'demo-media'")
                && description.contains("variant 'video_tagged'")
        }));
    }

    /// Protects hash dedupe by escalating shared tool-content/managed-file roots
    /// to full-save persistence.
    #[test]
    fn managed_external_data_collection_escalates_shared_hash_to_full() {
        let shared_hash = Hash::from_content(b"shared-tool-and-managed-file");
        let machine = MachineNickelDocument {
            tool_configs: BTreeMap::from([(
                "mediapm.tools.demo@latest".to_string(),
                ToolConfigSpec {
                    content_map: Some(BTreeMap::from([(
                        "windows/demo.exe".to_string(),
                        shared_hash,
                    )])),
                    ..ToolConfigSpec::default()
                },
            )]),
            ..MachineNickelDocument::default()
        };
        let lock = MediaPmState {
            managed_files: BTreeMap::from([(
                "sidecars/demo.info.json".to_string(),
                ManagedFileRecord {
                    media_id: "demo-media".to_string(),
                    variant: "infojson".to_string(),
                    hash: shared_hash.to_string(),
                    last_synced_unix_millis: 1,
                },
            )]),
            ..MediaPmState::default()
        };
        let mut external_data = BTreeMap::new();

        collect_managed_external_data_from_machine_and_lock(&machine, &lock, &mut external_data)
            .expect("managed external-data collection should dedupe shared hash");

        assert_eq!(external_data.len(), 1);
        let reference = external_data.get(&shared_hash).expect("shared hash row should exist");
        assert_eq!(reference.save, Some(OutputSaveMode::Full));
    }

    /// Protects dependency synthesis for ordered variant-flow step chains.
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "this regression keeps full variant-flow dependency assertions together for readability"
    )]
    fn variant_flow_creates_explicit_step_dependencies() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "remote-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                                generic_output_variant("primary"),
                            )]),
                            options: BTreeMap::from([(
                                "target_lufs".to_string(),
                                TransformInputValue::String("-14".to_string()),
                            )]),
                        },
                    ],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([
                (
                    "yt-dlp".to_string(),
                    "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
                ),
                (
                    "ffmpeg".to_string(),
                    "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ),
                (
                    "rsgain".to_string(),
                    "mediapm.tools.rsgain+github-releases-complexlogic-rsgain@latest".to_string(),
                ),
                ("sd".to_string(), "mediapm.tools.sd+conductor-common@latest".to_string()),
            ]),
            ..MediaPmState::default()
        };
        let machine = machine_with_active_tool_specs(&lock);

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.remote-a").expect("managed workflow");

        assert_eq!(workflow.steps.len(), 8);
        let download = &workflow.steps[0];
        let ffmpeg = &workflow.steps[1];
        let rsgain_extract = &workflow.steps[2];
        let rsgain = &workflow.steps[3];
        let metadata_export = &workflow.steps[4];
        let replaygain_metadata_rewrite = &workflow.steps[5];
        let r128_metadata_rewrite = &workflow.steps[6];
        let apply = &workflow.steps[7];

        assert!(download.depends_on.is_empty());
        assert_eq!(ffmpeg.depends_on, vec![download.id.clone()]);
        assert_eq!(rsgain_extract.depends_on, vec![ffmpeg.id.clone()]);
        assert_eq!(rsgain.depends_on, vec![rsgain_extract.id.clone()]);
        assert_eq!(metadata_export.depends_on, vec![rsgain.id.clone()]);
        assert_eq!(replaygain_metadata_rewrite.depends_on, vec![metadata_export.id.clone()]);
        assert_eq!(r128_metadata_rewrite.depends_on, vec![replaygain_metadata_rewrite.id.clone()]);
        assert!(apply.depends_on.contains(&r128_metadata_rewrite.id));
        assert!(apply.depends_on.contains(&ffmpeg.id));
        assert_eq!(
            replaygain_metadata_rewrite.inputs.get("pattern"),
            Some(&InputBinding::String("(?i)REPLAYGAIN_".to_string()))
        );
        assert_eq!(
            replaygain_metadata_rewrite.inputs.get("replacement"),
            Some(&InputBinding::String("replaygain_".to_string()))
        );
        assert_eq!(
            r128_metadata_rewrite.inputs.get("pattern"),
            Some(&InputBinding::String("(?i)R128_".to_string()))
        );
        assert_eq!(
            r128_metadata_rewrite.inputs.get("replacement"),
            Some(&InputBinding::String("R128_".to_string()))
        );
        assert_eq!(rsgain.inputs.get("album"), None);
        assert_eq!(rsgain.inputs.get("album_mode"), None);
        assert_eq!(rsgain.inputs.get("map_chapters"), None);
    }

    /// Protects expanded step-id numbering so each conductor step emitted from one
    /// mediapm step gets a unique `<mediapm_step>-<expanded_step>` prefix.
    #[test]
    fn expanded_step_ids_increment_within_each_mediapm_step() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "expanded-id-order".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    )]),
                    steps: vec![
                        MediaStep {
                            tool: MediaStepTool::MediaTagger,
                            input_variants: vec!["default".to_string()],
                            output_variants: BTreeMap::from([(
                                "tagged".to_string(),
                                generic_output_variant("primary"),
                            )]),
                            options: BTreeMap::new(),
                        },
                        MediaStep {
                            tool: MediaStepTool::Rsgain,
                            input_variants: vec!["tagged".to_string()],
                            output_variants: BTreeMap::from([(
                                "normalized".to_string(),
                                generic_output_variant("primary"),
                            )]),
                            options: BTreeMap::new(),
                        },
                    ],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([
                (
                    "media-tagger".to_string(),
                    "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
                ),
                (
                    "ffmpeg".to_string(),
                    "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ),
                (
                    "rsgain".to_string(),
                    "mediapm.tools.rsgain+github-releases-complexlogic-rsgain@latest".to_string(),
                ),
                ("sd".to_string(), "mediapm.tools.sd+conductor-common@latest".to_string()),
            ]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow =
            plan.workflows.get("mediapm.media.expanded-id-order").expect("managed workflow");

        let step_prefixes = workflow
            .steps
            .iter()
            .map(|step| {
                let mut parts = step.id.splitn(3, '-');
                let first = parts.next().expect("first step-id segment");
                let second = parts.next().expect("second step-id segment");
                format!("{first}-{second}")
            })
            .collect::<Vec<_>>();

        assert_eq!(step_prefixes, vec!["0-0", "0-1", "1-0", "1-1", "1-2", "1-3", "1-4", "1-5"],);
    }

    /// Protects media-tagger synthesis expansion into metadata-fetch and
    /// ffmpeg-apply step pair with deterministic dependency wiring.
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
    fn media_tagger_step_expands_to_metadata_and_apply_steps() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "tag-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::from([
                            (
                                "strict_identification".to_string(),
                                TransformInputValue::String("false".to_string()),
                            ),
                            (
                                "output_container".to_string(),
                                TransformInputValue::String("mp4".to_string()),
                            ),
                        ]),
                    }],
                },
            )]),
            tools: BTreeMap::from([(
                "media-tagger".to_string(),
                ToolRequirement {
                    version: None,
                    tag: Some("latest".to_string()),
                    dependencies: ToolRequirementDependencies {
                        ffmpeg_version: Some("inherit".to_string()),
                        deno_version: None,
                        sd_version: None,
                    },
                    recheck_seconds: None,
                    max_input_slots: None,
                    max_output_slots: None,
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([
                (
                    "media-tagger".to_string(),
                    "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
                ),
                (
                    "ffmpeg".to_string(),
                    "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ),
            ]),
            ..MediaPmState::default()
        };
        let mut machine = machine_with_active_tool_specs(&lock);
        machine.tools.insert(
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
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
        assert_eq!(metadata.outputs.get("content"), Some(&OutputPolicy { save: None }));
        assert_eq!(metadata.outputs.get("sandbox_artifacts"), Some(&OutputPolicy { save: None }));
        assert!(!metadata.inputs.contains_key("ffmpeg_version"));
        assert!(!metadata.inputs.contains_key("output_container"));

        assert_eq!(apply.tool, "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest");
        assert!(apply.depends_on.contains(&metadata.id));
        assert_eq!(
            apply.inputs.get("ffmetadata_content"),
            Some(&InputBinding::String(format!("${{step_output.{}.content}}", metadata.id)))
        );
        assert_eq!(
            apply.inputs.get("input_content_1"),
            Some(&InputBinding::String(format!(
                "${{step_output.{}.sandbox_artifacts:zip(coverart-slot-1.bin)}}",
                metadata.id
            )))
        );
        assert_eq!(
            apply.inputs.get("cover_art_slot_enabled_1"),
            Some(&InputBinding::String(format!(
                "${{step_output.{}.sandbox_artifacts:zip(coverart-slot-1.flag)}}",
                metadata.id
            )))
        );
        assert_eq!(apply.inputs.get("map_chapters"), None);
        assert_eq!(
            apply.inputs.get("trailing_args"),
            Some(&InputBinding::StringList(vec!["-map".to_string(), "0".to_string()]))
        );
        assert_eq!(apply.inputs.get("container"), Some(&InputBinding::String("mp4".to_string())));
        assert_eq!(
            apply.outputs.get("primary"),
            Some(&OutputPolicy { save: Some(OutputSaveMode::Full) })
        );

        let binding = resolve_media_variant_output_binding(
            document.media.get("tag-a").expect("tag-a source"),
            "tagged",
        )
        .expect("resolve tagged binding")
        .expect("tagged binding should exist");
        assert_eq!(binding.step_id, apply.id);
        assert_eq!(binding.output_name, "primary");
    }

    /// Protects media-tagger apply synthesis by preserving a supported upstream
    /// extension when the output variant does not override it.
    #[test]
    fn media_tagger_apply_preserves_upstream_supported_extension_by_default() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "tag-preserve-ext".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    )]),
                    steps: vec![
                        MediaStep {
                            tool: MediaStepTool::Ffmpeg,
                            input_variants: vec!["default".to_string()],
                            output_variants: BTreeMap::from([(
                                "audio_m4a".to_string(),
                                ffmpeg_output_variant_with_extension(0, "m4a"),
                            )]),
                            options: BTreeMap::new(),
                        },
                        MediaStep {
                            tool: MediaStepTool::MediaTagger,
                            input_variants: vec!["audio_m4a".to_string()],
                            output_variants: BTreeMap::from([(
                                "tagged".to_string(),
                                generic_output_variant("primary"),
                            )]),
                            options: BTreeMap::new(),
                        },
                    ],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([
                (
                    "media-tagger".to_string(),
                    "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
                ),
                (
                    "ffmpeg".to_string(),
                    "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ),
            ]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow =
            plan.workflows.get("mediapm.media.tag-preserve-ext").expect("managed workflow");
        let apply = workflow.steps.last().expect("media-tagger apply step");

        assert_eq!(
            apply.inputs.get("output_path_0"),
            Some(&InputBinding::String("output-0.m4a".to_string()))
        );
    }

    /// Protects `tools.media-tagger.dependencies.ffmpeg_version = "inherit"`
    /// behavior by
    /// requiring an active logical ffmpeg tool in lock state.
    #[test]
    fn media_tagger_inherit_ffmpeg_version_requires_active_ffmpeg_tool() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "tag-inherit-missing-ffmpeg".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::from([(
                            "recording_mbid".to_string(),
                            TransformInputValue::String(
                                "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae".to_string(),
                            ),
                        )]),
                    }],
                },
            )]),
            tools: BTreeMap::from([(
                "media-tagger".to_string(),
                ToolRequirement {
                    version: None,
                    tag: Some("latest".to_string()),
                    dependencies: ToolRequirementDependencies {
                        ffmpeg_version: Some("inherit".to_string()),
                        deno_version: None,
                        sd_version: None,
                    },
                    recheck_seconds: None,
                    max_input_slots: None,
                    max_output_slots: None,
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "media-tagger".to_string(),
                "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let error = build_media_workflow_plan(&document, &lock, &machine)
            .expect_err("inherit mode should fail when active ffmpeg tool is missing");
        let text = error.to_string();
        assert!(
            text.contains("tools.media-tagger.dependencies.ffmpeg_version='inherit'"),
            "unexpected error: {text}"
        );
    }

    /// Protects metadata-preserving media-tagger behavior by always forwarding
    /// source input into metadata fetch stages, even when MBID identity is
    /// explicitly provided.
    #[test]
    fn media_tagger_metadata_step_keeps_input_when_recording_mbid_is_set() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "tag-no-input".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::from([(
                            "recording_mbid".to_string(),
                            TransformInputValue::String(
                                "f4ec5f46-5f50-4f95-9f8d-2df2ec2fd2bc".to_string(),
                            ),
                        )]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([
                (
                    "media-tagger".to_string(),
                    "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
                ),
                (
                    "ffmpeg".to_string(),
                    "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ),
            ]),
            ..MediaPmState::default()
        };
        let machine = machine_with_active_tool_specs(&lock);

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.tag-no-input").expect("managed workflow");
        let metadata_step = workflow
            .steps
            .iter()
            .find(|step| step.id.ends_with("-metadata"))
            .expect("metadata step");

        let input_content = metadata_step
            .inputs
            .get("input_content")
            .and_then(|binding| match binding {
                InputBinding::String(value) => Some(value),
                InputBinding::StringList(_) => None,
            })
            .expect("input_content scalar binding");
        assert!(
            input_content.starts_with("${external_data.blake3:"),
            "expected metadata step to keep upstream content binding"
        );
        assert!(metadata_step.depends_on.is_empty());
        assert_eq!(
            metadata_step.inputs.get("recording_mbid"),
            Some(&InputBinding::String("f4ec5f46-5f50-4f95-9f8d-2df2ec2fd2bc".to_string()))
        );
        assert_eq!(
            metadata_step.inputs.get("strict_identification"),
            None,
            "media-tagger workflow inputs should omit strict_identification when callers rely on managed input defaults"
        );
    }

    /// Protects MBID sentinel passthrough so `none` reaches internal media-tagger
    /// runtime unchanged and can disable autodetection.
    #[test]
    fn media_tagger_metadata_step_preserves_none_mbid_sentinel() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "tag-none-sentinel".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::from([
                            (
                                "recording_mbid".to_string(),
                                TransformInputValue::String("none".to_string()),
                            ),
                            (
                                "release_mbid".to_string(),
                                TransformInputValue::String("none".to_string()),
                            ),
                        ]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([
                (
                    "media-tagger".to_string(),
                    "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
                ),
                (
                    "ffmpeg".to_string(),
                    "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ),
            ]),
            ..MediaPmState::default()
        };
        let machine = machine_with_active_tool_specs(&lock);

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow =
            plan.workflows.get("mediapm.media.tag-none-sentinel").expect("managed workflow");
        let metadata_step = workflow
            .steps
            .iter()
            .find(|step| step.id.ends_with("-metadata"))
            .expect("metadata step");

        assert_eq!(
            metadata_step.inputs.get("recording_mbid"),
            Some(&InputBinding::String("none".to_string()))
        );
        assert_eq!(
            metadata_step.inputs.get("release_mbid"),
            Some(&InputBinding::String("none".to_string()))
        );
    }

    /// Protects local import-step synthesis using builtin import output wiring.
    #[test]
    fn import_step_synthesizes_builtin_import_binding() {
        let document = MediaPmDocument {
        media: BTreeMap::from([(
            "local-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                artist: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::Import,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "source".to_string(),
                        generic_output_variant("primary"),
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

        let lock = MediaPmState::default();
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
        assert_eq!(
            step.outputs.get("result").and_then(|policy| policy.save),
            Some(OutputSaveMode::Full),
        );
    }

    /// Protects import-step bridging by translating mediapm primary-kind outputs
    /// to the builtin import contract's `result` output name for downstream wiring.
    #[test]
    fn import_variant_binding_uses_builtin_result_output_name() {
        let source = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
            tool: MediaStepTool::Import,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "local_media".to_string(),
                generic_output_variant("primary"),
            )]),
            options: BTreeMap::from([
                ("kind".to_string(), TransformInputValue::String("cas_hash".to_string())),
                (
                    "hash".to_string(),
                    TransformInputValue::String(
                        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    ),
                ),
            ]),
        }],
        };

        let binding =
            resolve_media_variant_output_binding(&source, "local_media").expect("resolve binding");
        let binding = binding.expect("binding should exist for imported variant");

        assert_eq!(binding.step_id, "0-0-import");
        assert_eq!(binding.output_name, "result");
    }

    /// Protects per-variant output policy mapping from mediapm schema into
    /// generated conductor workflow-step output overrides.
    #[test]
    fn step_output_variant_policy_maps_to_workflow_output_policy() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "policy-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                            json!({ "kind": "primary", "save": "full", "idx": 0 }),
                        )]),
                        options: BTreeMap::new(),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };
        let machine = machine_with_active_tool_specs(&lock);

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.policy-a").expect("managed workflow");
        let step = workflow.steps.first().expect("workflow step");

        assert_eq!(
            step.outputs.get("primary"),
            Some(&OutputPolicy { save: Some(OutputSaveMode::Full) }),
        );
    }

    /// Protects ffmpeg per-variant extension wiring by mapping output
    /// extension config into generated `output_path_<idx>` bindings.
    #[test]
    fn ffmpeg_output_variant_extension_updates_output_path_binding() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "ffmpeg-extension".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                            ffmpeg_output_variant_with_extension(0, "webm"),
                        )]),
                        options: BTreeMap::new(),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.ffmpeg-extension").expect("workflow");
        let step = workflow.steps.first().expect("workflow step");

        assert_eq!(
            step.inputs.get("output_path_0"),
            Some(&InputBinding::String("output-0.webm".to_string()))
        );
    }

    /// Protects ffmpeg extension-default behavior by inheriting upstream producer
    /// extension when output `extension` is omitted.
    #[test]
    fn ffmpeg_output_variant_without_extension_inherits_upstream_extension() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "ffmpeg-inherit-extension".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "source".to_string(),
                        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    )]),
                    steps: vec![
                        MediaStep {
                            tool: MediaStepTool::Ffmpeg,
                            input_variants: vec!["source".to_string()],
                            output_variants: BTreeMap::from([(
                                "audio_m4a".to_string(),
                                ffmpeg_output_variant_with_extension(0, "m4a"),
                            )]),
                            options: BTreeMap::new(),
                        },
                        MediaStep {
                            tool: MediaStepTool::Ffmpeg,
                            input_variants: vec!["audio_m4a".to_string()],
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

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow =
            plan.workflows.get("mediapm.media.ffmpeg-inherit-extension").expect("managed workflow");
        let second_step = workflow.steps.get(1).expect("second ffmpeg step");

        assert_eq!(
            second_step.inputs.get("output_path_0"),
            Some(&InputBinding::String("output-0.m4a".to_string()))
        );
    }

    /// Protects ffmpeg container-default behavior by inferring container from the
    /// effective primary output extension when `options.container` is omitted,
    /// including extension-alias canonicalization to valid muxer names.
    #[test]
    fn ffmpeg_infers_container_from_primary_output_extension() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "ffmpeg-infer-container".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                            ffmpeg_output_variant_with_extension(0, "mkv"),
                        )]),
                        options: BTreeMap::new(),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow =
            plan.workflows.get("mediapm.media.ffmpeg-infer-container").expect("managed workflow");
        let step = workflow.steps.first().expect("workflow step");

        assert_eq!(
            step.inputs.get("container"),
            Some(&InputBinding::String("matroska".to_string()))
        );
    }

    /// Protects ffmpeg container inference for extension aliases by canonicalizing
    /// to ffmpeg-accepted muxer names.
    #[test]
    fn ffmpeg_infers_canonical_container_from_extension_aliases() {
        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };
        let machine = machine_with_active_tool_specs(&lock);

        for (alias_extension, expected_container) in [
            ("m4a", "mp4"),
            ("m2ts", "mpegts"),
            ("wmv", "asf"),
            ("ogv", "ogg"),
            ("ism", "ismv"),
            ("qt", "mov"),
        ] {
            let media_id = format!("ffmpeg-infer-{alias_extension}-container");
            let document = MediaPmDocument {
            media: BTreeMap::from([(
                media_id.clone(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                            ffmpeg_output_variant_with_extension(0, alias_extension),
                        )]),
                        options: BTreeMap::new(),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

            let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
            let workflow =
                plan.workflows.get(&format!("mediapm.media.{media_id}")).expect("managed workflow");
            let step = workflow.steps.first().expect("workflow step");

            assert_eq!(
                step.inputs.get("container"),
                Some(&InputBinding::String(expected_container.to_string())),
                "expected extension alias '{alias_extension}' to infer canonical container '{expected_container}'"
            );
        }
    }

    /// Protects explicit ffmpeg container aliases by canonicalizing extension-style
    /// values (e.g. `mkv`) to valid muxer names.
    #[test]
    fn ffmpeg_explicit_container_aliases_are_canonicalized() {
        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };
        let machine = machine_with_active_tool_specs(&lock);

        for (alias_container, expected_container) in [
            ("mkv", "matroska"),
            ("m4b", "mp4"),
            ("m2ts", "mpegts"),
            ("wma", "asf"),
            ("oga", "ogg"),
            ("isma", "ismv"),
            ("qt", "mov"),
        ] {
            let media_id = format!("ffmpeg-explicit-{alias_container}-container-alias");
            let document = MediaPmDocument {
            media: BTreeMap::from([(
                media_id.clone(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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
                            ffmpeg_output_variant_with_extension(0, "mkv"),
                        )]),
                        options: BTreeMap::from([(
                            "container".to_string(),
                            TransformInputValue::String(alias_container.to_string()),
                        )]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

            let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
            let workflow =
                plan.workflows.get(&format!("mediapm.media.{media_id}")).expect("managed workflow");
            let step = workflow.steps.first().expect("workflow step");

            assert_eq!(
                step.inputs.get("container"),
                Some(&InputBinding::String(expected_container.to_string())),
                "expected explicit alias '{alias_container}' to canonicalize to '{expected_container}'"
            );
        }
    }

    /// Protects yt-dlp artifact variants by mapping non-primary outputs to
    /// artifact-bundle capture outputs instead of `content`.
    #[test]
    fn yt_dlp_artifact_variant_maps_output_policy_to_artifact_capture() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "policy-ytdlp".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "subtitles".to_string(),
                            json!({
                                "kind": "subtitles",
                                "save": true
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

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };
        let machine = machine_with_active_tool_specs(&lock);

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.policy-ytdlp").expect("managed workflow");
        let step = workflow.steps.first().expect("workflow step");

        assert_eq!(
            step.outputs.get("yt_dlp_subtitle_artifacts"),
            Some(&OutputPolicy { save: None }),
        );
        assert!(!step.outputs.contains_key("content"));
    }

    /// Protects sidecar capture routing by forcing an explicit output key even
    /// when per-variant save/force overrides are omitted.
    #[test]
    fn yt_dlp_sidecar_variant_without_policy_still_emits_artifact_output_key() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "policy-ytdlp-default-sidecar".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "thumbnail".to_string(),
                            yt_dlp_output_variant("thumbnails"),
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

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };
        let machine = machine_with_active_tool_specs(&lock);

        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan
            .workflows
            .get("mediapm.media.policy-ytdlp-default-sidecar")
            .expect("managed workflow");
        let step = workflow.steps.first().expect("workflow step");

        assert!(step.outputs.contains_key("yt_dlp_thumbnail_artifacts"));
        assert!(!step.outputs.contains_key("content"));
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
        );

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
        );

        assert_eq!(
            bindings.get("option_args"),
            Some(&InputBinding::StringList(vec!["--foo".to_string(), "--bar=baz".to_string()])),
        );
    }

    /// Protects scalar-first option typing for non-`option_args` inputs.
    #[test]
    fn step_option_bindings_accept_scalar_for_non_option_args_option() {
        let bindings = step_option_input_bindings(
            MediaStepTool::YtDlp,
            &BTreeMap::from([(
                "merge_output_format".to_string(),
                TransformInputValue::String("mkv".to_string()),
            )]),
        );

        assert_eq!(
            bindings.get("merge_output_format"),
            Some(&InputBinding::String("mkv".to_string())),
        );
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
        );

        assert!(!bindings.contains_key("uri"));
    }

    /// Protects hierarchy variant resolution so any variant exposed by any
    /// step remains selectable by name.
    #[test]
    fn variant_binding_resolves_non_latest_variant_name_when_still_unique() {
        let source = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![
                MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([
                        ("downloaded".to_string(), yt_dlp_output_variant("primary")),
                        ("subtitles".to_string(), yt_dlp_output_variant("subtitles")),
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

        assert_eq!(binding.step_id, "0-0-yt-dlp");
        assert_eq!(binding.output_name, "yt_dlp_subtitle_artifacts");
    }

    /// Protects duplicate output-variant semantics by selecting the latest
    /// producer when multiple steps expose the same variant name.
    #[test]
    fn variant_binding_uses_last_producer_for_duplicate_output_variant() {
        let source = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
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
                        generic_output_variant("primary"),
                    )]),
                    options: BTreeMap::new(),
                },
            ],
        };

        let binding =
            resolve_media_variant_output_binding(&source, "normalized").expect("resolve binding");
        let binding = binding.expect("binding should exist for normalized variant");

        assert_eq!(binding.step_id, "2-5-rsgain-ffmpeg-apply");
        assert_eq!(binding.output_name, "primary");
    }

    /// Protects rsgain synthesis by reusing a supported upstream tagged extension
    /// instead of falling back to FLAC extraction.
    #[test]
    fn rsgain_chain_reuses_supported_upstream_extension_to_avoid_transcoding() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "rsgain-preserve-ext".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    )]),
                    steps: vec![
                        MediaStep {
                            tool: MediaStepTool::Ffmpeg,
                            input_variants: vec!["default".to_string()],
                            output_variants: BTreeMap::from([(
                                "audio_m4a".to_string(),
                                ffmpeg_output_variant_with_extension(0, "m4a"),
                            )]),
                            options: BTreeMap::new(),
                        },
                        MediaStep {
                            tool: MediaStepTool::MediaTagger,
                            input_variants: vec!["audio_m4a".to_string()],
                            output_variants: BTreeMap::from([(
                                "tagged".to_string(),
                                generic_output_variant("primary"),
                            )]),
                            options: BTreeMap::new(),
                        },
                        MediaStep {
                            tool: MediaStepTool::Rsgain,
                            input_variants: vec!["tagged".to_string()],
                            output_variants: BTreeMap::from([(
                                "normalized".to_string(),
                                generic_output_variant("primary"),
                            )]),
                            options: BTreeMap::new(),
                        },
                    ],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([
                (
                    "media-tagger".to_string(),
                    "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
                ),
                (
                    "ffmpeg".to_string(),
                    "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ),
                (
                    "rsgain".to_string(),
                    "mediapm.tools.rsgain+github-releases-complexlogic-rsgain@latest".to_string(),
                ),
                ("sd".to_string(), "mediapm.tools.sd+conductor-common@latest".to_string()),
            ]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow =
            plan.workflows.get("mediapm.media.rsgain-preserve-ext").expect("managed workflow");

        let rsgain_extract = workflow
            .steps
            .iter()
            .find(|step| step.id.ends_with("-ffmpeg-extract"))
            .expect("rsgain extract step");
        let rsgain =
            workflow.steps.iter().find(|step| step.id.ends_with("-rsgain")).expect("rsgain step");
        let apply = workflow
            .steps
            .iter()
            .find(|step| step.id.contains("rsgain-ffmpeg-apply"))
            .expect("rsgain apply step");

        assert_eq!(
            rsgain_extract.inputs.get("output_path_0"),
            Some(&InputBinding::String("output-0.m4a".to_string()))
        );
        assert_eq!(
            rsgain_extract.inputs.get("codec_copy"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            rsgain.inputs.get("input_extension"),
            Some(&InputBinding::String("m4a".to_string()))
        );
        assert_eq!(
            apply.inputs.get("output_path_0"),
            Some(&InputBinding::String("output-0.m4a".to_string()))
        );
    }

    /// Protects ffmpeg runtime-limit configurability for high-index outputs.
    #[test]
    fn variant_binding_supports_custom_ffmpeg_output_limit() {
        let source = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
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
                        "kind": "primary",
                        "save": "full",
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
        assert_eq!(binding.output_name, "primary_70");
    }

    /// Protects yt-dlp description sidecar semantics by binding directly to
    /// file captures with no implicit ZIP member selector.
    #[test]
    fn yt_dlp_description_binding_uses_file_capture_without_zip_member() {
        let source = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "description".to_string(),
                    serde_json::json!({ "kind": "description", "save": "full" }),
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

    /// Protects yt-dlp annotation sidecar semantics by binding singular
    /// `annotation` variants directly to file captures.
    #[test]
    fn yt_dlp_annotation_binding_uses_file_capture_without_zip_member() {
        let source = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "annotation".to_string(),
                    serde_json::json!({ "kind": "annotation", "save": "full" }),
                )]),
                options: BTreeMap::from([(
                    "uri".to_string(),
                    TransformInputValue::String("https://example.com/video".to_string()),
                )]),
            }],
        };

        let binding = resolve_media_variant_output_binding(&source, "annotation")
            .expect("resolve annotation binding")
            .expect("annotation binding should exist");

        assert_eq!(binding.output_name, "yt_dlp_annotation_file");
        assert!(binding.zip_member.is_none());
    }

    /// Protects capture-kind override semantics by routing description
    /// variants to folder capture outputs when explicitly requested.
    #[test]
    fn yt_dlp_description_binding_honors_folder_capture_kind() {
        let source = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "description".to_string(),
                    serde_json::json!({
                        "kind": "description",
                        "capture_kind": "folder",
                        "save": "full"
                    }),
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

        assert_eq!(binding.output_name, "sandbox_artifacts");
    }

    /// Protects multi-output yt-dlp synthesis by generating one workflow step
    /// that enables all required sidecar toggles.
    #[test]
    fn yt_dlp_description_and_infojson_outputs_share_one_step() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "sidecar-flags".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([
                            (
                                "description".to_string(),
                                json!({ "kind": "description", "save": "full" }),
                            ),
                            (
                                "info_json".to_string(),
                                json!({ "kind": "infojson", "save": "full" }),
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

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.sidecar-flags").expect("workflow");

        assert_eq!(workflow.steps.len(), 1);
        let step = workflow.steps.first().expect("yt-dlp step");
        assert_eq!(step.id, "0-0-yt-dlp");

        assert_eq!(
            step.inputs.get("write_description"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            step.inputs.get("write_info_json"),
            Some(&InputBinding::String("true".to_string()))
        );

        let description_binding = resolve_media_variant_output_binding(
            document.media.get("sidecar-flags").expect("source"),
            "description",
        )
        .expect("resolve description binding")
        .expect("description binding should exist");
        let infojson_binding = resolve_media_variant_output_binding(
            document.media.get("sidecar-flags").expect("source"),
            "info_json",
        )
        .expect("resolve infojson binding")
        .expect("infojson binding should exist");

        assert_eq!(description_binding.step_id, "0-0-yt-dlp");
        assert_eq!(infojson_binding.step_id, "0-0-yt-dlp");
    }

    /// Protects thumbnail output synthesis by enabling thumbnail toggles while
    /// leaving caller defaults overrideable.
    #[test]
    fn yt_dlp_thumbnail_step_enables_thumbnail_outputs() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "thumbnail-only".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "thumbnails/".to_string(),
                            json!({ "kind": "thumbnails", "save": "full" }),
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

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.thumbnail-only").expect("workflow");
        let step = workflow.steps.first().expect("thumbnail step");

        assert_eq!(
            step.inputs.get("write_thumbnail"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            step.inputs.get("write_all_thumbnails"),
            Some(&InputBinding::String("false".to_string()))
        );
    }

    /// Protects subtitle output synthesis by enabling subtitle capture and
    /// avoiding forced disables for unrelated toggles.
    #[test]
    fn yt_dlp_subtitle_step_enables_subtitle_capture() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "subtitle-only".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "subtitles/".to_string(),
                            json!({ "kind": "subtitles", "save": "full" }),
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

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.subtitle-only").expect("workflow");
        let step = workflow.steps.first().expect("subtitle step");

        assert_eq!(step.inputs.get("write_subs"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(
            step.inputs.get("skip_download"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            step.inputs.get("write_thumbnail"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(
            step.inputs.get("write_comments"),
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
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![
                        MediaStep {
                            tool: MediaStepTool::YtDlp,
                            input_variants: Vec::new(),
                            output_variants: BTreeMap::from([(
                                "subtitles/".to_string(),
                                yt_dlp_output_variant("subtitles"),
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

        let lock = MediaPmState {
            active_tools: BTreeMap::from([
                (
                    "yt-dlp".to_string(),
                    "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
                ),
                (
                    "ffmpeg".to_string(),
                    "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ),
            ]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let error = build_media_workflow_plan(&document, &lock, &machine)
            .expect_err("plan should fail without exact scoped producer");
        assert!(
            error.to_string().contains("subtitles/en") && error.to_string().contains("unknown")
        );
    }

    /// Protects producer selection precedence so exact scoped outputs resolve
    /// successfully when both scoped and folder-like keys exist.
    #[test]
    fn scoped_input_variant_prefers_exact_output_over_folder_fallback() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "scoped-exact".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![
                        MediaStep {
                            tool: MediaStepTool::YtDlp,
                            input_variants: Vec::new(),
                            output_variants: BTreeMap::from([
                                ("subtitles/".to_string(), yt_dlp_output_variant("subtitles")),
                                (
                                    "subtitles/en".to_string(),
                                    json!({
                                        "kind": "subtitles",
                                        "save": "full",
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

        let lock = MediaPmState {
            active_tools: BTreeMap::from([
                (
                    "yt-dlp".to_string(),
                    "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
                ),
                (
                    "ffmpeg".to_string(),
                    "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ),
            ]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.scoped-exact").expect("managed workflow");

        let exact_producer_step = &workflow.steps[0];
        let consumer_step = &workflow.steps[1];
        assert_eq!(
            consumer_step.inputs.get("input_content_0"),
            Some(&InputBinding::String(format!(
                "${{step_output.{}.yt_dlp_subtitle_artifacts}}",
                exact_producer_step.id
            ))),
        );
    }

    /// Protects downloader language-selection ownership by keeping
    /// `sub_langs` sourced from step options instead of output-variant `langs`.
    #[test]
    fn yt_dlp_scoped_subtitle_variant_keeps_step_sub_langs_authoritative() {
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "auto-inputs".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "subtitles/en".to_string(),
                            json!({
                                "kind": "subtitles",
                                "save": "full",
                                "langs": "en"
                            }),
                        )]),
                        options: BTreeMap::from([
                            (
                                "uri".to_string(),
                                TransformInputValue::String(
                                    "https://example.com/video".to_string(),
                                ),
                            ),
                            (
                                "sub_langs".to_string(),
                                TransformInputValue::String("en,es".to_string()),
                            ),
                        ]),
                    }],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow = plan.workflows.get("mediapm.media.auto-inputs").expect("managed workflow");
        let step = workflow.steps.first().expect("yt-dlp step");

        assert_eq!(step.inputs.get("write_subs"), Some(&InputBinding::String("true".to_string())),);
        assert_eq!(step.inputs.get("sub_langs"), Some(&InputBinding::String("en,es".to_string())),);
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
                    id: None,
                    description: None,
                    title: None,
                    artist: None,
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

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = machine_with_active_tool_specs(&lock);
        let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
        let workflow =
            plan.workflows.get("mediapm.media.primary-output").expect("managed workflow");
        let step = workflow.steps.first().expect("yt-dlp step");

        assert!(!step.inputs.contains_key("skip_download"));
    }

    /// Verifies that when the generated tool identity differs from the previous
    /// managed step tool, but the previous tool id is still valid in the machine
    /// configuration (present in `machine.tools` with a non-empty content map),
    /// the previous tool id is preserved. This prevents unnecessary cache
    /// invalidation when only the tool version string has changed.
    #[test]
    fn preserve_existing_generated_step_tools_preserves_old_tool_when_valid() {
        let old_tool =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@old-preserve-v1".to_string();
        let new_tool =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@new-preserve-v1".to_string();
        let media_id = "preserve-old-tool-when-valid".to_string();
        let source = MediaSourceSpec {
            steps: vec![MediaStep {
                tool: MediaStepTool::Ffmpeg,
                input_variants: vec!["source".to_string()],
                output_variants: BTreeMap::from([(
                    "default".to_string(),
                    ffmpeg_output_variant(0),
                )]),
                options: BTreeMap::new(),
            }],
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::from([(
                "source".to_string(),
                "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            )]),
        };
        let explicit_snapshot =
            serde_json::to_value(&source.steps[0]).expect("serialize explicit step config");

        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.clone(), source)]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaPmState {
            active_tools: BTreeMap::from([("ffmpeg".to_string(), new_tool.clone())]),
            workflow_states: BTreeMap::from([(
                media_id.clone(),
                vec![ManagedWorkflowStepState {
                    explicit_config: explicit_snapshot.clone(),
                    impure_timestamp: Some(MediaPmImpureTimestamp {
                        epoch_seconds: 10,
                        subsec_nanos: 20,
                    }),
                }],
            )]),
            ..MediaPmState::default()
        };

        let mut machine = machine_with_active_tool_specs(&lock);
        // old_tool is valid: present in machine.tools with a non-empty content_map.
        machine.tools.insert(old_tool.clone(), executable_tool_spec("ffmpeg"));
        machine.tool_configs.insert(
            old_tool.clone(),
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([(
                    "linux/ffmpeg".to_string(),
                    Hash::from_content(b"ffmpeg-old"),
                )])),
                ..ToolConfigSpec::default()
            },
        );
        // Existing workflow step uses old_tool.
        machine.workflows.insert(
            format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "0-0-ffmpeg".to_string(),
                    tool: old_tool.clone(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([("primary".to_string(), OutputPolicy { save: None })]),
                }],
                ..WorkflowSpec::default()
            },
        );

        let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
            .expect("plan should succeed");
        let workflow = plan
            .workflows
            .get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"))
            .expect("managed workflow");

        assert_eq!(workflow.steps.len(), 1);
        // The old tool id should be preserved because it is still valid.
        assert_eq!(workflow.steps[0].tool, old_tool);
    }

    /// Verifies that when the generated tool identity differs from the previous
    /// managed step tool and the previous tool id is no longer valid (not present
    /// in `machine.tools`), the generated (active) tool identity is used instead.
    #[test]
    fn preserve_existing_generated_step_tools_refreshes_tool_when_old_invalid() {
        let old_tool =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@old-invalid-v1".to_string();
        let new_tool =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@new-invalid-v1".to_string();
        let media_id = "refresh-tool-when-old-invalid".to_string();
        let source = MediaSourceSpec {
            steps: vec![MediaStep {
                tool: MediaStepTool::Ffmpeg,
                input_variants: vec!["source".to_string()],
                output_variants: BTreeMap::from([(
                    "default".to_string(),
                    ffmpeg_output_variant(0),
                )]),
                options: BTreeMap::new(),
            }],
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::from([(
                "source".to_string(),
                "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            )]),
        };
        let explicit_snapshot =
            serde_json::to_value(&source.steps[0]).expect("serialize explicit step config");

        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.clone(), source)]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaPmState {
            active_tools: BTreeMap::from([("ffmpeg".to_string(), new_tool.clone())]),
            workflow_states: BTreeMap::from([(
                media_id.clone(),
                vec![ManagedWorkflowStepState {
                    explicit_config: explicit_snapshot.clone(),
                    impure_timestamp: Some(MediaPmImpureTimestamp {
                        epoch_seconds: 20,
                        subsec_nanos: 30,
                    }),
                }],
            )]),
            ..MediaPmState::default()
        };

        let mut machine = machine_with_active_tool_specs(&lock);
        // old_tool is NOT added to machine.tools, so preserved_step_tool_is_valid
        // returns false and the generated (active) tool is used.
        machine.workflows.insert(
            format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "0-0-ffmpeg".to_string(),
                    tool: old_tool.clone(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([("primary".to_string(), OutputPolicy { save: None })]),
                }],
                ..WorkflowSpec::default()
            },
        );

        let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
            .expect("plan should succeed");
        let workflow = plan
            .workflows
            .get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"))
            .expect("managed workflow");

        assert_eq!(workflow.steps.len(), 1);
        // The generated (active) tool should be used since old_tool is invalid.
        assert_eq!(workflow.steps[0].tool, new_tool);
    }

    /// Verifies that when the generated tool identity matches the previous managed
    /// step tool and the tool is valid, the tool identity is kept unchanged.
    #[test]
    fn preserve_existing_generated_step_tools_keeps_same_tool_when_unchanged() {
        let same_tool =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@same-v1".to_string();
        let media_id = "keep-same-tool-when-unchanged".to_string();
        let source = MediaSourceSpec {
            steps: vec![MediaStep {
                tool: MediaStepTool::Ffmpeg,
                input_variants: vec!["source".to_string()],
                output_variants: BTreeMap::from([(
                    "default".to_string(),
                    ffmpeg_output_variant(0),
                )]),
                options: BTreeMap::new(),
            }],
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::from([(
                "source".to_string(),
                "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            )]),
        };
        let explicit_snapshot =
            serde_json::to_value(&source.steps[0]).expect("serialize explicit step config");

        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.clone(), source)]),
            ..MediaPmDocument::default()
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([("ffmpeg".to_string(), same_tool.clone())]),
            workflow_states: BTreeMap::from([(
                media_id.clone(),
                vec![ManagedWorkflowStepState {
                    explicit_config: explicit_snapshot.clone(),
                    impure_timestamp: Some(MediaPmImpureTimestamp {
                        epoch_seconds: 30,
                        subsec_nanos: 40,
                    }),
                }],
            )]),
            ..MediaPmState::default()
        };

        let mut machine = machine_with_active_tool_specs(&lock);
        // same_tool is in machine.tools (from machine_with_active_tool_specs)
        // and is valid.
        machine.workflows.insert(
            format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "0-0-ffmpeg".to_string(),
                    tool: same_tool.clone(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([("primary".to_string(), OutputPolicy { save: None })]),
                }],
                ..WorkflowSpec::default()
            },
        );

        let plan =
            build_media_workflow_plan(&document, &lock, &machine).expect("plan should succeed");
        let workflow = plan
            .workflows
            .get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"))
            .expect("managed workflow");

        assert_eq!(workflow.steps.len(), 1);
        // The tool identity should remain unchanged.
        assert_eq!(workflow.steps[0].tool, same_tool);
    }
}
