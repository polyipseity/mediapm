//! Workflow-step synthesis from `mediapm.ncl` media-step configs.
//!
//! Each per-tool submodule converts a [`MediaStep`] + source config into one or
//! more [`WorkflowStepSpec`] entries for conductor execution.
//!
//! The entry point is [`reconcile_media_workflows`]: it drops previously
//! synthesized `mediapm.media.*` workflow names, rebuilds them from the
//! mediapm document, and persists the result into the conductor generated
//! document (adding only the external-data refs those workflows consume).

#![allow(dead_code)]

pub(crate) mod deno;
pub(crate) mod ffmpeg;
pub(crate) mod media_tagger;
pub(crate) mod rsgain;
pub(crate) mod sd;
pub(crate) mod spec;
pub(crate) mod variant_binding;
pub(crate) mod yt_dlp;
pub(crate) mod yt_dlp_inputs;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::str::FromStr;

use mediapm_cas::Hash;
use mediapm_conductor::{
    ExternalDataEntry, NickelDocument, OutputCaptureSpec, OutputSaveMode, SaveMode, WorkflowSpec,
    WorkflowStepSpec,
};

use crate::conductor_bridge::documents::save_conductor_generated_document;
use crate::conductor_bridge::sync::find_active_tool_spec;
pub(crate) use crate::conductor_bridge::tool_runtime::FfmpegSlotLimits;
use crate::config::output_types::{ResolvedStepVariantFlow, resolve_step_variant_flow};
use crate::config::source_types::step_option_scalar;
use crate::config::{
    GenericOutputVariantConfig, MediaPmDocument, MediaSourceSpec, MediaStep, MediaStepTool,
    OutputCaptureKind, OutputSaveConfig, OutputVariantValue, TransformInputValue,
};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
pub(crate) use variant_binding::resolve_media_variant_output_binding_with_limits;

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

/// Prefix for managed workflow names synthesized by mediapm.
pub(crate) const MANAGED_WORKFLOW_PREFIX: &str = "mediapm.media.";

/// Prefix for managed external data descriptions.
pub(crate) const MANAGED_EXTERNAL_DESCRIPTION_PREFIX: &str = "managed external data:";

/// Logical output name for source-ingest primary results.
pub(crate) const OUTPUT_PRIMARY: &str = "primary";
/// Logical output name for import result (CAS hash pointer).
pub(crate) const OUTPUT_IMPORT_RESULT: &str = "result";

/// Logical input name for source URI.
pub(crate) const INPUT_SOURCE_URL: &str = "source_url";
/// Logical input name for import kind selection.
pub(crate) const INPUT_IMPORT_KIND: &str = "kind";
/// Input name for the import builtin's CAS hash param (`kind=cas_hash`).
pub(crate) const INPUT_IMPORT_HASH: &str = "hash";
/// Value for import kind: CAS hash pointer.
pub(crate) const IMPORT_KIND_CAS_HASH: &str = "cas_hash";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Converts a mediapm [`OutputSaveConfig`] to a conductor [`OutputSaveMode`].
#[must_use]
pub(crate) fn conductor_output_save_mode(config: OutputSaveConfig) -> OutputSaveMode {
    match config {
        OutputSaveConfig::Bool(true) => OutputSaveMode::Saved,
        OutputSaveConfig::Bool(false) => OutputSaveMode::Unsaved,
        OutputSaveConfig::Full => OutputSaveMode::Full,
    }
}

/// Resolves the managed conductor tool id for one media-step tool.
///
/// Conductor matches workflow step `tool` by bare [`ToolSpec`] name (the
/// name-not-key contract), so the returned id is `spec.name` — never the
/// generated-doc map key (`{name}@{hash}`). The import builtin resolves
/// through the same lookup: registration inserts `"import@v1"` keyed with
/// `name: "import"`.
///
/// # Errors
///
/// Returns `MediaPmError::Workflow` when the logical tool is not active
/// (managed tools require a prior tool sync) or the import builtin is not
/// registered.
pub(crate) fn resolve_step_tool_id(
    tool: MediaStepTool,
    generated_doc: &NickelDocument,
) -> Result<String, MediaPmError> {
    let tool_name = tool.as_str();
    let (_, spec) = find_active_tool_spec(generated_doc, tool_name).ok_or_else(|| {
        if tool == MediaStepTool::Import {
            MediaPmError::Workflow(
                "builtin tool 'import@v1' is required but not registered in conductor machine config"
                    .to_string(),
            )
        } else {
            MediaPmError::Workflow(format!(
                "logical tool '{tool_name}' is required but not active; add it under mediapm.ncl tools and run tool sync"
            ))
        }
    })?;
    Ok(spec.name.clone())
}

/// Resolves the managed conductor tool id for a dependency tool.
///
/// `tool_name` is the dependency's bare logical tool id (for example
/// `"ffmpeg"`); see [`resolve_step_tool_id`] for the name-not-key contract.
///
/// # Errors
///
/// Returns `MediaPmError::Workflow` when the dependency is not active.
pub(crate) fn resolve_selected_dependency_tool_id(
    tool_name: &str,
    generated_doc: &NickelDocument,
) -> Result<String, MediaPmError> {
    let (_, spec) = find_active_tool_spec(generated_doc, tool_name).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "dependency tool '{tool_name}' is required but not active; add it under mediapm.ncl tools and run tool sync"
        ))
    })?;
    Ok(spec.name.clone())
}

/// Builds a step-output capture spec from one decoded variant config.
#[must_use]
pub(crate) fn variant_to_output_capture_spec(
    name: &str,
    config: &OutputVariantValue,
) -> OutputCaptureSpec {
    match config {
        OutputVariantValue::Generic(g) => {
            let (capture, save) = generic_variant_capture_and_save(g);
            OutputCaptureSpec {
                name: name.to_string(),
                capture,
                save,
                allow_empty: false,
                include_topmost_folder: true,
            }
        }
        OutputVariantValue::YtDlp(y) => {
            let (capture, save) = yt_dlp::yt_dlp_variant_capture_and_save(y);
            OutputCaptureSpec {
                name: name.to_string(),
                capture,
                save,
                allow_empty: false,
                include_topmost_folder: true,
            }
        }
    }
}

fn generic_variant_capture_and_save(config: &GenericOutputVariantConfig) -> (String, SaveMode) {
    let capture = match config.capture_kind {
        Some(OutputCaptureKind::Folder) => format!("file:{}/*", config.kind),
        _ => format!("file:{}", config.kind),
    };
    let save = match config.save {
        OutputSaveConfig::Bool(true) => SaveMode::True,
        OutputSaveConfig::Bool(false) => SaveMode::False,
        OutputSaveConfig::Full => SaveMode::Full,
    };
    (capture, save)
}

/// Returns managed workflow name for a source entry.
#[must_use]
pub(crate) fn managed_workflow_name(media_id: &str) -> String {
    format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")
}

/// Returns the default source-URI input binding for a media step.
///
/// `MediaSourceSpec` carries no URI field, so the default binding is empty;
/// the actual URI comes from the step `uri` option when present.
#[must_use]
pub(crate) fn source_uri_input(_source: &MediaSourceSpec) -> (String, String) {
    (INPUT_SOURCE_URL.to_string(), String::new())
}

/// Option keys never forwarded as raw tool inputs.
///
/// These are reserved for per-tool synthesizer handling: source-URI
/// resolution (`uri`), import kind/hash selection (`kind`, `hash`), and
/// media-tagger MBID lookups (`recording_mbid`, `release_mbid`).
const STEP_OPTION_INPUT_SKIP_LIST: &[&str] =
    &["uri", "kind", "hash", "recording_mbid", "release_mbid"];

/// Returns step-option input bindings as `(input_key, value_string)` entries.
///
/// Options on [`STEP_OPTION_INPUT_SKIP_LIST`] are handled by the per-tool
/// synthesizers and are never bound as raw tool inputs.
pub(crate) fn step_option_input_bindings(step: &MediaStep) -> Vec<(String, String)> {
    step.options
        .iter()
        .filter(|(key, _)| !STEP_OPTION_INPUT_SKIP_LIST.contains(&key.as_str()))
        .map(|(k, v)| {
            let value = match v {
                TransformInputValue::String(s) => s.clone(),
            };
            (k.clone(), value)
        })
        .collect()
}

/// Returns true when the given output-variant config has folder-like capture.
#[must_use]
pub(crate) fn variant_is_folder_capture(config: &OutputVariantValue) -> bool {
    match config {
        OutputVariantValue::Generic(g) => {
            matches!(g.capture_kind, Some(OutputCaptureKind::Folder))
        }
        OutputVariantValue::YtDlp(y) => matches!(
            y.kind,
            crate::config::YtDlpOutputKind::Subtitles
                | crate::config::YtDlpOutputKind::Thumbnails
                | crate::config::YtDlpOutputKind::Chapters
                | crate::config::YtDlpOutputKind::Links
        ),
    }
}

/// Prefix delegated step ids with the source media id to avoid collisions.
#[must_use]
pub(crate) fn qualify_step_id(source_id: &str, suffix: &str) -> String {
    format!("{source_id}.{suffix}")
}

// ---------------------------------------------------------------------------
// Synthesis orchestration
// ---------------------------------------------------------------------------

/// Resolves ffmpeg slot limits from the mediapm document's ffmpeg tool
/// requirement, falling back to config defaults when ffmpeg is not declared.
#[must_use]
pub(crate) fn resolve_ffmpeg_slot_limits(document: &MediaPmDocument) -> FfmpegSlotLimits {
    let (max_input, max_output) = document.tools.get("ffmpeg").map_or(
        (
            crate::config::defaults::DEFAULT_FFMPEG_MAX_INPUT_SLOTS,
            crate::config::defaults::DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS,
        ),
        |requirement| (requirement.max_input_slots, requirement.max_output_slots),
    );
    crate::conductor_bridge::tool_runtime::resolve_ffmpeg_slot_limits(max_input, max_output)
}

/// Builds per-output persistence overrides for one step output variant.
///
/// The generic binding name equals the variant key; per-tool naming
/// differences (yt-dlp output kinds, ffmpeg indexed slots) are handled by
/// the per-tool synthesizers.
///
/// # Errors
///
/// Returns `MediaPmError::Workflow` when the variant is not declared.
pub(crate) fn step_output_policy_overrides(
    output_variants: &BTreeMap<String, OutputVariantValue>,
    output_variant: &str,
) -> Result<BTreeMap<String, OutputCaptureSpec>, MediaPmError> {
    let value = output_variants.get(output_variant).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "missing output variant '{output_variant}' while resolving output policy"
        ))
    })?;
    let capture_spec = variant_to_output_capture_spec(output_variant, value);
    Ok(BTreeMap::from([(output_variant.to_string(), capture_spec)]))
}

/// One variant-source producer binding available to downstream steps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VariantProducer {
    /// Variant bytes come from one external-data reference.
    ExternalData { hash: Hash },
    /// Variant bytes come from one prior step output.
    StepOutput {
        step_id: String,
        output_name: String,
        zip_member: Option<String>,
        /// Tracked container extension for downstream synthesizers (no leading dot).
        extension: Option<String>,
    },
}

impl VariantProducer {
    /// Returns the tracked output extension when the producer records one.
    #[must_use]
    pub(crate) fn output_extension(&self) -> Option<&str> {
        match self {
            Self::ExternalData { .. } => None,
            Self::StepOutput { extension, .. } => extension.as_deref(),
        }
    }

    /// Renders this producer into one input expression plus optional
    /// execution-order dependency on the producing step id.
    ///
    /// # Errors
    ///
    /// Currently infallible; `Result` keeps the contract stable for future
    /// producer kinds that require configuration validation.
    #[allow(clippy::unnecessary_wraps)] // must match the fallible binding contract
    pub(crate) fn to_binding(&self) -> Result<(String, Option<String>), MediaPmError> {
        match self {
            Self::ExternalData { hash } => Ok((format!("${{external_data.{hash}}}"), None)),
            Self::StepOutput { step_id, output_name, zip_member, .. } => {
                let expression = if let Some(member) = zip_member.as_deref() {
                    format!("${{step_output.{step_id}.{output_name}:zip({member})}}")
                } else {
                    format!("${{step_output.{step_id}.{output_name}}}")
                };
                Ok((expression, Some(step_id.clone())))
            }
        }
    }
}

/// Resolves one input variant to a producer plus optional ZIP-member selector.
///
/// Resolution priority: exact key match only (plain lookup; the generic
/// per-mapping loop reports unknown variants itself).
pub(crate) fn resolve_input_variant_producer<'a>(
    input_variant: &str,
    producer_snapshot: &'a BTreeMap<String, VariantProducer>,
) -> Option<&'a VariantProducer> {
    producer_snapshot.get(input_variant)
}

/// Builds a deterministic step id for one variant-flow mapping edge.
pub(crate) fn media_step_id(
    step_index: usize,
    mapping_index: usize,
    tool: MediaStepTool,
    mapping: &ResolvedStepVariantFlow,
) -> String {
    format!(
        "step-{step_index}-{mapping_index}-{}-{}-to-{}",
        tool.as_str(),
        sanitize_identifier(&mapping.input),
        sanitize_identifier(&mapping.output)
    )
}

/// Builds a deterministic step id for one aggregated ffmpeg media step.
pub(crate) fn ffmpeg_step_id(step_index: usize) -> String {
    format!("step-{step_index}-ffmpeg")
}

/// Normalizes one identifier segment into a lowercase ASCII-safe token.
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
            ExternalDataEntry { description: None, save_mode: OutputSaveMode::Saved },
        );
        variant_producers.insert(variant.clone(), VariantProducer::ExternalData { hash });
    }

    Ok(())
}

/// Builds import-step output capture: the import builtin emits stdout bytes,
/// not sandbox file paths, regardless of variant policy kind labels.
fn import_step_output_capture(
    output_variants: &BTreeMap<String, OutputVariantValue>,
    output_variant: &str,
) -> Result<BTreeMap<String, OutputCaptureSpec>, MediaPmError> {
    let value = output_variants.get(output_variant).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "missing output variant '{output_variant}' while resolving import output policy"
        ))
    })?;
    let save = match value {
        OutputVariantValue::Generic(g) => match g.save {
            OutputSaveConfig::Bool(true) => SaveMode::True,
            OutputSaveConfig::Bool(false) => SaveMode::False,
            OutputSaveConfig::Full => SaveMode::Full,
        },
        OutputVariantValue::YtDlp(y) => match y.save {
            OutputSaveConfig::Bool(true) => SaveMode::True,
            OutputSaveConfig::Bool(false) => SaveMode::False,
            OutputSaveConfig::Full => SaveMode::Full,
        },
    };
    Ok(BTreeMap::from([(
        output_variant.to_string(),
        OutputCaptureSpec {
            name: output_variant.to_string(),
            capture: "stdout".to_string(),
            save,
            allow_empty: false,
            include_topmost_folder: true,
        },
    )]))
}

/// submodule). The CAS hash is bound to the import builtin's `hash` input
/// per its `kind=cas_hash` contract.
#[allow(clippy::too_many_arguments)]
fn synthesize_import_step(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    generated_doc: &NickelDocument,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
) -> Result<(), MediaPmError> {
    let tool_id = resolve_step_tool_id(step.tool, generated_doc)?;
    let kind = step_option_scalar(step, INPUT_IMPORT_KIND)
        .map_or_else(|| IMPORT_KIND_CAS_HASH.to_string(), ToString::to_string);
    let hash = step_option_scalar(step, "hash").map(ToString::to_string).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' step #{step_index} uses tool '{}' and must define options.hash",
            step.tool.as_str()
        ))
    })?;

    let mut pending_variant_updates = Vec::new();

    for (mapping_index, mapping) in mappings.iter().enumerate() {
        let step_id = media_step_id(step_index, mapping_index, step.tool, mapping);

        let mut inputs = BTreeMap::new();
        // The import builtin's `cas_hash` arm reads its payload under the
        // `hash` param (not `source_url`, which is the executable-side input
        // name used by the media tools).
        inputs.insert(INPUT_IMPORT_HASH.to_string(), hash.clone());
        inputs.insert(INPUT_IMPORT_KIND.to_string(), kind.clone());

        let outputs = import_step_output_capture(&step.output_variants, &mapping.output)?;

        workflow.steps.push(WorkflowStepSpec {
            id: step_id.clone(),
            tool: tool_id.clone(),
            inputs,
            outputs,
            max_retries: 0,
            depends_on: Vec::new(),
        });

        pending_variant_updates.push((
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id,
                output_name: mapping.output.clone(),
                zip_member: None,
                extension: None,
            },
        ));
    }

    for (output_variant, producer) in pending_variant_updates {
        variant_producers.insert(output_variant, producer);
    }

    Ok(())
}

/// Creates ordered workflow steps from unified media-step declarations.
///
/// Import steps are synthesized inline; the four managed tools delegate to
/// their per-tool submodule synthesizers (which own step building AND
/// variant-producer registration).
#[allow(clippy::too_many_arguments)]
fn synthesize_media_steps(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    source: &MediaSourceSpec,
    generated_doc: &NickelDocument,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
    media_tagger_cache_dir: &Path,
) -> Result<(), MediaPmError> {
    for (step_index, step) in source.steps.iter().enumerate() {
        let mappings = resolve_step_variant_flow(step)
            .map_err(|reason| MediaPmError::Workflow(format!("step #{step_index} {reason}")))?;
        let producer_snapshot = variant_producers.clone();

        match step.tool {
            MediaStepTool::Import => {
                synthesize_import_step(
                    workflow,
                    media_id,
                    step_index,
                    step,
                    &mappings,
                    generated_doc,
                    variant_producers,
                )?;
            }
            MediaStepTool::YtDlp => {
                let tool_id = resolve_step_tool_id(step.tool, generated_doc)?;
                yt_dlp::synthesize_yt_dlp_step(
                    workflow,
                    media_id,
                    step_index,
                    step,
                    &mappings,
                    &tool_id,
                    &producer_snapshot,
                    variant_producers,
                )?;
            }
            MediaStepTool::Ffmpeg => {
                let tool_id = resolve_step_tool_id(step.tool, generated_doc)?;
                ffmpeg::synthesize_ffmpeg_step(
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
            }
            MediaStepTool::MediaTagger => {
                let tool_id = resolve_step_tool_id(step.tool, generated_doc)?;
                media_tagger::synthesize_media_tagger_step(
                    workflow,
                    media_id,
                    step_index,
                    step,
                    &mappings,
                    &tool_id,
                    generated_doc,
                    &producer_snapshot,
                    variant_producers,
                    media_tagger_cache_dir,
                )?;
            }
            MediaStepTool::Rsgain => {
                let tool_id = resolve_step_tool_id(step.tool, generated_doc)?;
                rsgain::synthesize_rsgain_step_chain(
                    workflow,
                    media_id,
                    step_index,
                    step,
                    &mappings,
                    &tool_id,
                    generated_doc,
                    &producer_snapshot,
                    variant_producers,
                    ffmpeg_slot_limits,
                )?;
            }
        }
    }

    Ok(())
}

/// Desired managed workflows plus the external-data refs they consume.
#[derive(Debug, Clone, Default)]
pub(crate) struct MediaWorkflowPlan {
    /// Desired managed workflows keyed by workflow name.
    pub(crate) workflows: BTreeMap<String, WorkflowSpec>,
    /// Desired managed external-data refs keyed by CAS hash identity.
    pub(crate) external_data: BTreeMap<Hash, ExternalDataEntry>,
}

/// Builds the full managed workflow/external-data plan from `mediapm` config.
#[cfg(test)]
fn build_media_workflow_plan(
    document: &MediaPmDocument,
    generated_doc: &NickelDocument,
) -> Result<MediaWorkflowPlan, MediaPmError> {
    build_media_workflow_plan_with_limits(
        document,
        generated_doc,
        FfmpegSlotLimits::default(),
        Path::new(""),
    )
}

/// Builds the full managed workflow/external-data plan from `mediapm` config
/// with explicit ffmpeg slot limits.
fn build_media_workflow_plan_with_limits(
    document: &MediaPmDocument,
    generated_doc: &NickelDocument,
    ffmpeg_slot_limits: FfmpegSlotLimits,
    media_tagger_cache_dir: &Path,
) -> Result<MediaWorkflowPlan, MediaPmError> {
    let mut plan = MediaWorkflowPlan::default();

    for (media_id, source) in &document.media {
        let mut workflow = WorkflowSpec {
            name: managed_workflow_name(media_id),
            description: (!source.description.is_empty()).then(|| source.description.clone()),
            display_name: None,
            impure: false,
            steps: Vec::new(),
        };
        let mut variant_producers = BTreeMap::<String, VariantProducer>::new();

        seed_local_variant_sources(&mut plan, media_id, source, &mut variant_producers)?;
        synthesize_media_steps(
            &mut workflow,
            media_id,
            source,
            generated_doc,
            &mut variant_producers,
            ffmpeg_slot_limits,
            media_tagger_cache_dir,
        )?;

        plan.workflows.insert(managed_workflow_name(media_id), workflow);
    }

    Ok(plan)
}

/// Collects every `${external_data.{hash}}` reference found in user-owned
/// workflow step inputs and output captures.
fn collect_user_workflow_external_data_hashes(user_doc: &NickelDocument) -> BTreeSet<Hash> {
    let mut hashes = BTreeSet::new();
    for workflow in &user_doc.workflows {
        for step in &workflow.steps {
            for value in step.inputs.values() {
                collect_external_data_hash(value, &mut hashes);
            }
            for capture in step.outputs.values() {
                collect_external_data_hash(&capture.capture, &mut hashes);
            }
        }
    }
    hashes
}

/// Extracts all `${external_data.{hash}}` references from one string value.
fn collect_external_data_hash(value: &str, hashes: &mut BTreeSet<Hash>) {
    const PREFIX: &str = "${external_data.";
    let mut rest = value;
    while let Some(start) = rest.find(PREFIX) {
        let remainder = &rest[start + PREFIX.len()..];
        let Some(end) = remainder.find('}') else { break };
        if let Ok(hash) = Hash::from_str(&remainder[..end]) {
            hashes.insert(hash);
        }
        rest = remainder;
    }
}

/// Reconciles managed media workflows into the conductor generated document.
///
/// Drops all previously synthesized `mediapm.media.*` workflow names, rebuilds
/// them from the mediapm document, adds the external-data refs the workflows
/// consume (never overwriting entries the tools-phase reconcile left behind),
/// and persists via [`save_conductor_generated_document`]. External data is
/// never re-identified by description: descriptions stay `None` through
/// processing and the tools-phase reconcile owns the full rebuild.
///
/// # Errors
///
/// Returns `MediaPmError` when a workflow cannot be synthesized (invalid
/// variant flow, missing hash, unresolvable tool) or the generated document
/// fails to persist.
pub(crate) fn reconcile_media_workflows(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    generated_doc: &mut NickelDocument,
    user_doc: Option<&NickelDocument>,
) -> Result<(), MediaPmError> {
    generated_doc.workflows.retain(|workflow| !workflow.name.starts_with(MANAGED_WORKFLOW_PREFIX));

    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(document);
    let plan = build_media_workflow_plan_with_limits(
        document,
        generated_doc,
        ffmpeg_slot_limits,
        &paths.workspace_media_tagger_cache_dir(),
    )?;

    generated_doc.workflows.extend(plan.workflows.into_values());
    for (hash, entry) in plan.external_data {
        generated_doc.external_data.entry(hash).or_insert(entry);
    }

    // User-owned workflows may reference external-data hashes directly; add
    // placeholder entries so the generated-doc encode invariant
    // (`external_data` ⊇ referenced hashes) holds even before the tools
    // phase rebuilds the map from usage.
    if let Some(user_doc) = user_doc {
        for hash in collect_user_workflow_external_data_hashes(user_doc) {
            generated_doc.external_data.entry(hash).or_insert(ExternalDataEntry {
                description: None,
                save_mode: OutputSaveMode::Saved,
            });
        }
    }

    save_conductor_generated_document(paths, generated_doc)
}

#[cfg(test)]
mod tests {
    use super::*;

    use mediapm_conductor::{ToolKindSpec, ToolSpec};
    use serde_json::{Value, json};

    // ---------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------

    const ZERO_HASH: &str =
        "blake3:0000000000000000000000000000000000000000000000000000000000000000";
    const ONE_HASH: &str =
        "blake3:1111111111111111111111111111111111111111111111111111111111111111";
    const AAA_HASH: &str =
        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn generic_output_variant(kind: &str) -> Value {
        json!({ "kind": kind, "save": true })
    }

    fn ffmpeg_output_variant(idx: u32) -> Value {
        json!({ "kind": "output_content", "save": true, "idx": idx })
    }

    fn yt_dlp_output_variant(kind: &str) -> Value {
        json!({ "kind": kind, "save": true })
    }

    fn media_step(
        tool: MediaStepTool,
        input_variants: Vec<&str>,
        output_variants: Vec<(&str, Value)>,
        options: Vec<(&str, &str)>,
    ) -> MediaStep {
        MediaStep {
            tool,
            input_variants: input_variants.into_iter().map(str::to_string).collect(),
            output_variants: output_variants
                .into_iter()
                .map(|(key, value)| {
                    (
                        key.to_string(),
                        serde_json::from_value::<OutputVariantValue>(value)
                            .expect("decode output variant"),
                    )
                })
                .collect(),
            options: options
                .into_iter()
                .map(|(key, value)| {
                    (key.to_string(), TransformInputValue::String(value.to_string()))
                })
                .collect(),
        }
    }

    fn media_source(
        description: &str,
        variant_hashes: Vec<(&str, &str)>,
        steps: Vec<MediaStep>,
    ) -> MediaSourceSpec {
        MediaSourceSpec {
            description: description.to_string(),
            variant_hashes: variant_hashes
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            steps,
            ..MediaSourceSpec::default()
        }
    }

    fn media_document(media_id: &str, source: MediaSourceSpec) -> MediaPmDocument {
        MediaPmDocument {
            media: BTreeMap::from([(media_id.to_string(), source)]),
            ..MediaPmDocument::default()
        }
    }

    fn tools_document(names: &[&str]) -> NickelDocument {
        NickelDocument {
            tools: names
                .iter()
                .map(|name| {
                    (
                        name.to_string(),
                        ToolSpec {
                            name: name.to_string(),
                            kind: ToolKindSpec::default(),
                            ..ToolSpec::default()
                        },
                    )
                })
                .collect(),
            ..NickelDocument::default()
        }
    }

    fn import_document() -> NickelDocument {
        NickelDocument {
            tools: BTreeMap::from([(
                "import@v1".to_string(),
                ToolSpec {
                    name: "import".to_string(),
                    kind: ToolKindSpec::Builtin { builtin_id: "import@v1".to_string() },
                    ..ToolSpec::default()
                },
            )]),
            ..NickelDocument::default()
        }
    }

    fn build_plan(
        document: &MediaPmDocument,
        generated_doc: &NickelDocument,
    ) -> Result<MediaWorkflowPlan, MediaPmError> {
        build_media_workflow_plan_with_limits(
            document,
            generated_doc,
            resolve_ffmpeg_slot_limits(document),
            Path::new(""),
        )
    }

    // ---------------------------------------------------------------------
    // Workflow-plan synthesis
    // ---------------------------------------------------------------------

    #[test]
    fn plan_builds_exactly_one_workflow_per_media() {
        let document = MediaPmDocument {
            media: BTreeMap::from([
                ("media-a".to_string(), media_source("", vec![("default", ZERO_HASH)], vec![])),
                (
                    "media-b".to_string(),
                    media_source("custom media description", vec![("default", ONE_HASH)], vec![]),
                ),
            ]),
            ..MediaPmDocument::default()
        };
        let generated_doc = NickelDocument::default();
        let plan = build_plan(&document, &generated_doc).expect("plan");

        assert_eq!(plan.workflows.len(), 2);
        let workflow_a =
            plan.workflows.get(&managed_workflow_name("media-a")).expect("media-a workflow");
        assert_eq!(workflow_a.name, managed_workflow_name("media-a"));
        assert_eq!(workflow_a.description, None);
        let workflow_b =
            plan.workflows.get(&managed_workflow_name("media-b")).expect("media-b workflow");
        assert_eq!(workflow_b.name, managed_workflow_name("media-b"));
        assert_eq!(workflow_b.description.as_deref(), Some("custom media description"));
        // The old `workflow_id` field is gone, so no `custom.workflow.media-b` key exists.
        assert!(!plan.workflows.contains_key("custom.workflow.media-b"));

        assert_eq!(plan.external_data.len(), 2);
        for reference in plan.external_data.values() {
            assert!(reference.description.is_none());
            assert_eq!(reference.save_mode, OutputSaveMode::Saved);
        }
        assert!(plan.external_data.keys().all(|hash| hash.to_string().starts_with("blake3:")));
    }

    #[test]
    fn variant_flow_creates_explicit_step_dependencies() {
        let document = media_document(
            "remote-a",
            media_source(
                "",
                vec![],
                vec![
                    media_step(
                        MediaStepTool::YtDlp,
                        vec![],
                        vec![("default", yt_dlp_output_variant("primary"))],
                        vec![("uri", "https://example.com/video")],
                    ),
                    media_step(
                        MediaStepTool::Ffmpeg,
                        vec!["default"],
                        vec![("aac", ffmpeg_output_variant(0))],
                        vec![],
                    ),
                    media_step(
                        MediaStepTool::Rsgain,
                        vec!["aac"],
                        vec![("aac", generic_output_variant("output_content"))],
                        vec![],
                    ),
                ],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp", "ffmpeg", "rsgain", "sd"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let workflow = &plan.workflows[&managed_workflow_name("remote-a")];

        assert_eq!(workflow.steps.len(), 8);
        let download = &workflow.steps[0];
        let ffmpeg_step = &workflow.steps[1];
        let rsgain_apply = workflow
            .steps
            .iter()
            .find(|step| step.id.ends_with("-ffmpeg-apply"))
            .expect("rsgain apply step");
        assert_eq!(download.id, "step-0-0-yt-dlp-default-to-default");
        assert_eq!(ffmpeg_step.id, "step-1-ffmpeg");
        assert!(download.depends_on.is_empty());
        assert_eq!(ffmpeg_step.depends_on, vec![download.id.clone()]);
        assert!(
            rsgain_apply.depends_on.iter().any(|dep| dep.contains("sd-rewrite-r128-metadata")),
            "apply step should depend on sd metadata rewrite: {:?}",
            rsgain_apply.depends_on
        );
        assert_eq!(
            ffmpeg_step.inputs.get("output_path_0").map(String::as_str),
            Some("output-0.mkv"),
            "ffmpeg step must bind sandbox output path for slot 0"
        );
    }

    #[test]
    fn media_tagger_step_expands_to_metadata_and_apply_steps() {
        let document = media_document(
            "tag-a",
            media_source(
                "",
                vec![("default", AAA_HASH)],
                vec![media_step(
                    MediaStepTool::MediaTagger,
                    vec!["default"],
                    vec![("tagged", generic_output_variant("output_content"))],
                    vec![
                        ("strict_identification", "false"),
                        ("ffmpeg_version", "global"),
                        ("output_container", "mp4"),
                    ],
                )],
            ),
        );
        let generated_doc = tools_document(&["media-tagger", "ffmpeg"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let workflow = &plan.workflows[&managed_workflow_name("tag-a")];

        assert_eq!(workflow.steps.len(), 2);
        let metadata = &workflow.steps[0];
        let apply = &workflow.steps[1];
        assert_eq!(metadata.id, "step-0-0-media-tagger-default-to-tagged-metadata");
        assert_eq!(metadata.tool, "media-tagger");
        assert_eq!(apply.id, "step-0-0-media-tagger-default-to-tagged-apply");
        assert_eq!(apply.tool, "ffmpeg");

        let metadata_output = metadata.outputs.get("content").expect("metadata content output");
        assert_eq!(metadata_output.save, SaveMode::Full);
        assert_eq!(metadata_output.capture, "file:metadata/output.ffmeta");
        assert_eq!(metadata.inputs.get("ffmpeg_bin").map(String::as_str), Some("ffmpeg"));
        assert_eq!(metadata.inputs.get("strict_identification").map(String::as_str), Some("false"));
        assert!(!metadata.inputs.contains_key("ffmpeg_version"));
        assert!(!metadata.inputs.contains_key("output_container"));

        assert_eq!(apply.depends_on, vec![metadata.id.clone()]);
        let expected_ffmetadata = format!("${{step_output.{}.content}}", metadata.id);
        assert_eq!(apply.inputs.get("ffmetadata_content"), Some(&expected_ffmetadata));
        assert_eq!(apply.inputs.get("container").map(String::as_str), Some("mp4"));
        let apply_output = apply.outputs.get("primary").expect("apply primary output");
        assert_eq!(apply_output.save, SaveMode::True);
    }

    #[test]
    fn import_step_synthesizes_builtin_import_binding() {
        let document = media_document(
            "local-a",
            media_source(
                "",
                vec![],
                vec![media_step(
                    MediaStepTool::Import,
                    vec![],
                    vec![("source", generic_output_variant("output_content"))],
                    vec![("kind", "cas_hash"), ("hash", AAA_HASH)],
                )],
            ),
        );
        let generated_doc = import_document();
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let workflow = &plan.workflows[&managed_workflow_name("local-a")];

        assert_eq!(workflow.steps.len(), 1);
        let step = &workflow.steps[0];
        assert_eq!(step.id, "step-0-0-import-source-to-source");
        assert_eq!(step.tool, "import");
        assert_eq!(step.inputs.get("hash").map(String::as_str), Some(AAA_HASH));
        assert_eq!(step.inputs.get("kind").map(String::as_str), Some("cas_hash"));
        assert!(step.depends_on.is_empty());
        assert_eq!(step.outputs.get("source").map(|output| output.save), Some(SaveMode::True));
        assert_eq!(
            step.outputs.get("source").map(|output| output.capture.as_str()),
            Some("stdout")
        );
    }

    // ---------------------------------------------------------------------
    // Output-policy mapping
    // ---------------------------------------------------------------------

    #[test]
    fn step_output_variant_policy_maps_to_workflow_output_policy() {
        let document = media_document(
            "policy-a",
            media_source(
                "",
                vec![("source", AAA_HASH)],
                vec![media_step(
                    MediaStepTool::Ffmpeg,
                    vec!["source"],
                    vec![(
                        "normalized",
                        json!({ "kind": "output_content", "save": false, "idx": 0 }),
                    )],
                    vec![],
                )],
            ),
        );
        let generated_doc = tools_document(&["ffmpeg"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let step = &plan.workflows[&managed_workflow_name("policy-a")].steps[0];

        assert_eq!(step.outputs.get("primary").map(|output| output.save), Some(SaveMode::False));
    }

    #[test]
    fn yt_dlp_artifact_variant_maps_output_policy_to_artifact_capture() {
        let document = media_document(
            "policy-ytdlp",
            media_source(
                "",
                vec![],
                vec![media_step(
                    MediaStepTool::YtDlp,
                    vec![],
                    vec![("subtitles", json!({ "kind": "subtitles", "save": true }))],
                    vec![("uri", "https://example.com/video")],
                )],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let step = &plan.workflows[&managed_workflow_name("policy-ytdlp")].steps[0];

        assert_eq!(
            step.outputs.get("yt_dlp_subtitle_artifacts").map(|output| output.save),
            Some(SaveMode::True)
        );
        assert!(!step.outputs.contains_key("primary"));
    }

    #[test]
    fn yt_dlp_sidecar_variant_without_policy_still_emits_artifact_output_key() {
        let document = media_document(
            "policy-ytdlp-2",
            media_source(
                "",
                vec![],
                vec![media_step(
                    MediaStepTool::YtDlp,
                    vec![],
                    vec![("thumbnail", yt_dlp_output_variant("thumbnails"))],
                    vec![("uri", "https://example.com/video")],
                )],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let step = &plan.workflows[&managed_workflow_name("policy-ytdlp-2")].steps[0];

        assert!(step.outputs.contains_key("yt_dlp_thumbnail_artifacts"));
        assert!(!step.outputs.contains_key("primary"));
    }

    // ---------------------------------------------------------------------
    // Step-option bindings
    // ---------------------------------------------------------------------

    #[test]
    fn step_option_bindings_keep_non_option_args_values_scalar() {
        let step = media_step(
            MediaStepTool::YtDlp,
            vec![],
            vec![],
            vec![("merge_output_format", "mkv"), ("no_playlist", "true")],
        );
        let bindings = step_option_input_bindings(&step).into_iter().collect::<BTreeMap<_, _>>();

        assert_eq!(bindings.get("merge_output_format").map(String::as_str), Some("mkv"));
        assert_eq!(bindings.get("no_playlist").map(String::as_str), Some("true"));
    }

    #[test]
    fn step_option_bindings_keep_option_args_scalar() {
        let step = media_step(
            MediaStepTool::YtDlp,
            vec![],
            vec![],
            vec![("option_args", "--foo --bar=baz")],
        );
        let bindings = step_option_input_bindings(&step).into_iter().collect::<BTreeMap<_, _>>();

        assert_eq!(bindings.get("option_args").map(String::as_str), Some("--foo --bar=baz"));
    }

    #[test]
    fn step_option_bindings_skip_yt_dlp_uri_option() {
        let step = media_step(
            MediaStepTool::YtDlp,
            vec![],
            vec![],
            vec![("uri", "https://example.com/video")],
        );
        let bindings = step_option_input_bindings(&step).into_iter().collect::<BTreeMap<_, _>>();

        assert!(!bindings.contains_key("uri"));
    }

    // ---------------------------------------------------------------------
    // Variant-binding resolution
    // ---------------------------------------------------------------------

    #[test]
    fn variant_binding_resolves_non_latest_variant_name_when_still_unique() {
        let document = media_document(
            "remote-b",
            media_source(
                "",
                vec![],
                vec![
                    media_step(
                        MediaStepTool::YtDlp,
                        vec![],
                        vec![
                            ("downloaded", yt_dlp_output_variant("primary")),
                            ("subtitles", yt_dlp_output_variant("subtitles")),
                        ],
                        vec![("uri", "https://example.com/video")],
                    ),
                    media_step(
                        MediaStepTool::Ffmpeg,
                        vec!["downloaded"],
                        vec![("video_144p", ffmpeg_output_variant(0))],
                        vec![],
                    ),
                ],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp", "ffmpeg"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let workflow = &plan.workflows[&managed_workflow_name("remote-b")];

        assert_eq!(workflow.steps[0].id, "step-0-0-yt-dlp-downloaded-to-downloaded");
        assert!(workflow.steps[0].outputs.contains_key("primary"));
        assert_eq!(workflow.steps[1].id, "step-0-1-yt-dlp-subtitles-to-subtitles");
        assert!(workflow.steps[1].outputs.contains_key("yt_dlp_subtitle_artifacts"));
        assert_eq!(workflow.steps[2].id, "step-1-ffmpeg");
        assert_eq!(
            workflow.steps[2].inputs.get("input_content_0").map(String::as_str),
            Some("${step_output.step-0-0-yt-dlp-downloaded-to-downloaded.primary}")
        );
    }

    #[test]
    fn variant_binding_uses_last_producer_for_duplicate_output_variant() {
        let document = media_document(
            "normalized-a",
            media_source(
                "",
                vec![],
                vec![
                    media_step(
                        MediaStepTool::YtDlp,
                        vec![],
                        vec![("downloaded", yt_dlp_output_variant("primary"))],
                        vec![("uri", "https://example.com/video")],
                    ),
                    media_step(
                        MediaStepTool::Ffmpeg,
                        vec!["downloaded"],
                        vec![("normalized", ffmpeg_output_variant(0))],
                        vec![],
                    ),
                    media_step(
                        MediaStepTool::Rsgain,
                        vec!["normalized"],
                        vec![("normalized", generic_output_variant("output_content"))],
                        vec![],
                    ),
                ],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp", "ffmpeg", "rsgain", "sd"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let workflow = &plan.workflows[&managed_workflow_name("normalized-a")];

        let apply_step_id = "step-2-0-rsgain-normalized-to-normalized-ffmpeg-apply";
        let apply_step =
            workflow.steps.iter().find(|step| step.id == apply_step_id).expect("rsgain apply step");
        assert!(apply_step.outputs.contains_key("primary"));
    }

    #[test]
    fn variant_binding_supports_custom_ffmpeg_output_limit() {
        let document = media_document(
            "limit-a",
            media_source(
                "",
                vec![("default", AAA_HASH)],
                vec![media_step(
                    MediaStepTool::Ffmpeg,
                    vec!["default"],
                    vec![("hi", ffmpeg_output_variant(70))],
                    vec![],
                )],
            ),
        );
        let generated_doc = tools_document(&["ffmpeg"]);

        let error = build_media_workflow_plan_with_limits(
            &document,
            &generated_doc,
            resolve_ffmpeg_slot_limits(&document),
            Path::new(""),
        )
        .expect_err("default slot limits reject idx 70");
        assert!(
            error.to_string().contains("tools.ffmpeg.max_output_slots"),
            "unexpected error: {error}"
        );

        let plan = build_media_workflow_plan_with_limits(
            &document,
            &generated_doc,
            FfmpegSlotLimits { max_input_slots: 128, max_output_slots: 128 },
            Path::new(""),
        )
        .expect("custom slot limits accept idx 70");
        assert!(
            plan.workflows[&managed_workflow_name("limit-a")].steps[0]
                .outputs
                .contains_key("primary_70")
        );
    }

    // ---------------------------------------------------------------------
    // yt-dlp sidecar variants
    // ---------------------------------------------------------------------

    #[test]
    fn yt_dlp_description_binding_uses_file_capture_without_zip_member() {
        let document = media_document(
            "desc-a",
            media_source(
                "",
                vec![],
                vec![media_step(
                    MediaStepTool::YtDlp,
                    vec![],
                    vec![("description", yt_dlp_output_variant("description"))],
                    vec![("uri", "https://example.com/video")],
                )],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let step = &plan.workflows[&managed_workflow_name("desc-a")].steps[0];

        assert_eq!(step.id, "step-0-0-yt-dlp-description-to-description");
        let output = step.outputs.get("yt_dlp_description_file").expect("description output");
        assert_eq!(output.capture, "file_regex:^downloads/.+(?:__mediapm__)?[.]description$");
        assert!(!step.outputs.contains_key("primary"));
    }

    #[test]
    fn yt_dlp_description_and_infojson_steps_set_complementary_flags() {
        let document = media_document(
            "sidecar-a",
            media_source(
                "",
                vec![],
                vec![media_step(
                    MediaStepTool::YtDlp,
                    vec![],
                    vec![
                        ("description", yt_dlp_output_variant("description")),
                        ("info_json", yt_dlp_output_variant("infojson")),
                    ],
                    vec![("uri", "https://example.com/video")],
                )],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let workflow = &plan.workflows[&managed_workflow_name("sidecar-a")];

        let description_step = workflow
            .steps
            .iter()
            .find(|step| step.id == "step-0-0-yt-dlp-description-to-description")
            .expect("description step");
        let info_json_step = workflow
            .steps
            .iter()
            .find(|step| step.id == "step-0-1-yt-dlp-info_json-to-info_json")
            .expect("info-json step");
        assert_eq!(
            description_step.inputs.get("write_description").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            description_step.inputs.get("write_info_json").map(String::as_str),
            Some("false")
        );
        assert_eq!(info_json_step.inputs.get("write_info_json").map(String::as_str), Some("true"));
        assert_eq!(
            info_json_step.inputs.get("write_description").map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn yt_dlp_thumbnail_step_disables_description_and_infojson_sidecars() {
        let document = media_document(
            "thumb-a",
            media_source(
                "",
                vec![],
                vec![media_step(
                    MediaStepTool::YtDlp,
                    vec![],
                    vec![("thumbnails/", yt_dlp_output_variant("thumbnails"))],
                    vec![("uri", "https://example.com/video")],
                )],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let step = &plan.workflows[&managed_workflow_name("thumb-a")].steps[0];

        assert_eq!(step.id, "step-0-0-yt-dlp-thumbnails_-to-thumbnails_");
        assert_eq!(step.inputs.get("write_thumbnail").map(String::as_str), Some("true"));
        assert_eq!(step.inputs.get("write_description").map(String::as_str), Some("false"));
        assert_eq!(step.inputs.get("write_info_json").map(String::as_str), Some("false"));
    }

    // ---------------------------------------------------------------------
    // Scoped subtitle variants
    // ---------------------------------------------------------------------

    #[test]
    fn scoped_input_variant_requires_exact_producer_without_folder_fallback() {
        let document = media_document(
            "scoped-a",
            media_source(
                "",
                vec![],
                vec![
                    media_step(
                        MediaStepTool::YtDlp,
                        vec![],
                        vec![("subtitles/", yt_dlp_output_variant("subtitles"))],
                        vec![("uri", "https://example.com/video")],
                    ),
                    media_step(
                        MediaStepTool::Ffmpeg,
                        vec!["subtitles/en"],
                        vec![("aac", ffmpeg_output_variant(0))],
                        vec![],
                    ),
                ],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp", "ffmpeg"]);
        let error = build_plan(&document, &generated_doc).expect_err("no exact producer");
        assert!(
            error.to_string().contains("references unknown input variant 'subtitles/en'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn scoped_input_variant_prefers_exact_output_over_folder_fallback() {
        let document = media_document(
            "scoped-b",
            media_source(
                "",
                vec![],
                vec![
                    media_step(
                        MediaStepTool::YtDlp,
                        vec![],
                        vec![
                            ("subtitles/", yt_dlp_output_variant("subtitles")),
                            (
                                "subtitles/en",
                                json!({ "kind": "subtitles", "save": true, "langs": "en" }),
                            ),
                        ],
                        vec![("uri", "https://example.com/video")],
                    ),
                    media_step(
                        MediaStepTool::Ffmpeg,
                        vec!["subtitles/en"],
                        vec![("aac", ffmpeg_output_variant(0))],
                        vec![],
                    ),
                ],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp", "ffmpeg"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let workflow = &plan.workflows[&managed_workflow_name("scoped-b")];

        assert_eq!(workflow.steps[0].id, "step-0-0-yt-dlp-subtitles_-to-subtitles_");
        assert_eq!(workflow.steps[1].id, "step-0-1-yt-dlp-subtitles_en-to-subtitles_en");
        assert_eq!(workflow.steps[2].id, "step-1-ffmpeg");
        let expected =
            format!("${{step_output.{}.yt_dlp_subtitle_artifacts}}", workflow.steps[1].id);
        assert_eq!(workflow.steps[2].inputs.get("input_content_0"), Some(&expected));
    }

    #[test]
    fn yt_dlp_scoped_subtitle_variant_auto_injects_write_and_langs_inputs() {
        let document = media_document(
            "scoped-sub-a",
            media_source(
                "",
                vec![],
                vec![media_step(
                    MediaStepTool::YtDlp,
                    vec![],
                    vec![(
                        "subtitles/en",
                        json!({ "kind": "subtitles", "save": true, "langs": "en" }),
                    )],
                    vec![("uri", "https://example.com/video")],
                )],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let step = &plan.workflows[&managed_workflow_name("scoped-sub-a")].steps[0];

        assert_eq!(step.id, "step-0-0-yt-dlp-subtitles_en-to-subtitles_en");
        assert_eq!(step.inputs.get("write_subs").map(String::as_str), Some("true"));
        assert_eq!(step.inputs.get("sub_langs").map(String::as_str), Some("en"));
        assert_eq!(step.inputs.get("skip_download").map(String::as_str), Some("true"));
        assert!(!step.inputs.contains_key("output"));
        assert!(!step.outputs.contains_key("primary"));
    }

    #[test]
    fn yt_dlp_subtitles_en_file_capture_sets_zip_member_suffix() {
        use crate::tools::workflows::variant_binding::resolve_media_variant_output_binding;

        let document = media_document(
            "scoped-sub-en-file",
            media_source(
                "",
                vec![],
                vec![media_step(
                    MediaStepTool::YtDlp,
                    vec![],
                    vec![(
                        "subtitles_en",
                        json!({
                            "kind": "subtitles",
                            "save": true,
                            "capture_kind": "file",
                            "langs": "en"
                        }),
                    )],
                    vec![("uri", "https://example.com/video")],
                )],
            ),
        );
        let source = document.media.get("scoped-sub-en-file").expect("source");
        let binding = resolve_media_variant_output_binding(source, "subtitles_en")
            .expect("resolve binding")
            .expect("binding");
        assert_eq!(binding.zip_member.as_deref(), Some(".en.vtt"));
    }

    #[test]
    fn yt_dlp_primary_variant_does_not_auto_inject_skip_download() {
        let document = media_document(
            "primary-a",
            media_source(
                "",
                vec![],
                vec![media_step(
                    MediaStepTool::YtDlp,
                    vec![],
                    vec![("video", yt_dlp_output_variant("primary"))],
                    vec![("uri", "https://example.com/video")],
                )],
            ),
        );
        let generated_doc = tools_document(&["yt-dlp"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let step = &plan.workflows[&managed_workflow_name("primary-a")].steps[0];

        assert!(!step.inputs.contains_key("skip_download"));
    }

    #[test]
    fn import_and_media_tagger_generated_doc_roundtrips() {
        let document = media_document(
            "tag-demo",
            media_source(
                "",
                vec![("default", ZERO_HASH)],
                vec![
                    media_step(
                        MediaStepTool::Import,
                        vec![],
                        vec![("default", yt_dlp_output_variant("primary"))],
                        vec![("kind", "cas_hash"), ("hash", ZERO_HASH)],
                    ),
                    media_step(
                        MediaStepTool::MediaTagger,
                        vec!["default"],
                        vec![("tagged", yt_dlp_output_variant("primary"))],
                        vec![],
                    ),
                ],
            ),
        );
        let generated_doc = tools_document(&["import", "media-tagger", "ffmpeg"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let mut doc = generated_doc;
        doc.workflows.extend(plan.workflows.into_values());
        for (hash, entry) in plan.external_data {
            doc.external_data.insert(hash, entry);
        }
        let bytes = mediapm_conductor::encode_document(doc).expect("encode");
        mediapm_conductor::decode_document(&bytes).expect("decode roundtrip");
    }

    #[test]
    fn local_demo_tool_chain_without_media_tagger_roundtrips() {
        let document = media_document(
            "demo.local",
            media_source(
                "",
                vec![],
                vec![
                    media_step(
                        MediaStepTool::Import,
                        vec![],
                        vec![("video_untagged", yt_dlp_output_variant("primary"))],
                        vec![("kind", "cas_hash"), ("hash", ZERO_HASH)],
                    ),
                    media_step(
                        MediaStepTool::Ffmpeg,
                        vec!["video_untagged"],
                        vec![(
                            "audio",
                            json!({ "kind": "primary", "extension": "m4a", "save": true }),
                        )],
                        vec![("vn", "true")],
                    ),
                    media_step(
                        MediaStepTool::Rsgain,
                        vec!["audio"],
                        vec![("audio", generic_output_variant("output_content"))],
                        vec![("input_extension", "m4a")],
                    ),
                ],
            ),
        );
        let generated_doc = tools_document(&["import", "ffmpeg", "rsgain", "sd"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let mut doc = generated_doc;
        doc.workflows.extend(plan.workflows.into_values());
        for (hash, entry) in plan.external_data {
            doc.external_data.insert(hash, entry);
        }
        let bytes = mediapm_conductor::encode_document(doc).expect("encode");
        mediapm_conductor::decode_document(&bytes).expect("decode roundtrip");
    }

    #[test]
    fn local_demo_tool_chain_generated_doc_roundtrips() {
        let document = media_document(
            "demo.local",
            media_source(
                "",
                vec![],
                vec![
                    media_step(
                        MediaStepTool::Import,
                        vec![],
                        vec![("video_untagged", yt_dlp_output_variant("primary"))],
                        vec![("kind", "cas_hash"), ("hash", ZERO_HASH)],
                    ),
                    media_step(
                        MediaStepTool::Ffmpeg,
                        vec!["video_untagged"],
                        vec![(
                            "audio",
                            json!({ "kind": "primary", "extension": "m4a", "save": true }),
                        )],
                        vec![("vn", "true")],
                    ),
                    media_step(
                        MediaStepTool::Rsgain,
                        vec!["audio"],
                        vec![("audio", generic_output_variant("output_content"))],
                        vec![("input_extension", "m4a")],
                    ),
                    media_step(
                        MediaStepTool::MediaTagger,
                        vec!["audio"],
                        vec![("audio", yt_dlp_output_variant("primary"))],
                        vec![],
                    ),
                ],
            ),
        );
        let generated_doc = tools_document(&["import", "ffmpeg", "rsgain", "media-tagger", "sd"]);
        let plan = build_plan(&document, &generated_doc).expect("plan");
        let mut doc = generated_doc;
        doc.workflows.extend(plan.workflows.into_values());
        for (hash, entry) in plan.external_data {
            doc.external_data.insert(hash, entry);
        }
        let bytes = mediapm_conductor::encode_document(doc).expect("encode");
        mediapm_conductor::decode_document(&bytes).expect("decode roundtrip");
    }
}
