//! Orchestration state loading, variant source resolution, and instance matching.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::model::config::ImpureTimestamp;
use mediapm_conductor::{
    InputBinding, MachineNickelDocument, OrchestrationState, ToolCallInstance, ToolKindSpec,
    ToolSpec, decode_state, decode_state_document,
};

use crate::conductor_bridge::{
    managed_workflow_id_for_media, resolve_media_variant_output_binding_with_limits,
};
use crate::config::{HierarchyEntry, MediaPmDocument, MediaSourceSpec, media_source_uri};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;

use super::zip::{
    extract_zip_member_bytes, parse_external_data_reference, parse_step_output_reference,
};
use super::{
    ExpectedStepInputs, InputBindingHashResolution, MaterializationLookupContext,
    RequiredStepOutputNames, RequiredStepZipMembers, StepOutputHashes, VariantSourceBytes,
};

/// Loads persisted orchestration state referenced by runtime state pointer.
///
/// Returns `None` when the volatile runtime state document is absent, empty, or
/// does not carry a state pointer yet. A missing pointed CAS object is also
/// treated as unavailable state so sync can continue in mixed-backend flows
/// (for example in-memory conductor bootstrapping with filesystem materializer).
pub(super) async fn load_runtime_orchestration_state(
    paths: &MediaPmPaths,
    cas: &FileSystemCas,
) -> Result<Option<OrchestrationState>, MediaPmError> {
    if !paths.conductor_state_config.exists() {
        return Ok(None);
    }

    let state_bytes =
        fs::read(&paths.conductor_state_config).map_err(|source| MediaPmError::Io {
            operation: "reading conductor runtime state document for materialization".to_string(),
            path: paths.conductor_state_config.clone(),
            source,
        })?;

    if state_bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }

    let state_document = decode_state_document(&state_bytes).map_err(|error| {
        MediaPmError::Workflow(format!(
            "decoding conductor runtime state document '{}' failed: {error}",
            paths.conductor_state_config.display()
        ))
    })?;
    let Some(state_pointer) = state_document.state_pointer else {
        return Ok(None);
    };

    let Ok(state_blob) = cas.get(state_pointer).await else {
        return Ok(None);
    };

    let orchestration_state = decode_state(&state_blob).map_err(|error| {
        MediaPmError::Serialization(format!(
            "decoding persisted orchestration-state blob '{state_pointer}' failed: {error}"
        ))
    })?;

    Ok(Some(orchestration_state))
}

/// Resolves one hierarchy source reference.
pub(super) fn resolve_hierarchy_source<'a>(
    document: &'a MediaPmDocument,
    entry: &HierarchyEntry,
) -> Result<&'a MediaSourceSpec, MediaPmError> {
    document.media.get(&entry.media_id).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "hierarchy references unknown media id '{}'",
            entry.media_id
        ))
    })
}

/// Collects available variant names for one media source.
///
/// The selector resolver needs the union of local hash variants and declared
/// step output variants so hierarchy selector expressions can expand into
/// concrete variant names before materialization.
#[must_use]
pub(super) fn collect_media_source_available_variants(
    source: &MediaSourceSpec,
) -> BTreeSet<String> {
    let mut available = source.variant_hashes.keys().cloned().collect::<BTreeSet<_>>();
    for step in &source.steps {
        for variant in step.output_variants.keys() {
            available.insert(variant.clone());
        }
    }

    available
}

/// Resolves one source variant into concrete bytes for staging.
/// Resolves the direct CAS hash for one variant without fetching its payload
/// bytes.
///
/// Returns `Some(hash)` when the hash can be determined from workflow state or
/// local variant-hash metadata without performing a CAS object read. Returns
/// `None` when the variant requires ZIP-member extraction (the hash depends on
/// extracted content and cannot be determined without reading the archive) or
/// when no hash source is available.
///
/// This is used as a lightweight pre-check in `sync_hierarchy`: when the
/// returned hash matches an existing lock record and the final output path is
/// present on disk the re-materialization can be skipped entirely, avoiding
/// large CAS object reads for unchanged entries.
pub(super) async fn resolve_variant_source_hash(
    lookup: &MaterializationLookupContext,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<Option<Hash>, MediaPmError> {
    // Workflow state path: resolve step output hash without fetching bytes.
    if let Some(state) = lookup.orchestration_state.as_deref() {
        if let Some((workflow_hash, _notice)) =
            resolve_variant_hash_from_workflow_state(lookup, state, media_id, source, variant)
                .await?
        {
            // Only usable as a skip hint when the variant is not a ZIP member.
            let binding = resolve_media_variant_output_binding_with_limits(
                source,
                variant,
                lookup.ffmpeg_max_input_slots,
                lookup.ffmpeg_max_output_slots,
            )?;
            if binding.is_none_or(|b| b.zip_member.is_none()) {
                return Ok(Some(workflow_hash));
            }
        }
        return Ok(None);
    }

    // Local variant-hashes path: hash is available directly without a CAS
    // object read.
    if !source.variant_hashes.is_empty() {
        let hash_str =
            source.variant_hashes.get(variant).or_else(|| source.variant_hashes.get("default"));
        if let Some(hs) = hash_str
            && let Ok(hash) = hs.parse::<Hash>()
        {
            return Ok(Some(hash));
        }
    }

    Ok(None)
}

pub(super) async fn resolve_variant_source_bytes(
    lookup: &MaterializationLookupContext,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<VariantSourceBytes, MediaPmError> {
    let source_uri = media_source_uri(media_id, source);

    if let Some(state) = lookup.orchestration_state.as_deref()
        && let Some((workflow_hash, fallback_notice)) =
            resolve_variant_hash_from_workflow_state(lookup, state, media_id, source, variant)
                .await?
    {
        let bytes = lookup.cas.get(workflow_hash).await.map_err(|source| {
            MediaPmError::Workflow(format!(
                "workflow output hash '{workflow_hash}' for '{source_uri}' variant '{variant}' is missing from CAS: {source}"
            ))
        })?;

        let (materialized_bytes, source_hash) = if let Some(binding) =
            resolve_media_variant_output_binding_with_limits(
                source,
                variant,
                lookup.ffmpeg_max_input_slots,
                lookup.ffmpeg_max_output_slots,
            )? {
            if let Some(zip_member) = binding.zip_member.as_deref() {
                (
                    extract_zip_member_bytes(bytes.as_ref(), zip_member).map_err(|error| {
                        MediaPmError::Workflow(format!(
                            "extracting ZIP member '{zip_member}' for '{source_uri}' variant '{variant}' failed: {error}"
                        ))
                    })?,
                    None,
                )
            } else {
                (bytes.as_ref().to_vec(), Some(workflow_hash))
            }
        } else {
            (bytes.as_ref().to_vec(), Some(workflow_hash))
        };

        return Ok(VariantSourceBytes {
            bytes: materialized_bytes,
            notice: fallback_notice,
            source_hash,
        });
    }

    if source.variant_hashes.is_empty() {
        Err(MediaPmError::Workflow(format!(
            "source '{source_uri}' variant '{variant}' has no local variant hashes and no workflow output hash resolved from runtime state"
        )))
    } else {
        let hash_string = source
            .variant_hashes
            .get(variant)
            .or_else(|| source.variant_hashes.get("default"))
            .ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "local source '{source_uri}' does not define hash pointer for variant '{variant}'"
                ))
            })?
            .clone();

        let hash = hash_string.parse::<Hash>().map_err(|_| {
            MediaPmError::Workflow(format!(
                "local source '{source_uri}' variant '{variant}' has invalid CAS hash '{hash_string}': expected multihash string"
            ))
        })?;

        match lookup.cas.get(hash).await {
            Ok(bytes) => {
                if source.variant_hashes.contains_key(variant) {
                    Ok(VariantSourceBytes {
                        bytes: bytes.as_ref().to_vec(),
                        notice: None,
                        source_hash: Some(hash),
                    })
                } else {
                    Ok(VariantSourceBytes {
                        bytes: bytes.as_ref().to_vec(),
                        notice: Some(format!(
                            "variant '{variant}' missing for '{source_uri}'; used fallback variant 'default'"
                        )),
                        source_hash: Some(hash),
                    })
                }
            }
            Err(source) => Err(MediaPmError::Workflow(format!(
                "CAS hash '{hash}' for '{source_uri}' variant '{variant}' is missing from CAS: {source}"
            ))),
        }
    }
}

/// Resolves one materialization variant hash from workflow runtime outputs.
///
/// Returns `None` when the target variant is not produced by a workflow step,
/// when no managed workflow exists for the source, or when matching runtime
/// step instances are unavailable in current orchestration state.
async fn resolve_variant_hash_from_workflow_state(
    lookup: &MaterializationLookupContext,
    state: &OrchestrationState,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<Option<(Hash, Option<String>)>, MediaPmError> {
    let Some(binding) = resolve_media_variant_output_binding_with_limits(
        source,
        variant,
        lookup.ffmpeg_max_input_slots,
        lookup.ffmpeg_max_output_slots,
    )?
    else {
        return Ok(None);
    };

    let workflow_id = managed_workflow_id_for_media(media_id, source);
    let Some(workflow) = lookup.machine.workflows.get(&workflow_id) else {
        return Ok(None);
    };

    let Some(step_output_hashes) = resolve_workflow_step_output_hashes(
        lookup.cas.as_ref(),
        lookup.machine.as_ref(),
        state,
        workflow,
    )
    .await?
    else {
        return Ok(None);
    };

    let output_hash = step_output_hashes
        .get(&binding.step_id)
        .and_then(|outputs| outputs.get(&binding.output_name))
        .copied();

    let Some(hash) = output_hash else {
        return Ok(None);
    };

    let fallback_notice = if binding.used_default_variant {
        Some(format!(
            "variant '{variant}' missing for media '{media_id}'; used workflow fallback variant 'default'"
        ))
    } else {
        None
    };

    Ok(Some((hash, fallback_notice)))
}

/// Resolves concrete output hashes for each workflow step using orchestration state.
///
/// Steps are matched by immutable tool id, canonical tool metadata, and exact
/// resolved input hash identities. `None` is returned when a required step
/// instance cannot be resolved from the current persisted orchestration state.
pub(super) async fn resolve_workflow_step_output_hashes(
    cas: &FileSystemCas,
    machine: &MachineNickelDocument,
    state: &OrchestrationState,
    workflow: &mediapm_conductor::WorkflowSpec,
) -> Result<Option<StepOutputHashes>, MediaPmError> {
    let mut step_outputs = StepOutputHashes::new();
    let required_step_output_names = collect_required_step_output_names(workflow);
    let required_step_zip_members = collect_required_step_zip_members(workflow);

    for step in &workflow.steps {
        let expected_inputs =
            resolve_expected_input_hashes(cas, machine, &step.inputs, &step_outputs).await?;
        let Some(expected_inputs) = expected_inputs else {
            return Ok(None);
        };

        let expected_metadata = machine.tools.get(&step.tool).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "workflow step '{}' references unknown tool '{}' in machine config",
                step.id, step.tool
            ))
        })?;

        let required_output_names =
            required_step_output_names.get(&step.id).cloned().unwrap_or_default();
        let required_zip_members = required_step_zip_members.get(&step.id);

        let mut matching_instances = state
            .instances
            .iter()
            .filter_map(|(instance_id, instance)| {
                (instance.tool_name == step.tool
                    && tool_metadata_matches(expected_metadata, &instance.metadata)
                    && instance_matches_expected_inputs(instance, &expected_inputs)
                    && instance_matches_expected_output_names(instance, &step.outputs)
                    && instance_matches_required_output_names(instance, &required_output_names))
                .then_some((instance_id, instance))
            })
            .collect::<Vec<_>>();

        matching_instances.sort_by(|(left_id, left_instance), (right_id, right_instance)| {
            compare_instance_recency(
                left_id.as_str(),
                left_instance.impure_timestamp,
                right_id.as_str(),
                right_instance.impure_timestamp,
            )
        });

        let mut selected_instance = None;
        for (_, instance) in &matching_instances {
            if instance_has_materializable_required_outputs(
                cas,
                instance,
                &required_output_names,
                required_zip_members,
            )
            .await
            {
                selected_instance = Some(*instance);
                break;
            }
        }

        let Some(instance) =
            selected_instance.or_else(|| matching_instances.first().map(|(_, instance)| *instance))
        else {
            return Ok(None);
        };

        let output_hashes = instance
            .outputs
            .iter()
            .map(|(name, output)| (name.clone(), output.hash))
            .collect::<BTreeMap<_, _>>();
        step_outputs.insert(step.id.clone(), output_hashes);
    }

    Ok(Some(step_outputs))
}

/// Collects output names that must resolve for each workflow step.
///
/// Required names include:
/// - explicit step output policies (`step.outputs`), and
/// - downstream `${step_output.<step>.<output>...}` references.
fn collect_required_step_output_names(
    workflow: &mediapm_conductor::WorkflowSpec,
) -> RequiredStepOutputNames {
    let mut required = RequiredStepOutputNames::new();

    for step in &workflow.steps {
        for output_name in step.outputs.keys() {
            required.entry(step.id.clone()).or_default().insert(output_name.clone());
        }
    }

    for step in &workflow.steps {
        for binding in step.inputs.values() {
            let InputBinding::String(value) = binding else {
                continue;
            };

            if let Some(reference) = parse_step_output_reference(value) {
                required
                    .entry(reference.step_id.to_string())
                    .or_default()
                    .insert(reference.output_name.to_string());
            }
        }
    }

    required
}

/// Collects ZIP members that must be readable for each step-output reference.
fn collect_required_step_zip_members(
    workflow: &mediapm_conductor::WorkflowSpec,
) -> RequiredStepZipMembers {
    let mut required = RequiredStepZipMembers::new();

    for step in &workflow.steps {
        for binding in step.inputs.values() {
            let InputBinding::String(value) = binding else {
                continue;
            };

            let Some(reference) = parse_step_output_reference(value) else {
                continue;
            };

            let Some(zip_member) = reference.zip_member else {
                continue;
            };

            required
                .entry(reference.step_id.to_string())
                .or_default()
                .entry(reference.output_name.to_string())
                .or_default()
                .insert(zip_member.to_string());
        }
    }

    required
}

/// Returns true when one runtime instance exposes all required output names.
fn instance_matches_required_output_names(
    instance: &ToolCallInstance,
    required_output_names: &BTreeSet<String>,
) -> bool {
    required_output_names.iter().all(|output_name| instance.outputs.contains_key(output_name))
}

/// Compares two matching instances by recency for deterministic selection.
///
/// Higher recency wins:
/// 1. presence of impure timestamp,
/// 2. larger `epoch_seconds`,
/// 3. larger `subsec_nanos`,
/// 4. lexicographically larger instance id (stable tie-breaker).
fn compare_instance_recency(
    left_id: &str,
    left_timestamp: Option<ImpureTimestamp>,
    right_id: &str,
    right_timestamp: Option<ImpureTimestamp>,
) -> Ordering {
    let left_rank = instance_recency_rank(left_id, left_timestamp);
    let right_rank = instance_recency_rank(right_id, right_timestamp);
    right_rank.cmp(&left_rank)
}

/// Builds one sortable recency tuple for runtime instance selection.
fn instance_recency_rank(
    instance_id: &str,
    timestamp: Option<ImpureTimestamp>,
) -> (bool, u64, u32, &str) {
    match timestamp {
        Some(timestamp) => (true, timestamp.epoch_seconds, timestamp.subsec_nanos, instance_id),
        None => (false, 0, 0, instance_id),
    }
}

/// Returns whether required outputs are readable from CAS for one candidate.
///
/// This filters out stale instances whose required output hashes no longer
/// exist in CAS and verifies required ZIP-selector members when present.
async fn instance_has_materializable_required_outputs(
    cas: &FileSystemCas,
    instance: &ToolCallInstance,
    required_output_names: &BTreeSet<String>,
    required_zip_members: Option<&BTreeMap<String, BTreeSet<String>>>,
) -> bool {
    for output_name in required_output_names {
        let Some(output_ref) = instance.outputs.get(output_name) else {
            return false;
        };

        let Ok(output_bytes) = cas.get(output_ref.hash).await else {
            return false;
        };

        let Some(members) = required_zip_members.and_then(|by_output| by_output.get(output_name))
        else {
            continue;
        };

        for member in members {
            if extract_zip_member_bytes(output_bytes.as_ref(), member).is_err() {
                return false;
            }
        }
    }

    true
}

/// Returns true when two tool metadata specs represent the same runtime tool.
///
/// Builtin metadata persisted in orchestration state intentionally remains in a
/// strict minimal wire shape (`kind`/`name`/`version`) while decoded machine
/// config builtins may carry runtime defaults for `is_impure` and outputs.
/// Materializer instance matching must therefore compare builtin identity by
/// name/version only, while executable tools keep full-struct equality.
fn tool_metadata_matches(expected: &ToolSpec, actual: &ToolSpec) -> bool {
    match (&expected.kind, &actual.kind) {
        (
            ToolKindSpec::Builtin { name: expected_name, version: expected_version },
            ToolKindSpec::Builtin { name: actual_name, version: actual_version },
        ) => expected_name == actual_name && expected_version == actual_version,
        (ToolKindSpec::Executable { .. }, ToolKindSpec::Executable { .. }) => expected == actual,
        _ => false,
    }
}

/// Resolves expected input hashes for one workflow step from concrete bindings.
///
/// Returns `None` when the step depends on unresolved prior step output hashes.
async fn resolve_expected_input_hashes(
    cas: &FileSystemCas,
    machine: &MachineNickelDocument,
    step_inputs: &BTreeMap<String, InputBinding>,
    step_outputs: &StepOutputHashes,
) -> Result<Option<ExpectedStepInputs>, MediaPmError> {
    let mut expected = ExpectedStepInputs::default();

    for (input_name, binding) in step_inputs {
        match resolve_input_binding_hash(cas, machine, binding, step_outputs).await? {
            InputBindingHashResolution::Resolved(hash) => {
                expected.resolved_hashes.insert(input_name.clone(), hash);
            }
            InputBindingHashResolution::MissingPriorStepOutput => {
                return Ok(None);
            }
            InputBindingHashResolution::MissingMaterializedStepOutput => {
                expected.unresolved_hash_input_names.insert(input_name.clone());
            }
        }
    }

    Ok(Some(expected))
}

/// Resolves one workflow input binding into deterministic input hash identity.
///
/// This mirrors conductor runtime semantics:
/// - scalar values hash raw UTF-8 bytes,
/// - list values hash canonical JSON encoding,
/// - `${external_data.<hash>}` resolves directly to the declared CAS hash,
/// - `${step_output.<step_id>.<output_name>}` resolves from prior step outputs.
/// - `${step_output.<step_id>.<output_name>:zip(<member>)}` resolves ZIP
///   member bytes from that output before hashing.
async fn resolve_input_binding_hash(
    cas: &FileSystemCas,
    machine: &MachineNickelDocument,
    binding: &InputBinding,
    step_outputs: &StepOutputHashes,
) -> Result<InputBindingHashResolution, MediaPmError> {
    match binding {
        InputBinding::String(value) => {
            if let Some(reference) = parse_step_output_reference(value) {
                let Some(hash) = step_outputs
                    .get(reference.step_id)
                    .and_then(|outputs| outputs.get(reference.output_name))
                    .copied()
                else {
                    return Ok(InputBindingHashResolution::MissingPriorStepOutput);
                };

                if let Some(zip_member) = reference.zip_member {
                    let Ok(zip_bytes) = cas.get(hash).await else {
                        return Ok(InputBindingHashResolution::MissingMaterializedStepOutput);
                    };
                    let Ok(member_bytes) = extract_zip_member_bytes(zip_bytes.as_ref(), zip_member)
                    else {
                        return Ok(InputBindingHashResolution::MissingMaterializedStepOutput);
                    };
                    return Ok(InputBindingHashResolution::Resolved(Hash::from_content(
                        member_bytes.as_slice(),
                    )));
                }

                return Ok(InputBindingHashResolution::Resolved(hash));
            }

            if let Some(external_hash) = parse_external_data_reference(value)? {
                return machine
                    .external_data
                    .contains_key(&external_hash)
                    .then_some(InputBindingHashResolution::Resolved(external_hash))
                    .ok_or_else(|| {
                        MediaPmError::Workflow(format!(
                            "workflow binding references unknown external_data hash '{external_hash}'"
                        ))
                    });
            }

            Ok(InputBindingHashResolution::Resolved(Hash::from_content(value.as_bytes())))
        }
        InputBinding::StringList(values) => {
            let encoded = serde_json::to_vec(values).map_err(|error| {
                MediaPmError::Serialization(format!(
                    "encoding workflow string-list binding for deterministic hash resolution failed: {error}"
                ))
            })?;
            Ok(InputBindingHashResolution::Resolved(Hash::from_content(encoded.as_slice())))
        }
    }
}

/// Returns true when one runtime instance contains all expected input hashes.
///
/// Runtime may inject additional resolved inputs from tool-level defaults.
/// Materialization matching therefore treats step-declared bindings as a
/// required subset instead of requiring exact key-set equality.
pub(super) fn instance_matches_expected_inputs(
    instance: &ToolCallInstance,
    expected_inputs: &ExpectedStepInputs,
) -> bool {
    expected_inputs.resolved_hashes.iter().all(|(name, hash)| {
        instance.inputs.get(name).is_some_and(|resolved| resolved.hash == *hash)
    }) && expected_inputs
        .unresolved_hash_input_names
        .iter()
        .all(|name| instance.inputs.contains_key(name))
}

/// Returns true when one runtime instance exposes all step-declared outputs.
///
/// Multiple equivalent tool calls can share tool identity and resolved input
/// hashes while persisting different output families (for example `yt-dlp`
/// primary content vs sidecar-only captures). Materialization must constrain
/// matching to instances that provide the workflow step's expected output keys.
fn instance_matches_expected_output_names(
    instance: &ToolCallInstance,
    expected_outputs: &BTreeMap<String, mediapm_conductor::OutputPolicy>,
) -> bool {
    expected_outputs.keys().all(|output_name| instance.outputs.contains_key(output_name))
}
