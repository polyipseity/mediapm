//! Conductor-state-first variant hash/bytes resolution and instance matching.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{
    ConductorState, NickelDocument, ToolCallInstance, merge_step_input_bindings,
};

use crate::conductor_bridge::sync::find_active_tool_spec;
use crate::config::MediaPmDocument;
use crate::config::hierarchy_types::HierarchyEntry;
use crate::config::source_types::MediaSourceSpec;
use crate::error::MediaPmError;
use crate::tools::workflows::{
    managed_workflow_name, resolve_ffmpeg_slot_limits,
    resolve_media_variant_output_binding_with_limits,
};

use super::zip_reader::{
    extract_zip_member_bytes, parse_external_data_reference, parse_step_output_reference,
};
use super::{
    ExpectedStepInputs, InputBindingHashResolution, MaterializationLookupContext,
    RequiredStepOutputNames, RequiredStepZipMembers, StepOutputHashes, VariantSourceBytes,
};

/// Resolves the [`MediaSourceSpec`] for one hierarchy entry's media id.
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

/// Collects all available variant names for one media source.
#[must_use]
pub(super) fn collect_media_source_available_variants(
    source: &MediaSourceSpec,
) -> BTreeSet<String> {
    let mut variants = BTreeSet::new();
    for variant_name in source.variant_hashes.keys() {
        variants.insert(variant_name.clone());
    }
    for step in &source.steps {
        for variant_name in step.output_variants.keys() {
            variants.insert(variant_name.clone());
        }
    }
    variants
}

async fn cas_hash_is_readable(cas: &FileSystemCas, hash: Hash) -> bool {
    cas.get(hash).await.is_ok()
}

/// Resolves the CAS hash for one variant (local backfill first, then conductor state).
pub(super) async fn resolve_variant_hash(
    media_id: &str,
    variant_name: &str,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext,
) -> Result<Option<Hash>, MediaPmError> {
    if let Some(local_hash) = resolve_local_variant_hash(media_id, variant_name, source)?
        && cas_hash_is_readable(&lookup.cas, local_hash).await
    {
        return Ok(Some(local_hash));
    }

    if let Some(state) = lookup.conductor_state.as_ref()
        && let Some((workflow_hash, _notice)) =
            resolve_variant_hash_from_workflow_state(lookup, state, media_id, source, variant_name)
                .await?
    {
        let binding = resolve_media_variant_output_binding_with_limits(
            source,
            variant_name,
            lookup.ffmpeg_slot_limits.max_input_slots,
            lookup.ffmpeg_slot_limits.max_output_slots,
        )?;
        if binding.is_none_or(|binding| binding.zip_member.is_none())
            && cas_hash_is_readable(&lookup.cas, workflow_hash).await
        {
            return Ok(Some(workflow_hash));
        }
    }

    Ok(None)
}

fn is_zip_content(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0x50 && data[1] == 0x4B
}

/// Copies workflow output hashes into each media source's `variant_hashes` map
/// so hierarchy materialization can resolve variants when conductor instance
/// matching is unavailable.
pub(crate) async fn backfill_source_variant_hashes_from_workflow_outputs(
    document: &mut MediaPmDocument,
    generated_doc: &NickelDocument,
    conductor_state: &ConductorState,
    cas: &FileSystemCas,
) -> Result<(), MediaPmError> {
    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(document);

    for (media_id, source) in &mut document.media {
        let workflow_name = managed_workflow_name(media_id);
        let Some(workflow) =
            generated_doc.workflows.iter().find(|workflow| workflow.name == workflow_name)
        else {
            continue;
        };

        let Some(step_outputs) =
            resolve_workflow_step_output_hashes(cas, generated_doc, conductor_state, workflow)
                .await?
        else {
            continue;
        };

        for step in &source.steps {
            for variant_name in step.output_variants.keys() {
                let Some(binding) = resolve_media_variant_output_binding_with_limits(
                    source,
                    variant_name,
                    ffmpeg_slot_limits.max_input_slots,
                    ffmpeg_slot_limits.max_output_slots,
                )?
                else {
                    continue;
                };

                if let Some(hash) = step_outputs
                    .get(&binding.step_id)
                    .and_then(|outputs| outputs.get(&binding.output_name))
                    .copied()
                    && cas.get(hash).await.is_ok()
                {
                    source.variant_hashes.insert(variant_name.clone(), hash.to_string());
                }
            }
        }
    }

    Ok(())
}

/// Resolves variant bytes with optional fallback notice and source hash.
pub(super) async fn resolve_variant_source_bytes(
    lookup: &MaterializationLookupContext,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
    full_zip_archive: bool,
) -> Result<VariantSourceBytes, MediaPmError> {
    if let Some(state) = lookup.conductor_state.as_ref()
        && let Some((workflow_hash, fallback_notice)) =
            resolve_variant_hash_from_workflow_state(lookup, state, media_id, source, variant)
                .await?
        && let Ok(bytes) = lookup.cas.get(workflow_hash).await
        && (!full_zip_archive || is_zip_content(bytes.as_ref()))
    {
        let binding_result = resolve_media_variant_output_binding_with_limits(
            source,
            variant,
            lookup.ffmpeg_slot_limits.max_input_slots,
            lookup.ffmpeg_slot_limits.max_output_slots,
        );
        let materialized_result = binding_result.and_then(|binding| {
            if let Some(binding) = binding {
                if !full_zip_archive && let Some(zip_member) = binding.zip_member.as_deref() {
                    extract_zip_member_bytes(bytes.as_ref(), zip_member)
                        .map(|member_bytes| (member_bytes, None))
                        .map_err(|error| {
                            MediaPmError::Workflow(format!(
                                "extracting ZIP member '{zip_member}' for media '{media_id}' variant '{variant}' failed: {error}"
                            ))
                        })
                } else {
                    Ok((bytes.as_ref().to_vec(), Some(workflow_hash)))
                }
            } else {
                Ok((bytes.as_ref().to_vec(), Some(workflow_hash)))
            }
        });

        if let Ok((materialized_bytes, source_hash)) = materialized_result {
            return Ok(VariantSourceBytes {
                bytes: materialized_bytes,
                notice: fallback_notice,
                source_hash,
            });
        }
    }

    if source.variant_hashes.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' variant '{variant}' has no local variant hashes and no workflow output hash resolved from conductor state"
        )));
    }

    let hash_string = source
        .variant_hashes
        .get(variant)
        .or_else(|| source.variant_hashes.get("default"))
        .ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' does not define hash pointer for variant '{variant}'"
            ))
        })?
        .clone();

    let hash = hash_string.parse::<Hash>().map_err(|_| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' variant '{variant}' has invalid CAS hash '{hash_string}'"
        ))
    })?;

    match lookup.cas.get(hash).await {
        Ok(bytes) => {
            let notice = if source.variant_hashes.contains_key(variant) {
                None
            } else {
                Some(format!(
                    "variant '{variant}' missing for media '{media_id}'; used fallback variant 'default'"
                ))
            };
            Ok(VariantSourceBytes {
                bytes: bytes.as_ref().to_vec(),
                notice,
                source_hash: Some(hash),
            })
        }
        Err(source) => Err(MediaPmError::Workflow(format!(
            "CAS hash '{hash}' for media '{media_id}' variant '{variant}' is missing from CAS: {source}"
        ))),
    }
}

pub(super) fn resolve_local_variant_hash(
    media_id: &str,
    variant_name: &str,
    source: &MediaSourceSpec,
) -> Result<Option<Hash>, MediaPmError> {
    if let Some(hash_str) = source.variant_hashes.get(variant_name) {
        let hash: Hash = hash_str.parse().map_err(|e| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' variant '{variant_name}' hash '{hash_str}' is invalid: {e}"
            ))
        })?;
        return Ok(Some(hash));
    }

    if variant_name != "default"
        && let Some(hash_str) = source.variant_hashes.get("default")
    {
        let hash: Hash = hash_str.parse().map_err(|e| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' default variant hash '{hash_str}' is invalid: {e}"
            ))
        })?;
        return Ok(Some(hash));
    }

    Ok(None)
}

async fn resolve_variant_hash_from_workflow_state(
    lookup: &MaterializationLookupContext,
    state: &ConductorState,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<Option<(Hash, Option<String>)>, MediaPmError> {
    let Some(binding) = resolve_media_variant_output_binding_with_limits(
        source,
        variant,
        lookup.ffmpeg_slot_limits.max_input_slots,
        lookup.ffmpeg_slot_limits.max_output_slots,
    )?
    else {
        return Ok(None);
    };

    let workflow_name = managed_workflow_name(media_id);
    let Some(workflow) =
        lookup.generated_doc.workflows.iter().find(|workflow| workflow.name == workflow_name)
    else {
        return Ok(None);
    };

    let step_output_hashes = {
        let cached = lookup.step_output_hashes_cache.lock().unwrap().get(&workflow_name).cloned();
        if let Some(result) = cached {
            result
        } else {
            let result = resolve_workflow_step_output_hashes(
                &lookup.cas,
                &lookup.generated_doc,
                state,
                workflow,
            )
            .await?;
            lookup
                .step_output_hashes_cache
                .lock()
                .unwrap()
                .insert(workflow_name.clone(), result.clone());
            result
        }
    };

    let Some(step_output_hashes) = step_output_hashes else {
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

/// Resolves concrete output hashes for each workflow step using conductor state.
///
/// Strict input-key matching is attempted per step first; steps that miss under
/// strict matching are retried with relaxed instance selection so partial strict
/// success (for example ffmpeg) does not block yt-dlp sidecar resolution.
pub(super) async fn resolve_workflow_step_output_hashes(
    cas: &FileSystemCas,
    generated_doc: &NickelDocument,
    state: &ConductorState,
    workflow: &mediapm_conductor::WorkflowSpec,
) -> Result<Option<StepOutputHashes>, MediaPmError> {
    let mut step_outputs = StepOutputHashes::new();
    let required_step_output_names = collect_required_step_output_names(workflow);
    let required_step_zip_members = collect_required_step_zip_members(workflow);

    for step in &workflow.steps {
        if let Some(outputs) = resolve_single_step_output_hashes_strict(
            cas,
            generated_doc,
            state,
            step,
            &required_step_output_names,
            &required_step_zip_members,
            &step_outputs,
        )
        .await?
            && step_declared_outputs_resolved(step, &outputs)
        {
            step_outputs.insert(step.id.clone(), outputs);
        }
    }

    for step in &workflow.steps {
        if step_outputs
            .get(&step.id)
            .is_some_and(|outputs| step_declared_outputs_resolved(step, outputs))
        {
            continue;
        }
        if let Some(outputs) =
            resolve_single_step_output_hashes_relaxed(cas, generated_doc, state, step).await?
            && step_declared_outputs_resolved(step, &outputs)
        {
            step_outputs.insert(step.id.clone(), outputs);
        }
    }

    if step_outputs.is_empty() { Ok(None) } else { Ok(Some(step_outputs)) }
}

async fn resolve_single_step_output_hashes_strict(
    cas: &FileSystemCas,
    generated_doc: &NickelDocument,
    state: &ConductorState,
    step: &mediapm_conductor::WorkflowStepSpec,
    required_step_output_names: &RequiredStepOutputNames,
    required_step_zip_members: &RequiredStepZipMembers,
    prior_step_outputs: &StepOutputHashes,
) -> Result<Option<BTreeMap<String, Hash>>, MediaPmError> {
    let merged_step_inputs =
        merge_step_inputs_with_tool_defaults(generated_doc, &step.tool, &step.inputs)?;
    let expected_inputs =
        resolve_expected_input_hashes(cas, generated_doc, &merged_step_inputs, prior_step_outputs)
            .await?;
    let Some(expected_inputs) = expected_inputs else {
        return Ok(None);
    };

    let conductor_tool_key = find_active_tool_spec(generated_doc, &step.tool)
        .map(|(key, _)| key.clone())
        .ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "workflow step '{}' references unknown tool '{}' in generated config",
                step.id, step.tool
            ))
        })?;

    let required_output_names =
        required_step_output_names.get(&step.id).cloned().unwrap_or_default();
    let required_zip_members = required_step_zip_members.get(&step.id);

    let instance_key_resolved_values =
        build_instance_key_resolved_value_bytes(cas, &merged_step_inputs, &expected_inputs).await?;

    let mut matching_instances = state
        .tool_call_instances
        .iter()
        .filter_map(|(instance_key, instance)| {
            (instance.tool_call_id == conductor_tool_key
                && instance_matches_expected_inputs(
                    instance,
                    &expected_inputs,
                    &merged_step_inputs,
                    &instance_key_resolved_values,
                )
                && instance_matches_expected_output_names(instance, &step.outputs)
                && instance_matches_required_output_names(instance, &required_output_names))
            .then_some((instance_key, instance))
        })
        .collect::<Vec<_>>();

    matching_instances.sort_by(|(left_key, left_instance), (right_key, right_instance)| {
        compare_instance_recency(left_key, left_instance, right_key, right_instance)
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

    Ok(Some(output_hashes_from_instance(instance, &step.outputs)))
}

fn step_declared_outputs_resolved(
    step: &mediapm_conductor::WorkflowStepSpec,
    outputs: &BTreeMap<String, Hash>,
) -> bool {
    step.outputs.keys().all(|output_name| outputs.contains_key(output_name))
}

async fn resolve_single_step_output_hashes_relaxed(
    cas: &FileSystemCas,
    generated_doc: &NickelDocument,
    state: &ConductorState,
    step: &mediapm_conductor::WorkflowStepSpec,
) -> Result<Option<BTreeMap<String, Hash>>, MediaPmError> {
    let conductor_tool_key = find_active_tool_spec(generated_doc, &step.tool)
        .map(|(key, _)| key.clone())
        .ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "workflow step '{}' references unknown tool '{}' in generated config",
                step.id, step.tool
            ))
        })?;

    let required_output_names: BTreeSet<String> = step.outputs.keys().cloned().collect();
    let mut candidates = state
        .tool_call_instances
        .values()
        .filter(|instance| instance.tool_call_id == conductor_tool_key)
        .filter(|instance| {
            required_output_names
                .iter()
                .all(|output_name| instance.outputs.contains_key(output_name))
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        left.outputs
            .len()
            .cmp(&right.outputs.len())
            .then_with(|| right.executed_at.cmp(&left.executed_at))
    });

    for instance in candidates {
        let mut all_materialized = true;
        for output_name in &required_output_names {
            let Some(output) = instance.outputs.get(output_name) else {
                all_materialized = false;
                break;
            };
            if cas.get(output.hash).await.is_err() {
                all_materialized = false;
                break;
            }
        }
        if all_materialized {
            return Ok(Some(output_hashes_from_instance(instance, &step.outputs)));
        }
    }

    Ok(None)
}

fn output_hashes_from_instance(
    instance: &ToolCallInstance,
    step_outputs: &BTreeMap<String, mediapm_conductor::OutputCaptureSpec>,
) -> BTreeMap<String, Hash> {
    let mut output_hashes = instance
        .outputs
        .iter()
        .map(|(name, output)| (name.clone(), output.hash))
        .collect::<BTreeMap<_, _>>();
    if let Some(stdout) = instance.outputs.get("stdout") {
        for (output_name, spec) in step_outputs {
            if output_hashes.contains_key(output_name) {
                continue;
            }
            if matches!(spec.capture.as_str(), "stdout" | "stderr" | "process_code") {
                output_hashes.insert(output_name.clone(), stdout.hash);
            }
        }
    }
    output_hashes
}

/// Unions step-declared inputs with the active tool spec contract, matching
/// conductor `resolve_step_inputs` key collection.
fn merge_step_inputs_with_tool_defaults(
    generated_doc: &NickelDocument,
    step_tool: &str,
    step_inputs: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, MediaPmError> {
    let (_, tool_spec) = find_active_tool_spec(generated_doc, step_tool).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "workflow step references unknown tool '{step_tool}' in generated config",
        ))
    })?;
    Ok(merge_step_input_bindings(step_inputs, tool_spec))
}

async fn build_instance_key_resolved_value_bytes(
    cas: &FileSystemCas,
    merged_bindings: &BTreeMap<String, String>,
    expected_inputs: &ExpectedStepInputs,
) -> Result<BTreeMap<String, Vec<u8>>, MediaPmError> {
    use mediapm_conductor::{
        instance_key_resolved_value_bytes_for_hash_reference,
        instance_key_resolved_value_bytes_for_literal_binding,
    };

    let mut values = BTreeMap::new();
    for (key, raw_binding) in merged_bindings {
        let Some(resolved_hash) = expected_inputs.resolved_hashes.get(key) else {
            continue;
        };
        let value_bytes = if let Some(reference) = parse_step_output_reference(raw_binding) {
            if let Some(zip_member) = reference.zip_member {
                let zip_bytes = cas.get(*resolved_hash).await.map_err(|source| {
                    MediaPmError::Workflow(format!(
                        "reading ZIP output '{resolved_hash}' for instance-key member '{zip_member}' failed: {source}"
                    ))
                })?;
                extract_zip_member_bytes(zip_bytes.as_ref(), zip_member).map_err(|error| {
                    MediaPmError::Workflow(format!(
                        "extracting ZIP member '{zip_member}' for instance-key material failed: {error}"
                    ))
                })?
            } else {
                instance_key_resolved_value_bytes_for_hash_reference(*resolved_hash)
            }
        } else if parse_external_data_reference(raw_binding)?.is_some() {
            instance_key_resolved_value_bytes_for_hash_reference(*resolved_hash)
        } else {
            instance_key_resolved_value_bytes_for_literal_binding(raw_binding)
        };
        values.insert(key.clone(), value_bytes);
    }
    Ok(values)
}

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
            if let Some(reference) = parse_step_output_reference(binding) {
                required
                    .entry(reference.step_id.to_string())
                    .or_default()
                    .insert(reference.output_name.to_string());
            }
        }
    }

    required
}

fn collect_required_step_zip_members(
    workflow: &mediapm_conductor::WorkflowSpec,
) -> RequiredStepZipMembers {
    let mut required = RequiredStepZipMembers::new();

    for step in &workflow.steps {
        for value in step.inputs.values() {
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

fn instance_output_ref<'a>(
    instance: &'a ToolCallInstance,
    output_name: &str,
) -> Option<&'a mediapm_conductor::HashedValueRecord> {
    instance.outputs.get(output_name)
}

fn instance_matches_required_output_names(
    instance: &ToolCallInstance,
    required_output_names: &BTreeSet<String>,
) -> bool {
    required_output_names
        .iter()
        .all(|output_name| instance_output_ref(instance, output_name).is_some())
}

fn compare_instance_recency(
    left_key: &Hash,
    left_instance: &ToolCallInstance,
    right_key: &Hash,
    right_instance: &ToolCallInstance,
) -> Ordering {
    let left_rank = instance_recency_rank(left_key, left_instance);
    let right_rank = instance_recency_rank(right_key, right_instance);
    right_rank.cmp(&left_rank)
}

fn instance_recency_rank(instance_key: &Hash, instance: &ToolCallInstance) -> (bool, u64, String) {
    if instance.impure {
        (true, instance.executed_at.as_unix_nanos(), instance_key.to_hex())
    } else {
        (false, 0, instance_key.to_hex())
    }
}

async fn instance_has_materializable_required_outputs(
    cas: &FileSystemCas,
    instance: &ToolCallInstance,
    required_output_names: &BTreeSet<String>,
    required_zip_members: Option<&BTreeMap<String, BTreeSet<String>>>,
) -> bool {
    for output_name in required_output_names {
        let Some(output_ref) = instance_output_ref(instance, output_name) else {
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

async fn resolve_expected_input_hashes(
    cas: &FileSystemCas,
    generated_doc: &NickelDocument,
    step_inputs: &BTreeMap<String, String>,
    step_outputs: &StepOutputHashes,
) -> Result<Option<ExpectedStepInputs>, MediaPmError> {
    let mut expected = ExpectedStepInputs::default();

    for (input_name, value) in step_inputs {
        match resolve_input_binding_hash(cas, generated_doc, value, step_outputs).await? {
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

async fn resolve_input_binding_hash(
    cas: &FileSystemCas,
    generated_doc: &NickelDocument,
    value: &str,
    step_outputs: &StepOutputHashes,
) -> Result<InputBindingHashResolution, MediaPmError> {
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
            let Ok(member_bytes) = extract_zip_member_bytes(zip_bytes.as_ref(), zip_member) else {
                return Ok(InputBindingHashResolution::MissingMaterializedStepOutput);
            };
            return Ok(InputBindingHashResolution::Resolved(Hash::from_content(
                member_bytes.as_slice(),
            )));
        }

        return Ok(InputBindingHashResolution::Resolved(hash));
    }

    if let Some(external_hash) = parse_external_data_reference(value)? {
        return generated_doc
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

/// Returns true when one runtime instance contains all expected input hashes.
pub(super) fn instance_matches_expected_inputs(
    instance: &ToolCallInstance,
    expected_inputs: &ExpectedStepInputs,
    merged_step_inputs: &BTreeMap<String, String>,
    instance_key_resolved_values: &BTreeMap<String, Vec<u8>>,
) -> bool {
    if !expected_inputs.unresolved_hash_input_names.is_empty() {
        return false;
    }

    let input_hashes = mediapm_conductor::deterministic_input_hashes_from_resolved_value_bytes(
        merged_step_inputs,
        instance_key_resolved_values,
    );
    let materialized_hashes = mediapm_conductor::deterministic_materialized_input_hashes(instance);
    mediapm_conductor::instance_matches_stored_key(instance, &input_hashes, &materialized_hashes)
}

fn instance_matches_expected_output_names(
    instance: &ToolCallInstance,
    expected_outputs: &BTreeMap<String, mediapm_conductor::OutputCaptureSpec>,
) -> bool {
    expected_outputs.keys().all(|output_name| instance.outputs.contains_key(output_name))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use mediapm_cas::FileSystemCas;
    use mediapm_cas::Hash;
    use mediapm_conductor::{decode_document, decode_state_json};

    use super::*;

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
    }

    fn load_generated_doc() -> NickelDocument {
        let bytes = std::fs::read(fixture_path("demo_conductor_generated.ncl"))
            .expect("read demo generated conductor doc");
        decode_document(&bytes).expect("decode demo generated conductor doc")
    }

    fn load_conductor_state() -> ConductorState {
        let bytes = std::fs::read(fixture_path("demo_conductor_state.json"))
            .expect("read demo conductor state");
        decode_state_json(&bytes).expect("decode demo conductor state")
    }

    #[tokio::test]
    async fn demo_workflow_step_outputs_include_ffmpeg_and_apply() {
        let generated = load_generated_doc();
        let state = load_conductor_state();
        let workflow = generated
            .workflows
            .iter()
            .find(|workflow| workflow.name == "mediapm.media.demo.local.dQw4w9WgXcQ")
            .expect("demo workflow");
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        for instance in state.tool_call_instances.values() {
            for output in instance.outputs.values() {
                if cas.get(output.hash).await.is_err() {
                    let _ = cas.put(bytes::Bytes::from_static(b"fixture-bytes")).await;
                }
            }
        }

        let step_outputs = resolve_workflow_step_output_hashes(&cas, &generated, &state, workflow)
            .await
            .expect("resolve workflow step outputs")
            .expect("expected step outputs");

        assert!(
            step_outputs.contains_key("step-1-ffmpeg"),
            "expected ffmpeg step outputs, got: {:?}",
            step_outputs.keys().collect::<Vec<_>>()
        );
        assert!(
            step_outputs.contains_key("step-3-0-media-tagger-audio-to-audio-apply"),
            "expected media-tagger apply step outputs, got: {:?}",
            step_outputs.keys().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn demo_ffmpeg_instance_matches_step_inputs() {
        let generated = load_generated_doc();
        let state = load_conductor_state();
        let workflow = generated
            .workflows
            .iter()
            .find(|workflow| workflow.name == "mediapm.media.demo.local.dQw4w9WgXcQ")
            .expect("demo workflow");
        let ffmpeg_step =
            workflow.steps.iter().find(|step| step.id == "step-1-ffmpeg").expect("ffmpeg step");
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let import_step = workflow
            .steps
            .iter()
            .find(|step| step.id == "step-0-0-import-video_untagged-to-video_untagged")
            .expect("import step");
        let import_instance = state
            .tool_call_instances
            .values()
            .find(|instance| instance.tool_call_id == "import@v1")
            .expect("import instance");
        let import_stdout = import_instance.outputs.get("stdout").expect("import stdout");
        let _ = cas.put(bytes::Bytes::from_static(b"import-bytes")).await;

        let mut step_outputs = BTreeMap::new();
        step_outputs.insert(
            import_step.id.clone(),
            BTreeMap::from([(String::from("video_untagged"), import_stdout.hash)]),
        );

        let merged_ffmpeg_inputs = merge_step_inputs_with_tool_defaults(
            &generated,
            &ffmpeg_step.tool,
            &ffmpeg_step.inputs,
        )
        .expect("merge ffmpeg inputs");
        let expected_inputs =
            resolve_expected_input_hashes(&cas, &generated, &merged_ffmpeg_inputs, &step_outputs)
                .await
                .expect("resolve ffmpeg expected inputs")
                .expect("ffmpeg expected inputs");

        let conductor_tool_key = find_active_tool_spec(&generated, &ffmpeg_step.tool)
            .map(|(key, _)| key.clone())
            .expect("active ffmpeg tool");
        let instance_key_resolved_values =
            build_instance_key_resolved_value_bytes(&cas, &merged_ffmpeg_inputs, &expected_inputs)
                .await
                .expect("build ffmpeg instance-key resolved values");
        let ffmpeg_instance = state
            .tool_call_instances
            .values()
            .find(|instance| {
                instance.tool_call_id == conductor_tool_key
                    && instance_matches_expected_inputs(
                        instance,
                        &expected_inputs,
                        &merged_ffmpeg_inputs,
                        &instance_key_resolved_values,
                    )
            })
            .expect("ffmpeg instance matching step inputs");
        assert!(ffmpeg_instance.outputs.contains_key("primary"));
    }

    /// Domain separator the documented instance-key spec fixes for the v2 key
    /// scheme.
    ///
    /// Re-declared here instead of imported: the production constant is
    /// `pub(crate)` inside `mediapm-conductor`, and this oracle exists to pin the
    /// documented value, so a changed domain string must fail the oracle rather
    /// than move both sides together.
    const ORACLE_INSTANCE_KEY_DOMAIN_V2: &str = "mediapm/conductor/tool call instances/v2.1";

    /// Domain separator under which the checked-in fixture was recorded,
    /// before wall-clock time left the instance-key scheme.
    ///
    /// Pairing only — see [`previously_recorded_instance_key`].
    const LEGACY_INSTANCE_KEY_DOMAIN_V2: &str = "mediapm/conductor/tool call instances/v2";

    /// Returns whether a raw binding source is deterministic, per the documented
    /// spec: a source naming `${env.`, `context.os`, `:file(`, or `:folder(`
    /// resolves from the host rather than from the recorded inputs, so it stays
    /// out of the instance key.
    ///
    /// Deliberately a second implementation of the rule rather than a call to
    /// `is_deterministic_binding_source`: a filter that moves with production
    /// cannot detect production being wrong about which bindings are key material.
    fn oracle_is_deterministic_binding(raw_source: &str) -> bool {
        !(raw_source.contains("${env.")
            || raw_source.contains("context.os")
            || raw_source.contains(":file(")
            || raw_source.contains(":folder("))
    }

    /// Pins the oracle's restatement of the determinism rule against the spec.
    ///
    /// No binding source on the demo path carries any of the four markers, so
    /// without this an `oracle_is_deterministic_binding` that always returned `true`
    /// would still pass the fixture test — the rule would be restated but never
    /// exercised false. Each marker must make a source host-dependent, whether it
    /// stands alone or sits inside a longer source; a plain literal must not, and
    /// neither must a source that merely *looks* templated: the rule is a substring
    /// scan for those four markers, so an unrelated `${` interpolation and the bare
    /// word `folder` both stay deterministic.
    #[test]
    fn oracle_is_deterministic_binding_refuses_each_host_marker() {
        let cases = [
            ("${env.HOME}", false),
            ("--flag ${env.MY_VAR} --other", false),
            ("context.os", false),
            (r#"${context.os == "macos" ? "a" : "b"}"#, false),
            (":file(", false),
            ("${inputs.payload:file(payload.txt)}", false),
            (":folder(", false),
            ("${inputs.folder:folder(dir)}", false),
            ("mp4", true),
            ("", true),
            ("${step_output.step-0-0-import.video_untagged}", true),
            ("my-folder-name", true),
        ];
        for (source, expected) in cases {
            assert_eq!(
                oracle_is_deterministic_binding(source),
                expected,
                "source {source:?} must be classified as deterministic={expected}",
            );
        }
    }

    /// Recomposes one step's deterministic binding hashes from the spec: each
    /// deterministic binding contributes `composite(name bytes, resolved value
    /// bytes)`, walked in `BTreeMap` order (sorted by name).
    ///
    /// This is the composition the oracle exists to check, so it is written out
    /// here instead of calling `instance_key_hash_from_resolved_value_bytes` or
    /// `deterministic_input_hashes_from_resolved_value_bytes`. Contrast
    /// `arg_hashes` in `mediapm-conductor`'s `step_worker/cache.rs`, which
    /// deliberately *does* call the production composition because its job is to
    /// guard that helper. Different purpose, opposite rule: do not "de-duplicate"
    /// this against production, because a test that calls the code under test
    /// cannot detect that code being wrong.
    fn oracle_binding_hashes(
        merged_bindings: &BTreeMap<String, String>,
        resolved_values: &BTreeMap<String, Vec<u8>>,
    ) -> Vec<Hash> {
        let mut hashes = Vec::new();
        for (name, raw_source) in merged_bindings {
            let Some(value_bytes) = resolved_values.get(name) else {
                continue;
            };
            if !oracle_is_deterministic_binding(raw_source) {
                continue;
            }
            hashes.push(Hash::composite(&[
                Hash::from_content(name.as_bytes()),
                Hash::from_content(value_bytes),
            ]));
        }
        hashes
    }

    /// Recomputes one recorded instance's key from the spec.
    ///
    /// Components, in order: the versioned domain separator, the unified
    /// tools-catalog key, the deterministic binding hashes, and the
    /// instance's materialized-input hashes in `BTreeMap` order. No time
    /// component exists: the recorded `executed_at` is provenance, never key
    /// material, so a stored impure instance keeps matching across runs. The
    /// deterministic environment term is empty on the mediapm path, which
    /// passes no env hashes at all.
    ///
    /// Written out here rather than delegating to `derive_instance_key_v2` (or its
    /// `derive_tool_call_instance_key` re-export), for the reason given on
    /// [`Self::oracle_binding_hashes`].
    fn oracle_instance_key(instance: &ToolCallInstance, binding_hashes: &[Hash]) -> Hash {
        let mut parts = Vec::new();
        parts.push(Hash::from_content(ORACLE_INSTANCE_KEY_DOMAIN_V2.as_bytes()));
        parts.push(Hash::from_content(instance.tool_call_id.as_bytes()));
        parts.extend_from_slice(binding_hashes);
        parts.extend(instance.materialized_inputs.values().map(|record| record.hash));
        Hash::composite(&parts)
    }

    /// Independently derives every demo workflow step's instance key and pairs it
    /// with the recorded instance that key owns.
    ///
    /// *Resolution* comes from the production helpers — which hash a binding
    /// points at, and which bytes a binding resolves to — because resolution is
    /// not what this oracle checks. *Composition* comes from
    /// [`Self::oracle_binding_hashes`] and [`Self::oracle_instance_key`], which are
    /// this oracle's own implementation of the documented spec. The forbidden
    /// production calls are `instance_key_hash_from_resolved_value_bytes`,
    /// `deterministic_input_hashes_from_resolved_inputs`,
    /// `deterministic_input_hashes_from_resolved_value_bytes`, and
    /// `derive_instance_key_v2`; see [`Self::oracle_binding_hashes`] for why.
    ///
    /// Steps are walked in the workflow's declared (dependency) order, so each
    /// step's hash-reference bindings see the outputs recorded for the steps
    /// before it. Returns `(step_id, tool_call_id, key)` per step, or a diagnostic
    /// naming the step, its tool, and every recorded candidate key.
    ///
    /// Two checks do the enforcing, and only two. A step that derives no recorded
    /// key — what a stale fixture key produces — returns that diagnostic instead of
    /// re-deriving a key the fixture does not hold, and the closing count check
    /// rejects a run in which a recorded instance pairs with no step. The candidate
    /// slice is additionally required to hold exactly one entry, but that branch is
    /// defensive rather than reachable against this fixture: candidacy requires
    /// `derived == *stored_key` against the unique keys of a `BTreeMap`, so at most
    /// one entry can ever match and only the zero case can fire.
    async fn oracle_derive_demo_instance_keys(
        cas: &FileSystemCas,
        generated: &NickelDocument,
        state: &ConductorState,
        workflow: &mediapm_conductor::WorkflowSpec,
    ) -> Result<Vec<(String, String, Hash)>, String> {
        let mut step_outputs = StepOutputHashes::new();
        let mut claimed: BTreeSet<Hash> = BTreeSet::new();
        let mut paired = Vec::new();

        for step in &workflow.steps {
            let tool_call_id = find_active_tool_spec(generated, &step.tool)
                .map(|(key, _)| key.clone())
                .ok_or_else(|| {
                    format!(
                        "step '{}' references unknown tool '{}' in the generated document",
                        step.id, step.tool,
                    )
                })?;
            let merged = merge_step_inputs_with_tool_defaults(generated, &step.tool, &step.inputs)
                .map_err(|error| {
                    format!("step '{}': merging step inputs failed: {error}", step.id)
                })?;
            let Some(expected_inputs) =
                resolve_expected_input_hashes(cas, generated, &merged, &step_outputs)
                    .await
                    .map_err(|error| {
                        format!(
                            "step '{}': resolving expected input hashes failed: {error}",
                            step.id,
                        )
                    })?
            else {
                return Err(format!(
                    "step '{}': no recorded predecessor supplies its hash-backed inputs",
                    step.id,
                ));
            };
            let resolved_values =
                build_instance_key_resolved_value_bytes(cas, &merged, &expected_inputs)
                    .await
                    .map_err(|error| {
                        format!(
                            "step '{}': resolving instance-key value bytes failed: {error}",
                            step.id,
                        )
                    })?;
            let binding_hashes = oracle_binding_hashes(&merged, &resolved_values);

            let mut candidates = Vec::new();
            let mut diagnostics = Vec::new();
            for (stored_key, instance) in &state.tool_call_instances {
                if instance.tool_call_id != tool_call_id || claimed.contains(stored_key) {
                    continue;
                }
                let derived = oracle_instance_key(instance, &binding_hashes);
                diagnostics.push(format!("{} -> {}", stored_key.to_hex(), derived.to_hex()));
                if derived == *stored_key {
                    candidates.push((stored_key, instance));
                }
            }
            if candidates.len() != 1 {
                return Err(format!(
                    "step '{}' (tool '{tool_call_id}') pairs with {} recorded instance(s), \
                     expected exactly one; recorded -> independently derived: {diagnostics:?}",
                    step.id,
                    candidates.len(),
                ));
            }
            let (stored_key, instance) = candidates[0];
            if instance.instance_key != *stored_key {
                return Err(format!(
                    "step '{}' (tool '{tool_call_id}') is recorded under key {} but the instance \
                     body carries instance_key {}",
                    step.id,
                    stored_key.to_hex(),
                    instance.instance_key.to_hex(),
                ));
            }
            claimed.insert(*stored_key);
            paired.push((step.id.clone(), tool_call_id, *stored_key));
            step_outputs
                .insert(step.id.clone(), output_hashes_from_instance(instance, &step.outputs));
        }

        if claimed.len() != state.tool_call_instances.len() {
            let unpaired = state
                .tool_call_instances
                .keys()
                .filter(|key| !claimed.contains(key))
                .map(Hash::to_hex)
                .collect::<Vec<_>>();
            return Err(format!(
                "{} recorded instance(s) pair with no workflow step: {unpaired:?}",
                unpaired.len(),
            ));
        }

        Ok(paired)
    }

    /// The fixture's recorded instance keys are checked against an independent
    /// oracle: every demo workflow step's key is recomputed from the fixture's own
    /// inputs by the documented spec, with the oracle's own composition of the
    /// binding hashes and the instance-key components.
    ///
    /// This is the second implementation the fixture needs.
    /// `demo_workflow_step_outputs_include_ffmpeg_and_apply` and
    /// `demo_ffmpeg_instance_matches_step_inputs` derive keys through the
    /// production path and compare them with the fixture, so they catch a *future*
    /// derivation change but cannot catch a *wrong* derivation: both sides move
    /// together. Task 1 changed the instance-key material and re-derived this
    /// fixture's five keys with that same production path, which cost the fixture
    /// its independence for exactly those keys; this test buys it back.
    ///
    /// The oracle must not call the production key helpers, and it must stay that
    /// way; see [`Self::oracle_binding_hashes`]. It does use production
    /// *resolution* helpers — `find_active_tool_spec`,
    /// `merge_step_inputs_with_tool_defaults`, `resolve_expected_input_hashes`,
    /// `build_instance_key_resolved_value_bytes`, and
    /// `output_hashes_from_instance` — because resolution is not in question here.
    ///
    /// A deliberate change to the instance-key spec — the domain constant, the
    /// component order, or the name term of a binding hash — has to update this
    /// oracle by hand in the same commit that updates the production helper. That
    /// friction is the point: it turns a silent spec change, whose fixture
    /// regeneration the production path would have made self-consistent, into a
    /// reviewable one.
    ///
    /// A live demo run remains the only way to re-record the fixture's *inputs*:
    /// the commands, inputs, outputs, and timestamps a real sync captured. Task 1
    /// changed only the key each input derives, never the inputs themselves, which
    /// is why an oracle over the recorded inputs is enough to validate the keys.
    #[tokio::test]
    async fn demo_fixture_instance_keys_match_independent_spec_oracle() {
        let generated = load_generated_doc();
        let state = load_conductor_state();
        let workflow = generated
            .workflows
            .iter()
            .find(|workflow| workflow.name == "mediapm.media.demo.local.dQw4w9WgXcQ")
            .expect("demo workflow");
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let paired = oracle_derive_demo_instance_keys(&cas, &generated, &state, workflow)
            .await
            .expect("every recorded key must be the key the spec derives");

        assert_eq!(
            paired.len(),
            workflow.steps.len(),
            "every demo workflow step must pair with a recorded instance: {paired:?}",
        );
        assert!(
            paired.iter().any(|(_, tool_call_id, _)| tool_call_id == "import@v1"),
            "the oracle must cover the import@v1 instance: {paired:?}",
        );
        for (step_id, tool_call_id, key) in &paired {
            let instance = state.tool_call_instances.get(key).unwrap_or_else(|| {
                panic!(
                    "step '{step_id}' ({tool_call_id}) derives key {} that the fixture does not \
                     record",
                    key.to_hex(),
                )
            });
            assert_eq!(
                instance.tool_call_id,
                *tool_call_id,
                "step '{step_id}' derives key {} but the fixture records tool '{}' under it",
                key.to_hex(),
                instance.tool_call_id,
            );
        }
    }

    /// Proves the oracle is sensitive to a stale key rather than merely restating
    /// what the fixture says: re-keying one recorded instance in a copy of the
    /// loaded fixture makes the oracle refuse to pair that instance's step, naming
    /// the step, the tool, and the recorded candidate key.
    ///
    /// The perturbation is in memory, so the checked-in fixture keeps its exact
    /// bytes.
    #[tokio::test]
    async fn demo_fixture_instance_key_oracle_rejects_a_stale_key() {
        let generated = load_generated_doc();
        let mut state = load_conductor_state();
        let workflow = generated
            .workflows
            .iter()
            .find(|workflow| workflow.name == "mediapm.media.demo.local.dQw4w9WgXcQ")
            .expect("demo workflow");
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let import_key = state
            .tool_call_instances
            .iter()
            .find(|(_, instance)| instance.tool_call_id == "import@v1")
            .map(|(key, _)| *key)
            .expect("recorded import instance");
        let import_instance =
            state.tool_call_instances.remove(&import_key).expect("recorded import instance");
        let stale_key = Hash::from_content(b"task-6-oracle-stale-key-perturbation");
        assert_ne!(stale_key, import_key, "the perturbation must move the key");
        state.tool_call_instances.insert(stale_key, import_instance);

        let error = oracle_derive_demo_instance_keys(&cas, &generated, &state, workflow)
            .await
            .expect_err("a re-keyed instance must fail the oracle");
        assert!(
            error.contains("step-0-0-import-video_untagged-to-video_untagged"),
            "the diagnostic must name the step: {error}",
        );
        assert!(error.contains("import@v1"), "the diagnostic must name the tool: {error}");
    }

    /// Argument hashes as the derivation produced them *before* the input name
    /// became key material: the resolved value bytes alone, with
    /// non-deterministic bindings dropped.
    ///
    /// Not production code. It exists so [`regenerate_demo_conductor_state_fixture_keys`]
    /// can prove that a recorded `instance_key` belongs to a particular step's
    /// input material by reproducing the old
    /// `instance_key_hash_from_resolved_value_bytes(raw_source, value)` rule
    /// (filter on the binding source, hash the value) in the same merge order
    /// production uses.
    fn previous_derivation_arg_hashes(
        merged_bindings: &BTreeMap<String, String>,
        resolved_values: &BTreeMap<String, Vec<u8>>,
    ) -> Vec<Hash> {
        merged_bindings
            .iter()
            .filter(|(_, raw_source)| {
                mediapm_conductor::is_deterministic_binding_source(raw_source)
            })
            .filter_map(|(key, _)| resolved_values.get(key))
            .map(|value_bytes| Hash::from_content(value_bytes))
            .collect()
    }

    /// The key scheme the checked-in fixture was recorded under: the `/v2`
    /// domain separator plus the `executed_at` time term for impure
    /// instances, over the caller's argument hashes.
    ///
    /// Pairing only, never production: [`plan_demo_instance_key_rewrites`]
    /// accepts a recorded key that matches under this historical scheme, or
    /// under an older input-hash rule, or under the current scheme (a
    /// re-run against an already-regenerated fixture), and always rewrites
    /// it to the current [`derived_instance_key`] result. Keeping the
    /// historical composition here is what lets an instance-key derivation
    /// change regenerate the fixture instead of orphaning it.
    fn previously_recorded_instance_key(instance: &ToolCallInstance, arg_hashes: &[Hash]) -> Hash {
        let mut parts = Vec::new();
        parts.push(Hash::from_content(LEGACY_INSTANCE_KEY_DOMAIN_V2.as_bytes()));
        parts.push(Hash::from_content(instance.tool_call_id.as_bytes()));
        if instance.impure {
            parts.push(Hash::from_content(&instance.executed_at.as_unix_nanos().to_le_bytes()));
        }
        parts.extend_from_slice(arg_hashes);
        parts.extend(instance.materialized_inputs.values().map(|record| record.hash));
        Hash::composite(&parts)
    }

    /// Derives the instance key `instance` is recorded under for a given list of
    /// deterministic input hashes.
    ///
    /// Mirrors the executor's `derive_instance_key_v2` call exactly: every
    /// key derives from content alone (no time term, impure or pure), and the
    /// deterministic materialized-input hashes come from the instance's own
    /// content map. The caller supplies only the input-hash list, which is the
    /// sole term a change to the instance-key material moves.
    fn derived_instance_key(
        instance: &mediapm_conductor::ToolCallInstance,
        arg_hashes: &[Hash],
    ) -> Hash {
        mediapm_conductor::derive_tool_call_instance_key(
            &instance.tool_call_id,
            arg_hashes,
            &[],
            &mediapm_conductor::deterministic_materialized_input_hashes(instance),
        )
    }

    /// Pairs every recorded instance with the step whose input material derives
    /// its key, returning `stored_key -> (current_key, step_id)`.
    ///
    /// The pairing is established by the *previous* derivation (see
    /// [`previous_derivation_arg_hashes`]), so it rests on the fixture's own
    /// record rather than on this helper's assumptions. Steps are walked in
    /// dependency order and a paired instance's outputs become that step's
    /// outputs, which is how the production resolver reconstructs them; the
    /// recorded run's step outputs are then available to the hash-reference
    /// bindings of later steps. A step that pairs with no instance, or with more
    /// than one, aborts the run: the fixture is then ambiguous, not merely stale.
    async fn plan_demo_instance_key_rewrites(
        cas: &FileSystemCas,
        generated: &NickelDocument,
        state: &ConductorState,
        workflow: &mediapm_conductor::WorkflowSpec,
    ) -> BTreeMap<Hash, (Hash, String)> {
        let mut step_outputs: BTreeMap<String, BTreeMap<String, Hash>> = BTreeMap::new();
        let mut rewrites: BTreeMap<Hash, (Hash, String)> = BTreeMap::new();
        for step in &workflow.steps {
            let conductor_tool_key = find_active_tool_spec(generated, &step.tool)
                .map(|(key, _)| key.clone())
                .expect("active tool spec");
            let merged = merge_step_inputs_with_tool_defaults(generated, &step.tool, &step.inputs)
                .expect("merge step inputs");
            let Some(expected_inputs) =
                resolve_expected_input_hashes(cas, generated, &merged, &step_outputs)
                    .await
                    .expect("resolve expected inputs")
            else {
                panic!(
                    "step '{}' resolves no inputs; its recorded predecessors are missing",
                    step.id,
                );
            };
            let resolved_values =
                build_instance_key_resolved_value_bytes(cas, &merged, &expected_inputs)
                    .await
                    .expect("build resolved value bytes");
            let previous_hashes = previous_derivation_arg_hashes(&merged, &resolved_values);
            let current_hashes =
                mediapm_conductor::deterministic_input_hashes_from_resolved_value_bytes(
                    &merged,
                    &resolved_values,
                );

            let mut paired: Option<(&Hash, &ToolCallInstance)> = None;
            for (stored_key, instance) in &state.tool_call_instances {
                if instance.tool_call_id != conductor_tool_key {
                    continue;
                }
                let recorded_key = previously_recorded_instance_key(instance, &current_hashes);
                let previous_key = derived_instance_key(instance, &previous_hashes);
                let current_key = derived_instance_key(instance, &current_hashes);
                // `current_key == stored_key` also covers a re-run against an
                // already-regenerated fixture, which must be a no-op.
                if recorded_key != *stored_key
                    && previous_key != *stored_key
                    && current_key != *stored_key
                {
                    continue;
                }
                assert!(
                    paired.is_none(),
                    "step '{}' pairs with more than one recorded instance",
                    step.id,
                );
                paired = Some((stored_key, instance));
                if let Some((known_key, known_step)) = rewrites.get(stored_key) {
                    assert_eq!(
                        *known_key,
                        current_key,
                        "steps '{known_step}' and '{}' disagree about instance {}'s key",
                        step.id,
                        stored_key.to_hex(),
                    );
                } else {
                    let paired_under = if recorded_key == *stored_key {
                        " (paired under the recorded scheme)"
                    } else if previous_key == *stored_key {
                        " (paired)"
                    } else {
                        " (already current)"
                    };
                    println!(
                        "step {}\n  instance {}\n  previous {}{}\n  current  {}",
                        step.id,
                        instance.tool_call_id,
                        stored_key.to_hex(),
                        paired_under,
                        current_key.to_hex(),
                    );
                    rewrites.insert(*stored_key, (current_key, step.id.clone()));
                }
            }

            let Some((_, instance)) = paired else {
                panic!(
                    "no recorded instance pairs with step '{}' under either derivation",
                    step.id,
                );
            };
            step_outputs
                .insert(step.id.clone(), output_hashes_from_instance(instance, &step.outputs));
        }
        rewrites
    }

    /// Rewrites the recorded `instance_key`s in `tests/fixtures/demo_conductor_state.json`
    /// so the fixture stays a valid record of the conductor run it captured.
    ///
    /// The fixture is a checked-in record of a real run: each
    /// `tool_call_instances` key was derived from that run's input material, so
    /// any change to the instance-key derivation makes every recorded key
    /// unreachable and the demo materialization tests stop matching instances.
    /// Run this, from `src/mediapm`, whenever the instance-key material changes:
    ///
    /// ```text
    /// cargo bin cargo-nextest run -p mediapm --run-ignored ignored-only \
    ///   -E 'test(regenerate_demo_conductor_state_fixture_keys)' --no-capture
    /// ```
    ///
    /// The rewrite is keys-only. The fixture's JSON text has every occurrence of
    /// an old key string replaced by the new one — the `tool_call_instances` map
    /// key, that entry's `instance_key` field, and the matching `aux.instances`
    /// key — so every other byte of the file is preserved and the JSON stays
    /// valid (both are `blake3:<64 hex>`).
    ///
    /// **Pairing proof.** An instance is rewritten only when its stored key
    /// equals what [`previous_derivation_arg_hashes`] derives from the inputs its
    /// step resolves to, so the fixture's own record establishes which instance
    /// belongs to which step rather than this test assuming it. A key that
    /// matches neither the previous nor the current derivation aborts the run
    /// with the instance named, and nothing is written. Because the proof is
    /// anchored to the immediately previous rule, a second derivation change
    /// requires repointing [`previous_derivation_arg_hashes`] at the then-current
    /// rule before this test can run again.
    ///
    /// **Resolution path.** The new keys are derived from the materializer's live
    /// resolution (`merge_step_inputs_with_tool_defaults`,
    /// `resolve_expected_input_hashes`, `build_instance_key_resolved_value_bytes`)
    /// rather than the conductor's `resolve_step_inputs`, because that function is
    /// `pub(super)` inside `mediapm-conductor`'s `step_worker` module and is
    /// therefore unreachable from this crate. The two paths agree on the resolved
    /// value bytes — the fixture's previous keys matched them byte for byte under
    /// the old rule — so the derivation input is the same; the pairing proof above
    /// is what keeps the fixture independent of this test's own path.
    /// **Write.** The regenerated text goes to a sibling temporary file in the
    /// fixture's own directory (`demo_conductor_state.json.regenerated`) and is
    /// then renamed over the fixture, so a partial write can never be observed in
    /// a checked-in record; the sibling path keeps the rename on one filesystem,
    /// hence atomic. A failed rename leaves that temporary file behind — this is a
    /// manual maintenance tool, so the leftover is reported to the operator rather
    /// than hidden: re-running the test truncates and reuses it.
    #[tokio::test]
    #[ignore = "fixture maintenance: rewrites demo_conductor_state.json; run manually after an instance-key derivation change"]
    async fn regenerate_demo_conductor_state_fixture_keys() {
        let path = fixture_path("demo_conductor_state.json");
        let generated = load_generated_doc();
        let state = load_conductor_state();
        let workflow = generated
            .workflows
            .iter()
            .find(|workflow| workflow.name == "mediapm.media.demo.local.dQw4w9WgXcQ")
            .expect("demo workflow");

        // The fixture is stale by definition when this runs, so the production
        // step-output resolver — which matches instances by the *current*
        // derivation — finds nothing at all, and the pairing below reconstructs
        // the recorded run's step outputs instead.
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let rewrites = plan_demo_instance_key_rewrites(&cas, &generated, &state, workflow).await;

        // Prove every instance pairs before touching the file.
        for (stored_key, instance) in &state.tool_call_instances {
            assert!(
                rewrites.contains_key(stored_key),
                "instance {} ({}) pairs with no step's input material under either derivation; \
                 refusing to rewrite a key this fixture cannot prove the instance owns",
                stored_key.to_hex(),
                instance.tool_call_id,
            );
        }

        let mut text = std::fs::read_to_string(&path).expect("read demo conductor state fixture");
        let mut changed = Vec::new();
        for (stored_key, (current_key, step_id)) in &rewrites {
            if current_key == stored_key {
                println!("step {step_id}: {} already current", stored_key.to_hex());
                continue;
            }
            let needle = stored_key.to_string();
            let occurrences = text.matches(needle.as_str()).count();
            assert!(occurrences > 0, "instance key {needle} does not appear in the fixture text");
            println!("step {step_id}: {needle} -> {current_key} ({occurrences} occurrences)");
            text = text.replace(needle.as_str(), &current_key.to_string());
            changed.push((*stored_key, *current_key));
        }

        if changed.is_empty() {
            println!("fixture is already current; nothing written");
            return;
        }
        // Same-directory temporary file plus rename: the fixture is a checked-in
        // record, so a partial write must never be observable, and a sibling path
        // keeps the rename on one filesystem (hence atomic).
        let temporary_path = path.with_extension("json.regenerated");
        std::fs::write(&temporary_path, &text).expect("write regenerated demo conductor state");
        std::fs::rename(&temporary_path, &path)
            .expect("move regenerated demo conductor state over the fixture");
        println!("rewrote {} instance key(s)", changed.len());

        // The rewritten file must decode, carry no stale key, and hold exactly the
        // keys the current derivation produces.
        for (stored_key, current_key) in &changed {
            assert!(
                !text.contains(stored_key.to_string().as_str()),
                "stale key {} survived the rewrite",
                stored_key.to_hex(),
            );
            assert!(text.contains(current_key.to_string().as_str()));
        }
        let rewritten = load_conductor_state();
        assert_eq!(rewritten.tool_call_instances.len(), state.tool_call_instances.len());
        for (current_key, step_id) in rewrites.values() {
            assert!(
                rewritten.tool_call_instances.contains_key(current_key),
                "rewritten fixture is missing the key step '{step_id}' derives",
            );
        }
    }

    #[tokio::test]
    async fn merged_resolution_prefers_specialized_yt_dlp_sidecar_instance() {
        use mediapm_conductor::{
            HashedValueRecord, OutputCaptureSpec, SaveMode, ToolCallInstance, ToolKindSpec,
            ToolSpec, WorkflowSpec, WorkflowStepSpec,
        };
        use mediapm_utils::Timestamp;

        let generated = NickelDocument {
            tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                ToolSpec {
                    version: None,
                    name: "yt-dlp".to_string(),
                    kind: ToolKindSpec::default(),
                    ..ToolSpec::default()
                },
            )]),
            ..NickelDocument::default()
        };

        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let primary_infojson_hash =
            cas.put(bytes::Bytes::from_static(b"primary-infojson")).await.expect("put primary");
        let sidecar_infojson_hash =
            cas.put(bytes::Bytes::from_static(b"sidecar-infojson")).await.expect("put sidecar");

        let primary_key = Hash::from_content(b"primary-instance");
        let sidecar_key = Hash::from_content(b"sidecar-instance");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            primary_key,
            ToolCallInstance {
                instance_key: primary_key,
                tool_call_id: "yt-dlp".to_string(),
                impure: true,
                executed_at: Timestamp::from_unix_nanos(2),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([
                    (
                        "primary".to_string(),
                        HashedValueRecord { hash: primary_infojson_hash, deterministic: false },
                    ),
                    (
                        "yt_dlp_infojson_file".to_string(),
                        HashedValueRecord { hash: primary_infojson_hash, deterministic: false },
                    ),
                ]),
            },
        );
        state.tool_call_instances.insert(
            sidecar_key,
            ToolCallInstance {
                instance_key: sidecar_key,
                tool_call_id: "yt-dlp".to_string(),
                impure: true,
                executed_at: Timestamp::from_unix_nanos(1),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    HashedValueRecord { hash: sidecar_infojson_hash, deterministic: false },
                )]),
            },
        );

        let workflow = WorkflowSpec {
            name: "mediapm.media.test".to_string(),
            display_name: None,
            description: None,
            impure: true,
            steps: vec![WorkflowStepSpec {
                id: "step-0-0-yt-dlp-infojson-to-infojson".to_string(),
                tool: "yt-dlp".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    OutputCaptureSpec {
                        name: "yt_dlp_infojson_file".to_string(),
                        capture: "file:info.json".to_string(),
                        save: SaveMode::True,
                        allow_empty: false,
                        include_topmost_folder: true,
                    },
                )]),
                max_retries: 1,
                depends_on: Vec::new(),
            }],
        };

        let step_outputs = resolve_workflow_step_output_hashes(&cas, &generated, &state, &workflow)
            .await
            .expect("resolve workflow outputs")
            .expect("expected workflow step outputs");

        let outputs = step_outputs
            .get("step-0-0-yt-dlp-infojson-to-infojson")
            .expect("infojson step outputs");
        assert_eq!(
            outputs.get("yt_dlp_infojson_file"),
            Some(&sidecar_infojson_hash),
            "relaxed resolution should prefer the specialized yt-dlp sidecar instance"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "hermetic backfill scenario keeps seed + asserts in one readable test"
    )]
    async fn backfill_populates_infojson_from_resolved_yt_dlp_step_outputs() {
        use std::collections::BTreeMap;

        use mediapm_conductor::{
            HashedValueRecord, OutputCaptureSpec, SaveMode, ToolCallInstance, ToolKindSpec,
            ToolSpec, WorkflowSpec, WorkflowStepSpec,
        };
        use mediapm_utils::Timestamp;

        use crate::config::{
            MediaPmDocument, MediaSourceSpec, MediaStep, MediaStepTool, OutputVariantValue,
            TransformInputValue, YtDlpOutputKind, YtDlpOutputVariantConfig,
        };
        use crate::tools::workflows::{
            managed_workflow_name, resolve_ffmpeg_slot_limits,
            resolve_media_variant_output_binding_with_limits,
        };

        let generated = NickelDocument {
            tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                ToolSpec {
                    version: None,
                    name: "yt-dlp".to_string(),
                    kind: ToolKindSpec::default(),
                    ..ToolSpec::default()
                },
            )]),
            ..NickelDocument::default()
        };

        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let infojson_hash = cas
            .put(bytes::Bytes::from_static(br#"{"title":"Never Gonna Give You Up"}"#))
            .await
            .expect("put infojson");

        let workflow = WorkflowSpec {
            name: managed_workflow_name("youtube.dQw4w9WgXcQ"),
            description: None,
            display_name: None,
            impure: true,
            steps: vec![WorkflowStepSpec {
                id: "step-0-0-yt-dlp-infojson-to-infojson".to_string(),
                tool: "yt-dlp".to_string(),
                inputs: BTreeMap::from([
                    ("uri".to_string(), "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string()),
                    ("skip_download".to_string(), "true".to_string()),
                    ("write_info_json".to_string(), "true".to_string()),
                    ("write_description".to_string(), "false".to_string()),
                ]),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    OutputCaptureSpec {
                        name: "yt_dlp_infojson_file".to_string(),
                        capture: "file:info.json".to_string(),
                        save: SaveMode::True,
                        allow_empty: false,
                        include_topmost_folder: true,
                    },
                )]),
                max_retries: 0,
                depends_on: Vec::new(),
            }],
        };

        let mut state = ConductorState::new_empty();
        let instance_key = Hash::from_content(b"infojson-instance");
        state.tool_call_instances.insert(
            instance_key,
            ToolCallInstance {
                instance_key,
                tool_call_id: "yt-dlp".to_string(),
                impure: true,
                executed_at: Timestamp::from_unix_nanos(1),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    HashedValueRecord { hash: infojson_hash, deterministic: false },
                )]),
            },
        );

        let mut document = MediaPmDocument::default();
        document.media.insert(
            "youtube.dQw4w9WgXcQ".to_string(),
            MediaSourceSpec {
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([
                        (
                            "video".to_string(),
                            OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                                kind: YtDlpOutputKind::Primary,
                                ..Default::default()
                            }),
                        ),
                        (
                            "infojson".to_string(),
                            OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                                kind: YtDlpOutputKind::Infojson,
                                ..Default::default()
                            }),
                        ),
                    ]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String(
                            "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                        ),
                    )]),
                }],
                ..MediaSourceSpec::default()
            },
        );

        let mut generated_doc = generated.clone();
        generated_doc.workflows.push(workflow);

        let step_outputs = resolve_workflow_step_output_hashes(
            &cas,
            &generated_doc,
            &state,
            &generated_doc.workflows[0],
        )
        .await
        .expect("resolve step outputs")
        .expect("expected outputs");

        let ffmpeg_limits = resolve_ffmpeg_slot_limits(&document);
        let binding = resolve_media_variant_output_binding_with_limits(
            document.media.get("youtube.dQw4w9WgXcQ").expect("source"),
            "infojson",
            ffmpeg_limits.max_input_slots,
            ffmpeg_limits.max_output_slots,
        )
        .expect("binding")
        .expect("infojson binding");

        assert!(
            step_outputs.get(&binding.step_id).and_then(|o| o.get(&binding.output_name)).is_some(),
            "step outputs missing infojson: binding={binding:?} keys={:?}",
            step_outputs.keys().collect::<Vec<_>>()
        );

        backfill_source_variant_hashes_from_workflow_outputs(
            &mut document,
            &generated_doc,
            &state,
            &cas,
        )
        .await
        .expect("backfill");

        let source = document.media.get("youtube.dQw4w9WgXcQ").expect("source");
        assert_eq!(
            source.variant_hashes.get("infojson").map(String::as_str),
            Some(infojson_hash.to_string().as_str()),
            "backfill should insert infojson hash; got keys={:?}",
            source.variant_hashes.keys().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn relaxed_resolution_finds_infojson_on_primary_yt_dlp_download_instance() {
        use mediapm_conductor::{
            HashedValueRecord, OutputCaptureSpec, SaveMode, ToolCallInstance, ToolKindSpec,
            ToolSpec, WorkflowSpec, WorkflowStepSpec,
        };
        use mediapm_utils::Timestamp;

        let generated = NickelDocument {
            tools: BTreeMap::from([(
                "yt-dlp".to_string(),
                ToolSpec {
                    version: None,
                    name: "yt-dlp".to_string(),
                    kind: ToolKindSpec::default(),
                    ..ToolSpec::default()
                },
            )]),
            ..NickelDocument::default()
        };

        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let primary_video_hash =
            cas.put(bytes::Bytes::from_static(b"primary-video-bytes")).await.expect("put primary");
        let infojson_hash = cas
            .put(bytes::Bytes::from_static(br#"{"title":"Never Gonna Give You Up"}"#))
            .await
            .expect("put infojson");

        let primary_key = Hash::from_content(b"primary-download-instance");
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            primary_key,
            ToolCallInstance {
                instance_key: primary_key,
                tool_call_id: "yt-dlp".to_string(),
                impure: true,
                executed_at: Timestamp::from_unix_nanos(2),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([
                    (
                        "primary".to_string(),
                        HashedValueRecord { hash: primary_video_hash, deterministic: false },
                    ),
                    (
                        "yt_dlp_infojson_file".to_string(),
                        HashedValueRecord { hash: infojson_hash, deterministic: false },
                    ),
                    (
                        "yt_dlp_description_file".to_string(),
                        HashedValueRecord { hash: infojson_hash, deterministic: false },
                    ),
                ]),
            },
        );

        let workflow = WorkflowSpec {
            name: "mediapm.media.youtube.dQw4w9WgXcQ".to_string(),
            display_name: None,
            description: None,
            impure: true,
            steps: vec![WorkflowStepSpec {
                id: "step-0-2-yt-dlp-infojson-to-infojson".to_string(),
                tool: "yt-dlp".to_string(),
                inputs: BTreeMap::from([
                    ("uri".to_string(), "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string()),
                    ("skip_download".to_string(), "true".to_string()),
                    ("write_info_json".to_string(), "true".to_string()),
                    ("write_description".to_string(), "false".to_string()),
                ]),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    OutputCaptureSpec {
                        name: "yt_dlp_infojson_file".to_string(),
                        capture: "file:info.json".to_string(),
                        save: SaveMode::True,
                        allow_empty: false,
                        include_topmost_folder: true,
                    },
                )]),
                max_retries: 1,
                depends_on: Vec::new(),
            }],
        };

        let step_outputs = resolve_workflow_step_output_hashes(&cas, &generated, &state, &workflow)
            .await
            .expect("resolve workflow outputs")
            .expect("expected workflow step outputs");

        let outputs = step_outputs
            .get("step-0-2-yt-dlp-infojson-to-infojson")
            .expect("infojson step outputs");
        assert_eq!(
            outputs.get("yt_dlp_infojson_file"),
            Some(&infojson_hash),
            "relaxed resolution should reuse primary download instance infojson output"
        );
    }

    #[tokio::test]
    async fn relaxed_resolution_matches_versioned_yt_dlp_tool_call_id() {
        use mediapm_conductor::{
            HashedValueRecord, OutputCaptureSpec, SaveMode, ToolCallInstance, ToolKindSpec,
            ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
        };
        use mediapm_utils::Timestamp;

        let versioned_tool_id = "yt-dlp@blake3:fixture-version-hash".to_string();
        let generated = NickelDocument {
            tools: BTreeMap::from([(
                versioned_tool_id.clone(),
                ToolSpec {
                    version: None,
                    name: "yt-dlp".to_string(),
                    kind: ToolKindSpec::default(),
                    runtime: ToolRuntime {
                        content_map: BTreeMap::from([(
                            "yt-dlp".to_string(),
                            Hash::from_content(b"yt-dlp-binary").to_string(),
                        )]),
                        ..ToolRuntime::default()
                    },
                    ..ToolSpec::default()
                },
            )]),
            ..NickelDocument::default()
        };

        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let infojson_hash = cas
            .put(bytes::Bytes::from_static(br#"{"title":"Never Gonna Give You Up"}"#))
            .await
            .expect("put infojson");

        let instance_key = Hash::from_content(b"versioned-yt-dlp-instance");
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            instance_key,
            ToolCallInstance {
                instance_key,
                tool_call_id: versioned_tool_id.clone(),
                impure: true,
                executed_at: Timestamp::from_unix_nanos(1),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    HashedValueRecord { hash: infojson_hash, deterministic: false },
                )]),
            },
        );

        let workflow = WorkflowSpec {
            name: "mediapm.media.youtube.dQw4w9WgXcQ".to_string(),
            display_name: None,
            description: None,
            impure: true,
            steps: vec![WorkflowStepSpec {
                id: "step-0-2-yt-dlp-infojson-to-infojson".to_string(),
                tool: "yt-dlp".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    OutputCaptureSpec {
                        name: "yt_dlp_infojson_file".to_string(),
                        capture: "file:info.json".to_string(),
                        save: SaveMode::True,
                        allow_empty: false,
                        include_topmost_folder: true,
                    },
                )]),
                max_retries: 1,
                depends_on: Vec::new(),
            }],
        };

        let step_outputs = resolve_workflow_step_output_hashes(&cas, &generated, &state, &workflow)
            .await
            .expect("resolve workflow outputs")
            .expect("expected workflow step outputs");

        let outputs = step_outputs
            .get("step-0-2-yt-dlp-infojson-to-infojson")
            .expect("infojson step outputs");
        assert_eq!(
            outputs.get("yt_dlp_infojson_file"),
            Some(&infojson_hash),
            "versioned yt-dlp tool_call_id should still resolve infojson output"
        );
    }
}
