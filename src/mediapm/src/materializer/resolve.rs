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
///
/// Two outcomes are deliberately not errors. A conductor-state lookup that yields
/// nothing, or bytes that cannot be read or are not the ZIP archive the caller
/// asked for, falls through to the source's own variant hashes. That fallback is
/// why the enclosing block falls through instead of returning early.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when a ZIP member the binding names cannot
/// be extracted. The failure is reported rather than answered with the pack:
/// returning the archive as the variant's content would be a wrong success, and a
/// caller cannot tell that apart from the right answer.
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

        let (materialized_bytes, source_hash) = materialized_result?;
        return Ok(VariantSourceBytes {
            bytes: materialized_bytes,
            notice: fallback_notice,
            source_hash,
        });
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

    let (conductor_tool_key, _) = find_active_tool_spec(generated_doc, &step.tool)
        .map(|(key, spec)| (key.clone(), spec))
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

    Ok(Some(output_hashes_from_instance(instance)))
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
            return Ok(Some(output_hashes_from_instance(instance)));
        }
    }

    Ok(None)
}

/// Returns the hashes an instance recorded, keyed by the names it recorded them
/// under — exactly that, with no name inferred from another.
///
/// A declared stream capture (`stdout`, `stderr`, `process_code`) used to be
/// aliased to the recorded `stdout` hash when the instance had not recorded the
/// declared name. That predates name-addressed captures: it guessed that a
/// declaration naming a stream had to hold the stream's bytes, which is only
/// true of the implicit captures and not of a name that reads `stderr`. Under
/// the current record every name a step binds is recorded under its own name
/// and spec, and both resolution paths above require the bound names to be
/// present before selecting an instance, so the alias had no live case. An
/// inferred hash is worse than an absent one here: it materializes bytes the
/// step never asked for under a name that claims they are something else.
fn output_hashes_from_instance(instance: &ToolCallInstance) -> BTreeMap<String, Hash> {
    instance.outputs.iter().map(|(name, output)| (name.clone(), output.hash)).collect()
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

/// Returns the record this instance holds under `output_name`, if any.
///
/// The name is the whole address: the record carries the capture spec beside
/// the hash, but resolution here only needs the bytes the materializer will
/// read.
fn instance_output_ref<'a>(
    instance: &'a ToolCallInstance,
    output_name: &str,
) -> Option<&'a mediapm_conductor::CapturedOutput> {
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

/// Returns true when one runtime instance was recorded from the step's inputs
/// and materialized tool content.
///
/// Instance identity is input-derived only: the matcher recomposes the same
/// four terms the step worker composed — domain, unified tools-catalog key,
/// deterministic binding hashes and materialized-input hashes. A step's capture
/// declarations are not part of it, because two steps running the same work
/// share one instance however differently they name what they record. Finding
/// the names a step binds inside that instance is a separate question, answered
/// by name in [`instance_matches_expected_output_names`].
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

/// Returns whether `instance` records every name `expected_outputs` binds.
///
/// The grant is by name only, and deliberately so. The materializer is
/// resolving library paths for a chain that already ran, not deciding whether
/// bytes may be reused: that decision is the cache probe's, and it compares the
/// recorded capture identity ([`mediapm_conductor::OutputCaptureSpec`]).
/// Comparing identity here as well would refuse to materialize anything the
/// probe cannot prove was produced the same way — including every entry the V3
/// migration carried over with `capture: None`, which no declared spec can ever
/// match. Those media would silently stop appearing in the library, which is a
/// worse failure than binding a name whose bytes the workflow's own step
/// recorded.
///
/// The gap that leaves is worth stating: a declaration change no execution has
/// re-recorded yet is not noticed here, so a name this path binds may have been
/// produced under a capture the step no longer declares. It is a materialization
/// input rather than a reuse decision, so the run that follows still re-executes
/// the step and replaces the entry; what this path can deliver in the meantime is
/// the recorded bytes, which is what the library held before the change.
fn instance_matches_expected_output_names(
    instance: &ToolCallInstance,
    expected_outputs: &BTreeMap<String, mediapm_conductor::OutputCaptureSpec>,
) -> bool {
    expected_outputs.keys().all(|output_name| instance.outputs.contains_key(output_name))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    use mediapm_cas::FileSystemCas;
    use mediapm_cas::Hash;
    use mediapm_conductor::{decode_document, decode_state_json, encode_state_json};

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

    /// The demo workflow's import step, which the shared-instance tests
    /// duplicate.
    const IMPORT_STEP_ID: &str = "step-0-0-import-video_untagged-to-video_untagged";

    /// Id of the step the shared-instance tests add beside [`IMPORT_STEP_ID`]:
    /// one step more than the fixture's instances, which is what a second step
    /// sharing the import instance's work leaves behind.
    const DUPLICATED_IMPORT_STEP_ID: &str = "step-0-0-import-copy";

    /// Loads the demo workflow owned, so a test can add a step to it.
    fn load_demo_workflow(generated: &NickelDocument) -> mediapm_conductor::WorkflowSpec {
        generated
            .workflows
            .iter()
            .find(|workflow| workflow.name == "mediapm.media.demo.local.dQw4w9WgXcQ")
            .expect("demo workflow")
            .clone()
    }

    /// Returns the demo workflow plus a duplicate of its import step, appended
    /// after every recorded step.
    ///
    /// The copy is appended rather than inserted because step outputs resolve in
    /// declared order: the dependents the fixture records bind the original's
    /// output, and a copy placed before it would resolve nothing and change the
    /// inputs every later step derives its key from. `input_hash` replaces the
    /// copy's `hash` binding, which is the only difference between the two steps;
    /// `None` makes the copy run exactly the original's work, so both derive one
    /// instance key.
    fn demo_workflow_with_duplicated_import_step(
        workflow: &mediapm_conductor::WorkflowSpec,
        input_hash: Option<&str>,
    ) -> mediapm_conductor::WorkflowSpec {
        let mut duplicated = workflow.clone();
        let mut copy = workflow
            .steps
            .iter()
            .find(|step| step.id == IMPORT_STEP_ID)
            .expect("demo workflow carries the import step")
            .clone();
        copy.id = DUPLICATED_IMPORT_STEP_ID.to_string();
        if let Some(input_hash) = input_hash {
            copy.inputs.insert("hash".to_string(), input_hash.to_string());
        }
        duplicated.steps.push(copy);
        duplicated
    }

    /// Returns the recorded key of the fixture's import instance.
    fn import_instance_key(state: &ConductorState) -> Hash {
        *state
            .tool_call_instances
            .iter()
            .find(|(_, instance)| instance.tool_call_id == "import@v1")
            .map(|(key, _)| key)
            .expect("recorded import instance")
    }

    /// Returns the fixture state with the import instance's provenance naming a
    /// second step, which is the record a step reusing that instance leaves
    /// behind.
    fn state_with_import_instance_claimed_by_duplicate(state: &ConductorState) -> ConductorState {
        let mut claimed = state.clone();
        let key = import_instance_key(&claimed);
        claimed
            .tool_call_instances
            .get_mut(&key)
            .expect("recorded import instance")
            .step_ids
            .insert(DUPLICATED_IMPORT_STEP_ID.to_string());
        claimed
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

    /// A marked instance is still projected into the library.
    ///
    /// Invalidation asks for the next execution to produce the bytes again; it
    /// does not retract the bytes the library already holds. Refusing a marked
    /// record here would delete a file before its replacement exists, which is
    /// the destruction the marker exists to avoid — so materialization ignores
    /// the marker and the re-execution is what changes what is on disk.
    #[tokio::test]
    async fn a_marked_instance_is_still_materializable() {
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

        let before = resolve_workflow_step_output_hashes(&cas, &generated, &state, workflow)
            .await
            .expect("resolve before marking")
            .expect("expected step outputs");

        let mut marked = state.clone();
        for instance in marked.tool_call_instances.values_mut() {
            instance.invalidated_at = Some(mediapm_utils::Timestamp::now());
        }
        let after = resolve_workflow_step_output_hashes(&cas, &generated, &marked, workflow)
            .await
            .expect("resolve after marking")
            .expect("expected step outputs");

        assert_eq!(
            before, after,
            "an invalidation marker must not change what the materializer projects",
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

        let (conductor_tool_key, _) =
            find_active_tool_spec(&generated, &ffmpeg_step.tool).expect("active ffmpeg tool");
        let instance_key_resolved_values =
            build_instance_key_resolved_value_bytes(&cas, &merged_ffmpeg_inputs, &expected_inputs)
                .await
                .expect("build ffmpeg instance-key resolved values");
        let ffmpeg_instance = state
            .tool_call_instances
            .values()
            .find(|instance| {
                instance.tool_call_id == *conductor_tool_key
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

    /// A step under a tool that declares capture defaults matches the instance
    /// the same work recorded, whatever either side declares.
    ///
    /// Tool-level capture defaults are live: the step worker merges them under
    /// the step's own declarations before capturing, so a recorded instance
    /// carries every name a step with that work can bind. The matcher asks two
    /// questions, and this test pins the second one: the instance must carry the
    /// names the step binds, and must not be claimed by a sibling that binds a
    /// name it never recorded. Identity is the first question, and the instance
    /// here is keyed from the same inputs the expectations name, so it is input
    /// equality that decides it — no capture set on either side can make the
    /// instance unreachable.
    #[test]
    fn a_step_under_a_tool_with_default_captures_matches_its_own_instance() {
        let step_outputs = BTreeMap::from([(
            "primary".to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: "primary".to_string(),
                capture: "file_regex:^output-0(?:[.][^/\\\\]+)?$".to_string(),
                ..mediapm_conductor::OutputCaptureSpec::default()
            },
        )]);
        let sibling_outputs = BTreeMap::from([(
            "content".to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: "content".to_string(),
                capture: "folder:outputs".to_string(),
                ..mediapm_conductor::OutputCaptureSpec::default()
            },
        )]);
        let tool_call_id = "ffmpeg@blake3:fixture";
        let instance = ToolCallInstance {
            instance_key: mediapm_conductor::derive_tool_call_instance_key(tool_call_id, &[], &[]),
            tool_call_id: tool_call_id.to_string(),
            impure: false,
            invalidated_at: None,
            executed_at: mediapm_utils::Timestamp::from_unix_nanos(0),
            step_ids: BTreeSet::new(),
            command_args: Vec::new(),
            env_vars: BTreeMap::new(),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::from([(
                "primary".to_string(),
                mediapm_conductor::CapturedOutput {
                    hash: Hash::from_content(b"primary-bytes"),
                    capture: Some(step_outputs["primary"].clone()),
                },
            )]),
        };
        assert!(
            instance_matches_expected_output_names(&instance, &step_outputs),
            "the instance must carry the name this step binds",
        );
        assert!(
            !instance_matches_expected_output_names(&instance, &sibling_outputs),
            "a sibling step binding a name this instance never recorded is a separate question \
             from identity, and the name check answers it",
        );
    }

    /// A declared stream capture is never aliased to another name's bytes.
    ///
    /// `output_hashes_from_instance` used to map a declared `stderr`,
    /// `stdout` or `process_code` name the instance had not recorded to the
    /// recorded `stdout` hash. That was defensible while the only stream names
    /// were the implicit ones; with name-addressed captures it can serve bytes
    /// captured under a different name. Every name the step binds is recorded
    /// under its own name and spec by the merged capture set, and an instance
    /// missing a bound name is refused by the matcher rather than aliased, so
    /// the function must return exactly the recorded names.
    #[test]
    fn a_declared_stream_name_is_not_aliased_to_another_name() {
        let tool_call_id = "echo@v1";
        let stdout_hash = Hash::from_content(b"stdout-bytes");
        let instance = ToolCallInstance {
            instance_key: mediapm_conductor::derive_tool_call_instance_key(tool_call_id, &[], &[]),
            tool_call_id: tool_call_id.to_string(),
            impure: false,
            invalidated_at: None,
            executed_at: mediapm_utils::Timestamp::from_unix_nanos(0),
            step_ids: BTreeSet::new(),
            command_args: Vec::new(),
            env_vars: BTreeMap::new(),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::from([(
                "stdout".to_string(),
                mediapm_conductor::CapturedOutput {
                    hash: stdout_hash,
                    capture: Some(mediapm_conductor::OutputCaptureSpec {
                        name: "stdout".to_string(),
                        capture: "stdout".to_string(),
                        ..mediapm_conductor::OutputCaptureSpec::default()
                    }),
                },
            )]),
        };
        let declared_outputs = BTreeMap::from([(
            "transcript".to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: "transcript".to_string(),
                capture: "stderr".to_string(),
                ..mediapm_conductor::OutputCaptureSpec::default()
            },
        )]);
        assert_eq!(
            declared_outputs["transcript"].capture, "stderr",
            "the fixture only means something if the unrecorded name is a stream capture, \
             which is the shape the alias served",
        );

        let output_hashes = output_hashes_from_instance(&instance);

        assert_eq!(
            output_hashes,
            BTreeMap::from([("stdout".to_string(), stdout_hash)]),
            "a name the instance did not record must not be given another name's bytes",
        );
    }

    /// Domain separator the documented instance-key spec fixes for the current
    /// key scheme.
    ///
    /// The version is an integer and only moves forward, so a dotted revision
    /// (`v4.1`) must never appear here: a change to key material is a new
    /// integer, not a patch of this one.
    ///
    /// Re-declared here instead of rendered from `INSTANCE_KEY_SCHEME` or
    /// imported from `instance_key_domain`: this oracle exists to pin the
    /// documented value, so a changed domain string must fail the oracle rather
    /// than move both sides together.
    const ORACLE_INSTANCE_KEY_DOMAIN: &str = "mediapm/conductor/tool call instances/v4";

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
    /// tools-catalog key, the deterministic binding hashes, and the instance's
    /// materialized-input hashes in `BTreeMap` order. Captures do not
    /// participate: they are records addressed by name, and two steps running
    /// the same work share one instance however differently they declare what
    /// they record. No time component exists either: the recorded `executed_at`
    /// is provenance, never key material, so a stored impure instance keeps
    /// matching across runs.
    ///
    /// Written out here rather than delegating to `derive_instance_key` (or its
    /// `derive_tool_call_instance_key` façade), for the reason given on
    /// [`Self::oracle_binding_hashes`].
    fn oracle_instance_key(instance: &ToolCallInstance, binding_hashes: &[Hash]) -> Hash {
        let mut parts = Vec::new();
        parts.push(Hash::from_content(ORACLE_INSTANCE_KEY_DOMAIN.as_bytes()));
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
    /// `deterministic_input_hashes_from_resolved_value_bytes` and
    /// `derive_instance_key`; see [`Self::oracle_binding_hashes`] for why.
    ///
    /// Steps are walked in the workflow's declared (dependency) order, so each
    /// step's hash-reference bindings see the outputs recorded for the steps
    /// before it. Returns `(step_id, tool_call_id, key)` per step, or a diagnostic
    /// naming the step, its tool, and every recorded candidate key.
    ///
    /// **Accounting.** One instance may be claimed by several steps: the key is
    /// derived from inputs alone, so two steps that run the same work derive one
    /// key and the recorded provenance names both. The result therefore holds one
    /// entry per *step*, never one per instance, and the closing check asks the
    /// reverse question — does every recorded instance have at least one claiming
    /// step — rather than requiring the two counts to match. A step that derives
    /// no recorded key — what a stale fixture key produces — returns that
    /// diagnostic instead of re-deriving a key the fixture does not hold. The
    /// candidate slice is required to hold exactly one entry; candidacy requires
    /// `derived == *stored_key` against the unique keys of a `BTreeMap`, so at
    /// most one entry can ever match and only the zero case can fire.
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
            let (tool_call_id, _) = find_active_tool_spec(generated, &step.tool)
                .map(|(key, spec)| (key.clone(), spec))
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
                if instance.tool_call_id != tool_call_id {
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
            step_outputs.insert(step.id.clone(), output_hashes_from_instance(instance));
        }

        if claimed.len() != state.tool_call_instances.len() {
            let unpaired = state
                .tool_call_instances
                .keys()
                .filter(|key| !claimed.contains(key))
                .map(Hash::to_hex)
                .collect::<Vec<_>>();
            return Err(format!(
                "{} recorded instance(s) are claimed by no workflow step: {unpaired:?}",
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
    ///
    /// The closing accounting is written for instances that several steps may
    /// claim: `paired` holds one entry per step, while every *recorded instance*
    /// has to be claimed at least once. The fixture's five steps happen to claim
    /// five distinct instances, so the two counts agree here, but the rule is the
    /// one [`demo_fixture_instance_key_oracle_accepts_a_shared_instance`]
    /// exercises with six steps over five instances — a one-instance-per-step
    /// count would reject that record.
    #[tokio::test]
    async fn demo_fixture_instance_keys_match_independent_spec_oracle() {
        let generated = load_generated_doc();
        let state = load_conductor_state();
        let workflow = load_demo_workflow(&generated);
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let paired = oracle_derive_demo_instance_keys(&cas, &generated, &state, &workflow)
            .await
            .expect("every recorded key must be the key the spec derives");

        let claimed = paired.iter().map(|(_, _, key)| *key).collect::<BTreeSet<_>>();
        assert_eq!(
            claimed.len(),
            state.tool_call_instances.len(),
            "every recorded instance must be claimed by at least one step: {paired:?}",
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

    /// The oracle accepts one instance claimed by two steps: each step pairs
    /// with the same recorded instance, the instance is counted once, and both
    /// steps are counted.
    ///
    /// This is the accounting a one-instance-per-step rule cannot express, and
    /// the input is the record a second step leaving the same work behind
    /// produces: the duplicate runs the import step's exact inputs, so both
    /// derive the import instance's key. The closing `assert_ne!` is what makes
    /// this a test of sharing: a record whose steps claim distinct instances
    /// satisfies every assertion above while proving nothing about shared
    /// ownership, and the test refuses such a record.
    #[tokio::test]
    async fn demo_fixture_instance_key_oracle_accepts_a_shared_instance() {
        let generated = load_generated_doc();
        let state = state_with_import_instance_claimed_by_duplicate(&load_conductor_state());
        let workflow =
            demo_workflow_with_duplicated_import_step(&load_demo_workflow(&generated), None);
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let paired = oracle_derive_demo_instance_keys(&cas, &generated, &state, &workflow)
            .await
            .expect("a shared instance must pass the oracle");

        let claimed = paired.iter().map(|(_, _, key)| *key).collect::<BTreeSet<_>>();
        assert_eq!(
            claimed.len(),
            state.tool_call_instances.len(),
            "a shared instance is claimed once and counted once",
        );
        assert_ne!(
            paired.len(),
            state.tool_call_instances.len(),
            "this record must be shared, or the assertions above prove nothing new",
        );
        let shared_key = import_instance_key(&state);
        let claims = paired.iter().filter(|(_, _, key)| *key == shared_key).count();
        assert_eq!(claims, 2, "both import steps must pair with the shared instance: {paired:?}");
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

    /// The oracle refuses a record holding an instance no workflow step claims.
    ///
    /// `claimed` covers every instance the walk paired, and an instance left
    /// over — the stale record a workflow that lost a step leaves behind — is
    /// checked from the instance side, because no step's key derivation can name
    /// it. The diagnostic carries the keys that matched no step, which is the
    /// record the operator has to remove or explain.
    #[tokio::test]
    async fn oracle_rejects_an_instance_no_workflow_step_claims() {
        let generated = load_generated_doc();
        let mut state = load_conductor_state();
        let workflow = load_demo_workflow(&generated);
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let import_key = import_instance_key(&state);
        let mut stray = state.tool_call_instances[&import_key].clone();
        let stray_key = Hash::from_content(b"task-4-oracle-unclaimed-instance");
        assert_ne!(stray_key, import_key, "the stray record needs a key of its own");
        stray.instance_key = stray_key;
        state.tool_call_instances.insert(stray_key, stray);

        let error = oracle_derive_demo_instance_keys(&cas, &generated, &state, &workflow)
            .await
            .expect_err("an instance no workflow step claims must fail the oracle");
        assert!(
            error.contains("1 recorded instance(s) are claimed by no workflow step"),
            "the diagnostic must report the unclaimed count: {error}",
        );
        assert!(
            error.contains(&stray_key.to_hex()),
            "the diagnostic must name the unclaimed instance: {error}",
        );
    }

    /// One recorded instance's rewrite: the key each claiming step now derives,
    /// and the steps whose recorded provenance claims the instance.
    ///
    /// The claiming set is a set, not a single id, because one instance is shared
    /// by every step running the same work and the fixture's provenance names all
    /// of them; the disagreement diagnostic needs both names to say which two
    /// claims cannot both hold.
    #[derive(Debug)]
    struct DemoInstanceKeyRewrite {
        /// Key the claiming steps' inputs derive today.
        current_key: Hash,
        /// Steps whose recorded `step_ids` names this instance, in id order.
        claiming_steps: BTreeSet<String>,
    }

    /// Derives the deterministic binding hashes one workflow step's inputs
    /// produce today.
    ///
    /// Resolution is the materializer's own — merge the active tool's contract
    /// under the step's declarations, resolve every hash-backed binding against
    /// the recorded predecessors' outputs, then hash the resolved values —
    /// because the planner's subject is which step claims which instance, not how
    /// a binding resolves. The result is the key material for
    /// `plan_demo_instance_key_rewrites`, which composes it with the instance's
    /// own recorded tool identity and materialized inputs.
    ///
    /// A step whose recorded predecessors are missing cannot be planned: the
    /// fixture is then incomplete rather than stale, so the error names the step.
    async fn demo_step_input_hashes(
        cas: &FileSystemCas,
        generated: &NickelDocument,
        step: &mediapm_conductor::WorkflowStepSpec,
        step_outputs: &StepOutputHashes,
    ) -> Result<Vec<Hash>, String> {
        let merged = merge_step_inputs_with_tool_defaults(generated, &step.tool, &step.inputs)
            .map_err(|error| format!("step '{}': merging step inputs failed: {error}", step.id))?;
        let Some(expected_inputs) =
            resolve_expected_input_hashes(cas, generated, &merged, step_outputs).await.map_err(
                |error| {
                    format!("step '{}': resolving expected input hashes failed: {error}", step.id)
                },
            )?
        else {
            return Err(format!(
                "step '{}' resolves no inputs; its recorded predecessors are missing",
                step.id,
            ));
        };
        let resolved_values = build_instance_key_resolved_value_bytes(
            cas,
            &merged,
            &expected_inputs,
        )
        .await
        .map_err(|error| {
            format!("step '{}': resolving instance-key value bytes failed: {error}", step.id)
        })?;
        Ok(mediapm_conductor::deterministic_input_hashes_from_resolved_value_bytes(
            &merged,
            &resolved_values,
        ))
    }

    /// Pairs every recorded instance with the steps that claim it, returning one
    /// [`DemoInstanceKeyRewrite`] per stored key.
    ///
    /// The pairing comes from the instance's own provenance: `step_ids` names
    /// the steps the instance has served — the one that recorded it and every
    /// later one that reused it — so the step whose id the set holds owns it
    /// and no key derivation is consulted to decide the pairing. That is what
    /// lets this run survive a key-scheme change: the fixture's keys are stale
    /// by definition here, while the provenance it carries is not.
    ///
    /// Steps are walked in dependency order and a paired instance's outputs
    /// become that step's outputs, which is how the production resolver
    /// reconstructs them; the recorded run's step outputs are then available to
    /// the hash-reference bindings of later steps. A step that owns no recorded
    /// instance, or several, is an error: the fixture is then ambiguous, not
    /// merely stale. A recorded instance that no step claims is simply absent
    /// from the returned map, which the regeneration test treats as a refusal to
    /// rewrite it.
    ///
    /// Several steps may claim one instance, because the key is derived from
    /// inputs alone: two steps running identical work share it however
    /// differently they declare what they record. One rewrite per instance is
    /// the whole point of the fixture, so the claims are grouped by stored key
    /// and every claim of one instance has to derive the same key. Claims that
    /// disagree mean the fixture records provenance its recorded inputs cannot
    /// explain — the instance would have to move to two keys at once — so the
    /// run names the instance and both disagreeing steps instead of choosing
    /// one.
    ///
    /// Provenance decides *which* instance a step pairs with, and the step's
    /// active tool identity then has to agree: an instance whose recorded
    /// `tool_call_id` is some other tool's key is a stale record, so the run
    /// stops before any key is rewritten under a step that never ran that
    /// work.
    async fn plan_demo_instance_key_rewrites(
        cas: &FileSystemCas,
        generated: &NickelDocument,
        state: &ConductorState,
        workflow: &mediapm_conductor::WorkflowSpec,
    ) -> Result<BTreeMap<Hash, DemoInstanceKeyRewrite>, String> {
        let mut step_outputs: BTreeMap<String, BTreeMap<String, Hash>> = BTreeMap::new();
        let mut rewrites: BTreeMap<Hash, DemoInstanceKeyRewrite> = BTreeMap::new();
        for step in &workflow.steps {
            let Some(active_tool_key) =
                find_active_tool_spec(generated, &step.tool).map(|(key, _)| key.clone())
            else {
                return Err(format!(
                    "step '{}' references unknown tool '{}' in the generated document",
                    step.id, step.tool,
                ));
            };
            let current_hashes =
                demo_step_input_hashes(cas, generated, step, &step_outputs).await?;

            let paired = state
                .tool_call_instances
                .iter()
                .filter(|(_, instance)| instance.step_ids.contains(&step.id))
                .collect::<Vec<_>>();
            if paired.len() != 1 {
                return Err(format!(
                    "step '{}' must own exactly one recorded instance, found {}",
                    step.id,
                    paired.len(),
                ));
            }
            let (stored_key, instance) = paired[0];
            if instance.tool_call_id != active_tool_key {
                return Err(format!(
                    "step '{}' pairs with instance {} recorded for tool '{}', but the step's \
                     active tool is '{}'",
                    step.id,
                    stored_key.to_hex(),
                    instance.tool_call_id,
                    active_tool_key,
                ));
            }
            let current_key = mediapm_conductor::derive_tool_call_instance_key(
                &instance.tool_call_id,
                &current_hashes,
                &mediapm_conductor::deterministic_materialized_input_hashes(instance),
            );
            if let Some(rewrite) = rewrites.get_mut(stored_key) {
                if rewrite.current_key != current_key {
                    let known_step = rewrite.claiming_steps.first().map_or("", String::as_str);
                    return Err(format!(
                        "steps '{known_step}' and '{}' disagree about instance {}'s key: {} and \
                         {}",
                        step.id,
                        stored_key.to_hex(),
                        rewrite.current_key.to_hex(),
                        current_key.to_hex(),
                    ));
                }
                rewrite.claiming_steps.insert(step.id.clone());
            } else {
                println!(
                    "step {}\n  instance {}\n  previous {}\n  current  {}",
                    step.id,
                    instance.tool_call_id,
                    stored_key.to_hex(),
                    current_key.to_hex(),
                );
                rewrites.insert(
                    *stored_key,
                    DemoInstanceKeyRewrite {
                        current_key,
                        claiming_steps: BTreeSet::from([step.id.clone()]),
                    },
                );
            }
            step_outputs.insert(step.id.clone(), output_hashes_from_instance(instance));
        }
        Ok(rewrites)
    }

    /// Regeneration pairs instance → step through the instance's recorded
    /// provenance, so no historical key scheme is consulted.
    ///
    /// The pairing is only as trustworthy as the fixture's `step_ids` record:
    /// every claiming step the planner reports must be one the paired instance
    /// itself names. A planner that re-derived a historical key scheme instead
    /// would pair an instance with a step whose id that instance never
    /// recorded, which is what this pins.
    ///
    /// The key assertion is the load-bearing one: the fixture is current, so
    /// each rewrite must be keyed by the key the fixture records. A planner that
    /// derived the wrong key, or paired the wrong instance, moves `current_key`
    /// off the recorded key and fails here rather than rewriting the fixture with
    /// it. The provenance loop below is the weaker half — it restates what the
    /// planner's own filter guarantees — and is kept as the statement of the
    /// invariant a future pairing change must preserve.
    #[tokio::test]
    async fn fixture_regeneration_pairs_instances_by_recorded_step_id() {
        let generated = load_generated_doc();
        let state = load_conductor_state();
        let workflow = generated
            .workflows
            .iter()
            .find(|workflow| workflow.name == "mediapm.media.demo.local.dQw4w9WgXcQ")
            .expect("demo workflow");
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let rewrites = plan_demo_instance_key_rewrites(&cas, &generated, &state, workflow)
            .await
            .expect("plan demo instance key rewrites");
        assert_eq!(
            rewrites.len(),
            state.tool_call_instances.len(),
            "every recorded instance must be planned for a rewrite",
        );
        for (stored_key, rewrite) in &rewrites {
            assert_eq!(
                rewrite.current_key,
                *stored_key,
                "steps {:?} derive {} for the instance the fixture records under {}, so either \
                 the derived key or the pairing is wrong",
                rewrite.claiming_steps,
                rewrite.current_key.to_hex(),
                stored_key.to_hex(),
            );
            let instance = &state.tool_call_instances[stored_key];
            for step_id in &rewrite.claiming_steps {
                assert!(
                    instance.step_ids.contains(step_id),
                    "step {step_id} claims instance {} it did not record",
                    stored_key.to_hex(),
                );
            }
        }
    }

    /// Two claims of one instance that derive different keys are refused,
    /// naming the instance and both steps.
    ///
    /// The fixture records one key per instance, so a claim whose inputs derive
    /// another key means the provenance names a step whose work this record
    /// cannot explain. Moving the record to either key would break a real claim,
    /// so the run reports which instance and which two steps disagree instead of
    /// picking one.
    #[tokio::test]
    async fn shared_instance_claims_that_derive_different_keys_are_refused() {
        let generated = load_generated_doc();
        let state = state_with_import_instance_claimed_by_duplicate(&load_conductor_state());
        let workflow = demo_workflow_with_duplicated_import_step(
            &load_demo_workflow(&generated),
            Some("blake3:9d3cf1c1a5eb8f1a0ce42fbd6a1a8f13a52a2f9f1a3fbf0a2e6b7c8d9e0f1a2b"),
        );
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let error = plan_demo_instance_key_rewrites(&cas, &generated, &state, &workflow)
            .await
            .expect_err("one instance cannot move to two keys");
        assert!(error.contains(IMPORT_STEP_ID), "the diagnostic must name the first step: {error}");
        assert!(
            error.contains(DUPLICATED_IMPORT_STEP_ID),
            "the diagnostic must name the second step: {error}",
        );
        assert!(
            error.contains(&import_instance_key(&state).to_hex()),
            "the diagnostic must name the instance: {error}",
        );
    }

    /// One instance claimed by two steps regenerates to one rewrite naming both
    /// steps.
    ///
    /// A rewrite moves one recorded key, and the fixture records one key per
    /// instance, so two claims that agree on the key are one rewrite rather than
    /// two — the second claim adds a name to the same record instead of
    /// demanding a key of its own.
    #[tokio::test]
    async fn shared_instance_regeneration_emits_one_rewrite_for_every_claim() {
        let generated = load_generated_doc();
        let state = state_with_import_instance_claimed_by_duplicate(&load_conductor_state());
        let workflow =
            demo_workflow_with_duplicated_import_step(&load_demo_workflow(&generated), None);
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let rewrites = plan_demo_instance_key_rewrites(&cas, &generated, &state, &workflow)
            .await
            .expect("one instance claimed by two steps is one rewrite");

        assert_eq!(
            rewrites.len(),
            state.tool_call_instances.len(),
            "the plan holds one rewrite per instance, not one per claiming step",
        );
        let (stored_key, rewrite) = rewrites
            .iter()
            .find(|(_, rewrite)| rewrite.claiming_steps.len() == 2)
            .expect("the shared instance is planned like every other instance");
        assert_eq!(
            *stored_key,
            import_instance_key(&state),
            "the shared instance is the fixture's import record",
        );
        assert_eq!(
            rewrite.claiming_steps,
            BTreeSet::from([IMPORT_STEP_ID.to_string(), DUPLICATED_IMPORT_STEP_ID.to_string()]),
            "the rewrite names both steps that claim the instance",
        );
        assert_eq!(
            rewrite.current_key, *stored_key,
            "a current fixture keeps its key when the claims agree",
        );
    }

    /// The planner refuses a step whose instance records none of its id.
    ///
    /// Pairing is provenance-only, so a step whose id no recorded instance names
    /// has nothing to pair with and the run stops before any key is derived. This
    /// is the loud outcome a wholesale provenance-less fixture produces: the
    /// planner names the step that cannot be paired instead of guessing which
    /// instance it meant.
    #[tokio::test]
    async fn planner_refuses_a_step_whose_instance_records_no_provenance() {
        let generated = load_generated_doc();
        let mut state = load_conductor_state();
        let workflow = load_demo_workflow(&generated);
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let import_key = import_instance_key(&state);
        state
            .tool_call_instances
            .get_mut(&import_key)
            .expect("recorded import instance")
            .step_ids
            .clear();

        let error = plan_demo_instance_key_rewrites(&cas, &generated, &state, &workflow)
            .await
            .expect_err("a step with no recorded provenance cannot be paired");
        assert!(
            error.contains(&format!(
                "step '{IMPORT_STEP_ID}' must own exactly one recorded instance, found 0"
            )),
            "the diagnostic must name the step that owns nothing: {error}",
        );
    }

    /// The planner refuses an instance whose recorded tool is not the step's.
    ///
    /// Provenance alone says which instance a step claims; the recorded tool
    /// identity says whether that claim can be true, because a key derived under
    /// one tool's arguments cannot describe another tool's run. A record that
    /// disagrees stops the run before the rewrite, naming the step, the instance,
    /// the recorded tool and the active one.
    #[tokio::test]
    async fn planner_rejects_an_instance_recorded_for_another_tool() {
        let generated = load_generated_doc();
        let mut state = load_conductor_state();
        let workflow = load_demo_workflow(&generated);
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");

        let import_key = import_instance_key(&state);
        state
            .tool_call_instances
            .get_mut(&import_key)
            .expect("recorded import instance")
            .tool_call_id = "some-other-tool@v1".to_string();

        let error = plan_demo_instance_key_rewrites(&cas, &generated, &state, &workflow)
            .await
            .expect_err("an instance recorded for another tool must be refused");
        assert!(error.contains(IMPORT_STEP_ID), "the diagnostic must name the step: {error}");
        assert!(
            error.contains(&import_key.to_hex()),
            "the diagnostic must name the instance: {error}",
        );
        assert!(
            error.contains("recorded for tool 'some-other-tool@v1'"),
            "the diagnostic must name the recorded tool: {error}",
        );
        assert!(
            error.contains("active tool is 'import@v1'"),
            "the diagnostic must name the step's active tool: {error}",
        );
    }

    /// Environment variable that turns the fixture assertion into a rewrite.
    ///
    /// Enabled tokens are `1`, `true`, `yes`, and `on` — the set the online-demo
    /// gates use — so any other value leaves the test asserting rather than
    /// writing, and a typo cannot silently change a checked-in record.
    const UPDATE_FIXTURES_ENV: &str = "MEDIAPM_UPDATE_FIXTURES";

    /// Whether this run was asked to rewrite a checked-in fixture.
    ///
    /// Read per call and never cached, so the variable reaches the test from the
    /// command line rather than from whatever the process started with.
    #[must_use]
    fn fixture_rewrite_requested() -> bool {
        std::env::var(UPDATE_FIXTURES_ENV).is_ok_and(|value| {
            matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        })
    }

    /// Asserts that the `instance_key`s recorded in
    /// `tests/fixtures/demo_conductor_state.json` are the keys the current
    /// derivation produces, and rewrites them when asked.
    ///
    /// The fixture is a checked-in record of a real run: each
    /// `tool_call_instances` key was derived from that run's input material, so
    /// any change to the instance-key derivation makes every recorded key
    /// unreachable and the demo materialization tests stop matching instances.
    /// The test therefore runs in every suite and compares, and it writes only
    /// when `MEDIAPM_UPDATE_FIXTURES` carries an enabled token:
    ///
    /// ```text
    /// RUSTC_WRAPPER= MEDIAPM_UPDATE_FIXTURES=1 cargo test -p mediapm --lib \
    ///   demo_conductor_state_fixture_matches_its_derivation -- --nocapture
    /// ```
    ///
    /// Asserting by default is what keeps a derivation change from landing as an
    /// ignored test nobody runs: the suite names the stale key and the variable
    /// that rewrites it. `the_demo_fixture_is_in_the_current_wire_shape` holds
    /// the companion half — that the file also matches what `encode_state_json`
    /// produces — so this test reads the same fixture without comparing bytes.
    ///
    /// The rewrite replaces the recorded `instance_key` in the fixture's JSON
    /// text — the `tool_call_instances` map key, that entry's `instance_key`
    /// field, and the matching `aux.instances` key — so every other byte of the
    /// file is preserved. Provenance is read, never written: `step_ids` is the
    /// pairing record, the planner can pair an instance only through it, and a
    /// record the planner paired already carries the ids it paired by, so there
    /// is nothing to synthesize. A fixture that does not supply the provenance
    /// is refused before a byte is written, and which check refuses it says what
    /// is missing; see **Pairing** below.
    ///
    /// **Provenance.** Each instance records the steps that have served it in
    /// `step_ids` — the one that executed the work and every later one that
    /// reused it — and that set is the pairing key: a step owns the instance
    /// whose set names it. A shared instance names every claiming step, and the
    /// planner collapses them into one rewrite, so the set survives a
    /// key-scheme change — which is the state this test runs in, since every
    /// stored key is stale by definition.
    ///
    /// **Pairing.** The planner pairs through `step_ids` alone; no key derivation
    /// decides it. A record that does not supply the provenance is refused before
    /// anything is written, and the refusing check names what is missing: a step
    /// whose instance records none of its id stops the planner with `step '<id>'
    /// must own exactly one recorded instance, found 0`, since that step then has
    /// no instance to pair with at all, while an instance whose `step_ids` name no
    /// step of this workflow passes the planner and then fails the coverage check
    /// below, which names the key and the tool.
    ///
    /// **Resolution path.** The new keys are derived from the materializer's live
    /// resolution (`merge_step_inputs_with_tool_defaults`,
    /// `resolve_expected_input_hashes`, `build_instance_key_resolved_value_bytes`)
    /// rather than the conductor's `resolve_step_inputs`, because that function is
    /// `pub(super)` inside `mediapm-conductor`'s `step_worker` module and is
    /// therefore unreachable from this crate. The pairing does not rest on this
    /// path — it comes from the fixture's own `step_ids` — so a resolution helper
    /// that drifted would produce new keys, not a mismatched pairing.
    ///
    /// **Write.** The decoded state is re-encoded through `encode_state_json`
    /// before it is written, so the checked-in record always lands in the
    /// current wire shape. The bytes go to a sibling temporary file in the
    /// fixture's own directory (`demo_conductor_state.json.regenerated`) and are
    /// then renamed over the fixture, so a partial write can never be observed
    /// in a checked-in record; the sibling path keeps the rename on one
    /// filesystem, hence atomic. A failed rename leaves that temporary file
    /// behind — the rewrite is an operator action, so the leftover is reported
    /// rather than hidden: re-running truncates and reuses it.
    #[tokio::test]
    async fn demo_conductor_state_fixture_matches_its_derivation() {
        let path = fixture_path("demo_conductor_state.json");
        let generated = load_generated_doc();
        let state = load_conductor_state();
        let workflow = load_demo_workflow(&generated);

        // The fixture's keys are stale once the derivation moves, so the
        // production step-output resolver — which matches instances by key —
        // finds nothing at all; the plan below pairs through recorded provenance
        // and reconstructs the recorded run's step outputs from that instead.
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let rewrites = plan_demo_instance_key_rewrites(&cas, &generated, &state, &workflow)
            .await
            .expect("plan demo instance key rewrites");

        // Pairing holds in both modes, and it is what a rewrite must prove
        // before it may touch the file: every stored instance is claimed by some
        // step's recorded provenance, and every instance names the steps that
        // claim it.
        for (stored_key, instance) in &state.tool_call_instances {
            assert!(
                rewrites.contains_key(stored_key),
                "instance {} ({}) is claimed by no step's recorded provenance; \
                 refusing to rewrite a key this fixture cannot prove the instance owns",
                stored_key.to_hex(),
                instance.tool_call_id,
            );
            assert!(
                !instance.step_ids.is_empty(),
                "instance {} records no step, so no step can pair with it",
                stored_key.to_hex(),
            );
        }
        for (stored_key, rewrite) in &rewrites {
            let instance = state
                .tool_call_instances
                .get(stored_key)
                .expect("a planned rewrite names a recorded instance");
            for step_id in &rewrite.claiming_steps {
                assert!(
                    instance.step_ids.contains(step_id),
                    "instance {} must name step '{step_id}' as a step it served",
                    stored_key.to_hex(),
                );
            }
        }

        let drifted: Vec<String> = rewrites
            .iter()
            .filter(|(stored_key, rewrite)| rewrite.current_key != **stored_key)
            .map(|(stored_key, rewrite)| {
                format!(
                    "{} (steps {:?}) derives {}",
                    stored_key.to_hex(),
                    rewrite.claiming_steps,
                    rewrite.current_key.to_hex(),
                )
            })
            .collect();

        if !fixture_rewrite_requested() {
            assert!(
                drifted.is_empty(),
                "{} records {} stale instance key(s): {}; \
                 set {UPDATE_FIXTURES_ENV}=1 to rewrite them",
                path.display(),
                drifted.len(),
                drifted.join(", "),
            );
            return;
        }

        rewrite_demo_conductor_state_fixture(&path, &state, &rewrites);
    }

    /// Rewrites the fixture's recorded keys to the keys the current derivation
    /// produces.
    ///
    /// The caller proves every instance pairs with some step's recorded
    /// provenance before calling this, so a fixture the planner cannot pair is
    /// never touched. The replacement works on the file's JSON text — the
    /// `tool_call_instances` map key, that entry's `instance_key` field, and the
    /// matching `aux.instances` key carry the same string — and the edited text
    /// is decoded and re-encoded through `encode_state_json` before it is
    /// written, so the fixture always lands in the current wire shape. The bytes
    /// go to a sibling temporary file in the fixture's own directory and are
    /// then renamed over it, so a partial write is never observable in a
    /// checked-in record; the sibling path keeps the rename on one filesystem,
    /// hence atomic. A rewrite that would change no byte leaves the file alone
    /// and says so.
    fn rewrite_demo_conductor_state_fixture(
        path: &Path,
        state: &ConductorState,
        rewrites: &BTreeMap<Hash, DemoInstanceKeyRewrite>,
    ) {
        let original_bytes = std::fs::read(path).expect("read demo conductor state fixture");
        let mut text = String::from_utf8(original_bytes.clone())
            .expect("demo conductor state fixture is utf-8");

        let mut changed = Vec::new();
        for (stored_key, rewrite) in rewrites {
            if rewrite.current_key == *stored_key {
                println!(
                    "steps {:?}: {} already current",
                    rewrite.claiming_steps,
                    stored_key.to_hex(),
                );
                continue;
            }
            let needle = stored_key.to_string();
            let occurrences = text.matches(needle.as_str()).count();
            assert!(occurrences > 0, "instance key {needle} does not appear in the fixture text");
            println!(
                "steps {:?}: {needle} -> {} ({occurrences} occurrences)",
                rewrite.claiming_steps, rewrite.current_key,
            );
            text = text.replace(needle.as_str(), &rewrite.current_key.to_string());
            changed.push((*stored_key, rewrite.current_key));
        }

        // Validate the regenerated text before it can replace a checked-in
        // record: no stale key may survive, and every instance must carry the
        // provenance the caller's pairing check relies on.
        for (stored_key, current_key) in &changed {
            assert!(
                !text.contains(stored_key.to_string().as_str()),
                "stale key {} survived the rewrite",
                stored_key.to_hex(),
            );
            assert!(text.contains(current_key.to_string().as_str()));
        }
        let rewritten =
            decode_state_json(text.as_bytes()).expect("regenerated demo conductor state decodes");
        assert_eq!(rewritten.tool_call_instances.len(), state.tool_call_instances.len());
        for (key, instance) in &rewritten.tool_call_instances {
            assert!(
                !instance.step_ids.is_empty(),
                "instance {} records no step; the provenance insertion missed it",
                key.to_hex(),
            );
        }
        for rewrite in rewrites.values() {
            let current_key = rewrite.current_key;
            assert!(
                rewritten.tool_call_instances.contains_key(&current_key),
                "rewritten fixture is missing the key steps {:?} derive",
                rewrite.claiming_steps,
            );
            let instance = &rewritten.tool_call_instances[&current_key];
            for step_id in &rewrite.claiming_steps {
                assert!(
                    instance.step_ids.contains(step_id),
                    "rewritten fixture must name step '{step_id}' as a step the instance served",
                );
            }
        }

        // Re-encode from the value, not from the edited text: the fixture is a
        // checked-in record of the current wire shape, and a file that arrives
        // one migration behind would otherwise be migrated on every read and
        // never exercise the encoder.
        let mut encoded = encode_state_json(&rewritten).expect("encode regenerated demo state");
        encoded.push(b'\n');
        if encoded == original_bytes {
            println!("fixture is already current; nothing written");
            return;
        }

        // Same-directory temporary file plus rename: the fixture is a checked-in
        // record, so a partial write must never be observable, and a sibling path
        // keeps the rename on one filesystem (hence atomic).
        let temporary_path = path.with_extension("json.regenerated");
        std::fs::write(&temporary_path, &encoded).expect("write regenerated demo conductor state");
        std::fs::rename(&temporary_path, path)
            .expect("move regenerated demo conductor state over the fixture");
        if changed.is_empty() {
            println!("normalized the fixture to the current wire shape");
        } else {
            println!("rewrote {} instance key(s)", changed.len());
        }
    }

    /// The checked-in fixture is already in the current state wire shape.
    ///
    /// A fixture one migration behind is migrated on every read, so the write
    /// path the runtime uses — `encode_state_json` — is never exercised by any
    /// fixture-backed test and a shape change could land unnoticed. Re-encoding
    /// the stored bytes and comparing them holds that invariant directly: the
    /// file is either canonical or this test names the drift, and
    /// `demo_conductor_state_fixture_matches_its_derivation` with
    /// `MEDIAPM_UPDATE_FIXTURES=1` is what rewrites it.
    #[test]
    fn the_demo_fixture_is_in_the_current_wire_shape() {
        let path = fixture_path("demo_conductor_state.json");
        let bytes = std::fs::read(&path).expect("read demo conductor state fixture");
        let state = decode_state_json(&bytes).expect("decode demo conductor state fixture");
        let mut encoded = encode_state_json(&state).expect("encode demo conductor state");
        encoded.push(b'\n');

        assert_eq!(
            String::from_utf8(encoded).expect("encoded state is utf-8"),
            String::from_utf8(bytes).expect("fixture is utf-8"),
            "the checked-in demo state fixture must already be in the current wire shape; run \
             `MEDIAPM_UPDATE_FIXTURES=1 cargo test -p mediapm --lib \
             demo_conductor_state_fixture_matches_its_derivation -- --nocapture` to rewrite it",
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the two seeded instances and the step resolving between them read as one scenario"
    )]
    async fn merged_resolution_prefers_specialized_yt_dlp_sidecar_instance() {
        use mediapm_conductor::{
            CapturedOutput, OutputCaptureSpec, SaveMode, ToolCallInstance, ToolKindSpec, ToolSpec,
            WorkflowSpec, WorkflowStepSpec,
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
                invalidated_at: None,
                executed_at: Timestamp::from_unix_nanos(2),
                step_ids: BTreeSet::new(),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([
                    (
                        "primary".to_string(),
                        CapturedOutput { hash: primary_infojson_hash, capture: None },
                    ),
                    (
                        "yt_dlp_infojson_file".to_string(),
                        CapturedOutput { hash: primary_infojson_hash, capture: None },
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
                invalidated_at: None,
                executed_at: Timestamp::from_unix_nanos(1),
                step_ids: BTreeSet::new(),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    CapturedOutput { hash: sidecar_infojson_hash, capture: None },
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
            CapturedOutput, OutputCaptureSpec, SaveMode, ToolCallInstance, ToolKindSpec, ToolSpec,
            WorkflowSpec, WorkflowStepSpec,
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
                invalidated_at: None,
                executed_at: Timestamp::from_unix_nanos(1),
                step_ids: BTreeSet::new(),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    CapturedOutput { hash: infojson_hash, capture: None },
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
            CapturedOutput, OutputCaptureSpec, SaveMode, ToolCallInstance, ToolKindSpec, ToolSpec,
            WorkflowSpec, WorkflowStepSpec,
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
                invalidated_at: None,
                executed_at: Timestamp::from_unix_nanos(2),
                step_ids: BTreeSet::new(),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([
                    (
                        "primary".to_string(),
                        CapturedOutput { hash: primary_video_hash, capture: None },
                    ),
                    (
                        "yt_dlp_infojson_file".to_string(),
                        CapturedOutput { hash: infojson_hash, capture: None },
                    ),
                    (
                        "yt_dlp_description_file".to_string(),
                        CapturedOutput { hash: infojson_hash, capture: None },
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
            CapturedOutput, ConfigPayloadStaging, OutputCaptureSpec, SaveMode, ToolCallInstance,
            ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
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
                        // The generated document this fixture stands for copies
                        // its payload, which is the strategy that needs no root
                        // and therefore survives an encode.
                        payload_staging: ConfigPayloadStaging::Copy,
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
                invalidated_at: None,
                executed_at: Timestamp::from_unix_nanos(1),
                step_ids: BTreeSet::new(),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_infojson_file".to_string(),
                    CapturedOutput { hash: infojson_hash, capture: None },
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

    /// Media id whose managed workflow the demo fixtures record.
    ///
    /// The fixture's workflow is named for this id, which is what the arm under
    /// test looks up before it can reach the extraction.
    const PINNED_MEDIA_ID: &str = "demo.local.dQw4w9WgXcQ";

    /// Variant the fixture declares as a folder pack holding [`PINNED_MEMBER`].
    const PINNED_VARIANT: &str = "packed";

    /// Member the fixture's binding asks the extraction to pull out of the pack.
    ///
    /// Named rather than derived, because it is the member name the assertion
    /// looks for in the failure message.
    const PINNED_MEMBER: &str = "track.en.vtt";

    /// The ffmpeg step the demo workflow records, which the fixture's binding
    /// has to name for the arm to resolve an output hash at all.
    const PINNED_FFMPEG_STEP_ID: &str = "step-1-ffmpeg";

    /// Returns the demo fixtures' source spec, declaring one variant that the
    /// workflow records as a pack and materializes one member out of.
    ///
    /// The steps mirror the demo document's order so the binding derives the
    /// step id the recorded workflow actually carries; a different order would
    /// resolve to a step the fixture never ran.
    fn pinned_zip_member_source(input_hash: &str, pack_hash: &str) -> MediaSourceSpec {
        use crate::config::source_types::{MediaStep, MediaStepTool};
        use crate::config::{GenericOutputVariantConfig, OutputVariantValue};

        MediaSourceSpec {
            steps: vec![
                MediaStep {
                    tool: MediaStepTool::Import,
                    input_variants: vec![],
                    output_variants: BTreeMap::from([(
                        "default".into(),
                        OutputVariantValue::Generic(GenericOutputVariantConfig {
                            kind: "primary".to_string(),
                            ..Default::default()
                        }),
                    )]),
                    options: BTreeMap::new(),
                },
                MediaStep {
                    tool: MediaStepTool::Ffmpeg,
                    input_variants: vec!["default".to_string()],
                    output_variants: BTreeMap::from([(
                        PINNED_VARIANT.into(),
                        OutputVariantValue::Generic(GenericOutputVariantConfig {
                            kind: "primary".to_string(),
                            zip_member: PINNED_MEMBER.to_string(),
                            extension: "vtt".to_string(),
                            ..Default::default()
                        }),
                    )]),
                    options: BTreeMap::new(),
                },
            ],
            variant_hashes: BTreeMap::from([
                ("default".to_string(), input_hash.to_string()),
                (PINNED_VARIANT.to_string(), pack_hash.to_string()),
            ]),
            ..MediaSourceSpec::default()
        }
    }

    /// A failed ZIP-member extraction is reported, not answered with the pack.
    ///
    /// **This test names its own reach.** It calls [`resolve_variant_source_bytes`]
    /// directly with `full_zip_archive = false`, and no production caller does
    /// that: the only caller in `materializer::mod` hard-codes `true`, so every
    /// `!full_zip_archive` arm in the function is dead in production. What is
    /// pinned here is the helper's contract, not a user-visible path. The arm is
    /// worth pinning for the same reason it is dead: nothing in production will
    /// ever draw attention to it.
    ///
    /// The defect is not the extraction failing, which is correct. It is what the
    /// failure used to become: the `Err` was discarded, control fell out of the
    /// block into the raw-bytes fallback, and the caller received the *pack* as
    /// the variant's content. The assertion therefore distinguishes an error from
    /// a wrong success, which is why it checks the error and not only its message.
    ///
    /// The demo fixture supplies the recorded conductor state the arm needs before
    /// it extracts anything, so the resolution above the defect is real rather than
    /// stubbed.
    #[tokio::test]
    async fn a_directly_called_zip_member_extraction_failure_is_reported_rather_than_answered_with_the_pack()
     {
        let generated = load_generated_doc();
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        // The arm reads the bound step's bytes out of the store by the hash the
        // state records, so the state has to record the pack. A store is
        // content-addressed, which makes recording the pack's own hash the only
        // way to put chosen bytes behind a recorded hash.
        let pack_hash = cas.put(bytes::Bytes::from_static(b"not a zip archive")).await.unwrap();
        let mut state = load_conductor_state();
        // The fixture records more than one ffmpeg instance, and resolution
        // selects among them by inputs rather than by key, so every one of them
        // has to record the pack. Pointing only the first at it left the test
        // reading a hash nothing held, which is how it silently reached nothing.
        let mut recorded_pack = 0;
        for instance in state.tool_call_instances.values_mut() {
            if instance.tool_call_id.starts_with("ffmpeg@")
                && let Some(primary) = instance.outputs.get_mut("primary")
            {
                primary.hash = pack_hash;
                recorded_pack += 1;
            }
        }
        assert!(
            recorded_pack > 0,
            "the fixture must record the pack on an ffmpeg instance, or no step output resolves \
             to it"
        );
        assert!(
            cas.get(pack_hash).await.is_ok(),
            "the fixture's recorded output hash must be readable, or the arm bails before it \
             extracts anything and this test reaches nothing"
        );

        let input_hash = cas.put(bytes::Bytes::from_static(b"pinned input")).await.unwrap();
        let source = pinned_zip_member_source(&input_hash.to_string(), &pack_hash.to_string());

        // Precondition, asserted rather than assumed: if the binding stops
        // naming the recorded step and output, the arm stops resolving and the
        // assertions below would pass without the extraction ever running.
        let slot_limits = resolve_ffmpeg_slot_limits(&MediaPmDocument::default());
        let binding = resolve_media_variant_output_binding_with_limits(
            &source,
            PINNED_VARIANT,
            slot_limits.max_input_slots,
            slot_limits.max_output_slots,
        )
        .expect("the fixture's binding resolves")
        .expect("the fixture declares a binding for its variant");
        assert_eq!(
            binding.step_id, PINNED_FFMPEG_STEP_ID,
            "the binding must name the step the demo workflow recorded, or the arm resolves no \
             output hash and never reaches the extraction"
        );
        assert_eq!(
            binding.output_name, "primary",
            "the demo state records the ffmpeg step's `primary` output, and the binding has to \
             ask for that name or the lookup finds nothing"
        );
        assert_eq!(
            binding.zip_member.as_deref(),
            Some(PINNED_MEMBER),
            "the arm only extracts when the binding carries a member"
        );

        let lookup =
            MaterializationLookupContext::new(cas.clone(), Some(state), generated, slot_limits);
        let result =
            resolve_variant_source_bytes(&lookup, PINNED_MEDIA_ID, &source, PINNED_VARIANT, false)
                .await;

        let error = match result {
            Ok(payload) => panic!(
                "an extraction that failed must be reported; returning the pack's {} bytes instead \
                 is the defect this test exists for",
                payload.bytes.len()
            ),
            Err(error) => error,
        };
        assert!(
            matches!(&error, MediaPmError::Workflow(_)),
            "a failed extraction is a workflow error, not an I/O one: {error:?}"
        );
        let message = error.to_string();
        assert!(
            message.contains("extracting ZIP member"),
            "the message must name the extraction that failed rather than a later fallback: \
             {message}"
        );
        assert!(
            message.contains(PINNED_MEMBER),
            "the message must name the member the binding asked for: {message}"
        );
    }
}
