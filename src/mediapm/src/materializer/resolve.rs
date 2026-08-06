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
    managed_workflow_name, resolve_media_variant_output_binding_with_limits,
};

use super::zip::{
    extract_zip_member_bytes, parse_external_data_reference, parse_step_output_reference,
};
use super::{
    ExpectedStepInputs, InputBindingHashResolution, MaterializationLookupContext,
    RequiredStepOutputNames, RequiredStepZipMembers, StepOutputHashes, VariantSourceBytes,
};

// ---------------------------------------------------------------------------
// Hierarchy / variant enumeration
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Variant hash / bytes resolution
// ---------------------------------------------------------------------------

/// Resolves the CAS hash for one variant (conductor state first, then local).
pub(super) async fn resolve_variant_hash(
    media_id: &str,
    variant_name: &str,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext,
) -> Result<Option<Hash>, MediaPmError> {
    if let Some(state) = lookup.conductor_state.as_ref() {
        if let Some((workflow_hash, _notice)) =
            resolve_variant_hash_from_workflow_state(lookup, state, media_id, source, variant_name)
                .await?
        {
            let binding = resolve_media_variant_output_binding_with_limits(
                source,
                variant_name,
                lookup.ffmpeg_slot_limits.max_input_slots,
                lookup.ffmpeg_slot_limits.max_output_slots,
            )?;
            if binding.is_none_or(|binding| binding.zip_member.is_none()) {
                return Ok(Some(workflow_hash));
            }
            return Ok(None);
        }
    }

    resolve_local_variant_hash(media_id, variant_name, source)
}

/// Resolves variant bytes with optional fallback notice and source hash.
pub(super) async fn resolve_variant_source_bytes(
    lookup: &MaterializationLookupContext,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<VariantSourceBytes, MediaPmError> {
    if let Some(state) = lookup.conductor_state.as_ref()
        && let Some((workflow_hash, fallback_notice)) =
            resolve_variant_hash_from_workflow_state(lookup, state, media_id, source, variant)
                .await?
    {
        let bytes = lookup.cas.get(workflow_hash).await.map_err(|source| {
            MediaPmError::Workflow(format!(
                "workflow output hash '{workflow_hash}' for media '{media_id}' variant '{variant}' is missing from CAS: {source}"
            ))
        })?;

        let (materialized_bytes, source_hash) = if let Some(binding) =
            resolve_media_variant_output_binding_with_limits(
                source,
                variant,
                lookup.ffmpeg_slot_limits.max_input_slots,
                lookup.ffmpeg_slot_limits.max_output_slots,
            )? {
            if let Some(zip_member) = binding.zip_member.as_deref() {
                (
                    extract_zip_member_bytes(bytes.as_ref(), zip_member).map_err(|error| {
                        MediaPmError::Workflow(format!(
                            "extracting ZIP member '{zip_member}' for media '{media_id}' variant '{variant}' failed: {error}"
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

fn resolve_local_variant_hash(
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
        let merged_step_inputs =
            merge_step_inputs_with_tool_defaults(generated_doc, &step.tool, &step.inputs)?;
        let expected_inputs =
            resolve_expected_input_hashes(cas, generated_doc, &merged_step_inputs, &step_outputs)
                .await?;
        let Some(expected_inputs) = expected_inputs else {
            continue;
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
            build_instance_key_resolved_value_bytes(cas, &merged_step_inputs, &expected_inputs)
                .await?;

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
            continue;
        };

        let mut output_hashes = instance
            .outputs
            .iter()
            .map(|(name, output)| (name.clone(), output.hash))
            .collect::<BTreeMap<_, _>>();
        if let Some(stdout) = instance.outputs.get("stdout") {
            for output_name in step.outputs.keys() {
                output_hashes.entry(output_name.clone()).or_insert(stdout.hash);
            }
        }
        step_outputs.insert(step.id.clone(), output_hashes);
    }

    if step_outputs.is_empty() { Ok(None) } else { Ok(Some(step_outputs)) }
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
            "workflow step references unknown tool '{}' in generated config",
            step_tool
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
    instance
        .outputs
        .get(output_name)
        .or_else(|| (output_name != "stdout").then(|| instance.outputs.get("stdout")).flatten())
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
    expected_outputs.keys().all(|output_name| {
        instance.outputs.contains_key(output_name)
            || (output_name != "stdout" && instance.outputs.contains_key("stdout"))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use mediapm_cas::FileSystemCas;
    use mediapm_conductor::{decode_document, decode_state_json};
    use tempfile::tempdir;

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
        let cas_root = tempdir().expect("temp cas dir");
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
        let cas_root = tempdir().expect("temp cas dir");
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
}
