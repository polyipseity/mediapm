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
    // A hash this variant resolves to whose bytes the store no longer holds. It
    // is recorded rather than raised immediately because the other source of a
    // hash may still answer for the variant, which is the ordinary stale-seed
    // case; it is raised at the end, when nothing did.
    let mut lost: Option<Hash> = None;

    if let Some(local_hash) = resolve_local_variant_hash(media_id, variant_name, source)? {
        if cas_hash_is_readable(&lookup.cas, local_hash).await {
            return Ok(Some(local_hash));
        }
        lost = Some(local_hash);
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
        let readable = cas_hash_is_readable(&lookup.cas, workflow_hash).await;
        if binding.is_none_or(|binding| binding.zip_member.is_none()) && readable {
            return Ok(Some(workflow_hash));
        }
        // Only an unreadable workflow hash is a lost object. A readable one
        // whose binding names a ZIP member falls through on purpose: the member
        // is extracted by the caller, which asked for the pack's hash and not
        // for these bytes.
        if !readable {
            lost = Some(workflow_hash);
        }
    }

    match lost {
        Some(hash) => Err(missing_object_error(media_id, variant_name, hash)),
        None => Ok(None),
    }
}

/// Reports a variant whose resolved hash names bytes the store does not hold.
///
/// The distinction this message carries is the one the caller acts on: a variant
/// no producer has supplied is a workflow that has not run, while a hash that
/// resolved and then could not be read means the document and the recorded state
/// both promise bytes the library cannot hold. Only the second is this error.
fn missing_object_error(media_id: &str, variant: &str, hash: Hash) -> MediaPmError {
    MediaPmError::Workflow(format!(
        "media '{media_id}' variant '{variant}' resolved to CAS object {hash}, and the store does \
         not hold it"
    ))
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

/// Why a variant did not resolve to bytes.
///
/// The two kinds are the caller's decision, and they are why this is a type
/// rather than one error: a variant nothing has produced is a workflow that has
/// not run, which the materializer reports as a missing path, while bytes the
/// store lost are the run's failure.
#[derive(Debug)]
pub(super) enum VariantResolveFailure {
    /// No producer supplied this variant yet.
    NotProduced(MediaPmError),
    /// A hash resolved and the bytes behind it are unavailable or unusable.
    Fatal(MediaPmError),
}

/// Resolves variant bytes with optional fallback notice and source hash.
///
/// Three outcomes are deliberately not failures. A conductor-state lookup that
/// yields nothing falls through to the source's own variant hashes, as do bytes
/// that are not the ZIP archive the caller asked for. That fallback is why the
/// conductor-state block falls through instead of returning early. A variant
/// whose resolved hash cannot be read is *not* one of them: see
/// [`VariantResolveFailure`].
///
/// # Errors
///
/// Returns [`VariantResolveFailure::NotProduced`] when nothing names bytes for
/// this variant, and [`VariantResolveFailure::Fatal`] when a hash resolved but
/// the store cannot supply it, when the declared hash does not parse, or when a
/// ZIP member the binding names cannot be extracted. The member failure is
/// reported rather than answered with the pack: returning the archive as the
/// variant's content would be a wrong success, and a caller cannot tell that
/// apart from the right answer.
pub(super) async fn resolve_variant_source_bytes(
    lookup: &MaterializationLookupContext,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
    full_zip_archive: bool,
) -> Result<VariantSourceBytes, VariantResolveFailure> {
    use VariantResolveFailure::{Fatal, NotProduced};

    let workflow_resolution = match lookup.conductor_state.as_ref() {
        Some(state) => {
            resolve_variant_hash_from_workflow_state(lookup, state, media_id, source, variant)
                .await
                .map_err(Fatal)?
        }
        None => None,
    };

    if let Some((workflow_hash, fallback_notice)) = workflow_resolution {
        match lookup.cas.get(workflow_hash).await {
            // Bytes that are not the archive the caller asked for fall through
            // to the source's own hashes: the caller asked for specific
            // content, and this hash names content of another shape.
            Ok(bytes) if !full_zip_archive || is_zip_content(bytes.as_ref()) => {
                let binding = resolve_media_variant_output_binding_with_limits(
                    source,
                    variant,
                    lookup.ffmpeg_slot_limits.max_input_slots,
                    lookup.ffmpeg_slot_limits.max_output_slots,
                )
                .map_err(Fatal)?;
                // A caller asking for the whole archive never extracts: it
                // asked for these exact bytes and got them.
                let zip_member = binding
                    .as_ref()
                    .filter(|_| !full_zip_archive)
                    .and_then(|binding| binding.zip_member.as_deref());
                let (materialized_bytes, source_hash) = match zip_member {
                    Some(zip_member) => {
                        let member_bytes =
                            extract_zip_member_bytes(bytes.as_ref(), zip_member).map_err(
                                |error| {
                                    Fatal(MediaPmError::Workflow(format!(
                                        "extracting ZIP member '{zip_member}' for media '{media_id}' variant '{variant}' failed: {error}"
                                    )))
                                },
                            )?;
                        (member_bytes, None)
                    }
                    None => (bytes.as_ref().to_vec(), Some(workflow_hash)),
                };

                return Ok(VariantSourceBytes {
                    bytes: materialized_bytes,
                    notice: fallback_notice,
                    source_hash,
                });
            }
            Ok(_) => {}
            Err(_) => return Err(Fatal(missing_object_error(media_id, variant, workflow_hash))),
        }
    }

    if source.variant_hashes.is_empty() {
        return Err(NotProduced(MediaPmError::Workflow(format!(
            "media '{media_id}' variant '{variant}' has no local variant hashes and no workflow output hash resolved from conductor state"
        ))));
    }

    let Some(hash_string) =
        source.variant_hashes.get(variant).or_else(|| source.variant_hashes.get("default"))
    else {
        return Err(NotProduced(MediaPmError::Workflow(format!(
            "media '{media_id}' does not define hash pointer for variant '{variant}'"
        ))));
    };

    let hash = hash_string.parse::<Hash>().map_err(|_| {
        Fatal(MediaPmError::Workflow(format!(
            "media '{media_id}' variant '{variant}' has invalid CAS hash '{hash_string}'"
        )))
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
        Err(_) => Err(Fatal(missing_object_error(media_id, variant, hash))),
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
/// Every step is offered to strict input-key matching first. A step whose key
/// matches no recorded instance falls back to the instances that name it:
/// exactly one answers, several answer only when they record the same hash for
/// every required output, and several that disagree are an error rather than a
/// pick.
pub(super) async fn resolve_workflow_step_output_hashes(
    cas: &FileSystemCas,
    generated_doc: &NickelDocument,
    state: &ConductorState,
    workflow: &mediapm_conductor::WorkflowSpec,
) -> Result<Option<StepOutputHashes>, MediaPmError> {
    let mut step_outputs = StepOutputHashes::new();
    let required_step_output_names = collect_required_step_output_names(workflow);
    let required_step_zip_members = collect_required_step_zip_members(workflow);
    let mut unresolved_inputs_by_step: BTreeMap<String, UnresolvedInputs> = BTreeMap::new();

    for step in &workflow.steps {
        let strict = resolve_single_step_output_hashes_strict(
            cas,
            generated_doc,
            state,
            step,
            &required_step_output_names,
            &required_step_zip_members,
            &step_outputs,
        )
        .await?;
        // Kept for the relaxed pass: a refusal has to say that the strict key was
        // never derived, and which input stopped it.
        if !strict.unresolved_inputs.is_empty() {
            unresolved_inputs_by_step.insert(step.id.clone(), strict.unresolved_inputs);
        }
        if let Some(outputs) = strict.outputs
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
        let unresolved_inputs = unresolved_inputs_by_step.remove(&step.id).unwrap_or_default();
        if let Some(outputs) = resolve_single_step_output_hashes_relaxed(
            generated_doc,
            state,
            &workflow.name,
            step,
            &unresolved_inputs,
        )? && step_declared_outputs_resolved(step, &outputs)
        {
            step_outputs.insert(step.id.clone(), outputs);
        }
    }

    if step_outputs.is_empty() { Ok(None) } else { Ok(Some(step_outputs)) }
}

/// What one input's reconstruction failed on.
///
/// The causes are separate because a reader acts on them differently: a missing
/// object sends them to the store, an unreadable archive member sends them to the
/// bytes the store holds, a binding's own template sends them to the document, and
/// a prior step output sends them to the traversal or to the step that produced it.
enum UnresolvedInputCause {
    /// The binding names a step output this traversal has not resolved.
    PriorStepOutput,
    /// The store could not read the object the binding names.
    ///
    /// A miss, a corrupt object and an I/O failure all land here, so the phrase
    /// says the read failed and the detail says which way.
    MissingObject,
    /// The archive the binding names holds no member it selects, or the bytes it
    /// names are not an archive at all.
    MissingArchiveMember,
    /// The binding's own template did not resolve, so it names no value at all.
    UnresolvedBinding,
}

impl UnresolvedInputCause {
    /// The phrase the refusal uses to say why the input did not resolve.
    ///
    /// Each phrase is true of every way its cause arises, because a phrase that
    /// fits one of two causes sends a reader to the wrong place.
    fn phrase(&self) -> &'static str {
        match self {
            Self::PriorStepOutput => "names a step output this traversal did not resolve",
            Self::MissingObject => "names an object the store could not read",
            Self::MissingArchiveMember => "names an archive member the stored bytes do not contain",
            Self::UnresolvedBinding => "names a binding whose template did not resolve",
        }
    }
}

/// One input the strict pass could not reconstruct, and why.
struct UnresolvedInput {
    /// What the reconstruction failed on.
    cause: UnresolvedInputCause,
    /// The failing operation's own words, for the causes that have one.
    detail: Option<String>,
}

impl UnresolvedInput {
    /// This input as the refusal names it: its binding name, the cause, and the
    /// detail when the failing operation supplied one.
    fn describe(&self, input_name: &str) -> String {
        match self.detail.as_deref() {
            Some(detail) => format!("'{input_name}' ({}: {detail})", self.cause.phrase()),
            None => format!("'{input_name}' ({})", self.cause.phrase()),
        }
    }
}

/// Records the bindings whose template did not resolve, keeping a cause an
/// input already carries.
///
/// Each binding takes one path through
/// [`build_instance_key_resolved_value_bytes`]: a `:zip(...)` selector is
/// answered from the store and never reaches the template engine, and every
/// other binding reaches the engine and never the store. A name therefore
/// reaches one set only, and the check states that rather than assuming it: a
/// second cause for one input would replace the store's answer with a weaker
/// one.
fn record_unresolved_bindings(unresolved: &mut UnresolvedInputs, bindings: &BTreeSet<String>) {
    for input_name in bindings {
        if unresolved.contains_key(input_name) {
            continue;
        }
        unresolved.insert(
            input_name.clone(),
            UnresolvedInput { cause: UnresolvedInputCause::UnresolvedBinding, detail: None },
        );
    }
}

/// The inputs that stopped the strict pass, keyed by their binding names.
type UnresolvedInputs = BTreeMap<String, UnresolvedInput>;

/// The strict pass's answer for one step, and what it could not reconstruct.
struct ExpectedInputHashes {
    /// The expected input hashes, absent when an input names a step output this
    /// traversal has not resolved and no key can be derived at all.
    inputs: Option<ExpectedStepInputs>,
    /// The inputs that prevented a key, with the reason each failed.
    unresolved: UnresolvedInputs,
}

impl ExpectedInputHashes {
    /// The expected inputs alone, for the test helpers that compose no key and
    /// so have nothing to say about the inputs they could not resolve.
    #[cfg(test)]
    fn into_expected_inputs(self) -> Option<ExpectedStepInputs> {
        self.inputs
    }
}

/// The strict pass's answer for one step.
struct StrictStepResolution {
    /// The outputs the step's key matched, or nothing when no instance matched.
    outputs: Option<BTreeMap<String, Hash>>,
    /// The inputs that stopped this pass from deriving the step's key.
    unresolved_inputs: UnresolvedInputs,
}

async fn resolve_single_step_output_hashes_strict(
    cas: &FileSystemCas,
    generated_doc: &NickelDocument,
    state: &ConductorState,
    step: &mediapm_conductor::WorkflowStepSpec,
    required_step_output_names: &RequiredStepOutputNames,
    required_step_zip_members: &RequiredStepZipMembers,
    prior_step_outputs: &StepOutputHashes,
) -> Result<StrictStepResolution, MediaPmError> {
    let merged_step_inputs =
        merge_step_inputs_with_tool_defaults(generated_doc, &step.tool, &step.inputs)?;
    let expected =
        resolve_expected_input_hashes(cas, generated_doc, &merged_step_inputs, prior_step_outputs)
            .await?;
    let ExpectedInputHashes { inputs, mut unresolved } = expected;
    let Some(mut expected_inputs) = inputs else {
        return Ok(StrictStepResolution { outputs: None, unresolved_inputs: unresolved });
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

    let resolved_values = build_instance_key_resolved_value_bytes(
        cas,
        &step.inputs,
        &merged_step_inputs,
        &expected_inputs,
        prior_step_outputs,
    )
    .await?;
    // A binding the worker resolved and this pass could not leaves a term out of
    // the key, and the key derived without it is not this step's key.
    expected_inputs.unresolved_input_names.extend(resolved_values.unresolved_names());
    // A name whose hash never reconstructed already carries its own cause from
    // the pass that recorded it. Only the binding's own failures are new here, and
    // the engine reports their names without the reason each failed.
    record_unresolved_bindings(&mut unresolved, &resolved_values.unresolved_bindings);
    let key_material = materializer_key_material(
        generated_doc,
        &step.tool,
        &merged_step_inputs,
        &resolved_values.values,
    )?;

    let mut matching_instances = state
        .tool_call_instances
        .iter()
        .filter_map(|(instance_key, instance)| {
            (instance.tool_call_id == conductor_tool_key
                && instance_matches_expected_inputs(
                    instance_key,
                    instance,
                    &expected_inputs,
                    &key_material,
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
        return Ok(StrictStepResolution { outputs: None, unresolved_inputs: unresolved });
    };

    Ok(StrictStepResolution {
        outputs: Some(output_hashes_from_instance(instance)),
        unresolved_inputs: unresolved,
    })
}

fn step_declared_outputs_resolved(
    step: &mediapm_conductor::WorkflowStepSpec,
    outputs: &BTreeMap<String, Hash>,
) -> bool {
    step.outputs.keys().all(|output_name| outputs.contains_key(output_name))
}

/// Returns true when an instance records this step among the steps it served.
///
/// Both halves of a ref decide: the workflow and the step id. A ref written
/// before refs carried a workflow id reads with an empty one, and an empty
/// workflow id names no workflow, so such a record answers nothing through the
/// relaxed pass. The step is then a miss, and the next run executes it. An
/// instance that records no ref at all names no step and is not a candidate
/// either: the record is the only provenance a state has for what served a step.
fn instance_names_step(instance: &ToolCallInstance, workflow_name: &str, step_id: &str) -> bool {
    instance
        .step_ids
        .iter()
        .any(|recorded| recorded.step_id == step_id && recorded.workflow_id == workflow_name)
}

/// Resolves one step's outputs from the instances that name it, when its key
/// matched none.
///
/// A candidate is an instance of the step's tool that records this step among
/// the steps it served and records every required output name. Recorded names
/// decide candidacy. Whether the store still holds the bytes does not: a record
/// whose object is gone still answers for the step, and the read path reports
/// the lost object as the run's failure instead of this pass reporting a step
/// that produced nothing.
///
/// Three outcomes:
///
/// - no candidate: the step produced nothing this state can name, so the caller
///   records a miss;
/// - one candidate, or several that record the same hash for every required
///   output: that is the step's work, and its hashes answer. Agreement chooses
///   nothing, which makes it safe where a choice between records would not be;
/// - several that disagree on a required output: this refuses, naming the
///   workflow, the step, the tool key and every candidate. A step names the
///   work it performs, so a state where two records answer to it with different
///   bytes needs a re-run, which `mediapm media invalidate <media-id>
///   <step-index>` starts. One limit belongs with that advice: a step whose
///   inputs do not resolve derives no key, so the re-run reports the same
///   disagreement. The refusal names those inputs and the reason each failed.
fn resolve_single_step_output_hashes_relaxed(
    generated_doc: &NickelDocument,
    state: &ConductorState,
    workflow_name: &str,
    step: &mediapm_conductor::WorkflowStepSpec,
    unresolved_inputs: &UnresolvedInputs,
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
    let mut candidates = Vec::new();
    for (instance_key, instance) in &state.tool_call_instances {
        if instance.tool_call_id != conductor_tool_key {
            continue;
        }
        if !instance_names_step(instance, workflow_name, &step.id) {
            continue;
        }
        if !required_output_names.iter().all(|name| instance.outputs.contains_key(name)) {
            continue;
        }
        candidates.push((instance_key, instance));
    }

    match candidates.as_slice() {
        [] => Ok(None),
        [(_, instance)] => Ok(Some(output_hashes_from_instance(instance))),
        several => {
            let (_, first) = several[0];
            let agreed = several.iter().all(|(_, instance)| {
                required_output_names.iter().all(|name| {
                    instance.outputs.get(name).map(|output| output.hash)
                        == first.outputs.get(name).map(|output| output.hash)
                })
            });
            if agreed {
                return Ok(Some(output_hashes_from_instance(first)));
            }

            let described = several
                .iter()
                .map(|(instance_key, instance)| {
                    format!(
                        "{instance_key} ({} output(s), executed_at {} unix seconds)",
                        instance.outputs.len(),
                        instance.executed_at.as_unix_secs(),
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let strict_note = describe_unresolved_inputs(unresolved_inputs);
            Err(MediaPmError::Workflow(format!(
                "workflow '{workflow_name}' step '{}': no single instance answers for it, and {} \
                 instances of tool '{conductor_tool_key}' name that step while recording different \
                 required outputs: {described}; refusing to choose between them. \
                 `mediapm media invalidate <media-id> <step-index>` marks the record this state \
                 holds for the step, and the next sync runs it again.{strict_note}",
                step.id,
                several.len(),
            )))
        }
    }
}

/// Returns the hashes an instance recorded, keyed by the names it recorded them
/// under — exactly that, with no name inferred from another.
///
/// A declared stream capture (`stdout`, `stderr`, `process_code`) is never
/// aliased to the recorded `stdout` hash. The alias would guess that a
/// declaration naming a stream holds the stream's bytes, which is true of the
/// implicit captures and not of a name that reads `stderr`. Every name a step
/// binds is recorded under its own name and spec, and both resolution paths
/// above require the bound names to be present before selecting an instance. An
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

/// Rebuilds the key material of one step from a generated document.
///
/// The materializer does not run the command, so it feeds
/// [`mediapm_conductor::key_material_for_tool`] the same two inputs the executor
/// derives from: each binding's resolved value bytes, and the bindings
/// themselves. A selector's source is therefore the hash the binding names,
/// which is the object the executor would have staged.
fn materializer_key_material(
    generated_doc: &NickelDocument,
    step_tool: &str,
    merged_bindings: &BTreeMap<String, String>,
    resolved_values: &BTreeMap<String, Vec<u8>>,
) -> Result<Vec<Hash>, MediaPmError> {
    let (_, tool_spec) = find_active_tool_spec(generated_doc, step_tool).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "workflow step references unknown tool '{step_tool}' in generated config",
        ))
    })?;
    Ok(mediapm_conductor::key_material_for_tool(
        tool_command_parts(tool_spec),
        resolved_values,
        merged_bindings,
    ))
}

/// The bytes an instance key hashes, and the inputs that kept this pass from
/// producing them.
///
/// The two unresolved sets are kept apart because their causes come from different
/// places. A hash this pass never reconstructed was already recorded with its own
/// cause by [`resolve_expected_input_hashes`]; a binding the template engine could
/// not resolve is new here, and the engine reports its name without the reason it
/// failed.
#[derive(Default)]
struct ResolvedKeyValueBytes {
    /// Binding name to the bytes its template resolved to.
    values: BTreeMap<String, Vec<u8>>,
    /// Inputs whose hash this pass never reconstructed.
    unresolved_hashes: BTreeSet<String>,
    /// Inputs whose binding template did not resolve.
    unresolved_bindings: BTreeSet<String>,
}

impl ResolvedKeyValueBytes {
    /// Every input this pass could not turn into bytes, whichever cause applies.
    fn unresolved_names(&self) -> BTreeSet<String> {
        self.unresolved_hashes.union(&self.unresolved_bindings).cloned().collect()
    }
}

/// Value bytes of every binding the step's key material hashes.
///
/// The step worker derives those bytes by resolving each binding through the
/// template engine with no store handle, then hashing the result. A reader that
/// has only the document and the recorded instances has to reproduce exactly
/// that, so this asks the conductor for the worker's own resolution rather than
/// re-deriving it: a literal binding resolves to the text its template produces
/// on this host — a `${context.os …}` conditional selects a path — and a
/// hash-backed binding resolves to the hash text, because the worker holds no
/// handle to fetch bytes with.
///
/// Two shapes that resolution cannot answer are handled here:
///
/// - A `:zip(member)` selector needs the archive's bytes, which the step-input
///   context has no handle for; the member comes from the store here, as the
///   command surface resolves it.
/// - A binding whose template raises is reported in
///   [`mediapm_conductor::InstanceKeyInputValues::unresolved`], so the caller
///   refuses the strict match instead of hashing an omission.
async fn build_instance_key_resolved_value_bytes(
    cas: &FileSystemCas,
    step_inputs: &BTreeMap<String, String>,
    merged_bindings: &BTreeMap<String, String>,
    expected_inputs: &ExpectedStepInputs,
    prior_step_outputs: &StepOutputHashes,
) -> Result<ResolvedKeyValueBytes, MediaPmError> {
    let mut engine_bindings = BTreeMap::new();
    let mut resolved = ResolvedKeyValueBytes::default();

    for (key, raw_binding) in merged_bindings {
        let zip_member =
            parse_step_output_reference(raw_binding).and_then(|reference| reference.zip_member);
        let Some(zip_member) = zip_member else {
            engine_bindings.insert(key.clone(), raw_binding.clone());
            continue;
        };
        let Some(resolved_hash) = expected_inputs.resolved_hashes.get(key) else {
            resolved.unresolved_hashes.insert(key.clone());
            continue;
        };
        let zip_bytes = cas.get(*resolved_hash).await.map_err(|source| {
            MediaPmError::Workflow(format!(
                "reading ZIP output '{resolved_hash}' for instance-key member '{}' failed: {source}",
                zip_member.escape_debug()
            ))
        })?;
        let member_bytes =
            extract_zip_member_bytes(zip_bytes.as_ref(), zip_member).map_err(|error| {
                MediaPmError::Workflow(format!(
                    "extracting ZIP member '{}' for instance-key material failed: {error}",
                    zip_member.escape_debug()
                ))
            })?;
        resolved.values.insert(key.clone(), member_bytes);
    }

    let engine = mediapm_conductor::resolve_instance_key_input_values::<FileSystemCas>(
        &engine_bindings,
        step_inputs,
        prior_step_outputs,
    )
    .await;
    resolved.values.extend(engine.values);
    resolved.unresolved_bindings.extend(engine.unresolved);
    Ok(resolved)
}

/// The argv one declared tool runs, empty for a builtin.
fn tool_command_parts(tool_spec: &mediapm_conductor::ToolSpec) -> &[String] {
    match &tool_spec.kind {
        mediapm_conductor::ToolKindSpec::Executable { command, .. } => command,
        mediapm_conductor::ToolKindSpec::Builtin { .. } => &[],
    }
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

/// Returns the clause that names the inputs which stopped the strict pass.
///
/// Empty when the strict pass derived the step's key and still matched no
/// instance: nothing then stands between the re-run and a different answer, so
/// the refusal has no second limit to state.
fn describe_unresolved_inputs(unresolved_inputs: &UnresolvedInputs) -> String {
    if unresolved_inputs.is_empty() {
        return String::new();
    }
    let described = unresolved_inputs
        .iter()
        .map(|(input_name, unresolved)| unresolved.describe(input_name))
        .collect::<Vec<_>>()
        .join(", ");
    let noun = if unresolved_inputs.len() == 1 { "input" } else { "inputs" };
    format!(
        " The strict pass could not derive this step's key: {noun} {described} did not \
         resolve. The re-run meets this refusal as well until that does."
    )
}

/// Returns the hashes a step's inputs name, and the inputs this traversal could
/// not resolve.
///
/// A binding that names a step output the traversal has not resolved stops the
/// whole derivation: the key would be composed without that term, and a key
/// missing a term is not this step's key. A binding whose bytes the store lacks
/// leaves the input recorded as unresolved, which keeps every instance out of the
/// strict match for the same reason.
async fn resolve_expected_input_hashes(
    cas: &FileSystemCas,
    generated_doc: &NickelDocument,
    step_inputs: &BTreeMap<String, String>,
    step_outputs: &StepOutputHashes,
) -> Result<ExpectedInputHashes, MediaPmError> {
    let mut expected = ExpectedStepInputs::default();
    let mut unresolved = UnresolvedInputs::new();

    for (input_name, value) in step_inputs {
        match resolve_input_binding_hash(cas, generated_doc, value, step_outputs).await? {
            InputBindingHashResolution::Resolved(hash) => {
                expected.resolved_hashes.insert(input_name.clone(), hash);
            }
            InputBindingHashResolution::MissingPriorStepOutput => {
                unresolved.insert(
                    input_name.clone(),
                    UnresolvedInput { cause: UnresolvedInputCause::PriorStepOutput, detail: None },
                );
                return Ok(ExpectedInputHashes { inputs: None, unresolved });
            }
            InputBindingHashResolution::MissingMaterializedObject { detail } => {
                expected.unresolved_input_names.insert(input_name.clone());
                unresolved.insert(
                    input_name.clone(),
                    UnresolvedInput {
                        cause: UnresolvedInputCause::MissingObject,
                        detail: Some(detail),
                    },
                );
            }
            InputBindingHashResolution::MissingMaterializedArchiveMember { detail } => {
                expected.unresolved_input_names.insert(input_name.clone());
                unresolved.insert(
                    input_name.clone(),
                    UnresolvedInput {
                        cause: UnresolvedInputCause::MissingArchiveMember,
                        detail: Some(detail),
                    },
                );
            }
        }
    }

    Ok(ExpectedInputHashes { inputs: Some(expected), unresolved })
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
            let zip_bytes = match cas.get(hash).await {
                Ok(bytes) => bytes,
                Err(source) => {
                    return Ok(InputBindingHashResolution::MissingMaterializedObject {
                        detail: format!("reading ZIP output '{hash}' failed: {source}"),
                    });
                }
            };
            let member_bytes = match extract_zip_member_bytes(zip_bytes.as_ref(), zip_member) {
                Ok(bytes) => bytes,
                Err(error) => {
                    return Ok(InputBindingHashResolution::MissingMaterializedArchiveMember {
                        detail: format!(
                            "extracting ZIP member '{}' failed: {error}",
                            zip_member.escape_debug()
                        ),
                    });
                }
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
/// Instance identity is content-derived only: the matcher recomposes the two
/// terms the step worker composed — the domain and the unified tools-catalog key
/// — followed by the key material the command names in argv order, which
/// [`materializer_key_material`] rebuilds from the same spec. A step's capture
/// declarations are not part of it, because two steps running the same work
/// share one instance however differently they name what they record. Finding
/// the names a step binds inside that instance is a separate question, answered
/// by name in [`instance_matches_expected_output_names`].
pub(super) fn instance_matches_expected_inputs(
    instance_key: &Hash,
    instance: &ToolCallInstance,
    expected_inputs: &ExpectedStepInputs,
    key_material: &[Hash],
) -> bool {
    // An input whose hash this traversal could not reconstruct would drop out of
    // the key material, and the key derived without it is not this step's key.
    if !expected_inputs.unresolved_input_names.is_empty() {
        return false;
    }
    mediapm_conductor::instance_matches_stored_key(*instance_key, instance, key_material)
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

    /// Returns whether `instance` records serving `step_id`, whatever workflow
    /// the ref names.
    ///
    /// The demo fixture holds one workflow, so a bare step id names one step
    /// there. This is fixture tooling: the materializer's own pairing, and an
    /// invalidation request, compare the workflow as well as the step id.
    fn records_step(instance: &ToolCallInstance, step_id: &str) -> bool {
        instance.step_ids.iter().any(|recorded| recorded.step_id == step_id)
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
            .insert(mediapm_conductor::StepRef {
                workflow_id: "mediapm.media.demo.local.dQw4w9WgXcQ".to_string(),
                step_id: DUPLICATED_IMPORT_STEP_ID.to_string(),
            });
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

    /// Two steps over one tool, whose command reads an input whose default
    /// carries a host conditional.
    ///
    /// The shape is the demo's: `js_runtimes` is
    /// `deno:${context.os == … ? … : …}`, so the worker resolves the path this
    /// host selects for the command it runs, while the key material hashes the
    /// binding's text. A host-specific path is where the payload lives, not what
    /// the step does.
    const TWO_STEP_DOCUMENT: &str = r#"{
  external_data = {},
  tools = {
    "probe@v1" = {
      command = [
        "${*inputs.js_runtimes ? --js-runtimes | ''}",
        "${inputs.js_runtimes}",
        "${inputs.output_path}",
      ],
      default_inputs = {
        js_runtimes = "deno:${context.os == \"linux\" ? deps/linux/deno : deps/other/deno}",
        output_path = "",
      },
      inputs = {
        js_runtimes = { kind = "string", required = false },
        output_path = { kind = "string", required = false },
      },
      kind = "executable",
      name = "probe",
      outputs = {
        primary = { capture = "stdout", name = "primary" },
      },
      runtime = { impure = false, max_concurrent_calls = 0, max_retries = 0 },
      success_codes = [ 0 ],
    },
  },
  version = 2,
  workflows = [
    {
      description = "two steps over one tool",
      impure = false,
      name = "wf",
      steps = [
        {
          id = "step-0",
          inputs = { output_path = "first.txt" },
          max_retries = 0,
          outputs = { primary = { capture = "stdout", name = "primary" } },
          tool = "probe",
        },
        {
          depends_on = [ "step-0" ],
          id = "step-1",
          inputs = { output_path = "second.txt" },
          max_retries = 0,
          outputs = { primary = { capture = "stdout", name = "primary" } },
          tool = "probe",
        },
      ],
    },
  ],
}"#;

    /// Records the state a worker run of [`TWO_STEP_DOCUMENT`] leaves behind.
    ///
    /// Every instance is keyed through the worker's own derivation: the value
    /// bytes come from the template engine, and the key material and key come
    /// from the same public functions the executor calls. The later step is
    /// stamped newer, so a resolver that cannot tell the two instances apart
    /// prefers it for both steps — which is what the demo's broken run did.
    async fn state_recorded_by_the_worker(
        cas: &FileSystemCas,
    ) -> (NickelDocument, mediapm_conductor::WorkflowSpec, ConductorState) {
        use mediapm_conductor::{
            derive_tool_call_instance_key, key_material_for_tool, resolve_instance_key_input_values,
        };

        let generated =
            decode_document(TWO_STEP_DOCUMENT.as_bytes()).expect("decode the two-step document");
        let workflow = generated.workflows.first().expect("the document's workflow").clone();
        let mut state = ConductorState::new_empty();
        let mut step_outputs = StepOutputHashes::new();

        for (index, step) in workflow.steps.iter().enumerate() {
            let (tool_call_id, tool_spec) =
                find_active_tool_spec(&generated, &step.tool).expect("the step's tool spec");
            let merged = merge_step_inputs_with_tool_defaults(&generated, &step.tool, &step.inputs)
                .expect("merge step inputs with the tool's defaults");
            let values = resolve_instance_key_input_values::<FileSystemCas>(
                &merged,
                &step.inputs,
                &step_outputs,
            )
            .await;
            assert!(
                values.unresolved.is_empty(),
                "step '{}' has bindings the worker could not resolve either: {:?}",
                step.id,
                values.unresolved,
            );
            let key_material =
                key_material_for_tool(tool_command_parts(tool_spec), &values.values, &merged);
            let instance_key = derive_tool_call_instance_key(tool_call_id, &key_material);

            let mut outputs = BTreeMap::new();
            for output_name in step.outputs.keys() {
                let bytes = format!("step '{}' output '{}'", step.id, output_name);
                let hash = cas.put(bytes::Bytes::from(bytes)).await.expect("store step output");
                outputs.insert(
                    output_name.clone(),
                    mediapm_conductor::CapturedOutput { hash, capture: None },
                );
            }
            let output_hashes = outputs
                .iter()
                .map(|(name, output)| (name.clone(), output.hash))
                .collect::<BTreeMap<_, _>>();
            step_outputs.insert(step.id.clone(), output_hashes);

            let executed_at =
                mediapm_utils::Timestamp::from_unix_secs(1_700_000_000 + index as u64);
            state.tool_call_instances.insert(
                instance_key,
                mediapm_conductor::ToolCallInstance {
                    tool_call_id: tool_call_id.clone(),
                    impure: false,
                    invalidated_at: None,
                    executed_at,
                    last_referenced_at: executed_at,
                    step_ids: BTreeSet::from([mediapm_conductor::StepRef {
                        workflow_id: workflow.name.clone(),
                        step_id: step.id.clone(),
                    }]),
                    command_args: key_material,
                    env_vars: BTreeMap::new(),
                    materialized_inputs: BTreeMap::new(),
                    outputs,
                },
            );
        }

        assert_eq!(
            state.tool_call_instances.len(),
            workflow.steps.len(),
            "each step must key distinct work, or this witness cannot tell them apart",
        );
        (generated, workflow, state)
    }

    /// A state recorded the way the step worker records it binds every step to
    /// its own instance.
    ///
    /// Both steps run one tool, so the store holds two instances that record the
    /// same output name; the later step's record is newer and has the same
    /// output count. Only the key separates them. A reader whose derivation
    /// drifts from the worker's finds neither by key, and this is the test that
    /// says so before the wrong instance's bytes reach a library path.
    #[tokio::test]
    async fn steps_keyed_the_way_the_worker_keys_them_bind_their_own_outputs() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let (generated, workflow, state) = state_recorded_by_the_worker(&cas).await;
        let recorded = state
            .tool_call_instances
            .values()
            .flat_map(|instance| {
                instance.step_ids.iter().map(move |served| {
                    (
                        served.step_id.clone(),
                        instance
                            .outputs
                            .iter()
                            .map(|(n, o)| (n.clone(), o.hash))
                            .collect::<BTreeMap<_, _>>(),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();

        let resolved = resolve_workflow_step_output_hashes(&cas, &generated, &state, &workflow)
            .await
            .expect("resolve workflow step outputs")
            .expect("the recorded instances supply every step");

        for step in &workflow.steps {
            let expected = recorded.get(&step.id).expect("recorded outputs for the step");
            assert_eq!(
                resolved.get(&step.id),
                Some(expected),
                "step '{}' resolved to another step's record",
                step.id,
            );
        }
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
                .into_expected_inputs()
                .expect("ffmpeg expected inputs");

        let (conductor_tool_key, _) =
            find_active_tool_spec(&generated, &ffmpeg_step.tool).expect("active ffmpeg tool");
        let resolved_values = build_instance_key_resolved_value_bytes(
            &cas,
            &ffmpeg_step.inputs,
            &merged_ffmpeg_inputs,
            &expected_inputs,
            &step_outputs,
        )
        .await
        .expect("build ffmpeg instance-key resolved values");
        let key_material = materializer_key_material(
            &generated,
            &ffmpeg_step.tool,
            &merged_ffmpeg_inputs,
            &resolved_values.values,
        )
        .expect("derive ffmpeg key material");
        let ffmpeg_instance = state
            .tool_call_instances
            .iter()
            .find(|(instance_key, instance)| {
                instance.tool_call_id == *conductor_tool_key
                    && instance_matches_expected_inputs(
                        instance_key,
                        instance,
                        &expected_inputs,
                        &key_material,
                    )
            })
            .expect("ffmpeg instance matching step inputs");
        assert!(ffmpeg_instance.1.outputs.contains_key("primary"));
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
            tool_call_id: tool_call_id.to_string(),
            impure: false,
            invalidated_at: None,
            executed_at: mediapm_utils::Timestamp::from_unix_nanos(0),
            last_referenced_at: mediapm_utils::Timestamp::from_unix_nanos(0),
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
    /// `output_hashes_from_instance` returns exactly the recorded names. Mapping
    /// a declared `stderr`, `stdout` or `process_code` name the instance did not
    /// record onto the recorded `stdout` hash serves bytes captured under a
    /// different name. The merged capture set records every bound name under its
    /// own name and spec, and the matcher refuses an instance missing one rather
    /// than aliasing it, so the function must return exactly the recorded names.
    #[test]
    fn a_declared_stream_name_is_not_aliased_to_another_name() {
        let tool_call_id = "echo@v1";
        let stdout_hash = Hash::from_content(b"stdout-bytes");
        let instance = ToolCallInstance {
            tool_call_id: tool_call_id.to_string(),
            impure: false,
            invalidated_at: None,
            executed_at: mediapm_utils::Timestamp::from_unix_nanos(0),
            last_referenced_at: mediapm_utils::Timestamp::from_unix_nanos(0),
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
    /// (`v6.1`) must never appear here: a change to key material is a new
    /// integer, not a patch of this one.
    ///
    /// Re-declared here instead of rendered from `INSTANCE_KEY_SCHEME` or
    /// imported from `instance_key_domain`: this oracle exists to pin the
    /// documented value, so a changed domain string must fail the oracle rather
    /// than move both sides together.
    const ORACLE_INSTANCE_KEY_DOMAIN: &str = "mediapm/conductor/tool call instances/v6";

    /// Returns whether a command part participates in the key, per the
    /// documented spec: a part reading `${env.` reports whatever the host holds,
    /// so it is not key material. Everything else is, selectors included.
    ///
    /// Deliberately a second implementation of the rule rather than a call to
    /// `references_environment`: a filter that moves with production cannot
    /// detect production being wrong about what the key covers.
    fn oracle_is_key_material(part: &str) -> bool {
        !part.contains("${env.")
    }

    /// Splits a command part into its selectors, as `(destination, source)`
    /// pairs, in order.
    ///
    /// A second implementation of the scan on purpose: the oracle has to read
    /// the parts itself, or it would only prove that production agrees with
    /// production.
    fn oracle_selectors(part: &str) -> Vec<(String, String)> {
        let mut selectors = Vec::new();
        let mut remaining = part;
        while !remaining.is_empty() {
            let next = [":file(", ":folder("]
                .iter()
                .filter_map(|marker| remaining.find(marker).map(|offset| (offset, *marker)))
                .min_by_key(|(offset, _)| *offset);
            let Some((offset, marker)) = next else { break };
            let after = &remaining[offset + marker.len()..];
            let Some(close) = after.find(')') else { break };
            let destination = after[..close].to_string();
            let source = remaining[..offset]
                .rsplit(|character: char| {
                    !(character.is_ascii_alphanumeric()
                        || matches!(character, '_' | '.' | '-' | ':'))
                })
                .next()
                .unwrap_or_default()
                .to_string();
            selectors.push((destination, source));
            remaining = &after[close + 1..];
        }
        selectors
    }

    /// Pins the oracle's restatement of the key-material rule.
    ///
    /// No command part on the demo path reads the environment, so without this
    /// a rule that always returned `true` would still pass the fixture test.
    /// A selector part is key material, so it must report `true` here: the
    /// selector's destination and source hash identify the bytes the step
    /// stages, which is work, even though the part's own text cannot be hashed.
    #[test]
    fn oracle_is_key_material_refuses_the_environment_and_keeps_selectors() {
        let cases = [
            ("${env.HOME}", false),
            ("--flag ${env.MY_VAR} --other", false),
            ("${inputs.payload:file(payload.txt)}", true),
            ("${inputs.folder:folder(dir)}", true),
            ("mp4", true),
            ("", true),
            ("${step_output.step-0-0-import.video_untagged}", true),
            ("my-folder-name", true),
        ];
        for (part, expected) in cases {
            assert_eq!(
                oracle_is_key_material(part),
                expected,
                "part {part:?} must be classified as key material={expected}",
            );
        }
    }

    /// Pins the oracle's selector scan against the shapes the document writes.
    ///
    /// A selector sits inside a conditional in the real presets, so the source
    /// has to be read from the identifier before the colon rather than from the
    /// start of the reference.
    #[test]
    fn oracle_selectors_reads_the_conditional_shapes() {
        assert_eq!(
            oracle_selectors("${inputs.source:file(inputs/source.mkv)}"),
            vec![("inputs/source.mkv".to_string(), "inputs.source".to_string())],
        );
        assert_eq!(
            oracle_selectors("${{*inputs.video ? inputs.video:file(downloads/video.mkv) | ''}}",),
            vec![("downloads/video.mkv".to_string(), "inputs.video".to_string())],
        );
        assert!(oracle_selectors("--plain").is_empty());
    }

    /// Returns whether a binding source names work rather than the environment
    /// the step runs in.
    ///
    /// The second implementation of production's `binding_names_work`, kept
    /// separate for the reason [`Self::oracle_is_key_material`] gives. The
    /// classification decides a *value term*, not composition, so an oracle
    /// that borrowed it would stop being able to catch production moving it.
    fn oracle_binding_names_work(raw_binding: &str) -> bool {
        raw_binding.contains("step_output.")
            || raw_binding.contains("external_data.")
            || raw_binding.contains("inputs.")
    }

    /// The bytes one input contributes, per the documented spec.
    ///
    /// Resolved bytes when the binding names work, the binding's own text when
    /// it names only the host or the environment, and resolved bytes when the
    /// input records no binding at all.
    fn oracle_value_term<'a>(raw_binding: Option<&'a String>, resolved: &'a [u8]) -> &'a [u8] {
        match raw_binding {
            Some(text) if !oracle_binding_names_work(text) => text.as_bytes(),
            _ => resolved,
        }
    }

    /// Pins the value rule the demo fixture cannot reach.
    ///
    /// No demo builtin binds an input to `${context.` or `${env.`, so the fixture
    /// comparison alone cannot tell the scheme-6 rule from the dropped-slot rule
    /// it replaced. Both shapes are named here directly: the context-only
    /// binding keeps its slot and hashes its own text, and the binding that
    /// names a step output hashes the bytes it resolves to.
    #[test]
    fn oracle_key_material_keeps_a_context_only_binding_and_hashes_its_text() {
        let command_parts: Vec<String> = Vec::new();
        let resolved_values = BTreeMap::from([
            ("runtime".to_string(), b"deno:deps/deno/macos/deno".to_vec()),
            ("source".to_string(), b"blake3:00".to_vec()),
        ]);
        let raw_bindings = BTreeMap::from([
            (
                "runtime".to_string(),
                "deno:${context.os == \"macos\" ? deps/deno/macos/deno : deps/deno/other/deno}"
                    .to_string(),
            ),
            ("source".to_string(), "${step_output.step-0-0-import.video_untagged}".to_string()),
        ]);

        let material = oracle_key_material(&command_parts, &resolved_values, &raw_bindings);

        let expected = vec![
            Hash::composite(&[
                Hash::from_content("runtime".as_bytes()),
                Hash::from_content(raw_bindings["runtime"].as_bytes()),
            ]),
            Hash::composite(&[
                Hash::from_content("source".as_bytes()),
                Hash::from_content(&resolved_values["source"]),
            ]),
        ];
        assert_eq!(
            material, expected,
            "a context-only binding keeps its slot and hashes its text; a work binding hashes its bytes",
        );
    }

    /// Recomposes one tool's key material from the documented spec.
    ///
    /// A tool with no command parts keys its inputs in name order, each as
    /// `composite(name bytes, value)`. Every other tool keys its command, in
    /// argv order: a plain part contributes `composite(part bytes, the value of
    /// every input it reads)`, a selector part contributes
    /// `composite(destination bytes, source hash)`, and a part reading
    /// `${env.` contributes nothing.
    ///
    /// Every value term is chosen by [`Self::oracle_value_term`], so a binding
    /// that names only `${context.…}` or `${env.…}` contributes its text while a
    /// binding that names an input or a step output contributes the bytes it
    /// resolves to. Both branches use it, including the builtin branch, which
    /// keeps every declared input's slot: a dropped slot would let two inputs
    /// collide under distinct names.
    ///
    /// The composition is written out here rather than delegated to
    /// `key_material_for_tool`: a test that calls the code under test cannot
    /// detect that code being wrong. Resolution — which bytes a binding names —
    /// does come from production, because resolution is not what this oracle
    /// checks.
    fn oracle_key_material(
        command_parts: &[String],
        resolved_values: &BTreeMap<String, Vec<u8>>,
        raw_bindings: &BTreeMap<String, String>,
    ) -> Vec<Hash> {
        if command_parts.is_empty() {
            let mut material = Vec::new();
            for (name, value) in resolved_values {
                let term = oracle_value_term(raw_bindings.get(name), value);
                material.push(Hash::composite(&[
                    Hash::from_content(name.as_bytes()),
                    Hash::from_content(term),
                ]));
            }
            return material;
        }

        let mut material = Vec::new();
        for part in command_parts {
            if !oracle_is_key_material(part) {
                continue;
            }
            let selectors = oracle_selectors(part);
            if selectors.is_empty() {
                let mut parts = vec![Hash::from_content(part.as_bytes())];
                for name in oracle_referenced_inputs(part) {
                    let resolved = resolved_values.get(&name).map_or(&[][..], Vec::as_slice);
                    let term = oracle_value_term(raw_bindings.get(&name), resolved);
                    parts.push(Hash::from_content(term));
                }
                material.push(Hash::composite(&parts));
                continue;
            }
            for (destination, source) in selectors {
                let value = source
                    .strip_prefix("inputs.")
                    .and_then(|name| resolved_values.get(name))
                    .map_or(&[][..], Vec::as_slice);
                let source_hash = std::str::from_utf8(value)
                    .ok()
                    .and_then(|text| text.parse::<Hash>().ok())
                    .unwrap_or_else(|| Hash::from_content(value));
                material.push(Hash::composite(&[
                    Hash::from_content(destination.as_bytes()),
                    source_hash,
                ]));
            }
        }
        material
    }

    /// Every input a part reads, in order, by the oracle's own scan.
    fn oracle_referenced_inputs(part: &str) -> Vec<String> {
        let mut names = Vec::new();
        let mut remaining = part;
        while let Some(offset) = remaining.find("inputs.") {
            let after = &remaining[offset + "inputs.".len()..];
            let end = after
                .find(|character: char| {
                    !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
                })
                .unwrap_or(after.len());
            if end > 0 {
                names.push(after[..end].to_string());
            }
            remaining = &after[end..];
        }
        names
    }

    /// Recomputes one recorded instance's key from the spec.
    ///
    /// Components, in order: the versioned domain separator, the unified
    /// tools-catalog key, and the key material the command names. Captures do
    /// not participate: they are records addressed by name, and two steps
    /// running the same work share one instance however differently they
    /// declare what they record. No time component exists either: the recorded
    /// `executed_at` is provenance, never key material, so a stored impure
    /// instance keeps matching across runs.
    ///
    /// Written out here rather than delegating to `derive_instance_key` (or its
    /// `derive_tool_call_instance_key` façade), for the reason given on
    /// [`Self::oracle_key_material`].
    fn oracle_instance_key(instance: &ToolCallInstance, key_material: &[Hash]) -> Hash {
        let mut parts = Vec::new();
        parts.push(Hash::from_content(ORACLE_INSTANCE_KEY_DOMAIN.as_bytes()));
        parts.push(Hash::from_content(instance.tool_call_id.as_bytes()));
        parts.extend_from_slice(key_material);
        Hash::composite(&parts)
    }

    /// Independently derives every demo workflow step's instance key and pairs it
    /// with the recorded instance that key owns.
    ///
    /// *Resolution* comes from the production helpers — which hash a binding
    /// points at, and which bytes a binding resolves to — because resolution is
    /// not what this oracle checks. *Composition* comes from
    /// [`Self::oracle_key_material`] and [`Self::oracle_instance_key`], which are
    /// this oracle's own implementation of the documented spec. The forbidden
    /// production calls are `key_material_for_tool`,
    /// `key_material_for_command_parts` and `derive_instance_key`; see
    /// [`Self::oracle_key_material`] for why.
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
            let (tool_call_id, tool_spec) = find_active_tool_spec(generated, &step.tool)
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
                    .into_expected_inputs()
            else {
                return Err(format!(
                    "step '{}': no recorded predecessor supplies its hash-backed inputs",
                    step.id,
                ));
            };
            let resolved_values = build_instance_key_resolved_value_bytes(
                cas,
                &step.inputs,
                &merged,
                &expected_inputs,
                &step_outputs,
            )
            .await
            .map_err(|error| {
                format!("step '{}': resolving instance-key value bytes failed: {error}", step.id)
            })?;
            let unresolved = resolved_values.unresolved_names();
            if !unresolved.is_empty() {
                return Err(format!(
                    "step '{}': bindings the worker resolves did not resolve here: {unresolved:?}",
                    step.id,
                ));
            }
            let key_material = oracle_key_material(
                tool_command_parts(tool_spec),
                &resolved_values.values,
                &merged,
            );

            let mut candidates = Vec::new();
            let mut diagnostics = Vec::new();
            for (stored_key, instance) in &state.tool_call_instances {
                if instance.tool_call_id != tool_call_id {
                    continue;
                }
                let derived = oracle_instance_key(instance, &key_material);
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
    /// way; see [`Self::oracle_key_material`]. It does use production
    /// *resolution* helpers — `find_active_tool_spec`,
    /// `merge_step_inputs_with_tool_defaults`, `resolve_expected_input_hashes`,
    /// `build_instance_key_resolved_value_bytes`, and
    /// `output_hashes_from_instance` —
    /// because resolution is not in question here.
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
        let stray = state.tool_call_instances[&import_key].clone();
        let stray_key = Hash::from_content(b"task-4-oracle-unclaimed-instance");
        assert_ne!(stray_key, import_key, "the stray record needs a key of its own");
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

    /// One recorded instance's rewrite: the key each claiming step derives, and
    /// the steps whose recorded provenance claims the instance.
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
    async fn demo_step_key_material(
        cas: &FileSystemCas,
        generated: &NickelDocument,
        step: &mediapm_conductor::WorkflowStepSpec,
        step_outputs: &StepOutputHashes,
    ) -> Result<Vec<Hash>, String> {
        let merged = merge_step_inputs_with_tool_defaults(generated, &step.tool, &step.inputs)
            .map_err(|error| format!("step '{}': merging step inputs failed: {error}", step.id))?;
        let Some(expected_inputs) =
            resolve_expected_input_hashes(cas, generated, &merged, step_outputs)
                .await
                .map_err(|error| {
                    format!("step '{}': resolving expected input hashes failed: {error}", step.id)
                })?
                .into_expected_inputs()
        else {
            return Err(format!(
                "step '{}' resolves no inputs; its recorded predecessors are missing",
                step.id,
            ));
        };
        let resolved_values = build_instance_key_resolved_value_bytes(
            cas,
            &step.inputs,
            &merged,
            &expected_inputs,
            step_outputs,
        )
        .await
        .map_err(|error| {
            format!("step '{}': resolving instance-key value bytes failed: {error}", step.id)
        })?;
        let unresolved = resolved_values.unresolved_names();
        if !unresolved.is_empty() {
            return Err(format!(
                "step '{}': bindings the worker resolves did not resolve here: {unresolved:?}",
                step.id,
            ));
        }
        materializer_key_material(generated, &step.tool, &merged, &resolved_values.values)
            .map_err(|error| format!("step '{}': composing key material failed: {error}", step.id))
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
            let key_material = demo_step_key_material(cas, generated, step, &step_outputs).await?;

            let paired = state
                .tool_call_instances
                .iter()
                .filter(|(_, instance)| records_step(instance, &step.id))
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
                &key_material,
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
                    records_step(instance, step_id),
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

    /// Asserts that the instance keys recorded in
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
    /// The rewrite replaces the recorded key in the fixture's JSON text — the
    /// `tool_call_instances` map key, and every `aux.live_instances` entry that
    /// names it — so every other byte of the file is preserved. Provenance is read, never written: `step_ids` is the
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
    /// `resolve_expected_input_hashes`)
    /// rather than the conductor's `resolve_step_inputs`, because that function is
    /// `pub(super)` inside `mediapm-conductor`'s `step_worker` module and is
    /// therefore unreachable from this crate. The pairing does not rest on this
    /// path — it comes from the fixture's own `step_ids` — so a resolution helper
    /// that drifted would produce new keys, not a mismatched pairing.
    ///
    /// **Write.** The decoded state is re-keyed in memory — the map key and every
    /// `aux.live_instances` seed that names it move together — and then
    /// re-encoded through `encode_state_json`, so the checked-in record always
    /// lands in the current wire shape. The bytes go to a sibling temporary file in the
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
                    records_step(instance, step_id),
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
    /// never touched. The replacement works on the decoded value: the key is the
    /// `tool_call_instances` map key, and it appears again in every
    /// `aux.live_instances` entry that names it, so both move together by
    /// construction rather than by a substring of the file text that happens to
    /// match. The result is re-encoded once through `encode_state_json` before it
    /// is written, so the fixture always lands in the current wire shape. The bytes
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
        let mut rewritten =
            decode_state_json(&original_bytes).expect("decode demo conductor state fixture");

        // Re-key the decoded value, never the file text: the key is the
        // `tool_call_instances` map key and it appears again in every
        // `aux.live_instances` seed that names it, so both move together here by
        // construction rather than by a substring that happens to match.
        let mut changed = 0usize;
        for (stored_key, rewrite) in rewrites {
            if rewrite.current_key == *stored_key {
                println!(
                    "steps {:?}: {} already current",
                    rewrite.claiming_steps,
                    stored_key.to_hex(),
                );
                continue;
            }
            let instance = rewritten.tool_call_instances.remove(stored_key).unwrap_or_else(|| {
                panic!("instance key {} is not in the fixture", stored_key.to_hex())
            });
            for seeds in rewritten.aux.live_instances.values_mut() {
                if seeds.remove(stored_key) {
                    seeds.insert(rewrite.current_key);
                }
            }
            assert!(
                rewritten.tool_call_instances.insert(rewrite.current_key, instance).is_none(),
                "instance key {} is already recorded",
                rewrite.current_key.to_hex(),
            );
            println!(
                "steps {:?}: {} -> {}",
                rewrite.claiming_steps,
                stored_key.to_hex(),
                rewrite.current_key.to_hex(),
            );
            changed += 1;
        }

        // Validate the regenerated value before it can replace a checked-in
        // record: no stale key may survive, and every instance must carry the
        // provenance the caller's pairing check relies on.
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
                    records_step(instance, step_id),
                    "rewritten fixture must name step '{step_id}' as a step the instance served",
                );
            }
        }
        assert_eq!(rewritten.tool_call_instances.len(), state.tool_call_instances.len());

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
        if changed == 0 {
            println!("normalized the fixture to the current wire shape");
        } else {
            println!("rewrote {changed} instance key(s)");
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

    /// The yt-dlp tool document the relaxed-resolution tests resolve against.
    fn relaxed_generated_doc() -> NickelDocument {
        use mediapm_conductor::{ToolKindSpec, ToolSpec};

        NickelDocument {
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
        }
    }

    /// The one-step infojson workflow those tests resolve.
    fn infojson_workflow() -> mediapm_conductor::WorkflowSpec {
        use mediapm_conductor::{OutputCaptureSpec, SaveMode, WorkflowSpec, WorkflowStepSpec};

        WorkflowSpec {
            name: "mediapm.media.test".to_string(),
            display_name: None,
            description: None,
            impure: true,
            steps: vec![WorkflowStepSpec {
                id: INFOJSON_STEP_ID.to_string(),
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
        }
    }

    /// One yt-dlp instance recording the infojson output under `hash`.
    ///
    /// `recorded` is the ref the record carries: `Some` names this step, `None`
    /// leaves the record unnamed, which is the shape a record has when another
    /// step of the same tool wrote it.
    fn infojson_instance(
        hash: Hash,
        executed_nanos: u64,
        recorded: Option<mediapm_conductor::StepRef>,
    ) -> mediapm_conductor::ToolCallInstance {
        use mediapm_conductor::{CapturedOutput, ToolCallInstance};
        use mediapm_utils::Timestamp;

        ToolCallInstance {
            tool_call_id: "yt-dlp".to_string(),
            impure: true,
            invalidated_at: None,
            executed_at: Timestamp::from_unix_nanos(executed_nanos),
            last_referenced_at: Timestamp::from_unix_nanos(executed_nanos),
            step_ids: recorded.into_iter().collect(),
            command_args: Vec::new(),
            env_vars: BTreeMap::new(),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::from([
                ("primary".to_string(), CapturedOutput { hash, capture: None }),
                ("yt_dlp_infojson_file".to_string(), CapturedOutput { hash, capture: None }),
            ]),
        }
    }

    /// The ref that names this workflow's infojson step.
    fn infojson_step_ref(workflow_id: &str) -> mediapm_conductor::StepRef {
        mediapm_conductor::StepRef {
            workflow_id: workflow_id.to_string(),
            step_id: INFOJSON_STEP_ID.to_string(),
        }
    }

    /// The id of the step the relaxed-resolution tests resolve.
    const INFOJSON_STEP_ID: &str = "step-0-0-yt-dlp-infojson-to-infojson";

    /// Records `payload` in `cas` and returns the hash it was stored under.
    ///
    /// A store that refuses the write is the test's setup failing, so the panic
    /// names the payload rather than the store's error alone.
    async fn put_payload(cas: &FileSystemCas, payload: &'static [u8], what: &str) -> Hash {
        cas.put(bytes::Bytes::from_static(payload))
            .await
            .unwrap_or_else(|error| panic!("put {what}: {error}"))
    }

    /// Two records that name the step and disagree about a required output.
    ///
    /// The relaxed pass recovers outputs for a step whose strict key matches no
    /// recorded instance. Two records that both name the step and record
    /// different bytes for a required output cannot both be that step's work, and
    /// the key that would settle it did not match either, so the pass reports the
    /// state instead of picking one. The message names the workflow, because
    /// step ids repeat across media and this pass runs for every media in the
    /// document.
    #[tokio::test]
    async fn relaxed_resolution_refuses_instances_that_disagree_about_a_required_output() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let primary_hash =
            cas.put(bytes::Bytes::from_static(b"primary-infojson")).await.expect("put primary");
        let sidecar_hash =
            cas.put(bytes::Bytes::from_static(b"sidecar-infojson")).await.expect("put sidecar");

        let primary_key = Hash::from_content(b"primary-instance");
        let sidecar_key = Hash::from_content(b"sidecar-instance");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            primary_key,
            infojson_instance(
                primary_hash,
                2_000_000_000,
                Some(infojson_step_ref("mediapm.media.test")),
            ),
        );
        state.tool_call_instances.insert(
            sidecar_key,
            infojson_instance(
                sidecar_hash,
                1_000_000_000,
                Some(infojson_step_ref("mediapm.media.test")),
            ),
        );

        let workflow = infojson_workflow();
        let error =
            resolve_workflow_step_output_hashes(&cas, &relaxed_generated_doc(), &state, &workflow)
                .await
                .expect_err(
                    "two records that disagree about a required output must not be chosen between",
                );

        let message = error.to_string();
        assert!(
            message.contains(&workflow.name),
            "the refusal names the workflow a reader has to act on: {message}"
        );
        assert!(message.contains(INFOJSON_STEP_ID), "the refusal names the step: {message}");
        assert!(
            message.contains(&primary_key.to_string())
                && message.contains(&sidecar_key.to_string()),
            "the refusal names both candidates: {message}"
        );
        assert!(
            message.contains("media invalidate"),
            "the refusal names the remedy a user can run: {message}"
        );
        assert!(
            message.contains("yt-dlp"),
            "the refusal names the tool whose records disagree: {message}"
        );
        for candidate in [
            format!("{primary_key} (2 output(s), executed_at 2 unix seconds)"),
            format!("{sidecar_key} (2 output(s), executed_at 1 unix seconds)"),
        ] {
            assert!(
                message.contains(&candidate),
                "the refusal names a candidate's key, its output count and when it ran: \
                 {candidate} missing from {message}"
            );
        }
    }

    /// Two records that name the step and agree on every required output.
    ///
    /// Agreement is not a choice between records: whatever produced them recorded
    /// the same bytes for the surface this step binds, so the step's answer is
    /// settled. Refusing here would fail a state that holds no ambiguity.
    #[tokio::test]
    async fn relaxed_resolution_accepts_instances_that_agree_on_every_required_output() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let hash = cas.put(bytes::Bytes::from_static(b"shared-infojson")).await.expect("put info");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            Hash::from_content(b"first-instance"),
            infojson_instance(hash, 2, Some(infojson_step_ref("mediapm.media.test"))),
        );
        state.tool_call_instances.insert(
            Hash::from_content(b"second-instance"),
            infojson_instance(hash, 1, Some(infojson_step_ref("mediapm.media.test"))),
        );

        let workflow = infojson_workflow();
        let step_outputs =
            resolve_workflow_step_output_hashes(&cas, &relaxed_generated_doc(), &state, &workflow)
                .await
                .expect("agreement between candidates is not an ambiguity")
                .expect("the agreeing records answer for the step");

        assert_eq!(
            step_outputs
                .get(INFOJSON_STEP_ID)
                .and_then(|outputs| outputs.get("yt_dlp_infojson_file"))
                .copied(),
            Some(hash),
        );
    }

    /// A record of the same tool that never served this step is not a candidate.
    ///
    /// Candidacy follows the record's own provenance. Two yt-dlp records with
    /// the same output name are ordinary — one download produced the sidecar
    /// file and another produced the primary — and only the record that served
    /// the step can answer for it.
    #[tokio::test]
    async fn relaxed_resolution_ignores_a_record_that_never_served_the_step() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let named_hash =
            cas.put(bytes::Bytes::from_static(b"named-infojson")).await.expect("put named");
        let other_hash =
            cas.put(bytes::Bytes::from_static(b"other-infojson")).await.expect("put other");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            Hash::from_content(b"named-instance"),
            infojson_instance(named_hash, 1, Some(infojson_step_ref("mediapm.media.test"))),
        );
        state
            .tool_call_instances
            .insert(Hash::from_content(b"other-instance"), infojson_instance(other_hash, 2, None));

        let workflow = infojson_workflow();
        let step_outputs =
            resolve_workflow_step_output_hashes(&cas, &relaxed_generated_doc(), &state, &workflow)
                .await
                .expect("a record that never served the step is not a candidate")
                .expect("the record that names the step answers for it");

        assert_eq!(
            step_outputs
                .get(INFOJSON_STEP_ID)
                .and_then(|outputs| outputs.get("yt_dlp_infojson_file"))
                .copied(),
            Some(named_hash),
            "the record that names the step answers, not the newer record of the same tool",
        );
    }

    /// A ref that names no workflow names no workflow, so the record answers
    /// nothing.
    ///
    /// A ref written before refs carried a workflow id reads with an empty one.
    /// Both halves of a ref decide whether a record served a step, so an empty
    /// workflow id matches no workflow name. The record stops answering through
    /// the relaxed pass, the step is a miss, and the next run executes it.
    #[tokio::test]
    async fn relaxed_resolution_ignores_a_ref_that_names_no_workflow() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let hash = cas.put(bytes::Bytes::from_static(b"legacy-infojson")).await.expect("put info");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            Hash::from_content(b"legacy-instance"),
            infojson_instance(hash, 1, Some(infojson_step_ref(""))),
        );
        // The tool has to resolve for the pass to run at all, and the record
        // still answers nothing because its ref names no workflow.
        let workflow = infojson_workflow();
        let step_outputs =
            resolve_workflow_step_output_hashes(&cas, &relaxed_generated_doc(), &state, &workflow)
                .await
                .expect("a ref that names no workflow is not an error");

        assert!(
            step_outputs.is_none(),
            "a ref whose workflow id is empty names no workflow, so it answers for no step: \
             {step_outputs:?}"
        );
    }

    /// The refusal says why the strict pass could not settle the step, and names
    /// the input that stopped it.
    ///
    /// A step whose input names a step output this traversal never resolved
    /// derives no key, so the strict pass cannot answer it. The relaxed pass then
    /// refuses the disagreement it found, and a reader who follows the advice and
    /// re-runs lands in the same state. The message says that and names the
    /// input.
    #[tokio::test]
    async fn relaxed_resolution_names_the_input_the_strict_pass_could_not_resolve() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let primary_hash =
            cas.put(bytes::Bytes::from_static(b"primary-infojson")).await.expect("put primary");
        let sidecar_hash =
            cas.put(bytes::Bytes::from_static(b"sidecar-infojson")).await.expect("put sidecar");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            Hash::from_content(b"primary-instance"),
            infojson_instance(
                primary_hash,
                2_000_000_000,
                Some(infojson_step_ref("mediapm.media.test")),
            ),
        );
        state.tool_call_instances.insert(
            Hash::from_content(b"sidecar-instance"),
            infojson_instance(
                sidecar_hash,
                1_000_000_000,
                Some(infojson_step_ref("mediapm.media.test")),
            ),
        );

        let mut workflow = infojson_workflow();
        workflow.steps[0]
            .inputs
            .insert("url".to_string(), "${step_output.step-0-0-import.video_untagged}".to_string());

        let error =
            resolve_workflow_step_output_hashes(&cas, &relaxed_generated_doc(), &state, &workflow)
                .await
                .expect_err("two records that disagree must not be chosen between");

        let message = error.to_string();
        assert!(
            message.contains("The strict pass could not derive this step's key"),
            "the refusal says the key never matched, rather than implying it did: {message}"
        );
        assert!(
            message.contains("'url'")
                && message.contains("step output this traversal did not resolve"),
            "the refusal names the input that stopped the strict pass, and why: {message}"
        );
        assert!(
            !message.contains("cannot mark"),
            "the refusal no longer warns about a record the matcher cannot admit: {message}"
        );
        assert!(
            message.contains("no single instance answers for it"),
            "the refusal says the step has no single answering instance: {message}"
        );
        assert!(
            !message.contains("matched no instance by key"),
            "the refusal does not claim a key matched nothing when no key was derived: {message}"
        );
    }

    /// The refusal's second limit names every input that stopped the strict pass,
    /// with the reason each failed, and stays silent when nothing did.
    ///
    /// A key that was never derived is the one case where the advice to re-run
    /// lands in the same refusal, so the clause has to be exact about which
    /// inputs stand in the way.
    #[test]
    fn describe_unresolved_inputs_names_each_input_and_its_reason() {
        assert_eq!(
            describe_unresolved_inputs(&UnresolvedInputs::new()),
            "",
            "a strict pass that derived the key has no second limit to state"
        );

        let one = UnresolvedInputs::from([(
            "url".to_string(),
            UnresolvedInput { cause: UnresolvedInputCause::PriorStepOutput, detail: None },
        )]);
        let described = describe_unresolved_inputs(&one);
        assert!(
            described.contains("could not derive this step's key"),
            "the clause says the key was never derived: {described}"
        );
        assert!(
            described.contains("input 'url'")
                && described.contains(UnresolvedInputCause::PriorStepOutput.phrase()),
            "one input is named as an input, with the reason it failed: {described}"
        );

        let two = UnresolvedInputs::from([
            (
                "archive".to_string(),
                UnresolvedInput {
                    cause: UnresolvedInputCause::MissingArchiveMember,
                    detail: Some(
                        "extracting ZIP member 'payload.txt' failed: no such member".to_string(),
                    ),
                },
            ),
            (
                "url".to_string(),
                UnresolvedInput { cause: UnresolvedInputCause::PriorStepOutput, detail: None },
            ),
        ]);
        let described = describe_unresolved_inputs(&two);
        for expected in [
            "inputs 'archive' (names an archive member the stored bytes do not contain: \
             extracting ZIP member 'payload.txt' failed: no such member)",
            "'url' (names a step output this traversal did not resolve)",
        ] {
            assert!(
                described.contains(expected),
                "{expected} missing from the clause: {described}"
            );
        }
    }

    /// Every cause says something different about what a reader should look at.
    #[test]
    fn each_unresolved_cause_has_its_own_phrase() {
        let phrases = [
            UnresolvedInputCause::PriorStepOutput,
            UnresolvedInputCause::MissingObject,
            UnresolvedInputCause::MissingArchiveMember,
            UnresolvedInputCause::UnresolvedBinding,
        ]
        .map(|cause| cause.phrase());
        let distinct: BTreeSet<&str> = phrases.iter().copied().collect();
        assert_eq!(distinct.len(), phrases.len(), "two causes share a phrase: {phrases:?}");
    }

    /// The two ways an archive-backed input fails are told apart.
    ///
    /// A binding that selects a ZIP member fails when the store no longer holds
    /// the archive and when the archive it holds has no such member. The refusal
    /// tells a reader what to look for, so the two cannot share a phrase: the
    /// first sends them to the store, and the second names bytes the store holds
    /// and cannot read.
    #[tokio::test]
    async fn an_archive_input_names_whether_the_object_or_the_member_is_missing() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let not_an_archive =
            put_payload(&cas, b"these bytes are not a zip archive", "not an archive").await;
        let never_stored = Hash::from_content(b"an archive nothing stored");

        let step_inputs = BTreeMap::from([(
            "source".to_string(),
            "${step_output.pack.pack:zip(payload.txt)}".to_string(),
        )]);
        let with_pack = |hash: Hash| {
            BTreeMap::from([("pack".to_string(), BTreeMap::from([("pack".to_string(), hash)]))])
        };

        let unreadable = resolve_expected_input_hashes(
            &cas,
            &relaxed_generated_doc(),
            &step_inputs,
            &with_pack(not_an_archive),
        )
        .await
        .expect("an unreadable archive is an unresolved input, not an error");
        let absent = resolve_expected_input_hashes(
            &cas,
            &relaxed_generated_doc(),
            &step_inputs,
            &with_pack(never_stored),
        )
        .await
        .expect("an archive the store never held is an unresolved input, not an error");

        let unreadable = unreadable
            .unresolved
            .get("source")
            .expect("the archive input is unresolved")
            .describe("source");
        let absent = absent
            .unresolved
            .get("source")
            .expect("the absent input is unresolved")
            .describe("source");

        assert!(
            absent.contains("store could not read"),
            "an archive the store lost is reported as a read the store could not do: {absent}"
        );
        assert!(
            !unreadable.contains("could not read"),
            "bytes the store holds are not reported as missing from it: {unreadable}"
        );
        assert_ne!(
            absent, unreadable,
            "the two ways an archive input fails send a reader to different places"
        );
    }

    /// Removes the store's read-only mark from a committed blob so a test can
    /// replace the bytes behind the name.
    ///
    /// The store marks every committed object read-only, and Windows refuses to
    /// unlink a file that carries the attribute. POSIX governs removal by the
    /// directory's write permission, so nothing there needs changing.
    #[cfg(windows)]
    fn make_blob_replaceable(path: &Path) {
        let mut permissions = std::fs::metadata(path).expect("blob metadata").permissions();
        permissions.set_readonly(false);
        std::fs::set_permissions(path, permissions).expect("clear the read-only attribute");
    }

    /// Nothing to do off Windows, where `remove_file` needs no mode change.
    #[cfg(not(windows))]
    fn make_blob_replaceable(_path: &Path) {}

    /// An object the store holds and cannot read is not reported as absent.
    ///
    /// The phrase a cause contributes has to be true of every way that cause
    /// arises. This one is built from any failed store read, and a store asked to
    /// verify an object whose bytes no longer hash to their name fails that read
    /// with `corrupt object`. Saying the store does not hold such an object sends
    /// a reader looking for a missing file beside the one that is there.
    #[tokio::test]
    async fn an_object_the_store_holds_and_cannot_read_is_not_reported_as_absent() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open_with_strategies(
            cas_root.path(),
            vec![mediapm_cas::VerifyTriggerStrategy::Always],
        )
        .await
        .expect("open a verifying cas");
        // Larger than the WAL's inline limit, so the store keeps the payload in
        // the blob store and a read has to verify it. A smaller object stays
        // inline in the log, and a read answers from there without touching the
        // bytes this test replaced.
        let archive = cas
            .put(bytes::Bytes::from(vec![0x41_u8; 1_200_000]))
            .await
            .expect("put a payload the store keeps out of the log");
        let path = cas.object_path_for_hash(archive).expect("a file store names the object's path");
        assert!(path.is_file(), "the payload is stored as a blob: {}", path.display());
        make_blob_replaceable(&path);
        std::fs::remove_file(&path).expect("remove the committed blob");
        std::fs::write(&path, b"bytes the store never hashed")
            .expect("write bytes under that name");

        let step_inputs = BTreeMap::from([(
            "source".to_string(),
            "${step_output.pack.pack:zip(payload.txt)}".to_string(),
        )]);
        let pack =
            BTreeMap::from([("pack".to_string(), BTreeMap::from([("pack".to_string(), archive)]))]);

        let unresolved =
            resolve_expected_input_hashes(&cas, &relaxed_generated_doc(), &step_inputs, &pack)
                .await
                .expect("an unreadable object is an unresolved input, not an error");
        let described = unresolved
            .unresolved
            .get("source")
            .expect("the archive input is unresolved")
            .describe("source");

        assert!(
            described.contains("corrupt"),
            "the store's own words say which read failed: {described}"
        );
        assert!(
            described.contains(UnresolvedInputCause::MissingObject.phrase()),
            "the cause names a read the store could not do: {described}"
        );
        assert!(
            !described.contains("does not hold"),
            "bytes the store holds are not reported as missing from it: {described}"
        );
    }

    /// A member name cannot split the line that reports it.
    ///
    /// A ZIP member selector is document text, like a media id, so a name
    /// carrying a newline must not reach the refusal unescaped: one input's
    /// failure would otherwise read as two lines, the second of which looks like
    /// a message of its own.
    #[tokio::test]
    async fn a_member_name_cannot_split_the_line_that_reports_it() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let not_an_archive =
            put_payload(&cas, b"these bytes are not a zip archive", "not an archive").await;

        let step_inputs = BTreeMap::from([(
            "source".to_string(),
            "${step_output.pack.pack:zip(payload\ntxt)}".to_string(),
        )]);
        let pack = BTreeMap::from([(
            "pack".to_string(),
            BTreeMap::from([("pack".to_string(), not_an_archive)]),
        )]);

        let unresolved =
            resolve_expected_input_hashes(&cas, &relaxed_generated_doc(), &step_inputs, &pack)
                .await
                .expect("an unreadable archive is an unresolved input, not an error");
        let described = unresolved
            .unresolved
            .get("source")
            .expect("the archive input is unresolved")
            .describe("source");

        assert!(
            !described.contains('\n'),
            "the member name cannot break the line it is reported on: {described}"
        );
        assert!(
            described.contains(r"payload\ntxt"),
            "the escaped name still says which member was selected: {described}"
        );
    }

    /// A binding failure does not replace the cause an input already carries.
    ///
    /// The two failures reach different sets by construction, and the merge keeps
    /// the recorded cause anyway. Overwriting would replace the store's answer
    /// with one that names the document, which sends a reader to the wrong file.
    #[test]
    fn a_binding_failure_does_not_replace_the_cause_an_input_carries() {
        let mut unresolved = UnresolvedInputs::from([(
            "source".to_string(),
            UnresolvedInput {
                cause: UnresolvedInputCause::MissingObject,
                detail: Some("reading ZIP output 'h' failed: corrupt object h".to_string()),
            },
        )]);
        record_unresolved_bindings(&mut unresolved, &BTreeSet::from(["source".to_string()]));

        let kept = unresolved.get("source").expect("the input stays unresolved");
        assert_eq!(
            kept.cause.phrase(),
            UnresolvedInputCause::MissingObject.phrase(),
            "the store's answer survives a binding failure for the same input"
        );
        assert_eq!(
            kept.detail.as_deref(),
            Some("reading ZIP output 'h' failed: corrupt object h"),
            "the failing read's own words survive as well"
        );
    }

    /// A binding whose template does not resolve is not a lost object.
    ///
    /// The template engine reports the binding name and not the failure, so this
    /// pass can say only that the binding did not resolve. Recording it as a
    /// missing object would send a reader to the store for a fault in the
    /// document, and the store still holds every byte the step names.
    #[tokio::test]
    async fn a_binding_that_does_not_resolve_is_not_recorded_as_a_lost_object() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let bindings = BTreeMap::from([("url".to_string(), "${inputs.missing}".to_string())]);

        let resolved = build_instance_key_resolved_value_bytes(
            &cas,
            &bindings,
            &bindings,
            &ExpectedStepInputs::default(),
            &StepOutputHashes::new(),
        )
        .await
        .expect("a binding that does not resolve is reported, not raised");

        assert!(
            resolved.unresolved_bindings.contains("url"),
            "the binding, not a store read, is what failed: {:?}",
            resolved.unresolved_names()
        );
        assert!(
            resolved.values.is_empty(),
            "a binding that does not resolve contributes no bytes: {:?}",
            resolved.values
        );
    }

    /// A record that served two workflows answers both, and no third.
    ///
    /// The record's refs are its provenance: it served each step it names. A
    /// request for either workflow is answered by the bytes it recorded, and a
    /// request for a workflow it never served is a miss rather than a refusal.
    #[tokio::test]
    async fn relaxed_resolution_answers_for_a_record_that_served_two_workflows() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let hash = cas.put(bytes::Bytes::from_static(b"shared-work-infojson")).await.expect("put");

        let mut instance =
            infojson_instance(hash, 1, Some(infojson_step_ref("mediapm.media.first")));
        instance.step_ids.insert(infojson_step_ref("mediapm.media.second"));

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(Hash::from_content(b"two-workflow-instance"), instance);

        let named = |name: &str| {
            let mut workflow = infojson_workflow();
            workflow.name = name.to_string();
            workflow
        };

        for served in ["mediapm.media.first", "mediapm.media.second"] {
            let step_outputs = resolve_workflow_step_output_hashes(
                &cas,
                &relaxed_generated_doc(),
                &state,
                &named(served),
            )
            .await
            .expect("a workflow the record served is not an error")
            .expect("the record served this workflow, so it answers for it");

            assert_eq!(
                step_outputs
                    .get(INFOJSON_STEP_ID)
                    .and_then(|outputs| outputs.get("yt_dlp_infojson_file"))
                    .copied(),
                Some(hash),
                "the record answers for {served}, which it served",
            );
        }

        let step_outputs = resolve_workflow_step_output_hashes(
            &cas,
            &relaxed_generated_doc(),
            &state,
            &named("mediapm.media.third"),
        )
        .await
        .expect("a workflow the record never served is not an error");

        assert!(
            step_outputs.is_none(),
            "the record served two workflows and no third: {step_outputs:?}"
        );
    }

    /// A record that served this step id in another workflow is not a candidate.
    ///
    /// Step ids repeat across media, so the id alone does not name a step. A
    /// record whose ref pairs this id with another workflow holds that
    /// workflow's work, and counting it as a candidate would either answer for a
    /// step that never ran or turn a miss into a refusal.
    #[tokio::test]
    async fn relaxed_resolution_ignores_a_record_from_another_workflow() {
        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let other_hash =
            cas.put(bytes::Bytes::from_static(b"other-workflow-infojson")).await.expect("put");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            Hash::from_content(b"other-workflow-instance"),
            infojson_instance(other_hash, 1, Some(infojson_step_ref("mediapm.media.other"))),
        );

        let workflow = infojson_workflow();
        let step_outputs =
            resolve_workflow_step_output_hashes(&cas, &relaxed_generated_doc(), &state, &workflow)
                .await
                .expect("a record of another workflow is not an error");

        assert!(
            step_outputs.is_none(),
            "a record that served this step id in another workflow answers for nothing here: \
             {step_outputs:?}"
        );
    }

    /// A step declaring two outputs refuses when the candidates agree on one and
    /// differ on the other.
    ///
    /// Agreement has to hold for every required name. A comparison satisfied by
    /// one matching name accepts a record whose other bytes belong to different
    /// work, which is the wrong-content binding this pass exists to refuse.
    #[tokio::test]
    async fn relaxed_resolution_refuses_candidates_that_disagree_about_one_of_two_outputs() {
        use mediapm_conductor::{CapturedOutput, OutputCaptureSpec, SaveMode, ToolCallInstance};
        use mediapm_utils::Timestamp;

        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        let shared_hash =
            cas.put(bytes::Bytes::from_static(b"shared-infojson")).await.expect("put shared");
        let first_description = cas
            .put(bytes::Bytes::from_static(b"first-description"))
            .await
            .expect("put first description");
        let second_description = cas
            .put(bytes::Bytes::from_static(b"second-description"))
            .await
            .expect("put second description");

        let two_output_instance = |infojson: Hash, description: Hash, executed: u64| {
            let mut outputs = BTreeMap::new();
            outputs.insert(
                "yt_dlp_infojson_file".to_string(),
                CapturedOutput { hash: infojson, capture: None },
            );
            outputs.insert(
                "yt_dlp_description_file".to_string(),
                CapturedOutput { hash: description, capture: None },
            );
            ToolCallInstance {
                tool_call_id: "yt-dlp".to_string(),
                impure: true,
                invalidated_at: None,
                executed_at: Timestamp::from_unix_nanos(executed),
                last_referenced_at: Timestamp::from_unix_nanos(executed),
                step_ids: [infojson_step_ref("mediapm.media.test")].into_iter().collect(),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs,
            }
        };

        let first_key = Hash::from_content(b"first-two-output-instance");
        let second_key = Hash::from_content(b"second-two-output-instance");

        let mut state = ConductorState::new_empty();
        state
            .tool_call_instances
            .insert(first_key, two_output_instance(shared_hash, first_description, 2_000_000_000));
        state.tool_call_instances.insert(
            second_key,
            two_output_instance(shared_hash, second_description, 1_000_000_000),
        );

        let mut workflow = infojson_workflow();
        let capture = |name: &str| OutputCaptureSpec {
            name: name.to_string(),
            capture: format!("file:{name}"),
            save: SaveMode::True,
            allow_empty: false,
            include_topmost_folder: true,
        };
        let step = workflow.steps.first_mut().expect("the fixture declares one step");
        step.outputs
            .insert("yt_dlp_description_file".to_string(), capture("yt_dlp_description_file"));
        step.outputs.insert("yt_dlp_infojson_file".to_string(), capture("yt_dlp_infojson_file"));

        let error =
            resolve_workflow_step_output_hashes(&cas, &relaxed_generated_doc(), &state, &workflow)
                .await
                .expect_err("agreement on one output does not settle the other required output");

        let message = error.to_string();
        assert!(message.contains(INFOJSON_STEP_ID), "the refusal names the step: {message}");
        assert!(
            message.contains(&first_key.to_string()) && message.contains(&second_key.to_string()),
            "the refusal names both candidates, whose only disagreement is the second output: \
             {message}"
        );
    }

    /// A lone record whose object the store no longer holds still answers, and
    /// the read path reports the missing object.
    ///
    /// The record says the step ran and where its bytes went. Dropping it for
    /// want of those bytes would report a step that produced nothing, which is
    /// the miss a caller treats as a workflow that has not run; the run instead
    /// has to fail on the object the store lost.
    #[tokio::test]
    async fn a_lone_candidate_whose_object_is_gone_reports_the_missing_object() {
        use crate::config::{
            MediaPmDocument, MediaSourceSpec, MediaStep, MediaStepTool, OutputVariantValue,
            TransformInputValue, YtDlpOutputKind, YtDlpOutputVariantConfig,
        };
        use crate::materializer::metadata::MaterializationLookupContext;
        use crate::tools::workflows::{managed_workflow_name, resolve_ffmpeg_slot_limits};

        let cas_root = mediapm_utils::temp::artifact_dir().expect("temp cas dir");
        let cas = FileSystemCas::open(cas_root.path()).await.expect("open cas");
        // Never stored: the record names bytes the store does not hold.
        let lost_hash = Hash::from_content(b"infojson-the-store-lost");

        let media_id = "youtube.dQw4w9WgXcQ";
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            Hash::from_content(b"lone-instance"),
            infojson_instance(
                lost_hash,
                1,
                Some(infojson_step_ref(&managed_workflow_name(media_id))),
            ),
        );

        let mut generated_doc = relaxed_generated_doc();
        let mut workflow = infojson_workflow();
        workflow.name = managed_workflow_name(media_id);
        generated_doc.workflows.push(workflow);

        let mut document = MediaPmDocument::default();
        document.media.insert(
            media_id.to_string(),
            MediaSourceSpec {
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "infojson".to_string(),
                        OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                            kind: YtDlpOutputKind::Infojson,
                            ..Default::default()
                        }),
                    )]),
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

        let lookup = MaterializationLookupContext::new(
            cas.clone(),
            Some(state),
            generated_doc,
            resolve_ffmpeg_slot_limits(&document),
        );
        let source = document.media.get(media_id).expect("source");
        let Err(failure) =
            resolve_variant_source_bytes(&lookup, media_id, source, "infojson", true).await
        else {
            panic!("a recorded object the store lost is the run's failure");
        };

        match failure {
            VariantResolveFailure::Fatal(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("store does not hold"),
                    "the failure names the lost object rather than a step that produced nothing: \
                     {message}"
                );
                assert!(message.contains(&lost_hash.to_string()), "{message}");
            }
            VariantResolveFailure::NotProduced(error) => panic!(
                "a lone record whose object is gone must not read as a step that produced nothing: {error}"
            ),
        }
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
                tool_call_id: "yt-dlp".to_string(),
                impure: true,
                invalidated_at: None,
                executed_at: Timestamp::from_unix_nanos(1),
                last_referenced_at: Timestamp::from_unix_nanos(1),
                step_ids: BTreeSet::from([mediapm_conductor::StepRef {
                    workflow_id: managed_workflow_name("youtube.dQw4w9WgXcQ").clone(),
                    step_id: "step-0-0-yt-dlp-infojson-to-infojson".to_string(),
                }]),
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
                tool_call_id: "yt-dlp".to_string(),
                impure: true,
                invalidated_at: None,
                executed_at: Timestamp::from_unix_nanos(2),
                last_referenced_at: Timestamp::from_unix_nanos(2),
                step_ids: BTreeSet::from([mediapm_conductor::StepRef {
                    workflow_id: "mediapm.media.youtube.dQw4w9WgXcQ".to_string(),
                    step_id: "step-0-2-yt-dlp-infojson-to-infojson".to_string(),
                }]),
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
                tool_call_id: versioned_tool_id.clone(),
                impure: true,
                invalidated_at: None,
                executed_at: Timestamp::from_unix_nanos(1),
                last_referenced_at: Timestamp::from_unix_nanos(1),
                step_ids: BTreeSet::from([mediapm_conductor::StepRef {
                    workflow_id: "mediapm.media.youtube.dQw4w9WgXcQ".to_string(),
                    step_id: "step-0-2-yt-dlp-infojson-to-infojson".to_string(),
                }]),
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
    /// The defect is not the extraction failing, which is correct. It is what a
    /// discarded `Err` does: control falls out of the block into the raw-bytes
    /// fallback, and the caller receives the *pack* as the variant's content.
    /// The assertion therefore distinguishes an error from a wrong success,
    /// which is why it checks the error and not only its message.
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
        let pack_hash = put_payload(&cas, b"not a zip archive", "the not-a-zip payload").await;
        let mut state = load_conductor_state();
        // The fixture records more than one ffmpeg instance, and each of them
        // writes the output the binding names, so every one of them records the
        // pack; leaving one on its original hash would leave the lookup reading
        // bytes nothing holds.
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
        // A step's key material names the bytes it read, so replacing an output
        // leaves every later step that reads it with a key describing work it did
        // not do. Re-deriving keeps this state resolvable by key instead of
        // leaning on fallback selection, which refuses to choose between two
        // records that could both answer.
        let workflow = load_demo_workflow(&generated);
        let rewrites = plan_demo_instance_key_rewrites(&cas, &generated, &state, &workflow)
            .await
            .expect("the mutated state must pair one recorded instance per step");
        let mut instances = std::mem::take(&mut state.tool_call_instances);
        let mut rekeyed = BTreeMap::new();
        for (stored_key, rewrite) in rewrites {
            let instance = instances
                .remove(&stored_key)
                .expect("the planner pairs only instances the state records");
            rekeyed.insert(rewrite.current_key, instance);
        }
        assert!(
            instances.is_empty(),
            "every recorded instance must be re-keyed, or a stale key survives to be resolved by \
             fallback"
        );
        state.tool_call_instances = rekeyed;
        assert!(
            cas.get(pack_hash).await.is_ok(),
            "the fixture's recorded output hash must be readable, or the arm bails before it \
             extracts anything and this test reaches nothing"
        );

        let input_hash = put_payload(&cas, b"pinned input", "the pinned input").await;
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
            // The pack was found and read, so the failure is the extraction
            // itself rather than a variant nothing produced.
            Err(VariantResolveFailure::Fatal(error)) => error,
            Err(VariantResolveFailure::NotProduced(error)) => panic!(
                "the pack and its member are both in the fixture, so nothing here is unproduced: \
                 {error}"
            ),
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
