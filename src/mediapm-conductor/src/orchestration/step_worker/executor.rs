//! Core step execution orchestration.
//!
//! [`execute_step`] runs a step through all phases: input resolution, cache
//! probe, sandbox materialization, execution, and output capture. Persistence
//! is not a phase here: each captured entry records the capture spec that
//! produced it, and that spec carries the persistence decision.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use mediapm_cas::{CasApi, ConstraintApi, Hash};
use mediapm_utils::Timestamp;

use crate::config::OutputCaptureSpec;
use crate::error::ConductorError;
use crate::instance_key::{INSTANCE_KEY_VERSION, input_binding_to_string};
use crate::orchestration::protocol::{
    StepExecutionBundle, StepExecutionRequest, UnifiedToolSpec, find_tool_entry_by_name,
};
use crate::state::{ResolvedInput, StepRef, ToolCallInstance};
use crate::work_material::{
    StepWorkMaterial, merge_output_specs, output_tokens, producible_output_specs,
    warn_discarded_step_fields,
};

use super::cache::{CurrentMaterial, LookupResult, lookup_instance};
use super::capture::capture_outputs;
use super::inputs::resolve_step_inputs;
use super::process::{ExecutionResult, run_builtin, run_executable_process};
use super::sandbox::{
    create_sandbox, pre_qualify_payload_arguments, prepare_tool_payload,
    resolve_executable_under_payload_root,
};
use super::template::{ResolvedCommandParts, StagedInputs, TemplateContext, resolve_command_parts};

/// One dispatched execution: what the tool produced, the argv it ran, and the
/// inputs its command staged on the way.
struct DispatchedExecution {
    /// What the capture phase reads: streams, exit status, written files.
    result: ExecutionResult,
    /// Key material in order, one hash per key-material part (a builtin's
    /// inputs by name; a command's parts in argv order).
    ///
    /// Hashes alone, and exactly the list the instance key composes, so the
    /// recorded argv and the key material cannot drift apart.
    command_args: Vec<Hash>,
    /// Inputs the command's selectors staged, keyed by sandbox-relative
    /// destination (empty for a builtin, which has no command parts).
    materialized_inputs: BTreeMap<String, Hash>,
}

/// Dispatches execution: runs a builtin or resolves and runs an executable
/// process.
async fn dispatch_tool_execution<C: CasApi + ConstraintApi + Send + Sync>(
    cas: &C,
    tool_spec: &UnifiedToolSpec,
    request: &StepExecutionRequest,
    resolved_inputs: &[ResolvedInput],
    sandbox_dir: &Path,
    key_material: &[Hash],
) -> Result<DispatchedExecution, ConductorError> {
    if tool_spec.command_parts.is_empty() {
        let args: BTreeMap<String, String> =
            resolved_inputs.iter().map(|ri| (ri.key.clone(), ri.value.clone())).collect();
        let builtin_id = tool_spec.builtin_id.as_deref().unwrap_or(&request.step.tool);
        let result = run_builtin(
            cas,
            builtin_id,
            &args,
            &request.outermost_config_dir,
            sandbox_dir,
            &request.host_builtins,
        )
        .await?;
        Ok(DispatchedExecution {
            result,
            command_args: key_material.to_vec(),
            materialized_inputs: BTreeMap::new(),
        })
    } else {
        let resolved_inputs_map: BTreeMap<String, String> =
            resolved_inputs.iter().map(|ri| (ri.key.clone(), ri.value.clone())).collect();
        let cmd_ctx = TemplateContext::<C> {
            cas: Some(cas),
            step_outputs: &request.step_outputs,
            env_vars: &tool_spec.execution_env_vars,
            tokens: &BTreeMap::new(),
            sandbox_dir: Some(sandbox_dir),
            host_os: std::env::consts::OS,
            inputs: &resolved_inputs_map,
            staged_inputs: None,
        };
        let ResolvedCommandParts { parts, staged_inputs } =
            resolve_command_parts(&tool_spec.command_parts, &cmd_ctx).await?;
        let mut resolved_parts = parts;
        // `in_place` executes the tool where the provisioner put it, so the
        // executable the document names relative to the sandbox is resolved
        // against the payload root before anything runs, and so is every
        // companion path an argument names.
        resolve_executable_under_payload_root(&mut resolved_parts, tool_spec)?;
        pre_qualify_payload_arguments(&mut resolved_parts, tool_spec);
        let result = run_executable_process(
            &resolved_parts,
            &tool_spec.success_codes,
            sandbox_dir,
            &tool_spec.execution_env_vars,
        )
        .await?;
        Ok(DispatchedExecution {
            result,
            command_args: key_material.to_vec(),
            materialized_inputs: staged_input_records(staged_inputs),
        })
    }
}

/// Turns the staged destinations into the map the instance carries.
///
/// The hash is the content the selector staged, which no sandbox path or host
/// environment can move. What varies between runs is the destination's absolute
/// path, and that is not what the record holds.
fn staged_input_records(staged_inputs: StagedInputs) -> BTreeMap<String, Hash> {
    staged_inputs.into_iter().collect()
}

/// Derived material for a resolved step, shared by cache lookup and execution.
struct PreparedStep<'a> {
    /// The logical tool-call identifier used for instance key derivation.
    ///
    /// This is the step's tool name, not the versioned catalog map key, so a
    /// tool payload update does not change instance identity.
    tool_call_id: String,
    /// The resolved tool specification.
    tool_spec: &'a UnifiedToolSpec,
    /// Command key material in argv order.
    key_material: Vec<Hash>,
    /// Names this execution can produce, each with its producing spec.
    producible_outputs: BTreeMap<String, OutputCaptureSpec>,
    /// Merged tool and step output declarations.
    merged_outputs: BTreeMap<String, OutputCaptureSpec>,
    /// Current execution material used for cache lookup.
    current: CurrentMaterial,
    /// Latest-version instance key for this step execution.
    k_latest: Hash,
    /// Timestamp recorded for impure provenance and fresh executions.
    executed_at: Timestamp,
    /// Environment variables for the dispatched tool execution.
    env_vars: BTreeMap<String, crate::state::EnvVarEntry>,
    /// Probe requirements filtered to outputs this step can actually produce.
    required_output_specs: BTreeMap<String, OutputCaptureSpec>,
}

/// Computes the key material and lookup context for a resolved step.
fn prepare_step_execution<'a>(
    request: &'a StepExecutionRequest,
    resolved_inputs: &[ResolvedInput],
) -> Result<PreparedStep<'a>, ConductorError> {
    // Resolve the tool spec by name, but key instance identity on the step's
    // logical tool name rather than the versioned catalog map key. The catalog
    // key for a managed tool includes its content hash, so using it would make
    // a payload update look like a different tool and invalidate finished work.
    // The step's tool reference is the stable identity: upgrading the tool
    // binary changes no step's instance key.
    let tool_spec = find_tool_entry_by_name(&request.unified.tools, &request.step.tool)
        .map(|(_, spec)| spec)
        .ok_or_else(|| {
            ConductorError::Workflow(format!(
                "step '{}' references unknown tool '{}'",
                request.step.id, request.step.tool,
            ))
        })?;
    let tool_call_id = request.step.tool.clone();

    // Key material (argv/inputs) and current material for v8 lookup/derivation.
    let mut raw_bindings = request.step.inputs.clone();
    for (key, binding) in &tool_spec.default_inputs {
        raw_bindings.entry(key.clone()).or_insert_with(|| input_binding_to_string(binding));
    }
    let input_values: BTreeMap<String, Vec<u8>> = resolved_inputs
        .iter()
        .map(|input| (input.key.clone(), input.value.as_bytes().to_vec()))
        .collect();

    // One declaration of what this step would run, shared with mediapm's
    // materializer, so the key both derive cannot drift.
    warn_discarded_step_fields(&tool_spec.outputs, &request.step.outputs);
    let material = StepWorkMaterial::for_step(
        &tool_spec.command_parts,
        &tool_spec.literal_env_vars,
        &tool_spec.outputs,
        &request.step.outputs,
        &raw_bindings,
        &input_values,
    );
    let k_latest = material.key_at(&tool_call_id, INSTANCE_KEY_VERSION).ok_or_else(|| {
        ConductorError::Internal(format!(
            "failed to derive v{INSTANCE_KEY_VERSION} instance key for tool '{tool_call_id}'"
        ))
    })?;
    let key_material = material.command_args.clone();
    let current = CurrentMaterial {
        command_args: material.command_args,
        output_tokens: material.output_tokens,
        materialized_inputs: material.materialized_inputs,
    };

    // Tool-level capture defaults merged with the step's own declarations, for
    // the capture and probe paths.
    let merged_outputs = merge_output_specs(&tool_spec.outputs, &request.step.outputs);
    let producible_outputs = producible_output_specs(&merged_outputs);

    let executed_at = request.impure_timestamp.unwrap_or_else(Timestamp::now);
    let env_vars = build_env_vars(&tool_spec.execution_env_vars, &tool_spec.literal_env_vars);

    let required_output_specs: BTreeMap<String, OutputCaptureSpec> = request
        .required_output_names
        .iter()
        .filter_map(|name| producible_outputs.get(name).map(|spec| (name.clone(), spec.clone())))
        .collect();

    Ok(PreparedStep {
        tool_call_id,
        tool_spec,
        key_material,
        producible_outputs,
        merged_outputs,
        current,
        k_latest,
        executed_at,
        env_vars,
        required_output_specs,
    })
}

/// Outcome of consulting the instance cache for a prepared step.
enum CacheDecision {
    /// A reusable cached instance is ready to settle.
    Reuse(Box<StepExecutionBundle>),
    /// The step must execute, carrying lookup-maintenance side effects.
    Execute {
        /// Side-effect mutations to instances other than the settled one.
        lookup_side_effects: BTreeMap<Hash, ToolCallInstance>,
        /// A maintenance re-key to preserve when a hit fell back to execution.
        maintenance_rekey: Option<(Hash, Hash)>,
    },
}

/// Attempts to satisfy a step from the instance cache.
async fn try_cache_hit<C: CasApi + ConstraintApi + Send + Sync>(
    cas: &C,
    request: &StepExecutionRequest,
    prepared: &PreparedStep<'_>,
) -> Result<CacheDecision, ConductorError> {
    let decision = lookup_instance(
        &request.workflow_id,
        &request.step.id,
        &prepared.tool_call_id,
        &prepared.current,
        &request.state_snapshot,
        &prepared.required_output_specs,
    );
    match decision {
        LookupResult::Hit { instance, instance_key, rekeyed_from } => {
            // A hit requires the recorded blobs to still be present.
            if required_output_blobs_exist(cas, &instance, &prepared.required_output_specs).await {
                // Record the settling step (idempotent union at settlement).
                let mut instance = *instance;
                instance.step_ids_nk.insert(StepRef {
                    workflow_id: request.workflow_id.clone(),
                    step_id: request.step.id.clone(),
                });
                return Ok(CacheDecision::Reuse(Box::new(StepExecutionBundle {
                    instance,
                    instance_key,
                    rekeyed_from,
                    lookup_side_effects: BTreeMap::new(),
                    maintenance_rekey: None,
                    cache_hit: true,
                    producible_outputs: BTreeMap::new(),
                })));
            }

            // Missing blobs turn the hit into a miss, but lookup/migration
            // maintenance is independent of reuse and must not be discarded.
            // Persist the migrated/attempted state so subsequent runs do not
            // repeat migration work unnecessarily, then execute fresh.
            Ok(CacheDecision::Execute {
                maintenance_rekey: rekeyed_from.map(|old_key| (old_key, instance_key)),
                lookup_side_effects: BTreeMap::from([(instance_key, *instance)]),
            })
        }
        LookupResult::Miss { side_effects } => Ok(CacheDecision::Execute {
            lookup_side_effects: side_effects,
            maintenance_rekey: None,
        }),
    }
}

/// Executes a prepared step on a cache miss or forced rerun.
async fn execute_prepared_step<C: CasApi + ConstraintApi + Send + Sync>(
    cas: &C,
    request: &StepExecutionRequest,
    prepared: &PreparedStep<'_>,
    resolved_inputs: &[ResolvedInput],
    lookup_side_effects: BTreeMap<Hash, ToolCallInstance>,
    maintenance_rekey: Option<(Hash, Hash)>,
) -> Result<StepExecutionBundle, ConductorError> {
    execute_miss(
        cas,
        request,
        &prepared.tool_call_id,
        prepared.tool_spec,
        resolved_inputs,
        &prepared.key_material,
        &prepared.producible_outputs,
        &prepared.merged_outputs,
        prepared.k_latest,
        prepared.executed_at,
        prepared.env_vars.clone(),
        lookup_side_effects,
        maintenance_rekey,
    )
    .await
}

/// Executes one step: resolves inputs, runs the tool, captures outputs.
pub(super) async fn execute_step<C: CasApi + ConstraintApi + Send + Sync>(
    cas: &C,
    request: StepExecutionRequest,
) -> Result<StepExecutionBundle, ConductorError> {
    let resolved_inputs = resolve_step_inputs::<C>(&request).await?;
    let prepared = prepare_step_execution(&request, &resolved_inputs)?;

    if !request.force_rerun {
        match try_cache_hit(cas, &request, &prepared).await? {
            CacheDecision::Reuse(bundle) => return Ok(*bundle),
            CacheDecision::Execute { lookup_side_effects, maintenance_rekey } => {
                return execute_prepared_step(
                    cas,
                    &request,
                    &prepared,
                    &resolved_inputs,
                    lookup_side_effects,
                    maintenance_rekey,
                )
                .await;
            }
        }
    }

    execute_prepared_step(cas, &request, &prepared, &resolved_inputs, BTreeMap::new(), None).await
}

/// Executes a step on cache miss (or forced rerun/blob-miss fallback).
#[allow(clippy::too_many_arguments)]
async fn execute_miss<C: CasApi + ConstraintApi + Send + Sync>(
    cas: &C,
    request: &StepExecutionRequest,
    tool_call_id: &str,
    tool_spec: &UnifiedToolSpec,
    resolved_inputs: &[ResolvedInput],
    key_material: &[Hash],
    producible_outputs: &BTreeMap<String, OutputCaptureSpec>,
    merged_outputs: &BTreeMap<String, OutputCaptureSpec>,
    k_latest: Hash,
    executed_at: Timestamp,
    env_vars: BTreeMap<String, crate::state::EnvVarEntry>,
    lookup_side_effects: BTreeMap<Hash, ToolCallInstance>,
    maintenance_rekey: Option<(Hash, Hash)>,
) -> Result<StepExecutionBundle, ConductorError> {
    // Sandbox is named by the predicted LATEST key for determinism.
    let sandbox_dir = create_sandbox(&request.conductor_tmp_dir, &k_latest).await?;
    prepare_tool_payload(cas, tool_spec, &sandbox_dir).await?;

    let dispatched = dispatch_tool_execution(
        cas,
        tool_spec,
        request,
        resolved_inputs,
        &sandbox_dir,
        key_material,
    )
    .await?;

    let captured = capture_outputs(cas, merged_outputs, &dispatched.result, &sandbox_dir).await?;

    // Derive the stored v8 key from actual material (actual staged inputs).
    let actual_output_tokens = output_tokens(producible_outputs);
    let actual_materialized_inputs = dispatched.materialized_inputs.clone();
    let actual_key = crate::instance_key::derive_instance_key_for_version(
        INSTANCE_KEY_VERSION,
        tool_call_id,
        &dispatched.command_args,
        Some(&crate::instance_key::V8KeyMaterial {
            output_tokens: &actual_output_tokens,
            materialized_inputs: &actual_materialized_inputs,
        }),
    )
    .unwrap_or(k_latest);

    let instance = ToolCallInstance {
        key_version: INSTANCE_KEY_VERSION,
        attempted_key_version_nk: INSTANCE_KEY_VERSION,
        tool_call_id: tool_call_id.to_string(),
        command_args: dispatched.command_args,
        env_vars_ck: env_vars,
        materialized_inputs: actual_materialized_inputs,
        outputs: captured.records,
        impure_nk: tool_spec.is_impure,
        invalidated_at_nk: None,
        executed_at_nk: executed_at,
        last_referenced_at_nk: executed_at,
        step_ids_nk: BTreeSet::from([StepRef {
            workflow_id: request.workflow_id.clone(),
            step_id: request.step.id.clone(),
        }]),
    };

    Ok(StepExecutionBundle {
        instance,
        instance_key: actual_key,
        rekeyed_from: None,
        lookup_side_effects,
        maintenance_rekey,
        cache_hit: false,
        producible_outputs: producible_outputs.clone(),
    })
}

/// Returns whether every required output still resolves in the store.
///
/// `required` is the probe's own filtered map — the names this step binds that
/// the merged capture set can actually produce — so every name iterated here has
/// a recorded entry by the time the probe has answered. A raw requirement may
/// name an output no instance of this work can hold, and a presence check over
/// that set could never pass: it would force a re-execution on every run of a
/// workflow whose resolution is going to fail regardless.
///
/// Only the required names are checked. An output nothing downstream reads
/// cannot break this run, and stat-ing every recorded output would turn a
/// cheap probe into a sweep of the step's whole output set; an empty required
/// set therefore checks nothing, by design rather than by omission.
///
/// This answers presence, not integrity: a corrupt blob that still exists is a
/// verification concern for the read path, not a cache-probe one. Any `stat`
/// failure counts as absent — a store that cannot answer has not proven the
/// bytes are there, and re-executing is the safe response either way.
async fn required_output_blobs_exist<C: CasApi>(
    cas: &C,
    instance: &ToolCallInstance,
    required: &BTreeMap<String, OutputCaptureSpec>,
) -> bool {
    for name in required.keys() {
        let Some(record) = instance.outputs.get(name) else {
            return false;
        };
        let Some(hash) = record.hash_nk else {
            // An allowed-empty capture that matched nothing has no blob to verify.
            continue;
        };
        if cas.stat(hash).await.is_err() {
            return false;
        }
    }
    true
}

/// Builds the recorded environment set from configured execution env vars:
/// only non-empty values, each content-hashed (never stored literally).
///
/// Each entry is tagged by provenance: a name the document fixed as a
/// non-empty literal (`literal_env_vars`) records [`crate::state::EnvVarSource::Literal`];
/// any other configured execution var records [`crate::state::EnvVarSource::Inherited`].
/// New executions always store `Some(...)`. Recorded, never key material: the
/// instance-key derivation takes no env term, so a changed host/inherited value
/// re-runs nothing implicitly while a changed literal still moves the key
/// through the tool's literal-env key material.
fn build_env_vars(
    execution_env_vars: &BTreeMap<String, String>,
    literal_env_vars: &BTreeMap<String, String>,
) -> BTreeMap<String, crate::state::EnvVarEntry> {
    use crate::state::{EnvVarEntry, EnvVarSource};

    execution_env_vars
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(name, value)| {
            let source_nk = if literal_env_vars.contains_key(name) {
                Some(EnvVarSource::Literal)
            } else {
                Some(EnvVarSource::Inherited)
            };
            (name.clone(), EnvVarEntry { hash_ck: Hash::from_content(value.as_bytes()), source_nk })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use mediapm_cas::Hash;

    use super::{build_env_vars, execute_step};
    use crate::config::{OutputCaptureSpec, SaveMode, WorkflowStepSpec};
    use crate::orchestration::protocol::{
        StepExecutionBundle, StepExecutionRequest, UnifiedNickelDocument, UnifiedToolSpec,
    };
    use crate::state::{ConductorState, StepRef};
    use crate::work_material::{
        discarded_step_fields, merge_output_specs, producible_output_specs,
    };

    /// Builds a capture spec for `name` reading the implicit stream `stream`,
    /// with every optional field at its documented default.
    fn stream_spec(name: &str, stream: &str) -> OutputCaptureSpec {
        OutputCaptureSpec {
            name: name.to_string(),
            capture: crate::config::CaptureKind::parse_token(stream).expect("valid capture"),
            ..OutputCaptureSpec::default()
        }
    }

    /// A step that restates the tool's own declaration discards nothing.
    ///
    /// The comparison is by value, not by presence: a step repeating the
    /// selector the tool already declares is declaring what already holds, and
    /// warning about it would train readers to ignore the diagnostic.
    #[test]
    fn a_step_repeating_the_tool_declaration_discards_nothing() {
        let tool = stream_spec("content", "stdout");
        let step = stream_spec("content", "stdout");

        assert_eq!(
            discarded_step_fields(&tool, &step).len(),
            0,
            "a step restating the tool's declaration discards no field"
        );
    }

    /// Each field the tool overrides is named, in a fixed order.
    ///
    /// A step's selector is dropped for a tool-declared name, and a declaration
    /// that silently lost it would look as though it still applied. The step's
    /// `allow_empty` is taken from the step, so it is not discarded. Folder
    /// layout is part of the capture token.
    #[test]
    fn every_overridden_step_field_is_named_in_order() {
        let tool = stream_spec("content", "stdout");
        let step = OutputCaptureSpec {
            name: "content".to_string(),
            capture: crate::config::CaptureKind::parse_token("file:out.bin")
                .expect("valid capture"),
            save: SaveMode::False,
            allow_empty: true,
        };

        assert_eq!(discarded_step_fields(&tool, &step), vec!["capture"]);
    }

    /// A `save`-only step declaration discards nothing.
    ///
    /// Persistence is exactly what a step owns for a tool-declared name, so the
    /// merge must accept it without a diagnostic.
    #[test]
    fn a_save_only_step_declaration_discards_nothing() {
        let tool = stream_spec("content", "stdout");
        let step = OutputCaptureSpec { save: SaveMode::Full, ..tool.clone() };

        assert_eq!(
            discarded_step_fields(&tool, &step).len(),
            0,
            "a save-only step declaration discards no field"
        );
    }

    /// Runs one builtin `echo` step whose tool declares `tool_defaults` as its
    /// capture defaults and whose step declaration is `step_outputs`.
    ///
    /// `required` is the set of output names a probe hit must satisfy. The
    /// tool spec is filled directly because the step worker's own merge is what
    /// this module tests; the document bridge that fills it in production is
    /// covered by `config::documents` tests.
    async fn execute_echo_step(
        cas: &mediapm_cas::InMemoryCas,
        state: ConductorState,
        step_id: &str,
        tool_defaults: BTreeMap<String, OutputCaptureSpec>,
        step_outputs: BTreeMap<String, OutputCaptureSpec>,
        required: BTreeSet<String>,
    ) -> StepExecutionBundle {
        let tmp = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let tool_id = "echo@v1";
        let tool = UnifiedToolSpec {
            name: "echo".to_string(),
            version: None,
            is_impure: false,
            max_concurrent_calls: None,
            max_retries: 0,
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            command_parts: Vec::new(),
            success_codes: Vec::new(),
            execution_env_vars: BTreeMap::new(),
            literal_env_vars: BTreeMap::new(),
            outputs: tool_defaults,
            tool_content_map: BTreeMap::new(),
            payload_root: String::new(),
            payload_staging: crate::config::PayloadStaging::Copy,
            builtin_id: Some(tool_id.to_string()),
        };
        let request = StepExecutionRequest {
            unified: Arc::new(UnifiedNickelDocument {
                tools: BTreeMap::from([(tool_id.to_string(), tool)]),
                workflows: BTreeMap::new(),
                tool_content_hashes: BTreeSet::new(),
                external_data_policies: BTreeMap::new(),
                runtime: crate::config::ConductorRuntimeConfig::default(),
            }),
            workflow_id: "test-workflow".to_string(),
            step: WorkflowStepSpec {
                id: step_id.to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::from([("text".to_string(), "hi".to_string())]),
                outputs: step_outputs,
                runtime: Some(crate::config::StepRuntime { max_retries: Some(0) }),
                depends_on: Vec::new(),
            },
            impure_timestamp: None,
            state_snapshot: Arc::new(state),
            outermost_config_dir: tmp.path().to_path_buf(),
            conductor_tmp_dir: tmp.path().join("runtime"),
            host_builtins: Arc::new(crate::tools::HostBuiltins::default()),
            step_outputs: Arc::new(BTreeMap::new()),
            required_output_names: required,
            force_rerun: false,
        };
        execute_step(cas, request).await.expect("the echo builtin runs")
    }

    /// The declared output specification is key material in v8.
    ///
    /// Two steps running the same tool with the same inputs but different
    /// declared/producible output specs produce different instance keys.
    #[tokio::test]
    async fn declared_output_spec_is_key_material_in_v8() {
        let cas = mediapm_cas::new_in_memory_cas();
        let tool_defaults =
            BTreeMap::from([("result".to_string(), stream_spec("result", "stdout"))]);
        let first = execute_echo_step(
            &cas,
            ConductorState::new_empty(),
            "step-a-echo",
            tool_defaults.clone(),
            BTreeMap::new(),
            BTreeSet::from(["result".to_string()]),
        )
        .await;
        assert!(!first.cache_hit, "the first run of this work has no instance to reuse");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(first.instance_key, first.instance.clone());
        state.step_instances.insert(
            "test-workflow".to_string(),
            BTreeMap::from([("step-a-echo".to_string(), first.instance_key)]),
        );

        let second = execute_echo_step(
            &cas,
            state,
            "step-b-echo",
            tool_defaults,
            BTreeMap::from([("scratch".to_string(), stream_spec("scratch", "stderr"))]),
            BTreeSet::from(["result".to_string()]),
        )
        .await;

        assert!(
            !second.cache_hit,
            "different declared output specs must not reuse the same instance in v8",
        );
        assert_ne!(
            second.instance_key, first.instance_key,
            "a step's declared output spec must move the instance key in v8",
        );
    }

    /// The capture phase records the tool's default captures beside the step's
    /// own declarations.
    ///
    /// The tool owns the capture identity of a name it declares and the step
    /// contributes only `save` for that name; a step-only name is kept whole
    /// (see [`merge_output_specs`]).
    ///
    /// This is what makes one instance hold the names every step with that work
    /// can bind, which is what replaces the capture term the key used to carry.
    #[tokio::test]
    async fn a_tool_default_capture_is_recorded_beside_the_step_capture() {
        let cas = mediapm_cas::new_in_memory_cas();
        let bundle = execute_echo_step(
            &cas,
            ConductorState::new_empty(),
            "step-0-echo",
            BTreeMap::from([("result".to_string(), stream_spec("result", "stdout"))]),
            BTreeMap::from([("override".to_string(), stream_spec("override", "stderr"))]),
            BTreeSet::new(),
        )
        .await;

        let names = bundle.instance.outputs.keys().cloned().collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "override".to_string(),
                "process_code".to_string(),
                "result".to_string(),
                "stderr".to_string(),
                "stdout".to_string(),
            ],
            "the merged declarations are recorded beside the implicit stream captures",
        );
        assert_eq!(
            bundle.instance.outputs["result"].capture, "stdout",
            "the tool's default must be recorded under the spec that produced it",
        );
        assert_eq!(
            bundle.instance.outputs["override"].capture, "stderr",
            "the step's declaration must be the spec the name is recorded under",
        );
        assert_eq!(
            merge_output_specs(
                &BTreeMap::from([("result".to_string(), stream_spec("result", "stdout"))]),
                &BTreeMap::new(),
            )
            .len(),
            1,
            "the merged set the capture phase records is the tool's defaults plus the step's",
        );
    }

    /// A miss hands settlement the names it declared to produce, and a hit hands
    /// it nothing.
    ///
    /// The declared map is what lets settlement drop a stored entry this
    /// execution produced nothing under (`file:` captures record nothing on a
    /// zero match). A hit produced nothing at all — it returns the stored
    /// instance — so an empty map is the statement that it claims nothing and
    /// has nothing to drop, not a field left unfilled.
    #[tokio::test]
    async fn a_miss_declares_its_producible_names_and_a_hit_declares_none() {
        let cas = mediapm_cas::new_in_memory_cas();
        let tool_defaults =
            BTreeMap::from([("result".to_string(), stream_spec("result", "stdout"))]);
        let first = execute_echo_step(
            &cas,
            ConductorState::new_empty(),
            "step-a-echo",
            tool_defaults.clone(),
            BTreeMap::new(),
            BTreeSet::from(["result".to_string()]),
        )
        .await;
        assert!(!first.cache_hit, "the first run of this work has no instance to reuse");
        assert_eq!(
            first.producible_outputs.keys().cloned().collect::<Vec<_>>(),
            vec![
                "process_code".to_string(),
                "result".to_string(),
                "stderr".to_string(),
                "stdout".to_string(),
            ],
            "a miss must declare the tool defaults, its own names and the stream captures",
        );

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(first.instance_key, first.instance.clone());
        let second = execute_echo_step(
            &cas,
            state,
            "step-b-echo",
            tool_defaults,
            BTreeMap::new(),
            BTreeSet::from(["result".to_string()]),
        )
        .await;

        assert!(second.cache_hit, "identical work under a recorded instance must be reused");
        assert!(
            second.producible_outputs.is_empty(),
            "a hit produced nothing, so it may neither claim nor drop a name: {:?}",
            second.producible_outputs.keys().collect::<Vec<_>>(),
        );
    }

    /// The probe's producible set is the merged declarations plus the stream
    /// captures every execution records.
    ///
    /// A lookup hit rejected solely for missing blobs must not discard
    /// migration/re-key maintenance before falling back to execution.
    ///
    /// The hit's metadata migration is independent of reuse: even when the
    /// recorded blobs are absent and the step must execute fresh, the migrated
    /// record (re-keyed and/or with `attempted_key_version_nk` advanced) is
    /// preserved as lookup maintenance so subsequent runs do not repeat the
    /// migration work unnecessarily.
    #[tokio::test]
    async fn blob_missing_migrated_hit_preserves_rekey_maintenance() {
        use crate::instance_key::{
            INSTANCE_KEY_VERSION, V7_INSTANCE_KEY_VERSION, V8KeyMaterial,
            derive_instance_key_for_version, derive_instance_key_v7,
        };

        let cas = mediapm_cas::new_in_memory_cas();
        let tool_defaults =
            BTreeMap::from([("result".to_string(), stream_spec("result", "stdout"))]);
        let required = BTreeSet::from(["result".to_string()]);

        let first = execute_echo_step(
            &cas,
            ConductorState::new_empty(),
            "step-a-echo",
            tool_defaults.clone(),
            BTreeMap::new(),
            required.clone(),
        )
        .await;
        assert!(!first.cache_hit, "first run must execute");

        let mut downgraded = first.instance.clone();
        downgraded.key_version = V7_INSTANCE_KEY_VERSION;
        downgraded.attempted_key_version_nk = V7_INSTANCE_KEY_VERSION;
        let result_output =
            downgraded.outputs.get_mut("result").expect("echo step records result output");
        result_output.hash_nk = Some(Hash::from_content(b"missing-blob"));
        let v7_key = derive_instance_key_v7(&downgraded.tool_call_id, &downgraded.command_args);
        assert_ne!(v7_key, first.instance_key);

        let output_tokens: BTreeMap<String, String> = downgraded
            .outputs
            .iter()
            .map(|(name, recorded)| (name.clone(), recorded.capture.clone()))
            .collect();
        let migrated_v8_key = derive_instance_key_for_version(
            INSTANCE_KEY_VERSION,
            &downgraded.tool_call_id,
            &downgraded.command_args,
            Some(&V8KeyMaterial {
                output_tokens: &output_tokens,
                materialized_inputs: &downgraded.materialized_inputs,
            }),
        )
        .expect("v8 key derivable for migrated record");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(v7_key, downgraded);
        state.step_instances.insert(
            "test-workflow".to_string(),
            BTreeMap::from([("step-a-echo".to_string(), v7_key)]),
        );

        let bundle =
            execute_echo_step(&cas, state, "step-a-echo", tool_defaults, BTreeMap::new(), required)
                .await;

        assert!(!bundle.cache_hit, "missing blobs must force fresh execution");
        assert_eq!(
            bundle.maintenance_rekey,
            Some((v7_key, migrated_v8_key)),
            "migrated blob-missing hit must preserve its re-key maintenance",
        );
        let maintained = bundle
            .lookup_side_effects
            .get(&migrated_v8_key)
            .expect("migrated maintenance instance must be preserved");
        assert_eq!(maintained.key_version, INSTANCE_KEY_VERSION);
        assert_eq!(maintained.attempted_key_version_nk, INSTANCE_KEY_VERSION);
    }

    /// A dependent may bind `${step_output.<step>.stdout}`, a name neither the
    /// tool nor the step declares. The capture phase records it regardless, so
    /// the probe has to treat it as producible; filtering against the merged map
    /// alone dropped it, which skipped the CAS presence check for exactly the
    /// names every execution writes. A declared entry keeps its own spec, as the
    /// capture phase does.
    #[test]
    fn producible_outputs_include_the_implicit_streams() {
        let declared_stdout = OutputCaptureSpec {
            capture: crate::config::CaptureKind::parse_token("file:out.txt")
                .expect("valid capture"),
            ..stream_spec("stdout", "stdout")
        };
        let merged = BTreeMap::from([
            ("result".to_string(), stream_spec("result", "stdout")),
            ("stdout".to_string(), declared_stdout.clone()),
        ]);

        let producible = producible_output_specs(&merged);

        assert_eq!(
            producible.keys().cloned().collect::<Vec<_>>(),
            vec![
                "process_code".to_string(),
                "result".to_string(),
                "stderr".to_string(),
                "stdout".to_string(),
            ],
            "the implicit streams must join the merged declarations",
        );
        assert_eq!(
            producible["stdout"], declared_stdout,
            "a declared name must keep its declared spec, as the capture phase records it",
        );
        for name in ["stderr", "process_code"] {
            assert_eq!(
                producible[name],
                stream_spec(name, name),
                "an undeclared stream must be producible under the implicit spec",
            );
        }
    }

    /// A probe hit requires each required name to be recorded with the spec the
    /// step's declaration asks for, when the tool does not declare that name.
    ///
    /// The same name captured another way is a miss: reusing those bytes would
    /// hand the step content it would not have produced, which is exactly what
    /// the recorded spec exists to prevent. The name here is step-only, because
    /// a tool-declared name is the tool's to define and a step changes only its
    /// persistence.
    #[tokio::test]
    async fn a_recorded_spec_that_differs_from_the_declared_one_is_a_miss() {
        let cas = mediapm_cas::new_in_memory_cas();
        let first = execute_echo_step(
            &cas,
            ConductorState::new_empty(),
            "step-a-echo",
            BTreeMap::new(),
            BTreeMap::from([("result".to_string(), stream_spec("result", "stdout"))]),
            BTreeSet::from(["result".to_string()]),
        )
        .await;

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(first.instance_key, first.instance.clone());

        let second = execute_echo_step(
            &cas,
            state,
            "step-b-echo",
            BTreeMap::new(),
            BTreeMap::from([("result".to_string(), stream_spec("result", "stderr"))]),
            BTreeSet::from(["result".to_string()]),
        )
        .await;

        assert!(
            !second.cache_hit,
            "an instance recorded under a different spec for the name must not serve this step",
        );
        assert_eq!(
            second.instance.outputs["result"].capture, "stderr",
            "the re-execution records the spec the step declares",
        );
    }

    /// Two steps that declare different subsets of a tool's artifacts reuse one
    /// instance, because the tool defines every name it declares.
    ///
    /// This is the regression the merge rule exists for. The first step records
    /// `links` and `thumbnails` under the tool's specs while declaring only
    /// `links`; the second step declares only `thumbnails`. If the step's
    /// declaration could re-select a tool-declared name, the recorded
    /// `thumbnails` spec would be the tool's and the second step's requirement
    /// the step's, so the second step would re-execute and replace the entry the
    /// first step needs — and the two would alternate forever on every sync.
    #[tokio::test]
    async fn two_steps_declaring_different_tool_artifacts_reuse_one_instance() {
        let cas = mediapm_cas::new_in_memory_cas();
        let tool_defaults = BTreeMap::from([
            (
                "links".to_string(),
                OutputCaptureSpec {
                    name: "links".to_string(),
                    capture: crate::config::CaptureKind::parse_token("file:downloads/links.list")
                        .expect("valid capture"),
                    allow_empty: true,
                    ..OutputCaptureSpec::default()
                },
            ),
            (
                "thumbnails".to_string(),
                OutputCaptureSpec {
                    name: "thumbnails".to_string(),
                    capture: crate::config::CaptureKind::parse_token("file:downloads/thumbs.list")
                        .expect("valid capture"),
                    allow_empty: true,
                    ..OutputCaptureSpec::default()
                },
            ),
        ]);

        let first = execute_echo_step(
            &cas,
            ConductorState::new_empty(),
            "step-a-echo",
            tool_defaults.clone(),
            BTreeMap::from([(
                "links".to_string(),
                OutputCaptureSpec {
                    name: "links".to_string(),
                    capture: crate::config::CaptureKind::parse_token("file:downloads/links.list")
                        .expect("valid capture"),
                    allow_empty: true,
                    ..OutputCaptureSpec::default()
                },
            )]),
            BTreeSet::from(["links".to_string()]),
        )
        .await;
        assert!(!first.cache_hit, "the first run of this work has no instance to reuse");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(first.instance_key, first.instance.clone());

        let second = execute_echo_step(
            &cas,
            state,
            "step-b-echo",
            tool_defaults,
            BTreeMap::from([(
                "thumbnails".to_string(),
                OutputCaptureSpec {
                    name: "thumbnails".to_string(),
                    capture: crate::config::CaptureKind::parse_token("file:downloads/thumbs.list")
                        .expect("valid capture"),
                    allow_empty: true,
                    ..OutputCaptureSpec::default()
                },
            )]),
            BTreeSet::from(["thumbnails".to_string()]),
        )
        .await;

        assert!(
            second.cache_hit,
            "a name the tool declares must be reusable by a step whose declaration differs only in tolerance",
        );
    }

    /// A required name the merged capture set cannot produce does not force a
    /// re-execution.
    ///
    /// The requirement is a union: a dependent may reference a name no instance
    /// of this work can hold — a misconfigured `${step_output.…}` reference, for
    /// instance. Such a name is filtered out before the probe and the CAS check,
    /// because no recorded entry can ever satisfy it, so asking the store about it
    /// would turn every run of a workflow that is going to fail at resolution into
    /// a full re-execution for no added information. The producible name beside it
    /// is still required, which is what keeps the filter from becoming a bypass.
    #[tokio::test]
    async fn a_required_name_the_merged_set_cannot_produce_does_not_force_a_rerun() {
        let cas = mediapm_cas::new_in_memory_cas();
        let tool_defaults =
            BTreeMap::from([("result".to_string(), stream_spec("result", "stdout"))]);
        let first = execute_echo_step(
            &cas,
            ConductorState::new_empty(),
            "step-a-echo",
            tool_defaults.clone(),
            BTreeMap::new(),
            BTreeSet::from(["result".to_string()]),
        )
        .await;
        assert!(!first.cache_hit, "the first run of this work has no instance to reuse");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(first.instance_key, first.instance.clone());

        let second = execute_echo_step(
            &cas,
            state,
            "step-b-echo",
            tool_defaults,
            BTreeMap::new(),
            BTreeSet::from(["result".to_string(), "phantom".to_string()]),
        )
        .await;

        assert!(
            second.cache_hit,
            "a requirement the merged set cannot produce must not be read as a missing blob",
        );
    }

    /// A step declaring a name the tool also declares keeps the tool's capture
    /// and takes the step's tolerance and persistence decisions.
    ///
    /// The tool owns the sandbox layout a capture reads, while whether this step
    /// permits an empty match and whether it persists the family are local to the
    /// step.
    #[test]
    fn a_tool_declared_name_keeps_the_tool_capture_and_takes_step_tolerance() {
        let tool_spec = OutputCaptureSpec {
            name: "links".to_string(),
            capture: crate::config::CaptureKind::parse_token("folder_regex:^downloads/links/.+$")
                .expect("valid capture"),
            save: SaveMode::True,
            allow_empty: true,
        };
        let step_spec = OutputCaptureSpec {
            name: "links".to_string(),
            capture: crate::config::CaptureKind::parse_token("folder:downloads/links")
                .expect("valid capture"),
            save: SaveMode::False,
            allow_empty: false,
        };

        let merged = merge_output_specs(
            &BTreeMap::from([("links".to_string(), tool_spec.clone())]),
            &BTreeMap::from([("links".to_string(), step_spec.clone())]),
        );

        assert_eq!(
            merged["links"],
            OutputCaptureSpec {
                save: step_spec.save,
                allow_empty: step_spec.allow_empty,
                ..tool_spec
            },
            "the tool's capture survives the step's declaration; the step's allow_empty and save win",
        );
    }

    /// A name only the step declares keeps the step's whole specification.
    ///
    /// Nothing else defines that name, so the step's selector, tolerance and
    /// folder shape are the only meaning it has.
    #[test]
    fn a_step_only_name_keeps_the_step_specification() {
        let tool_spec = stream_spec("content", "file:out.mkv");
        let step_spec = OutputCaptureSpec {
            name: "scratch".to_string(),
            capture: crate::config::CaptureKind::Stderr,
            save: SaveMode::False,
            allow_empty: true,
        };

        let merged = merge_output_specs(
            &BTreeMap::from([("content".to_string(), tool_spec.clone())]),
            &BTreeMap::from([("scratch".to_string(), step_spec.clone())]),
        );

        assert_eq!(merged["scratch"], step_spec, "a step-only name has no tool definition to keep");
        assert_eq!(merged["content"], tool_spec, "the step declares nothing about this name");
    }

    /// A name only the tool declares is recorded exactly as the tool declares
    /// it, whatever else the step names.
    #[test]
    fn a_tool_only_name_is_unchanged() {
        let content = stream_spec("content", "file:out.mkv");
        let links = OutputCaptureSpec {
            allow_empty: true,
            ..stream_spec("links", "folder_regex:^downloads/links/.+$")
        };

        let merged = merge_output_specs(
            &BTreeMap::from([
                ("content".to_string(), content.clone()),
                ("links".to_string(), links.clone()),
            ]),
            &BTreeMap::from([("scratch".to_string(), stream_spec("scratch", "stderr"))]),
        );

        assert_eq!(merged["content"], content);
        assert_eq!(merged["links"], links);
        assert_eq!(merged["scratch"], stream_spec("scratch", "stderr"));
    }

    /// A merged spec's `name` is the map key it is stored under, whatever the
    /// declaration called itself.
    ///
    /// Capture records `spec.name`, while the probe, a dependent's
    /// `${step_output.…}` reference and the materializer all address the map key.
    /// A declaration whose name disagrees with its key would therefore record a
    /// name nothing can bind: the step would miss on every sync and re-execute
    /// forever, and the name it did record would be unreachable. The key is the
    /// identity, so both sides are normalized to it.
    #[test]
    fn a_specification_name_is_normalized_to_its_map_key() {
        let tool_spec = stream_spec("thumbnails", "file:downloads/thumbs.list");
        let step_spec = stream_spec("wrong", "stdout");

        let merged = merge_output_specs(
            &BTreeMap::from([("links".to_string(), tool_spec)]),
            &BTreeMap::from([("result".to_string(), step_spec)]),
        );

        assert_eq!(merged["links"].name, "links", "a tool-declared name takes its map key");
        assert_eq!(merged["result"].name, "result", "a step-only name takes its map key");
        assert_eq!(
            merged["links"].capture,
            crate::config::CaptureKind::parse_token("file:downloads/thumbs.list")
                .expect("valid capture"),
            "normalizing the name must not disturb the selector it was declared with",
        );
    }

    /// A recorded instance names the step that recorded it, so the fixture's
    /// regeneration can pair instance -> step without key history.
    #[tokio::test]
    async fn a_recorded_instance_names_the_step_that_recorded_it() {
        let cas = mediapm_cas::new_in_memory_cas();
        let bundle = execute_echo_step(
            &cas,
            ConductorState::new_empty(),
            "step-7-echo",
            BTreeMap::from([("result".to_string(), stream_spec("result", "stdout"))]),
            BTreeMap::new(),
            BTreeSet::from(["result".to_string()]),
        )
        .await;

        assert_eq!(
            bundle.instance.step_ids_nk,
            BTreeSet::from([StepRef {
                workflow_id: "test-workflow".to_string(),
                step_id: "step-7-echo".to_string(),
            }]),
        );
    }

    /// A cache hit records the reusing step, and a hit whose id the instance
    /// already names leaves the record untouched.
    ///
    /// `step_ids` is the only pairing record the fixture carries, so an
    /// instance shared by several steps must name all of them: a second step
    /// that reuses an existing instance without being recorded leaves
    /// regeneration unable to pair it, and the run aborts. The third step's
    /// full-value equality is the change guard — an id already present must
    /// return the stored record unchanged, or every steady-state sync would
    /// rewrite the state document for provenance that is already correct.
    #[tokio::test]
    async fn a_cache_hit_records_the_reusing_step_and_leaves_a_known_step_alone() {
        let cas = mediapm_cas::new_in_memory_cas();
        let tool_defaults =
            BTreeMap::from([("result".to_string(), stream_spec("result", "stdout"))]);
        let required = BTreeSet::from(["result".to_string()]);
        let first = execute_echo_step(
            &cas,
            ConductorState::new_empty(),
            "step-a-echo",
            tool_defaults.clone(),
            BTreeMap::new(),
            required.clone(),
        )
        .await;
        assert!(!first.cache_hit, "the first run of this work has no instance to reuse");
        assert_eq!(
            first.instance.step_ids_nk,
            BTreeSet::from([StepRef {
                workflow_id: "test-workflow".to_string(),
                step_id: "step-a-echo".to_string(),
            }]),
        );

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(first.instance_key, first.instance.clone());

        let second = execute_echo_step(
            &cas,
            state.clone(),
            "step-b-echo",
            tool_defaults.clone(),
            BTreeMap::new(),
            required.clone(),
        )
        .await;
        assert!(second.cache_hit, "identical work must hit the instance the first step recorded");
        assert_eq!(
            second.instance.step_ids_nk,
            BTreeSet::from([
                StepRef {
                    workflow_id: "test-workflow".to_string(),
                    step_id: "step-a-echo".to_string(),
                },
                StepRef {
                    workflow_id: "test-workflow".to_string(),
                    step_id: "step-b-echo".to_string(),
                },
            ]),
            "the reusing step must be recorded beside the step that executed the work",
        );

        state.tool_call_instances.insert(second.instance_key, second.instance.clone());
        let third =
            execute_echo_step(&cas, state, "step-b-echo", tool_defaults, BTreeMap::new(), required)
                .await;
        assert!(third.cache_hit, "a recorded instance must keep serving the same work");
        assert_eq!(
            third.instance, second.instance,
            "a hit whose step id is already recorded must leave the record byte-identical",
        );
    }

    // -----------------------------------------------------------------------
    // Env minimal-set cleaning and provenance
    // -----------------------------------------------------------------------

    #[test]
    fn env_vars_minimal_set_filters_empty_values() {
        let records = build_env_vars(
            &BTreeMap::from([
                ("KEEP".to_string(), "value".to_string()),
                ("EMPTY".to_string(), String::new()),
            ]),
            &BTreeMap::new(),
        );
        assert_eq!(records.len(), 1);
        assert!(records.contains_key("KEEP"));
        assert!(!records.contains_key("EMPTY"));
    }

    #[test]
    fn env_vars_values_hashed_never_literal() {
        let records = build_env_vars(
            &BTreeMap::from([("FOO".to_string(), "raw-secret".to_string())]),
            &BTreeMap::new(),
        );
        let record = &records["FOO"];
        assert_eq!(record.hash_ck, Hash::from_content(b"raw-secret"));
        assert_ne!(record.hash_ck, Hash::from_content(b"raw-secret-other"));
    }

    #[test]
    fn env_vars_empty_input_yields_empty_records() {
        assert!(build_env_vars(&BTreeMap::new(), &BTreeMap::new()).is_empty());
    }

    #[test]
    fn build_env_vars_tags_literal_and_inherited_provenance() {
        let records = build_env_vars(
            &BTreeMap::from([
                ("DOC".to_string(), "fixed".to_string()),
                ("HOST".to_string(), "ambient".to_string()),
            ]),
            &BTreeMap::from([("DOC".to_string(), "fixed".to_string())]),
        );

        assert_eq!(
            records["DOC"].source_nk,
            Some(crate::state::EnvVarSource::Literal),
            "a name in literal_env_vars must record literal provenance",
        );
        assert_eq!(
            records["HOST"].source_nk,
            Some(crate::state::EnvVarSource::Inherited),
            "an execution env var not fixed by the document must record inherited provenance",
        );
        assert_eq!(
            records["DOC"].hash_ck,
            Hash::from_content(b"fixed"),
            "literal entries are still hashed, never stored literally",
        );
        assert_eq!(
            records["HOST"].hash_ck,
            Hash::from_content(b"ambient"),
            "inherited entries are still hashed, never stored literally",
        );
    }

    /// Runs one executable step that writes `sidecar.txt` and no video file.
    ///
    /// The tool declares a tolerant `content` video `file_regex` and a strict
    /// sidecar `file` capture. `step_outputs` controls which names the step
    /// declares, so a caller can inherit the tool's tolerant `content` or restate
    /// it with different tolerance.
    async fn execute_sidecar_writer_step(
        cas: &mediapm_cas::InMemoryCas,
        step_outputs: BTreeMap<String, OutputCaptureSpec>,
    ) -> Result<StepExecutionBundle, crate::error::ConductorError> {
        let tmp = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let tool_id = "writer@v1";
        let tool_defaults = BTreeMap::from([
            (
                "content".to_string(),
                OutputCaptureSpec {
                    name: "content".to_string(),
                    capture: crate::config::CaptureKind::parse_token("file_regex:^.*\\.mkv$")
                        .expect("valid capture"),
                    save: SaveMode::True,
                    allow_empty: true,
                },
            ),
            (
                "sidecar".to_string(),
                OutputCaptureSpec {
                    name: "sidecar".to_string(),
                    capture: crate::config::CaptureKind::parse_token("file:sidecar.txt")
                        .expect("valid capture"),
                    save: SaveMode::True,
                    allow_empty: false,
                },
            ),
        ]);
        let tool = UnifiedToolSpec {
            name: "writer".to_string(),
            version: None,
            is_impure: false,
            max_concurrent_calls: None,
            max_retries: 0,
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            command_parts: vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "printf '%s' sidecar > sidecar.txt".to_string(),
            ],
            success_codes: vec![0],
            execution_env_vars: BTreeMap::new(),
            literal_env_vars: BTreeMap::new(),
            outputs: tool_defaults,
            tool_content_map: BTreeMap::new(),
            payload_root: String::new(),
            payload_staging: crate::config::PayloadStaging::Copy,
            builtin_id: None,
        };
        let request = StepExecutionRequest {
            unified: Arc::new(UnifiedNickelDocument {
                tools: BTreeMap::from([(tool_id.to_string(), tool)]),
                workflows: BTreeMap::new(),
                tool_content_hashes: BTreeSet::new(),
                external_data_policies: BTreeMap::new(),
                runtime: crate::config::ConductorRuntimeConfig::default(),
            }),
            workflow_id: "test-workflow".to_string(),
            step: WorkflowStepSpec {
                id: "step-0-writer".to_string(),
                tool: "writer".to_string(),
                inputs: BTreeMap::new(),
                outputs: step_outputs,
                runtime: Some(crate::config::StepRuntime { max_retries: Some(0) }),
                depends_on: Vec::new(),
            },
            impure_timestamp: None,
            state_snapshot: Arc::new(ConductorState::new_empty()),
            outermost_config_dir: tmp.path().to_path_buf(),
            conductor_tmp_dir: tmp.path().join("runtime"),
            host_builtins: Arc::new(crate::tools::HostBuiltins::default()),
            step_outputs: Arc::new(BTreeMap::new()),
            required_output_names: BTreeSet::from(["sidecar".to_string()]),
            force_rerun: false,
        };
        execute_step(cas, request).await
    }

    /// A step that declares only a sidecar inherits the tool's tolerant
    /// `content` video `file_regex`, so a run that produces no video records an
    /// allowed-empty `content` rather than failing the step.
    #[tokio::test]
    async fn a_sidecar_step_does_not_fail_on_an_inherited_video_capture() {
        let cas = mediapm_cas::new_in_memory_cas();
        let bundle = execute_sidecar_writer_step(
            &cas,
            BTreeMap::from([(
                "sidecar".to_string(),
                OutputCaptureSpec {
                    name: "sidecar".to_string(),
                    capture: crate::config::CaptureKind::parse_token("file:sidecar.txt")
                        .expect("valid capture"),
                    save: SaveMode::True,
                    allow_empty: false,
                },
            )]),
        )
        .await
        .expect("inheriting a tolerant content capture must not fail a sidecar-only step");

        let sidecar =
            bundle.instance.outputs.get("sidecar").expect("the sidecar output must be recorded");
        assert!(sidecar.hash_nk.is_some(), "the sidecar capture must record its hash");
        let content = bundle
            .instance
            .outputs
            .get("content")
            .expect("the inherited content output must be recorded");
        assert_eq!(
            content.hash_nk, None,
            "an inherited tolerant video capture with no match must record hash_nk: null",
        );
    }

    /// A step that restates a tool-declared `content` capture may tighten its
    /// tolerance, so a required video match still fails when nothing matches.
    #[tokio::test]
    async fn a_sidecar_step_redeclaring_content_without_tolerance_still_fails_when_nothing_matches()
    {
        let cas = mediapm_cas::new_in_memory_cas();
        let result = execute_sidecar_writer_step(
            &cas,
            BTreeMap::from([
                (
                    "sidecar".to_string(),
                    OutputCaptureSpec {
                        name: "sidecar".to_string(),
                        capture: crate::config::CaptureKind::parse_token("file:sidecar.txt")
                            .expect("valid capture"),
                        save: SaveMode::True,
                        allow_empty: false,
                    },
                ),
                (
                    "content".to_string(),
                    OutputCaptureSpec {
                        name: "content".to_string(),
                        capture: crate::config::CaptureKind::parse_token("file_regex:^.*\\.mkv$")
                            .expect("valid capture"),
                        save: SaveMode::True,
                        allow_empty: false,
                    },
                ),
            ]),
        )
        .await;

        let error = result.expect_err(
            "restating content with allow_empty: false must fail when no video matches",
        );
        let message = error.to_string();
        assert!(
            message.contains("allow_empty is false") || message.contains("matched nothing"),
            "unexpected error when a restated strict content capture matches nothing: {message}",
        );
    }
}
