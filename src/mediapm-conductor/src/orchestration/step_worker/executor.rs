//! Core step execution orchestration.
//!
//! [`execute_step`] runs a step through all phases: input resolution, cache
//! probe, sandbox materialization, execution, and output capture. Persistence
//! is not a phase here: each captured entry records the capture spec that
//! produced it, and that spec carries the persistence decision.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use mediapm_cas::{CasApi, Hash};
use mediapm_utils::Timestamp;

use crate::config::OutputCaptureSpec;
use crate::error::ConductorError;
use crate::orchestration::protocol::{
    StepExecutionBundle, StepExecutionRequest, UnifiedToolSpec, find_tool_entry_by_name,
};
use crate::state::{HashedValueRecord, ResolvedInput, ToolCallInstance};

use super::cache::probe_cache;
use super::capture::{capture_outputs, implicit_stream_specs};
use super::inputs::resolve_step_inputs;
use super::process::{ExecutionResult, run_builtin, run_executable_process};
use super::sandbox::{create_sandbox, prepare_tool_payload, resolve_executable_under_payload_root};
use super::template::{TemplateContext, resolve_command_parts};
use crate::instance_key::{derive_instance_key, deterministic_input_hashes_from_resolved_inputs};

/// Dispatches execution: runs a builtin or resolves and runs an executable
/// process. Returns the execution result plus the resolved command-argument
/// records (empty for builtins) to store on the tool call instance.
async fn dispatch_tool_execution<C: CasApi + Send + Sync>(
    cas: &C,
    tool_spec: &UnifiedToolSpec,
    request: &StepExecutionRequest,
    resolved_inputs: &[ResolvedInput],
    sandbox_dir: &Path,
) -> Result<(ExecutionResult, Vec<HashedValueRecord>), ConductorError> {
    if tool_spec.command_parts.is_empty() {
        let args: BTreeMap<String, String> =
            resolved_inputs.iter().map(|ri| (ri.key.clone(), ri.value.clone())).collect();
        let builtin_id = tool_spec.builtin_id.as_deref().unwrap_or(&request.step.tool);
        let result =
            run_builtin(cas, builtin_id, &args, &request.outermost_config_dir, sandbox_dir).await?;
        Ok((result, Vec::new()))
    } else {
        let resolved_inputs_map: BTreeMap<String, String> =
            resolved_inputs.iter().map(|ri| (ri.key.clone(), ri.value.clone())).collect();
        let cmd_ctx = TemplateContext::<C> {
            cas: Some(cas),
            step_outputs: &request.step_outputs,
            env_vars: &BTreeMap::new(),
            tokens: &BTreeMap::new(),
            sandbox_dir: Some(sandbox_dir),
            host_os: std::env::consts::OS,
            inputs: &resolved_inputs_map,
        };
        let (mut resolved_parts, determinism) =
            resolve_command_parts(&tool_spec.command_parts, &cmd_ctx).await?;
        // `in_place` executes the tool where the provisioner put it, so the
        // executable the document names relative to the sandbox is resolved
        // against the payload root before anything runs.
        resolve_executable_under_payload_root(&mut resolved_parts, tool_spec)?;
        let command_args = resolved_parts
            .iter()
            .zip(&determinism)
            .map(|(part, deterministic)| HashedValueRecord {
                hash: Hash::from_content(part.as_bytes()),
                deterministic: *deterministic,
            })
            .collect();
        let result = run_executable_process(
            &resolved_parts,
            &tool_spec.success_codes,
            sandbox_dir,
            &tool_spec.execution_env_vars,
        )
        .await?;
        Ok((result, command_args))
    }
}

/// Executes one step: resolves inputs, runs the tool, captures outputs.
pub(super) async fn execute_step<C: CasApi + Send + Sync>(
    cas: &C,
    request: StepExecutionRequest,
) -> Result<StepExecutionBundle, ConductorError> {
    let resolved_inputs = resolve_step_inputs::<C>(&request).await?;

    // Resolve the tool call id from the unified tools map key (the versioned
    // id such as `"echo@v1"`), not the bare `spec.name` used by step
    // references. The instance key must be stable across runs of the same
    // declared tool even if the step references it by name.
    let (tool_call_id, tool_spec) =
        find_tool_entry_by_name(&request.unified.tools, &request.step.tool)
            .map(|(key, spec)| (key.clone(), spec))
            .ok_or_else(|| {
                ConductorError::Workflow(format!(
                    "step '{}' references unknown tool '{}'",
                    request.step.id, request.step.tool,
                ))
            })?;

    let deterministic_input_hashes = deterministic_input_hashes_from_resolved_inputs(
        &resolved_inputs,
        &request.step.inputs,
        &tool_spec.default_inputs,
    );

    // Materialized content-map inputs keyed by sandbox-relative path, sorted
    // by path. Only hash-valued entries participate in the key (inline bytes
    // are skipped); content hashes are tool-fixed and deterministic.
    let materialized_inputs = build_materialized_inputs(&tool_spec.tool_content_map);
    let deterministic_materialized_hashes: Vec<Hash> =
        materialized_inputs.values().map(|record| record.hash).collect();

    // Tool-level capture defaults merged with the step's own declarations. The
    // tool owns the capture identity of every name it declares (its selector,
    // `allow_empty` and `include_topmost_folder`); a step declaring the same
    // name contributes only `save`, and a step-only name is kept whole. That is
    // the set the capture phase records below, and the set a probe hit has to
    // satisfy, because one instance is shared by every step running this work:
    // a step able to re-select a tool-declared name would make two steps
    // disagree about bytes one execution produced.
    let merged_outputs = merge_output_specs(&tool_spec.outputs, &request.step.outputs);

    // Captures are not key material. An instance recorded by the same work
    // holds the names a reusing step binds, addressed by name and carrying the
    // spec that produced each one; the key answers only whether the work itself
    // has been done, which inputs alone can answer.
    let executed_at = request.impure_timestamp.unwrap_or_else(Timestamp::now);
    let instance_key = derive_instance_key(
        &tool_call_id,
        &deterministic_input_hashes,
        &deterministic_materialized_hashes,
    );

    // Environment values are stored hashed (never literally); the minimal set
    // is the non-empty configured execution env vars.
    let env_vars = build_env_vars(&tool_spec.execution_env_vars);

    // Cache probe. `force_rerun` is the run-scoped explicit invalidation: it
    // skips the probe so every step executes fresh, no matter how recently
    // its instance was recorded. Time never invalidates an instance;
    // changed key material or this flag does.
    //
    // A hit requires every name this step binds — the dependents' references
    // unioned with the step's own declarations — to be recorded under the
    // capture identity the producible set gives it. A name recorded another way,
    // or recorded as `None` by the v2 migration, is a miss: reusing it would hand
    // the step bytes it would not have produced. Whether the store still holds
    // the bytes is a separate fact state cannot answer, so the CAS is asked
    // directly; a missing blob turns the hit into a miss instead of failing
    // later, mid-materialization, with no route back to the step that produced
    // it.
    //
    // A required name the work cannot produce is not a probe condition: no
    // instance this step's work records can hold it, so the dependent's
    // reference fails at resolution instead. That filter is what the blob check
    // is handed, rather than the raw requirement, because a name absent from the
    // producible set can never resolve and checking it would force a
    // re-execution on every run of a workflow that is going to fail anyway.
    let producible_outputs = producible_output_specs(&merged_outputs);
    let required_output_specs: BTreeMap<String, OutputCaptureSpec> = request
        .required_output_names
        .iter()
        .filter_map(|name| producible_outputs.get(name).map(|spec| (name.clone(), spec.clone())))
        .collect();
    let cached_instance = if request.force_rerun {
        None
    } else {
        match probe_cache(&instance_key, &request.state_snapshot, &required_output_specs) {
            Some(instance)
                if required_output_blobs_exist(cas, &instance, &required_output_specs).await =>
            {
                Some(instance)
            }
            _ => None,
        }
    };

    if let Some(mut cached) = cached_instance {
        // A hit records the reusing step before returning: `step_ids` is the
        // provenance the demo fixture pairs through, and a shared instance
        // must name every step it served, not only the one that executed it.
        // The insert is idempotent — an id the set already holds leaves it as
        // it was, so a steady-state sync returns a record identical to the
        // stored one. Returning the bundle is what persists the addition: the
        // coordinator merges the bundle's instance into the run state, while
        // GC refresh still happens through the referenced-keys set at GC
        // time, not by per-hit mutation.
        cached.step_ids.insert(request.step.id.clone());
        // A hit declares nothing: it produced no artifact this run, so the
        // settlement may add the reusing step to `step_ids` and must leave the
        // recorded outputs exactly as the store holds them.
        return Ok(StepExecutionBundle {
            instance: cached,
            cache_hit: true,
            producible_outputs: BTreeMap::new(),
        });
    }

    let sandbox_dir = create_sandbox(&request.conductor_tmp_dir, &instance_key).await?;
    prepare_tool_payload(cas, tool_spec, &sandbox_dir).await?;

    let (execution_result, command_args) =
        dispatch_tool_execution(cas, tool_spec, &request, &resolved_inputs, &sandbox_dir).await?;

    // Capture outputs: the merged set computed before the probe, so the probe
    // and the capture phase cannot disagree about which names exist.
    let captured = capture_outputs(cas, &merged_outputs, &execution_result, &sandbox_dir).await?;

    let instance = ToolCallInstance {
        instance_key,
        tool_call_id,
        impure: tool_spec.is_impure,
        // A fresh execution clears any marker: the bytes below were produced
        // now, under the config in hand, so they may be reused again. Without
        // this, re-executing invalidated work would leave the record marked and
        // the next run would miss forever on a key it cannot change.
        invalidated_at: None,
        executed_at,
        step_ids: BTreeSet::from([request.step.id.clone()]),
        command_args,
        env_vars,
        materialized_inputs,
        outputs: captured.records,
    };

    Ok(StepExecutionBundle { instance, cache_hit: false, producible_outputs })
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
        if cas.stat(record.hash).await.is_err() {
            return false;
        }
    }
    true
}

/// Returns every output name this step's work can produce, with the spec that
/// produces it.
///
/// The merged declarations are not the whole set: `stdout`, `stderr` and
/// `process_code` are recorded by every execution (see [`implicit_stream_specs`]),
/// so a dependent referencing one of them is asking for a name the instance
/// holds even though neither the tool nor the step declared it. A merged-set-only
/// filter would therefore skip the CAS check for exactly the names every
/// execution writes, so the check would be missing where the recorded entry is
/// guaranteed to exist.
///
/// A declared entry wins over the implicit one for its name, matching the
/// capture phase, which records a declared `stdout` under the declared spec and
/// skips the implicit spec.
fn producible_output_specs(
    merged_outputs: &BTreeMap<String, OutputCaptureSpec>,
) -> BTreeMap<String, OutputCaptureSpec> {
    let mut producible = merged_outputs.clone();
    for (name, spec) in implicit_stream_specs() {
        producible.entry(name).or_insert(spec);
    }
    producible
}

/// Names the fields of `step` that `tool` overrides for one output.
///
/// A step may restate a tool-declared name for any reason, so silence here would
/// leave a declaration that has no effect looking as though it had one: the tool
/// keeps `capture`, `allow_empty` and `include_topmost_folder`, and only `save`
/// survives. Comparing values rather than presence is deliberate — a step that
/// repeats the tool's own selector is declaring what already holds and deserves
/// no warning, while one that names a different selector is stating an intent
/// that is being discarded.
///
/// The order is fixed so the diagnostic reads the same way every run.
///
/// `name` is not in the list. The merge rewrites it to the map key, but that is
/// normalization rather than a discarded intent: a key identifies its entry and
/// a spec's own `name` restates it, so a step whose `name` disagrees with its
/// key has a malformed spec, not an unfulfilled declaration.
fn discarded_step_fields(tool: &OutputCaptureSpec, step: &OutputCaptureSpec) -> Vec<&'static str> {
    let mut discarded = Vec::new();
    if step.capture != tool.capture {
        discarded.push("capture");
    }
    if step.allow_empty != tool.allow_empty {
        discarded.push("allow_empty");
    }
    if step.include_topmost_folder != tool.include_topmost_folder {
        discarded.push("include_topmost_folder");
    }
    discarded
}

/// Merges tool-level output specs (defaults) with step-level output specs.
///
/// The tool owns the meaning of every artifact name it declares: its `capture`,
/// `allow_empty` and `include_topmost_folder` are kept, and a step declaring
/// the same name contributes only its `save`. A step's full declaration is
/// authoritative only for a name the tool does not declare, and a name only
/// the tool declares is kept unchanged.
///
/// The reason is that one instance is shared by every step running the same
/// work, and a name-addressed record is reusable only when the capture beside it
/// is the one the reusing step would have produced. The artifact layout is a
/// property of the tool — the sandbox path a capture reads, and whether a
/// family may legitimately be empty, follow from how the tool writes it — so a
/// step that could re-select a tool-declared name would let two steps disagree
/// about bytes one execution produced, and each would re-execute to replace the
/// other's record forever.
///
/// Persistence is the one thing a step owns: whether its own materialization
/// keeps a family is local to that step and changes no bytes. That is also why
/// the probe compares capture identity rather than the whole spec
/// ([`super::cache::probe_cache`]): `save` is recorded beside the entry for
/// provenance, but a reused entry is byte-for-byte the same artifact whether the
/// reusing step persists it or not, so letting it decide a miss would put two
/// steps with different persistence on a forever-re-executing loop.
///
/// Every entry's `name` is set to the map key it is stored under. Capture records
/// `spec.name`, while the probe, the dependents' references and the materializer
/// address the key; a declaration whose name disagrees with its key would record
/// a name nothing can bind, and the step would re-execute on every sync. The key
/// is the identity, so the key wins.
///
/// The result is what the capture phase records and what a probe hit must
/// satisfy, so one instance holds every name a step with this work can bind.
/// None of it is key material: the instance key derives from inputs alone.
fn merge_output_specs(
    tool_outputs: &BTreeMap<String, OutputCaptureSpec>,
    step_outputs: &BTreeMap<String, OutputCaptureSpec>,
) -> BTreeMap<String, OutputCaptureSpec> {
    let mut merged = tool_outputs.clone();
    for (name, step_spec) in step_outputs {
        match merged.get_mut(name) {
            Some(tool_spec) => {
                let discarded = discarded_step_fields(tool_spec, step_spec);
                if !discarded.is_empty() {
                    tracing::warn!(
                        "output '{name}' is declared by the tool, so the step's {} {} ignored; \
                         the tool's capture, allow_empty and include_topmost_folder apply, and \
                         only `save` is taken from the step",
                        discarded.join(", "),
                        if discarded.len() == 1 { "is" } else { "are" },
                    );
                }
                tool_spec.save = step_spec.save;
            }
            None => {
                merged.insert(name.clone(), step_spec.clone());
            }
        }
    }
    for (name, spec) in &mut merged {
        if spec.name != *name {
            spec.name.clone_from(name);
        }
    }
    merged
}

/// Builds the recorded environment set from configured execution env vars:
/// only non-empty values, each content-hashed (never stored literally). Every
/// record is nondeterministic, and the instance-key derivation takes no env
/// term at all, so a changed variable invalidates nothing implicitly.
fn build_env_vars(
    execution_env_vars: &BTreeMap<String, String>,
) -> BTreeMap<String, HashedValueRecord> {
    execution_env_vars
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(name, value)| {
            (
                name.clone(),
                HashedValueRecord {
                    hash: Hash::from_content(value.as_bytes()),
                    deterministic: false,
                },
            )
        })
        .collect()
}

/// Builds the recorded materialized-input map from the tool content map:
/// sandbox-relative path → content hash, deterministic (tool-fixed). Entries
/// whose value is not a content hash (e.g. inline bytes) are skipped; the
/// `BTreeMap` yields sorted-by-path order.
fn build_materialized_inputs(
    tool_content_map: &BTreeMap<String, String>,
) -> BTreeMap<String, HashedValueRecord> {
    tool_content_map
        .iter()
        .filter_map(|(path, value)| {
            value
                .parse::<Hash>()
                .ok()
                .map(|hash| (path.clone(), HashedValueRecord { hash, deterministic: true }))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use mediapm_cas::Hash;

    use super::{
        build_env_vars, build_materialized_inputs, discarded_step_fields, execute_step,
        merge_output_specs, producible_output_specs,
    };
    use crate::config::{OutputCaptureSpec, SaveMode, WorkflowStepSpec};
    use crate::orchestration::protocol::{
        StepExecutionBundle, StepExecutionRequest, UnifiedNickelDocument, UnifiedToolSpec,
    };
    use crate::state::ConductorState;

    /// Builds a capture spec for `name` reading the implicit stream `stream`,
    /// with every optional field at its documented default.
    fn stream_spec(name: &str, stream: &str) -> OutputCaptureSpec {
        OutputCaptureSpec {
            name: name.to_string(),
            capture: stream.to_string(),
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

        assert!(discarded_step_fields(&tool, &step).is_empty());
    }

    /// Each field the tool overrides is named, in a fixed order.
    ///
    /// A step's selector, `allow_empty` and `include_topmost_folder` are all
    /// dropped for a tool-declared name, and a declaration that silently lost
    /// one of them would look as though it still applied.
    #[test]
    fn every_overridden_step_field_is_named_in_order() {
        let tool = stream_spec("content", "stdout");
        let step = OutputCaptureSpec {
            name: "content".to_string(),
            capture: "file:out.bin".to_string(),
            save: SaveMode::False,
            allow_empty: true,
            include_topmost_folder: false,
        };

        assert_eq!(
            discarded_step_fields(&tool, &step),
            vec!["capture", "allow_empty", "include_topmost_folder"],
        );
    }

    /// A `save`-only step declaration discards nothing.
    ///
    /// Persistence is exactly what a step owns for a tool-declared name, so the
    /// merge must accept it without a diagnostic.
    #[test]
    fn a_save_only_step_declaration_discards_nothing() {
        let tool = stream_spec("content", "stdout");
        let step = OutputCaptureSpec { save: SaveMode::Full, ..tool.clone() };

        assert!(discarded_step_fields(&tool, &step).is_empty());
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
            max_concurrent_calls: 0,
            max_retries: 0,
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            command_parts: Vec::new(),
            success_codes: Vec::new(),
            execution_env_vars: BTreeMap::new(),
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
            step: WorkflowStepSpec {
                id: step_id.to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::from([("text".to_string(), "hi".to_string())]),
                outputs: step_outputs,
                max_retries: 0,
                depends_on: Vec::new(),
            },
            impure_timestamp: None,
            state_snapshot: Arc::new(state),
            outermost_config_dir: tmp.path().to_path_buf(),
            conductor_tmp_dir: tmp.path().join("runtime"),
            step_outputs: Arc::new(BTreeMap::new()),
            required_output_names: required,
            force_rerun: false,
        };
        execute_step(cas, request).await.expect("the echo builtin runs")
    }

    /// The captures a step declares are not key material.
    ///
    /// Two steps running the same tool with the same inputs have done the same
    /// work, so one execution serves both however differently they name what
    /// they record. Hashing the declarations instead put the second step under
    /// its own key, so it re-ran the command and the fixture's shared instances
    /// could not exist.
    #[tokio::test]
    async fn declared_captures_are_not_key_material() {
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
        assert!(
            first.instance.invalidated_at.is_none(),
            "a fresh execution must record the instance without an invalidation marker",
        );

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(first.instance.instance_key, first.instance.clone());

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
            second.cache_hit,
            "identical work must be reused however each step names what it captures",
        );
        assert_eq!(
            second.instance.instance_key, first.instance.instance_key,
            "a step's declared captures must not move the instance key",
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
            bundle.instance.outputs["result"].capture,
            Some(stream_spec("result", "stdout")),
            "the tool's default must be recorded under the spec that produced it",
        );
        assert_eq!(
            bundle.instance.outputs["override"].capture,
            Some(stream_spec("override", "stderr")),
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
        state.tool_call_instances.insert(first.instance.instance_key, first.instance.clone());
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
    /// A dependent may bind `${step_output.<step>.stdout}`, a name neither the
    /// tool nor the step declares. The capture phase records it regardless, so
    /// the probe has to treat it as producible; filtering against the merged map
    /// alone dropped it, which skipped the CAS presence check for exactly the
    /// names every execution writes. A declared entry keeps its own spec, as the
    /// capture phase does.
    #[test]
    fn producible_outputs_include_the_implicit_streams() {
        let declared_stdout = OutputCaptureSpec {
            capture: "file:out.txt".to_string(),
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
        state.tool_call_instances.insert(first.instance.instance_key, first.instance.clone());

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
            second.instance.outputs["result"].capture,
            Some(stream_spec("result", "stderr")),
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
                    capture: "file:downloads/links.list".to_string(),
                    allow_empty: true,
                    ..OutputCaptureSpec::default()
                },
            ),
            (
                "thumbnails".to_string(),
                OutputCaptureSpec {
                    name: "thumbnails".to_string(),
                    capture: "file:downloads/thumbs.list".to_string(),
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
                    capture: "file:downloads/links.list".to_string(),
                    allow_empty: false,
                    ..OutputCaptureSpec::default()
                },
            )]),
            BTreeSet::from(["links".to_string()]),
        )
        .await;
        assert!(!first.cache_hit, "the first run of this work has no instance to reuse");

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(first.instance.instance_key, first.instance.clone());

        let second = execute_echo_step(
            &cas,
            state,
            "step-b-echo",
            tool_defaults,
            BTreeMap::from([(
                "thumbnails".to_string(),
                OutputCaptureSpec {
                    name: "thumbnails".to_string(),
                    capture: "file:downloads/thumbs.list".to_string(),
                    allow_empty: false,
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
        state.tool_call_instances.insert(first.instance.instance_key, first.instance.clone());

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

    /// A step declaring a name the tool also declares keeps the tool's selector
    /// and tolerance, and contributes only its persistence decision.
    ///
    /// The tool owns what a name means because the sandbox layout and the
    /// tolerance for an empty family are properties of how the tool writes the
    /// artifact, while whether a step persists it is local to that step.
    #[test]
    fn a_tool_declared_name_keeps_the_tool_specification() {
        let tool_spec = OutputCaptureSpec {
            name: "links".to_string(),
            capture: "folder_regex:^downloads/links/.+$".to_string(),
            save: SaveMode::True,
            allow_empty: true,
            include_topmost_folder: false,
        };
        let step_spec = OutputCaptureSpec {
            name: "links".to_string(),
            capture: "folder:downloads/links".to_string(),
            save: SaveMode::False,
            allow_empty: false,
            include_topmost_folder: true,
        };

        let merged = merge_output_specs(
            &BTreeMap::from([("links".to_string(), tool_spec.clone())]),
            &BTreeMap::from([("links".to_string(), step_spec)]),
        );

        assert_eq!(
            merged["links"],
            OutputCaptureSpec { save: SaveMode::False, ..tool_spec },
            "the tool's selector and tolerance survive the step's declaration; only save is the step's",
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
            capture: "stderr".to_string(),
            save: SaveMode::False,
            allow_empty: true,
            include_topmost_folder: false,
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
            include_topmost_folder: false,
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
            merged["links"].capture, "file:downloads/thumbs.list",
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

        assert_eq!(bundle.instance.step_ids, BTreeSet::from(["step-7-echo".to_string()]));
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
        assert_eq!(first.instance.step_ids, BTreeSet::from(["step-a-echo".to_string()]));

        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(first.instance.instance_key, first.instance.clone());

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
            second.instance.step_ids,
            BTreeSet::from(["step-a-echo".to_string(), "step-b-echo".to_string()]),
            "the reusing step must be recorded beside the step that executed the work",
        );

        state.tool_call_instances.insert(second.instance.instance_key, second.instance.clone());
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
    // Env minimal-set cleaning
    // -----------------------------------------------------------------------

    #[test]
    fn env_vars_minimal_set_filters_empty_values() {
        let records = build_env_vars(&BTreeMap::from([
            ("KEEP".to_string(), "value".to_string()),
            ("EMPTY".to_string(), String::new()),
        ]));
        assert_eq!(records.len(), 1);
        assert!(records.contains_key("KEEP"));
        assert!(!records.contains_key("EMPTY"));
    }

    #[test]
    fn env_vars_values_hashed_never_literal() {
        let records =
            build_env_vars(&BTreeMap::from([("FOO".to_string(), "raw-secret".to_string())]));
        let record = &records["FOO"];
        assert_eq!(record.hash, Hash::from_content(b"raw-secret"));
        assert_ne!(record.hash, Hash::from_content(b"raw-secret-other"));
    }

    #[test]
    fn env_vars_all_nondeterministic() {
        let records = build_env_vars(&BTreeMap::from([
            ("A".to_string(), "1".to_string()),
            ("B".to_string(), "2".to_string()),
        ]));
        assert!(records.values().all(|record| !record.deterministic));
    }

    #[test]
    fn env_vars_empty_input_yields_empty_records() {
        assert!(build_env_vars(&BTreeMap::new()).is_empty());
    }

    // -----------------------------------------------------------------------
    // Materialized inputs: sandbox-relative paths, sorted, hash-only
    // -----------------------------------------------------------------------

    #[test]
    fn materialized_inputs_uses_sandbox_relative_paths() {
        let key = Hash::from_content(b"payload");
        let records =
            build_materialized_inputs(&BTreeMap::from([("bin/tool".to_string(), key.to_string())]));
        assert!(records.contains_key("bin/tool"));
    }

    #[test]
    fn materialized_inputs_sorted_by_path() {
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        let records = build_materialized_inputs(&BTreeMap::from([
            ("z/last".to_string(), b.to_string()),
            ("a/first".to_string(), a.to_string()),
        ]));
        let paths: Vec<&String> = records.keys().collect();
        assert_eq!(paths, vec!["a/first", "z/last"]);
    }

    #[test]
    fn materialized_inputs_skip_non_hash_values() {
        let a = Hash::from_content(b"a");
        let records = build_materialized_inputs(&BTreeMap::from([
            ("bin/hashed".to_string(), a.to_string()),
            ("inline/bytes".to_string(), "not-a-hash".to_string()),
        ]));
        assert_eq!(records.len(), 1);
        assert!(records.contains_key("bin/hashed"));
        assert!(!records.contains_key("inline/bytes"));
    }

    #[test]
    fn materialized_inputs_all_deterministic() {
        let a = Hash::from_content(b"a");
        let records =
            build_materialized_inputs(&BTreeMap::from([("bin/tool".to_string(), a.to_string())]));
        assert!(records.values().all(|record| record.deterministic));
    }
}
