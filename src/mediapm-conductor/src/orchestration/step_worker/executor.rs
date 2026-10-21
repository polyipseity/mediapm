//! Core step execution orchestration.
//!
//! [`execute_step`] runs a step through all phases: input resolution, cache
//! probe, materialization, execution, output capture, and persistence merge.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use mediapm_cas::{CasApi, Hash};
use mediapm_utils::Timestamp;

use crate::config::OutputCaptureSpec;
use crate::error::ConductorError;
use crate::orchestration::protocol::{
    StepExecutionBundle, StepExecutionRequest, UnifiedToolSpec, find_tool_entry_by_name,
};
use crate::state::versions::derive_instance_key_v2;
use crate::state::{HashedValueRecord, PersistenceFlags, ResolvedInput, ToolCallInstance};

use super::cache::probe_cache;
use super::capture::capture_outputs;
use super::inputs::resolve_step_inputs;
use super::process::{ExecutionResult, run_builtin, run_executable_process};
use super::sandbox::{create_sandbox, materialize_content_map};
use super::template::{TemplateContext, resolve_command_parts};
use crate::instance_key::{
    deterministic_input_hashes_from_resolved_inputs, step_output_specs_hash,
};

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
        let (resolved_parts, determinism) =
            resolve_command_parts(&tool_spec.command_parts, &cmd_ctx).await?;
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

    // The captures this step will record are key material: an instance claims
    // the outputs it captured, so two steps running the same command but
    // recording different files must not share a key. The hashed set is the
    // step's own map, which is exactly what the capture phase below records:
    // the unified document does not bridge a tool's default captures, so those
    // never reach the sandbox and cannot make the instance stale.
    let output_specs_hash = step_output_specs_hash(&request.step.outputs);
    let executed_at = request.impure_timestamp.unwrap_or_else(Timestamp::now);
    let instance_key = derive_instance_key_v2(
        &tool_call_id,
        output_specs_hash,
        &deterministic_input_hashes,
        &[], // env is nondeterministic by default; never keyed
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
    // A probe hit is a claim about recorded state — an instance exists and
    // names the outputs its dependents bind. Whether the store still holds the
    // bytes those names point at is a separate fact state cannot answer, so the
    // CAS is asked directly. A missing blob turns the hit into a miss instead
    // of failing later, mid-materialization, with no route back to the step
    // that produced it: a pure step recomputes the same bytes, an impure step
    // fetches them again.
    let cached_instance = if request.force_rerun {
        None
    } else {
        match probe_cache(&instance_key, &request.state_snapshot, &request.required_output_names) {
            Some(instance)
                if required_output_blobs_exist(cas, &instance, &request.required_output_names)
                    .await =>
            {
                Some(instance)
            }
            _ => None,
        }
    };

    if let Some(cached) = cached_instance {
        // Return the cached instance unchanged; GC refresh happens through
        // the referenced-keys set at GC time, not by per-hit mutation.
        return Ok(StepExecutionBundle {
            instance: cached,
            save_modes: BTreeMap::new(),
            cache_hit: true,
        });
    }

    let sandbox_dir = create_sandbox(&request.conductor_tmp_dir, &instance_key).await?;
    materialize_content_map(cas, &tool_spec.tool_content_map, &sandbox_dir).await?;

    let (execution_result, command_args) =
        dispatch_tool_execution(cas, tool_spec, &request, &resolved_inputs, &sandbox_dir).await?;

    // Capture outputs.
    // Merge tool-level outputs (defaults) with step-level outputs (overrides).
    let merged_outputs = merge_output_specs(&tool_spec.outputs, &request.step.outputs);
    let persistence = {
        use crate::config::SaveMode;
        let any_unsaved = merged_outputs.values().any(|o| o.save == SaveMode::False);
        let any_full = merged_outputs.values().any(|o| o.save == SaveMode::Full);
        PersistenceFlags { save: !any_unsaved, force_full: any_full }
    };
    let captured =
        capture_outputs(cas, &merged_outputs, &execution_result, &sandbox_dir, persistence).await?;

    let instance = ToolCallInstance {
        instance_key,
        tool_call_id,
        impure: tool_spec.is_impure,
        executed_at,
        step_ids: BTreeSet::from([request.step.id.clone()]),
        command_args,
        env_vars,
        materialized_inputs,
        outputs: captured.records,
    };

    Ok(StepExecutionBundle { instance, save_modes: captured.save_modes, cache_hit: false })
}

/// Returns whether every output this step's dependents bind still resolves in
/// the store.
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
    required_output_names: &BTreeSet<String>,
) -> bool {
    for name in required_output_names {
        let Some(record) = instance.outputs.get(name) else {
            return false;
        };
        if cas.stat(record.hash).await.is_err() {
            return false;
        }
    }
    true
}

/// Merges tool-level output specs (defaults) with step-level output specs
/// (overrides). Step-level outputs override tool-level outputs by name,
/// and step-level outputs not present in the tool-level set are appended.
///
/// Only the capture phase reads the result. Tool-level defaults are not key
/// material because the unified document never carries them: see
/// [`crate::step_output_specs_hash`].
fn merge_output_specs(
    tool_outputs: &BTreeMap<String, OutputCaptureSpec>,
    step_outputs: &BTreeMap<String, OutputCaptureSpec>,
) -> BTreeMap<String, OutputCaptureSpec> {
    let mut merged = tool_outputs.clone();
    for (name, spec) in step_outputs {
        merged.insert(name.clone(), spec.clone());
    }
    merged
}

/// Builds the recorded environment set from configured execution env vars:
/// only non-empty values, each content-hashed (never stored literally). Every
/// record is nondeterministic — env is never keyed (the executor passes an
/// empty env-hash slice to `derive_instance_key_v2`).
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
    use std::collections::BTreeMap;

    use mediapm_cas::Hash;

    use super::{
        build_env_vars, build_materialized_inputs, derive_instance_key_v2, execute_step,
        merge_output_specs, step_output_specs_hash,
    };
    use crate::config::OutputCaptureSpec;
    use crate::orchestration::protocol::{
        StepExecutionBundle, StepExecutionRequest, UnifiedNickelDocument, UnifiedToolSpec,
    };
    use std::collections::BTreeSet;
    use std::sync::Arc;

    /// Runs one builtin step under `step_id` and returns what it recorded.
    ///
    /// The bundle is returned beside the two candidate capture terms and the
    /// input term the executor derived, so a caller can tell which capture term
    /// became key material as well as observe the instance's provenance. The
    /// tool spec is filled directly because the real path cannot produce this
    /// shape: [`crate::config::documents::NickelDocument::to_unified`] leaves a
    /// tool's `outputs` empty, which is what makes the step's map the whole
    /// capture set. Filling it here is what lets the test tell the two terms
    /// apart.
    async fn execute_builtin_step(step_id: &str) -> (StepExecutionBundle, Hash, Hash, Vec<Hash>) {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let tool_id = "echo@v1";
        let tool_outputs = BTreeMap::from([(
            "result".to_string(),
            OutputCaptureSpec {
                name: "result".to_string(),
                capture: "stdout".to_string(),
                ..OutputCaptureSpec::default()
            },
        )]);
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
            outputs: tool_outputs.clone(),
            tool_content_map: BTreeMap::new(),
            builtin_id: Some(tool_id.to_string()),
        };
        let step_outputs = BTreeMap::new();
        let request = StepExecutionRequest {
            unified: Arc::new(UnifiedNickelDocument {
                tools: BTreeMap::from([(tool_id.to_string(), tool)]),
                workflows: BTreeMap::new(),
                tool_content_hashes: BTreeSet::new(),
                external_data_policies: BTreeMap::new(),
                runtime: crate::config::ConductorRuntimeConfig::default(),
            }),
            step: crate::config::WorkflowStepSpec {
                id: step_id.to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::from([("text".to_string(), "hi".to_string())]),
                outputs: step_outputs.clone(),
                max_retries: 0,
                depends_on: Vec::new(),
            },
            impure_timestamp: None,
            state_snapshot: Arc::new(crate::state::ConductorState::new_empty()),
            outermost_config_dir: tmp.path().to_path_buf(),
            conductor_tmp_dir: tmp.path().join("runtime"),
            step_outputs: Arc::new(BTreeMap::new()),
            required_output_names: BTreeSet::from(["result".to_string()]),
            force_rerun: false,
        };
        let bundle = execute_step(&cas, request).await.expect("the echo builtin runs");
        let merged_term = step_output_specs_hash(&merge_output_specs(&tool_outputs, &step_outputs));
        let step_only_term = step_output_specs_hash(&step_outputs);
        // The one deterministic binding this step resolves, so the expected
        // keys below carry the same input term the executor derived. The input
        // term is not what this test varies.
        let input_hashes = vec![
            crate::instance_key::instance_key_hash_from_resolved_value_bytes("text", "hi", b"hi")
                .expect("a literal binding is deterministic"),
        ];
        (bundle, merged_term, step_only_term, input_hashes)
    }

    /// The executor keys a step by the captures it declares, and a tool-level
    /// capture default is not key material.
    ///
    /// A tool's default captures never reach the capture phase on the conductor
    /// path — [`crate::config::documents::NickelDocument::to_unified`] leaves
    /// the unified tool's `outputs` empty — so they are not part of what an
    /// instance claims, and a key that covered them would be keyed by something
    /// the run never records. The fixture here fills the tool map precisely so
    /// the two candidate terms differ; the assertion then says which one the
    /// executor used.
    ///
    /// This is also the tripwire for bridging those defaults later: a change
    /// that makes tool-level captures live without adding them to the capture
    /// term fails here, where the coupling is cheapest to notice.
    #[tokio::test]
    async fn a_tool_level_capture_default_is_not_key_material() {
        let (bundle, merged_term, step_only_term, input_hashes) =
            execute_builtin_step("step-0-echo").await;
        assert_ne!(
            merged_term, step_only_term,
            "the fixture must distinguish the two terms, or the assertion below cannot fail",
        );
        assert_eq!(
            bundle.instance.instance_key,
            derive_instance_key_v2("echo@v1", step_only_term, &input_hashes, &[], &[]),
            "the executor must hash the step's declared captures, which is what it records",
        );
        assert_ne!(
            bundle.instance.instance_key,
            derive_instance_key_v2("echo@v1", merged_term, &input_hashes, &[], &[]),
            "a tool-level default that the capture phase never reaches must not move the key",
        );
    }

    /// The instance a step records names that step, so the fixture's
    /// regeneration can pair instance → step without key history.
    #[tokio::test]
    async fn a_recorded_instance_names_the_step_that_recorded_it() {
        let (bundle, ..) = execute_builtin_step("step-7-echo").await;
        assert_eq!(bundle.instance.step_ids, BTreeSet::from(["step-7-echo".to_string()]));
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
