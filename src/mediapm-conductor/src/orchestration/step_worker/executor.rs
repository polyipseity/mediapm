//! Core step execution orchestration.
//!
//! [`execute_step`] runs a step through all phases: input resolution, cache
//! probe, materialization, execution, output capture, and persistence merge.

use std::collections::BTreeMap;
use std::path::Path;

use mediapm_cas::{CasApi, Hash};
use mediapm_utils::Timestamp;

use crate::config::OutputCaptureSpec;
use crate::error::ConductorError;
use crate::orchestration::protocol::{StepExecutionBundle, StepExecutionRequest, UnifiedToolSpec};
use crate::state::versions::derive_instance_key_v2;
use crate::state::{HashedValueRecord, PersistenceFlags, ResolvedInput, ToolCallInstance};

use super::cache::probe_cache;
use super::capture::capture_outputs;
use super::inputs::resolve_step_inputs;
use super::process::{ExecutionResult, run_builtin, run_executable_process};
use super::sandbox::{create_sandbox, materialize_content_map};
use super::template::{TemplateContext, is_deterministic_command_part, resolve_command_parts};

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
            run_builtin(builtin_id, &args, &request.outermost_config_dir, sandbox_dir).await?;
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
    let (tool_call_id, tool_spec) = request
        .unified
        .tools
        .iter()
        .find(|(_, spec)| spec.name == request.step.tool)
        .map(|(key, spec)| (key.clone(), spec))
        .ok_or_else(|| {
            ConductorError::Workflow(format!(
                "step '{}' references unknown tool '{}'",
                request.step.id, request.step.tool,
            ))
        })?;

    // Deterministic resolved-input hashes sorted by input key. A resolved
    // input participates in the key only when its raw binding source (step
    // input or tool default) is deterministic; env-derived and OS-conditional
    // bindings are excluded so a different host cannot produce a false hit.
    let mut deterministic_input_hashes = Vec::new();
    for ri in &resolved_inputs {
        let raw_source = request
            .step
            .inputs
            .get(&ri.key)
            .cloned()
            .or_else(|| {
                tool_spec.default_inputs.get(&ri.key).map(|binding| match binding {
                    crate::config::InputBinding::String(s) => s.clone(),
                    crate::config::InputBinding::Vec(v) => {
                        serde_json::to_string(v).unwrap_or_default()
                    }
                })
            })
            .unwrap_or_default();
        if is_deterministic_command_part(&raw_source) {
            deterministic_input_hashes.push(Hash::from_content(ri.value.as_bytes()));
        }
    }

    // Materialized content-map inputs keyed by sandbox-relative path, sorted
    // by path. Only hash-valued entries participate in the key (inline bytes
    // are skipped); content hashes are tool-fixed and deterministic.
    let materialized_inputs = build_materialized_inputs(&tool_spec.tool_content_map);
    let deterministic_materialized_hashes: Vec<Hash> =
        materialized_inputs.values().map(|record| record.hash).collect();

    let executed_at = request.impure_timestamp.unwrap_or_else(Timestamp::now);
    let instance_key = derive_instance_key_v2(
        &tool_call_id,
        tool_spec.is_impure,
        request.impure_timestamp.map_or(0, Timestamp::as_unix_nanos),
        &deterministic_input_hashes,
        &[], // env is nondeterministic by default; never keyed
        &deterministic_materialized_hashes,
    );

    // Environment values are stored hashed (never literally); the minimal set
    // is the non-empty configured execution env vars.
    let env_vars = build_env_vars(&tool_spec.execution_env_vars);

    // Cache probe.
    let (cache_hit, _cached_instance) =
        probe_cache(&instance_key, &request.state_snapshot, &request.required_output_names);

    if cache_hit {
        // Return the cached instance unchanged; GC refresh happens through
        // the referenced-keys set at GC time, not by per-hit mutation.
        let cached =
            request.state_snapshot.tool_call_instances.get(&instance_key).ok_or_else(|| {
                ConductorError::Internal(format!(
                    "cache probe reported hit but tool call instance '{}' not found in state",
                    instance_key.to_hex(),
                ))
            })?;
        return Ok(StepExecutionBundle {
            instance: cached.clone(),
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
        command_args,
        env_vars,
        materialized_inputs,
        outputs: captured.records,
    };

    Ok(StepExecutionBundle { instance, save_modes: captured.save_modes, cache_hit: false })
}

/// Merge tool-level output specs (defaults) with step-level output specs
/// (overrides). Step-level outputs override tool-level outputs by name,
/// and step-level outputs not present in the tool-level set are appended.
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

    use super::{build_env_vars, build_materialized_inputs};
    use crate::config::{OutputCaptureSpec, SaveMode};

    use super::merge_output_specs;

    fn spec(name: &str, capture: &str) -> (String, OutputCaptureSpec) {
        (
            name.to_string(),
            OutputCaptureSpec {
                name: name.to_string(),
                capture: capture.to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: false,
            },
        )
    }

    #[test]
    fn merge_output_specs_step_overrides_tool() {
        let merged = merge_output_specs(
            &BTreeMap::from([spec("r", "stdout")]),
            &BTreeMap::from([spec("r", "stderr")]),
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged["r"].capture, "stderr");
    }

    #[test]
    fn merge_output_specs_appends_new() {
        let merged = merge_output_specs(
            &BTreeMap::from([spec("a", "stdout")]),
            &BTreeMap::from([spec("b", "stderr")]),
        );
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_output_specs_empty_tool_uses_step() {
        let merged = merge_output_specs(&BTreeMap::new(), &BTreeMap::from([spec("x", "stdout")]));
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn merge_output_specs_empty_step_uses_tool() {
        let merged =
            merge_output_specs(&BTreeMap::from([spec("y", "process_code")]), &BTreeMap::new());
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn merge_output_specs_partial_override() {
        let merged = merge_output_specs(
            &BTreeMap::from([spec("a", "stdout"), spec("b", "stderr")]),
            &BTreeMap::from([spec("a", "file:out.txt")]),
        );
        assert_eq!(merged.len(), 2);
        assert_eq!(merged["a"].capture, "file:out.txt");
        assert_eq!(merged["b"].capture, "stderr");
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
