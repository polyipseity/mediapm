//! Core step execution orchestration.
//!
//! [`execute_step`] runs a step through all phases: input resolution, cache
//! probe, materialization, execution, output capture, and persistence merge.

use std::collections::BTreeMap;
use std::path::Path;

use mediapm_cas::CasApi;

use crate::config::OutputCaptureSpec;
use crate::error::ConductorError;
use crate::orchestration::protocol::{StepExecutionBundle, StepExecutionRequest, UnifiedToolSpec};
use crate::state::{PersistenceFlags, ResolvedInput, ToolCallInstance};

use super::cache::{derive_instance_key, probe_cache};
use super::capture::capture_outputs;
use super::inputs::resolve_step_inputs;
use super::process::{ExecutionResult, run_builtin, run_executable_process};
use super::sandbox::{create_sandbox, materialize_content_map};
use super::template::{TemplateContext, resolve_command_parts};

/// Dispatches execution: runs a builtin or resolves and runs an executable process.
async fn dispatch_tool_execution<C: CasApi + Send + Sync>(
    cas: &C,
    tool_spec: &UnifiedToolSpec,
    request: &StepExecutionRequest,
    resolved_inputs: &[ResolvedInput],
    sandbox_dir: &Path,
) -> Result<ExecutionResult, ConductorError> {
    if tool_spec.command_parts.is_empty() {
        let args: BTreeMap<String, String> =
            resolved_inputs.iter().map(|ri| (ri.key.clone(), ri.value.clone())).collect();
        run_builtin(&request.step.tool, &args, &request.outermost_config_dir, sandbox_dir).await
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
        let resolved_parts = resolve_command_parts(&tool_spec.command_parts, &cmd_ctx).await?;
        run_executable_process(
            &resolved_parts,
            &tool_spec.success_codes,
            sandbox_dir,
            &tool_spec.execution_env_vars,
        )
        .await
    }
}

/// Executes one step: resolves inputs, runs the tool, captures outputs.
pub(super) async fn execute_step<C: CasApi + Send + Sync>(
    cas: &C,
    request: StepExecutionRequest,
) -> Result<StepExecutionBundle, ConductorError> {
    let resolved_inputs = resolve_step_inputs::<C>(&request).await?;

    // Derive tool call instance key from tool + resolved inputs.
    let tool_spec = request.unified.tools.get(&request.step.tool).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "step '{}' references unknown tool '{}'",
            request.step.id, request.step.tool,
        ))
    })?;

    let instance_key = derive_instance_key(&resolved_inputs, request.impure_timestamp);

    // Cache probe.
    let (cache_hit, _cached_instance) =
        probe_cache(&instance_key, &request.state_snapshot, &request.required_output_names);

    if cache_hit {
        // Return a rematerialized result from cache.
        let cached =
            request.state_snapshot.tool_call_instances.get(&instance_key).ok_or_else(|| {
                ConductorError::Internal(format!(
                    "cache probe reported hit but tool call instance '{instance_key}' not found in state",
                ))
            })?;

        return Ok(StepExecutionBundle { instance: cached.clone() });
    }

    let sandbox_dir = create_sandbox(&request.conductor_tmp_dir, &instance_key).await?;
    materialize_content_map(cas, &tool_spec.tool_content_map, &sandbox_dir).await?;

    let execution_result =
        dispatch_tool_execution(cas, tool_spec, &request, &resolved_inputs, &sandbox_dir).await?;

    // Capture outputs.
    // Merge tool-level outputs (defaults) with step-level outputs (overrides).
    let merged_outputs = merge_output_specs(&tool_spec.outputs, &request.step.outputs);
    let persistence = {
        let any_unsaved = merged_outputs.values().any(|o| !o.save);
        PersistenceFlags { save: !any_unsaved, force_full: false }
    };
    let outputs =
        capture_outputs(cas, &merged_outputs, &execution_result, &sandbox_dir, persistence).await?;

    let tool_id = format!("{}@{}", tool_spec.inputs.len(), "?"); // simplified tool id for now
    let instance = ToolCallInstance {
        instance_key: instance_key.clone(),
        tool_id,
        inputs: resolved_inputs.clone(),
        outputs,
        worker_index: 0,
        executed: true,
        rematerialized: false,
        conductor_gc_last_referenced_at: crate::config::ImpureTimestamp::default(),
    };

    Ok(StepExecutionBundle { instance })
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
