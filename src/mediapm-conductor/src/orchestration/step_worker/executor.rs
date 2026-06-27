//! Core step execution orchestration.
//!
//! [`execute_step`] runs a step through all phases: input resolution, cache
//! probe, materialization, execution, output capture, and persistence merge.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use mediapm_cas::CasApi;

use crate::config::OutputCaptureSpec;
use crate::error::ConductorError;
use crate::orchestration::protocol::{StepExecutionBundle, StepExecutionRequest, StepPhaseTiming};
use crate::state::{PersistenceFlags, ToolCallInstance};

use super::cache::{derive_instance_key, probe_cache};
use super::capture::capture_outputs;
use super::inputs::resolve_step_inputs;
use super::process::{run_builtin, run_executable_process};
use super::sandbox::{create_sandbox, materialize_content_map};
use super::template::{TemplateContext, resolve_command_parts};

/// Executes one step: resolves inputs, runs the tool, captures outputs.
#[allow(clippy::too_many_lines)]
pub(super) async fn execute_step<C: CasApi + Send + Sync>(
    cas: &C,
    request: StepExecutionRequest,
) -> Result<StepExecutionBundle, ConductorError> {
    let start = Instant::now();
    let mut phase_timings = StepPhaseTiming::default();

    // Phase 1: Resolve inputs.
    let t0 = Instant::now();
    let resolved_inputs = resolve_step_inputs::<C>(&request).await?;
    phase_timings.resolve_inputs_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Derive tool call instance key from tool + resolved inputs.
    let tool_spec = request.unified.tools.get(&request.step.tool).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "step '{}' references unknown tool '{}'",
            request.step.id, request.step.tool,
        ))
    })?;

    let instance_key = derive_instance_key(tool_spec, &resolved_inputs, request.impure_timestamp);

    // Phase 2: Cache probe.
    let t1 = Instant::now();
    let (cache_hit, _cached_instance) =
        probe_cache(&instance_key, &request.state_snapshot, &request.required_output_names);
    phase_timings.cache_probe_ms = t1.elapsed().as_secs_f64() * 1000.0;

    if cache_hit {
        // Return a rematerialized result from cache.
        let cached =
            request.state_snapshot.tool_call_instances.get(&instance_key).ok_or_else(|| {
                ConductorError::Internal(format!(
                    "cache probe reported hit but tool call instance '{instance_key}' not found in state",
                ))
            })?;

        return Ok(StepExecutionBundle {
            step_id: request.step.id.clone(),
            tool_name: request.step.tool.clone(),
            worker_index: 0,
            instance_key,
            instance: cached.clone(),
            requested_output_names: request.required_output_names.into_iter().collect(),
            executed: false,
            rematerialized: true,
            pending_unsaved_hashes: BTreeSet::new(),
            elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
            phase_timings,
        });
    }

    // Phase 3: Materialize tool content map.
    let t2 = Instant::now();
    let sandbox_dir = create_sandbox(&request.conductor_tmp_dir, &instance_key).await?;
    materialize_content_map(cas, &tool_spec.tool_content_map, &sandbox_dir).await?;
    phase_timings.materialization_ms = t2.elapsed().as_secs_f64() * 1000.0;

    // Phase 4: Execute.
    let t3 = Instant::now();
    let execution_result = if tool_spec.command_parts.is_empty() {
        // Builtin execution: collect inputs as args map.
        let args: BTreeMap<String, String> =
            resolved_inputs.iter().map(|ri| (ri.key.clone(), ri.value.clone())).collect();
        run_builtin(&request.step.tool, &args, &request.outermost_config_dir, &sandbox_dir).await?
    } else {
        // Resolve template references in command parts against resolved inputs.
        let resolved_inputs_map: BTreeMap<String, String> =
            resolved_inputs.iter().map(|ri| (ri.key.clone(), ri.value.clone())).collect();
        let cmd_ctx = TemplateContext::<C> {
            cas: Some(cas),
            step_outputs: &request.step_outputs,
            env_vars: &BTreeMap::new(),
            tokens: &BTreeMap::new(),
            sandbox_dir: Some(&sandbox_dir),
            host_os: std::env::consts::OS,
            inputs: &resolved_inputs_map,
        };
        let resolved_parts = resolve_command_parts(&tool_spec.command_parts, &cmd_ctx).await?;
        run_executable_process(
            &resolved_parts,
            &tool_spec.success_codes,
            &sandbox_dir,
            &tool_spec.execution_env_vars,
        )
        .await?
    };
    phase_timings.execution_ms = t3.elapsed().as_secs_f64() * 1000.0;

    // Phase 5: Capture outputs.
    let t4 = Instant::now();
    // Merge tool-level outputs (defaults) with step-level outputs (overrides).
    let merged_outputs = merge_output_specs(&tool_spec.outputs, &request.step.outputs);
    let persistence = {
        let any_unsaved = merged_outputs.values().any(|o| !o.save);
        PersistenceFlags { save: !any_unsaved, force_full: false }
    };
    let outputs =
        capture_outputs(cas, &merged_outputs, &execution_result, &sandbox_dir, persistence).await?;
    phase_timings.capture_outputs_ms = t4.elapsed().as_secs_f64() * 1000.0;

    // Phase 6: Persistence merge.
    let t5 = Instant::now();
    phase_timings.persistence_merge_ms = t5.elapsed().as_secs_f64() * 1000.0;

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

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

    Ok(StepExecutionBundle {
        step_id: request.step.id.clone(),
        tool_name: request.step.tool.clone(),
        worker_index: 0,
        instance_key,
        instance,
        requested_output_names: request.required_output_names.into_iter().collect(),
        executed: true,
        rematerialized: false,
        pending_unsaved_hashes: BTreeSet::new(),
        elapsed_ms,
        phase_timings,
    })
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
