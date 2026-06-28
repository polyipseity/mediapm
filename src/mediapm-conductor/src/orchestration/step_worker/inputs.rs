//! Step input resolution via template interpolation.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::CasApi;

use crate::error::ConductorError;
use crate::orchestration::protocol::{StepExecutionRequest, StepOutputs};
use crate::state::ResolvedInput;

use super::template::{TemplateContext, resolve_template};

/// Resolves step inputs from the step's declared inputs + tool defaults
/// using the full template interpolation engine.
pub(super) async fn resolve_step_inputs<C: CasApi + Send + Sync>(
    request: &StepExecutionRequest,
) -> Result<Vec<ResolvedInput>, ConductorError> {
    let tool_spec = request.unified.tools.get(&request.step.tool).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "step '{}' references unknown tool '{}'",
            request.step.id, request.step.tool,
        ))
    })?;

    let mut resolved = Vec::new();

    // Build a TemplateContext from the request.
    let ctx = TemplateContext::<C> {
        cas: None, // step-workers don't have direct CAS for template resolution
        step_outputs: &request.step_outputs,
        env_vars: &BTreeMap::new(), // env vars are not passed through step execution
        tokens: &BTreeMap::new(),
        sandbox_dir: None,
        host_os: std::env::consts::OS,
        inputs: &request.step.inputs,
    };

    // Collect all input keys from tool spec.
    let all_keys: BTreeSet<&String> = tool_spec.inputs.keys().collect();

    for key in all_keys {
        let value = request
            .step
            .inputs
            .get(key)
            .or_else(|| tool_spec.default_inputs.get(key))
            .cloned()
            .unwrap_or_default();

        // Resolve all template references via the full engine.
        let resolved_value = resolve_template(&value, &ctx).await?;

        resolved.push(ResolvedInput { key: key.clone(), value: resolved_value });
    }

    Ok(resolved)
}

/// Resolves `${step_output.<step_id>.<name>}` references only (legacy).
///
/// Uses regex-based substitution to remain synchronous and runtime-independent.
/// The async [`resolve_step_inputs`] entry point uses the full template engine.
#[cfg_attr(not(test), expect(dead_code))]
pub(super) fn resolve_step_output_refs(
    value: &str,
    step_outputs: &StepOutputs,
) -> Result<String, ConductorError> {
    let re =
        regex::Regex::new(r"\$\{step_output\.([^.]+)\.([^}]+)\}").expect("valid step output regex");
    let mut result = value.to_string();

    for cap in re.captures_iter(value) {
        let dep_step_id = cap[1].to_string();
        let output_name = cap[2].to_string();

        let output_hash = step_outputs
            .get(&dep_step_id)
            .and_then(|outputs| outputs.get(&output_name))
            .ok_or_else(|| {
                ConductorError::Workflow(format!(
                    "step output '${{step_output.{dep_step_id}.{output_name}}}' not found",
                ))
            })?;

        result = result.replace(&cap[0], &output_hash.to_string());
    }

    Ok(result)
}
