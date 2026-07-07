//! Step input resolution via template interpolation.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::CasApi;

use crate::error::ConductorError;
use crate::orchestration::protocol::{StepExecutionRequest, StepOutputs, find_tool_by_name};
use crate::state::ResolvedInput;

use super::template::{TemplateContext, resolve_template};

/// Resolves step inputs from the step's declared inputs + tool defaults
/// using the full template interpolation engine.
pub(super) async fn resolve_step_inputs<C: CasApi + Send + Sync>(
    request: &StepExecutionRequest,
) -> Result<Vec<ResolvedInput>, ConductorError> {
    let tool_spec =
        find_tool_by_name(&request.unified.tools, &request.step.tool).ok_or_else(|| {
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
        let value: String = request
            .step
            .inputs
            .get(key)
            .cloned()
            .or_else(|| {
                tool_spec.default_inputs.get(key).map(|binding| match binding {
                    crate::config::InputBinding::String(s) => s.clone(),
                    crate::config::InputBinding::Vec(v) => {
                        serde_json::to_string(v).unwrap_or_default()
                    }
                })
            })
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies that `InputBinding::String` resolves as a plain string value.
    #[test]
    fn string_binding_resolves_as_plain_string() {
        let binding = crate::config::InputBinding::String("hello".to_string());
        let resolved = match binding {
            crate::config::InputBinding::String(s) => s,
            crate::config::InputBinding::Vec(v) => serde_json::to_string(&v).unwrap_or_default(),
        };
        assert_eq!(resolved, "hello");
    }

    /// Verifies that `InputBinding::Vec` resolves as a JSON-encoded array.
    #[test]
    fn vec_binding_resolves_as_json_array() {
        let binding = crate::config::InputBinding::Vec(vec!["a".to_string(), "b".to_string()]);
        let resolved = match binding {
            crate::config::InputBinding::String(s) => s,
            crate::config::InputBinding::Vec(v) => serde_json::to_string(&v).unwrap_or_default(),
        };
        assert_eq!(resolved, r#"["a","b"]"#);
    }

    /// Verifies that an empty `InputBinding::Vec` resolves as an empty JSON array.
    #[test]
    fn empty_vec_binding_resolves_as_empty_json_array() {
        let binding = crate::config::InputBinding::Vec(vec![]);
        let resolved = match binding {
            crate::config::InputBinding::String(s) => s,
            crate::config::InputBinding::Vec(v) => serde_json::to_string(&v).unwrap_or_default(),
        };
        assert_eq!(resolved, r#"[]"#);
    }

    /// Verifies `resolve_step_output_refs` replaces a known step output reference.
    #[test]
    fn resolve_step_output_refs_replaces_known_reference() {
        let mut step_outputs = StepOutputs::new();
        let mut outputs = BTreeMap::new();
        outputs.insert("result".to_string(), mediapm_cas::Hash::from_bytes([1; 32]));
        step_outputs.insert("step1".to_string(), outputs);

        let result =
            resolve_step_output_refs("${step_output.step1.result}", &step_outputs).unwrap();
        assert!(result.starts_with("blake3:"), "expected blake3 hash prefix, got: {result}");
    }

    /// Verifies `resolve_step_output_refs` errors on missing step output.
    #[test]
    fn resolve_step_output_refs_errors_on_missing_reference() {
        let step_outputs = StepOutputs::new();
        let result = resolve_step_output_refs("${step_output.missing.out}", &step_outputs);
        assert!(result.is_err());
    }
}
