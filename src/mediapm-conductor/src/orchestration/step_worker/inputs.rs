//! Step input resolution via template interpolation.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::{CasApi, Hash};

use crate::error::ConductorError;
use crate::orchestration::protocol::{StepExecutionRequest, find_tool_by_name};
// Test-only: `StepOutputs` is referenced solely by the legacy
// `resolve_step_output_refs` shim and its tests.
#[cfg(test)]
use crate::orchestration::protocol::StepOutputs;
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
        staged_inputs: None,
    };

    // Collect all input keys: the tool spec declares its known input
    // contract, but steps may also bind inputs directly (e.g. builtin steps
    // like import bind `kind`/`hash` without declaring them on the tool
    // spec); union both sets so step-declared bindings are never dropped.
    let mut all_keys: BTreeSet<&String> = tool_spec.inputs.keys().collect();
    all_keys.extend(request.step.inputs.keys());

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

/// The value bytes one step's instance key hashes, and the bindings that could
/// not be resolved.
///
/// A binding that raised is reported rather than dropped: the key derived
/// without it is not the step's key, and a caller that cannot tell the
/// difference would match an instance recorded from different work.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InstanceKeyInputValues {
    /// Binding name to the bytes the template resolved to.
    pub values: BTreeMap<String, Vec<u8>>,
    /// Binding names whose template did not resolve.
    pub unresolved: BTreeSet<String>,
}

/// Resolves the values an instance key hashes, the way the step worker resolves
/// them.
///
/// The key of a step is derived twice: once by the worker that runs it, and once
/// by a reader that has only the document and the recorded instances. The two
/// only agree while both hash the same bytes, so the reader calls this function
/// rather than re-implementing resolution: it builds the context
/// `resolve_step_inputs` builds — no store handle, the step's own bindings as
/// `${inputs.…}`, the recorded step outputs as `${step_output.…}`, the host OS
/// as `${context.os}`, and no environment — and resolves every binding through
/// the same engine.
///
/// `bindings` is the step's input map merged with the tool's declared defaults,
/// which is the map the worker iterates; `step_inputs` is the step's own map,
/// which is what `${inputs.…}` reads inside a default.
///
/// A binding whose template raises is reported in
/// [`InstanceKeyInputValues::unresolved`] instead of being hashed from its
/// unexpanded text.
pub async fn resolve_instance_key_input_values<C: CasApi + Send + Sync>(
    bindings: &BTreeMap<String, String>,
    step_inputs: &BTreeMap<String, String>,
    step_outputs: &BTreeMap<String, BTreeMap<String, Hash>>,
) -> InstanceKeyInputValues {
    let env_vars = BTreeMap::new();
    let tokens = BTreeMap::new();
    let ctx = TemplateContext::<C> {
        cas: None,
        step_outputs,
        env_vars: &env_vars,
        tokens: &tokens,
        sandbox_dir: None,
        host_os: std::env::consts::OS,
        inputs: step_inputs,
        staged_inputs: None,
    };

    let mut resolved = InstanceKeyInputValues::default();
    for (key, raw_binding) in bindings {
        match resolve_template(raw_binding, &ctx).await {
            Ok(value) => {
                resolved.values.insert(key.clone(), value.into_bytes());
            }
            Err(_) => {
                resolved.unresolved.insert(key.clone());
            }
        }
    }
    resolved
}

/// Resolves `${step_output.<step_id>.<name>}` references only (legacy).
///
/// Uses regex-based substitution to remain synchronous and runtime-independent.
/// The async [`resolve_step_inputs`] entry point uses the full template engine.
///
/// Test-only: superseded by that engine's own `step_output.` handling in
/// [`super::template`], which every production path goes through. This shim
/// is retained so the legacy substitution contract stays covered by tests.
#[cfg(test)]
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

    /// A binding that reads the host OS resolves to the OS name, not to its
    /// template.
    ///
    /// The demo's tools bind `js_runtimes` to
    /// `deno:${context.os == "linux" ? … | …}`. The worker still resolves the
    /// branch, because the command it runs names the selected path; the key
    /// material hashes the binding's text instead, because the branch chooses
    /// where the payload lives rather than what the step does.
    #[tokio::test]
    async fn a_context_reading_binding_resolves_to_the_host_os_but_does_not_key_it() {
        let mut bindings = BTreeMap::new();
        bindings.insert(
            "js_runtimes".to_string(),
            "deno:${context.os == \"linux\" ? deps/deno/linux/deno : deps/deno/other/deno}"
                .to_string(),
        );
        let step_inputs = bindings.clone();
        let step_outputs = BTreeMap::new();

        let resolved = resolve_instance_key_input_values::<mediapm_cas::InMemoryCas>(
            &bindings,
            &step_inputs,
            &step_outputs,
        )
        .await;

        let expected = if std::env::consts::OS == "linux" {
            "deno:deps/deno/linux/deno"
        } else {
            "deno:deps/deno/other/deno"
        };
        assert_eq!(
            resolved.values.get("js_runtimes").map(Vec::as_slice),
            Some(expected.as_bytes()),
            "a context-reading binding must resolve to the host's branch, not to its template",
        );
        assert!(resolved.unresolved.is_empty(), "every binding here resolves");

        // Resolution and key material answer different questions, and this pins
        // the difference: the resolved value is the host's branch, while the key
        // material hashes the binding's text, so the two hosts share one key.
        let parts = vec!["--js-runtimes".to_string(), "${inputs.js_runtimes}".to_string()];
        let other_host = if std::env::consts::OS == "linux" {
            b"deno:deps/deno/other/deno".to_vec()
        } else {
            b"deno:deps/deno/linux/deno".to_vec()
        };
        let other_values = BTreeMap::from([("js_runtimes".to_string(), other_host)]);
        assert_eq!(
            crate::instance_key::key_material_for_tool(&parts, &resolved.values, &bindings),
            crate::instance_key::key_material_for_tool(&parts, &other_values, &bindings),
            "a binding that reads only the host must key its text, not the branch it selects",
        );
        let composed = Hash::composite(&[
            Hash::from_content(parts[1].as_bytes()),
            Hash::from_content(bindings["js_runtimes"].as_bytes()),
        ]);
        assert_eq!(
            crate::instance_key::key_material_for_tool(&parts, &resolved.values, &bindings)[1],
            composed,
            "the term is the binding's text composed with the part that reads it",
        );
    }

    /// A binding that reads a step output resolves to that output's hash text.
    ///
    /// The worker runs with no store handle, so `${step_output.…}` hands the
    /// hash on rather than fetching bytes; a reader that fetched them would hash
    /// content the worker never hashed.
    #[tokio::test]
    async fn a_step_output_binding_resolves_to_the_hash_text() {
        let hash = Hash::from_content(b"primary");
        let mut outputs = BTreeMap::new();
        outputs.insert("primary".to_string(), hash);
        let mut step_outputs = BTreeMap::new();
        step_outputs.insert("step-1".to_string(), outputs);
        let mut bindings = BTreeMap::new();
        bindings.insert("input".to_string(), "${step_output.step-1.primary}".to_string());

        let resolved = resolve_instance_key_input_values::<mediapm_cas::InMemoryCas>(
            &bindings,
            &bindings,
            &step_outputs,
        )
        .await;

        assert_eq!(
            resolved.values.get("input").map(Vec::as_slice),
            Some(hash.to_string().as_bytes()),
        );
    }

    /// A binding whose template raises is reported, never hashed as written.
    #[tokio::test]
    async fn an_unresolvable_binding_is_reported() {
        let mut bindings = BTreeMap::new();
        bindings.insert("path".to_string(), "${step_output.absent.primary}".to_string());
        let step_outputs = BTreeMap::new();

        let resolved = resolve_instance_key_input_values::<mediapm_cas::InMemoryCas>(
            &bindings,
            &bindings,
            &step_outputs,
        )
        .await;

        assert!(!resolved.values.contains_key("path"), "an unresolved binding has no value");
        assert!(
            resolved.unresolved.contains("path"),
            "an unresolved binding must be named, or its absence reads as an empty value",
        );
    }

    /// Verifies that an empty `InputBinding::Vec` resolves as an empty JSON array.
    #[test]
    fn empty_vec_binding_resolves_as_empty_json_array() {
        let binding = crate::config::InputBinding::Vec(vec![]);
        let resolved = match binding {
            crate::config::InputBinding::String(s) => s,
            crate::config::InputBinding::Vec(v) => serde_json::to_string(&v).unwrap_or_default(),
        };
        assert_eq!(resolved, r"[]");
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
