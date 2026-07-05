//! Shared spec-generation types and helpers for managed-tool workflow files.
//!
//! This module provides the [`TokenSpec`] enum, option-token rendering
//! helpers, and template-literal utilities consumed by each per-tool
//! workflow module. No tool-specific constants live here.

#![allow(dead_code)]
// TODO: Step-synthesis template helpers are unused until Stream A is wired.

use std::collections::BTreeMap;

use mediapm_conductor::{
    InputBinding, OutputCaptureSpec, ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec,
};

/// Describes how one option input is rendered as CLI tokens.
///
/// Each variant carries the CLI flag string(s) to emit when the input is
/// present. The flag strings use the actual tool CLI conventions (e.g.
/// dashes for long flags) rather than deriving from the input name.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TokenSpec {
    /// `--flag=value` — emitted when the input has a non-empty value.
    Pair(&'static str),
    /// `--flag` — emitted only when the input value is `"true"`.
    Bool(&'static str),
    /// `--flag value` — emitted as a pair when the input is `"true"`.
    BoolPair(&'static str, &'static str),
    /// No token (e.g. list inputs like `option_args`).
    None,
}

// ── Unpack helpers ───────────────────────────────────────────────────────

/// Builds one unpack conditional token gated on non-empty scalar presence.
#[must_use]
pub(crate) fn unpack_if_truthy(input_name: &str, rendered_argument: &str) -> String {
    format!("${{*inputs.{input_name} ? {rendered_argument} | ''}}")
}

/// Builds one unpack conditional token gated on scalar equality.
#[must_use]
pub(crate) fn unpack_if_equals(
    input_name: &str,
    expected_value: &str,
    rendered_argument: &str,
) -> String {
    format!("${{*inputs.{input_name} == \"{expected_value}\" ? {rendered_argument} | ''}}")
}

/// Builds one scalar unpack token `${*inputs.<name>}`.
#[must_use]
pub(crate) fn unpack_scalar(input_name: &str) -> String {
    format!("${{*inputs.{input_name}}}")
}

/// Builds `${*inputs.<name> ? <flag> | ''}` + `${*inputs.<name>}` tokens.
#[must_use]
pub(crate) fn pair_option_tokens(input_name: &str, flag: &str) -> Vec<String> {
    vec![unpack_if_truthy(input_name, flag), unpack_scalar(input_name)]
}

/// Builds conditional tokens that emit one flag when the option is `"true"`.
#[must_use]
pub(crate) fn bool_flag_tokens(input_name: &str, flag: &str) -> Vec<String> {
    vec![unpack_if_equals(input_name, "true", flag)]
}

/// Builds conditional tokens that emit `flag value` when the option is `"true"`.
#[must_use]
pub(crate) fn bool_value_pair_tokens(input_name: &str, flag: &str, value: &str) -> Vec<String> {
    vec![unpack_if_equals(input_name, "true", flag), unpack_if_equals(input_name, "true", value)]
}

// ── Option-token resolution ──────────────────────────────────────────────

/// Resolves option templates for one logical tool option input.
#[must_use]
pub(crate) fn option_tokens_for_input(
    input_name: &str,
    token_specs: &[(&str, TokenSpec)],
) -> Vec<String> {
    if input_name == "option_args" {
        return vec![unpack_scalar(input_name)];
    }

    let spec = token_specs.iter().find(|(name, _)| *name == input_name).map(|(_, spec)| *spec);

    match spec {
        Some(TokenSpec::Pair(flag)) => pair_option_tokens(input_name, flag),
        Some(TokenSpec::Bool(flag)) => bool_flag_tokens(input_name, flag),
        Some(TokenSpec::BoolPair(flag, value)) => bool_value_pair_tokens(input_name, flag, value),
        Some(TokenSpec::None) => Vec::new(),
        None => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
    }
}

/// Renders option argument templates for ordered option inputs.
#[must_use]
pub(crate) fn command_option_tokens_for_tool(
    input_names: &[&str],
    token_specs: &[(&str, TokenSpec)],
) -> Vec<String> {
    input_names
        .iter()
        .flat_map(|input_name| option_tokens_for_input(input_name, token_specs))
        .collect()
}

// ── Template literal escaping ────────────────────────────────────────────

/// Escapes a literal string value for use inside conductor NCL templates.
#[must_use]
pub(crate) fn escape_template_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('%', "%%")
}

// ── Sandbox path normalization ───────────────────────────────────────────

/// Resolves a sandbox-relative path from a tool command's `content_map` entry.
#[must_use]
pub(crate) fn resolve_sandbox_path(content_map_key: &str) -> String {
    format!("inputs/{content_map_key}")
}

/// Removes sandbox path prefix from a fully qualified sandbox path.
#[must_use]
pub(crate) fn strip_sandbox_prefix(path: &str) -> &str {
    path.strip_prefix("inputs/").unwrap_or(path)
}

// ── Spec-build helpers ───────────────────────────────────────────────────

/// Builds a full [`ToolSpec`] and [`ToolRuntime`] from per-tool parts.
///
/// Callers (per-tool workflow files) provide the individual component
/// builders; this helper assembles them into the final pair.
#[must_use]
pub(crate) fn assemble_tool_spec(
    tool_name: &str,
    content_map: BTreeMap<String, String>,
    command: Vec<String>,
    inputs: BTreeMap<String, ToolInputSpec>,
    outputs: BTreeMap<String, OutputCaptureSpec>,
    default_inputs: BTreeMap<String, InputBinding>,
    impure: bool,
    max_concurrent_calls: usize,
    max_retries: usize,
) -> (ToolSpec, ToolRuntime) {
    let runtime = ToolRuntime {
        content_map,
        impure,
        inherited_env_vars: Vec::new(),
        max_concurrent_calls,
        max_retries,
    };

    let spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command,
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: tool_name.to_string(),
        inputs,
        default_inputs,
        outputs,
        runtime: runtime.clone(),
    };

    (spec, runtime)
}
