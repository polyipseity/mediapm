//! deno workflow spec generation.
//!
//! Produces the [`ToolSpec`] and [`ToolRuntime`] for the managed `deno`
//! JavaScript/TypeScript runtime (used as a companion dependency for tools
//! like `yt-dlp`). Does not produce workflow steps.
//!
//! The spec is minimal — no tool-specific inputs, outputs, or command
//! tokens beyond the common leading/trailing args. deno is not run as a
//! mediapm workflow step; it is only provisioned as a managed binary that
//! companion tools can invoke.

use std::collections::BTreeMap;

use mediapm_conductor::{
    InputBinding, OutputCaptureSpec, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec,
};

use crate::conductor_bridge::constants::{
    INPUT_CONTENT, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS, OUTPUT_CONTENT,
    OUTPUT_SANDBOX_ARTIFACTS,
};

use super::spec::assemble_tool_spec;

/// Fixed sandbox path used for deno input materialization.
const SANDBOX_INPUT_FILE: &str = "inputs/input.bin";

/// Builds the deno executable command vector.
///
/// Unlike media-processing tools, deno receives no tool-specific arguments —
/// only the executable path and the standard leading/trailing arg slots.
#[must_use]
fn build_deno_command(command_path: &str) -> Vec<String> {
    vec![
        command_path.to_string(),
        format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
        format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"),
    ]
}

/// Builds the deno input spec map.
///
/// Includes the common leading-args and trailing-args inputs plus an unused
/// `INPUT_CONTENT` slot for consistency with the legacy fallthrough path.
/// deno has no tool-specific option inputs in the mediapm workflow model.
#[must_use]
fn build_deno_inputs() -> BTreeMap<String, ToolInputSpec> {
    BTreeMap::from([
        (
            INPUT_LEADING_ARGS.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        ),
        (
            INPUT_CONTENT.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        ),
        (
            INPUT_TRAILING_ARGS.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        ),
    ])
}

/// Builds the deno output capture spec map.
///
/// Provides the standard content/sandbox-artifacts captures plus terminal
/// output signals. No yt-dlp-style artifact bundles or tool-specific
/// captures are needed.
#[must_use]
fn build_deno_outputs() -> BTreeMap<String, OutputCaptureSpec> {
    BTreeMap::from([
        (
            OUTPUT_CONTENT.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_CONTENT.to_string(),
                capture: format!("file:{SANDBOX_INPUT_FILE}"),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
                capture: "folder:inputs".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "stdout".to_string(),
            OutputCaptureSpec {
                name: "stdout".to_string(),
                capture: "stdout".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "stderr".to_string(),
            OutputCaptureSpec {
                name: "stderr".to_string(),
                capture: "stderr".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "process_code".to_string(),
            OutputCaptureSpec {
                name: "process_code".to_string(),
                capture: "process_code".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
    ])
}

/// Builds default input values for deno.
///
/// Leading/trailing args and content all default to empty — deno receives no
/// mandatory arguments from mediapm.
#[must_use]
fn build_deno_default_input_defaults() -> BTreeMap<String, InputBinding> {
    BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), InputBinding::Vec(vec![])),
        (INPUT_CONTENT.to_string(), InputBinding::String(String::new())),
        (INPUT_TRAILING_ARGS.to_string(), InputBinding::Vec(vec![])),
    ])
}

/// Builds the full [`ToolSpec`] and [`ToolRuntime`] for the managed `deno` tool.
///
/// deno is a pure tool with no network access requirements. It is provisioned
/// as a managed binary for companion tool use, not as a workflow step runner.
#[must_use]
pub(crate) fn build_deno_spec(
    content_map: BTreeMap<String, String>,
    command_path: &str,
) -> (ToolSpec, ToolRuntime) {
    assemble_tool_spec(
        "deno",
        content_map,
        build_deno_command(command_path),
        build_deno_inputs(),
        build_deno_outputs(),
        build_deno_default_input_defaults(),
        false, // impure
        0,     // max_concurrent_calls
        0,     // max_retries
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_deno_command_includes_basic_tokens() {
        let command = build_deno_command("/tools/deno");
        assert_eq!(command.len(), 3);
        assert_eq!(command[0], "/tools/deno");
        assert!(command[1].contains("leading_args"));
        assert!(command[2].contains("trailing_args"));
    }

    #[test]
    fn build_deno_inputs_has_common_inputs_and_content() {
        let inputs = build_deno_inputs();
        assert_eq!(inputs.len(), 3);
        assert!(inputs.contains_key(INPUT_LEADING_ARGS));
        assert!(inputs.contains_key(INPUT_CONTENT));
        assert!(inputs.contains_key(INPUT_TRAILING_ARGS));
    }

    #[test]
    fn build_deno_outputs_includes_standard_captures() {
        let outputs = build_deno_outputs();
        assert!(outputs.contains_key(OUTPUT_CONTENT));
        assert!(outputs.contains_key(OUTPUT_SANDBOX_ARTIFACTS));
        assert!(outputs.contains_key("stdout"));
        assert!(outputs.contains_key("stderr"));
        assert!(outputs.contains_key("process_code"));
    }

    #[test]
    fn build_deno_spec_sets_correct_runtime_defaults() {
        let content_map = BTreeMap::from([("bin/deno".to_string(), "abc123".to_string())]);
        let (_spec, runtime) = build_deno_spec(content_map, "/tools/deno");
        assert!(!runtime.impure);
        assert_eq!(runtime.max_concurrent_calls, 0);
        assert_eq!(runtime.max_retries, 0);
    }
}
