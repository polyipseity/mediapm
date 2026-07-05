//! sd workflow spec generation.
//!
//! Produces the [`ToolSpec`] and [`ToolRuntime`] for the managed `sd`
//! (string-replacement) tool. Does not produce workflow steps — sd is a
//! utility tool used internally by step expansions.

use std::collections::BTreeMap;

use mediapm_conductor::{
    InputBinding, OutputCaptureSpec, SaveMode, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec,
};

use crate::conductor_bridge::constants::{
    INPUT_CONTENT, INPUT_LEADING_ARGS, INPUT_SD_PATTERN, INPUT_SD_REPLACEMENT, INPUT_TRAILING_ARGS,
    OUTPUT_CONTENT, OUTPUT_SANDBOX_ARTIFACTS,
};

use super::spec::assemble_tool_spec;

/// Fixed sandbox file path used by `sd` in-place rewrite operations.
const SANDBOX_SD_INPUT_FILE: &str = "inputs/input.ffmeta";

/// Static default values for sd inputs.
const SD_INPUT_DEFAULTS: &[(&str, &str)] = &[("pattern", ""), ("replacement", "")];

/// Builds the sd executable command vector.
#[must_use]
fn build_sd_command(command_path: &str) -> Vec<String> {
    vec![
        command_path.to_string(),
        format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
        format!("${{*inputs.{INPUT_SD_PATTERN}}}"),
        format!("${{*inputs.{INPUT_SD_REPLACEMENT}}}"),
        format!("${{inputs.{INPUT_CONTENT}:file({SANDBOX_SD_INPUT_FILE})}}"),
        format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"),
    ]
}

/// Builds the sd input spec map.
#[must_use]
fn build_sd_inputs() -> BTreeMap<String, ToolInputSpec> {
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
            INPUT_TRAILING_ARGS.to_string(),
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
            INPUT_SD_PATTERN.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        ),
        (
            INPUT_SD_REPLACEMENT.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        ),
    ])
}

/// Builds the sd output capture spec map.
#[must_use]
fn build_sd_outputs() -> BTreeMap<String, OutputCaptureSpec> {
    BTreeMap::from([
        (
            OUTPUT_CONTENT.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_CONTENT.to_string(),
                capture: format!("file:{SANDBOX_SD_INPUT_FILE}"),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
                capture: "folder:inputs".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "stdout".to_string(),
            OutputCaptureSpec {
                name: "stdout".to_string(),
                capture: "stdout".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "stderr".to_string(),
            OutputCaptureSpec {
                name: "stderr".to_string(),
                capture: "stderr".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "process_code".to_string(),
            OutputCaptureSpec {
                name: "process_code".to_string(),
                capture: "process_code".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
    ])
}

/// Builds default input values for sd.
#[must_use]
fn build_sd_default_input_defaults() -> BTreeMap<String, InputBinding> {
    let mut defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), InputBinding::Vec(vec![])),
        (INPUT_TRAILING_ARGS.to_string(), InputBinding::Vec(vec![])),
    ]);
    for (key, value) in SD_INPUT_DEFAULTS {
        defaults.insert(key.to_string(), InputBinding::String(value.to_string()));
    }
    defaults
}

/// Builds the full [`ToolSpec`] and [`ToolRuntime`] for the managed `sd` tool.
#[must_use]
pub(crate) fn build_sd_spec(
    content_map: BTreeMap<String, String>,
    command_path: &str,
) -> (ToolSpec, ToolRuntime) {
    assemble_tool_spec(
        "sd",
        content_map,
        build_sd_command(command_path),
        build_sd_inputs(),
        build_sd_outputs(),
        build_sd_default_input_defaults(),
        false, // impure
        0,     // max_concurrent_calls
        0,     // max_retries
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conductor_bridge::constants::INPUT_CONTENT;

    #[test]
    fn build_sd_command_includes_required_tokens() {
        let command = build_sd_command("sd");
        assert!(command.iter().any(|c| c.contains(INPUT_LEADING_ARGS)));
        assert!(command.iter().any(|c| c.contains(INPUT_SD_PATTERN)));
        assert!(command.iter().any(|c| c.contains(INPUT_SD_REPLACEMENT)));
        assert!(command.iter().any(|c| c.contains(INPUT_CONTENT)));
        assert!(command.iter().any(|c| c.contains(INPUT_TRAILING_ARGS)));
    }

    #[test]
    fn build_sd_inputs_includes_sd_specific_entries() {
        let inputs = build_sd_inputs();
        assert!(inputs.contains_key(INPUT_CONTENT));
        assert!(inputs.contains_key(INPUT_SD_PATTERN));
        assert!(inputs.contains_key(INPUT_SD_REPLACEMENT));
        assert!(inputs.contains_key(INPUT_LEADING_ARGS));
        assert!(inputs.contains_key(INPUT_TRAILING_ARGS));
    }

    #[test]
    fn build_sd_outputs_includes_standard_and_content_entries() {
        let outputs = build_sd_outputs();
        assert!(outputs.contains_key(OUTPUT_CONTENT));
        assert!(outputs.contains_key(OUTPUT_SANDBOX_ARTIFACTS));
        assert!(outputs.contains_key("stdout"));
        assert!(outputs.contains_key("stderr"));
        assert!(outputs.contains_key("process_code"));
    }

    #[test]
    fn build_sd_defaults_include_empty_pattern_and_replacement() {
        let defaults = build_sd_default_input_defaults();
        assert_eq!(defaults.get("pattern"), Some(&InputBinding::String(String::new())));
        assert_eq!(defaults.get("replacement"), Some(&InputBinding::String(String::new())));
    }

    #[test]
    fn build_sd_spec_preserves_content_map() {
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/sd".into(), "abc".into());
        content_map.insert("macos/sd".into(), "def".into());
        content_map.insert("windows/sd.exe".into(), "ghi".into());

        let (_spec, runtime) = build_sd_spec(content_map.clone(), "sd");

        assert_eq!(runtime.content_map.len(), 3);
        assert_eq!(runtime.content_map["linux/sd"], "abc");
        assert_eq!(runtime.content_map["macos/sd"], "def");
        assert_eq!(runtime.content_map["windows/sd.exe"], "ghi");
    }

    #[test]
    fn build_sd_spec_returns_executable_kind() {
        let content_map = BTreeMap::new();
        let (spec, _runtime) = build_sd_spec(content_map, "sd");
        let mediapm_conductor::ToolKindSpec::Executable { command, .. } = &spec.kind else {
            panic!("expected Executable kind");
        };
        assert_eq!(command.first().unwrap(), "sd");
        assert_eq!(spec.name, "sd");
    }

    #[test]
    fn build_sd_spec_sets_impure_false() {
        let content_map = BTreeMap::new();
        let (_spec, runtime) = build_sd_spec(content_map, "sd");
        assert!(!runtime.impure);
    }
}
