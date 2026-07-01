//! Rsgain workflow step synthesis.
//!
//! Produces the conductor workflow steps for one `rsgain` loudness analysis step.
//! Also provides spec-generation functions for building the managed rsgain tool
//! definition from its command, inputs, outputs, and default configuration.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;

use mediapm_conductor::{
    OutputCaptureSpec, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec, WorkflowStepSpec,
};

use crate::conductor_bridge::constants::*;
use crate::config::{MediaSourceSpec, MediaStep};

use super::spec::{TokenSpec, assemble_tool_spec, command_option_tokens_for_tool};
use super::{
    OUTPUT_PRIMARY, qualify_step_id, resolve_step_tool_id, step_option_input_bindings,
    variant_to_output_capture_spec,
};

/// Synthesizes one rsgain workflow step (or step chain) from a media step
/// definition.
///
/// For album-mode rsgain, returns two steps (scan + tag). For single-track
/// mode, returns one step.
///
/// # Errors
///
#[must_use]
pub(crate) fn synthesize_rsgain_step_chain(
    source: &MediaSourceSpec,
    step_index: usize,
    step: &MediaStep,
) -> Vec<WorkflowStepSpec> {
    let mut steps = Vec::new();

    let step_id =
        qualify_step_id(source.id.as_deref().unwrap_or("unknown"), &format!("rsgain_{step_index}"));

    let mut inputs = BTreeMap::new();
    for (k, v) in step_option_input_bindings(step) {
        inputs.insert(k, v);
    }

    let mut outputs = BTreeMap::new();
    for (name, variant_json) in &step.output_variants {
        if let Ok(config) =
            crate::config::DecodedOutputVariantConfig::from_json_value(variant_json.clone())
        {
            outputs.insert(name.clone(), variant_to_output_capture_spec(name, &config));
        }
    }
    if outputs.is_empty() {
        outputs.insert(
            OUTPUT_PRIMARY.to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: OUTPUT_PRIMARY.to_string(),
                capture: "file:loudness.*".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
    }

    steps.push(WorkflowStepSpec {
        id: step_id,
        tool: resolve_step_tool_id(crate::config::MediaStepTool::Rsgain),
        inputs,
        outputs,
        max_retries: 0,
        depends_on: Vec::new(),
    });

    steps
}

// ---------------------------------------------------------------------------
// Spec-generation helpers — rsgain managed-tool definition
// ---------------------------------------------------------------------------

/// Internal rsgain-only input selecting sandbox materialization extension.
const INPUT_RSGAIN_INPUT_EXTENSION: &str = "input_extension";
/// File extensions supported by rsgain for in-place tag writing.
const SUPPORTED_RSGAIN_INPUT_EXTENSIONS: &[&str] = &[
    "flac", "ogg", "oga", "spx", "opus", "mp2", "mp3", "mp4", "m4a", "wma", "wv", "ape", "wav",
    "aiff", "aif", "snd", "tak",
];

const RSGAIN_INPUT_DEFAULTS: &[(&str, &str)] = &[
    ("input_extension", "flac"),
    ("album", "false"),
    ("album_mode", "false"),
    ("target_lufs", "-18"),
    ("tagmode", "i"),
    ("clip_mode", "p"),
    ("true_peak", "true"),
    ("max_peak", "0"),
    ("preserve_mtimes", "true"),
    ("album_aes77", "false"),
    ("dual_mono", "false"),
    ("lowercase", "false"),
    ("opus_mode", "2"),
    ("skip_existing", "false"),
    ("preserve_mtime", "false"),
    ("skip_tags", "false"),
    ("dry_run", "false"),
    ("quiet", "false"),
    ("output", ""),
    ("multithread", "1"),
    ("loudness", "-18"),
    ("jobs", "1"),
    ("preset", ""),
];

const RSGAIN_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("input_extension", TokenSpec::None),
    ("mode", TokenSpec::None),
    ("album", TokenSpec::Bool("--album")),
    ("album_mode", TokenSpec::Bool("--album")),
    ("album_aes77", TokenSpec::Bool("--album-aes77")),
    ("skip_existing", TokenSpec::Bool("--skip-existing")),
    ("tagmode", TokenSpec::Pair("--tagmode")),
    ("target_lufs", TokenSpec::Pair("--loudness")),
    ("loudness", TokenSpec::Pair("--loudness")),
    ("clip_mode", TokenSpec::Pair("--clip-mode")),
    ("true_peak", TokenSpec::Bool("--true-peak")),
    ("dual_mono", TokenSpec::Bool("--dual-mono")),
    ("max_peak", TokenSpec::Pair("--max-peak")),
    ("lowercase", TokenSpec::Bool("--lowercase")),
    ("id3v2_version", TokenSpec::Pair("--id3v2-version")),
    ("opus_mode", TokenSpec::Pair("--opus-mode")),
    ("jobs", TokenSpec::Pair("--multithread")),
    ("multithread", TokenSpec::Pair("--multithread")),
    ("preset", TokenSpec::Pair("--preset")),
    ("dry_run", TokenSpec::Bool("--dry-run")),
    ("output", TokenSpec::Pair("--output")),
    ("quiet", TokenSpec::Bool("--quiet")),
    ("skip_tags", TokenSpec::BoolPair("--tagmode", "s")),
    ("preserve_mtime", TokenSpec::Bool("--preserve-mtimes")),
    ("preserve_mtimes", TokenSpec::Bool("--preserve-mtimes")),
];

const RSGAIN_OPTION_INPUTS: &[&str] = &[
    "option_args",
    "input_extension",
    "mode",
    "album",
    "album_aes77",
    "skip_existing",
    "tagmode",
    "target_lufs",
    "loudness",
    "clip_mode",
    "true_peak",
    "dual_mono",
    "album_mode",
    "max_peak",
    "lowercase",
    "id3v2_version",
    "opus_mode",
    "multithread",
    "jobs",
    "preset",
    "dry_run",
    "output",
    "quiet",
    "skip_tags",
    "preserve_mtime",
    "preserve_mtimes",
    "loudness_range",
    "integrated_loudness",
    "true_peak_level",
    "lra_loudness",
    "loudness_correction",
    "sample_peak",
    "bit_depth",
    "dynamic_range",
    "dynamic_range_max",
    "dynamic_range_count",
    "dynamic_range_avg",
    "dynamic_range_stdev",
    "dynamic_range_threshold",
    "dynamic_range_histogram",
    "dynamic_range_histogram_count",
    "dynamic_range_histogram_bins",
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[must_use]
fn rsgain_input_file_path(extension: &str) -> String {
    format!("inputs/input.{extension}")
}

#[must_use]
fn rsgain_output_file_regex() -> String {
    format!("^inputs/input[.](?:{})$", SUPPORTED_RSGAIN_INPUT_EXTENSIONS.join("|"))
}

// ---------------------------------------------------------------------------
// Spec builders
// ---------------------------------------------------------------------------

#[must_use]
fn build_rsgain_command(command_path: &str) -> Vec<String> {
    let mut command = vec![
        command_path.to_string(),
        format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
        "custom".to_string(),
    ];
    command.extend(command_option_tokens_for_tool(RSGAIN_OPTION_INPUTS, RSGAIN_TOKEN_SPECS));
    command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));
    for extension in SUPPORTED_RSGAIN_INPUT_EXTENSIONS {
        let input_path = rsgain_input_file_path(extension);
        command.push(format!(
            "${{*inputs.{INPUT_RSGAIN_INPUT_EXTENSION} == \"{extension}\" ? inputs.{INPUT_CONTENT}:file({input_path}) | ''}}"
        ));
    }
    command
}

#[must_use]
fn build_rsgain_inputs() -> BTreeMap<String, ToolInputSpec> {
    let mut inputs = BTreeMap::from([
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
    ]);
    for option_input in RSGAIN_OPTION_INPUTS {
        inputs.insert(
            (*option_input).to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    }
    inputs
}

#[must_use]
fn build_rsgain_outputs() -> BTreeMap<String, OutputCaptureSpec> {
    BTreeMap::from([
        (
            OUTPUT_CONTENT.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_CONTENT.to_string(),
                capture: format!("file_regex:{}", rsgain_output_file_regex()),
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

#[must_use]
fn build_rsgain_default_input_defaults() -> BTreeMap<String, String> {
    let mut defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), String::new()),
        (INPUT_TRAILING_ARGS.to_string(), String::new()),
    ]);
    for option_input in RSGAIN_OPTION_INPUTS {
        defaults.entry((*option_input).to_string()).or_default();
    }
    for (key, value) in RSGAIN_INPUT_DEFAULTS {
        defaults.insert(key.to_string(), value.to_string());
    }
    defaults
}

/// Builds the full [`ToolSpec`] and [`ToolRuntime`] for the managed rsgain tool.
#[must_use]
pub(crate) fn build_rsgain_spec(
    content_map: BTreeMap<String, String>,
    command_path: &str,
) -> (ToolSpec, ToolRuntime) {
    assemble_tool_spec(
        "rsgain",
        content_map,
        build_rsgain_command(command_path),
        build_rsgain_inputs(),
        build_rsgain_outputs(),
        build_rsgain_default_input_defaults(),
        false, // impure
        0,     // max_concurrent_calls
        0,     // max_retries
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_rsgain_outputs_use_regex_for_content() {
        let outputs = build_rsgain_outputs();
        assert!(outputs.contains_key(OUTPUT_CONTENT));
        let content = &outputs[OUTPUT_CONTENT];
        assert!(content.capture.starts_with("file_regex:"), "got: {}", content.capture);
    }

    #[test]
    fn build_rsgain_outputs_include_standard_captures() {
        let outputs = build_rsgain_outputs();
        assert!(outputs.contains_key("stdout"), "missing stdout output");
        assert!(outputs.contains_key("stderr"), "missing stderr output");
        assert!(outputs.contains_key("process_code"), "missing process_code output");
    }

    #[test]
    fn build_rsgain_defaults_match_expected_loudness_profile() {
        let defaults = build_rsgain_default_input_defaults();
        assert_eq!(defaults.get("target_lufs").map(String::as_str), Some("-18"));
        assert_eq!(defaults.get("album").map(String::as_str), Some("false"));
        assert_eq!(defaults.get("tagmode").map(String::as_str), Some("i"));
        assert_eq!(defaults.get("true_peak").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("clip_mode").map(String::as_str), Some("p"));
        assert_eq!(defaults.get("max_peak").map(String::as_str), Some("0"));
    }

    #[test]
    fn build_rsgain_command_includes_input_extension_conditionals() {
        let command = build_rsgain_command("rsgain");
        assert!(command.iter().any(|c| c.contains("custom")), "expected 'custom' subcommand");
        assert!(
            command.iter().any(|c| c.contains("input_extension")),
            "expected input_extension conditionals"
        );
    }
}
