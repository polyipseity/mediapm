//! Rsgain workflow step synthesis.
//!
//! Produces the conductor workflow steps for one `rsgain` loudness analysis step.
//! Also provides spec-generation functions for building the managed rsgain tool
//! definition from its command, inputs, outputs, and default configuration.

#![allow(dead_code)]

use std::collections::BTreeMap;

use mediapm_conductor::{
    InputBinding, OutputCaptureSpec, SaveMode, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec,
    WorkflowSpec, WorkflowStepSpec,
};

use mediapm_conductor::tools::helpers::build_os_conditional_selector;

use crate::conductor_bridge::constants::{
    INPUT_CONTENT, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS, OUTPUT_CONTENT,
    OUTPUT_SANDBOX_ARTIFACTS,
};
use crate::config::output_types::ResolvedStepVariantFlow;
use crate::config::{MediaStep, MediaStepTool, OutputVariantValue};
use crate::error::MediaPmError;

use super::spec::{TokenSpec, assemble_tool_spec, command_option_tokens_for_tool};
use super::yt_dlp_inputs::resolve_step_output_binding;
use super::{
    FfmpegSlotLimits, VariantProducer, media_step_id, resolve_input_variant_producer,
    step_option_input_bindings, variant_to_output_capture_spec,
};

/// Synthesizes one rsgain workflow step per variant-flow mapping edge.
///
/// rsgain runs in single-track mode (in-place loudness tagging), so each
/// mapping produces exactly one step: the mapping's input variant is bound to
/// the `input_content` input from its producer (adding an execution-order
/// dependency when the producer is a prior step output), and option inputs are
/// bound from the step `options` map.
///
/// Output captures are keyed by the generic variant's `kind` label (for
/// example `output_content`), matching the old step-output-policy keying; the
/// variant-keyed `super::step_output_policy_overrides` helper is not used
/// here. Each mapping registers its `kind`-named output as a
/// [`VariantProducer::StepOutput`] entry keyed by the variant name.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when the mapping's input variant has no
/// producer, the output variant is missing or decodes as a non-generic
/// (yt-dlp-shaped) config, or a producer binding fails.
#[allow(clippy::too_many_arguments)]
pub(crate) fn synthesize_rsgain_step_chain(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    tool_id: &str,
    producer_snapshot: &BTreeMap<String, VariantProducer>,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
) -> Result<(), MediaPmError> {
    let mut pending_variant_updates = Vec::new();

    for (mapping_index, mapping) in mappings.iter().enumerate() {
        let step_id = media_step_id(step_index, mapping_index, step.tool, mapping);
        let mut depends_on = Vec::new();
        let mut inputs = BTreeMap::new();

        let producer = resolve_input_variant_producer(&mapping.input, producer_snapshot)
            .ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{step_index} references unknown input variant '{}'",
                    mapping.input
                ))
            })?;
        let (input_binding, dependency) = producer.to_binding()?;
        inputs.insert(INPUT_CONTENT.to_string(), input_binding);
        if let Some(step_dependency) = dependency {
            depends_on.push(step_dependency);
        }

        let variant_value = step.output_variants.get(&mapping.output).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} is missing output variant '{}'",
                mapping.output
            ))
        })?;
        if !matches!(variant_value, OutputVariantValue::Generic(_)) {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} output variant '{}' must decode as rsgain generic output config",
                mapping.output
            )));
        }

        inputs.extend(step_option_input_bindings(step));
        if !inputs.contains_key(INPUT_RSGAIN_INPUT_EXTENSION)
            && let OutputVariantValue::Generic(config) = variant_value
            && !config.extension.is_empty()
        {
            inputs.insert(INPUT_RSGAIN_INPUT_EXTENSION.to_string(), config.extension.clone());
        }

        // rsgain is a single-stream in-place tagger; only ffmpeg slots are
        // bounded, so the default zeroed limits are used for the binding.
        let output_binding = resolve_step_output_binding(
            MediaStepTool::Rsgain,
            &step.output_variants,
            &mapping.output,
            FfmpegSlotLimits::default(),
        )?;

        let mut outputs = BTreeMap::new();
        let capture_spec =
            variant_to_output_capture_spec(&output_binding.output_name, variant_value);
        outputs.insert(
            output_binding.output_name.clone(),
            OutputCaptureSpec {
                name: output_binding.output_name.clone(),
                capture: format!("file_regex:{}", rsgain_output_file_regex()),
                save: capture_spec.save,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );

        workflow.steps.push(WorkflowStepSpec {
            id: step_id.clone(),
            tool: tool_id.to_string(),
            inputs,
            outputs,
            max_retries: 0,
            depends_on,
        });

        pending_variant_updates.push((
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id,
                output_name: output_binding.output_name,
                zip_member: output_binding.zip_member,
            },
        ));
    }

    for (output_variant, producer) in pending_variant_updates {
        variant_producers.insert(output_variant, producer);
    }

    Ok(())
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
    ("opus_mode", ""),
    ("skip_existing", "false"),
    ("preserve_mtime", "false"),
    ("skip_tags", "false"),
    ("dry_run", "false"),
    ("quiet", "false"),
    ("output", ""),
    ("multithread", ""),
    ("loudness", "-18"),
    ("jobs", ""),
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
    ("jobs", TokenSpec::None),
    ("multithread", TokenSpec::None),
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
    format!("^input[.](?:{})$", SUPPORTED_RSGAIN_INPUT_EXTENSIONS.join("|"))
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
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        ),
        (
            INPUT_TRAILING_ARGS.to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        ),
        (INPUT_CONTENT.to_string(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
    ]);
    for option_input in RSGAIN_OPTION_INPUTS {
        inputs.insert(
            (*option_input).to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
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

#[must_use]
fn build_rsgain_default_input_defaults() -> BTreeMap<String, InputBinding> {
    let mut defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), InputBinding::Vec(vec![])),
        (INPUT_TRAILING_ARGS.to_string(), InputBinding::Vec(vec![])),
    ]);
    for option_input in RSGAIN_OPTION_INPUTS {
        defaults.entry((*option_input).to_string()).or_default();
    }
    for (key, value) in RSGAIN_INPUT_DEFAULTS {
        defaults.insert(key.to_string(), InputBinding::String(value.to_string()));
    }
    defaults
}

/// Builds the full [`ToolSpec`] and [`ToolRuntime`] for the managed rsgain tool.
#[must_use]
pub(crate) fn build_rsgain_spec(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> (ToolSpec, ToolRuntime) {
    let command_path = build_os_conditional_selector(os_exec_paths);
    assemble_tool_spec(
        "rsgain",
        content_map,
        build_rsgain_command(&command_path),
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
        assert_eq!(defaults.get("target_lufs"), Some(&InputBinding::String("-18".to_string())));
        assert_eq!(defaults.get("album"), Some(&InputBinding::String("false".to_string())));
        assert_eq!(defaults.get("tagmode"), Some(&InputBinding::String("i".to_string())));
        assert_eq!(defaults.get("true_peak"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(defaults.get("clip_mode"), Some(&InputBinding::String("p".to_string())));
        assert_eq!(defaults.get("max_peak"), Some(&InputBinding::String("0".to_string())));
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

    #[test]
    fn build_rsgain_spec_uses_os_exec_paths() {
        let content_map = BTreeMap::new();
        let os_exec_paths = BTreeMap::from([("linux".into(), "rsgain".into())]);
        let (_spec, runtime) = build_rsgain_spec(content_map, &os_exec_paths);
        assert!(!runtime.impure);
    }
}
