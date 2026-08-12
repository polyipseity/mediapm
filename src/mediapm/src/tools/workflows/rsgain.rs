//! Rsgain workflow step synthesis.
//!
//! Produces the conductor workflow steps for one `rsgain` loudness analysis step.
//! Also provides spec-generation functions for building the managed rsgain tool
//! definition from its command, inputs, outputs, and default configuration.

#![allow(dead_code)]

use std::collections::BTreeMap;

use mediapm_conductor::{
    InputBinding, NickelDocument, OutputCaptureSpec, SaveMode, ToolInputKind, ToolInputSpec,
    ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
};

use mediapm_conductor::tools::helpers::build_os_conditional_selector;

use crate::conductor_bridge::constants::{
    INPUT_CONTENT, INPUT_FFMETADATA_CONTENT, INPUT_LEADING_ARGS, INPUT_SD_PATTERN,
    INPUT_SD_REPLACEMENT, INPUT_TRAILING_ARGS, OUTPUT_CONTENT, OUTPUT_SANDBOX_ARTIFACTS,
};
use crate::config::output_types::ResolvedStepVariantFlow;
use crate::config::source_types::step_option_scalar;
use crate::config::{MediaStep, MediaStepTool, OutputVariantValue, TransformInputValue};
use crate::error::MediaPmError;

use super::ffmpeg::{
    ffmpeg_input_content_name, ffmpeg_output_capture_name, ffmpeg_output_file_regex,
    ffmpeg_output_path_input_name, ffmpeg_sandbox_output_path,
};
use super::spec::{TokenSpec, assemble_tool_spec, command_option_tokens_for_tool};
use super::yt_dlp_inputs::resolve_step_output_binding;
use super::{
    FfmpegSlotLimits, VariantProducer, conductor_output_save_mode, media_step_id,
    resolve_input_variant_producer, resolve_selected_dependency_tool_id, resolve_step_tool_id,
    step_option_input_bindings,
};

/// File extensions the provisioned `rsgain` binary accepts for in-place tagging.
const RSGAIN_RUNTIME_INPUT_EXTENSIONS: &[&str] = &[
    "flac", "ogg", "oga", "spx", "opus", "mp2", "mp3", "mp4", "m4a", "wma", "wv", "ape", "wav",
    "aiff", "aif", "snd", "tak",
];

/// Expands one `rsgain` config step into ffmpeg extract → rsgain → metadata export →
/// sd rewrite → ffmpeg apply.
///
/// Cross-step `ffmpeg` and `sd` dependencies are consumed here: ffmpeg extracts
/// audio and metadata, `sd` normalizes `ReplayGain` tag names for ffmetadata merge,
/// and the final ffmpeg apply step writes tags back onto the source container.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when dependency tools are missing, the input
/// variant has no producer, or output variant decoding fails.
#[expect(
    clippy::too_many_lines,
    reason = "rsgain chain synthesis keeps the full extract/tag/apply pipeline explicit"
)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn synthesize_rsgain_step_chain(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    tool_id: &str,
    generated_doc: &NickelDocument,
    producer_snapshot: &BTreeMap<String, VariantProducer>,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<(), MediaPmError> {
    let ffmpeg_tool_id = resolve_selected_dependency_tool_id("ffmpeg", generated_doc)?;
    let sd_tool_id = resolve_selected_dependency_tool_id("sd", generated_doc)?;
    let rsgain_tool_id = if tool_id.is_empty() {
        resolve_step_tool_id(MediaStepTool::Rsgain, generated_doc)?
    } else {
        tool_id.to_string()
    };

    for (mapping_index, mapping) in mappings.iter().enumerate() {
        let producer = resolve_input_variant_producer(&mapping.input, producer_snapshot)
            .ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{step_index} references unknown input variant '{}'",
                    mapping.input
                ))
            })?;
        let rsgain_input_extension =
            resolve_rsgain_input_extension(media_id, step_index, step, producer)?;
        let extract_codec_copy = !rsgain_input_extension.eq_ignore_ascii_case("flac");

        let (input_binding, input_dependency) = producer.to_binding()?;
        let base_step_id = media_step_id(step_index, mapping_index, step.tool, mapping);
        let extract_step_id = format!("{base_step_id}-ffmpeg-extract");
        let rsgain_step_id = format!("{base_step_id}-rsgain");
        let metadata_export_step_id = format!("{base_step_id}-ffmpeg-export-metadata");
        let metadata_rewrite_step_id = format!("{base_step_id}-sd-rewrite-metadata");
        let metadata_r128_rewrite_step_id = format!("{base_step_id}-sd-rewrite-r128-metadata");
        let apply_step_id = format!("{base_step_id}-ffmpeg-apply");

        let output_variant_value = step.output_variants.get(&mapping.output).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "missing output variant '{}' while resolving output policy",
                mapping.output
            ))
        })?;
        if !matches!(output_variant_value, OutputVariantValue::Generic(_)) {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} output variant '{}' must decode as rsgain generic output config",
                mapping.output
            )));
        }
        let OutputVariantValue::Generic(output_config) = output_variant_value else {
            unreachable!("validated above");
        };

        let output_binding = resolve_step_output_binding(
            MediaStepTool::Rsgain,
            &step.output_variants,
            &mapping.output,
            ffmpeg_slot_limits,
        )?;
        let apply_output_extension =
            resolve_rsgain_output_extension(Some(&output_config.extension), producer);
        let apply_output_capture = ffmpeg_output_capture_name(0);
        let apply_save_mode = conductor_output_save_mode(output_config.save);

        let extract_inputs = BTreeMap::from([
            (ffmpeg_input_content_name(0), input_binding.clone()),
            (
                ffmpeg_output_path_input_name(0),
                ffmpeg_sandbox_output_path(0, Some(&rsgain_input_extension)),
            ),
            (INPUT_LEADING_ARGS.to_string(), "[]".to_string()),
            (INPUT_TRAILING_ARGS.to_string(), r#"["-map","0:a?"]"#.to_string()),
            ("codec_copy".to_string(), extract_codec_copy.to_string()),
            ("vn".to_string(), "true".to_string()),
            ("movflags".to_string(), String::new()),
            ("map_metadata".to_string(), "-1".to_string()),
            ("map_chapters".to_string(), "-1".to_string()),
        ]);
        let mut extract_depends_on = Vec::new();
        if let Some(step_dependency) = input_dependency.clone() {
            extract_depends_on.push(step_dependency);
        }

        workflow.steps.push(WorkflowStepSpec {
            id: extract_step_id.clone(),
            tool: ffmpeg_tool_id.clone(),
            inputs: extract_inputs,
            depends_on: extract_depends_on,
            outputs: BTreeMap::from([(
                apply_output_capture.clone(),
                ffmpeg_slot_output_capture(0),
            )]),
            max_retries: 0,
        });

        let mut rsgain_inputs = BTreeMap::from([
            (
                INPUT_CONTENT.to_string(),
                format!("${{step_output.{extract_step_id}.{apply_output_capture}}}"),
            ),
            (
                INPUT_LEADING_ARGS.to_string(),
                step_option_json_list(step, INPUT_LEADING_ARGS).unwrap_or_else(|| "[]".to_string()),
            ),
            (
                INPUT_TRAILING_ARGS.to_string(),
                step_option_json_list(step, INPUT_TRAILING_ARGS)
                    .unwrap_or_else(|| "[]".to_string()),
            ),
            (INPUT_RSGAIN_INPUT_EXTENSION.to_string(), rsgain_input_extension.clone()),
        ]);
        for (key, value) in step_option_input_bindings(step) {
            if key != INPUT_RSGAIN_INPUT_EXTENSION {
                rsgain_inputs.insert(key, value);
            }
        }

        workflow.steps.push(WorkflowStepSpec {
            id: rsgain_step_id.clone(),
            tool: rsgain_tool_id.clone(),
            inputs: rsgain_inputs,
            depends_on: vec![extract_step_id.clone()],
            outputs: BTreeMap::from([(
                output_binding.output_name.clone(),
                rsgain_workflow_output_capture(&output_binding.output_name),
            )]),
            max_retries: 0,
        });

        let metadata_export_inputs = BTreeMap::from([
            (
                ffmpeg_input_content_name(0),
                format!("${{step_output.{rsgain_step_id}.{}}}", output_binding.output_name),
            ),
            (ffmpeg_output_path_input_name(0), ffmpeg_sandbox_output_path(0, Some("ffmeta"))),
            (INPUT_LEADING_ARGS.to_string(), "[]".to_string()),
            (INPUT_TRAILING_ARGS.to_string(), r#"["-f","ffmetadata"]"#.to_string()),
            ("codec_copy".to_string(), "true".to_string()),
            ("movflags".to_string(), String::new()),
            ("an".to_string(), "true".to_string()),
            ("vn".to_string(), "true".to_string()),
            ("sn".to_string(), "true".to_string()),
            ("dn".to_string(), "true".to_string()),
        ]);

        workflow.steps.push(WorkflowStepSpec {
            id: metadata_export_step_id.clone(),
            tool: ffmpeg_tool_id.clone(),
            inputs: metadata_export_inputs,
            depends_on: vec![rsgain_step_id.clone()],
            outputs: BTreeMap::from([(
                apply_output_capture.clone(),
                ffmpeg_slot_output_capture(0),
            )]),
            max_retries: 0,
        });

        let metadata_rewrite_inputs = BTreeMap::from([
            (
                INPUT_CONTENT.to_string(),
                format!("${{step_output.{metadata_export_step_id}.{apply_output_capture}}}"),
            ),
            (INPUT_LEADING_ARGS.to_string(), "[]".to_string()),
            (INPUT_TRAILING_ARGS.to_string(), "[]".to_string()),
            (INPUT_SD_PATTERN.to_string(), "(?i)REPLAYGAIN_".to_string()),
            (INPUT_SD_REPLACEMENT.to_string(), "replaygain_".to_string()),
        ]);

        workflow.steps.push(WorkflowStepSpec {
            id: metadata_rewrite_step_id.clone(),
            tool: sd_tool_id.clone(),
            inputs: metadata_rewrite_inputs,
            depends_on: vec![metadata_export_step_id.clone()],
            outputs: BTreeMap::from([(OUTPUT_CONTENT.to_string(), sd_output_capture())]),
            max_retries: 0,
        });

        let metadata_r128_rewrite_inputs = BTreeMap::from([
            (
                INPUT_CONTENT.to_string(),
                format!("${{step_output.{metadata_rewrite_step_id}.{OUTPUT_CONTENT}}}"),
            ),
            (INPUT_LEADING_ARGS.to_string(), "[]".to_string()),
            (INPUT_TRAILING_ARGS.to_string(), "[]".to_string()),
            (INPUT_SD_PATTERN.to_string(), "(?i)R128_".to_string()),
            (INPUT_SD_REPLACEMENT.to_string(), "R128_".to_string()),
        ]);

        workflow.steps.push(WorkflowStepSpec {
            id: metadata_r128_rewrite_step_id.clone(),
            tool: sd_tool_id.clone(),
            inputs: metadata_r128_rewrite_inputs,
            depends_on: vec![metadata_rewrite_step_id.clone()],
            outputs: BTreeMap::from([(OUTPUT_CONTENT.to_string(), sd_output_capture())]),
            max_retries: 0,
        });

        let mut apply_depends_on = vec![metadata_r128_rewrite_step_id.clone()];
        let mut apply_inputs = BTreeMap::from([
            (ffmpeg_input_content_name(0), input_binding),
            (
                INPUT_FFMETADATA_CONTENT.to_string(),
                format!("${{step_output.{metadata_r128_rewrite_step_id}.{OUTPUT_CONTENT}}}"),
            ),
            (
                ffmpeg_output_path_input_name(0),
                ffmpeg_sandbox_output_path(0, apply_output_extension.as_deref()),
            ),
            (INPUT_LEADING_ARGS.to_string(), "[]".to_string()),
            (INPUT_TRAILING_ARGS.to_string(), r#"["-map","0","-map_metadata","1"]"#.to_string()),
            ("metadata".to_string(), "replaygain_reference_loudness=89.0 dB".to_string()),
            ("map_metadata".to_string(), "0".to_string()),
            ("codec_copy".to_string(), "true".to_string()),
            ("movflags".to_string(), String::new()),
        ]);
        if let Some(container) =
            apply_output_extension.as_deref().map(ffmpeg_container_for_extension)
        {
            apply_inputs.insert("container".to_string(), container);
        }
        if let Some(step_dependency) = input_dependency {
            apply_depends_on.push(step_dependency);
        }

        workflow.steps.push(WorkflowStepSpec {
            id: apply_step_id.clone(),
            tool: ffmpeg_tool_id.clone(),
            inputs: apply_inputs,
            depends_on: apply_depends_on,
            outputs: BTreeMap::from([(
                apply_output_capture.clone(),
                OutputCaptureSpec {
                    name: apply_output_capture.clone(),
                    capture: format!("file_regex:{}", ffmpeg_output_file_regex(0)),
                    save: match apply_save_mode {
                        mediapm_conductor::OutputSaveMode::Saved => SaveMode::True,
                        mediapm_conductor::OutputSaveMode::Unsaved => SaveMode::False,
                        mediapm_conductor::OutputSaveMode::Full => SaveMode::Full,
                    },
                    allow_empty: false,
                    include_topmost_folder: true,
                },
            )]),
            max_retries: 0,
        });

        variant_producers.insert(
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: apply_step_id,
                output_name: apply_output_capture,
                zip_member: output_binding.zip_member,
                extension: apply_output_extension,
            },
        );
    }

    Ok(())
}

/// Resolves managed rsgain extraction extension for the ffmpeg audio-extract step.
fn resolve_rsgain_input_extension(
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    input_producer: &VariantProducer,
) -> Result<String, MediaPmError> {
    let configured = step_option_scalar(step, "input_extension")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);

    let resolved = configured
        .or_else(|| {
            input_producer.output_extension().and_then(|extension| {
                RSGAIN_RUNTIME_INPUT_EXTENSIONS
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(extension))
                    .then(|| extension.to_string())
            })
        })
        .unwrap_or_else(|| "flac".to_string());

    if RSGAIN_RUNTIME_INPUT_EXTENSIONS
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(&resolved))
    {
        return Ok(resolved);
    }

    Err(MediaPmError::Workflow(format!(
        "media '{media_id}' step #{step_index} options.input_extension='{resolved}' is unsupported for managed rsgain; supported values are: {}",
        RSGAIN_RUNTIME_INPUT_EXTENSIONS.join(", "),
    )))
}

/// Resolves the effective output extension for the final ffmpeg apply step.
#[must_use]
fn resolve_rsgain_output_extension(
    configured_extension: Option<&str>,
    input_producer: &VariantProducer,
) -> Option<String> {
    match configured_extension.map(str::trim) {
        Some("") => None,
        Some(value) => normalize_output_extension(Some(value)),
        None => input_producer
            .output_extension()
            .map(ToString::to_string)
            .or_else(|| Some("mkv".to_string())),
    }
}

#[must_use]
fn normalize_output_extension(extension: Option<&str>) -> Option<String> {
    let extension = extension?.trim();
    if extension.is_empty() {
        return None;
    }
    Some(extension.trim_start_matches('.').to_ascii_lowercase())
}

#[must_use]
fn ffmpeg_slot_output_capture(index: usize) -> OutputCaptureSpec {
    OutputCaptureSpec {
        name: ffmpeg_output_capture_name(index),
        capture: format!("file_regex:{}", ffmpeg_output_file_regex(index)),
        save: SaveMode::True,
        allow_empty: false,
        include_topmost_folder: true,
    }
}

#[must_use]
fn rsgain_workflow_output_capture(output_name: &str) -> OutputCaptureSpec {
    OutputCaptureSpec {
        name: output_name.to_string(),
        capture: format!("file_regex:{}", rsgain_output_file_regex()),
        save: SaveMode::True,
        allow_empty: false,
        include_topmost_folder: true,
    }
}

#[must_use]
fn sd_output_capture() -> OutputCaptureSpec {
    OutputCaptureSpec {
        name: OUTPUT_CONTENT.to_string(),
        capture: "file:inputs/input.ffmeta".to_string(),
        save: SaveMode::True,
        allow_empty: false,
        include_topmost_folder: true,
    }
}

fn ffmpeg_container_for_extension(extension: &str) -> String {
    match extension.trim_start_matches('.').to_ascii_lowercase().as_str() {
        "mkv" | "mka" | "mks" | "mk3d" => "matroska".to_string(),
        "mp4" | "m4a" | "m4v" | "m4b" | "m4r" | "3gp" | "3g2" => "mp4".to_string(),
        other => other.to_string(),
    }
}

fn step_option_json_list(step: &MediaStep, key: &str) -> Option<String> {
    match step.options.get(key) {
        Some(TransformInputValue::String(value)) => {
            let items: Vec<String> = value.split_whitespace().map(ToString::to_string).collect();
            serde_json::to_string(&items).ok()
        }
        None => None,
    }
}

// ---------------------------------------------------------------------------
// Spec-generation helpers — rsgain managed-tool definition
// ---------------------------------------------------------------------------

/// Internal rsgain-only input selecting sandbox materialization extension.
const INPUT_RSGAIN_INPUT_EXTENSION: &str = "input_extension";
/// File extensions supported by rsgain command template materialization.
const SUPPORTED_RSGAIN_INPUT_EXTENSIONS: &[&str] = RSGAIN_RUNTIME_INPUT_EXTENSIONS;

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
        assert!(
            command.iter().any(|c| c.contains("m4a")),
            "expected m4a input_extension conditional branch"
        );
        assert!(
            !command.iter().any(|c| c.contains("mkv")),
            "mkv must not be passed directly to rsgain; video flows use ffmpeg extract"
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
