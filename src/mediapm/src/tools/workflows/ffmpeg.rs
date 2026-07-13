//! Ffmpeg workflow step synthesis and spec generation.
//!
//! Produces the conductor workflow steps for one `ffmpeg` media transform step,
//! and provides builders for the full [`ToolSpec`] / [`ToolRuntime`] used during
//! managed-tool registration.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;

use mediapm_conductor::{
    InputBinding, OutputCaptureSpec, SaveMode, ToolInputSpec, ToolRuntime, ToolSpec,
    WorkflowStepSpec,
};

use mediapm_conductor::tools::helpers::build_os_conditional_selector;

use crate::conductor_bridge::tool_runtime::FfmpegSlotLimits;
use crate::config::{DecodedOutputVariantConfig, MediaSourceSpec, MediaStep};

use super::{
    OUTPUT_PRIMARY, qualify_step_id, resolve_step_tool_id, step_option_input_bindings,
    variant_to_output_capture_spec,
};

/// Synthesizes one or more ffmpeg workflow steps from a media step definition.
///
/// # Errors
///
#[must_use]
pub(crate) fn synthesize_ffmpeg_step(
    _source: &MediaSourceSpec,
    step_index: usize,
    step: &MediaStep,
) -> Vec<WorkflowStepSpec> {
    let step_id = qualify_step_id("unknown", &format!("ffmpeg_{step_index}"));

    let mut inputs = BTreeMap::new();
    inputs.insert(
        "source_url".to_string(),
        step_option_input_bindings(step)
            .into_iter()
            .find(|(k, _)| k == "source_url")
            .map(|(_, v)| v)
            .unwrap_or_default(),
    );

    let mut outputs = BTreeMap::new();
    for (name, variant_json) in &step.output_variants {
        if let Ok(config) = DecodedOutputVariantConfig::from_json_value(variant_json.clone()) {
            outputs.insert(name.clone(), variant_to_output_capture_spec(name, &config));
        }
    }
    if outputs.is_empty() {
        outputs.insert(
            OUTPUT_PRIMARY.to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: OUTPUT_PRIMARY.to_string(),
                capture: "file:output.*".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
    }

    vec![WorkflowStepSpec {
        id: step_id,
        tool: resolve_step_tool_id(crate::config::MediaStepTool::Ffmpeg),
        inputs,
        outputs,
        max_retries: 0,
        depends_on: Vec::new(),
    }]
}

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

/// Prefix for indexed ffmpeg content inputs.
const INPUT_FFMPEG_CONTENT_PREFIX: &str = "input_content_";
/// Prefix for indexed ffmpeg output-path option inputs.
const INPUT_FFMPEG_OUTPUT_PATH_PREFIX: &str = "output_path_";
/// Fixed sandbox metadata path for ffmpeg metadata-input materialization.
const SANDBOX_FFMETADATA_INPUT_FILE: &str = "inputs/input.ffmeta";
/// Default extension (with leading dot) for generated ffmpeg output files.
const DEFAULT_FFMPEG_OUTPUT_EXTENSION_WITH_DOT: &str = ".mkv";
/// MOV/ISOBMFF-family container values that trigger `-movflags +faststart`.
const FFMPEG_MOV_FASTSTART_CONTAINERS: &[&str] =
    &["mp4", "mov", "m4a", "m4v", "3gp", "3g2", "f4v", "ism", "ismv", "ipod", "psp"];
/// Matroska-family container values that trigger `-cues_to_front 1`.
const FFMPEG_MATROSKA_CUES_TO_FRONT_CONTAINERS: &[&str] =
    &["matroska", "mkv", "mka", "mks", "mk3d", "webm"];
/// Static default input values for ffmpeg.
const FFMPEG_STATIC_DEFAULTS: &[(&str, &str)] = &[
    ("vn", "false"),
    ("an", "false"),
    ("codec_copy", "true"),
    ("map_metadata", "0"),
    ("map_chapters", "0"),
    ("movflags", ""),
    ("cues_to_front", ""),
    ("hide_banner", "true"),
    ("faststart", "false"),
    ("shortest", "false"),
    ("stats", "false"),
    ("no_overwrite", "false"),
    ("start_at_zero", "true"),
    ("copy_ts", "false"),
];

// ---------------------------------------------------------------------------
// Token specs
// ---------------------------------------------------------------------------

/// Map from option input name to TokenSpec for ffmpeg.
const FFMPEG_TOKEN_SPECS: &[(&str, super::spec::TokenSpec)] = &[
    ("an", super::spec::TokenSpec::Bool("-an")),
    ("aspect", super::spec::TokenSpec::Pair("-aspect")),
    ("audio_bitrate", super::spec::TokenSpec::Pair("-b:a")),
    ("audio_codec", super::spec::TokenSpec::Pair("-c:a")),
    ("audio_filters", super::spec::TokenSpec::Pair("-af")),
    ("audio_quality", super::spec::TokenSpec::Pair("-q:a")),
    ("bitstream_filter", super::spec::TokenSpec::Pair("-bsf")),
    ("bufsize", super::spec::TokenSpec::Pair("-bufsize")),
    ("channel_layout", super::spec::TokenSpec::Pair("-channel_layout")),
    ("channels", super::spec::TokenSpec::Pair("-ac")),
    ("codec_copy", super::spec::TokenSpec::BoolPair("-c", "copy")),
    ("container", super::spec::TokenSpec::Pair("-f")),
    ("copy_ts", super::spec::TokenSpec::Bool("-copyts")),
    ("crf", super::spec::TokenSpec::Pair("-crf")),
    ("cues_to_front", super::spec::TokenSpec::BoolPair("-cues_to_front", "1")),
    ("disposition", super::spec::TokenSpec::Pair("-disposition")),
    ("dn", super::spec::TokenSpec::Bool("-dn")),
    ("duration", super::spec::TokenSpec::Pair("-t")),
    ("faststart", super::spec::TokenSpec::BoolPair("-movflags", "+faststart")),
    ("filter_complex", super::spec::TokenSpec::Pair("-filter_complex")),
    ("force_key_frames", super::spec::TokenSpec::Pair("-force_key_frames")),
    ("fps_mode", super::spec::TokenSpec::Pair("-fps_mode")),
    ("frame_rate", super::spec::TokenSpec::Pair("-r")),
    ("hide_banner", super::spec::TokenSpec::Bool("-hide_banner")),
    ("hwaccel", super::spec::TokenSpec::Pair("-hwaccel")),
    ("id3v2_version", super::spec::TokenSpec::Pair("-id3v2_version")),
    ("level", super::spec::TokenSpec::Pair("-level")),
    ("log_level", super::spec::TokenSpec::Pair("-loglevel")),
    ("map", super::spec::TokenSpec::Pair("-map")),
    ("map_channel", super::spec::TokenSpec::Pair("-map_channel")),
    ("map_chapters", super::spec::TokenSpec::Pair("-map_chapters")),
    ("map_metadata", super::spec::TokenSpec::Pair("-map_metadata")),
    ("max_muxing_queue_size", super::spec::TokenSpec::Pair("-max_muxing_queue_size")),
    ("maxrate", super::spec::TokenSpec::Pair("-maxrate")),
    ("metadata", super::spec::TokenSpec::Pair("-metadata")),
    ("movflags", super::spec::TokenSpec::Pair("-movflags")),
    ("no_overwrite", super::spec::TokenSpec::Bool("-n")),
    ("pixel_format", super::spec::TokenSpec::Pair("-pix_fmt")),
    ("preset", super::spec::TokenSpec::Pair("-preset")),
    ("profile", super::spec::TokenSpec::Pair("-profile:v")),
    ("progress", super::spec::TokenSpec::Pair("-progress")),
    ("sample_format", super::spec::TokenSpec::Pair("-sample_fmt")),
    ("sample_rate", super::spec::TokenSpec::Pair("-ar")),
    ("shortest", super::spec::TokenSpec::Bool("-shortest")),
    ("sn", super::spec::TokenSpec::Bool("-sn")),
    ("start_at_zero", super::spec::TokenSpec::Bool("-start_at_zero")),
    ("start_time", super::spec::TokenSpec::Pair("-ss")),
    ("stats", super::spec::TokenSpec::Bool("-stats")),
    ("stream_loop", super::spec::TokenSpec::Pair("-stream_loop")),
    ("strict", super::spec::TokenSpec::Pair("-strict")),
    ("threads", super::spec::TokenSpec::Pair("-threads")),
    ("timestamp", super::spec::TokenSpec::Pair("-timestamp")),
    ("to", super::spec::TokenSpec::Pair("-to")),
    ("tune", super::spec::TokenSpec::Pair("-tune")),
    ("video_bitrate", super::spec::TokenSpec::Pair("-b:v")),
    ("video_codec", super::spec::TokenSpec::Pair("-c:v")),
    ("video_filters", super::spec::TokenSpec::Pair("-vf")),
    ("video_quality", super::spec::TokenSpec::Pair("-q:v")),
    ("vn", super::spec::TokenSpec::Bool("-vn")),
];

// ---------------------------------------------------------------------------
// Option input names
// ---------------------------------------------------------------------------

/// Ordered ffmpeg option input names for CLI token generation.
const FFMPEG_OPTION_INPUTS: &[&str] = &[
    "option_args",
    "an",
    "aspect",
    "ass_subtitles",
    "audio_bitrate",
    "audio_codec",
    "audio_filters",
    "audio_quality",
    "bitstream_filter",
    "bufsize",
    "channel_layout",
    "channels",
    "codec_audio",
    "codec_copy",
    "codec_subtitle",
    "codec_video",
    "color_balance",
    "color_brightness",
    "color_contrast",
    "color_gamma",
    "color_saturation",
    "container",
    "copy_ts",
    "crf",
    "crop",
    "cues_to_front",
    "deinterlace",
    "denoise",
    "disposition",
    "dn",
    "duration",
    "faststart",
    "filter_audio",
    "filter_complex",
    "filter_subtitle",
    "filter_video",
    "force_key_frames",
    "fps_mode",
    "frame_rate",
    "frames",
    "hflip",
    "hide_banner",
    "hwaccel",
    "id3v2_version",
    "level",
    "log_level",
    "map",
    "map_channel",
    "map_chapters",
    "map_metadata",
    "max_muxing_queue_size",
    "maxrate",
    "metadata",
    "movflags",
    "no_overwrite",
    "overwrite_output",
    "pixel_format",
    "preset",
    "profile",
    "progress",
    "resolution",
    "rotate",
    "sample_format",
    "sample_rate",
    "sharpness",
    "shortest",
    "sn",
    "ss",
    "start_at_zero",
    "start_time",
    "stats",
    "stream_loop",
    "strict",
    "subtitles",
    "threads",
    "timestamp",
    "to",
    "tune",
    "vflip",
    "video_bitrate",
    "video_codec",
    "video_filters",
    "video_quality",
    "vn",
    "volume",
    "watermark",
    "watermark_position",
];

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Returns indexed ffmpeg input-content field name.
#[must_use]
fn ffmpeg_input_content_name(index: u32) -> String {
    format!("{INPUT_FFMPEG_CONTENT_PREFIX}{index}")
}

/// Returns indexed ffmpeg cover-art slot enabled input field name.
#[must_use]
fn ffmpeg_cover_slot_enabled_input_name(index: u32) -> String {
    format!("cover_art_slot_enabled_{index}")
}

/// Returns indexed ffmpeg output-path input field name.
#[must_use]
fn ffmpeg_output_path_input_name(index: u32) -> String {
    format!("{INPUT_FFMPEG_OUTPUT_PATH_PREFIX}{index}")
}

/// Returns indexed ffmpeg output capture name.
#[must_use]
fn ffmpeg_output_capture_name(index: u32) -> String {
    if index == 0 { "primary".to_string() } else { format!("primary_{index}") }
}

/// Returns sandbox-relative ffmpeg input file path for one indexed slot.
#[must_use]
fn ffmpeg_input_file_path(index: u32) -> String {
    format!("inputs/input-{index}.bin")
}

/// Returns sandbox-relative ffmpeg output file path for one indexed slot.
#[must_use]
fn ffmpeg_output_file_path(index: u32) -> String {
    format!("output-{index}{DEFAULT_FFMPEG_OUTPUT_EXTENSION_WITH_DOT}")
}

/// Returns regex pattern for one indexed ffmpeg output capture path.
#[must_use]
fn ffmpeg_output_file_regex(index: u32) -> String {
    format!(r"^output-{index}(?:[.][^/\\]+)?$")
}

// ---------------------------------------------------------------------------
// Cover-art and container helpers
// ---------------------------------------------------------------------------

/// Builds ffmpeg cover-art map/disposition templates for managed media-tagger apply workflows.
#[must_use]
fn ffmpeg_cover_art_tokens(max_input_slots: u32, max_output_slots: u32) -> Vec<String> {
    let _ = max_output_slots;
    let mut tokens = Vec::new();
    for slot_index in 1..max_input_slots {
        let enabled_input = ffmpeg_cover_slot_enabled_input_name(slot_index);
        let ffmpeg_input_index = slot_index + 1;
        let output_video_index = slot_index - 1;
        tokens.push(super::spec::unpack_if_equals(&enabled_input, "true", "-map"));
        tokens.push(format!(
            "${{*inputs.{enabled_input} == \"true\" ? \"{ffmpeg_input_index}:v:0?\" | ''}}"
        ));
        tokens.push(super::spec::unpack_if_equals(
            &enabled_input,
            "true",
            &format!("-disposition:v:{output_video_index}"),
        ));
        tokens.push(super::spec::unpack_if_equals(&enabled_input, "true", "attached_pic"));
    }
    tokens
}

/// Builds one OR-joined container-equality condition string.
#[must_use]
fn ffmpeg_container_any_of_condition(containers: &[&str]) -> String {
    containers
        .iter()
        .map(|container| format!("inputs.container == \"{container}\""))
        .collect::<Vec<_>>()
        .join(" || ")
}

// ---------------------------------------------------------------------------
// Spec builder functions
// ---------------------------------------------------------------------------

/// Builds the ffmpeg executable command vector.
#[allow(clippy::too_many_lines)]
#[must_use]
fn build_ffmpeg_command(
    command_path: &str,
    max_input_slots: u32,
    max_output_slots: u32,
) -> Vec<String> {
    use super::spec::command_option_tokens_for_tool;
    use crate::conductor_bridge::constants::{
        INPUT_FFMETADATA_CONTENT, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS,
    };

    let mut command = vec![
        command_path.to_string(),
        format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
        "-y".to_string(),
    ];

    let input_name_0 = ffmpeg_input_content_name(0);
    let input_path_0 = ffmpeg_input_file_path(0);
    command.push(format!("${{*inputs.{input_name_0} ? -i | ''}}"));
    command.push(format!(
        "${{*inputs.{input_name_0} ? inputs.{input_name_0}:file({input_path_0}) | ''}}"
    ));

    command.push(format!("${{*inputs.{INPUT_FFMETADATA_CONTENT} ? -i | ''}}"));
    command.push(format!(
        "${{*inputs.{INPUT_FFMETADATA_CONTENT} ? inputs.{INPUT_FFMETADATA_CONTENT}:file({SANDBOX_FFMETADATA_INPUT_FILE}) | ''}}"
    ));

    for index in 1..max_input_slots {
        let input_name = ffmpeg_input_content_name(index);
        let input_path = ffmpeg_input_file_path(index);
        command.push(format!("${{*inputs.{input_name} ? -i | ''}}"));
        command.push(format!(
            "${{*inputs.{input_name} ? inputs.{input_name}:file({input_path}) | ''}}"
        ));
    }

    command.extend(command_option_tokens_for_tool(FFMPEG_OPTION_INPUTS, FFMPEG_TOKEN_SPECS));

    let mov_family_condition = ffmpeg_container_any_of_condition(FFMPEG_MOV_FASTSTART_CONTAINERS);
    command.push(format!("${{{mov_family_condition} ? -movflags | ''}}"));
    command.push(format!("${{{mov_family_condition} ? +faststart | ''}}"));

    let matroska_family_condition =
        ffmpeg_container_any_of_condition(FFMPEG_MATROSKA_CUES_TO_FRONT_CONTAINERS);
    command.push(format!("${{{matroska_family_condition} ? -cues_to_front | ''}}"));
    command.push(format!("${{{matroska_family_condition} ? 1 | ''}}"));

    command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));
    command.extend(ffmpeg_cover_art_tokens(max_input_slots, max_output_slots));

    for index in 0..max_output_slots {
        let output_path_input = ffmpeg_output_path_input_name(index);
        command.push(format!("${{*inputs.{output_path_input}}}"));
    }

    command
}

/// Builds ffmpeg input spec map.
#[must_use]
fn build_ffmpeg_inputs(
    max_input_slots: u32,
    max_output_slots: u32,
) -> BTreeMap<String, ToolInputSpec> {
    use crate::conductor_bridge::constants::{
        INPUT_FFMETADATA_CONTENT, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS,
    };
    use mediapm_conductor::ToolInputKind;

    let mut inputs = BTreeMap::from([
        (
            INPUT_LEADING_ARGS.to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        ),
        (
            INPUT_TRAILING_ARGS.to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        ),
    ]);

    for option_input in FFMPEG_OPTION_INPUTS {
        inputs.insert(
            (*option_input).to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        );
    }

    for index in 0..max_input_slots {
        inputs.insert(
            ffmpeg_input_content_name(index),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        );
    }
    for index in 1..max_input_slots {
        inputs.insert(
            ffmpeg_cover_slot_enabled_input_name(index),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        );
    }
    for index in 0..max_output_slots {
        inputs.insert(
            ffmpeg_output_path_input_name(index),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        );
    }
    inputs.insert(
        INPUT_FFMETADATA_CONTENT.to_string(),
        ToolInputSpec { kind: ToolInputKind::String, required: false },
    );

    inputs
}

/// Builds ffmpeg output capture spec map.
#[must_use]
fn build_ffmpeg_outputs(max_output_slots: u32) -> BTreeMap<String, OutputCaptureSpec> {
    use crate::conductor_bridge::constants::{OUTPUT_CONTENT, OUTPUT_SANDBOX_ARTIFACTS};

    let mut outputs = BTreeMap::new();
    outputs.insert(
        OUTPUT_CONTENT.to_string(),
        OutputCaptureSpec {
            name: OUTPUT_CONTENT.to_string(),
            capture: format!("file_regex:{}", ffmpeg_output_file_regex(0)),
            save: SaveMode::True,
            allow_empty: false,
            include_topmost_folder: true,
        },
    );
    for index in 0..max_output_slots {
        let path_regex = ffmpeg_output_file_regex(index);
        let capture_name = ffmpeg_output_capture_name(index);
        outputs.insert(
            capture_name.clone(),
            OutputCaptureSpec {
                name: capture_name,
                capture: format!("file_regex:{path_regex}"),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        outputs.insert(
            format!("{OUTPUT_CONTENT}_{index}"),
            OutputCaptureSpec {
                name: format!("{OUTPUT_CONTENT}_{index}"),
                capture: format!("file_regex:{path_regex}"),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
    }
    outputs.insert(
        OUTPUT_SANDBOX_ARTIFACTS.to_string(),
        OutputCaptureSpec {
            name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            capture: "folder:inputs".to_string(),
            save: SaveMode::True,
            allow_empty: false,
            include_topmost_folder: true,
        },
    );
    outputs.insert(
        "stdout".to_string(),
        OutputCaptureSpec {
            name: "stdout".to_string(),
            capture: "stdout".to_string(),
            save: SaveMode::True,
            allow_empty: false,
            include_topmost_folder: true,
        },
    );
    outputs.insert(
        "stderr".to_string(),
        OutputCaptureSpec {
            name: "stderr".to_string(),
            capture: "stderr".to_string(),
            save: SaveMode::True,
            allow_empty: false,
            include_topmost_folder: true,
        },
    );
    outputs.insert(
        "process_code".to_string(),
        OutputCaptureSpec {
            name: "process_code".to_string(),
            capture: "process_code".to_string(),
            save: SaveMode::True,
            allow_empty: false,
            include_topmost_folder: true,
        },
    );
    outputs
}

/// Builds ffmpeg default input values.
#[must_use]
fn build_ffmpeg_default_input_defaults(
    max_input_slots: u32,
    max_output_slots: u32,
) -> BTreeMap<String, InputBinding> {
    use crate::conductor_bridge::constants::{
        INPUT_FFMETADATA_CONTENT, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS,
    };

    let mut defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), InputBinding::Vec(vec![])),
        (INPUT_TRAILING_ARGS.to_string(), InputBinding::Vec(vec![])),
    ]);

    for option_input in FFMPEG_OPTION_INPUTS {
        defaults.entry((*option_input).to_string()).or_default();
    }

    for (key, value) in FFMPEG_STATIC_DEFAULTS {
        defaults.insert(key.to_string(), InputBinding::String(value.to_string()));
    }

    for index in 0..max_input_slots {
        defaults.insert(ffmpeg_input_content_name(index), InputBinding::String(String::new()));
    }
    for index in 1..max_input_slots {
        defaults.insert(
            ffmpeg_cover_slot_enabled_input_name(index),
            InputBinding::String(String::new()),
        );
    }
    for index in 0..max_output_slots {
        defaults.insert(ffmpeg_output_path_input_name(index), InputBinding::String(String::new()));
    }
    defaults.insert(INPUT_FFMETADATA_CONTENT.to_string(), InputBinding::String(String::new()));

    defaults
}

// ---------------------------------------------------------------------------
// Public spec builder
// ---------------------------------------------------------------------------

/// Builds the full [`mediapm_conductor::ToolSpec`] and [`mediapm_conductor::ToolRuntime`] for ffmpeg.
#[must_use]
pub(crate) fn build_ffmpeg_spec(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
    slot_limits: FfmpegSlotLimits,
) -> (ToolSpec, ToolRuntime) {
    let command_path = build_os_conditional_selector(os_exec_paths);
    super::spec::assemble_tool_spec(
        "ffmpeg",
        content_map,
        build_ffmpeg_command(
            &command_path,
            slot_limits.max_input_slots,
            slot_limits.max_output_slots,
        ),
        build_ffmpeg_inputs(slot_limits.max_input_slots, slot_limits.max_output_slots),
        build_ffmpeg_outputs(slot_limits.max_output_slots),
        build_ffmpeg_default_input_defaults(
            slot_limits.max_input_slots,
            slot_limits.max_output_slots,
        ),
        false, // impure
        0,     // max_concurrent_calls
        0,     // max_retries
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_ffmpeg_command_includes_template_tokens() {
        let command = build_ffmpeg_command("ffmpeg", 2, 2);
        assert!(command.iter().any(|c| c.contains("-i")), "expected -i token in ffmpeg command");
        assert!(
            command.iter().any(|c| c.contains("hide_banner")),
            "expected hide_banner reference in ffmpeg command"
        );
    }

    #[test]
    fn build_ffmpeg_inputs_includes_option_and_slot_inputs() {
        let inputs = build_ffmpeg_inputs(2, 2);
        assert!(inputs.contains_key("codec_audio"), "missing 'codec_audio' input");
        assert!(inputs.contains_key("codec_video"), "missing 'codec_video' input");
        assert!(inputs.contains_key(&ffmpeg_input_content_name(0)), "missing input_content_0");
        assert!(inputs.contains_key(&ffmpeg_output_path_input_name(0)), "missing output_path_0");
        assert!(inputs.contains_key("ffmetadata_content"), "missing ffmetadata input");
    }

    #[test]
    fn build_ffmpeg_outputs_use_regex_capture_for_slotted_outputs() {
        let outputs = build_ffmpeg_outputs(3);
        assert!(outputs.contains_key("primary"), "missing primary output");
        assert!(outputs.contains_key("primary_1"), "missing primary_1 output");
        assert!(outputs.contains_key("primary_2"), "missing primary_2 output");
        let primary = &outputs["primary"];
        assert!(
            primary.capture.starts_with("file_regex:"),
            "primary capture should use file_regex, got: {}",
            primary.capture
        );
    }

    #[test]
    fn build_ffmpeg_outputs_include_standard_captures() {
        let outputs = build_ffmpeg_outputs(2);
        assert!(outputs.contains_key("stdout"), "missing stdout output");
        assert!(outputs.contains_key("stderr"), "missing stderr output");
        assert!(outputs.contains_key("process_code"), "missing process_code output");
    }

    #[test]
    fn build_ffmpeg_defaults_include_static_and_dynamic_entries() {
        let defaults = build_ffmpeg_default_input_defaults(2, 2);
        assert_eq!(defaults.get("hide_banner"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(defaults.get("codec_copy"), Some(&InputBinding::String("true".to_string())));
        assert!(defaults.contains_key(&ffmpeg_input_content_name(0)));
        assert!(defaults.contains_key(&ffmpeg_input_content_name(1)));
        assert!(defaults.contains_key(&ffmpeg_output_path_input_name(0)));
        assert!(defaults.contains_key(&ffmpeg_output_path_input_name(1)));
    }

    #[test]
    fn build_ffmpeg_spec_sets_runtime_defaults() {
        use crate::conductor_bridge::tool_runtime::FfmpegSlotLimits;
        let content_map = BTreeMap::new();
        let os_exec_paths = BTreeMap::from([("linux".into(), "ffmpeg".into())]);
        let (_spec, runtime) = build_ffmpeg_spec(
            content_map,
            &os_exec_paths,
            FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 },
        );
        assert_eq!(runtime.max_concurrent_calls, 0);
        assert_eq!(runtime.max_retries, 0);
        assert!(runtime.inherited_env_vars.is_empty());
    }
}
