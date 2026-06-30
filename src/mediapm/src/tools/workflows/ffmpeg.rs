//! Ffmpeg workflow step synthesis and spec generation.
//!
//! Produces the conductor workflow steps for one `ffmpeg` media transform step,
//! and provides builders for the full [`ToolSpec`] / [`ToolRuntime`] used during
//! managed-tool registration.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;

use mediapm_conductor::{
    OutputCaptureSpec, ToolInputSpec, ToolRuntime, ToolSpec, WorkflowStepSpec,
};

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
    source: &MediaSourceSpec,
    step_index: usize,
    step: &MediaStep,
) -> Vec<WorkflowStepSpec> {
    let step_id =
        qualify_step_id(source.id.as_deref().unwrap_or("unknown"), &format!("ffmpeg_{step_index}"));

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
                save: true,
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
    ("audio_codec", super::spec::TokenSpec::Pair("-c:a")),
    ("video_codec", super::spec::TokenSpec::Pair("-c:v")),
    ("container", super::spec::TokenSpec::Pair("-f")),
    ("audio_bitrate", super::spec::TokenSpec::Pair("-b:a")),
    ("video_bitrate", super::spec::TokenSpec::Pair("-b:v")),
    ("audio_quality", super::spec::TokenSpec::Pair("-q:a")),
    ("video_quality", super::spec::TokenSpec::Pair("-q:v")),
    ("crf", super::spec::TokenSpec::Pair("-crf")),
    ("preset", super::spec::TokenSpec::Pair("-preset")),
    ("threads", super::spec::TokenSpec::Pair("-threads")),
    ("log_level", super::spec::TokenSpec::Pair("-loglevel")),
    ("progress", super::spec::TokenSpec::Pair("-progress")),
    ("tune", super::spec::TokenSpec::Pair("-tune")),
    ("profile", super::spec::TokenSpec::Pair("-profile:v")),
    ("level", super::spec::TokenSpec::Pair("-level")),
    ("pixel_format", super::spec::TokenSpec::Pair("-pix_fmt")),
    ("frame_rate", super::spec::TokenSpec::Pair("-r")),
    ("sample_rate", super::spec::TokenSpec::Pair("-ar")),
    ("channels", super::spec::TokenSpec::Pair("-ac")),
    ("audio_filters", super::spec::TokenSpec::Pair("-af")),
    ("video_filters", super::spec::TokenSpec::Pair("-vf")),
    ("filter_complex", super::spec::TokenSpec::Pair("-filter_complex")),
    ("start_time", super::spec::TokenSpec::Pair("-ss")),
    ("duration", super::spec::TokenSpec::Pair("-t")),
    ("to", super::spec::TokenSpec::Pair("-to")),
    ("movflags", super::spec::TokenSpec::Pair("-movflags")),
    ("cues_to_front", super::spec::TokenSpec::BoolPair("-cues_to_front", "1")),
    ("map_metadata", super::spec::TokenSpec::Pair("-map_metadata")),
    ("map_chapters", super::spec::TokenSpec::Pair("-map_chapters")),
    ("map", super::spec::TokenSpec::Pair("-map")),
    ("map_channel", super::spec::TokenSpec::Pair("-map_channel")),
    ("copy_ts", super::spec::TokenSpec::Bool("-copyts")),
    ("start_at_zero", super::spec::TokenSpec::Bool("-start_at_zero")),
    ("stats", super::spec::TokenSpec::Bool("-stats")),
    ("no_overwrite", super::spec::TokenSpec::Bool("-n")),
    ("codec_copy", super::spec::TokenSpec::BoolPair("-c", "copy")),
    ("faststart", super::spec::TokenSpec::BoolPair("-movflags", "+faststart")),
    ("hwaccel", super::spec::TokenSpec::Pair("-hwaccel")),
    ("sample_format", super::spec::TokenSpec::Pair("-sample_fmt")),
    ("channel_layout", super::spec::TokenSpec::Pair("-channel_layout")),
    ("metadata", super::spec::TokenSpec::Pair("-metadata")),
    ("timestamp", super::spec::TokenSpec::Pair("-timestamp")),
    ("disposition", super::spec::TokenSpec::Pair("-disposition")),
    ("fps_mode", super::spec::TokenSpec::Pair("-fps_mode")),
    ("force_key_frames", super::spec::TokenSpec::Pair("-force_key_frames")),
    ("aspect", super::spec::TokenSpec::Pair("-aspect")),
    ("stream_loop", super::spec::TokenSpec::Pair("-stream_loop")),
    ("max_muxing_queue_size", super::spec::TokenSpec::Pair("-max_muxing_queue_size")),
    ("strict", super::spec::TokenSpec::Pair("-strict")),
    ("maxrate", super::spec::TokenSpec::Pair("-maxrate")),
    ("bufsize", super::spec::TokenSpec::Pair("-bufsize")),
    ("bitstream_filter", super::spec::TokenSpec::Pair("-bsf")),
    ("id3v2_version", super::spec::TokenSpec::Pair("-id3v2_version")),
    ("shortest", super::spec::TokenSpec::Bool("-shortest")),
    ("vn", super::spec::TokenSpec::Bool("-vn")),
    ("an", super::spec::TokenSpec::Bool("-an")),
    ("sn", super::spec::TokenSpec::Bool("-sn")),
    ("dn", super::spec::TokenSpec::Bool("-dn")),
    ("hide_banner", super::spec::TokenSpec::Bool("-hide_banner")),
];

// ---------------------------------------------------------------------------
// Option input names
// ---------------------------------------------------------------------------

/// Ordered ffmpeg option input names for CLI token generation.
const FFMPEG_OPTION_INPUTS: &[&str] = &[
    "option_args",
    "vn",
    "an",
    "sn",
    "dn",
    "codec_copy",
    "audio_codec",
    "video_codec",
    "container",
    "audio_bitrate",
    "video_bitrate",
    "audio_quality",
    "video_quality",
    "crf",
    "preset",
    "threads",
    "log_level",
    "progress",
    "tune",
    "profile",
    "level",
    "pixel_format",
    "frame_rate",
    "sample_rate",
    "channels",
    "audio_filters",
    "video_filters",
    "filter_complex",
    "start_time",
    "duration",
    "to",
    "movflags",
    "cues_to_front",
    "map_metadata",
    "map_chapters",
    "map",
    "map_channel",
    "copy_ts",
    "start_at_zero",
    "stats",
    "no_overwrite",
    "faststart",
    "hwaccel",
    "sample_format",
    "channel_layout",
    "metadata",
    "timestamp",
    "disposition",
    "fps_mode",
    "force_key_frames",
    "aspect",
    "stream_loop",
    "max_muxing_queue_size",
    "strict",
    "maxrate",
    "bufsize",
    "bitstream_filter",
    "id3v2_version",
    "shortest",
    "hide_banner",
    "codec_video",
    "codec_audio",
    "codec_subtitle",
    "overwrite_output",
    "volume",
    "filter_video",
    "filter_audio",
    "filter_subtitle",
    "ss",
    "frames",
    "resolution",
    "aspect",
    "crop",
    "rotate",
    "hflip",
    "vflip",
    "color_balance",
    "color_brightness",
    "color_contrast",
    "color_saturation",
    "color_gamma",
    "deinterlace",
    "denoise",
    "sharpness",
    "subtitles",
    "ass_subtitles",
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
        let enabled_input = format!("cover_slot_{slot_index}_enabled");
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
    command.push(format!("${{*inputs.{input_name_0} ? {input_name_0}:file({input_path_0}) | ''}}"));

    command.push(format!("${{*inputs.{INPUT_FFMETADATA_CONTENT} ? -i | ''}}"));
    command.push(format!(
        "${{*inputs.{INPUT_FFMETADATA_CONTENT} ? {INPUT_FFMETADATA_CONTENT}:file({SANDBOX_FFMETADATA_INPUT_FILE}) | ''}}"
    ));

    for index in 1..max_input_slots {
        let input_name = ffmpeg_input_content_name(index);
        let input_path = ffmpeg_input_file_path(index);
        command.push(format!("${{*inputs.{input_name} ? -i | ''}}"));
        command.push(format!("${{*inputs.{input_name} ? {input_name}:file({input_path}) | ''}}"));
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
    ]);

    for option_input in FFMPEG_OPTION_INPUTS {
        inputs.insert(
            (*option_input).to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    }

    for index in 0..max_input_slots {
        inputs.insert(
            ffmpeg_input_content_name(index),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    }
    for index in 1..max_input_slots {
        inputs.insert(
            ffmpeg_cover_slot_enabled_input_name(index),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    }
    for index in 0..max_output_slots {
        inputs.insert(
            ffmpeg_output_path_input_name(index),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    }
    inputs.insert(
        INPUT_FFMETADATA_CONTENT.to_string(),
        ToolInputSpec { kind: ToolInputKind::String, description: String::new(), required: false },
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
            save: true,
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
                save: true,
            },
        );
        outputs.insert(
            format!("{OUTPUT_CONTENT}_{index}"),
            OutputCaptureSpec {
                name: format!("{OUTPUT_CONTENT}_{index}"),
                capture: format!("file_regex:{path_regex}"),
                save: true,
            },
        );
    }
    outputs.insert(
        OUTPUT_SANDBOX_ARTIFACTS.to_string(),
        OutputCaptureSpec {
            name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            capture: "folder:inputs".to_string(),
            save: true,
        },
    );
    outputs.insert(
        "stdout".to_string(),
        OutputCaptureSpec { name: "stdout".to_string(), capture: "stdout".to_string(), save: true },
    );
    outputs.insert(
        "stderr".to_string(),
        OutputCaptureSpec { name: "stderr".to_string(), capture: "stderr".to_string(), save: true },
    );
    outputs.insert(
        "process_code".to_string(),
        OutputCaptureSpec {
            name: "process_code".to_string(),
            capture: "process_code".to_string(),
            save: true,
        },
    );
    outputs
}

/// Builds ffmpeg default input values.
#[must_use]
fn build_ffmpeg_default_input_defaults(
    max_input_slots: u32,
    max_output_slots: u32,
) -> BTreeMap<String, String> {
    use crate::conductor_bridge::constants::{
        INPUT_FFMETADATA_CONTENT, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS,
    };

    let mut defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), String::new()),
        (INPUT_TRAILING_ARGS.to_string(), String::new()),
    ]);

    for option_input in FFMPEG_OPTION_INPUTS {
        defaults.entry((*option_input).to_string()).or_default();
    }

    for (key, value) in FFMPEG_STATIC_DEFAULTS {
        defaults.insert(key.to_string(), value.to_string());
    }

    for index in 0..max_input_slots {
        defaults.insert(ffmpeg_input_content_name(index), String::new());
    }
    for index in 1..max_input_slots {
        defaults.insert(ffmpeg_cover_slot_enabled_input_name(index), String::new());
    }
    for index in 0..max_output_slots {
        defaults.insert(ffmpeg_output_path_input_name(index), String::new());
    }
    defaults.insert(INPUT_FFMETADATA_CONTENT.to_string(), String::new());

    defaults
}

// ---------------------------------------------------------------------------
// Public spec builder
// ---------------------------------------------------------------------------

/// Builds the full [`mediapm_conductor::ToolSpec`] and [`mediapm_conductor::ToolRuntime`] for ffmpeg.
#[must_use]
pub(crate) fn build_ffmpeg_spec(
    content_map: BTreeMap<String, String>,
    command_path: &str,
    slot_limits: FfmpegSlotLimits,
) -> (ToolSpec, ToolRuntime) {
    super::spec::assemble_tool_spec(
        "ffmpeg",
        content_map,
        build_ffmpeg_command(
            command_path,
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
    fn build_ffmpeg_defaults_include_static_and_dynamic_entries() {
        let defaults = build_ffmpeg_default_input_defaults(2, 2);
        assert_eq!(defaults.get("hide_banner").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("codec_copy").map(String::as_str), Some("true"));
        assert!(defaults.contains_key(&ffmpeg_input_content_name(0)));
        assert!(defaults.contains_key(&ffmpeg_input_content_name(1)));
        assert!(defaults.contains_key(&ffmpeg_output_path_input_name(0)));
        assert!(defaults.contains_key(&ffmpeg_output_path_input_name(1)));
    }

    #[test]
    fn build_ffmpeg_spec_sets_runtime_defaults() {
        use crate::conductor_bridge::tool_runtime::FfmpegSlotLimits;
        let content_map = BTreeMap::new();
        let (_spec, runtime) = build_ffmpeg_spec(
            content_map,
            "ffmpeg",
            FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 },
        );
        assert_eq!(runtime.max_concurrent_calls, 0);
        assert_eq!(runtime.max_retries, 0);
        assert!(runtime.inherited_env_vars.is_empty());
    }
}
