//! Tool command, environment, and config-policy helpers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mediapm_cas::Hash;
use mediapm_conductor::model::config::ToolInputKind;
use mediapm_conductor::{
    InputBinding, OutputCaptureSpec, ToolConfigSpec, ToolInputSpec, ToolKindSpec, ToolOutputSpec,
    ToolSpec,
};

use crate::config::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS as DEFAULT_FFMPEG_MAX_INPUT_SLOTS_U32,
    DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS as DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS_U32, ToolRequirement,
};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::tools::downloader::{ProvisionedToolPayload, ResolvedToolIdentity};

/// Reserved list-input name injected right after executable token.
const INPUT_LEADING_ARGS: &str = "leading_args";
/// Reserved list-input name injected after all generated operation arguments.
const INPUT_TRAILING_ARGS: &str = "trailing_args";
/// Common scalar input used by transform tools to consume upstream bytes.
const INPUT_CONTENT: &str = "input_content";
/// Prefix for indexed ffmpeg content inputs.
const INPUT_FFMPEG_CONTENT_PREFIX: &str = "input_content_";
/// Optional scalar input carrying FFmetadata bytes for ffmpeg metadata merge.
const INPUT_FFMETADATA_CONTENT: &str = "ffmetadata_content";
/// Prefix for indexed ffmpeg output-path option inputs.
const INPUT_FFMPEG_OUTPUT_PATH_PREFIX: &str = "output_path_";
/// Scalar URL input used by download tools.
const INPUT_SOURCE_URL: &str = "source_url";

/// Fixed sandbox input path used when materializing byte-content inputs.
const SANDBOX_INPUT_FILE: &str = "inputs/input.bin";
/// Fixed sandbox input file path for audio tools that edit media in place.
const SANDBOX_AUDIO_INPUT_FILE: &str = "inputs/input.mp3";
/// Fixed sandbox metadata path for ffmpeg metadata-input materialization.
const SANDBOX_FFMETADATA_INPUT_FILE: &str = "inputs/input.ffmeta";
/// Fixed sandbox output file path for yt-dlp download output.
const YT_DLP_OUTPUT_FILE: &str = "downloads/yt-dlp-output.media";
/// Fixed sandbox output file path for yt-dlp description sidecar output.
const YT_DLP_DESCRIPTION_OUTPUT_FILE: &str = "downloads/yt-dlp-output.media.description";
/// Fixed sandbox output file path for yt-dlp info-json sidecar output.
const YT_DLP_INFOJSON_OUTPUT_FILE: &str = "downloads/yt-dlp-output.media.info.json";
/// Sandbox directory where yt-dlp materializes downloaded output artifacts.
const SANDBOX_DOWNLOADS_DIR: &str = "downloads";
/// Sandbox directory where template `:file(...)` inputs are materialized.
const SANDBOX_INPUTS_DIR: &str = "inputs";
/// Fixed sandbox output file path for media-tagger FFmetadata documents.
const MEDIA_TAGGER_OUTPUT_FILE: &str = "metadata/output.ffmeta";
/// Output capture name exposing the full sandbox artifact tree.
const OUTPUT_SANDBOX_ARTIFACTS: &str = "sandbox_artifacts";
/// Output capture name exposing yt-dlp subtitle artifact bundle.
const OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS: &str = "yt_dlp_subtitle_artifacts";
/// Output capture name exposing yt-dlp thumbnail artifact bundle.
const OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS: &str = "yt_dlp_thumbnail_artifacts";
/// Output capture name exposing yt-dlp annotation artifact bundle.
const OUTPUT_YT_DLP_ANNOTATION_ARTIFACTS: &str = "yt_dlp_annotation_artifacts";
/// Output capture name exposing yt-dlp description file payload.
const OUTPUT_YT_DLP_DESCRIPTION_FILE: &str = "yt_dlp_description_file";
/// Output capture name exposing yt-dlp infojson file payload.
const OUTPUT_YT_DLP_INFOJSON_FILE: &str = "yt_dlp_infojson_file";
/// Output capture name exposing yt-dlp comments artifact bundle.
const OUTPUT_YT_DLP_COMMENTS_ARTIFACTS: &str = "yt_dlp_comments_artifacts";
/// Output capture name exposing yt-dlp internet-shortcut artifact bundle.
const OUTPUT_YT_DLP_LINK_ARTIFACTS: &str = "yt_dlp_link_artifacts";
/// Output capture name exposing yt-dlp split-chapter artifact bundle.
const OUTPUT_YT_DLP_CHAPTER_ARTIFACTS: &str = "yt_dlp_chapter_artifacts";
/// Output capture name exposing yt-dlp playlist-video artifact bundle.
const OUTPUT_YT_DLP_PLAYLIST_VIDEO_ARTIFACTS: &str = "yt_dlp_playlist_video_artifacts";
/// Output capture name exposing yt-dlp playlist-thumbnail artifact bundle.
const OUTPUT_YT_DLP_PLAYLIST_THUMBNAIL_ARTIFACTS: &str = "yt_dlp_playlist_thumbnail_artifacts";
/// Output capture name exposing yt-dlp playlist-description artifact bundle.
const OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_ARTIFACTS: &str = "yt_dlp_playlist_description_artifacts";
/// Output capture name exposing yt-dlp playlist-infojson artifact bundle.
const OUTPUT_YT_DLP_PLAYLIST_INFOJSON_ARTIFACTS: &str = "yt_dlp_playlist_infojson_artifacts";
/// Platform-prefixed env var carrying mediapm executable path for the
/// internal media-tagger Windows launcher.
pub(super) const MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV: &str =
    "MEDIAPM_MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS";
/// Platform-prefixed env var carrying mediapm executable path for the
/// internal media-tagger Linux launcher.
pub(super) const MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV: &str =
    "MEDIAPM_MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX";
/// Platform-prefixed env var carrying mediapm executable path for the
/// internal media-tagger macOS launcher.
pub(super) const MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV: &str =
    "MEDIAPM_MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS";

/// Default number of indexed ffmpeg content inputs supported by generated
/// managed tool contracts when runtime config does not override the value.
pub(crate) const DEFAULT_FFMPEG_MAX_INPUT_SLOTS: usize =
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS_U32 as usize;
/// Default number of indexed ffmpeg output slots supported by generated
/// managed tool contracts when runtime config does not override the value.
pub(crate) const DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS: usize =
    DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS_U32 as usize;

/// Runtime-resolved ffmpeg slot limits used by tool and workflow synthesis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FfmpegSlotLimits {
    /// Maximum number of indexed ffmpeg input content slots.
    pub(crate) max_input_slots: usize,
    /// Maximum number of indexed ffmpeg output slots.
    pub(crate) max_output_slots: usize,
}

impl Default for FfmpegSlotLimits {
    fn default() -> Self {
        Self {
            max_input_slots: DEFAULT_FFMPEG_MAX_INPUT_SLOTS,
            max_output_slots: DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS,
        }
    }
}

/// Resolves ffmpeg slot limits from effective tool requirements.
///
/// This rejects invalid non-positive values with actionable remediation hints
/// that point to the exact `mediapm.ncl` tool keys.
pub(crate) fn resolve_ffmpeg_slot_limits(
    tools: &BTreeMap<String, ToolRequirement>,
) -> Result<FfmpegSlotLimits, MediaPmError> {
    let ffmpeg_requirement = tools.iter().find_map(|(tool_name, requirement)| {
        tool_name.eq_ignore_ascii_case("ffmpeg").then_some(requirement)
    });

    let max_input_slots = resolve_ffmpeg_slot_limit(
        ffmpeg_requirement.and_then(|requirement| requirement.max_input_slots),
        DEFAULT_FFMPEG_MAX_INPUT_SLOTS,
        "tools.ffmpeg.max_input_slots",
    )?;
    let max_output_slots = resolve_ffmpeg_slot_limit(
        ffmpeg_requirement.and_then(|requirement| requirement.max_output_slots),
        DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS,
        "tools.ffmpeg.max_output_slots",
    )?;

    Ok(FfmpegSlotLimits { max_input_slots, max_output_slots })
}

/// Resolves one optional ffmpeg slot limit with defaulting and validation.
fn resolve_ffmpeg_slot_limit(
    configured_value: Option<u32>,
    default_value: usize,
    runtime_key: &str,
) -> Result<usize, MediaPmError> {
    let resolved_value = match configured_value {
        Some(value) => usize::try_from(value).map_err(|_| {
            MediaPmError::Workflow(format!(
                "{runtime_key} value '{value}' cannot be represented on this platform; remove it to use default {default_value}"
            ))
        })?,
        None => default_value,
    };

    if resolved_value == 0 {
        return Err(MediaPmError::Workflow(format!(
            "{runtime_key} must be at least 1; reduce ffmpeg fan-out usage or set a positive limit in mediapm.ncl (default {default_value})"
        )));
    }

    Ok(resolved_value)
}

/// Ordered yt-dlp option-input names injected into the generated command.
const YT_DLP_OPTION_INPUTS: &[&str] = &[
    "option_args",
    "format",
    "format_sort",
    "extract_audio",
    "audio_format",
    "audio_quality",
    "remux_video",
    "recode_video",
    "merge_output_format",
    "embed_thumbnail",
    "embed_metadata",
    "embed_subs",
    "embed_chapters",
    "embed_info_json",
    "write_subs",
    "write_auto_subs",
    "sub_langs",
    "sub_format",
    "convert_subs",
    "write_thumbnail",
    "write_all_thumbnails",
    "convert_thumbnails",
    "write_info_json",
    "write_comments",
    "write_description",
    "write_link",
    "split_chapters",
    "playlist_items",
    "no_playlist",
    "skip_download",
    "retries",
    "limit_rate",
    "concurrent_fragments",
    "proxy",
    "socket_timeout",
    "user_agent",
    "referer",
    "add_header",
    "cookies",
    "cookies_from_browser",
    "ffmpeg_location",
    "paths",
    "output",
    "parse_metadata",
    "replace_in_metadata",
    "download_sections",
    "postprocessor_args",
    "extractor_args",
    "http_chunk_size",
    "download_archive",
    "sponsorblock_mark",
    "sponsorblock_remove",
];

/// Ordered ffmpeg option-input names injected into the generated command.
const FFMPEG_OPTION_INPUTS: &[&str] = &[
    "option_args",
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
    "map_metadata",
    "map",
    "map_channel",
    "copy_ts",
    "start_at_zero",
    "stats",
    "no_overwrite",
    "codec_copy",
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
    "shortest",
    "vn",
    "an",
    "sn",
    "dn",
    "id3v2_version",
];

/// Ordered rsgain option-input names injected into the generated command.
const RSGAIN_OPTION_INPUTS: &[&str] = &[
    "option_args",
    "mode",
    "album",
    "album_aes77",
    "skip_existing",
    "tagmode",
    "loudness",
    "target_lufs",
    "clip_mode",
    "true_peak",
    "dual_mono",
    "album_mode",
    "max_peak",
    "lowercase",
    "id3v2_version",
    "opus_mode",
    "jobs",
    "multithread",
    "preset",
    "dry_run",
    "output",
    "quiet",
    "skip_tags",
    "preserve_mtime",
    "preserve_mtimes",
];

/// Ordered internal media-tagger option inputs injected into command args.
const MEDIA_TAGGER_OPTION_INPUTS: &[&str] = &[
    "option_args",
    "acoustid_endpoint",
    "musicbrainz_endpoint",
    "strict_identification",
    "recording_mbid",
    "release_mbid",
    "ffmpeg_bin",
];

/// Returns indexed ffmpeg input-content field name.
#[must_use]
pub(super) fn ffmpeg_input_content_name(index: usize) -> String {
    format!("{INPUT_FFMPEG_CONTENT_PREFIX}{index}")
}

/// Returns indexed ffmpeg output-path input field name.
#[must_use]
pub(super) fn ffmpeg_output_path_input_name(index: usize) -> String {
    format!("{INPUT_FFMPEG_OUTPUT_PATH_PREFIX}{index}")
}

/// Returns indexed ffmpeg output capture name.
#[must_use]
pub(super) fn ffmpeg_output_capture_name(index: usize) -> String {
    format!("output_content_{index}")
}

/// Returns sandbox-relative ffmpeg output file path for one indexed slot.
#[must_use]
pub(super) fn ffmpeg_output_file_path(index: usize) -> String {
    format!("output-{index}.mp3")
}

/// Returns sandbox-relative ffmpeg input file path for one indexed slot.
#[must_use]
fn ffmpeg_input_file_path(index: usize) -> String {
    format!("inputs/input-{index}.bin")
}

/// Builds executable command vector for one provisioned tool payload.
pub(super) fn build_tool_command(
    tool_name: &str,
    provisioned: &ProvisionedToolPayload,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Vec<String> {
    let lower_name = tool_name.to_ascii_lowercase();

    if lower_name == "yt-dlp" {
        let mut command = vec![
            provisioned.command_selector.clone(),
            format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
            "--no-progress".to_string(),
            "--no-part".to_string(),
            "--output".to_string(),
            YT_DLP_OUTPUT_FILE.to_string(),
        ];
        command.extend(command_option_tokens_for_tool("yt-dlp", YT_DLP_OPTION_INPUTS));
        command.push(format!("${{inputs.{INPUT_SOURCE_URL}}}"));
        command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));
        return command;
    }

    if lower_name == "ffmpeg" {
        let mut command = vec![
            provisioned.command_selector.clone(),
            format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
            "-y".to_string(),
        ];

        for index in 0..ffmpeg_slot_limits.max_input_slots {
            let input_name = ffmpeg_input_content_name(index);
            let input_path = ffmpeg_input_file_path(index);
            command.push(format!("${{*inputs.{input_name} ? -i | ''}}"));
            command.push(format!(
                "${{*inputs.{input_name} ? inputs.{input_name}:file({input_path}) | ''}}"
            ));
        }

        command.push(format!("${{*inputs.{INPUT_FFMETADATA_CONTENT} ? -i | ''}}"));
        command.push(format!(
            "${{*inputs.{INPUT_FFMETADATA_CONTENT} ? inputs.{INPUT_FFMETADATA_CONTENT}:file({SANDBOX_FFMETADATA_INPUT_FILE}) | ''}}"
        ));

        command.extend(command_option_tokens_for_tool("ffmpeg", FFMPEG_OPTION_INPUTS));
        command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));

        for index in 0..ffmpeg_slot_limits.max_output_slots {
            let output_path_input = ffmpeg_output_path_input_name(index);
            command.push(format!("${{*inputs.{output_path_input}}}"));
        }

        return command;
    }

    if lower_name == "media-tagger" {
        let mut command = vec![
            provisioned.command_selector.clone(),
            format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
            "--input".to_string(),
            format!("${{inputs.{INPUT_CONTENT}:file({SANDBOX_AUDIO_INPUT_FILE})}}"),
            "--output".to_string(),
            MEDIA_TAGGER_OUTPUT_FILE.to_string(),
        ];
        command.extend(command_option_tokens_for_tool("media-tagger", MEDIA_TAGGER_OPTION_INPUTS));
        command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));
        return command;
    }

    if lower_name == "rsgain" {
        let mut command = vec![
            provisioned.command_selector.clone(),
            format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
            "custom".to_string(),
        ];
        command.extend(command_option_tokens_for_tool("rsgain", RSGAIN_OPTION_INPUTS));
        command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));
        command.push(format!("${{inputs.{INPUT_CONTENT}:file({SANDBOX_AUDIO_INPUT_FILE})}}"));
        return command;
    }

    vec![
        provisioned.command_selector.clone(),
        format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
        format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"),
    ]
}

/// Builds one complete executable tool specification for generated tool rows.
pub(super) fn build_tool_spec(
    paths: &MediaPmPaths,
    tool_name: &str,
    provisioned: &ProvisionedToolPayload,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<ToolSpec, MediaPmError> {
    let _ = paths;
    Ok(ToolSpec {
        is_impure: false,
        inputs: build_tool_inputs(tool_name, ffmpeg_slot_limits),
        kind: ToolKindSpec::Executable {
            command: build_tool_command(tool_name, provisioned, ffmpeg_slot_limits),
            env_vars: BTreeMap::new(),
            success_codes: success_codes_for_tool(tool_name),
        },
        outputs: build_tool_outputs(tool_name, ffmpeg_slot_limits),
    })
}

pub(super) fn success_codes_for_tool(tool_name: &str) -> Vec<i32> {
    let _ = tool_name;
    vec![0]
}

/// Builds declared input contract for one managed executable tool.
fn build_tool_inputs(
    tool_name: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, ToolInputSpec> {
    let mut inputs = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), ToolInputSpec { kind: ToolInputKind::StringList }),
        (INPUT_TRAILING_ARGS.to_string(), ToolInputSpec { kind: ToolInputKind::StringList }),
    ]);

    for option_input in option_input_names_for_tool(tool_name) {
        let kind = if *option_input == "option_args" {
            ToolInputKind::StringList
        } else {
            ToolInputKind::String
        };
        inputs.insert((*option_input).to_string(), ToolInputSpec { kind });
    }

    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        inputs.insert(INPUT_SOURCE_URL.to_string(), ToolInputSpec { kind: ToolInputKind::String });
    } else if tool_name.eq_ignore_ascii_case("ffmpeg") {
        for index in 0..ffmpeg_slot_limits.max_input_slots {
            inputs.insert(
                ffmpeg_input_content_name(index),
                ToolInputSpec { kind: ToolInputKind::String },
            );
        }
        for index in 0..ffmpeg_slot_limits.max_output_slots {
            inputs.insert(
                ffmpeg_output_path_input_name(index),
                ToolInputSpec { kind: ToolInputKind::String },
            );
        }
        inputs.insert(
            INPUT_FFMETADATA_CONTENT.to_string(),
            ToolInputSpec { kind: ToolInputKind::String },
        );
    } else {
        inputs.insert(INPUT_CONTENT.to_string(), ToolInputSpec { kind: ToolInputKind::String });
    }

    inputs
}

/// Builds declared output capture contracts for one managed executable tool.
fn build_tool_outputs(
    tool_name: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, ToolOutputSpec> {
    let sandbox_artifacts_path = sandbox_artifacts_folder_for_tool(tool_name).to_string();

    let mut outputs = BTreeMap::new();

    if tool_name.eq_ignore_ascii_case("ffmpeg") {
        outputs.insert(
            "output_content".to_string(),
            ToolOutputSpec {
                capture: OutputCaptureSpec::File { path: ffmpeg_output_file_path(0) },
            },
        );
        for index in 0..ffmpeg_slot_limits.max_output_slots {
            outputs.insert(
                ffmpeg_output_capture_name(index),
                ToolOutputSpec {
                    capture: OutputCaptureSpec::File { path: ffmpeg_output_file_path(index) },
                },
            );
        }
    } else {
        let output_content_path = if tool_name.eq_ignore_ascii_case("yt-dlp") {
            YT_DLP_OUTPUT_FILE
        } else if tool_name.eq_ignore_ascii_case("media-tagger") {
            MEDIA_TAGGER_OUTPUT_FILE
        } else if tool_name.eq_ignore_ascii_case("rsgain") {
            SANDBOX_AUDIO_INPUT_FILE
        } else {
            SANDBOX_INPUT_FILE
        };

        outputs.insert(
            "output_content".to_string(),
            ToolOutputSpec {
                capture: OutputCaptureSpec::File { path: output_content_path.to_string() },
            },
        );
    }

    outputs.insert(
        OUTPUT_SANDBOX_ARTIFACTS.to_string(),
        ToolOutputSpec {
            capture: OutputCaptureSpec::Folder {
                path: sandbox_artifacts_path.clone(),
                include_topmost_folder: false,
            },
        },
    );
    outputs.insert("stdout".to_string(), ToolOutputSpec { capture: OutputCaptureSpec::Stdout {} });
    outputs.insert("stderr".to_string(), ToolOutputSpec { capture: OutputCaptureSpec::Stderr {} });
    outputs.insert(
        "process_code".to_string(),
        ToolOutputSpec { capture: OutputCaptureSpec::ProcessCode {} },
    );

    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        for output_name in [
            OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
            OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
            OUTPUT_YT_DLP_ANNOTATION_ARTIFACTS,
            OUTPUT_YT_DLP_COMMENTS_ARTIFACTS,
            OUTPUT_YT_DLP_LINK_ARTIFACTS,
            OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
            OUTPUT_YT_DLP_PLAYLIST_VIDEO_ARTIFACTS,
            OUTPUT_YT_DLP_PLAYLIST_THUMBNAIL_ARTIFACTS,
            OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_ARTIFACTS,
            OUTPUT_YT_DLP_PLAYLIST_INFOJSON_ARTIFACTS,
        ] {
            outputs.insert(
                output_name.to_string(),
                ToolOutputSpec {
                    capture: OutputCaptureSpec::Folder {
                        path: sandbox_artifacts_path.clone(),
                        include_topmost_folder: false,
                    },
                },
            );
        }

        outputs.insert(
            OUTPUT_YT_DLP_DESCRIPTION_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureSpec::File {
                    path: YT_DLP_DESCRIPTION_OUTPUT_FILE.to_string(),
                },
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_INFOJSON_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureSpec::File { path: YT_DLP_INFOJSON_OUTPUT_FILE.to_string() },
            },
        );
    }

    outputs
}

/// Returns a concrete sandbox-relative folder path for archive output capture.
///
/// Conductor rejects empty/current-directory-normalized output paths for
/// folder captures. Managed tool output contracts therefore point at a stable,
/// concrete directory that each command flow already materializes.
#[must_use]
fn sandbox_artifacts_folder_for_tool(tool_name: &str) -> &'static str {
    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        SANDBOX_DOWNLOADS_DIR
    } else {
        SANDBOX_INPUTS_DIR
    }
}

/// Returns the default execution-concurrency policy for one logical tool.
///
/// Policy notes:
/// - `yt-dlp` defaults to at most one active call so remote download pressure
///   remains predictable and does not overrun provider throttling by default,
/// - all other tools keep conductor's unbounded default (`-1`) unless users
///   explicitly set a stricter value in config.
#[must_use]
pub(super) fn default_max_concurrent_calls(tool_name: &str) -> i32 {
    if tool_name.eq_ignore_ascii_case("yt-dlp") { 1 } else { -1 }
}

/// Returns the default retry budget policy for one logical tool.
///
/// Policy notes:
/// - `yt-dlp` keeps this at `1` because the downloader already has its own
///   internal network retry controls,
/// - other tools keep sentinel `-1` so conductor runtime default behavior is
///   used.
#[must_use]
pub(super) fn default_max_retries(tool_name: &str) -> i32 {
    if tool_name.eq_ignore_ascii_case("yt-dlp") { 1 } else { -1 }
}

/// Merges existing runtime tool config with default policy and fresh content map.
pub(super) fn merge_tool_config_defaults(
    existing: Option<&ToolConfigSpec>,
    tool_name: &str,
    content_map: BTreeMap<String, Hash>,
    default_description: String,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> ToolConfigSpec {
    let default_limit = default_max_concurrent_calls(tool_name);
    let default_retries = default_max_retries(tool_name);

    let mut max_concurrent_calls = default_limit;
    let mut max_retries = default_retries;
    let mut description = Some(default_description);
    let mut input_defaults = default_input_defaults_for_tool(tool_name, ffmpeg_slot_limits);
    let mut env_vars = BTreeMap::new();

    if let Some(config) = existing {
        if config.max_concurrent_calls != -1 {
            max_concurrent_calls = config.max_concurrent_calls;
        }
        if config.max_retries != -1 {
            max_retries = config.max_retries;
        }
        if let Some(existing_description) = &config.description
            && !existing_description.trim().is_empty()
        {
            description = Some(existing_description.clone());
        }

        for (input_name, binding) in &config.input_defaults {
            input_defaults.insert(input_name.clone(), binding.clone());
        }

        env_vars.extend(config.env_vars.clone());
    }

    ToolConfigSpec {
        max_concurrent_calls,
        max_retries,
        description,
        input_defaults,
        env_vars,
        content_map: Some(content_map),
    }
}

/// Builds default generated input bindings for one managed tool.
///
/// These defaults prioritize high-quality, metadata-preserving outputs while
/// still allowing users to override all behavior through `input_defaults` or
/// step-level media `options` values.
fn default_input_defaults_for_tool(
    tool_name: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, InputBinding> {
    let mut input_defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), InputBinding::StringList(Vec::new())),
        (INPUT_TRAILING_ARGS.to_string(), InputBinding::StringList(Vec::new())),
    ]);

    for option_input in option_input_names_for_tool(tool_name) {
        let default_binding = if *option_input == "option_args" {
            InputBinding::StringList(Vec::new())
        } else {
            InputBinding::String(String::new())
        };
        input_defaults.insert((*option_input).to_string(), default_binding);
    }

    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        input_defaults.insert("format".to_string(), InputBinding::String("best".to_string()));
        input_defaults
            .insert("write_description".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("write_info_json".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("ffmpeg_location".to_string(), InputBinding::String("ffmpeg".to_string()));
    } else if tool_name.eq_ignore_ascii_case("ffmpeg") {
        for index in 0..ffmpeg_slot_limits.max_input_slots {
            input_defaults
                .insert(ffmpeg_input_content_name(index), InputBinding::String(String::new()));
        }
        for index in 0..ffmpeg_slot_limits.max_output_slots {
            input_defaults
                .insert(ffmpeg_output_path_input_name(index), InputBinding::String(String::new()));
        }
        input_defaults.insert(
            INPUT_FFMETADATA_CONTENT.to_string(),
            InputBinding::String(";FFMETADATA1\n".to_string()),
        );
        input_defaults.insert("vn".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("audio_codec".to_string(), InputBinding::String("libmp3lame".to_string()));
        input_defaults.insert("audio_quality".to_string(), InputBinding::String("2".to_string()));
        input_defaults.insert("map_metadata".to_string(), InputBinding::String("0".to_string()));
        input_defaults.insert("id3v2_version".to_string(), InputBinding::String("3".to_string()));
    } else if tool_name.eq_ignore_ascii_case("rsgain") {
        input_defaults.insert("album".to_string(), InputBinding::String("false".to_string()));
        input_defaults.insert("album_mode".to_string(), InputBinding::String("false".to_string()));
        input_defaults.insert("target_lufs".to_string(), InputBinding::String("-18".to_string()));
        input_defaults.insert("tagmode".to_string(), InputBinding::String("i".to_string()));
        input_defaults.insert("clip_mode".to_string(), InputBinding::String("p".to_string()));
        input_defaults.insert("true_peak".to_string(), InputBinding::String("true".to_string()));
        input_defaults.insert("max_peak".to_string(), InputBinding::String("0".to_string()));
        input_defaults
            .insert("preserve_mtimes".to_string(), InputBinding::String("true".to_string()));
    } else if tool_name.eq_ignore_ascii_case("media-tagger") {
        input_defaults
            .insert("strict_identification".to_string(), InputBinding::String("true".to_string()));
        input_defaults.insert(
            "acoustid_endpoint".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_ACOUSTID_ENDPOINT.to_string(),
            ),
        );
        input_defaults.insert(
            "musicbrainz_endpoint".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_MUSICBRAINZ_ENDPOINT.to_string(),
            ),
        );
        input_defaults.insert("ffmpeg_bin".to_string(), InputBinding::String("ffmpeg".to_string()));
    }

    input_defaults
}

/// Returns ordered option-input names for the provided managed tool.
#[must_use]
fn option_input_names_for_tool(tool_name: &str) -> &'static [&'static str] {
    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        YT_DLP_OPTION_INPUTS
    } else if tool_name.eq_ignore_ascii_case("ffmpeg") {
        FFMPEG_OPTION_INPUTS
    } else if tool_name.eq_ignore_ascii_case("rsgain") {
        RSGAIN_OPTION_INPUTS
    } else if tool_name.eq_ignore_ascii_case("media-tagger") {
        MEDIA_TAGGER_OPTION_INPUTS
    } else {
        &[]
    }
}

/// Renders option argument templates for ordered option inputs.
///
/// The generated templates rely on conductor conditional-unpack semantics so
/// `mediapm` can keep option bindings value-centric (`string` values) while
/// still producing correct CLI key/value argv forms at runtime.
#[must_use]
fn command_option_tokens_for_tool(tool_name: &str, input_names: &[&str]) -> Vec<String> {
    input_names
        .iter()
        .flat_map(|input_name| option_tokens_for_input(tool_name, input_name))
        .collect::<Vec<_>>()
}

/// Resolves option templates for one logical tool option input.
#[must_use]
fn option_tokens_for_input(tool_name: &str, input_name: &str) -> Vec<String> {
    if input_name == "option_args" {
        return vec![format!("${{*inputs.{input_name}}}")];
    }

    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        return match input_name {
            "format" => pair_option_tokens(input_name, "-f"),
            "format_sort" => pair_option_tokens(input_name, "-S"),
            "extract_audio" => bool_flag_tokens(input_name, "--extract-audio"),
            "audio_format" => pair_option_tokens(input_name, "--audio-format"),
            "audio_quality" => pair_option_tokens(input_name, "--audio-quality"),
            "remux_video" => pair_option_tokens(input_name, "--remux-video"),
            "recode_video" => pair_option_tokens(input_name, "--recode-video"),
            "merge_output_format" => pair_option_tokens(input_name, "--merge-output-format"),
            "embed_thumbnail" => {
                bool_switch_tokens(input_name, "--embed-thumbnail", "--no-embed-thumbnail")
            }
            "embed_metadata" => {
                bool_switch_tokens(input_name, "--embed-metadata", "--no-embed-metadata")
            }
            "embed_subs" => bool_switch_tokens(input_name, "--embed-subs", "--no-embed-subs"),
            "embed_chapters" => {
                bool_switch_tokens(input_name, "--embed-chapters", "--no-embed-chapters")
            }
            "embed_info_json" => {
                bool_switch_tokens(input_name, "--embed-info-json", "--no-embed-info-json")
            }
            "write_subs" => bool_switch_tokens(input_name, "--write-subs", "--no-write-subs"),
            "write_auto_subs" => {
                bool_switch_tokens(input_name, "--write-auto-subs", "--no-write-auto-subs")
            }
            "sub_langs" => pair_option_tokens(input_name, "--sub-langs"),
            "sub_format" => pair_option_tokens(input_name, "--sub-format"),
            "convert_subs" => pair_option_tokens(input_name, "--convert-subs"),
            "write_thumbnail" => {
                bool_switch_tokens(input_name, "--write-thumbnail", "--no-write-thumbnail")
            }
            "write_all_thumbnails" => bool_switch_tokens(
                input_name,
                "--write-all-thumbnails",
                "--no-write-all-thumbnails",
            ),
            "convert_thumbnails" => pair_option_tokens(input_name, "--convert-thumbnails"),
            "write_info_json" => {
                bool_switch_tokens(input_name, "--write-info-json", "--no-write-info-json")
            }
            "write_comments" => {
                bool_switch_tokens(input_name, "--write-comments", "--no-write-comments")
            }
            "write_description" => {
                bool_switch_tokens(input_name, "--write-description", "--no-write-description")
            }
            "write_link" => bool_switch_tokens(input_name, "--write-link", "--no-write-link"),
            "split_chapters" => {
                bool_switch_tokens(input_name, "--split-chapters", "--no-split-chapters")
            }
            "playlist_items" => pair_option_tokens(input_name, "--playlist-items"),
            "no_playlist" => bool_flag_tokens(input_name, "--no-playlist"),
            "skip_download" => bool_flag_tokens(input_name, "--skip-download"),
            "retries" => pair_option_tokens(input_name, "--retries"),
            "limit_rate" => pair_option_tokens(input_name, "--limit-rate"),
            "concurrent_fragments" => pair_option_tokens(input_name, "--concurrent-fragments"),
            "proxy" => pair_option_tokens(input_name, "--proxy"),
            "socket_timeout" => pair_option_tokens(input_name, "--socket-timeout"),
            "user_agent" => pair_option_tokens(input_name, "--user-agent"),
            "referer" => pair_option_tokens(input_name, "--referer"),
            "add_header" => pair_option_tokens(input_name, "--add-header"),
            "cookies" => pair_option_tokens(input_name, "--cookies"),
            "cookies_from_browser" => pair_option_tokens(input_name, "--cookies-from-browser"),
            "paths" => pair_option_tokens(input_name, "--paths"),
            "output" => pair_option_tokens(input_name, "--output"),
            "parse_metadata" => pair_option_tokens(input_name, "--parse-metadata"),
            "replace_in_metadata" => pair_option_tokens(input_name, "--replace-in-metadata"),
            "download_sections" => pair_option_tokens(input_name, "--download-sections"),
            "postprocessor_args" => pair_option_tokens(input_name, "--postprocessor-args"),
            "extractor_args" => pair_option_tokens(input_name, "--extractor-args"),
            "http_chunk_size" => pair_option_tokens(input_name, "--http-chunk-size"),
            "download_archive" => pair_option_tokens(input_name, "--download-archive"),
            "sponsorblock_mark" => pair_option_tokens(input_name, "--sponsorblock-mark"),
            "sponsorblock_remove" => pair_option_tokens(input_name, "--sponsorblock-remove"),
            _ => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
        };
    }

    if tool_name.eq_ignore_ascii_case("ffmpeg") {
        return match input_name {
            "audio_codec" => pair_option_tokens(input_name, "-c:a"),
            "video_codec" => pair_option_tokens(input_name, "-c:v"),
            "container" => pair_option_tokens(input_name, "-f"),
            "audio_bitrate" => pair_option_tokens(input_name, "-b:a"),
            "video_bitrate" => pair_option_tokens(input_name, "-b:v"),
            "audio_quality" => pair_option_tokens(input_name, "-q:a"),
            "video_quality" => pair_option_tokens(input_name, "-q:v"),
            "crf" => pair_option_tokens(input_name, "-crf"),
            "preset" => pair_option_tokens(input_name, "-preset"),
            "threads" => pair_option_tokens(input_name, "-threads"),
            "log_level" => pair_option_tokens(input_name, "-loglevel"),
            "progress" => pair_option_tokens(input_name, "-progress"),
            "tune" => pair_option_tokens(input_name, "-tune"),
            "profile" => pair_option_tokens(input_name, "-profile:v"),
            "level" => pair_option_tokens(input_name, "-level"),
            "pixel_format" => pair_option_tokens(input_name, "-pix_fmt"),
            "frame_rate" => pair_option_tokens(input_name, "-r"),
            "sample_rate" => pair_option_tokens(input_name, "-ar"),
            "channels" => pair_option_tokens(input_name, "-ac"),
            "audio_filters" => pair_option_tokens(input_name, "-af"),
            "video_filters" => pair_option_tokens(input_name, "-vf"),
            "filter_complex" => pair_option_tokens(input_name, "-filter_complex"),
            "start_time" => pair_option_tokens(input_name, "-ss"),
            "duration" => pair_option_tokens(input_name, "-t"),
            "to" => pair_option_tokens(input_name, "-to"),
            "movflags" => pair_option_tokens(input_name, "-movflags"),
            "map_metadata" => pair_option_tokens(input_name, "-map_metadata"),
            "map" => pair_option_tokens(input_name, "-map"),
            "map_channel" => pair_option_tokens(input_name, "-map_channel"),
            "copy_ts" => bool_flag_tokens(input_name, "-copyts"),
            "start_at_zero" => bool_flag_tokens(input_name, "-start_at_zero"),
            "stats" => bool_flag_tokens(input_name, "-stats"),
            "no_overwrite" => bool_flag_tokens(input_name, "-n"),
            "codec_copy" => bool_value_pair_tokens(input_name, "-c", "copy"),
            "faststart" => bool_value_pair_tokens(input_name, "-movflags", "+faststart"),
            "hwaccel" => pair_option_tokens(input_name, "-hwaccel"),
            "sample_format" => pair_option_tokens(input_name, "-sample_fmt"),
            "channel_layout" => pair_option_tokens(input_name, "-channel_layout"),
            "metadata" => pair_option_tokens(input_name, "-metadata"),
            "timestamp" => pair_option_tokens(input_name, "-timestamp"),
            "disposition" => pair_option_tokens(input_name, "-disposition"),
            "fps_mode" => pair_option_tokens(input_name, "-fps_mode"),
            "force_key_frames" => pair_option_tokens(input_name, "-force_key_frames"),
            "aspect" => pair_option_tokens(input_name, "-aspect"),
            "stream_loop" => pair_option_tokens(input_name, "-stream_loop"),
            "max_muxing_queue_size" => pair_option_tokens(input_name, "-max_muxing_queue_size"),
            "strict" => pair_option_tokens(input_name, "-strict"),
            "maxrate" => pair_option_tokens(input_name, "-maxrate"),
            "bufsize" => pair_option_tokens(input_name, "-bufsize"),
            "bitstream_filter" => pair_option_tokens(input_name, "-bsf"),
            "id3v2_version" => pair_option_tokens(input_name, "-id3v2_version"),
            "shortest" => bool_flag_tokens(input_name, "-shortest"),
            "vn" => bool_flag_tokens(input_name, "-vn"),
            "an" => bool_flag_tokens(input_name, "-an"),
            "sn" => bool_flag_tokens(input_name, "-sn"),
            "dn" => bool_flag_tokens(input_name, "-dn"),
            _ => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
        };
    }

    if tool_name.eq_ignore_ascii_case("rsgain") {
        return match input_name {
            "mode" => Vec::new(),
            "album" | "album_mode" => bool_flag_tokens(input_name, "--album"),
            "album_aes77" => bool_flag_tokens(input_name, "--album-aes77"),
            "skip_existing" => bool_flag_tokens(input_name, "--skip-existing"),
            "tagmode" => pair_option_tokens(input_name, "--tagmode"),
            "target_lufs" | "loudness" => pair_option_tokens(input_name, "--loudness"),
            "clip_mode" => pair_option_tokens(input_name, "--clip-mode"),
            "true_peak" => bool_flag_tokens(input_name, "--true-peak"),
            "dual_mono" => bool_flag_tokens(input_name, "--dual-mono"),
            "max_peak" => pair_option_tokens(input_name, "--max-peak"),
            "lowercase" => bool_flag_tokens(input_name, "--lowercase"),
            "id3v2_version" => pair_option_tokens(input_name, "--id3v2-version"),
            "opus_mode" => pair_option_tokens(input_name, "--opus-mode"),
            "jobs" | "multithread" => pair_option_tokens(input_name, "--multithread"),
            "preset" => pair_option_tokens(input_name, "--preset"),
            "dry_run" => bool_flag_tokens(input_name, "--dry-run"),
            "output" => pair_option_tokens(input_name, "--output"),
            "quiet" => bool_flag_tokens(input_name, "--quiet"),
            "skip_tags" => bool_value_pair_tokens(input_name, "--tagmode", "s"),
            "preserve_mtime" | "preserve_mtimes" => {
                bool_flag_tokens(input_name, "--preserve-mtimes")
            }
            _ => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
        };
    }

    if tool_name.eq_ignore_ascii_case("media-tagger") {
        return match input_name {
            "acoustid_endpoint" => pair_option_tokens(input_name, "--acoustid-endpoint"),
            "musicbrainz_endpoint" => pair_option_tokens(input_name, "--musicbrainz-endpoint"),
            "strict_identification" => bool_flag_tokens(input_name, "--strict-identification"),
            "recording_mbid" => pair_option_tokens(input_name, "--recording-mbid"),
            "release_mbid" => pair_option_tokens(input_name, "--release-mbid"),
            "ffmpeg_bin" => pair_option_tokens(input_name, "--ffmpeg-bin"),
            _ => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
        };
    }

    pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-")))
}

/// Builds `${*inputs.<name> ? <flag> | ''}` + `${*inputs.<name>}` tokens for
/// one key/value CLI option.
#[must_use]
fn pair_option_tokens(input_name: &str, flag: &str) -> Vec<String> {
    vec![unpack_if_truthy(input_name, flag), unpack_scalar(input_name)]
}

/// Builds conditional tokens that emit one flag only when the option value is
/// exactly `"true"`.
///
/// Any other value is treated as disabled for CLI flag rendering.
#[must_use]
fn bool_flag_tokens(input_name: &str, flag: &str) -> Vec<String> {
    vec![unpack_if_equals(input_name, "true", flag)]
}

/// Builds conditional tokens that emit `true_flag` only when the option value
/// is exactly `"true"`.
///
/// The `false` branch is intentionally omitted in mediapm for simpler
/// value-centric option behavior: non-`"true"` values produce no boolean CLI
/// toggle token.
#[must_use]
fn bool_switch_tokens(input_name: &str, true_flag: &str, _false_flag: &str) -> Vec<String> {
    bool_flag_tokens(input_name, true_flag)
}

/// Builds conditional tokens that emit one `flag value` pair only when the
/// option value is exactly `"true"`.
#[must_use]
fn bool_value_pair_tokens(input_name: &str, flag: &str, value: &str) -> Vec<String> {
    vec![unpack_if_equals(input_name, "true", flag), unpack_if_equals(input_name, "true", value)]
}

/// Builds one unpack conditional token gated on non-empty scalar presence.
#[must_use]
fn unpack_if_truthy(input_name: &str, rendered_argument: &str) -> String {
    format!("${{*inputs.{input_name} ? {rendered_argument} | ''}}")
}

/// Builds one unpack conditional token gated on scalar equality.
#[must_use]
fn unpack_if_equals(input_name: &str, expected_value: &str, rendered_argument: &str) -> String {
    format!("${{*inputs.{input_name} == \"{expected_value}\" ? {rendered_argument} | ''}}")
}

/// Builds one scalar unpack token `${*inputs.<name>}`.
#[must_use]
fn unpack_scalar(input_name: &str) -> String {
    format!("${{*inputs.{input_name}}}")
}

/// Builds one default human description for generated tool runtime config rows.
#[must_use]
pub(super) fn default_tool_config_description(
    tool_name: &str,
    identity: &ResolvedToolIdentity,
    tool_description: &str,
) -> String {
    let git_hash = identity.git_hash.as_deref().unwrap_or("n/a");
    let version = identity.version.as_deref().unwrap_or("n/a");
    let tag = identity.tag.as_deref().unwrap_or("n/a");
    let release = identity.release_description.as_deref().unwrap_or("n/a");

    format!(
        "tool: {tool_name}\ngit_hash: {git_hash}\nversion: {version}\ntag: {tag}\ncatalog_description: {tool_description}\nrelease_description: {release}"
    )
}

/// Builds executable environment overrides for one tool requirement.
pub(super) fn build_tool_env(
    _paths: &MediaPmPaths,
    tool_name: &str,
) -> Result<BTreeMap<String, String>, MediaPmError> {
    let mut env_vars = BTreeMap::new();

    if tool_name.eq_ignore_ascii_case("media-tagger") {
        let launcher_key = media_tagger_launcher_mediapm_env_var_for_host()?;
        let mediapm_binary = resolve_media_tagger_launcher_binary_path()?;
        let escaped_binary = escape_template_literal(mediapm_binary.to_string_lossy().as_ref());
        env_vars.insert(launcher_key.to_string(), escaped_binary);
    }

    Ok(env_vars)
}

/// Resolves executable path used by generated media-tagger launchers.
///
/// This prefers the real `mediapm` CLI binary even when tool reconciliation is
/// triggered from examples/tests whose process executable is not `mediapm`.
fn resolve_media_tagger_launcher_binary_path() -> Result<PathBuf, MediaPmError> {
    let current_exe = std::env::current_exe().map_err(|error| {
        MediaPmError::Workflow(format!(
            "failed to resolve current process executable while preparing internal media-tagger launcher env: {error}"
        ))
    })?;

    if executable_file_stem_eq_ignore_ascii_case(&current_exe, "mediapm") {
        return Ok(current_exe);
    }

    if let Some(from_env) = std::env::var_os("CARGO_BIN_EXE_mediapm") {
        let candidate = PathBuf::from(from_env);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    let binary_name = if cfg!(windows) { "mediapm.exe" } else { "mediapm" };
    for ancestor in current_exe.ancestors().skip(1).take(6) {
        let candidate = ancestor.join(binary_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(MediaPmError::Workflow(format!(
        "failed to resolve mediapm executable path for internal media-tagger launcher; current executable was '{}'",
        current_exe.display()
    )))
}

/// Returns true when executable filename stem matches expected text.
fn executable_file_stem_eq_ignore_ascii_case(path: &Path, expected_stem: &str) -> bool {
    path.file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|stem| stem.eq_ignore_ascii_case(expected_stem))
}

/// Escapes plain string literals for conductor template rendering.
#[must_use]
fn escape_template_literal(value: &str) -> String {
    value.replace('\\', "\\\\")
}

/// Resolves the internal media-tagger launcher env var key for one host OS.
pub(super) fn media_tagger_launcher_mediapm_env_var_for_host() -> Result<&'static str, MediaPmError>
{
    media_tagger_launcher_mediapm_env_var_for_os(std::env::consts::OS)
}

/// Resolves the internal media-tagger launcher env var key for one target OS.
pub(super) fn media_tagger_launcher_mediapm_env_var_for_os(
    os: &str,
) -> Result<&'static str, MediaPmError> {
    match os {
        "windows" => Ok(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV),
        "linux" => Ok(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV),
        "macos" => Ok(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV),
        other => Err(MediaPmError::Workflow(format!(
            "unsupported host platform '{other}' for internal media-tagger launcher env mapping"
        ))),
    }
}

/// Validates executable command selectors against generated content-map keys.
pub(super) fn validate_tool_command(
    tool_name: &str,
    command_vector: &[String],
    content_map: &BTreeMap<String, Hash>,
) -> Result<(), MediaPmError> {
    let Some(binary) = command_vector.first() else {
        return Err(MediaPmError::Workflow(format!("tool '{tool_name}' command is empty")));
    };

    if binary.contains("context.os") {
        let selectors = extract_platform_conditional_paths(binary)?;
        for (target, path) in selectors {
            if !content_map_contains_command_target(content_map, &path) {
                return Err(MediaPmError::Workflow(format!(
                    "tool '{tool_name}' command selector for '{target}' references '{path}', but content_map has no such key"
                )));
            }
        }
        return Ok(());
    }

    if !content_map_contains_command_target(content_map, binary) {
        return Err(MediaPmError::Workflow(format!(
            "tool '{tool_name}' command target '{binary}' is missing from content_map"
        )));
    }

    Ok(())
}

/// Returns true when one command target can be materialized by `content_map`.
///
/// Supported matches:
/// - direct file key equality (`target == key`),
/// - directory ZIP keys ending with `/` or `\\` where `target` is under that
///   directory,
/// - root ZIP keys (`./` or `.\\`) that materialize all relative paths.
fn content_map_contains_command_target(content_map: &BTreeMap<String, Hash>, target: &str) -> bool {
    if content_map.contains_key(target) {
        return true;
    }

    let normalized_target = normalize_sandbox_relative_path(target);
    for key in content_map.keys() {
        let normalized_key = normalize_sandbox_relative_path(key);
        if normalized_key == "./" {
            return true;
        }

        if key.ends_with('/') || key.ends_with('\\') {
            let prefix = normalized_key.trim_start_matches("./");
            if prefix.is_empty() || normalized_target.starts_with(prefix) {
                return true;
            }
        }
    }

    false
}

/// Normalizes one sandbox-relative path key/value to slash-separated text.
fn normalize_sandbox_relative_path(value: &str) -> String {
    value.replace('\\', "/")
}

/// Parses `${context.os == "<target>" ? <path> | <fallback>}` selector
/// paths from one command token.
pub(super) fn extract_platform_conditional_paths(
    template: &str,
) -> Result<BTreeMap<String, String>, MediaPmError> {
    let mut result = BTreeMap::new();
    let mut cursor = 0usize;

    while let Some(start_rel) = template[cursor..].find("${") {
        let start = cursor + start_rel;
        let remainder = &template[start + 2..];
        let Some(end_rel) = remainder.find('}') else {
            return Err(MediaPmError::Workflow(format!(
                "invalid command selector '{template}': missing closing '}}'"
            )));
        };
        let token = &remainder[..end_rel];

        if let Some((target, value)) = parse_platform_conditional_path_token(token)? {
            result.insert(target, value);
        }

        cursor = start + 2 + end_rel + 1;
    }

    if result.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "tool command '{template}' did not contain any context.os selectors"
        )));
    }

    Ok(result)
}

/// Parses one `${...}` token into a platform target/path selector when present.
fn parse_platform_conditional_path_token(
    token: &str,
) -> Result<Option<(String, String)>, MediaPmError> {
    if !token.contains("context.os") {
        return Ok(None);
    }

    let Some((condition, branches)) = token.split_once('?') else {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; expected '?<true>|<false>'"
        )));
    };
    let Some((true_branch, _false_branch)) = branches.split_once('|') else {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; expected '<true>|<false>'"
        )));
    };

    let condition = condition.trim();
    let Some(remainder) = condition.strip_prefix("context.os") else {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; condition must start with 'context.os'"
        )));
    };
    let remainder = remainder.trim_start();
    let Some(remainder) = remainder.strip_prefix("==") else {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; condition must use '=='"
        )));
    };
    let target = parse_quoted_selector_value(remainder.trim()).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; target must be quoted"
        ))
    })?;

    let true_branch = true_branch.trim();
    let path = if let Some(decoded) = parse_quoted_selector_value(true_branch) {
        decoded
    } else {
        true_branch.to_string()
    };
    if path.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; true branch path is empty"
        )));
    }

    Ok(Some((target, path)))
}

/// Parses one single- or double-quoted selector fragment.
#[must_use]
fn parse_quoted_selector_value(value: &str) -> Option<String> {
    if value.len() < 2 {
        return None;
    }
    let first = value.chars().next()?;
    let last = value.chars().last()?;
    if !((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
        return None;
    }

    Some(value[first.len_utf8()..value.len() - last.len_utf8()].to_string())
}

/// Returns whether one stored tool specification currently points to a
/// workspace-local executable binary that exists on disk.
pub(super) fn tool_spec_has_binary(spec: &ToolSpec) -> bool {
    let ToolKindSpec::Executable { command, .. } = &spec.kind else {
        return false;
    };
    let Some(first) = command.first() else {
        return false;
    };
    Path::new(first).exists()
}

#[cfg(test)]
mod tests {
    use mediapm_conductor::InputBinding;

    use super::{
        FfmpegSlotLimits, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS, default_input_defaults_for_tool,
        option_input_names_for_tool,
    };

    /// Verifies generated input defaults include every declared managed-tool
    /// option key so runtime config remains explicit and override-friendly.
    #[test]
    fn input_defaults_include_all_declared_option_inputs() {
        for tool_name in ["yt-dlp", "ffmpeg", "rsgain", "media-tagger"] {
            let defaults = default_input_defaults_for_tool(tool_name, FfmpegSlotLimits::default());
            for option_name in option_input_names_for_tool(tool_name) {
                assert!(
                    defaults.contains_key(*option_name),
                    "missing input_defaults entry '{option_name}' for tool '{tool_name}'"
                );
            }

            assert!(defaults.contains_key(INPUT_LEADING_ARGS));
            assert!(defaults.contains_key(INPUT_TRAILING_ARGS));
        }
    }

    /// Verifies default rsgain options match the expected loudness profile and
    /// explicit peak-safety behavior.
    #[test]
    fn rsgain_defaults_match_expected_loudness_profile() {
        let defaults = default_input_defaults_for_tool("rsgain", FfmpegSlotLimits::default());

        assert_eq!(defaults.get("target_lufs"), Some(&InputBinding::String("-18".to_string())));
        assert_eq!(defaults.get("album"), Some(&InputBinding::String("false".to_string())));
        assert_eq!(defaults.get("album_mode"), Some(&InputBinding::String("false".to_string())));
        assert_eq!(defaults.get("true_peak"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(defaults.get("clip_mode"), Some(&InputBinding::String("p".to_string())));
        assert_eq!(defaults.get("max_peak"), Some(&InputBinding::String("0".to_string())));
    }
}
