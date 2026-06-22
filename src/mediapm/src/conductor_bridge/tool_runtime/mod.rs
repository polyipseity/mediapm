//! Tool command, environment, and config-policy helpers.
#![allow(dead_code)]

use std::collections::BTreeMap;

use mediapm_cas::Hash;
use mediapm_conductor::model::config::ToolInputKind;
use mediapm_conductor::{ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec};

use super::legacy::{InputBinding, OutputCaptureKind, ToolConfigSpec, ToolOutputSpec};

use super::constants::*;
use crate::config::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS as DEFAULT_FFMPEG_MAX_INPUT_SLOTS_U32,
    DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS as DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS_U32, ToolRequirement,
};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::tools::downloader::{ProvisionedToolPayload, ResolvedToolIdentity};

/// Prefix for indexed ffmpeg content inputs.
const INPUT_FFMPEG_CONTENT_PREFIX: &str = "input_content_";
/// Prefix for indexed ffmpeg output-path option inputs.
const INPUT_FFMPEG_OUTPUT_PATH_PREFIX: &str = "output_path_";
/// Internal rsgain-only input selecting sandbox materialization extension.
const INPUT_RSGAIN_INPUT_EXTENSION: &str = "input_extension";

/// Fixed sandbox input path used when materializing byte-content inputs.
const SANDBOX_INPUT_FILE: &str = "inputs/input.bin";
/// Fixed sandbox input file path for media tools that edit payloads in place.
///
/// The `.media` extension intentionally avoids implying any specific codec or
/// container so workflows can preserve source-container semantics (for example
/// MKV with video+audio) across `media-tagger` and `rsgain` stages.
const SANDBOX_MEDIA_INPUT_FILE: &str = "inputs/input.media";
/// File extensions supported by `rsgain 3.7` for in-place tag writing.
///
/// Upstream support matrix:
/// - FLAC (`.flac`),
/// - Ogg (`.ogg`, `.oga`, `.spx`),
/// - Opus (`.opus`),
/// - MP2 (`.mp2`),
/// - MP3 (`.mp3`),
/// - MP4 (`.mp4`, `.m4a`),
/// - WMA (`.wma`),
/// - `WavPack` (`.wv`),
/// - APE (`.ape`),
/// - WAV (`.wav`),
/// - AIFF/AU-family (`.aiff`, `.aif`, `.snd`),
/// - TAK (`.tak`).
///
/// Managed runtime materializes the exact configured extension so the upstream
/// `rsgain` binary can recognize the input type from its path suffix and
/// workflows can prefer codec-copy extraction whenever a supported upstream
/// container is already available.
pub(crate) const SUPPORTED_RSGAIN_INPUT_EXTENSIONS: &[&str] = &[
    "flac", "ogg", "oga", "spx", "opus", "mp2", "mp3", "mp4", "m4a", "wma", "wv", "ape", "wav",
    "aiff", "aif", "snd", "tak",
];
/// Default extension (with leading dot) for generated ffmpeg output files.
const DEFAULT_FFMPEG_OUTPUT_EXTENSION_WITH_DOT: &str = ".mkv";
/// Fixed sandbox metadata path for ffmpeg metadata-input materialization.
const SANDBOX_FFMETADATA_INPUT_FILE: &str = "inputs/input.ffmeta";
/// Fixed sandbox file path used by `sd` in-place rewrite operations.
const SANDBOX_SD_INPUT_FILE: &str = "inputs/input.ffmeta";
/// Managed yt-dlp output template that embeds a marker before extension.
#[cfg(test)]
const YT_DLP_DEFAULT_OUTPUT_TEMPLATE: &str =
    "%(title)s [%(id)s]%(playlist_index|)s__mediapm__.%(ext)s";
/// Managed yt-dlp extractor args that keep comment extraction bounded while
/// Skips translated-subtitle variants that cause provider-side HTTP 429 throttling
/// on broad `sub_langs` selectors.
#[cfg(test)]
const YT_DLP_DEFAULT_EXTRACTOR_ARGS: &str = "youtube:skip=translated_subs";
/// Regex for yt-dlp output file paths.
const YT_DLP_OUTPUT_CONTENT_REGEX: &str = "^downloads/.+(?:__mediapm__)?[.](?:3gp|aac|aiff?|alac|asf|avi|flac|m4a|m4v|mka|mkv|mov|mp3|mp4|mpeg|mpg|oga|ogg|opus|wav|weba|webm|wma)$";
/// Regex for yt-dlp description output file paths.
const YT_DLP_DESCRIPTION_OUTPUT_REGEX: &str = "^downloads/.+(?:__mediapm__)?[.]description$";
/// Regex for yt-dlp info-json output file paths.
const YT_DLP_INFOJSON_OUTPUT_REGEX: &str = "^downloads/.+(?:__mediapm__)?[.]info[.]json$";
/// Regex for yt-dlp download archive file path.
const YT_DLP_ARCHIVE_OUTPUT_REGEX: &str = "^downloads/archive[.]txt$";
/// Regex for yt-dlp subtitle-family sidecar files.
const YT_DLP_SUBTITLE_ARTIFACTS_REGEX: &str = "^downloads/(.+?)(?:__mediapm__)?((?:[.][^/.]+)?[.](?:ass|dfxp|json3|lrc|srt|srv1|srv2|srv3|ssa|ttml|vtt))$";
/// Regex for yt-dlp thumbnail sidecar files.
const YT_DLP_THUMBNAIL_ARTIFACTS_REGEX: &str = "^downloads/(?:(.+?)__mediapm__((?:[.][0-9]+)?[.](?:avif|bmp|gif|jpe?g|png|webp))|(.+?)([.](?:avif|bmp|gif|jpe?g|png|webp)))$";
/// Regex for yt-dlp annotation sidecar file output.
const YT_DLP_ANNOTATION_OUTPUT_REGEX: &str =
    "^downloads/.+(?:__mediapm__)?[.](?:annotation[.]xml|annotation)$";
/// Regex for yt-dlp internet-shortcut sidecar files.
const YT_DLP_LINK_ARTIFACTS_REGEX: &str =
    "^downloads/(.+?)(?:__mediapm__)?([.](?:desktop|url|webloc))$";
/// Regex for yt-dlp split-chapter media files.
const YT_DLP_CHAPTER_ARTIFACTS_REGEX: &str = "^downloads/(?:(.+?)__mediapm__((?: - .+)?[.](?:3gp|aac|aiff?|alac|asf|avi|flac|m4a|m4v|mka|mkv|mov|mp3|mp4|mpeg|mpg|oga|ogg|opus|wav|weba|webm|wma))|(.+?)((?: - .+)?[.](?:3gp|aac|aiff?|alac|asf|avi|flac|m4a|m4v|mka|mkv|mov|mp3|mp4|mpeg|mpg|oga|ogg|opus|wav|weba|webm|wma)))$";
/// Regex for yt-dlp playlist-description file output.
const YT_DLP_PLAYLIST_DESCRIPTION_OUTPUT_REGEX: &str =
    "^downloads/.+\\x5B[^/]+\\x5D[0-9]+(?:__mediapm__)?[.]description$";
/// Regex for yt-dlp playlist-infojson file output.
const YT_DLP_PLAYLIST_INFOJSON_OUTPUT_REGEX: &str =
    "^downloads/.+\\x5B[^/]+\\x5D[0-9]+(?:__mediapm__)?[.]info[.]json$";
/// Sandbox directory where yt-dlp materializes downloaded output artifacts.
const SANDBOX_DOWNLOADS_DIR: &str = "downloads";
/// Sandbox directory where template `:file(...)` inputs are materialized.
const SANDBOX_INPUTS_DIR: &str = "inputs";
/// Fixed sandbox output file path for media-tagger `FFmetadata` documents.
const MEDIA_TAGGER_OUTPUT_FILE: &str = "metadata/output.ffmeta";

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

/// Environment variable carrying resolved yt-dlp ffmpeg companion path for
/// `ffmpeg_location` input default injection.
pub(super) const YT_DLP_FFMPEG_LOCATION_ENV: &str = "MEDIAPM_YT_DLP_FFMPEG_LOCATION";
/// Environment variable carrying resolved yt-dlp deno (`js_runtimes`) companion path for
/// `js_runtimes` input default injection.
pub(super) const YT_DLP_JS_RUNTIMES_ENV: &str = "MEDIAPM_YT_DLP_JS_RUNTIMES";

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

/// MOV/ISOBMFF-family container values that should receive managed
/// `-movflags +faststart` auto-injection.
///
/// These values include common file extensions (`mp4`, `m4a`, `m4v`) and
/// ffmpeg muxer names/aliases (`ism`, `ismv`, `ipod`, `psp`, `f4v`).
const FFMPEG_MOV_FASTSTART_CONTAINERS: &[&str] =
    &["mp4", "mov", "m4a", "m4v", "3gp", "3g2", "f4v", "ism", "ismv", "ipod", "psp"];

/// Matroska-family container values that should receive managed
/// `-cues_to_front 1` auto-injection.
///
/// Includes extension-style aliases (`mkv`, `mka`, `mks`, `mk3d`) and the
/// canonical muxer name (`matroska`) plus `webm`.
const FFMPEG_MATROSKA_CUES_TO_FRONT_CONTAINERS: &[&str] =
    &["matroska", "mkv", "mka", "mks", "mk3d", "webm"];

/// Returns indexed ffmpeg input-content field name.
#[must_use]
pub(crate) fn ffmpeg_input_content_name(index: usize) -> String {
    format!("{INPUT_FFMPEG_CONTENT_PREFIX}{index}")
}

/// Returns indexed ffmpeg cover-art slot flag input field name.
#[must_use]
pub(crate) fn ffmpeg_cover_slot_enabled_input_name(index: usize) -> String {
    format!("cover_art_slot_enabled_{index}")
}

/// Returns indexed ffmpeg output-path input field name.
#[must_use]
pub(crate) fn ffmpeg_output_path_input_name(index: usize) -> String {
    format!("{INPUT_FFMPEG_OUTPUT_PATH_PREFIX}{index}")
}

/// Returns indexed ffmpeg output capture name.
#[must_use]
pub(crate) fn ffmpeg_output_capture_name(index: usize) -> String {
    if index == 0 { "primary".to_string() } else { format!("primary_{index}") }
}

/// Returns sandbox-relative ffmpeg output file path for one indexed slot.
#[must_use]
pub(crate) fn ffmpeg_output_file_path(index: usize) -> String {
    format!("output-{index}{DEFAULT_FFMPEG_OUTPUT_EXTENSION_WITH_DOT}")
}

/// Returns regex pattern for one indexed ffmpeg output capture path.
#[must_use]
fn ffmpeg_output_file_regex(index: usize) -> String {
    format!(r"^output-{index}(?:[.][^/\\\\]+)?$")
}

/// Returns sandbox-relative ffmpeg input file path for one indexed slot.
#[must_use]
fn ffmpeg_input_file_path(index: usize) -> String {
    format!("inputs/input-{index}.bin")
}

/// Returns sandbox-relative rsgain input file path for one supported extension.
#[must_use]
fn rsgain_input_file_path(extension: &str) -> String {
    format!("inputs/input.{extension}")
}

/// Returns regex pattern for rsgain-modified file captures.
#[must_use]
fn rsgain_output_file_regex() -> String {
    format!("^inputs/input[.](?:{})$", SUPPORTED_RSGAIN_INPUT_EXTENSIONS.join("|"))
}

/// Builds executable command vector for one provisioned tool payload.
#[allow(clippy::too_many_lines)]
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

        for index in 1..ffmpeg_slot_limits.max_input_slots {
            let input_name = ffmpeg_input_content_name(index);
            let input_path = ffmpeg_input_file_path(index);
            command.push(format!("${{*inputs.{input_name} ? -i | ''}}"));
            command.push(format!(
                "${{*inputs.{input_name} ? inputs.{input_name}:file({input_path}) | ''}}"
            ));
        }

        command.extend(command_option_tokens_for_tool("ffmpeg", FFMPEG_OPTION_INPUTS));

        // Auto-inject container-conditional flags after the regular options block.
        // `container` may be set explicitly by users or inferred by workflow
        // synthesis from the effective output extension when omitted.
        // If the user also sets `movflags`/`cues_to_front` explicitly, ffmpeg
        // treats repeated occurrences additively so both values remain valid.

        // MOV/ISOBMFF family: inject `-movflags +faststart` so progressive
        // download can start without waiting for the full moov atom at EOF.
        let mov_family_condition =
            ffmpeg_container_any_of_condition(FFMPEG_MOV_FASTSTART_CONTAINERS);
        command.push(format!("${{{mov_family_condition} ? -movflags | ''}}"));
        command.push(format!("${{{mov_family_condition} ? +faststart | ''}}"));

        // Matroska/WebM family: inject `-cues_to_front 1` so cue entries are
        // written toward the front for faster seekability.
        let matroska_family_condition =
            ffmpeg_container_any_of_condition(FFMPEG_MATROSKA_CUES_TO_FRONT_CONTAINERS);
        command.push(format!("${{{matroska_family_condition} ? -cues_to_front | ''}}"));
        command.push(format!("${{{matroska_family_condition} ? 1 | ''}}"));

        command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));
        command.extend(ffmpeg_cover_art_tokens(ffmpeg_slot_limits));

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
            format!("${{*inputs.{INPUT_CONTENT} ? --input | ''}}"),
            format!(
                "${{*inputs.{INPUT_CONTENT} ? inputs.{INPUT_CONTENT}:file({SANDBOX_MEDIA_INPUT_FILE}) | ''}}"
            ),
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
        for extension in SUPPORTED_RSGAIN_INPUT_EXTENSIONS {
            let input_path = rsgain_input_file_path(extension);
            command.push(format!(
                "${{*inputs.{INPUT_RSGAIN_INPUT_EXTENSION} == \"{extension}\" ? inputs.{INPUT_CONTENT}:file({input_path}) | ''}}"
            ));
        }
        return command;
    }

    if lower_name == "sd" {
        return vec![
            provisioned.command_selector.clone(),
            format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
            format!("${{*inputs.{INPUT_SD_PATTERN}}}"),
            format!("${{*inputs.{INPUT_SD_REPLACEMENT}}}"),
            format!("${{inputs.{INPUT_CONTENT}:file({SANDBOX_SD_INPUT_FILE})}}"),
            format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"),
        ];
    }

    vec![
        provisioned.command_selector.clone(),
        format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
        format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"),
    ]
}

/// Builds one complete executable tool specification for generated tool rows.
pub(crate) fn build_tool_spec(
    _paths: &MediaPmPaths,
    tool_name: &str,
    provisioned: &ProvisionedToolPayload,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> ToolSpec {
    ToolSpec {
        kind: ToolKindSpec::Executable {
            command: build_tool_command(tool_name, provisioned, ffmpeg_slot_limits),
            env_vars: BTreeMap::new(),
            success_codes: success_codes_for_tool(tool_name),
        },
        name: tool_name.to_string(),
        version: String::new(),
        inputs: build_tool_inputs(tool_name, ffmpeg_slot_limits),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime {
            content_map: BTreeMap::new(),
            impure: is_internet_required_tool(tool_name),
            inherited_env_vars: BTreeMap::new(),
            max_concurrent_calls: default_max_concurrent_calls(tool_name),
            max_retries: default_max_retries(tool_name),
        },
    }
}

/// Returns whether one managed tool requires external network access.
#[must_use]
fn is_internet_required_tool(tool_name: &str) -> bool {
    tool_name.eq_ignore_ascii_case("yt-dlp") || tool_name.eq_ignore_ascii_case("media-tagger")
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

    for option_input in option_input_names_for_tool(tool_name) {
        let kind = if *option_input == "option_args" {
            ToolInputKind::String
        } else {
            ToolInputKind::String
        };
        inputs.insert(
            (*option_input).to_string(),
            ToolInputSpec { kind, description: String::new(), required: false },
        );
    }

    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        inputs.insert(
            INPUT_SOURCE_URL.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    } else if tool_name.eq_ignore_ascii_case("ffmpeg") {
        for index in 0..ffmpeg_slot_limits.max_input_slots {
            inputs.insert(
                ffmpeg_input_content_name(index),
                ToolInputSpec {
                    kind: ToolInputKind::String,
                    description: String::new(),
                    required: false,
                },
            );
        }
        for index in 1..ffmpeg_slot_limits.max_input_slots {
            inputs.insert(
                ffmpeg_cover_slot_enabled_input_name(index),
                ToolInputSpec {
                    kind: ToolInputKind::String,
                    description: String::new(),
                    required: false,
                },
            );
        }
        for index in 0..ffmpeg_slot_limits.max_output_slots {
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
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    } else if tool_name.eq_ignore_ascii_case("sd") {
        inputs.insert(
            INPUT_CONTENT.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
        inputs.insert(
            INPUT_SD_PATTERN.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
        inputs.insert(
            INPUT_SD_REPLACEMENT.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    } else {
        inputs.insert(
            INPUT_CONTENT.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    }

    inputs
}

/// Builds declared output capture contracts for one managed executable tool.
#[allow(clippy::too_many_lines)]
fn build_tool_outputs(
    tool_name: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, ToolOutputSpec> {
    let sandbox_artifacts_path = sandbox_artifacts_folder_for_tool(tool_name).to_string();

    let mut outputs = BTreeMap::new();

    if tool_name.eq_ignore_ascii_case("ffmpeg") {
        outputs.insert(
            OUTPUT_CONTENT.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureKind::FileRegex { path_regex: ffmpeg_output_file_regex(0) },
                allow_empty: false,
            },
        );
        for index in 0..ffmpeg_slot_limits.max_output_slots {
            let path_regex = ffmpeg_output_file_regex(index);
            outputs.insert(
                ffmpeg_output_capture_name(index),
                ToolOutputSpec {
                    capture: OutputCaptureKind::FileRegex { path_regex: path_regex.clone() },
                    allow_empty: false,
                },
            );
            outputs.insert(
                format!("{OUTPUT_CONTENT}_{index}"),
                ToolOutputSpec {
                    capture: OutputCaptureKind::FileRegex { path_regex },
                    allow_empty: false,
                },
            );
        }
    } else {
        let output_capture = if tool_name.eq_ignore_ascii_case("yt-dlp") {
            OutputCaptureKind::FileRegex { path_regex: YT_DLP_OUTPUT_CONTENT_REGEX.to_string() }
        } else if tool_name.eq_ignore_ascii_case("media-tagger") {
            OutputCaptureKind::File { path: MEDIA_TAGGER_OUTPUT_FILE.to_string() }
        } else if tool_name.eq_ignore_ascii_case("rsgain") {
            OutputCaptureKind::FileRegex { path_regex: rsgain_output_file_regex() }
        } else if tool_name.eq_ignore_ascii_case("sd") {
            OutputCaptureKind::File { path: SANDBOX_SD_INPUT_FILE.to_string() }
        } else {
            OutputCaptureKind::File { path: SANDBOX_INPUT_FILE.to_string() }
        };

        outputs.insert(
            OUTPUT_CONTENT.to_string(),
            ToolOutputSpec { capture: output_capture, allow_empty: false },
        );

        if tool_name.eq_ignore_ascii_case("yt-dlp") {
            outputs.insert(
                "primary".to_string(),
                ToolOutputSpec {
                    capture: OutputCaptureKind::FileRegex {
                        path_regex: YT_DLP_OUTPUT_CONTENT_REGEX.to_string(),
                    },
                    allow_empty: false,
                },
            );
        }
    }

    outputs.insert(
        OUTPUT_SANDBOX_ARTIFACTS.to_string(),
        ToolOutputSpec {
            capture: OutputCaptureKind::Folder {
                path: sandbox_artifacts_path.clone(),
                include_topmost_folder: false,
            },
            // Sandbox artifact folders may be empty when the tool produces no
            // incidental sidecar files (e.g., rsgain, sd, and most ffmpeg runs).
            allow_empty: true,
        },
    );
    outputs.insert(
        "stdout".to_string(),
        ToolOutputSpec { capture: OutputCaptureKind::Stdout {}, allow_empty: true },
    );
    outputs.insert(
        "stderr".to_string(),
        ToolOutputSpec { capture: OutputCaptureKind::Stderr {}, allow_empty: true },
    );
    outputs.insert(
        "process_code".to_string(),
        // process_code is always present for a completed subprocess; allow_empty is true
        // because the conductor treats absent process-code the same as an empty output
        // when the process terminates cleanly without emitting an exit-code record.
        ToolOutputSpec { capture: OutputCaptureKind::ProcessCode {}, allow_empty: true },
    );

    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        for (output_name, path_regex) in [
            (OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS, YT_DLP_SUBTITLE_ARTIFACTS_REGEX),
            (OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS, YT_DLP_THUMBNAIL_ARTIFACTS_REGEX),
            (OUTPUT_YT_DLP_LINK_ARTIFACTS, YT_DLP_LINK_ARTIFACTS_REGEX),
            (OUTPUT_YT_DLP_CHAPTER_ARTIFACTS, YT_DLP_CHAPTER_ARTIFACTS_REGEX),
        ] {
            outputs.insert(
                output_name.to_string(),
                // FolderRegex outputs may match no files when the relevant sidecar type
                // (subtitles, thumbnails, links, chapters) is absent for the media item.
                ToolOutputSpec {
                    capture: OutputCaptureKind::FolderRegex { path_regex: path_regex.to_string() },
                    allow_empty: true,
                },
            );
        }

        // All FileRegex sidecar outputs below may produce no file when yt-dlp does not
        // emit the corresponding sidecar (e.g. annotation files are retired, description
        // files are absent for some extractors, playlist sidecars are absent for single-
        // video downloads). Marking them allow_empty avoids hard workflow failures for
        // conditionally-produced artifacts.
        outputs.insert(
            OUTPUT_YT_DLP_DESCRIPTION_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureKind::FileRegex {
                    path_regex: YT_DLP_DESCRIPTION_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_ANNOTATION_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureKind::FileRegex {
                    path_regex: YT_DLP_ANNOTATION_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_INFOJSON_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureKind::FileRegex {
                    path_regex: YT_DLP_INFOJSON_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_ARCHIVE_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureKind::FileRegex {
                    path_regex: YT_DLP_ARCHIVE_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureKind::FileRegex {
                    path_regex: YT_DLP_PLAYLIST_DESCRIPTION_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureKind::FileRegex {
                    path_regex: YT_DLP_PLAYLIST_INFOJSON_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
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
    } else if tool_name.eq_ignore_ascii_case("media-tagger") {
        // Capture only the dedicated cover-art output folder, not the entire
        // inputs/ directory which includes the large input media file.
        "coverart"
    } else {
        SANDBOX_INPUTS_DIR
    }
}

/// Returns the default execution-concurrency policy for one logical tool.
///
/// Returns the default max concurrent calls for a tool.
///
/// Policy notes:
/// - `yt-dlp` defaults to at most one active call so remote download pressure
///   remains predictable and does not overrun provider throttling by default,
/// - all other tools return `0` (unbounded) unless users explicitly set a
///   stricter value in config.
#[must_use]
pub(super) fn default_max_concurrent_calls(tool_name: &str) -> usize {
    if tool_name.eq_ignore_ascii_case("yt-dlp") { 1 } else { 0 }
}

/// Returns the default retry budget policy for one logical tool.
///
/// Policy notes:
/// - `yt-dlp` keeps this at `1` because the downloader already has its own
///   internal network retry controls,
/// - other tools return `0` (use conductor runtime default).
#[must_use]
pub(super) fn default_max_retries(tool_name: &str) -> usize {
    if tool_name.eq_ignore_ascii_case("yt-dlp") { 1 } else { 0 }
}

/// Merges existing runtime tool config with default policy and fresh content map.
///
/// Returns a legacy [`ToolConfigSpec`] whose `max_concurrent_calls`/`max_retries`
/// use conductor's sentinel convention (`0` = use runtime default). Callers
/// should convert the result to v2 [`ToolRuntime`] before pushing into
/// `machine.tools` via [`legacy_to_runtime`].
pub(crate) fn merge_tool_config_defaults(
    existing: Option<&ToolConfigSpec>,
    _paths: &MediaPmPaths,
    tool_name: &str,
    content_map: BTreeMap<String, Hash>,
    default_description: String,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> ToolConfigSpec {
    let default_limit: i32 = default_max_concurrent_calls(tool_name).try_into().unwrap_or(0);
    let default_retries: i32 = default_max_retries(tool_name).try_into().unwrap_or(0);

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

/// Converts a legacy [`ToolConfigSpec`] + [`ToolKindSpec`] + tool name into
/// v2 [`ToolSpec`] and [`ToolRuntime`].
///
/// The returned [`ToolRuntime`] derives `max_concurrent_calls`/`max_retries`
/// from the config spec (treating `-1` as `0` = unlimited), and the
/// [`ToolSpec`] carries `name`, `kind`, `inputs`, `default_inputs`, and
/// `runtime`.
pub(crate) fn legacy_to_tool_spec(
    config: ToolConfigSpec,
    kind: ToolKindSpec,
    name: &str,
    inputs: BTreeMap<String, ToolInputSpec>,
) -> (ToolSpec, ToolRuntime) {
    let max_concurrent_calls: usize =
        if config.max_concurrent_calls >= 0 { config.max_concurrent_calls as usize } else { 0 };
    let max_retries: usize = if config.max_retries >= 0 { config.max_retries as usize } else { 0 };

    let content_map = config.content_map.unwrap_or_default();

    let runtime = ToolRuntime {
        content_map: content_map.into_iter().map(|(k, v)| (k, v.to_string())).collect(),
        impure: false,
        inherited_env_vars: config.env_vars,
        max_concurrent_calls,
        max_retries,
    };

    let spec = ToolSpec {
        kind,
        name: name.to_string(),
        version: String::new(),
        inputs,
        default_inputs: config
            .input_defaults
            .into_iter()
            .map(|(k, v)| match v {
                InputBinding::String(s) => (k, s),
                InputBinding::StringList(list) => (k, list.join(",")),
            })
            .collect(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };

    (spec, runtime)
}

/// Builds default generated input bindings for one managed tool.
///
/// These defaults prioritize high-quality, metadata-preserving outputs while
/// still allowing users to override all behavior through `input_defaults` or
/// step-level media `options` values.
// ── Static string-valued default input tables ────────────────────────────

const YT_DLP_INPUT_DEFAULTS: &[(&str, &str)] = &[
    ("paths", "downloads"),
    ("output", "%(title)s [%(id)s]%(playlist_index|)s__mediapm__.%(ext)s"),
    ("format", "bestvideo*+bestaudio/best"),
    ("sub_langs", "all"),
    ("merge_output_format", "mkv"),
    ("extractor_args", "youtube:skip=translated_subs"),
    ("embed_metadata", "true"),
    ("embed_chapters", "true"),
    ("embed_info_json", "true"),
    ("write_subs", "true"),
    ("write_auto_subs", "false"),
    ("write_thumbnail", "true"),
    ("write_all_thumbnails", "false"),
    ("write_info_json", "true"),
    ("clean_info_json", "true"),
    ("write_comments", "false"),
    ("write_description", "true"),
    ("write_annotations", "false"),
    ("write_chapters", "true"),
    ("write_link", "true"),
    ("write_url_link", "true"),
    ("write_webloc_link", "true"),
    ("write_desktop_link", "true"),
    ("download_archive", "downloads/archive.txt"),
    ("split_chapters", "false"),
    ("no_playlist", "true"),
    ("cache_dir", ""),
    ("ffmpeg_location", "ffmpeg"),
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
];

const SD_INPUT_DEFAULTS: &[(&str, &str)] = &[("pattern", ""), ("replacement", "")];

const MEDIA_TAGGER_INPUT_DEFAULTS: &[(&str, &str)] = &[
    ("strict_identification", "true"),
    ("write_all_tags", "true"),
    ("write_all_images", "true"),
    ("save_images_to_tags", "true"),
    ("embed_only_one_front_image", "false"),
    ("ca_providers", "caa_release,url_relationships,caa_release_group"),
    ("caa_image_types", "all,-matrix/runout,-raw/unedited,-watermark"),
    ("caa_image_size", "full"),
    ("caa_approved_only", "false"),
    ("preserve_images", "false"),
    ("clear_existing_tags", "false"),
    ("enable_tag_saving", "true"),
    ("release_ars", "true"),
    ("cover_art_slot_count", "16"),
    ("acoustid_endpoint", "https://api.acoustid.org/v2/lookup"),
    ("musicbrainz_endpoint", "https://musicbrainz.org/ws/2"),
    ("cache_dir", ""),
    ("cache_expiry_seconds", "86400"),
];

const FFMPEG_STATIC_DEFAULTS: &[(&str, &str)] = &[
    ("vn", "false"),
    ("an", "false"),
    ("codec_copy", "true"),
    ("map_metadata", "0"),
    ("map_chapters", "0"),
    ("movflags", ""),
    ("cues_to_front", ""),
    ("hide_banner", "true"),
];

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

    // Static string defaults from const lookup tables.
    let static_defaults: &[(&str, &str)] = if tool_name.eq_ignore_ascii_case("yt-dlp") {
        YT_DLP_INPUT_DEFAULTS
    } else if tool_name.eq_ignore_ascii_case("rsgain") {
        RSGAIN_INPUT_DEFAULTS
    } else if tool_name.eq_ignore_ascii_case("sd") {
        SD_INPUT_DEFAULTS
    } else if tool_name.eq_ignore_ascii_case("media-tagger") {
        MEDIA_TAGGER_INPUT_DEFAULTS
    } else if tool_name.eq_ignore_ascii_case("ffmpeg") {
        FFMPEG_STATIC_DEFAULTS
    } else {
        &[]
    };
    for &(name, value) in static_defaults {
        input_defaults.insert(name.to_string(), InputBinding::String(value.to_string()));
    }

    // Ffmpeg indexed slot defaults (dynamic — computed at runtime per slot limit).
    if tool_name.eq_ignore_ascii_case("ffmpeg") {
        for index in 0..ffmpeg_slot_limits.max_input_slots {
            input_defaults
                .insert(ffmpeg_input_content_name(index), InputBinding::String(String::new()));
        }
        for index in 1..ffmpeg_slot_limits.max_input_slots {
            input_defaults.insert(
                ffmpeg_cover_slot_enabled_input_name(index),
                InputBinding::String(String::new()),
            );
        }
        for index in 0..ffmpeg_slot_limits.max_output_slots {
            input_defaults
                .insert(ffmpeg_output_path_input_name(index), InputBinding::String(String::new()));
        }
        input_defaults
            .insert(INPUT_FFMETADATA_CONTENT.to_string(), InputBinding::String(String::new()));
    }

    input_defaults
}

/// Builds one default human description for generated tool runtime config rows.
#[must_use]
pub(crate) fn default_tool_config_description(
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
    paths: &MediaPmPaths,
    tool_name: &str,
) -> Result<BTreeMap<String, String>, MediaPmError> {
    let mut env_vars = BTreeMap::new();

    if tool_name.eq_ignore_ascii_case("media-tagger") {
        let launcher_key = media_tagger_launcher_mediapm_env_var_for_host()?;
        let mediapm_binary = resolve_media_tagger_launcher_binary_path(paths)?;
        let escaped_binary = escape_template_literal(mediapm_binary.to_string_lossy().as_ref());
        env_vars.insert(launcher_key.to_string(), escaped_binary);
    }

    Ok(env_vars)
}

mod option_constants;
mod option_tokens;

use self::option_constants::{
    FFMPEG_OPTION_INPUTS, MEDIA_TAGGER_OPTION_INPUTS, RSGAIN_OPTION_INPUTS, YT_DLP_OPTION_INPUTS,
};
#[cfg(test)]
use self::option_tokens::option_tokens_for_input;
use self::option_tokens::{
    command_option_tokens_for_tool, ffmpeg_container_any_of_condition, ffmpeg_cover_art_tokens,
    option_input_names_for_tool,
};

mod launcher;
mod template;

use self::launcher::resolve_media_tagger_launcher_binary_path;
use self::template::escape_template_literal;
pub(super) use self::template::{
    extract_platform_conditional_paths, media_tagger_launcher_mediapm_env_var_for_host,
    tool_spec_has_binary, validate_tool_command,
};

#[cfg(test)]
mod tests {
    use std::fs;

    use std::collections::BTreeMap;

    use mediapm_cas::Hash;
    use tempfile::tempdir;

    use crate::conductor_bridge::legacy::InputBinding;
    use crate::conductor_bridge::legacy::OutputCaptureKind as OutputCaptureSpec;

    use crate::conductor_bridge::tool_runtime::{
        FfmpegSlotLimits, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS, OUTPUT_CONTENT,
        OUTPUT_SANDBOX_ARTIFACTS, OUTPUT_YT_DLP_ANNOTATION_FILE, OUTPUT_YT_DLP_ARCHIVE_FILE,
        OUTPUT_YT_DLP_CHAPTER_ARTIFACTS, OUTPUT_YT_DLP_DESCRIPTION_FILE,
        OUTPUT_YT_DLP_INFOJSON_FILE, OUTPUT_YT_DLP_LINK_ARTIFACTS,
        OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE, OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE,
        OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS, OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS, SANDBOX_DOWNLOADS_DIR,
        YT_DLP_DEFAULT_EXTRACTOR_ARGS, YT_DLP_DEFAULT_OUTPUT_TEMPLATE, YT_DLP_OPTION_INPUTS,
        build_tool_outputs, default_input_defaults_for_tool, option_input_names_for_tool,
        option_tokens_for_input, rsgain_output_file_regex, validate_tool_command,
    };

    use crate::conductor_bridge::tool_runtime::launcher::{
        find_workspace_root_for_target_dir, resolve_profile_adjacent_mediapm_binary_for_example,
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

    /// Verifies default rsgain options keep single-track mode with the
    /// expected loudness profile and explicit peak-safety behavior.
    #[test]
    fn rsgain_defaults_match_expected_loudness_profile() {
        let defaults = default_input_defaults_for_tool("rsgain", FfmpegSlotLimits::default());

        assert_eq!(defaults.get("target_lufs"), Some(&InputBinding::String("-18".to_string())));
        assert_eq!(defaults.get("album"), Some(&InputBinding::String("false".to_string())));
        assert_eq!(defaults.get("album_mode"), Some(&InputBinding::String("false".to_string())));
        assert_eq!(defaults.get("tagmode"), Some(&InputBinding::String("i".to_string())));
        assert_eq!(defaults.get("true_peak"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(defaults.get("clip_mode"), Some(&InputBinding::String("p".to_string())));
        assert_eq!(defaults.get("max_peak"), Some(&InputBinding::String("0".to_string())));
    }

    /// Verifies yt-dlp defaults prioritize one best thumbnail while keeping
    /// manual subtitles enabled and automatic subtitles disabled by default.
    #[test]
    fn yt_dlp_defaults_prefer_single_best_thumbnail_with_split_subtitle_defaults() {
        let defaults = default_input_defaults_for_tool("yt-dlp", FfmpegSlotLimits::default());

        assert_eq!(defaults.get("write_subs"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(
            defaults.get("write_auto_subs"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(defaults.get("sub_langs"), Some(&InputBinding::String("all".to_string())));
        assert!(
            YT_DLP_DEFAULT_EXTRACTOR_ARGS.contains("skip=translated_subs"),
            "yt-dlp managed defaults should skip auto-translated subtitles"
        );
        assert_eq!(
            defaults.get("paths"),
            Some(&InputBinding::String(SANDBOX_DOWNLOADS_DIR.to_string()))
        );
        assert_eq!(
            defaults.get("output"),
            Some(&InputBinding::String(YT_DLP_DEFAULT_OUTPUT_TEMPLATE.to_string()))
        );
        assert_eq!(
            defaults.get("extractor_args"),
            Some(&InputBinding::String(YT_DLP_DEFAULT_EXTRACTOR_ARGS.to_string()))
        );
        assert!(
            YT_DLP_DEFAULT_OUTPUT_TEMPLATE.contains("%(playlist_index|)s"),
            "yt-dlp default template should include playlist-index marker slot"
        );
        assert_eq!(
            defaults.get("write_thumbnail"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            defaults.get("write_all_thumbnails"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(
            defaults.get("clean_info_json"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            defaults.get("write_comments"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(
            defaults.get("write_annotations"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(defaults.get("write_chapters"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(defaults.get("write_url_link"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(
            defaults.get("write_webloc_link"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            defaults.get("write_desktop_link"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            defaults.get("download_archive"),
            Some(&InputBinding::String("downloads/archive.txt".to_string()))
        );
        assert_eq!(defaults.get("cache_dir"), Some(&InputBinding::String(String::new())));
    }

    /// Verifies simplified subtitle input wiring controls only manual subtitle
    /// switches through `write_subs`.
    #[test]
    fn yt_dlp_write_subs_tokens_cover_manual_switch_only() {
        assert!(YT_DLP_OPTION_INPUTS.contains(&"write_auto_subs"));

        let tokens = option_tokens_for_input("yt-dlp", "write_subs");
        assert!(
            tokens.contains(&"${*inputs.write_subs == \"true\" ? --write-subs | ''}".to_string())
        );
        assert!(
            !tokens.iter().any(|token| token.contains("write-auto-subs")),
            "write_subs should not emit automatic-subtitle flags"
        );
    }

    /// Verifies `write_auto_subs` is independently mapped to automatic subtitle
    /// CLI switches.
    #[test]
    fn yt_dlp_write_auto_subs_tokens_cover_auto_subtitle_switch_only() {
        let tokens = option_tokens_for_input("yt-dlp", "write_auto_subs");
        assert!(tokens.contains(
            &"${*inputs.write_auto_subs == \"true\" ? --write-auto-subs | ''}".to_string()
        ));
        assert!(
            !tokens.iter().any(|token| token.contains("--write-subs")),
            "write_auto_subs should not emit manual-subtitle flags"
        );
    }

    /// Verifies media-tagger defaults include strict-identification behavior,
    /// runtime-root cache location, and one-day cache expiry budget.
    #[test]
    fn media_tagger_defaults_include_workspace_cache_and_expiry() {
        let defaults = default_input_defaults_for_tool("media-tagger", FfmpegSlotLimits::default());

        assert_eq!(
            defaults.get("strict_identification"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            defaults.get("embed_only_one_front_image"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(
            defaults.get("save_images_to_tags"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            defaults.get("ca_providers"),
            Some(&InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_CA_PROVIDERS.to_string()
            ))
        );
        assert_eq!(
            defaults.get("caa_image_types"),
            Some(&InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_CAA_IMAGE_TYPES.to_string()
            ))
        );
        assert_eq!(
            defaults.get("caa_image_size"),
            Some(&InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_CAA_IMAGE_SIZE.to_string()
            ))
        );
        assert_eq!(
            defaults.get("caa_approved_only"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(
            defaults.get("enable_tag_saving"),
            Some(&InputBinding::String("true".to_string()))
        );

        assert_eq!(defaults.get("cache_dir"), Some(&InputBinding::String(String::new())));
        assert_eq!(
            defaults.get("cache_expiry_seconds"),
            Some(&InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_CACHE_EXPIRY_SECONDS.to_string()
            ))
        );
    }

    /// Verifies rsgain output capture accepts the full managed supported
    /// extension set and no longer exposes legacy unsupported placeholders.
    #[test]
    fn rsgain_output_capture_supports_expected_sandbox_path_variants() {
        let outputs = build_tool_outputs("rsgain", FfmpegSlotLimits::default());
        let output = outputs.get("content").expect("missing content capture");

        let OutputCaptureSpec::FileRegex { path_regex } = &output.capture else {
            panic!("expected file-regex capture for rsgain content");
        };

        assert_eq!(path_regex, &rsgain_output_file_regex());
        assert!(!path_regex.contains("mkv"));
        assert!(!path_regex.contains("mka"));
    }

    /// Verifies yt-dlp sidecar-family outputs use regex folder captures so
    /// one shared downloader run can publish isolated artifact bundles.
    #[test]
    fn yt_dlp_sidecar_outputs_use_regex_folder_captures() {
        let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());

        for output_name in [OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS, OUTPUT_YT_DLP_LINK_ARTIFACTS] {
            let output = outputs
                .get(output_name)
                .unwrap_or_else(|| panic!("missing output capture '{output_name}'"));
            match &output.capture {
                OutputCaptureSpec::FolderRegex { path_regex } => {
                    assert!(
                        path_regex.contains("(.+?)"),
                        "expected capture-group rename regex for '{output_name}', got '{path_regex}'"
                    );
                }
                other => {
                    panic!("expected folder_regex capture for '{output_name}', got '{other:?}'")
                }
            }
        }
    }

    /// Verifies yt-dlp optional sidecar/runtime captures allow empty matches
    /// while primary media outputs stay required.
    #[test]
    fn yt_dlp_optional_outputs_allow_empty_but_primary_outputs_remain_required() {
        let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());

        for required_output in [OUTPUT_CONTENT, "primary"] {
            let output = outputs
                .get(required_output)
                .unwrap_or_else(|| panic!("missing output capture '{required_output}'"));
            assert!(
                !output.allow_empty,
                "required yt-dlp output '{required_output}' must not allow empty capture"
            );
        }

        for optional_output in [
            OUTPUT_SANDBOX_ARTIFACTS,
            "stdout",
            "stderr",
            "process_code",
            OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
            OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
            OUTPUT_YT_DLP_LINK_ARTIFACTS,
            OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
            OUTPUT_YT_DLP_DESCRIPTION_FILE,
            OUTPUT_YT_DLP_ANNOTATION_FILE,
            OUTPUT_YT_DLP_INFOJSON_FILE,
            OUTPUT_YT_DLP_ARCHIVE_FILE,
            OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE,
            OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE,
        ] {
            let output = outputs
                .get(optional_output)
                .unwrap_or_else(|| panic!("missing output capture '{optional_output}'"));
            assert!(
                output.allow_empty,
                "optional yt-dlp output '{optional_output}' should allow empty capture"
            );
        }
    }

    /// Verifies chapter-family regex supports optional split suffixes so
    /// marker stripping also applies to unsplit yt-dlp output names.
    #[test]
    fn yt_dlp_chapter_regex_supports_unsplit_outputs_without_marker_leak() {
        let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());
        let chapter_output =
            outputs.get(OUTPUT_YT_DLP_CHAPTER_ARTIFACTS).expect("missing chapter output capture");

        let OutputCaptureSpec::FolderRegex { path_regex } = &chapter_output.capture else {
            panic!("expected folder_regex capture for chapter artifacts");
        };

        assert!(
            path_regex.contains("(?: - .+)?"),
            "chapter regex should allow optional split suffix for unsplit outputs: {path_regex}"
        );
        assert!(
            path_regex.contains("__mediapm__(") && path_regex.contains("|(.+?)("),
            "chapter regex should include marker-present vs marker-absent alternation captures: {path_regex}"
        );
    }

    /// Verifies thumbnail-family regex strips `__mediapm__` and supports an
    /// optional numeric index (e.g. `.0.jpg`) produced by yt-dlp when
    /// `write_all_thumbnails` is enabled.
    #[test]
    fn yt_dlp_thumbnail_regex_strips_marker_and_supports_numeric_index() {
        let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());
        let thumbnail_output = outputs
            .get(OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS)
            .expect("missing thumbnail output capture");

        let OutputCaptureSpec::FolderRegex { path_regex } = &thumbnail_output.capture else {
            panic!("expected folder_regex capture for thumbnail artifacts");
        };

        let re = regex::Regex::new(path_regex).expect("thumbnail regex must be valid");

        // Single thumbnail with marker — no numeric index.
        let captures = re
            .captures("downloads/Title [abc123]__mediapm__.jpg")
            .expect("single-thumbnail with marker should match");
        let parts: Vec<&str> = captures.iter().skip(1).flatten().map(|m| m.as_str()).collect();
        assert_eq!(parts.join(""), "Title [abc123].jpg", "marker must be stripped");

        // Numbered thumbnail with marker and numeric index.
        let captures = re
            .captures("downloads/Title [abc123]__mediapm__.0.jpg")
            .expect("indexed thumbnail with marker should match");
        let parts: Vec<&str> = captures.iter().skip(1).flatten().map(|m| m.as_str()).collect();
        assert_eq!(
            parts.join(""),
            "Title [abc123].0.jpg",
            "marker must be stripped while numeric index is preserved"
        );

        // Thumbnail without marker — passthrough behavior.
        let captures = re
            .captures("downloads/Title [abc123].jpg")
            .expect("thumbnail without marker should match");
        let parts: Vec<&str> = captures.iter().skip(1).flatten().map(|m| m.as_str()).collect();
        assert_eq!(parts.join(""), "Title [abc123].jpg", "passthrough path must be preserved");
    }
    /// Verifies singular yt-dlp annotation output uses file capture semantics
    /// rather than folder-regex artifact bundling.
    #[test]
    fn yt_dlp_annotation_output_uses_file_capture() {
        let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());
        let annotation_output =
            outputs.get(OUTPUT_YT_DLP_ANNOTATION_FILE).expect("missing annotation output capture");

        let OutputCaptureSpec::FileRegex { path_regex } = &annotation_output.capture else {
            panic!("expected file_regex capture for annotation output");
        };

        assert!(
            path_regex.contains("annotation"),
            "annotation capture regex should target annotation sidecar files: {path_regex}"
        );
    }

    /// Verifies playlist sidecar file captures require a playlist index marker
    /// so single-item/non-playlist downloads do not leak into playlist file
    /// outputs.
    #[test]
    fn yt_dlp_playlist_file_regexes_require_playlist_index_marker() {
        let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());

        for output_name in
            [OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE, OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE]
        {
            let playlist_output = outputs
                .get(output_name)
                .unwrap_or_else(|| panic!("missing playlist output capture '{output_name}'"));

            let OutputCaptureSpec::FileRegex { path_regex } = &playlist_output.capture else {
                panic!("expected file_regex capture for playlist output '{output_name}'");
            };

            assert!(
                path_regex.contains("\\x5B") && path_regex.contains("\\x5D[0-9]+"),
                "playlist capture regex should require bracket-terminated id followed by playlist index: {path_regex}"
            );
        }
    }

    /// Verifies platform-conditional command validation accepts a
    /// cross-platform content-map that satisfies all selector branches.
    #[test]
    fn validate_tool_command_accepts_all_platform_selector_targets() {
        let command = "${context.os == \"windows\" ? windows/tool.exe | ''}${context.os == \"linux\" ? linux/tool | ''}${context.os == \"macos\" ? macos/tool | ''}".to_string();

        let content_map = BTreeMap::from([
            ("windows/tool.exe".to_string(), Hash::from_content(b"windows-tool")),
            ("linux/tool".to_string(), Hash::from_content(b"linux-tool")),
            ("macos/tool".to_string(), Hash::from_content(b"macos-tool")),
        ]);

        validate_tool_command("fixture", &[command], &content_map)
            .expect("cross-platform platform content should validate");
    }

    /// Verifies platform-conditional command validation still fails when the
    /// non-host selector branch target is missing from `content_map`.
    #[test]
    fn validate_tool_command_rejects_missing_non_host_selector_target() {
        let command = "${context.os == \"windows\" ? windows/tool.exe | ''}${context.os == \"linux\" ? linux/tool | ''}${context.os == \"macos\" ? macos/tool | ''}".to_string();
        let content_map = BTreeMap::from([
            ("windows/tool.exe".to_string(), Hash::from_content(b"windows-tool")),
            ("linux/tool".to_string(), Hash::from_content(b"linux-tool")),
        ]);

        let error = validate_tool_command("fixture", &[command], &content_map)
            .expect_err("missing selector target should fail validation");
        let message = error.to_string();
        assert!(
            message.contains("command selector") && message.contains("content_map"),
            "unexpected validation error message: {message}"
        );
    }

    /// Verifies launcher resolution can reuse a profile-adjacent mediapm
    /// binary when examples run from `target/<profile>/examples`.
    #[test]
    fn resolve_profile_adjacent_binary_for_example_uses_neighboring_mediapm_binary() {
        let temp = tempdir().expect("tempdir");
        let target_dir = temp.path().join("target");
        let profile_dir = target_dir.join("debug");
        let examples_dir = profile_dir.join("examples");
        fs::create_dir_all(&examples_dir).expect("create examples directory");

        let current_exe = examples_dir.join(if cfg!(windows) { "demo.exe" } else { "demo" });
        fs::write(&current_exe, b"example").expect("write current exe fixture");

        let expected_binary =
            profile_dir.join(if cfg!(windows) { "mediapm.exe" } else { "mediapm" });
        fs::write(&expected_binary, b"mediapm").expect("write mediapm binary fixture");

        let resolved = resolve_profile_adjacent_mediapm_binary_for_example(&current_exe)
            .expect("resolution should succeed");
        assert_eq!(resolved.as_deref(), Some(expected_binary.as_path()));
    }

    /// Verifies non-example executables do not trigger profile-adjacent
    /// launcher path resolution.
    #[test]
    fn resolve_profile_adjacent_binary_for_non_example_returns_none() {
        let temp = tempdir().expect("tempdir");
        let current_exe = temp.path().join(if cfg!(windows) { "demo.exe" } else { "demo" });
        fs::write(&current_exe, b"example").expect("write current exe fixture");

        let resolved = resolve_profile_adjacent_mediapm_binary_for_example(&current_exe)
            .expect("resolution should succeed");
        assert!(resolved.is_none());
    }

    /// Verifies workspace-root inference climbs from target directories to the
    /// nearest Cargo workspace manifest.
    #[test]
    fn find_workspace_root_for_target_dir_detects_manifest_ancestor() {
        let temp = tempdir().expect("tempdir");
        let workspace_root = temp.path().join("workspace");
        fs::create_dir_all(&workspace_root).expect("create workspace root");
        fs::write(workspace_root.join("Cargo.toml"), b"[workspace]\nmembers=[]\n")
            .expect("write workspace manifest");

        let target_dir = workspace_root.join("target").join("custom-profile");
        fs::create_dir_all(&target_dir).expect("create target dir");

        let inferred =
            find_workspace_root_for_target_dir(&target_dir).expect("workspace root should resolve");
        assert_eq!(inferred, workspace_root);
    }
}
