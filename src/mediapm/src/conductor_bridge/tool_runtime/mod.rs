//! Tool command, environment, and config-policy helpers.

use std::collections::BTreeMap;

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
/// Optional scalar input carrying `FFmetadata` bytes for ffmpeg metadata merge.
const INPUT_FFMETADATA_CONTENT: &str = "ffmetadata_content";
/// Prefix for indexed ffmpeg output-path option inputs.
const INPUT_FFMPEG_OUTPUT_PATH_PREFIX: &str = "output_path_";
/// Internal rsgain-only input selecting sandbox materialization extension.
const INPUT_RSGAIN_INPUT_EXTENSION: &str = "input_extension";
/// Required regex pattern input for `sd` text replacement operations.
const INPUT_SD_PATTERN: &str = "pattern";
/// Required replacement-string input for `sd` text replacement operations.
const INPUT_SD_REPLACEMENT: &str = "replacement";
/// Scalar URL input used by download tools.
const INPUT_SOURCE_URL: &str = "source_url";

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
const YT_DLP_DEFAULT_OUTPUT_TEMPLATE: &str =
    "%(title)s [%(id)s]%(playlist_index|)s__mediapm__.%(ext)s";
/// Managed yt-dlp extractor args that keep comment extraction bounded while
/// Skips translated-subtitle variants that cause provider-side HTTP 429 throttling
/// on broad `sub_langs` selectors. Comment extraction bounds (`comment_sort`,
/// `max_comments`) are intentionally omitted from this base default because they
/// are redundant when `write_comments` is controlled through its own option key.
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
/// Output capture name exposing one tool's primary file content payload.
const OUTPUT_CONTENT: &str = "content";
/// Output capture name exposing the full sandbox artifact tree.
const OUTPUT_SANDBOX_ARTIFACTS: &str = "sandbox_artifacts";
/// Output capture name exposing yt-dlp subtitle artifact bundle.
const OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS: &str = "yt_dlp_subtitle_artifacts";
/// Output capture name exposing yt-dlp thumbnail artifact bundle.
const OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS: &str = "yt_dlp_thumbnail_artifacts";
/// Output capture name exposing yt-dlp annotation file payload.
const OUTPUT_YT_DLP_ANNOTATION_FILE: &str = "yt_dlp_annotation_file";
/// Output capture name exposing yt-dlp description file payload.
const OUTPUT_YT_DLP_DESCRIPTION_FILE: &str = "yt_dlp_description_file";
/// Output capture name exposing yt-dlp infojson file payload.
const OUTPUT_YT_DLP_INFOJSON_FILE: &str = "yt_dlp_infojson_file";
/// Output capture name exposing yt-dlp download-archive file payload.
const OUTPUT_YT_DLP_ARCHIVE_FILE: &str = "yt_dlp_archive_file";
/// Output capture name exposing yt-dlp internet-shortcut artifact bundle.
const OUTPUT_YT_DLP_LINK_ARTIFACTS: &str = "yt_dlp_link_artifacts";
/// Output capture name exposing yt-dlp split-chapter artifact bundle.
const OUTPUT_YT_DLP_CHAPTER_ARTIFACTS: &str = "yt_dlp_chapter_artifacts";
/// Output capture name exposing yt-dlp playlist-description file payload.
const OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE: &str = "yt_dlp_playlist_description_file";
/// Output capture name exposing yt-dlp playlist-infojson file payload.
const OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE: &str = "yt_dlp_playlist_infojson_file";
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
/// Environment variable carrying resolved yt-dlp deno (js_runtimes) companion path for
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
pub(super) fn ffmpeg_input_content_name(index: usize) -> String {
    format!("{INPUT_FFMPEG_CONTENT_PREFIX}{index}")
}

/// Returns indexed ffmpeg cover-art slot flag input field name.
#[must_use]
pub(super) fn ffmpeg_cover_slot_enabled_input_name(index: usize) -> String {
    format!("cover_art_slot_enabled_{index}")
}

/// Returns indexed ffmpeg output-path input field name.
#[must_use]
pub(super) fn ffmpeg_output_path_input_name(index: usize) -> String {
    format!("{INPUT_FFMPEG_OUTPUT_PATH_PREFIX}{index}")
}

/// Returns indexed ffmpeg output capture name.
#[must_use]
pub(super) fn ffmpeg_output_capture_name(index: usize) -> String {
    if index == 0 { "primary".to_string() } else { format!("primary_{index}") }
}

/// Returns sandbox-relative ffmpeg output file path for one indexed slot.
#[must_use]
pub(super) fn ffmpeg_output_file_path(index: usize) -> String {
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
pub(super) fn build_tool_spec(
    _paths: &MediaPmPaths,
    tool_name: &str,
    provisioned: &ProvisionedToolPayload,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> ToolSpec {
    ToolSpec {
        is_impure: is_internet_required_tool(tool_name),
        inputs: build_tool_inputs(tool_name, ffmpeg_slot_limits),
        kind: ToolKindSpec::Executable {
            command: build_tool_command(tool_name, provisioned, ffmpeg_slot_limits),
            env_vars: BTreeMap::new(),
            success_codes: success_codes_for_tool(tool_name),
        },
        outputs: build_tool_outputs(tool_name, ffmpeg_slot_limits),
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
        for index in 1..ffmpeg_slot_limits.max_input_slots {
            inputs.insert(
                ffmpeg_cover_slot_enabled_input_name(index),
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
    } else if tool_name.eq_ignore_ascii_case("sd") {
        inputs.insert(INPUT_CONTENT.to_string(), ToolInputSpec { kind: ToolInputKind::String });
        inputs.insert(INPUT_SD_PATTERN.to_string(), ToolInputSpec { kind: ToolInputKind::String });
        inputs.insert(
            INPUT_SD_REPLACEMENT.to_string(),
            ToolInputSpec { kind: ToolInputKind::String },
        );
    } else {
        inputs.insert(INPUT_CONTENT.to_string(), ToolInputSpec { kind: ToolInputKind::String });
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
                capture: OutputCaptureSpec::FileRegex { path_regex: ffmpeg_output_file_regex(0) },
                allow_empty: false,
            },
        );
        for index in 0..ffmpeg_slot_limits.max_output_slots {
            let path_regex = ffmpeg_output_file_regex(index);
            outputs.insert(
                ffmpeg_output_capture_name(index),
                ToolOutputSpec {
                    capture: OutputCaptureSpec::FileRegex { path_regex: path_regex.clone() },
                    allow_empty: false,
                },
            );
            outputs.insert(
                format!("{OUTPUT_CONTENT}_{index}"),
                ToolOutputSpec {
                    capture: OutputCaptureSpec::FileRegex { path_regex },
                    allow_empty: false,
                },
            );
        }
    } else {
        let output_capture = if tool_name.eq_ignore_ascii_case("yt-dlp") {
            OutputCaptureSpec::FileRegex { path_regex: YT_DLP_OUTPUT_CONTENT_REGEX.to_string() }
        } else if tool_name.eq_ignore_ascii_case("media-tagger") {
            OutputCaptureSpec::File { path: MEDIA_TAGGER_OUTPUT_FILE.to_string() }
        } else if tool_name.eq_ignore_ascii_case("rsgain") {
            OutputCaptureSpec::FileRegex { path_regex: rsgain_output_file_regex() }
        } else if tool_name.eq_ignore_ascii_case("sd") {
            OutputCaptureSpec::File { path: SANDBOX_SD_INPUT_FILE.to_string() }
        } else {
            OutputCaptureSpec::File { path: SANDBOX_INPUT_FILE.to_string() }
        };

        outputs.insert(
            OUTPUT_CONTENT.to_string(),
            ToolOutputSpec { capture: output_capture, allow_empty: false },
        );

        if tool_name.eq_ignore_ascii_case("yt-dlp") {
            outputs.insert(
                "primary".to_string(),
                ToolOutputSpec {
                    capture: OutputCaptureSpec::FileRegex {
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
            capture: OutputCaptureSpec::Folder {
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
        ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: true },
    );
    outputs.insert(
        "stderr".to_string(),
        ToolOutputSpec { capture: OutputCaptureSpec::Stderr {}, allow_empty: true },
    );
    outputs.insert(
        "process_code".to_string(),
        // process_code is always present for a completed subprocess; allow_empty is true
        // because the conductor treats absent process-code the same as an empty output
        // when the process terminates cleanly without emitting an exit-code record.
        ToolOutputSpec { capture: OutputCaptureSpec::ProcessCode {}, allow_empty: true },
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
                    capture: OutputCaptureSpec::FolderRegex { path_regex: path_regex.to_string() },
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
                capture: OutputCaptureSpec::FileRegex {
                    path_regex: YT_DLP_DESCRIPTION_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_ANNOTATION_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureSpec::FileRegex {
                    path_regex: YT_DLP_ANNOTATION_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_INFOJSON_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureSpec::FileRegex {
                    path_regex: YT_DLP_INFOJSON_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_ARCHIVE_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureSpec::FileRegex {
                    path_regex: YT_DLP_ARCHIVE_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureSpec::FileRegex {
                    path_regex: YT_DLP_PLAYLIST_DESCRIPTION_OUTPUT_REGEX.to_string(),
                },
                allow_empty: true,
            },
        );
        outputs.insert(
            OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE.to_string(),
            ToolOutputSpec {
                capture: OutputCaptureSpec::FileRegex {
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
    _paths: &MediaPmPaths,
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
#[expect(
    clippy::too_many_lines,
    reason = "per-tool defaults table is intentionally explicit for schema stability"
)]
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
        input_defaults
            .insert("paths".to_string(), InputBinding::String(SANDBOX_DOWNLOADS_DIR.to_string()));
        input_defaults.insert(
            "output".to_string(),
            InputBinding::String(YT_DLP_DEFAULT_OUTPUT_TEMPLATE.to_string()),
        );
        input_defaults.insert(
            "format".to_string(),
            InputBinding::String("bestvideo*+bestaudio/best".to_string()),
        );
        input_defaults.insert("sub_langs".to_string(), InputBinding::String("all".to_string()));
        input_defaults
            .insert("merge_output_format".to_string(), InputBinding::String("mkv".to_string()));
        input_defaults.insert(
            "extractor_args".to_string(),
            InputBinding::String(YT_DLP_DEFAULT_EXTRACTOR_ARGS.to_string()),
        );
        input_defaults
            .insert("embed_metadata".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("embed_chapters".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("embed_info_json".to_string(), InputBinding::String("true".to_string()));
        input_defaults.insert("write_subs".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("write_auto_subs".to_string(), InputBinding::String("false".to_string()));
        input_defaults
            .insert("write_thumbnail".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("write_all_thumbnails".to_string(), InputBinding::String("false".to_string()));
        input_defaults
            .insert("write_info_json".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("clean_info_json".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("write_comments".to_string(), InputBinding::String("false".to_string()));
        input_defaults
            .insert("write_description".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("write_annotations".to_string(), InputBinding::String("false".to_string()));
        input_defaults
            .insert("write_chapters".to_string(), InputBinding::String("true".to_string()));
        input_defaults.insert("write_link".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("write_url_link".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("write_webloc_link".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("write_desktop_link".to_string(), InputBinding::String("true".to_string()));
        input_defaults.insert(
            "download_archive".to_string(),
            InputBinding::String("downloads/archive.txt".to_string()),
        );
        input_defaults
            .insert("split_chapters".to_string(), InputBinding::String("false".to_string()));
        // Prevents single-item URLs from being treated as playlist downloads by default.
        // Explicitly set to "false" in steps that intentionally download full playlists.
        input_defaults.insert("no_playlist".to_string(), InputBinding::String("true".to_string()));
        input_defaults.insert("cache_dir".to_string(), InputBinding::String(String::new()));
        input_defaults
            .insert("ffmpeg_location".to_string(), InputBinding::String("ffmpeg".to_string()));
    } else if tool_name.eq_ignore_ascii_case("ffmpeg") {
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
        input_defaults.insert("vn".to_string(), InputBinding::String("false".to_string()));
        input_defaults.insert("an".to_string(), InputBinding::String("false".to_string()));
        input_defaults.insert("codec_copy".to_string(), InputBinding::String("true".to_string()));
        input_defaults.insert("map_metadata".to_string(), InputBinding::String("0".to_string()));
        input_defaults.insert("map_chapters".to_string(), InputBinding::String("0".to_string()));
        // `movflags` and `cues_to_front` default to empty so they do not emit
        // any flag unless the user sets them explicitly or the auto-inject
        // container-conditional tokens in `build_tool_command` fire based on
        // the `container` input value.  This avoids applying MP4-only
        // `+faststart` to Matroska outputs and avoids applying
        // `cues_to_front` to non-Matroska muxers.
        input_defaults.insert("movflags".to_string(), InputBinding::String(String::new()));
        input_defaults.insert("cues_to_front".to_string(), InputBinding::String(String::new()));
        // Suppress ffmpeg version/build banner on every invocation; reduces stderr noise
        // and avoids unnecessary output buffering in conductor's subprocess capture.
        // Steps that need the banner for diagnostics can override with hide_banner = "false".
        input_defaults.insert("hide_banner".to_string(), InputBinding::String("true".to_string()));
    } else if tool_name.eq_ignore_ascii_case("rsgain") {
        input_defaults.insert(
            INPUT_RSGAIN_INPUT_EXTENSION.to_string(),
            InputBinding::String("flac".to_string()),
        );
        input_defaults.insert("album".to_string(), InputBinding::String("false".to_string()));
        input_defaults.insert("album_mode".to_string(), InputBinding::String("false".to_string()));
        input_defaults.insert("target_lufs".to_string(), InputBinding::String("-18".to_string()));
        input_defaults.insert("tagmode".to_string(), InputBinding::String("i".to_string()));
        input_defaults.insert("clip_mode".to_string(), InputBinding::String("p".to_string()));
        input_defaults.insert("true_peak".to_string(), InputBinding::String("true".to_string()));
        input_defaults.insert("max_peak".to_string(), InputBinding::String("0".to_string()));
        input_defaults
            .insert("preserve_mtimes".to_string(), InputBinding::String("true".to_string()));
    } else if tool_name.eq_ignore_ascii_case("sd") {
        input_defaults.insert(INPUT_SD_PATTERN.to_string(), InputBinding::String(String::new()));
        input_defaults
            .insert(INPUT_SD_REPLACEMENT.to_string(), InputBinding::String(String::new()));
    } else if tool_name.eq_ignore_ascii_case("media-tagger") {
        input_defaults
            .insert("strict_identification".to_string(), InputBinding::String("true".to_string()));
        input_defaults
            .insert("write_all_tags".to_string(), InputBinding::String("true".to_string()));
        // Default is "true" so cover art is captured when identification succeeds.
        // Demo examples explicitly set this to "false" to reduce AcoustID/MusicBrainz
        // cover-art network pressure during automated runs.
        input_defaults
            .insert("write_all_images".to_string(), InputBinding::String("true".to_string()));
        input_defaults.insert(
            "save_images_to_tags".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_SAVE_IMAGES_TO_TAGS.to_string(),
            ),
        );
        input_defaults.insert(
            "embed_only_one_front_image".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE.to_string(),
            ),
        );
        input_defaults.insert(
            "ca_providers".to_string(),
            InputBinding::String(crate::builtins::media_tagger::DEFAULT_CA_PROVIDERS.to_string()),
        );
        input_defaults.insert(
            "caa_image_types".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_CAA_IMAGE_TYPES.to_string(),
            ),
        );
        input_defaults.insert(
            "caa_image_size".to_string(),
            InputBinding::String(crate::builtins::media_tagger::DEFAULT_CAA_IMAGE_SIZE.to_string()),
        );
        input_defaults.insert(
            "caa_approved_only".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_CAA_APPROVED_ONLY.to_string(),
            ),
        );
        input_defaults.insert(
            "preserve_images".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_PRESERVE_IMAGES.to_string(),
            ),
        );
        input_defaults.insert(
            "clear_existing_tags".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_CLEAR_EXISTING_TAGS.to_string(),
            ),
        );
        input_defaults.insert(
            "enable_tag_saving".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_ENABLE_TAG_SAVING.to_string(),
            ),
        );
        input_defaults.insert(
            "release_ars".to_string(),
            InputBinding::String(crate::builtins::media_tagger::DEFAULT_RELEASE_ARS.to_string()),
        );
        input_defaults.insert(
            "cover_art_slot_count".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_COVER_ART_SLOT_COUNT.to_string(),
            ),
        );
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
        input_defaults.insert("cache_dir".to_string(), InputBinding::String(String::new()));
        input_defaults.insert(
            "cache_expiry_seconds".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_CACHE_EXPIRY_SECONDS.to_string(),
            ),
        );
    }

    input_defaults
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
mod tests;
