//! Tool command, environment, and config-policy helpers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

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
    "sub_langs",
    "sub_format",
    "convert_subs",
    "write_thumbnail",
    "write_all_thumbnails",
    "convert_thumbnails",
    "write_info_json",
    "clean_info_json",
    "write_comments",
    "write_description",
    "write_annotations",
    "write_chapters",
    "write_link",
    "write_url_link",
    "write_webloc_link",
    "write_desktop_link",
    "split_chapters",
    "playlist_items",
    "no_playlist",
    "skip_download",
    "retries",
    "limit_rate",
    "concurrent_fragments",
    "proxy",
    "socket_timeout",
    "sleep_subtitles",
    "user_agent",
    "referer",
    "add_header",
    "cookies",
    "cookies_from_browser",
    "cache_dir",
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
    "cues_to_front",
    "map_metadata",
    "map_chapters",
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
    "hide_banner",
];

/// Ordered rsgain option-input names injected into the generated command.
const RSGAIN_OPTION_INPUTS: &[&str] = &[
    "option_args",
    "input_extension",
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
    "cache_dir",
    "cache_expiry_seconds",
    "strict_identification",
    "write_all_tags",
    "write_all_images",
    "cover_art_slot_count",
    "recording_mbid",
    "release_mbid",
];

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
        // These fire only when the `container` input is explicitly set to a
        // recognised family member, so workflows that do not set `container`
        // are unaffected.  If the user also sets `movflags` or `cues_to_front`
        // explicitly, ffmpeg treats multiple `-movflags` / `-cues_to_front`
        // occurrences additively, so both values are honoured.

        // MOV/MPEG-4 family: inject `-movflags +faststart` so progressive
        // download starts without waiting for the full moov atom at EOF.
        let mp4_family_condition = "inputs.container == \"mp4\" || inputs.container == \"mov\" \
             || inputs.container == \"m4a\" || inputs.container == \"m4v\" \
             || inputs.container == \"3gp\" || inputs.container == \"3g2\"";
        command.push(format!("${{{mp4_family_condition} ? -movflags | ''}}"));
        command.push(format!("${{{mp4_family_condition} ? +faststart | ''}}"));

        // Matroska/WebM family: inject `-cues_to_front 1` so cue entries
        // are written before the cluster data, enabling immediate seeking
        // without a full container scan.
        let mkv_family_condition = "inputs.container == \"mkv\" || inputs.container == \"mka\" \
             || inputs.container == \"mks\" || inputs.container == \"webm\"";
        command.push(format!("${{{mkv_family_condition} ? -cues_to_front | ''}}"));
        command.push(format!("${{{mkv_family_condition} ? 1 | ''}}"));

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
    paths: &MediaPmPaths,
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
    let mut input_defaults = default_input_defaults_for_tool(tool_name, paths, ffmpeg_slot_limits);
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
    paths: &MediaPmPaths,
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
        let yt_dlp_cache_dir = paths.workspace_yt_dlp_cache_dir().to_string_lossy().to_string();

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
        input_defaults.insert("cache_dir".to_string(), InputBinding::String(yt_dlp_cache_dir));
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
        let media_tagger_cache_dir =
            paths.workspace_media_tagger_cache_dir().to_string_lossy().to_string();

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
            "cover_art_slot_count".to_string(),
            InputBinding::String(ffmpeg_slot_limits.max_input_slots.saturating_sub(1).to_string()),
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
        input_defaults
            .insert("cache_dir".to_string(), InputBinding::String(media_tagger_cache_dir));
        input_defaults.insert(
            "cache_expiry_seconds".to_string(),
            InputBinding::String(
                crate::builtins::media_tagger::DEFAULT_CACHE_EXPIRY_SECONDS.to_string(),
            ),
        );
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
#[expect(
    clippy::too_many_lines,
    reason = "option-token mapping is intentionally exhaustive and declarative"
)]
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
            "write_subs" => vec![
                "${*inputs.write_subs == \"true\" ? --write-subs | ''}".to_string(),
                "${*inputs.write_subs == \"false\" ? --no-write-subs | ''}".to_string(),
                "${*inputs.write_subs == \"true\" ? --write-auto-subs | ''}".to_string(),
                "${*inputs.write_subs == \"false\" ? --no-write-auto-subs | ''}".to_string(),
            ],
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
            "clean_info_json" => {
                bool_switch_tokens(input_name, "--clean-info-json", "--no-clean-info-json")
            }
            "write_comments" => {
                bool_switch_tokens(input_name, "--write-comments", "--no-write-comments")
            }
            "write_description" => {
                bool_switch_tokens(input_name, "--write-description", "--no-write-description")
            }
            "write_annotations" => {
                bool_switch_tokens(input_name, "--write-annotations", "--no-write-annotations")
            }
            "write_chapters" => {
                bool_switch_tokens(input_name, "--write-chapters", "--no-write-chapters")
            }
            "write_link" => bool_switch_tokens(input_name, "--write-link", "--no-write-link"),
            "write_url_link" => {
                bool_switch_tokens(input_name, "--write-url-link", "--no-write-url-link")
            }
            "write_webloc_link" => {
                bool_switch_tokens(input_name, "--write-webloc-link", "--no-write-webloc-link")
            }
            "write_desktop_link" => {
                bool_switch_tokens(input_name, "--write-desktop-link", "--no-write-desktop-link")
            }
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
            "sleep_subtitles" => pair_option_tokens(input_name, "--sleep-subtitles"),
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
            "cues_to_front" => bool_value_pair_tokens(input_name, "-cues_to_front", "1"),
            "map_metadata" => pair_option_tokens(input_name, "-map_metadata"),
            "map_chapters" => pair_option_tokens(input_name, "-map_chapters"),
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
            "hide_banner" => bool_flag_tokens(input_name, "-hide_banner"),
            _ => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
        };
    }

    if tool_name.eq_ignore_ascii_case("rsgain") {
        return match input_name {
            "input_extension" | "mode" => Vec::new(),
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
            "cache_dir" => pair_option_tokens(input_name, "--cache-dir"),
            "cache_expiry_seconds" => pair_option_tokens(input_name, "--cache-expiry-seconds"),
            "strict_identification" => bool_flag_tokens(input_name, "--strict-identification"),
            "write_all_tags" => bool_flag_tokens(input_name, "--write-all-tags"),
            "write_all_images" => bool_flag_tokens(input_name, "--write-all-images"),
            "cover_art_slot_count" => pair_option_tokens(input_name, "--cover-art-slot-count"),
            "recording_mbid" => pair_option_tokens(input_name, "--recording-mbid"),
            "release_mbid" => pair_option_tokens(input_name, "--release-mbid"),
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

/// Builds ffmpeg cover-art map/disposition templates for managed media-tagger
/// apply workflows.
#[must_use]
fn ffmpeg_cover_art_tokens(ffmpeg_slot_limits: FfmpegSlotLimits) -> Vec<String> {
    let mut tokens = Vec::new();

    for slot_index in 1..ffmpeg_slot_limits.max_input_slots {
        let enabled_input = ffmpeg_cover_slot_enabled_input_name(slot_index);
        let ffmpeg_input_index = slot_index + 1;
        let output_video_index = slot_index - 1;

        tokens.push(unpack_if_equals(&enabled_input, "true", "-map"));
        tokens.push(format!(
            "${{*inputs.{enabled_input} == \"true\" ? \"{ffmpeg_input_index}:v:0?\" | ''}}"
        ));
        tokens.push(unpack_if_equals(
            &enabled_input,
            "true",
            &format!("-disposition:v:{output_video_index}"),
        ));
        tokens.push(unpack_if_equals(&enabled_input, "true", "attached_pic"));
    }

    tokens
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

/// Resolves executable path used by generated media-tagger launchers.
///
/// This prefers the real `mediapm` CLI binary even when tool reconciliation is
/// triggered from examples/tests whose process executable is not `mediapm`.
///
/// When running from a crate example (`target/<profile>/examples/*.exe`), this
/// first looks for a profile-adjacent `target/<profile>/mediapm` binary. If
/// absent, it attempts one best-effort `cargo build --package mediapm --bin
/// mediapm --target-dir <target_dir>` using the inferred workspace root so
/// internal media-tagger launcher wiring remains available during example runs.
fn resolve_media_tagger_launcher_binary_path(
    paths: &MediaPmPaths,
) -> Result<PathBuf, MediaPmError> {
    if let Some(candidate) = resolve_workspace_mediapm_binary(&paths.root_dir) {
        return Ok(candidate);
    }

    let current_exe = std::env::current_exe().map_err(|error| {
        MediaPmError::Workflow(format!(
            "failed to resolve current process executable while preparing internal media-tagger launcher env: {error}"
        ))
    })?;

    if executable_file_stem_eq_ignore_ascii_case(&current_exe, "mediapm") {
        return Ok(current_exe);
    }

    if let Some(profile_adjacent_binary) =
        resolve_profile_adjacent_mediapm_binary_for_example(&current_exe)?
    {
        return Ok(profile_adjacent_binary);
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

/// Resolves a profile-adjacent `mediapm` binary when running from examples.
///
/// Returns `Ok(None)` when the current executable is not an example binary or
/// when no workspace root can be inferred for build fallback.
fn resolve_profile_adjacent_mediapm_binary_for_example(
    current_exe: &Path,
) -> Result<Option<PathBuf>, MediaPmError> {
    let binary_name = if cfg!(windows) { "mediapm.exe" } else { "mediapm" };

    let examples_dir = current_exe.parent();
    if !examples_dir.is_some_and(|dir| {
        dir.file_name()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|name| name.eq_ignore_ascii_case("examples"))
    }) {
        return Ok(None);
    }

    let Some(profile_dir) = examples_dir.and_then(Path::parent) else {
        return Ok(None);
    };

    if !profile_dir.file_name().and_then(std::ffi::OsStr::to_str).is_some_and(|profile_name| {
        profile_name.eq_ignore_ascii_case("debug") || profile_name.eq_ignore_ascii_case("release")
    }) {
        return Ok(None);
    }

    let candidate = profile_dir.join(binary_name);
    if candidate.is_file() {
        return Ok(Some(candidate));
    }

    let Some(target_dir) = profile_dir.parent() else {
        return Ok(None);
    };
    let Some(workspace_root) = find_workspace_root_for_target_dir(target_dir) else {
        return Ok(None);
    };

    build_workspace_mediapm_binary(&workspace_root, target_dir)?;
    if candidate.is_file() {
        return Ok(Some(candidate));
    }

    Ok(None)
}

/// Finds the nearest ancestor that appears to be a Cargo workspace root.
#[must_use]
fn find_workspace_root_for_target_dir(target_dir: &Path) -> Option<PathBuf> {
    target_dir
        .ancestors()
        .find_map(|ancestor| ancestor.join("Cargo.toml").is_file().then(|| ancestor.to_path_buf()))
}

/// Builds `mediapm` binary into one specific target directory.
fn build_workspace_mediapm_binary(
    workspace_root: &Path,
    target_dir: &Path,
) -> Result<(), MediaPmError> {
    let output = Command::new("cargo")
        .arg("build")
        .arg("--package")
        .arg("mediapm")
        .arg("--bin")
        .arg("mediapm")
        .arg("--target-dir")
        .arg(target_dir)
        .current_dir(workspace_root)
        .output()
        .map_err(|error| {
            MediaPmError::Workflow(format!(
                "failed to execute cargo build while resolving internal media-tagger launcher binary at '{}': {error}",
                workspace_root.display()
            ))
        })?;

    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(MediaPmError::Workflow(format!(
        "failed to build mediapm binary for internal media-tagger launcher (workspace='{}', target_dir='{}', status={}):\nstdout:\n{}\nstderr:\n{}",
        workspace_root.display(),
        target_dir.display(),
        output.status,
        stdout.trim(),
        stderr.trim(),
    )))
}

/// Resolves a workspace-local `target/<profile>/mediapm` executable path.
#[must_use]
fn resolve_workspace_mediapm_binary(root_dir: &Path) -> Option<PathBuf> {
    let binary_name = if cfg!(windows) { "mediapm.exe" } else { "mediapm" };

    for profile in ["debug", "release"] {
        let candidate = root_dir.join("target").join(profile).join(binary_name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    None
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
///
/// For platform-conditional selectors, every declared platform branch must map
/// to at least one `content_map` key so planned tool payloads stay
/// cross-platform complete.
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
    use std::fs;

    use std::collections::BTreeMap;

    use mediapm_cas::Hash;
    use mediapm_conductor::OutputCaptureSpec;
    use tempfile::tempdir;

    use mediapm_conductor::InputBinding;

    use super::{
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

    use super::{
        find_workspace_root_for_target_dir, resolve_profile_adjacent_mediapm_binary_for_example,
    };

    /// Creates stable fixture paths for default-input tests.
    fn fixture_paths() -> crate::paths::MediaPmPaths {
        crate::paths::MediaPmPaths::from_root("/workspace")
    }

    /// Verifies generated input defaults include every declared managed-tool
    /// option key so runtime config remains explicit and override-friendly.
    #[test]
    fn input_defaults_include_all_declared_option_inputs() {
        let paths = fixture_paths();

        for tool_name in ["yt-dlp", "ffmpeg", "rsgain", "media-tagger"] {
            let defaults =
                default_input_defaults_for_tool(tool_name, &paths, FfmpegSlotLimits::default());
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
        let defaults = default_input_defaults_for_tool(
            "rsgain",
            &fixture_paths(),
            FfmpegSlotLimits::default(),
        );

        assert_eq!(defaults.get("target_lufs"), Some(&InputBinding::String("-18".to_string())));
        assert_eq!(defaults.get("album"), Some(&InputBinding::String("false".to_string())));
        assert_eq!(defaults.get("album_mode"), Some(&InputBinding::String("false".to_string())));
        assert_eq!(defaults.get("tagmode"), Some(&InputBinding::String("i".to_string())));
        assert_eq!(defaults.get("true_peak"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(defaults.get("clip_mode"), Some(&InputBinding::String("p".to_string())));
        assert_eq!(defaults.get("max_peak"), Some(&InputBinding::String("0".to_string())));
    }

    /// Verifies yt-dlp defaults prioritize one best thumbnail while keeping
    /// unified subtitle capture enabled.
    #[test]
    fn yt_dlp_defaults_prefer_single_best_thumbnail_with_unified_subtitles() {
        let paths = fixture_paths();
        let defaults =
            default_input_defaults_for_tool("yt-dlp", &paths, FfmpegSlotLimits::default());

        assert_eq!(defaults.get("write_subs"), Some(&InputBinding::String("true".to_string())));
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
        assert_eq!(
            defaults.get("cache_dir"),
            Some(&InputBinding::String(
                paths.workspace_yt_dlp_cache_dir().to_string_lossy().to_string()
            ))
        );
    }

    /// Verifies unified subtitle input wiring controls both manual and
    /// automatic yt-dlp subtitle switches through `write_subs`.
    #[test]
    fn yt_dlp_write_subs_tokens_cover_manual_and_automatic_switches() {
        assert!(!YT_DLP_OPTION_INPUTS.contains(&"write_auto_subs"));

        let tokens = option_tokens_for_input("yt-dlp", "write_subs");
        assert!(
            tokens.contains(&"${*inputs.write_subs == \"true\" ? --write-subs | ''}".to_string())
        );
        assert!(
            tokens
                .contains(&"${*inputs.write_subs == \"false\" ? --no-write-subs | ''}".to_string())
        );
        assert!(
            tokens.contains(
                &"${*inputs.write_subs == \"true\" ? --write-auto-subs | ''}".to_string()
            )
        );
        assert!(tokens.contains(
            &"${*inputs.write_subs == \"false\" ? --no-write-auto-subs | ''}".to_string()
        ));
    }

    /// Verifies media-tagger defaults include strict-identification behavior,
    /// runtime-root cache location, and one-day cache expiry budget.
    #[test]
    fn media_tagger_defaults_include_workspace_cache_and_expiry() {
        let paths = fixture_paths();
        let defaults =
            default_input_defaults_for_tool("media-tagger", &paths, FfmpegSlotLimits::default());

        assert_eq!(
            defaults.get("strict_identification"),
            Some(&InputBinding::String("true".to_string()))
        );

        assert_eq!(
            defaults.get("cache_dir"),
            Some(&InputBinding::String(
                paths.workspace_media_tagger_cache_dir().to_string_lossy().to_string()
            ))
        );
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
