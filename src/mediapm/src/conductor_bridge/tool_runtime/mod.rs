//! Managed-tool runtime contract builders.
//!
//! This module produces [`ToolSpec`] and [`ToolRuntime`] entries for each
//! mediapm managed tool (yt-dlp, ffmpeg, rsgain, media-tagger, sd). It does
//! NOT produce workflow steps — step synthesis lives upstream in the
//! materializer/hierarchy modules.
//!
//! Sub-modules:
//! - [`option_constants`] — ordered option name definitions used for CLI token generation
//! - [`option_tokens`] — token-spec mappings and cover-art / container-any-of conditions
//! - [`template`] — command-template validation, environment-variable substitution, sandbox-path normalization
//! - [`launcher`] — media-tagger launcher binary path resolution

pub(crate) mod launcher;
pub(crate) mod option_constants;
pub(crate) mod option_tokens;
pub(crate) mod template;

use std::collections::BTreeMap;

use mediapm_conductor::{
    OutputCaptureSpec, ToolInputKind, ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec,
};

use crate::conductor_bridge::constants::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS, DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, INPUT_CONTENT,
    INPUT_FFMETADATA_CONTENT, INPUT_LEADING_ARGS, INPUT_SOURCE_URL, INPUT_TRAILING_ARGS,
    OUTPUT_CONTENT, OUTPUT_SANDBOX_ARTIFACTS,
};

// ── Sandbox path / regex constants ───────────────────────────────────────

/// Prefix for indexed ffmpeg content inputs.
const INPUT_FFMPEG_CONTENT_PREFIX: &str = "input_content_";
/// Prefix for indexed ffmpeg output-path option inputs.
const INPUT_FFMPEG_OUTPUT_PATH_PREFIX: &str = "output_path_";
/// Internal rsgain-only input selecting sandbox materialization extension.
const INPUT_RSGAIN_INPUT_EXTENSION: &str = "input_extension";

/// Fixed sandbox input path used when materializing byte-content inputs.
const SANDBOX_INPUT_FILE: &str = "inputs/input.bin";
/// Fixed sandbox input file path for media tools that edit payloads in place.
const SANDBOX_MEDIA_INPUT_FILE: &str = "inputs/input.media";
/// File extensions supported by `rsgain 3.7` for in-place tag writing.
const SUPPORTED_RSGAIN_INPUT_EXTENSIONS: &[&str] = &[
    "flac", "ogg", "oga", "spx", "opus", "mp2", "mp3", "mp4", "m4a", "wma", "wv", "ape", "wav",
    "aiff", "aif", "snd", "tak",
];
/// Default extension (with leading dot) for generated ffmpeg output files.
#[allow(dead_code)]
const DEFAULT_FFMPEG_OUTPUT_EXTENSION_WITH_DOT: &str = ".mkv";
/// Fixed sandbox metadata path for ffmpeg metadata-input materialization.
const SANDBOX_FFMETADATA_INPUT_FILE: &str = "inputs/input.ffmeta";
/// Fixed sandbox file path used by `sd` in-place rewrite operations.
const SANDBOX_SD_INPUT_FILE: &str = "inputs/input.ffmeta";
/// Sandbox directory where yt-dlp materializes downloaded output artifacts.
const SANDBOX_DOWNLOADS_DIR: &str = "downloads";
/// Sandbox directory where template `:file(...)` inputs are materialized.
const SANDBOX_INPUTS_DIR: &str = "inputs";
/// Fixed sandbox output file path for media-tagger `FFmetadata` documents.
const MEDIA_TAGGER_OUTPUT_FILE: &str = "metadata/output.ffmeta";

/// Regex for yt-dlp output content paths (media files).
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

/// MOV/ISOBMFF-family container values that should receive managed
/// `-movflags +faststart` auto-injection.
const FFMPEG_MOV_FASTSTART_CONTAINERS: &[&str] =
    &["mp4", "mov", "m4a", "m4v", "3gp", "3g2", "f4v", "ism", "ismv", "ipod", "psp"];

/// Matroska-family container values that should receive managed
/// `-cues_to_front 1` auto-injection.
const FFMPEG_MATROSKA_CUES_TO_FRONT_CONTAINERS: &[&str] =
    &["matroska", "mkv", "mka", "mks", "mk3d", "webm"];

// ── Slot path / name helpers ─────────────────────────────────────────────

/// Returns indexed ffmpeg input-content field name.
#[must_use]
pub(crate) fn ffmpeg_input_content_name(index: u32) -> String {
    format!("{INPUT_FFMPEG_CONTENT_PREFIX}{index}")
}

/// Returns indexed ffmpeg cover-art slot flag input field name.
#[must_use]
fn ffmpeg_cover_slot_enabled_input_name(index: u32) -> String {
    format!("cover_art_slot_enabled_{index}")
}

/// Returns indexed ffmpeg output-path input field name.
#[must_use]
pub(crate) fn ffmpeg_output_path_input_name(index: u32) -> String {
    format!("{INPUT_FFMPEG_OUTPUT_PATH_PREFIX}{index}")
}

/// Returns indexed ffmpeg output capture name.
#[must_use]
pub(crate) fn ffmpeg_output_capture_name(index: u32) -> String {
    if index == 0 { "primary".to_string() } else { format!("primary_{index}") }
}

/// Returns sandbox-relative ffmpeg output file path for one indexed slot.
#[must_use]
#[allow(dead_code)]
fn ffmpeg_output_file_path(index: u32) -> String {
    format!("output-{index}{DEFAULT_FFMPEG_OUTPUT_EXTENSION_WITH_DOT}")
}

/// Returns regex pattern for one indexed ffmpeg output capture path.
#[must_use]
fn ffmpeg_output_file_regex(index: u32) -> String {
    format!(r"^output-{index}(?:[.][^/\\]+)?$")
}

/// Returns sandbox-relative ffmpeg input file path for one indexed slot.
#[must_use]
fn ffmpeg_input_file_path(index: u32) -> String {
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

/// Returns a concrete sandbox-relative folder path for archive output capture.
#[must_use]
fn sandbox_artifacts_folder_for_tool(tool_name: &str) -> &str {
    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        SANDBOX_DOWNLOADS_DIR
    } else if tool_name.eq_ignore_ascii_case("media-tagger") {
        "coverart"
    } else {
        SANDBOX_INPUTS_DIR
    }
}

/// Returns whether one managed tool requires external network access.
#[must_use]
fn is_internet_required_tool(tool_name: &str) -> bool {
    tool_name.eq_ignore_ascii_case("yt-dlp") || tool_name.eq_ignore_ascii_case("media-tagger")
}

/// Returns exit success codes for one managed tool.
#[must_use]
fn success_codes_for_tool(_tool_name: &str) -> Vec<i32> {
    vec![0]
}

/// ffmpeg slot-limit configuration derived from tool requirements.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FfmpegSlotLimits {
    /// Maximum number of ffmpeg input content / cover-art slots.
    #[allow(dead_code)]
    pub(crate) max_input_slots: u32,
    /// Maximum number of ffmpeg indexed output-file slots.
    #[allow(dead_code)]
    pub(crate) max_output_slots: u32,
}

/// Resolves ffmpeg slot limits from config default or overrides.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn resolve_ffmpeg_slot_limits(
    max_input: Option<u32>,
    max_output: Option<u32>,
) -> FfmpegSlotLimits {
    FfmpegSlotLimits {
        max_input_slots: max_input.unwrap_or(DEFAULT_FFMPEG_MAX_INPUT_SLOTS as u32),
        max_output_slots: max_output.unwrap_or(DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS as u32),
    }
}

// ── Option‑token helpers ─────────────────────────────────────────────────

use self::option_constants::{
    FFMPEG_OPTION_INPUTS, MEDIA_TAGGER_OPTION_INPUTS, RSGAIN_OPTION_INPUTS, YT_DLP_OPTION_INPUTS,
};
use self::option_tokens::{
    command_option_tokens_for_tool, ffmpeg_container_any_of_condition, ffmpeg_cover_art_tokens,
};

// ── Command template builder ─────────────────────────────────────────────

/// Builds executable command vector for one managed tool.
///
/// Returns a vector of command tokens using Nickel template syntax
/// (`${...}`) for input binding at conductor runtime.
#[allow(clippy::too_many_lines)]
fn build_tool_command(
    tool_name: &str,
    command_path: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Vec<String> {
    let lower_name = tool_name.to_ascii_lowercase();

    if lower_name == "yt-dlp" {
        let mut command = vec![
            command_path.to_string(),
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

        for index in 1..ffmpeg_slot_limits.max_input_slots {
            let input_name = ffmpeg_input_content_name(index);
            let input_path = ffmpeg_input_file_path(index);
            command.push(format!("${{*inputs.{input_name} ? -i | ''}}"));
            command.push(format!(
                "${{*inputs.{input_name} ? inputs.{input_name}:file({input_path}) | ''}}"
            ));
        }

        command.extend(command_option_tokens_for_tool("ffmpeg", FFMPEG_OPTION_INPUTS));

        let mov_family_condition =
            ffmpeg_container_any_of_condition(FFMPEG_MOV_FASTSTART_CONTAINERS);
        command.push(format!("${{{mov_family_condition} ? -movflags | ''}}"));
        command.push(format!("${{{mov_family_condition} ? +faststart | ''}}"));

        let matroska_family_condition =
            ffmpeg_container_any_of_condition(FFMPEG_MATROSKA_CUES_TO_FRONT_CONTAINERS);
        command.push(format!("${{{matroska_family_condition} ? -cues_to_front | ''}}"));
        command.push(format!("${{{matroska_family_condition} ? 1 | ''}}"));

        command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));
        command.extend(ffmpeg_cover_art_tokens(
            ffmpeg_slot_limits.max_input_slots,
            ffmpeg_slot_limits.max_output_slots,
        ));

        for index in 0..ffmpeg_slot_limits.max_output_slots {
            let output_path_input = ffmpeg_output_path_input_name(index);
            command.push(format!("${{*inputs.{output_path_input}}}"));
        }

        return command;
    }

    if lower_name == "media-tagger" {
        let mut command = vec![
            command_path.to_string(),
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
            command_path.to_string(),
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
        let sd_pattern = crate::conductor_bridge::constants::INPUT_SD_PATTERN;
        let sd_replacement = crate::conductor_bridge::constants::INPUT_SD_REPLACEMENT;
        return vec![
            command_path.to_string(),
            format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
            format!("${{*inputs.{sd_pattern}}}"),
            format!("${{*inputs.{sd_replacement}}}"),
            format!("${{inputs.{INPUT_CONTENT}:file({SANDBOX_SD_INPUT_FILE})}}"),
            format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"),
        ];
    }

    vec![
        command_path.to_string(),
        format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
        format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"),
    ]
}

// ── Default policy helpers ───────────────────────────────────────────────

#[must_use]
fn default_max_concurrent_calls(tool_name: &str) -> usize {
    usize::from(tool_name.eq_ignore_ascii_case("yt-dlp"))
}

#[must_use]
fn default_max_retries(tool_name: &str) -> usize {
    usize::from(tool_name.eq_ignore_ascii_case("yt-dlp"))
}

// ── Input / output spec builders ─────────────────────────────────────────

fn build_tool_inputs(
    tool_name: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, ToolInputSpec> {
    use self::option_constants::option_input_names_for_tool;

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
        inputs.insert(
            (*option_input).to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
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
            crate::conductor_bridge::constants::INPUT_SD_PATTERN.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
        inputs.insert(
            crate::conductor_bridge::constants::INPUT_SD_REPLACEMENT.to_string(),
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

#[allow(clippy::too_many_lines)]
fn build_tool_outputs(
    tool_name: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, OutputCaptureSpec> {
    use crate::conductor_bridge::constants::{
        OUTPUT_YT_DLP_ANNOTATION_FILE, OUTPUT_YT_DLP_ARCHIVE_FILE, OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
        OUTPUT_YT_DLP_DESCRIPTION_FILE, OUTPUT_YT_DLP_INFOJSON_FILE, OUTPUT_YT_DLP_LINK_ARTIFACTS,
        OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE, OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE,
        OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS, OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
    };

    let sandbox_artifacts_path = sandbox_artifacts_folder_for_tool(tool_name);

    let mut outputs = BTreeMap::new();

    if tool_name.eq_ignore_ascii_case("ffmpeg") {
        outputs.insert(
            OUTPUT_CONTENT.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_CONTENT.to_string(),
                capture: format!("file_regex:{}", ffmpeg_output_file_regex(0)),
                save: true,
            },
        );
        for index in 0..ffmpeg_slot_limits.max_output_slots {
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
    } else {
        let output_capture = if tool_name.eq_ignore_ascii_case("yt-dlp") {
            format!("file_regex:{YT_DLP_OUTPUT_CONTENT_REGEX}")
        } else if tool_name.eq_ignore_ascii_case("media-tagger") {
            format!("file:{MEDIA_TAGGER_OUTPUT_FILE}")
        } else if tool_name.eq_ignore_ascii_case("rsgain") {
            format!("file_regex:{}", rsgain_output_file_regex())
        } else if tool_name.eq_ignore_ascii_case("sd") {
            format!("file:{SANDBOX_SD_INPUT_FILE}")
        } else {
            format!("file:{SANDBOX_INPUT_FILE}")
        };

        outputs.insert(
            OUTPUT_CONTENT.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_CONTENT.to_string(),
                capture: output_capture,
                save: true,
            },
        );

        if tool_name.eq_ignore_ascii_case("yt-dlp") {
            outputs.insert(
                "primary".to_string(),
                OutputCaptureSpec {
                    name: "primary".to_string(),
                    capture: format!("file_regex:{YT_DLP_OUTPUT_CONTENT_REGEX}"),
                    save: true,
                },
            );
        }
    }

    outputs.insert(
        OUTPUT_SANDBOX_ARTIFACTS.to_string(),
        OutputCaptureSpec {
            name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            capture: format!("folder:{sandbox_artifacts_path}"),
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

    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        for (output_name, path_regex) in [
            (&*OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS, YT_DLP_SUBTITLE_ARTIFACTS_REGEX),
            (&*OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS, YT_DLP_THUMBNAIL_ARTIFACTS_REGEX),
            (&*OUTPUT_YT_DLP_LINK_ARTIFACTS, YT_DLP_LINK_ARTIFACTS_REGEX),
            (&*OUTPUT_YT_DLP_CHAPTER_ARTIFACTS, YT_DLP_CHAPTER_ARTIFACTS_REGEX),
        ] {
            outputs.insert(
                output_name.to_string(),
                OutputCaptureSpec {
                    name: output_name.to_string(),
                    capture: format!("folder_regex:{path_regex}"),
                    save: true,
                },
            );
        }

        for (output_name, path_regex) in [
            (&*OUTPUT_YT_DLP_DESCRIPTION_FILE, YT_DLP_DESCRIPTION_OUTPUT_REGEX),
            (&*OUTPUT_YT_DLP_ANNOTATION_FILE, YT_DLP_ANNOTATION_OUTPUT_REGEX),
            (&*OUTPUT_YT_DLP_INFOJSON_FILE, YT_DLP_INFOJSON_OUTPUT_REGEX),
            (&*OUTPUT_YT_DLP_ARCHIVE_FILE, YT_DLP_ARCHIVE_OUTPUT_REGEX),
            (&*OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE, YT_DLP_PLAYLIST_DESCRIPTION_OUTPUT_REGEX),
            (&*OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE, YT_DLP_PLAYLIST_INFOJSON_OUTPUT_REGEX),
        ] {
            outputs.insert(
                output_name.to_string(),
                OutputCaptureSpec {
                    name: output_name.to_string(),
                    capture: format!("file_regex:{path_regex}"),
                    save: true,
                },
            );
        }
    }

    outputs
}

fn build_default_input_defaults(
    tool_name: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, String> {
    use self::option_constants::option_input_names_for_tool;

    let mut defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), String::new()),
        (INPUT_TRAILING_ARGS.to_string(), String::new()),
    ]);

    for option_input in option_input_names_for_tool(tool_name) {
        defaults.entry((*option_input).to_string()).or_default();
    }

    // Apply static defaults per tool (will override empty placeholders above).
    let static_defaults: &[(&str, &str)] = match tool_name {
        n if n.eq_ignore_ascii_case("yt-dlp") => YT_DLP_INPUT_DEFAULTS,
        n if n.eq_ignore_ascii_case("rsgain") => RSGAIN_INPUT_DEFAULTS,
        n if n.eq_ignore_ascii_case("media-tagger") => MEDIA_TAGGER_INPUT_DEFAULTS,
        n if n.eq_ignore_ascii_case("sd") => SD_INPUT_DEFAULTS,
        n if n.eq_ignore_ascii_case("ffmpeg") => FFMPEG_STATIC_DEFAULTS,
        _ => &[],
    };

    for (key, value) in static_defaults {
        defaults.insert(key.to_string(), value.to_string());
    }

    // Ffmpeg indexed slot defaults (dynamic — computed at runtime per slot limit).
    if tool_name.eq_ignore_ascii_case("ffmpeg") {
        for index in 0..ffmpeg_slot_limits.max_input_slots {
            defaults.insert(ffmpeg_input_content_name(index), String::new());
        }
        for index in 1..ffmpeg_slot_limits.max_input_slots {
            defaults.insert(ffmpeg_cover_slot_enabled_input_name(index), String::new());
        }
        for index in 0..ffmpeg_slot_limits.max_output_slots {
            defaults.insert(ffmpeg_output_path_input_name(index), String::new());
        }
        defaults.insert(INPUT_FFMETADATA_CONTENT.to_string(), String::new());
    }

    defaults
}

/// Builds a full [`ToolSpec`] and [`ToolRuntime`] for one managed tool.
///
/// `content_map` maps sandbox-relative paths to CAS hash hex strings
/// (output of the fetch + CAS-import step in the sync pipeline).
/// `command_path` is the sandbox-relative path to the main executable.
#[allow(clippy::too_many_lines)]
pub(crate) fn build_tool_spec(
    tool_name: &str,
    content_map: BTreeMap<String, String>,
    command_path: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> (ToolSpec, ToolRuntime) {
    let inputs = build_tool_inputs(tool_name, ffmpeg_slot_limits);
    let outputs = build_tool_outputs(tool_name, ffmpeg_slot_limits);
    let default_inputs = build_default_input_defaults(tool_name, ffmpeg_slot_limits);
    let command = build_tool_command(tool_name, command_path, ffmpeg_slot_limits);

    let runtime = ToolRuntime {
        content_map,
        impure: is_internet_required_tool(tool_name),
        inherited_env_vars: Vec::new(),
        max_concurrent_calls: default_max_concurrent_calls(tool_name),
        max_retries: default_max_retries(tool_name),
    };

    let spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command,
            env_vars: BTreeMap::new(),
            success_codes: success_codes_for_tool(tool_name),
        },
        name: tool_name.to_string(),
        version: String::new(),
        inputs,
        default_inputs,
        outputs,
        runtime: runtime.clone(),
    };

    (spec, runtime)
}

// ── Static defaults tables ───────────────────────────────────────────────

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
    ("embed_thumbnail", "false"),
    ("embed_subs", "false"),
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
    ("faststart", "false"),
    ("shortest", "false"),
    ("stats", "false"),
    ("no_overwrite", "false"),
    ("start_at_zero", "true"),
    ("copy_ts", "false"),
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::conductor_bridge::constants::{
        OUTPUT_YT_DLP_ARCHIVE_FILE, OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
        OUTPUT_YT_DLP_DESCRIPTION_FILE, OUTPUT_YT_DLP_INFOJSON_FILE, OUTPUT_YT_DLP_LINK_ARTIFACTS,
        OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS, OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
    };

    #[test]
    fn build_tool_spec_returns_executable_kind() {
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/sd".into(), "hash123".into());
        content_map.insert("macos/sd".into(), "hash456".into());
        content_map.insert("windows/sd.exe".into(), "hash789".into());

        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (spec, runtime) = build_tool_spec("sd", content_map.clone(), "sd", limits);

        let ToolKindSpec::Executable { command, .. } = &spec.kind else {
            panic!("expected Executable kind");
        };
        assert_eq!(command.first().unwrap(), "sd");
        assert_eq!(runtime.content_map, content_map);
        assert!(!runtime.impure);
        assert_eq!(spec.name, "sd");
    }

    #[test]
    fn build_tool_spec_preserves_content_map() {
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/sd".into(), "abc".into());
        content_map.insert("macos/sd".into(), "def".into());
        content_map.insert("windows/sd.exe".into(), "ghi".into());

        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (_spec, runtime) = build_tool_spec("sd", content_map.clone(), "sd", limits);

        assert_eq!(runtime.content_map.len(), 3);
        assert_eq!(runtime.content_map["linux/sd"], "abc");
        assert_eq!(runtime.content_map["macos/sd"], "def");
        assert_eq!(runtime.content_map["windows/sd.exe"], "ghi");
    }

    #[test]
    fn build_tool_spec_sets_runtime_defaults() {
        let content_map = BTreeMap::new();
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (_spec, runtime) = build_tool_spec("ffmpeg", content_map, "ffmpeg", limits);

        assert_eq!(runtime.max_concurrent_calls, 0);
        assert_eq!(runtime.max_retries, 0);
        assert!(runtime.inherited_env_vars.is_empty());
    }

    #[test]
    fn yt_dlp_increases_concurrency_and_retries() {
        let content_map = BTreeMap::new();
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (_spec, runtime) = build_tool_spec("yt-dlp", content_map, "yt-dlp", limits);

        assert_eq!(runtime.max_concurrent_calls, 1);
        assert_eq!(runtime.max_retries, 1);
    }

    #[test]
    fn yt_dlp_is_impure() {
        let content_map = BTreeMap::new();
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (_spec, runtime) = build_tool_spec("yt-dlp", content_map, "yt-dlp", limits);
        assert!(runtime.impure);
    }

    #[test]
    fn media_tagger_is_impure() {
        let content_map = BTreeMap::new();
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (_spec, runtime) = build_tool_spec("media-tagger", content_map, "media-tagger", limits);
        assert!(runtime.impure);
    }

    #[test]
    fn build_tool_command_includes_yt_dlp_template_tokens() {
        let command = build_tool_command("yt-dlp", "yt-dlp", FfmpegSlotLimits::default());
        assert!(
            command.iter().any(|c| c.contains("--no-progress")),
            "expected --no-progress in yt-dlp command"
        );
        assert!(
            command.iter().any(|c| c.contains("inputs.")),
            "expected input template expressions in yt-dlp command"
        );
        assert!(
            command.iter().any(|c| c.contains("format")),
            "expected format-related tokens in yt-dlp command"
        );
    }

    #[test]
    fn build_tool_command_includes_ffmpeg_template_tokens() {
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let command = build_tool_command("ffmpeg", "ffmpeg", limits);
        assert!(command.iter().any(|c| c.contains("-i")), "expected -i token in ffmpeg command");
        assert!(
            command.iter().any(|c| c.contains("hide_banner")),
            "expected hide_banner reference in ffmpeg command"
        );
    }

    #[test]
    fn build_tool_inputs_includes_option_inputs_for_yt_dlp() {
        let inputs = build_tool_inputs("yt-dlp", FfmpegSlotLimits::default());
        assert!(inputs.contains_key("format"), "missing 'format' input");
        assert!(inputs.contains_key("sub_langs"), "missing 'sub_langs' input");
        assert!(inputs.contains_key("extract_audio"), "missing 'extract_audio' input");
        assert!(inputs.contains_key(INPUT_SOURCE_URL), "missing source_url input");
        assert!(inputs.contains_key(INPUT_LEADING_ARGS), "missing leading_args input");
    }

    #[test]
    fn build_tool_inputs_includes_option_inputs_for_ffmpeg() {
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let inputs = build_tool_inputs("ffmpeg", limits);
        assert!(inputs.contains_key("codec_audio"), "missing 'codec_audio' input");
        assert!(inputs.contains_key("codec_video"), "missing 'codec_video' input");
        assert!(inputs.contains_key(&ffmpeg_input_content_name(0)), "missing input_content_0");
        assert!(inputs.contains_key(&ffmpeg_output_path_input_name(0)), "missing output_path_0");
        assert!(inputs.contains_key(INPUT_FFMETADATA_CONTENT), "missing ffmetadata input");
    }

    #[test]
    fn build_tool_inputs_includes_sd_content_and_pattern() {
        let inputs = build_tool_inputs("sd", FfmpegSlotLimits::default());
        assert!(inputs.contains_key(INPUT_CONTENT), "missing 'content' input for sd");
        assert!(inputs.contains_key("pattern"), "missing 'pattern' input for sd");
        assert!(inputs.contains_key("replacement"), "missing 'replacement' input for sd");
    }

    #[test]
    fn yt_dlp_outputs_include_sidecar_captures() {
        let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());
        assert!(outputs.contains_key(OUTPUT_CONTENT), "missing content output");
        assert!(
            outputs.contains_key(OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS),
            "missing subtitle artifacts output"
        );
        assert!(
            outputs.contains_key(OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS),
            "missing thumbnail artifacts output"
        );
        assert!(
            outputs.contains_key(OUTPUT_YT_DLP_LINK_ARTIFACTS),
            "missing link artifacts output"
        );
        assert!(
            outputs.contains_key(OUTPUT_YT_DLP_CHAPTER_ARTIFACTS),
            "missing chapter artifacts output"
        );
        assert!(outputs.contains_key(OUTPUT_YT_DLP_DESCRIPTION_FILE), "missing description output");
        assert!(outputs.contains_key(OUTPUT_YT_DLP_INFOJSON_FILE), "missing infojson output");
        assert!(outputs.contains_key(OUTPUT_YT_DLP_ARCHIVE_FILE), "missing archive output");
    }

    #[test]
    fn all_outputs_include_stdout_stderr_process_code() {
        for tool_name in &["yt-dlp", "ffmpeg", "rsgain", "media-tagger", "sd"] {
            let outputs = build_tool_outputs(
                tool_name,
                FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 },
            );
            assert!(outputs.contains_key("stdout"), "missing stdout output for {tool_name}");
            assert!(outputs.contains_key("stderr"), "missing stderr output for {tool_name}");
            assert!(
                outputs.contains_key("process_code"),
                "missing process_code output for {tool_name}"
            );
        }
    }

    #[test]
    fn input_defaults_include_all_declared_option_inputs() {
        for tool_name in ["yt-dlp", "ffmpeg", "rsgain", "media-tagger"] {
            let defaults = build_default_input_defaults(tool_name, FfmpegSlotLimits::default());
            for option_name in super::option_constants::option_input_names_for_tool(tool_name) {
                assert!(
                    defaults.contains_key(*option_name),
                    "missing input_defaults entry '{option_name}' for tool '{tool_name}'"
                );
            }

            assert!(defaults.contains_key(INPUT_LEADING_ARGS));
            assert!(defaults.contains_key(INPUT_TRAILING_ARGS));
        }
    }

    #[test]
    fn rsgain_defaults_match_expected_loudness_profile() {
        let defaults = build_default_input_defaults("rsgain", FfmpegSlotLimits::default());
        assert_eq!(defaults.get("target_lufs").map(String::as_str), Some("-18"));
        assert_eq!(defaults.get("album").map(String::as_str), Some("false"));
        assert_eq!(defaults.get("tagmode").map(String::as_str), Some("i"));
        assert_eq!(defaults.get("true_peak").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("clip_mode").map(String::as_str), Some("p"));
        assert_eq!(defaults.get("max_peak").map(String::as_str), Some("0"));
    }

    #[test]
    fn ffmpeg_defaults_include_static_and_dynamic_entries() {
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let defaults = build_default_input_defaults("ffmpeg", limits);
        assert_eq!(defaults.get("hide_banner").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("codec_copy").map(String::as_str), Some("true"));
        assert!(defaults.contains_key(&ffmpeg_input_content_name(0)));
        assert!(defaults.contains_key(&ffmpeg_input_content_name(1)));
        assert!(defaults.contains_key(&ffmpeg_output_path_input_name(0)));
        assert!(defaults.contains_key(&ffmpeg_output_path_input_name(1)));
    }

    #[test]
    fn yt_dlp_defaults_prefer_single_best_thumbnail_with_split_subtitle_defaults() {
        let defaults = build_default_input_defaults("yt-dlp", FfmpegSlotLimits::default());
        assert_eq!(defaults.get("write_subs").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("write_auto_subs").map(String::as_str), Some("false"));
        assert_eq!(defaults.get("sub_langs").map(String::as_str), Some("all"));
        assert!(
            defaults.get("extractor_args").map_or(false, |v| v.contains("skip=translated_subs"))
        );
        assert_eq!(defaults.get("write_thumbnail").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("write_all_thumbnails").map(String::as_str), Some("false"));
        assert_eq!(defaults.get("clean_info_json").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("write_comments").map(String::as_str), Some("false"));
        assert_eq!(defaults.get("write_chapters").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("write_url_link").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("write_desktop_link").map(String::as_str), Some("true"));
        assert_eq!(
            defaults.get("download_archive").map(String::as_str),
            Some("downloads/archive.txt")
        );
        // Default embed-thumbnail is false because the container-conditional
        // embedding behavior is handled by downstream steps.
        assert_eq!(defaults.get("embed_thumbnail").map(String::as_str), Some("false"));
    }

    #[test]
    fn media_tagger_defaults_include_cache_and_endpoints() {
        let defaults = build_default_input_defaults("media-tagger", FfmpegSlotLimits::default());
        assert_eq!(defaults.get("strict_identification").map(String::as_str), Some("true"));
        assert_eq!(defaults.get("embed_only_one_front_image").map(String::as_str), Some("false"));
        assert_eq!(defaults.get("cache_expiry_seconds").map(String::as_str), Some("86400"));
    }

    #[test]
    fn ffmpeg_outputs_use_regex_capture_for_slotted_outputs() {
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 3 };
        let outputs = build_tool_outputs("ffmpeg", limits);
        assert!(outputs.contains_key("primary"), "missing primary output");
        assert!(outputs.contains_key("primary_1"), "missing primary_1 output");
        assert!(outputs.contains_key("primary_2"), "missing primary_2 output");
        assert!(outputs.contains_key(&ffmpeg_output_capture_name(0)));
        assert!(outputs.contains_key(&ffmpeg_output_capture_name(1)));

        // Verify capture string format
        let primary = &outputs["primary"];
        assert!(
            primary.capture.starts_with("file_regex:"),
            "primary capture should use file_regex, got: {}",
            primary.capture
        );
    }

    #[test]
    fn sandbox_artifacts_folder_differentiates_media_tagger() {
        assert_eq!(sandbox_artifacts_folder_for_tool("yt-dlp"), "downloads");
        assert_eq!(sandbox_artifacts_folder_for_tool("media-tagger"), "coverart");
        assert_eq!(sandbox_artifacts_folder_for_tool("ffmpeg"), "inputs");
        assert_eq!(sandbox_artifacts_folder_for_tool("rsgain"), "inputs");
    }
}
