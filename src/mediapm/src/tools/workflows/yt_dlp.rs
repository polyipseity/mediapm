//! yt-dlp workflow step synthesis.
//!
//! Produces the conductor workflow step for one `yt-dlp` media download step.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;

use mediapm_conductor::{
    OutputCaptureSpec, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec, WorkflowStepSpec,
};

use crate::conductor_bridge::constants::*;
use crate::config::{DecodedOutputVariantConfig, MediaSourceSpec, MediaStep};

use super::spec::{TokenSpec, assemble_tool_spec, command_option_tokens_for_tool};
use super::{
    OUTPUT_PRIMARY, qualify_step_id, resolve_step_tool_id, source_uri_input,
    step_option_input_bindings, variant_to_output_capture_spec,
};

/// Synthesizes the yt-dlp workflow step from a media step definition.
///
/// Configures standard inputs (`source_url`, format, subtitles, etc.),
/// output captures for each declared variant, and sets the tool reference
/// to the managed `yt-dlp-managed` conductor tool.
///
/// # Errors
///
/// Returns [`MediaPmError`] when required configuration is missing or invalid.
pub(crate) fn synthesize_yt_dlp_step(
    source: &MediaSourceSpec,
    step_index: usize,
    step: &MediaStep,
) -> Vec<WorkflowStepSpec> {
    let step_id =
        qualify_step_id(source.id.as_deref().unwrap_or("unknown"), &format!("yt_dlp_{step_index}"));

    let mut inputs = BTreeMap::from([source_uri_input(source)]);
    for (k, v) in step_option_input_bindings(step) {
        inputs.insert(k, v);
    }

    // Always inject format if not explicitly provided.
    inputs.entry("format".to_string()).or_insert_with(|| "bestvideo+bestaudio/best".to_string());

    let mut outputs = BTreeMap::new();
    for (name, variant_json) in &step.output_variants {
        if let Ok(config) = DecodedOutputVariantConfig::from_json_value(variant_json.clone()) {
            outputs.insert(name.clone(), variant_to_output_capture_spec(name, &config));
        }
    }

    // When no explicit variants, add sensible defaults.
    if outputs.is_empty() {
        outputs.insert(
            OUTPUT_PRIMARY.to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: OUTPUT_PRIMARY.to_string(),
                capture: "file:primary.*".to_string(),
                save: true,
            },
        );
        outputs.insert(
            "subtitles".to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: "subtitles".to_string(),
                capture: "file:subtitles/*".to_string(),
                save: true,
            },
        );
        outputs.insert(
            "thumbnails".to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: "thumbnails".to_string(),
                capture: "file:thumbnails/*".to_string(),
                save: false,
            },
        );
    }

    vec![WorkflowStepSpec {
        id: step_id,
        tool: resolve_step_tool_id(crate::config::MediaStepTool::YtDlp),
        inputs,
        outputs,
        max_retries: 1,
        depends_on: Vec::new(),
    }]
}

/// Sandbox directory where yt-dlp materializes downloaded output artifacts.
const SANDBOX_DOWNLOADS_DIR: &str = "downloads";

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

/// Static default values for yt-dlp inputs.
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

/// Map from option input name to TokenSpec for yt-dlp.
const YT_DLP_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("format", TokenSpec::Pair("-f")),
    ("format_sort", TokenSpec::Pair("-S")),
    ("extract_audio", TokenSpec::Bool("--extract-audio")),
    ("audio_format", TokenSpec::Pair("--audio-format")),
    ("audio_quality", TokenSpec::Pair("--audio-quality")),
    ("remux_video", TokenSpec::Pair("--remux-video")),
    ("recode_video", TokenSpec::Pair("--recode-video")),
    ("merge_output_format", TokenSpec::Pair("--merge-output-format")),
    ("embed_thumbnail", TokenSpec::Bool("--embed-thumbnail")),
    ("embed_metadata", TokenSpec::Bool("--embed-metadata")),
    ("embed_subs", TokenSpec::Bool("--embed-subs")),
    ("embed_chapters", TokenSpec::Bool("--embed-chapters")),
    ("embed_info_json", TokenSpec::Bool("--embed-info-json")),
    ("write_subs", TokenSpec::Bool("--write-subs")),
    ("write_auto_subs", TokenSpec::Bool("--write-auto-subs")),
    ("sub_langs", TokenSpec::Pair("--sub-langs")),
    ("sub_format", TokenSpec::Pair("--sub-format")),
    ("convert_subs", TokenSpec::Pair("--convert-subs")),
    ("write_thumbnail", TokenSpec::Bool("--write-thumbnail")),
    ("write_all_thumbnails", TokenSpec::Bool("--write-all-thumbnails")),
    ("convert_thumbnails", TokenSpec::Pair("--convert-thumbnails")),
    ("write_info_json", TokenSpec::Bool("--write-info-json")),
    ("clean_info_json", TokenSpec::Bool("--clean-info-json")),
    ("write_comments", TokenSpec::Bool("--write-comments")),
    ("write_description", TokenSpec::Bool("--write-description")),
    ("write_annotations", TokenSpec::Bool("--write-annotations")),
    ("write_chapters", TokenSpec::Bool("--write-chapters")),
    ("write_link", TokenSpec::Bool("--write-link")),
    ("write_url_link", TokenSpec::Bool("--write-url-link")),
    ("write_webloc_link", TokenSpec::Bool("--write-webloc-link")),
    ("write_desktop_link", TokenSpec::Bool("--write-desktop-link")),
    ("split_chapters", TokenSpec::Bool("--split-chapters")),
    ("playlist_items", TokenSpec::Pair("--playlist-items")),
    ("no_playlist", TokenSpec::Bool("--no-playlist")),
    ("skip_download", TokenSpec::Bool("--skip-download")),
    ("retries", TokenSpec::Pair("--retries")),
    ("limit_rate", TokenSpec::Pair("--limit-rate")),
    ("concurrent_fragments", TokenSpec::Pair("--concurrent-fragments")),
    ("proxy", TokenSpec::Pair("--proxy")),
    ("socket_timeout", TokenSpec::Pair("--socket-timeout")),
    ("sleep_subtitles", TokenSpec::Pair("--sleep-subtitles")),
    ("user_agent", TokenSpec::Pair("--user-agent")),
    ("referer", TokenSpec::Pair("--referer")),
    ("add_header", TokenSpec::Pair("--add-header")),
    ("cookies", TokenSpec::Pair("--cookies")),
    ("cookies_from_browser", TokenSpec::Pair("--cookies-from-browser")),
    ("paths", TokenSpec::Pair("--paths")),
    ("js_runtimes", TokenSpec::Pair("--js-runtimes")),
    ("output", TokenSpec::Pair("--output")),
    ("parse_metadata", TokenSpec::Pair("--parse-metadata")),
    ("replace_in_metadata", TokenSpec::Pair("--replace-in-metadata")),
    ("download_sections", TokenSpec::Pair("--download-sections")),
    ("postprocessor_args", TokenSpec::Pair("--postprocessor-args")),
    ("extractor_args", TokenSpec::Pair("--extractor-args")),
    ("http_chunk_size", TokenSpec::Pair("--http-chunk-size")),
    ("download_archive", TokenSpec::Pair("--download-archive")),
    ("sponsorblock_mark", TokenSpec::Pair("--sponsorblock-mark")),
    ("sponsorblock_remove", TokenSpec::Pair("--sponsorblock-remove")),
];

/// Ordered yt-dlp option input names for CLI token generation.
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
    "yes_playlist",
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
    "js_runtimes",
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
    "playlist_start",
    "playlist_end",
    "playlist_reverse",
    "playlist_random",
    "match_title",
    "reject_title",
    "max_downloads",
    "min_filesize",
    "max_filesize",
    "date",
    "dateafter",
    "datebefore",
    "fragment_retries",
    "skip_unavailable_fragments",
    "abort_on_unavailable_fragment",
    "concurrent_fragment_downloads",
];

/// Builds the yt-dlp executable command vector.
#[must_use]
fn build_yt_dlp_command(command_path: &str) -> Vec<String> {
    let mut command = vec![
        command_path.to_string(),
        format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
        "--no-progress".to_string(),
        "--no-part".to_string(),
    ];
    command.extend(command_option_tokens_for_tool(YT_DLP_OPTION_INPUTS, YT_DLP_TOKEN_SPECS));
    command.push(format!("${{inputs.{INPUT_SOURCE_URL}}}"));
    command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));
    command
}

/// Builds yt-dlp input spec map.
#[must_use]
fn build_yt_dlp_inputs() -> BTreeMap<String, ToolInputSpec> {
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
            INPUT_SOURCE_URL.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        ),
    ]);
    for option_input in YT_DLP_OPTION_INPUTS {
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

/// Builds yt-dlp output capture spec map.
#[must_use]
fn build_yt_dlp_outputs() -> BTreeMap<String, OutputCaptureSpec> {
    let mut outputs = BTreeMap::from([
        (
            OUTPUT_CONTENT.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_CONTENT.to_string(),
                capture: format!("file_regex:{YT_DLP_OUTPUT_CONTENT_REGEX}"),
                save: true,
            },
        ),
        (
            "primary".to_string(),
            OutputCaptureSpec {
                name: "primary".to_string(),
                capture: format!("file_regex:{YT_DLP_OUTPUT_CONTENT_REGEX}"),
                save: true,
            },
        ),
        (
            OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
                capture: format!("folder:{SANDBOX_DOWNLOADS_DIR}"),
                save: true,
            },
        ),
        (
            "stdout".to_string(),
            OutputCaptureSpec {
                name: "stdout".to_string(),
                capture: "stdout".to_string(),
                save: true,
            },
        ),
        (
            "stderr".to_string(),
            OutputCaptureSpec {
                name: "stderr".to_string(),
                capture: "stderr".to_string(),
                save: true,
            },
        ),
        (
            "process_code".to_string(),
            OutputCaptureSpec {
                name: "process_code".to_string(),
                capture: "process_code".to_string(),
                save: true,
            },
        ),
    ]);

    for (output_name, path_regex) in [
        (OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS, YT_DLP_SUBTITLE_ARTIFACTS_REGEX),
        (OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS, YT_DLP_THUMBNAIL_ARTIFACTS_REGEX),
        (OUTPUT_YT_DLP_LINK_ARTIFACTS, YT_DLP_LINK_ARTIFACTS_REGEX),
        (OUTPUT_YT_DLP_CHAPTER_ARTIFACTS, YT_DLP_CHAPTER_ARTIFACTS_REGEX),
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
        (OUTPUT_YT_DLP_DESCRIPTION_FILE, YT_DLP_DESCRIPTION_OUTPUT_REGEX),
        (OUTPUT_YT_DLP_ANNOTATION_FILE, YT_DLP_ANNOTATION_OUTPUT_REGEX),
        (OUTPUT_YT_DLP_INFOJSON_FILE, YT_DLP_INFOJSON_OUTPUT_REGEX),
        (OUTPUT_YT_DLP_ARCHIVE_FILE, YT_DLP_ARCHIVE_OUTPUT_REGEX),
        (OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE, YT_DLP_PLAYLIST_DESCRIPTION_OUTPUT_REGEX),
        (OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE, YT_DLP_PLAYLIST_INFOJSON_OUTPUT_REGEX),
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

    outputs
}

/// Builds yt-dlp default input values.
#[must_use]
fn build_yt_dlp_default_input_defaults() -> BTreeMap<String, String> {
    let mut defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), String::new()),
        (INPUT_TRAILING_ARGS.to_string(), String::new()),
    ]);
    for option_input in YT_DLP_OPTION_INPUTS {
        defaults.entry((*option_input).to_string()).or_default();
    }
    for (key, value) in YT_DLP_INPUT_DEFAULTS {
        defaults.insert(key.to_string(), value.to_string());
    }
    defaults
}

/// Builds the full [`ToolSpec`] and [`ToolRuntime`] for the managed yt-dlp tool.
#[must_use]
pub(crate) fn build_yt_dlp_spec(
    content_map: BTreeMap<String, String>,
    command_path: &str,
) -> (ToolSpec, ToolRuntime) {
    assemble_tool_spec(
        "yt-dlp",
        content_map,
        build_yt_dlp_command(command_path),
        build_yt_dlp_inputs(),
        build_yt_dlp_outputs(),
        build_yt_dlp_default_input_defaults(),
        true, // impure — yt-dlp requires network
        1,    // max_concurrent_calls
        1,    // max_retries
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_yt_dlp_command_includes_template_tokens() {
        let command = build_yt_dlp_command("yt-dlp");
        assert!(
            command.iter().any(|c| c.contains("--no-progress")),
            "expected --no-progress in yt-dlp command"
        );
        assert!(
            command.iter().any(|c| c.contains("inputs.")),
            "expected input template expressions"
        );
        assert!(command.iter().any(|c| c.contains("format")), "expected format-related tokens");
    }

    #[test]
    fn build_yt_dlp_inputs_includes_option_inputs() {
        let inputs = build_yt_dlp_inputs();
        assert!(inputs.contains_key("format"), "missing 'format' input");
        assert!(inputs.contains_key("sub_langs"), "missing 'sub_langs' input");
        assert!(inputs.contains_key("extract_audio"), "missing 'extract_audio' input");
        assert!(inputs.contains_key(INPUT_SOURCE_URL), "missing source_url input");
        assert!(inputs.contains_key(INPUT_LEADING_ARGS), "missing leading_args input");
    }

    #[test]
    fn build_yt_dlp_outputs_include_sidecar_captures() {
        let outputs = build_yt_dlp_outputs();
        assert!(outputs.contains_key(OUTPUT_CONTENT), "missing content output");
        assert!(
            outputs.contains_key(OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS),
            "missing subtitle artifacts"
        );
        assert!(
            outputs.contains_key(OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS),
            "missing thumbnail artifacts"
        );
        assert!(outputs.contains_key(OUTPUT_YT_DLP_LINK_ARTIFACTS), "missing link artifacts");
        assert!(outputs.contains_key(OUTPUT_YT_DLP_CHAPTER_ARTIFACTS), "missing chapter artifacts");
        assert!(outputs.contains_key(OUTPUT_YT_DLP_DESCRIPTION_FILE), "missing description output");
        assert!(outputs.contains_key(OUTPUT_YT_DLP_INFOJSON_FILE), "missing infojson output");
        assert!(outputs.contains_key(OUTPUT_YT_DLP_ARCHIVE_FILE), "missing archive output");
    }

    #[test]
    fn build_yt_dlp_defaults_prefer_single_best_thumbnail() {
        let defaults = build_yt_dlp_default_input_defaults();
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
        assert_eq!(defaults.get("embed_thumbnail").map(String::as_str), Some("false"));
    }

    #[test]
    fn build_yt_dlp_spec_sets_impure_and_concurrency() {
        let content_map = BTreeMap::new();
        let (_spec, runtime) = build_yt_dlp_spec(content_map, "yt-dlp");
        assert!(runtime.impure);
        assert_eq!(runtime.max_concurrent_calls, 1);
        assert_eq!(runtime.max_retries, 1);
    }
}
