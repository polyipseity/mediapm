//! yt-dlp workflow step synthesis.
//!
//! Produces the conductor workflow step for one `yt-dlp` media download step.

use std::collections::BTreeMap;

use mediapm_conductor::config::CaptureKind;
use mediapm_conductor::{
    InputBinding, OutputCaptureSpec, SaveMode, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec,
    WorkflowSpec, WorkflowStepSpec,
};

use mediapm_conductor::tools::helpers::build_os_conditional_selector;

use crate::conductor_bridge::constants::{
    INPUT_LEADING_ARGS, INPUT_SOURCE_URL, INPUT_TRAILING_ARGS, OUTPUT_CONTENT,
    OUTPUT_SANDBOX_ARTIFACTS, OUTPUT_YT_DLP_ANNOTATION_FILE, OUTPUT_YT_DLP_ARCHIVE_FILE,
    OUTPUT_YT_DLP_CHAPTER_ARTIFACTS, OUTPUT_YT_DLP_DESCRIPTION_FILE, OUTPUT_YT_DLP_INFOJSON_FILE,
    OUTPUT_YT_DLP_LINK_ARTIFACTS, OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE,
    OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE, OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
    OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
};
use crate::config::output_types::ResolvedStepVariantFlow;
use crate::config::source_types::step_option_scalar;
use crate::config::{MediaStep, MediaStepTool, OutputVariantValue, YtDlpOutputKind};
use crate::error::MediaPmError;

use super::spec::{TokenSpec, assemble_tool_spec, command_option_tokens_for_tool};
use super::yt_dlp_inputs::{resolve_step_output_binding, yt_dlp_variant_inputs};
use super::{
    AutoSaveContext, FfmpegSlotLimits, VariantProducer, media_step_id, resolve_variant_save,
    step_option_input_bindings, variant_to_output_capture_spec,
};

/// Synthesizes yt-dlp workflow steps from one media step definition.
///
/// Produces one [`WorkflowStepSpec`] per variant-flow mapping edge. Online
/// downloaders never resolve upstream producers: the source URI arrives via
/// the step `uri` option (empty when absent — source entries carry no URI
/// field), and steps carry no execution-order dependencies.
///
/// Output captures are keyed by the logical output name resolved from the
/// variant kind (for example `yt_dlp_subtitle_artifacts`), matching the old
/// step-output-policy keying; the variant-keyed
/// `super::step_output_policy_overrides` helper is not used here.
///
/// # Errors
///
/// Returns [`MediaPmError`] when required configuration is missing or invalid.
#[expect(
    clippy::too_many_arguments,
    reason = "yt-dlp step synthesis threads workflow, planning, source, and producer state explicitly to keep orchestration deterministic"
)]
pub(crate) fn synthesize_yt_dlp_step(
    workflow: &mut WorkflowSpec,
    _media_id: &str,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    tool_id: &str,
    _producer_snapshot: &BTreeMap<String, VariantProducer>,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
    context: &AutoSaveContext,
) -> Result<(), MediaPmError> {
    let mut pending_variant_updates = Vec::new();

    for (mapping_index, mapping) in mappings.iter().enumerate() {
        let step_id = media_step_id(step_index, mapping_index, step.tool, mapping);

        let variant_value = step.output_variants.get(&mapping.output).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "missing output variant '{}' while decoding yt-dlp config",
                mapping.output
            ))
        })?;
        let config = match variant_value {
            OutputVariantValue::YtDlp(config) => config,
            OutputVariantValue::Generic(_) => {
                return Err(MediaPmError::Workflow(format!(
                    "decoded non-yt-dlp output variant config for yt-dlp key '{}'",
                    mapping.output
                )));
            }
        };

        // Download-archive sidecars are produced by the primary download sandbox; a
        // standalone archive-only step would run before the download and capture nothing.
        if matches!(config.kind, YtDlpOutputKind::Archive) {
            continue;
        }

        let mut inputs = BTreeMap::new();
        // Source entries carry no URI field, so the URI arrives via the step
        // `uri` option when present (empty fallback mirrors `source_uri_input`).
        let source_uri =
            step_option_scalar(step, "uri").map_or_else(String::new, ToString::to_string);
        inputs.insert(INPUT_SOURCE_URL.to_string(), source_uri);
        inputs.extend(step_option_input_bindings(step));

        // Always inject format if not explicitly provided.
        inputs
            .entry("format".to_string())
            .or_insert_with(|| "bestvideo+bestaudio/best".to_string());

        inputs.extend(yt_dlp_variant_inputs(config));

        // yt-dlp dispatch omits ffmpeg slot limits (only ffmpeg slots are
        // bounded), so the default zeroed limits are used for the binding.
        let output_binding = resolve_step_output_binding(
            MediaStepTool::YtDlp,
            &step.output_variants,
            &mapping.output,
            FfmpegSlotLimits::default(),
        )?;

        let mut outputs = BTreeMap::new();
        outputs.insert(
            output_binding.output_name.clone(),
            variant_to_output_capture_spec(
                &output_binding.output_name,
                &mapping.output,
                variant_value,
                step_index,
                context,
                step.tool.is_impure(),
            )?,
        );

        let mut producers_for_step = vec![(
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: step_id.clone(),
                output_name: output_binding.output_name,
                zip_member: output_binding.zip_member,
                extension: None,
            },
        )];

        if matches!(config.kind, YtDlpOutputKind::Primary)
            && let Some(archive_variant) = step.output_variants.get("archive")
        {
            outputs.insert(
                OUTPUT_YT_DLP_ARCHIVE_FILE.to_string(),
                variant_to_output_capture_spec(
                    OUTPUT_YT_DLP_ARCHIVE_FILE,
                    "archive",
                    archive_variant,
                    step_index,
                    context,
                    step.tool.is_impure(),
                )?,
            );
            producers_for_step.push((
                "archive".to_string(),
                VariantProducer::StepOutput {
                    step_id: step_id.clone(),
                    output_name: OUTPUT_YT_DLP_ARCHIVE_FILE.to_string(),
                    zip_member: None,
                    extension: None,
                },
            ));
        }

        workflow.steps.push(WorkflowStepSpec {
            id: step_id.clone(),
            tool: tool_id.to_string(),
            inputs,
            outputs,
            runtime: Some(mediapm_conductor::config::StepRuntime { max_retries: Some(1) }),
            depends_on: Vec::new(),
        });

        pending_variant_updates.extend(producers_for_step);
    }

    for (output_variant, producer) in pending_variant_updates {
        variant_producers.insert(output_variant, producer);
    }

    Ok(())
}

/// Builds workflow-step capture selectors for one yt-dlp output variant.
///
/// The capture token and the materialization member come from one resolver so
/// they cannot disagree: language selections scope the subtitle capture regex
/// for both families, and a member exists only when declared.
///
/// # Errors
///
/// Returns [`MediaPmError::ConfigValidation`] with code `MPM-E011` when the
/// variant declares a shape that cannot be one capture.
pub(crate) fn yt_dlp_variant_capture_and_save(
    config: &crate::config::YtDlpOutputVariantConfig,
    variant: &str,
    step_index: usize,
    context: &AutoSaveContext,
    step_impure: bool,
) -> Result<(CaptureKind, mediapm_conductor::SaveMode), MediaPmError> {
    let resolved = super::resolve_variant_capture(variant, config)?;
    let save = resolve_variant_save(config.save, variant, step_index, context, step_impure);
    Ok((resolved.capture, save))
}

/// Sandbox directory where yt-dlp materializes downloaded output artifacts.
const SANDBOX_DOWNLOADS_DIR: &str = "downloads";

/// Regex for yt-dlp output content paths (media files).
pub(crate) const YT_DLP_OUTPUT_CONTENT_REGEX: &str = "^downloads/.+(?:__mediapm__)?[.](?:3gp|aac|aiff?|alac|asf|avi|flac|m4a|m4v|mka|mkv|mov|mp3|mp4|mpeg|mpg|oga|ogg|opus|wav|weba|webm|wma)$";
/// Regex for yt-dlp description output file paths.
pub(crate) const YT_DLP_DESCRIPTION_OUTPUT_REGEX: &str =
    "^downloads/.+(?:__mediapm__)?[.]description$";
/// Regex for yt-dlp info-json output file paths.
pub(crate) const YT_DLP_INFOJSON_OUTPUT_REGEX: &str =
    "^downloads/.+(?:__mediapm__)?[.]info[.]json$";
/// Regex for yt-dlp download archive file path.
pub(crate) const YT_DLP_ARCHIVE_OUTPUT_REGEX: &str = "^downloads/archive[.]txt$";
/// Regex for yt-dlp subtitle-family sidecar files.
pub(crate) const YT_DLP_SUBTITLE_ARTIFACTS_REGEX: &str = "^downloads/(.+?)(?:__mediapm__)?((?:[.][^/.]+)?[.](?:ass|dfxp|json3|lrc|srt|srv1|srv2|srv3|ssa|ttml|vtt))$";
/// Regex for yt-dlp thumbnail sidecar files.
pub(crate) const YT_DLP_THUMBNAIL_ARTIFACTS_REGEX: &str = "^downloads/(?:(.+?)__mediapm__((?:[.][0-9]+)?[.](?:avif|bmp|gif|jpe?g|png|webp))|(.+?)([.](?:avif|bmp|gif|jpe?g|png|webp)))$";
/// Regex for yt-dlp annotation sidecar file output.
pub(crate) const YT_DLP_ANNOTATION_OUTPUT_REGEX: &str =
    "^downloads/.+(?:__mediapm__)?[.](?:annotation[.]xml|annotation)$";
/// Regex for yt-dlp internet-shortcut sidecar files.
pub(crate) const YT_DLP_LINK_ARTIFACTS_REGEX: &str =
    "^downloads/(.+?)(?:__mediapm__)?([.](?:desktop|url|webloc))$";
/// Regex for yt-dlp split-chapter media files.
pub(crate) const YT_DLP_CHAPTER_ARTIFACTS_REGEX: &str = "^downloads/(?:(.+?)__mediapm__((?: - .+)?[.](?:3gp|aac|aiff?|alac|asf|avi|flac|m4a|m4v|mka|mkv|mov|mp3|mp4|mpeg|mpg|oga|ogg|opus|wav|weba|webm|wma))|(.+?)((?: - .+)?[.](?:3gp|aac|aiff?|alac|asf|avi|flac|m4a|m4v|mka|mkv|mov|mp3|mp4|mpeg|mpg|oga|ogg|opus|wav|weba|webm|wma)))$";
/// Regex for yt-dlp playlist-description file output.
pub(crate) const YT_DLP_PLAYLIST_DESCRIPTION_OUTPUT_REGEX: &str =
    "^downloads/.+\\x5B[^/]+\\x5D[0-9]+(?:__mediapm__)?[.]description$";
/// Regex for yt-dlp playlist-infojson file output.
pub(crate) const YT_DLP_PLAYLIST_INFOJSON_OUTPUT_REGEX: &str =
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
    ("write_link", "true"),
    ("write_url_link", "true"),
    ("write_webloc_link", "true"),
    ("write_desktop_link", "true"),
    ("download_archive", "downloads/archive.txt"),
    ("split_chapters", "false"),
    ("no_playlist", "true"),
    ("cache_dir", ""),
    ("embed_thumbnail", "false"),
    ("embed_subs", "false"),
];

/// Map from option input name to `TokenSpec` for yt-dlp.
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
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        ),
        (
            INPUT_TRAILING_ARGS.to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        ),
        (
            INPUT_SOURCE_URL.to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        ),
    ]);
    for option_input in YT_DLP_OPTION_INPUTS {
        inputs.insert(
            (*option_input).to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        );
    }
    inputs
}

/// Builds the base yt-dlp output capture specs (content, streams, and sandbox).
///
/// The video defaults (`content` and `primary`) tolerate an empty match at the
/// tool level: a step that skips the download inherits these names and produces
/// no video. A step that needs a video restates `primary` and keeps its own
/// strictness via the merge rules.
fn base_yt_dlp_outputs() -> BTreeMap<String, OutputCaptureSpec> {
    BTreeMap::from([
        (
            OUTPUT_CONTENT.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_CONTENT.to_string(),
                capture: CaptureKind::parse_token(&format!(
                    "file_regex:{YT_DLP_OUTPUT_CONTENT_REGEX}"
                ))
                .expect("valid yt-dlp content capture token"),
                save: SaveMode::True,
                allow_empty: true,
            },
        ),
        (
            "primary".to_string(),
            OutputCaptureSpec {
                name: "primary".to_string(),
                capture: CaptureKind::parse_token(&format!(
                    "file_regex:{YT_DLP_OUTPUT_CONTENT_REGEX}"
                ))
                .expect("valid yt-dlp content capture token"),
                save: SaveMode::True,
                allow_empty: true,
            },
        ),
        (
            OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
                capture: CaptureKind::parse_token(&format!("folder:{SANDBOX_DOWNLOADS_DIR}"))
                    .expect("valid yt-dlp downloads folder capture token"),
                save: SaveMode::True,
                allow_empty: true,
            },
        ),
        (
            "stdout".to_string(),
            OutputCaptureSpec {
                name: "stdout".to_string(),
                capture: CaptureKind::parse_token("stdout")
                    .expect("valid yt-dlp stdout capture token"),
                save: SaveMode::True,
                allow_empty: false,
            },
        ),
        (
            "stderr".to_string(),
            OutputCaptureSpec {
                name: "stderr".to_string(),
                capture: CaptureKind::parse_token("stderr")
                    .expect("valid yt-dlp stderr capture token"),
                save: SaveMode::True,
                allow_empty: false,
            },
        ),
        (
            "process_code".to_string(),
            OutputCaptureSpec {
                name: "process_code".to_string(),
                capture: CaptureKind::parse_token("process_code")
                    .expect("valid yt-dlp process_code capture token"),
                save: SaveMode::True,
                allow_empty: false,
            },
        ),
    ])
}

/// Builds yt-dlp output capture spec map.
///
/// These are live defaults: every yt-dlp step captures the whole set, under its
/// own declarations for the names it binds. Which sidecar families an
/// invocation writes depends on the URL and the options, so the folder-typed
/// families tolerate an empty match, and the video defaults (`content` and
/// `primary`) do the same because a step that skips the download inherits these
/// names and produces no video. A step that needs a video restates `primary`
/// and keeps its own strictness via the merge rules.
#[must_use]
fn build_yt_dlp_outputs() -> BTreeMap<String, OutputCaptureSpec> {
    let mut outputs = base_yt_dlp_outputs();

    // Subtitle, thumbnail, link, and chapter captures are variant-dependent:
    // the effective family and (for subtitles) the language-scoped regex come
    // from the resolved variant config. Each producing step declares its own
    // capture, so the tool must not force a default capture that would override
    // a step's resolved token at merge time. `allow_empty` defaults to the
    // folder-family tolerance when a step omits an explicit declaration.
    for (output_name, path_regex) in [
        (OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS, YT_DLP_SUBTITLE_ARTIFACTS_REGEX),
        (OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS, YT_DLP_THUMBNAIL_ARTIFACTS_REGEX),
        (OUTPUT_YT_DLP_LINK_ARTIFACTS, YT_DLP_LINK_ARTIFACTS_REGEX),
        (OUTPUT_YT_DLP_CHAPTER_ARTIFACTS, YT_DLP_CHAPTER_ARTIFACTS_REGEX),
    ] {
        let _ = (output_name, path_regex);
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
                capture: CaptureKind::parse_token(&format!("file_regex:{path_regex}"))
                    .expect("valid yt-dlp capture token"),
                save: SaveMode::True,
                allow_empty: true,
            },
        );
    }

    outputs
}

/// Builds yt-dlp default input values.
#[must_use]
fn build_yt_dlp_default_input_defaults() -> BTreeMap<String, InputBinding> {
    let mut defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), InputBinding::Vec(vec![])),
        (INPUT_TRAILING_ARGS.to_string(), InputBinding::Vec(vec![])),
    ]);
    for option_input in YT_DLP_OPTION_INPUTS {
        defaults.entry((*option_input).to_string()).or_default();
    }
    for (key, value) in YT_DLP_INPUT_DEFAULTS {
        defaults.insert(key.to_string(), InputBinding::String(value.to_string()));
    }
    defaults
}

/// Builds the full [`ToolSpec`] and [`ToolRuntime`] for the managed yt-dlp tool.
#[must_use]
pub(crate) fn build_yt_dlp_spec(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> (ToolSpec, ToolRuntime) {
    let command_path = build_os_conditional_selector(os_exec_paths);
    let (mut spec, mut runtime) = assemble_tool_spec(
        "yt-dlp",
        content_map,
        build_yt_dlp_command(&command_path),
        build_yt_dlp_inputs(),
        build_yt_dlp_outputs(),
        build_yt_dlp_default_input_defaults(),
        true, // impure — yt-dlp requires network
    );
    // yt-dlp is the one tool that overrides both policies: a download must not
    // run beside another one, and a failed download must not silently repeat a
    // network call.
    runtime.max_concurrent_calls = Some(1);
    runtime.max_retries = 0;
    spec.runtime.max_concurrent_calls = Some(1);
    spec.runtime.max_retries = 0;
    // The hermetic stub chain publishes these names in the host environment.
    // Declaring them lets whitelisted yt-dlp steps receive the emit-mode stub's
    // manifest and the invocation log when a run mounts the chain.
    for name in ["MEDIAPM_STUB_MANIFEST", "MEDIAPM_STUB_INVOCATIONS"] {
        runtime.inherited_env_vars.push(name.to_owned());
        spec.runtime.inherited_env_vars.push(name.to_owned());
    }
    (spec, runtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OutputCaptureKind;

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

    /// Variant-dependent families are declared by producing steps, not by the
    /// tool defaults.
    ///
    /// Their capture token and family come from the resolved variant config
    /// (including language scoping and `capture_kind`), so a tool-level default
    /// would override a step's resolved capture at merge time and collapse
    /// distinct variants onto one instance key.
    #[test]
    fn variant_dependent_family_captures_are_not_tool_level_defaults() {
        let outputs = build_yt_dlp_outputs();
        assert!(outputs.contains_key(OUTPUT_CONTENT), "missing content output");
        for name in [
            OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
            OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
            OUTPUT_YT_DLP_LINK_ARTIFACTS,
            OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
        ] {
            assert!(
                !outputs.contains_key(name),
                "variant-dependent capture '{name}' must be declared by the producing step",
            );
        }
        assert!(outputs.contains_key(OUTPUT_YT_DLP_DESCRIPTION_FILE), "missing description output");
        assert!(outputs.contains_key(OUTPUT_YT_DLP_INFOJSON_FILE), "missing infojson output");
        assert!(outputs.contains_key(OUTPUT_YT_DLP_ARCHIVE_FILE), "missing archive output");
    }

    #[test]
    fn build_yt_dlp_defaults_prefer_single_best_thumbnail() {
        let defaults = build_yt_dlp_default_input_defaults();
        assert_eq!(defaults.get("write_subs"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(
            defaults.get("write_auto_subs"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(defaults.get("sub_langs"), Some(&InputBinding::String("all".to_string())));
        assert!(defaults.get("extractor_args").is_some_and(|v| {
            matches!(v, InputBinding::String(s) if s.contains("skip=translated_subs"))
        }));
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
        assert_eq!(defaults.get("write_url_link"), Some(&InputBinding::String("true".to_string())));
        assert_eq!(
            defaults.get("write_desktop_link"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            defaults.get("download_archive"),
            Some(&InputBinding::String("downloads/archive.txt".to_string()))
        );
        assert_eq!(
            defaults.get("embed_thumbnail"),
            Some(&InputBinding::String("false".to_string()))
        );
    }

    /// A comment variant captures the info JSON.
    ///
    /// yt-dlp stores comments in the info JSON rather than in a separate
    /// `comment.*` file, so the comment variant must reuse the info JSON
    /// capture token.
    #[test]
    fn comment_variant_captures_info_json() {
        let config = crate::config::YtDlpOutputVariantConfig {
            kind: YtDlpOutputKind::Comment,
            ..crate::config::YtDlpOutputVariantConfig::default()
        };
        let context = super::AutoSaveContext::new(std::collections::BTreeSet::new(), None);
        let (capture, _save) =
            yt_dlp_variant_capture_and_save(&config, "comment", 0, &context, false)
                .expect("comment variant capture");
        assert_eq!(
            capture.to_token(),
            format!("file_regex:{YT_DLP_INFOJSON_OUTPUT_REGEX}"),
            "comment output must be captured from the info JSON",
        );
    }

    /// A folder-shaped yt-dlp kind with no override defaults to a folder capture.
    #[test]
    fn yt_dlp_folder_shaped_kind_defaults_to_folder_capture() {
        let context = super::AutoSaveContext::new(std::collections::BTreeSet::new(), None);
        let spec = crate::tools::workflows::variant_to_output_capture_spec(
            OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
            "subtitles",
            &crate::config::OutputVariantValue::YtDlp(crate::config::YtDlpOutputVariantConfig {
                kind: YtDlpOutputKind::Subtitles,
                ..crate::config::YtDlpOutputVariantConfig::default()
            }),
            0,
            &context,
            false,
        )
        .expect("subtitles capture");
        assert_eq!(
            spec.capture.to_token(),
            format!("folder_regex:{YT_DLP_SUBTITLE_ARTIFACTS_REGEX}")
        );
        assert!(spec.allow_empty, "a folder capture tolerates nothing being captured");
    }

    /// A file-shaped yt-dlp kind with no override defaults to a file capture.
    #[test]
    fn yt_dlp_file_shaped_kind_defaults_to_file_capture() {
        let context = super::AutoSaveContext::new(std::collections::BTreeSet::new(), None);
        let spec = crate::tools::workflows::variant_to_output_capture_spec(
            crate::tools::workflows::OUTPUT_PRIMARY,
            "primary",
            &crate::config::OutputVariantValue::YtDlp(crate::config::YtDlpOutputVariantConfig {
                kind: YtDlpOutputKind::Primary,
                ..crate::config::YtDlpOutputVariantConfig::default()
            }),
            0,
            &context,
            false,
        )
        .expect("primary capture");
        assert_eq!(spec.capture.to_token(), format!("file_regex:{YT_DLP_OUTPUT_CONTENT_REGEX}"));
        assert!(!spec.allow_empty, "a file capture does not tolerate an empty match");
    }

    /// The tool-level video defaults tolerate an empty match.
    ///
    /// A step that skips the download inherits `content` and `primary` and
    /// produces no video, so these defaults must not fail the capture; a step
    /// that needs a video restates `primary` and keeps its own strictness.
    #[test]
    fn yt_dlp_video_defaults_tolerate_an_empty_match() {
        let outputs = build_yt_dlp_outputs();
        assert!(
            outputs[OUTPUT_CONTENT].allow_empty,
            "content default must tolerate a step that skips download",
        );
        assert!(
            outputs["primary"].allow_empty,
            "primary default must tolerate a step that skips download",
        );
    }

    /// A generated primary video step keeps its `primary` capture strict.
    ///
    /// The tolerant tool-level default is overridden by the step's own
    /// declaration for a name it binds, so a step that needs a video still
    /// fails when the video is missing.
    #[test]
    fn the_primary_video_step_declares_a_strict_capture() {
        use crate::config::output_types::ResolvedStepVariantFlow;
        use crate::config::{
            MediaStep, MediaStepTool, OutputVariantValue, YtDlpOutputKind, YtDlpOutputVariantConfig,
        };
        use crate::tools::workflows::AutoSaveContext;
        use mediapm_conductor::WorkflowSpec;
        use std::collections::{BTreeMap, BTreeSet};

        let mut workflow = WorkflowSpec::default();
        let step = MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "downloaded".to_string(),
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..YtDlpOutputVariantConfig::default()
                }),
            )]),
            options: BTreeMap::new(),
        };
        let mappings = vec![ResolvedStepVariantFlow {
            input: "downloaded".to_string(),
            output: "downloaded".to_string(),
        }];
        let mut variant_producers = BTreeMap::new();
        let context = AutoSaveContext::new(BTreeSet::new(), Some(0));

        synthesize_yt_dlp_step(
            &mut workflow,
            "media-a",
            0,
            &step,
            &mappings,
            "yt-dlp",
            &BTreeMap::new(),
            &mut variant_producers,
            &context,
        )
        .expect("primary step synthesis");

        let step_spec = workflow.steps.first().expect("generated step");
        let primary = step_spec.outputs.get("primary").expect("primary output");
        assert!(!primary.allow_empty, "a step that needs a video must keep primary strict");
    }

    /// An explicit file override forces a file capture for a folder-shaped kind.
    #[test]
    fn yt_dlp_explicit_file_override_on_folder_shaped_kind() {
        let context = super::AutoSaveContext::new(std::collections::BTreeSet::new(), None);
        let spec = crate::tools::workflows::variant_to_output_capture_spec(
            OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
            "subtitles",
            &crate::config::OutputVariantValue::YtDlp(crate::config::YtDlpOutputVariantConfig {
                kind: YtDlpOutputKind::Subtitles,
                capture_kind: Some(OutputCaptureKind::File),
                ..crate::config::YtDlpOutputVariantConfig::default()
            }),
            0,
            &context,
            false,
        )
        .expect("file-override subtitles capture");
        assert_eq!(
            spec.capture.to_token(),
            format!("file_regex:{YT_DLP_SUBTITLE_ARTIFACTS_REGEX}")
        );
        assert!(!spec.allow_empty, "a file capture does not tolerate an empty match");
    }

    /// An explicit folder override forces a folder capture for a file-shaped kind.
    #[test]
    fn yt_dlp_explicit_folder_override_on_file_shaped_kind() {
        let context = super::AutoSaveContext::new(std::collections::BTreeSet::new(), None);
        let spec = crate::tools::workflows::variant_to_output_capture_spec(
            crate::tools::workflows::OUTPUT_PRIMARY,
            "primary",
            &crate::config::OutputVariantValue::YtDlp(crate::config::YtDlpOutputVariantConfig {
                kind: YtDlpOutputKind::Primary,
                capture_kind: Some(OutputCaptureKind::Folder),
                ..crate::config::YtDlpOutputVariantConfig::default()
            }),
            0,
            &context,
            false,
        )
        .expect("folder-override primary capture");
        assert_eq!(spec.capture.to_token(), format!("folder_regex:{YT_DLP_OUTPUT_CONTENT_REGEX}"));
        assert!(spec.allow_empty, "a folder capture tolerates nothing being captured");
    }

    /// An explicit folder override on a folder-shaped subtitles kind keeps the
    /// folder capture.
    #[test]
    fn yt_dlp_explicit_folder_override_on_subtitles_kind() {
        let context = super::AutoSaveContext::new(std::collections::BTreeSet::new(), None);
        let spec = crate::tools::workflows::variant_to_output_capture_spec(
            OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
            "subtitles",
            &crate::config::OutputVariantValue::YtDlp(crate::config::YtDlpOutputVariantConfig {
                kind: YtDlpOutputKind::Subtitles,
                capture_kind: Some(OutputCaptureKind::Folder),
                ..crate::config::YtDlpOutputVariantConfig::default()
            }),
            0,
            &context,
            false,
        )
        .expect("folder-override subtitles capture");
        assert_eq!(
            spec.capture.to_token(),
            format!("folder_regex:{YT_DLP_SUBTITLE_ARTIFACTS_REGEX}")
        );
        assert!(spec.allow_empty, "a folder capture tolerates nothing being captured");
    }

    /// An explicit folder override on a folder-shaped thumbnails kind keeps the
    /// folder capture.
    #[test]
    fn yt_dlp_explicit_folder_override_on_thumbnails_kind() {
        let context = super::AutoSaveContext::new(std::collections::BTreeSet::new(), None);
        let spec = crate::tools::workflows::variant_to_output_capture_spec(
            OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
            "thumbnails",
            &crate::config::OutputVariantValue::YtDlp(crate::config::YtDlpOutputVariantConfig {
                kind: YtDlpOutputKind::Thumbnails,
                capture_kind: Some(OutputCaptureKind::Folder),
                ..crate::config::YtDlpOutputVariantConfig::default()
            }),
            0,
            &context,
            false,
        )
        .expect("folder-override thumbnails capture");
        assert_eq!(
            spec.capture.to_token(),
            format!("folder_regex:{YT_DLP_THUMBNAIL_ARTIFACTS_REGEX}")
        );
        assert!(spec.allow_empty, "a folder capture tolerates nothing being captured");
    }

    /// An explicit folder override on a folder-shaped chapters kind keeps the
    /// folder capture.
    #[test]
    fn yt_dlp_explicit_folder_override_on_chapters_kind() {
        let context = super::AutoSaveContext::new(std::collections::BTreeSet::new(), None);
        let spec = crate::tools::workflows::variant_to_output_capture_spec(
            OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
            "chapters",
            &crate::config::OutputVariantValue::YtDlp(crate::config::YtDlpOutputVariantConfig {
                kind: YtDlpOutputKind::Chapters,
                capture_kind: Some(OutputCaptureKind::Folder),
                ..crate::config::YtDlpOutputVariantConfig::default()
            }),
            0,
            &context,
            false,
        )
        .expect("folder-override chapters capture");
        assert_eq!(
            spec.capture.to_token(),
            format!("folder_regex:{YT_DLP_CHAPTER_ARTIFACTS_REGEX}")
        );
        assert!(spec.allow_empty, "a folder capture tolerates nothing being captured");
    }

    /// An explicit folder override on a folder-shaped links kind keeps the
    /// folder capture.
    #[test]
    fn yt_dlp_explicit_folder_override_on_links_kind() {
        let context = super::AutoSaveContext::new(std::collections::BTreeSet::new(), None);
        let spec = crate::tools::workflows::variant_to_output_capture_spec(
            OUTPUT_YT_DLP_LINK_ARTIFACTS,
            "links",
            &crate::config::OutputVariantValue::YtDlp(crate::config::YtDlpOutputVariantConfig {
                kind: YtDlpOutputKind::Links,
                capture_kind: Some(OutputCaptureKind::Folder),
                ..crate::config::YtDlpOutputVariantConfig::default()
            }),
            0,
            &context,
            false,
        )
        .expect("folder-override links capture");
        assert_eq!(spec.capture.to_token(), format!("folder_regex:{YT_DLP_LINK_ARTIFACTS_REGEX}"));
        assert!(spec.allow_empty, "a folder capture tolerates nothing being captured");
    }

    #[test]
    fn yt_dlp_states_both_policies_explicitly() {
        let (spec, runtime) = build_yt_dlp_spec(BTreeMap::new(), &BTreeMap::new());
        assert_eq!(
            runtime.max_concurrent_calls,
            Some(1),
            "yt-dlp must cap concurrency at one, because two downloads into one sandbox collide"
        );
        assert_eq!(
            runtime.max_retries, 0,
            "yt-dlp must not retry by default, because a retried download repeats a network call"
        );
        assert_eq!(
            spec.runtime.max_concurrent_calls,
            Some(1),
            "the generated spec must carry the same cap, otherwise a skipped or serialized tool reverts to unlimited"
        );
        assert_eq!(
            spec.runtime.max_retries, 0,
            "the generated spec must carry the same retry policy, otherwise a skipped or serialized tool reverts to the default"
        );
    }
}
