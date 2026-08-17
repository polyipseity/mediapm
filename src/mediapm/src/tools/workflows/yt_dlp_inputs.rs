//! yt-dlp output variant decoding and input-binding resolution.
//!
//! Provides decoding helpers for yt-dlp specific output variant configs and
//! step-output binding resolution across workflow steps.

use std::collections::BTreeMap;

use mediapm_conductor::tools::helpers::build_os_conditional_selector;

use crate::conductor_bridge::constants::{
    DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, OUTPUT_YT_DLP_ANNOTATION_FILE, OUTPUT_YT_DLP_ARCHIVE_FILE,
    OUTPUT_YT_DLP_CHAPTER_ARTIFACTS, OUTPUT_YT_DLP_DESCRIPTION_FILE, OUTPUT_YT_DLP_INFOJSON_FILE,
    OUTPUT_YT_DLP_LINK_ARTIFACTS, OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
    OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
};
use crate::config::{
    MediaStepTool, OutputCaptureKind, OutputVariantValue, YtDlpOutputKind, YtDlpOutputVariantConfig,
};
use crate::error::MediaPmError;

use super::{FfmpegSlotLimits, OUTPUT_PRIMARY};

/// Output capture name for yt-dlp comment artifacts (no shared constant exists).
const YT_DLP_COMMENTS_ARTIFACTS: &str = "yt_dlp_comments_artifacts";

/// Decodes a yt-dlp variant config from raw JSON [`serde_json::Value`].
///
/// Falls back to a generic variant decoding when the value is not structured
/// as a yt-dlp-specific variant object.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the variant value cannot be decoded as either
/// a yt-dlp or generic variant config.
pub(crate) fn decode_yt_dlp_output_variant_config(
    value: serde_json::Value,
) -> Result<OutputVariantValue, MediaPmError> {
    serde_json::from_value::<OutputVariantValue>(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode yt-dlp variant: {e}")))
}

/// Step output binding pointing to another step's output by name and optional
/// zip member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepOutputBinding {
    /// Logical output name targeted by this binding.
    pub output_name: String,
    /// Zip-member path inside a folder output (`None` = whole folder).
    pub zip_member: Option<String>,
}

/// Resolves one step output binding from the step's tool and variant config.
///
/// Generic (non-ffmpeg) variants bind to their raw `kind`; ffmpeg variants bind
/// to the indexed output capture name validated against the ffmpeg slot limit.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when the variant is missing, the ffmpeg
/// index is invalid, or it exceeds `tools.ffmpeg.max_output_slots`.
pub(crate) fn resolve_step_output_binding(
    tool: MediaStepTool,
    output_variants: &BTreeMap<String, OutputVariantValue>,
    output_variant: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<StepOutputBinding, MediaPmError> {
    let value = output_variants.get(output_variant).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "missing output variant '{output_variant}' while resolving step output binding"
        ))
    })?;

    Ok(match value {
        OutputVariantValue::Generic(config) => {
            let output_name = if matches!(tool, MediaStepTool::Ffmpeg) {
                let index = config.idx;
                let output_index = usize::try_from(index).map_err(|_| {
                    MediaPmError::Workflow(format!(
                        "invalid ffmpeg idx '{index}' for output variant '{output_variant}'"
                    ))
                })?;
                if output_index >= ffmpeg_slot_limits.max_output_slots {
                    return Err(MediaPmError::Workflow(format!(
                        "output variant '{output_variant}' uses ffmpeg idx '{index}' but tools.ffmpeg.max_output_slots is {}; reduce idx usage or increase tools.ffmpeg.max_output_slots (default {DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS})",
                        ffmpeg_slot_limits.max_output_slots,
                    )));
                }
                crate::tools::workflows::ffmpeg::ffmpeg_output_capture_name(output_index)
            } else {
                config.kind.clone()
            };

            StepOutputBinding {
                output_name,
                zip_member: if config.zip_member.is_empty() {
                    None
                } else {
                    Some(config.zip_member.clone())
                },
            }
        }
        OutputVariantValue::YtDlp(config) => StepOutputBinding {
            output_name: yt_dlp_output_name_for_kind(config.kind),
            zip_member: yt_dlp_zip_member_for_variant(config),
        },
    })
}

/// OS-conditional inlined companion-dep path for yt-dlp post-processing.
#[must_use]
fn yt_dlp_managed_ffmpeg_location_selector() -> String {
    build_os_conditional_selector(&BTreeMap::from([
        ("linux".to_string(), "../deps/ffmpeg/linux/ffmpeg".to_string()),
        ("macos".to_string(), "../deps/ffmpeg/macos/ffmpeg".to_string()),
        ("windows".to_string(), "../deps/ffmpeg/windows/ffmpeg.exe".to_string()),
    ]))
}

/// OS-conditional inlined companion-dep path for the deno JS runtime yt-dlp
/// requires for modern `YouTube` extraction. yt-dlp's `--js-runtimes` expects
/// the `RUNTIME[:PATH]` form, so the `deno:` runtime name prefixes the
/// OS-conditional path selector.
#[must_use]
fn yt_dlp_managed_js_runtimes_selector() -> String {
    let inner = build_os_conditional_selector(&BTreeMap::from([
        ("linux".to_string(), "../deps/deno/linux/deno".to_string()),
        ("macos".to_string(), "../deps/deno/macos/deno".to_string()),
        ("windows".to_string(), "../deps/deno/windows/deno.exe".to_string()),
    ]));
    format!("deno:{inner}")
}

/// Resolves optional ZIP-member selector for yt-dlp variant materialization.
#[must_use]
fn yt_dlp_zip_member_for_variant(config: &YtDlpOutputVariantConfig) -> Option<String> {
    if !config.zip_member.is_empty() {
        return Some(config.zip_member.clone());
    }

    if config.capture_kind == Some(OutputCaptureKind::File)
        && matches!(config.kind, YtDlpOutputKind::Subtitles)
        && !config.langs.is_empty()
    {
        return Some(format!(".{}.vtt", config.langs));
    }

    None
}

/// Logical output capture name for a yt-dlp output variant kind.
#[must_use]
pub(crate) fn yt_dlp_output_name_for_kind(kind: YtDlpOutputKind) -> String {
    match kind {
        YtDlpOutputKind::Primary => OUTPUT_PRIMARY.to_string(),
        YtDlpOutputKind::Chapters => OUTPUT_YT_DLP_CHAPTER_ARTIFACTS.to_string(),
        YtDlpOutputKind::Subtitles => OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS.to_string(),
        YtDlpOutputKind::Thumbnails => OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS.to_string(),
        YtDlpOutputKind::Description => OUTPUT_YT_DLP_DESCRIPTION_FILE.to_string(),
        YtDlpOutputKind::Infojson => OUTPUT_YT_DLP_INFOJSON_FILE.to_string(),
        YtDlpOutputKind::Comment => YT_DLP_COMMENTS_ARTIFACTS.to_string(),
        YtDlpOutputKind::Archive => OUTPUT_YT_DLP_ARCHIVE_FILE.to_string(),
        YtDlpOutputKind::Annotation => OUTPUT_YT_DLP_ANNOTATION_FILE.to_string(),
        YtDlpOutputKind::Links => OUTPUT_YT_DLP_LINK_ARTIFACTS.to_string(),
    }
}

/// yt-dlp input name used for the `convert` override semantics.
#[must_use]
pub(crate) fn yt_dlp_convert_input_name_for_kind(kind: YtDlpOutputKind) -> &'static str {
    match kind {
        YtDlpOutputKind::Subtitles => "convert_subs",
        YtDlpOutputKind::Thumbnails => "convert_thumbnails",
        _ => "recode_video",
    }
}

/// Builds yt-dlp option inputs from one value-driven output-variant config.
#[must_use]
pub(crate) fn yt_dlp_variant_inputs(config: &YtDlpOutputVariantConfig) -> BTreeMap<String, String> {
    let mut inputs = BTreeMap::new();

    if !matches!(config.kind, YtDlpOutputKind::Primary) {
        inputs.insert("skip_download".to_string(), "true".to_string());
    }

    match config.kind {
        YtDlpOutputKind::Primary => {
            inputs.insert("ffmpeg_location".to_string(), yt_dlp_managed_ffmpeg_location_selector());
            inputs.insert("js_runtimes".to_string(), yt_dlp_managed_js_runtimes_selector());
        }
        YtDlpOutputKind::Chapters => {
            inputs.insert("split_chapters".to_string(), "true".to_string());
            inputs.insert("write_description".to_string(), "false".to_string());
            inputs.insert("write_info_json".to_string(), "false".to_string());
        }
        YtDlpOutputKind::Subtitles => {
            inputs.insert("write_subs".to_string(), "true".to_string());
            inputs.insert("write_auto_subs".to_string(), "true".to_string());
            inputs.insert("convert_subs".to_string(), "vtt".to_string());
            inputs.insert("write_description".to_string(), "false".to_string());
            inputs.insert("write_info_json".to_string(), "false".to_string());
        }
        YtDlpOutputKind::Thumbnails => {
            inputs.insert("write_thumbnail".to_string(), "true".to_string());
            inputs.insert("write_description".to_string(), "false".to_string());
            inputs.insert("write_info_json".to_string(), "false".to_string());
        }
        YtDlpOutputKind::Description => {
            inputs.insert("write_description".to_string(), "true".to_string());
            inputs.insert("write_info_json".to_string(), "false".to_string());
        }
        YtDlpOutputKind::Infojson => {
            inputs.insert("write_info_json".to_string(), "true".to_string());
            inputs.insert("write_description".to_string(), "false".to_string());
        }
        YtDlpOutputKind::Comment => {
            inputs.insert("write_comments".to_string(), "true".to_string());
            inputs.insert("write_description".to_string(), "false".to_string());
            inputs.insert("write_info_json".to_string(), "false".to_string());
        }
        YtDlpOutputKind::Archive => {
            // Old workflow synthesis had no Archive arm; mirror the annotation
            // sidecar family so only the archive artifact is produced. No
            // `write_archive` token exists in the yt-dlp option spec.
            inputs.insert("write_description".to_string(), "false".to_string());
            inputs.insert("write_info_json".to_string(), "false".to_string());
        }
        YtDlpOutputKind::Annotation => {
            inputs.insert("write_description".to_string(), "false".to_string());
            inputs.insert("write_info_json".to_string(), "false".to_string());
        }
        YtDlpOutputKind::Links => {
            inputs.insert("write_link".to_string(), "true".to_string());
            inputs.insert("write_description".to_string(), "false".to_string());
            inputs.insert("write_info_json".to_string(), "false".to_string());
        }
    }

    if !config.langs.is_empty() {
        inputs.insert("sub_langs".to_string(), config.langs.clone());
    }
    if !config.sub_format.is_empty() {
        inputs.insert("sub_format".to_string(), config.sub_format.clone());
    }
    if !config.convert.is_empty() {
        inputs.insert(
            yt_dlp_convert_input_name_for_kind(config.kind).to_string(),
            config.convert.clone(),
        );
    }

    inputs
}
