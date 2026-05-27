//! yt-dlp input/output binding helpers for workflow synthesis.
//!
//! This module isolates yt-dlp-specific toggle-input construction, output-kind
//! mapping, and per-variant policy decoding from the shared step synthesizer,
//! keeping downloader output semantics self-contained.

use std::collections::BTreeMap;

use mediapm_conductor::{InputBinding, OutputPolicy};
use serde_json::Value;

use crate::config::{
    DecodedOutputVariantConfig, MediaStepTool, OutputCaptureKind, YtDlpOutputKind,
    YtDlpOutputVariantConfig, decode_output_variant_config, decode_output_variant_policy,
};
use crate::error::MediaPmError;

use super::{
    DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, FfmpegSlotLimits, OUTPUT_IMPORT_RESULT, OUTPUT_PRIMARY,
    OUTPUT_SANDBOX_ARTIFACTS, OUTPUT_YT_DLP_ANNOTATION_FILE, OUTPUT_YT_DLP_ARCHIVE_FILE,
    OUTPUT_YT_DLP_CHAPTER_ARTIFACTS, OUTPUT_YT_DLP_DESCRIPTION_FILE, OUTPUT_YT_DLP_INFOJSON_FILE,
    OUTPUT_YT_DLP_LINK_ARTIFACTS, OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE,
    OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE, OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
    OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS, YT_DLP_MANAGED_ARCHIVE_FILE, conductor_output_save_mode,
    ffmpeg_output_capture_name,
};

/// Resolved output binding behavior for one step output-variant entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StepOutputBinding {
    /// Step output name to reference in `${step_output...}` bindings.
    pub(super) output_name: String,
    /// Optional ZIP member selector applied during downstream input binding.
    pub(super) zip_member: Option<String>,
}

/// Decodes one yt-dlp output-variant config entry for a specific map key.
pub(super) fn decode_yt_dlp_output_variant_config(
    variant_key: &str,
    output_variants: &BTreeMap<String, Value>,
) -> Result<YtDlpOutputVariantConfig, MediaPmError> {
    let value = output_variants.get(variant_key).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "missing output variant '{variant_key}' while decoding yt-dlp config"
        ))
    })?;

    match decode_output_variant_config(MediaStepTool::YtDlp, variant_key, value)
        .map_err(MediaPmError::Workflow)?
    {
        DecodedOutputVariantConfig::YtDlp(config) => Ok(config),
        DecodedOutputVariantConfig::Generic(_) => Err(MediaPmError::Workflow(format!(
            "decoded non-yt-dlp output variant config for yt-dlp key '{variant_key}'"
        ))),
    }
}

/// Resolves one output variant to the generated step output binding behavior.
pub(super) fn resolve_step_output_binding(
    tool: MediaStepTool,
    output_variants: &BTreeMap<String, Value>,
    output_variant: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<StepOutputBinding, MediaPmError> {
    let value = output_variants.get(output_variant).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "missing output variant '{output_variant}' while resolving step output binding"
        ))
    })?;

    let decoded = decode_output_variant_config(tool, output_variant, value)
        .map_err(MediaPmError::Workflow)?;

    Ok(match decoded {
        DecodedOutputVariantConfig::Generic(config) => {
            let output_name = if matches!(tool, MediaStepTool::Ffmpeg) {
                let index = config.idx.ok_or_else(|| {
                    MediaPmError::Workflow(format!(
                        "missing ffmpeg idx for output variant '{output_variant}'"
                    ))
                })?;
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
                ffmpeg_output_capture_name(output_index)
            } else if matches!(tool, MediaStepTool::Import) {
                OUTPUT_IMPORT_RESULT.to_string()
            } else {
                config.kind
            };

            StepOutputBinding { output_name, zip_member: config.zip_member }
        }
        DecodedOutputVariantConfig::YtDlp(config) => {
            let capture_kind = config.effective_capture_kind();
            StepOutputBinding {
                output_name: yt_dlp_output_name_for_kind(config.kind, capture_kind).to_string(),
                zip_member: config.zip_member,
            }
        }
    })
}

/// Returns yt-dlp boolean sidecar toggle input keys used by managed runtime
/// defaults and per-variant overrides.
///
/// Per-variant sidecar workflow steps set all of these toggles to `"false"`
/// first, then selectively enable only the toggle required by the variant
/// kind. This keeps each sidecar family isolated and prevents mixed output
/// artifact directories.
#[must_use]
fn yt_dlp_sidecar_toggle_inputs() -> [&'static str; 12] {
    [
        "write_subs",
        "write_thumbnail",
        "write_all_thumbnails",
        "write_description",
        "write_annotations",
        "write_chapters",
        "write_info_json",
        "write_url_link",
        "write_webloc_link",
        "write_desktop_link",
        "write_comments",
        "write_link",
    ]
}

/// Counts non-empty comma-separated selector values.
/// Builds merged yt-dlp option inputs from requested output-variant kinds.
///
/// This enables the minimum required toggles so one yt-dlp call can produce
/// multiple declared outputs efficiently.
///
/// Language downloader selection remains authoritative in
/// `steps[*].options.sub_langs`; variant-level language hints are used only by
/// capture/materialization behavior.
pub(super) fn yt_dlp_inputs_for_output_variants(
    output_configs: &[YtDlpOutputVariantConfig],
) -> Result<BTreeMap<String, InputBinding>, MediaPmError> {
    let mut inputs = BTreeMap::new();
    let true_binding = || InputBinding::String("true".to_string());
    let false_binding = || InputBinding::String("false".to_string());

    if output_configs.is_empty() {
        return Ok(inputs);
    }

    for toggle in yt_dlp_sidecar_toggle_inputs() {
        inputs.insert(toggle.to_string(), false_binding());
    }
    inputs.insert("split_chapters".to_string(), false_binding());

    let has_primary_or_sandbox = output_configs
        .iter()
        .any(|config| matches!(config.kind, YtDlpOutputKind::Primary | YtDlpOutputKind::Sandbox));
    if !has_primary_or_sandbox {
        inputs.insert("skip_download".to_string(), true_binding());
    }

    for config in output_configs {
        match config.kind {
            YtDlpOutputKind::Primary => {}
            YtDlpOutputKind::Sandbox => {
                for toggle in yt_dlp_sidecar_toggle_inputs() {
                    inputs.insert(toggle.to_string(), true_binding());
                }
                inputs.insert("embed_chapters".to_string(), true_binding());
                inputs.insert("split_chapters".to_string(), true_binding());
            }
            YtDlpOutputKind::Subtitles => {
                inputs.insert("write_subs".to_string(), true_binding());
            }
            YtDlpOutputKind::Thumbnails => {
                inputs.insert("write_thumbnail".to_string(), true_binding());
            }
            YtDlpOutputKind::Description | YtDlpOutputKind::PlaylistDescription => {
                inputs.insert("write_description".to_string(), true_binding());
            }
            YtDlpOutputKind::Annotation => {
                inputs.insert("write_annotations".to_string(), true_binding());
            }
            YtDlpOutputKind::Infojson | YtDlpOutputKind::PlaylistInfojson => {
                inputs.insert("write_info_json".to_string(), true_binding());
            }
            YtDlpOutputKind::Comment => {
                inputs.insert("write_comments".to_string(), true_binding());
                inputs.insert("write_info_json".to_string(), true_binding());
            }
            YtDlpOutputKind::Archive => {
                merge_yt_dlp_scalar_override(
                    &mut inputs,
                    "download_archive",
                    YT_DLP_MANAGED_ARCHIVE_FILE,
                )?;
            }
            YtDlpOutputKind::Links => {
                inputs.insert("write_link".to_string(), true_binding());
                inputs.insert("write_url_link".to_string(), true_binding());
                inputs.insert("write_webloc_link".to_string(), true_binding());
                inputs.insert("write_desktop_link".to_string(), true_binding());
            }
            YtDlpOutputKind::Chapters => {
                inputs.insert("write_chapters".to_string(), true_binding());
                inputs.insert("split_chapters".to_string(), true_binding());
            }
        }

        if let Some(sub_format) = config.sub_format.as_deref() {
            merge_yt_dlp_scalar_override(&mut inputs, "sub_format", sub_format)?;
        }
        if let Some(convert) = config.convert.as_deref() {
            merge_yt_dlp_scalar_override(
                &mut inputs,
                yt_dlp_convert_input_name_for_kind(config.kind),
                convert,
            )?;
        }
    }

    Ok(inputs)
}

/// Merges one scalar yt-dlp per-variant override while rejecting conflicts.
fn merge_yt_dlp_scalar_override(
    inputs: &mut BTreeMap<String, InputBinding>,
    key: &str,
    value: &str,
) -> Result<(), MediaPmError> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Ok(());
    }

    match inputs.get(key) {
        Some(InputBinding::String(existing)) if existing == normalized => Ok(()),
        Some(InputBinding::String(existing)) => Err(MediaPmError::Workflow(format!(
            "yt-dlp multi-output step has conflicting '{key}' values: '{existing}' vs '{normalized}'"
        ))),
        Some(_) => Err(MediaPmError::Workflow(format!(
            "yt-dlp multi-output step cannot merge non-scalar input override for '{key}'"
        ))),
        None => {
            inputs.insert(key.to_string(), InputBinding::String(normalized.to_string()));
            Ok(())
        }
    }
}

/// Resolves yt-dlp input name used for `convert` override semantics.
#[must_use]
fn yt_dlp_convert_input_name_for_kind(kind: YtDlpOutputKind) -> &'static str {
    match kind {
        YtDlpOutputKind::Subtitles => "convert_subs",
        YtDlpOutputKind::Thumbnails => "convert_thumbnails",
        _ => "recode_video",
    }
}

/// Maps one value-driven yt-dlp output kind to generated output capture name.
#[must_use]
fn yt_dlp_output_name_for_kind(
    kind: YtDlpOutputKind,
    capture_kind: OutputCaptureKind,
) -> &'static str {
    match kind {
        YtDlpOutputKind::Primary => {
            if matches!(capture_kind, OutputCaptureKind::Folder) {
                OUTPUT_SANDBOX_ARTIFACTS
            } else {
                OUTPUT_PRIMARY
            }
        }
        YtDlpOutputKind::Sandbox => OUTPUT_SANDBOX_ARTIFACTS,
        YtDlpOutputKind::Subtitles => OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
        YtDlpOutputKind::Thumbnails => OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
        YtDlpOutputKind::Description => {
            if matches!(capture_kind, OutputCaptureKind::Folder) {
                OUTPUT_SANDBOX_ARTIFACTS
            } else {
                OUTPUT_YT_DLP_DESCRIPTION_FILE
            }
        }
        YtDlpOutputKind::Annotation => {
            if matches!(capture_kind, OutputCaptureKind::Folder) {
                OUTPUT_SANDBOX_ARTIFACTS
            } else {
                OUTPUT_YT_DLP_ANNOTATION_FILE
            }
        }
        YtDlpOutputKind::Archive => OUTPUT_YT_DLP_ARCHIVE_FILE,
        YtDlpOutputKind::Infojson | YtDlpOutputKind::Comment => {
            if matches!(capture_kind, OutputCaptureKind::Folder) {
                OUTPUT_SANDBOX_ARTIFACTS
            } else {
                OUTPUT_YT_DLP_INFOJSON_FILE
            }
        }
        YtDlpOutputKind::Links => OUTPUT_YT_DLP_LINK_ARTIFACTS,
        YtDlpOutputKind::Chapters => OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
        YtDlpOutputKind::PlaylistDescription => OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE,
        YtDlpOutputKind::PlaylistInfojson => OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE,
    }
}

/// Builds conductor output-policy overrides for one resolved output variant.
pub(super) fn step_output_policy_overrides(
    tool: MediaStepTool,
    output_variants: &BTreeMap<String, Value>,
    output_variant: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> Result<BTreeMap<String, OutputPolicy>, MediaPmError> {
    let options = output_variants
        .get(output_variant)
        .map(|value| decode_output_variant_policy(tool, output_variant, value))
        .transpose()
        .map_err(MediaPmError::Workflow)?
        .ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "missing output variant '{output_variant}' while resolving output policy"
            ))
        })?;
    let output_binding =
        resolve_step_output_binding(tool, output_variants, output_variant, ffmpeg_slot_limits)?;

    let policy = OutputPolicy { save: conductor_output_save_mode(options.save) };

    Ok(BTreeMap::from([(output_binding.output_name, policy)]))
}
