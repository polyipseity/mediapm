//! yt-dlp output variant decoding and input-binding resolution.
//!
//! Provides decoding helpers for yt-dlp specific output variant configs and
//! step-output binding resolution across workflow steps.

use std::collections::BTreeMap;

use crate::config::{DecodedOutputVariantConfig, OutputCaptureKind, YtDlpOutputKind};
use crate::error::MediaPmError;

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
) -> Result<DecodedOutputVariantConfig, MediaPmError> {
    DecodedOutputVariantConfig::from_json_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode yt-dlp variant: {e}")))
}

/// Step output binding pointing to another step's output by name and optional
/// zip member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepOutputBinding {
    /// Logical output name targeted by this binding.
    pub output_name: String,
    /// Optional zip-member path inside a folder output.
    pub zip_member: Option<String>,
}

/// Resolves one step output binding from the next step's input consumption
/// pattern and the current step's variant configuration.
///
/// This is used to wire downstream step inputs to upstream outputs.
#[must_use]
pub(crate) fn resolve_step_output_binding(
    upstream_variants: &BTreeMap<String, DecodedOutputVariantConfig>,
    _target_input_key: &str,
) -> Option<StepOutputBinding> {
    // Simplified resolution: pick the first folder or primary variant output.
    for (name, config) in upstream_variants {
        let is_folder = match config {
            DecodedOutputVariantConfig::Generic(g) => {
                matches!(g.capture_kind, Some(OutputCaptureKind::Folder))
            }
            DecodedOutputVariantConfig::YtDlp(y) => matches!(
                y.kind,
                YtDlpOutputKind::Subtitles
                    | YtDlpOutputKind::Thumbnails
                    | YtDlpOutputKind::Chapters
                    | YtDlpOutputKind::Links
            ),
        };
        if name == "primary" {
            return Some(StepOutputBinding { output_name: name.clone(), zip_member: None });
        }
        if is_folder {
            return Some(StepOutputBinding { output_name: name.clone(), zip_member: None });
        }
    }
    None
}
