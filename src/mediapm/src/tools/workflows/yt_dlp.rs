//! yt-dlp workflow step synthesis.
//!
//! Produces the conductor workflow step for one `yt-dlp` media download step.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;

use mediapm_conductor::WorkflowStepSpec;

use crate::config::{DecodedOutputVariantConfig, MediaSourceSpec, MediaStep};
use crate::error::MediaPmError;

use super::{
    OUTPUT_PRIMARY, qualify_step_id, resolve_step_tool_id, source_uri_input,
    step_option_input_bindings, variant_to_output_capture_spec,
};

/// Synthesizes the yt-dlp workflow step from a media step definition.
///
/// Configures standard inputs (source_url, format, subtitles, etc.),
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
) -> Result<Vec<WorkflowStepSpec>, MediaPmError> {
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

    Ok(vec![WorkflowStepSpec {
        id: step_id,
        tool: resolve_step_tool_id(crate::config::MediaStepTool::YtDlp),
        inputs,
        outputs,
        max_retries: 1,
        depends_on: Vec::new(),
    }])
}
