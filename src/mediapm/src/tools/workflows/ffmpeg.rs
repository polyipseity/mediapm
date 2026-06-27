//! Ffmpeg workflow step synthesis.
//!
//! Produces the conductor workflow steps for one `ffmpeg` media transform step.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;

use mediapm_conductor::WorkflowStepSpec;

use crate::config::{DecodedOutputVariantConfig, MediaSourceSpec, MediaStep};
use crate::error::MediaPmError;

use super::{
    OUTPUT_PRIMARY, qualify_step_id, resolve_step_tool_id, step_option_input_bindings,
    variant_to_output_capture_spec,
};

/// Synthesizes one or more ffmpeg workflow steps from a media step definition.
///
/// # Errors
///
/// Returns [`MediaPmError`] when required configuration is missing or invalid.
pub(crate) fn synthesize_ffmpeg_step(
    source: &MediaSourceSpec,
    step_index: usize,
    step: &MediaStep,
) -> Result<Vec<WorkflowStepSpec>, MediaPmError> {
    let step_id =
        qualify_step_id(source.id.as_deref().unwrap_or("unknown"), &format!("ffmpeg_{step_index}"));

    let mut inputs = BTreeMap::new();
    inputs.insert(
        "source_url".to_string(),
        step_option_input_bindings(step)
            .into_iter()
            .find(|(k, _)| k == "source_url")
            .map(|(_, v)| v)
            .unwrap_or_default(),
    );

    let mut outputs = BTreeMap::new();
    for (name, variant_json) in &step.output_variants {
        if let Ok(config) = DecodedOutputVariantConfig::from_json_value(variant_json.clone()) {
            outputs.insert(name.clone(), variant_to_output_capture_spec(name, &config));
        }
    }
    if outputs.is_empty() {
        outputs.insert(
            OUTPUT_PRIMARY.to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: OUTPUT_PRIMARY.to_string(),
                capture: "file:output.*".to_string(),
                save: true,
            },
        );
    }

    Ok(vec![WorkflowStepSpec {
        id: step_id,
        tool: resolve_step_tool_id(crate::config::MediaStepTool::Ffmpeg),
        inputs,
        outputs,
        max_retries: 0,
        depends_on: Vec::new(),
    }])
}
