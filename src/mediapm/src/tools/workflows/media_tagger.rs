//! Media-tagger workflow step synthesis.
//!
//! Produces the conductor workflow steps for one `media-tagger` metadata step.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;

use mediapm_conductor::WorkflowStepSpec;

use crate::config::{MediaSourceSpec, MediaStep};
use crate::error::MediaPmError;

use super::{
    OUTPUT_PRIMARY, qualify_step_id, resolve_step_tool_id, source_uri_input,
    step_option_input_bindings, variant_to_output_capture_spec,
};

/// Synthesizes one media-tagger workflow step from a media step definition.
///
/// # Errors
///
/// Returns [`MediaPmError`] when required configuration is missing or invalid.
pub(crate) fn synthesize_media_tagger_step(
    source: &MediaSourceSpec,
    step_index: usize,
    step: &MediaStep,
) -> Result<Vec<WorkflowStepSpec>, MediaPmError> {
    let step_id = qualify_step_id(
        source.id.as_deref().unwrap_or("unknown"),
        &format!("media_tagger_{step_index}"),
    );

    let mut inputs = BTreeMap::from([source_uri_input(source)]);
    for (k, v) in step_option_input_bindings(step) {
        inputs.insert(k, v);
    }

    let mut outputs = BTreeMap::new();
    for (name, variant_json) in &step.output_variants {
        if let Ok(config) =
            crate::config::DecodedOutputVariantConfig::from_json_value(variant_json.clone())
        {
            outputs.insert(name.clone(), variant_to_output_capture_spec(name, &config));
        }
    }
    if outputs.is_empty() {
        outputs.insert(
            OUTPUT_PRIMARY.to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: OUTPUT_PRIMARY.to_string(),
                capture: "file:tagged.*".to_string(),
                save: true,
            },
        );
    }

    Ok(vec![WorkflowStepSpec {
        id: step_id,
        tool: resolve_step_tool_id(crate::config::MediaStepTool::MediaTagger),
        inputs,
        outputs,
        max_retries: 0,
        depends_on: Vec::new(),
    }])
}
