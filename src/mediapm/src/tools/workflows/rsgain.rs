//! Rsgain workflow step synthesis.
//!
//! Produces the conductor workflow steps for one `rsgain` loudness analysis step.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;

use mediapm_conductor::WorkflowStepSpec;

use crate::config::{MediaSourceSpec, MediaStep};

use super::{
    OUTPUT_PRIMARY, qualify_step_id, resolve_step_tool_id, step_option_input_bindings,
    variant_to_output_capture_spec,
};

/// Synthesizes one rsgain workflow step (or step chain) from a media step
/// definition.
///
/// For album-mode rsgain, returns two steps (scan + tag). For single-track
/// mode, returns one step.
///
/// # Errors
///
#[must_use]
pub(crate) fn synthesize_rsgain_step_chain(
    source: &MediaSourceSpec,
    step_index: usize,
    step: &MediaStep,
) -> Vec<WorkflowStepSpec> {
    let mut steps = Vec::new();

    let step_id =
        qualify_step_id(source.id.as_deref().unwrap_or("unknown"), &format!("rsgain_{step_index}"));

    let mut inputs = BTreeMap::new();
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
                capture: "file:loudness.*".to_string(),
                save: true,
            },
        );
    }

    steps.push(WorkflowStepSpec {
        id: step_id,
        tool: resolve_step_tool_id(crate::config::MediaStepTool::Rsgain),
        inputs,
        outputs,
        max_retries: 0,
        depends_on: Vec::new(),
    });

    steps
}
