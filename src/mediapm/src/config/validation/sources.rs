//! Cross-field validation for media source spec entries.
//!
//! Validates media-source structural invariants that cannot be expressed
//! directly in Serde deserialization attributes: step ordering, variant
//! continuity, tool-specific constraints, and metadata consistency.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::MediaPmError;

use super::super::source_types::MediaSourceSpec;

/// Validates all media-source entries in one document.
pub fn validate_sources(
    media: &BTreeMap<String, MediaSourceSpec>,
    available_variants: &BTreeSet<String>,
) -> Result<(), MediaPmError> {
    for (media_id, source) in media {
        validate_source(source, media_id, available_variants)?;
    }
    Ok(())
}

/// Validates one media-source entry.
fn validate_source(
    source: &MediaSourceSpec,
    media_id: &str,
    _available_variants: &BTreeSet<String>,
) -> Result<(), MediaPmError> {
    if let Some(ref id) = source.id {
        return Err(MediaPmError::InvalidSource(format!(
            "media source '{media_id}' may not define 'id' field (found '{id}'); use hierarchy ids instead"
        )));
    }

    if source.steps.is_empty() {
        return Err(MediaPmError::InvalidSource(format!(
            "media source '{media_id}' must define at least one processing step"
        )));
    }

    // Step 1 must be a source-ingest tool.
    if !source.steps[0].tool.is_source_ingest_tool() {
        return Err(MediaPmError::InvalidSource(format!(
            "media source '{media_id}': first step must be a source-ingest tool \
             (import or yt-dlp), found '{}'",
            source.steps[0].tool.as_str()
        )));
    }

    // Non-first steps must not be source-ingest tools.
    for (index, step) in source.steps.iter().enumerate().skip(1) {
        if step.tool.is_source_ingest_tool() {
            return Err(MediaPmError::InvalidSource(format!(
                "media source '{media_id}': step #{index} is '{}', \
                 but source-ingest tools may only appear as the first step",
                step.tool.as_str()
            )));
        }
    }

    // Validate variant continuity and slot limits.
    let mut produced_variants: BTreeSet<String> = BTreeSet::new();
    for (step_index, step) in source.steps.iter().enumerate() {
        // Step input variants must have been produced by a previous step or
        // match the source's `variant_hashes` seeds.
        for input in &step.input_variants {
            if !produced_variants.contains(input) && !source.variant_hashes.contains_key(input) {
                return Err(MediaPmError::InvalidSource(format!(
                    "media source '{media_id}': step #{step_index} consumes \
                     unknown input variant '{input}' (not produced by any \
                     prior step and not seeded in variant_hashes)"
                )));
            }
        }

        // Track produced variant names.
        for output in step.output_variants.keys() {
            produced_variants.insert(output.clone());
        }
    }

    // TODO: Add tool-specific constraints:
    // - ffmpeg slot counts must not exceed `tools.ffmpeg` limits.
    // - yt-dlp output variants must target known yt-dlp output kinds.
    // - media-tagger output variants must reference known media-tagger slots.

    Ok(())
}
