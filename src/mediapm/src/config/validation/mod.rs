//! Cross-field document validation for mediapm configuration.
//!
//! Validates structural invariants that span multiple configuration sections:
//! media sources reference available variants, hierarchy entries reference
//! declared media ids, playlist items resolve to known hierarchy nodes, etc.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::MediaPmError;

mod hierarchy;
mod sources;

/// Runs all cross-field validation passes on one document.
///
/// MUST be called after deserialization and before any document values are
/// used for workflow generation.
pub fn validate_document(
    media: &BTreeMap<String, MediaSourceSpec>,
    hierarchy: &[HierarchyNode],
) -> Result<(), MediaPmError> {
    let known_media_ids: BTreeSet<String> = media.keys().cloned().collect();

    // Source-level validation.
    // Build a complete variant set from all sources for selector resolution.
    let mut all_variants = BTreeSet::new();
    for source in media.values() {
        for step in &source.steps {
            for variant in step.output_variants.keys() {
                all_variants.insert(variant.clone());
            }
        }
    }
    // Also include variant_hashes keys as "known variants" for source
    // continuity checks.
    for source in media.values() {
        for variant in source.variant_hashes.keys() {
            all_variants.insert(variant.clone());
        }
    }

    sources::validate_sources(media, &all_variants)?;

    // Hierarchy-level validation.
    hierarchy::validate_hierarchy(hierarchy, &known_media_ids)?;

    Ok(())
}

// Re-exports for sibling module access.
use super::hierarchy_types::HierarchyNode;
use super::source_types::MediaSourceSpec;
