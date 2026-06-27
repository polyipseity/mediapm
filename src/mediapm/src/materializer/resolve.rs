//! Variant hash/bytes resolution and media source lookup.
//!
//! Provides functions to resolve variant content hashes from media source
//! config (including default fallback), fetch full content bytes from CAS,
//! look up a media source by hierarchy media-id reference, and enumerate
//! available variant names for a source.

use std::collections::BTreeSet;

use mediapm_cas::{CasApi, Hash};

use crate::config::MediaPmDocument;
use crate::config::hierarchy_types::HierarchyEntry;
use crate::config::source_types::MediaSourceSpec;
use crate::error::MediaPmError;

use super::metadata::MaterializationLookupContext;

// ---------------------------------------------------------------------------
// Variant hash resolution
// ---------------------------------------------------------------------------

/// Resolves the CAS hash for one variant from a media source.
///
/// Tries an exact variant-name match first, then falls back to `"default"`
/// when the requested name is not `"default"` itself. Returns `Ok(None)` when
/// no matching hash is found.
pub(super) async fn resolve_variant_hash(
    media_id: &str,
    variant_name: &str,
    source: &MediaSourceSpec,
    _lookup_context: &MaterializationLookupContext,
) -> Result<Option<Hash>, MediaPmError> {
    // Exact variant-name match.
    if let Some(hash_str) = source.variant_hashes.get(variant_name) {
        let hash: Hash = hash_str.parse().map_err(|e| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' variant '{variant_name}' hash '{hash_str}' is invalid: {e}"
            ))
        })?;
        return Ok(Some(hash));
    }

    // Fallback to "default" variant.
    if variant_name != "default"
        && let Some(hash_str) = source.variant_hashes.get("default")
    {
        let hash: Hash = hash_str.parse().map_err(|e| {
            MediaPmError::Workflow(format!(
                "media '{media_id}' default variant hash '{hash_str}' is invalid: {e}"
            ))
        })?;
        return Ok(Some(hash));
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// Variant bytes resolution
// ---------------------------------------------------------------------------

/// Fetches the full content bytes for one variant from the CAS store.
///
/// Returns `Ok(None)` when no hash is available for the variant. Returns
/// `Err` on CAS read failures or hash parse errors.
pub(super) async fn resolve_variant_bytes(
    media_id: &str,
    variant_name: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
) -> Result<Option<Vec<u8>>, MediaPmError> {
    let hash = resolve_variant_hash(media_id, variant_name, source, lookup_context).await?;

    match hash {
        Some(hash) => {
            let bytes = lookup_context.cas.get(hash).await.map_err(|e| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' variant '{variant_name}': CAS get({hash}) failed: {e}"
                ))
            })?;
            Ok(Some(bytes.to_vec()))
        }
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Hierarchy source resolution
// ---------------------------------------------------------------------------

/// Resolves the [`MediaSourceSpec`] for one hierarchy entry's media id.
pub(super) fn resolve_hierarchy_source<'a>(
    document: &'a MediaPmDocument,
    entry: &HierarchyEntry,
) -> Result<&'a MediaSourceSpec, MediaPmError> {
    document.media.get(&entry.media_id).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "hierarchy references unknown media id '{}'",
            entry.media_id
        ))
    })
}

// ---------------------------------------------------------------------------
// Available variant enumeration
// ---------------------------------------------------------------------------

/// Collects all available variant names for one media source.
///
/// Combines keys from `variant_hashes` and `output_variants` across all
/// workflow steps.
pub(super) fn collect_media_source_available_variants(
    source: &MediaSourceSpec,
) -> BTreeSet<String> {
    let mut variants = BTreeSet::new();

    for variant_name in source.variant_hashes.keys() {
        variants.insert(variant_name.clone());
    }

    for step in &source.steps {
        for variant_name in step.output_variants.keys() {
            variants.insert(variant_name.clone());
        }
    }

    variants
}
