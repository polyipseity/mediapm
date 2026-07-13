//! Provider source definitions for `media-tagger`.
//!
//! `media-tagger` is an internal launcher tool shipped with mediapm
//! itself, not an external download. The sources list is intentionally
//! empty to signal that the tool does not go through the fetch/postprocess
//! pipeline. The lifecycle module handles launcher file generation
//! separately.

use mediapm_conductor::tools::provider::ResolvedToolFetch;

/// Returns an empty resolved fetch, signalling no external provisioning.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch { tool_id: "media-tagger".to_string(), sources: vec![], total_items: 0 }
}
