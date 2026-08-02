//! V2 wire envelopes and migration definitions.
//!
//! This module provides the V2-specific deserialization envelopes and the
//! `Migrate` implementation that transforms V2 JSON into the current
//! runtime document model (`MediaPmDocument`).  V2 is the active schema: it
//! deliberately drops the legacy `state` payload, which is managed
//! separately via `state.json`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::super::{
    MediaPmDocument, MediaRuntimeStorage, ToolRequirement, hierarchy_types, source_types,
};
use super::Migrate;

use serde_json::Value;

// ---------------------------------------------------------------------------
// V2 wire envelopes
// ---------------------------------------------------------------------------

/// V2 deserialization envelope for `mediapm.ncl`.
///
/// Deliberately omits the legacy `state` payload: state is managed separately
/// via `state.json`, so V2 documents never carry it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MediaPmDocumentEnvelopeV2 {
    /// Schema version marker.
    pub(super) version: u32,
    /// Media source registry entries keyed by id.
    #[serde(default)]
    pub(super) media: BTreeMap<String, source_types::MediaSourceSpec>,
    /// Hierarchy node declarations.
    #[serde(default)]
    pub(super) hierarchy: Vec<hierarchy_types::HierarchyNode>,
    /// Managed tool requirement declarations keyed by tool id.
    #[serde(default)]
    pub(super) tools: BTreeMap<String, ToolRequirement>,
    /// Runtime configuration overrides.
    #[serde(default)]
    pub(super) runtime: MediaRuntimeStorage,
}

// ---------------------------------------------------------------------------
// FromWire impls (V2 wire ↔ runtime model)
// ---------------------------------------------------------------------------

impl From<MediaPmDocumentEnvelopeV2> for MediaPmDocument {
    fn from(envelope: MediaPmDocumentEnvelopeV2) -> Self {
        Self {
            version: envelope.version,
            media: envelope.media,
            hierarchy: envelope.hierarchy,
            tools: envelope.tools,
            runtime: envelope.runtime,
            state: None,
        }
    }
}

impl From<&MediaPmDocument> for MediaPmDocumentEnvelopeV2 {
    fn from(doc: &MediaPmDocument) -> Self {
        Self {
            version: 2,
            media: doc.media.clone(),
            hierarchy: doc.hierarchy.clone(),
            tools: doc.tools.clone(),
            runtime: doc.runtime.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Migrate implementation
// ---------------------------------------------------------------------------

pub(super) fn mediapm_document_v2_iso() -> &'static str {
    "mediapm_document_v2_iso"
}

impl Migrate for MediaPmDocumentEnvelopeV2 {
    fn version() -> u32 {
        2
    }

    fn decode(value: Value) -> Result<Self, crate::error::MediaPmError> {
        serde_json::from_value(value).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to decode V2 document envelope: {err}"
            ))
        })
    }

    fn encode(&self) -> Result<Value, crate::error::MediaPmError> {
        serde_json::to_value(self).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to encode V2 document envelope: {err}"
            ))
        })
    }
}
