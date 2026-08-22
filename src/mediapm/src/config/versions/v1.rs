//! V1 wire envelopes and migration definitions.
//!
//! This module provides the V1-specific deserialization envelopes and the
//! `Migrate` implementation that transforms V1 JSON into the current
//! runtime document model (`MediaPmDocument`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::super::{
    MediaPmDocument, MediaPmState, MediaRuntimeStorageLatest, MediaRuntimeStorageV1,
    ToolRequirement, hierarchy_types, source_types,
};
use super::Migrate;

use serde_json::Value;

// ---------------------------------------------------------------------------
// V1 wire envelopes
// ---------------------------------------------------------------------------

/// V1 deserialization envelope for `mediapm.ncl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MediaPmDocumentEnvelopeV1 {
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
    pub(super) runtime: MediaRuntimeStorageV1,
    /// Legacy `state` payload accepted on V1 documents.
    ///
    /// Dropped when unifying into the runtime model; state is managed
    /// separately via `state.json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) state: Option<MediaPmState>,
}

// ---------------------------------------------------------------------------
// FromWire impls (V1 wire → runtime model)
// ---------------------------------------------------------------------------

impl From<MediaPmDocumentEnvelopeV1> for MediaPmDocument {
    fn from(envelope: MediaPmDocumentEnvelopeV1) -> Self {
        Self {
            version: envelope.version,
            media: envelope.media,
            hierarchy: envelope.hierarchy,
            tools: envelope.tools,
            runtime: MediaRuntimeStorageLatest::from(envelope.runtime),
            // The legacy `state` payload is dropped when unifying into the
            // runtime model: state is managed separately via `state.json`.
            state: None,
        }
    }
}

impl From<&MediaPmDocument> for MediaPmDocumentEnvelopeV1 {
    fn from(doc: &MediaPmDocument) -> Self {
        Self {
            version: 1,
            media: doc.media.clone(),
            hierarchy: doc.hierarchy.clone(),
            tools: doc.tools.clone(),
            runtime: MediaRuntimeStorageV1::from(doc.runtime.clone()),
            state: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Migrate implementation
// ---------------------------------------------------------------------------

pub(super) fn mediapm_document_v1_iso() -> &'static str {
    "mediapm_document_v1_iso"
}

impl Migrate for MediaPmDocumentEnvelopeV1 {
    fn version() -> u32 {
        1
    }

    fn decode(value: Value) -> Result<Self, crate::error::MediaPmError> {
        serde_json::from_value(value).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to decode V1 document envelope: {err}"
            ))
        })
    }

    fn encode(&self) -> Result<Value, crate::error::MediaPmError> {
        serde_json::to_value(self).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to encode V1 document envelope: {err}"
            ))
        })
    }
}
