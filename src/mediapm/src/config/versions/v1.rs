//! V1 wire envelopes and migration definitions.
//!
//! This module provides the V1-specific deserialization envelopes and the
//! `Migrate` implementation that transforms V1 JSON into the current
//! runtime document model (`MediaPmDocument`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::super::{
    MediaPmDocument, MediaRuntimeStorage, ToolRequirement, hierarchy_types, source_types,
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
    pub(super) runtime: MediaRuntimeStorage,
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
            runtime: envelope.runtime,
        }
    }
}

// ---------------------------------------------------------------------------
// Migrate implementation
// ---------------------------------------------------------------------------

pub(super) fn mediapm_document_v1_iso() -> &'static str {
    "mediapm_document_v1_iso"
}

impl Migrate for MediaPmDocument {
    fn version() -> u32 {
        1
    }

    fn decode(value: Value) -> Result<Self, crate::error::MediaPmError> {
        let envelope: MediaPmDocumentEnvelopeV1 = serde_json::from_value(value).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to decode V1 document envelope: {err}"
            ))
        })?;

        Ok(MediaPmDocument::from(envelope))
    }

    fn encode(&self) -> Result<Value, crate::error::MediaPmError> {
        let envelope = MediaPmDocumentEnvelopeV1 {
            version: 1,
            media: self.media.clone(),
            hierarchy: self.hierarchy.clone(),
            tools: self.tools.clone(),
            runtime: self.runtime.clone(),
        };

        serde_json::to_value(envelope).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to encode V1 document envelope: {err}"
            ))
        })
    }
}
