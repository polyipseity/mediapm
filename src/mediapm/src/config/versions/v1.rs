//! V1 wire envelopes and migration definitions.
//!
//! This module provides the V1-specific deserialization envelopes and the
//! `Migrate` implementation that transforms V1 JSON into the current
//! runtime document model (`MediaPmDocument`).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::super::{
    ActiveToolInstance, ManagedWorkflowStepState, MediaPmDocument, MediaPmImpureTimestamp,
    MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement, hierarchy_types,
    source_types,
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
    /// Runtime configuration overrides.
    #[serde(default)]
    pub(super) runtime: MediaRuntimeStorage,
    /// Conductor config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) conductor: Option<BTreeMap<String, Value>>,
}

/// V1 state envelope for `state.ncl` files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MediaPmStateEnvelopeV1 {
    /// Schema version marker.
    pub(super) version: u32,
    /// Per-media-source state entries.
    #[serde(default)]
    pub(super) media_state: BTreeMap<String, MediaPmStateWireV1>,
    /// Per-tool registry state.
    #[serde(default)]
    pub(super) tools: BTreeMap<String, ToolRegistryStateWireV1>,
    /// Hash of the state snapshot at last materialization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) last_materialized_state_hash: Option<String>,
    /// Set of files currently managed.
    #[serde(default)]
    pub(super) managed_files: BTreeSet<String>,
    /// Fetched-tool registry.
    #[serde(default)]
    pub(super) tool_registry: BTreeMap<String, ToolRegistryEntryWireV1>,
    /// Active tool deployments.
    #[serde(default)]
    pub(super) active_tools: BTreeMap<String, ActiveToolInstanceWireV1>,
}

// ---------------------------------------------------------------------------
// V1 wire state type
// ---------------------------------------------------------------------------

/// V1 wire representation of one media-source state entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MediaPmStateWireV1 {
    /// Optional pre-seeded CAS hash pointers keyed by variant name.
    #[serde(default)]
    pub(super) variant_hashes: BTreeMap<String, String>,
    /// Optional number of completed steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) steps_completed: Option<u32>,
    /// Optional last impure sync timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) last_impure_sync_at: Option<MediaPmImpureTimestampWireV1>,
}

/// V1 wire representation of one tool registry entry state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ToolRegistryStateWireV1 {
    /// Optional tool version string (from version selector).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) version: Option<String>,
    /// Optional tool tag string (from tag selector).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) tag: Option<String>,
}

/// V1 wire representation of a tool registry entry (fetch/deployment metadata).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ToolRegistryEntryWireV1 {
    /// Tool version as fetched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) version: Option<String>,
    /// Tag as fetched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) tag: Option<String>,
    /// CAS content hash of the fetched payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) fetch_hash: Option<String>,
    /// Unix-epoch seconds when the payload was deployed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) deployed_at: Option<u64>,
}

/// V1 wire representation of an active tool instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ActiveToolInstanceWireV1 {
    /// Tool identifier.
    pub(super) tool_id: String,
    /// CAS content hash.
    pub(super) content_hash: String,
    /// Filesystem path.
    pub(super) deployed_path: String,
}

/// V1 wire representation of an impure sync timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MediaPmImpureTimestampWireV1 {
    /// The `utc_epoch_seconds` field.
    pub(super) utc_epoch_seconds: u64,
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
            runtime: envelope.runtime,
            conductor: envelope.conductor.unwrap_or_default(),
        }
    }
}

impl From<MediaPmStateEnvelopeV1> for MediaPmState {
    fn from(envelope: MediaPmStateEnvelopeV1) -> Self {
        MediaPmState {
            media: envelope.media_state.into_iter().map(|(key, wire)| (key, wire.into())).collect(),
            tools: envelope.tools.into_iter().map(|(key, wire)| (key, wire.into())).collect(),
            last_materialized_state_hash: envelope.last_materialized_state_hash,
            managed_files: envelope.managed_files,
            tool_registry: envelope
                .tool_registry
                .into_iter()
                .map(|(key, wire)| (key, wire.into()))
                .collect(),
            active_tools: envelope
                .active_tools
                .into_iter()
                .map(|(key, wire)| (key, wire.into()))
                .collect(),
            ..Default::default()
        }
    }
}

impl From<MediaPmStateWireV1> for ManagedWorkflowStepState {
    fn from(wire: MediaPmStateWireV1) -> Self {
        Self {
            variant_hashes: wire.variant_hashes,
            steps_completed: wire.steps_completed,
            last_impure_sync_at: wire
                .last_impure_sync_at
                .map(|ts| MediaPmImpureTimestamp { utc_epoch_seconds: ts.utc_epoch_seconds }),
        }
    }
}

impl From<ToolRegistryStateWireV1> for ToolRequirement {
    fn from(wire: ToolRegistryStateWireV1) -> Self {
        Self {
            version: wire.version.clone().map(source_types::MediaMetadataValue::Literal),
            tag: wire.tag.clone(),
            ..ToolRequirement::default()
        }
    }
}

impl From<ToolRegistryEntryWireV1> for ToolRegistryEntry {
    fn from(wire: ToolRegistryEntryWireV1) -> Self {
        Self {
            version: wire.version,
            tag: wire.tag,
            fetch_hash: wire.fetch_hash,
            deployed_at: wire.deployed_at,
        }
    }
}

impl From<ActiveToolInstanceWireV1> for ActiveToolInstance {
    fn from(wire: ActiveToolInstanceWireV1) -> Self {
        Self {
            tool_id: wire.tool_id,
            content_hash: wire.content_hash,
            deployed_path: wire.deployed_path,
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
            runtime: self.runtime.clone(),
            conductor: if self.conductor.is_empty() { None } else { Some(self.conductor.clone()) },
        };

        serde_json::to_value(envelope).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to encode V1 document envelope: {err}"
            ))
        })
    }
}

impl Migrate for MediaPmState {
    fn version() -> u32 {
        1
    }

    fn decode(value: Value) -> Result<Self, crate::error::MediaPmError> {
        let envelope: MediaPmStateEnvelopeV1 = serde_json::from_value(value).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to decode V1 state envelope: {err}"
            ))
        })?;

        Ok(MediaPmState::from(envelope))
    }

    fn encode(&self) -> Result<Value, crate::error::MediaPmError> {
        let media_state: BTreeMap<String, MediaPmStateWireV1> = self
            .media
            .iter()
            .map(|(key, state)| {
                (
                    key.clone(),
                    MediaPmStateWireV1 {
                        variant_hashes: state.variant_hashes.clone(),
                        steps_completed: state.steps_completed,
                        last_impure_sync_at: state.last_impure_sync_at.as_ref().map(|ts| {
                            MediaPmImpureTimestampWireV1 { utc_epoch_seconds: ts.utc_epoch_seconds }
                        }),
                    },
                )
            })
            .collect();

        let tools: BTreeMap<String, ToolRegistryStateWireV1> = self
            .tools
            .iter()
            .map(|(key, tool_req)| {
                (
                    key.clone(),
                    ToolRegistryStateWireV1 {
                        version: tool_req.normalized_version(),
                        tag: tool_req.normalized_tag(),
                    },
                )
            })
            .collect();

        let tool_registry: BTreeMap<String, ToolRegistryEntryWireV1> = self
            .tool_registry
            .iter()
            .map(|(key, entry)| {
                (
                    key.clone(),
                    ToolRegistryEntryWireV1 {
                        version: entry.version.clone(),
                        tag: entry.tag.clone(),
                        fetch_hash: entry.fetch_hash.clone(),
                        deployed_at: entry.deployed_at,
                    },
                )
            })
            .collect();

        let active_tools: BTreeMap<String, ActiveToolInstanceWireV1> = self
            .active_tools
            .iter()
            .map(|(key, instance)| {
                (
                    key.clone(),
                    ActiveToolInstanceWireV1 {
                        tool_id: instance.tool_id.clone(),
                        content_hash: instance.content_hash.clone(),
                        deployed_path: instance.deployed_path.clone(),
                    },
                )
            })
            .collect();

        let envelope = MediaPmStateEnvelopeV1 {
            version: 1,
            media_state,
            tools,
            last_materialized_state_hash: self.last_materialized_state_hash.clone(),
            managed_files: self.managed_files.clone(),
            tool_registry,
            active_tools,
        };

        serde_json::to_value(envelope).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to encode V1 state envelope: {err}"
            ))
        })
    }
}
