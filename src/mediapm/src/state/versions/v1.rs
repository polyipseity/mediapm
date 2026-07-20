//! V1 wire formats for state migration.
//!
//! Both the pre-rewrite wrapper format (`state` key with nested payload) and
//! the post-rewrite flat format are handled here. V1 is never written by the
//! current code — these types exist solely for migration-on-read.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

use crate::config::{
    ManagedFileRecord, ManagedWorkflowStepState, MediaPmImpureTimestamp, MediaPmState,
};
use crate::error::MediaPmError;

// ---------------------------------------------------------------------------
// V1 wire envelopes
// ---------------------------------------------------------------------------

/// V1 state envelope wrapper (old Nickel-sourced format with `state` key).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub(super) struct MediaPmStateV1Envelope {
    /// Schema version marker.
    pub(super) version: u32,
    /// Nested state payload.
    pub(super) state: MediaPmStateV1Payload,
}

/// V1 state payload (inside the `state` key, or directly at top level for
/// flat map format).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub(super) struct MediaPmStateV1Payload {
    /// Managed files as a path→record map.
    #[serde(default)]
    pub(super) managed_files: BTreeMap<String, ManagedFileRecordV1>,
    /// Tool registry entries.
    #[serde(default)]
    pub(super) tool_registry: BTreeMap<String, ToolRegistryRecordV1>,
    /// Active tool deployments (tool id → registry key).
    #[serde(default)]
    pub(super) active_tools: BTreeMap<String, String>,
    /// Workflow step states keyed by media id (each value is a history vec).
    #[serde(default)]
    pub(super) workflow_states: BTreeMap<String, Vec<ManagedWorkflowStepStateV1>>,
    /// Hash of last materialized state (dropped in V2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) last_materialized_state_hash: Option<String>,
}

/// V1 managed file record (same fields as [`ManagedFileRecord`]).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub(super) struct ManagedFileRecordV1 {
    /// Media source id.
    pub(super) media_id: String,
    /// Output variant name.
    pub(super) variant: String,
    /// Content hash (blake3:...).
    pub(super) hash: String,
}

/// V1 tool registry entry (pre-rewrite format).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub(super) struct ToolRegistryRecordV1 {
    /// Tool name.
    pub(super) name: String,
    /// Tool version.
    pub(super) version: String,
    /// Tool source.
    pub(super) source: String,
    /// Registry multihash.
    pub(super) registry_multihash: String,
    /// Unix-epoch seconds of last transition.
    pub(super) last_transition_unix_seconds: u64,
}

/// V1 managed workflow step state (with history vec).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub(super) struct ManagedWorkflowStepStateV1 {
    /// Pre-seeded CAS hash pointers keyed by variant name.
    #[serde(default)]
    pub(super) variant_hashes: BTreeMap<String, String>,
    /// Number of completed steps.
    #[serde(default)]
    pub(super) steps_completed: u32,
    /// Optional last impure sync timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) last_impure_sync_at: Option<MediaPmImpureTimestampV1>,
}

/// V1 impure sync timestamp.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub(super) struct MediaPmImpureTimestampV1 {
    /// Seconds since Unix epoch.
    pub(super) utc_epoch_seconds: u64,
}

// ---------------------------------------------------------------------------
// V1 → current migration
// ---------------------------------------------------------------------------

/// Converts a V1 JSON value (any known V1 shape) into [`MediaPmState`].
///
/// Accepts:
/// - Wrapper format: `{ "version": 1, "state": { ... } }`
/// - Flat map format: `{ "version": 1, "managed_files": { "path": {...} }, ... }`
/// - Flat set format: `{ "version": 1, "managed_files": ["path", ...], ... }`
pub(crate) fn from_v1_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    // Wrapper format (has a nested "state" key).
    if value.get("state").is_some() {
        let envelope: MediaPmStateV1Envelope = serde_json::from_value(value).map_err(|e| {
            MediaPmError::Serialization(format!("failed to decode V1 state envelope: {e}"))
        })?;
        return from_v1_payload(envelope.state);
    }

    // Flat format: try map-style (managed_files as BTreeMap) first.
    if let Ok(payload) = serde_json::from_value::<MediaPmStateV1Payload>(value.clone()) {
        return from_v1_payload(payload);
    }

    // Fall back to set-style (managed_files as BTreeSet).
    migrate_flat_v1_fields(value)
}

/// Converts a [`MediaPmStateV1Payload`] into [`MediaPmState`].
fn from_v1_payload(payload: MediaPmStateV1Payload) -> Result<MediaPmState, MediaPmError> {
    // Map managed files (same key-value structure).
    let managed_files: BTreeMap<String, ManagedFileRecord> = payload
        .managed_files
        .into_iter()
        .map(|(key, record)| {
            (
                key,
                ManagedFileRecord {
                    media_id: record.media_id,
                    variant: record.variant,
                    hash: record.hash,
                },
            )
        })
        .collect();

    // Convert workflow_states from Vec<T> to T (take last entry per vec).
    let workflow_states: BTreeMap<String, ManagedWorkflowStepState> = payload
        .workflow_states
        .into_iter()
        .map(|(key, mut vec)| {
            let state = if vec.is_empty() {
                ManagedWorkflowStepState::default()
            } else {
                let last = vec.remove(vec.len() - 1);
                ManagedWorkflowStepState {
                    variant_hashes: last.variant_hashes,
                    steps_completed: last.steps_completed,
                    last_impure_sync_at: last.last_impure_sync_at.map(|ts| {
                        MediaPmImpureTimestamp { utc_epoch_seconds: ts.utc_epoch_seconds }
                    }),
                }
            };
            (key, state)
        })
        .collect();

    // tool_registry, active_tools, last_materialized_state_hash are dropped.
    Ok(MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files,
        managed_tools: BTreeMap::new(),
        workflow_states,
    })
}

/// Migrates a flat V1 value (post-rewrite set format) into [`MediaPmState`].
///
/// The flat set format has `managed_files` as `BTreeSet<String>` and
/// `workflow_states` directly at the top level.
fn migrate_flat_v1_fields(value: Value) -> Result<MediaPmState, MediaPmError> {
    let managed_files_set: BTreeSet<String> = serde_json::from_value(
        value.get("managed_files").cloned().unwrap_or_default(),
    )
    .map_err(|e| MediaPmError::Serialization(format!("failed to decode V1 managed_files: {e}")))?;

    let managed_files: BTreeMap<String, ManagedFileRecord> = managed_files_set
        .into_iter()
        .map(|path| {
            let record = ManagedFileRecord {
                media_id: String::new(),
                variant: String::new(),
                hash: path.clone(),
            };
            (path, record)
        })
        .collect();

    let workflow_states: BTreeMap<String, ManagedWorkflowStepState> =
        serde_json::from_value(value.get("workflow_states").cloned().unwrap_or_default()).map_err(
            |e| MediaPmError::Serialization(format!("failed to decode V1 workflow_states: {e}")),
        )?;

    Ok(MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files,
        managed_tools: BTreeMap::new(),
        workflow_states,
    })
}
