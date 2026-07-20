//! Wire-format serialization for [`MediaPmState`].
//!
//! All persisted state uses JSON with a `version` discriminator. V1 → V2
//! migration is handled transparently on read. Writes always produce V2.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{ManagedFileRecord, ManagedWorkflowStepState, MediaPmState, ToolRegistryEntry};
use crate::error::MediaPmError;

// ---------------------------------------------------------------------------
// V2 wire envelope (current)
// ---------------------------------------------------------------------------

/// V2 wire representation of [`MediaPmState`].
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MediaPmStateV2 {
    /// Schema version marker (always 2).
    version: u32,
    /// Managed files keyed by filesystem path.
    #[serde(default)]
    managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Managed tool deployment metadata keyed by tool id.
    #[serde(default)]
    managed_tools: BTreeMap<String, ToolRegistryEntry>,
    /// Workflow step states keyed by media id.
    #[serde(default)]
    workflow_states: BTreeMap<String, ManagedWorkflowStepState>,
}

// ---------------------------------------------------------------------------
// V1 wire envelopes (migration only)
// ---------------------------------------------------------------------------

/// V1 state envelope wrapper (old Nickel-sourced format with `state` key).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct MediaPmStateV1Envelope {
    /// Schema version marker.
    version: u32,
    /// Nested state payload.
    state: MediaPmStateV1Payload,
}

/// V1 state payload (inside the `state` key).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct MediaPmStateV1Payload {
    /// Per-media-source workflow states.
    #[serde(default)]
    managed_files: BTreeMap<String, ManagedFileRecordV1>,
    /// Tool registry entries.
    #[serde(default)]
    tool_registry: BTreeMap<String, ToolRegistryRecordV1>,
    /// Active tool deployments.
    #[serde(default)]
    active_tools: BTreeMap<String, String>,
    /// Workflow step states keyed by media id.
    #[serde(default)]
    workflow_states: BTreeMap<String, Vec<ManagedWorkflowStepStateV1>>,
    /// Hash of last materialized state (dropped in V2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_materialized_state_hash: Option<String>,
}

/// V1 managed file record (same fields as current `ManagedFileRecord`).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct ManagedFileRecordV1 {
    /// Media source id.
    media_id: String,
    /// Output variant name.
    variant: String,
    /// Content hash (blake3:...).
    hash: String,
}

/// V1 tool registry entry.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct ToolRegistryRecordV1 {
    /// Tool name.
    name: String,
    /// Tool version.
    version: String,
    /// Tool source.
    source: String,
    /// Registry multihash.
    registry_multihash: String,
    /// Unix-epoch seconds of last transition.
    last_transition_unix_seconds: u64,
}

/// V1 managed workflow step state.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct ManagedWorkflowStepStateV1 {
    /// Pre-seeded CAS hash pointers keyed by variant name.
    #[serde(default)]
    variant_hashes: BTreeMap<String, String>,
    /// Number of completed steps.
    #[serde(default)]
    steps_completed: u32,
    /// Optional last impure sync timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_impure_sync_at: Option<MediaPmImpureTimestampV1>,
}

/// V1 impure sync timestamp.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct MediaPmImpureTimestampV1 {
    /// Seconds since Unix epoch.
    utc_epoch_seconds: u64,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Decodes one [`Value`] (from JSON deserialization) into a
/// [`MediaPmState`], handling version dispatch.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the version is unsupported, or
/// [`MediaPmError::Serialization`] if deserialization fails.
pub fn from_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    let version = value
        .get("version")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| MediaPmError::Workflow("missing or invalid 'version' field".to_string()))?;

    match version {
        1 => from_v1_json_value(value),
        2 => from_v2_json_value(value),
        v => Err(MediaPmError::Workflow(format!("unsupported mediapm state schema version {v}",))),
    }
}

/// Encodes one [`MediaPmState`] into a [`Value`] (V2 format).
///
/// Always produces V2 output regardless of input version.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] if serialization fails.
pub fn to_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    let v2 = MediaPmStateV2 {
        version: 2,
        managed_files: state.managed_files.clone(),
        managed_tools: state.managed_tools.clone(),
        workflow_states: state.workflow_states.clone(),
    };

    serde_json::to_value(v2)
        .map_err(|e| MediaPmError::Serialization(format!("failed to serialize state to JSON: {e}")))
}

/// Migrates one [`Value`] from old Nickel format (both V1-envelope and
/// flat post-rewrite) into a [`MediaPmState`].
///
/// Accepts two formats:
/// - V1 wrapper: `{ "version": 1, "state": { ... } }`
/// - Flat post-rewrite: `{ "version": 1, "workflow_states": { ... }, "managed_files": [...], ... }`
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the format is unrecognized, or
/// [`MediaPmError::Serialization`] if deserialization fails.
pub fn migrate_from_old_nickel(value: Value) -> Result<MediaPmState, MediaPmError> {
    // Check for V1 wrapper format (has a nested "state" key).
    if value.get("state").is_some() {
        let envelope: MediaPmStateV1Envelope = serde_json::from_value(value).map_err(|e| {
            MediaPmError::Serialization(format!("failed to decode V1 state envelope: {e}"))
        })?;
        return from_v1_payload(envelope.state);
    }

    // Flat post-rewrite format (no "state" key, has "version").
    let version = value
        .get("version")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| MediaPmError::Workflow("missing 'version' field".to_string()))?;

    match version {
        1 => {
            // Flat V1: has workflow_states directly, plus managed_files as
            // BTreeSet<String>, tool_registry, active_tools, etc.
            // Convert managed_files from BTreeSet to BTreeMap.
            let managed_files_set: BTreeSet<String> =
                serde_json::from_value(value.get("managed_files").cloned().unwrap_or_default())
                    .map_err(|e| {
                        MediaPmError::Serialization(format!(
                            "failed to decode V1 managed_files: {e}"
                        ))
                    })?;

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

            // Deserialize workflow_states directly (already at BTreeMap<String, ManagedWorkflowStepState>).
            let workflow_states: BTreeMap<String, ManagedWorkflowStepState> =
                serde_json::from_value(value.get("workflow_states").cloned().unwrap_or_default())
                    .map_err(|e| {
                    MediaPmError::Serialization(format!("failed to decode V1 workflow_states: {e}"))
                })?;

            Ok(MediaPmState {
                version: crate::config::defaults::MEDIAPM_STATE_VERSION,
                managed_files,
                managed_tools: BTreeMap::new(),
                workflow_states,
            })
        }
        2 => from_v2_json_value(value),
        v => Err(MediaPmError::Workflow(format!("unsupported mediapm state schema version {v}",))),
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Deserializes a V2 JSON value directly into [`MediaPmState`].
fn from_v2_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    let v2: MediaPmStateV2 = serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V2 state: {e}")))?;

    Ok(MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files: v2.managed_files,
        managed_tools: v2.managed_tools,
        workflow_states: v2.workflow_states,
    })
}

/// Deserializes a V1 JSON value (flat envelope) into [`MediaPmState`].
fn from_v1_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    // Flat V1 with managed_files as BTreeSet, tool_registry, active_tools,
    // workflow_states, last_materialized_state_hash.
    // First try the V1Payload format.
    let payload: MediaPmStateV1Payload = serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V1 state: {e}")))?;

    from_v1_payload(payload)
}

/// Converts a [`MediaPmStateV1Payload`] into [`MediaPmState`].
fn from_v1_payload(payload: MediaPmStateV1Payload) -> Result<MediaPmState, MediaPmError> {
    // Convert managed_files from record to map (same key-value mapping).
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

    // Convert workflow_states from Vec<T> to T (take last entry in each Vec).
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
                        crate::config::MediaPmImpureTimestamp {
                            utc_epoch_seconds: ts.utc_epoch_seconds,
                        }
                    }),
                }
            };
            (key, state)
        })
        .collect();

    // Drop: tool_registry, active_tools, last_materialized_state_hash.
    Ok(MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files,
        managed_tools: BTreeMap::new(),
        workflow_states,
    })
}
