//! V1 wire formats for state migration.
//!
//! Both the pre-rewrite wrapper format (`state` key with nested payload) and
//! the post-rewrite flat format are handled here. V1 is never written by the
//! current code — these types exist solely for migration-on-read.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`
//!   beyond the resolved runtime model it is declared to bridge.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.
//! - Files outside `versions/` must reach versioned symbols only through
//!   `versions/mod.rs`, never through a direct `versions::vX` path.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

use crate::config::{ManagedFileRecord, ManagedWorkflowStepState, MediaPmState};
use crate::error::MediaPmError;

/// V1 state envelope wrapper (old Nickel-sourced format with `state` key).
#[derive(Debug, Clone, Deserialize)]
pub(super) struct MediaPmStateV1Envelope {
    /// Schema version marker. Accepted so the wrapper parses; the V1 reader
    /// dispatches on the caller's extracted marker, so the value is not read.
    #[expect(
        dead_code,
        reason = "accept-and-discard: the version marker is read by versions/mod.rs dispatch before the envelope is decoded, so re-reading it here would duplicate the check"
    )]
    pub(super) version: u32,
    /// Nested state payload.
    pub(super) state: MediaPmStateV1Payload,
}

/// V1 state payload (inside the `state` key, or directly at top level for
/// flat map format).
///
/// `tool_registry`, `active_tools`, and `last_materialized_state_hash` are
/// accepted-and-discarded: V2 dropped all three, and no runtime state is
/// derived from them. They are destructured once, by name, in
/// [`from_v1_payload`] so the discard is stated in code rather than left
/// implicit in a partial move.
#[derive(Debug, Clone, Deserialize)]
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
pub(super) struct ManagedFileRecordV1 {
    /// Media source id.
    pub(super) media_id: String,
    /// Output variant name.
    pub(super) variant: String,
    /// Content hash (blake3:...).
    pub(super) hash: String,
}

/// V1 tool registry entry (pre-rewrite format).
///
/// Every field is accepted so a V1 document parses, and every field is
/// discarded by the V1 → V2 migration: the V2 format replaced `tool_registry`
/// with a `managed_tools` map whose provenance this format does not carry.
/// The value is read once, by name, in [`from_v1_payload`] purely to make that
/// discard explicit and compiler-checked, never to produce output.
#[derive(Debug, Clone, Deserialize)]
#[expect(
    dead_code,
    reason = "accept-and-discard: these fields exist so a V1 document deserializes, and the V1→V2 migration drops tool_registry wholesale (V2 replaced it with a managed_tools map this format cannot populate). No runtime state is derived from them."
)]
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
pub(super) struct MediaPmImpureTimestampV1 {
    /// Seconds since Unix epoch.
    pub(super) utc_epoch_seconds: u64,
}

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
        return Ok(from_v1_payload(envelope.state));
    }

    // Flat format: try map-style (managed_files as BTreeMap) first.
    if let Ok(payload) = serde_json::from_value::<MediaPmStateV1Payload>(value.clone()) {
        return Ok(from_v1_payload(payload));
    }

    // Fall back to set-style (managed_files as BTreeSet).
    migrate_flat_v1_fields(&value)
}

/// Converts a [`MediaPmStateV1Payload`] into [`MediaPmState`].
fn from_v1_payload(payload: MediaPmStateV1Payload) -> MediaPmState {
    // Destructure in full: the three fields V2 dropped are bound here so the
    // discard is explicit and the compiler proves the struct is fully
    // accounted for. If a future version ever needs one, this is the place it
    // stops being dropped. The bindings are unused by construction — the
    // names carry the intent.
    let MediaPmStateV1Payload {
        managed_files,
        tool_registry: _discarded_tool_registry,
        active_tools: _discarded_active_tools,
        workflow_states,
        last_materialized_state_hash: _discarded_materialized_state_hash,
    } = payload;

    // Map managed files (same key-value structure).
    let managed_files: BTreeMap<String, ManagedFileRecord> = managed_files
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
    let workflow_states: BTreeMap<String, ManagedWorkflowStepState> = workflow_states
        .into_iter()
        .map(|(key, mut vec)| {
            let state = if vec.is_empty() {
                ManagedWorkflowStepState::default()
            } else {
                let last = vec.remove(vec.len() - 1);
                ManagedWorkflowStepState {
                    variant_hashes: last.variant_hashes,
                    steps_completed: last.steps_completed,
                    last_impure_sync_at: last
                        .last_impure_sync_at
                        .map(|ts| mediapm_utils::Timestamp::from_unix_secs(ts.utc_epoch_seconds)),
                }
            };
            (key, state)
        })
        .collect();

    MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files,
        managed_tools: Vec::new(),
        workflow_states,
    }
}

/// Migrates a flat V1 value (post-rewrite set format) into [`MediaPmState`].
///
/// The flat set format has `managed_files` as `BTreeSet<String>` and
/// `workflow_states` directly at the top level.
fn migrate_flat_v1_fields(value: &Value) -> Result<MediaPmState, MediaPmError> {
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
        managed_tools: Vec::new(),
        workflow_states,
    })
}
