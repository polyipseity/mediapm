//! V2 wire format for state persistence.
//!
//! V2 is the current stable format, always used for writes. V1 inputs are
//! migrated to V2 on read.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{ManagedFileRecord, ManagedWorkflowStepState, MediaPmState, ToolRegistryEntry};
use crate::error::MediaPmError;

#[allow(dead_code)]
/// V2 wire representation of [`MediaPmState`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MediaPmStateV2 {
    /// Schema version marker (always 2).
    pub(super) version: u32,
    /// Managed files keyed by filesystem path.
    #[serde(default)]
    pub(super) managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Managed tool deployment metadata keyed by tool id.
    #[serde(default)]
    pub(super) managed_tools: BTreeMap<String, ToolRegistryEntry>,
    /// Workflow step states keyed by media id.
    #[serde(default)]
    pub(super) workflow_states: BTreeMap<String, ManagedWorkflowStepState>,
}

#[allow(dead_code)]
/// Encodes a [`MediaPmState`] as a V2 JSON [`Value`] (backward compat).
///
/// Converts the native Vec-based managed_tools into a BTreeMap keyed by
/// tool_id for consumers expecting the V2 format.
pub(crate) fn to_v2_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    let managed_tools: BTreeMap<String, ToolRegistryEntry> =
        state.managed_tools.iter().map(|entry| (entry.tool_id.clone(), entry.clone())).collect();
    let v2 = MediaPmStateV2 {
        version: 2,
        managed_files: state.managed_files.clone(),
        managed_tools,
        workflow_states: state.workflow_states.clone(),
    };

    serde_json::to_value(v2)
        .map_err(|e| MediaPmError::Serialization(format!("failed to serialize state to JSON: {e}")))
}

#[allow(dead_code)]
/// Decodes a V2 JSON [`Value`] into [`MediaPmState`].
///
/// # Note
/// This function is retained for backward compatibility but is no longer
/// called by the version dispatch in [`super::super::ser`]. V2 state data
/// is now bridged through [`super::v3::from_v2_into_v3`] which correctly
/// handles the old wire format (no `tool_id` field, `Option<String>`
/// content_map_hash).
pub(crate) fn from_v2_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    let v2: MediaPmStateV2 = serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V2 state: {e}")))?;

    let managed_tools: Vec<ToolRegistryEntry> = v2
        .managed_tools
        .into_iter()
        .map(|(tool_id, entry)| ToolRegistryEntry { tool_id, ..entry })
        .collect();

    Ok(MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files: v2.managed_files,
        managed_tools,
        workflow_states: v2.workflow_states,
    })
}
