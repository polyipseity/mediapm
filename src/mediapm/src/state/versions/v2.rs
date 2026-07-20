//! V2 wire format for state persistence.
//!
//! V2 is the current stable format, always used for writes. V1 inputs are
//! migrated to V2 on read.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{ManagedFileRecord, ManagedWorkflowStepState, MediaPmState, ToolRegistryEntry};
use crate::error::MediaPmError;

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

/// Encodes a [`MediaPmState`] as a V2 JSON [`Value`].
pub(crate) fn to_v2_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    let v2 = MediaPmStateV2 {
        version: 2,
        managed_files: state.managed_files.clone(),
        managed_tools: state.managed_tools.clone(),
        workflow_states: state.workflow_states.clone(),
    };

    serde_json::to_value(v2)
        .map_err(|e| MediaPmError::Serialization(format!("failed to serialize state to JSON: {e}")))
}

/// Decodes a V2 JSON [`Value`] into [`MediaPmState`].
pub(crate) fn from_v2_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    let v2: MediaPmStateV2 = serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V2 state: {e}")))?;

    Ok(MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files: v2.managed_files,
        managed_tools: v2.managed_tools,
        workflow_states: v2.workflow_states,
    })
}
