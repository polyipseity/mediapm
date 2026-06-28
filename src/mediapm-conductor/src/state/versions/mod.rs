//! Versioned persistence envelopes for orchestration state.
//!
//! ## Versioning policy
//!
//! - Each `v<N>.rs` file owns one wire-format version.
//! - Version modules must **never** directly import unversioned runtime state
//!   structs from `super::super` (the parent `state` module).
//! - This `mod.rs` is the **only** bridge between the latest wire version and
//!   unversioned runtime state.
//! - Consumers outside `state/versions/` must use only the APIs
//!   re-exported from this module, never `versions::v<N>` directly.

use crate::error::ConductorError;
use crate::state::{OrchestrationState, ToolCallInstance};

mod v1;
mod v2;

// ---------------------------------------------------------------------------
// Bridge: V2 wire types ↔ runtime types
// ---------------------------------------------------------------------------

impl From<v2::AuxDataV2> for crate::state::AuxData {
    fn from(aux: v2::AuxDataV2) -> Self {
        Self {
            tool_call_instance_counter: aux.tool_call_instance_counter,
            conductor_gc_epoch: crate::config::ImpureTimestamp::from_unix_nanos(
                aux.conductor_gc_epoch.0.into(),
            ),
        }
    }
}

impl From<crate::state::AuxData> for v2::AuxDataV2 {
    fn from(aux: crate::state::AuxData) -> Self {
        Self {
            tool_call_instance_counter: aux.tool_call_instance_counter,
            conductor_gc_epoch: v2::ImpureTimestampV2(
                u64::try_from(aux.conductor_gc_epoch.as_unix_nanos()).unwrap_or(u64::MAX),
            ),
        }
    }
}

impl From<v2::OutputSaveModeV2> for crate::state::OutputSaveMode {
    fn from(mode: v2::OutputSaveModeV2) -> Self {
        match mode {
            v2::OutputSaveModeV2::Unsaved => Self::Unsaved,
            v2::OutputSaveModeV2::Saved => Self::Saved,
            v2::OutputSaveModeV2::Full => Self::Full,
        }
    }
}

impl From<crate::state::OutputSaveMode> for v2::OutputSaveModeV2 {
    fn from(mode: crate::state::OutputSaveMode) -> Self {
        match mode {
            crate::state::OutputSaveMode::Unsaved => Self::Unsaved,
            crate::state::OutputSaveMode::Saved => Self::Saved,
            crate::state::OutputSaveMode::Full => Self::Full,
        }
    }
}

impl From<v2::ResolvedInputV2> for crate::state::ResolvedInput {
    fn from(input: v2::ResolvedInputV2) -> Self {
        Self { key: input.key, value: input.value }
    }
}

impl From<crate::state::ResolvedInput> for v2::ResolvedInputV2 {
    fn from(input: crate::state::ResolvedInput) -> Self {
        Self { key: input.key, value: input.value }
    }
}

impl From<v2::OutputRefV2> for crate::state::OutputRef {
    fn from(out: v2::OutputRefV2) -> Self {
        Self { name: out.name, hash: out.hash, save_mode: out.save_mode.into() }
    }
}

impl From<crate::state::OutputRef> for v2::OutputRefV2 {
    fn from(out: crate::state::OutputRef) -> Self {
        Self { name: out.name, hash: out.hash, save_mode: out.save_mode.into() }
    }
}

impl From<v2::ToolCallInstanceV2> for ToolCallInstance {
    fn from(inst: v2::ToolCallInstanceV2) -> Self {
        Self {
            instance_key: inst.instance_key,
            tool_id: inst.tool_id,
            inputs: inst.inputs.into_iter().map(Into::into).collect(),
            outputs: inst.outputs.into_iter().map(Into::into).collect(),
            worker_index: inst.worker_index,
            executed: inst.executed,
            rematerialized: inst.rematerialized,
            conductor_gc_last_referenced_at: crate::config::ImpureTimestamp::from_unix_nanos(
                inst.conductor_gc_last_referenced_at.0.into(),
            ),
        }
    }
}

impl From<ToolCallInstance> for v2::ToolCallInstanceV2 {
    fn from(inst: ToolCallInstance) -> Self {
        Self {
            instance_key: inst.instance_key,
            tool_id: inst.tool_id,
            inputs: inst.inputs.into_iter().map(Into::into).collect(),
            outputs: inst.outputs.into_iter().map(Into::into).collect(),
            worker_index: inst.worker_index,
            executed: inst.executed,
            rematerialized: inst.rematerialized,
            conductor_gc_last_referenced_at: v2::ImpureTimestampV2(
                u64::try_from(inst.conductor_gc_last_referenced_at.as_unix_nanos())
                    .unwrap_or(u64::MAX),
            ),
        }
    }
}

impl From<v2::OrchestrationStateV2> for OrchestrationState {
    fn from(state: v2::OrchestrationStateV2) -> Self {
        Self {
            version: state.version,
            tool_call_instances: state
                .tool_call_instances
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            aux: state.aux.into(),
        }
    }
}

impl From<OrchestrationState> for v2::OrchestrationStateV2 {
    fn from(state: OrchestrationState) -> Self {
        Self {
            version: v2::ORCHESTRATION_STATE_VERSION_V2,
            tool_call_instances: state
                .tool_call_instances
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            aux: state.aux.into(),
        }
    }
}

/// Extracts the numeric `version` field from a JSON blob.
pub(crate) fn peek_version_marker(bytes: &[u8]) -> Result<u32, ConductorError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| ConductorError::Serialization(e.to_string()))?;
    let marker = value.get("version").and_then(serde_json::Value::as_u64).ok_or_else(|| {
        ConductorError::Serialization(
            "missing or non-numeric 'version' field in state JSON".to_string(),
        )
    })?;
    u32::try_from(marker).map_err(|_| {
        ConductorError::Serialization(format!("version marker {marker} exceeds u32 range"))
    })
}

// ---------------------------------------------------------------------------
// JSON-based helpers (v2 — current plain-JSON persistence path)
// ---------------------------------------------------------------------------

/// Decodes an orchestration state from inline JSON bytes, checking that the
/// version marker matches the latest schema.
///
/// # Errors
///
/// Returns an error if the version is unsupported. Unknown/missing versions
/// are reported with a clear message.
pub fn decode_state_json(bytes: &[u8]) -> Result<OrchestrationState, ConductorError> {
    // Peek the version marker first.
    let version = peek_version_marker(bytes)?;

    if v2::is_orchestration_state_version_v2(version) {
        // Deserialise through the V2 wire type so the version boundary is
        // explicit, then bridge to the runtime representation.
        let v2_state: v2::OrchestrationStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v2_state.into())
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} (expected {})",
            v2::ORCHESTRATION_STATE_VERSION_V2
        )))
    }
}

/// Encodes an orchestration state as pretty JSON, ensuring the version marker
/// matches the latest schema.
///
/// # Errors
///
/// Returns an error if serialization to JSON fails.
pub fn encode_state_json(state: &OrchestrationState) -> Result<Vec<u8>, ConductorError> {
    // Route through V2 wire type for explicit version boundary.
    let v2_state: v2::OrchestrationStateV2 = state.clone().into();
    serde_json::to_vec_pretty(&v2_state).map_err(|e| ConductorError::Serialization(e.to_string()))
}
