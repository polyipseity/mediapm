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

use std::collections::BTreeMap;

use mediapm_cas::{CasApi, Hash};

use crate::error::ConductorError;
use crate::state::{OrchestrationState, ToolCallInstance};

mod v1;
mod v2;

/// Returns the latest supported orchestration-state schema marker.
#[must_use]
pub(crate) const fn latest_state_version() -> u32 {
    v2::ORCHESTRATION_STATE_VERSION_V2
}

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
    #[allow(clippy::cast_possible_truncation)]
    fn from(aux: crate::state::AuxData) -> Self {
        Self {
            tool_call_instance_counter: aux.tool_call_instance_counter,
            conductor_gc_epoch: v2::ImpureTimestampV2(aux.conductor_gc_epoch.as_unix_nanos() as u64),
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
    #[allow(clippy::cast_possible_truncation)]
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
                inst.conductor_gc_last_referenced_at.as_unix_nanos() as u64,
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

// ---------------------------------------------------------------------------
// CAS-based encoding / decoding (kept for backward compatibility with old
// persisted state; currently unused — new state is stored as inline JSON)
// ---------------------------------------------------------------------------

/// Encodes runtime orchestration state using CAS-backed version 1 format.
///
/// Each instance is individually encoded and stored in CAS. The envelope
/// (containing instance refs) is also stored in CAS. Returns the envelope hash.
pub(crate) async fn encode_state<C: CasApi>(
    cas: &C,
    mut state: OrchestrationState,
) -> Result<Hash, ConductorError> {
    state.version = v1::ORCHESTRATION_STATE_VERSION_V1;

    let mut instance_refs = BTreeMap::new();
    for (key, instance) in state.tool_call_instances {
        let encoded = serde_json::to_vec(&instance)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        let hash = cas.put(encoded.into()).await?;
        instance_refs.insert(key, v1::InstanceRefV1 { hash });
    }

    let envelope = v1::OrchestrationStateEnvelopeV1 {
        version: v1::ORCHESTRATION_STATE_VERSION_V1,
        instances: instance_refs,
        aux: state.aux,
    };

    let envelope_bytes =
        serde_json::to_vec(&envelope).map_err(|e| ConductorError::Serialization(e.to_string()))?;
    let envelope_hash = cas.put(envelope_bytes.into()).await?;
    Ok(envelope_hash)
}

/// Decodes orchestration state from a CAS envelope hash.
///
/// Supports v1 (CAS-backed, migrated through V2) format. V2 (JSON inline)
/// state should use [`decode_state_json`] instead.
pub(crate) async fn decode_state<C: CasApi>(
    cas: &C,
    envelope_hash: &Hash,
) -> Result<OrchestrationState, ConductorError> {
    let envelope_bytes = cas.get(*envelope_hash).await?;

    // Peek the version marker.
    let version = peek_version_marker(&envelope_bytes)?;

    if v1::is_orchestration_state_version_v1(version) {
        let envelope: v1::OrchestrationStateEnvelopeV1 = serde_json::from_slice(&envelope_bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        let v2_state = v2::migrate_v1_to_v2(cas, envelope).await?;
        Ok(v2_state.into())
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported CAS orchestration state version: {version}"
        )))
    }
}

/// Extracts the numeric `version` field from a JSON blob.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn peek_version_marker(bytes: &[u8]) -> Result<u32, ConductorError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| ConductorError::Serialization(e.to_string()))?;
    let marker = value.get("version").and_then(serde_json::Value::as_u64).ok_or_else(|| {
        ConductorError::Serialization(
            "missing or non-numeric 'version' field in state JSON".to_string(),
        )
    })?;
    Ok(marker as u32)
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
