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
use crate::state::{AuxData, HashedValueRecord, InstanceAux, OrchestrationState, ToolCallInstance};

mod v1;
mod v2;

pub(crate) use v2::derive_instance_key_v2;

// ---------------------------------------------------------------------------
// Bridge: V2 wire types ↔ runtime types
// ---------------------------------------------------------------------------

impl From<v2::AuxDataV2> for AuxData {
    fn from(aux: v2::AuxDataV2) -> Self {
        Self {
            tool_call_instance_counter: aux.tool_call_instance_counter,
            conductor_gc_epoch: mediapm_utils::Timestamp::from_unix_nanos(aux.conductor_gc_epoch.0),
            instances: aux.instances.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<AuxData> for v2::AuxDataV2 {
    fn from(aux: AuxData) -> Self {
        Self {
            tool_call_instance_counter: aux.tool_call_instance_counter,
            conductor_gc_epoch: v2::ImpureTimestampV2(aux.conductor_gc_epoch.as_unix_nanos()),
            instances: aux.instances.into_iter().map(|(k, v)| (k, v.into())).collect(),
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

impl From<v2::HashedValueRecordV2> for HashedValueRecord {
    fn from(record: v2::HashedValueRecordV2) -> Self {
        Self { hash: record.hash, deterministic: record.deterministic }
    }
}

impl From<HashedValueRecord> for v2::HashedValueRecordV2 {
    fn from(record: HashedValueRecord) -> Self {
        Self { hash: record.hash, deterministic: record.deterministic }
    }
}

impl From<v2::InstanceAuxV2> for InstanceAux {
    fn from(aux: v2::InstanceAuxV2) -> Self {
        Self {
            save_modes: aux.save_modes.into_iter().map(|(k, v)| (k, v.into())).collect(),
            last_referenced_at: mediapm_utils::Timestamp::from_unix_nanos(aux.last_referenced_at.0),
        }
    }
}

impl From<InstanceAux> for v2::InstanceAuxV2 {
    fn from(aux: InstanceAux) -> Self {
        Self {
            save_modes: aux.save_modes.into_iter().map(|(k, v)| (k, v.into())).collect(),
            last_referenced_at: v2::ImpureTimestampV2(aux.last_referenced_at.as_unix_nanos()),
        }
    }
}

impl From<v2::ToolCallInstanceV2> for ToolCallInstance {
    fn from(inst: v2::ToolCallInstanceV2) -> Self {
        Self {
            instance_key: inst.instance_key,
            tool_call_id: inst.tool_call_id,
            impure: inst.impure,
            executed_at: mediapm_utils::Timestamp::from_unix_nanos(inst.executed_at.0),
            command_args: inst.command_args.into_iter().map(Into::into).collect(),
            env_vars: inst.env_vars.into_iter().map(|(k, v)| (k, v.into())).collect(),
            materialized_inputs: inst
                .materialized_inputs
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            outputs: inst.outputs.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<ToolCallInstance> for v2::ToolCallInstanceV2 {
    fn from(inst: ToolCallInstance) -> Self {
        Self {
            instance_key: inst.instance_key,
            tool_call_id: inst.tool_call_id,
            impure: inst.impure,
            executed_at: v2::ImpureTimestampV2(inst.executed_at.as_unix_nanos()),
            command_args: inst.command_args.into_iter().map(Into::into).collect(),
            env_vars: inst.env_vars.into_iter().map(|(k, v)| (k, v.into())).collect(),
            materialized_inputs: inst
                .materialized_inputs
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            outputs: inst.outputs.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<v2::ConductorStateV2> for OrchestrationState {
    fn from(state: v2::ConductorStateV2) -> Self {
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

impl From<OrchestrationState> for v2::ConductorStateV2 {
    fn from(state: OrchestrationState) -> Self {
        Self {
            version: v2::CONDUCTOR_STATE_VERSION_V2,
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

    if v2::is_conductor_state_version_v2(version) {
        // Deserialise through the V2 wire type so the version boundary is
        // explicit, then bridge to the runtime representation.
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v2_state.into())
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} (expected {})",
            v2::CONDUCTOR_STATE_VERSION_V2
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
    let v2_state: v2::ConductorStateV2 = state.clone().into();
    serde_json::to_vec_pretty(&v2_state).map_err(|e| ConductorError::Serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::Hash;
    use mediapm_utils::Timestamp;

    use super::*;
    use crate::state::{OutputSaveMode, STATE_VERSION};

    fn sample_state() -> OrchestrationState {
        let key = Hash::from_content(b"key");
        let instance = ToolCallInstance {
            instance_key: key,
            tool_call_id: "echo@v1".to_string(),
            impure: false,
            executed_at: Timestamp::from_unix_nanos(5),
            command_args: vec![HashedValueRecord {
                hash: Hash::from_content(b"arg"),
                deterministic: true,
            }],
            env_vars: BTreeMap::from([(
                "FOO".to_string(),
                HashedValueRecord { hash: Hash::from_content(b"val"), deterministic: false },
            )]),
            materialized_inputs: BTreeMap::from([(
                "bin/tool".to_string(),
                HashedValueRecord { hash: Hash::from_content(b"payload"), deterministic: true },
            )]),
            outputs: BTreeMap::from([(
                "stdout".to_string(),
                HashedValueRecord { hash: Hash::from_content(b"out"), deterministic: true },
            )]),
        };
        OrchestrationState {
            version: STATE_VERSION,
            tool_call_instances: BTreeMap::from([(key, instance)]),
            aux: AuxData {
                tool_call_instance_counter: 3,
                conductor_gc_epoch: Timestamp::from_unix_nanos(9),
                instances: BTreeMap::from([(
                    key,
                    InstanceAux {
                        save_modes: BTreeMap::from([("stdout".to_string(), OutputSaveMode::Saved)]),
                        last_referenced_at: Timestamp::from_unix_nanos(42),
                    },
                )]),
            },
        }
    }

    /// `instance_key` and `aux.instances` keyed by `Hash` survive the
    /// encode/decode round-trip, including `save_modes` + `last_referenced_at`.
    #[test]
    fn state_json_round_trip_hash_keys_and_aux() {
        let state = sample_state();
        let encoded = encode_state_json(&state).unwrap();
        let decoded = decode_state_json(&encoded).unwrap();
        assert_eq!(decoded, state);
    }

    /// The instance wire record carries no `version` field (fact 26) — only
    /// the envelope has one. The `instance_key` hash round-trips as the map key.
    #[test]
    fn instance_json_has_no_version_field() {
        let encoded = encode_state_json(&sample_state()).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["version"], serde_json::json!(2));
        let instances = json["tool_call_instances"].as_object().unwrap();
        let instance = instances.values().next().unwrap();
        assert!(
            instance.get("version").is_none(),
            "ToolCallInstance must not serialize a version field"
        );
        assert!(instance.get("instance_key").is_some());
    }
}
