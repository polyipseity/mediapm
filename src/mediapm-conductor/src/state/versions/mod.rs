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
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - Version dispatch lives here, not in any caller. Adding a version is a new
//!   `vN.rs` plus a new arm in the decoders below.
//! - Do not directly re-export `versions::vX` structs or paths as public API;
//!   expose unversioned functions and keep versioned internals encapsulated.
//! - Never delete a read path for a version that could exist in a CAS blob or
//!   a state JSON on disk. V1 and V2 are migration-only formats that stay
//!   readable; V3 is the current format.

use crate::error::ConductorError;
use crate::state::{
    AuxData, CapturedOutput, ConductorState, HashedValueRecord, InstanceAux, ToolCallInstance,
};
use mediapm_cas::CasApi;

mod v1;
mod v2;
mod v3;

// ---------------------------------------------------------------------------
// Bridge: V3 wire types ↔ runtime types
// ---------------------------------------------------------------------------

impl From<v3::SaveModeV3> for crate::config::SaveMode {
    fn from(mode: v3::SaveModeV3) -> Self {
        match mode {
            v3::SaveModeV3::False => Self::False,
            v3::SaveModeV3::True => Self::True,
            v3::SaveModeV3::Full => Self::Full,
        }
    }
}

impl From<crate::config::SaveMode> for v3::SaveModeV3 {
    fn from(mode: crate::config::SaveMode) -> Self {
        match mode {
            crate::config::SaveMode::False => Self::False,
            crate::config::SaveMode::True => Self::True,
            crate::config::SaveMode::Full => Self::Full,
        }
    }
}

impl From<v3::OutputCaptureSpecV3> for crate::config::OutputCaptureSpec {
    fn from(spec: v3::OutputCaptureSpecV3) -> Self {
        Self {
            name: spec.name,
            capture: spec.capture,
            save: spec.save.into(),
            allow_empty: spec.allow_empty,
            include_topmost_folder: spec.include_topmost_folder,
        }
    }
}

impl From<crate::config::OutputCaptureSpec> for v3::OutputCaptureSpecV3 {
    fn from(spec: crate::config::OutputCaptureSpec) -> Self {
        Self {
            name: spec.name,
            capture: spec.capture,
            save: spec.save.into(),
            allow_empty: spec.allow_empty,
            include_topmost_folder: spec.include_topmost_folder,
        }
    }
}

impl From<v3::CapturedOutputV3> for CapturedOutput {
    fn from(output: v3::CapturedOutputV3) -> Self {
        Self { hash: output.hash, capture: output.capture.map(Into::into) }
    }
}

impl From<CapturedOutput> for v3::CapturedOutputV3 {
    fn from(output: CapturedOutput) -> Self {
        Self { hash: output.hash, capture: output.capture.map(Into::into) }
    }
}

impl From<v3::HashedValueRecordV3> for HashedValueRecord {
    fn from(record: v3::HashedValueRecordV3) -> Self {
        Self { hash: record.hash, deterministic: record.deterministic }
    }
}

impl From<HashedValueRecord> for v3::HashedValueRecordV3 {
    fn from(record: HashedValueRecord) -> Self {
        Self { hash: record.hash, deterministic: record.deterministic }
    }
}

impl From<v3::InstanceAuxV3> for InstanceAux {
    fn from(aux: v3::InstanceAuxV3) -> Self {
        Self {
            last_referenced_at: mediapm_utils::Timestamp::from_unix_nanos(aux.last_referenced_at.0),
        }
    }
}

impl From<InstanceAux> for v3::InstanceAuxV3 {
    fn from(aux: InstanceAux) -> Self {
        Self { last_referenced_at: v3::ImpureTimestampV3(aux.last_referenced_at.as_unix_nanos()) }
    }
}

impl From<v3::ToolCallInstanceV3> for ToolCallInstance {
    fn from(inst: v3::ToolCallInstanceV3) -> Self {
        Self {
            instance_key: inst.instance_key,
            tool_call_id: inst.tool_call_id,
            impure: inst.impure,
            executed_at: mediapm_utils::Timestamp::from_unix_nanos(inst.executed_at.0),
            step_ids: inst.step_ids,
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

impl From<ToolCallInstance> for v3::ToolCallInstanceV3 {
    fn from(inst: ToolCallInstance) -> Self {
        Self {
            instance_key: inst.instance_key,
            tool_call_id: inst.tool_call_id,
            impure: inst.impure,
            executed_at: v3::ImpureTimestampV3(inst.executed_at.as_unix_nanos()),
            step_ids: inst.step_ids,
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

impl From<v3::AuxDataV3> for AuxData {
    fn from(aux: v3::AuxDataV3) -> Self {
        Self {
            tool_call_instance_counter: aux.tool_call_instance_counter,
            conductor_gc_epoch: mediapm_utils::Timestamp::from_unix_nanos(aux.conductor_gc_epoch.0),
            instances: aux.instances.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<AuxData> for v3::AuxDataV3 {
    fn from(aux: AuxData) -> Self {
        Self {
            tool_call_instance_counter: aux.tool_call_instance_counter,
            conductor_gc_epoch: v3::ImpureTimestampV3(aux.conductor_gc_epoch.as_unix_nanos()),
            instances: aux.instances.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<v3::ConductorStateV3> for ConductorState {
    fn from(state: v3::ConductorStateV3) -> Self {
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

impl From<ConductorState> for v3::ConductorStateV3 {
    fn from(state: ConductorState) -> Self {
        Self {
            version: v3::CONDUCTOR_STATE_VERSION_V3,
            tool_call_instances: state
                .tool_call_instances
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            aux: state.aux.into(),
        }
    }
}

/// Returns whether `marker` is the v1 orchestration-state wire version.
pub(crate) fn is_orchestration_state_version_v1(marker: u32) -> bool {
    v1::is_orchestration_state_version_v1(marker)
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
// JSON-based helpers (v3 — current plain-JSON persistence path)
// ---------------------------------------------------------------------------

/// Decodes an orchestration state from inline JSON bytes, checking that the
/// version marker matches the latest schema.
///
/// # Errors
///
/// Returns an error if the version is unsupported. Unknown/missing versions
/// are reported with a clear message.
pub fn decode_state_json(bytes: &[u8]) -> Result<ConductorState, ConductorError> {
    let version = peek_version_marker(bytes)?;

    if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v3_state.into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v3::from_v2_into_v3(v2_state).into())
    } else if v1::is_orchestration_state_version_v1(version) {
        Err(ConductorError::Serialization(format!(
            "orchestration state version {version} requires CAS-backed migration; \
             use decode_state_json_with_cas"
        )))
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} \
             (supported: {}, {}, {})",
            v3::CONDUCTOR_STATE_VERSION_V3,
            v2::CONDUCTOR_STATE_VERSION_V2,
            v1::ORCHESTRATION_STATE_VERSION_V1
        )))
    }
}

/// Decodes persisted state JSON, migrating v1 CAS-referenced envelopes when
/// needed.
///
/// # Errors
///
/// Returns an error when the version is unsupported or migration fails.
pub async fn decode_state_json_with_cas<C: CasApi>(
    cas: &C,
    bytes: &[u8],
) -> Result<ConductorState, ConductorError> {
    let version = peek_version_marker(bytes)?;

    if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v3_state.into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v3::from_v2_into_v3(v2_state).into())
    } else if v1::is_orchestration_state_version_v1(version) {
        let envelope: v1::OrchestrationStateEnvelopeV1 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        let v2_state = v2::migrate_v1_to_v2(cas, envelope).await?;
        Ok(v3::from_v2_into_v3(v2_state).into())
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} \
             (supported: {}, {}, {})",
            v3::CONDUCTOR_STATE_VERSION_V3,
            v2::CONDUCTOR_STATE_VERSION_V2,
            v1::ORCHESTRATION_STATE_VERSION_V1
        )))
    }
}

/// Encodes an orchestration state as pretty JSON with the latest-schema
/// version marker.
///
/// # Errors
///
/// Returns an error if serialization to JSON fails.
pub fn encode_state_json(state: &ConductorState) -> Result<Vec<u8>, ConductorError> {
    let v3_state: v3::ConductorStateV3 = state.clone().into();
    serde_json::to_vec_pretty(&v3_state).map_err(|e| ConductorError::Serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mediapm_cas::Hash;
    use mediapm_utils::Timestamp;

    use super::*;
    use crate::config::{OutputCaptureSpec, SaveMode};
    use crate::state::{CapturedOutput, STATE_VERSION};

    /// A capture spec whose every field differs from the defaults, so a bridge
    /// that dropped or re-defaulted one of them would change the value.
    fn sample_spec() -> OutputCaptureSpec {
        OutputCaptureSpec {
            name: "stdout".to_string(),
            capture: "stdout".to_string(),
            save: SaveMode::False,
            allow_empty: true,
            include_topmost_folder: false,
        }
    }

    fn sample_state() -> ConductorState {
        let key = Hash::from_content(b"key");
        let instance = ToolCallInstance {
            instance_key: key,
            tool_call_id: "echo@v1".to_string(),
            impure: false,
            executed_at: Timestamp::from_unix_nanos(5),
            step_ids: BTreeSet::from(["step-0-echo".to_string()]),
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
                CapturedOutput { hash: Hash::from_content(b"out"), capture: Some(sample_spec()) },
            )]),
        };
        ConductorState {
            version: STATE_VERSION,
            tool_call_instances: BTreeMap::from([(key, instance)]),
            aux: AuxData {
                tool_call_instance_counter: 3,
                conductor_gc_epoch: Timestamp::from_unix_nanos(9),
                instances: BTreeMap::from([(
                    key,
                    InstanceAux { last_referenced_at: Timestamp::from_unix_nanos(42) },
                )]),
            },
        }
    }

    /// `instance_key`, `aux.instances` keyed by `Hash`, and each captured
    /// entry's capture spec survive the encode/decode bridge.
    ///
    /// The spec is what a reusing step compares its declaration against, so a
    /// bridge that dropped it would make every recorded name look unknown and
    /// every reuse a miss — a silent, permanent re-execution rather than a
    /// visible failure.
    #[test]
    fn state_json_round_trip_preserves_capture_specs_and_aux() {
        let state = sample_state();
        let encoded = encode_state_json(&state).unwrap();
        let decoded = decode_state_json(&encoded).unwrap();
        assert_eq!(decoded, state);
    }

    /// The encoded envelope declares the current schema version, so a reader
    /// can dispatch on it without guessing from the field layout.
    #[test]
    fn encoded_state_declares_the_current_version() {
        let encoded = encode_state_json(&sample_state()).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["version"], serde_json::json!(STATE_VERSION));
        assert_eq!(json["version"], serde_json::json!(3));
    }

    /// Round-tripping an instance through the wire type preserves the steps
    /// that recorded it; the record-without-provenance case is pinned in
    /// `v2`'s tests.
    #[test]
    fn instance_step_ids_survive_the_wire_bridge() {
        let runtime = ToolCallInstance {
            instance_key: Hash::from_content(b"key"),
            tool_call_id: "echo".to_string(),
            impure: false,
            executed_at: Timestamp::from_unix_nanos(0),
            command_args: Vec::new(),
            env_vars: BTreeMap::new(),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            step_ids: BTreeSet::from(["step-1-echo".to_string()]),
        };
        let wire: v3::ToolCallInstanceV3 = runtime.clone().into();
        let back: ToolCallInstance = wire.into();
        assert_eq!(back.step_ids, BTreeSet::from(["step-1-echo".to_string()]));
    }

    /// The instance wire record carries no `version` field (fact 26) — only
    /// the envelope has one. The `instance_key` hash round-trips as the map key.
    #[test]
    fn instance_json_has_no_version_field() {
        let encoded = encode_state_json(&sample_state()).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        let instances = json["tool_call_instances"].as_object().unwrap();
        let instance = instances.values().next().unwrap();
        assert!(
            instance.get("version").is_none(),
            "ToolCallInstance must not serialize a version field"
        );
        assert!(instance.get("instance_key").is_some());
    }
}
