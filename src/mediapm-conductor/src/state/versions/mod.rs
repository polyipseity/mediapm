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
//!   readable; V3 and V4 are both readable, and V4 is the current format.

use crate::error::ConductorError;
use crate::state::{
    AuxData, CapturedOutput, ConductorState, HashedValueRecord, InstanceAux, ToolCallInstance,
};
use mediapm_cas::CasApi;

mod v1;
mod v2;
mod v3;
mod v4;

// ---------------------------------------------------------------------------
// Bridge: V4 wire types ↔ runtime types
// ---------------------------------------------------------------------------
// Older versions reach these types only through their own migration: `v4`'s
// `from_v3_into_v4` is the V3 reader, so there is exactly one bridge to the
// runtime shape and no second place for a field to be forgotten.

impl From<v4::SaveModeV4> for crate::config::SaveMode {
    fn from(mode: v4::SaveModeV4) -> Self {
        match mode {
            v4::SaveModeV4::False => Self::False,
            v4::SaveModeV4::True => Self::True,
            v4::SaveModeV4::Full => Self::Full,
        }
    }
}

impl From<crate::config::SaveMode> for v4::SaveModeV4 {
    fn from(mode: crate::config::SaveMode) -> Self {
        match mode {
            crate::config::SaveMode::False => Self::False,
            crate::config::SaveMode::True => Self::True,
            crate::config::SaveMode::Full => Self::Full,
        }
    }
}

impl From<v4::OutputCaptureSpecV4> for crate::config::OutputCaptureSpec {
    fn from(spec: v4::OutputCaptureSpecV4) -> Self {
        Self {
            name: spec.name,
            capture: spec.capture,
            save: spec.save.into(),
            allow_empty: spec.allow_empty,
            include_topmost_folder: spec.include_topmost_folder,
        }
    }
}

impl From<crate::config::OutputCaptureSpec> for v4::OutputCaptureSpecV4 {
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

impl From<v4::CapturedOutputV4> for CapturedOutput {
    fn from(output: v4::CapturedOutputV4) -> Self {
        Self { hash: output.hash, capture: output.capture.map(Into::into) }
    }
}

impl From<CapturedOutput> for v4::CapturedOutputV4 {
    fn from(output: CapturedOutput) -> Self {
        Self { hash: output.hash, capture: output.capture.map(Into::into) }
    }
}

impl From<v4::HashedValueRecordV4> for HashedValueRecord {
    fn from(record: v4::HashedValueRecordV4) -> Self {
        Self { hash: record.hash, deterministic: record.deterministic }
    }
}

impl From<HashedValueRecord> for v4::HashedValueRecordV4 {
    fn from(record: HashedValueRecord) -> Self {
        Self { hash: record.hash, deterministic: record.deterministic }
    }
}

impl From<v4::InstanceAuxV4> for InstanceAux {
    fn from(aux: v4::InstanceAuxV4) -> Self {
        Self {
            last_referenced_at: mediapm_utils::Timestamp::from_unix_nanos(aux.last_referenced_at.0),
        }
    }
}

impl From<InstanceAux> for v4::InstanceAuxV4 {
    fn from(aux: InstanceAux) -> Self {
        Self { last_referenced_at: v4::ImpureTimestampV4(aux.last_referenced_at.as_unix_nanos()) }
    }
}

impl From<v4::ToolCallInstanceV4> for ToolCallInstance {
    fn from(inst: v4::ToolCallInstanceV4) -> Self {
        Self {
            instance_key: inst.instance_key,
            tool_call_id: inst.tool_call_id,
            impure: inst.impure,
            invalidated_at: inst
                .invalidated_at
                .map(|stamp| mediapm_utils::Timestamp::from_unix_nanos(stamp.0)),
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

impl From<ToolCallInstance> for v4::ToolCallInstanceV4 {
    fn from(inst: ToolCallInstance) -> Self {
        Self {
            instance_key: inst.instance_key,
            tool_call_id: inst.tool_call_id,
            impure: inst.impure,
            invalidated_at: inst
                .invalidated_at
                .map(|stamp| v4::ImpureTimestampV4(stamp.as_unix_nanos())),
            executed_at: v4::ImpureTimestampV4(inst.executed_at.as_unix_nanos()),
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

impl From<v4::AuxDataV4> for AuxData {
    fn from(aux: v4::AuxDataV4) -> Self {
        Self {
            tool_call_instance_counter: aux.tool_call_instance_counter,
            conductor_gc_epoch: mediapm_utils::Timestamp::from_unix_nanos(aux.conductor_gc_epoch.0),
            instances: aux.instances.into_iter().map(|(k, v)| (k, v.into())).collect(),
            live_instances: aux.live_instances,
        }
    }
}

impl From<AuxData> for v4::AuxDataV4 {
    fn from(aux: AuxData) -> Self {
        Self {
            tool_call_instance_counter: aux.tool_call_instance_counter,
            conductor_gc_epoch: v4::ImpureTimestampV4(aux.conductor_gc_epoch.as_unix_nanos()),
            instances: aux.instances.into_iter().map(|(k, v)| (k, v.into())).collect(),
            live_instances: aux.live_instances,
        }
    }
}

impl From<v4::ConductorStateV4> for ConductorState {
    fn from(state: v4::ConductorStateV4) -> Self {
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

impl From<ConductorState> for v4::ConductorStateV4 {
    fn from(state: ConductorState) -> Self {
        Self {
            version: v4::CONDUCTOR_STATE_VERSION_V4,
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

    if v4::is_conductor_state_version_v4(version) {
        let v4_state: v4::ConductorStateV4 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v4_state.into())
    } else if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v4::from_v3_into_v4(v3_state).into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v4::from_v3_into_v4(v3::from_v2_into_v3(v2_state)).into())
    } else if v1::is_orchestration_state_version_v1(version) {
        Err(ConductorError::Serialization(format!(
            "orchestration state version {version} requires CAS-backed migration; \
             use decode_state_json_with_cas"
        )))
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} \
             (supported: {}, {}, {}, {})",
            v4::CONDUCTOR_STATE_VERSION_V4,
            v3::CONDUCTOR_STATE_VERSION_V3,
            v2::CONDUCTOR_STATE_VERSION_V2,
            v1::ORCHESTRATION_STATE_VERSION_V1
        )))
    }
}

/// Decodes persisted state JSON, migrating v1 CAS-referenced envelopes when
/// needed.
///
/// Reads every version this build still understands, in order: V4 as written,
/// V3 and V2 through the V3→V4 bridge, and V1 through the CAS-backed envelope
/// migration first, then the same bridge. A document whose marker matches none
/// of them is an error that names each supported version, never a best-effort
/// decode.
///
/// # Errors
///
/// Returns an error when the version is unsupported or migration fails.
pub async fn decode_state_json_with_cas<C: CasApi>(
    cas: &C,
    bytes: &[u8],
) -> Result<ConductorState, ConductorError> {
    let version = peek_version_marker(bytes)?;

    if v4::is_conductor_state_version_v4(version) {
        let v4_state: v4::ConductorStateV4 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v4_state.into())
    } else if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v4::from_v3_into_v4(v3_state).into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v4::from_v3_into_v4(v3::from_v2_into_v3(v2_state)).into())
    } else if v1::is_orchestration_state_version_v1(version) {
        let envelope: v1::OrchestrationStateEnvelopeV1 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        let v2_state = v2::migrate_v1_to_v2(cas, envelope).await?;
        Ok(v4::from_v3_into_v4(v3::from_v2_into_v3(v2_state)).into())
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} \
             (supported: {}, {}, {}, {})",
            v4::CONDUCTOR_STATE_VERSION_V4,
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
    let v4_state: v4::ConductorStateV4 = state.clone().into();
    serde_json::to_vec_pretty(&v4_state).map_err(|e| ConductorError::Serialization(e.to_string()))
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
            invalidated_at: Some(Timestamp::from_unix_nanos(11)),
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
                live_instances: BTreeMap::from([("default".to_string(), BTreeSet::from([key]))]),
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
        assert_eq!(json["version"], serde_json::json!(4));
    }

    /// A document already written as V4 decodes unchanged, marker and all.
    ///
    /// This is the arm that keeps a reader from re-migrating a current file: a
    /// dispatcher that lacked it would report the file's own version as
    /// unsupported, and one that sent it through the V3 reader would drop the
    /// seed and the markers V3 cannot express.
    #[test]
    fn a_v4_document_decodes_with_both_new_fields() {
        let state = sample_state();
        let encoded = encode_state_json(&state).expect("state encodes");
        assert_eq!(
            peek_version_marker(&encoded).expect("marker is readable"),
            v4::CONDUCTOR_STATE_VERSION_V4,
        );

        let decoded = decode_state_json(&encoded).expect("the v4 document decodes");

        assert_eq!(decoded, state);
        let instance = decoded.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.invalidated_at, Some(Timestamp::from_unix_nanos(11)));
        assert_eq!(decoded.aux.live_instances.keys().collect::<Vec<_>>(), vec!["default"]);
    }

    /// A V3 document on disk still decodes, through the V4 bridge.
    ///
    /// Every workspace that ran before this version holds one of these. The
    /// defaults it lands on are the safe pair: nothing is invalidated, and no
    /// workflow has a recorded seed, so the GC evicts nothing until the next
    /// successful run writes one.
    #[test]
    fn a_v3_document_decodes_through_the_v4_bridge() {
        let json = format!(
            r#"{{
                "version": 3,
                "tool_call_instances": {{
                    "{}": {{
                        "instance_key": "{}",
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 3,
                        "step_ids": ["step-0-echo"],
                        "command_args": [],
                        "env_vars": {{}},
                        "materialized_inputs": {{}},
                        "outputs": {{}}
                    }}
                }},
                "aux": {{
                    "tool_call_instance_counter": 1,
                    "conductor_gc_epoch": 2,
                    "instances": {{}}
                }}
            }}"#,
            Hash::from_content(b"key"),
            Hash::from_content(b"key"),
        );

        let decoded = decode_state_json(json.as_bytes()).expect("a v3 document decodes");

        assert_eq!(decoded.version, STATE_VERSION);
        assert_eq!(decoded.aux.conductor_gc_epoch, Timestamp::from_unix_nanos(2));
        assert!(decoded.aux.live_instances.is_empty());
        let instance = decoded.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.invalidated_at, None);
        assert_eq!(instance.step_ids, BTreeSet::from(["step-0-echo".to_string()]));
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
            invalidated_at: None,
            executed_at: Timestamp::from_unix_nanos(0),
            command_args: Vec::new(),
            env_vars: BTreeMap::new(),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            step_ids: BTreeSet::from(["step-1-echo".to_string()]),
        };
        let wire: v4::ToolCallInstanceV4 = runtime.clone().into();
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
