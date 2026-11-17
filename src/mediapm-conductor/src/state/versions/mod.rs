//! Versioned persistence envelopes for orchestration state.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - Version dispatch lives here, not in any caller. Adding a version is a new
//!   `vN.rs` plus a new arm in the decoders below.
//! - Do not directly re-export `versions::vX` structs or paths as public API.
//! - Never delete a read path for a version that could exist on disk.
//! - V1 and V2 are migration-only formats that stay readable; V3, V4 and V5
//!   migrate forward to V6; V6 is the current format.

use crate::error::ConductorError;
use crate::state::{
    CapturedOutput, ConductorState, EnvVarEntry, EnvVarSource, StepRef, ToolCallInstance,
};
use mediapm_cas::CasApi;
use mediapm_utils::Timestamp;

mod v1;
mod v2;
mod v3;
mod v4;
mod v5;
mod v6;

// ---------------------------------------------------------------------------
// Bridge: V6 wire types ↔ runtime types
// ---------------------------------------------------------------------------

impl From<v6::EnvVarSourceV6> for EnvVarSource {
    fn from(source: v6::EnvVarSourceV6) -> Self {
        match source {
            v6::EnvVarSourceV6::Literal => Self::Literal,
            v6::EnvVarSourceV6::Inherited => Self::Inherited,
        }
    }
}

impl From<EnvVarSource> for v6::EnvVarSourceV6 {
    fn from(source: EnvVarSource) -> Self {
        match source {
            EnvVarSource::Literal => Self::Literal,
            EnvVarSource::Inherited => Self::Inherited,
        }
    }
}

impl From<v6::EnvVarEntryV6> for EnvVarEntry {
    fn from(entry: v6::EnvVarEntryV6) -> Self {
        Self { hash_ck: entry.hash_ck, source_nk: entry.source_nk.map(Into::into) }
    }
}

impl From<EnvVarEntry> for v6::EnvVarEntryV6 {
    fn from(entry: EnvVarEntry) -> Self {
        Self { hash_ck: entry.hash_ck, source_nk: entry.source_nk.map(Into::into) }
    }
}

impl From<v6::CapturedOutputV6> for CapturedOutput {
    fn from(output: v6::CapturedOutputV6) -> Self {
        Self { capture: output.capture, hash_nk: output.hash_nk, saved_nk: output.saved_nk }
    }
}

impl From<CapturedOutput> for v6::CapturedOutputV6 {
    fn from(output: CapturedOutput) -> Self {
        Self { capture: output.capture, hash_nk: output.hash_nk, saved_nk: output.saved_nk }
    }
}

impl From<v6::StepRefV6> for StepRef {
    fn from(step: v6::StepRefV6) -> Self {
        Self { workflow_id: step.workflow_id, step_id: step.step_id }
    }
}

impl From<StepRef> for v6::StepRefV6 {
    fn from(step: StepRef) -> Self {
        Self { workflow_id: step.workflow_id, step_id: step.step_id }
    }
}

impl From<v6::ToolCallInstanceV6> for ToolCallInstance {
    fn from(inst: v6::ToolCallInstanceV6) -> Self {
        Self {
            key_version: inst.key_version,
            attempted_key_version_nk: inst.attempted_key_version_nk,
            tool_call_id: inst.tool_call_id,
            command_args: inst.command_args,
            env_vars_ck: inst.env_vars_ck.into_iter().map(|(k, v)| (k, v.into())).collect(),
            materialized_inputs: inst.materialized_inputs,
            outputs: inst.outputs.into_iter().map(|(k, v)| (k, v.into())).collect(),
            impure_nk: inst.impure_nk,
            invalidated_at_nk: inst
                .invalidated_at_nk
                .map(|stamp| Timestamp::from_unix_nanos(stamp.0)),
            executed_at_nk: Timestamp::from_unix_nanos(inst.executed_at_nk.0),
            last_referenced_at_nk: Timestamp::from_unix_nanos(inst.last_referenced_at_nk.0),
            step_ids_nk: inst.step_ids_nk.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<ToolCallInstance> for v6::ToolCallInstanceV6 {
    fn from(inst: ToolCallInstance) -> Self {
        Self {
            key_version: inst.key_version,
            attempted_key_version_nk: inst.attempted_key_version_nk,
            tool_call_id: inst.tool_call_id,
            command_args: inst.command_args,
            env_vars_ck: inst.env_vars_ck.into_iter().map(|(k, v)| (k, v.into())).collect(),
            materialized_inputs: inst.materialized_inputs,
            outputs: inst.outputs.into_iter().map(|(k, v)| (k, v.into())).collect(),
            impure_nk: inst.impure_nk,
            invalidated_at_nk: inst
                .invalidated_at_nk
                .map(|stamp| v6::WireTimestampV6(stamp.as_unix_nanos())),
            executed_at_nk: v6::WireTimestampV6(inst.executed_at_nk.as_unix_nanos()),
            last_referenced_at_nk: v6::WireTimestampV6(inst.last_referenced_at_nk.as_unix_nanos()),
            step_ids_nk: inst.step_ids_nk.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<v6::ConductorStateV6> for ConductorState {
    fn from(state: v6::ConductorStateV6) -> Self {
        Self {
            version: state.version,
            conductor_gc_ran_at: Timestamp::from_unix_nanos(state.conductor_gc_ran_at),
            step_instances: state.step_instances,
            tool_call_instances: state
                .tool_call_instances
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<ConductorState> for v6::ConductorStateV6 {
    fn from(state: ConductorState) -> Self {
        Self {
            version: v6::CONDUCTOR_STATE_VERSION_V6,
            conductor_gc_ran_at: state.conductor_gc_ran_at.as_unix_nanos(),
            step_instances: state.step_instances,
            tool_call_instances: state
                .tool_call_instances
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
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

/// Decodes an orchestration state from inline JSON bytes.
///
/// # Errors
///
/// Returns an error if the version is unsupported.
pub fn decode_state_json(bytes: &[u8]) -> Result<ConductorState, ConductorError> {
    let version = peek_version_marker(bytes)?;

    if v6::is_conductor_state_version_v6(version) {
        let v6_state: v6::ConductorStateV6 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6_state.into())
    } else if v5::is_conductor_state_version_v5(version) {
        let v5_state: v5::ConductorStateV5 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6::from_v5_into_v6(v5_state).into())
    } else if v4::is_conductor_state_version_v4(version) {
        let v4_state: v4::ConductorStateV4 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6::from_v5_into_v6(v5::from_v4_into_v5(v4_state)).into())
    } else if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(v3_state))).into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(v3::from_v2_into_v3(
            v2_state,
        ))))
        .into())
    } else if v1::is_orchestration_state_version_v1(version) {
        Err(ConductorError::Serialization(format!(
            "orchestration state version {version} requires CAS-backed migration; \
             use decode_state_json_with_cas"
        )))
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} \
             (supported: {}, {}, {}, {}, {}, {})",
            v6::CONDUCTOR_STATE_VERSION_V6,
            v5::CONDUCTOR_STATE_VERSION_V5,
            v4::CONDUCTOR_STATE_VERSION_V4,
            v3::CONDUCTOR_STATE_VERSION_V3,
            v2::CONDUCTOR_STATE_VERSION_V2,
            v1::ORCHESTRATION_STATE_VERSION_V1
        )))
    }
}

/// Decodes persisted state JSON, migrating older versions when needed.
///
/// # Errors
///
/// Returns an error when the version is unsupported or migration fails.
pub async fn decode_state_json_with_cas<C: CasApi>(
    cas: &C,
    bytes: &[u8],
) -> Result<ConductorState, ConductorError> {
    let version = peek_version_marker(bytes)?;

    if v6::is_conductor_state_version_v6(version) {
        let v6_state: v6::ConductorStateV6 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6_state.into())
    } else if v5::is_conductor_state_version_v5(version) {
        let v5_state: v5::ConductorStateV5 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6::from_v5_into_v6(v5_state).into())
    } else if v4::is_conductor_state_version_v4(version) {
        let v4_state: v4::ConductorStateV4 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6::from_v5_into_v6(v5::from_v4_into_v5(v4_state)).into())
    } else if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(v3_state))).into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(v3::from_v2_into_v3(
            v2_state,
        ))))
        .into())
    } else if v1::is_orchestration_state_version_v1(version) {
        let envelope: v1::OrchestrationStateEnvelopeV1 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        let v2_state = v2::migrate_v1_to_v2(cas, envelope).await?;
        Ok(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(v3::from_v2_into_v3(
            v2_state,
        ))))
        .into())
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} \
             (supported: {}, {}, {}, {}, {}, {})",
            v6::CONDUCTOR_STATE_VERSION_V6,
            v5::CONDUCTOR_STATE_VERSION_V5,
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
    let v6_state: v6::ConductorStateV6 = state.clone().into();
    serde_json::to_vec_pretty(&v6_state).map_err(|e| ConductorError::Serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mediapm_cas::Hash;
    use mediapm_utils::Timestamp;

    use super::*;
    use crate::state::{STATE_VERSION, StepRef};

    fn sample_state() -> ConductorState {
        let key = Hash::from_content(b"key");
        let instance = ToolCallInstance {
            key_version: 8,
            attempted_key_version_nk: 8,
            tool_call_id: "echo@v1".to_string(),
            command_args: vec![Hash::from_content(b"arg")],
            env_vars_ck: BTreeMap::from([(
                "FOO".to_string(),
                EnvVarEntry {
                    hash_ck: Hash::from_content(b"val"),
                    source_nk: Some(EnvVarSource::Literal),
                },
            )]),
            materialized_inputs: BTreeMap::from([(
                "bin/tool".to_string(),
                Hash::from_content(b"payload"),
            )]),
            outputs: BTreeMap::from([(
                "stdout".to_string(),
                CapturedOutput {
                    capture: "stdout".to_string(),
                    hash_nk: Some(Hash::from_content(b"out")),
                    saved_nk: true,
                },
            )]),
            impure_nk: false,
            invalidated_at_nk: Some(Timestamp::from_unix_nanos(11)),
            executed_at_nk: Timestamp::from_unix_nanos(5),
            last_referenced_at_nk: Timestamp::from_unix_nanos(42),
            step_ids_nk: BTreeSet::from([StepRef {
                workflow_id: "default".to_string(),
                step_id: "step-0-echo".to_string(),
            }]),
        };
        ConductorState {
            version: STATE_VERSION,
            conductor_gc_ran_at: Timestamp::from_unix_nanos(9),
            step_instances: BTreeMap::from([(
                "default".to_string(),
                BTreeMap::from([("step-0-echo".to_string(), key)]),
            )]),
            tool_call_instances: BTreeMap::from([(key, instance)]),
        }
    }

    #[test]
    fn state_json_round_trip_preserves_every_recorded_field() {
        let state = sample_state();
        let encoded = encode_state_json(&state).unwrap();
        let decoded = decode_state_json(&encoded).unwrap();
        assert_eq!(decoded, state);
    }

    #[test]
    fn encoded_state_declares_the_current_version() {
        let encoded = encode_state_json(&sample_state()).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["version"], serde_json::json!(STATE_VERSION));
        assert_eq!(json["version"], serde_json::json!(6));
    }

    #[test]
    fn a_v6_document_decodes_unchanged() {
        let state = sample_state();
        let encoded = encode_state_json(&state).expect("state encodes");
        assert_eq!(peek_version_marker(&encoded).expect("marker is readable"), 6);

        let decoded = decode_state_json(&encoded).expect("the v6 document decodes");
        assert_eq!(decoded, state);
        let instance = decoded.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.invalidated_at_nk, Some(Timestamp::from_unix_nanos(11)));
        assert_eq!(
            decoded.step_instances["default"]["step-0-echo"],
            decoded.tool_call_instances.keys().next().copied().unwrap()
        );
    }

    #[test]
    fn a_v5_document_migrates_to_v6() {
        let key = Hash::from_content(b"key");
        let json = format!(
            r#"{{
                "version": 5,
                "tool_call_instances": {{
                    "{key}": {{
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 7,
                        "last_referenced_at": 42,
                        "step_ids": [{{ "workflow_id": "default", "step_id": "step-0-echo" }}],
                        "command_args": [],
                        "env_vars": {{ "FOO": "{}" }},
                        "materialized_inputs": {{}},
                        "outputs": {{
                            "out": {{
                                "hash": "{}",
                                "capture": {{
                                    "name": "out",
                                    "capture": "file:out.bin",
                                    "save": "false",
                                    "allow_empty": false,
                                    "include_topmost_folder": true
                                }}
                            }},
                            "dropped": {{ "hash": "{}", "capture": null }}
                        }}
                    }}
                }},
                "aux": {{ "conductor_gc_epoch": 9, "live_instances": {{ "default": ["{key}"] }} }}
            }}"#,
            Hash::from_content(b"val"),
            Hash::from_content(b"out"),
            Hash::from_content(b"dropped"),
        );

        let decoded = decode_state_json(json.as_bytes()).expect("a v5 document decodes");
        assert_eq!(decoded.version, 6);
        assert_eq!(decoded.conductor_gc_ran_at, Timestamp::from_unix_nanos(9));
        assert_eq!(decoded.step_instances["default"]["step-0-echo"], key);
        let instance = &decoded.tool_call_instances[&key];
        assert_eq!(instance.key_version, 7);
        assert_eq!(instance.attempted_key_version_nk, 7);
        assert_eq!(instance.env_vars_ck["FOO"].source_nk, None);
        assert_eq!(instance.invalidated_at_nk, None);
        assert_eq!(instance.outputs.len(), 1);
        assert_eq!(instance.outputs["out"].hash_nk, Some(Hash::from_content(b"out")));
        assert!(!instance.outputs["out"].saved_nk);
    }

    #[test]
    fn a_v3_document_decodes_through_the_v6_ladder() {
        let key = Hash::from_content(b"key");
        let json = format!(
            r#"{{
                "version": 3,
                "tool_call_instances": {{
                    "{key}": {{
                        "instance_key": "{key}",
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
        );

        let decoded = decode_state_json(json.as_bytes()).expect("a v3 document decodes");
        assert_eq!(decoded.version, STATE_VERSION);
        assert_eq!(decoded.conductor_gc_ran_at, Timestamp::from_unix_nanos(2));
        assert_eq!(
            decoded.step_instances,
            BTreeMap::from([(String::new(), BTreeMap::from([("step-0-echo".to_string(), key)]),)]),
            "v6 migration backfills step_instances from instance step provenance",
        );
        let instance = decoded.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.invalidated_at_nk, None);
        assert_eq!(instance.key_version, 7);
        assert_eq!(instance.attempted_key_version_nk, 7);
        assert_eq!(
            instance.step_ids_nk,
            BTreeSet::from([StepRef {
                workflow_id: String::new(),
                step_id: "step-0-echo".to_string(),
            }]),
        );
        assert_eq!(instance.last_referenced_at_nk, Timestamp::default());
    }

    #[test]
    fn an_unsupported_version_is_refused_and_names_the_supported_set() {
        let error =
            decode_state_json(br#"{"version": 99}"#).expect_err("version 99 must not be decoded");
        let message = error.to_string();
        assert!(message.contains("unsupported orchestration state version: 99"), "{message}");
        for supported in ["6", "5", "4", "3", "2", "1"] {
            assert!(message.contains(supported), "missing {supported} in {message}");
        }
    }

    #[test]
    fn a_v1_document_without_cas_is_refused_and_points_at_the_cas_decoder() {
        let error = decode_state_json(br#"{"version": 1}"#)
            .expect_err("a v1 document needs the CAS-backed decoder");
        assert!(
            error.to_string().contains("requires CAS-backed migration")
                && error.to_string().contains("decode_state_json_with_cas"),
            "{error}",
        );
    }

    #[test]
    fn nullable_facts_are_explicit_null_in_v6_json() {
        let key = Hash::from_content(b"key");
        let state = ConductorState {
            version: 6,
            conductor_gc_ran_at: Timestamp::from_unix_nanos(1),
            step_instances: BTreeMap::new(),
            tool_call_instances: BTreeMap::from([(
                key,
                ToolCallInstance {
                    key_version: 7,
                    attempted_key_version_nk: 7,
                    tool_call_id: "t".to_string(),
                    command_args: Vec::new(),
                    env_vars_ck: BTreeMap::from([(
                        "K".to_string(),
                        EnvVarEntry { hash_ck: Hash::from_content(b"v"), source_nk: None },
                    )]),
                    materialized_inputs: BTreeMap::new(),
                    outputs: BTreeMap::from([(
                        "o".to_string(),
                        CapturedOutput {
                            capture: "stdout".to_string(),
                            hash_nk: None,
                            saved_nk: false,
                        },
                    )]),
                    impure_nk: false,
                    invalidated_at_nk: None,
                    executed_at_nk: Timestamp::from_unix_nanos(1),
                    last_referenced_at_nk: Timestamp::from_unix_nanos(1),
                    step_ids_nk: BTreeSet::new(),
                },
            )]),
        };
        let encoded = serde_json::to_value(encode_state_json(&state).unwrap()).unwrap();
        let inst = encoded["tool_call_instances"][key.to_string()].clone();
        assert_eq!(inst["invalidated_at_nk"], serde_json::Value::Null);
        assert_eq!(inst["outputs"]["o"]["hash_nk"], serde_json::Value::Null);
        assert_eq!(inst["env_vars_ck"]["K"]["source_nk"], serde_json::Value::Null);
    }

    #[test]
    fn v6_rejects_v5_only_fields() {
        let json = br#"{"version":6,"conductor_gc_ran_at":0,"step_instances":{},"tool_call_instances":{},"aux":{"conductor_gc_epoch":0}}"#;
        let err = decode_state_json(json).expect_err("aux must be rejected");
        assert!(err.to_string().contains("unknown") || err.to_string().contains("aux"), "{err}");
    }
}
