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
//!   readable; V3, V4 and V5 are all readable, and V5 is the current format.

use crate::error::ConductorError;
use crate::state::{AuxData, CapturedOutput, ConductorState, StepRef, ToolCallInstance};
use mediapm_cas::CasApi;

mod v1;
mod v2;
mod v3;
mod v4;
mod v5;

// ---------------------------------------------------------------------------
// Bridge: V5 wire types ↔ runtime types
// ---------------------------------------------------------------------------
// Older versions reach these types only through their own migration: `v5`'s
// `from_v4_into_v5` is the V4 reader, so there is exactly one bridge to the
// runtime shape and no second place for a field to be forgotten.

impl From<v5::SaveModeV5> for crate::config::SaveMode {
    fn from(mode: v5::SaveModeV5) -> Self {
        match mode {
            v5::SaveModeV5::False => Self::False,
            v5::SaveModeV5::True => Self::True,
            v5::SaveModeV5::Full => Self::Full,
        }
    }
}

impl From<crate::config::SaveMode> for v5::SaveModeV5 {
    fn from(mode: crate::config::SaveMode) -> Self {
        match mode {
            crate::config::SaveMode::False => Self::False,
            crate::config::SaveMode::True => Self::True,
            crate::config::SaveMode::Full => Self::Full,
        }
    }
}

impl From<v5::OutputCaptureSpecV5> for crate::config::OutputCaptureSpec {
    fn from(spec: v5::OutputCaptureSpecV5) -> Self {
        Self {
            name: spec.name,
            capture: spec.capture,
            save: spec.save.into(),
            allow_empty: spec.allow_empty,
            include_topmost_folder: spec.include_topmost_folder,
        }
    }
}

impl From<crate::config::OutputCaptureSpec> for v5::OutputCaptureSpecV5 {
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

impl From<v5::CapturedOutputV5> for CapturedOutput {
    fn from(output: v5::CapturedOutputV5) -> Self {
        Self { hash: output.hash, capture: output.capture.map(Into::into) }
    }
}

impl From<CapturedOutput> for v5::CapturedOutputV5 {
    fn from(output: CapturedOutput) -> Self {
        Self { hash: output.hash, capture: output.capture.map(Into::into) }
    }
}

impl From<v5::StepRefV5> for StepRef {
    fn from(step: v5::StepRefV5) -> Self {
        Self { workflow_id: step.workflow_id, step_id: step.step_id }
    }
}

impl From<StepRef> for v5::StepRefV5 {
    fn from(step: StepRef) -> Self {
        Self { workflow_id: step.workflow_id, step_id: step.step_id }
    }
}

impl From<v5::ToolCallInstanceV5> for ToolCallInstance {
    fn from(inst: v5::ToolCallInstanceV5) -> Self {
        Self {
            tool_call_id: inst.tool_call_id,
            impure: inst.impure,
            invalidated_at: inst
                .invalidated_at
                .map(|stamp| mediapm_utils::Timestamp::from_unix_nanos(stamp.0)),
            executed_at: mediapm_utils::Timestamp::from_unix_nanos(inst.executed_at.0),
            last_referenced_at: mediapm_utils::Timestamp::from_unix_nanos(
                inst.last_referenced_at.0,
            ),
            step_ids: inst.step_ids.into_iter().map(Into::into).collect(),
            command_args: inst.command_args,
            env_vars: inst.env_vars,
            materialized_inputs: inst.materialized_inputs,
            outputs: inst.outputs.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<ToolCallInstance> for v5::ToolCallInstanceV5 {
    fn from(inst: ToolCallInstance) -> Self {
        Self {
            tool_call_id: inst.tool_call_id,
            impure: inst.impure,
            invalidated_at: inst
                .invalidated_at
                .map(|stamp| v5::WireTimestampV5(stamp.as_unix_nanos())),
            executed_at: v5::WireTimestampV5(inst.executed_at.as_unix_nanos()),
            last_referenced_at: v5::WireTimestampV5(inst.last_referenced_at.as_unix_nanos()),
            step_ids: inst.step_ids.into_iter().map(Into::into).collect(),
            command_args: inst.command_args,
            env_vars: inst.env_vars,
            materialized_inputs: inst.materialized_inputs,
            outputs: inst.outputs.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<v5::AuxDataV5> for AuxData {
    fn from(aux: v5::AuxDataV5) -> Self {
        Self {
            conductor_gc_epoch: mediapm_utils::Timestamp::from_unix_nanos(aux.conductor_gc_epoch.0),
            live_instances: aux.live_instances,
        }
    }
}

impl From<AuxData> for v5::AuxDataV5 {
    fn from(aux: AuxData) -> Self {
        Self {
            conductor_gc_epoch: v5::WireTimestampV5(aux.conductor_gc_epoch.as_unix_nanos()),
            live_instances: aux.live_instances,
        }
    }
}

impl From<v5::ConductorStateV5> for ConductorState {
    fn from(state: v5::ConductorStateV5) -> Self {
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

impl From<ConductorState> for v5::ConductorStateV5 {
    fn from(state: ConductorState) -> Self {
        Self {
            version: v5::CONDUCTOR_STATE_VERSION_V5,
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

    if v5::is_conductor_state_version_v5(version) {
        let v5_state: v5::ConductorStateV5 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v5_state.into())
    } else if v4::is_conductor_state_version_v4(version) {
        let v4_state: v4::ConductorStateV4 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v5::from_v4_into_v5(v4_state).into())
    } else if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v5::from_v4_into_v5(v4::from_v3_into_v4(v3_state)).into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v5::from_v4_into_v5(v4::from_v3_into_v4(v3::from_v2_into_v3(v2_state))).into())
    } else if v1::is_orchestration_state_version_v1(version) {
        Err(ConductorError::Serialization(format!(
            "orchestration state version {version} requires CAS-backed migration; \
             use decode_state_json_with_cas"
        )))
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} \
             (supported: {}, {}, {}, {}, {})",
            v5::CONDUCTOR_STATE_VERSION_V5,
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
/// Reads every version this build still understands, in order: V5 as written,
/// V4 through the V4→V5 migration, V3 and V2 through the V3→V4 bridge first,
/// and V1 through the CAS-backed envelope migration before both. A document
/// whose marker matches none of them is an error that names each supported
/// version, never a best-effort decode.
///
/// # Errors
///
/// Returns an error when the version is unsupported or migration fails.
pub async fn decode_state_json_with_cas<C: CasApi>(
    cas: &C,
    bytes: &[u8],
) -> Result<ConductorState, ConductorError> {
    let version = peek_version_marker(bytes)?;

    if v5::is_conductor_state_version_v5(version) {
        let v5_state: v5::ConductorStateV5 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v5_state.into())
    } else if v4::is_conductor_state_version_v4(version) {
        let v4_state: v4::ConductorStateV4 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v5::from_v4_into_v5(v4_state).into())
    } else if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v5::from_v4_into_v5(v4::from_v3_into_v4(v3_state)).into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v5::from_v4_into_v5(v4::from_v3_into_v4(v3::from_v2_into_v3(v2_state))).into())
    } else if v1::is_orchestration_state_version_v1(version) {
        let envelope: v1::OrchestrationStateEnvelopeV1 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        let v2_state = v2::migrate_v1_to_v2(cas, envelope).await?;
        Ok(v5::from_v4_into_v5(v4::from_v3_into_v4(v3::from_v2_into_v3(v2_state))).into())
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} \
             (supported: {}, {}, {}, {}, {})",
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
    let v5_state: v5::ConductorStateV5 = state.clone().into();
    serde_json::to_vec_pretty(&v5_state).map_err(|e| ConductorError::Serialization(e.to_string()))
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
            tool_call_id: "echo@v1".to_string(),
            impure: false,
            invalidated_at: Some(Timestamp::from_unix_nanos(11)),
            executed_at: Timestamp::from_unix_nanos(5),
            last_referenced_at: Timestamp::from_unix_nanos(42),
            step_ids: BTreeSet::from([StepRef {
                workflow_id: "default".to_string(),
                step_id: "step-0-echo".to_string(),
            }]),
            command_args: vec![Hash::from_content(b"arg")],
            env_vars: BTreeMap::from([("FOO".to_string(), Hash::from_content(b"val"))]),
            materialized_inputs: BTreeMap::from([(
                "bin/tool".to_string(),
                Hash::from_content(b"payload"),
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
                conductor_gc_epoch: Timestamp::from_unix_nanos(9),
                live_instances: BTreeMap::from([("default".to_string(), BTreeSet::from([key]))]),
            },
        }
    }

    /// Every recorded field survives the encode/decode bridge: the capture
    /// spec, the argv hashes, the environment hashes, the staged-input hashes,
    /// the GC clock and the workflow-qualified step ref.
    ///
    /// The spec is what a reusing step compares its declaration against, so a
    /// bridge that dropped it would make every recorded name look unknown and
    /// every reuse a miss — a silent, permanent re-execution rather than a
    /// visible failure. The step ref is what an invalidation request matches, so
    /// a bridge that dropped its workflow would make marking a step reach
    /// nothing.
    #[test]
    fn state_json_round_trip_preserves_every_recorded_field() {
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
        assert_eq!(json["version"], serde_json::json!(5));
    }

    /// A document already written as V5 decodes unchanged, marker and all.
    ///
    /// This is the arm that keeps a reader from re-migrating a current file: a
    /// dispatcher that lacked it would report the file's own version as
    /// unsupported, and one that sent it through an older reader would drop
    /// every field that reader cannot express.
    #[test]
    fn a_v5_document_decodes_unchanged() {
        let state = sample_state();
        let encoded = encode_state_json(&state).expect("state encodes");
        assert_eq!(
            peek_version_marker(&encoded).expect("marker is readable"),
            v5::CONDUCTOR_STATE_VERSION_V5,
        );

        let decoded = decode_state_json(&encoded).expect("the v5 document decodes");

        assert_eq!(decoded, state);
        let instance = decoded.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.invalidated_at, Some(Timestamp::from_unix_nanos(11)));
        assert_eq!(decoded.aux.live_instances.keys().collect::<Vec<_>>(), vec!["default"]);
    }

    /// A V3 document on disk still decodes, through the V4→V5 ladder.
    ///
    /// Every workspace that ran before either version holds one of these. The
    /// defaults it lands on are the safe pair: nothing is invalidated, and no
    /// workflow has a recorded seed, so the GC evicts nothing until the next
    /// successful run writes one. Its step id arrives unqualified, because the
    /// V3 wire could not say which workflow it belonged to.
    #[test]
    fn a_v3_document_decodes_through_the_v5_ladder() {
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
        assert_eq!(decoded.aux.conductor_gc_epoch, Timestamp::from_unix_nanos(2));
        assert!(decoded.aux.live_instances.is_empty());
        let instance = decoded.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.invalidated_at, None);
        assert_eq!(
            instance.step_ids,
            BTreeSet::from([StepRef {
                workflow_id: String::new(),
                step_id: "step-0-echo".to_string(),
            }]),
            "a step id the old wire could not qualify migrates with no workflow",
        );
        assert_eq!(
            instance.last_referenced_at,
            Timestamp::default(),
            "a record with no aux clock starts at the epoch, which is what V4 read a missing entry as",
        );
    }

    /// Round-tripping an instance through the wire type preserves the steps
    /// that recorded it, workflow and all; the record-without-provenance case
    /// is pinned in `v2`'s tests.
    #[test]
    fn instance_step_ids_survive_the_wire_bridge() {
        let runtime = ToolCallInstance {
            tool_call_id: "echo".to_string(),
            impure: false,
            invalidated_at: None,
            executed_at: Timestamp::from_unix_nanos(0),
            last_referenced_at: Timestamp::from_unix_nanos(0),
            command_args: Vec::new(),
            env_vars: BTreeMap::new(),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            step_ids: BTreeSet::from([StepRef {
                workflow_id: "mediapm.media.demo.local.dQw4w9WgXcQ".to_string(),
                step_id: "step-1-echo".to_string(),
            }]),
        };
        let wire: v5::ToolCallInstanceV5 = runtime.clone().into();
        let back: ToolCallInstance = wire.into();
        assert_eq!(
            back.step_ids,
            BTreeSet::from([StepRef {
                workflow_id: "mediapm.media.demo.local.dQw4w9WgXcQ".to_string(),
                step_id: "step-1-echo".to_string(),
            }]),
        );
    }

    /// A v4 document migrates to v5: the map key becomes the record's only
    /// identity, the GC clock moves onto the record, the argv flag is dropped,
    /// and a step id the v4 wire could not qualify becomes a ref that matches no
    /// workflow.
    ///
    /// The last point is the safe direction for a workflow-scoped invalidation:
    /// a migrated record cannot be marked by a request that names a workflow,
    /// so an operator's `media invalidate` marks nothing rather than marking
    /// work under some other workflow. The record re-acquires a qualified ref
    /// the next time the step serves it, whether by executing or by hitting it.
    #[test]
    fn a_v4_document_migrates_to_v5() {
        let key = Hash::from_content(b"key");
        let arg = Hash::from_content(b"arg");
        let json = format!(
            r#"{{
                "version": 4,
                "tool_call_instances": {{
                    "{key}": {{
                        "instance_key": "{key}",
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 7,
                        "step_ids": ["step-0-echo"],
                        "command_args": [{{ "hash": "{arg}", "deterministic": true }}],
                        "env_vars": {{}},
                        "materialized_inputs": {{}},
                        "outputs": {{}}
                    }}
                }},
                "aux": {{
                    "tool_call_instance_counter": 5,
                    "conductor_gc_epoch": 9,
                    "instances": {{ "{key}": {{ "last_referenced_at": 42 }} }},
                    "live_instances": {{ "default": ["{key}"] }}
                }}
            }}"#,
        );

        let decoded = decode_state_json(json.as_bytes()).expect("a v4 document decodes");

        assert_eq!(STATE_VERSION, 5, "the runtime marker is the version this migration lands on");
        assert_eq!(decoded.version, 5);
        assert_eq!(decoded.aux.conductor_gc_epoch, Timestamp::from_unix_nanos(9));
        assert_eq!(
            decoded.aux.live_instances.get("default"),
            Some(&BTreeSet::from([key])),
            "the GC seed is carried over, so a migrated document keeps its instances",
        );
        let instance = decoded.tool_call_instances[&key].clone();
        assert_eq!(
            instance.step_ids,
            BTreeSet::from([StepRef {
                workflow_id: String::new(),
                step_id: "step-0-echo".to_string(),
            }]),
            "a v4 step id could not name its workflow, so it migrates unqualified",
        );
        assert_eq!(
            instance.last_referenced_at,
            Timestamp::from_unix_nanos(42),
            "the GC clock moves off the aux map and onto the record it describes",
        );
        assert_eq!(
            instance.command_args,
            vec![arg],
            "the argv hashes are carried over in order, with the flag dropped",
        );
    }

    /// A v4 record whose clock has no aux entry starts at the epoch, which is
    /// exactly what the V4 reader's `map_or(0, …)` treated a missing entry as,
    /// so the migration changes no eviction decision.
    #[test]
    fn a_v4_record_with_no_aux_clock_migrates_to_the_epoch() {
        let key = Hash::from_content(b"key");
        let json = format!(
            r#"{{
                "version": 4,
                "tool_call_instances": {{
                    "{key}": {{
                        "instance_key": "{key}",
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 7,
                        "command_args": [],
                        "env_vars": {{}},
                        "materialized_inputs": {{}},
                        "outputs": {{}}
                    }}
                }},
                "aux": {{
                    "tool_call_instance_counter": 0,
                    "conductor_gc_epoch": 9,
                    "instances": {{}}
                }}
            }}"#,
        );

        let decoded = decode_state_json(json.as_bytes()).expect("a v4 document decodes");

        assert_eq!(decoded.tool_call_instances[&key].last_referenced_at, Timestamp::default());
    }

    /// The recorded instance does not carry its own key: the map key is its only
    /// home, so a document cannot hold two identities that disagree.
    ///
    /// Asserted on the serialized document rather than on the struct, so it
    /// cannot be satisfied by a field that merely serializes under another name.
    /// `a_saved_state_file_is_whole_and_leaves_no_temporary_file` holds the same
    /// property against a file written by the real save path.
    #[test]
    fn the_recorded_instance_does_not_carry_its_own_key() {
        let encoded = encode_state_json(&sample_state()).expect("state encodes");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("state is json");
        let instances = json["tool_call_instances"].as_object().expect("instances map");
        let (map_key, instance) = instances.iter().next().expect("one instance");

        assert!(map_key.starts_with("blake3:"), "the map is keyed by content hash");
        assert!(
            instance.get("instance_key").is_none(),
            "the record must not repeat the key it is filed under",
        );
    }

    /// The instance wire record carries no `version` field (fact 26) — only
    /// the envelope has one, and the instance's identity is its map key.
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
    }
}
