//! Versioned persistence envelopes for orchestration state.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - Version dispatch lives here, not in any caller. Adding a version is a new
//!   `vN.rs` plus a new arm in the decoders below.
//! - Do not directly re-export `versions::vX` structs or paths as public API.
//! - Never delete a read path for a version that could exist on disk.
//! - V1 and V2 are migration-only formats that stay readable; V3 through V6
//!   migrate forward to V7; V7 is the current format.

use crate::derived_hash::DerivedHash;
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
pub(crate) mod v7;

// ---------------------------------------------------------------------------
// Bridge: versioned wire types ↔ runtime types
// ---------------------------------------------------------------------------

/// Reduces a key-participating digest to a V6 hash.
///
/// V6 is a read-only migration format, so a keyed digest cannot be written
/// back into it without changing what the digest means.
fn v6_hash_from_derived(value: DerivedHash) -> mediapm_cas::Hash {
    match value {
        DerivedHash::Unkeyed(hash) => hash,
        DerivedHash::Keyed(_) => panic!("a keyed digest cannot be written as V6 state"),
    }
}

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
        Self { hash_ck: entry.hash_ck.into(), source_nk: entry.source_nk.map(Into::into) }
    }
}

impl From<EnvVarEntry> for v6::EnvVarEntryV6 {
    fn from(entry: EnvVarEntry) -> Self {
        Self {
            hash_ck: v6_hash_from_derived(entry.hash_ck),
            source_nk: entry.source_nk.map(Into::into),
        }
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
            command_args: inst.command_args.into_iter().map(Into::into).collect(),
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
            command_args: inst.command_args.into_iter().map(v6_hash_from_derived).collect(),
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
            step_instances: state
                .step_instances
                .into_iter()
                .map(|(workflow, steps)| {
                    (workflow, steps.into_iter().map(|(step, key)| (step, key.into())).collect())
                })
                .collect(),
            tool_call_instances: state
                .tool_call_instances
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            instance_key_secret_verifier: None,
        }
    }
}

impl From<ConductorState> for v6::ConductorStateV6 {
    fn from(state: ConductorState) -> Self {
        Self {
            version: v6::CONDUCTOR_STATE_VERSION_V6,
            conductor_gc_ran_at: state.conductor_gc_ran_at.as_unix_nanos(),
            step_instances: state
                .step_instances
                .into_iter()
                .map(|(workflow, steps)| {
                    (
                        workflow,
                        steps
                            .into_iter()
                            .map(|(step, key)| (step, v6_hash_from_derived(key)))
                            .collect(),
                    )
                })
                .collect(),
            tool_call_instances: state
                .tool_call_instances
                .into_iter()
                .map(|(k, v)| (v6_hash_from_derived(k), v.into()))
                .collect(),
        }
    }
}

impl From<v7::EnvVarSourceV7> for EnvVarSource {
    fn from(source: v7::EnvVarSourceV7) -> Self {
        match source {
            v7::EnvVarSourceV7::Literal => Self::Literal,
            v7::EnvVarSourceV7::Inherited => Self::Inherited,
        }
    }
}

impl From<EnvVarSource> for v7::EnvVarSourceV7 {
    fn from(source: EnvVarSource) -> Self {
        match source {
            EnvVarSource::Literal => Self::Literal,
            EnvVarSource::Inherited => Self::Inherited,
        }
    }
}

impl From<v7::EnvVarEntryV7> for EnvVarEntry {
    fn from(entry: v7::EnvVarEntryV7) -> Self {
        Self { hash_ck: entry.hash_ck.into(), source_nk: entry.source_nk.map(Into::into) }
    }
}

impl From<EnvVarEntry> for v7::EnvVarEntryV7 {
    fn from(entry: EnvVarEntry) -> Self {
        Self { hash_ck: entry.hash_ck.into(), source_nk: entry.source_nk.map(Into::into) }
    }
}

impl From<v7::CapturedOutputV7> for CapturedOutput {
    fn from(output: v7::CapturedOutputV7) -> Self {
        Self { capture: output.capture, hash_nk: output.hash_nk, saved_nk: output.saved_nk }
    }
}

impl From<CapturedOutput> for v7::CapturedOutputV7 {
    fn from(output: CapturedOutput) -> Self {
        Self { capture: output.capture, hash_nk: output.hash_nk, saved_nk: output.saved_nk }
    }
}

impl From<v7::StepRefV7> for StepRef {
    fn from(step: v7::StepRefV7) -> Self {
        Self { workflow_id: step.workflow_id, step_id: step.step_id }
    }
}

impl From<StepRef> for v7::StepRefV7 {
    fn from(step: StepRef) -> Self {
        Self { workflow_id: step.workflow_id, step_id: step.step_id }
    }
}

impl From<v7::ToolCallInstanceV7> for ToolCallInstance {
    fn from(inst: v7::ToolCallInstanceV7) -> Self {
        Self {
            key_version: inst.key_version,
            attempted_key_version_nk: inst.attempted_key_version_nk,
            tool_call_id: inst.tool_call_id,
            command_args: inst.command_args.into_iter().map(Into::into).collect(),
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

impl From<ToolCallInstance> for v7::ToolCallInstanceV7 {
    fn from(inst: ToolCallInstance) -> Self {
        Self {
            key_version: inst.key_version,
            attempted_key_version_nk: inst.attempted_key_version_nk,
            tool_call_id: inst.tool_call_id,
            command_args: inst.command_args.into_iter().map(Into::into).collect(),
            env_vars_ck: inst.env_vars_ck.into_iter().map(|(k, v)| (k, v.into())).collect(),
            materialized_inputs: inst.materialized_inputs,
            outputs: inst.outputs.into_iter().map(|(k, v)| (k, v.into())).collect(),
            impure_nk: inst.impure_nk,
            invalidated_at_nk: inst
                .invalidated_at_nk
                .map(|stamp| v7::WireTimestampV7(stamp.as_unix_nanos())),
            executed_at_nk: v7::WireTimestampV7(inst.executed_at_nk.as_unix_nanos()),
            last_referenced_at_nk: v7::WireTimestampV7(inst.last_referenced_at_nk.as_unix_nanos()),
            step_ids_nk: inst.step_ids_nk.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<v7::ConductorStateV7> for ConductorState {
    fn from(state: v7::ConductorStateV7) -> Self {
        Self {
            version: state.version,
            conductor_gc_ran_at: Timestamp::from_unix_nanos(state.conductor_gc_ran_at),
            step_instances: state
                .step_instances
                .into_iter()
                .map(|(workflow, steps)| {
                    (workflow, steps.into_iter().map(|(step, key)| (step, key.into())).collect())
                })
                .collect(),
            tool_call_instances: state
                .tool_call_instances
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            instance_key_secret_verifier: state.instance_key_secret_verifier.map(|verifier| {
                verifier.keyed_hash().expect("V7 deserialization validated the verifier")
            }),
        }
    }
}

impl From<ConductorState> for v7::ConductorStateV7 {
    fn from(state: ConductorState) -> Self {
        Self {
            version: v7::CONDUCTOR_STATE_VERSION_V7,
            conductor_gc_ran_at: state.conductor_gc_ran_at.as_unix_nanos(),
            step_instances: state
                .step_instances
                .into_iter()
                .map(|(workflow, steps)| {
                    (workflow, steps.into_iter().map(|(step, key)| (step, key.into())).collect())
                })
                .collect(),
            tool_call_instances: state
                .tool_call_instances
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            instance_key_secret_verifier: state
                .instance_key_secret_verifier
                .map(v7::WireKeyedHashV7::from_keyed),
        }
    }
}

impl From<v7::DerivedHashV7> for DerivedHash {
    fn from(value: v7::DerivedHashV7) -> Self {
        value.as_str().parse().expect("V7 deserialization validated the digest")
    }
}

impl From<DerivedHash> for v7::DerivedHashV7 {
    fn from(value: DerivedHash) -> Self {
        match value {
            DerivedHash::Unkeyed(hash) => Self::from_hash(hash),
            DerivedHash::Keyed(keyed) => Self::from_keyed(keyed),
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

    if v7::is_conductor_state_version_v7(version) {
        let v7_state: v7::ConductorStateV7 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7_state.into())
    } else if v6::is_conductor_state_version_v6(version) {
        let v6_state: v6::ConductorStateV6 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6_state).into())
    } else if v5::is_conductor_state_version_v5(version) {
        let v5_state: v5::ConductorStateV5 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6::from_v5_into_v6(v5_state)).into())
    } else if v4::is_conductor_state_version_v4(version) {
        let v4_state: v4::ConductorStateV4 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6::from_v5_into_v6(v5::from_v4_into_v5(v4_state))).into())
    } else if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(
            v3_state,
        ))))
        .into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(
            v3::from_v2_into_v3(v2_state),
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
             (supported: {}, {}, {}, {}, {}, {}, {})",
            v7::CONDUCTOR_STATE_VERSION_V7,
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

    if v7::is_conductor_state_version_v7(version) {
        let v7_state: v7::ConductorStateV7 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7_state.into())
    } else if v6::is_conductor_state_version_v6(version) {
        let v6_state: v6::ConductorStateV6 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6_state).into())
    } else if v5::is_conductor_state_version_v5(version) {
        let v5_state: v5::ConductorStateV5 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6::from_v5_into_v6(v5_state)).into())
    } else if v4::is_conductor_state_version_v4(version) {
        let v4_state: v4::ConductorStateV4 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6::from_v5_into_v6(v5::from_v4_into_v5(v4_state))).into())
    } else if v3::is_conductor_state_version_v3(version) {
        let v3_state: v3::ConductorStateV3 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(
            v3_state,
        ))))
        .into())
    } else if v2::is_conductor_state_version_v2(version) {
        let v2_state: v2::ConductorStateV2 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        Ok(v7::from_v6_into_v7(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(
            v3::from_v2_into_v3(v2_state),
        ))))
        .into())
    } else if v1::is_orchestration_state_version_v1(version) {
        let envelope: v1::OrchestrationStateEnvelopeV1 = serde_json::from_slice(bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        let v2_state = v2::migrate_v1_to_v2(cas, envelope).await?;
        Ok(v7::from_v6_into_v7(v6::from_v5_into_v6(v5::from_v4_into_v5(v4::from_v3_into_v4(
            v3::from_v2_into_v3(v2_state),
        ))))
        .into())
    } else {
        Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} \
             (supported: {}, {}, {}, {}, {}, {}, {})",
            v7::CONDUCTOR_STATE_VERSION_V7,
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
    let v7_state: v7::ConductorStateV7 = state.clone().into();
    serde_json::to_vec_pretty(&v7_state).map_err(|e| ConductorError::Serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mediapm_cas::Hash;
    use mediapm_utils::Timestamp;

    use super::*;
    use crate::state::{STATE_VERSION, StepRef};

    fn sample_state() -> ConductorState {
        let key = DerivedHash::Unkeyed(Hash::from_content(b"key"));
        let instance = ToolCallInstance {
            key_version: 8,
            attempted_key_version_nk: 8,
            tool_call_id: "echo@v1".to_string(),
            command_args: vec![DerivedHash::Unkeyed(Hash::from_content(b"arg"))],
            env_vars_ck: BTreeMap::from([(
                "FOO".to_string(),
                EnvVarEntry {
                    hash_ck: DerivedHash::Unkeyed(Hash::from_content(b"val")),
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
            instance_key_secret_verifier: None,
        }
    }

    /// Builds a raw V6 document so migration is exercised from the wire shape,
    /// not from a runtime value that already uses the current schema.
    fn sample_v6_state_bytes() -> Vec<u8> {
        let key = Hash::from_content(b"key");
        let state = v6::ConductorStateV6 {
            version: v6::CONDUCTOR_STATE_VERSION_V6,
            conductor_gc_ran_at: 9,
            step_instances: BTreeMap::from([(
                "default".to_string(),
                BTreeMap::from([("step-0-echo".to_string(), key)]),
            )]),
            tool_call_instances: BTreeMap::from([(
                key,
                v6::ToolCallInstanceV6 {
                    key_version: 8,
                    attempted_key_version_nk: 8,
                    tool_call_id: "echo@v1".to_string(),
                    command_args: vec![Hash::from_content(b"arg")],
                    env_vars_ck: BTreeMap::from([(
                        "FOO".to_string(),
                        v6::EnvVarEntryV6 {
                            hash_ck: Hash::from_content(b"val"),
                            source_nk: Some(v6::EnvVarSourceV6::Literal),
                        },
                    )]),
                    materialized_inputs: BTreeMap::from([(
                        "bin/tool".to_string(),
                        Hash::from_content(b"payload"),
                    )]),
                    outputs: BTreeMap::from([(
                        "stdout".to_string(),
                        v6::CapturedOutputV6 {
                            capture: "stdout".to_string(),
                            hash_nk: Some(Hash::from_content(b"out")),
                            saved_nk: true,
                        },
                    )]),
                    impure_nk: false,
                    invalidated_at_nk: Some(v6::WireTimestampV6(11)),
                    executed_at_nk: v6::WireTimestampV6(5),
                    last_referenced_at_nk: v6::WireTimestampV6(42),
                    step_ids_nk: BTreeSet::from([v6::StepRefV6 {
                        workflow_id: "default".to_string(),
                        step_id: "step-0-echo".to_string(),
                    }]),
                },
            )]),
        };
        serde_json::to_vec(&state).expect("v6 fixture encodes")
    }

    #[test]
    fn state_json_round_trip_preserves_every_recorded_field() {
        let state = sample_state();
        let encoded = encode_state_json(&state).unwrap();
        let decoded = decode_state_json(&encoded).unwrap();
        assert_eq!(decoded, state);
    }

    /// v9 keyed instance keys and the secret verifier round trip without ever
    /// storing the secret itself.
    #[test]
    fn keyed_records_and_secret_verifier_round_trip_through_v7_wire() {
        use crate::instance_key_secret::InstanceKeySecret;
        use crate::keyed_hash::KeyedHash;

        let secret = InstanceKeySecret::from_bytes_for_test([13; 32]);
        let verifier = secret.verifier();
        let key = DerivedHash::Keyed(KeyedHash::hash(&secret, "test/instance", b"key"));
        let mut state = sample_state();
        let (old_key, mut instance) =
            state.tool_call_instances.pop_first().expect("sample instance");
        instance.key_version = crate::instance_key::INSTANCE_KEY_VERSION;
        instance.attempted_key_version_nk = crate::instance_key::INSTANCE_KEY_VERSION;
        instance.command_args =
            vec![DerivedHash::Keyed(KeyedHash::hash(&secret, "test/command", b"arg"))];
        state.tool_call_instances.remove(&old_key);
        state.tool_call_instances.insert(key, instance.clone());
        state
            .step_instances
            .insert("default".to_string(), BTreeMap::from([("step-0-echo".to_string(), key)]));
        state.instance_key_secret_verifier = Some(verifier);

        let encoded = encode_state_json(&state).expect("keyed state encodes");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json reads");
        assert!(
            json["instance_key_secret_verifier"]
                .as_str()
                .expect("verifier is a string")
                .starts_with("blake3-keyed:"),
            "only the verifier, never the secret, is persisted",
        );
        assert_eq!(decode_state_json(&encoded).expect("keyed state decodes"), state);
    }

    #[test]
    fn encoded_state_declares_the_current_version() {
        let encoded = encode_state_json(&sample_state()).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["version"], serde_json::json!(STATE_VERSION));
        assert_eq!(json["version"], serde_json::json!(7));
    }

    #[test]
    fn a_v7_document_decodes_unchanged() {
        let state = sample_state();
        let encoded = encode_state_json(&state).expect("state encodes");
        assert_eq!(peek_version_marker(&encoded).expect("marker is readable"), 7);

        let decoded = decode_state_json(&encoded).expect("the v7 document decodes");
        assert_eq!(decoded, state);
        let instance = decoded.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.invalidated_at_nk, Some(Timestamp::from_unix_nanos(11)));
        assert_eq!(
            decoded.step_instances["default"]["step-0-echo"],
            decoded.tool_call_instances.keys().next().copied().unwrap()
        );
    }

    #[test]
    fn v6_state_migrates_to_v7_with_null_secret_verifier() {
        let v6 = sample_v6_state_bytes();
        let state = decode_state_json(&v6).expect("v6 state migrates");
        assert_eq!(state.version, 7);
        assert_eq!(state.instance_key_secret_verifier, None);
        let encoded = encode_state_json(&state).expect("v7 state encodes");
        let value: serde_json::Value = serde_json::from_slice(&encoded).expect("state is JSON");
        assert_eq!(value["version"], 7);
        assert!(value["instance_key_secret_verifier"].is_null());
    }

    #[test]
    fn a_v5_document_migrates_to_v7() {
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
        assert_eq!(decoded.version, 7);
        assert_eq!(decoded.conductor_gc_ran_at, Timestamp::from_unix_nanos(9));
        assert_eq!(decoded.step_instances["default"]["step-0-echo"], DerivedHash::Unkeyed(key));
        let instance = &decoded.tool_call_instances[&DerivedHash::Unkeyed(key)];
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
            BTreeMap::from([(
                String::new(),
                BTreeMap::from([("step-0-echo".to_string(), DerivedHash::Unkeyed(key))]),
            )]),
            "V7 migration backfills step_instances from instance step provenance",
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
        for supported in ["7", "6", "5", "4", "3", "2", "1"] {
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
    fn nullable_facts_are_explicit_null_in_v7_json() {
        let key = DerivedHash::Unkeyed(Hash::from_content(b"key"));
        let state = ConductorState {
            version: 7,
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
                        EnvVarEntry {
                            hash_ck: DerivedHash::Unkeyed(Hash::from_content(b"v")),
                            source_nk: None,
                        },
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
            instance_key_secret_verifier: None,
        };
        let encoded = serde_json::to_value(encode_state_json(&state).unwrap()).unwrap();
        let inst = encoded["tool_call_instances"][key.to_string()].clone();
        assert_eq!(inst["invalidated_at_nk"], serde_json::Value::Null);
        assert_eq!(inst["outputs"]["o"]["hash_nk"], serde_json::Value::Null);
        assert_eq!(inst["env_vars_ck"]["K"]["source_nk"], serde_json::Value::Null);
        assert_eq!(encoded["instance_key_secret_verifier"], serde_json::Value::Null);
    }

    #[test]
    fn v6_rejects_v5_only_fields() {
        let json = br#"{"version":6,"conductor_gc_ran_at":0,"step_instances":{},"tool_call_instances":{},"aux":{"conductor_gc_epoch":0}}"#;
        let err = decode_state_json(json).expect_err("aux must be rejected");
        assert!(err.to_string().contains("unknown") || err.to_string().contains("aux"), "{err}");
    }

    /// A state document with no version marker is refused rather than decoded.
    #[test]
    fn a_missing_version_marker_is_refused() {
        let err = peek_version_marker(br#"{"step_instances":{}}"#)
            .expect_err("a missing marker must be refused");
        assert!(
            err.to_string().contains("missing or non-numeric 'version' field"),
            "unexpected error: {err}"
        );
    }

    /// A non-numeric version marker is refused rather than coerced.
    #[test]
    fn a_non_numeric_version_marker_is_refused() {
        let err = peek_version_marker(br#"{"version":"6"}"#)
            .expect_err("a non-numeric marker must be refused");
        assert!(
            err.to_string().contains("missing or non-numeric 'version' field"),
            "unexpected error: {err}"
        );
    }

    /// A version marker beyond the `u32` range is refused.
    #[test]
    fn a_version_marker_beyond_u32_is_refused() {
        let marker = u64::from(u32::MAX) + 1;
        let json = format!(r#"{{"version":{marker}}}"#);
        let err = peek_version_marker(json.as_bytes()).expect_err("a marker beyond u32 must fail");
        assert!(err.to_string().contains("exceeds u32 range"), "unexpected error: {err}");
    }
}
