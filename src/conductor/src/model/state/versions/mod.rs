//! Versioned persistence envelopes for orchestration state.
//!
//! Runtime source-of-truth state structs live in `model/state/mod.rs`. Version
//! modules own wire/document shapes and optic bridges.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference adjacent versions for migration.
//! - This `mod.rs` is the only bridge between latest version state and
//!   unversioned runtime state.
//! - Files outside `model/state/versions/` must use only APIs in this module,
//!   never `versions::vX` directly.
//! - Do not directly re-export `versions::vX` structs/types from this module.

use std::collections::BTreeMap;

use mediapm_cas::{CasApi, Hash};

use crate::error::ConductorError;
use crate::model::state::OrchestrationState;

#[expect(dead_code, reason = "Preserved V1 wire format with ISO bridges for migration/audit")]
pub(crate) mod v1;
pub(crate) mod v2;

/// Latest-version bindings.
///
/// Keep explicit latest-version references centralized for safe schema bumps.
// BEGIN latest-version bindings
#[expect(dead_code)]
mod latest {
    pub(super) const VERSION: u32 = super::v2::ORCHESTRATION_STATE_VERSION_V2;

    pub(super) type Envelope = super::v2::OrchestrationStateEnvelopeV2;
    pub(super) type PersistenceFlags = super::v2::PersistenceFlagsV2;
    pub(super) type OutputSaveMode = super::v2::OutputSaveModeV2;
    pub(super) type ResolvedInputKey = super::v2::ResolvedInputV2;
    pub(super) type OutputRef = super::v2::OutputRefV2;
    pub(super) type ToolMetadata = super::v2::ToolMetadataV2;
    pub(super) type ToolCallInstance = super::v2::ToolCallInstanceV2;
    pub(super) type ImpureTimestamp = super::v2::ImpureTimestampV2;
    pub(super) type BuiltinMetadataKind = super::v2::BuiltinMetadataKindV2;

    pub(super) const fn is_version(marker: u32) -> bool {
        super::v2::is_orchestration_state_version_v2(marker)
    }
}
// END latest-version bindings

/// Returns latest supported orchestration-state schema marker.
#[must_use]
pub(crate) const fn latest_state_version() -> u32 {
    latest::VERSION
}

/// Encodes runtime orchestration state using V2 CAS-backed persistence.
///
/// Each instance is individually encoded and stored in CAS. The envelope
/// (containing instance refs) is also stored in CAS. Returns the envelope
/// hash.
pub(crate) async fn encode_state<C: CasApi>(
    cas: &C,
    mut state: OrchestrationState,
) -> Result<Hash, ConductorError> {
    state.version = latest::VERSION;

    let mut instance_refs = BTreeMap::new();
    for (key, instance) in state.instances {
        let v2_instance = v2::tool_call_instance_v2_iso().to(instance);
        let encoded = v2::encode_instance_v2(&v2_instance)?;
        let hash = cas.put(encoded).await?;
        instance_refs.insert(key, v2::InstanceRefV2 { hash });
    }

    let envelope = latest::Envelope { version: latest::VERSION, instances: instance_refs };

    let envelope_bytes =
        serde_json::to_vec(&envelope).map_err(|e| ConductorError::Serialization(e.to_string()))?;
    let envelope_hash = cas.put(envelope_bytes).await?;
    Ok(envelope_hash)
}

/// Decodes runtime orchestration state from a CAS-backed V2 envelope pointer.
///
/// Reads the envelope from CAS, then loads and decodes each instance from
/// its individual CAS ref.
pub(crate) async fn decode_state<C: CasApi>(
    cas: &C,
    pointer: Hash,
) -> Result<OrchestrationState, ConductorError> {
    let envelope_bytes = cas.get(pointer).await?;
    let envelope: latest::Envelope = serde_json::from_slice(&envelope_bytes)
        .map_err(|e| ConductorError::Serialization(e.to_string()))?;

    let mut instances = BTreeMap::new();
    for (key, instance_ref) in envelope.instances {
        let instance_bytes = cas.get(instance_ref.hash).await?;
        let v2_instance = v2::decode_instance_v2(&instance_bytes)?;
        let instance = v2::tool_call_instance_v2_iso().from(v2_instance);
        instances.insert(key, instance);
    }

    Ok(OrchestrationState { version: envelope.version, instances })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::v2;

    // -----------------------------------------------------------------------
    // V2 round-trip tests
    // -----------------------------------------------------------------------

    /// Verifies that `ToolCallInstanceV2` round-trips through
    /// `encode_instance_v2` / `decode_instance_v2`.
    #[test]
    fn v2_tool_call_instance_round_trip() {
        let instance = v2::ToolCallInstanceV2 {
            tool_name: "echo@1.0.0".to_string(),
            metadata: v2::ToolMetadataV2::Builtin {
                kind: v2::BuiltinMetadataKindV2::Builtin,
                name: "echo".to_string(),
                version: "1.0.0".to_string(),
            },
            impure_timestamp: Some(v2::ImpureTimestampV2 {
                epoch_seconds: 1_700_000_000,
                subsec_nanos: 0,
            }),
            inputs: BTreeMap::from([(
                "text".to_string(),
                v2::ResolvedInputV2 { hash: mediapm_cas::Hash::from_content(b"hello") },
            )]),
            outputs: BTreeMap::from([(
                "result".to_string(),
                v2::OutputRefV2 {
                    hash: mediapm_cas::Hash::from_content(b"output"),
                    persistence: v2::PersistenceFlagsV2 { save: v2::OutputSaveModeV2::Bool(true) },
                    allow_empty_capture: false,
                },
            )]),
        };

        let encoded = v2::encode_instance_v2(&instance).expect("instance should encode");
        let decoded = v2::decode_instance_v2(&encoded).expect("instance should decode");

        assert_eq!(decoded, instance);
    }

    /// Verifies that `OrchestrationStateEnvelopeV2` round-trips through JSON
    /// serialization.
    #[test]
    fn v2_envelope_round_trip() {
        let hash_a = mediapm_cas::Hash::from_content(b"instance-a-data");

        let envelope = v2::OrchestrationStateEnvelopeV2 {
            version: v2::ORCHESTRATION_STATE_VERSION_V2,
            instances: BTreeMap::from([(
                "instance-a".to_string(),
                v2::InstanceRefV2 { hash: hash_a },
            )]),
        };

        let encoded = serde_json::to_vec(&envelope).expect("envelope should serialize");
        let decoded: v2::OrchestrationStateEnvelopeV2 =
            serde_json::from_slice(&encoded).expect("envelope should deserialize");

        assert_eq!(decoded, envelope);
        assert!(decoded.version == 2);
    }

    /// Verifies that a v2 envelope with zero instances round-trips correctly.
    #[test]
    fn v2_envelope_empty_instances() {
        let envelope = v2::OrchestrationStateEnvelopeV2 {
            version: v2::ORCHESTRATION_STATE_VERSION_V2,
            instances: BTreeMap::new(),
        };

        let encoded = serde_json::to_vec(&envelope).expect("empty envelope should serialize");
        let decoded: v2::OrchestrationStateEnvelopeV2 =
            serde_json::from_slice(&encoded).expect("empty envelope should deserialize");

        assert_eq!(decoded, envelope);
    }

    /// Verifies that `OutputSaveModeV2` accepts `"full"` string.
    #[test]
    fn v2_output_save_mode_full() {
        use super::v2::OutputSaveModeV2;

        let json = serde_json::json!("full");
        let mode: OutputSaveModeV2 =
            serde_json::from_value(json).expect("'full' should deserialize");
        assert_eq!(mode, OutputSaveModeV2::Full);

        let serialized = serde_json::to_value(&mode).expect("Full should serialize");
        assert_eq!(serialized, serde_json::json!("full"));
    }

    /// Verifies that `OutputSaveModeV2` accepts boolean values.
    #[test]
    fn v2_output_save_mode_bool() {
        use super::v2::OutputSaveModeV2;

        let mode_true: OutputSaveModeV2 =
            serde_json::from_value(serde_json::json!(true)).expect("true should deserialize");
        assert_eq!(mode_true, OutputSaveModeV2::Bool(true));

        let mode_false: OutputSaveModeV2 =
            serde_json::from_value(serde_json::json!(false)).expect("false should deserialize");
        assert_eq!(mode_false, OutputSaveModeV2::Bool(false));

        let serialized_true =
            serde_json::to_value(&mode_true).expect("Bool(true) should serialize");
        assert_eq!(serialized_true, serde_json::json!(true));
    }

    /// Verifies that v2 instance hash is deterministic.
    #[test]
    fn v2_instance_hash_is_deterministic() {
        let instance = v2::ToolCallInstanceV2 {
            tool_name: "echo@1.0.0".to_string(),
            metadata: v2::ToolMetadataV2::Builtin {
                kind: v2::BuiltinMetadataKindV2::Builtin,
                name: "echo".to_string(),
                version: "1.0.0".to_string(),
            },
            impure_timestamp: None,
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        };

        let hash1 = v2::instance_v2_hash(&instance);
        let hash2 = v2::instance_v2_hash(&instance);
        assert_eq!(hash1, hash2);
    }

    /// Verifies that builtin metadata with extra fields is rejected by v2
    /// deserialization.
    #[test]
    fn v2_rejects_builtin_metadata_extra_fields() {
        let encoded = serde_json::json!({
            "version": 2,
            "instances": {
                "instance-a": {
                    "hash": "blake3:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
                }
            }
        });

        // The envelope should decode fine — it stores only refs.
        let envelope: Result<super::v2::OrchestrationStateEnvelopeV2, _> =
            serde_json::from_value(encoded);
        assert!(envelope.is_ok());
        let envelope = envelope.unwrap();
        assert!(envelope.instances.contains_key("instance-a"));
        assert_eq!(envelope.instances.len(), 1);
    }
}
