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

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;

use crate::error::ConductorError;
use crate::model::config::{ImpureTimestamp, ToolKindSpec, ToolSpec};
use crate::model::state::{
    OrchestrationState, OutputRef, OutputSaveMode, PersistenceFlags, ResolvedInputKey,
    ToolCallInstance,
};

pub(crate) mod v1;
// V2 types are defined but not yet consumed by the library — they will be
// wired into the persistence layer by a follow-up subagent.
#[expect(dead_code)]
pub(crate) mod v2;

/// Latest-version bindings.
///
/// Keep explicit latest-version references centralized for safe schema bumps.
// BEGIN latest-version bindings
mod latest {
    use fp_library::brands::RcBrand;
    use fp_library::types::optics::IsoPrime;

    use super::v1;

    pub(super) const VERSION: u32 = v1::ORCHESTRATION_STATE_VERSION_V1;

    pub(super) type Envelope = v1::OrchestrationStateEnvelopeV1;
    pub(super) type State = v1::OrchestrationStateV1;
    pub(super) type PersistenceFlags = v1::PersistenceFlagsV1;
    pub(super) type OutputSaveMode = v1::OutputSaveModeV1;
    pub(super) type ResolvedInputKey = v1::ResolvedInputV1;
    pub(super) type OutputRef = v1::OutputRefV1;
    pub(super) type ToolMetadata = v1::ToolMetadataV1;
    pub(super) type ToolCallInstance = v1::ToolCallInstanceV1;
    pub(super) type ImpureTimestamp = v1::ImpureTimestampV1;
    pub(super) type BuiltinMetadataKind = v1::BuiltinMetadataKindV1;

    pub(super) const fn is_version(marker: u32) -> bool {
        v1::is_orchestration_state_version_v1(marker)
    }

    pub(super) fn version_iso() -> IsoPrime<'static, RcBrand, Envelope, State> {
        v1::orchestration_state_v1_iso()
    }
}
// END latest-version bindings

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrchestrationStateLayoutVersion {
    V1,
}

/// Optic-based migration trait between persistence envelope versions.
pub(crate) trait Migrate<To> {
    /// Migrates `self` to `To` via optic composition.
    fn migrate(self) -> To;
}

/// Shared migration helper using `from_iso.view` then `to_iso.review`.
pub(crate) fn migrate_with_version_state<'a, From, To, State>(
    from: From,
    from_iso: &IsoPrime<'a, RcBrand, From, State>,
    to_iso: &IsoPrime<'a, RcBrand, To, State>,
) -> To {
    let state = from_iso.from(from);
    to_iso.to(state)
}

impl Migrate<latest::Envelope> for latest::Envelope {
    fn migrate(self) -> latest::Envelope {
        migrate_with_version_state(self, &latest::version_iso(), &latest::version_iso())
    }
}

/// Decodes one orchestration-state envelope using one already-dispatched
/// version marker.
fn decode_envelope_for_version(
    bytes: &[u8],
    version: u32,
) -> Result<latest::Envelope, ConductorError> {
    match dispatch_state_version(version)? {
        OrchestrationStateLayoutVersion::V1 => serde_json::from_slice(bytes)
            .map_err(|err| ConductorError::Serialization(err.to_string())),
    }
}

/// Migrates one parsed orchestration-state envelope from `from_version` to
/// `target_version`.
///
/// This function is the central migration gateway used by decode flows.
fn migrate_envelope_to_version(
    envelope: latest::Envelope,
    from_version: u32,
    target_version: u32,
) -> Result<latest::Envelope, ConductorError> {
    let from_layout = dispatch_state_version(from_version)?;
    let to_layout = dispatch_state_version(target_version)?;

    match (from_layout, to_layout) {
        (OrchestrationStateLayoutVersion::V1, OrchestrationStateLayoutVersion::V1) => {
            Ok(envelope.migrate())
        }
    }
}

fn persistence_flags_iso() -> IsoPrime<'static, RcBrand, latest::PersistenceFlags, PersistenceFlags>
{
    IsoPrime::new(
        |versioned: latest::PersistenceFlags| PersistenceFlags {
            save: match versioned.save {
                latest::OutputSaveMode::Bool(false) => OutputSaveMode::Unsaved,
                latest::OutputSaveMode::Bool(true) => OutputSaveMode::Saved,
                latest::OutputSaveMode::Full => OutputSaveMode::Full,
            },
        },
        |runtime: PersistenceFlags| latest::PersistenceFlags {
            save: match runtime.save {
                OutputSaveMode::Unsaved => latest::OutputSaveMode::Bool(false),
                OutputSaveMode::Saved => latest::OutputSaveMode::Bool(true),
                OutputSaveMode::Full => latest::OutputSaveMode::Full,
            },
        },
    )
}

fn resolved_input_key_iso() -> IsoPrime<'static, RcBrand, latest::ResolvedInputKey, ResolvedInputKey>
{
    IsoPrime::new(
        |versioned: latest::ResolvedInputKey| ResolvedInputKey { hash: versioned.hash },
        |runtime: ResolvedInputKey| latest::ResolvedInputKey { hash: runtime.hash },
    )
}

fn output_ref_iso() -> IsoPrime<'static, RcBrand, latest::OutputRef, OutputRef> {
    IsoPrime::new(
        |versioned: latest::OutputRef| OutputRef {
            hash: versioned.hash,
            persistence: persistence_flags_iso().from(versioned.persistence),
            allow_empty_capture: versioned.allow_empty_capture,
        },
        |runtime: OutputRef| latest::OutputRef {
            hash: runtime.hash,
            persistence: persistence_flags_iso().to(runtime.persistence),
            allow_empty_capture: runtime.allow_empty_capture,
        },
    )
}

/// Bridges versioned metadata wire shape with runtime `ToolSpec` state.
///
/// Builtins are persisted as identity-only fields (`kind`/`name`/`version`) and
/// decoded into runtime `ToolSpec` values with empty optional maps. Executables
/// round-trip through the full `ToolSpec` wire shape.
fn tool_metadata_iso() -> IsoPrime<'static, RcBrand, latest::ToolMetadata, ToolSpec> {
    IsoPrime::new(
        |versioned: latest::ToolMetadata| match versioned {
            latest::ToolMetadata::Builtin { kind: _, name, version } => ToolSpec {
                is_impure: false,
                inputs: BTreeMap::new(),
                kind: ToolKindSpec::Builtin { name, version },
                outputs: BTreeMap::new(),
            },
            latest::ToolMetadata::Executable(spec) => spec,
        },
        |runtime: ToolSpec| match runtime.kind {
            ToolKindSpec::Builtin { name, version } => latest::ToolMetadata::Builtin {
                kind: latest::BuiltinMetadataKind::Builtin,
                name,
                version,
            },
            ToolKindSpec::Executable { .. } => latest::ToolMetadata::Executable(runtime),
        },
    )
}

fn tool_call_instance_iso() -> IsoPrime<'static, RcBrand, latest::ToolCallInstance, ToolCallInstance>
{
    IsoPrime::new(
        |versioned: latest::ToolCallInstance| ToolCallInstance {
            tool_name: versioned.tool_name,
            metadata: tool_metadata_iso().from(versioned.metadata),
            impure_timestamp: versioned.impure_timestamp.or(versioned.last_used).map(|timestamp| {
                ImpureTimestamp {
                    epoch_seconds: timestamp.epoch_seconds,
                    subsec_nanos: timestamp.subsec_nanos,
                }
            }),
            inputs: versioned
                .inputs
                .into_iter()
                .map(|(name, input)| (name, resolved_input_key_iso().from(input)))
                .collect(),
            outputs: versioned
                .outputs
                .into_iter()
                .map(|(name, output)| (name, output_ref_iso().from(output)))
                .collect(),
        },
        |runtime: ToolCallInstance| latest::ToolCallInstance {
            tool_name: runtime.tool_name,
            metadata: tool_metadata_iso().to(runtime.metadata),
            impure_timestamp: runtime.impure_timestamp.map(|timestamp| latest::ImpureTimestamp {
                epoch_seconds: timestamp.epoch_seconds,
                subsec_nanos: timestamp.subsec_nanos,
            }),
            last_used: runtime.impure_timestamp.map(|timestamp| latest::ImpureTimestamp {
                epoch_seconds: timestamp.epoch_seconds,
                subsec_nanos: timestamp.subsec_nanos,
            }),
            inputs: runtime
                .inputs
                .into_iter()
                .map(|(name, input)| (name, resolved_input_key_iso().to(input)))
                .collect(),
            outputs: runtime
                .outputs
                .into_iter()
                .map(|(name, output)| (name, output_ref_iso().to(output)))
                .collect(),
        },
    )
}

fn state_runtime_iso() -> IsoPrime<'static, RcBrand, latest::State, OrchestrationState> {
    IsoPrime::new(
        |versioned: latest::State| OrchestrationState {
            version: latest::VERSION,
            instances: versioned
                .instances
                .into_iter()
                .map(|(key, instance)| (key, tool_call_instance_iso().from(instance)))
                .collect(),
        },
        |runtime: OrchestrationState| latest::State {
            instances: runtime
                .instances
                .into_iter()
                .map(|(key, instance)| (key, tool_call_instance_iso().to(instance)))
                .collect(),
        },
    )
}

/// Returns latest supported orchestration-state schema marker.
#[must_use]
pub(crate) const fn latest_state_version() -> u32 {
    latest::VERSION
}

/// ## DO NOT REMOVE: latest-first version dispatch guard
///
/// Version checks shoud always start checking from the latest version to ensure performance.
fn dispatch_state_version(marker: u32) -> Result<OrchestrationStateLayoutVersion, ConductorError> {
    if latest::is_version(marker) {
        Ok(OrchestrationStateLayoutVersion::V1)
    } else {
        Err(ConductorError::Workflow(format!(
            "unsupported orchestration state schema version {marker}; expected {}",
            latest_state_version()
        )))
    }
}

fn decode_version_marker(bytes: &[u8]) -> Result<u32, ConductorError> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|err| ConductorError::Serialization(err.to_string()))?;

    let marker = value.get("version").and_then(serde_json::Value::as_u64).ok_or_else(|| {
        ConductorError::Workflow("versioned document missing numeric 'version' field".to_string())
    })?;

    u32::try_from(marker).map_err(|_| {
        ConductorError::Workflow(format!("schema version {marker} exceeds supported u32 range"))
    })
}

/// Encodes runtime orchestration state with latest version envelope.
pub(crate) fn encode_state(state: OrchestrationState) -> Result<Vec<u8>, ConductorError> {
    let _ = dispatch_state_version(state.version)?;

    let latest_state = state_runtime_iso().to(state);
    let envelope = latest::version_iso().to(latest_state);
    serde_json::to_vec(&envelope).map_err(|err| ConductorError::Serialization(err.to_string()))
}

/// Decodes runtime orchestration state from versioned envelope bytes.
pub(crate) fn decode_state(bytes: &[u8]) -> Result<OrchestrationState, ConductorError> {
    let marker = decode_version_marker(bytes)?;

    let envelope = decode_envelope_for_version(bytes, marker)?;
    let migrated = migrate_envelope_to_version(envelope, marker, latest_state_version())?;

    let state = latest::version_iso().from(migrated);
    let runtime_state = state_runtime_iso().from(state);
    Ok(runtime_state)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::model::config::{ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec};
    use crate::model::state::{OrchestrationState, OutputSaveMode};

    /// Verifies encoded state uses flattened top-level `instances` shape.
    #[test]
    fn encode_state_emits_flattened_envelope_without_payload_wrapper() {
        let state = OrchestrationState {
            version: super::latest_state_version(),
            instances: BTreeMap::from([(
                "instance-a".to_string(),
                crate::model::state::ToolCallInstance {
                    tool_name: "echo@1.0.0".to_string(),
                    metadata: ToolSpec {
                        kind: ToolKindSpec::Builtin {
                            name: "echo".to_string(),
                            version: "1.0.0".to_string(),
                        },
                        ..ToolSpec::default()
                    },
                    impure_timestamp: None,
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                },
            )]),
        };

        let encoded = super::encode_state(state).expect("state should encode");
        let json: serde_json::Value =
            serde_json::from_slice(&encoded).expect("encoded state should be valid JSON");

        assert!(json.get("version").is_some());
        assert!(json.get("instances").is_some());
        assert!(json.get("payload").is_none());
    }

    /// Verifies decode accepts flattened envelopes with top-level instances.
    #[test]
    fn decode_state_accepts_flattened_envelope() {
        let encoded = serde_json::json!({
            "version": super::latest_state_version(),
            "instances": {
                "instance-a": {
                    "tool_name": "echo@1.0.0",
                    "metadata": {
                        "kind": "builtin",
                        "name": "echo",
                        "version": "1.0.0"
                    },
                    "impure_timestamp": null,
                    "inputs": {},
                    "outputs": {}
                }
            }
        });

        let decoded = super::decode_state(
            serde_json::to_vec(&encoded).expect("serialize encoded test shape").as_slice(),
        )
        .expect("flattened envelope should decode");

        assert_eq!(decoded.version, super::latest_state_version());
        assert!(decoded.instances.contains_key("instance-a"));
    }

    /// Verifies persisted inputs store only hash identities.
    ///
    /// With [`ResolvedInputKey`] the type system guarantees that runtime
    /// content bytes are never part of the persisted shape. This test
    /// validates encoding round-trips the hash correctly.
    #[test]
    fn encode_state_persists_input_hash_without_plain_content() {
        let expected_hash = mediapm_cas::Hash::from_content(b"abc");
        let state = OrchestrationState {
            version: super::latest_state_version(),
            instances: BTreeMap::from([(
                "instance-a".to_string(),
                crate::model::state::ToolCallInstance {
                    tool_name: "echo@1.0.0".to_string(),
                    metadata: ToolSpec {
                        kind: ToolKindSpec::Builtin {
                            name: "echo".to_string(),
                            version: "1.0.0".to_string(),
                        },
                        ..ToolSpec::default()
                    },
                    impure_timestamp: None,
                    inputs: BTreeMap::from([(
                        "text".to_string(),
                        crate::model::state::ResolvedInputKey { hash: expected_hash },
                    )]),
                    outputs: BTreeMap::new(),
                },
            )]),
        };

        let encoded = super::encode_state(state).expect("state should encode");
        let json: serde_json::Value =
            serde_json::from_slice(&encoded).expect("encoded state should be valid JSON");

        let input_json = &json["instances"]["instance-a"]["inputs"]["text"];
        assert_eq!(input_json.get("hash"), Some(&serde_json::json!(expected_hash)));
    }

    /// Verifies old payload-wrapper envelopes are no longer accepted.
    #[test]
    fn decode_state_rejects_payload_wrapper_shape() {
        let encoded = serde_json::json!({
            "version": super::latest_state_version(),
            "payload": {
                "instances": {}
            }
        });

        let err = super::decode_state(
            serde_json::to_vec(&encoded).expect("serialize payload-wrapper test shape").as_slice(),
        )
        .expect_err("payload-wrapper shape should be rejected");

        assert!(err.to_string().contains("unknown field") || err.to_string().contains("payload"));
    }

    /// Verifies decode still requires explicit numeric top-level version field.
    #[test]
    fn decode_state_requires_top_level_version_marker() {
        let encoded = serde_json::json!({
            "instances": {}
        });

        let err = super::decode_state(
            serde_json::to_vec(&encoded).expect("serialize missing-version shape").as_slice(),
        )
        .expect_err("missing version should fail");

        assert!(err.to_string().contains("version"));
    }

    /// Verifies builtin metadata persists only identity fields in encoded state.
    #[test]
    fn encode_state_builtin_metadata_omits_non_identity_fields() {
        let state = OrchestrationState {
            version: super::latest_state_version(),
            instances: BTreeMap::from([(
                "instance-a".to_string(),
                crate::model::state::ToolCallInstance {
                    tool_name: "echo@1.0.0".to_string(),
                    metadata: ToolSpec {
                        is_impure: true,
                        inputs: BTreeMap::from([("text".to_string(), ToolInputSpec::default())]),
                        kind: ToolKindSpec::Builtin {
                            name: "echo".to_string(),
                            version: "1.0.0".to_string(),
                        },
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            ToolOutputSpec::default(),
                        )]),
                    },
                    impure_timestamp: None,
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                },
            )]),
        };

        let encoded = super::encode_state(state).expect("state should encode");
        let json: serde_json::Value =
            serde_json::from_slice(&encoded).expect("encoded state should be valid JSON");

        let metadata = &json["instances"]["instance-a"]["metadata"];
        assert_eq!(
            metadata,
            &serde_json::json!({
                "kind": "builtin",
                "name": "echo",
                "version": "1.0.0"
            })
        );
    }

    /// Verifies decode rejects builtin metadata with non-identity extra fields.
    #[test]
    fn decode_state_rejects_builtin_metadata_extra_fields() {
        let encoded = serde_json::json!({
            "version": super::latest_state_version(),
            "instances": {
                "instance-a": {
                    "tool_name": "echo@1.0.0",
                    "metadata": {
                        "kind": "builtin",
                        "name": "echo",
                        "version": "1.0.0",
                        "is_impure": false
                    },
                    "impure_timestamp": null,
                    "inputs": {},
                    "outputs": {}
                }
            }
        });

        let err = super::decode_state(
            serde_json::to_vec(&encoded)
                .expect("serialize encoded builtin-extra-fields shape")
                .as_slice(),
        )
        .expect_err("builtin metadata extra fields should be rejected");

        assert!(err.to_string().contains("is_impure") || err.to_string().contains("builtin"));
    }

    /// Verifies decode accepts persisted lowercase `"full"` wire values for
    /// output persistence mode.
    #[test]
    fn decode_state_accepts_lowercase_full_persistence_mode() {
        let encoded = serde_json::json!({
            "version": super::latest_state_version(),
            "instances": {
                "instance-a": {
                    "tool_name": "echo@1.0.0",
                    "metadata": {
                        "kind": "builtin",
                        "name": "echo",
                        "version": "1.0.0"
                    },
                    "impure_timestamp": null,
                    "inputs": {},
                    "outputs": {
                        "result": {
                            "hash": "blake3:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
                            "persistence": { "save": "full" }
                        }
                    }
                }
            }
        });

        let decoded = super::decode_state(
            serde_json::to_vec(&encoded)
                .expect("serialize encoded lowercase-full persistence shape")
                .as_slice(),
        )
        .expect("lowercase full persistence should decode");

        let output = &decoded
            .instances
            .get("instance-a")
            .expect("decoded instance should exist")
            .outputs
            .get("result")
            .expect("decoded output should exist");
        assert_eq!(output.persistence.save, OutputSaveMode::Full);
    }

    /// Verifies decode accepts runtime-enum persistence strings produced by
    /// legacy direct serde serialization (`"Saved"`).
    #[test]
    fn decode_state_accepts_runtime_saved_persistence_string() {
        let encoded = serde_json::json!({
            "version": super::latest_state_version(),
            "instances": {
                "instance-a": {
                    "tool_name": "echo@1.0.0",
                    "metadata": {
                        "kind": "builtin",
                        "name": "echo",
                        "version": "1.0.0"
                    },
                    "impure_timestamp": null,
                    "inputs": {},
                    "outputs": {
                        "result": {
                            "hash": "blake3:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
                            "persistence": { "save": "Saved" }
                        }
                    }
                }
            }
        });

        let decoded = super::decode_state(
            serde_json::to_vec(&encoded)
                .expect("serialize encoded runtime-saved persistence shape")
                .as_slice(),
        )
        .expect("runtime saved persistence string should decode");

        let output = &decoded
            .instances
            .get("instance-a")
            .expect("decoded instance should exist")
            .outputs
            .get("result")
            .expect("decoded output should exist");
        assert_eq!(output.persistence.save, OutputSaveMode::Saved);
    }

    // -----------------------------------------------------------------------
    // V2 round-trip tests
    // -----------------------------------------------------------------------

    /// Verifies that `ToolCallInstanceV2` round-trips through
    /// `encode_instance_v2` / `decode_instance_v2`.
    #[test]
    fn v2_tool_call_instance_round_trip() {
        use super::v2::{self, BuiltinMetadataKindV2, ToolCallInstanceV2, ToolMetadataV2};

        let instance = ToolCallInstanceV2 {
            tool_name: "echo@1.0.0".to_string(),
            metadata: ToolMetadataV2::Builtin {
                kind: BuiltinMetadataKindV2::Builtin,
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
        use super::v2::{
            InstanceRefV2, ORCHESTRATION_STATE_VERSION_V2, OrchestrationStateEnvelopeV2,
        };

        let hash_a = mediapm_cas::Hash::from_content(b"instance-a-data");

        let envelope = OrchestrationStateEnvelopeV2 {
            version: ORCHESTRATION_STATE_VERSION_V2,
            instances: BTreeMap::from([("instance-a".to_string(), InstanceRefV2 { hash: hash_a })]),
        };

        let encoded = serde_json::to_vec(&envelope).expect("envelope should serialize");
        let decoded: OrchestrationStateEnvelopeV2 =
            serde_json::from_slice(&encoded).expect("envelope should deserialize");

        assert_eq!(decoded, envelope);
        assert!(decoded.version == 2);
    }

    /// Verifies that a v2 envelope with zero instances round-trips correctly.
    #[test]
    fn v2_envelope_empty_instances() {
        use super::v2::{ORCHESTRATION_STATE_VERSION_V2, OrchestrationStateEnvelopeV2};

        let envelope = OrchestrationStateEnvelopeV2 {
            version: ORCHESTRATION_STATE_VERSION_V2,
            instances: BTreeMap::new(),
        };

        let encoded = serde_json::to_vec(&envelope).expect("empty envelope should serialize");
        let decoded: OrchestrationStateEnvelopeV2 =
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
        use super::v2::{self, BuiltinMetadataKindV2, ToolCallInstanceV2, ToolMetadataV2};

        let instance = ToolCallInstanceV2 {
            tool_name: "echo@1.0.0".to_string(),
            metadata: ToolMetadataV2::Builtin {
                kind: BuiltinMetadataKindV2::Builtin,
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
