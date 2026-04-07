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
    OrchestrationState, OutputRef, PersistenceFlags, ResolvedInput, ToolCallInstance,
};

pub(crate) mod v1;

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
    pub(super) type ResolvedInput = v1::ResolvedInputV1;
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

fn persistence_flags_iso() -> IsoPrime<'static, RcBrand, latest::PersistenceFlags, PersistenceFlags>
{
    IsoPrime::new(
        |versioned: latest::PersistenceFlags| PersistenceFlags {
            save: versioned.save,
            force_full: versioned.force_full,
        },
        |runtime: PersistenceFlags| latest::PersistenceFlags {
            save: runtime.save,
            force_full: runtime.force_full,
        },
    )
}

fn resolved_input_iso() -> IsoPrime<'static, RcBrand, latest::ResolvedInput, ResolvedInput> {
    IsoPrime::new(
        |versioned: latest::ResolvedInput| ResolvedInput::from_hash(versioned.hash),
        |runtime: ResolvedInput| latest::ResolvedInput { hash: runtime.hash },
    )
}

fn output_ref_iso() -> IsoPrime<'static, RcBrand, latest::OutputRef, OutputRef> {
    IsoPrime::new(
        |versioned: latest::OutputRef| OutputRef {
            hash: versioned.hash,
            persistence: persistence_flags_iso().from(versioned.persistence),
        },
        |runtime: OutputRef| latest::OutputRef {
            hash: runtime.hash,
            persistence: persistence_flags_iso().to(runtime.persistence),
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
            impure_timestamp: versioned.impure_timestamp.map(|timestamp| ImpureTimestamp {
                epoch_seconds: timestamp.epoch_seconds,
                subsec_nanos: timestamp.subsec_nanos,
            }),
            inputs: versioned
                .inputs
                .into_iter()
                .map(|(name, input)| (name, resolved_input_iso().from(input)))
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
            inputs: runtime
                .inputs
                .into_iter()
                .map(|(name, input)| (name, resolved_input_iso().to(input)))
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

    let envelope = match dispatch_state_version(marker)? {
        OrchestrationStateLayoutVersion::V1 => {
            let envelope: latest::Envelope = serde_json::from_slice(bytes)
                .map_err(|err| ConductorError::Serialization(err.to_string()))?;
            envelope.migrate()
        }
    };

    let state = latest::version_iso().from(envelope);
    Ok(state_runtime_iso().from(state))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::model::config::{
        InputBinding, ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec,
    };
    use crate::model::state::OrchestrationState;

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

    /// Verifies persisted inputs store only hash identities, not inline bytes.
    #[test]
    fn encode_state_persists_input_hash_without_plain_content() {
        let input = crate::model::state::ResolvedInput::from_plain_content(b"abc".to_vec());
        let expected_hash = input.hash;
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
                    inputs: BTreeMap::from([("text".to_string(), input)]),
                    outputs: BTreeMap::new(),
                },
            )]),
        };

        let encoded = super::encode_state(state).expect("state should encode");
        let json: serde_json::Value =
            serde_json::from_slice(&encoded).expect("encoded state should be valid JSON");

        let input_json = &json["instances"]["instance-a"]["inputs"]["text"];
        assert_eq!(input_json.get("hash"), Some(&serde_json::json!(expected_hash)));
        assert!(input_json.get("plain_content").is_none());
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
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            ToolInputSpec {
                                default: Some(InputBinding::String("fallback".to_string())),
                                ..ToolInputSpec::default()
                            },
                        )]),
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
}
