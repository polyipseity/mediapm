//! Version 2 persistence envelopes for orchestration state.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This module owns only V2 persisted shapes.
//! - It must not import unversioned runtime orchestration-state structs.
//! - Cross-version migration (when added) may only reference adjacent versions
//!   via optic composition.

use std::collections::BTreeMap;
use std::fmt;

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use mediapm_cas::Hash;
use serde::de::{self, Error as _, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ConductorError;
use crate::model::config::{ToolKindSpec, ToolSpec};

/// V2 orchestration-state schema marker.
pub const ORCHESTRATION_STATE_VERSION_V2: u32 = 2;

/// Returns whether `marker` matches orchestration-state V2 schema marker.
#[must_use]
pub const fn is_orchestration_state_version_v2(marker: u32) -> bool {
    marker == ORCHESTRATION_STATE_VERSION_V2
}

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

/// V2 persistence flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceFlagsV2 {
    /// Effective tri-state save policy.
    pub save: OutputSaveModeV2,
}

/// V2 persisted tri-state save mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSaveModeV2 {
    /// Boolean save policy (`false` or `true`).
    Bool(bool),
    /// Full-save policy keyword.
    Full,
}

impl Serialize for OutputSaveModeV2 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Full => serializer.serialize_str("full"),
        }
    }
}

impl<'de> Deserialize<'de> for OutputSaveModeV2 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OutputSaveModeV2Visitor;

        impl Visitor<'_> for OutputSaveModeV2Visitor {
            type Value = OutputSaveModeV2;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "a boolean save mode, or one of the strings \"full\", \"saved\", \"unsaved\"",
                )
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OutputSaveModeV2::Bool(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value.eq_ignore_ascii_case("full") {
                    return Ok(OutputSaveModeV2::Full);
                }

                if value.eq_ignore_ascii_case("saved") || value.eq_ignore_ascii_case("true") {
                    return Ok(OutputSaveModeV2::Bool(true));
                }

                if value.eq_ignore_ascii_case("unsaved") || value.eq_ignore_ascii_case("false") {
                    return Ok(OutputSaveModeV2::Bool(false));
                }

                Err(E::invalid_value(de::Unexpected::Str(value), &self))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_any(OutputSaveModeV2Visitor)
    }
}

/// Structured timezone-independent impure timestamp used by the V2 wire shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImpureTimestampV2 {
    /// Whole UTC seconds since Unix epoch.
    pub epoch_seconds: u64,
    /// Nanoseconds within the current second.
    pub subsec_nanos: u32,
}

/// V2 CAS pointer to a resolved input payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedInputV2 {
    /// CAS hash identity for the resolved input payload.
    pub hash: Hash,
}

/// V2 output reference record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRefV2 {
    /// Output hash.
    pub hash: Hash,
    /// Effective persistence flags.
    pub persistence: PersistenceFlagsV2,
    /// Whether this output was captured as intentionally empty.
    ///
    /// Defaults to `false` for compatibility with state written before this
    /// field was introduced.
    #[serde(default)]
    pub allow_empty_capture: bool,
}

/// V2 CAS pointer to one tool-call instance blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceRefV2 {
    /// CAS hash of the encoded `ToolCallInstanceV2` blob.
    pub hash: Hash,
}

/// V2 envelope-level auxiliary metadata for one tool-call instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuxDataV2 {
    /// When this instance was last confirmed reachable from GC roots.
    #[serde(default)]
    pub last_reachable: Option<ImpureTimestampV2>,
}

/// Builtin metadata kind marker used by orchestration-state V2 wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinMetadataKindV2 {
    /// Builtin tool metadata projection marker.
    Builtin,
}

/// V2 state metadata shape.
///
/// Builtins persist only identity fields (`kind`/`name`/`version`).
/// Executables retain full `ToolSpec` shape.
///
/// Decode invariants:
/// - builtin metadata must be exactly `{ kind, name, version }`,
/// - any additional builtin fields are rejected,
/// - executable metadata continues to decode through `ToolSpec`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ToolMetadataV2 {
    /// Identity-only metadata for builtin tools.
    Builtin {
        /// Builtin-kind marker (`"builtin"`).
        kind: BuiltinMetadataKindV2,
        /// Builtin name.
        name: String,
        /// Builtin semantic version.
        version: String,
    },
    /// Full metadata payload for executable tools.
    Executable(ToolSpec),
}

impl<'de> Deserialize<'de> for ToolMetadataV2 {
    /// Decodes one V2 metadata record with explicit `kind` dispatch.
    ///
    /// This custom implementation keeps builtin metadata strict by rejecting
    /// unknown fields for `kind = "builtin"` while preserving executable
    /// decoding through `ToolSpec`.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Wire shape for strict builtin metadata decoding.
        #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct BuiltinMetadataWireV2 {
            /// Builtin kind marker.
            kind: BuiltinMetadataKindV2,
            /// Builtin name.
            name: String,
            /// Builtin semantic version.
            version: String,
        }

        let value = serde_json::Value::deserialize(deserializer)?;
        let kind = value
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| D::Error::custom("tool metadata must define string field 'kind'"))?;

        match kind {
            "builtin" => {
                let builtins: BuiltinMetadataWireV2 =
                    serde_json::from_value(value).map_err(D::Error::custom)?;
                Ok(Self::Builtin {
                    kind: builtins.kind,
                    name: builtins.name,
                    version: builtins.version,
                })
            }
            "executable" => {
                let spec: ToolSpec = serde_json::from_value(value).map_err(D::Error::custom)?;
                match spec.kind {
                    ToolKindSpec::Executable { .. } => Ok(Self::Executable(spec)),
                    ToolKindSpec::Builtin { .. } => Err(D::Error::custom(
                        "executable metadata must decode to executable tool kind",
                    )),
                }
            }
            other => Err(D::Error::custom(format!("unsupported tool metadata kind '{other}'"))),
        }
    }
}

// ---------------------------------------------------------------------------
// Core V2 types
// ---------------------------------------------------------------------------

/// V2 tool-call instance record.
///
/// Differs from V1 by omitting the `last_used` field — GC metadata moves to
/// the envelope level in future versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallInstanceV2 {
    /// Immutable tool map key used by the workflow step.
    pub tool_name: String,
    /// Tool metadata persisted in normalized V2 shape.
    pub metadata: ToolMetadataV2,
    /// Optional machine-managed impurity timestamp.
    #[serde(default)]
    pub impure_timestamp: Option<ImpureTimestampV2>,
    /// Resolved inputs.
    #[serde(default)]
    pub inputs: BTreeMap<String, ResolvedInputV2>,
    /// Output references.
    #[serde(default)]
    pub outputs: BTreeMap<String, OutputRefV2>,
}

/// V2 orchestration-state persistence envelope.
///
/// Stores only instance refs (CAS pointers) instead of inline instance data.
/// Individual instance blobs are stored separately and loaded on demand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestrationStateEnvelopeV2 {
    /// Schema marker.
    pub version: u32,
    /// Deterministic instance table of CAS refs.
    #[serde(default)]
    pub instances: BTreeMap<String, InstanceRefV2>,
    /// Envelope-level auxiliary metadata keyed by instance key.
    #[serde(default)]
    pub aux: BTreeMap<String, AuxDataV2>,
}

// ---------------------------------------------------------------------------
// Optics bridges (V2 persistence ↔ runtime types)
// ---------------------------------------------------------------------------

/// Isomorphism between `InstanceRefV2` and `Hash`.
#[must_use]
#[expect(dead_code, reason = "Available for V2 optics consumers")]
pub fn instance_ref_v2_iso() -> IsoPrime<'static, RcBrand, InstanceRefV2, Hash> {
    IsoPrime::new(|ref_: InstanceRefV2| ref_.hash, |hash: Hash| InstanceRefV2 { hash })
}

/// Isomorphism between `AuxDataV2` and runtime `crate::model::state::AuxData`.
#[must_use]
pub fn aux_data_v2_iso() -> IsoPrime<'static, RcBrand, AuxDataV2, crate::model::state::AuxData> {
    IsoPrime::new(
        |versioned: AuxDataV2| crate::model::state::AuxData {
            // Inject now() for None — old state predating the aux envelope
            // or instances created before the non-null guarantee may carry
            // last_reachable: None on the wire. Decode is the only boundary
            // where None is handled; the runtime type is non-optional.
            last_unreachable: match versioned.last_reachable {
                Some(ts) => crate::model::config::ImpureTimestamp {
                    epoch_seconds: ts.epoch_seconds,
                    subsec_nanos: ts.subsec_nanos,
                },
                None => crate::model::config::ImpureTimestamp::now(),
            },
        },
        |runtime: crate::model::state::AuxData| AuxDataV2 {
            last_reachable: Some(ImpureTimestampV2 {
                epoch_seconds: runtime.last_unreachable.epoch_seconds,
                subsec_nanos: runtime.last_unreachable.subsec_nanos,
            }),
        },
    )
}

/// Isomorphism between `ResolvedInputV2` and runtime `crate::model::state::ResolvedInputKey`.
#[must_use]
pub fn resolved_input_v2_iso()
-> IsoPrime<'static, RcBrand, ResolvedInputV2, crate::model::state::ResolvedInputKey> {
    IsoPrime::new(
        |versioned: ResolvedInputV2| crate::model::state::ResolvedInputKey { hash: versioned.hash },
        |runtime: crate::model::state::ResolvedInputKey| ResolvedInputV2 { hash: runtime.hash },
    )
}

/// Isomorphism between `OutputRefV2` and runtime `crate::model::state::OutputRef`.
#[must_use]
pub fn output_ref_v2_iso() -> IsoPrime<'static, RcBrand, OutputRefV2, crate::model::state::OutputRef>
{
    IsoPrime::new(
        |versioned: OutputRefV2| {
            let save = match versioned.persistence.save {
                OutputSaveModeV2::Bool(false) => crate::model::state::OutputSaveMode::Unsaved,
                OutputSaveModeV2::Bool(true) => crate::model::state::OutputSaveMode::Saved,
                OutputSaveModeV2::Full => crate::model::state::OutputSaveMode::Full,
            };
            crate::model::state::OutputRef {
                hash: versioned.hash,
                persistence: crate::model::state::PersistenceFlags { save },
                allow_empty_capture: versioned.allow_empty_capture,
            }
        },
        |runtime: crate::model::state::OutputRef| {
            let save = match runtime.persistence.save {
                crate::model::state::OutputSaveMode::Unsaved => OutputSaveModeV2::Bool(false),
                crate::model::state::OutputSaveMode::Saved => OutputSaveModeV2::Bool(true),
                crate::model::state::OutputSaveMode::Full => OutputSaveModeV2::Full,
            };
            OutputRefV2 {
                hash: runtime.hash,
                persistence: PersistenceFlagsV2 { save },
                allow_empty_capture: runtime.allow_empty_capture,
            }
        },
    )
}

/// Bridges V2 tool metadata wire shape with runtime `ToolSpec` state.
///
/// Builtins are persisted as identity-only fields (`kind`/`name`/`version`) and
/// decoded into runtime `ToolSpec` values with empty optional maps. Executables
/// round-trip through the full `ToolSpec` wire shape.
#[must_use]
pub fn tool_metadata_v2_iso() -> IsoPrime<'static, RcBrand, ToolMetadataV2, ToolSpec> {
    IsoPrime::new(
        |versioned: ToolMetadataV2| match versioned {
            ToolMetadataV2::Builtin { kind: _, name, version } => ToolSpec {
                is_impure: false,
                inputs: BTreeMap::new(),
                kind: ToolKindSpec::Builtin { name, version },
                outputs: BTreeMap::new(),
            },
            ToolMetadataV2::Executable(spec) => spec,
        },
        |runtime: ToolSpec| match runtime.kind {
            ToolKindSpec::Builtin { name, version } => {
                ToolMetadataV2::Builtin { kind: BuiltinMetadataKindV2::Builtin, name, version }
            }
            ToolKindSpec::Executable { .. } => ToolMetadataV2::Executable(runtime),
        },
    )
}

/// Isomorphism between `ToolCallInstanceV2` and runtime `crate::model::state::ToolCallInstance`.
#[must_use]
pub fn tool_call_instance_v2_iso()
-> IsoPrime<'static, RcBrand, ToolCallInstanceV2, crate::model::state::ToolCallInstance> {
    IsoPrime::new(
        |versioned: ToolCallInstanceV2| {
            let metadata = tool_metadata_v2_iso().from(versioned.metadata);
            let impure_timestamp =
                versioned.impure_timestamp.map(|ts| crate::model::config::ImpureTimestamp {
                    epoch_seconds: ts.epoch_seconds,
                    subsec_nanos: ts.subsec_nanos,
                });
            let inputs = versioned
                .inputs
                .into_iter()
                .map(|(name, input)| (name, resolved_input_v2_iso().from(input)))
                .collect();
            let outputs = versioned
                .outputs
                .into_iter()
                .map(|(name, output)| (name, output_ref_v2_iso().from(output)))
                .collect();

            crate::model::state::ToolCallInstance {
                tool_name: versioned.tool_name,
                metadata,
                impure_timestamp,
                inputs,
                outputs,
            }
        },
        |runtime: crate::model::state::ToolCallInstance| {
            let impure_timestamp = runtime.impure_timestamp.map(|ts| ImpureTimestampV2 {
                epoch_seconds: ts.epoch_seconds,
                subsec_nanos: ts.subsec_nanos,
            });
            let inputs = runtime
                .inputs
                .into_iter()
                .map(|(name, input)| (name, resolved_input_v2_iso().to(input)))
                .collect();
            let outputs = runtime
                .outputs
                .into_iter()
                .map(|(name, output)| (name, output_ref_v2_iso().to(output)))
                .collect();

            ToolCallInstanceV2 {
                tool_name: runtime.tool_name,
                metadata: tool_metadata_v2_iso().to(runtime.metadata),
                impure_timestamp,
                inputs,
                outputs,
            }
        },
    )
}

/// Isomorphism between `OrchestrationStateEnvelopeV2` and
/// `(BTreeMap<String, Hash>, crate::model::state::OrchestrationState)`.
///
/// The envelope's `instances` map is split into a separate ref-map and runtime
/// state.  Instances stored in the returned runtime state are eagerly
/// constructed with [`tool_call_instance_v2_iso`]; callers needing lazy
/// loading must handle instance blob fetch + decode themselves.
#[must_use]
#[expect(dead_code, reason = "Available for V2 optics consumers")]
pub fn orchestration_state_v2_iso() -> IsoPrime<
    'static,
    RcBrand,
    OrchestrationStateEnvelopeV2,
    (BTreeMap<String, Hash>, crate::model::state::OrchestrationState),
> {
    IsoPrime::new(
        |envelope: OrchestrationStateEnvelopeV2| {
            let refs: BTreeMap<String, Hash> =
                envelope.instances.iter().map(|(key, ref_)| (key.clone(), ref_.hash)).collect();
            let runtime_state = crate::model::state::OrchestrationState {
                version: envelope.version,
                instances: BTreeMap::new(),
                aux: envelope
                    .aux
                    .into_iter()
                    .map(|(k, v)| (k, aux_data_v2_iso().from(v)))
                    .collect(),
                referenced_instance_keys: std::collections::HashSet::new(),
            };
            (refs, runtime_state)
        },
        |(refs, runtime_state): (
            BTreeMap<String, Hash>,
            crate::model::state::OrchestrationState,
        )| {
            let instances: BTreeMap<String, InstanceRefV2> =
                refs.into_iter().map(|(key, hash)| (key, InstanceRefV2 { hash })).collect();
            OrchestrationStateEnvelopeV2 {
                version: runtime_state.version,
                instances,
                aux: runtime_state
                    .aux
                    .into_iter()
                    .map(|(k, v)| (k, aux_data_v2_iso().to(v)))
                    .collect(),
            }
        },
    )
}

// ---------------------------------------------------------------------------
// Encode/decode helpers for individual instance blobs
// ---------------------------------------------------------------------------

/// Encodes one `ToolCallInstanceV2` into a serialized byte blob suitable for
/// CAS storage.
///
/// # Errors
///
/// Returns an error when JSON serialization fails.
pub fn encode_instance_v2(instance: &ToolCallInstanceV2) -> Result<Vec<u8>, ConductorError> {
    serde_json::to_vec(instance).map_err(|e| ConductorError::Serialization(e.to_string()))
}

/// Decodes one `ToolCallInstanceV2` from serialized CAS blob bytes.
///
/// # Errors
///
/// Returns an error when JSON deserialization fails.
pub fn decode_instance_v2(bytes: &[u8]) -> Result<ToolCallInstanceV2, ConductorError> {
    serde_json::from_slice(bytes).map_err(|e| ConductorError::Serialization(e.to_string()))
}

/// Computes the CAS hash of one encoded `ToolCallInstanceV2`.
///
/// This is equivalent to `Hash::from_content(encode_instance_v2(instance)?)` but
/// avoids a clone.
#[must_use]
#[allow(dead_code)]
pub fn instance_v2_hash(instance: &ToolCallInstanceV2) -> Hash {
    // SAFETY: encoding an in-memory struct should never fail; unwrap is safe.
    let bytes = serde_json::to_vec(instance)
        .expect("ToolCallInstanceV2 serialization should not fail for in-memory data");
    Hash::from_content(&bytes)
}
