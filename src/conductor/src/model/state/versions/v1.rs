//! Version 1 persistence envelopes for orchestration state.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This module owns only V1 persisted shapes.
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

use crate::model::config::{ToolKindSpec, ToolSpec};

/// V1 orchestration-state schema marker.
pub const ORCHESTRATION_STATE_VERSION_V1: u32 = 1;

/// Returns whether `marker` matches orchestration-state V1 schema marker.
#[must_use]
pub const fn is_orchestration_state_version_v1(marker: u32) -> bool {
    marker == ORCHESTRATION_STATE_VERSION_V1
}

/// V1 persistence flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceFlagsV1 {
    /// Effective tri-state save policy.
    pub save: OutputSaveModeV1,
}

/// V1 persisted tri-state save mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSaveModeV1 {
    /// Boolean save policy (`false` or `true`).
    Bool(bool),
    /// Full-save policy keyword.
    Full,
}

impl Serialize for OutputSaveModeV1 {
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

impl<'de> Deserialize<'de> for OutputSaveModeV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OutputSaveModeV1Visitor;

        impl Visitor<'_> for OutputSaveModeV1Visitor {
            type Value = OutputSaveModeV1;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "a boolean save mode, or one of the strings \"full\", \"saved\", \"unsaved\"",
                )
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OutputSaveModeV1::Bool(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value.eq_ignore_ascii_case("full") {
                    return Ok(OutputSaveModeV1::Full);
                }

                if value.eq_ignore_ascii_case("saved") || value.eq_ignore_ascii_case("true") {
                    return Ok(OutputSaveModeV1::Bool(true));
                }

                if value.eq_ignore_ascii_case("unsaved") || value.eq_ignore_ascii_case("false") {
                    return Ok(OutputSaveModeV1::Bool(false));
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

        deserializer.deserialize_any(OutputSaveModeV1Visitor)
    }
}

/// Structured timezone-independent impure timestamp used by the latest V1 wire shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImpureTimestampV1 {
    /// Whole UTC seconds since Unix epoch.
    pub epoch_seconds: u64,
    /// Nanoseconds within the current second.
    pub subsec_nanos: u32,
}

/// V1 resolved input record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedInputV1 {
    /// CAS hash identity for the resolved input payload.
    pub hash: Hash,
}

/// V1 output reference record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRefV1 {
    /// Output hash.
    pub hash: Hash,
    /// Effective persistence flags.
    pub persistence: PersistenceFlagsV1,
}

/// Builtin metadata kind marker used by orchestration-state V1 wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinMetadataKindV1 {
    /// Builtin tool metadata projection marker.
    Builtin,
}

/// V1 state metadata shape.
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
pub enum ToolMetadataV1 {
    /// Identity-only metadata for builtin tools.
    Builtin {
        /// Builtin-kind marker (`"builtin"`).
        kind: BuiltinMetadataKindV1,
        /// Builtin name.
        name: String,
        /// Builtin semantic version.
        version: String,
    },
    /// Full metadata payload for executable tools.
    Executable(ToolSpec),
}

impl<'de> Deserialize<'de> for ToolMetadataV1 {
    /// Decodes one V1 metadata record with explicit `kind` dispatch.
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
        struct BuiltinMetadataWireV1 {
            /// Builtin kind marker.
            kind: BuiltinMetadataKindV1,
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
                let builtins: BuiltinMetadataWireV1 =
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

/// V1 tool-call instance record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallInstanceV1 {
    /// Immutable tool map key used by the workflow step.
    pub tool_name: String,
    /// Tool metadata persisted in normalized V1 shape.
    pub metadata: ToolMetadataV1,
    /// Optional machine-managed impurity timestamp.
    #[serde(default)]
    pub impure_timestamp: Option<ImpureTimestampV1>,
    /// Resolved inputs.
    #[serde(default)]
    pub inputs: BTreeMap<String, ResolvedInputV1>,
    /// Output references.
    #[serde(default)]
    pub outputs: BTreeMap<String, OutputRefV1>,
}

/// V1 orchestration-state payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OrchestrationStateV1 {
    /// Deterministic instance table.
    #[serde(default)]
    pub instances: BTreeMap<String, ToolCallInstanceV1>,
}

/// V1 orchestration-state persistence envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestrationStateEnvelopeV1 {
    /// Schema marker.
    pub version: u32,
    /// Deterministic instance table.
    #[serde(default)]
    pub instances: BTreeMap<String, ToolCallInstanceV1>,
}

/// Isomorphism between V1 state envelope and V1 state payload.
#[must_use]
pub fn orchestration_state_v1_iso()
-> IsoPrime<'static, RcBrand, OrchestrationStateEnvelopeV1, OrchestrationStateV1> {
    IsoPrime::new(
        |envelope: OrchestrationStateEnvelopeV1| OrchestrationStateV1 {
            instances: envelope.instances,
        },
        |state: OrchestrationStateV1| OrchestrationStateEnvelopeV1 {
            version: ORCHESTRATION_STATE_VERSION_V1,
            instances: state.instances,
        },
    )
}
