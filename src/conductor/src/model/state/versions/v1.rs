//! Version 1 persistence envelopes for orchestration state.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This module owns only V1 persisted shapes.
//! - It must not import unversioned runtime orchestration-state structs.
//! - Cross-version migration (when added) may only reference adjacent versions
//!   via optic composition.

use std::collections::BTreeMap;

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use crate::model::config::ToolSpec;
use crate::{impl_output_save_mode_serde, impl_tool_metadata_deserialize};

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

impl_output_save_mode_serde!(OutputSaveModeV1, OutputSaveModeV1Visitor);

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
    /// Whether this output was captured as intentionally empty.
    ///
    /// Defaults to `false` for compatibility with state written before this
    /// field was introduced.
    #[serde(default)]
    pub allow_empty_capture: bool,
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

impl_tool_metadata_deserialize!(ToolMetadataV1, BuiltinMetadataKindV1, BuiltinMetadataWireV1);

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
    /// Optional last-use timestamp for GC ordering.
    #[serde(default)]
    pub last_used: Option<ImpureTimestampV1>,
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

// ---------------------------------------------------------------------------
// ISO bridges: V1 ↔ V2 (adjacent-version migration)
// ---------------------------------------------------------------------------
//
// These provide bidirectional type conversions between structurally similar V1
// and V2 persisted shapes. V2 drops `last_used` from `ToolCallInstance` and
// uses CAS-backed envelope refs instead of inline instance data.

use super::v2;

/// Isomorphism between [`OutputSaveModeV1`] and [`v2::OutputSaveModeV2`].
#[must_use]
pub fn output_save_mode_v1_v2_iso()
-> IsoPrime<'static, RcBrand, OutputSaveModeV1, v2::OutputSaveModeV2> {
    IsoPrime::new(
        |v1: OutputSaveModeV1| match v1 {
            OutputSaveModeV1::Bool(b) => v2::OutputSaveModeV2::Bool(b),
            OutputSaveModeV1::Full => v2::OutputSaveModeV2::Full,
        },
        |v2: v2::OutputSaveModeV2| match v2 {
            v2::OutputSaveModeV2::Bool(b) => OutputSaveModeV1::Bool(b),
            v2::OutputSaveModeV2::Full => OutputSaveModeV1::Full,
        },
    )
}

/// Isomorphism between [`PersistenceFlagsV1`] and [`v2::PersistenceFlagsV2`].
#[must_use]
pub fn persistence_flags_v1_v2_iso()
-> IsoPrime<'static, RcBrand, PersistenceFlagsV1, v2::PersistenceFlagsV2> {
    IsoPrime::new(
        |v1: PersistenceFlagsV1| v2::PersistenceFlagsV2 {
            save: output_save_mode_v1_v2_iso().from(v1.save),
        },
        |v2: v2::PersistenceFlagsV2| PersistenceFlagsV1 {
            save: output_save_mode_v1_v2_iso().to(v2.save),
        },
    )
}

/// Isomorphism between [`ImpureTimestampV1`] and [`v2::ImpureTimestampV2`].
#[must_use]
pub fn impure_timestamp_v1_v2_iso()
-> IsoPrime<'static, RcBrand, ImpureTimestampV1, v2::ImpureTimestampV2> {
    IsoPrime::new(
        |v1: ImpureTimestampV1| v2::ImpureTimestampV2 {
            epoch_seconds: v1.epoch_seconds,
            subsec_nanos: v1.subsec_nanos,
        },
        |v2: v2::ImpureTimestampV2| ImpureTimestampV1 {
            epoch_seconds: v2.epoch_seconds,
            subsec_nanos: v2.subsec_nanos,
        },
    )
}

/// Isomorphism between [`ResolvedInputV1`] and [`v2::ResolvedInputV2`].
#[must_use]
pub fn resolved_input_v1_v2_iso() -> IsoPrime<'static, RcBrand, ResolvedInputV1, v2::ResolvedInputV2>
{
    IsoPrime::new(
        |v1: ResolvedInputV1| v2::ResolvedInputV2 { hash: v1.hash },
        |v2: v2::ResolvedInputV2| ResolvedInputV1 { hash: v2.hash },
    )
}

/// Isomorphism between [`OutputRefV1`] and [`v2::OutputRefV2`].
#[must_use]
pub fn output_ref_v1_v2_iso() -> IsoPrime<'static, RcBrand, OutputRefV1, v2::OutputRefV2> {
    IsoPrime::new(
        |v1: OutputRefV1| v2::OutputRefV2 {
            hash: v1.hash,
            persistence: persistence_flags_v1_v2_iso().from(v1.persistence),
            allow_empty_capture: v1.allow_empty_capture,
        },
        |v2: v2::OutputRefV2| OutputRefV1 {
            hash: v2.hash,
            persistence: persistence_flags_v1_v2_iso().to(v2.persistence),
            allow_empty_capture: v2.allow_empty_capture,
        },
    )
}

/// Isomorphism between [`BuiltinMetadataKindV1`] and [`v2::BuiltinMetadataKindV2`].
#[must_use]
pub fn builtin_metadata_kind_v1_v2_iso()
-> IsoPrime<'static, RcBrand, BuiltinMetadataKindV1, v2::BuiltinMetadataKindV2> {
    IsoPrime::new(
        |_: BuiltinMetadataKindV1| v2::BuiltinMetadataKindV2::Builtin,
        |_: v2::BuiltinMetadataKindV2| BuiltinMetadataKindV1::Builtin,
    )
}

/// Isomorphism between [`ToolMetadataV1`] and [`v2::ToolMetadataV2`].
#[must_use]
pub fn tool_metadata_v1_v2_iso() -> IsoPrime<'static, RcBrand, ToolMetadataV1, v2::ToolMetadataV2> {
    IsoPrime::new(
        |v1: ToolMetadataV1| match v1 {
            ToolMetadataV1::Builtin { kind, name, version } => v2::ToolMetadataV2::Builtin {
                kind: builtin_metadata_kind_v1_v2_iso().from(kind),
                name,
                version,
            },
            ToolMetadataV1::Executable(spec) => v2::ToolMetadataV2::Executable(spec),
        },
        |v2: v2::ToolMetadataV2| match v2 {
            v2::ToolMetadataV2::Builtin { kind, name, version } => ToolMetadataV1::Builtin {
                kind: builtin_metadata_kind_v1_v2_iso().to(kind),
                name,
                version,
            },
            v2::ToolMetadataV2::Executable(spec) => ToolMetadataV1::Executable(spec),
        },
    )
}

/// Isomorphism between [`ToolCallInstanceV1`] and [`v2::ToolCallInstanceV2`].
///
/// V1-specific fields:
/// - `last_used` is dropped when converting V1 → V2 (V2 moved GC metadata
///   to the envelope level).
/// - When converting V2 → V1, `last_used` is set to `None`.
#[must_use]
pub fn tool_call_instance_v1_v2_iso()
-> IsoPrime<'static, RcBrand, ToolCallInstanceV1, v2::ToolCallInstanceV2> {
    IsoPrime::new(
        |v1: ToolCallInstanceV1| v2::ToolCallInstanceV2 {
            tool_name: v1.tool_name,
            metadata: tool_metadata_v1_v2_iso().from(v1.metadata),
            impure_timestamp: v1.impure_timestamp.map(|ts| impure_timestamp_v1_v2_iso().from(ts)),
            inputs: v1
                .inputs
                .into_iter()
                .map(|(k, v)| (k, resolved_input_v1_v2_iso().from(v)))
                .collect(),
            outputs: v1
                .outputs
                .into_iter()
                .map(|(k, v)| (k, output_ref_v1_v2_iso().from(v)))
                .collect(),
        },
        |v2: v2::ToolCallInstanceV2| ToolCallInstanceV1 {
            tool_name: v2.tool_name,
            metadata: tool_metadata_v1_v2_iso().to(v2.metadata),
            impure_timestamp: v2.impure_timestamp.map(|ts| impure_timestamp_v1_v2_iso().to(ts)),
            last_used: None,
            inputs: v2
                .inputs
                .into_iter()
                .map(|(k, v)| (k, resolved_input_v1_v2_iso().to(v)))
                .collect(),
            outputs: v2
                .outputs
                .into_iter()
                .map(|(k, v)| (k, output_ref_v1_v2_iso().to(v)))
                .collect(),
        },
    )
}
