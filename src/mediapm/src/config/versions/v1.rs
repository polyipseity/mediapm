//! Typed version-1 persisted envelope for `mediapm.ncl`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must not import unversioned runtime structs from outside
//!   `config/versions/`.
//! - A `vX` module may reference only the immediately previous version and only
//!   for migration/isomorphism.
//! - Latest-version bridging to runtime structs is owned by
//!   `config/versions/mod.rs`.

use std::collections::BTreeMap;

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Version marker
// ---------------------------------------------------------------------------

/// Version marker for the V1 `mediapm.ncl` envelope.
pub(crate) const MEDIAPM_NICKEL_VERSION_V1: u32 = 1;

/// Returns whether `marker` matches the V1 schema marker.
#[must_use]
pub(crate) const fn is_mediapm_nickel_version_v1(marker: u32) -> bool {
    marker == MEDIAPM_NICKEL_VERSION_V1
}

// ---------------------------------------------------------------------------
// Wire types matching NCL `PlatformInheritedEnvVarsV1`
// ---------------------------------------------------------------------------

/// Platform-keyed inherited env-var names, matching NCL `PlatformInheritedEnvVarsV1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlatformInheritedEnvVarsWireV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) windows: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) linux: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) macos: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Wire types matching NCL `RuntimeStorageV1`
// ---------------------------------------------------------------------------

/// Runtime storage wire type matching NCL `MediaRuntimeStorageV1`.
///
/// Fields typed as `Value` correspond to NCL `Dyn` contracts where the runtime
/// type (`MediaRuntimeStorage`) uses a concrete struct that cannot be imported
/// here (policy guard).
///
/// Unknown fields are rejected so removed/legacy runtime keys fail decode
/// (mirroring the final deserialization guard on `MediaRuntimeStorage`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct MediaRuntimeStorageWireV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) mediapm_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) hierarchy_root_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) mediapm_tmp_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) materialization_preference_order: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conductor_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conductor_machine_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conductor_state_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conductor_tmp_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conductor_schema_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) inherited_env_vars: Option<PlatformInheritedEnvVarsWireV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) media_state_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) env_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) env_generated_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) mediapm_schema_dir: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) instance_ttl_seconds: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) path_sanitization: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) profiler_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Wire field for `MediaRuntimeStorage.verify_materialization`.
    pub(crate) verify_materialization: Option<bool>,
}

// ---------------------------------------------------------------------------
// Wire types matching NCL state contracts
// ---------------------------------------------------------------------------

/// Impure timestamp wire type matching NCL `MediaPmImpureTimestampV1`.
///
/// Both fields are `Value` because NCL uses `Dyn` contracts for them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct MediaPmImpureTimestampWireV1 {
    #[serde(default)]
    pub(crate) epoch_seconds: Value,
    #[serde(default)]
    pub(crate) subsec_nanos: Value,
}

/// Managed workflow step state wire type matching NCL `ManagedWorkflowStepStateV1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedWorkflowStepStateWireV1 {
    pub(crate) explicit_config: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) impure_timestamp: Option<MediaPmImpureTimestampWireV1>,
}

/// Machine-managed state wire type matching NCL `MediaPmStateV1`.
///
/// `managed_files`, `tool_registry`, and `active_tools` are `Value` because
/// NCL uses `Dyn` contracts for them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct MediaPmStateWireV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) managed_files: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tool_registry: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) active_tools: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) workflow_states: Option<BTreeMap<String, Vec<ManagedWorkflowStepStateWireV1>>>,
}

// ---------------------------------------------------------------------------
// Top-level envelope and state types
// ---------------------------------------------------------------------------

/// Version-local typed state for V1 persisted payload fields.
///
/// All top-level `mediapm.ncl` fields except `version`.
///
/// Unknown fields are rejected so removed/legacy top-level keys (for example
/// `runtime_storage`) fail decode (mirroring the final deserialization guard
/// on `MediaPmDocument`).
///
/// Note: `deny_unknown_fields` is intentionally omitted here — serde does not
/// enforce it correctly on a `#[serde(flatten)]`-ed child struct. The parent
/// envelope carries the guard instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct MediaPmDocumentStateV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) runtime: Option<MediaRuntimeStorageWireV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tools: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) media: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) hierarchy: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) state: Option<MediaPmStateWireV1>,
}

/// Top-level V1 persisted document envelope.
///
/// `deny_unknown_fields` lives here (not on the flattened child state) because
/// serde does not enforce it correctly on `#[serde(flatten)]`-ed structs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct MediaPmDocumentEnvelopeV1 {
    /// Explicit schema marker.
    pub(crate) version: u32,
    /// Typed persisted payload fields (flattened so they sit at the same
    /// JSON level as `version`, matching the wire shape).
    #[serde(flatten)]
    pub(crate) state: MediaPmDocumentStateV1,
}

/// Isomorphism between V1 envelope and V1 local state.
///
/// Forward strips `version`; reverse adds version marker.
#[must_use]
pub(crate) fn mediapm_document_v1_iso()
-> IsoPrime<'static, RcBrand, MediaPmDocumentEnvelopeV1, MediaPmDocumentStateV1> {
    IsoPrime::new(
        |envelope: MediaPmDocumentEnvelopeV1| envelope.state,
        |state: MediaPmDocumentStateV1| MediaPmDocumentEnvelopeV1 {
            version: MEDIAPM_NICKEL_VERSION_V1,
            state,
        },
    )
}
