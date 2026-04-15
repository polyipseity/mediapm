//! Latest persisted Nickel envelope shape for conductor configuration documents.
//!
//! ## DO NOT REMOVE: latest schema bridge guard
//!
//! - This file is the **only** Rust struct bridge for persisted Nickel envelopes.
//! - Keep exactly one latest bridge module (`v_latest.rs`) in this directory.
//! - Historical schema migration must remain in Nickel (`vX.ncl`) and be
//!   evaluated before Rust deserialization.
//! - `mod.rs` should deserialize only into the types defined in this file.
//! - If the latest schema marker changes, update `NICKEL_VERSION_LATEST`, the
//!   Rust structs here, and the corresponding latest `vX.ncl` together.

use std::collections::BTreeMap;

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

/// Latest persisted Nickel schema marker supported by the Rust bridge.
pub(crate) const NICKEL_VERSION_LATEST: u32 = 1;

/// Returns whether `marker` matches the latest Rust bridge schema marker.
#[must_use]
pub(crate) const fn is_nickel_version_latest(marker: u32) -> bool {
    marker == NICKEL_VERSION_LATEST
}

/// Optional per-output persistence override in the latest persisted schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct OutputPolicyLatest {
    /// Optional `save` override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) save: Option<bool>,
    /// Optional `force_full` override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) force_full: Option<bool>,
}

/// Persisted kind selector for one declared tool input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolInputKindLatest {
    /// Scalar string input (default).
    #[default]
    String,
    /// Ordered list-of-strings input.
    StringList,
}

fn is_default_tool_input_kind_latest(kind: &ToolInputKindLatest) -> bool {
    matches!(kind, ToolInputKindLatest::String)
}

/// Declared tool input entry in the latest persisted schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolInputSpecLatest {
    /// Declared input value kind.
    #[serde(default, skip_serializing_if = "is_default_tool_input_kind_latest")]
    pub(crate) kind: ToolInputKindLatest,
}

/// Tool definition persisted in the latest Nickel schema.
///
/// This enum intentionally keeps builtin fields minimal:
/// builtin entries may only carry `kind`, `name`, and `version`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ToolSpecLatest {
    /// Executable tool definition.
    Executable {
        /// Whether this executable is impure and should receive deterministic
        /// timestamp invalidation.
        #[serde(default)]
        is_impure: bool,
        /// Declared executable input contract map.
        #[serde(default)]
        inputs: BTreeMap<String, ToolInputSpecLatest>,
        /// Command vector where first entry is executable path and remaining
        /// entries are process arguments.
        #[serde(default)]
        command: Vec<String>,
        /// Executable-only runtime environment templates.
        #[serde(default)]
        env_vars: BTreeMap<String, String>,
        /// Exit codes treated as successful completion for this executable.
        #[serde(
            default = "default_success_codes",
            deserialize_with = "deserialize_integral_codes"
        )]
        success_codes: Vec<i32>,
        /// Declared executable outputs.
        #[serde(default)]
        outputs: BTreeMap<String, ToolOutputSpecLatest>,
    },
    /// Builtin tool definition.
    Builtin {
        /// Builtin name.
        name: String,
        /// Builtin semantic version.
        version: String,
    },
}

/// Capture selector for one declared output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum OutputCaptureLatest {
    /// Capture standard output bytes.
    Stdout {},
    /// Capture standard error bytes.
    Stderr {},
    /// Capture process exit code as UTF-8 bytes.
    ProcessCode {},
    /// Capture bytes from one relative file path.
    File { path: String },
    /// Capture directory contents as one ZIP payload.
    Folder {
        /// Relative directory path that should be zipped.
        path: String,
        /// Whether to include the topmost folder node as ZIP root.
        #[serde(default)]
        include_topmost_folder: bool,
    },
}

fn default_success_codes() -> Vec<i32> {
    vec![0]
}

fn default_max_concurrent_calls() -> i32 {
    -1
}

fn default_max_retries() -> i32 {
    -1
}

fn deserialize_integral_codes<'de, D>(deserializer: D) -> Result<Vec<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Vec::<f64>::deserialize(deserializer)?
        .into_iter()
        .map(parse_integral_success_code::<D::Error>)
        .collect()
}

fn parse_integral_success_code<E>(value: f64) -> Result<i32, E>
where
    E: serde::de::Error,
{
    if !value.is_finite() || value.fract() != 0.0 {
        return Err(serde::de::Error::custom(format!(
            "expected integral success code, got {value}"
        )));
    }
    if value < i32::MIN as f64 || value > i32::MAX as f64 {
        return Err(serde::de::Error::custom(format!("success code {value} is outside i32 range")));
    }
    Ok(value as i32)
}

fn deserialize_integral_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    parse_integral_u64::<D::Error>(f64::deserialize(deserializer)?, "epoch_seconds")
}

fn deserialize_integral_u32<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    parse_integral_u32::<D::Error>(f64::deserialize(deserializer)?, "subsec_nanos")
}

fn parse_integral_u64<E>(value: f64, field: &str) -> Result<u64, E>
where
    E: serde::de::Error,
{
    if !value.is_finite() || value.fract() != 0.0 || value < 0.0 {
        return Err(serde::de::Error::custom(format!(
            "expected non-negative integral {field}, got {value}"
        )));
    }
    if value > u64::MAX as f64 {
        return Err(serde::de::Error::custom(format!("{field} {value} exceeds u64 range")));
    }
    Ok(value as u64)
}

fn parse_integral_u32<E>(value: f64, field: &str) -> Result<u32, E>
where
    E: serde::de::Error,
{
    if !value.is_finite() || value.fract() != 0.0 || value < 0.0 {
        return Err(serde::de::Error::custom(format!(
            "expected non-negative integral {field}, got {value}"
        )));
    }
    if value > u32::MAX as f64 {
        return Err(serde::de::Error::custom(format!("{field} {value} exceeds u32 range")));
    }
    Ok(value as u32)
}

/// Timezone-independent impure execution timestamp in persisted schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct ImpureTimestampLatest {
    /// Whole UTC seconds since Unix epoch.
    #[serde(deserialize_with = "deserialize_integral_u64")]
    pub(crate) epoch_seconds: u64,
    /// Nanoseconds within the second.
    #[serde(deserialize_with = "deserialize_integral_u32")]
    pub(crate) subsec_nanos: u32,
}

/// Grouped runtime storage-path configuration in persisted schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct RuntimeStorageLatest {
    /// Runtime storage root directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conductor_dir: Option<String>,
    /// Optional volatile state document path override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) state_config: Option<String>,
    /// Optional filesystem CAS store directory override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cas_store_dir: Option<String>,
}

impl RuntimeStorageLatest {
    /// Returns whether the grouped runtime-storage record has no overrides.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.conductor_dir.is_none() && self.state_config.is_none() && self.cas_store_dir.is_none()
    }
}

/// Declared output contract in the latest persisted schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolOutputSpecLatest {
    /// Capture source for this output.
    pub(crate) capture: OutputCaptureLatest,
}

/// External content metadata persisted in the latest Nickel schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExternalContentRefLatest {
    /// Optional human description.
    #[serde(default)]
    pub(crate) description: Option<String>,
}

/// Per-tool runtime execution configuration persisted in the latest schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolConfigSpecLatest {
    /// Maximum concurrent calls for this tool (`-1` means unlimited).
    #[serde(default = "default_max_concurrent_calls")]
    pub(crate) max_concurrent_calls: i32,
    /// Maximum retries after the first failed call (`-1` means use runtime
    /// default retry behavior).
    #[serde(default = "default_max_retries")]
    pub(crate) max_retries: i32,
    /// Optional human-facing description for runtime tool configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Optional per-tool default input bindings.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) input_defaults: BTreeMap<String, InputBindingLatest>,
    /// Optional explicit runtime environment map merged during tool execution.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) env_var: BTreeMap<String, String>,
    /// Optional content map used for executable sandbox materialization.
    ///
    /// Key semantics are runtime-defined and mirrored from `v1.ncl` docs:
    /// - trailing `/` or `\\` keys target directories and require ZIP payload
    ///   hashes,
    /// - all other keys target regular files and write raw bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) content_map: Option<BTreeMap<String, Hash>>,
}

/// Workflow specification persisted in the latest Nickel schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct WorkflowSpecLatest {
    /// Optional human-facing workflow label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    /// Optional human-facing workflow description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Ordered workflow step list.
    #[serde(default)]
    pub(crate) steps: Vec<WorkflowStepSpecLatest>,
}

/// One workflow step persisted in the latest Nickel schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkflowStepSpecLatest {
    /// Stable step identifier within one workflow.
    pub(crate) id: String,
    /// Referenced tool name.
    pub(crate) tool: String,
    /// Input bindings by logical input name.
    ///
    /// Step inputs represent tool-call input data for both builtin and
    /// executable tools. Values are scalar strings or list-of-strings;
    /// `${...}` forms are parsed by runtime/schema validation.
    #[serde(default)]
    pub(crate) inputs: BTreeMap<String, InputBindingLatest>,
    /// Explicit ordering dependencies on prior step ids.
    #[serde(default)]
    pub(crate) depends_on: Vec<String>,
    /// Step-local output persistence-policy map.
    ///
    /// This does not declare step outputs; output names remain tool-defined.
    #[serde(default)]
    pub(crate) outputs: BTreeMap<String, OutputPolicyLatest>,
}

/// Persisted workflow input binding value.
///
/// Special forms are string-based, for example `${external_data.<hash>}` and
/// `${step_output.<step_id>.<output_name>}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum InputBindingLatest {
    /// Scalar string binding.
    String(String),
    /// Ordered list-of-strings binding.
    StringList(Vec<String>),
}

/// Latest shared configuration state shape used by the Rust bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct NickelStateLatest {
    /// Grouped runtime storage-path configuration.
    #[serde(default, skip_serializing_if = "RuntimeStorageLatest::is_empty")]
    pub(crate) runtime: RuntimeStorageLatest,
    /// External content metadata keyed by CAS hash identity.
    #[serde(default)]
    pub(crate) external_data: BTreeMap<Hash, ExternalContentRefLatest>,
    /// Tool definitions.
    #[serde(default)]
    pub(crate) tools: BTreeMap<String, ToolSpecLatest>,
    /// Workflow definitions.
    #[serde(default)]
    pub(crate) workflows: BTreeMap<String, WorkflowSpecLatest>,
    /// Runtime execution configuration keyed by tool name.
    #[serde(default)]
    pub(crate) tool_configs: BTreeMap<String, ToolConfigSpecLatest>,
    /// Impure timestamps keyed by workflow id, then step id.
    #[serde(default)]
    pub(crate) impure_timestamps: BTreeMap<String, BTreeMap<String, ImpureTimestampLatest>>,
    /// Orchestration state pointer.
    #[serde(default)]
    pub(crate) state_pointer: Option<Hash>,
}

/// Latest persisted Nickel envelope shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct NickelEnvelopeLatest {
    /// Explicit schema marker.
    pub(crate) version: u32,
    /// Grouped runtime storage-path configuration.
    #[serde(default, skip_serializing_if = "RuntimeStorageLatest::is_empty")]
    pub(crate) runtime: RuntimeStorageLatest,
    /// External content metadata keyed by CAS hash identity.
    #[serde(default)]
    pub(crate) external_data: BTreeMap<Hash, ExternalContentRefLatest>,
    /// Tool definitions.
    #[serde(default)]
    pub(crate) tools: BTreeMap<String, ToolSpecLatest>,
    /// Workflow definitions.
    #[serde(default)]
    pub(crate) workflows: BTreeMap<String, WorkflowSpecLatest>,
    /// Runtime execution configuration keyed by tool name.
    #[serde(default)]
    pub(crate) tool_configs: BTreeMap<String, ToolConfigSpecLatest>,
    /// Impure timestamps keyed by workflow id, then step id.
    #[serde(default)]
    pub(crate) impure_timestamps: BTreeMap<String, BTreeMap<String, ImpureTimestampLatest>>,
    /// Orchestration state pointer.
    #[serde(default)]
    pub(crate) state_pointer: Option<Hash>,
}

/// Isomorphism between latest persisted document envelope and shared state shape.
#[must_use]
pub(crate) fn nickel_latest_iso()
-> IsoPrime<'static, RcBrand, NickelEnvelopeLatest, NickelStateLatest> {
    IsoPrime::new(
        |envelope: NickelEnvelopeLatest| NickelStateLatest {
            runtime: envelope.runtime,
            external_data: envelope.external_data,
            tools: envelope.tools,
            workflows: envelope.workflows,
            tool_configs: envelope.tool_configs,
            impure_timestamps: envelope.impure_timestamps,
            state_pointer: envelope.state_pointer,
        },
        |state: NickelStateLatest| NickelEnvelopeLatest {
            version: NICKEL_VERSION_LATEST,
            runtime: state.runtime,
            external_data: state.external_data,
            tools: state.tools,
            workflows: state.workflows,
            tool_configs: state.tool_configs,
            impure_timestamps: state.impure_timestamps,
            state_pointer: state.state_pointer,
        },
    )
}
