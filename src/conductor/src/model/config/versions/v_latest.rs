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
use std::fmt;

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use mediapm_cas::Hash;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

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
    /// Optional tri-state `save` override.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_output_save_latest"
    )]
    pub(crate) save: Option<OutputSaveLatest>,
}

/// Persisted tri-state save mode for output-policy fields.
///
/// Wire shape intentionally remains compact:
/// - `false` => unsaved,
/// - `true` => saved,
/// - `"full"` => full-data preferred save.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputSaveLatest {
    /// Boolean save mode (`false` or `true`).
    Bool(bool),
    /// Full-save mode keyword.
    Full,
}

impl Serialize for OutputSaveLatest {
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

impl<'de> Deserialize<'de> for OutputSaveLatest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OutputSaveLatestVisitor;

        impl Visitor<'_> for OutputSaveLatestVisitor {
            type Value = OutputSaveLatest;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a boolean save mode or the string \"full\"")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OutputSaveLatest::Bool(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "full" {
                    Ok(OutputSaveLatest::Full)
                } else {
                    Err(E::invalid_value(de::Unexpected::Str(value), &self))
                }
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_any(OutputSaveLatestVisitor)
    }
}

/// Deserializes optional tri-state save values from persisted config records.
fn deserialize_optional_output_save_latest<'de, D>(
    deserializer: D,
) -> Result<Option<OutputSaveLatest>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<Value>::deserialize(deserializer)?;
    match raw {
        None => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(OutputSaveLatest::Bool(value))),
        Some(Value::String(value)) if value == "full" => Ok(Some(OutputSaveLatest::Full)),
        Some(other) => {
            Err(de::Error::custom(format!("save must be false, true, or \"full\"; got {other}")))
        }
    }
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

/// Returns whether a value equals its type default for serde skip checks.
fn is_default_value<T>(value: &T) -> bool
where
    T: Default + PartialEq,
{
    value == &T::default()
}

/// Declared tool input entry in the latest persisted schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolInputSpecLatest {
    /// Declared input value kind.
    #[serde(default, skip_serializing_if = "is_default_value")]
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
    /// Capture bytes from one regex-selected relative file path.
    FileRegex { path_regex: String },
    /// Capture directory contents as one ZIP payload.
    Folder {
        /// Relative directory path that should be zipped.
        path: String,
        /// Whether to include the topmost folder node as ZIP root.
        #[serde(default)]
        include_topmost_folder: bool,
    },
    /// Capture one ZIP payload containing files selected by regex.
    ///
    /// When regex capture groups match a selected file path, the capture
    /// strings are joined and used as the ZIP member path. If no capture
    /// groups match, the original sandbox-relative path is preserved.
    FolderRegex { path_regex: String },
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
    if value < f64::from(i32::MIN) || value > f64::from(i32::MAX) {
        return Err(serde::de::Error::custom(format!("success code {value} is outside i32 range")));
    }

    format!("{value:.0}")
        .parse::<i32>()
        .map_err(|_| serde::de::Error::custom(format!("success code {value} is outside i32 range")))
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

    format!("{value:.0}")
        .parse::<u64>()
        .map_err(|_| serde::de::Error::custom(format!("{field} {value} exceeds u64 range")))
}

fn parse_integral_u32<E>(value: f64, field: &str) -> Result<u32, E>
where
    E: serde::de::Error,
{
    let parsed = parse_integral_u64::<E>(value, field)?;
    u32::try_from(parsed)
        .map_err(|_| serde::de::Error::custom(format!("{field} {value} exceeds u32 range")))
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
    pub(crate) conductor_state_config: Option<String>,
    /// Optional filesystem CAS store directory override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cas_store_dir: Option<String>,
    /// Optional temporary execution sandbox directory override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conductor_tmp_dir: Option<String>,
    /// Optional schema export directory override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conductor_schema_dir: Option<String>,
    /// Optional additional inherited host environment-variable names keyed by
    /// platform.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) inherited_env_vars: Option<BTreeMap<String, Vec<String>>>,
    /// Optional toggle for shared global user-level managed-tool cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) use_user_tool_cache: Option<bool>,
}

impl RuntimeStorageLatest {
    /// Returns whether the grouped runtime-storage record has no overrides.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.conductor_dir.is_none()
            && self.conductor_state_config.is_none()
            && self.cas_store_dir.is_none()
            && self.conductor_tmp_dir.is_none()
            && self.conductor_schema_dir.is_none()
            && self.inherited_env_vars.is_none()
            && self.use_user_tool_cache.is_none()
    }
}

/// Declared output contract in the latest persisted schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolOutputSpecLatest {
    /// Capture source for this output.
    pub(crate) capture: OutputCaptureLatest,
    /// Whether a missing capture is treated as empty rather than an error.
    #[serde(default)]
    pub(crate) allow_empty: bool,
}

/// External content metadata persisted in the latest Nickel schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExternalContentRefLatest {
    /// Optional human description.
    #[serde(default)]
    pub(crate) description: Option<String>,
    /// Optional persisted save override for this external-data root.
    ///
    /// Wire shape mirrors output-policy `save` values:
    /// - `true` for regular saved mode,
    /// - `"full"` for full-data preferred mode.
    ///
    /// `false` is parsed here for compatibility but rejected by runtime
    /// validation when used under `external_data`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_output_save_latest"
    )]
    pub(crate) save: Option<OutputSaveLatest>,
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
    pub(crate) env_vars: BTreeMap<String, String>,
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
