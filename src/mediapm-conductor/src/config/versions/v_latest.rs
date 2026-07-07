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

use serde::{Deserialize, Serialize};

use crate::config::{
    NickelDocument, OutputCaptureSpec, SaveMode, ToolInputKind, ToolInputSpec, ToolKindSpec,
    ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
};

/// Latest persisted Nickel schema marker supported by the Rust bridge.
pub(crate) const NICKEL_VERSION_LATEST: u32 = 2;

/// Returns whether `marker` matches the latest Rust bridge schema marker.
#[must_use]
#[expect(dead_code)]
pub(crate) const fn is_nickel_version_latest(marker: u32) -> bool {
    marker == NICKEL_VERSION_LATEST
}

/// Expected `version` field name in Nickel documents.
#[expect(dead_code)]
pub(crate) const VERSION_FIELD: &str = "version";

// ---------------------------------------------------------------------------
// Persisted envelope types — these match the `v1.ncl` Nickel contract exactly
// and are what `serde_json` deserializes after Nickel evaluation.
// ---------------------------------------------------------------------------

/// Latest persisted output policy (bool or "full").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum OutputPolicyLatest {
    /// Boolean save mode.
    Bool(bool),
    /// Full-data-preferred save mode.
    Full,
}

/// Latest persisted save mode: `"false"`, `"true"`, or `"full"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SaveModeLatest {
    /// Do not persist this output.
    False,
    /// Persist this output normally.
    True,
    /// Force full persistence even when empty or the step fails.
    Full,
}

impl Default for SaveModeLatest {
    fn default() -> Self {
        Self::True
    }
}

/// Latest persisted output capture spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OutputCaptureSpecLatest {
    /// Logical output name.
    pub(crate) name: String,
    /// Capture source selector.
    pub(crate) capture: String,
    /// Whether to persist this output to CAS.
    #[serde(default = "default_save_output", skip_serializing_if = "is_save_mode_true_latest")]
    pub(crate) save: SaveModeLatest,
    /// Whether an empty capture result is acceptable.
    #[serde(default, skip_serializing_if = "is_false")]
    pub(crate) allow_empty: bool,
    /// Whether `folder:` listings include the topmost folder name.
    #[serde(default = "default_include_topmost_folder", skip_serializing_if = "is_true")]
    pub(crate) include_topmost_folder: bool,
}

const fn default_save_output() -> SaveModeLatest {
    SaveModeLatest::True
}

const fn default_include_topmost_folder() -> bool {
    true
}

const fn is_save_mode_true_latest(v: &SaveModeLatest) -> bool {
    matches!(*v, SaveModeLatest::True)
}

const fn is_true(v: &bool) -> bool {
    *v
}

const fn is_false(v: &bool) -> bool {
    !*v
}

/// Latest persisted tool input kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolInputKindLatest {
    /// Simple string input.
    #[default]
    String,
    /// Content-addressed file input.
    Content,
    /// Environment variable passthrough.
    Env,
    /// JSON array of strings.
    StringList,
}

/// Latest persisted tool input spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolInputSpecLatest {
    /// Declared value kind.
    #[serde(default)]
    pub(crate) kind: ToolInputKindLatest,
    /// Whether this input is required.
    #[serde(default)]
    pub(crate) required: bool,
}

/// Latest persisted input binding (string or array of strings).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum InputBindingLatest {
    /// Single string value.
    String(String),
    /// Array of string values.
    Vec(Vec<String>),
}

impl Default for InputBindingLatest {
    fn default() -> Self {
        Self::String(String::new())
    }
}

impl From<InputBindingLatest> for super::super::InputBinding {
    fn from(b: InputBindingLatest) -> Self {
        match b {
            InputBindingLatest::String(s) => super::super::InputBinding::String(s),
            InputBindingLatest::Vec(v) => super::super::InputBinding::Vec(v),
        }
    }
}

impl From<super::super::InputBinding> for InputBindingLatest {
    fn from(b: super::super::InputBinding) -> Self {
        match b {
            super::super::InputBinding::String(s) => InputBindingLatest::String(s),
            super::super::InputBinding::Vec(v) => InputBindingLatest::Vec(v),
        }
    }
}

/// Latest persisted tool runtime config.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct ToolRuntimeLatest {
    /// Content map.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) content_map: BTreeMap<String, String>,
    /// Impure flag.
    #[serde(default)]
    pub(crate) impure: bool,
    /// Inherited env var names to resolve from host environment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) inherited_env_vars: Vec<String>,
    /// Max concurrent calls.
    #[serde(default)]
    pub(crate) max_concurrent_calls: usize,
    /// Max retries.
    #[serde(default)]
    pub(crate) max_retries: usize,
}

/// Runtime configuration for the conductor itself (not per-tool).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct ConductorRuntimeConfigLatest {
    /// Whether impure tool calls may be retried automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) retry_impure: Option<bool>,
    /// Platform-keyed inherited env var names.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) platform_inherited_env_vars: BTreeMap<String, Vec<String>>,
}

/// Latest persisted tool kind (tagged by `kind` field).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ToolKindLatest {
    /// Builtin tool.
    Builtin {
        /// Versioned builtin identifier (e.g. "echo@v1").
        builtin_id: String,
    },
    /// External executable command.
    Executable {
        /// Executable command (path or name on PATH).
        command: Vec<String>,
        /// Environment variables for the process.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        env_vars: BTreeMap<String, String>,
        /// Accepted exit codes (empty = any non-negative).
        #[serde(default)]
        success_codes: Vec<i32>,
    },
}

/// Latest persisted tool spec.
///
/// Custom Serialize/Deserialize flattens the tagged `kind` enum into a flat
/// record shape matching the Nickel v2 contract: `kind = "builtin"` as a plain
/// string with variant-specific fields (`name`, `version`, `command`, etc.) as
/// sibling entries rather than nested under `kind = { kind = "builtin", ... }`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ToolSpecLatest {
    /// Tool kind.
    pub(crate) kind: ToolKindLatest,
    /// Logical tool name (display-only).
    pub(crate) name: String,
    /// Declared inputs.
    pub(crate) inputs: BTreeMap<String, ToolInputSpecLatest>,
    /// Default input values.
    pub(crate) default_inputs: BTreeMap<String, InputBindingLatest>,
    /// Declared output specs keyed by output name.
    pub(crate) outputs: BTreeMap<String, OutputCaptureSpecLatest>,
    /// Runtime config.
    pub(crate) runtime: ToolRuntimeLatest,
}

impl Serialize for ToolSpecLatest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(None)?;

        match &self.kind {
            ToolKindLatest::Builtin { builtin_id } => {
                map.serialize_entry("kind", "builtin")?;
                map.serialize_entry("builtin_id", builtin_id)?;
            }
            ToolKindLatest::Executable { command, env_vars, success_codes } => {
                map.serialize_entry("kind", "executable")?;
                map.serialize_entry("command", command)?;
                if !env_vars.is_empty() {
                    map.serialize_entry("env_vars", env_vars)?;
                }
                if !success_codes.is_empty() {
                    map.serialize_entry("success_codes", success_codes)?;
                }
            }
        }

        map.serialize_entry("name", &self.name)?;

        if !self.inputs.is_empty() {
            map.serialize_entry("inputs", &self.inputs)?;
        }
        if !self.default_inputs.is_empty() {
            map.serialize_entry("default_inputs", &self.default_inputs)?;
        }
        if !self.outputs.is_empty() {
            map.serialize_entry("outputs", &self.outputs)?;
        }
        map.serialize_entry("runtime", &self.runtime)?;

        map.end()
    }
}

impl<'de> Deserialize<'de> for ToolSpecLatest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        // Capture the entire record as a JSON value then extract fields.
        // This intermediate step lets us flatten the tagged `kind` enum into a
        // flat string + sibling variant fields.
        let mut value = serde_json::Value::deserialize(deserializer)?;

        // The Nickel deserializer exports all numbers as f64 (including
        // integers).  serde_json::from_value for Rust integer types (such as
        // `usize` in ToolRuntimeLatest) rejects f64 values.  Walk the value
        // tree and convert any float representing a whole number into its
        // corresponding integer representation so downstream
        // serde_json::from_value calls succeed.
        fn normalize_numbers(val: &mut serde_json::Value) {
            const MAX_U64_AS_F64: f64 = u64::MAX as f64;
            const MAX_I64_AS_F64: f64 = i64::MAX as f64;
            const MIN_I64_AS_F64: f64 = i64::MIN as f64;

            match val {
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        if f.is_finite() && f.fract() == 0.0 {
                            if f >= 0.0 && f <= MAX_U64_AS_F64 {
                                *val =
                                    serde_json::Value::Number(serde_json::Number::from(f as u64));
                            } else if f >= MIN_I64_AS_F64 && f <= MAX_I64_AS_F64 {
                                *val =
                                    serde_json::Value::Number(serde_json::Number::from(f as i64));
                            }
                        }
                    }
                }
                serde_json::Value::Array(arr) => {
                    arr.iter_mut().for_each(normalize_numbers);
                }
                serde_json::Value::Object(obj) => {
                    obj.values_mut().for_each(normalize_numbers);
                }
                _ => {}
            }
        }

        normalize_numbers(&mut value);

        let map = value
            .as_object()
            .ok_or_else(|| D::Error::custom("expected a map for ToolSpecLatest"))?;

        let kind_str = map
            .get("kind")
            .and_then(|v| v.as_str())
            .ok_or_else(|| D::Error::missing_field("kind"))?;

        let name = map
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| D::Error::missing_field("name"))?
            .to_string();

        let kind = match kind_str {
            "builtin" => {
                let builtin_id = map
                    .get("builtin_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| D::Error::missing_field("builtin_id"))?
                    .to_string();
                ToolKindLatest::Builtin { builtin_id }
            }
            "executable" => {
                let command: Vec<String> = map
                    .get("command")
                    .ok_or_else(|| D::Error::missing_field("command"))?
                    .as_array()
                    .ok_or_else(|| D::Error::custom("expected command to be an array of strings"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .ok_or_else(|| {
                                D::Error::custom("expected command element to be a string")
                            })
                            .map(String::from)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let env_vars = map
                    .get("env_vars")
                    .map(|v| serde_json::from_value(v.clone()))
                    .transpose()
                    .map_err(|e| D::Error::custom(format!("invalid env_vars: {e}")))?
                    .unwrap_or_default();

                let success_codes = map
                    .get("success_codes")
                    .map(|v| serde_json::from_value(v.clone()))
                    .transpose()
                    .map_err(|e| D::Error::custom(format!("invalid success_codes: {e}")))?
                    .unwrap_or_default();

                ToolKindLatest::Executable { command, env_vars, success_codes }
            }
            other => {
                return Err(D::Error::custom(format!(
                    "unknown tool kind '{other}'; expected 'builtin' or 'executable'"
                )));
            }
        };

        let inputs = map
            .get("inputs")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| D::Error::custom(format!("invalid inputs: {e}")))?
            .unwrap_or_default();

        let default_inputs = map
            .get("default_inputs")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| D::Error::custom(format!("invalid default_inputs: {e}")))?
            .unwrap_or_default();

        let outputs = map
            .get("outputs")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| D::Error::custom(format!("invalid outputs: {e}")))?
            .unwrap_or_default();

        let runtime = map
            .get("runtime")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| D::Error::custom(format!("invalid runtime: {e}")))?
            .unwrap_or_default();

        Ok(ToolSpecLatest { kind, name, inputs, default_inputs, outputs, runtime })
    }
}

/// Latest persisted workflow step spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct WorkflowStepSpecLatest {
    /// Step id.
    pub(crate) id: String,
    /// Referenced tool name.
    pub(crate) tool: String,
    /// Input values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) inputs: BTreeMap<String, String>,
    /// Output capture specs keyed by output name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) outputs: BTreeMap<String, OutputCaptureSpecLatest>,
    /// Max retries.
    #[serde(default)]
    pub(crate) max_retries: usize,
    /// Explicit dependencies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) depends_on: Vec<String>,
}

/// Latest persisted workflow spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct WorkflowSpecLatest {
    /// Workflow name.
    pub(crate) name: String,
    /// Display label.
    #[serde(default)]
    pub(crate) display_name: String,
    /// Description.
    #[serde(default)]
    pub(crate) description: String,
    /// Impure flag.
    #[serde(default)]
    pub(crate) impure: bool,
    /// Ordered steps.
    #[serde(default)]
    pub(crate) steps: Vec<WorkflowStepSpecLatest>,
}

/// Latest persisted external data entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExternalDataEntryLatest {
    /// CAS hash of the external blob.
    pub(crate) hash: mediapm_cas::Hash,
    /// Human-readable description.
    pub(crate) description: String,
    /// Save policy for this blob.
    pub(crate) save_mode: OutputPolicyLatest,
}

/// Top-level Nickel envelope for the latest schema version.
///
/// This is the primary deserialization target after Nickel evaluation and
/// migration.  All persisted documents produce this type on decode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct NickelEnvelopeLatest {
    /// Schema version marker.
    pub(crate) version: u32,
    /// Tool definitions keyed by tool name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) tools: BTreeMap<String, ToolSpecLatest>,
    /// Workflow definitions.
    #[serde(default)]
    pub(crate) workflows: Vec<WorkflowSpecLatest>,
    /// External data entries keyed by CAS hash.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) external_data: BTreeMap<mediapm_cas::Hash, ExternalDataEntryLatest>,
    /// Conductor-level runtime configuration.
    #[serde(default)]
    pub(crate) runtime: ConductorRuntimeConfigLatest,
}

// ---------------------------------------------------------------------------
// Bridge: persisted envelope → runtime config types
// ---------------------------------------------------------------------------

impl From<NickelEnvelopeLatest> for NickelDocument {
    fn from(envelope: NickelEnvelopeLatest) -> Self {
        NickelDocument {
            tools: envelope
                .tools
                .into_iter()
                .map(|(name, spec)| (name, tool_spec_from_latest(spec)))
                .collect(),
            workflows: envelope.workflows.into_iter().map(workflow_spec_from_latest).collect(),
            external_data: envelope
                .external_data
                .into_iter()
                .map(|(hash, entry)| {
                    (
                        hash,
                        super::super::ExternalDataEntry {
                            description: entry.description,
                            save_mode: match entry.save_mode {
                                OutputPolicyLatest::Bool(true) => {
                                    crate::state::OutputSaveMode::Saved
                                }
                                OutputPolicyLatest::Bool(false) => {
                                    crate::state::OutputSaveMode::Unsaved
                                }
                                OutputPolicyLatest::Full => crate::state::OutputSaveMode::Full,
                            },
                        },
                    )
                })
                .collect(),
            runtime: envelope.runtime.into(),
        }
    }
}

impl From<NickelDocument> for NickelEnvelopeLatest {
    fn from(doc: NickelDocument) -> Self {
        NickelEnvelopeLatest {
            version: NICKEL_VERSION_LATEST,
            tools: doc
                .tools
                .into_iter()
                .map(|(name, spec)| (name, tool_spec_to_latest(spec)))
                .collect(),
            workflows: doc.workflows.into_iter().map(workflow_spec_to_latest).collect(),
            external_data: doc
                .external_data
                .into_iter()
                .map(|(hash, entry)| {
                    (
                        hash,
                        ExternalDataEntryLatest {
                            hash,
                            description: entry.description,
                            save_mode: match entry.save_mode {
                                crate::state::OutputSaveMode::Saved => {
                                    OutputPolicyLatest::Bool(true)
                                }
                                crate::state::OutputSaveMode::Unsaved => {
                                    OutputPolicyLatest::Bool(false)
                                }
                                crate::state::OutputSaveMode::Full => OutputPolicyLatest::Full,
                            },
                        },
                    )
                })
                .collect(),
            runtime: doc.runtime.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

impl From<ConductorRuntimeConfigLatest> for super::super::ConductorRuntimeConfig {
    fn from(rt: ConductorRuntimeConfigLatest) -> Self {
        super::super::ConductorRuntimeConfig {
            // `None` (absent in config) resolves to `false` at the boundary.
            retry_impure: rt.retry_impure.unwrap_or(false),
            platform_inherited_env_vars: rt.platform_inherited_env_vars,
        }
    }
}

impl From<super::super::ConductorRuntimeConfig> for ConductorRuntimeConfigLatest {
    fn from(rt: super::super::ConductorRuntimeConfig) -> Self {
        ConductorRuntimeConfigLatest {
            retry_impure: Some(rt.retry_impure),
            platform_inherited_env_vars: rt.platform_inherited_env_vars,
        }
    }
}

fn tool_spec_from_latest(spec: ToolSpecLatest) -> ToolSpec {
    ToolSpec {
        kind: match spec.kind {
            ToolKindLatest::Builtin { builtin_id } => ToolKindSpec::Builtin { builtin_id },
            ToolKindLatest::Executable { command, env_vars, success_codes } => {
                ToolKindSpec::Executable { command, env_vars, success_codes }
            }
        },
        name: spec.name,
        inputs: spec
            .inputs
            .into_iter()
            .map(|(name, input)| {
                (
                    name,
                    ToolInputSpec {
                        kind: match input.kind {
                            ToolInputKindLatest::String => ToolInputKind::String,
                            ToolInputKindLatest::Content => ToolInputKind::Content,
                            ToolInputKindLatest::Env => ToolInputKind::Env,
                            ToolInputKindLatest::StringList => ToolInputKind::StringList,
                        },
                        required: input.required,
                    },
                )
            })
            .collect(),
        default_inputs: spec
            .default_inputs
            .into_iter()
            .map(|(k, v)| (k, super::super::InputBinding::from(v)))
            .collect(),
        outputs: spec
            .outputs
            .into_iter()
            .map(|(name, o)| {
                let name_clone = name.clone();
                (
                    name,
                    OutputCaptureSpec {
                        name: name_clone,
                        capture: o.capture,
                        save: match o.save {
                            SaveModeLatest::False => SaveMode::False,
                            SaveModeLatest::True => SaveMode::True,
                            SaveModeLatest::Full => SaveMode::Full,
                        },
                        allow_empty: o.allow_empty,
                        include_topmost_folder: o.include_topmost_folder,
                    },
                )
            })
            .collect(),
        runtime: tool_runtime_from_latest(spec.runtime),
    }
}

fn tool_spec_to_latest(spec: ToolSpec) -> ToolSpecLatest {
    ToolSpecLatest {
        kind: match spec.kind {
            ToolKindSpec::Builtin { builtin_id } => ToolKindLatest::Builtin { builtin_id },
            ToolKindSpec::Executable { command, env_vars, success_codes } => {
                ToolKindLatest::Executable { command, env_vars, success_codes }
            }
        },
        name: spec.name,
        inputs: spec
            .inputs
            .into_iter()
            .map(|(name, input)| {
                (
                    name,
                    ToolInputSpecLatest {
                        kind: match input.kind {
                            ToolInputKind::String => ToolInputKindLatest::String,
                            ToolInputKind::Content => ToolInputKindLatest::Content,
                            ToolInputKind::Env => ToolInputKindLatest::Env,
                            ToolInputKind::StringList => ToolInputKindLatest::StringList,
                        },
                        required: input.required,
                    },
                )
            })
            .collect(),
        default_inputs: spec
            .default_inputs
            .into_iter()
            .map(|(k, v)| (k, InputBindingLatest::from(v)))
            .collect(),
        outputs: spec
            .outputs
            .into_iter()
            .map(|(name, o)| {
                let name_clone = name.clone();
                (
                    name,
                    OutputCaptureSpecLatest {
                        name: name_clone,
                        capture: o.capture,
                        save: match o.save {
                            SaveMode::False => SaveModeLatest::False,
                            SaveMode::True => SaveModeLatest::True,
                            SaveMode::Full => SaveModeLatest::Full,
                        },
                        allow_empty: o.allow_empty,
                        include_topmost_folder: o.include_topmost_folder,
                    },
                )
            })
            .collect(),
        runtime: tool_runtime_to_latest(spec.runtime),
    }
}

fn tool_runtime_from_latest(rt: ToolRuntimeLatest) -> ToolRuntime {
    ToolRuntime {
        content_map: rt.content_map,
        impure: rt.impure,
        inherited_env_vars: rt.inherited_env_vars,
        max_concurrent_calls: rt.max_concurrent_calls,
        max_retries: rt.max_retries,
    }
}

fn tool_runtime_to_latest(rt: ToolRuntime) -> ToolRuntimeLatest {
    ToolRuntimeLatest {
        content_map: rt.content_map,
        impure: rt.impure,
        inherited_env_vars: rt.inherited_env_vars,
        max_concurrent_calls: rt.max_concurrent_calls,
        max_retries: rt.max_retries,
    }
}

fn workflow_spec_from_latest(spec: WorkflowSpecLatest) -> WorkflowSpec {
    WorkflowSpec {
        name: spec.name,
        display_name: spec.display_name,
        description: spec.description,
        impure: spec.impure,
        steps: spec.steps.into_iter().map(step_spec_from_latest).collect(),
    }
}

fn workflow_spec_to_latest(spec: WorkflowSpec) -> WorkflowSpecLatest {
    WorkflowSpecLatest {
        name: spec.name,
        display_name: spec.display_name,
        description: spec.description,
        impure: spec.impure,
        steps: spec.steps.into_iter().map(step_spec_to_latest).collect(),
    }
}

fn step_spec_from_latest(step: WorkflowStepSpecLatest) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: step.id,
        tool: step.tool,
        inputs: step.inputs,
        outputs: step
            .outputs
            .into_iter()
            .map(|(name, o)| {
                let name_clone = name.clone();
                (
                    name,
                    OutputCaptureSpec {
                        name: name_clone,
                        capture: o.capture,
                        save: match o.save {
                            SaveModeLatest::False => SaveMode::False,
                            SaveModeLatest::True => SaveMode::True,
                            SaveModeLatest::Full => SaveMode::Full,
                        },
                        allow_empty: o.allow_empty,
                        include_topmost_folder: o.include_topmost_folder,
                    },
                )
            })
            .collect(),
        max_retries: step.max_retries,
        depends_on: step.depends_on,
    }
}

fn step_spec_to_latest(step: WorkflowStepSpec) -> WorkflowStepSpecLatest {
    WorkflowStepSpecLatest {
        id: step.id,
        tool: step.tool,
        inputs: step.inputs,
        outputs: step
            .outputs
            .into_iter()
            .map(|(name, o)| {
                let name_clone = name.clone();
                (
                    name,
                    OutputCaptureSpecLatest {
                        name: name_clone,
                        capture: o.capture,
                        save: match o.save {
                            SaveMode::False => SaveModeLatest::False,
                            SaveMode::True => SaveModeLatest::True,
                            SaveMode::Full => SaveModeLatest::Full,
                        },
                        allow_empty: o.allow_empty,
                        include_topmost_folder: o.include_topmost_folder,
                    },
                )
            })
            .collect(),
        max_retries: step.max_retries,
        depends_on: step.depends_on,
    }
}

#[cfg(test)]
mod tests {
    //! Tests for latest envelope ↔ runtime config conversion and
    //! serialization round-trip through the Nickel encoding pipeline.
    use super::*;

    /// Verifies that `NickelEnvelopeLatest` round-trips through
    /// `NickelDocument` without data loss.
    #[test]
    fn envelope_round_trip() {
        let envelope = NickelEnvelopeLatest {
            version: NICKEL_VERSION_LATEST,
            tools: BTreeMap::from([(
                "echo@v1".to_string(),
                ToolSpecLatest {
                    kind: ToolKindLatest::Builtin { builtin_id: "echo@v1".to_string() },
                    name: "echo".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntimeLatest::default(),
                },
            )]),
            workflows: vec![],
            runtime: ConductorRuntimeConfigLatest::default(),
            external_data: BTreeMap::new(),
        };

        let doc: NickelDocument = envelope.clone().into();
        let back: NickelEnvelopeLatest = doc.into();

        assert_eq!(envelope.version, back.version);
        assert_eq!(envelope.tools.len(), back.tools.len());
        assert!(back.tools.contains_key("echo@v1"));
        assert_eq!(back.tools["echo@v1"].name, "echo".to_string());
    }

    /// Verifies that a document containing both Builtin and Executable tools
    /// survives a full `encode_document` → `decode_document` round-trip
    /// through the Nickel rendering and evaluation pipeline.
    #[test]
    fn tool_spec_encode_decode_round_trip() {
        let doc = NickelDocument {
            tools: BTreeMap::from([
                (
                    "echo@v1".to_string(),
                    ToolSpec {
                        kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
                        name: "echo".to_string(),
                        inputs: BTreeMap::new(),
                        default_inputs: BTreeMap::new(),
                        outputs: BTreeMap::new(),
                        runtime: ToolRuntime::default(),
                    },
                ),
                (
                    "ffmpeg".to_string(),
                    ToolSpec {
                        kind: ToolKindSpec::Executable {
                            command: vec!["ffmpeg".to_string()],
                            env_vars: BTreeMap::from([(
                                "PATH".to_string(),
                                "/usr/bin".to_string(),
                            )]),
                            success_codes: vec![0, 1],
                        },
                        name: "ffmpeg".to_string(),
                        inputs: BTreeMap::from([(
                            "input_file".to_string(),
                            ToolInputSpec { kind: ToolInputKind::Content, required: true },
                        )]),
                        default_inputs: BTreeMap::new(),
                        outputs: BTreeMap::from([(
                            "output".to_string(),
                            OutputCaptureSpec {
                                name: "output".to_string(),
                                capture: "stdout".to_string(),
                                save: SaveMode::False,
                                allow_empty: false,
                                include_topmost_folder: true,
                            },
                        )]),
                        runtime: ToolRuntime {
                            content_map: BTreeMap::new(),
                            impure: true,
                            inherited_env_vars: Vec::new(),
                            max_concurrent_calls: 2,
                            max_retries: 1,
                        },
                    },
                ),
            ]),
            workflows: vec![],
            runtime: crate::config::ConductorRuntimeConfig::default(),
            external_data: BTreeMap::new(),
        };

        let encoded = super::super::encode_document(doc.clone()).expect("encode");
        let decoded = super::super::decode_document(&encoded).expect("decode");

        assert_eq!(doc.tools.len(), decoded.tools.len(), "tool count mismatch");

        // Verify Builtin tool round-trip.
        let echo_orig = doc.tools.get("echo@v1").expect("echo in original");
        let echo_decoded = decoded.tools.get("echo@v1").expect("echo in decoded");
        assert_eq!(echo_orig.kind, echo_decoded.kind, "echo kind mismatch");
        assert_eq!(echo_orig.name, echo_decoded.name, "echo name mismatch");

        // Verify Executable tool round-trip.
        let ffmpeg_orig = doc.tools.get("ffmpeg").expect("ffmpeg in original");
        let ffmpeg_decoded = decoded.tools.get("ffmpeg").expect("ffmpeg in decoded");
        assert_eq!(ffmpeg_orig.kind, ffmpeg_decoded.kind, "ffmpeg kind mismatch");
        assert_eq!(ffmpeg_orig.name, ffmpeg_decoded.name, "ffmpeg name mismatch");
    }
}
