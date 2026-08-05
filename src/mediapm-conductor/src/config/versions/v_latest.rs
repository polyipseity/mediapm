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
#[derive(Default)]
pub(crate) enum SaveModeLatest {
    /// Do not persist this output.
    False,
    /// Persist this output normally.
    #[default]
    True,
    /// Force full persistence even when empty or the step fails.
    Full,
}

/// Latest persisted output capture spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if invokes the predicate with &T"
)]
const fn is_save_mode_true_latest(v: &SaveModeLatest) -> bool {
    matches!(*v, SaveModeLatest::True)
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if invokes the predicate with &T"
)]
const fn is_true(v: &bool) -> bool {
    *v
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if invokes the predicate with &T"
)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub(crate) struct ConductorRuntimeConfigLatest {
    /// Whether impure tool calls may be retried automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) retry_impure: Option<bool>,
    /// Platform-keyed inherited env var names (typed; keys closed to
    /// windows/linux/macos, S-D3).
    #[serde(default, skip_serializing_if = "super::super::PlatformInheritedEnvVars::is_empty")]
    pub(crate) platform_inherited_env_vars: super::super::PlatformInheritedEnvVars,
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

/// Flat-record keys accepted by `ToolSpecLatest`'s custom Deserialize (S-D2).
const TOOL_SPEC_LATEST_KNOWN_KEYS: [&str; 10] = [
    "kind",
    "name",
    "builtin_id",
    "command",
    "env_vars",
    "success_codes",
    "inputs",
    "default_inputs",
    "outputs",
    "runtime",
];

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
    #[expect(clippy::too_many_lines, reason = "custom flattening deserializer is necessarily long")]
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        // The Nickel deserializer exports all numbers as f64 (including
        // integers).  serde_json::from_value for Rust integer types (such as
        // `usize` in ToolRuntimeLatest) rejects f64 values.  Walk the value
        // tree and convert any float representing a whole number into its
        // corresponding integer representation so downstream
        // serde_json::from_value calls succeed.
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_precision_loss,
            clippy::cast_sign_loss,
            reason = "f64 bounds approximate u64/i64 limits; the range checks make each cast lossless within its arm"
        )]
        fn normalize_numbers(val: &mut serde_json::Value) {
            const MAX_U64_AS_F64: f64 = u64::MAX as f64;
            const MAX_I64_AS_F64: f64 = i64::MAX as f64;
            const MIN_I64_AS_F64: f64 = i64::MIN as f64;

            match val {
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64()
                        && f.is_finite()
                        && f.fract() == 0.0
                    {
                        if (0.0..=MAX_U64_AS_F64).contains(&f) {
                            *val = serde_json::Value::Number(serde_json::Number::from(f as u64));
                        } else if (MIN_I64_AS_F64..=MAX_I64_AS_F64).contains(&f) {
                            *val = serde_json::Value::Number(serde_json::Number::from(f as i64));
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

        // Capture the entire record as a JSON value then extract fields.
        // This intermediate step lets us flatten the tagged `kind` enum into a
        // flat string + sibling variant fields.
        let mut value = serde_json::Value::deserialize(deserializer)?;

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

        // S-D2: reject unknown keys on the flat record.  Derive-based
        // `deny_unknown_fields` does not apply to custom Deserialize impls,
        // so the check is manual over the captured map.
        if let Some(unknown) =
            map.keys().find(|k| !TOOL_SPEC_LATEST_KNOWN_KEYS.contains(&k.as_str()))
        {
            return Err(D::Error::custom(format!("unknown field '{unknown}' for ToolSpecLatest")));
        }

        Ok(ToolSpecLatest { kind, name, inputs, default_inputs, outputs, runtime })
    }
}

/// Latest persisted workflow step spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowSpecLatest {
    /// Workflow name.
    pub(crate) name: String,
    /// Display label.
    ///
    /// Omitted when absent: the v2 Nickel contract types these fields as
    /// `| NonEmptyStringV2 | optional`, which rejects empty strings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    /// Description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Impure flag.
    #[serde(default)]
    pub(crate) impure: bool,
    /// Ordered steps.
    #[serde(default)]
    pub(crate) steps: Vec<WorkflowStepSpecLatest>,
}

/// Latest persisted external data entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExternalDataEntryLatest {
    /// CAS hash of the external blob (redundant with map key; kept for
    /// compatibility).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) hash: Option<mediapm_cas::Hash>,
    /// Human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// Save policy for this blob.
    ///
    /// Optional: absent defaults to `Saved` at the decode boundary (explicit
    /// beats implicit).
    #[serde(rename = "save", default, skip_serializing_if = "Option::is_none")]
    pub(crate) save_mode: Option<OutputPolicyLatest>,
}

/// Top-level Nickel envelope for the latest schema version.
///
/// This is the primary deserialization target after Nickel evaluation and
/// migration.  All persisted documents produce this type on decode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
                            save_mode: entry.save_mode.map_or(
                                crate::state::OutputSaveMode::Saved,
                                |save_mode| match save_mode {
                                    OutputPolicyLatest::Bool(true) => {
                                        crate::state::OutputSaveMode::Saved
                                    }
                                    OutputPolicyLatest::Bool(false) => {
                                        crate::state::OutputSaveMode::Unsaved
                                    }
                                    OutputPolicyLatest::Full => crate::state::OutputSaveMode::Full,
                                },
                            ),
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
                            hash: Some(hash),
                            description: entry.description,
                            save_mode: Some(match entry.save_mode {
                                crate::state::OutputSaveMode::Saved => {
                                    OutputPolicyLatest::Bool(true)
                                }
                                crate::state::OutputSaveMode::Unsaved => {
                                    OutputPolicyLatest::Bool(false)
                                }
                                crate::state::OutputSaveMode::Full => OutputPolicyLatest::Full,
                            }),
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

        // S-D4: the tightened serde schema must be lossless — serialize the
        // bridge envelope and re-decode it through the strict structs.
        let json = serde_json::to_value(&back).expect("envelope serializes");
        let redecoded: NickelEnvelopeLatest =
            serde_json::from_value(json).expect("strict envelope must re-decode");
        assert_eq!(redecoded, back, "strict serde round-trip must be lossless");
    }

    /// Verifies the shared generated-file banner: every encoded document
    /// starts with the identical banner, and `encode → decode → encode` is
    /// byte-stable (the `#`-comment banner is ignored by the Nickel
    /// evaluator, so re-encoding reproduces the exact same bytes).
    #[test]
    fn encode_document_banner_round_trip_byte_stable() {
        let doc = NickelDocument {
            tools: BTreeMap::new(),
            workflows: vec![],
            runtime: crate::config::ConductorRuntimeConfig::default(),
            external_data: BTreeMap::new(),
        };

        let first = super::super::encode_document(doc.clone()).expect("encode");
        assert!(
            first.starts_with(mediapm_utils::generated::GENERATED_FILE_BANNER.as_bytes()),
            "encoded document must start with the shared generated-file banner",
        );

        let decoded = super::super::decode_document(&first).expect("decode");
        let second = super::super::encode_document(decoded).expect("re-encode");
        assert_eq!(first, second, "encode → decode → encode must be byte-stable");
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

    // ---------------------------------------------------------------------
    // Rust serde strictness (S-D2)
    // ---------------------------------------------------------------------

    /// S-D2: `NickelEnvelopeLatest` rejects unknown fields via serde.
    #[test]
    fn strict_serde_envelope_rejects_unknown_field() {
        let err = serde_json::from_value::<NickelEnvelopeLatest>(serde_json::json!({
            "version": 2,
            "bogus_field": 1,
        }))
        .expect_err("unknown envelope field must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
    }

    /// S-D2: `ExternalDataEntryLatest` rejects unknown fields via serde.
    #[test]
    fn strict_serde_external_data_entry_rejects_unknown_field() {
        let err = serde_json::from_value::<ExternalDataEntryLatest>(serde_json::json!({
            "description": "x",
            "save": true,
            "bogus_field": 1,
        }))
        .expect_err("unknown external data entry field must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
    }

    /// S-D2: `ToolSpecLatest` rejects unknown fields. `deny_unknown_fields`
    /// cannot be applied to a custom `Deserialize` impl, so this behavior
    /// must be enforced manually inside the flattening deserializer.
    #[test]
    fn strict_serde_tool_spec_latest_rejects_unknown_field() {
        let err = serde_json::from_value::<ToolSpecLatest>(serde_json::json!({
            "kind": "builtin",
            "builtin_id": "echo@v1",
            "name": "echo",
            "bogus_field": 1,
        }))
        .expect_err("unknown ToolSpecLatest field must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
    }

    /// S-D2: the remaining latest-version spec structs reject unknown fields
    /// via serde.
    #[test]
    fn strict_serde_latest_specs_reject_unknown_fields() {
        let err = serde_json::from_value::<WorkflowSpecLatest>(serde_json::json!({
            "name": "wf",
            "bogus_field": 1,
        }))
        .expect_err("unknown WorkflowSpecLatest field must be rejected");
        assert!(format!("{err}").contains("bogus_field"));

        let err = serde_json::from_value::<WorkflowStepSpecLatest>(serde_json::json!({
            "id": "s1",
            "tool": "echo",
            "bogus_field": 1,
        }))
        .expect_err("unknown WorkflowStepSpecLatest field must be rejected");
        assert!(format!("{err}").contains("bogus_field"));

        let err = serde_json::from_value::<ToolRuntimeLatest>(serde_json::json!({
            "impure": true,
            "bogus_field": 1,
        }))
        .expect_err("unknown ToolRuntimeLatest field must be rejected");
        assert!(format!("{err}").contains("bogus_field"));

        let err = serde_json::from_value::<OutputCaptureSpecLatest>(serde_json::json!({
            "name": "out",
            "capture": "stdout",
            "bogus_field": 1,
        }))
        .expect_err("unknown OutputCaptureSpecLatest field must be rejected");
        assert!(format!("{err}").contains("bogus_field"));

        let err = serde_json::from_value::<ToolInputSpecLatest>(serde_json::json!({
            "kind": "string",
            "bogus_field": 1,
        }))
        .expect_err("unknown ToolInputSpecLatest field must be rejected");
        assert!(format!("{err}").contains("bogus_field"));

        let err = serde_json::from_value::<ConductorRuntimeConfigLatest>(serde_json::json!({
            "bogus_field": 1,
        }))
        .expect_err("unknown ConductorRuntimeConfigLatest field must be rejected");
        assert!(format!("{err}").contains("bogus_field"));
    }
}
