//! Runtime configuration document model.
//!
//! This module provides the simplified conductor configuration surface used by
//! runtime planning and CLI workflows.  Key simplifications relative to the
//! old three-document model:
//!
//! - No `Option` wrappers on config fields where sensible defaults exist;
//!   defaults are centralized in `crate::defaults`.
//! - Per-tool `runtime` (replaces the old separate `tool_configs` map) —
//!   runtime-execution tuning now lives inline on each tool.
//! - No `PlatformInheritedEnvVars` — simplified to a single flat map.
//! - No `fp-library` optics; versioning follows `mediapm-cas` pattern.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub mod documents;
pub(crate) use documents::NickelDocument;
pub(crate) mod nickel_io;
pub mod versions;

/// Optional per-output persistence override.
///
/// Controls whether a workflow step output is saved to CAS, and with what
/// level of detail.  `Bool(true)` saves normally, `Bool(false)` marks the
/// output as unsaved (volatile only), and `Full` requests full-data preference
/// hints during persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OutputPolicy {
    /// Boolean save mode (`false` = unsaved, `true` = saved).
    Bool(bool),
    /// Full-data-preferred save mode.
    Full,
}

impl Default for OutputPolicy {
    fn default() -> Self {
        Self::Bool(true)
    }
}

/// Tool content map entry: relative path → CAS hash or inline content
/// description string.
pub type ToolContentMap = BTreeMap<String, String>;

/// External data entry: CAS hash → description + save mode.
///
/// Describes how a CAS-referenced blob that is NOT part of any tool content
/// map should be retained and persisted.  Entries are declared in config so
/// conductor GC does not prune these hashes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalDataEntry {
    /// Human-readable description of this external data.
    ///
    /// Optional: present only when the source config document carries one.
    /// Descriptions are never merged or compared across documents; they are
    /// preserved per hash at save time from the file being overwritten.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Save policy governing this blob.
    pub save_mode: crate::state::OutputSaveMode,
}

/// Kind of tool input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ToolInputKind {
    /// Simple key-value string pair.
    #[default]
    String,
    /// File content addressed by CAS hash.
    Content,
    /// Environment variable passthrough.
    Env,
    /// JSON array of strings (e.g., `["--arg1", "--arg2"]`).
    StringList,
}

/// Specification for one tool input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ToolInputSpec {
    /// Declared value kind for this input.
    #[serde(default)]
    pub kind: ToolInputKind,
    /// Whether this input is required for every tool call.
    #[serde(default)]
    pub required: bool,
}

/// Whether to persist a captured output to CAS: `"false"`, `"true"`, or `"full"`.
///
/// - `False`: do not persist this output.
/// - `True` (default): persist this output normally.
/// - `Full`: persist even when empty or the step fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum SaveMode {
    /// Do not persist this output.
    False,
    /// Persist this output normally.
    #[default]
    True,
    /// Force full persistence even when empty or the step fails.
    Full,
}

/// Default for `OutputCaptureSpec.save`: persist.
const fn default_save_mode() -> SaveMode {
    SaveMode::True
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if invokes the predicate with &T"
)]
fn is_save_mode_true(v: &SaveMode) -> bool {
    matches!(v, SaveMode::True)
}

/// Default for `OutputCaptureSpec.allow_empty`: skip on missing.
const fn default_allow_empty() -> bool {
    false
}

/// Default for `OutputCaptureSpec.include_topmost_folder`: include folder name.
const fn default_include_topmost_folder() -> bool {
    true
}

/// Input binding: a single string or an array of strings.
///
/// Used in `default_inputs` to support both scalar and list defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InputBinding {
    /// Single string value.
    String(String),
    /// Array of string values (JSON-encoded for template splat resolution).
    Vec(Vec<String>),
}

impl Default for InputBinding {
    fn default() -> Self {
        Self::String(String::new())
    }
}

impl From<String> for InputBinding {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<Vec<String>> for InputBinding {
    fn from(v: Vec<String>) -> Self {
        Self::Vec(v)
    }
}

/// Capture/output spec for a step.
///
/// Describes how output bytes are captured from a tool execution:
/// - `stdout` / `stderr`: capture standard streams,
/// - `process_code`: capture exit code as text,
/// - `file:<path>`: capture bytes from a relative file path,
/// - `file_regex:<pattern>`: capture bytes from a file matching a regex.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputCaptureSpec {
    /// Logical output name.
    pub name: String,
    /// Capture source selector (`stdout`, `stderr`, `process_code`, `file:<path>`,
    /// `file_regex:<pattern>`, etc.).
    pub capture: String,
    /// Whether to persist this output to CAS. One of `"false"`, `"true"`, `"full"`.
    #[serde(default = "default_save_mode", skip_serializing_if = "is_save_mode_true")]
    pub save: SaveMode,
    /// Whether an empty capture result (e.g. missing file) is acceptable.
    /// When `true`, an empty result is stored as empty bytes instead of
    /// silently skipping the output.
    #[serde(default = "default_allow_empty")]
    pub allow_empty: bool,
    /// Whether `folder:` capture listings include the topmost folder name.
    /// When `true` (default), paths in the listing are relative to the
    /// sandbox root (including the topmost folder). When `false`, paths
    /// are relative to the captured folder itself.
    #[serde(default = "default_include_topmost_folder")]
    pub include_topmost_folder: bool,
}

impl Default for OutputCaptureSpec {
    fn default() -> Self {
        Self {
            name: String::new(),
            capture: String::new(),
            save: SaveMode::True,
            allow_empty: false,
            include_topmost_folder: true,
        }
    }
}

/// Runtime configuration for a tool (replaces the old separate `tool_configs`
/// map in the V1 schema).
///
/// The `runtime` property on a tool holds fields that must NOT be part of the
/// tool-call-instance identity computation.  Changes to runtime fields affect
/// scheduling and sandbox materialization but do not invalidate cached outputs.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRuntime {
    /// Content map: relative path → CAS hash or inline base64 payload.
    ///
    /// Keys ending with `/` designate directory extraction targets (ZIP
    /// payload).  Key `./` unpacks into the sandbox root.  Runtime rejects
    /// conflicts where two entries would write the same file path.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub content_map: ToolContentMap,
    /// Whether this tool performs impure (side-effecting) operations.
    ///
    /// Impure tools receive timestamp injection at planning time which makes
    /// each call unique and prevents cache hits across runs.
    #[serde(default)]
    pub impure: bool,
    /// Environment variable names to inherit from the host process.
    ///
    /// These are resolved at `to_unified()` time against the current host
    /// environment and merged with runtime-wide inherited env vars.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inherited_env_vars: Vec<String>,
    /// Maximum concurrent calls allowed for this tool.
    ///
    /// `0` means unlimited.
    #[serde(default)]
    pub max_concurrent_calls: usize,
    /// Maximum retry attempts for this tool after the first failed attempt.
    #[serde(default)]
    pub max_retries: usize,
}

/// Platform-grouped inherited environment-variable names (S-D3).
///
/// Keys are closed to the three supported platforms (`windows`, `linux`,
/// `macos`), mirroring the v2 Nickel `PlatformInheritedEnvVarsV2` contract.
/// Unknown platform keys are rejected at the serde boundary and env var
/// names must be non-empty.  Absent platform keys default to empty lists.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformInheritedEnvVars {
    /// Variable names inherited on Windows.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_non_empty_env_names"
    )]
    pub windows: Vec<String>,
    /// Variable names inherited on Linux.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_non_empty_env_names"
    )]
    pub linux: Vec<String>,
    /// Variable names inherited on macOS.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_non_empty_env_names"
    )]
    pub macos: Vec<String>,
}

impl PlatformInheritedEnvVars {
    /// Returns `true` when no platform has configured env var names.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.windows.is_empty() && self.linux.is_empty() && self.macos.is_empty()
    }

    /// Returns the env var names configured for `platform` (`"windows"`,
    /// `"linux"`, or `"macos"`), or `None` for any other platform string.
    #[must_use]
    pub fn env_names_for(&self, platform: &str) -> Option<&Vec<String>> {
        match platform {
            "windows" => Some(&self.windows),
            "linux" => Some(&self.linux),
            "macos" => Some(&self.macos),
            _ => None,
        }
    }
}

/// Deserializes one platform's inherited env var name list, rejecting empty
/// names (mirrors the Nickel `NonEmptyStringV2` contract, S-D3).
fn deserialize_non_empty_env_names<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    let names = Vec::<String>::deserialize(deserializer)?;
    if let Some(empty) = names.iter().find(|name| name.is_empty()) {
        return Err(D::Error::custom(format!(
            "environment variable names must be non-empty (found empty name {empty:?})"
        )));
    }
    Ok(names)
}

/// Runtime configuration for the conductor itself (not per-tool).
///
/// This is a serde-deserialization boundary type. Fields with meaningful
/// defaults are resolved to their resolved values at the boundary — no
/// `Option<T>` wraps a value that has a sensible default.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConductorRuntimeConfig {
    /// Whether impure tool calls may be retried automatically.
    ///
    /// `None` (absent in config) resolves to `false` at the boundary.
    #[serde(default)]
    pub retry_impure: bool,
    /// Platform-keyed inherited env var names.
    ///
    /// Keys are closed to `windows`/`linux`/`macos`; each value lists
    /// environment variable *names* to inherit from the host process.  These
    /// are resolved at `to_unified()` time against the current platform.
    #[serde(default, skip_serializing_if = "PlatformInheritedEnvVars::is_empty")]
    pub platform_inherited_env_vars: PlatformInheritedEnvVars,
}

/// Kind of tool definition.
///
/// Only two kinds: builtin (built into conductor-builtins crates) or
/// executable (an external command).  Whether a tool is downloaded and cached
/// by conductor is orthogonal to its kind — that is controlled by the
/// runtime content map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolKindSpec {
    /// Built-in tool known to conductor-builtins crates.
    Builtin {
        /// Versioned builtin identifier (e.g. "echo@v1").
        builtin_id: String,
    },
    /// External executable command (on PATH or with an absolute path).
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

/// Specification for a tool.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSpec {
    /// Tool kind (builtin, executable).
    pub kind: ToolKindSpec,
    /// Logical tool name (display-only).
    pub name: String,
    /// Declared input specifications keyed by input name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub inputs: BTreeMap<String, ToolInputSpec>,
    /// Default input values applied when workflow steps omit matching keys.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub default_inputs: BTreeMap<String, InputBinding>,
    /// Declared output specifications for this tool keyed by output name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub outputs: BTreeMap<String, OutputCaptureSpec>,
    /// Runtime configuration (`content_map`, impure, concurrency, retry, etc.).
    #[serde(default)]
    pub runtime: ToolRuntime,
}

impl Default for ToolKindSpec {
    fn default() -> Self {
        Self::Executable { command: Vec::new(), env_vars: BTreeMap::new(), success_codes: vec![0] }
    }
}

/// Specification for a complete workflow DAG.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSpec {
    /// Logical workflow name (used for invocation).
    pub name: String,
    /// Human-readable display label.
    ///
    /// Optional: present only when the source config document carries one.
    /// Display labels are never merged or compared across documents; they are
    /// preserved per name at save time from the file being overwritten.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Human-readable workflow description.
    ///
    /// Optional: present only when the source config document carries one.
    /// Descriptions are never merged or compared across documents; they are
    /// preserved per name at save time from the file being overwritten.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether this workflow contains impure (side-effecting) steps.
    #[serde(default)]
    pub impure: bool,
    /// Ordered list of workflow steps before topological sorting.
    #[serde(default)]
    pub steps: Vec<WorkflowStepSpec>,
}

/// Specification for one workflow step (tool call reference).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowStepSpec {
    /// Unique step identifier within one workflow.
    pub id: String,
    /// Referenced tool name (must match a declared `ToolSpec.name`).
    pub tool: String,
    /// Input values keyed by logical tool-input name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub inputs: BTreeMap<String, String>,
    /// Per-output persistence policy overrides keyed by output name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub outputs: BTreeMap<String, OutputCaptureSpec>,
    /// Override the tool's `max_retries` for this specific step.
    #[serde(default)]
    pub max_retries: usize,
    /// Explicit execution-order dependencies on prior step ids.
    ///
    /// Every `${step_output.<step_id>...}` reference in `inputs` must have a
    /// matching `depends_on` entry.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

/// Returns platform-default inherited environment-variable names.
///
/// These names are merged into executable runtime environments before tool
/// environment overrides so callers can keep baseline process invariants
/// without repeating them per tool.
#[must_use]
pub fn default_runtime_inherited_env_vars() -> BTreeMap<String, String> {
    if cfg!(windows) {
        BTreeMap::from([
            ("PATH".to_string(), String::new()),
            ("SYSTEMROOT".to_string(), String::new()),
            ("USERNAME".to_string(), String::new()),
            ("WINDIR".to_string(), String::new()),
            ("TEMP".to_string(), String::new()),
            ("TMP".to_string(), String::new()),
        ])
    } else {
        BTreeMap::from([
            ("HOME".to_string(), String::new()),
            ("PATH".to_string(), String::new()),
            ("TMPDIR".to_string(), String::new()),
            ("USER".to_string(), String::new()),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies default `OutputPolicy` is `Bool(true)`.
    #[test]
    fn output_policy_default_is_saved() {
        let policy = OutputPolicy::default();
        assert_eq!(policy, OutputPolicy::Bool(true));
    }

    /// Verifies default `ToolRuntime` has impure=false and zero retries.
    #[test]
    fn tool_runtime_defaults() {
        let rt = ToolRuntime::default();
        assert!(!rt.impure);
        assert_eq!(rt.max_retries, 0);
        assert!(rt.content_map.is_empty());
    }

    /// Verifies `default_runtime_inherited_env_vars` returns
    /// platform-appropriate inherited env var names.
    #[test]
    fn default_inherited_env_vars_are_platform_appropriate() {
        let vars = default_runtime_inherited_env_vars();
        assert!(!vars.is_empty(), "should have entries on all platforms");
        assert!(vars.contains_key("PATH"), "PATH must be present on all platforms");
        if cfg!(windows) {
            assert!(vars.contains_key("WINDIR"));
            assert!(vars.contains_key("USERNAME"));
        } else {
            assert!(vars.contains_key("HOME"));
            assert!(vars.contains_key("USER"));
            assert!(vars.contains_key("TMPDIR"));
        }
    }

    // ── InputBinding ──────────────────────────────────────────────────────

    /// Verifies `InputBinding::default()` is `String("")`.
    #[test]
    fn input_binding_default_is_empty_string() {
        let binding = InputBinding::default();
        assert_eq!(binding, InputBinding::String(String::new()));
    }

    /// Verifies `From<String>` produces the `String` variant.
    #[test]
    fn input_binding_from_string() {
        let binding = InputBinding::from("hello".to_string());
        assert_eq!(binding, InputBinding::String("hello".to_string()));
    }

    /// Verifies `From<Vec<String>>` produces the `Vec` variant.
    #[test]
    fn input_binding_from_vec() {
        let binding = InputBinding::from(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(binding, InputBinding::Vec(vec!["a".to_string(), "b".to_string()]));
    }

    /// Verifies `InputBinding::String` serializes to a plain JSON string.
    #[test]
    fn input_binding_string_serde() {
        let binding = InputBinding::String("test-value".to_string());
        let json = serde_json::to_string(&binding).expect("serialize");
        assert_eq!(json, "\"test-value\"");
        let back: InputBinding = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, binding);
    }

    /// Verifies `InputBinding::Vec` serializes to a JSON array of strings.
    #[test]
    fn input_binding_vec_serde() {
        let binding = InputBinding::Vec(vec!["x".to_string(), "y".to_string()]);
        let json = serde_json::to_string(&binding).expect("serialize");
        assert_eq!(json, "[\"x\",\"y\"]");
        let back: InputBinding = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, binding);
    }

    /// Verifies that an empty JSON string deserializes as `String("")`.
    #[test]
    fn input_binding_empty_string_deser() {
        let binding: InputBinding = serde_json::from_str("\"\"").expect("deserialize empty string");
        assert_eq!(binding, InputBinding::String(String::new()));
    }

    /// Verifies that an empty JSON array deserializes as `Vec(vec![])`.
    #[test]
    fn input_binding_empty_vec_deser() {
        let binding: InputBinding = serde_json::from_str("[]").expect("deserialize empty array");
        assert_eq!(binding, InputBinding::Vec(vec![]));
    }

    // ── Property-based tests (proptest) ───────────────────────────────

    use proptest::prelude::*;

    proptest! {
        /// `OutputPolicy` round-trips through serde_json without data loss.
        #[test]
        fn output_policy_serde_roundtrip(policy: OutputPolicy) {
            let json = serde_json::to_string(&policy).expect("serialize");
            let back: OutputPolicy = serde_json::from_str(&json).expect("deserialize");
            prop_assert_eq!(policy, back,
                "OutputPolicy serde roundtrip failed: {:?} -> {:?} -> {:?}", policy, json, back);
        }

        /// `SaveMode` conversion to/from `SaveModeLatest` is lossless.
        #[test]
        fn save_mode_conversion_roundtrip(mode: SaveMode) {
            let latest: super::versions::v_latest::SaveModeLatest = match mode {
                SaveMode::True => super::versions::v_latest::SaveModeLatest::True,
                SaveMode::False => super::versions::v_latest::SaveModeLatest::False,
                SaveMode::Full => super::versions::v_latest::SaveModeLatest::Full,
            };
            let back = match latest {
                super::versions::v_latest::SaveModeLatest::True => SaveMode::True,
                super::versions::v_latest::SaveModeLatest::False => SaveMode::False,
                super::versions::v_latest::SaveModeLatest::Full => SaveMode::Full,
            };
            prop_assert_eq!(mode, back,
                "SaveMode roundtrip failed for mode={:?}", mode);
        }

        /// `OutputCaptureSpec` round-trips through serde_json without data loss.
        #[test]
        fn output_capture_spec_serde_roundtrip(spec: OutputCaptureSpec) {
            let json = serde_json::to_string(&spec).expect("serialize");
            let back: OutputCaptureSpec = serde_json::from_str(&json).expect("deserialize");
            let spec_debug = format!("{spec:?}");
            let back_debug = format!("{back:?}");
            prop_assert_eq!(spec, back,
                "OutputCaptureSpec serde roundtrip failed: {} -> {} -> {}", spec_debug, json, back_debug);
        }
    }

    impl proptest::arbitrary::Arbitrary for OutputPolicy {
        type Parameters = ();
        type Strategy = proptest::strategy::BoxedStrategy<Self>;

        fn arbitrary_with((): ()) -> Self::Strategy {
            use proptest::strategy::Strategy;
            prop_oneof![any::<bool>().prop_map(OutputPolicy::Bool), Just(OutputPolicy::Full),]
                .boxed()
        }
    }

    impl proptest::arbitrary::Arbitrary for SaveMode {
        type Parameters = ();
        type Strategy = proptest::strategy::BoxedStrategy<Self>;

        fn arbitrary_with((): ()) -> Self::Strategy {
            use proptest::strategy::Strategy;
            prop_oneof![Just(SaveMode::True), Just(SaveMode::False), Just(SaveMode::Full),].boxed()
        }
    }

    impl proptest::arbitrary::Arbitrary for OutputCaptureSpec {
        type Parameters = ();
        type Strategy = proptest::strategy::BoxedStrategy<Self>;

        fn arbitrary_with((): ()) -> Self::Strategy {
            use proptest::strategy::Strategy;
            (any::<String>(), any::<String>(), any::<SaveMode>(), any::<bool>(), any::<bool>())
                .prop_map(|(name, capture, save, allow_empty, include_topmost_folder)| {
                    OutputCaptureSpec { name, capture, save, allow_empty, include_topmost_folder }
                })
                .boxed()
        }
    }

    // ── OutputCaptureSpec serde ───────────────────────────────────────────

    #[test]
    fn output_capture_spec_full_roundtrip() {
        let spec = OutputCaptureSpec {
            name: "output1".to_string(),
            capture: "stdout".to_string(),
            save: SaveMode::Full,
            allow_empty: true,
            include_topmost_folder: false,
        };
        let json = serde_json::to_string(&spec).expect("serialize");
        let back: OutputCaptureSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(spec, back);
    }

    #[test]
    fn output_capture_spec_defaults_roundtrip() {
        let spec = OutputCaptureSpec::default();
        let json = serde_json::to_string(&spec).expect("serialize");
        let back: OutputCaptureSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(spec, back);
    }

    #[test]
    fn output_capture_spec_default_save_skipped_in_json() {
        let spec = OutputCaptureSpec {
            name: "out".to_string(),
            capture: "stdout".to_string(),
            save: SaveMode::True,
            ..Default::default()
        };
        let json = serde_json::to_string(&spec).expect("serialize");
        assert!(!json.contains("\"save\""), "default save=True should be skipped: {json}");
    }

    #[test]
    fn output_capture_spec_false_save_present_in_json() {
        let spec = OutputCaptureSpec {
            name: "out".to_string(),
            capture: "stdout".to_string(),
            save: SaveMode::False,
            ..Default::default()
        };
        let json = serde_json::to_string(&spec).expect("serialize");
        assert!(json.contains("\"save\""), "save=False should be present: {json}");
    }

    #[test]
    fn output_capture_spec_full_save_present_in_json() {
        let spec = OutputCaptureSpec {
            name: "out".to_string(),
            capture: "stdout".to_string(),
            save: SaveMode::Full,
            ..Default::default()
        };
        let json = serde_json::to_string(&spec).expect("serialize");
        assert!(json.contains("\"save\""), "save=Full should be present: {json}");
    }
}
