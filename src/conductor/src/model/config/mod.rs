//! Runtime configuration document model.
//!
//! This module is the Phase-2 configuration surface used by runtime planning
//! and CLI workflows.
//!
//! Design notes:
//! - Runtime structs are version-agnostic.
//! - Persisted representation is handled by `versions/` modules and bridged
//!   through fp-library optics.
//! - Type names keep the `Nickel*` prefix because the underlying wire format
//!   and migration contracts remain Nickel-based, even though the module path
//!   is now `model::config`.

use std::collections::BTreeMap;

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use crate::error::ConductorError;
use crate::model::state::PersistenceFlags;

pub(crate) mod versions;

/// Split identity representation used by runtime Nickel metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NickelIdentity {
    /// Leading identity component.
    pub first: String,
    /// Trailing identity component.
    pub last: String,
}

impl Default for NickelIdentity {
    fn default() -> Self {
        Self { first: "default".to_string(), last: "identity".to_string() }
    }
}

/// Shared runtime metadata carried by user/machine Nickel documents.
///
/// This metadata is runtime-facing only. Persisted `v1.ncl` documents do not
/// store a `meta` section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NickelDocumentMetadata {
    /// Stable logical configuration id.
    pub id: String,
    /// Split identity fields used by the versioned schema.
    pub identity: NickelIdentity,
}

impl Default for NickelDocumentMetadata {
    fn default() -> Self {
        Self { id: "default-config".to_string(), identity: NickelIdentity::default() }
    }
}

/// Optional policy overrides for one named output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OutputPolicy {
    /// Optional save override; falls back to inherited/default value when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub save: Option<bool>,
    /// Optional force-full override; falls back to inherited/default value when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_full: Option<bool>,
}

/// Timezone-independent impure-execution timestamp.
///
/// This timestamp uses Unix epoch UTC split into integral components to
/// preserve nanosecond precision without relying on large floating-point
/// numbers in Nickel serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ImpureTimestamp {
    /// Whole seconds since Unix epoch (UTC).
    pub epoch_seconds: u64,
    /// Nanoseconds within `epoch_seconds`, in range `0..=999_999_999`.
    pub subsec_nanos: u32,
}

/// Runtime storage path defaults persisted in user/machine config documents.
///
/// This shape is persisted as a grouped `runtime_storage` record in
/// `conductor.ncl` and `conductor.machine.ncl`.
///
/// Paths are stored as strings so runtime can resolve relative values against
/// config locations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RuntimeStorageConfig {
    /// Root directory for runtime-managed artifacts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_dir: Option<String>,
    /// Optional override path for the volatile state document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_ncl: Option<String>,
    /// Optional override path for filesystem CAS storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cas_store_dir: Option<String>,
}

impl RuntimeStorageConfig {
    /// Returns whether all runtime-storage override fields are absent.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.conductor_dir.is_none() && self.state_ncl.is_none() && self.cas_store_dir.is_none()
    }
}

impl OutputPolicy {
    /// Resolves optional overrides against a concrete base policy.
    #[must_use]
    pub fn resolve(self, base: PersistenceFlags) -> PersistenceFlags {
        PersistenceFlags {
            save: self.save.unwrap_or(base.save),
            force_full: self.force_full.unwrap_or(base.force_full),
        }
    }
}

/// Nickel document loaded from `conductor.ncl`.
///
/// This document now shares the same schema surface as `conductor.machine.ncl`.
/// The distinction between the two files is operational rather than structural:
/// the program edits `conductor.machine.ncl`, while `conductor.ncl` is treated
/// as user-edited input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UserNickelDocument {
    /// Runtime-only metadata (not persisted in `v1.ncl`).
    #[serde(default)]
    pub metadata: NickelDocumentMetadata,
    /// Grouped runtime storage path configuration persisted under
    /// `runtime_storage`.
    #[serde(default, skip_serializing_if = "RuntimeStorageConfig::is_empty")]
    pub runtime_storage: RuntimeStorageConfig,
    /// Named external content references.
    #[serde(default)]
    pub external_data: BTreeMap<String, ExternalContentRef>,
    /// Tool definitions keyed by logical tool name.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolSpec>,
    /// Workflow DAG definitions keyed by workflow id.
    #[serde(default)]
    pub workflows: BTreeMap<String, WorkflowSpec>,
    /// Runtime-only tool execution configuration (`tool_name -> config`).
    #[serde(default)]
    pub tool_configs: BTreeMap<String, ToolConfigSpec>,
    /// Impure timestamps merged into execution planning.
    ///
    /// Layout: `workflow_id -> (step_id -> timestamp)`.
    #[serde(default)]
    pub impure_timestamps: BTreeMap<String, BTreeMap<String, ImpureTimestamp>>,
    /// Optional orchestration-state CAS pointer.
    #[serde(default)]
    pub state_pointer: Option<Hash>,
}

/// External content reference resolved through CAS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalContentRef {
    /// Stable hash identity for the external content blob.
    pub hash: Hash,
    /// Optional human description.
    #[serde(default)]
    pub description: Option<String>,
}

/// Add-external-data request options for machine document mutations.
///
/// This helper makes machine-document updates explicit and validated so
/// end-user automation can avoid direct map mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddExternalDataOptions {
    /// Concrete external-data reference to store.
    pub reference: ExternalContentRef,
    /// Existing-entry conflict policy.
    ///
    /// - `false` (default): adding an existing name fails fast.
    /// - `true`: existing entries with the same name are replaced.
    pub overwrite_existing: bool,
}

impl AddExternalDataOptions {
    /// Creates strict non-overwrite add-external-data options.
    #[must_use]
    pub fn new(reference: ExternalContentRef) -> Self {
        Self { reference, overwrite_existing: false }
    }

    /// Sets conflict policy for existing external-data entries.
    #[must_use]
    pub fn overwrite_existing(mut self, value: bool) -> Self {
        self.overwrite_existing = value;
        self
    }
}

/// Tool definition from one conductor configuration document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSpec {
    /// Whether this tool is impure and should receive timestamp injection.
    #[serde(default)]
    pub is_impure: bool,
    /// Declared tool input contract (`input_name -> input spec`).
    ///
    /// Every workflow-step input must reference a declared entry here.
    #[serde(default)]
    pub inputs: BTreeMap<String, ToolInputSpec>,
    /// Tool execution identity and static process metadata.
    #[serde(flatten)]
    pub kind: ToolKindSpec,
    /// Declared outputs keyed by logical output name.
    ///
    /// Each output specifies only capture source (stdout/stderr/file).
    ///
    /// Persistence policy (`save`, `force_full`) belongs to workflow tool-call
    /// sites via [`WorkflowStepSpec::outputs`] and [`OutputPolicy`], not the
    /// reusable tool definition.
    #[serde(default)]
    pub outputs: BTreeMap<String, ToolOutputSpec>,
}

impl Default for ToolSpec {
    fn default() -> Self {
        Self {
            is_impure: false,
            inputs: BTreeMap::new(),
            kind: ToolKindSpec::Builtin { name: "echo".to_string(), version: "1.0.0".to_string() },
            outputs: BTreeMap::from([("result".to_string(), ToolOutputSpec::default())]),
        }
    }
}

/// Tool definition kind selector and static execution metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolKindSpec {
    /// Registers one builtin implementation by immutable (`name`, `version`).
    ///
    /// Builtins are not implicitly available; each builtin call target must be
    /// declared in `tools` with this variant.
    Builtin {
        /// Builtin name.
        name: String,
        /// Builtin semantic version.
        version: String,
    },
    /// Defines one executable process contract.
    Executable {
        /// Command vector templates rendered in-order.
        ///
        /// The first rendered entry is the executable path and must be one
        /// sandbox-relative path. Remaining entries are process arguments.
        ///
        /// Resolution is always sandbox-relative to one ad hoc temporary
        /// execution directory created per step execution. The executable file
        /// is therefore expected to come from runtime materialization (for
        /// example `tool_configs.<tool>.content_map`) rather than host-global
        /// PATH lookup.
        #[serde(default)]
        command: Vec<String>,
        /// Optional runtime environment templates for this executable.
        #[serde(default)]
        env_vars: BTreeMap<String, String>,
        /// Exit codes treated as successful completion for this executable.
        #[serde(default = "default_success_codes")]
        success_codes: Vec<i32>,
    },
}

/// Runtime execution configuration for one declared tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolConfigSpec {
    /// Maximum number of concurrent calls allowed for this tool.
    ///
    /// Semantics:
    /// - `-1`: unlimited (default),
    /// - positive integer: hard limit on concurrent calls.
    ///
    /// `0` and values smaller than `-1` are invalid.
    #[serde(default = "default_max_concurrent_calls")]
    pub max_concurrent_calls: i32,
    /// Optional per-tool content map (`relative_path -> hash`) for executable
    /// sandbox materialization.
    ///
    /// Semantics for each key/value entry:
    /// - Key is one sandbox-relative path under the per-execution temp
    ///   directory.
    /// - Key ending with `/` or `\\` designates a directory target; the value
    ///   must reference one ZIP payload in CAS, and runtime unpacks that ZIP
    ///   into the destination directory.
    /// - Directory key `./` (or `.\\`) is supported and means unpack directly
    ///   into the sandbox root.
    /// - Key without trailing slash/backslash designates one regular file
    ///   target; runtime writes the CAS bytes directly to that file.
    /// - Runtime preflights all entries and rejects conflicts where two
    ///   separate entries would materialize the same file path; sibling files
    ///   in the same folders are allowed and merged.
    /// - Absolute paths and escaping paths (for example `..`) are rejected.
    ///
    /// This configuration is invalid for builtin tools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_map: Option<BTreeMap<String, Hash>>,
}

impl Default for ToolConfigSpec {
    fn default() -> Self {
        Self { max_concurrent_calls: default_max_concurrent_calls(), content_map: None }
    }
}

/// Tool-config mutation mode used by [`AddToolOptions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddToolConfigMode {
    /// Leaves any existing `tool_configs.<tool_name>` entry unchanged.
    ///
    /// This mode is useful when callers only want to update the reusable tool
    /// definition without touching runtime-execution tuning.
    KeepExisting,
    /// Replaces or inserts one concrete tool-config value.
    ///
    /// For builtin tools, `content_map` must remain `None`.
    Replace(ToolConfigSpec),
    /// Removes any existing `tool_configs.<tool_name>` entry.
    ///
    /// This mode makes config removal explicit when replacing a tool
    /// definition that should no longer have runtime config.
    Remove,
}

/// Add-tool request options for user/machine runtime configuration documents.
///
/// This API intentionally captures both reusable tool definition (`ToolSpec`)
/// and runtime execution config (`ToolConfigSpec`) so callers can perform one
/// atomic, validated update instead of manually mutating multiple maps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddToolOptions {
    /// Tool specification to write into `tools.<tool_name>`.
    pub spec: ToolSpec,
    /// Existing-entry conflict policy.
    ///
    /// - `false` (default): adding a tool that already exists fails fast.
    /// - `true`: existing entries for this tool name can be replaced.
    pub overwrite_existing: bool,
    /// Tool-config mutation policy for `tool_configs.<tool_name>`.
    pub config_mode: AddToolConfigMode,
}

impl AddToolOptions {
    /// Creates add-tool options with strict non-overwrite behavior.
    ///
    /// Default config behavior is [`AddToolConfigMode::KeepExisting`], so the
    /// call updates only `tools.<tool_name>` unless a different config mode is
    /// selected.
    #[must_use]
    pub fn new(spec: ToolSpec) -> Self {
        Self { spec, overwrite_existing: false, config_mode: AddToolConfigMode::KeepExisting }
    }

    /// Sets conflict policy for existing tool entries.
    #[must_use]
    pub fn overwrite_existing(mut self, value: bool) -> Self {
        self.overwrite_existing = value;
        self
    }

    /// Replaces/inserts `tool_configs.<tool_name>` with one concrete value.
    #[must_use]
    pub fn with_tool_config(mut self, config: ToolConfigSpec) -> Self {
        self.config_mode = AddToolConfigMode::Replace(config);
        self
    }

    /// Removes `tool_configs.<tool_name>` for the target tool.
    #[must_use]
    pub fn remove_tool_config(mut self) -> Self {
        self.config_mode = AddToolConfigMode::Remove;
        self
    }
}

fn default_max_concurrent_calls() -> i32 {
    -1
}

/// Tool input declaration entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ToolInputKind {
    /// Scalar string input (default).
    #[default]
    String,
    /// Ordered list of string arguments.
    ///
    /// Runtime treats this as list data and allows command-argument unpacking
    /// only through standalone unpack tokens in executable command templates.
    StringList,
}

fn is_default_tool_input_kind(kind: &ToolInputKind) -> bool {
    matches!(kind, ToolInputKind::String)
}

/// Tool input declaration entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ToolInputSpec {
    /// Declared value kind for this input.
    ///
    /// When omitted, runtime defaults to [`ToolInputKind::String`].
    #[serde(default, skip_serializing_if = "is_default_tool_input_kind")]
    pub kind: ToolInputKind,
    /// Optional default literal value used when a step omits this input.
    ///
    /// Defaults are scalar-string only. List defaults are intentionally not
    /// supported by schema/runtime validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

/// Unified process selector for one tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessSpec {
    /// Executes one external binary.
    ///
    /// The first rendered `command` entry is interpreted as a relative path
    /// from one ad hoc temporary execution directory. Tool content is
    /// materialized there from merged
    /// `tool_configs.<tool>.content_map` only if the step actually needs to
    /// run. Directory-form content-map keys (trailing `/` or `\\`) are treated
    /// as ZIP payload extraction targets; key `./` (or `.\\`) unpacks directly
    /// at sandbox root; all other keys materialize raw files. Runtime rejects
    /// conflicting `content_map` entries that would overwrite each other.
    Executable {
        /// Command vector templates rendered in-order.
        ///
        /// First rendered entry is executable path. Remaining entries are
        /// process arguments.
        #[serde(default)]
        command: Vec<String>,
        /// Optional runtime environment templates for this executable.
        ///
        /// Template syntax supports JavaScript-template-like `${...}`
        /// interpolation.
        /// Supported forms:
        /// - `${<name>}` → bare input-key interpolation.
        /// - `${inputs.<name>}` → injects lossy UTF-8 text from resolved input
        ///   `<name>`.
        /// - `${inputs["<name>"]}` / `${inputs['<name>']}` → bracket-notation
        ///   input lookup.
        /// - `${<selector>:file(<relative_path>)}` → materializes selected
        ///   input bytes to `<relative_path>` and injects the resulting path
        ///   string.
        /// - `${inputs.<name>:file(<relative_path>)}` → queues the input bytes
        ///   for materialization at `<relative_path>` under one ad hoc
        ///   temporary execution directory and injects the resulting relative
        ///   path string.
        /// - `${inputs.<name>:zip(<entry_path>)}` → treats the selected input
        ///   bytes as one ZIP archive, extracts exactly `<entry_path>`, and
        ///   injects its content as lossy UTF-8 text.
        /// - `${inputs.<name>:zip(<entry_path>):file(<relative_path>)}` →
        ///   extracts one ZIP entry and queues that extracted content for
        ///   materialization at `<relative_path>`, then injects the path
        ///   string.
        /// - `${os.<target>?<value>}` → includes `<value>` only when host OS
        ///   matches `<target>` (`windows`, `linux`, or `macos`), otherwise
        ///   renders empty content.
        /// - `\${...}` escapes interpolation start and renders literal
        ///   `${...}`.
        /// - JavaScript-like string escapes in literal spans are supported,
        ///   such as `\\`, `\n`, `\t`, `\xNN`, and `\u{NNNN}`.
        ///
        /// Notes:
        /// - `<relative_path>` must be relative; absolute paths are rejected.
        /// - ZIP selectors only support ZIP payloads and fail for non-ZIP
        ///   input bytes.
        /// - ZIP selectors fail when `<entry_path>` is missing or resolves to
        ///   a directory.
        /// - any unknown input reference fails workflow resolution.
        /// - unsupported `${...}` expressions fail workflow resolution.
        /// - unsupported/trailing escape sequences fail workflow resolution.
        /// - `${...` without a closing `}` and malformed `:file(...)` forms
        ///   fail workflow resolution.
        #[serde(default)]
        env_vars: BTreeMap<String, String>,
        /// Exit codes treated as successful completion for this executable.
        ///
        /// `0` is conventionally included. Additional non-zero codes can be
        /// supplied for tools that encode warning-like success states via exit
        /// status.
        #[serde(default = "default_success_codes")]
        success_codes: Vec<i32>,
    },
    /// Executes one registered builtin implementation.
    Builtin {
        /// Builtin name.
        name: String,
        /// Builtin semantic version.
        version: String,
        /// Named argument templates rendered by key.
        #[serde(default)]
        args: BTreeMap<String, String>,
    },
}

/// Capture-source selector for one tool output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputCaptureSpec {
    /// Capture process/builtin standard output bytes.
    Stdout {},
    /// Capture process/builtin standard error bytes.
    Stderr {},
    /// Capture process/builtin exit code as UTF-8 text bytes.
    ProcessCode {},
    /// Capture file bytes from one relative path.
    File {
        /// Relative file path resolved from one ad hoc temporary execution
        /// directory.
        path: String,
    },
    /// Capture one directory snapshot by zipping it recursively.
    ///
    /// The runtime uses the builtin archive/zip implementation to build a ZIP
    /// payload with stored (no-compression) entries and captures the resulting
    /// archive bytes as this output's content.
    Folder {
        /// Relative directory path resolved from one ad hoc temporary
        /// execution directory.
        path: String,
        /// Whether the top-level folder name itself is included as the ZIP
        /// root entry.
        ///
        /// - `false` (default): ZIP contains only the folder contents.
        /// - `true`: ZIP contains the folder node and all descendants.
        #[serde(default)]
        include_topmost_folder: bool,
    },
}

/// Declared output contract for one tool output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOutputSpec {
    /// Capture source for this output.
    ///
    /// For `kind = "file"`, the configured `path` supports `${...}` template
    /// interpolation using the same rules as process/runtime template values.
    pub capture: OutputCaptureSpec,
}

impl Default for ToolOutputSpec {
    fn default() -> Self {
        Self { capture: OutputCaptureSpec::Stdout {} }
    }
}

fn default_success_codes() -> Vec<i32> {
    vec![0]
}

/// One workflow DAG.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkflowSpec {
    /// Ordered steps before topological sorting.
    #[serde(default)]
    pub steps: Vec<WorkflowStepSpec>,
}

/// One workflow step (tool call reference).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStepSpec {
    /// Unique step id within one workflow.
    pub id: String,
    /// Referenced tool name.
    pub tool: String,
    /// Inputs keyed by logical tool-input name.
    ///
    /// Semantics:
    /// - inputs provide data to the called tool (builtin or executable),
    /// - for executable tools, names are validated against declared
    ///   `ToolSpec.inputs` and defaults,
    /// - input values are either one scalar string or one list of strings,
    /// - scalar-string values support `${...}` interpolation with these
    ///   expression forms:
    ///   - `${external_data.<name>}`,
    ///   - `${step_output.<step_id>.<output_name>}`,
    /// - list values apply the same interpolation rules per list item,
    /// - plain text outside `${...}` spans is preserved literally in each
    ///   item,
    /// - input-binding interpolation does **not** support materialization
    ///   directives like `:file(...)` or `:folder(...)`.
    #[serde(default)]
    pub inputs: BTreeMap<String, InputBinding>,
    /// Explicit execution-order dependencies on prior step ids.
    ///
    /// Use this when a step depends on side effects from earlier steps but
    /// does not consume their `${step_output...}` values as input.
    ///
    /// Contract:
    /// - every `${step_output.<step_id>.<output_name>}` reference in `inputs`
    ///   must list `<step_id>` in `depends_on`,
    /// - missing explicit `depends_on` entries for referenced step outputs are
    ///   rejected during workflow validation.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Step-local output persistence policy overrides.
    ///
    /// This does **not** declare or filter step outputs. Output exposure is
    /// fully defined by the referenced tool's `ToolSpec.outputs`; all declared
    /// tool outputs are always exposed.
    #[serde(default)]
    pub outputs: BTreeMap<String, OutputPolicy>,
}

/// Input binding value for workflow steps.
///
/// Bindings are either one scalar string or one list of scalar strings.
/// Scalar string content may include `${...}` interpolation tokens mixed with
/// plain text and is parsed by [`parse_input_binding`]. List bindings apply
/// the same parsing to each list item independently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InputBinding {
    /// Scalar string binding.
    String(String),
    /// Ordered list-of-strings binding.
    StringList(Vec<String>),
}

impl InputBinding {
    /// Returns human-readable binding kind name for diagnostics.
    #[must_use]
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::StringList(_) => "string_list",
        }
    }

    /// Visits each scalar binding item in deterministic order.
    ///
    /// Scalar bindings visit exactly one item. List bindings visit each list
    /// element in order.
    pub fn try_for_each_scalar<F>(&self, mut callback: F) -> Result<(), ConductorError>
    where
        F: FnMut(usize, &str) -> Result<(), ConductorError>,
    {
        match self {
            Self::String(value) => callback(0, value),
            Self::StringList(values) => {
                for (index, value) in values.iter().enumerate() {
                    callback(index, value)?;
                }
                Ok(())
            }
        }
    }
}

impl Default for InputBinding {
    fn default() -> Self {
        Self::String(String::new())
    }
}

impl From<String> for InputBinding {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for InputBinding {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<Vec<String>> for InputBinding {
    fn from(values: Vec<String>) -> Self {
        Self::StringList(values)
    }
}

/// Prefix for `${external_data.<name>}` interpolation expression bodies.
const INPUT_BINDING_EXTERNAL_DATA_PREFIX: &str = "external_data.";

/// Prefix for `${step_output.<step_id>.<output_name>}` expression bodies.
const INPUT_BINDING_STEP_OUTPUT_PREFIX: &str = "step_output.";

/// Token-start marker for interpolation spans in workflow-step input bindings.
const INPUT_BINDING_TOKEN_START: &str = "${";

/// Parsed token segment in one workflow-step input binding string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedInputBindingSegment<'a> {
    /// Plain literal text payload.
    Literal(&'a str),
    /// Interpolated external-data reference looked up from top-level
    /// `external_data`.
    ExternalData {
        /// External-data reference name.
        name: &'a str,
    },
    /// Interpolated prior-step output reference.
    StepOutput {
        /// Dependency step id that produced the output.
        step_id: &'a str,
        /// Output name on the dependency step.
        output: &'a str,
    },
}

/// Parses one `${...}` expression body from a workflow-step input binding.
fn parse_input_binding_expression<'a>(
    expression: &'a str,
    binding: &str,
) -> Result<ParsedInputBindingSegment<'a>, ConductorError> {
    if expression.contains(":file(") || expression.contains(":folder(") {
        return Err(ConductorError::Workflow(format!(
            "unsupported input binding expression '${{{expression}}}' in '{binding}'; supported interpolation forms are '${{external_data.<name>}}' and '${{step_output.<step_id>.<output_name>}}'. Input bindings do not support materialization directives like ':file(...)' or ':folder(...)'"
        )));
    }

    if let Some(name) = expression.strip_prefix(INPUT_BINDING_EXTERNAL_DATA_PREFIX) {
        if name.trim().is_empty() {
            return Err(ConductorError::Workflow(
                "input binding '${external_data.<name>}' requires a non-empty <name>".to_string(),
            ));
        }
        return Ok(ParsedInputBindingSegment::ExternalData { name });
    }

    if let Some(selector) = expression.strip_prefix(INPUT_BINDING_STEP_OUTPUT_PREFIX) {
        let Some((step_id, output)) = selector.split_once('.') else {
            return Err(ConductorError::Workflow(
                "input binding '${step_output.<step_id>.<output_name>}' requires both step id and output name"
                    .to_string(),
            ));
        };
        if step_id.trim().is_empty() || output.trim().is_empty() {
            return Err(ConductorError::Workflow(
                "input binding '${step_output.<step_id>.<output_name>}' requires non-empty step id and output name"
                    .to_string(),
            ));
        }
        return Ok(ParsedInputBindingSegment::StepOutput { step_id, output });
    }

    Err(ConductorError::Workflow(format!(
        "unsupported input binding expression '${{{expression}}}' in '{binding}'; supported interpolation forms are '${{external_data.<name>}}' and '${{step_output.<step_id>.<output_name>}}'. Input bindings do not support materialization directives like ':file(...)' or ':folder(...)'"
    )))
}

/// Parses one workflow-step input binding string into interpolation segments.
///
/// Rules:
/// - plain text outside `${...}` tokens is preserved as literal content,
/// - supported interpolation expressions are `${external_data.<name>}`,
///   and `${step_output.<step_id>.<output_name>}`,
/// - unsupported `${...}` expressions fail fast with explicit errors,
/// - `${...` without a closing `}` fails fast.
pub(crate) fn parse_input_binding(
    binding: &str,
) -> Result<Vec<ParsedInputBindingSegment<'_>>, ConductorError> {
    let mut segments = Vec::new();
    let mut cursor = 0usize;

    while let Some(start_relative) = binding[cursor..].find(INPUT_BINDING_TOKEN_START) {
        let token_start = cursor + start_relative;
        if token_start > cursor {
            segments.push(ParsedInputBindingSegment::Literal(&binding[cursor..token_start]));
        }

        let expression_start = token_start + INPUT_BINDING_TOKEN_START.len();
        let Some(end_relative) = binding[expression_start..].find('}') else {
            return Err(ConductorError::Workflow(format!(
                "input binding '{binding}' contains '${{' without a matching closing '}}'"
            )));
        };
        let expression_end = expression_start + end_relative;
        let expression = &binding[expression_start..expression_end];
        segments.push(parse_input_binding_expression(expression, binding)?);
        cursor = expression_end + 1;
    }

    if cursor < binding.len() {
        segments.push(ParsedInputBindingSegment::Literal(&binding[cursor..]));
    }

    if segments.is_empty() {
        segments.push(ParsedInputBindingSegment::Literal(binding));
    }

    Ok(segments)
}

/// Nickel document loaded from `conductor.machine.ncl`.
///
/// This document shares the same schema surface as `conductor.ncl`. The only
/// special behavior is that runtime writes flow back to this file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MachineNickelDocument {
    /// Runtime-only metadata (not persisted in `v1.ncl`).
    #[serde(default)]
    pub metadata: NickelDocumentMetadata,
    /// Grouped runtime storage path configuration persisted under
    /// `runtime_storage`.
    #[serde(default, skip_serializing_if = "RuntimeStorageConfig::is_empty")]
    pub runtime_storage: RuntimeStorageConfig,
    /// Named external content references.
    #[serde(default)]
    pub external_data: BTreeMap<String, ExternalContentRef>,
    /// Tool definitions keyed by logical tool name.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolSpec>,
    /// Workflow DAG definitions keyed by workflow id.
    #[serde(default)]
    pub workflows: BTreeMap<String, WorkflowSpec>,
    /// Runtime-only tool execution configuration (`tool_name -> config`).
    #[serde(default)]
    pub tool_configs: BTreeMap<String, ToolConfigSpec>,
    /// Machine-injected timestamps for impure tool calls.
    ///
    /// Layout: `workflow_id -> (step_id -> timestamp)`.
    #[serde(default)]
    pub impure_timestamps: BTreeMap<String, BTreeMap<String, ImpureTimestamp>>,
    /// Current orchestration-state CAS pointer.
    #[serde(default)]
    pub state_pointer: Option<Hash>,
}

/// Nickel document loaded from `.conductor/state.ncl`.
///
/// This document stores volatile runtime-managed state only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StateNickelDocument {
    /// Machine-injected timestamps for impure tool calls.
    ///
    /// Layout: `workflow_id -> (step_id -> timestamp)`.
    #[serde(default)]
    pub impure_timestamps: BTreeMap<String, BTreeMap<String, ImpureTimestamp>>,
    /// Current orchestration-state CAS pointer.
    #[serde(default)]
    pub state_pointer: Option<Hash>,
}

impl UserNickelDocument {
    /// Adds one tool definition (and optional tool config) to user document state.
    ///
    /// Validation rules:
    /// - `tool_name` must be non-empty after trimming,
    /// - duplicates fail unless `overwrite_existing = true`,
    /// - builtin tools cannot end up with `content_map` in effective config.
    pub fn add_tool(
        &mut self,
        tool_name: impl Into<String>,
        options: AddToolOptions,
    ) -> Result<(), ConductorError> {
        add_tool_to_maps(&mut self.tools, &mut self.tool_configs, tool_name.into(), options)
    }
}

impl MachineNickelDocument {
    /// Adds one tool definition (and optional tool config) to machine document state.
    ///
    /// Validation rules mirror [`UserNickelDocument::add_tool`].
    pub fn add_tool(
        &mut self,
        tool_name: impl Into<String>,
        options: AddToolOptions,
    ) -> Result<(), ConductorError> {
        add_tool_to_maps(&mut self.tools, &mut self.tool_configs, tool_name.into(), options)
    }

    /// Adds one external-data entry to machine document state.
    ///
    /// Validation rules:
    /// - `name` must be non-empty after trimming,
    /// - duplicates fail unless `overwrite_existing = true`.
    pub fn add_external_data(
        &mut self,
        name: impl Into<String>,
        options: AddExternalDataOptions,
    ) -> Result<(), ConductorError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ConductorError::Workflow(
                "external data name cannot be empty when adding machine external data".to_string(),
            ));
        }

        if !options.overwrite_existing && self.external_data.contains_key(&name) {
            return Err(ConductorError::Workflow(format!(
                "external data '{name}' already exists in machine config; set overwrite_existing=true to replace it"
            )));
        }

        self.external_data.insert(name, options.reference);
        Ok(())
    }
}

/// Validates and applies one add-tool request against document maps.
///
/// This helper keeps user/machine document add-tool semantics identical.
fn add_tool_to_maps(
    tools: &mut BTreeMap<String, ToolSpec>,
    tool_configs: &mut BTreeMap<String, ToolConfigSpec>,
    tool_name: String,
    options: AddToolOptions,
) -> Result<(), ConductorError> {
    if tool_name.trim().is_empty() {
        return Err(ConductorError::Workflow(
            "tool name cannot be empty when adding a tool".to_string(),
        ));
    }

    if !options.overwrite_existing
        && (tools.contains_key(&tool_name) || tool_configs.contains_key(&tool_name))
    {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_name}' already exists; set overwrite_existing=true to replace it"
        )));
    }

    validate_add_tool_config_mode(&tool_name, &options.spec, &options.config_mode, tool_configs)?;

    tools.insert(tool_name.clone(), options.spec);
    match options.config_mode {
        AddToolConfigMode::KeepExisting => {}
        AddToolConfigMode::Replace(config) => {
            tool_configs.insert(tool_name, config);
        }
        AddToolConfigMode::Remove => {
            tool_configs.remove(&tool_name);
        }
    }

    Ok(())
}

/// Validates builtin/content-map invariants for one add-tool request.
fn validate_add_tool_config_mode(
    tool_name: &str,
    spec: &ToolSpec,
    config_mode: &AddToolConfigMode,
    existing_configs: &BTreeMap<String, ToolConfigSpec>,
) -> Result<(), ConductorError> {
    let has_content_map = match config_mode {
        AddToolConfigMode::KeepExisting => existing_configs
            .get(tool_name)
            .is_some_and(|config| config.content_map.as_ref().is_some()),
        AddToolConfigMode::Replace(config) => config.content_map.as_ref().is_some(),
        AddToolConfigMode::Remove => false,
    };

    if has_content_map && matches!(&spec.kind, ToolKindSpec::Builtin { .. }) {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_name}' is builtin and cannot have tool_configs.content_map"
        )));
    }

    Ok(())
}

/// Encodes `conductor.ncl` with the latest persistence version envelope.
pub fn encode_user_document(document: UserNickelDocument) -> Result<Vec<u8>, ConductorError> {
    versions::encode_user_document(document)
}

/// Decodes `conductor.ncl` from versioned persistence bytes.
pub fn decode_user_document(bytes: &[u8]) -> Result<UserNickelDocument, ConductorError> {
    versions::decode_user_document(bytes)
}

/// Encodes `conductor.machine.ncl` with the latest persistence version envelope.
pub fn encode_machine_document(document: MachineNickelDocument) -> Result<Vec<u8>, ConductorError> {
    versions::encode_machine_document(document)
}

/// Decodes `conductor.machine.ncl` from versioned persistence bytes.
pub fn decode_machine_document(bytes: &[u8]) -> Result<MachineNickelDocument, ConductorError> {
    versions::decode_machine_document(bytes)
}

/// Encodes `.conductor/state.ncl` with the latest persistence version envelope.
pub fn encode_state_document(document: StateNickelDocument) -> Result<Vec<u8>, ConductorError> {
    versions::encode_state_document(document)
}

/// Decodes `.conductor/state.ncl` from versioned persistence bytes.
pub fn decode_state_document(bytes: &[u8]) -> Result<StateNickelDocument, ConductorError> {
    versions::decode_state_document(bytes)
}

/// Evaluates fixed Nickel migrations/contracts plus user, machine, and state configuration.
pub fn evaluate_total_configuration_sources(
    user_source: &str,
    machine_source: &str,
    state_source: &str,
) -> Result<(), ConductorError> {
    versions::evaluate_total_configuration_sources(user_source, machine_source, state_source)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::Hash;

    use super::{
        AddExternalDataOptions, AddToolConfigMode, AddToolOptions, ExternalContentRef,
        MachineNickelDocument, ToolConfigSpec, ToolKindSpec, ToolSpec, UserNickelDocument,
    };

    /// Verifies add-tool can insert both tool spec and tool config in one call.
    #[test]
    fn add_tool_inserts_spec_and_config() {
        let mut document = UserNickelDocument::default();
        let options = AddToolOptions::new(ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["demo-tool".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        })
        .with_tool_config(ToolConfigSpec {
            max_concurrent_calls: 2,
            content_map: Some(BTreeMap::from([(
                "payload.txt".to_string(),
                Hash::from_content(b"demo-hash-a"),
            )])),
        });

        document.add_tool("demo@1.0.0", options).expect("add tool with config should succeed");

        assert!(document.tools.contains_key("demo@1.0.0"));
        assert!(document.tool_configs.contains_key("demo@1.0.0"));
    }

    /// Verifies duplicate insertion fails unless overwrite policy is enabled.
    #[test]
    fn add_tool_rejects_duplicate_without_overwrite() {
        let mut document = UserNickelDocument::default();
        let options = AddToolOptions::new(ToolSpec::default());

        document.add_tool("echo@1.0.0", options.clone()).expect("first insert should succeed");

        let error = document
            .add_tool("echo@1.0.0", options)
            .expect_err("second insert without overwrite should fail");
        assert!(error.to_string().contains("already exists"));
    }

    /// Verifies overwrite mode can replace an entry and drop stale config.
    #[test]
    fn add_tool_overwrite_can_remove_existing_config() {
        let mut document = UserNickelDocument::default();

        document
            .add_tool(
                "tool@1.0.0",
                AddToolOptions::new(ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["first".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    ..ToolSpec::default()
                })
                .with_tool_config(ToolConfigSpec {
                    max_concurrent_calls: 1,
                    content_map: Some(BTreeMap::from([(
                        "payload.txt".to_string(),
                        Hash::from_content(b"demo-hash-b"),
                    )])),
                }),
            )
            .expect("initial tool insert should succeed");

        document
            .add_tool(
                "tool@1.0.0",
                AddToolOptions::new(ToolSpec::default())
                    .overwrite_existing(true)
                    .remove_tool_config(),
            )
            .expect("overwrite with remove config should succeed");

        assert!(document.tools.contains_key("tool@1.0.0"));
        assert!(!document.tool_configs.contains_key("tool@1.0.0"));
    }

    /// Verifies builtin entries reject `content_map` at add-tool validation time.
    #[test]
    fn add_tool_rejects_builtin_with_content_map() {
        let mut document = UserNickelDocument::default();

        let error = document
            .add_tool(
                "echo@1.0.0",
                AddToolOptions {
                    spec: ToolSpec::default(),
                    overwrite_existing: false,
                    config_mode: AddToolConfigMode::Replace(ToolConfigSpec {
                        max_concurrent_calls: 1,
                        content_map: Some(BTreeMap::from([(
                            "payload.txt".to_string(),
                            Hash::from_content(b"demo-hash-c"),
                        )])),
                    }),
                },
            )
            .expect_err("builtin content_map should fail validation");

        assert!(error.to_string().contains("cannot have tool_configs.content_map"));
    }

    /// Verifies machine external-data insertion succeeds for new names.
    #[test]
    fn add_machine_external_data_inserts_entry() {
        let mut machine = MachineNickelDocument::default();
        machine
            .add_external_data(
                "fixture.txt",
                AddExternalDataOptions::new(ExternalContentRef {
                    hash: Hash::from_content(b"fixture"),
                    description: Some("fixture payload".to_string()),
                }),
            )
            .expect("machine external data insert should succeed");

        assert!(machine.external_data.contains_key("fixture.txt"));
    }

    /// Verifies duplicate machine external-data insertion fails unless overwrite mode is enabled.
    #[test]
    fn add_machine_external_data_rejects_duplicate_without_overwrite() {
        let mut machine = MachineNickelDocument::default();
        machine
            .add_external_data(
                "fixture.txt",
                AddExternalDataOptions::new(ExternalContentRef {
                    hash: Hash::from_content(b"fixture-a"),
                    description: None,
                }),
            )
            .expect("first insert should succeed");

        let error = machine
            .add_external_data(
                "fixture.txt",
                AddExternalDataOptions::new(ExternalContentRef {
                    hash: Hash::from_content(b"fixture-b"),
                    description: None,
                }),
            )
            .expect_err("duplicate insert without overwrite should fail");

        assert!(error.to_string().contains("already exists"));
    }
}
