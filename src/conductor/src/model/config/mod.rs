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

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use crate::error::ConductorError;
use crate::model::state::{OutputSaveMode, PersistenceFlags};
use crate::tools::downloader::use_user_download_cache_enabled;

pub(crate) mod versions;

/// Platform-keyed inherited environment-variable names.
///
/// Keys are normalized case-insensitively at merge/read time so user-authored
/// casing (`windows`, `Windows`, `WINDOWS`, ...) does not change runtime
/// behavior.
pub type PlatformInheritedEnvVars = BTreeMap<String, Vec<String>>;

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
    /// Optional tri-state save override; falls back to inherited/default value
    /// when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub save: Option<OutputSaveMode>,
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
/// This shape is persisted as a grouped `runtime` record in
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
    pub conductor_state_config: Option<String>,
    /// Optional override path for filesystem CAS storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cas_store_dir: Option<String>,
    /// Optional override path for temporary execution sandboxes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_tmp_dir: Option<String>,
    /// Optional override path for exported conductor schemas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_schema_dir: Option<String>,
    /// Optional additional inherited host environment-variable names keyed by
    /// platform.
    ///
    /// Runtime always includes one host-default baseline for safety-sensitive
    /// process execution (`SYSTEMROOT`, `WINDIR`, `TEMP`, `TMP` on Windows;
    /// empty baseline on other platforms). When this field is present, runtime
    /// reads only the active host-platform entry (`windows`, `linux`,
    /// `macos`, ...) and merges names onto that baseline in declaration order
    /// with case-insensitive deduplication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_env_vars: Option<PlatformInheritedEnvVars>,
    /// Optional toggle for shared global user-level managed-tool download
    /// cache.
    ///
    /// When omitted, the cache is enabled by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_user_tool_cache: Option<bool>,
}

/// Returns host-specific default inherited environment-variable names keyed by
/// platform.
///
/// These names are merged into executable runtime environments before
/// `tools.<tool>.env_vars` and `tool_configs.<tool>.env_vars` so callers can
/// keep baseline process invariants without repeating them per tool.
#[must_use]
pub fn default_runtime_inherited_env_vars_for_host() -> PlatformInheritedEnvVars {
    if cfg!(windows) {
        BTreeMap::from([(
            "windows".to_string(),
            vec![
                "SYSTEMROOT".to_string(),
                "WINDIR".to_string(),
                "TEMP".to_string(),
                "TMP".to_string(),
            ],
        )])
    } else {
        BTreeMap::new()
    }
}

/// Returns one normalized host platform key used by runtime env-var mapping.
#[must_use]
fn host_platform_key() -> String {
    std::env::consts::OS.to_ascii_lowercase()
}

/// Normalizes one platform key authored in runtime inherited env-var config.
#[must_use]
fn normalize_runtime_platform_key(raw_platform: &str) -> Option<String> {
    let trimmed = raw_platform.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_ascii_lowercase()) }
}

fn append_unique_env_var_names(target: &mut Vec<String>, source: impl IntoIterator<Item = String>) {
    for raw_name in source {
        let trimmed = raw_name.trim();
        if trimmed.is_empty() {
            continue;
        }

        if target.iter().any(|existing| existing.eq_ignore_ascii_case(trimmed)) {
            continue;
        }

        target.push(trimmed.to_string());
    }
}

/// Appends one platform-scoped inherited env-var list for the active host.
fn append_platform_inherited_env_var_names_for_host(
    target: &mut Vec<String>,
    source: &PlatformInheritedEnvVars,
    host_platform: &str,
) {
    for (platform_key, names) in source {
        if normalize_runtime_platform_key(platform_key).as_deref() == Some(host_platform) {
            append_unique_env_var_names(target, names.iter().cloned());
        }
    }
}

impl RuntimeStorageConfig {
    /// Returns whether all runtime-storage override fields are absent.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.conductor_dir.is_none()
            && self.conductor_state_config.is_none()
            && self.cas_store_dir.is_none()
            && self.conductor_tmp_dir.is_none()
            && self.conductor_schema_dir.is_none()
            && self.inherited_env_vars.is_none()
            && self.use_user_tool_cache.is_none()
    }

    /// Returns whether shared global user-level download cache should be used.
    ///
    /// Absent configuration defaults to `true` so repeated tool downloads can
    /// reuse payload bytes across local workspaces for this user.
    #[must_use]
    pub const fn use_user_tool_cache_enabled(&self) -> bool {
        use_user_download_cache_enabled(self.use_user_tool_cache)
    }

    /// Returns inherited runtime environment names merged with host defaults.
    ///
    /// Runtime reads only the active host-platform entry from configured
    /// `runtime.inherited_env_vars` values.
    ///
    /// Ordering is deterministic: host defaults first, then configured host
    /// values. Duplicate names are removed using case-insensitive matching.
    #[must_use]
    pub fn inherited_env_vars_with_defaults(&self) -> Vec<String> {
        let host_platform = host_platform_key();
        let mut merged = Vec::new();

        append_platform_inherited_env_var_names_for_host(
            &mut merged,
            &default_runtime_inherited_env_vars_for_host(),
            &host_platform,
        );

        if let Some(configured) = &self.inherited_env_vars {
            append_platform_inherited_env_var_names_for_host(
                &mut merged,
                configured,
                &host_platform,
            );
        }

        merged
    }
}

impl OutputPolicy {
    /// Resolves optional overrides against a concrete base policy.
    #[must_use]
    pub fn resolve(self, base: PersistenceFlags) -> PersistenceFlags {
        PersistenceFlags { save: self.save.unwrap_or(base.save) }
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
    /// Grouped runtime storage path configuration persisted under `runtime`.
    #[serde(default, skip_serializing_if = "RuntimeStorageConfig::is_empty")]
    pub runtime: RuntimeStorageConfig,
    /// External content metadata keyed by CAS hash identity.
    #[serde(default)]
    pub external_data: BTreeMap<Hash, ExternalContentRef>,
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
    /// Optional human description.
    #[serde(default)]
    pub description: Option<String>,
    /// Optional save policy for this external-data root.
    ///
    /// Supported values:
    /// - `true` => regular saved behavior,
    /// - `"full"` => saved behavior with full-data preference hints,
    /// - `None` => runtime default behavior.
    ///
    /// `false` (`OutputSaveMode::Unsaved`) is invalid for external-data
    /// references and rejected during validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub save: Option<OutputSaveMode>,
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
    /// - `false` (default): adding an existing hash fails fast.
    /// - `true`: existing entries for the same hash are replaced.
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

/// Validates one external-data save mode for machine-document insertion.
fn validate_external_data_save_mode(
    save_mode: Option<OutputSaveMode>,
) -> Result<(), ConductorError> {
    if matches!(save_mode, Some(OutputSaveMode::Unsaved)) {
        return Err(ConductorError::Workflow(
            "external_data save policy cannot be false/unsaved; use true/saved or \"full\""
                .to_string(),
        ));
    }

    Ok(())
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
    /// Persistence policy (`save`) belongs to workflow tool-call
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
    /// Maximum retry count for one tool call after the initial attempt.
    ///
    /// Semantics:
    /// - `-1`: use runtime default retry behavior,
    /// - `0`: execute once with no retries,
    /// - positive integer: retry at most that many times after the first
    ///   failed attempt.
    ///
    /// Values smaller than `-1` are invalid.
    #[serde(default = "default_max_retries")]
    pub max_retries: i32,
    /// Optional human-facing description for this tool runtime configuration.
    ///
    /// This field is informational only and must not affect runtime identity,
    /// scheduling, deduplication, or CAS key computation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional per-tool default input bindings applied when workflow steps
    /// omit matching input keys.
    ///
    /// This allows operator-managed runtime defaults (for example list-style
    /// CLI argument bundles) to live beside tool runtime config rather than
    /// being repeated across workflow steps.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub input_defaults: BTreeMap<String, InputBinding>,
    /// Optional explicit runtime environment map for tool execution.
    ///
    /// This map contributes process environment variables at runtime without
    /// changing reusable tool identity. Runtime merges these entries with
    /// runtime inherited env vars and executable `tools.<tool>.env_vars`.
    ///
    /// Duplicate keys are rejected only between explicit maps
    /// (`tools.<tool>.env_vars` and `tool_configs.<tool>.env_vars`) so
    /// operators can still override inherited host values intentionally.
    ///
    /// This configuration is invalid for builtin tools.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env_vars: BTreeMap<String, String>,
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
    /// - Every referenced CAS hash must also appear in top-level
    ///   `external_data` so configuration-owned tool payload roots remain
    ///   visible to pruning logic.
    ///
    /// This configuration is invalid for builtin tools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_map: Option<BTreeMap<String, Hash>>,
}

impl Default for ToolConfigSpec {
    fn default() -> Self {
        Self {
            max_concurrent_calls: default_max_concurrent_calls(),
            max_retries: default_max_retries(),
            description: None,
            input_defaults: BTreeMap::new(),
            env_vars: BTreeMap::new(),
            content_map: None,
        }
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

fn default_max_retries() -> i32 {
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

/// Returns whether a value equals its type default for serde skip checks.
fn is_default_value<T>(value: &T) -> bool
where
    T: Default + PartialEq,
{
    value == &T::default()
}

/// Tool input declaration entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ToolInputSpec {
    /// Declared value kind for this input.
    ///
    /// When omitted, runtime defaults to [`ToolInputKind::String`].
    #[serde(default, skip_serializing_if = "is_default_value")]
    pub kind: ToolInputKind,
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
        /// - `${context.os}` → injects host platform text
        ///   (`windows`, `linux`, or `macos`).
        /// - `${<left> <op> <right> ? <true> | <false>}` → comparison
        ///   conditional with operators `==`, `!=`, `<`, `<=`, `>`, `>=`; the
        ///   selected branch is rendered recursively and may include selector
        ///   or materialization forms.
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
    /// Capture bytes from one file selected by a regex against the
    /// sandbox-relative path.
    ///
    /// The runtime evaluates this regex against normalized relative paths
    /// using forward slashes (`/`) regardless of host platform. Exactly one
    /// regular file must match.
    FileRegex {
        /// Regex matched against normalized sandbox-relative file paths.
        path_regex: String,
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
    /// Capture one ZIP payload containing all files selected by regex.
    ///
    /// The runtime evaluates this regex against normalized sandbox-relative
    /// file and directory paths using forward slashes (`/`). Matched
    /// directories contribute all descendant files. The resulting ZIP always
    /// preserves paths relative to the sandbox root unless regex capture
    /// groups are present: when one or more captures match, their strings are
    /// joined and used as the ZIP member relative path; when no capture groups
    /// match, the original sandbox-relative path is kept.
    FolderRegex {
        /// Regex matched against normalized sandbox-relative paths.
        path_regex: String,
    },
}

/// Declared output contract for one tool output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOutputSpec {
    /// Capture source for this output.
    ///
    /// For `kind = "file"` and `kind = "folder"`, the configured `path`
    /// supports `${...}` template interpolation using the same rules as
    /// process/runtime template values. Regex capture kinds evaluate the final
    /// regex pattern against normalized sandbox-relative paths.
    pub capture: OutputCaptureSpec,
    /// Whether a capture that produces no output (missing file, no regex
    /// match, missing folder) is treated as a successful empty output rather
    /// than a workflow error.
    ///
    /// When `true` and the capture source is absent or empty, the output is
    /// stored as an empty capture in the orchestration state. Downstream steps
    /// that reference an empty-capture output as a step input receive a
    /// workflow error at resolution time, preventing silent empty-payload
    /// propagation.
    ///
    /// This flag is appropriate for conditional outputs — artifacts that a
    /// tool produces only when certain options are active (for example
    /// subtitle sidecars, thumbnail sidecars, description files). Marking
    /// these outputs `allow_empty = true` prevents spurious workflow failures
    /// when the tool is configured to skip the optional artifact.
    ///
    /// Defaults to `false`: missing captures are errors by default.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_empty: bool,
}

impl Default for ToolOutputSpec {
    fn default() -> Self {
        Self { capture: OutputCaptureSpec::Stdout {}, allow_empty: false }
    }
}

fn default_success_codes() -> Vec<i32> {
    vec![0]
}

/// One workflow DAG.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkflowSpec {
    /// Optional human-facing workflow label.
    ///
    /// This metadata is informational only and must not affect deterministic
    /// instance identity, topological ordering, or caching behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Optional human-facing workflow description.
    ///
    /// This metadata is informational only and must not affect deterministic
    /// instance identity, topological ordering, or caching behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
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
    ///   - `${external_data.<hash>}`,
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
    ///
    /// # Errors
    ///
    /// Returns any error produced by `callback` for one visited scalar item.
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

/// Prefix for `${external_data.<hash>}` interpolation expression bodies.
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
        /// External-data reference hash key.
        hash: Hash,
    },
    /// Interpolated prior-step output reference.
    StepOutput {
        /// Dependency step id that produced the output.
        step_id: &'a str,
        /// Output name on the dependency step.
        output: &'a str,
        /// Optional ZIP member selector extracted from output bytes.
        zip_member: Option<&'a str>,
    },
}

/// Parses one optional trailing `:zip(<member>)` selector.
fn parse_optional_zip_selector(expression: &str) -> Result<(&str, Option<&str>), ConductorError> {
    if !expression.contains(":zip(") {
        return Ok((expression, None));
    }

    let Some(without_suffix) = expression.strip_suffix(')') else {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; malformed :zip(...) selector"
        )));
    };

    let Some((selector, member)) = without_suffix.rsplit_once(":zip(") else {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; malformed :zip(...) selector"
        )));
    };

    let member = member.trim();
    if member.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; :zip(...) requires one non-empty member key"
        )));
    }
    if member.contains('/') || member.contains('\\') {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; :zip(...) member key must be flat and must not contain path separators"
        )));
    }

    if selector.trim().is_empty() {
        return Err(ConductorError::Workflow(format!(
            "invalid input binding expression '${{{expression}}}'; :zip(...) requires one non-empty selector prefix"
        )));
    }

    Ok((selector.trim(), Some(member)))
}

/// Parses one `${...}` expression body from a workflow-step input binding.
fn parse_input_binding_expression<'a>(
    expression: &'a str,
    binding: &str,
) -> Result<ParsedInputBindingSegment<'a>, ConductorError> {
    if expression.contains(":file(") || expression.contains(":folder(") {
        return Err(ConductorError::Workflow(format!(
            "unsupported input binding expression '${{{expression}}}' in '{binding}'; supported interpolation forms are '${{external_data.<hash>}}' and '${{step_output.<step_id>.<output_name>}}'. Input bindings do not support materialization directives like ':file(...)' or ':folder(...)'"
        )));
    }

    let (selector_expression, zip_member) = parse_optional_zip_selector(expression)?;

    if let Some(hash_text) = selector_expression.strip_prefix(INPUT_BINDING_EXTERNAL_DATA_PREFIX) {
        let hash_text = hash_text.trim();
        if hash_text.is_empty() {
            return Err(ConductorError::Workflow(
                "input binding '${external_data.<hash>}' requires a non-empty <hash>".to_string(),
            ));
        }
        let hash = Hash::from_str(hash_text).map_err(|error| {
            ConductorError::Workflow(format!(
                "input binding '${{external_data.{hash_text}}}' must reference a valid CAS hash key: {error}"
            ))
        })?;
        if zip_member.is_some() {
            return Err(ConductorError::Workflow(format!(
                "unsupported input binding expression '${{{expression}}}' in '{binding}'; :zip(...) is currently supported only for step_output references"
            )));
        }
        return Ok(ParsedInputBindingSegment::ExternalData { hash });
    }

    if let Some(selector) = selector_expression.strip_prefix(INPUT_BINDING_STEP_OUTPUT_PREFIX) {
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
        return Ok(ParsedInputBindingSegment::StepOutput { step_id, output, zip_member });
    }

    Err(ConductorError::Workflow(format!(
        "unsupported input binding expression '${{{expression}}}' in '{binding}'; supported interpolation forms are '${{external_data.<hash>}}', '${{step_output.<step_id>.<output_name>}}', and '${{step_output.<step_id>.<output_name>:zip(<member>)}}'. Input bindings do not support materialization directives like ':file(...)' or ':folder(...)'"
    )))
}

/// Parses one workflow-step input binding string into interpolation segments.
///
/// Rules:
/// - plain text outside `${...}` tokens is preserved as literal content,
/// - supported interpolation expressions are `${external_data.<hash>}`,
///   and `${step_output.<step_id>.<output_name>}`,
/// - `${step_output.<step_id>.<output_name>:zip(<member>)}` additionally
///   selects one ZIP member from the referenced output bytes,
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
    /// Grouped runtime storage path configuration persisted under `runtime`.
    #[serde(default, skip_serializing_if = "RuntimeStorageConfig::is_empty")]
    pub runtime: RuntimeStorageConfig,
    /// External content metadata keyed by CAS hash identity.
    #[serde(default)]
    pub external_data: BTreeMap<Hash, ExternalContentRef>,
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
    /// - builtin tools cannot end up with `content_map` in effective config,
    /// - content-map hashes are reconciled into managed `external_data` roots.
    ///
    /// # Errors
    ///
    /// Returns an error when validation fails (for example empty tool names,
    /// conflicting entries without overwrite mode, or invalid builtin/config
    /// combinations).
    pub fn add_tool(
        &mut self,
        tool_name: impl Into<String>,
        options: AddToolOptions,
    ) -> Result<(), ConductorError> {
        add_tool_to_maps(
            &mut self.tools,
            &mut self.tool_configs,
            &mut self.external_data,
            tool_name.into(),
            options,
        )
    }

    /// Reconciles managed external-data CAS roots with current tool content maps.
    ///
    /// This helper guarantees that every hash referenced from
    /// `tool_configs.<tool>.content_map` appears in `external_data` and removes
    /// stale managed tool-content root entries when no configured tool refers
    /// to those hashes anymore.
    pub fn sync_tool_content_external_data_roots(&mut self) {
        sync_tool_content_external_data_roots(&mut self.external_data, &self.tool_configs);
    }
}

impl MachineNickelDocument {
    /// Adds one tool definition (and optional tool config) to machine document state.
    ///
    /// Validation rules mirror [`UserNickelDocument::add_tool`].
    ///
    /// # Errors
    ///
    /// Returns an error when validation fails (for example empty tool names,
    /// conflicting entries without overwrite mode, or invalid builtin/config
    /// combinations).
    pub fn add_tool(
        &mut self,
        tool_name: impl Into<String>,
        options: AddToolOptions,
    ) -> Result<(), ConductorError> {
        add_tool_to_maps(
            &mut self.tools,
            &mut self.tool_configs,
            &mut self.external_data,
            tool_name.into(),
            options,
        )
    }

    /// Reconciles managed external-data CAS roots with current tool content maps.
    ///
    /// This helper guarantees that every hash referenced from
    /// `tool_configs.<tool>.content_map` appears in `external_data` and removes
    /// stale managed tool-content root entries when no configured tool refers
    /// to those hashes anymore.
    pub fn sync_tool_content_external_data_roots(&mut self) {
        sync_tool_content_external_data_roots(&mut self.external_data, &self.tool_configs);
    }

    /// Adds one external-data entry to machine document state.
    ///
    /// Validation rules:
    /// - `hash` is the external-data map key,
    /// - duplicates fail unless `overwrite_existing = true`.
    ///
    /// # Errors
    ///
    /// Returns an error when `hash` already exists and overwrite mode is not
    /// enabled.
    pub fn add_external_data(
        &mut self,
        hash: Hash,
        options: AddExternalDataOptions,
    ) -> Result<(), ConductorError> {
        validate_external_data_save_mode(options.reference.save)?;

        if !options.overwrite_existing && self.external_data.contains_key(&hash) {
            return Err(ConductorError::Workflow(format!(
                "external data '{hash}' already exists in machine config; set overwrite_existing=true to replace it"
            )));
        }

        self.external_data.insert(hash, options.reference);
        Ok(())
    }
}

/// Prefix reserved for managed external-data descriptions that root tool
/// content-map CAS hashes against pruning.
const MANAGED_TOOL_CONTENT_DESCRIPTION_PREFIX: &str = "managed tool content CAS root for";

/// Collects all CAS hashes referenced by configured tool content maps.
fn collect_tool_content_map_hashes(
    tool_configs: &BTreeMap<String, ToolConfigSpec>,
) -> BTreeSet<Hash> {
    tool_configs
        .values()
        .flat_map(|config| config.content_map.iter().flat_map(|map| map.values().copied()))
        .collect()
}

/// Returns true when one external-data description marks managed tool content.
fn is_managed_tool_content_description(description: Option<&str>) -> bool {
    description.is_some_and(|text| text.starts_with(MANAGED_TOOL_CONTENT_DESCRIPTION_PREFIX))
}

/// Reconciles managed external-data roots against current tool content-map hashes.
///
/// Behavior:
/// - ensures each referenced content-map hash appears at least once in
///   `external_data`,
/// - removes stale managed tool-content entries whose hash no longer appears
///   in any configured tool content map,
/// - preserves non-managed `external_data` entries even when their hashes are
///   unrelated to tool content maps.
fn sync_tool_content_external_data_roots(
    external_data: &mut BTreeMap<Hash, ExternalContentRef>,
    tool_configs: &BTreeMap<String, ToolConfigSpec>,
) {
    let referenced_hashes = collect_tool_content_map_hashes(tool_configs);

    external_data.retain(|hash, reference| {
        referenced_hashes.contains(hash)
            || !is_managed_tool_content_description(reference.description.as_deref())
    });

    for hash in referenced_hashes {
        external_data.entry(hash).or_insert_with(|| ExternalContentRef {
            description: Some(format!("{MANAGED_TOOL_CONTENT_DESCRIPTION_PREFIX} {hash}")),
            save: None,
        });
    }
}

/// Validates and applies one add-tool request against document maps.
///
/// This helper keeps user/machine document add-tool semantics identical.
fn add_tool_to_maps(
    tools: &mut BTreeMap<String, ToolSpec>,
    tool_configs: &mut BTreeMap<String, ToolConfigSpec>,
    external_data: &mut BTreeMap<Hash, ExternalContentRef>,
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

    sync_tool_content_external_data_roots(external_data, tool_configs);

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

    let has_input_defaults = match config_mode {
        AddToolConfigMode::KeepExisting => {
            existing_configs.get(tool_name).is_some_and(|config| !config.input_defaults.is_empty())
        }
        AddToolConfigMode::Replace(config) => !config.input_defaults.is_empty(),
        AddToolConfigMode::Remove => false,
    };

    if has_input_defaults && matches!(&spec.kind, ToolKindSpec::Builtin { .. }) {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_name}' is builtin and cannot have tool_configs.input_defaults"
        )));
    }

    let has_env_vars = match config_mode {
        AddToolConfigMode::KeepExisting => {
            existing_configs.get(tool_name).is_some_and(|config| !config.env_vars.is_empty())
        }
        AddToolConfigMode::Replace(config) => !config.env_vars.is_empty(),
        AddToolConfigMode::Remove => false,
    };

    if has_env_vars && matches!(&spec.kind, ToolKindSpec::Builtin { .. }) {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_name}' is builtin and cannot have tool_configs.env_vars"
        )));
    }

    Ok(())
}

/// Encodes `conductor.ncl` with the latest persistence version envelope.
///
/// # Errors
///
/// Returns an error when the document cannot be converted into the latest
/// versioned envelope or serialized as Nickel source.
pub fn encode_user_document(document: UserNickelDocument) -> Result<Vec<u8>, ConductorError> {
    versions::encode_user_document(document)
}

/// Decodes `conductor.ncl` from versioned persistence bytes.
///
/// # Errors
///
/// Returns an error when UTF-8 decoding, Nickel migration/validation, or
/// bridge deserialization fails.
pub fn decode_user_document(bytes: &[u8]) -> Result<UserNickelDocument, ConductorError> {
    versions::decode_user_document(bytes)
}

/// Encodes `conductor.machine.ncl` with the latest persistence version envelope.
///
/// # Errors
///
/// Returns an error when the document cannot be converted into the latest
/// versioned envelope or serialized as Nickel source.
pub fn encode_machine_document(document: MachineNickelDocument) -> Result<Vec<u8>, ConductorError> {
    versions::encode_machine_document(document)
}

/// Decodes `conductor.machine.ncl` from versioned persistence bytes.
///
/// # Errors
///
/// Returns an error when UTF-8 decoding, Nickel migration/validation, or
/// bridge deserialization fails.
pub fn decode_machine_document(bytes: &[u8]) -> Result<MachineNickelDocument, ConductorError> {
    versions::decode_machine_document(bytes)
}

/// Encodes `.conductor/state.ncl` with the latest persistence version envelope.
///
/// # Errors
///
/// Returns an error when the state document cannot be converted into the
/// latest envelope or serialized as Nickel source.
pub fn encode_state_document(document: StateNickelDocument) -> Result<Vec<u8>, ConductorError> {
    versions::encode_state_document(document)
}

/// Decodes `.conductor/state.ncl` from versioned persistence bytes.
///
/// # Errors
///
/// Returns an error when UTF-8 decoding, volatile-shape validation, Nickel
/// migration/validation, or bridge deserialization fails.
pub fn decode_state_document(bytes: &[u8]) -> Result<StateNickelDocument, ConductorError> {
    versions::decode_state_document(bytes)
}

/// Evaluates fixed Nickel migrations/contracts plus user, machine, and state configuration.
///
/// # Errors
///
/// Returns an error when any document fails version checks, schema validation,
/// or merged configuration evaluation.
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
        MachineNickelDocument, OutputSaveMode, ToolConfigSpec, ToolKindSpec, ToolSpec,
        UserNickelDocument,
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
            max_retries: -1,
            description: Some("demo executable runtime config".to_string()),
            input_defaults: BTreeMap::new(),
            env_vars: BTreeMap::new(),
            content_map: Some(BTreeMap::from([(
                "payload.txt".to_string(),
                Hash::from_content(b"demo-hash-a"),
            )])),
        });

        document.add_tool("demo@1.0.0", options).expect("add tool with config should succeed");

        assert!(document.tools.contains_key("demo@1.0.0"));
        assert!(document.tool_configs.contains_key("demo@1.0.0"));
        assert!(document.external_data.contains_key(&Hash::from_content(b"demo-hash-a")));
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
                    max_retries: -1,
                    description: Some("initial executable runtime config".to_string()),
                    input_defaults: BTreeMap::new(),
                    env_vars: BTreeMap::new(),
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
        assert!(!document.external_data.contains_key(&Hash::from_content(b"demo-hash-b")));
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
                        max_retries: -1,
                        description: Some("invalid builtin runtime config".to_string()),
                        input_defaults: BTreeMap::new(),
                        env_vars: BTreeMap::new(),
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

    /// Verifies machine external-data insertion succeeds for new hash keys.
    #[test]
    fn add_machine_external_data_inserts_entry() {
        let mut machine = MachineNickelDocument::default();
        let fixture_hash = Hash::from_content(b"fixture");
        machine
            .add_external_data(
                fixture_hash,
                AddExternalDataOptions::new(ExternalContentRef {
                    description: Some("fixture payload".to_string()),
                    save: None,
                }),
            )
            .expect("machine external data insert should succeed");

        assert!(machine.external_data.contains_key(&fixture_hash));
    }

    /// Verifies duplicate machine external-data insertion fails unless overwrite mode is enabled.
    #[test]
    fn add_machine_external_data_rejects_duplicate_without_overwrite() {
        let mut machine = MachineNickelDocument::default();
        let fixture_hash = Hash::from_content(b"fixture-a");
        machine
            .add_external_data(
                fixture_hash,
                AddExternalDataOptions::new(ExternalContentRef { description: None, save: None }),
            )
            .expect("first insert should succeed");

        let error = machine
            .add_external_data(
                fixture_hash,
                AddExternalDataOptions::new(ExternalContentRef { description: None, save: None }),
            )
            .expect_err("duplicate insert without overwrite should fail");

        assert!(error.to_string().contains("already exists"));
    }

    /// Verifies machine external-data insertion rejects unsaved (`false`) save policy.
    #[test]
    fn add_machine_external_data_rejects_unsaved_save_policy() {
        let mut machine = MachineNickelDocument::default();
        let fixture_hash = Hash::from_content(b"fixture-unsaved");

        let error = machine
            .add_external_data(
                fixture_hash,
                AddExternalDataOptions::new(ExternalContentRef {
                    description: Some("fixture unsaved".to_string()),
                    save: Some(OutputSaveMode::Unsaved),
                }),
            )
            .expect_err("unsaved external-data save policy should fail validation");

        assert!(error.to_string().contains("cannot be false/unsaved"));
    }

    /// Verifies stale managed tool-content roots are removed while non-managed
    /// external-data entries are preserved.
    #[test]
    fn sync_tool_content_external_data_roots_prunes_only_managed_entries() {
        let stale_hash = Hash::from_content(b"stale-tool-content");
        let kept_hash = Hash::from_content(b"kept-user-entry");
        let active_hash = Hash::from_content(b"active-tool-content");

        let mut machine = MachineNickelDocument {
            external_data: BTreeMap::from([
                (
                    stale_hash,
                    ExternalContentRef {
                        description: Some("managed tool content CAS root for stale".to_string()),
                        save: None,
                    },
                ),
                (
                    kept_hash,
                    ExternalContentRef {
                        description: Some("user-managed fixture".to_string()),
                        save: None,
                    },
                ),
            ]),
            tool_configs: BTreeMap::from([(
                "tool@1.0.0".to_string(),
                ToolConfigSpec {
                    max_concurrent_calls: -1,
                    max_retries: -1,
                    description: None,
                    input_defaults: BTreeMap::new(),
                    env_vars: BTreeMap::new(),
                    content_map: Some(BTreeMap::from([("bin/tool".to_string(), active_hash)])),
                },
            )]),
            ..MachineNickelDocument::default()
        };

        machine.sync_tool_content_external_data_roots();

        assert!(machine.external_data.contains_key(&kept_hash));
        assert!(!machine.external_data.contains_key(&stale_hash));
        assert!(machine.external_data.contains_key(&active_hash));
    }
}
