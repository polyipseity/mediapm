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

//!
//! This module holds the resolved (option-free) types only. The active
//! `*Latest` wire family lives in the `v_latest` file inside [`versions`] and
//! is reached exclusively through the unversioned entry points in
//! [`versions`] (`versions::from_boundary`, `versions::into_boundary`,
//! `versions::merge_document_sources`). No versioned symbol is re-exported
//! from here, and no code in this module names a versioned wire type in
//! either direction.

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
/// Every mode writes the capture when the step runs; the mode decides how long
/// the bytes live and whether the store may re-encode them, never whether they
/// are produced.
///
/// - `False`: the bytes stop being garbage-collection roots once the producing
///   instance is superseded.
/// - `True` (default): the bytes are retained as a saved artifact.
/// - `Full`: as `True`, and the object's only permitted base is recorded as
///   the empty object in the CAS, so the maintenance optimizer never stores it
///   as a delta against another blob. It may still serve as another object's
///   delta base.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum SaveMode {
    /// Drop the bytes once the producing instance is superseded.
    False,
    /// Retain the bytes as a saved artifact.
    #[default]
    True,
    /// Retain the bytes and never re-encode the object as a delta.
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

/// Capture kind for a step output.
///
/// The token is the kind and the kind is the layout: folder-style captures
/// encode whether the topmost folder is included in the token itself. Parsing
/// splits on the first `:` and rejoins the remainder, so a pattern or path that
/// contains `:` survives intact. Unknown prefixes and empty remainders are
/// rejected with the offending token named.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CaptureKind {
    /// Capture the child process's standard output stream.
    Stdout,
    /// Capture the child process's standard error stream.
    Stderr,
    /// Capture the child process's exit code as text.
    ProcessCode,
    /// Capture bytes from one sandbox file at `<path>`.
    File(String),
    /// Capture bytes from the single sandbox file matching `<pattern>`.
    FileRegex(String),
    /// Zip everything under `<path>`, with member paths relative to the sandbox root.
    Folder(String),
    /// Zip everything under `<path>`, with member paths relative to `<path>`.
    FolderFlat(String),
    /// Zip the matching folder, with member paths relative to the sandbox root.
    FolderRegex(String),
    /// Zip the matching folder, with member paths relative to that folder.
    FolderFlatRegex(String),
}

impl CaptureKind {
    /// Parses one capture token into a [`CaptureKind`].
    ///
    /// # Errors
    ///
    /// Returns an error naming the token when the prefix is unknown or the
    /// remainder after the first `:` is empty for a kind that requires one.
    pub fn parse_token(token: &str) -> Result<Self, String> {
        match token {
            "stdout" => Ok(Self::Stdout),
            "stderr" => Ok(Self::Stderr),
            "process_code" => Ok(Self::ProcessCode),
            _ => {
                let Some((prefix, remainder)) = token.split_once(':') else {
                    return Err(format!("unknown capture token '{token}'"));
                };
                if remainder.is_empty() {
                    return Err(format!(
                        "capture token '{token}' has an empty remainder after '{prefix}:'"
                    ));
                }
                let subject = format!("capture token '{token}'");
                match prefix {
                    "file" => {
                        validate_sandbox_relative_path(remainder, &subject)?;
                        Ok(Self::File(remainder.to_string()))
                    }
                    "file_regex" => Ok(Self::FileRegex(remainder.to_string())),
                    "folder" => {
                        validate_sandbox_relative_path(remainder, &subject)?;
                        Ok(Self::Folder(remainder.to_string()))
                    }
                    "folder_flat" => {
                        validate_sandbox_relative_path(remainder, &subject)?;
                        Ok(Self::FolderFlat(remainder.to_string()))
                    }
                    "folder_regex" => Ok(Self::FolderRegex(remainder.to_string())),
                    "folder_flat_regex" => Ok(Self::FolderFlatRegex(remainder.to_string())),
                    _ => Err(format!("unknown capture token '{token}'")),
                }
            }
        }
    }

    /// Renders this capture kind back to its token form.
    #[must_use]
    pub fn to_token(&self) -> String {
        match self {
            Self::Stdout => "stdout".to_string(),
            Self::Stderr => "stderr".to_string(),
            Self::ProcessCode => "process_code".to_string(),
            Self::File(path) => format!("file:{path}"),
            Self::FileRegex(pattern) => format!("file_regex:{pattern}"),
            Self::Folder(path) => format!("folder:{path}"),
            Self::FolderFlat(path) => format!("folder_flat:{path}"),
            Self::FolderRegex(pattern) => format!("folder_regex:{pattern}"),
            Self::FolderFlatRegex(pattern) => format!("folder_flat_regex:{pattern}"),
        }
    }

    /// Returns whether the capture reads a folder family.
    ///
    /// Folder-family captures may legitimately match nothing depending on what a
    /// tool produced; file captures either read one file or read none.
    #[must_use]
    pub const fn is_folder_family(&self) -> bool {
        matches!(
            self,
            Self::Folder(_) | Self::FolderFlat(_) | Self::FolderRegex(_) | Self::FolderFlatRegex(_)
        )
    }

    /// Returns the path/pattern remainder for kinds that carry one.
    #[must_use]
    pub fn remainder(&self) -> Option<&str> {
        match self {
            Self::Stdout | Self::Stderr | Self::ProcessCode => None,
            Self::File(value)
            | Self::FileRegex(value)
            | Self::Folder(value)
            | Self::FolderFlat(value)
            | Self::FolderRegex(value)
            | Self::FolderFlatRegex(value) => Some(value),
        }
    }
}

/// Validates that a sandbox-relative path stays inside the sandbox.
///
/// Refuses an empty path, an absolute path, a drive or UNC prefix, any `..`
/// component, and a backslash separator. A leading `./` and a trailing `/`
/// are ordinary spellings and stay accepted.
///
/// `subject` names the token or selector being validated and is included in
/// the error so callers can report which input broke which rule.
///
/// # Errors
///
/// Returns an error naming `subject` and the broken rule when `path` is not a
/// sandbox-relative path.
pub(crate) fn validate_sandbox_relative_path(path: &str, subject: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err(format!("{subject} must not be an empty path"));
    }
    if path.contains('\\') {
        return Err(format!("{subject} must not contain a backslash separator"));
    }
    if path.starts_with("//")
        || path
            .as_bytes()
            .get(1)
            .is_some_and(|byte| *byte == b':' && path.as_bytes()[0].is_ascii_alphabetic())
    {
        return Err(format!("{subject} must not contain a drive or UNC prefix"));
    }
    if std::path::Path::new(path).is_absolute() {
        return Err(format!("{subject} must not be an absolute path"));
    }
    for component in std::path::Path::new(path).components() {
        match component {
            std::path::Component::ParentDir => {
                return Err(format!("{subject} must not contain a '..' component"));
            }
            std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                return Err(format!("{subject} must not be an absolute path"));
            }
            std::path::Component::CurDir | std::path::Component::Normal(_) => {}
        }
    }
    Ok(())
}

impl std::fmt::Display for CaptureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_token())
    }
}

impl std::str::FromStr for CaptureKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_token(s)
    }
}

impl Serialize for CaptureKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_token())
    }
}

impl<'de> Deserialize<'de> for CaptureKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let token = String::deserialize(deserializer)?;
        Self::parse_token(&token).map_err(serde::de::Error::custom)
    }
}

/// Capture/output spec for a step.
///
/// Describes how output bytes are captured from a tool execution via a parsed
/// [`CaptureKind`] token plus persistence/tolerance settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputCaptureSpec {
    /// Logical output name.
    pub name: String,
    /// Parsed capture source selector.
    pub capture: CaptureKind,
    /// The persistence intent this record carries, one of `"false"`, `"true"`,
    /// `"full"`.
    ///
    /// It decides retention and encoding, not capture: every capture is written
    /// to the CAS whatever this says, and no reader consults it to decide what a
    /// step may reuse or materialize. `False` keeps the bytes only while the
    /// producing instance is reachable, so the conductor's GC reclaims them once
    /// that instance is superseded. `Full` records the empty object as the
    /// object's only permitted base, so the optimizer never stores it as a delta
    /// against another blob.
    ///
    /// This is the one field a step may set on a name its tool already
    /// declares: the tool owns the capture itself (selector and `allow_empty`)
    /// and the step owns only whether the bytes are kept. Persistence is a
    /// decision about the store, not about what the work produces.
    #[serde(default = "default_save_mode", skip_serializing_if = "is_save_mode_true")]
    pub save: SaveMode,
    /// Whether an empty capture result (e.g. missing file) is acceptable.
    /// When `true`, an empty result is stored as empty bytes instead of
    /// silently skipping the output.
    #[serde(default = "default_allow_empty")]
    pub allow_empty: bool,
}

impl Default for OutputCaptureSpec {
    fn default() -> Self {
        Self {
            name: String::new(),
            capture: CaptureKind::Stdout,
            save: SaveMode::True,
            allow_empty: false,
        }
    }
}

/// How a step obtains the payload of the tool it executes.
///
/// The payload itself is named by [`ToolRuntime::content_map`] and, once the
/// document records it, by [`ToolRuntime::payload_root`]. This enum decides
/// what a step does with that payload: run the command under the provisioned
/// directory, link that directory into the step sandbox, or write a copy of the
/// content map into the sandbox.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadStaging {
    /// Run the command under the provisioned payload directory, so nothing is
    /// written into the step sandbox for the tool. Requires a payload root.
    #[default]
    InPlace,
    /// Link the provisioned payload tree into the step sandbox, so the files
    /// share the provisioned inodes. Requires a payload root, and the tree it
    /// links is the one the content map names: an entry the root does not hold
    /// is refused before the link pass rather than left missing from the
    /// sandbox.
    ///
    /// A file the filesystem refuses to link — a payload root and a sandbox on
    /// different volumes — is copied instead of failing the step, so this
    /// strategy always produces the tree; the copy costs the bytes the link
    /// would have shared.
    HardLink,
    /// Write a copy of the content map into the step sandbox, unpacking
    /// archives as the content map declares. Needs no payload root.
    Copy,
}

impl PayloadStaging {
    /// Returns the strategy's persisted name, which is the name the Nickel
    /// contract uses.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InPlace => "in_place",
            Self::HardLink => "hard_link",
            Self::Copy => "copy",
        }
    }
}

/// How a document asks for a tool's payload to be staged.
///
/// This is the *choice* a document states; [`PayloadStaging`] is the *resolved*
/// strategy a step executes. [`Self::Inherit`] means the tool takes the
/// document's default — [`ConductorRuntimeConfig::payload_staging`] — and the
/// other three state a strategy outright. The distinction cannot outlive the
/// document boundary: [`Self::resolve`] turns a choice into a concrete
/// [`PayloadStaging`] while the document is read, so nothing downstream of the
/// document — a step worker in particular — can hold an unresolved strategy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigPayloadStaging {
    /// Take the document's default staging strategy.
    #[default]
    Inherit,
    /// Run the command under the provisioned payload directory.
    InPlace,
    /// Link the provisioned payload tree into the step sandbox.
    HardLink,
    /// Write a copy of the content map into the step sandbox.
    Copy,
}

impl ConfigPayloadStaging {
    /// Returns the choice's persisted name, which is the name the Nickel
    /// contract uses.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::InPlace => "in_place",
            Self::HardLink => "hard_link",
            Self::Copy => "copy",
        }
    }

    /// Resolves this choice against the default the document states.
    ///
    /// [`Self::Inherit`] answers the default and every other variant answers
    /// itself, so the result is always a strategy the executor can run.
    #[must_use]
    pub const fn resolve(self, document_default: PayloadStaging) -> PayloadStaging {
        match self {
            Self::Inherit => document_default,
            Self::InPlace => PayloadStaging::InPlace,
            Self::HardLink => PayloadStaging::HardLink,
            Self::Copy => PayloadStaging::Copy,
        }
    }
}

/// Default maximum retries for a tool after the first failed attempt.
///
/// Three is the documented default: enough to ride out a transient failure
/// without making a broken step expensive to notice.
pub const DEFAULT_TOOL_MAX_RETRIES: usize = 3;

/// Runtime configuration for a tool (replaces the old separate `tool_configs`
/// map in the V1 schema).
///
/// The `runtime` property on a tool holds fields that must NOT be part of the
/// tool-call-instance identity computation.  Changes to runtime fields affect
/// scheduling and sandbox materialization but do not invalidate cached outputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Impurity still records provenance and forces a fresh instance per call.
    /// It no longer gates retries: `max_retries` answers that directly, per
    /// tool and per step, because a failed attempt moves no state pointer.
    #[serde(default)]
    pub impure: bool,
    /// Environment variable names to inherit from the host process.
    ///
    /// These are resolved at `to_unified()` time against the current host
    /// environment and merged with runtime-wide inherited env vars. They are not
    /// the last word on a tool's environment: a literal of the same key in the
    /// kind's `env_vars` is folded after them and wins.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inherited_env_vars: Vec<String>,
    /// Maximum concurrent calls allowed for this tool.
    ///
    /// `None` means unlimited, which is also the default. A document that wants a
    /// cap states a positive count; `0` is refused at load rather than silently
    /// meaning unlimited, because the two are different intents.
    #[serde(default)]
    pub max_concurrent_calls: Option<usize>,
    /// Maximum retry attempts for this tool after the first failed attempt.
    #[serde(default = "default_tool_max_retries")]
    pub max_retries: usize,
    /// Absolute path of the directory holding this tool's provisioned payload
    /// tree.
    ///
    /// Empty means the document names none, which is only valid for a staging
    /// strategy that does not read it. `mediapm sync` writes it once the
    /// payload is materialized.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payload_root: String,
    /// How this tool asks for its payload to be staged.
    ///
    /// The default is [`ConfigPayloadStaging::Inherit`], which takes
    /// [`ConductorRuntimeConfig::payload_staging`]; a document that states no
    /// default there stages in place. `in_place` and `hard_link` both need a
    /// non-empty [`Self::payload_root`], and a document that carries a content
    /// map, asks for one of them, and names no root fails validation at load
    /// rather than silently copying.
    #[serde(default)]
    pub payload_staging: ConfigPayloadStaging,
}

const fn default_tool_max_retries() -> usize {
    DEFAULT_TOOL_MAX_RETRIES
}

impl Default for ToolRuntime {
    fn default() -> Self {
        Self {
            content_map: BTreeMap::new(),
            impure: false,
            inherited_env_vars: Vec::new(),
            max_concurrent_calls: None,
            // Three retries is the documented default and the only reason this is
            // hand-written: a derived `Default` would say zero and quietly make
            // the default dead.
            max_retries: DEFAULT_TOOL_MAX_RETRIES,
            payload_root: String::new(),
            payload_staging: ConfigPayloadStaging::Inherit,
        }
    }
}

/// Platform-grouped inherited environment-variable names (S-D3).
///
/// Keys are closed to the three supported platforms (`windows`, `linux`,
/// `macos`), mirroring the v2 Nickel `PlatformInheritedEnvVarsV2` contract.
/// Unknown platform keys are rejected at the serde boundary and env var
/// names must be non-empty. An absent platform field resolves to that
/// platform's default list; an explicitly stated empty array means the tool
/// inherits no names from that platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformInheritedEnvVars {
    /// Variable names inherited on Windows.
    #[serde(
        default = "default_windows_platform_inherited_env_var_names",
        deserialize_with = "deserialize_non_empty_env_names"
    )]
    pub windows: Vec<String>,
    /// Variable names inherited on Linux.
    #[serde(
        default = "default_linux_platform_inherited_env_var_names",
        deserialize_with = "deserialize_non_empty_env_names"
    )]
    pub linux: Vec<String>,
    /// Variable names inherited on macOS.
    #[serde(
        default = "default_macos_platform_inherited_env_var_names",
        deserialize_with = "deserialize_non_empty_env_names"
    )]
    pub macos: Vec<String>,
}

impl Default for PlatformInheritedEnvVars {
    fn default() -> Self {
        Self {
            windows: default_windows_platform_inherited_env_var_names(),
            linux: default_linux_platform_inherited_env_var_names(),
            macos: default_macos_platform_inherited_env_var_names(),
        }
    }
}

/// Returns the default inherited environment-variable names for Windows.
#[must_use]
pub fn default_windows_platform_inherited_env_var_names() -> Vec<String> {
    WINDOWS_PLATFORM_INHERITED_ENV_VAR_NAMES.iter().map(|&name| name.to_string()).collect()
}

/// Returns the default inherited environment-variable names for Linux.
#[must_use]
pub fn default_linux_platform_inherited_env_var_names() -> Vec<String> {
    LINUX_PLATFORM_INHERITED_ENV_VAR_NAMES.iter().map(|&name| name.to_string()).collect()
}

/// Returns the default inherited environment-variable names for macOS.
#[must_use]
pub fn default_macos_platform_inherited_env_var_names() -> Vec<String> {
    MACOS_PLATFORM_INHERITED_ENV_VAR_NAMES.iter().map(|&name| name.to_string()).collect()
}

impl PlatformInheritedEnvVars {
    /// Returns `true` when no platform has configured env var names.
    ///
    /// This is the explicitly-empty state and is no longer the default: an
    /// absent field resolves to the platform defaults instead.
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

/// Deserializes a tool's literal environment map, rejecting a name a child
/// process cannot be handed unambiguously.
///
/// An empty name reaches `Command::env` as a junk `=value` entry, a `=` splits
/// the name a second time on Windows, and a NUL fails the spawn with an error
/// that names no document key. All three are refused here, where the error can
/// still name the tool's field, rather than at the spawn site.
fn deserialize_env_var_map<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    let map = BTreeMap::<String, String>::deserialize(deserializer)?;
    for name in map.keys() {
        if name.is_empty() {
            return Err(D::Error::custom(
                "environment variable names must be non-empty (found an empty name)",
            ));
        }
        if name.contains('=') {
            return Err(D::Error::custom(format!(
                "environment variable name {name:?} must not contain '=', which separates a name \
                 from its value in a child's environment"
            )));
        }
        if name.contains('\0') {
            return Err(D::Error::custom(format!(
                "environment variable name {name:?} must not contain a NUL"
            )));
        }
    }
    Ok(map)
}

/// Platform environment configuration for the conductor runtime.
///
/// Groups the platform-keyed inherited env var names under a single
/// sub-record so the top-level [`ConductorRuntimeConfig`] stays flat for
/// scalar flags while platform-specific data is namespaced.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePlatformEnvConfig {
    /// Platform-keyed inherited env var names.
    ///
    /// Keys are closed to `windows`/`linux`/`macos`; each value lists
    /// environment variable *names* to inherit from the host process.  These
    /// are resolved at `to_unified()` time against the current platform.
    /// Absent from the wire means "use platform defaults"; the resolved
    /// config always carries the effective lists.
    #[serde(default)]
    pub platform_inherited_env_vars: PlatformInheritedEnvVars,
}

/// Runtime configuration for the conductor itself (not per-tool).
///
/// This is a serde-deserialization boundary type. Fields with meaningful
/// defaults are resolved to their resolved values at the boundary — no
/// `Option<T>` wraps a value that has a sensible default.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct ConductorRuntimeConfig {
    /// Platform environment configuration (inherited env var names).
    #[serde(default)]
    pub environment: RuntimePlatformEnvConfig,
    /// How a step stages the payload of a tool that names no strategy of its
    /// own.
    ///
    /// This is the document-level default, not a per-tool decision: a tool's
    /// own [`ToolRuntime::payload_staging`] wins where it states one. Absent
    /// resolves to [`PayloadStaging::InPlace`] at the boundary.
    #[serde(default)]
    pub payload_staging: PayloadStaging,
}

impl<'de> Deserialize<'de> for ConductorRuntimeConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error as _, MapAccess, Visitor};
        use std::fmt;

        struct ConductorRuntimeConfigVisitor;

        impl<'de> Visitor<'de> for ConductorRuntimeConfigVisitor {
            type Value = ConductorRuntimeConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("conductor runtime config object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut environment = RuntimePlatformEnvConfig::default();
                let mut payload_staging = PayloadStaging::default();

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "environment" => {
                            environment = map.next_value()?;
                        }
                        // Flat form accepted for direct serde tests (S-D3):
                        // `platform_inherited_env_vars` at the top level maps
                        // into the nested `environment` sub-record.
                        "platform_inherited_env_vars" => {
                            environment.platform_inherited_env_vars = map.next_value()?;
                        }
                        "payload_staging" => {
                            payload_staging = map.next_value()?;
                        }
                        other => {
                            return Err(A::Error::unknown_field(
                                other,
                                &["environment", "payload_staging", "platform_inherited_env_vars"],
                            ));
                        }
                    }
                }

                Ok(ConductorRuntimeConfig { environment, payload_staging })
            }
        }

        deserializer.deserialize_map(ConductorRuntimeConfigVisitor)
    }
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
        /// Environment variables for the process a step spawns, stated as literal values.
        ///
        /// These are the document's own statement about a name's value, so they
        /// outrank every other source of a tool's environment: an inherited name
        /// of the same key ([`ToolRuntime::inherited_env_vars`], which copies
        /// whatever the host holds) and a platform default both give way.
        ///
        /// An empty value means "inherit the host's value". It is resolved when
        /// the document is converted for a run, so the map then carries the
        /// host's value at that moment; a name the host does not define stays
        /// empty, which is the marker the spawn site reads as "leave it unset".
        ///
        /// Names are validated on decode: an empty name, or one carrying `=` or a
        /// NUL, is refused rather than handed to the child as an ambiguity.
        #[serde(
            default,
            skip_serializing_if = "BTreeMap::is_empty",
            deserialize_with = "deserialize_env_var_map"
        )]
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
    /// Human-readable tool version, absent when the document makes no claim.
    ///
    /// Merged and compared like `kind` and `name`, so a version declared in
    /// one document reaches every merged spec. `None` is the wire shape for
    /// an omitted Nickel `| optional` field; an empty string is not
    /// representable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
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

/// What a step is allowed to override about its tool's runtime.
///
/// Retries are the only member. Impurity and the concurrency cap are properties
/// of the tool call itself, not of one invocation of it, so a document that
/// states either on a step fails to load instead of being ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepRuntime {
    /// Retries after the first failed attempt; `None` inherits the tool's value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<usize>,
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
    /// Capture specs keyed by output name.
    ///
    /// A name the tool declares takes the tool's selector and tolerance with
    /// this step's `save`; a name only this step declares takes this whole
    /// spec. See `merge_output_specs` in the step executor.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub outputs: BTreeMap<String, OutputCaptureSpec>,
    /// Runtime overrides for this step. Absent means inherit everything from the tool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<StepRuntime>,
    /// Explicit execution-order dependencies on prior step ids.
    ///
    /// Every `${step_output.<step_id>...}` reference in `inputs` must have a
    /// matching `depends_on` entry.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

/// Default inherited environment-variable names for Linux.
const LINUX_PLATFORM_INHERITED_ENV_VAR_NAMES: &[&str] = &[
    // Spawn and identity.
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TMPDIR",
    "TEMP",
    "TMP",
    // Locale and terminal.
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
    "TZ",
    "TERM",
    // Network.
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "NIX_SSL_CERT_FILE",
    "CURL_CA_BUNDLE",
    "REQUESTS_CA_BUNDLE",
    // Stores and trust.
    "XDG_CACHE_HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "XDG_RUNTIME_DIR",
    "DENO_DIR",
    "DENO_CERT",
    "DENO_TLS_CA_STORE",
    "NODE_EXTRA_CA_CERTS",
    // Fonts and ffmpeg logging.
    "FONTCONFIG_FILE",
    "FONTCONFIG_PATH",
    "AV_LOG_FORCE_NOCOLOR",
    "AV_LOG_FORCE_COLOR",
    // GPU and display, reachable through the preset's `hwaccel`.
    "LIBVA_DRIVER_NAME",
    "LIBVA_DRIVERS_PATH",
    "VK_ICD_FILENAMES",
    "VK_DRIVER_FILES",
    "CUDA_VISIBLE_DEVICES",
    "NVIDIA_VISIBLE_DEVICES",
    "NVIDIA_DRIVER_CAPABILITIES",
    "ROCR_VISIBLE_DEVICES",
    "HIP_VISIBLE_DEVICES",
    "DISPLAY",
    "WAYLAND_DISPLAY",
];

/// Default inherited environment-variable names for macOS.
///
/// The same as Linux without the GPU and display group. The same tools run
/// there through `VideoToolbox`, which takes no driver-selection variable, and
/// CUDA is not a macOS concern.
const MACOS_PLATFORM_INHERITED_ENV_VAR_NAMES: &[&str] = &[
    // Spawn and identity.
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TMPDIR",
    "TEMP",
    "TMP",
    // Locale and terminal.
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
    "TZ",
    "TERM",
    // Network.
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "NIX_SSL_CERT_FILE",
    "CURL_CA_BUNDLE",
    "REQUESTS_CA_BUNDLE",
    // Stores and trust.
    "XDG_CACHE_HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "XDG_RUNTIME_DIR",
    "DENO_DIR",
    "DENO_CERT",
    "DENO_TLS_CA_STORE",
    "NODE_EXTRA_CA_CERTS",
    // Fonts and ffmpeg logging.
    "FONTCONFIG_FILE",
    "FONTCONFIG_PATH",
    "AV_LOG_FORCE_NOCOLOR",
    "AV_LOG_FORCE_COLOR",
];

/// Default inherited environment-variable names for Windows.
const WINDOWS_PLATFORM_INHERITED_ENV_VAR_NAMES: &[&str] = &[
    // Spawn and shell.
    "PATH",
    "SYSTEMROOT",
    "WINDIR",
    "SYSTEMDRIVE",
    "COMSPEC",
    "PATHEXT",
    // Identity and home.
    "USERPROFILE",
    "USERNAME",
    "USERDOMAIN",
    "HOMEDRIVE",
    "HOMEPATH",
    "HOME",
    // Folders.
    "APPDATA",
    "LOCALAPPDATA",
    "PROGRAMDATA",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "COMMONPROGRAMFILES",
    "ALLUSERSPROFILE",
    "PUBLIC",
    // Temp and locale.
    "TEMP",
    "TMP",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TZ",
    // Network, one spelling each because Windows folds case.
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "CURL_CA_BUNDLE",
    "REQUESTS_CA_BUNDLE",
    // Stores and trust.
    "DENO_DIR",
    "DENO_CERT",
    "DENO_TLS_CA_STORE",
    "NODE_EXTRA_CA_CERTS",
    // Fonts and ffmpeg logging.
    "FONTCONFIG_FILE",
    "FONTCONFIG_PATH",
    "AV_LOG_FORCE_NOCOLOR",
    "AV_LOG_FORCE_COLOR",
    // OS facts and GPU.
    "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE",
    "OS",
    "CUDA_VISIBLE_DEVICES",
];

/// Returns the default inherited environment-variable names for `platform`.
///
/// The key is the platform name (`windows`, `linux`, or `macos`), not the
/// host. An unknown platform yields an empty list. This is the one source of
/// truth for the document's `platform_inherited_env_vars` defaults.
#[must_use]
pub fn default_platform_inherited_env_var_names(platform: &str) -> &'static [&'static str] {
    match platform {
        "windows" => WINDOWS_PLATFORM_INHERITED_ENV_VAR_NAMES,
        "linux" => LINUX_PLATFORM_INHERITED_ENV_VAR_NAMES,
        "macos" => MACOS_PLATFORM_INHERITED_ENV_VAR_NAMES,
        _ => &[],
    }
}

/// Returns the host platform key used by platform-inherited environment names.
#[must_use]
pub fn host_platform_inherited_env_var_key() -> &'static str {
    if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else {
        "linux"
    }
}

/// Returns platform-default inherited environment-variable names for this host.
///
/// The names come from [`default_platform_inherited_env_var_names`]; values are
/// left empty and resolved from the host environment when a tool runs.
#[must_use]
pub fn default_runtime_inherited_env_vars() -> BTreeMap<String, String> {
    default_platform_inherited_env_var_names(host_platform_inherited_env_var_key())
        .iter()
        .map(|&name| (name.to_string(), String::new()))
        .collect()
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

    /// Verifies default `ToolRuntime` has impure=false and three retries.
    #[test]
    fn tool_runtime_defaults() {
        let rt = ToolRuntime::default();
        assert!(!rt.impure);
        assert_eq!(rt.max_retries, DEFAULT_TOOL_MAX_RETRIES);
        assert_eq!(rt.max_concurrent_calls, None);
        assert!(rt.content_map.is_empty());
    }

    /// Verifies `default_runtime_inherited_env_vars` returns the host
    /// platform's default inherited env var names.
    #[test]
    fn default_inherited_env_vars_are_platform_appropriate() {
        let vars = default_runtime_inherited_env_vars();
        assert!(vars.contains_key("PATH"), "PATH must be present on all platforms");
        assert!(
            vars.values().all(String::is_empty),
            "names are resolved from the host at execution time, so defaults carry empty values"
        );
        if cfg!(windows) {
            assert_eq!(vars.len(), 46);
            assert!(vars.contains_key("SYSTEMROOT"));
        } else if cfg!(target_os = "macos") {
            assert_eq!(vars.len(), 40);
            assert!(vars.contains_key("HOME"));
            assert!(!vars.contains_key("DISPLAY"), "macOS defaults omit the GPU and display group");
        } else {
            assert_eq!(vars.len(), 51);
            assert!(vars.contains_key("HOME"));
            assert!(
                vars.contains_key("DISPLAY"),
                "Linux defaults include the GPU and display group"
            );
        }
    }

    /// Pins the Linux default list exactly.
    #[test]
    fn default_platform_inherited_env_var_names_linux_are_pinned() {
        let linux = default_platform_inherited_env_var_names("linux");
        assert_eq!(linux.len(), 51, "linux default list length");
        assert_eq!(
            linux,
            &[
                // Spawn and identity.
                "PATH",
                "HOME",
                "USER",
                "LOGNAME",
                "SHELL",
                "TMPDIR",
                "TEMP",
                "TMP",
                // Locale and terminal.
                "LANG",
                "LC_ALL",
                "LC_CTYPE",
                "LC_MESSAGES",
                "TZ",
                "TERM",
                // Network.
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "NO_PROXY",
                "http_proxy",
                "https_proxy",
                "all_proxy",
                "no_proxy",
                "SSL_CERT_FILE",
                "SSL_CERT_DIR",
                "NIX_SSL_CERT_FILE",
                "CURL_CA_BUNDLE",
                "REQUESTS_CA_BUNDLE",
                // Stores and trust.
                "XDG_CACHE_HOME",
                "XDG_CONFIG_HOME",
                "XDG_DATA_HOME",
                "XDG_STATE_HOME",
                "XDG_RUNTIME_DIR",
                "DENO_DIR",
                "DENO_CERT",
                "DENO_TLS_CA_STORE",
                "NODE_EXTRA_CA_CERTS",
                // Fonts and ffmpeg logging.
                "FONTCONFIG_FILE",
                "FONTCONFIG_PATH",
                "AV_LOG_FORCE_NOCOLOR",
                "AV_LOG_FORCE_COLOR",
                // GPU and display, reachable through the preset's `hwaccel`.
                "LIBVA_DRIVER_NAME",
                "LIBVA_DRIVERS_PATH",
                "VK_ICD_FILENAMES",
                "VK_DRIVER_FILES",
                "CUDA_VISIBLE_DEVICES",
                "NVIDIA_VISIBLE_DEVICES",
                "NVIDIA_DRIVER_CAPABILITIES",
                "ROCR_VISIBLE_DEVICES",
                "HIP_VISIBLE_DEVICES",
                "DISPLAY",
                "WAYLAND_DISPLAY",
            ]
        );
    }

    /// Pins the macOS default list and its relationship to Linux.
    #[test]
    fn default_platform_inherited_env_var_names_macos_are_pinned() {
        let linux = default_platform_inherited_env_var_names("linux");
        let macos = default_platform_inherited_env_var_names("macos");
        assert_eq!(macos.len(), 40, "macos default list length");
        assert_eq!(
            macos,
            &[
                // Spawn and identity.
                "PATH",
                "HOME",
                "USER",
                "LOGNAME",
                "SHELL",
                "TMPDIR",
                "TEMP",
                "TMP",
                // Locale and terminal.
                "LANG",
                "LC_ALL",
                "LC_CTYPE",
                "LC_MESSAGES",
                "TZ",
                "TERM",
                // Network.
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "NO_PROXY",
                "http_proxy",
                "https_proxy",
                "all_proxy",
                "no_proxy",
                "SSL_CERT_FILE",
                "SSL_CERT_DIR",
                "NIX_SSL_CERT_FILE",
                "CURL_CA_BUNDLE",
                "REQUESTS_CA_BUNDLE",
                // Stores and trust.
                "XDG_CACHE_HOME",
                "XDG_CONFIG_HOME",
                "XDG_DATA_HOME",
                "XDG_STATE_HOME",
                "XDG_RUNTIME_DIR",
                "DENO_DIR",
                "DENO_CERT",
                "DENO_TLS_CA_STORE",
                "NODE_EXTRA_CA_CERTS",
                // Fonts and ffmpeg logging.
                "FONTCONFIG_FILE",
                "FONTCONFIG_PATH",
                "AV_LOG_FORCE_NOCOLOR",
                "AV_LOG_FORCE_COLOR",
            ]
        );
        assert_eq!(
            &linux[..40],
            macos,
            "macOS is the Linux list without the GPU and display group"
        );
        assert_eq!(
            &linux[40..],
            &[
                "LIBVA_DRIVER_NAME",
                "LIBVA_DRIVERS_PATH",
                "VK_ICD_FILENAMES",
                "VK_DRIVER_FILES",
                "CUDA_VISIBLE_DEVICES",
                "NVIDIA_VISIBLE_DEVICES",
                "NVIDIA_DRIVER_CAPABILITIES",
                "ROCR_VISIBLE_DEVICES",
                "HIP_VISIBLE_DEVICES",
                "DISPLAY",
                "WAYLAND_DISPLAY",
            ],
            "Linux differs from macOS only by the GPU and display group"
        );
    }

    /// Pins the Windows default list and its case-folding invariants.
    #[test]
    fn default_platform_inherited_env_var_names_windows_are_pinned() {
        let windows = default_platform_inherited_env_var_names("windows");
        assert_eq!(windows.len(), 46, "windows default list length");
        assert_eq!(
            windows,
            &[
                // Spawn and shell.
                "PATH",
                "SYSTEMROOT",
                "WINDIR",
                "SYSTEMDRIVE",
                "COMSPEC",
                "PATHEXT",
                // Identity and home.
                "USERPROFILE",
                "USERNAME",
                "USERDOMAIN",
                "HOMEDRIVE",
                "HOMEPATH",
                "HOME",
                // Folders.
                "APPDATA",
                "LOCALAPPDATA",
                "PROGRAMDATA",
                "PROGRAMFILES",
                "PROGRAMFILES(X86)",
                "COMMONPROGRAMFILES",
                "ALLUSERSPROFILE",
                "PUBLIC",
                // Temp and locale.
                "TEMP",
                "TMP",
                "LANG",
                "LC_ALL",
                "LC_CTYPE",
                "TZ",
                // Network, one spelling each because Windows folds case.
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "NO_PROXY",
                "SSL_CERT_FILE",
                "SSL_CERT_DIR",
                "CURL_CA_BUNDLE",
                "REQUESTS_CA_BUNDLE",
                // Stores and trust.
                "DENO_DIR",
                "DENO_CERT",
                "DENO_TLS_CA_STORE",
                "NODE_EXTRA_CA_CERTS",
                // Fonts and ffmpeg logging.
                "FONTCONFIG_FILE",
                "FONTCONFIG_PATH",
                "AV_LOG_FORCE_NOCOLOR",
                "AV_LOG_FORCE_COLOR",
                // OS facts and GPU.
                "NUMBER_OF_PROCESSORS",
                "PROCESSOR_ARCHITECTURE",
                "OS",
                "CUDA_VISIBLE_DEVICES",
            ]
        );

        assert!(
            default_platform_inherited_env_var_names("unknown").is_empty(),
            "an unknown platform yields an empty list"
        );

        // Windows folds case, so a name that duplicates another across case
        // would be read twice and the fold would silently drop one value.
        let mut folded: Vec<String> =
            windows.iter().map(|name| name.to_ascii_lowercase()).collect();
        let before = folded.len();
        folded.sort();
        folded.dedup();
        assert_eq!(folded.len(), before, "windows list duplicates a name across case");
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

    impl proptest::arbitrary::Arbitrary for CaptureKind {
        type Parameters = ();
        type Strategy = proptest::strategy::BoxedStrategy<Self>;

        fn arbitrary_with((): ()) -> Self::Strategy {
            use proptest::prelude::*;
            let regex_remainder = "[a-zA-Z0-9_./:-]{1,24}";
            let path_segment = "[a-zA-Z0-9_-]{1,8}";
            let safe_path = (
                proptest::bool::ANY,
                proptest::collection::vec(path_segment, 1..4),
                proptest::bool::ANY,
            )
                .prop_map(|(leading_dot, segments, trailing_slash)| {
                    let mut path = if leading_dot { "./".to_string() } else { String::new() };
                    path.push_str(&segments.join("/"));
                    if trailing_slash {
                        path.push('/');
                    }
                    path
                });
            prop_oneof![
                Just(CaptureKind::Stdout),
                Just(CaptureKind::Stderr),
                Just(CaptureKind::ProcessCode),
                safe_path.clone().prop_map(CaptureKind::File),
                regex_remainder.prop_map(CaptureKind::FileRegex),
                safe_path.clone().prop_map(CaptureKind::Folder),
                safe_path.prop_map(CaptureKind::FolderFlat),
                regex_remainder.prop_map(CaptureKind::FolderRegex),
                regex_remainder.prop_map(CaptureKind::FolderFlatRegex),
            ]
            .boxed()
        }
    }

    impl proptest::arbitrary::Arbitrary for OutputCaptureSpec {
        type Parameters = ();
        type Strategy = proptest::strategy::BoxedStrategy<Self>;

        fn arbitrary_with((): ()) -> Self::Strategy {
            use proptest::strategy::Strategy;
            (any::<String>(), any::<CaptureKind>(), any::<SaveMode>(), any::<bool>())
                .prop_map(|(name, capture, save, allow_empty)| OutputCaptureSpec {
                    name,
                    capture,
                    save,
                    allow_empty,
                })
                .boxed()
        }
    }

    // ── OutputCaptureSpec serde ───────────────────────────────────────────

    #[test]
    fn output_capture_spec_full_roundtrip() {
        let spec = OutputCaptureSpec {
            name: "output1".to_string(),
            capture: CaptureKind::Stdout,
            save: SaveMode::Full,
            allow_empty: true,
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
            capture: CaptureKind::Stdout,
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
            capture: CaptureKind::Stdout,
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
            capture: CaptureKind::Stdout,
            save: SaveMode::Full,
            ..Default::default()
        };
        let json = serde_json::to_string(&spec).expect("serialize");
        assert!(json.contains("\"save\""), "save=Full should be present: {json}");
    }

    // ── ConductorRuntimeConfig (resolved, no-Option) ─────────────────────

    /// Documents that the resolved `ConductorRuntimeConfig` carries plain
    /// values (not `Option`): the platform env-var lists are the document
    /// defaults and the payload staging default is `in_place`.
    #[test]
    fn conductor_runtime_config_default_has_platform_default_inherited_env_vars() {
        let cfg = ConductorRuntimeConfig::default();
        assert_eq!(cfg.payload_staging, PayloadStaging::InPlace);
        assert_eq!(cfg.environment.platform_inherited_env_vars.windows.len(), 46);
        assert_eq!(cfg.environment.platform_inherited_env_vars.linux.len(), 51);
        assert_eq!(cfg.environment.platform_inherited_env_vars.macos.len(), 40);
        assert!(
            !cfg.environment.platform_inherited_env_vars.is_empty(),
            "default platform lists are stated, so the struct is not empty"
        );
    }

    /// Absent `platform_inherited_env_vars` resolves to the platform defaults,
    /// a stated list wins, and an explicitly empty array yields an empty
    /// environment for that platform.
    #[test]
    fn platform_inherited_env_vars_absent_uses_defaults_and_explicit_empty_wins() {
        let absent: RuntimePlatformEnvConfig =
            serde_json::from_str("{}").expect("absent platform_inherited_env_vars");
        assert_eq!(absent.platform_inherited_env_vars.windows.len(), 46);
        assert_eq!(absent.platform_inherited_env_vars.linux.len(), 51);
        assert_eq!(absent.platform_inherited_env_vars.macos.len(), 40);

        let stated: RuntimePlatformEnvConfig = serde_json::from_str(
            r#"{"platform_inherited_env_vars":{"linux":["PATH"],"macos":[]}}"#,
        )
        .expect("stated platform_inherited_env_vars");
        assert_eq!(stated.platform_inherited_env_vars.linux, vec!["PATH".to_string()]);
        assert!(
            stated.platform_inherited_env_vars.macos.is_empty(),
            "an explicitly empty array yields an empty environment for that platform"
        );
        assert_eq!(
            stated.platform_inherited_env_vars.windows.len(),
            46,
            "an absent platform field inside a stated block resolves to that platform's defaults"
        );
    }

    // ── CaptureKind (T1) ──────────────────────────────────────────────────

    #[test]
    fn capture_kind_roundtrips_all_eight_tokens() {
        let cases = [
            ("stdout", CaptureKind::Stdout),
            ("stderr", CaptureKind::Stderr),
            ("process_code", CaptureKind::ProcessCode),
            ("file:out.mkv", CaptureKind::File("out.mkv".to_string())),
            (
                "file_regex:^downloads/.+[.]json$",
                CaptureKind::FileRegex("^downloads/.+[.]json$".to_string()),
            ),
            ("folder:downloads", CaptureKind::Folder("downloads".to_string())),
            ("folder_flat:downloads", CaptureKind::FolderFlat("downloads".to_string())),
            ("folder_regex:^downloads/.+$", CaptureKind::FolderRegex("^downloads/.+$".to_string())),
            (
                "folder_flat_regex:^downloads/.+$",
                CaptureKind::FolderFlatRegex("^downloads/.+$".to_string()),
            ),
        ];
        for (token, expected) in cases {
            let parsed = CaptureKind::parse_token(token).expect(token);
            assert_eq!(parsed, expected, "parse mismatch for {token}");
            assert_eq!(parsed.to_token(), token, "render mismatch for {token}");
            let json = serde_json::to_string(&OutputCaptureSpec {
                name: "x".to_string(),
                capture: parsed.clone(),
                ..OutputCaptureSpec::default()
            })
            .expect("serialize");
            assert!(
                json.contains(&format!("\"capture\":\"{}\"", token.replace('\\', "\\\\"))),
                "{json}"
            );
        }
    }

    #[test]
    fn capture_kind_preserves_remainder_after_first_colon() {
        let token = "folder_flat_regex:^foo:bar:(.+)$";
        let parsed = CaptureKind::parse_token(token).expect("parse");
        assert_eq!(parsed, CaptureKind::FolderFlatRegex("^foo:bar:(.+)$".to_string()));
        assert_eq!(parsed.to_token(), token);
    }

    #[test]
    fn capture_kind_rejects_unknown_prefix_naming_token() {
        let err = CaptureKind::parse_token("bogus:path").expect_err("must fail");
        assert!(err.contains("unknown capture token"), "{err}");
        assert!(err.contains("bogus:path"), "{err}");
    }

    #[test]
    fn capture_kind_rejects_unknown_bare_token_naming_token() {
        let err = CaptureKind::parse_token("nope").expect_err("must fail");
        assert!(err.contains("unknown capture token"), "{err}");
        assert!(err.contains("nope"), "{err}");
    }

    #[test]
    fn capture_kind_rejects_empty_remainder_naming_token() {
        for token in [
            "file:",
            "file_regex:",
            "folder:",
            "folder_flat:",
            "folder_regex:",
            "folder_flat_regex:",
        ] {
            let err = CaptureKind::parse_token(token).expect_err(token);
            assert!(err.contains("empty remainder"), "{err}: {token}");
            assert!(err.contains(token), "{err}: {token}");
        }
    }

    #[test]
    fn output_capture_spec_rejects_unknown_capture_token() {
        let err =
            serde_json::from_str::<OutputCaptureSpec>(r#"{"name":"x","capture":"wat:thing"}"#)
                .expect_err("must fail");
        let msg = err.to_string();
        assert!(msg.contains("unknown capture token"), "{msg}");
        assert!(msg.contains("wat:thing"), "{msg}");
    }

    #[test]
    fn capture_kind_refuses_escaping_paths_for_file_folder_and_folder_flat() {
        let cases = [
            ("/abs/path", "must not be an absolute path"),
            ("..", "must not contain a '..' component"),
            ("a/../b", "must not contain a '..' component"),
            ("C:/drive", "must not contain a drive or UNC prefix"),
            ("C:drive", "must not contain a drive or UNC prefix"),
            ("back\\slash", "must not contain a backslash separator"),
        ];
        for (path, expected_rule) in cases {
            for prefix in ["file", "folder", "folder_flat"] {
                let token = format!("{prefix}:{path}");
                let err = CaptureKind::parse_token(&token)
                    .expect_err(&format!("token '{token}' must be refused"));
                assert!(
                    err.contains(&format!("capture token '{token}'")),
                    "error must name token '{token}': {err}"
                );
                assert!(err.contains(expected_rule), "expected rule '{expected_rule}' in: {err}");
            }
        }
    }

    #[test]
    fn capture_kind_accepts_nested_relative_paths_for_file_folder_and_folder_flat() {
        for prefix in ["file", "folder", "folder_flat"] {
            for path in ["nested/path/file.txt", "./nested/path", "nested/path/"] {
                let token = format!("{prefix}:{path}");
                let parsed = CaptureKind::parse_token(&token)
                    .unwrap_or_else(|err| panic!("token '{token}' must be accepted: {err}"));
                assert_eq!(parsed.remainder(), Some(path), "remainder for {token}");
            }
        }
    }
}
