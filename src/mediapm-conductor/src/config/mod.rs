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
/// - `Full`: as `True`, and the object is marked keep-full in the CAS, so the
///   maintenance optimizer never stores it as a delta against another blob. It
///   may still serve as another object's delta base.
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
                match prefix {
                    "file" => Ok(Self::File(remainder.to_string())),
                    "file_regex" => Ok(Self::FileRegex(remainder.to_string())),
                    "folder" => Ok(Self::Folder(remainder.to_string())),
                    "folder_flat" => Ok(Self::FolderFlat(remainder.to_string())),
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

    /// Returns whether a folder-style capture includes the topmost folder name
    /// in archived member paths.
    ///
    /// This is a compatibility helper for code paths that still reason about the
    /// legacy boolean layout flag; the flag is encoded in the capture kind.
    #[must_use]
    pub const fn includes_topmost_folder(&self) -> bool {
        match self {
            Self::Folder(_) | Self::FolderRegex(_) => true,
            Self::FolderFlat(_) | Self::FolderFlatRegex(_) => false,
            Self::Stdout
            | Self::Stderr
            | Self::ProcessCode
            | Self::File(_)
            | Self::FileRegex(_) => true,
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
    /// that instance is superseded. `Full` marks the object keep-full, so the
    /// optimizer never stores it as a delta against another blob.
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
    /// Impurity gates retries: an impure step retries only when the run sets
    /// `retry_impure`, because repeating a side effect is not free the way
    /// recomputing pure output is. It never affects caching — instance keys are
    /// content-derived, so an impure step with unchanged material is served
    /// from the instance cache like any other, and re-executing it takes
    /// changed material or an explicit forced re-run.
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
    /// `0` means unlimited.
    #[serde(default)]
    pub max_concurrent_calls: usize,
    /// Maximum retry attempts for this tool after the first failed attempt.
    #[serde(default)]
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
    #[serde(default, skip_serializing_if = "PlatformInheritedEnvVars::is_empty")]
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
    /// Whether impure tool calls may be retried automatically.
    ///
    /// Absent in config resolves to `false` at the boundary.
    #[serde(default)]
    pub retry_impure: bool,
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
                let mut retry_impure = false;
                let mut seen_retry_impure = false;
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
                        "retry_impure" => {
                            retry_impure = map.next_value()?;
                            seen_retry_impure = true;
                        }
                        "payload_staging" => {
                            payload_staging = map.next_value()?;
                        }
                        other => {
                            return Err(A::Error::unknown_field(
                                other,
                                &[
                                    "environment",
                                    "payload_staging",
                                    "platform_inherited_env_vars",
                                    "retry_impure",
                                ],
                            ));
                        }
                    }
                }

                let _ = seen_retry_impure;
                Ok(ConductorRuntimeConfig { environment, retry_impure, payload_staging })
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
            use proptest::strategy::Strategy;
            let nonempty = "[a-zA-Z0-9_./:-]{1,24}";
            prop_oneof![
                Just(CaptureKind::Stdout),
                Just(CaptureKind::Stderr),
                Just(CaptureKind::ProcessCode),
                nonempty.prop_map(CaptureKind::File),
                nonempty.prop_map(CaptureKind::FileRegex),
                nonempty.prop_map(CaptureKind::Folder),
                nonempty.prop_map(CaptureKind::FolderFlat),
                nonempty.prop_map(CaptureKind::FolderRegex),
                nonempty.prop_map(CaptureKind::FolderFlatRegex),
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
    /// values (not `Option`): defaults are `retry_impure = false` and all
    /// platform env-var lists empty.
    #[test]
    fn conductor_runtime_config_default_has_no_option_fields() {
        let cfg = ConductorRuntimeConfig::default();
        assert!(!cfg.retry_impure);
        assert_eq!(
            cfg.environment.platform_inherited_env_vars.windows.len(),
            0,
            "windows inherits no env var by default"
        );
        assert_eq!(
            cfg.environment.platform_inherited_env_vars.linux.len(),
            0,
            "linux inherits no env var by default"
        );
        assert_eq!(
            cfg.environment.platform_inherited_env_vars.macos.len(),
            0,
            "macos inherits no env var by default"
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
}
