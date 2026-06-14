//! Public API contracts for the conductor crate.

use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash as _, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use mediapm_cas::{CasIntegrityConfig, FileSystemCas, Hash};
use serde::{Deserialize, Serialize};

use crate::error::ConductorError;
use crate::model::config::{MachineNickelDocument, ToolKindSpec};
use crate::model::state::{
    OrchestrationState, decode_state_from_slice, persisted_state_json_pretty,
};
#[cfg(feature = "tool-presets")]
pub use crate::tools::{
    CommonExecutablePayload, CommonExecutableTool, fetch_common_executable_tool_payload,
};

/// Default runtime storage directory name under one config-root anchor.
const DEFAULT_CONDUCTOR_DIR_NAME: &str = ".conductor";

/// Default volatile state file name under the resolved conductor directory.
const DEFAULT_STATE_FILE_NAME: &str = "state.ncl";

/// Default filesystem CAS store directory name under the resolved conductor directory.
const DEFAULT_CAS_STORE_DIR_NAME: &str = "store";

/// Default tool-content cache directory name under the resolved conductor directory.
const DEFAULT_TOOLS_DIR_NAME: &str = "tools";

/// Default schema export directory under one resolved runtime root.
const DEFAULT_SCHEMA_EXPORT_DIR_NAME: &str = "conductor";

/// Default schema export parent folder under one resolved runtime root.
const DEFAULT_SCHEMA_EXPORT_PARENT_DIR_NAME: &str = "config";

/// Grouped runtime storage-path configuration for one conductor invocation.
///
/// This keeps all runtime-managed filesystem paths in one place:
/// - `conductor_dir` anchors runtime-owned artifacts,
/// - `conductor_state_config` is the volatile state document path,
/// - `cas_store_dir` is the filesystem CAS root,
/// - `conductor_tmp_dir` is the temporary execution sandbox root,
/// - `conductor_schema_dir` is the schema export directory,
/// - `conductor_tools_dir` is the tool-content cache root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStoragePaths {
    /// Root folder for runtime-owned artifacts.
    ///
    /// Default: `.conductor`.
    pub conductor_dir: PathBuf,
    /// Volatile state document path.
    ///
    /// Default: `<conductor_dir>/state.ncl`.
    pub conductor_state_config: PathBuf,
    /// Filesystem CAS store root used by command-line defaults.
    ///
    /// Default: `<conductor_dir>/store`.
    pub cas_store_dir: PathBuf,
    /// Temporary execution sandbox root (OS-backed with per-conductor-dir hash
    /// path: `<os-temp>/mediapm-conductor-<conductor-dir-hash>`).
    pub conductor_tmp_dir: PathBuf,
    /// Schema export directory path.
    ///
    /// Default: `<conductor_dir>/config/conductor`.
    pub conductor_schema_dir: PathBuf,
    /// Tool-content cache root path.
    ///
    /// The tool-content cache stores one ready-to-execute payload directory per
    /// tool id.  Entries are keyed on the full `content_map` and expire after
    /// 24 hours of non-use.
    ///
    /// Default: `<conductor_dir>/tools`.
    pub conductor_tools_dir: PathBuf,
}

impl RuntimeStoragePaths {
    /// Returns grouped runtime-storage defaults rooted under `.conductor`.
    #[must_use]
    pub fn new() -> Self {
        Self::default_for(&PathBuf::from(DEFAULT_CONDUCTOR_DIR_NAME))
    }

    /// Returns grouped runtime-storage defaults rooted under the given directory.
    #[must_use]
    pub fn default_for(conductor_dir: &Path) -> Self {
        let key = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            conductor_dir.hash(&mut hasher);
            format!("{:016x}", hasher.finish())
        };
        Self {
            conductor_dir: conductor_dir.to_path_buf(),
            conductor_state_config: conductor_dir.join(DEFAULT_STATE_FILE_NAME),
            cas_store_dir: conductor_dir.join(DEFAULT_CAS_STORE_DIR_NAME),
            conductor_tmp_dir: std::env::temp_dir().join(format!("mediapm-conductor-{key}")),
            conductor_schema_dir: schema_export_dir(conductor_dir),
            conductor_tools_dir: conductor_dir.join(DEFAULT_TOOLS_DIR_NAME),
        }
    }

    /// Resolves all runtime storage paths for a specific user/machine config
    /// location pair.
    ///
    /// The resolved `conductor_dir` is used as the anchor for resolving
    /// default-valued fields; explicitly overridden fields are resolved
    /// against the user config parent (or machine config parent as fallback).
    #[must_use]
    pub fn resolve_for(&self, user_ncl: &Path, machine_ncl: &Path) -> Self {
        let anchor = user_ncl.parent().or_else(|| machine_ncl.parent()).unwrap_or(Path::new("."));
        let conductor_dir = Self::resolve_path(anchor, &self.conductor_dir);

        // Start with defaults computed from the resolved conductor_dir.
        let mut result = Self::default_for(&conductor_dir);
        result.conductor_dir = conductor_dir;

        // Detect explicit overrides: a field that differs from the default
        // computed from the (unresolved) conductor_dir is an override and
        // should be resolved against the config-parent anchor.
        let base = Self::default_for(&self.conductor_dir);

        if self.conductor_state_config != base.conductor_state_config {
            result.conductor_state_config =
                Self::resolve_path(anchor, &self.conductor_state_config);
        }
        if self.cas_store_dir != base.cas_store_dir {
            result.cas_store_dir = Self::resolve_path(anchor, &self.cas_store_dir);
        }
        if self.conductor_schema_dir != base.conductor_schema_dir {
            result.conductor_schema_dir = Self::resolve_path(anchor, &self.conductor_schema_dir);
        }
        if self.conductor_tools_dir != base.conductor_tools_dir {
            result.conductor_tools_dir = Self::resolve_path(anchor, &self.conductor_tools_dir);
        }
        // `conductor_tmp_dir` is computed from the resolved conductor_dir
        // hash — the `default_for` value above already uses the correct
        // resolved conductor_dir.

        result
    }

    /// Resolves one candidate path against the provided anchor.
    #[must_use]
    fn resolve_path(anchor: &Path, path: &Path) -> PathBuf {
        if path.is_absolute() { path.to_path_buf() } else { anchor.join(path) }
    }
}

impl Default for RuntimeStoragePaths {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolved managed executable path prepared by conductor tool-cache logic.
///
/// The returned executable is always rooted under the conductor-owned
/// tool-content cache and is ready for host execution on the active platform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedToolExecutableResolution {
    /// Immutable tool id selected from machine configuration.
    pub tool_id: String,
    /// Absolute host path to the prepared executable payload.
    pub executable_path: PathBuf,
}

/// Resolves one managed executable from machine config using conductor-owned
/// tool-cache preparation and filesystem CAS.
///
/// This helper is the cache-authoritative path for launching managed tools:
/// it validates selector/config state, prepares the persistent payload cache
/// from `tool_configs.<tool>.content_map`, resolves the host command selector,
/// and returns the final executable path under `<conductor_tools_dir>`.
///
/// # Errors
///
/// Returns [`ConductorError`] when selector resolution fails, machine config is
/// invalid, tool content mapping is missing, CAS access fails, cache
/// preparation fails, or the resolved executable path is absent.
pub async fn resolve_managed_tool_executable_with_filesystem_cas(
    machine_ncl: &Path,
    cas_store_dir: &Path,
    conductor_tools_dir: &Path,
    selector: &str,
) -> Result<ManagedToolExecutableResolution, ConductorError> {
    let machine = load_machine_document(machine_ncl)?;
    let tool_id = resolve_managed_tool_id(&machine, selector)?;
    let tool_spec = machine.tools.get(&tool_id).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "managed tool '{tool_id}' is missing from conductor machine config"
        ))
    })?;
    let command_selector = match &tool_spec.kind {
        ToolKindSpec::Executable { command, .. } => {
            command.first().map(String::as_str).ok_or_else(|| {
                ConductorError::Workflow(format!(
                    "managed tool '{tool_id}' has no executable command configured"
                ))
            })?
        }
        ToolKindSpec::Builtin { .. } => {
            return Err(ConductorError::Workflow(format!(
                "tool selector '{selector}' resolved to builtin tool '{tool_id}', which has no managed executable binary"
            )));
        }
    };
    let content_map = machine
        .tool_configs
        .get(&tool_id)
        .and_then(|config| config.content_map.as_ref())
        .filter(|map| !map.is_empty())
        .ok_or_else(|| {
            ConductorError::Workflow(format!(
                "managed tool '{tool_id}' has no tool_configs content_map; run sync/import first"
            ))
        })?;

    let cas = Arc::new(FileSystemCas::open(cas_store_dir).await.map_err(|source| {
        ConductorError::Workflow(format!(
            "opening conductor CAS store '{}' for managed tool resolution failed: {source}",
            cas_store_dir.display()
        ))
    })?);
    let tool_cache =
        crate::tool_cache::ToolContentCache::new(conductor_tools_dir.to_path_buf(), cas, None);
    let cache_entry = tool_cache.materialize(&tool_id, content_map).await?;
    let payload_dir = cache_entry.payload_dir().to_path_buf();

    let host_relative = resolve_host_command_selector_path(command_selector)?
        .ok_or_else(|| {
            ConductorError::Workflow(format!(
                "managed tool '{tool_id}' command selector '{command_selector}' does not resolve to a host executable path for os '{}'",
                std::env::consts::OS
            ))
        })?;
    let relative_path =
        normalize_managed_tool_relative_command_path(&host_relative).ok_or_else(
            || {
                ConductorError::Workflow(format!(
                    "managed tool '{tool_id}' command selector '{command_selector}' resolved to an invalid relative path"
                ))
            },
        )?;
    let executable_path = payload_dir.join(relative_path);
    if !executable_path.is_file() {
        return Err(ConductorError::Workflow(format!(
            "managed tool '{tool_id}' executable is missing at '{}' after cache preparation",
            executable_path.display()
        )));
    }

    Ok(ManagedToolExecutableResolution { tool_id, executable_path })
}

/// Loads one machine document for managed executable resolution.
fn load_machine_document(machine_ncl: &Path) -> Result<MachineNickelDocument, ConductorError> {
    if !machine_ncl.exists() {
        return Ok(MachineNickelDocument::default());
    }

    let bytes = fs::read(machine_ncl).map_err(|source| ConductorError::Io {
        operation: "reading conductor machine configuration for managed tool resolution"
            .to_string(),
        path: machine_ncl.to_path_buf(),
        source,
    })?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(MachineNickelDocument::default());
    }

    crate::model::config::decode_machine_document(&bytes)
}

/// Resolves one immutable managed tool id from selector text.
fn resolve_managed_tool_id(
    machine: &MachineNickelDocument,
    selector: &str,
) -> Result<String, ConductorError> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Err(ConductorError::Workflow(
            "managed tool selector must be non-empty".to_string(),
        ));
    }

    if let Some(exact) = machine.tools.keys().find(|tool_id| tool_id.eq_ignore_ascii_case(selector))
    {
        return Ok(exact.clone());
    }

    let mut matches = machine
        .tools
        .keys()
        .filter(|tool_id| logical_name_matches_tool_id(tool_id, selector))
        .cloned()
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();

    match matches.as_slice() {
        [only] => Ok(only.clone()),
        [] => Err(ConductorError::Workflow(format!(
            "tool selector '{selector}' did not match any managed tool id in conductor machine config"
        ))),
        _ => {
            // When multiple tools match one logical name, prefer the one
            // with a non-empty content_map (the active tool). Generated
            // configs have at most one such tool per logical name.
            let with_content = matches
                .iter()
                .filter(|id| {
                    machine
                        .tool_configs
                        .get(*id)
                        .and_then(|c| c.content_map.as_ref())
                        .is_some_and(|m| !m.is_empty())
                })
                .collect::<Vec<_>>();
            if with_content.len() == 1 {
                return Ok(with_content[0].clone());
            }
            Err(ConductorError::Workflow(format!(
                "tool selector '{selector}' matched multiple managed tool ids ({}) and the content_map tiebreaker could not resolve; pass --tool <immutable-id>",
                matches.join(", ")
            )))
        }
    }
}

/// Returns true when immutable tool id belongs to one logical tool name.
fn logical_name_matches_tool_id(tool_id: &str, logical_name: &str) -> bool {
    if tool_id.eq_ignore_ascii_case(logical_name) {
        return true;
    }

    let Some((prefix, _)) = tool_id.split_once('@') else {
        return false;
    };

    let marker = "mediapm.tools.";
    let canonical_prefix =
        if prefix.len() >= marker.len() && prefix[..marker.len()].eq_ignore_ascii_case(marker) {
            &prefix[marker.len()..]
        } else {
            prefix
        };
    let canonical_name =
        canonical_prefix.split_once('+').map_or(canonical_prefix, |(name, _)| name);

    canonical_name.trim().eq_ignore_ascii_case(logical_name)
}

/// Resolves one host command selector path for the active platform.
fn resolve_host_command_selector_path(
    command_selector: &str,
) -> Result<Option<String>, ConductorError> {
    if command_selector.contains("context.os") {
        let selectors = extract_platform_conditional_paths(command_selector)?;
        return Ok(selectors.get(std::env::consts::OS).cloned());
    }

    let trimmed = command_selector.trim();
    if trimmed.is_empty() { Ok(None) } else { Ok(Some(trimmed.to_string())) }
}

/// Parses `${context.os == "<target>" ? <path> | <fallback>}` selectors.
fn extract_platform_conditional_paths(
    template: &str,
) -> Result<BTreeMap<String, String>, ConductorError> {
    let mut result = BTreeMap::new();
    let mut cursor = 0usize;

    while let Some(start_rel) = template[cursor..].find("${") {
        let start = cursor + start_rel;
        let remainder = &template[start + 2..];
        let Some(end_rel) = remainder.find('}') else {
            return Err(ConductorError::Workflow(format!(
                "invalid command selector '{template}': missing closing '}}'"
            )));
        };
        let token = &remainder[..end_rel];

        if let Some((target, value)) = parse_platform_conditional_path_token(token)? {
            result.insert(target, value);
        }

        cursor = start + 2 + end_rel + 1;
    }

    if result.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool command '{template}' did not contain any context.os selectors"
        )));
    }

    Ok(result)
}

/// Parses one `${...}` token into a platform target/path selector.
fn parse_platform_conditional_path_token(
    token: &str,
) -> Result<Option<(String, String)>, ConductorError> {
    if !token.contains("context.os") {
        return Ok(None);
    }

    let Some((condition, branches)) = token.split_once('?') else {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; expected '?<true>|<false>'"
        )));
    };
    let Some((true_branch, _false_branch)) = branches.split_once('|') else {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; expected '<true>|<false>'"
        )));
    };

    let condition = condition.trim();
    let Some(remainder) = condition.strip_prefix("context.os") else {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; condition must start with 'context.os'"
        )));
    };
    let remainder = remainder.trim_start();
    let Some(remainder) = remainder.strip_prefix("==") else {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; condition must use '=='"
        )));
    };
    let target = parse_quoted_selector_value(remainder.trim()).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; target must be quoted"
        ))
    })?;

    let true_branch = true_branch.trim();
    let path = if let Some(decoded) = parse_quoted_selector_value(true_branch) {
        decoded
    } else {
        true_branch.to_string()
    };
    if path.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; true branch path is empty"
        )));
    }

    Ok(Some((target, path)))
}

/// Parses one single- or double-quoted selector value.
fn parse_quoted_selector_value(value: &str) -> Option<String> {
    if value.len() < 2 {
        return None;
    }
    let first = value.chars().next()?;
    let last = value.chars().last()?;
    if !((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
        return None;
    }

    Some(value[first.len_utf8()..value.len() - last.len_utf8()].to_string())
}

/// Normalizes one managed-tool relative command path for payload lookup.
fn normalize_managed_tool_relative_command_path(relative_command_path: &str) -> Option<String> {
    let normalized = relative_command_path
        .trim()
        .replace('\\', "/")
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string();

    if normalized.is_empty() {
        return None;
    }

    let path = Path::new(&normalized);
    if path.components().any(|component| matches!(component, std::path::Component::ParentDir)) {
        return None;
    }
    if path.is_absolute() {
        return None;
    }

    Some(
        Path::new(&normalized)
            .components()
            .map(|component| component.as_os_str().to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join("/"),
    )
}

/// Summary of one workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSummary {
    /// Number of instances that were executed or re-materialized.
    pub executed_instances: usize,
    /// Number of instances served fully from cache.
    pub cached_instances: usize,
    /// Number of executed instances that were re-materialized because outputs were absent.
    pub rematerialized_instances: usize,
}

impl RunSummary {
    /// Creates an empty run summary.
    #[must_use]
    pub const fn new() -> Self {
        Self { executed_instances: 0, cached_instances: 0, rematerialized_instances: 0 }
    }
}

impl Default for RunSummary {
    fn default() -> Self {
        Self::new()
    }
}

/// One step completion event sent from conductor to mediapm for progress reporting.
#[derive(Debug, Clone)]
pub struct WorkflowStepEvent {
    /// Total number of steps across all workflows.
    pub total_steps: usize,
    /// Number of steps completed so far.
    pub completed_steps: usize,
    /// The workflow name for this event.
    pub workflow_name: String,
    /// The step id for this event.
    pub step_id: String,
    /// Human-readable display name for the workflow being executed.
    pub workflow_display_name: String,
    /// Whether this step was freshly executed or served from cache.
    pub executed: bool,
    /// Index of the worker that executed this step.
    pub worker_index: usize,
    /// Total number of workers in the pool.
    pub worker_count: usize,
}

/// Sender type for workflow step progress events.
pub type WorkflowProgressSender = tokio::sync::mpsc::UnboundedSender<WorkflowStepEvent>;

/// Runtime options controlling one `run_workflow` invocation.
#[derive(Debug, Clone)]
pub struct RunWorkflowOptions {
    /// Allows user-side tool definitions to override previously locked machine
    /// definitions when the same immutable tool name is redefined.
    ///
    /// When `false` (default), any redefinition mismatch fails fast.
    pub allow_tool_redefinition: bool,
    /// Grouped runtime storage paths used by this invocation.
    ///
    /// Defaults:
    /// - `conductor_dir = .conductor`
    /// - `conductor_state_config = <conductor_dir>/state.ncl`
    /// - `cas_store_dir = <conductor_dir>/store`
    /// - `conductor_tmp_dir = <os-temp>/mediapm-conductor-<conductor-dir-hash>`
    /// - `conductor_schema_dir = <conductor_dir>/config/conductor`
    pub runtime_storage_paths: RuntimeStoragePaths,
    /// Additional host environment variable names inherited into executable
    /// runtime process environments.
    ///
    /// This list is merged with runtime document defaults and host-specific
    /// baseline names (for example `SYSTEMROOT`/`WINDIR` on Windows).
    pub runtime_inherited_env_vars: Vec<String>,
    /// Optional JSON profile artifact output path for this run.
    ///
    /// When set, conductor writes one structured runtime profile report after
    /// successful workflow execution and state persistence.
    ///
    /// Takes precedence over [`profiler_enabled`](Self::profiler_enabled). The
    /// `MEDIAPM_CONDUCTOR_PROFILE_JSON` environment variable is also consulted
    /// as a fallback when neither this field nor the environment variable are
    /// set but `profiler_enabled` is `true`.
    pub profile_output_path: Option<PathBuf>,
    /// Enables conductor workflow profiling when no explicit
    /// [`profile_output_path`](Self::profile_output_path) is provided.
    ///
    /// When `true` and `profile_output_path` is `None`, conductor automatically
    /// resolves the profile output path to `<conductor_dir>/profile.json`.
    /// The environment variable `MEDIAPM_CONDUCTOR_PROFILE_JSON` is still
    /// consulted first as an override before this auto-path fires.
    ///
    /// Defaults to `false` so profiling is opt-in per call site.
    pub profiler_enabled: bool,
    /// Optional sender for step-level progress events, allowing callers to
    /// render progress bars during multi-workflow execution.
    pub progress_sender: Option<WorkflowProgressSender>,
    /// Optional CAS integrity configuration applied to the filesystem store
    /// used during this workflow invocation.
    ///
    /// When set, overrides default CAS integrity behavior for read
    /// verification sampling, staleness timeouts, and reconstructed-byte
    /// caching.  When `None`, the CAS store uses its own defaults.
    pub cas_integrity_config: Option<CasIntegrityConfig>,
    /// Enables CorruptObject retry even for impure workflow steps.
    ///
    /// When `true`, impure steps that encounter corrupt CAS objects will
    /// be retried with a cleared tool cache, matching the behavior that
    /// pure workflows already have. Defaults to `false`.
    pub retry_impure: bool,
}

impl PartialEq for RunWorkflowOptions {
    fn eq(&self, other: &Self) -> bool {
        self.allow_tool_redefinition == other.allow_tool_redefinition
            && self.runtime_storage_paths == other.runtime_storage_paths
            && self.runtime_inherited_env_vars == other.runtime_inherited_env_vars
            && self.profile_output_path == other.profile_output_path
            && self.profiler_enabled == other.profiler_enabled
            && self.progress_sender.is_some() == other.progress_sender.is_some()
            && self.cas_integrity_config == other.cas_integrity_config
            && self.retry_impure == other.retry_impure
    }
}
impl Eq for RunWorkflowOptions {}

impl RunWorkflowOptions {
    /// Returns strict-safe defaults for workflow execution.
    #[must_use]
    pub fn strict() -> Self {
        Self {
            allow_tool_redefinition: false,
            runtime_storage_paths: RuntimeStoragePaths::default(),
            runtime_inherited_env_vars: Vec::new(),
            profile_output_path: None,
            profiler_enabled: false,
            progress_sender: None,
            cas_integrity_config: None,
            retry_impure: false,
        }
    }
}

impl Default for RunWorkflowOptions {
    fn default() -> Self {
        Self::strict()
    }
}

/// Runtime options for state export/import/edit operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateMutationOptions {
    /// Grouped runtime storage paths used by this invocation.
    ///
    /// Defaults:
    /// - `conductor_dir = .conductor`
    /// - `conductor_state_config = <conductor_dir>/state.ncl`
    /// - `cas_store_dir = <conductor_dir>/store`
    /// - `conductor_tmp_dir = <os-temp>/mediapm-conductor-<conductor-dir-hash>`
    /// - `conductor_schema_dir = <conductor_dir>/config/conductor`
    pub runtime_storage_paths: RuntimeStoragePaths,
    /// Additional host environment variable names inherited while evaluating
    /// configuration documents for this operation.
    pub runtime_inherited_env_vars: Vec<String>,
}

impl StateMutationOptions {
    /// Returns strict-safe defaults for state mutation operations.
    #[must_use]
    pub fn strict() -> Self {
        Self {
            runtime_storage_paths: RuntimeStoragePaths::default(),
            runtime_inherited_env_vars: Vec::new(),
        }
    }
}

impl Default for StateMutationOptions {
    fn default() -> Self {
        Self::strict()
    }
}

/// Async API contract for the conductor.
#[async_trait]
pub trait ConductorApi: Send + Sync {
    /// Executes workflows using user and machine configuration inputs.
    ///
    /// Resolution semantics:
    /// - runtime merges `conductor.ncl`, `conductor.machine.ncl`, and
    ///   one volatile state document,
    /// - each document must define an explicit top-level numeric `version`,
    /// - any detected cross-document conflict fails the run with a workflow
    ///   error.
    ///
    /// Mutation boundary:
    /// - `conductor.ncl` is treated as user-edited input.
    /// - runtime writes setup/managed metadata to `conductor.machine.ncl`.
    /// - runtime writes volatile state (`impure_timestamps`, `state_pointer`)
    ///   plus required `version` marker to
    ///   `<runtime_storage_paths.conductor_dir>/state.ncl` by default.
    async fn run_workflow(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
    ) -> Result<RunSummary, ConductorError> {
        self.run_workflow_with_options(user_ncl, machine_ncl, RunWorkflowOptions::default()).await
    }

    /// Executes workflows using user and machine configuration inputs with one
    /// explicit runtime option set.
    ///
    /// Safety default: when `allow_tool_redefinition=false`, once one tool is
    /// defined, subsequent conflicting redefinitions for the same immutable tool
    /// name are rejected.
    async fn run_workflow_with_options(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<RunSummary, ConductorError>;

    /// Submits a workflow for background execution, returning a handle ID.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails.
    async fn submit_workflow(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<u64, ConductorError>;

    /// Polls a previously submitted workflow by handle ID.
    ///
    /// Returns `None` if the workflow is still running, `Some(Ok(...))` on
    /// success, or `Some(Err(...))` on failure.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails.
    async fn poll_workflow(
        &self,
        handle_id: u64,
    ) -> Result<Option<Result<RunSummary, ConductorError>>, ConductorError>;

    /// Returns the current in-memory orchestration-state snapshot.
    async fn get_state(&self) -> Result<OrchestrationState, ConductorError>;

    /// Loads the effective orchestration state for one user/machine/runtime
    /// path configuration.
    ///
    /// Resolution semantics mirror workflow execution:
    /// - load and validate `conductor.ncl`, `conductor.machine.ncl`, and
    ///   resolved volatile state document,
    /// - resolve one effective `state_pointer`,
    /// - load the pointed orchestration state from CAS.
    async fn load_resolved_state(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: StateMutationOptions,
    ) -> Result<OrchestrationState, ConductorError>;

    /// Replaces orchestration state for one user/machine/runtime path
    /// configuration.
    ///
    /// Mutation boundary:
    /// - validate the candidate state against currently resolved merged config,
    /// - persist only the new state blob in CAS,
    /// - update only the volatile state document `state_pointer`.
    async fn replace_resolved_state(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        state: OrchestrationState,
        options: StateMutationOptions,
    ) -> Result<Hash, ConductorError>;

    /// Exports effective orchestration state to one JSON file.
    ///
    /// The file payload uses persisted wire-envelope JSON shape.
    ///
    /// # Errors
    ///
    /// Returns an error when state resolution fails, parent directory creation
    /// fails, or writing the export file fails.
    async fn export_state_to_path(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: StateMutationOptions,
        output_path: &Path,
    ) -> Result<Hash, ConductorError> {
        let state = self.load_resolved_state(user_ncl, machine_ncl, options).await?;
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
                operation: "creating parent directory for exported state".to_string(),
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let rendered = persisted_state_json_pretty(&state)?;
        fs::write(output_path, rendered.as_bytes()).map_err(|source| ConductorError::Io {
            operation: "writing exported orchestration state".to_string(),
            path: output_path.to_path_buf(),
            source,
        })?;

        Ok(Hash::from_content(rendered.as_bytes()))
    }

    /// Imports orchestration state from one JSON file and publishes it through
    /// volatile state pointer update.
    ///
    /// # Errors
    ///
    /// Returns an error when reading or decoding input JSON fails, state
    /// validation fails against resolved config, CAS persistence fails, or
    /// volatile pointer persistence fails.
    async fn import_state_from_path(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: StateMutationOptions,
        input_path: &Path,
    ) -> Result<Hash, ConductorError> {
        let bytes = fs::read(input_path).map_err(|source| ConductorError::Io {
            operation: "reading imported orchestration state".to_string(),
            path: input_path.to_path_buf(),
            source,
        })?;
        let state = decode_state_from_slice(&bytes)?;
        self.replace_resolved_state(user_ncl, machine_ncl, state, options).await
    }

    /// Returns runtime diagnostics including worker queue metrics and scheduler traces.
    async fn get_runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError>;
}

/// Canonical default conductor configuration paths.
#[must_use]
pub fn default_state_paths() -> (PathBuf, PathBuf) {
    (PathBuf::from("conductor.ncl"), PathBuf::from("conductor.machine.ncl"))
}

/// Resolves the conductor schema export directory under one runtime root.
///
/// The default runtime export target is:
/// `<runtime_root>/config/conductor`.
#[must_use]
pub fn schema_export_dir(runtime_root: &Path) -> PathBuf {
    runtime_root.join(DEFAULT_SCHEMA_EXPORT_PARENT_DIR_NAME).join(DEFAULT_SCHEMA_EXPORT_DIR_NAME)
}

/// Exports embedded conductor Nickel schemas into one resolved schema directory.
///
/// This writes `mod.ncl` and `v1.ncl` under
/// the provided export directory, creating the directory tree when needed.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when creating the export directory or
/// writing schema files fails.
pub fn export_nickel_config_schemas(export_dir: &Path) -> Result<(), ConductorError> {
    fs::create_dir_all(export_dir).map_err(|source| ConductorError::Io {
        operation: "creating runtime schema export directory".to_string(),
        path: export_dir.to_path_buf(),
        source,
    })?;

    let schemas = [
        ("mod.ncl", include_str!("model/config/versions/mod.ncl")),
        ("v1.ncl", include_str!("model/config/versions/v1.ncl")),
    ];

    for (file_name, content) in schemas {
        let path = export_dir.join(file_name);
        fs::write(&path, content).map_err(|source| ConductorError::Io {
            operation: format!("writing exported Nickel schema '{file_name}'"),
            path,
            source,
        })?;
    }

    Ok(())
}

/// Snapshot of conductor runtime diagnostics and scheduling telemetry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeDiagnostics {
    /// Number of active step workers in the execution pool.
    pub worker_pool_size: usize,
    /// Scheduler-level adaptive model diagnostics.
    pub scheduler: SchedulerDiagnostics,
    /// Per-worker queue and execution counters.
    pub workers: Vec<WorkerQueueDiagnostics>,
    /// Recent scheduler trace events in chronological order.
    pub recent_traces: Vec<SchedulerTraceEvent>,
}

/// Scheduler model diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchedulerDiagnostics {
    /// EWMA alpha used to blend new runtime observations.
    pub ewma_alpha: f64,
    /// Default estimated cost for tools without history.
    pub unknown_cost_ms: f64,
    /// Current per-tool runtime estimates.
    pub tool_estimates: Vec<ToolRuntimeEstimate>,
}

/// One tool runtime estimate entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRuntimeEstimate {
    /// Tool name.
    pub tool_name: String,
    /// Current EWMA estimate in milliseconds.
    pub estimated_ms: f64,
}

/// Per-worker queue and execution telemetry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerQueueDiagnostics {
    /// Worker index in the pool.
    pub worker_index: usize,
    /// Total steps assigned to this worker.
    pub assigned_steps_total: u64,
    /// Total steps completed by this worker.
    pub completed_steps_total: u64,
    /// Current in-flight requests assigned to this worker.
    pub in_flight: u64,
    /// Peak in-flight requests observed for this worker.
    pub peak_in_flight: u64,
    /// Steps assigned to this worker in the most recently planned level.
    pub last_level_assigned_steps: u64,
    /// Estimated runtime load (ms) assigned in the most recently planned level.
    pub last_level_estimated_load_ms: f64,
    /// Cumulative estimated runtime load assigned to this worker.
    pub cumulative_estimated_load_ms: f64,
    /// Cumulative observed runtime load completed by this worker.
    pub cumulative_observed_load_ms: f64,
}

/// Scheduler trace event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchedulerTraceEvent {
    /// Monotonic trace sequence number.
    pub sequence: u64,
    /// UTC timestamp in nanoseconds since Unix epoch.
    pub timestamp_unix_nanos: u128,
    /// Event-specific payload.
    pub kind: SchedulerTraceKind,
}

/// Scheduler trace event payload variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SchedulerTraceKind {
    /// One workflow level was planned for dispatch.
    LevelPlanned {
        /// Workflow name.
        workflow_name: String,
        /// Zero-based level index in topological execution.
        level_index: usize,
        /// Number of steps in this level.
        step_count: usize,
        /// Worker count used for planning.
        worker_count: usize,
    },
    /// One step was assigned to a worker.
    StepAssigned {
        /// Workflow name.
        workflow_name: String,
        /// Level index.
        level_index: usize,
        /// Step id.
        step_id: String,
        /// Tool name.
        tool_name: String,
        /// Target worker index.
        worker_index: usize,
        /// Estimated runtime used for scheduling.
        estimated_ms: f64,
    },
    /// One step finished execution.
    StepCompleted {
        /// Step id.
        step_id: String,
        /// Tool name.
        tool_name: String,
        /// Worker index.
        worker_index: usize,
        /// Whether this step executed (vs cache hit).
        executed: bool,
        /// Observed latency.
        observed_ms: f64,
    },

    /// Scheduler EWMA estimate updated for one tool.
    EwmaUpdated {
        /// Tool name.
        tool_name: String,
        /// Previous estimate, if known.
        previous_estimate_ms: Option<f64>,
        /// Observed runtime used for update.
        observed_ms: f64,
        /// New estimate after update.
        new_estimate_ms: f64,
    },
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[cfg(feature = "tool-presets")]
    use super::CommonExecutableTool;
    use super::{RuntimeStoragePaths, export_nickel_config_schemas, schema_export_dir};

    /// Protects grouped-runtime default resolution rooted at `.conductor`.
    #[test]
    fn default_runtime_storage_paths_resolve_under_conductor_directory() {
        let user_ncl = PathBuf::from("workspace").join("conductor.ncl");
        let machine_ncl = PathBuf::from("workspace").join("conductor.machine.ncl");
        let resolved = RuntimeStoragePaths::default().resolve_for(&user_ncl, &machine_ncl);

        assert_eq!(resolved.conductor_dir, PathBuf::from("workspace").join(".conductor"));
        assert_eq!(
            resolved.conductor_state_config,
            PathBuf::from("workspace").join(".conductor").join("state.ncl")
        );
        assert_eq!(
            resolved.cas_store_dir,
            PathBuf::from("workspace").join(".conductor").join("store")
        );
        assert!(resolved.conductor_tmp_dir.starts_with(std::env::temp_dir()));
        assert!(resolved.conductor_tmp_dir.to_string_lossy().contains("mediapm-conductor-"));
        assert_eq!(
            resolved.conductor_schema_dir,
            PathBuf::from("workspace").join(".conductor").join("config").join("conductor")
        );
        assert_eq!(
            resolved.conductor_tools_dir,
            PathBuf::from("workspace").join(".conductor").join("tools")
        );
    }

    /// Protects explicit runtime path overrides while preserving grouped
    /// default behavior for unspecified fields.
    #[test]
    fn runtime_storage_path_overrides_are_applied_per_field() {
        let user_ncl = PathBuf::from("workspace").join("conductor.ncl");
        let machine_ncl = PathBuf::from("workspace").join("conductor.machine.ncl");
        let mut paths = RuntimeStoragePaths::default_for(&PathBuf::from("runtime-root"));
        paths.conductor_state_config = PathBuf::from("runtime/custom-state.ncl");
        paths.conductor_schema_dir = PathBuf::from("runtime/custom-schemas");
        let resolved = paths.resolve_for(&user_ncl, &machine_ncl);

        assert_eq!(resolved.conductor_dir, PathBuf::from("workspace").join("runtime-root"));
        assert_eq!(
            resolved.conductor_state_config,
            PathBuf::from("workspace").join("runtime/custom-state.ncl")
        );
        assert_eq!(
            resolved.cas_store_dir,
            PathBuf::from("workspace").join("runtime-root").join("store")
        );
        assert!(resolved.conductor_tmp_dir.starts_with(std::env::temp_dir()));
        assert!(resolved.conductor_tmp_dir.to_string_lossy().contains("mediapm-conductor-"));
        assert_eq!(
            resolved.conductor_schema_dir,
            PathBuf::from("workspace").join("runtime/custom-schemas")
        );
        assert_eq!(
            resolved.conductor_tools_dir,
            PathBuf::from("workspace").join("runtime-root").join("tools")
        );
    }

    /// Protects default runtime path anchoring when user and machine config
    /// documents live under different parent directories.
    #[test]
    fn runtime_defaults_anchor_to_user_config_parent_when_parents_differ() {
        let user_ncl = PathBuf::from("workspace").join("config").join("conductor.ncl");
        let machine_ncl = PathBuf::from("workspace").join("runtime").join("conductor.machine.ncl");

        let resolved = RuntimeStoragePaths::default().resolve_for(&user_ncl, &machine_ncl);

        assert_eq!(
            resolved.conductor_dir,
            PathBuf::from("workspace").join("config").join(".conductor")
        );
        assert_eq!(
            resolved.conductor_state_config,
            PathBuf::from("workspace").join("config").join(".conductor").join("state.ncl")
        );
        assert_eq!(
            resolved.cas_store_dir,
            PathBuf::from("workspace").join("config").join(".conductor").join("store")
        );
        assert!(resolved.conductor_tmp_dir.starts_with(std::env::temp_dir()));
        assert!(resolved.conductor_tmp_dir.to_string_lossy().contains("mediapm-conductor-"));
        assert_eq!(
            resolved.conductor_schema_dir,
            PathBuf::from("workspace")
                .join("config")
                .join(".conductor")
                .join("config")
                .join("conductor")
        );
    }

    /// Protects schema-export path resolution under runtime root.
    #[test]
    fn schema_export_dir_uses_runtime_root() {
        let runtime_root = PathBuf::from("workspace");
        assert_eq!(
            schema_export_dir(&runtime_root),
            PathBuf::from("workspace").join("config").join("conductor")
        );
    }

    /// Protects embedded schema export into runtime-managed defaults.
    #[test]
    fn export_nickel_config_schemas_writes_schema_files() {
        let root = tempdir().expect("tempdir");
        let export_dir = root.path().join("runtime").join("schemas").join("conductor");

        export_nickel_config_schemas(&export_dir).expect("schema export should succeed");

        let mod_schema = export_dir.join("mod.ncl");
        let v1_schema = export_dir.join("v1.ncl");

        assert!(mod_schema.exists(), "mod.ncl should be exported");
        assert!(v1_schema.exists(), "v1.ncl should be exported");
        assert!(!std::fs::read(mod_schema).expect("mod schema").is_empty());
        assert!(!std::fs::read(v1_schema).expect("v1 schema").is_empty());
    }

    /// Protects stable tool-preset selector metadata for release downloads.
    #[cfg(feature = "tool-presets")]
    #[test]
    fn common_sd_tool_selector_fields_are_stable() {
        assert_eq!(CommonExecutableTool::Sd.logical_tool_name(), "mediapm-conductor.tools.sd");
        assert!(CommonExecutableTool::Sd.executable_file_name().starts_with("sd"));
    }
}
