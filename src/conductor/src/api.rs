//! Public API contracts for the conductor crate.

use std::fs;
use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::error::ConductorError;
use crate::model::state::OrchestrationState;

/// Default runtime storage directory name under one config-root anchor.
const DEFAULT_CONDUCTOR_DIR_NAME: &str = ".conductor";

/// Default volatile state file name under the resolved conductor directory.
const DEFAULT_STATE_FILE_NAME: &str = "state.ncl";

/// Default filesystem CAS store directory name under the resolved conductor directory.
const DEFAULT_CAS_STORE_DIR_NAME: &str = "store";

/// Default schema export directory under one resolved runtime root.
const DEFAULT_SCHEMA_EXPORT_DIR_NAME: &str = "conductor";

/// Default schema export parent folder under one resolved runtime root.
const DEFAULT_SCHEMA_EXPORT_PARENT_DIR_NAME: &str = "config";

/// Grouped runtime storage-path configuration for one conductor invocation.
///
/// This keeps all runtime-managed filesystem paths in one place:
/// - `conductor_dir` anchors runtime-owned artifacts,
/// - `config_state` optionally overrides the volatile state document path,
/// - `cas_store_dir` optionally overrides the default CAS filesystem root.
///
/// When `config_state` or `cas_store_dir` is `None`, their defaults are derived
/// from `conductor_dir` as `state.ncl` and `store/` respectively.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStoragePaths {
    /// Root folder for runtime-owned artifacts.
    ///
    /// Default: `.conductor`.
    pub conductor_dir: PathBuf,
    /// Optional override path for the volatile state document.
    ///
    /// Default: `<conductor_dir>/state.ncl`.
    pub config_state: Option<PathBuf>,
    /// Optional override path for the filesystem CAS store root used by
    /// command-line defaults.
    ///
    /// Default: `<conductor_dir>/store`.
    pub cas_store_dir: Option<PathBuf>,
}

impl RuntimeStoragePaths {
    /// Returns grouped runtime-storage defaults rooted under `.conductor`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            conductor_dir: PathBuf::from(DEFAULT_CONDUCTOR_DIR_NAME),
            config_state: None,
            cas_store_dir: None,
        }
    }

    /// Resolves all runtime storage paths for a specific user/machine config
    /// location pair.
    ///
    /// Relative paths are resolved against the user config parent when
    /// available, otherwise the machine config parent, otherwise `.`.
    #[must_use]
    pub fn resolve_for(&self, user_ncl: &Path, machine_ncl: &Path) -> ResolvedRuntimeStoragePaths {
        let anchor = user_ncl.parent().or_else(|| machine_ncl.parent()).unwrap_or(Path::new("."));
        let conductor_dir = Self::resolve_path(anchor, &self.conductor_dir);
        let config_state = self.config_state.as_ref().map_or_else(
            || conductor_dir.join(DEFAULT_STATE_FILE_NAME),
            |path| Self::resolve_path(anchor, path),
        );
        let cas_store_dir = self.cas_store_dir.as_ref().map_or_else(
            || conductor_dir.join(DEFAULT_CAS_STORE_DIR_NAME),
            |path| Self::resolve_path(anchor, path),
        );

        ResolvedRuntimeStoragePaths { conductor_dir, config_state, cas_store_dir }
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

/// Concrete runtime storage paths after resolving relative values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRuntimeStoragePaths {
    /// Resolved runtime root folder.
    pub conductor_dir: PathBuf,
    /// Resolved volatile state document path.
    pub config_state: PathBuf,
    /// Resolved filesystem CAS store root path.
    pub cas_store_dir: PathBuf,
}

/// Summary of one workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Runtime options controlling one `run_workflow` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// - `state_config = <conductor_dir>/state.ncl`
    /// - `cas_store_dir = <conductor_dir>/store`
    pub runtime_storage_paths: RuntimeStoragePaths,
    /// Additional host environment variable names inherited into executable
    /// runtime process environments.
    ///
    /// This list is merged with runtime document defaults and host-specific
    /// baseline names (for example `SYSTEMROOT`/`WINDIR` on Windows).
    pub runtime_inherited_env_vars: Vec<String>,
}

impl RunWorkflowOptions {
    /// Returns strict-safe defaults for workflow execution.
    #[must_use]
    pub fn strict() -> Self {
        Self {
            allow_tool_redefinition: false,
            runtime_storage_paths: RuntimeStoragePaths::default(),
            runtime_inherited_env_vars: Vec::new(),
        }
    }
}

impl Default for RunWorkflowOptions {
    fn default() -> Self {
        Self::strict()
    }
}

/// Async API contract for Phase 2 conductor.
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

    /// Returns the current in-memory orchestration-state snapshot.
    async fn get_state(&self) -> Result<OrchestrationState, ConductorError>;

    /// Returns runtime diagnostics including worker queue metrics and scheduler traces.
    async fn get_runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError>;
}

/// Canonical default configuration paths for Phase 2.
#[must_use]
pub fn default_state_paths() -> (PathBuf, PathBuf) {
    (PathBuf::from("conductor.ncl"), PathBuf::from("conductor.machine.ncl"))
}

/// Resolves grouped runtime storage paths for one user/machine pair.
#[must_use]
pub fn resolve_runtime_storage_paths(
    user_ncl: &Path,
    machine_ncl: &Path,
    runtime_storage_paths: &RuntimeStoragePaths,
) -> ResolvedRuntimeStoragePaths {
    runtime_storage_paths.resolve_for(user_ncl, machine_ncl)
}

/// Resolves the conductor schema export directory under one runtime root.
///
/// The default runtime export target is:
/// `<runtime_root>/config/conductor`.
#[must_use]
pub fn schema_export_dir(runtime_root: &Path) -> PathBuf {
    runtime_root.join(DEFAULT_SCHEMA_EXPORT_PARENT_DIR_NAME).join(DEFAULT_SCHEMA_EXPORT_DIR_NAME)
}

/// Exports embedded conductor Nickel schemas into one resolved runtime root.
///
/// This writes `mod.ncl` and `v1.ncl` under
/// `<runtime_root>/config/conductor`, creating the directory tree when
/// needed.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when creating the export directory or
/// writing schema files fails.
pub fn export_nickel_config_schemas(runtime_root: &Path) -> Result<(), ConductorError> {
    let export_dir = schema_export_dir(runtime_root);
    fs::create_dir_all(&export_dir).map_err(|source| ConductorError::Io {
        operation: "creating runtime schema export directory".to_string(),
        path: export_dir.clone(),
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
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct SchedulerDiagnostics {
    /// EWMA alpha used to blend new runtime observations.
    pub ewma_alpha: f64,
    /// Default estimated cost for tools without history.
    pub unknown_cost_ms: f64,
    /// Current per-tool runtime estimates.
    pub tool_estimates: Vec<ToolRuntimeEstimate>,
    /// Number of step dispatches that required fallback execution.
    pub rpc_fallbacks_total: u64,
}

/// One tool runtime estimate entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolRuntimeEstimate {
    /// Tool name.
    pub tool_name: String,
    /// Current EWMA estimate in milliseconds.
    pub estimated_ms: f64,
}

/// Per-worker queue and execution telemetry.
#[derive(Debug, Clone, PartialEq)]
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
    /// Number of RPC dispatch failures encountered for this worker.
    pub rpc_failures_total: u64,
    /// Number of fallback local executions used for this worker.
    pub fallback_executions_total: u64,
}

/// Scheduler trace event.
#[derive(Debug, Clone, PartialEq)]
pub struct SchedulerTraceEvent {
    /// Monotonic trace sequence number.
    pub sequence: u64,
    /// UTC timestamp in nanoseconds since Unix epoch.
    pub timestamp_unix_nanos: u128,
    /// Event-specific payload.
    pub kind: SchedulerTraceKind,
}

/// Scheduler trace event payload variants.
#[derive(Debug, Clone, PartialEq)]
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
        /// Whether this completion used fallback local execution.
        fallback_used: bool,
        /// Observed latency.
        observed_ms: f64,
    },
    /// A worker RPC dispatch failed and fallback path was used.
    RpcFallback {
        /// Step id.
        step_id: String,
        /// Worker index where RPC failed.
        worker_index: usize,
        /// Error message from actor RPC layer.
        reason: String,
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

    use super::{
        RuntimeStoragePaths, export_nickel_config_schemas, resolve_runtime_storage_paths,
        schema_export_dir,
    };

    /// Protects grouped-runtime default resolution rooted at `.conductor`.
    #[test]
    fn default_runtime_storage_paths_resolve_under_conductor_directory() {
        let user_ncl = PathBuf::from("workspace").join("conductor.ncl");
        let machine_ncl = PathBuf::from("workspace").join("conductor.machine.ncl");
        let resolved =
            resolve_runtime_storage_paths(&user_ncl, &machine_ncl, &RuntimeStoragePaths::default());

        assert_eq!(resolved.conductor_dir, PathBuf::from("workspace").join(".conductor"));
        assert_eq!(
            resolved.config_state,
            PathBuf::from("workspace").join(".conductor").join("state.ncl")
        );
        assert_eq!(
            resolved.cas_store_dir,
            PathBuf::from("workspace").join(".conductor").join("store")
        );
    }

    /// Protects explicit runtime path overrides while preserving grouped
    /// default behavior for unspecified fields.
    #[test]
    fn runtime_storage_path_overrides_are_applied_per_field() {
        let user_ncl = PathBuf::from("workspace").join("conductor.ncl");
        let machine_ncl = PathBuf::from("workspace").join("conductor.machine.ncl");
        let resolved = resolve_runtime_storage_paths(
            &user_ncl,
            &machine_ncl,
            &RuntimeStoragePaths {
                conductor_dir: PathBuf::from("runtime-root"),
                config_state: Some(PathBuf::from("runtime/custom-state.ncl")),
                cas_store_dir: None,
            },
        );

        assert_eq!(resolved.conductor_dir, PathBuf::from("workspace").join("runtime-root"));
        assert_eq!(
            resolved.config_state,
            PathBuf::from("workspace").join("runtime/custom-state.ncl")
        );
        assert_eq!(
            resolved.cas_store_dir,
            PathBuf::from("workspace").join("runtime-root").join("store")
        );
    }

    /// Protects default runtime path anchoring when user and machine config
    /// documents live under different parent directories.
    #[test]
    fn runtime_defaults_anchor_to_user_config_parent_when_parents_differ() {
        let user_ncl = PathBuf::from("workspace").join("config").join("conductor.ncl");
        let machine_ncl = PathBuf::from("workspace").join("runtime").join("conductor.machine.ncl");

        let resolved =
            resolve_runtime_storage_paths(&user_ncl, &machine_ncl, &RuntimeStoragePaths::default());

        assert_eq!(
            resolved.conductor_dir,
            PathBuf::from("workspace").join("config").join(".conductor")
        );
        assert_eq!(
            resolved.config_state,
            PathBuf::from("workspace").join("config").join(".conductor").join("state.ncl")
        );
        assert_eq!(
            resolved.cas_store_dir,
            PathBuf::from("workspace").join("config").join(".conductor").join("store")
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
        let runtime_root = root.path().join("runtime");

        export_nickel_config_schemas(&runtime_root).expect("schema export should succeed");

        let export_dir = runtime_root.join("config").join("conductor");
        let mod_schema = export_dir.join("mod.ncl");
        let v1_schema = export_dir.join("v1.ncl");

        assert!(mod_schema.exists(), "mod.ncl should be exported");
        assert!(v1_schema.exists(), "v1.ncl should be exported");
        assert!(!std::fs::read(mod_schema).expect("mod schema").is_empty());
        assert!(!std::fs::read(v1_schema).expect("v1 schema").is_empty());
    }
}
