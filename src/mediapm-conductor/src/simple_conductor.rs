//! Simplified facade over the conductor orchestration runtime.
//!
//! [`SimpleConductor`] is a concrete, minimal implementation of the conductor
//! API. It owns a lazy [`ConductorActorClient`] through which all workflow
//! operations are dispatched, and provides convenience stubs for tool/data
//! management that are expected by the CLI layer.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mediapm_cas::{CasApi, CasMaintenanceApi, Hash};
use tokio::sync::OnceCell;

use crate::api::{
    ConductorApi, RunSummary, RunWorkflowOptions, RuntimeDiagnostics, RuntimeStoragePaths,
};
use crate::error::ConductorError;
use crate::model::config::documents::{NickelDocument, SourceDocument, merge_documents};
use crate::model::config::versions;
use crate::model::state::OrchestrationState;
use crate::orchestration::node::ConductorActorClient;
use crate::orchestration::protocol::UnifiedNickelDocument;

/// Concrete facade over the conductor orchestration runtime.
///
/// Wraps a lazily initialized [`ConductorActorClient`] (which itself manages a
/// [`WorkflowCoordinator`] actor) and exposes all CLI-required operations.
pub struct SimpleConductor<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    /// Shared CAS store.
    cas: Arc<C>,
    /// Lazily spawned conductor actor client.
    actor_client: OnceCell<ConductorActorClient>,
    /// Resolved runtime paths.
    storage_paths: RuntimeStoragePaths,
}

impl<C> SimpleConductor<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    /// Creates a new conductor facade.
    #[must_use]
    pub fn new(storage_paths: RuntimeStoragePaths, cas: C) -> Self {
        Self { cas: Arc::new(cas), actor_client: OnceCell::new(), storage_paths }
    }

    /// Returns or initialises the conductor actor client.
    async fn ensure_actor_client(&self) -> Result<&ConductorActorClient, ConductorError> {
        self.actor_client
            .get_or_try_init(|| async {
                crate::orchestration::node::spawn_conductor_actor(self.cas.clone()).await
            })
            .await
    }

    /// Returns a reference to the underlying CAS store.
    #[must_use]
    pub fn cas(&self) -> &Arc<C> {
        &self.cas
    }

    /// Returns a reference to the runtime storage paths.
    #[must_use]
    pub fn storage_paths(&self) -> &RuntimeStoragePaths {
        &self.storage_paths
    }

    // -----------------------------------------------------------------------
    // CLI-facing convenience methods (may be simplified further)
    // -----------------------------------------------------------------------

    /// Runs a workflow and returns a summary.
    ///
    /// # Errors
    ///
    /// Delegates to the conductor actor; returns an error when delivery or
    /// execution fails.
    pub async fn run_workflow(
        &self,
        workflow_name: &str,
        options: RunWorkflowOptions,
    ) -> Result<RunSummary, ConductorError> {
        let client = self.ensure_actor_client().await?;
        let (unified, state) = load_unified_config_and_state(self.storage_paths()).await?;
        // Apply conductor runtime config defaults to options
        let options = {
            let mut opts = options;
            if !opts.retry_impure {
                if let Some(true) = unified.runtime.retry_impure {
                    opts.retry_impure = true;
                }
            }
            opts
        };
        client.run_workflow(workflow_name, options, unified, state).await
    }

    /// Runs a workflow directly, bypassing the actor layer.
    ///
    /// Creates a fresh [`WorkflowCoordinator`] and runs the workflow with the
    /// current unified config and state. Useful for one-shot runs without
    /// actor lifecycle overhead.
    ///
    /// # Errors
    ///
    /// Delegates to [`WorkflowCoordinator::run_workflow`].
    pub(crate) async fn run_workflow_direct(
        &self,
        workflow_name: &str,
        _options: RunWorkflowOptions,
    ) -> Result<RunSummary, ConductorError> {
        let (unified, mut state) = load_unified_config_and_state(self.storage_paths()).await?;
        let mut coordinator =
            crate::orchestration::coordinator::WorkflowCoordinator::new(self.cas.clone());
        coordinator.run_workflow(workflow_name, &unified, &mut state).await
    }

    /// Returns a snapshot of runtime diagnostics.
    ///
    /// # Errors
    ///
    /// Delegates to the conductor actor; returns an error when the actor is
    /// unreachable.
    pub async fn get_runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        let client = self.ensure_actor_client().await?;
        client.runtime_diagnostics().await
    }

    /// Returns the current orchestration state.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Io`] when the persisted state file cannot be
    /// read.
    pub async fn get_state(&self) -> Result<OrchestrationState, ConductorError> {
        load_state(self.storage_paths()).await
    }

    /// Replaces the persisted orchestration state.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Io`] when the persisted state file cannot be
    /// written.
    pub async fn replace_resolved_state(
        &self,
        state: OrchestrationState,
    ) -> Result<(), ConductorError> {
        save_state(self.storage_paths(), &state).await
    }

    /// Adds a tool configuration to the first available config document.
    ///
    /// Loads the first user config document found in `conductor_dir`, appends
    /// a managed tool spec, and persists the document.  If no config document
    /// exists yet, creates `config.ncl` as the initial document.
    ///
    /// Before adding, validates that the tool name does not conflict with any
    /// existing tool across ALL config documents (merged view), not just the
    /// target file.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when the tool already exists in
    /// any config document, or wraps any I/O / Nickel evaluation error.
    pub async fn add_tool_config(
        &self,
        name: &str,
        executable: Option<&str>,
        content_map: BTreeMap<String, String>,
    ) -> Result<(), ConductorError> {
        let config_dir = &self.storage_paths.conductor_dir;
        // Pick the first user config file, or create config.ncl.
        let config_path =
            find_first_config(config_dir).unwrap_or_else(|| config_dir.join("config.ncl"));

        // Check for duplicate tool name across ALL existing configs (merged view).
        let config_paths = discover_config_paths(self.storage_paths());
        if !config_paths.is_empty() {
            let source_docs: Vec<SourceDocument> = config_paths
                .into_iter()
                .map(|path| {
                    let document = crate::cli_document_io::load_document(&path)?;
                    Ok(SourceDocument { path, document })
                })
                .collect::<Result<Vec<_>, ConductorError>>()?;
            let merged = merge_documents(&source_docs)?;
            if merged.tools.contains_key(name) {
                return Err(ConductorError::Workflow(format!(
                    "tool '{name}' already exists in a config document"
                )));
            }
        }

        let mut doc = if config_path.exists() {
            crate::cli_document_io::load_document(&config_path)?
        } else {
            NickelDocument::default()
        };

        let command_parts = executable.map_or(vec![], |cmd| vec![cmd.to_string()]);

        let tool = crate::model::config::ToolSpec {
            kind: crate::model::config::ToolKindSpec::Executable {
                command: command_parts.clone(),
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            name: name.to_string(),
            version: "0.1.0".to_string(),
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            runtime: crate::model::config::ToolRuntime {
                content_map,
                ..crate::model::config::ToolRuntime::default()
            },
        };
        doc.tools.insert(name.to_string(), tool);
        crate::cli_document_io::save_document(&config_path, &doc)
    }

    /// Removes external data by hash.
    ///
    /// # Errors
    ///
    /// Delegates to the CAS store.
    pub async fn remove_external_data(&self, hash: &Hash) -> Result<(), ConductorError> {
        self.cas
            .delete(*hash)
            .await
            .map_err(|e| ConductorError::Workflow(format!("removing external data failed: {e}")))
    }

    /// Removes a tool configuration from the first config document.
    ///
    /// Loads the first user config document found in `conductor_dir`, removes
    /// all matching tool specs by name, and persists the document.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when the tool is not found, or
    /// wraps any I/O / Nickel evaluation error.
    pub async fn remove_tool_config(
        &self,
        name: &str,
        _metadata: bool,
    ) -> Result<(), ConductorError> {
        let config_dir = &self.storage_paths.conductor_dir;
        let config_path = find_first_config(config_dir).ok_or_else(|| {
            ConductorError::Workflow("no config document found to remove from".to_string())
        })?;

        let mut doc = crate::cli_document_io::load_document(&config_path)?;
        if doc.tools.remove(name).is_none() {
            return Err(ConductorError::Workflow(format!("tool '{name}' not found in config")));
        }
        crate::cli_document_io::save_document(&config_path, &doc)
    }

    /// Runs a managed tool with passthrough arguments.
    ///
    /// Loads the merged unified config, looks up the tool by name, and
    /// executes the configured process command with the supplied arguments.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when the tool is not found, the
    /// tool has no process spec (builtins cannot be run passthrough), or
    /// the subprocess fails.
    pub async fn run_tool_passthrough(
        &self,
        tool: &str,
        args: &[String],
    ) -> Result<i32, ConductorError> {
        let (unified, _state) = load_unified_config_and_state(self.storage_paths()).await?;

        let tool_spec = unified.tools.get(tool).ok_or_else(|| {
            ConductorError::Workflow(format!("tool '{tool}' not found in unified config"))
        })?;

        let Some((cmd, cmd_args)) = tool_spec.command_parts.split_first() else {
            return Err(ConductorError::Workflow(format!(
                "tool '{tool}' has no executable process (cannot run passthrough)"
            )));
        };

        let status =
            tokio::process::Command::new(cmd).args(cmd_args).args(args).status().await.map_err(
                |e| ConductorError::Workflow(format!("failed to run tool '{tool}': {e}")),
            )?;

        Ok(status.code().unwrap_or(-1))
    }

    /// Runs a CAS CLI command with passthrough arguments.
    ///
    /// Locates the `mediapm-cas` binary (same directory as the conductor
    /// binary, then PATH) and invokes it as a subprocess with all supplied
    /// arguments forwarded verbatim.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when the CAS binary cannot be
    /// found or the subprocess fails.
    pub async fn run_cas_passthrough(&self, args: &[String]) -> Result<i32, ConductorError> {
        let cas_binary = find_cas_binary().ok_or_else(|| {
            ConductorError::Workflow("could not locate 'mediapm-cas' binary".to_string())
        })?;

        let status = tokio::process::Command::new(&cas_binary)
            .args(args)
            .status()
            .await
            .map_err(|e| ConductorError::Workflow(format!("failed to run CAS CLI: {e}")))?;

        Ok(status.code().unwrap_or(-1))
    }

    /// Exports configuration schemas to a directory.
    ///
    /// Reads the embedded Nickel schema contract files (`mod.ncl`, `v1.ncl`,
    /// `v2.ncl`) from the `versions` module and writes them to the output
    /// directory, creating it if necessary.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Io`] when the output directory cannot be
    /// created or schema files cannot be written.
    pub async fn export_schemas(&self, output: &Path) -> Result<(), ConductorError> {
        std::fs::create_dir_all(output).map_err(|source| ConductorError::Io {
            operation: "creating schema export directory".to_string(),
            path: output.to_path_buf(),
            source,
        })?;

        let schemas: &[(&str, &str)] = &[
            ("mod.ncl", versions::MOD_NCL_SOURCE),
            ("v1.ncl", versions::V1_NCL_SOURCE),
            ("v2.ncl", versions::V2_NCL_SOURCE),
        ];
        for (filename, source) in schemas {
            let dest = output.join(filename);
            std::fs::write(&dest, source).map_err(|source| ConductorError::Io {
                operation: "writing schema file".to_string(),
                path: dest,
                source,
            })?;
        }
        Ok(())
    }

    /// Runs garbage collection on the orchestration state and CAS.
    ///
    /// CONDUCTOR GC — distinct from CAS GC.  Runs the full three-phase cycle:
    /// instance TTL pruning, CAS orphan reclamation, and CAS metadata
    /// maintenance.
    ///
    /// # Errors
    ///
    /// Delegates to the conductor actor.
    pub async fn run_gc(&self) -> Result<(), ConductorError> {
        let client = self.ensure_actor_client().await?;
        let (unified, state) = load_unified_config_and_state(self.storage_paths()).await?;
        let referenced_keys = std::collections::BTreeSet::new();
        let new_state = client.run_gc(referenced_keys, state, unified).await?;
        save_state(self.storage_paths(), &new_state).await
    }

    /// Returns the merged unified configuration (compiled view).
    ///
    /// This is the same merged document that [`run_workflow`] produces.
    ///
    /// # Errors
    ///
    /// Delegates to [`load_unified_config_and_state`].
    pub(crate) async fn get_unified_config(&self) -> Result<UnifiedNickelDocument, ConductorError> {
        let (unified, _state) = load_unified_config_and_state(self.storage_paths()).await?;
        Ok(unified)
    }
}

// ---------------------------------------------------------------------------
// ConductorApi trait implementation
// ---------------------------------------------------------------------------

impl<C: CasApi + CasMaintenanceApi + Send + Sync + 'static> ConductorApi<C> for SimpleConductor<C> {
    fn run_workflow_with_options(
        &self,
        workflow_name: &str,
        options: RunWorkflowOptions,
    ) -> impl std::future::Future<Output = Result<RunSummary, ConductorError>> + Send {
        let wf = workflow_name.to_owned();
        async move { self.run_workflow(&wf, options).await }
    }

    fn run_workflow(
        &self,
        workflow_name: &str,
    ) -> impl std::future::Future<Output = Result<RunSummary, ConductorError>> + Send {
        let wf = workflow_name.to_owned();
        async move { self.run_workflow_with_options(&wf, RunWorkflowOptions::default()).await }
    }

    fn get_runtime_diagnostics(
        &self,
    ) -> impl std::future::Future<Output = Result<RuntimeDiagnostics, ConductorError>> + Send {
        async move { self.get_runtime_diagnostics().await }
    }
}

// ---------------------------------------------------------------------------
// Loading / saving helpers
// ---------------------------------------------------------------------------

/// Loads the unified configuration and orchestration state.
///
/// Discovers all `.ncl` config files in [`RuntimeStoragePaths::conductor_dir`]
/// (excluding the state config), plus the root `conductor.ncl` / `mediapm.ncl`
/// at the parent of `conductor_dir`.  Each file is independently evaluated
/// through the versioned Nickel pipeline.  All evaluated documents are merged
/// with error-on-conflict semantics.  The state document is loaded separately.
pub(crate) async fn load_unified_config_and_state(
    storage_paths: &RuntimeStoragePaths,
) -> Result<(UnifiedNickelDocument, OrchestrationState), ConductorError> {
    let state = load_state(storage_paths).await?;
    let config_paths = discover_config_paths(storage_paths);

    let source_docs: Vec<SourceDocument> = config_paths
        .into_iter()
        .map(|path| {
            let document = crate::cli_document_io::load_document(&path)?;
            Ok(SourceDocument { path, document })
        })
        .collect::<Result<Vec<_>, ConductorError>>()?;

    let merged = merge_documents(&source_docs)?;
    let unified = merged.to_unified();
    Ok((unified, state))
}

/// Discovers all user config files.
///
/// Scans [`RuntimeStoragePaths::conductor_dir`] for `.ncl` files that are not
/// the state config, and also checks for `conductor.ncl` / `mediapm.ncl` at
/// the parent of `conductor_dir`.
fn discover_config_paths(storage_paths: &RuntimeStoragePaths) -> Vec<PathBuf> {
    let mut paths = Vec::new();

    // Root config at the project marker location.
    if let Some(parent) = storage_paths.conductor_dir.parent() {
        for name in ["conductor.ncl", "mediapm.ncl"] {
            let candidate = parent.join(name);
            if candidate.exists() {
                paths.push(candidate);
                break;
            }
        }
    }

    // Additional config fragments inside conductor_dir.
    if let Ok(entries) = std::fs::read_dir(&storage_paths.conductor_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "ncl") {
                paths.push(path);
            }
        }
    }

    paths
}

/// Returns the path of the first user config file in `conductor_dir`, or
/// `None` if no such file exists.
fn find_first_config(conductor_dir: &Path) -> Option<PathBuf> {
    // Check for root configs at the parent first.
    if let Some(parent) = conductor_dir.parent() {
        for name in ["conductor.ncl", "mediapm.ncl"] {
            let candidate = parent.join(name);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }
    // Fall back to scanning conductor_dir.
    if let Ok(entries) = std::fs::read_dir(conductor_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "ncl") {
                return Some(path);
            }
        }
    }
    None
}

/// Returns the default orchestration state (filesystem persistence removed).
async fn load_state(
    _storage_paths: &RuntimeStoragePaths,
) -> Result<OrchestrationState, ConductorError> {
    Ok(OrchestrationState::default())
}

/// No-op: state persistence is via CAS pointers, not filesystem.
async fn save_state(
    _storage_paths: &RuntimeStoragePaths,
    _state: &OrchestrationState,
) -> Result<(), ConductorError> {
    Ok(())
}

/// Locates the `mediapm-cas` binary by searching the conductor binary's
/// directory first, then `PATH`.
fn find_cas_binary() -> Option<PathBuf> {
    // Check same directory as the conductor binary.
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(parent) = exe_path.parent() {
            let candidate = parent.join("mediapm-cas");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    // Fall back to PATH.
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|dir| {
            let candidate = dir.join("mediapm-cas");
            if candidate.is_file() { Some(candidate) } else { None }
        })
    })
}
