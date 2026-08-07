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
use crate::config::documents::{NickelDocument, SourceDocument, merge_documents};
use crate::config::versions;
use crate::error::ConductorError;
use crate::orchestration::node::ConductorActorClient;
use crate::orchestration::protocol::{UnifiedNickelDocument, find_tool_by_name};
use crate::state::ConductorState;

/// Concrete facade over the conductor orchestration runtime.
///
/// Wraps a lazily initialized [`ConductorActorClient`] (which itself manages a
/// [`WorkflowCoordinator`] actor) and exposes all CLI-required operations.
///
/// Persists [`ConductorState`] across workflow runs so that subsequent
/// runs can benefit from cached tool-call instances.
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
    /// Persisted orchestration state, shared across workflow runs.
    state: std::sync::Mutex<ConductorState>,
}

impl<C> SimpleConductor<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    /// Creates a new conductor facade.
    #[must_use]
    pub fn new(storage_paths: RuntimeStoragePaths, cas: C) -> Self {
        Self {
            cas: Arc::new(cas),
            actor_client: OnceCell::new(),
            storage_paths,
            state: std::sync::Mutex::new(ConductorState::default()),
        }
    }

    /// Returns or initialises the conductor actor client.
    async fn ensure_actor_client(&self) -> Result<&ConductorActorClient, ConductorError> {
        self.actor_client
            .get_or_try_init(|| async {
                crate::orchestration::node::spawn_conductor_actor(
                    self.cas.clone(),
                    self.storage_paths.conductor_tmp_dir.clone(),
                )
                .await
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
    /// Persists the orchestration state across runs so that repeated
    /// deterministic workflows hit the cache on subsequent calls.
    ///
    /// # Errors
    ///
    /// Delegates to the conductor actor; returns an error when delivery or
    /// execution fails.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    pub async fn run_workflow(
        &self,
        workflow_name: &str,
        options: RunWorkflowOptions,
    ) -> Result<RunSummary, ConductorError> {
        let client = self.ensure_actor_client().await?;
        let (unified, fresh_state) =
            load_unified_config_and_state_async(&*self.cas, self.storage_paths()).await?;
        // Apply conductor runtime config defaults to options
        let options = {
            let mut opts = options;
            if !opts.retry_impure && unified.runtime.retry_impure {
                opts.retry_impure = true;
            }
            opts
        };
        // Seed the in-memory state from the freshly loaded file state when no
        // session state exists yet, so the actor receives the latest cached
        // instances from disk.
        {
            let mut guard = self.state.lock().expect("state lock");
            if guard.tool_call_instances.is_empty() {
                *guard = fresh_state;
            }
        }
        // Take the persisted state (or default if empty) so the actor
        // receives the latest cached instances from the previous run.
        let state = {
            let mut guard = self.state.lock().expect("state lock");
            if guard.tool_call_instances.is_empty() {
                ConductorState::default()
            } else {
                std::mem::take(&mut *guard)
            }
        };
        let (summary, updated_state) =
            client.run_workflow(workflow_name, options, unified, state).await?;
        *self.state.lock().expect("state lock") = updated_state.clone();
        // Persist state after each run so later sessions resume from disk.
        save_state_file(self.storage_paths(), &updated_state)?;
        Ok(summary)
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

    /// Loads persisted conductor state from disk into the in-memory mutex when
    /// it is still empty, migrating v1 envelopes through CAS when needed.
    ///
    /// # Errors
    ///
    /// Returns an error when the state file cannot be read or migration fails.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    pub async fn ensure_persisted_state_loaded(&self) -> Result<(), ConductorError> {
        if self.state.lock().expect("state lock").tool_call_instances.is_empty()
            && let Some(state) = load_state_file_async(&*self.cas, self.storage_paths()).await?
        {
            *self.state.lock().expect("state lock") = state;
        }
        Ok(())
    }

    /// Returns the current orchestration state (from in-memory persistence).
    ///
    /// Call [`ensure_persisted_state_loaded`] before this when the conductor
    /// may need to read or migrate a v1 state file from disk.
    ///
    /// # Errors
    ///
    /// Currently infallible; returns the in-memory orchestration state clone.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    pub fn get_state(&self) -> Result<ConductorState, ConductorError> {
        Ok(self.state.lock().expect("state lock").clone())
    }

    /// Replaces the persisted orchestration state.
    ///
    /// # Errors
    ///
    /// Currently infallible.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    pub fn replace_resolved_state(&self, new_state: ConductorState) -> Result<(), ConductorError> {
        *self.state.lock().expect("state lock") = new_state;
        Ok(())
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
    pub fn add_tool_config(
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
                    let envelope = crate::cli_document_io::load_document_envelope(&path)?;
                    Ok(SourceDocument { path, envelope })
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

        let tool = crate::config::ToolSpec {
            kind: crate::config::ToolKindSpec::Executable {
                command: executable.map_or(vec![], |cmd| vec![cmd.to_string()]),
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            name: name.to_string(),
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            runtime: crate::config::ToolRuntime {
                content_map,
                ..crate::config::ToolRuntime::default()
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
        Ok(self.cas.delete(*hash).await?)
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
    pub fn remove_tool_config(&self, name: &str, _metadata: bool) -> Result<(), ConductorError> {
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
        let unified = load_unified_config(self.storage_paths())?;

        let tool_spec = find_tool_by_name(&unified.tools, tool).ok_or_else(|| {
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
    pub fn export_schemas(&self, output: &Path) -> Result<(), ConductorError> {
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
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    pub async fn run_gc(&self) -> Result<(), ConductorError> {
        let client = self.ensure_actor_client().await?;
        let unified = load_unified_config(self.storage_paths())?;
        let referenced_keys = std::collections::BTreeSet::new();
        let state = self.state.lock().expect("state lock").clone();
        let new_state = client.run_gc(referenced_keys, state, unified).await?;
        *self.state.lock().expect("state lock") = new_state;
        Ok(())
    }

    /// Returns the merged unified configuration (compiled view).
    ///
    /// This is the same merged document that [`run_workflow`] produces.
    ///
    /// # Errors
    ///
    /// Delegates to [`load_unified_config`].
    pub(crate) fn get_unified_config(&self) -> Result<UnifiedNickelDocument, ConductorError> {
        load_unified_config(self.storage_paths())
    }

    /// Deterministically stops the conductor actor and releases every
    /// actor-owned resource: the step-worker pool, the background GC task,
    /// and the CAS clone held by the coordinator state.
    ///
    /// Awaited, this guarantees the underlying CAS handle is no longer
    /// referenced by the conductor actor, so a filesystem-backed CAS can be
    /// reopened by a later instance. It is a no-op when the actor was never
    /// spawned (for example, config-only usage).
    ///
    /// # Errors
    ///
    /// Returns an error when the shutdown RPC or the bounded actor stop wait
    /// fails.
    pub async fn shutdown(&self) -> Result<(), ConductorError> {
        if let Some(client) = self.actor_client.get() {
            client.shutdown().await?;
        }
        Ok(())
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

    #[allow(clippy::manual_async_fn)]
    fn get_runtime_diagnostics(
        &self,
    ) -> impl std::future::Future<Output = Result<RuntimeDiagnostics, ConductorError>> + Send {
        async move { self.get_runtime_diagnostics().await }
    }
}

// ---------------------------------------------------------------------------
// Loading / saving helpers
// ---------------------------------------------------------------------------

/// Loads the merged unified configuration (no state file read).
fn load_unified_config(
    storage_paths: &RuntimeStoragePaths,
) -> Result<UnifiedNickelDocument, ConductorError> {
    let config_paths = discover_config_paths(storage_paths);

    let source_docs: Vec<SourceDocument> = config_paths
        .into_iter()
        .map(|path| {
            let envelope = crate::cli_document_io::load_document_envelope(&path)?;
            Ok(SourceDocument { path, envelope })
        })
        .collect::<Result<Vec<_>, ConductorError>>()?;

    let merged = merge_documents(&source_docs)?;
    Ok(merged.to_unified())
}

/// Loads the unified configuration and persisted orchestration state.
async fn load_unified_config_and_state_async<C: CasApi>(
    cas: &C,
    storage_paths: &RuntimeStoragePaths,
) -> Result<(UnifiedNickelDocument, ConductorState), ConductorError> {
    let unified = load_unified_config(storage_paths)?;
    let state = load_state_file_async(cas, storage_paths).await?.unwrap_or_default();
    Ok((unified, state))
}

/// Loads the persisted conductor state from the state file, if present.
async fn load_state_file_async<C: CasApi>(
    cas: &C,
    storage_paths: &RuntimeStoragePaths,
) -> Result<Option<ConductorState>, ConductorError> {
    let path = &storage_paths.state_file_path;
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)
        .map_err(|source| ConductorError::io("reading state file", path, source))?;
    let version = crate::state::versions::peek_version_marker(&bytes)?;
    let state = crate::state::versions::decode_state_json_with_cas(cas, &bytes).await?;
    if crate::state::versions::is_orchestration_state_version_v1(version) {
        save_state_file(storage_paths, &state)?;
    }
    Ok(Some(state))
}

/// Loads a v2 state file synchronously (unit tests).
#[cfg(test)]
fn load_state_file(
    storage_paths: &RuntimeStoragePaths,
) -> Result<Option<ConductorState>, ConductorError> {
    let path = &storage_paths.state_file_path;
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)
        .map_err(|source| ConductorError::io("reading state file", path, source))?;
    crate::state::versions::decode_state_json(&bytes).map(Some)
}

/// Persists the conductor state to the state file as pretty JSON.
fn save_state_file(
    storage_paths: &RuntimeStoragePaths,
    state: &ConductorState,
) -> Result<(), ConductorError> {
    let path = &storage_paths.state_file_path;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            ConductorError::io("creating state file directory", parent, source)
        })?;
    }
    let bytes = crate::state::versions::encode_state_json(state)?;
    std::fs::write(path, bytes)
        .map_err(|source| ConductorError::io("writing state file", path, source))
}

/// Discovers all user config files.
///
/// Honors explicit config-document paths when set; otherwise scans
/// [`RuntimeStoragePaths::conductor_dir`] for `.ncl` files and checks for
/// `conductor.ncl` at the parent of `conductor_dir`.
fn discover_config_paths(storage_paths: &RuntimeStoragePaths) -> Vec<PathBuf> {
    // Explicit doc paths (mediapm passes `mediapm.conductor.ncl` +
    // `mediapm.conductor.generated.ncl`): load exactly those, in order,
    // skipping missing files so optional user docs do not fail loading.
    if !storage_paths.config_doc_paths.is_empty() {
        return storage_paths
            .config_doc_paths
            .iter()
            .filter(|path| path.exists())
            .cloned()
            .collect();
    }

    let mut paths = Vec::new();

    // Standalone root config at the project marker location.
    if let Some(parent) = storage_paths.conductor_dir.parent() {
        for name in ["conductor.ncl"] {
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
            if path.extension().is_some_and(|e| e == "ncl") {
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
        let candidate = parent.join("conductor.ncl");
        if candidate.exists() {
            return Some(candidate);
        }
    }
    // Fall back to scanning conductor_dir.
    if let Ok(entries) = std::fs::read_dir(conductor_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "ncl") {
                return Some(path);
            }
        }
    }
    None
}

/// Locates the `mediapm-cas` binary by searching the conductor binary's
/// directory first, then `PATH`.
fn find_cas_binary() -> Option<PathBuf> {
    // Check same directory as the conductor binary.
    if let Ok(exe_path) = std::env::current_exe()
        && let Some(parent) = exe_path.parent()
    {
        let candidate = parent.join("mediapm-cas");
        if candidate.is_file() {
            return Some(candidate);
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

impl<C: CasApi + CasMaintenanceApi + Send + Sync + 'static> Drop for SimpleConductor<C> {
    /// Best-effort stop of the conductor actor on drop.
    ///
    /// Sends a fire-and-forget stop signal so the actor (and its linked step
    /// workers) begin shutting down even when the caller never awaited
    /// [`SimpleConductor::shutdown`]. This cannot deterministically wait for
    /// teardown — blocking from inside an async runtime context is
    /// forbidden — so callers that need deterministic release (for example,
    /// tests reopening a filesystem CAS) must await
    /// [`SimpleConductor::shutdown`] first.
    fn drop(&mut self) {
        if let Some(client) = self.actor_client.get() {
            client.stop();
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::state::ConductorState;

    /// Explicit config-doc paths are used as-is (existing files only), in
    /// order; missing explicit paths are skipped.
    #[test]
    fn discover_config_paths_uses_explicit_paths_in_order() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let conductor_dir = tmp.path().join("conductor_dir");
        let paths = RuntimeStoragePaths::new(&conductor_dir);

        let user = tmp.path().join("user.ncl");
        let generated = tmp.path().join("generated.ncl");
        std::fs::write(&user, "{}").expect("write user.ncl");

        let paths = paths.with_config_paths(
            vec![user.clone(), generated.clone()],
            tmp.path().join("state.json"),
        );
        assert_eq!(discover_config_paths(&paths), vec![user]);
    }

    /// Standalone discovery never picks up `mediapm.ncl` at the parent.
    #[test]
    fn discover_config_paths_does_not_discover_mediapm_ncl() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let conductor_dir = tmp.path().join("conductor_dir");
        std::fs::create_dir_all(&conductor_dir).expect("create conductor_dir");

        let mediapm = tmp.path().join("mediapm.ncl");
        let conductor = conductor_dir.join("conductor.ncl");
        std::fs::write(&mediapm, "{}").expect("write mediapm.ncl");
        std::fs::write(&conductor, "{}").expect("write conductor.ncl");

        let paths = RuntimeStoragePaths::new(&conductor_dir);
        let found = discover_config_paths(&paths);
        assert!(found.contains(&conductor));
        assert!(!found.contains(&mediapm));
    }

    /// `find_first_config` prefers `conductor.ncl` over `mediapm.ncl` at the
    /// parent.
    #[test]
    fn find_first_config_drops_mediapm_ncl() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let conductor_dir = tmp.path().join("conductor_dir");

        let mediapm = tmp.path().join("mediapm.ncl");
        let conductor = tmp.path().join("conductor.ncl");
        std::fs::write(&mediapm, "{}").expect("write mediapm.ncl");
        std::fs::write(&conductor, "{}").expect("write conductor.ncl");

        assert_eq!(find_first_config(&conductor_dir), Some(conductor));
    }

    /// A saved state file round-trips through `save_state_file` /
    /// `load_state_file`.
    #[test]
    fn state_file_roundtrip() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(&tmp.path().join("conductor_dir"));

        let state = ConductorState::new_empty();
        save_state_file(&paths, &state).expect("save state file");
        let loaded = load_state_file(&paths).expect("load state file");
        assert_eq!(loaded, Some(state));
    }

    /// A missing state file loads as `None`, not an error.
    #[test]
    fn state_file_missing_returns_none() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(&tmp.path().join("conductor_dir"));
        assert_eq!(load_state_file(&paths).expect("load missing state file"), None);
    }

    /// A corrupt state file surfaces as an error rather than a silent default.
    #[test]
    fn state_file_corrupt_errors() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(&tmp.path().join("conductor_dir"));
        std::fs::create_dir_all(&paths.conductor_dir).expect("create conductor dir");
        std::fs::write(&paths.state_file_path, b"not json").expect("write corrupt state");
        assert!(load_state_file(&paths).is_err());
    }

    /// `with_config_paths` relocates the state file to the custom path.
    #[test]
    fn state_file_path_is_configurable() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let custom = tmp.path().join("custom").join("state.json");
        let paths = RuntimeStoragePaths::new(&tmp.path().join("conductor_dir"))
            .with_config_paths(Vec::new(), custom.clone());

        let state = ConductorState::new_empty();
        save_state_file(&paths, &state).expect("save state file");
        assert!(custom.exists());
        assert_eq!(load_state_file(&paths).expect("load state file"), Some(state));
    }
}
