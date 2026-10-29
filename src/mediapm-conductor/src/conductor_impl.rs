//! Facade over the conductor orchestration runtime.
//!
//! [`Conductor`] is the concrete implementation of the conductor API.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use mediapm_cas::{BackgroundMaintenanceGuard, CasApi, CasMaintenanceApi, ConstraintApi, Hash};
use tokio::sync::OnceCell;

use crate::api::{
    ConductorApi, InstanceMarkerCounts, RunSummary, RunWorkflowOptions, RuntimeDiagnostics,
    RuntimeStoragePaths,
};
use crate::config::documents::NickelDocument;
use crate::config::versions;
use crate::error::ConductorError;
use crate::orchestration::node::ConductorActorClient;
use crate::orchestration::protocol::{UnifiedNickelDocument, find_tool_by_name};
use crate::state::{ConductorState, StepRef};

/// Concrete facade over the conductor orchestration runtime.
///
/// Wraps a lazily initialized `ConductorActorClient` and persists
/// [`ConductorState`] across runs so repeated deterministic workflows hit the cache.
pub struct Conductor<C>
where
    C: CasApi + CasMaintenanceApi + ConstraintApi + Send + Sync + 'static,
{
    /// Shared CAS store.
    cas: Arc<C>,
    /// Lazily spawned conductor actor client.
    actor_client: OnceCell<ConductorActorClient>,
    /// Resolved runtime paths.
    storage_paths: RuntimeStoragePaths,
    /// Persisted orchestration state, shared across workflow runs and with the
    /// background maintenance task, which keeps its own handle on it.
    state: Arc<std::sync::Mutex<ConductorState>>,
    /// Serializes the operations that read, modify, and write back the whole
    /// state: a workflow run, a GC pass, and the background loop.
    ///
    /// Each of them holds the state out of the mutex for the length of an RPC,
    /// so two of them at once can each start from the same snapshot and the
    /// later write erases the earlier one's records. Holding this for the whole
    /// operation makes the last write the current one.
    state_gate: Arc<tokio::sync::Mutex<()>>,
    /// Cancellation guard for the background maintenance task, if it was
    /// started. Dropping the facade drops the guard, which aborts the task.
    background_maintenance: std::sync::Mutex<Option<BackgroundMaintenanceGuard>>,
    /// Seconds the background maintenance loop waits after each pass.
    ///
    /// Production leaves this at
    /// [`DEFAULT_CONDUCTOR_GC_INTERVAL_SECONDS`](crate::defaults::DEFAULT_CONDUCTOR_GC_INTERVAL_SECONDS);
    /// tests shorten it because a day is not a test duration.
    background_gc_interval_secs: u64,
}

impl<C> Conductor<C>
where
    C: CasApi + CasMaintenanceApi + ConstraintApi + Send + Sync + 'static,
{
    /// Creates a new conductor facade.
    #[must_use]
    pub fn new(storage_paths: RuntimeStoragePaths, cas: C) -> Self {
        Self {
            cas: Arc::new(cas),
            actor_client: OnceCell::new(),
            storage_paths,
            state: Arc::new(std::sync::Mutex::new(ConductorState::default())),
            state_gate: Arc::new(tokio::sync::Mutex::new(())),
            background_maintenance: std::sync::Mutex::new(None),
            background_gc_interval_secs: crate::defaults::DEFAULT_CONDUCTOR_GC_INTERVAL_SECONDS,
        }
    }

    /// Returns or initialises the conductor actor client.
    ///
    /// Spawning the actor is also what starts background maintenance: a facade
    /// that never runs a workflow holds nothing worth collecting, and one that
    /// does gets its collection for as long as it lives.
    async fn ensure_actor_client(&self) -> Result<&ConductorActorClient, ConductorError> {
        ensure_runtime_root(&self.storage_paths)?;
        let client = self
            .actor_client
            .get_or_try_init(|| async {
                crate::orchestration::node::spawn_conductor_actor(
                    self.cas.clone(),
                    self.storage_paths.conductor_tmp_dir.clone(),
                )
                .await
            })
            .await?;
        self.start_background_maintenance();
        Ok(client)
    }

    /// Starts the background maintenance loop unless it is already running.
    ///
    /// The loop runs the full GC cycle — instance eviction, orphan reclamation,
    /// CAS metadata maintenance — and persists what it decided. Deleting a blob
    /// assumes every live byte is accounted for, and the loop's account comes
    /// from the document: every hash a document names is declared in
    /// `external_data` (see `NickelDocument::validate_external_data_invariant`),
    /// and every byte a run produced is a recorded instance output, so the root
    /// set the pass computes names the application's bytes as well as the
    /// conductor's own. Failures are logged and the next pass tries again —
    /// maintenance is not the reason a workflow fails.
    ///
    /// A pass is started as soon as the loop is, not after the first interval,
    /// so a long-lived process collects on the run that creates it.
    ///
    /// # Panics
    ///
    /// Panics if the guard slot is poisoned, which means a previous holder
    /// panicked while starting or stopping the task.
    fn start_background_maintenance(&self) {
        let mut slot = self.background_maintenance.lock().expect("background maintenance lock");
        if slot.as_ref().is_some_and(|guard| !guard.is_finished()) {
            return;
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_flag = cancelled.clone();
        let state = self.state.clone();
        let gate = self.state_gate.clone();
        let cas = self.cas.clone();
        let paths = self.storage_paths.clone();
        let interval = self.background_gc_interval_secs;
        let handle = tokio::spawn(async move {
            loop {
                if cancelled_flag.load(Ordering::Relaxed) {
                    break;
                }
                if runtime_root_is_gone(&paths) {
                    break;
                }
                if let Err(error) = maintenance_pass(&*cas, &paths, &state, &gate).await {
                    tracing::warn!("conductor background maintenance failed: {error}");
                }
                if cancelled_flag.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        });
        *slot = Some(BackgroundMaintenanceGuard::new(cancelled, handle));
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

    /// Runs a workflow and returns a summary.
    ///
    /// Persists the orchestration state across runs so repeated deterministic
    /// workflows hit the cache on subsequent calls.
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
        // One state-mutating operation at a time: the run replaces the recorded
        // instances wholesale, so a pass that snapshot the state underneath it
        // would write its snapshot back over the run's records.
        let _gate = self.state_gate.lock().await;
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
    /// Returns an error when the actor is unreachable.
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
    /// Call [`Self::ensure_persisted_state_loaded`] before this when the conductor
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

    /// Marks every recorded instance that served one of `steps` as not to be
    /// reused, and persists the state.
    ///
    /// This is the programmatic half of `conductor state invalidate-tool-call`,
    /// scoped by step rather than by instance key: the caller knows which steps
    /// it wants re-run, and the keys are content-derived — a caller cannot name
    /// an instance it has not already found. An instance is selected when any of
    /// the step refs it has served is in `steps`.
    ///
    /// A ref names a workflow as well as a step id, so marking one workflow's
    /// step leaves a same-named step of another workflow alone. That matters
    /// because step ids are unique only inside their workflow: mediapm names each
    /// step after its media, and nothing stops two documents from reusing a step
    /// id, so a bare-id match would mark work the caller never named.
    ///
    /// An instance that already carries a marker keeps the timestamp it has: the
    /// field says since when the bytes have been marked, so re-marking must not
    /// restart that clock.
    ///
    /// # Errors
    ///
    /// Returns an error when the persisted state cannot be loaded, or when the
    /// updated state cannot be written. A write failure leaves the in-memory
    /// state untouched, so the two never disagree about what was saved.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    pub async fn invalidate_instances_for_steps(
        &self,
        steps: &BTreeSet<StepRef>,
    ) -> Result<InstanceMarkerCounts, ConductorError> {
        self.set_instances_marker(steps, true).await
    }

    /// Clears the invalidation marker from every instance that served one of
    /// `steps`, and persists the state.
    ///
    /// The undo of [`Self::invalidate_instances_for_steps`]. Clearing a marker
    /// that is not there is counted, not an error: the caller asked for a state
    /// the record is already in.
    ///
    /// # Errors
    ///
    /// Returns an error when the persisted state cannot be loaded, or when the
    /// updated state cannot be written.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    pub async fn revalidate_instances_for_steps(
        &self,
        steps: &BTreeSet<StepRef>,
    ) -> Result<InstanceMarkerCounts, ConductorError> {
        self.set_instances_marker(steps, false).await
    }

    /// Applies one marker request to the recorded instances of `steps`.
    ///
    /// The state is loaded from disk first, because the caller may be the first
    /// thing the process does, and written back before the in-memory snapshot is
    /// replaced, because a failed write must not leave the process holding an
    /// edit the file does not have.
    ///
    /// # Errors
    ///
    /// Returns an error when the state cannot be loaded or written.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    async fn set_instances_marker(
        &self,
        steps: &BTreeSet<StepRef>,
        invalidated: bool,
    ) -> Result<InstanceMarkerCounts, ConductorError> {
        self.update_state(|state| {
            let mut counts = InstanceMarkerCounts::default();
            for instance in state.tool_call_instances.values_mut() {
                if instance.step_ids.is_disjoint(steps) {
                    continue;
                }
                let already_set = instance.invalidated_at.is_some() == invalidated;
                if already_set {
                    counts.unchanged += 1;
                    continue;
                }
                instance.invalidated_at =
                    if invalidated { Some(mediapm_utils::Timestamp::now()) } else { None };
                counts.changed += 1;
            }
            Ok(counts)
        })
        .await
    }

    /// Reads the persisted state, applies `edit`, and publishes the result.
    ///
    /// The one path for an edit that is not a workflow run: load from disk first
    /// (the caller may be the first thing the process does), hand the loaded
    /// snapshot to `edit`, save it, then replace the in-memory snapshot. The
    /// order is durable-first, so a failed write never leaves the process holding
    /// an edit the file does not have.
    ///
    /// Runs under [`Self::state_gate`], because an edit that only writes when it
    /// changed something is exactly what a concurrent pass can undo: the pass
    /// would write back the snapshot it read before the edit landed.
    ///
    /// # Errors
    ///
    /// Returns an error when the state cannot be loaded, `edit` fails, or the
    /// result cannot be written.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    pub(crate) async fn update_state<T, F>(&self, edit: F) -> Result<T, ConductorError>
    where
        F: FnOnce(&mut ConductorState) -> Result<T, ConductorError>,
    {
        let _gate = self.state_gate.lock().await;
        self.ensure_persisted_state_loaded().await?;
        let mut state = self.get_state()?;
        let outcome = edit(&mut state)?;
        self.save_state(&state)?;
        self.replace_resolved_state(state)?;
        Ok(outcome)
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
            let merged = versions::merge_document_sources(&config_paths)?;
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
            version: None,
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
                // A tool added through the API stages its payload by copying the
                // content map, which is the strategy that needs no payload
                // root: this method is given none.
                payload_staging: crate::config::ConfigPayloadStaging::Copy,
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
    /// Writes the embedded Nickel contract sources — the registry `mod.ncl` and
    /// every version file it imports — into `output`, creating it if necessary.
    ///
    /// A reader of the directory therefore sees exactly the sources this build
    /// evaluates a document against, and the directory evaluates on its own.
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

        // The registry imports its version files by name, so the export has to
        // carry every one of them: a directory holding `mod.ncl` without a file
        // it imports cannot be evaluated, which is the only reason to write it.
        for (filename, source) in versions::contract_sources() {
            let dest = output.join(filename);
            mediapm_utils::fs::write_bytes_atomically(&dest, source.as_bytes()).map_err(
                |source| ConductorError::Io {
                    operation: "writing schema file".to_string(),
                    path: dest,
                    source,
                },
            )?;
        }
        Ok(())
    }

    /// Runs garbage collection on the orchestration state and CAS.
    ///
    /// CONDUCTOR GC — distinct from CAS GC.  Runs the full three-phase cycle:
    /// instance reachability pruning, CAS orphan reclamation, and CAS metadata
    /// maintenance.
    ///
    /// The report says what the pass decided, not only what it counted: a pass
    /// that refused to walk reports zero instances for want of a judgement,
    /// while a pass over an empty library reports zero for want of a library.
    /// The caller prints which happened, because the counts cannot say.
    ///
    /// Which instances survive is decided inside the pass, from the seeds each
    /// successful run recorded and the input edges their records name.  A
    /// caller cannot supply a key set: the only caller that ever did passed an
    /// empty one, which would have made every sweep a full wipe.
    ///
    /// The pass deletes the blobs of every instance it evicts, so its result is
    /// written to the state file before this returns: the eviction is the only
    /// record that those bytes are gone, and a later process reads the file.
    ///
    /// # Errors
    ///
    /// Delegates to the conductor actor, and to the state-file write.
    ///
    /// This is the full cycle, which includes deleting every CAS blob no record
    /// names. It assumes the caller chose the moment: an operator running
    /// `conductor gc` asserts that nothing is in flight, which is why this path
    /// is not the one the background loop runs — a timer cannot assert that, and
    /// a store shared with an application holds bytes that are staged and not
    /// yet recorded.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory orchestration state mutex is poisoned.
    pub async fn run_gc(&self) -> Result<crate::gc::ConductorGcReport, ConductorError> {
        ensure_runtime_root(&self.storage_paths)?;
        let client = self.ensure_actor_client().await?;
        let unified = load_unified_config(self.storage_paths())?;
        let _gate = self.state_gate.lock().await;
        let state = self.state.lock().expect("state lock").clone();
        let (new_state, report) = client.run_gc(state, unified).await?;
        self.save_state(&new_state)?;
        *self.state.lock().expect("state lock") = new_state;
        Ok(report)
    }

    /// Writes one state snapshot to the configured state file.
    ///
    /// The single saver: the workflow run, the GC pass, the background loop, and
    /// the marker commands all publish state through it, so the on-disk shape and
    /// the way it is replaced cannot diverge between them.
    ///
    /// # Errors
    ///
    /// Returns an error when the state cannot be encoded or written.
    pub(crate) fn save_state(&self, state: &ConductorState) -> Result<(), ConductorError> {
        ensure_runtime_root(self.storage_paths())?;
        save_state_file(self.storage_paths(), state)
    }

    /// Returns the merged unified configuration (compiled view).
    ///
    /// This is the same merged document that [`Self::run_workflow`] produces.
    ///
    /// # Errors
    ///
    /// Delegates to [`load_unified_config`].
    ///
    /// The `cli` feature's document-printing subcommands are its only
    /// consumers, so it is compiled with that feature rather than left dead in
    /// a build without it.
    #[cfg(feature = "cli")]
    pub(crate) fn get_unified_config(&self) -> Result<UnifiedNickelDocument, ConductorError> {
        load_unified_config(self.storage_paths())
    }

    /// Deterministically stops the conductor actor and the background
    /// maintenance task, releasing every resource they hold: the step-worker
    /// pool, the coordinator's CAS clone, and the maintenance loop's own.
    ///
    /// Awaited, this guarantees no background pass is running and the
    /// underlying CAS handle is no longer referenced by the actor, so a
    /// filesystem-backed CAS can be reopened by a later instance. It is safe to
    /// call when neither was started (for example, config-only usage).
    ///
    /// # Errors
    ///
    /// Returns an error when the shutdown RPC or the bounded actor stop wait
    /// fails.
    ///
    /// # Panics
    ///
    /// Panics if the guard slot is poisoned.
    pub async fn shutdown(&self) -> Result<(), ConductorError> {
        let guard = self.background_maintenance.lock().expect("background maintenance lock").take();
        if let Some(guard) = guard {
            guard.shutdown().await;
        }
        if let Some(client) = self.actor_client.get() {
            client.shutdown().await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConductorApi trait implementation
// ---------------------------------------------------------------------------

impl<C: CasApi + CasMaintenanceApi + ConstraintApi + Send + Sync + 'static> ConductorApi<C>
    for Conductor<C>
{
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

/// Runs one background maintenance pass and publishes its result.
///
/// The pass is the full cycle, [`crate::gc::run_conductor_gc`]: instance
/// eviction decided by the recorded seeds, orphan reclamation, then CAS metadata
/// maintenance. It may delete payload bytes, and what makes that safe on a timer
/// is the document: every hash a document names is declared in `external_data`
/// (enforced at load by
/// `NickelDocument::validate_external_data_invariant`) and every
/// byte a run produced is a recorded instance output, so the root set names what
/// the store holds for both the conductor and the application sharing it. The
/// result is saved before the in-memory snapshot is replaced: the evictions and
/// deletions it made are what the next process must read, and the file is where
/// it reads them.
///
/// The config is loaded after the gate is taken, immediately before the cycle
/// that acts on it. The root set comes from that document, so a document read
/// before the gate could be one a concurrent reconcile has already replaced:
/// the pass would then delete bytes a newer declaration names. Holding the gate
/// across the read is what keeps the pass acting on the document it decided
/// from. A reconcile is not itself gated, so the two can still interleave
/// between the read and the reclamation; an application that declares a hash and
/// stages its bytes under that declaration is not affected by the gap.
///
/// The gate covers the state work too, so a run that is recording instances and
/// a pass that is evicting them never start from the same snapshot.
///
/// An empty in-memory snapshot is filled from the file first. A facade whose
/// actor was spawned for something other than a workflow run has never loaded
/// the state, and passing its empty snapshot would write it over the records on
/// disk.
///
/// # Errors
///
/// Returns a config, state-load, GC, or write error. The caller logs it and
/// waits for the next interval; maintenance never fails a workflow.
async fn maintenance_pass<C>(
    cas: &C,
    paths: &RuntimeStoragePaths,
    state: &std::sync::Mutex<ConductorState>,
    gate: &tokio::sync::Mutex<()>,
) -> Result<(), ConductorError>
where
    C: CasApi + CasMaintenanceApi,
{
    let _gate = gate.lock().await;
    let unified = load_unified_config(paths)?;
    let mut snapshot = state.lock().expect("state lock").clone();
    if snapshot.tool_call_instances.is_empty()
        && let Some(persisted) = load_state_file_async(cas, paths).await?
    {
        snapshot = persisted;
    }
    crate::gc::run_conductor_gc(
        cas,
        &mut snapshot,
        &unified,
        crate::defaults::DEFAULT_CONDUCTOR_GC_TTL_SECONDS,
    )
    .await?;
    save_state_file(paths, &snapshot)?;
    *state.lock().expect("state lock") = snapshot;
    Ok(())
}

/// Loads the merged unified configuration (no state file read).
fn load_unified_config(
    storage_paths: &RuntimeStoragePaths,
) -> Result<UnifiedNickelDocument, ConductorError> {
    let config_paths = discover_config_paths(storage_paths);

    let merged = versions::merge_document_sources(&config_paths)?;
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
///
/// The document is published atomically, so a reader in another process — or the
/// next load after a crash — sees either the previous state or the new one, never
/// a prefix of either. The state file's directory must exist; a foreground entry
/// point creates it through [`ensure_runtime_root`].
///
/// # Errors
///
/// Returns an error when the state cannot be encoded, and an I/O error when the
/// file cannot be written — including the case where its directory is gone.
fn save_state_file(
    storage_paths: &RuntimeStoragePaths,
    state: &ConductorState,
) -> Result<(), ConductorError> {
    let bytes = crate::state::versions::encode_state_json(state)?;
    write_bytes_atomically(&storage_paths.state_file_path, &bytes)
}

/// Publishes `bytes` at `path` by writing a sibling file and renaming it over
/// the target.
///
/// The write itself lives in [`mediapm_utils::fs`], which the mediapm state save
/// shares; this wrapper only maps the I/O failure into [`ConductorError`] with
/// the conductor's own operation label.
///
/// # Errors
///
/// Returns an I/O error when the document cannot be published, including the case
/// where its directory is gone.
fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> Result<(), ConductorError> {
    mediapm_utils::fs::write_bytes_atomically(path, bytes)
        .map_err(|source| ConductorError::io("writing state file", path, source))
}

/// Creates the runtime root the state file lives in.
///
/// A root belongs to the actor that owns it, so only a foreground entry point —
/// an operation the caller asked for — creates one. The write path deliberately
/// does not: a save that could create its own parent would resurrect a directory
/// its owner had already removed, which is how a background pass used to leave an
/// orphan holding one state file.
///
/// # Errors
///
/// Returns an I/O error when the directory cannot be created, and a workflow
/// error when the state file path names no parent to create.
fn ensure_runtime_root(paths: &RuntimeStoragePaths) -> Result<(), ConductorError> {
    let parent = state_file_parent(paths)?;
    std::fs::create_dir_all(&parent)
        .map_err(|source| ConductorError::io("creating the conductor runtime root", parent, source))
}

/// The directory the state file lives in.
///
/// # Errors
///
/// Returns a workflow error when the state file path names no parent, which
/// would leave a save nowhere to publish.
fn state_file_parent(paths: &RuntimeStoragePaths) -> Result<PathBuf, ConductorError> {
    paths.state_file_path.parent().map(Path::to_path_buf).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "the state file path '{}' has no parent directory",
            paths.state_file_path.display()
        ))
    })
}

/// Whether the runtime root the maintenance loop maintains is gone.
///
/// A gone root means the owner is gone and the store went with it, so the loop
/// stops rather than warning on every interval for the rest of the process's
/// life.
fn runtime_root_is_gone(paths: &RuntimeStoragePaths) -> bool {
    !state_file_parent(paths).is_ok_and(|parent| parent.is_dir())
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

impl<C: CasApi + CasMaintenanceApi + ConstraintApi + Send + Sync + 'static> Drop for Conductor<C> {
    /// Best-effort stop of the conductor actor on drop.
    ///
    /// Sends a fire-and-forget stop signal so the actor (and its linked step
    /// workers) begin shutting down even when the caller never awaited
    /// [`Conductor::shutdown`]. This cannot deterministically wait for
    /// teardown — blocking from inside an async runtime context is
    /// forbidden — so callers that need deterministic release (for example,
    /// tests reopening a filesystem CAS) must await
    /// [`Conductor::shutdown`] first.
    fn drop(&mut self) {
        if let Some(client) = self.actor_client.get() {
            client.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

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
    ///
    /// The root is created the way a foreground entry point creates it: a save
    /// requires the directory to exist rather than creating it, so that a pass
    /// running past its owner's teardown fails instead of resurrecting the tree.
    #[test]
    fn state_file_roundtrip() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(&tmp.path().join("conductor_dir"));
        ensure_runtime_root(&paths).expect("create runtime root");

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

    /// Builds a conductor over a scratch directory whose state file already
    /// holds `state`, returning the conductor and the directory that must
    /// outlive it.
    fn conductor_with_state(
        state: &ConductorState,
    ) -> (tempfile::TempDir, Conductor<mediapm_cas::InMemoryCas>) {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(dir.path());
        save_state_file(&paths, state).expect("seed state file");
        (dir, Conductor::new(paths, mediapm_cas::new_in_memory_cas()))
    }

    /// A step ref inside `workflow_id`, so a test names the pair an
    /// invalidation request is matched against.
    fn step_ref(workflow_id: &str, step_id: &str) -> StepRef {
        StepRef { workflow_id: workflow_id.to_string(), step_id: step_id.to_string() }
    }

    /// Builds a state with one recorded instance that served `step`.
    ///
    /// The key is derived from the pair, so two instances serving a same-named
    /// step of two workflows get distinct keys and the test compares what the
    /// marker reached rather than which record it could have reached.
    fn state_with_step_instance(step: &StepRef) -> (ConductorState, Hash) {
        let key = Hash::from_content(format!("{}/{}", step.workflow_id, step.step_id).as_bytes());
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            key,
            crate::state::ToolCallInstance {
                tool_call_id: "echo@v1".to_string(),
                impure: false,
                invalidated_at: None,
                executed_at: mediapm_utils::Timestamp::default(),
                last_referenced_at: mediapm_utils::Timestamp::default(),
                step_ids: BTreeSet::from([step.clone()]),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
            },
        );
        (state, key)
    }

    /// Builds a state with two instances: one serving `step_id` of `left`, one
    /// serving the same step id of `right`.
    fn state_with_same_step_in_two_workflows(
        left: &str,
        right: &str,
        step_id: &str,
    ) -> (ConductorState, Hash, Hash) {
        let (mut state, left_key) = state_with_step_instance(&step_ref(left, step_id));
        let (other, right_key) = state_with_step_instance(&step_ref(right, step_id));
        state.tool_call_instances.extend(other.tool_call_instances);
        (state, left_key, right_key)
    }

    /// Marking one workflow's step leaves a same-named step of another workflow
    /// alone.
    ///
    /// Step ids are unique only inside their workflow: mediapm synthesizes one
    /// workflow per media and names the steps after their media, so two media can
    /// well carry a `step-0-echo`. A request that named only the id would mark
    /// both, and the user's `media invalidate a 0` would silently discard b's
    /// cached work — the defect this test exists to catch.
    #[tokio::test]
    async fn marking_a_step_marks_only_instances_of_that_workflow() {
        let (state, left_key, right_key) =
            state_with_same_step_in_two_workflows("media.a", "media.b", "step-0-echo");
        let (_dir, conductor) = conductor_with_state(&state);

        let counts = conductor
            .invalidate_instances_for_steps(&BTreeSet::from([step_ref("media.a", "step-0-echo")]))
            .await
            .expect("mark a's step");

        assert_eq!(counts, InstanceMarkerCounts { changed: 1, unchanged: 0 });
        let on_disk = load_state_file(conductor.storage_paths())
            .expect("load state file")
            .expect("state file must exist");
        assert!(
            on_disk.tool_call_instances[&left_key].invalidated_at.is_some(),
            "the targeted workflow's instance must be marked",
        );
        assert_eq!(
            on_disk.tool_call_instances[&right_key].invalidated_at, None,
            "another workflow's same-named step must be untouched",
        );
    }

    /// Marking by step id reaches the instance that served the step, persists,
    /// and is idempotent.
    ///
    /// The marker is read by the probe, which reads the state that was loaded
    /// from the file, so a mark that stayed in memory would be invisible to the
    /// next process and to the next run in this one.
    #[tokio::test]
    async fn invalidating_instances_for_steps_marks_and_persists() {
        let step = step_ref("default", "step-0-echo");
        let (state, key) = state_with_step_instance(&step);
        let (_dir, conductor) = conductor_with_state(&state);
        let steps = BTreeSet::from([step.clone()]);

        let counts = conductor.invalidate_instances_for_steps(&steps).await.expect("mark");
        assert_eq!(counts, InstanceMarkerCounts { changed: 1, unchanged: 0 });

        let on_disk = load_state_file(conductor.storage_paths())
            .expect("load state file")
            .expect("state file must exist");
        let marked_at = on_disk.tool_call_instances[&key]
            .invalidated_at
            .expect("the marker must reach the file");

        // A repeated mark reports the state it found and keeps the timestamp, so
        // "marked since" does not restart on every retry.
        let again = conductor.invalidate_instances_for_steps(&steps).await.expect("mark again");
        assert_eq!(again, InstanceMarkerCounts { changed: 0, unchanged: 1 });
        let on_disk = load_state_file(conductor.storage_paths())
            .expect("load state file")
            .expect("state file must exist");
        assert_eq!(on_disk.tool_call_instances[&key].invalidated_at, Some(marked_at));

        // A step no instance served reaches nothing and says so.
        let unrelated = conductor
            .invalidate_instances_for_steps(&BTreeSet::from([step_ref("default", "step-9-echo")]))
            .await
            .expect("mark an unserved step");
        assert_eq!(unrelated, InstanceMarkerCounts::default());

        let cleared = conductor.revalidate_instances_for_steps(&steps).await.expect("clear");
        assert_eq!(cleared, InstanceMarkerCounts { changed: 1, unchanged: 0 });
        let on_disk = load_state_file(conductor.storage_paths())
            .expect("load state file")
            .expect("state file must exist");
        assert_eq!(on_disk.tool_call_instances[&key].invalidated_at, None);
    }

    /// Clearing a marker that is not there changes nothing, including the file.
    #[tokio::test]
    async fn revalidating_an_unmarked_instance_writes_nothing() {
        let step = step_ref("default", "step-0-echo");
        let (state, _key) = state_with_step_instance(&step);
        let (_dir, conductor) = conductor_with_state(&state);
        let path = conductor.storage_paths().state_file_path.clone();
        let before = std::fs::read(&path).expect("read state file");

        let counts =
            conductor.revalidate_instances_for_steps(&BTreeSet::from([step])).await.expect("clear");

        assert_eq!(counts, InstanceMarkerCounts { changed: 0, unchanged: 1 });
        assert_eq!(std::fs::read(&path).expect("read state file"), before);
    }

    /// Writes a config document declaring one workflow, at the location
    /// standalone discovery reads.
    fn write_single_workflow_config(dir: &Path, workflow: &str) {
        let doc = NickelDocument {
            tools: BTreeMap::new(),
            workflows: vec![crate::config::WorkflowSpec {
                name: workflow.to_string(),
                ..crate::config::WorkflowSpec::default()
            }],
            ..NickelDocument::default()
        };
        std::fs::write(
            dir.join("conductor.ncl"),
            versions::encode_document(doc).expect("encode config"),
        )
        .expect("write config");
    }

    /// Writes a single-workflow config that also declares `declared` in
    /// `external_data`.
    ///
    /// A declaration is what roots a hash the conductor did not produce itself,
    /// which is the case the background pass has to get right.
    fn write_single_workflow_config_declaring(dir: &Path, workflow: &str, declared: &[Hash]) {
        let doc = NickelDocument {
            tools: BTreeMap::new(),
            workflows: vec![crate::config::WorkflowSpec {
                name: workflow.to_string(),
                ..crate::config::WorkflowSpec::default()
            }],
            external_data: declared
                .iter()
                .map(|hash| {
                    (
                        *hash,
                        crate::config::ExternalDataEntry {
                            description: None,
                            save_mode: crate::state::OutputSaveMode::Saved,
                        },
                    )
                })
                .collect(),
            ..NickelDocument::default()
        };
        std::fs::write(
            dir.join("conductor.ncl"),
            versions::encode_document(doc).expect("encode config"),
        )
        .expect("write config");
    }

    /// Builds a recorded instance with no inputs and no outputs, so it is
    /// reachable only if a seed names it.
    fn bare_instance(_key: Hash) -> crate::state::ToolCallInstance {
        crate::state::ToolCallInstance {
            tool_call_id: "echo@v1".to_string(),
            impure: false,
            invalidated_at: None,
            executed_at: mediapm_utils::Timestamp::default(),
            last_referenced_at: mediapm_utils::Timestamp::default(),
            step_ids: BTreeSet::new(),
            command_args: Vec::new(),
            env_vars: BTreeMap::new(),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        }
    }

    /// A GC pass leaves its evictions in the state file.
    ///
    /// The pass deletes the blobs of everything it evicts, so a pass whose
    /// result stays in memory deletes the bytes and keeps the records that
    /// promised they were there. The file is the only record a later process
    /// reads, which makes the on-disk assertion the one that matters.
    #[tokio::test]
    async fn run_gc_leaves_its_evictions_in_the_state_file() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        write_single_workflow_config(dir.path(), "default");
        let paths = RuntimeStoragePaths::new(dir.path());

        let live = Hash::from_content(b"live");
        let superseded = Hash::from_content(b"superseded");
        let mut state = ConductorState::new_empty();
        // Each record is judged on its own clock, so the seeded one is what
        // survives and the one stamped at the epoch is what ages out.
        let mut live_instance = bare_instance(live);
        live_instance.last_referenced_at = mediapm_utils::Timestamp::now();
        let mut superseded_instance = bare_instance(superseded);
        superseded_instance.last_referenced_at = mediapm_utils::Timestamp::from_unix_nanos(0);
        state.tool_call_instances.insert(live, live_instance);
        state.tool_call_instances.insert(superseded, superseded_instance);
        state.aux = crate::state::AuxData {
            // A pass measures the grace period against the epoch it finds, so a
            // current epoch is what puts the unseeded record past its deadline.
            conductor_gc_epoch: mediapm_utils::Timestamp::now(),
            live_instances: BTreeMap::from([("default".to_string(), BTreeSet::from([live]))]),
        };
        save_state_file(&paths, &state).expect("seed state file");

        let conductor = Conductor::new(paths, mediapm_cas::new_in_memory_cas());
        conductor.replace_resolved_state(state).expect("replace state");
        conductor.run_gc().await.expect("gc");
        conductor.shutdown().await.expect("shutdown");

        let on_disk = load_state_file(conductor.storage_paths())
            .expect("load state file")
            .expect("state file must exist");
        assert!(
            !on_disk.tool_call_instances.contains_key(&superseded),
            "the eviction must reach the file: {on_disk:?}",
        );
        assert!(
            on_disk.tool_call_instances.contains_key(&live),
            "a seeded instance survives the pass and the write",
        );
    }

    /// A save publishes the whole document and leaves no temporary file.
    ///
    /// The temporary is an implementation detail of the replacement, not an
    /// artifact of it: one left behind would accumulate per crash and, worse,
    /// would be the file a later reader might pick up.
    #[test]
    fn a_saved_state_file_is_whole_and_leaves_no_temporary_file() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(dir.path());
        let (state, key) = state_with_step_instance(&step_ref("default", "step-0-echo"));

        save_state_file(&paths, &state).expect("save state file");

        let loaded = load_state_file(&paths).expect("load state file").expect("state file exists");
        assert_eq!(loaded, state, "the file must hold the whole document");
        let written = std::fs::read_to_string(&paths.state_file_path).expect("read state file");
        assert!(
            !written.contains("instance_key"),
            "the record is filed under its key and must not repeat it: {written}",
        );
        assert!(
            written.contains(&key.to_string()),
            "the key the record is filed under must be in the file: {written}",
        );
        let mut names: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![paths.state_file_path.file_name().expect("name").to_string_lossy().into_owned()],
            "a save must leave exactly the state file, and the record it holds is {key}",
        );
    }

    /// An interrupted save leaves the previous document in place.
    ///
    /// The bytes never reach the target except by rename, so a write that dies
    /// halfway is invisible: the file still holds the state the last good save
    /// published, and a reader — the next process — is none the wiser. Written
    /// as it was, an interrupted `File::create` would have truncated the file
    /// and the next load would have failed on a document that never existed.
    #[test]
    fn an_interrupted_save_keeps_the_previous_file_and_cleans_up() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(dir.path());
        let (state, _key) = state_with_step_instance(&step_ref("default", "step-0-echo"));
        save_state_file(&paths, &state).expect("save the good state");
        let published = std::fs::read(&paths.state_file_path).expect("read published state");

        let failed = mediapm_utils::fs::write_atomically(&paths.state_file_path, |file| {
            file.write_all(b"{\"version\": 4, \"tool_call_inst")?;
            Err(std::io::Error::other("the disk went away"))
        });
        assert!(failed.is_err(), "the writer's failure must be reported");

        assert_eq!(
            std::fs::read(&paths.state_file_path).expect("read published state"),
            published,
            "the previous document must survive a save that never finished",
        );
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names.len(), 1, "the abandoned temporary must be removed: {names:?}");
    }

    /// A save must not recreate the directory its owner removed.
    ///
    /// The write path is the last step of a background pass, so a
    /// `create_dir_all` here resurrects a removed runtime root as an orphan
    /// holding one `state.json`. Failing instead is what lets the pass stop.
    #[test]
    fn a_state_save_into_a_removed_root_does_not_recreate_it() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(dir.path());
        let root = paths.state_file_path.parent().expect("state parent").to_path_buf();
        drop(dir);

        let error = save_state_file(&paths, &ConductorState::new_empty())
            .expect_err("a save into a removed root must fail");

        assert!(!root.exists(), "the save recreated the removed runtime root: {error}");
    }

    /// The loop stops when its root is removed, and stays stopped.
    ///
    /// Mirrors `the_background_loop_runs_a_pass_and_stops_on_shutdown`, which
    /// proves the loop runs a pass on its interval. This one removes the root and
    /// then brings it back: a loop that stopped writes nothing, while a loop that
    /// kept running would run a pass in the second window and publish the state
    /// file again. That second window is what makes the break observable rather
    /// than merely consistent with the strict write path, which keeps a removed
    /// root removed whether the loop stops or not.
    #[tokio::test]
    async fn the_background_loop_stops_when_its_root_is_removed() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        write_single_workflow_config(dir.path(), "default");
        let paths = RuntimeStoragePaths::new(dir.path());
        let state = ConductorState::new_empty();
        save_state_file(&paths, &state).expect("seed state file");
        let root = paths.state_file_path.parent().expect("state parent").to_path_buf();
        let state_file = paths.state_file_path.clone();

        let mut conductor = Conductor::new(paths, mediapm_cas::new_in_memory_cas());
        conductor.background_gc_interval_secs = 1;
        conductor.replace_resolved_state(state).expect("replace state");
        conductor.start_background_maintenance();
        tokio::time::sleep(Duration::from_secs(2)).await;

        drop(dir);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!root.exists(), "a removed runtime root must not be recreated by the loop");

        std::fs::create_dir_all(&root).expect("recreate the runtime root");
        tokio::time::sleep(Duration::from_secs(2)).await;
        conductor.shutdown().await.expect("shutdown");

        // Removed before the assertion, not after: the tempdir that owned this
        // path is already dropped, so a failure here would otherwise leave the
        // recreated root behind for the workspace janitor to find.
        let wrote_a_pass = state_file.exists();
        mediapm_utils::temp::remove_dir_all_with_retry(&root).expect("remove the recreated root");
        assert!(!wrote_a_pass, "a stopped loop must not run a pass after its root comes back");
    }

    /// A guard whose task has ended does not block the next one.
    ///
    /// The loop stops on its own when its runtime root goes, and it leaves its
    /// guard in the slot while doing so. A conductor that read that spent guard as
    /// running would never maintain anything again, so the restart is the
    /// property: the epoch the first loop published moves once the root is back.
    /// Stopping through `shutdown` does not exercise this, because that empties
    /// the slot on its way out.
    #[tokio::test]
    async fn a_stopped_background_loop_can_be_started_again() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        write_single_workflow_config(dir.path(), "default");
        let paths = RuntimeStoragePaths::new(dir.path());
        let root = paths.state_file_path.parent().expect("state parent").to_path_buf();
        let live = Hash::from_content(b"live");
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(live, bare_instance(live));
        state.aux.live_instances =
            BTreeMap::from([("default".to_string(), BTreeSet::from([live]))]);
        save_state_file(&paths, &state).expect("seed state file");

        let mut conductor = Conductor::new(paths, mediapm_cas::new_in_memory_cas());
        conductor.background_gc_interval_secs = 1;
        conductor.replace_resolved_state(state).expect("replace state");
        conductor.start_background_maintenance();
        tokio::time::sleep(Duration::from_secs(2)).await;

        drop(dir);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after_stopping = conductor.get_state().expect("state").aux.conductor_gc_epoch;
        assert!(
            after_stopping > mediapm_utils::Timestamp::from_unix_nanos(0),
            "the first loop must have run a pass before it stopped: {after_stopping:?}",
        );

        std::fs::create_dir_all(&root).expect("recreate the runtime root");
        write_single_workflow_config(&root, "default");
        conductor.start_background_maintenance();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after_restarting = conductor.get_state().expect("state").aux.conductor_gc_epoch;
        conductor.shutdown().await.expect("shutdown");

        // Removed before the assertion, not after: the tempdir that owned this
        // path is already dropped, so a failure here would otherwise leave the
        // recreated root behind for the workspace janitor to find.
        mediapm_utils::temp::remove_dir_all_with_retry(&root).expect("remove the recreated root");
        assert!(
            after_restarting > after_stopping,
            "a guard whose task has ended must not block a new loop: {after_stopping:?} then \
             {after_restarting:?}",
        );
    }

    /// A removed root reads as gone, and an existing one does not.
    #[test]
    fn runtime_root_is_gone_tracks_the_state_directory() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(dir.path());
        std::fs::create_dir_all(paths.state_file_path.parent().expect("state parent"))
            .expect("create root");
        assert!(!runtime_root_is_gone(&paths), "an existing root is not gone");

        drop(dir);
        assert!(runtime_root_is_gone(&paths), "a removed root must read as gone");
    }

    /// A foreground entry point creates the root it is about to write into.
    #[test]
    fn ensure_runtime_root_creates_the_state_directory() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(&dir.path().join("nested").join("runtime"));

        ensure_runtime_root(&paths).expect("create root");

        assert!(paths.state_file_path.parent().expect("state parent").is_dir());
    }

    /// The background loop runs a pass on its interval and stops on shutdown.
    ///
    /// The pass is observable through the GC epoch, which only a completed pass
    /// advances, and through the file it writes: a loop that ran the cycle and
    /// dropped the result would show the first and not the second.
    #[tokio::test]
    async fn the_background_loop_runs_a_pass_and_stops_on_shutdown() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        write_single_workflow_config(dir.path(), "default");
        let paths = RuntimeStoragePaths::new(dir.path());
        let live = Hash::from_content(b"live");
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(live, bare_instance(live));
        state.aux.live_instances =
            BTreeMap::from([("default".to_string(), BTreeSet::from([live]))]);
        save_state_file(&paths, &state).expect("seed state file");

        let mut conductor = Conductor::new(paths, mediapm_cas::new_in_memory_cas());
        conductor.background_gc_interval_secs = 1;
        conductor.replace_resolved_state(state).expect("replace state");
        conductor.start_background_maintenance();

        tokio::time::sleep(Duration::from_secs(2)).await;
        conductor.shutdown().await.expect("shutdown");
        let after = conductor.get_state().expect("state");
        assert!(
            after.aux.conductor_gc_epoch > mediapm_utils::Timestamp::from_unix_nanos(0),
            "at least one pass must have completed: {:?}",
            after.aux.conductor_gc_epoch,
        );
        let on_disk = load_state_file(conductor.storage_paths())
            .expect("load state file")
            .expect("state file must exist");
        assert_eq!(
            on_disk.aux.conductor_gc_epoch, after.aux.conductor_gc_epoch,
            "the pass must publish the state it computed",
        );
    }

    /// The background pass runs the full cycle: it keeps what the document
    /// declares and reclaims what nothing names.
    ///
    /// Both halves are the point, and they are one pass apart. A declared hash is
    /// a root, so bytes the application supplied through a document survive the
    /// timer — which is what makes the cycle safe unattended. The same pass must
    /// still collect a blob neither a record nor a declaration names, or
    /// unattended maintenance would never free anything and the cycle would be
    /// maintenance in name only.
    #[tokio::test]
    async fn a_background_pass_keeps_declared_bytes_and_reclaims_orphans() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(dir.path());
        let live = Hash::from_content(b"live");
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(live, bare_instance(live));
        state.aux.live_instances =
            BTreeMap::from([("default".to_string(), BTreeSet::from([live]))]);
        save_state_file(&paths, &state).expect("seed state file");

        let cas = mediapm_cas::new_in_memory_cas();
        let declared = cas
            .put(bytes::Bytes::from_static(b"a payload the document declares"))
            .await
            .expect("put declared payload");
        let orphan = cas
            .put(bytes::Bytes::from_static(b"a payload nothing names"))
            .await
            .expect("put orphan payload");
        write_single_workflow_config_declaring(dir.path(), "default", &[declared]);

        let mut conductor = Conductor::new(paths, cas);
        conductor.background_gc_interval_secs = 1;
        conductor.replace_resolved_state(state).expect("replace state");
        conductor.start_background_maintenance();

        tokio::time::sleep(Duration::from_secs(2)).await;
        conductor.shutdown().await.expect("shutdown");
        let swept = conductor.get_state().expect("state");
        assert!(
            swept.aux.conductor_gc_epoch > mediapm_utils::Timestamp::from_unix_nanos(0),
            "the background pass must have run, or neither half proves anything: {:?}",
            swept.aux.conductor_gc_epoch,
        );
        assert!(
            conductor.cas().get(declared).await.is_ok(),
            "a declared hash is a root, so the pass must keep its bytes",
        );
        assert!(
            conductor.cas().get(orphan).await.is_err(),
            "a blob no record and no declaration names is an orphan the pass reclaims",
        );
    }

    /// Dropping the facade cancels the loop, and no later pass reaches the file.
    ///
    /// The guard's `Drop` aborts the task, so a facade that goes away takes its
    /// maintenance with it. What is observable from outside is the write: a pass
    /// that kept running would keep advancing the epoch in the state file, and a
    /// pass that was aborted mid-flight after the facade was gone cannot have
    /// written, because the writer is the same code the guard stopped.
    #[tokio::test]
    async fn dropping_the_facade_cancels_the_pass() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        write_single_workflow_config(dir.path(), "default");
        let paths = RuntimeStoragePaths::new(dir.path());
        let live = Hash::from_content(b"live");
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(live, bare_instance(live));
        state.aux.live_instances =
            BTreeMap::from([("default".to_string(), BTreeSet::from([live]))]);
        save_state_file(&paths, &state).expect("seed state file");

        let state_path = paths.state_file_path.clone();
        let mut conductor = Conductor::new(paths, mediapm_cas::new_in_memory_cas());
        conductor.background_gc_interval_secs = 1;
        conductor.replace_resolved_state(state).expect("replace state");
        conductor.start_background_maintenance();
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(conductor);

        let settled = load_state_file(&RuntimeStoragePaths::new(dir.path()))
            .expect("load state file")
            .expect("state file must exist");
        tokio::time::sleep(Duration::from_millis(2_200)).await;
        let later = load_state_file(&RuntimeStoragePaths::new(dir.path()))
            .expect("load state file")
            .expect("state file must exist");
        assert_eq!(
            later.aux.conductor_gc_epoch,
            settled.aux.conductor_gc_epoch,
            "a dropped facade must not keep passing over {}",
            state_path.display(),
        );
    }

    /// `with_config_paths` relocates the state file to the custom path.
    ///
    /// The custom path's directory is the root, so it is created the way a
    /// foreground entry point creates it before the save that needs it.
    #[test]
    fn state_file_path_is_configurable() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let custom = tmp.path().join("custom").join("state.json");
        let paths = RuntimeStoragePaths::new(&tmp.path().join("conductor_dir"))
            .with_config_paths(Vec::new(), custom.clone());
        ensure_runtime_root(&paths).expect("create runtime root");

        let state = ConductorState::new_empty();
        save_state_file(&paths, &state).expect("save state file");
        assert!(custom.exists());
        assert_eq!(load_state_file(&paths).expect("load state file"), Some(state));
    }
}
