//! Deterministic workflow coordinator for actor-backed conductor orchestration.
//!
//! This module keeps workflow sequencing, impure-timestamp planning, and state
//! merge logic in one place while delegating side effects to dedicated actors:
//! document loading, workflow-level execution, and CAS-backed state
//! persistence.
//!
//! # Module structure note
//!
//! Most non-trivial logic lives in `impl WorkflowCoordinator<C>` methods that
//! take `&mut self`, plus a set of closely related static associated functions
//! (dependency-graph construction, state merge, impure-timestamp planning)
//! that reference the coordinator's generic parameter `C`. Splitting the
//! static helpers into a sibling file would impose `super::` noise on every
//! call and require threading the `C` bound across file boundaries. The
//! external `coordinator_tests.rs` already handles test isolation.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use mediapm_cas::{CasApi, Hash};
use ractor::{ActorRef, call_t};

use crate::api::{
    RunSummary, RunWorkflowOptions, RuntimeDiagnostics, SchedulerDiagnostics, StateMutationOptions,
    WorkflowProgressSender, WorkflowStepEvent, resolve_runtime_storage_paths,
};
use crate::error::ConductorError;
use crate::model::config::{
    ImpureTimestamp, InputBinding, ParsedInputBindingSegment, WorkflowSpec, WorkflowStepSpec,
    parse_input_binding,
};
use crate::model::state::{AuxData, OrchestrationState, merge_persistence_flags};
use crate::runtime_env::load_runtime_env_files;
use crate::tool_cache::ToolContentCache;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, sleep};

use super::actors::documents::{DocumentLoaderClient, spawn_document_loader_actor};
use super::actors::scheduler::{SchedulerClient, spawn_scheduler_actor};
use super::actors::state_store::{StateStoreClient, spawn_state_store_actor};
use super::actors::step_worker::{StepWorkerMessage, spawn_step_worker_pool};
use super::config::{default_worker_pool_size, profile_output_path_from_env, rpc_timeout_ms};
use super::profiler::{
    StepExecutionProfile, StepPhaseTimingProfile, WorkflowRunProfile, write_profile_json,
};
use super::protocol::{
    CommitStateRequest, LoadedDocuments, StepCompletionRecord, StepExecutionBundle,
    StepExecutionRequest, StepOutputs, UnifiedNickelDocument, UnifiedToolSpec,
};

/// Summary and cleanup metadata returned by one workflow run.
#[derive(Debug)]
struct ExecutionOutcome {
    /// User-visible run summary accumulated across all workflows.
    summary: RunSummary,
    /// Unsaved hashes that may be deleted after state persistence completes.
    pending_unsaved_hashes: BTreeSet<Hash>,
    /// Per-step execution timing records collected for profiler reporting.
    step_executions: Vec<StepExecutionProfile>,
}

/// Deterministic workflow coordinator rooted in one CAS implementation.
pub(super) struct WorkflowCoordinator<C>
where
    C: CasApi,
{
    /// Shared CAS handle passed into child actors.
    pub(super) cas: Arc<C>,
    /// Lazily-initialized tool-content cache for managed tool extraction.
    tool_cache: Option<Arc<ToolContentCache<C>>>,
    /// Typed client for the document-loader actor.
    document_loader: Option<DocumentLoaderClient>,
    /// Typed client for the workflow scheduler actor.
    scheduler: Option<SchedulerClient>,
    /// Worker actor pool for step execution.
    workers: Vec<ActorRef<StepWorkerMessage>>,
    /// Typed client for the orchestration state-store actor.
    state_store: Option<StateStoreClient>,
}

/// Default TTL for workflow instances: 7 days.
const DEFAULT_INSTANCE_TTL_SECONDS: u64 = 604_800;

impl<C> WorkflowCoordinator<C>
where
    C: CasApi + Send + Sync + 'static,
{
    /// Creates a coordinator bound to one CAS implementation.
    #[must_use]
    pub(super) fn new(cas: Arc<C>) -> Self {
        Self {
            cas,
            tool_cache: None,
            document_loader: None,
            scheduler: None,
            workers: Vec::new(),
            state_store: None,
        }
    }

    /// Returns a clone of the state-store client if one has been initialized.
    #[must_use]
    pub(super) fn state_store(&self) -> Option<StateStoreClient> {
        self.state_store.clone()
    }

    /// Replaces the state-store client so the coordinator uses an externally
    /// provided store instance.
    pub(super) fn set_state_store(&mut self, store: StateStoreClient) {
        self.state_store = Some(store);
    }

    /// Returns the current in-memory orchestration-state snapshot published by the state-store actor.
    pub(super) async fn current_state(&self) -> Result<OrchestrationState, ConductorError> {
        if let Some(state_store) = &self.state_store {
            return state_store.current_state().await;
        }

        Ok(OrchestrationState::default())
    }

    /// Returns runtime diagnostics from the scheduler when it exists.
    pub(super) async fn runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        if let Some(scheduler) = &self.scheduler {
            return scheduler.runtime_diagnostics().await;
        }

        Ok(Self::empty_runtime_diagnostics())
    }

    /// Runs instance GC on the state-store's in-memory state with an optional
    /// TTL override. When `ttl_override` is `None`, the store's configured TTL
    /// is used; if neither is set the call is a no-op.
    pub(super) async fn run_gc(&self, ttl_override: Option<u64>) -> Result<(), ConductorError> {
        if let Some(state_store) = &self.state_store {
            return state_store.run_gc(ttl_override).await;
        }
        Ok(())
    }

    /// Builds an empty diagnostics shell when runtime snapshots are unavailable.
    #[must_use]
    fn empty_runtime_diagnostics() -> RuntimeDiagnostics {
        RuntimeDiagnostics {
            worker_pool_size: 0,
            scheduler: SchedulerDiagnostics {
                ewma_alpha: super::config::scheduler_ewma_alpha(),
                unknown_cost_ms: super::config::unknown_step_cost_ms(),
                tool_estimates: Vec::new(),
            },
            workers: Vec::new(),
            recent_traces: Vec::new(),
        }
    }

    /// Returns current Unix wall-clock timestamp in nanoseconds.
    #[must_use]
    fn now_unix_nanos() -> u128 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
    }

    /// Ensures all supporting actors are spawned before workflow execution.
    ///
    /// Note: workers are not initialized here because they require a
    /// `tools_dir` parameter that is only available after runtime paths are
    /// resolved. Callers must invoke [`ensure_workers`](Self::ensure_workers)
    /// separately when the tools directory is known.
    pub(super) async fn ensure_runtime_support(&mut self) -> Result<(), ConductorError> {
        self.ensure_document_loader().await?;
        self.ensure_scheduler().await?;
        self.ensure_state_store().await?;
        Ok(())
    }

    /// Lazily spawns the document-loader actor.
    async fn ensure_document_loader(&mut self) -> Result<(), ConductorError> {
        if self.document_loader.is_none() {
            self.document_loader = Some(spawn_document_loader_actor().await?);
        }
        Ok(())
    }

    /// Lazily spawns the workflow scheduler actor.
    async fn ensure_scheduler(&mut self) -> Result<(), ConductorError> {
        if self.scheduler.is_none() {
            self.scheduler = Some(spawn_scheduler_actor().await?);
        }
        Ok(())
    }

    /// Lazily spawns the step-worker pool if not already initialized.
    ///
    /// Creates a [`ToolContentCache`] rooted at `tools_dir` and passes shared
    /// references to each worker actor for concurrent tool-content extraction.
    async fn ensure_workers(&mut self, tools_dir: &Path) -> Result<(), ConductorError> {
        if self.workers.is_empty() {
            let pool_size = default_worker_pool_size();
            let tool_cache =
                Arc::new(ToolContentCache::new(tools_dir.to_path_buf(), self.cas.clone(), None));
            self.workers =
                spawn_step_worker_pool(self.cas.clone(), tool_cache.clone(), pool_size).await?;
            self.tool_cache = Some(tool_cache);
        }
        Ok(())
    }

    /// Lazily spawns the CAS-backed orchestration state-store actor.
    ///
    /// Instance GC TTL starts at the 7-day default until the first config
    /// load potentially overrides it via `StateStoreClient::set_instance_ttl`.
    async fn ensure_state_store(&mut self) -> Result<(), ConductorError> {
        if self.state_store.is_none() {
            self.state_store = Some(
                spawn_state_store_actor(self.cas.clone(), Some(DEFAULT_INSTANCE_TTL_SECONDS))
                    .await?,
            );
        }
        Ok(())
    }

    /// Executes workflows using `conductor.ncl` and `conductor.machine.ncl`
    /// paths with strict-safe defaults.
    pub(super) async fn run_workflow(
        &mut self,
        user_ncl: &Path,
        machine_ncl: &Path,
    ) -> Result<RunSummary, ConductorError> {
        self.run_workflow_with_options(user_ncl, machine_ncl, RunWorkflowOptions::default()).await
    }

    /// Executes workflows using `conductor.ncl` and `conductor.machine.ncl` paths
    /// with explicit runtime options.
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps workflow lifecycle sequencing explicit and auditable"
    )]
    pub(super) async fn run_workflow_with_options(
        &mut self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<RunSummary, ConductorError> {
        if user_ncl.as_os_str().is_empty() || machine_ncl.as_os_str().is_empty() {
            return Err(ConductorError::Workflow(
                "conductor.ncl and conductor.machine.ncl paths must be non-empty".to_string(),
            ));
        }
        let run_started_unix_nanos = Self::now_unix_nanos();
        let resolved_runtime_paths =
            resolve_runtime_storage_paths(user_ncl, machine_ncl, &options.runtime_storage_paths);
        let mut effective_options = options;
        let runtime_env_names = load_runtime_env_files(&resolved_runtime_paths.conductor_dir)?;
        Self::append_unique_env_var_names(
            &mut effective_options.runtime_inherited_env_vars,
            &runtime_env_names,
        );
        let progress_sender = effective_options.progress_sender.take();
        let conductor_state_config = resolved_runtime_paths.conductor_state_config.clone();
        let profile_output_path = effective_options
            .profile_output_path
            .clone()
            .or_else(profile_output_path_from_env)
            .or_else(|| {
                effective_options
                    .profiler_enabled
                    .then(|| resolved_runtime_paths.conductor_dir.join("profile.json"))
            });

        self.ensure_runtime_support().await?;
        self.ensure_workers(&resolved_runtime_paths.conductor_tools_dir).await?;
        let document_loader = self.document_loader.clone().ok_or_else(|| {
            ConductorError::Internal("document loader actor was not initialized".to_string())
        })?;
        let scheduler = self.scheduler.clone().ok_or_else(|| {
            ConductorError::Internal("scheduler actor was not initialized".to_string())
        })?;
        let state_store = self.state_store.clone().ok_or_else(|| {
            ConductorError::Internal("state store actor was not initialized".to_string())
        })?;

        let LoadedDocuments { machine_document, mut state_document, prior_state_pointer, unified } =
            document_loader
                .load_and_unify(user_ncl, machine_ncl, &conductor_state_config, effective_options)
                .await?;
        state_store.set_instance_ttl(
            machine_document.runtime.instance_ttl_seconds.or(Some(DEFAULT_INSTANCE_TTL_SECONDS)),
        )?;
        let mut state = state_store.load_state_from_pointer(prior_state_pointer).await?;
        state.external_data = unified.external_data.clone();
        let outermost_config_dir = Self::absolute_outermost_config_dir(
            user_ncl.parent().or_else(|| machine_ncl.parent()).unwrap_or_else(|| Path::new(".")),
        )?;

        let execution_outcome = self
            .execute_workflows(
                scheduler.clone(),
                &unified,
                &mut state_document,
                &mut state,
                &resolved_runtime_paths.conductor_tools_dir,
                &resolved_runtime_paths.conductor_tmp_dir,
                &outermost_config_dir,
                progress_sender,
            )
            .await;

        let (summary, step_executions) = match execution_outcome {
            Ok(ExecutionOutcome { summary, pending_unsaved_hashes, step_executions }) => {
                let current_state_pointer = state_store
                    .commit_run(CommitStateRequest {
                        next_state: state.clone(),
                        pending_unsaved_hashes,
                        unified: unified.clone(),
                        prior_state_pointer,
                    })
                    .await?;
                state_document.state_pointer = Some(current_state_pointer);
                document_loader
                    .persist_machine_document(machine_ncl, machine_document.clone())
                    .await?;
                document_loader
                    .persist_state_document(&conductor_state_config, state_document.clone())
                    .await?;
                (summary, step_executions)
            }
            Err(run_error) => {
                let checkpoint_pointer = state_store
                    .commit_run(CommitStateRequest {
                        next_state: state.clone(),
                        pending_unsaved_hashes: BTreeSet::new(),
                        unified: unified.clone(),
                        prior_state_pointer,
                    })
                    .await?;
                state_document.state_pointer = Some(checkpoint_pointer);
                document_loader.persist_machine_document(machine_ncl, machine_document).await?;
                document_loader
                    .persist_state_document(&conductor_state_config, state_document)
                    .await?;
                return Err(run_error);
            }
        };

        if let Some(output_path) = profile_output_path {
            let runtime_diagnostics = scheduler.runtime_diagnostics().await.unwrap_or_else(|error| {
                eprintln!(
                    "warning: failed collecting runtime diagnostics for profiler output '{}': {error}",
                    output_path.display()
                );
                Self::empty_runtime_diagnostics()
            });
            let run_finished_unix_nanos = Self::now_unix_nanos();
            let profile = WorkflowRunProfile::new(
                run_started_unix_nanos,
                run_finished_unix_nanos,
                user_ncl,
                machine_ncl,
                &resolved_runtime_paths.conductor_dir,
                &conductor_state_config,
                summary,
                step_executions,
                runtime_diagnostics,
            );
            write_profile_json(&output_path, &profile)?;
        }

        Ok(summary)
    }

    /// Loads effective orchestration state for one user/machine/runtime path
    /// set and validates it against the currently resolved merged config.
    pub(super) async fn load_resolved_state_with_options(
        &mut self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: StateMutationOptions,
    ) -> Result<OrchestrationState, ConductorError> {
        if user_ncl.as_os_str().is_empty() || machine_ncl.as_os_str().is_empty() {
            return Err(ConductorError::Workflow(
                "conductor.ncl and conductor.machine.ncl paths must be non-empty".to_string(),
            ));
        }

        self.ensure_runtime_support().await?;
        let document_loader = self.document_loader.clone().ok_or_else(|| {
            ConductorError::Internal("document loader actor was not initialized".to_string())
        })?;
        let state_store = self.state_store.clone().ok_or_else(|| {
            ConductorError::Internal("state store actor was not initialized".to_string())
        })?;

        let resolved_runtime_paths =
            resolve_runtime_storage_paths(user_ncl, machine_ncl, &options.runtime_storage_paths);
        let mut runtime_inherited_env_vars = options.runtime_inherited_env_vars;
        let runtime_env_names = load_runtime_env_files(&resolved_runtime_paths.conductor_dir)?;
        Self::append_unique_env_var_names(&mut runtime_inherited_env_vars, &runtime_env_names);
        let load_options = RunWorkflowOptions {
            allow_tool_redefinition: false,
            runtime_storage_paths: options.runtime_storage_paths,
            runtime_inherited_env_vars,
            profile_output_path: None,
            profiler_enabled: false,
            progress_sender: None,
            cas_integrity_config: None,
        };
        let LoadedDocuments { prior_state_pointer, unified, .. } = document_loader
            .load_and_unify(
                user_ncl,
                machine_ncl,
                &resolved_runtime_paths.conductor_state_config,
                load_options,
            )
            .await?;

        let mut state = state_store.load_state_from_pointer(prior_state_pointer).await?;
        state.external_data = unified.external_data.clone();
        Self::validate_state_against_unified(&state, &unified)?;
        // Publish the loaded state to the state-store's in-memory snapshot so
        // subsequent current_state() / run_gc() calls reflect persisted state
        // (for example after a background workflow task completes).
        state_store.persist_and_publish_state(state.clone()).await?;
        Ok(state)
    }

    /// Replaces effective orchestration state for one user/machine/runtime path
    /// set and persists only the new CAS state blob plus volatile
    /// `state_pointer`.
    pub(super) async fn replace_resolved_state_with_options(
        &mut self,
        user_ncl: &Path,
        machine_ncl: &Path,
        next_state: OrchestrationState,
        options: StateMutationOptions,
    ) -> Result<Hash, ConductorError> {
        if user_ncl.as_os_str().is_empty() || machine_ncl.as_os_str().is_empty() {
            return Err(ConductorError::Workflow(
                "conductor.ncl and conductor.machine.ncl paths must be non-empty".to_string(),
            ));
        }

        self.ensure_runtime_support().await?;
        let document_loader = self.document_loader.clone().ok_or_else(|| {
            ConductorError::Internal("document loader actor was not initialized".to_string())
        })?;
        let state_store = self.state_store.clone().ok_or_else(|| {
            ConductorError::Internal("state store actor was not initialized".to_string())
        })?;

        let resolved_runtime_paths =
            resolve_runtime_storage_paths(user_ncl, machine_ncl, &options.runtime_storage_paths);
        let mut runtime_inherited_env_vars = options.runtime_inherited_env_vars;
        let runtime_env_names = load_runtime_env_files(&resolved_runtime_paths.conductor_dir)?;
        Self::append_unique_env_var_names(&mut runtime_inherited_env_vars, &runtime_env_names);
        let load_options = RunWorkflowOptions {
            allow_tool_redefinition: false,
            runtime_storage_paths: options.runtime_storage_paths,
            runtime_inherited_env_vars,
            profile_output_path: None,
            profiler_enabled: false,
            progress_sender: None,
            cas_integrity_config: None,
        };
        let LoadedDocuments { mut state_document, unified, machine_document, .. } = document_loader
            .load_and_unify(
                user_ncl,
                machine_ncl,
                &resolved_runtime_paths.conductor_state_config,
                load_options,
            )
            .await?;
        state_store.set_instance_ttl(
            machine_document.runtime.instance_ttl_seconds.or(Some(DEFAULT_INSTANCE_TTL_SECONDS)),
        )?;

        Self::validate_state_against_unified(&next_state, &unified)?;
        let mut next_state = next_state;
        next_state.external_data = unified.external_data.clone();
        let next_pointer = state_store.persist_and_publish_state(next_state).await?;
        state_document.state_pointer = Some(next_pointer);
        document_loader
            .persist_state_document(&resolved_runtime_paths.conductor_state_config, state_document)
            .await?;
        Ok(next_pointer)
    }

    /// Validates that a state snapshot references only known tool/input/output
    /// contracts from the currently resolved merged config.
    fn validate_state_against_unified(
        state: &OrchestrationState,
        unified: &UnifiedNickelDocument,
    ) -> Result<(), ConductorError> {
        for (instance_key, instance) in &state.instances {
            let Some(tool) = unified.tools.get(&instance.tool_name) else {
                return Err(ConductorError::Workflow(format!(
                    "state instance '{instance_key}' references unknown tool '{}' under current merged config",
                    instance.tool_name
                )));
            };

            for input_name in instance.inputs.keys() {
                let is_declared = tool.inputs.contains_key(input_name);
                let has_default = tool.default_inputs.contains_key(input_name);
                if !is_declared && !has_default {
                    return Err(ConductorError::Workflow(format!(
                        "state instance '{instance_key}' for tool '{}' references unknown input '{input_name}' under current merged config",
                        instance.tool_name
                    )));
                }
            }

            for output_name in instance.outputs.keys() {
                if !tool.outputs.contains_key(output_name) {
                    return Err(ConductorError::Workflow(format!(
                        "state instance '{instance_key}' for tool '{}' references unknown output '{output_name}' under current merged config",
                        instance.tool_name
                    )));
                }
            }
        }

        Ok(())
    }

    /// Appends env-var names with trimming and case-insensitive deduplication.
    fn append_unique_env_var_names(target: &mut Vec<String>, source: &[String]) {
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
}

/// Per-workflow mutable state maintained across the dependency-stream dispatch loop.
#[derive(Debug)]
struct WorkflowDepState {
    /// Remaining unsatisfied dependency count per step id.
    remaining_deps: BTreeMap<String, usize>,
    /// For each step id, which step ids depend on it.
    dependents: BTreeMap<String, BTreeSet<String>>,
    /// Step definitions keyed by step id.
    steps: BTreeMap<String, WorkflowStepSpec>,
    /// Output hashes produced by completed steps in this workflow.
    step_outputs: Arc<StepOutputs>,
    /// Pre-computed required output names per step for worker requests.
    required_outputs: BTreeMap<String, BTreeSet<String>>,
    /// Per-workflow summary accumulated across all step completions.
    summary: RunSummary,
    /// Unsaved hashes accumulated within this workflow run.
    pending_unsaved_hashes: BTreeSet<Hash>,
}

/// One completed-step event yielded by the dependency-stream dispatch loop.
#[derive(Debug)]
struct StepCompletionEvent {
    /// Workflow the step belongs to.
    workflow_name: String,
    /// Step identifier within the workflow.
    step_id: String,
    /// Worker index that executed this step.
    worker_index: usize,
    /// Execution result.
    result: Result<StepExecutionBundle, ConductorError>,
}

impl<C> WorkflowCoordinator<C>
where
    C: CasApi + Send + Sync + 'static,
{
    /// Executes all unified workflows using a dependency-stream dispatch model.
    ///
    /// Builds per-workflow dependency graphs from the unified workflow specs,
    /// then dispatches ready steps through a FuturesUnordered-driven stream.
    /// As each step completes, dependents are checked for readiness and added
    /// to the ready queue. Completion events are recorded through the scheduler
    /// for EWMA runtime estimation.
    #[expect(
        clippy::similar_names,
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "each argument represents a distinct runtime context that must be threaded through to the execution pipeline; grouping them would create an ad-hoc context struct with no additional clarity"
    )]
    async fn execute_workflows(
        &self,
        scheduler: SchedulerClient,
        unified: &UnifiedNickelDocument,
        state_document: &mut crate::model::config::StateNickelDocument,
        state: &mut OrchestrationState,
        _tools_dir: &Path,
        conductor_tmp_dir: &Path,
        outermost_config_dir: &Path,
        progress_sender: Option<WorkflowProgressSender>,
    ) -> Result<ExecutionOutcome, ConductorError> {
        // Emit early progress event so the caller's progress bar renders
        // immediately, even before dep-graph construction and step execution.
        if let Some(ref tx) = progress_sender {
            let _ = tx.send(WorkflowStepEvent {
                total_steps: 1,
                completed_steps: 0,
                workflow_name: String::new(),
                step_id: "initializing".into(),
                workflow_display_name: "Initializing".into(),
                executed: false,
                worker_index: 0,
                worker_count: 1,
            });
        }

        let unified_shared = Arc::new(unified.clone());
        let mut summary = RunSummary::new();
        let mut pending_unsaved_hashes = BTreeSet::new();
        let mut step_executions = Vec::new();

        // ── Phase 1: Build dependency graphs and per-workflow state ──

        let mut dep_states: BTreeMap<String, WorkflowDepState> = BTreeMap::new();
        let mut workflow_is_pure_map: BTreeMap<String, bool> = BTreeMap::new();
        let mut workflow_display_names: BTreeMap<String, String> = BTreeMap::new();
        let mut all_impure_timestamps: BTreeMap<String, BTreeMap<String, Option<ImpureTimestamp>>> =
            BTreeMap::new();

        for (workflow_name, workflow) in &unified.workflows {
            let display_name = Self::workflow_display_name(workflow_name, workflow).to_string();
            workflow_display_names.insert(workflow_name.clone(), display_name);

            let is_pure = Self::workflow_is_pure(workflow_name, workflow, &unified.tools)?;
            workflow_is_pure_map.insert(workflow_name.clone(), is_pure);

            for warning in Self::collect_unnecessary_depends_on_warnings(
                workflow_name,
                workflow,
                &unified.tools,
            )? {
                eprintln!("warning: {warning}");
            }

            // Build steps map with dedup validation.
            let mut steps: BTreeMap<String, WorkflowStepSpec> = BTreeMap::new();
            for step in &workflow.steps {
                if step.id.trim().is_empty() {
                    return Err(ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' contains a step with empty id"
                    )));
                }
                if steps.insert(step.id.clone(), step.clone()).is_some() {
                    return Err(ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' contains duplicate step id '{}'",
                        step.id
                    )));
                }
            }

            // Build remaining_deps (indegree) and dependents map.
            let mut remaining_deps: BTreeMap<String, usize> =
                steps.keys().cloned().map(|id| (id, 0usize)).collect();
            let mut dependents: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

            for step in &workflow.steps {
                let mut seen = BTreeSet::new();
                for dependency in &step.depends_on {
                    if !seen.insert(dependency.clone()) {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' contains duplicate depends_on entry '{dependency}'",
                            step.id
                        )));
                    }
                    if !steps.contains_key(dependency) {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' depends on unknown step '{dependency}'",
                            step.id
                        )));
                    }
                    if dependency == &step.id {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' depends on itself",
                            step.id
                        )));
                    }
                    dependents.entry(dependency.clone()).or_default().insert(step.id.clone());
                    if let Some(value) = remaining_deps.get_mut(&step.id) {
                        *value = value.saturating_add(1);
                    }
                }

                // Validate that step-output references have matching depends_on.
                let referenced = Self::collect_referenced_step_ids(
                    workflow_name,
                    step,
                    "dependency validation",
                )?;
                for ref_id in &referenced {
                    if !step.depends_on.contains(ref_id) {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' references '${{step_output.{ref_id}.<output_name>}}' but does not list '{ref_id}' in depends_on",
                            step.id
                        )));
                    }
                }
            }

            // Cycle detection via topological traversal of current deps.
            let mut indegree_cycle = remaining_deps.clone();
            let mut ready_cycle: Vec<String> = indegree_cycle
                .iter()
                .filter_map(|(id, d)| (*d == 0).then_some(id.clone()))
                .collect();
            let mut seen_count = 0usize;
            while let Some(id) = ready_cycle.pop() {
                seen_count = seen_count.saturating_add(1);
                if let Some(deps) = dependents.get(&id) {
                    for dep_id in deps {
                        if let Some(value) = indegree_cycle.get_mut(dep_id) {
                            *value = value.saturating_sub(1);
                            if *value == 0 {
                                ready_cycle.push(dep_id.clone());
                            }
                        }
                    }
                }
            }
            if seen_count != workflow.steps.len() {
                return Err(ConductorError::Workflow(format!(
                    "workflow '{workflow_name}' contains a dependency cycle"
                )));
            }

            // Pre-compute required outputs per step.
            let required_outputs = Self::collect_required_outputs_by_step(workflow_name, workflow)?;

            // Plan impure timestamps for all steps.
            let mut wf_timestamps: BTreeMap<String, Option<ImpureTimestamp>> = BTreeMap::new();
            for step in &workflow.steps {
                let tool = unified.tools.get(&step.tool).ok_or_else(|| {
                    ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' step '{}' references unknown tool '{}'",
                        step.id, step.tool
                    ))
                })?;
                let timestamp = if tool.is_impure {
                    let workflow_timestamps =
                        state_document.impure_timestamps.entry(workflow_name.clone()).or_default();
                    Some(
                        *workflow_timestamps
                            .entry(step.id.clone())
                            .or_insert_with(Self::fresh_timestamp),
                    )
                } else {
                    None
                };
                wf_timestamps.insert(step.id.clone(), timestamp);
            }
            all_impure_timestamps.insert(workflow_name.clone(), wf_timestamps);

            dep_states.insert(
                workflow_name.clone(),
                WorkflowDepState {
                    remaining_deps,
                    dependents,
                    steps,
                    step_outputs: Arc::new(BTreeMap::new()),
                    required_outputs,
                    summary: RunSummary::new(),
                    pending_unsaved_hashes: BTreeSet::new(),
                },
            );
        }

        // Pre-compute total steps across all workflows for progress reporting.
        let total_steps: usize = dep_states.values().map(|ds| ds.steps.len()).sum();

        if dep_states.is_empty() {
            return Ok(ExecutionOutcome { summary, pending_unsaved_hashes, step_executions });
        }

        // ── Phase 2: Dependency-stream dispatch loop ──

        let mut completed_steps = 0usize;
        let state_snapshot = Arc::new(state.clone());
        let worker_count = self.workers.len();
        if worker_count == 0 {
            return Err(ConductorError::Internal(
                "no step workers available for execution".to_string(),
            ));
        }
        let mut next_worker = 0usize;
        let mut global_ready_queue: VecDeque<(String, String)> = VecDeque::new();
        let mut in_flight: FuturesUnordered<
            Pin<Box<dyn Future<Output = StepCompletionEvent> + Send>>,
        > = FuturesUnordered::new();

        // Build per-tool semaphores from max_concurrent_calls config.
        let tool_semaphores: BTreeMap<&str, Option<Arc<Semaphore>>> = unified
            .tools
            .iter()
            .map(|(name, spec)| {
                let sem = if spec.max_concurrent_calls > 0 {
                    #[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
                    let permits = spec.max_concurrent_calls as usize;
                    Some(Arc::new(Semaphore::new(permits)))
                } else {
                    None // -1 means unlimited
                };
                (name.as_str(), sem)
            })
            .collect();

        // Seed ready queue with zero-indegree steps from all workflows.
        for (wf_name, dep_state) in &dep_states {
            for (step_id, count) in &dep_state.remaining_deps {
                if *count == 0 {
                    global_ready_queue.push_back((wf_name.clone(), step_id.clone()));
                }
            }
        }

        loop {
            // Submit ready steps to workers with available capacity.
            let max_submit = worker_count.saturating_sub(in_flight.len());

            // Track whether any step was dispatched during this submit window.
            let mut dispatched = false;
            for _ in 0..max_submit {
                // Scan the ready queue for a step whose tool has capacity.
                let mut dispatched_this_scan = false;
                let scan_len = global_ready_queue.len();
                for _ in 0..scan_len {
                    let Some((wf_name, step_id)) = global_ready_queue.pop_front() else {
                        break;
                    };

                    let Some(dep_state) = dep_states.get(&wf_name) else {
                        continue;
                    };
                    let Some(step_spec) = dep_state.steps.get(&step_id) else {
                        continue;
                    };

                    // Check tool concurrency: acquire a semaphore permit if the tool
                    // has a max_concurrent_calls limit. Re-queue the step if at capacity.
                    let sem_for_tool =
                        tool_semaphores.get(step_spec.tool.as_str()).and_then(|s| s.as_ref());
                    let permit = if let Some(sem) = sem_for_tool {
                        if let Ok(p) = sem.clone().try_acquire_owned() {
                            Some(p)
                        } else {
                            // Tool at capacity — re-queue and try next step.
                            global_ready_queue.push_back((wf_name, step_id));
                            continue;
                        }
                    } else {
                        None // unlimited concurrency
                    };

                    // Look up per-tool max_retries from the unified config.
                    let max_retries = unified_shared
                        .tools
                        .get(&step_spec.tool)
                        .map_or(0, |spec| spec.max_retries);

                    let required_output_names =
                        dep_state.required_outputs.get(&step_id).cloned().unwrap_or_default();
                    let step_outputs = Arc::clone(&dep_state.step_outputs);
                    let impure_timestamp = all_impure_timestamps
                        .get(&wf_name)
                        .and_then(|ts| ts.get(&step_id))
                        .copied()
                        .flatten();

                    let request = StepExecutionRequest {
                        unified: unified_shared.clone(),
                        step: step_spec.clone(),
                        impure_timestamp,
                        workflow_name: wf_name.clone(),
                        state_snapshot: state_snapshot.clone(),
                        outermost_config_dir: outermost_config_dir.to_path_buf(),
                        conductor_tmp_dir: conductor_tmp_dir.to_path_buf(),
                        step_outputs,
                        required_output_names,
                    };

                    let worker_index = next_worker % worker_count;
                    next_worker = next_worker.saturating_add(1);
                    let worker = self.workers[worker_index].clone();
                    let is_pure = workflow_is_pure_map.get(&wf_name).copied().unwrap_or(false);
                    let tool_cache = self.tool_cache.clone();
                    in_flight.push(Box::pin(async move {
                        Self::dispatch_step_rpc(
                            worker,
                            request,
                            wf_name,
                            step_id,
                            worker_index,
                            max_retries,
                            permit,
                            is_pure,
                            tool_cache,
                        )
                        .await
                    }));
                    dispatched = true;
                    dispatched_this_scan = true;
                    break;
                }
                if !dispatched_this_scan {
                    break;
                }
            }
            if !dispatched && in_flight.is_empty() {
                break;
            }

            if in_flight.is_empty() {
                break;
            }

            // Wait for the next step to complete.
            let Some(event) = in_flight.next().await else {
                break;
            };

            // Process completion.
            let StepCompletionEvent {
                workflow_name: event_wf,
                step_id: event_step,
                worker_index: event_wi,
                result,
            } = event;

            let bundle = match result {
                Ok(bundle) => bundle,
                Err(err) => {
                    return Err(err);
                }
            };

            // Profile record — kept in the StepExecutionProfile shape compatible
            // with the profiler.  workflow_attempt and level_index are set to 0
            // in the new non-level model.
            step_executions.push(StepExecutionProfile {
                workflow_name: event_wf.clone(),
                workflow_display_name: workflow_display_names
                    .get(&event_wf)
                    .cloned()
                    .unwrap_or_else(|| event_wf.clone()),
                workflow_attempt: 0,
                level_index: 0,
                step_id: event_step.clone(),
                tool_name: bundle.tool_name.clone(),
                worker_index: event_wi,
                executed: bundle.executed,
                rematerialized: bundle.rematerialized,
                elapsed_ms: bundle.elapsed_ms,
                requested_output_count: bundle.requested_output_names.len(),
                pending_unsaved_hashes_count: bundle.pending_unsaved_hashes.len(),
                phase_timings: StepPhaseTimingProfile {
                    resolve_inputs_ms: bundle.phase_timings.resolve_inputs_ms,
                    resolve_specs_ms: bundle.phase_timings.resolve_specs_ms,
                    cache_probe_ms: bundle.phase_timings.cache_probe_ms,
                    materialization_ms: bundle.phase_timings.materialization_ms,
                    execution_ms: bundle.phase_timings.execution_ms,
                    capture_outputs_ms: bundle.phase_timings.capture_outputs_ms,
                    persistence_merge_ms: bundle.phase_timings.persistence_merge_ms,
                },
            });

            // Record completion with scheduler for EWMA runtime estimation.
            let record = StepCompletionRecord {
                step_id: event_step.clone(),
                tool_name: bundle.tool_name.clone(),
                worker_index: event_wi,
                executed: bundle.executed,
                observed_ms: bundle.elapsed_ms,
            };
            if let Err(err) = scheduler.record_completion(record).await {
                eprintln!("warning: failed to record step completion: {err}");
            }

            // Merge result into per-workflow state.
            if let Some(dep_state) = dep_states.get_mut(&event_wf) {
                if bundle.executed {
                    dep_state.summary.executed_instances =
                        dep_state.summary.executed_instances.saturating_add(1);
                    if bundle.rematerialized {
                        dep_state.summary.rematerialized_instances =
                            dep_state.summary.rematerialized_instances.saturating_add(1);
                    }
                } else {
                    dep_state.summary.cached_instances =
                        dep_state.summary.cached_instances.saturating_add(1);
                }

                dep_state
                    .pending_unsaved_hashes
                    .extend(bundle.pending_unsaved_hashes.iter().copied());

                let executed = bundle.executed;
                let step_hashes = Self::merge_step_result_into_state(
                    state,
                    bundle,
                    &mut dep_state.pending_unsaved_hashes,
                )?;
                Arc::make_mut(&mut dep_state.step_outputs).insert(event_step.clone(), step_hashes);

                // Decrement remaining_deps for dependents; push newly ready steps.
                if let Some(dependents_set) = dep_state.dependents.get(&event_step) {
                    for dep_id in dependents_set {
                        if let Some(count) = dep_state.remaining_deps.get_mut(dep_id) {
                            *count = count.saturating_sub(1);
                            if *count == 0 {
                                global_ready_queue.push_back((event_wf.clone(), dep_id.clone()));
                            }
                        }
                    }
                }

                // Send progress event after step completion.
                if let Some(ref tx) = progress_sender {
                    completed_steps = completed_steps.saturating_add(1);
                    let _ = tx.send(WorkflowStepEvent {
                        total_steps,
                        completed_steps,
                        workflow_name: event_wf.clone(),
                        step_id: event_step.clone(),
                        workflow_display_name: workflow_display_names
                            .get(&event_wf)
                            .cloned()
                            .unwrap_or_else(|| event_wf.clone()),
                        executed,
                        worker_index: event_wi,
                        worker_count,
                    });
                }
            }
        }

        // ── Phase 3: Aggregate per-workflow summaries ──

        for dep_state in dep_states.values() {
            summary.executed_instances =
                summary.executed_instances.saturating_add(dep_state.summary.executed_instances);
            summary.cached_instances =
                summary.cached_instances.saturating_add(dep_state.summary.cached_instances);
            summary.rematerialized_instances = summary
                .rematerialized_instances
                .saturating_add(dep_state.summary.rematerialized_instances);
            pending_unsaved_hashes.extend(dep_state.pending_unsaved_hashes.iter().copied());
        }

        Ok(ExecutionOutcome { summary, pending_unsaved_hashes, step_executions })
    }

    /// Routes one step execution to a worker via RPC. Retries up to
    /// `max_retries` times on failure, with the semaphore permit held across
    /// all attempts so the concurrency slot stays occupied.
    #[allow(clippy::too_many_arguments, clippy::cast_sign_loss)]
    async fn dispatch_step_rpc(
        worker: ActorRef<StepWorkerMessage>,
        request: StepExecutionRequest,
        workflow_name: String,
        step_id: String,
        worker_index: usize,
        max_retries: i32,
        _permit: Option<OwnedSemaphorePermit>,
        is_pure: bool,
        tool_cache: Option<Arc<ToolContentCache<C>>>,
    ) -> StepCompletionEvent {
        let max_attempts = max_retries.max(0) as u32 + 1;
        for attempt in 0..max_attempts {
            let call_result = match call_t!(
                worker.clone(),
                StepWorkerMessage::ExecuteStep,
                rpc_timeout_ms(),
                Box::new(request.clone())
            ) {
                Ok(Ok(bundle)) => {
                    return StepCompletionEvent {
                        workflow_name: workflow_name.clone(),
                        step_id: step_id.clone(),
                        worker_index,
                        result: Ok(bundle),
                    };
                }
                Ok(Err(err)) => Err(err),
                Err(rpc_err) => Err(ConductorError::Internal(format!(
                    "worker RPC failed for step '{step_id}': {rpc_err}",
                ))),
            };

            // For pure workflows, invalidate the tool cache on CorruptObject
            // so the retry re-fetches clean content from CAS.
            if is_pure {
                if let Err(ConductorError::Cas(mediapm_cas::CasError::CorruptObject { .. })) =
                    &call_result
                {
                    tracing::warn!(
                        tool_id = %request.step.tool,
                        "corrupt CAS object detected for pure workflow step, invalidating \
                         tool cache entry before retry"
                    );
                    if let Some(ref cache) = tool_cache {
                        let _ = cache.invalidate_tool_entry(&request.step.tool).await;
                    }
                }
            }

            let is_last_attempt = attempt.saturating_add(1) >= max_attempts;
            if is_last_attempt {
                return StepCompletionEvent {
                    workflow_name,
                    step_id,
                    worker_index,
                    result: call_result,
                };
            }

            sleep(Duration::from_millis(500)).await;
        }
        unreachable!()
    }

    /// Returns whether every step tool in one workflow is pure.
    fn workflow_is_pure(
        workflow_name: &str,
        workflow: &WorkflowSpec,
        tools: &BTreeMap<String, UnifiedToolSpec>,
    ) -> Result<bool, ConductorError> {
        for step in &workflow.steps {
            let tool = tools.get(&step.tool).ok_or_else(|| {
                ConductorError::Workflow(format!(
                    "workflow '{workflow_name}' step '{}' references unknown tool '{}'",
                    step.id, step.tool
                ))
            })?;
            if tool.is_impure {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Returns the user-facing workflow label used by progress UI rendering.
    ///
    /// When workflow metadata declares a display `name`, that label is shown
    /// instead of the map-key workflow id. Runtime identity and state storage
    /// still use the workflow id key.
    fn workflow_display_name<'a>(workflow_id: &'a str, workflow: &'a WorkflowSpec) -> &'a str {
        workflow.name.as_deref().unwrap_or(workflow_id)
    }

    /// Merges one finished step result into the mutable orchestration state.
    ///
    /// If multiple workflow steps resolve to the same deterministic instance
    /// key, this merge computes effective output persistence using
    /// [`merge_persistence_flags`] so persisted orchestration state reflects
    /// the combined caller intent (`save` uses tri-state max ordering:
    /// `unsaved < saved < full`).
    ///
    /// When one merge replaces an existing output hash with a new hash for the
    /// same deterministic instance/output slot, the displaced hash is queued in
    /// `pending_unsaved_hashes` for post-commit cleanup eligibility. Cleanup is
    /// still centralized in the state-store commit path, so displaced hashes
    /// are never deleted if workflow execution fails before commit.
    ///
    fn merge_step_result_into_state(
        state: &mut OrchestrationState,
        result: StepExecutionBundle,
        pending_unsaved_hashes: &mut BTreeSet<Hash>,
    ) -> Result<BTreeMap<String, Option<Hash>>, ConductorError> {
        let StepExecutionBundle {
            step_id: _,
            tool_name: _,
            worker_index: _,
            instance_key,
            instance,
            requested_output_names,
            executed: _,
            rematerialized: _,
            pending_unsaved_hashes: _,
            elapsed_ms: _,
            phase_timings: _,
        } = result;

        // Mark the instance as referenced from GC roots and initialise its
        // last_unreachable timestamp on first creation so GC will not evict
        // it immediately if it becomes unreferenced.
        state.referenced_instance_keys.insert(instance_key.clone());
        state
            .aux
            .entry(instance_key.clone())
            .or_insert(AuxData { last_unreachable: ImpureTimestamp::now() });

        let entry = state.instances.entry(instance_key.clone());
        let final_instance = match entry {
            std::collections::btree_map::Entry::Vacant(vacant) => vacant.insert(instance),
            std::collections::btree_map::Entry::Occupied(mut occupied) => {
                let existing = occupied.get_mut();
                existing.tool_name = instance.tool_name;
                existing.metadata = instance.metadata;
                existing.impure_timestamp = instance.impure_timestamp;
                existing.inputs = instance.inputs;

                for (output_name, next_output) in instance.outputs {
                    match existing.outputs.get_mut(&output_name) {
                        Some(current_output) => {
                            if current_output.hash != next_output.hash {
                                pending_unsaved_hashes.insert(current_output.hash);
                            }
                            current_output.hash = next_output.hash;
                            current_output.persistence = merge_persistence_flags([
                                current_output.persistence,
                                next_output.persistence,
                            ]);
                        }
                        None => {
                            existing.outputs.insert(output_name, next_output);
                        }
                    }
                }

                occupied.into_mut()
            }
        };

        requested_output_names
            .into_iter()
            .map(|name| {
                final_instance
                    .outputs
                    .get(&name)
                    .map(|output| {
                        // Empty captures are represented as None in step_outputs so
                        // downstream input-resolution can detect and reject them with
                        // a descriptive error instead of propagating empty bytes silently.
                        let hash_slot =
                            if output.allow_empty_capture { None } else { Some(output.hash) };
                        (name.clone(), hash_slot)
                    })
                    .ok_or_else(|| {
                        ConductorError::Internal(format!(
                            "instance '{instance_key}' missing output '{name}' after merge"
                        ))
                    })
            })
            .collect()
    }

    /// Collects warning messages for `depends_on` edges that do not consume a
    /// referenced output and do not target impure producer tools.
    ///
    /// These warnings are non-fatal and surfaced to users through stderr so
    /// they can simplify unnecessary ordering edges.
    fn collect_unnecessary_depends_on_warnings(
        workflow_name: &str,
        workflow: &WorkflowSpec,
        tools: &BTreeMap<String, UnifiedToolSpec>,
    ) -> Result<Vec<String>, ConductorError> {
        let steps_by_id =
            workflow.steps.iter().map(|step| (step.id.as_str(), step)).collect::<BTreeMap<_, _>>();

        let mut warnings = Vec::new();
        for step in &workflow.steps {
            let referenced_dependencies =
                Self::collect_referenced_step_ids(workflow_name, step, "depends_on warning")?;

            let mut explicit_dependencies = BTreeSet::new();
            for dependency_step_id in &step.depends_on {
                if !explicit_dependencies.insert(dependency_step_id.as_str()) {
                    continue;
                }

                if referenced_dependencies.contains(dependency_step_id) {
                    continue;
                }

                let Some(producer_step) = steps_by_id.get(dependency_step_id.as_str()) else {
                    continue;
                };
                let Some(producer_tool) = tools.get(&producer_step.tool) else {
                    continue;
                };
                if producer_tool.is_impure {
                    continue;
                }

                warnings.push(format!(
                    "workflow '{workflow_name}' step '{}' has depends_on '{dependency_step_id}' but does not consume '${{step_output.{dependency_step_id}.<output_name>}}' and tool '{}' is pure; consider removing this depends_on edge",
                    step.id,
                    producer_step.tool
                ));
            }
        }

        Ok(warnings)
    }

    /// Collects all producer step ids referenced by `${step_output...}`
    /// interpolation segments in one step input map.
    fn collect_referenced_step_ids(
        workflow_name: &str,
        step: &WorkflowStepSpec,
        context: &str,
    ) -> Result<BTreeSet<String>, ConductorError> {
        let mut referenced = BTreeSet::new();

        for (input_name, binding) in &step.inputs {
            binding.try_for_each_scalar(|item_index, binding_item| {
                let parsed_segments = parse_input_binding(binding_item).map_err(|err| {
                    ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' step '{}' input '{input_name}' has invalid {}binding '{}' while evaluating {context}: {err}",
                        step.id,
                        if matches!(binding, InputBinding::StringList(_)) {
                            format!("list item {item_index} ")
                        } else {
                            String::new()
                        },
                        binding_item,
                    ))
                })?;

                for segment in parsed_segments {
                    if let ParsedInputBindingSegment::StepOutput { step_id, .. } = segment {
                        referenced.insert(step_id.to_string());
                    }
                }

                Ok(())
            })?;
        }

        Ok(referenced)
    }

    /// Collects per-step output names that are referenced by downstream
    /// `${step_output.<step_id>.<output_name>}` input bindings.
    fn collect_required_outputs_by_step(
        workflow_name: &str,
        workflow: &WorkflowSpec,
    ) -> Result<BTreeMap<String, BTreeSet<String>>, ConductorError> {
        let mut required = BTreeMap::<String, BTreeSet<String>>::new();

        for step in &workflow.steps {
            for (input_name, binding) in &step.inputs {
                binding.try_for_each_scalar(|item_index, binding_item| {
                    let parsed_segments = parse_input_binding(binding_item).map_err(|err| {
                        ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' input '{input_name}' has invalid {}binding '{}': {err}",
                            step.id,
                            if matches!(binding, InputBinding::StringList(_)) {
                                format!("list item {item_index} ")
                            } else {
                                String::new()
                            },
                            binding_item,
                        ))
                    })?;

                    for segment in parsed_segments {
                        if let ParsedInputBindingSegment::StepOutput {
                            step_id,
                            output,
                            ..
                        } = segment
                        {
                            required
                                .entry(step_id.to_string())
                                .or_default()
                                .insert(output.to_string());
                        }
                    }

                    Ok(())
                })?;
            }
        }

        Ok(required)
    }

    /// Generates the monotonic impure timestamp stored in state config.
    fn fresh_timestamp() -> ImpureTimestamp {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        ImpureTimestamp { epoch_seconds: now.as_secs(), subsec_nanos: now.subsec_nanos() }
    }

    /// Resolves one outermost config directory into an absolute filesystem path.
    ///
    /// The execution hub forwards this directory to step workers so builtin
    /// `import`, builtin `export`, and builtin `fs` can resolve relative paths
    /// deterministically against the outermost config root.
    fn absolute_outermost_config_dir(
        directory: &Path,
    ) -> Result<std::path::PathBuf, ConductorError> {
        if directory.is_absolute() {
            return Ok(directory.to_path_buf());
        }

        let current_dir = std::env::current_dir().map_err(|source| ConductorError::Io {
            operation: "resolving current working directory for outermost config directory"
                .to_string(),
            path: Path::new(".").to_path_buf(),
            source,
        })?;
        Ok(current_dir.join(directory))
    }
}

#[cfg(test)]
#[path = "coordinator_tests.rs"]
mod tests;
