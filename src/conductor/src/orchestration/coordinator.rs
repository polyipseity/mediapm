//! Deterministic workflow coordinator for actor-backed conductor orchestration.
//!
//! This module keeps workflow sequencing, impure-timestamp planning, and state
//! merge logic in one place while delegating side effects to dedicated actors:
//! document loading, workflow-level execution, and CAS-backed state
//! persistence.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mediapm_cas::{CasApi, CasError, Hash};
use pulsebar::MultiProgress;

use crate::api::{
    RunSummary, RunWorkflowOptions, RuntimeDiagnostics, SchedulerDiagnostics,
    resolve_runtime_storage_paths,
};
use crate::error::ConductorError;
use crate::model::config::{
    ImpureTimestamp, InputBinding, ParsedInputBindingSegment, WorkflowSpec, WorkflowStepSpec,
    parse_input_binding,
};
use crate::model::state::{OrchestrationState, merge_persistence_flags};

use super::actors::documents::{DocumentLoaderClient, spawn_document_loader_actor};
use super::actors::execution_hub::{ExecutionHubClient, spawn_execution_hub_actor};
use super::actors::state_store::{StateStoreClient, spawn_state_store_actor};
use super::config::profile_output_path_from_env;
use super::profiler::{StepExecutionProfile, WorkflowRunProfile, write_profile_json};
use super::protocol::{
    CommitStateRequest, LevelExecutionRequest, LoadedDocuments, StepExecutionBundle, StepOutputs,
    UnifiedNickelDocument, UnifiedToolSpec,
};

/// Settle delay that allows the `MultiProgress` background render thread to flush
/// bar states after `finish_success` / `finish_error` before the `MultiProgress`
/// is dropped.
///
/// The render interval is 50 ms; 75 ms gives the thread at least one full cycle
/// to render the terminal row before the render loop is stopped.
const WORKFLOW_PROGRESS_SETTLE_MS: u64 = 75;

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
    cas: Arc<C>,
    /// Typed client for the document-loader actor.
    document_loader: Option<DocumentLoaderClient>,
    /// Typed client for the workflow execution hub actor.
    execution_hub: Option<ExecutionHubClient>,
    /// Typed client for the orchestration state-store actor.
    state_store: Option<StateStoreClient>,
}

impl<C> WorkflowCoordinator<C>
where
    C: CasApi + Send + Sync + 'static,
{
    /// Creates a coordinator bound to one CAS implementation.
    #[must_use]
    pub(super) fn new(cas: Arc<C>) -> Self {
        Self { cas, document_loader: None, execution_hub: None, state_store: None }
    }

    /// Returns the current in-memory orchestration-state snapshot published by the state-store actor.
    pub(super) async fn current_state(&self) -> Result<OrchestrationState, ConductorError> {
        if let Some(state_store) = &self.state_store {
            return state_store.current_state().await;
        }

        Ok(OrchestrationState::default())
    }

    /// Returns runtime diagnostics from the execution hub when it exists.
    pub(super) async fn runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        if let Some(execution_hub) = &self.execution_hub {
            return execution_hub.runtime_diagnostics().await;
        }

        Ok(Self::empty_runtime_diagnostics())
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
                rpc_fallbacks_total: 0,
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
    async fn ensure_runtime_support(&mut self) -> Result<(), ConductorError> {
        self.ensure_document_loader().await?;
        self.ensure_execution_hub().await?;
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

    /// Lazily spawns the workflow execution hub actor.
    async fn ensure_execution_hub(&mut self) -> Result<(), ConductorError> {
        if self.execution_hub.is_none() {
            self.execution_hub = Some(spawn_execution_hub_actor(self.cas.clone()).await?);
        }
        Ok(())
    }

    /// Lazily spawns the CAS-backed orchestration state-store actor.
    async fn ensure_state_store(&mut self) -> Result<(), ConductorError> {
        if self.state_store.is_none() {
            self.state_store = Some(spawn_state_store_actor(self.cas.clone()).await?);
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
        let conductor_state_config = resolved_runtime_paths.conductor_state_config.clone();
        let profile_output_path =
            options.profile_output_path.clone().or_else(profile_output_path_from_env);

        self.ensure_runtime_support().await?;
        let document_loader = self.document_loader.clone().ok_or_else(|| {
            ConductorError::Internal("document loader actor was not initialized".to_string())
        })?;
        let execution_hub = self.execution_hub.clone().ok_or_else(|| {
            ConductorError::Internal("execution hub actor was not initialized".to_string())
        })?;
        let state_store = self.state_store.clone().ok_or_else(|| {
            ConductorError::Internal("state store actor was not initialized".to_string())
        })?;

        let LoadedDocuments { machine_document, mut state_document, prior_state_pointer, unified } =
            document_loader
                .load_and_unify(user_ncl, machine_ncl, &conductor_state_config, options)
                .await?;
        let mut state = state_store.load_state_from_pointer(prior_state_pointer).await?;
        let outermost_config_dir = Self::absolute_outermost_config_dir(
            user_ncl.parent().or_else(|| machine_ncl.parent()).unwrap_or_else(|| Path::new(".")),
        )?;

        let execution_outcome = self
            .execute_workflows(
                execution_hub.clone(),
                &unified,
                &mut state_document,
                &mut state,
                &resolved_runtime_paths.conductor_tmp_dir,
                &outermost_config_dir,
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
            let runtime_diagnostics = execution_hub.runtime_diagnostics().await.unwrap_or_else(|error| {
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

    /// Executes all unified workflows level by level using the execution-hub actor.
    ///
    /// Progress-row messages intentionally avoid duplicate numeric counters so
    /// pulsebar can own count rendering while message text focuses on active
    /// step ids.
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
    async fn execute_workflows(
        &self,
        execution_hub: ExecutionHubClient,
        unified: &UnifiedNickelDocument,
        state_document: &mut crate::model::config::StateNickelDocument,
        state: &mut OrchestrationState,
        runtime_tmp_dir: &Path,
        outermost_config_dir: &Path,
    ) -> Result<ExecutionOutcome, ConductorError> {
        let unified_shared = Arc::new(unified.clone());
        let mut summary = RunSummary::new();
        let mut pending_unsaved_hashes = BTreeSet::new();
        let mut step_executions = Vec::new();

        // One MultiProgress drives all workflow bars for this run.
        // The background render thread fires every 50 ms so bars update
        // continuously even when execute_level is blocking in a long-running
        // tool subprocess — without requiring mutations on the hot path.
        let multi = MultiProgress::new();

        for (workflow_name, workflow) in &unified.workflows {
            let workflow_display_name = Self::workflow_display_name(workflow_name, workflow);
            let workflow_is_pure = Self::workflow_is_pure(workflow_name, workflow, &unified.tools)?;

            for warning in Self::collect_unnecessary_depends_on_warnings(
                workflow_name,
                workflow,
                &unified.tools,
            )? {
                eprintln!("warning: {warning}");
            }

            let total_steps = workflow.steps.len();
            if total_steps == 0 {
                multi.add_bar(1).with_message(workflow_display_name).finish_success("ready");
                continue;
            }

            let required_outputs_by_step =
                Self::collect_required_outputs_by_step(workflow_name, workflow)?;
            let mut recovery_attempted = false;
            let mut workflow_attempt = 0usize;

            'workflow_retry: loop {
                let levels = Self::topological_levels(workflow_name, workflow)?;
                let workflow_start_message = workflow_display_name.to_string();
                let workflow_progress = multi
                    .add_bar(total_steps.max(1) as u64)
                    .with_message(&workflow_start_message)
                    .with_format("{msg}  {bar}  {pos}/{total}  {rate}/s  ETA {eta}  {elapsed}");
                // Force the initial bar state into the MultiProgress snapshot so the
                // background render thread emits at least one row before the first
                // level is dispatched.
                workflow_progress.set_position(0);
                let mut dispatched_steps = 0usize;
                let mut step_outputs: StepOutputs = BTreeMap::new();
                let mut workflow_summary = RunSummary::new();
                let mut workflow_pending_unsaved_hashes = BTreeSet::new();

                for (level_index, level) in levels.into_iter().enumerate() {
                    let level_step_count = level.len();
                    workflow_progress.set_message(&Self::workflow_level_progress_message(
                        workflow_display_name,
                        dispatched_steps,
                        total_steps,
                        &level,
                    ));
                    // Advance on dispatch: the MultiProgress background thread
                    // renders this state every 50 ms, so the bar visually moves
                    // while the long-running execute_level await is in progress.
                    workflow_progress.set_position(dispatched_steps as u64);
                    workflow_progress.advance(level_step_count as u64);
                    dispatched_steps = dispatched_steps.saturating_add(level_step_count);
                    let state_snapshot = Arc::new(state.clone());
                    let step_outputs_snapshot = Arc::new(step_outputs.clone());
                    let impure_timestamps = Self::plan_impure_timestamps(
                        unified,
                        state_document,
                        workflow_name,
                        &level,
                    )?;

                    let dispatch_outcomes = match execution_hub
                        .execute_level(LevelExecutionRequest {
                            workflow_name: workflow_name.clone(),
                            level_index,
                            level: level.into_iter().cloned().collect(),
                            unified: unified_shared.clone(),
                            state_snapshot,
                            runtime_tmp_dir: runtime_tmp_dir.to_path_buf(),
                            outermost_config_dir: outermost_config_dir.to_path_buf(),
                            step_outputs: step_outputs_snapshot,
                            required_outputs_by_step: required_outputs_by_step.clone(),
                            impure_timestamps,
                        })
                        .await
                    {
                        Ok(outcomes) => outcomes,
                        Err(error) => {
                            let recoverable =
                                Self::recoverable_corrupt_output_context(workflow_name, &error);
                            let Some((
                                consumer_step_id,
                                producer_step_id,
                                output_name,
                                output_hash,
                            )) = recoverable
                            else {
                                workflow_progress.finish_error("failed");
                                tokio::time::sleep(Duration::from_millis(
                                    WORKFLOW_PROGRESS_SETTLE_MS,
                                ))
                                .await;
                                return Err(error);
                            };

                            if !workflow_is_pure {
                                workflow_progress.finish_error("failed");
                                tokio::time::sleep(Duration::from_millis(
                                    WORKFLOW_PROGRESS_SETTLE_MS,
                                ))
                                .await;
                                return Err(ConductorError::Workflow(format!(
                                    "{error}; workflow '{workflow_name}' is impure, so automatic corruption recovery is disabled"
                                )));
                            }

                            if recovery_attempted {
                                workflow_progress.finish_error("failed");
                                tokio::time::sleep(Duration::from_millis(
                                    WORKFLOW_PROGRESS_SETTLE_MS,
                                ))
                                .await;
                                return Err(error);
                            }

                            recovery_attempted = true;
                            let removed_instances = self
                                .recover_from_corrupt_output_hash(
                                    state,
                                    output_hash,
                                    &mut workflow_pending_unsaved_hashes,
                                )
                                .await?;

                            eprintln!(
                                "warning: workflow '{workflow_name}' detected corrupted cached output '{producer_step_id}.{output_name}' (hash '{output_hash}') while resolving step '{consumer_step_id}'; dropped {removed_instances} cached instance(s), removed corrupt CAS object, and retrying pure workflow once"
                            );

                            workflow_attempt = workflow_attempt.saturating_add(1);
                            workflow_progress.finish_error("retrying");
                            // Give the managed render thread one cycle to paint
                            // the retry status before allocating the replacement
                            // workflow bar for the next retry attempt.
                            tokio::time::sleep(Duration::from_millis(WORKFLOW_PROGRESS_SETTLE_MS))
                                .await;
                            continue 'workflow_retry;
                        }
                    };

                    for outcome in dispatch_outcomes {
                        let result = outcome.result;
                        step_executions.push(StepExecutionProfile {
                            workflow_name: workflow_name.clone(),
                            workflow_display_name: workflow_display_name.to_string(),
                            workflow_attempt,
                            level_index,
                            step_id: result.step_id.clone(),
                            tool_name: result.tool_name.clone(),
                            worker_index: result.worker_index,
                            executed: result.executed,
                            rematerialized: result.rematerialized,
                            fallback_used: result.fallback_used,
                            elapsed_ms: result.elapsed_ms,
                            requested_output_count: result.requested_output_names.len(),
                            pending_unsaved_hashes_count: result.pending_unsaved_hashes.len(),
                        });

                        if result.executed {
                            workflow_summary.executed_instances =
                                workflow_summary.executed_instances.saturating_add(1);
                            if result.rematerialized {
                                workflow_summary.rematerialized_instances =
                                    workflow_summary.rematerialized_instances.saturating_add(1);
                            }
                        } else {
                            workflow_summary.cached_instances =
                                workflow_summary.cached_instances.saturating_add(1);
                        }

                        workflow_pending_unsaved_hashes
                            .extend(result.pending_unsaved_hashes.iter().copied());
                        let step_id = result.step_id.clone();
                        let step_hashes = Self::merge_step_result_into_state(
                            state,
                            result,
                            &mut workflow_pending_unsaved_hashes,
                        )?;
                        step_outputs.insert(step_id, step_hashes);
                    }
                }

                summary.executed_instances =
                    summary.executed_instances.saturating_add(workflow_summary.executed_instances);
                summary.cached_instances =
                    summary.cached_instances.saturating_add(workflow_summary.cached_instances);
                summary.rematerialized_instances = summary
                    .rematerialized_instances
                    .saturating_add(workflow_summary.rematerialized_instances);
                pending_unsaved_hashes.extend(workflow_pending_unsaved_hashes);

                workflow_progress.finish_success("ready");
                break;
            }
        }

        // Allow the background render thread one final cycle to flush finished
        // bar states before `MultiProgress` is dropped and the thread is stopped.
        tokio::time::sleep(Duration::from_millis(WORKFLOW_PROGRESS_SETTLE_MS)).await;
        Ok(ExecutionOutcome { summary, pending_unsaved_hashes, step_executions })
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

    /// Extracts structured corruption context when one workflow output read failed integrity checks.
    fn recoverable_corrupt_output_context(
        workflow_name: &str,
        error: &ConductorError,
    ) -> Option<(String, String, String, Hash)> {
        let ConductorError::CorruptWorkflowOutput(context) = error else {
            return None;
        };

        let error_workflow = &context.workflow_name;

        if error_workflow != workflow_name {
            return None;
        }

        Some((
            context.consumer_step_id.clone(),
            context.producer_step_id.clone(),
            context.output_name.clone(),
            context.output_hash,
        ))
    }

    /// Drops cached instances that reference one corrupt output hash and removes that object from CAS.
    async fn recover_from_corrupt_output_hash(
        &self,
        state: &mut OrchestrationState,
        output_hash: Hash,
        pending_unsaved_hashes: &mut BTreeSet<Hash>,
    ) -> Result<usize, ConductorError> {
        let affected_keys = state
            .instances
            .iter()
            .filter_map(|(key, instance)| {
                instance
                    .outputs
                    .values()
                    .any(|output| output.hash == output_hash)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();

        for key in &affected_keys {
            state.instances.remove(key);
        }

        pending_unsaved_hashes.insert(output_hash);
        match self.cas.delete(output_hash).await {
            Ok(()) | Err(CasError::NotFound(_)) => {}
            Err(source) => return Err(ConductorError::Cas(source)),
        }

        Ok(affected_keys.len())
    }

    /// Returns the user-facing workflow label used by progress UI rendering.
    ///
    /// When workflow metadata declares a display `name`, that label is shown
    /// instead of the map-key workflow id. Runtime identity and state storage
    /// still use the workflow id key.
    fn workflow_display_name<'a>(workflow_id: &'a str, workflow: &'a WorkflowSpec) -> &'a str {
        workflow.name.as_deref().unwrap_or(workflow_id)
    }

    /// Builds one progress-row message that surfaces currently running step ids.
    ///
    /// Conductor progress bars now advance when a level is dispatched. This
    /// message keeps each row informative by showing the currently running
    /// level's step id preview while execution is in-flight.
    fn workflow_level_progress_message(
        workflow_display_name: &str,
        _dispatched_steps: usize,
        _total_steps: usize,
        level: &[&WorkflowStepSpec],
    ) -> String {
        let step_preview = Self::workflow_level_step_preview(level);
        format!("{workflow_display_name}  {step_preview}")
    }

    /// Renders a compact preview of step ids in one execution level.
    fn workflow_level_step_preview(level: &[&WorkflowStepSpec]) -> String {
        match level {
            [] => "...".to_string(),
            [single] => single.id.clone(),
            [first, second] => format!("{}, {}", first.id, second.id),
            [first, second, rest @ ..] => {
                format!("{}, {}, +{} more", first.id, second.id, rest.len())
            }
        }
    }

    /// Preallocates impure timestamps for one level before execution begins.
    fn plan_impure_timestamps(
        unified: &UnifiedNickelDocument,
        state_document: &mut crate::model::config::StateNickelDocument,
        workflow_name: &str,
        level: &[&WorkflowStepSpec],
    ) -> Result<BTreeMap<String, Option<ImpureTimestamp>>, ConductorError> {
        let mut impure_timestamps = BTreeMap::new();
        for step in level {
            let tool = unified.tools.get(&step.tool).ok_or_else(|| {
                ConductorError::Workflow(format!(
                    "workflow '{workflow_name}' step '{}' references unknown tool '{}'",
                    step.id, step.tool
                ))
            })?;
            let timestamp = if tool.is_impure {
                let workflow_timestamps =
                    state_document.impure_timestamps.entry(workflow_name.to_string()).or_default();
                let ts = *workflow_timestamps
                    .entry(step.id.clone())
                    .or_insert_with(Self::fresh_timestamp);
                Some(ts)
            } else {
                None
            };
            impure_timestamps.insert(step.id.clone(), timestamp);
        }
        Ok(impure_timestamps)
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
            fallback_used: _,
        } = result;

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

    /// Produces deterministic topological levels for one workflow.
    fn topological_levels<'a>(
        workflow_name: &str,
        workflow: &'a WorkflowSpec,
    ) -> Result<Vec<Vec<&'a WorkflowStepSpec>>, ConductorError> {
        let mut steps_by_id: BTreeMap<String, &WorkflowStepSpec> = BTreeMap::new();

        for step in &workflow.steps {
            if step.id.trim().is_empty() {
                return Err(ConductorError::Workflow(format!(
                    "workflow '{workflow_name}' contains a step with empty id"
                )));
            }
            if steps_by_id.insert(step.id.clone(), step).is_some() {
                return Err(ConductorError::Workflow(format!(
                    "workflow '{workflow_name}' contains duplicate step id '{}'",
                    step.id
                )));
            }
        }

        let mut indegree: BTreeMap<String, usize> =
            steps_by_id.keys().cloned().map(|id| (id, 0usize)).collect();
        let mut edges: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        for step in &workflow.steps {
            let mut dependencies: BTreeSet<String> = BTreeSet::new();
            for dependency in &step.depends_on {
                if !dependencies.insert(dependency.clone()) {
                    return Err(ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' step '{}' contains duplicate depends_on entry '{dependency}'",
                        step.id
                    )));
                }
            }

            let referenced_dependencies =
                Self::collect_referenced_step_ids(workflow_name, step, "topological validation")?;
            for referenced_step_id in referenced_dependencies {
                if !dependencies.contains(&referenced_step_id) {
                    return Err(ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' step '{}' references '${{step_output.{referenced_step_id}.<output_name>}}' but does not list '{referenced_step_id}' in depends_on",
                        step.id
                    )));
                }
            }

            for dependency in dependencies {
                if !steps_by_id.contains_key(&dependency) {
                    return Err(ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' step '{}' depends on unknown step '{dependency}'",
                        step.id
                    )));
                }
                if dependency == step.id {
                    return Err(ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' step '{}' depends on itself",
                        step.id
                    )));
                }

                edges.entry(dependency).or_default().insert(step.id.clone());
                if let Some(value) = indegree.get_mut(&step.id) {
                    *value = value.saturating_add(1);
                }
            }
        }

        let mut ready: BTreeSet<String> = indegree
            .iter()
            .filter_map(|(id, degree)| (*degree == 0).then_some(id.clone()))
            .collect();
        let mut levels = Vec::new();
        let mut seen = 0usize;

        while !ready.is_empty() {
            let current_ids: Vec<String> = ready.iter().cloned().collect();
            ready.clear();

            let mut current_level = Vec::with_capacity(current_ids.len());
            for id in &current_ids {
                let step = steps_by_id.get(id).copied().ok_or_else(|| {
                    ConductorError::Internal(format!(
                        "topological level produced unknown step id '{id}'"
                    ))
                })?;
                current_level.push(step);
                seen = seen.saturating_add(1);
            }
            levels.push(current_level);

            for id in current_ids {
                if let Some(dependents) = edges.get(&id) {
                    for dependent in dependents {
                        if let Some(value) = indegree.get_mut(dependent) {
                            *value = value.saturating_sub(1);
                            if *value == 0 {
                                ready.insert(dependent.clone());
                            }
                        }
                    }
                }
            }
        }

        if seen != workflow.steps.len() {
            return Err(ConductorError::Workflow(format!(
                "workflow '{workflow_name}' contains a dependency cycle"
            )));
        }

        Ok(levels)
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
