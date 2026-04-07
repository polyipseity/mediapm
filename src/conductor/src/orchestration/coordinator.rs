//! Deterministic workflow coordinator for actor-backed conductor orchestration.
//!
//! This module keeps workflow sequencing, impure-timestamp planning, and state
//! merge logic in one place while delegating side effects to dedicated actors:
//! document loading, workflow-level execution, and CAS-backed state
//! persistence.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::{CasApi, Hash};
use pulsebar::ProgressBar;

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
use super::protocol::{
    CommitStateRequest, LevelExecutionRequest, LoadedDocuments, StepExecutionBundle, StepOutputs,
    UnifiedNickelDocument, UnifiedToolSpec,
};

/// Summary and cleanup metadata returned by one workflow run.
#[derive(Debug)]
struct ExecutionOutcome {
    /// User-visible run summary accumulated across all workflows.
    summary: RunSummary,
    /// Unsaved hashes that may be deleted after state persistence completes.
    pending_unsaved_hashes: BTreeSet<Hash>,
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

        Ok(RuntimeDiagnostics {
            worker_pool_size: 0,
            scheduler: SchedulerDiagnostics {
                ewma_alpha: super::config::scheduler_ewma_alpha(),
                unknown_cost_ms: super::config::unknown_step_cost_ms(),
                tool_estimates: Vec::new(),
                rpc_fallbacks_total: 0,
            },
            workers: Vec::new(),
            recent_traces: Vec::new(),
        })
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
        let resolved_runtime_paths =
            resolve_runtime_storage_paths(user_ncl, machine_ncl, &options.runtime_storage_paths);
        let state_ncl = resolved_runtime_paths.config_state.clone();

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
            document_loader.load_and_unify(user_ncl, machine_ncl, &state_ncl, options).await?;
        let mut state = state_store.load_state_from_pointer(prior_state_pointer).await?;
        let outermost_config_dir = Self::absolute_outermost_config_dir(
            user_ncl.parent().or_else(|| machine_ncl.parent()).unwrap_or_else(|| Path::new(".")),
        )?;

        let outcome = self
            .execute_workflows(
                execution_hub,
                &unified,
                &mut state_document,
                &mut state,
                &resolved_runtime_paths.conductor_dir,
                &outermost_config_dir,
            )
            .await?;
        let current_state_pointer = state_store
            .commit_run(CommitStateRequest {
                next_state: state,
                pending_unsaved_hashes: outcome.pending_unsaved_hashes,
                unified: unified.clone(),
                prior_state_pointer,
            })
            .await?;
        state_document.state_pointer = Some(current_state_pointer);
        document_loader.persist_machine_document(machine_ncl, machine_document).await?;
        document_loader.persist_state_document(&state_ncl, state_document).await?;

        Ok(outcome.summary)
    }

    /// Executes all unified workflows level by level using the execution-hub actor.
    ///
    /// Progress-bar labels intentionally stay compact (task name only) so
    /// pulsebar's built-in counters and percentage display remain readable
    /// without duplicate text.
    async fn execute_workflows(
        &self,
        execution_hub: ExecutionHubClient,
        unified: &UnifiedNickelDocument,
        state_document: &mut crate::model::config::StateNickelDocument,
        state: &mut OrchestrationState,
        runtime_storage_dir: &Path,
        outermost_config_dir: &Path,
    ) -> Result<ExecutionOutcome, ConductorError> {
        let unified_shared = Arc::new(unified.clone());
        let mut summary = RunSummary::new();
        let mut pending_unsaved_hashes = BTreeSet::new();

        for (workflow_name, workflow) in &unified.workflows {
            for warning in Self::collect_unnecessary_depends_on_warnings(
                workflow_name,
                workflow,
                &unified.tools,
            )? {
                eprintln!("warning: {warning}");
            }

            let levels = Self::topological_levels(workflow_name, workflow)?;
            let total_steps = workflow.steps.len();
            let workflow_progress =
                ProgressBar::new(total_steps.max(1) as u64).with_message(workflow_name);

            if total_steps == 0 {
                workflow_progress.finish_success(&format!("{workflow_name} complete"));
                continue;
            }

            let required_outputs_by_step =
                Self::collect_required_outputs_by_step(workflow_name, workflow)?;
            let mut step_outputs: StepOutputs = BTreeMap::new();

            for (level_index, level) in levels.into_iter().enumerate() {
                let level_step_count = level.len();
                let state_snapshot = Arc::new(state.clone());
                let step_outputs_snapshot = Arc::new(step_outputs.clone());
                let impure_timestamps =
                    Self::plan_impure_timestamps(unified, state_document, workflow_name, &level)?;

                let dispatch_outcomes = match execution_hub
                    .execute_level(LevelExecutionRequest {
                        workflow_name: workflow_name.clone(),
                        level_index,
                        level: level.into_iter().cloned().collect(),
                        unified: unified_shared.clone(),
                        state_snapshot,
                        runtime_storage_dir: runtime_storage_dir.to_path_buf(),
                        outermost_config_dir: outermost_config_dir.to_path_buf(),
                        step_outputs: step_outputs_snapshot,
                        required_outputs_by_step: required_outputs_by_step.clone(),
                        impure_timestamps,
                    })
                    .await
                {
                    Ok(outcomes) => outcomes,
                    Err(error) => {
                        workflow_progress.finish_error(&format!("{workflow_name} failed"));
                        return Err(error);
                    }
                };

                for outcome in dispatch_outcomes {
                    let result = outcome.result;
                    if result.executed {
                        summary.executed_instances = summary.executed_instances.saturating_add(1);
                        if result.rematerialized {
                            summary.rematerialized_instances =
                                summary.rematerialized_instances.saturating_add(1);
                        }
                    } else {
                        summary.cached_instances = summary.cached_instances.saturating_add(1);
                    }

                    pending_unsaved_hashes.extend(result.pending_unsaved_hashes.iter().copied());
                    let step_id = result.step_id.clone();
                    let step_hashes = Self::merge_step_result_into_state(state, result)?;
                    step_outputs.insert(step_id, step_hashes);
                }

                workflow_progress.advance(level_step_count as u64);
            }

            workflow_progress.finish_success(&format!("{workflow_name} complete"));
        }

        Ok(ExecutionOutcome { summary, pending_unsaved_hashes })
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
    /// the combined caller intent (`save`: AND, `force_full`: OR).
    fn merge_step_result_into_state(
        state: &mut OrchestrationState,
        result: StepExecutionBundle,
    ) -> Result<BTreeMap<String, Hash>, ConductorError> {
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
                    .map(|output| (name.clone(), output.hash))
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
                        if let ParsedInputBindingSegment::StepOutput { step_id, output } = segment {
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
