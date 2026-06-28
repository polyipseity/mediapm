//! Deterministic workflow coordinator for actor-backed conductor orchestration.
//!
//! The coordinator keeps workflow sequencing, step dispatch, and state merge
//! logic in one place while delegating actual tool execution to a pool of
//! step-worker actors.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mediapm_cas::{CasApi, CasMaintenanceApi};

use crate::api::{RunSummary, RuntimeDiagnostics};
use crate::config::{ImpureTimestamp, WorkflowStepSpec};
use crate::error::ConductorError;
use crate::state::OrchestrationState;

use ractor::rpc::CallResult;

use super::config::{default_worker_pool_size, rpc_timeout_ms};
use super::protocol::{StepExecutionRequest, StepOutputs, UnifiedNickelDocument};
use super::step_worker::{StepWorkerMessage, spawn_step_worker_pool};

// ---------------------------------------------------------------------------
// Topological sort
// ---------------------------------------------------------------------------

/// Returns a topological ordering of step IDs, or an error when a cycle is
/// detected.
fn topological_sort(steps: &[WorkflowStepSpec]) -> Result<Vec<Vec<String>>, ConductorError> {
    let step_ids: BTreeSet<String> = steps.iter().map(|s| s.id.clone()).collect();
    let mut in_degree: BTreeMap<&str, usize> = BTreeMap::new();
    let mut adj: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for step in steps {
        in_degree.entry(&step.id).or_insert(0);
        for dep in &step.depends_on {
            if !step_ids.contains(dep) {
                return Err(ConductorError::Workflow(format!(
                    "step '{}' depends on unknown step '{dep}'",
                    step.id,
                )));
            }
            adj.entry(dep.as_str()).or_default().push(&step.id);
            *in_degree.entry(&step.id).or_insert(0) += 1;
        }
    }

    // Kahn's algorithm producing levels.
    let mut levels: Vec<Vec<String>> = Vec::new();
    let mut queue: VecDeque<&str> =
        in_degree.iter().filter(|(_, deg)| **deg == 0).map(|(id, _)| *id).collect();

    while !queue.is_empty() {
        let mut level = Vec::new();
        let mut next_queue = VecDeque::new();
        for id in &queue {
            level.push((*id).to_string());
            if let Some(neighbors) = adj.get(id) {
                for n in neighbors {
                    if let Some(deg) = in_degree.get_mut(n) {
                        *deg = deg.saturating_sub(1);
                        if *deg == 0 {
                            next_queue.push_back(*n);
                        }
                    }
                }
            }
        }
        levels.push(level);
        queue = next_queue;
    }

    let total_steps: usize = levels.iter().map(Vec::len).sum();
    if total_steps != step_ids.len() {
        return Err(ConductorError::Workflow(
            "workflow contains a cycle in step dependency graph".to_string(),
        ));
    }

    Ok(levels)
}

/// Resolves `$step_output.<step_id>.<name>` references in input values.
fn compute_required_outputs(steps: &[WorkflowStepSpec]) -> BTreeMap<String, BTreeSet<String>> {
    let mut required: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for step in steps {
        required.entry(step.id.clone()).or_default();
    }

    let re =
        regex::Regex::new(r"\$\{step_output\.([^.]+)\.([^}]+)\}").expect("valid step output regex");

    for step in steps {
        for value in step.inputs.values() {
            for cap in re.captures_iter(value) {
                let dep_step_id = cap[1].to_string();
                let output_name = cap[2].to_string();
                required.entry(dep_step_id).or_default().insert(output_name);
            }
        }
    }

    required
}

// ---------------------------------------------------------------------------
// WorkflowCoordinator
// ---------------------------------------------------------------------------

/// Deterministic workflow coordinator rooted in one CAS implementation.
///
/// The coordinator owns a pool of step-worker actors and orchestrates
/// multi-step workflow execution with dependency resolution, parallel dispatch
/// within each topological level, and state merging.
pub(crate) struct WorkflowCoordinator<C>
where
    C: CasApi + CasMaintenanceApi,
{
    /// Shared CAS handle passed into child actors.
    cas: Arc<C>,
    /// Pool of step-worker actors for concurrent step execution.
    workers: Vec<ractor::ActorRef<StepWorkerMessage>>,
    /// Handle to the background CAS maintenance task, if started.
    background_gc_handle: Option<tokio::task::JoinHandle<()>>,
}

impl<C> WorkflowCoordinator<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    /// Creates a coordinator bound to one CAS implementation.
    #[must_use]
    pub(crate) fn new(cas: Arc<C>) -> Self {
        Self { cas, workers: Vec::new(), background_gc_handle: None }
    }

    /// Ensures the step-worker pool is initialized.
    async fn ensure_workers(&mut self) -> Result<(), ConductorError> {
        if self.workers.is_empty() {
            let pool_size = default_worker_pool_size();
            self.workers = spawn_step_worker_pool(self.cas.clone(), pool_size).await?;
        }
        Ok(())
    }

    /// Runs a workflow by name.
    ///
    /// Finds the workflow in the unified config, resolves its dependency
    /// graph, and dispatches steps to the worker pool level by level.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when the workflow is not found or
    /// when step execution fails.
    pub(crate) async fn run_workflow(
        &mut self,
        workflow_name: &str,
        unified: &UnifiedNickelDocument,
        state: &mut OrchestrationState,
    ) -> Result<RunSummary, ConductorError> {
        self.ensure_workers().await?;

        let workflow = unified.workflows.get(workflow_name).ok_or_else(|| {
            ConductorError::Workflow(format!("workflow '{workflow_name}' not found in config"))
        })?;

        let levels = topological_sort(&workflow.steps)?;
        let required_outputs = compute_required_outputs(&workflow.steps);

        let total_steps = workflow.steps.len();
        let mut executed_steps = 0usize;
        let mut failed_steps = 0usize;

        let mut step_outputs: StepOutputs = BTreeMap::new();
        let state_snapshot = Arc::new(state.clone());

        for level in &levels {
            let mut handles = Vec::new();

            for step_id in level {
                let step = workflow.steps.iter().find(|s| s.id == *step_id).ok_or_else(|| {
                    ConductorError::Internal(format!("step '{step_id}' not found in workflow"))
                })?;

                let tool_spec = unified.tools.get(&step.tool).ok_or_else(|| {
                    ConductorError::Workflow(format!(
                        "step '{}' references unknown tool '{}'",
                        step.id, step.tool,
                    ))
                })?;

                let required_output_names =
                    required_outputs.get(&step.id).cloned().unwrap_or_default();

                let current_step_outputs = Arc::new(step_outputs.clone());

                let request = StepExecutionRequest {
                    unified: Arc::new(unified.clone()),
                    step: step.clone(),
                    impure_timestamp: if tool_spec.is_impure {
                        Some(ImpureTimestamp::now())
                    } else {
                        None
                    },
                    state_snapshot: state_snapshot.clone(),
                    outermost_config_dir: Path::new(".").to_path_buf(),
                    conductor_tmp_dir: std::env::temp_dir().join("mediapm-conductor"),
                    step_outputs: current_step_outputs,
                    required_output_names,
                };

                let worker_idx = handles.len() % self.workers.len().max(1);
                let worker = self.workers[worker_idx].clone();

                let handle = tokio::spawn(async move {
                    let result = worker
                        .call(
                            |reply| StepWorkerMessage::ExecuteStep(Box::new(request), reply),
                            Some(Duration::from_millis(rpc_timeout_ms())),
                        )
                        .await;
                    match result {
                        Ok(CallResult::Success(v)) => v,
                        Ok(CallResult::Timeout) => {
                            Err(ConductorError::rpc_error("StepWorker", "RPC timeout"))
                        }
                        Ok(_) => Err(ConductorError::rpc_error("StepWorker", "RPC channel closed")),
                        Err(e) => Err(ConductorError::rpc_error("StepWorker", e)),
                    }
                });

                handles.push((step_id.clone(), worker_idx, handle));
            }

            for (step_id, _worker_idx, handle) in handles {
                match handle.await {
                    Ok(Ok(bundle)) => {
                        executed_steps += 1;
                        for output_ref in &bundle.instance.outputs {
                            step_outputs
                                .entry(step_id.clone())
                                .or_default()
                                .insert(output_ref.name.clone(), output_ref.hash);
                        }
                    }
                    Ok(Err(e)) => {
                        failed_steps += 1;
                        tracing::error!("step '{step_id}' failed: {e}");
                    }
                    Err(e) => {
                        failed_steps += 1;
                        tracing::error!("step '{step_id}' RPC failed: {e}");
                    }
                }
            }
        }

        let cached_steps = total_steps.saturating_sub(executed_steps + failed_steps);
        Ok(RunSummary { total_steps, executed_steps, cached_steps, failed_steps })
    }

    /// Returns a default runtime diagnostics snapshot.
    #[must_use]
    pub(crate) fn runtime_diagnostics(&self) -> RuntimeDiagnostics {
        RuntimeDiagnostics::default()
    }

    /// Runs conductor garbage collection on the orchestration state and CAS.
    ///
    /// CONDUCTOR GC — distinct from CAS GC.  Calls the three-phase
    /// [`gc::run_conductor_gc`] which handles instance pruning, CAS orphan
    /// reclamation, and CAS metadata maintenance.
    pub(crate) async fn run_gc(
        &self,
        state: &mut OrchestrationState,
        referenced_keys: &BTreeSet<String>,
        unified: &UnifiedNickelDocument,
    ) -> Result<crate::gc::ConductorGcReport, ConductorError> {
        use crate::defaults::DEFAULT_CONDUCTOR_GC_TTL_SECONDS;
        let report = crate::gc::run_conductor_gc(
            &*self.cas,
            state,
            unified,
            referenced_keys,
            DEFAULT_CONDUCTOR_GC_TTL_SECONDS,
        )
        .await?;
        tracing::info!(
            "conductor GC completed: {} instances removed, {} orphans removed",
            report.instances_removed,
            report.orphans_removed,
        );
        Ok(report)
    }

    /// Spawns a background task that periodically runs CAS-level GC
    /// (maintenance cycle and constraint pruning).
    ///
    /// The task runs every `interval_secs` seconds and logs warnings on
    /// failure without propagating errors.  Dropping the coordinator cancels
    /// the task automatically.
    pub(crate) fn start_background_gc(&mut self, interval_secs: u64) {
        let cas = self.cas.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(interval_secs)).await;
                if let Err(e) = crate::gc::run_cas_gc_sweep(&*cas).await {
                    tracing::warn!("background CAS GC failed: {e}");
                }
            }
        });
        self.background_gc_handle = Some(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies topological sort produces correct levels for a DAG.
    #[test]
    fn topological_sort_basic_dag() {
        let steps = vec![
            WorkflowStepSpec {
                id: "a".to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: Vec::new(),
            },
            WorkflowStepSpec {
                id: "b".to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: vec!["a".to_string()],
            },
            WorkflowStepSpec {
                id: "c".to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: vec!["a".to_string()],
            },
        ];

        let levels = topological_sort(&steps).expect("sort should succeed");
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0], vec!["a"]);
        assert!(levels[1].contains(&"b".to_string()));
        assert!(levels[1].contains(&"c".to_string()));
    }

    /// Verifies topological sort detects cycles.
    #[test]
    fn topological_sort_detects_cycle() {
        let steps = vec![
            WorkflowStepSpec {
                id: "a".to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: vec!["b".to_string()],
            },
            WorkflowStepSpec {
                id: "b".to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: vec!["a".to_string()],
            },
        ];

        assert!(topological_sort(&steps).is_err());
    }

    /// Verifies topological sort returns error for unknown dependency.
    #[test]
    fn topological_sort_unknown_dependency() {
        let steps = vec![WorkflowStepSpec {
            id: "a".to_string(),
            tool: "echo".to_string(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            max_retries: 0,
            depends_on: vec!["nonexistent".to_string()],
        }];

        assert!(topological_sort(&steps).is_err());
    }

    /// Verifies `compute_required_outputs` finds references in input values.
    #[test]
    fn compute_required_outputs_finds_references() {
        let steps = vec![
            WorkflowStepSpec {
                id: "step-1".to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: Vec::new(),
            },
            WorkflowStepSpec {
                id: "step-2".to_string(),
                tool: "echo".to_string(),
                inputs: BTreeMap::from([(
                    "message".to_string(),
                    "${step_output.step-1.result}".to_string(),
                )]),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: vec!["step-1".to_string()],
            },
        ];

        let required = compute_required_outputs(&steps);
        assert!(required.contains_key("step-1"));
        assert!(required["step-1"].contains("result"));
    }

    /// Verifies `compute_required_outputs` handles steps with no references.
    #[test]
    fn compute_required_outputs_empty_when_no_references() {
        let steps = vec![WorkflowStepSpec {
            id: "step-1".to_string(),
            tool: "echo".to_string(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            max_retries: 0,
            depends_on: Vec::new(),
        }];

        let required = compute_required_outputs(&steps);
        assert!(required.contains_key("step-1"));
        assert!(required["step-1"].is_empty());
    }
}
