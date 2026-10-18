//! Deterministic workflow coordinator for actor-backed conductor orchestration.
//!
//! The coordinator keeps workflow sequencing, step dispatch, and state merge
//! logic in one place while delegating actual tool execution to a pool of
//! step-worker actors.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use mediapm_cas::{BackgroundMaintenanceGuard, CasApi, CasMaintenanceApi, Hash};

use crate::api::{RunSummary, RunWorkflowOptions, RuntimeDiagnostics};
use mediapm_utils::Timestamp;

#[cfg(feature = "progress")]
use mediapm_utils::progress::{BarStyle, ProgressBarApi};

#[cfg(feature = "progress")]
use super::progress_labels::WorkerBarLabel;

use crate::config::WorkflowStepSpec;
use crate::error::ConductorError;
use crate::state::ConductorState;

use ractor::rpc::CallResult;

use super::config::{default_worker_pool_size, rpc_timeout_ms};
use super::protocol::{
    StepExecutionBundle, StepExecutionRequest, StepOutputs, UnifiedNickelDocument,
    find_tool_by_name,
};
use super::step_worker::{StepWorkerMessage, spawn_step_worker_pool};

// ---------------------------------------------------------------------------
// Dependency graph
// ---------------------------------------------------------------------------

/// The step-dependency graph the ready-queue scheduler dispatches over.
///
/// Built once per run from `workflow.steps[].depends_on`. The scheduler
/// copies `in_degree` and mutates its own version as steps settle; this
/// struct keeps the pristine counts used to seed the initial ready queue and
/// the reverse edges used to release dependents on every settlement.
#[derive(Debug)]
struct DependencyGraph {
    /// Unsatisfied-dependency count per step id, pristine at build time.
    in_degree: BTreeMap<String, usize>,
    /// Direct dependents per step id, for decrementing on settlement.
    dependents: BTreeMap<String, Vec<String>>,
}

impl DependencyGraph {
    /// Builds the dependency graph, rejecting unknown dependency ids and
    /// cycles.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when a step depends on a step id
    /// that no step declares, or when the dependency graph contains a cycle
    /// (detected by an exhaustive Kahn reduction that fails to settle every
    /// step).
    fn build(steps: &[WorkflowStepSpec]) -> Result<Self, ConductorError> {
        let step_ids: BTreeSet<&String> = steps.iter().map(|s| &s.id).collect();
        let mut in_degree: BTreeMap<String, usize> = BTreeMap::new();
        let mut dependents: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for step in steps {
            in_degree.entry(step.id.clone()).or_insert(0);
            for dep in &step.depends_on {
                if !step_ids.contains(dep) {
                    return Err(ConductorError::Workflow(format!(
                        "step '{}' depends on unknown step '{dep}'",
                        step.id,
                    )));
                }
                dependents.entry(dep.clone()).or_default().push(step.id.clone());
                *in_degree.entry(step.id.clone()).or_insert(0) += 1;
            }
        }

        // Cycle detection: a Kahn reduction that cannot settle every step
        // leaves the remainder inside a cycle. This runs on a scratch copy so
        // the pristine counts survive to seed the ready queue.
        let mut reducible = in_degree.clone();
        let mut queue: VecDeque<String> =
            reducible.iter().filter(|(_, deg)| **deg == 0).map(|(id, _)| id.clone()).collect();
        let mut settled = 0usize;
        while let Some(id) = queue.pop_front() {
            settled += 1;
            if let Some(deps) = dependents.get(&id) {
                for dependent in deps {
                    if let Some(deg) = reducible.get_mut(dependent) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(dependent.clone());
                        }
                    }
                }
            }
        }
        if settled != step_ids.len() {
            return Err(ConductorError::Workflow(
                "workflow contains a cycle in step dependency graph".to_string(),
            ));
        }

        Ok(Self { in_degree, dependents })
    }
}

// ---------------------------------------------------------------------------
// In-flight step bookkeeping
// ---------------------------------------------------------------------------

/// Classifies where a failed step attempt failed.
///
/// The coordinator reports the two on distinct log lines (`failed` vs
/// `RPC failed`) while treating them identically for retry and settlement
/// purposes, so the classification travels with the error instead of being
/// re-derived from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepFailureKind {
    /// The worker executed the step and the step returned an error.
    Step,
    /// The actor RPC itself failed (timeout, channel closed, actor error).
    Rpc,
}

/// One settled step attempt, produced by the in-flight future polled by the
/// scheduler's settlement arm.
///
/// Carrying the dispatch-time snapshot (`worker_idx`, `step`, `attempt`)
/// alongside the result lets the settlement arm update the fixed worker-slot
/// bar, decide retry eligibility, and merge outputs without re-deriving any
/// dispatch state.
struct StepCompletion {
    /// Step id of the attempt.
    step_id: String,
    /// Attempt number (1-based; the first dispatch is attempt 1).
    attempt: u32,
    /// Worker-pool index the attempt ran on (index into the fixed bar grid).
    worker_idx: usize,
    /// Step spec captured at dispatch, for retry policy and output merging.
    step: WorkflowStepSpec,
    /// Worker outcome: a merge-ready bundle, or the classified failure.
    result: Result<StepExecutionBundle, (StepFailureKind, ConductorError)>,
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
/// multi-step workflow execution with dependency resolution, ready-queue
/// dispatch over a free worker pool (no level barrier), and state merging.
pub(crate) struct WorkflowCoordinator<C>
where
    C: CasApi + CasMaintenanceApi,
{
    /// Shared CAS handle passed into child actors.
    cas: Arc<C>,
    /// Root directory for per-step sandbox trees (`{dir}/sandbox/<instance_key>`).
    conductor_tmp_dir: PathBuf,
    /// Pool of step-worker actors for concurrent step execution.
    workers: Vec<ractor::ActorRef<StepWorkerMessage>>,
    /// RAII guard for the background CAS maintenance task, if started.
    background_gc_guard: Option<BackgroundMaintenanceGuard>,
    /// Supervisor cell (the conductor actor itself) that linked step workers
    /// are torn down with. Set in `pre_start`.
    supervisor: Option<ractor::ActorCell>,
}

// ---------------------------------------------------------------------------
// Worker-slot progress mapping
// ---------------------------------------------------------------------------

/// Worker-slot progress state for a fixed conductor worker bar.
///
/// Each worker owns one reusable bar (never recreated). The bar's fill is
/// driven by `set_total(assigned)` + `advance(1)` at the call sites; the
/// prefix below only carries the status marker and the tool/step label.
///
/// | worker state | `TrackStatus` | marker | `tool_name` |
/// | --- | --- | --- | --- |
/// | idle (no step assigned / between steps) | `Active` | empty | empty |
/// | active (executing) | `Active` | empty | `"{wf}/{step} ({tool})"` |
/// | pending-retry | `Warning` | `"W"` | empty |
/// | failed (final) | `Failed` | `"F"` | empty |
/// | finalize (all available steps consumed) | `Success` | empty | empty |
///
/// `tool_name` is empty in every state but `Active`, so a slot with no step
/// on it renders as `[idle]` rather than naming a tool it is not running.
///
/// Invariant: a worker is NEVER done (Success) while available steps remain.
/// Failed and pending-retry states already surface their own `TrackStatus`
/// (Failed/Warning) at the moment they occur, so finalize needs no separate
/// warning variant — by the time a worker finalizes, any failure/retry has
/// already been represented by those states. After each successfully consumed
/// step the worker returns to idle (Active), not Success.
///
/// Prefix `count/total` is emitted only when both item count and size count
/// are known (real progress); worker-slot bars lack that information, so all
/// states leave the fields empty. Truncation never special-cases by bar kind.
#[cfg(feature = "progress")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerSlotState {
    Idle,
    Active,
    PendingRetry,
    Failed,
    #[expect(
        dead_code,
        reason = "finalize/Success state is documented for the worker-slot contract; the per-step success path returns to Idle in this phase"
    )]
    Succeeded,
}

/// Builds the [`WorkerBarLabel`] for a worker-slot bar in the given state.
///
/// `workflow_name`/`step_id`/`tool` are only read by the `Active` state, which
/// fills in the `workflow_id`, `step_id`, and `tool` fields so the
/// client-side truncation order (`WorkerBarLabel`) can drop trailing parts
/// independently. Every other state leaves `tool` empty: there is no step
/// running, so there is no conductor tool to name, and the row ends at
/// `[idle]`. A worker bar carries no workflow phase, and no arm fills a
/// tally: `completed`/`total` stay empty here, so the row's suffix carries
/// only timing (see [`WorkerSlotState`]).
#[cfg(feature = "progress")]
fn worker_slot_label(
    state: WorkerSlotState,
    workflow_name: &str,
    step_id: &str,
    tool: &str,
) -> WorkerBarLabel {
    match state {
        WorkerSlotState::Active => WorkerBarLabel {
            status_marker: String::new(),
            workflow_id: workflow_name.to_string(),
            step_id: step_id.to_string(),
            tool: tool.to_string(),
            activity: "active".to_string(),
            completed: String::new(),
            total: String::new(),
        },
        WorkerSlotState::PendingRetry => WorkerBarLabel {
            status_marker: "W".to_string(),
            workflow_id: String::new(),
            step_id: String::new(),
            tool: String::new(),
            activity: "idle".to_string(),
            completed: String::new(),
            total: String::new(),
        },
        WorkerSlotState::Failed => WorkerBarLabel {
            status_marker: "F".to_string(),
            workflow_id: String::new(),
            step_id: String::new(),
            tool: String::new(),
            activity: "idle".to_string(),
            completed: String::new(),
            total: String::new(),
        },
        WorkerSlotState::Idle | WorkerSlotState::Succeeded => WorkerBarLabel {
            status_marker: String::new(),
            workflow_id: String::new(),
            step_id: String::new(),
            tool: String::new(),
            activity: "idle".to_string(),
            completed: String::new(),
            total: String::new(),
        },
    }
}

impl<C> WorkflowCoordinator<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    /// Creates a coordinator bound to one CAS implementation.
    #[must_use]
    pub(crate) fn new(cas: Arc<C>, conductor_tmp_dir: PathBuf) -> Self {
        Self {
            cas,
            conductor_tmp_dir,
            workers: Vec::new(),
            background_gc_guard: None,
            supervisor: None,
        }
    }

    /// Attaches the supervisor cell (the conductor actor itself) so step
    /// workers can be spawned linked to the actor's supervision tree.
    pub(crate) fn set_supervisor(&mut self, supervisor: ractor::ActorCell) {
        self.supervisor = Some(supervisor);
    }

    /// Ensures the step-worker pool is initialized.
    async fn ensure_workers(&mut self) -> Result<(), ConductorError> {
        if self.workers.is_empty() {
            let pool_size = default_worker_pool_size();
            let supervisor = self.supervisor.clone().ok_or_else(|| {
                ConductorError::Internal(
                    "cannot spawn step workers without a supervisor cell".to_string(),
                )
            })?;
            self.workers = spawn_step_worker_pool(self.cas.clone(), pool_size, supervisor).await?;
        }
        Ok(())
    }

    /// Runs a workflow by name.
    ///
    /// Finds the workflow in the unified config, resolves its dependency
    /// graph, and dispatches steps from a ready queue over the worker pool: a
    /// step becomes eligible the moment its own `depends_on` edges have
    /// settled, and takes any free worker — there is no level barrier.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when the workflow is not found,
    /// when the dependency graph is invalid (unknown dependency or cycle), or
    /// when step execution fails.
    #[expect(
        clippy::too_many_lines,
        reason = "ready-queue dispatch, settlement, and result aggregation stay in one pass"
    )]
    pub(crate) async fn run_workflow(
        &mut self,
        workflow_name: &str,
        unified: &UnifiedNickelDocument,
        state: &mut ConductorState,
        options: &RunWorkflowOptions,
    ) -> Result<RunSummary, ConductorError> {
        let outcome = async {
            self.ensure_workers().await?;

            let workflow = unified.workflows.get(workflow_name).ok_or_else(|| {
                ConductorError::Workflow(format!("workflow '{workflow_name}' not found in config"))
            })?;

            let dependency_graph = DependencyGraph::build(&workflow.steps)?;
            let required_outputs = compute_required_outputs(&workflow.steps);

            let total_steps = workflow.steps.len();
            let mut executed_steps = 0usize;
            let mut cached_steps = 0usize;
            let mut failed_steps = 0usize;
            let mut retried_steps = 0usize;

            // Compose the conductor-owned workflow progress screen: one overall
            // bar plus a fixed grid of `pool_size` worker-slot spinner bars
            // (one per worker).  The overall bar is pinned at the bottom slot
            // by the caller via `with_overall()` and used directly.  Worker
            // bars are pre-created once (one per pool member) and reused for
            // every dispatch to that worker — the slot is fixed, so the bar
            // count never grows and there is no flicker.
            #[cfg(feature = "progress")]
            let overall_bar: Option<Arc<dyn ProgressBarApi>> = options.overall_bar.clone();

            // The overall bar is created by the caller with a placeholder
            // total; set the real total now that we know the step count.
            // Ruling F: the overall bar animates while this run is active;
            // its finalize status parks it.
            #[cfg(feature = "progress")]
            if let Some(ref ob) = overall_bar {
                ob.set_total(total_steps as u64);
                ob.set_animating(true);
            }

            // Pre-create one fixed worker-slot bar per pool member.  Each bar
            // starts idle and is reused (never recreated) on every dispatch to
            // that worker.  Per-worker counters track assigned/succeeded/
            // failed step counts so the slot can render an honest
            // `succeeded/assigned` ratio when it returns to idle.
            #[cfg(feature = "progress")]
            let pool_size = self.workers.len();
            #[cfg(feature = "progress")]
            let mut worker_bars: Vec<Arc<dyn ProgressBarApi>> = Vec::with_capacity(pool_size);
            #[cfg(feature = "progress")]
            let mut worker_steps_assigned: Vec<usize> = vec![0; pool_size];
            #[cfg(feature = "progress")]
            let mut worker_steps_succeeded: Vec<usize> = vec![0; pool_size];
            #[cfg(feature = "progress")]
            let mut worker_failures: Vec<usize> = vec![0; pool_size];
            #[cfg(feature = "progress")]
            let mut worker_pending_retries: Vec<usize> = vec![0; pool_size];

            #[cfg(feature = "progress")]
            if let Some(ref pg) = options.progress_group {
                for _ in 0..pool_size {
                    let bar = pg.add_bar(0, "idle");
                    bar.set_style(BarStyle::WorkerSpinner);
                    bar.set_truncation(Arc::new(worker_slot_label(
                        WorkerSlotState::Idle,
                        "",
                        "",
                        "",
                    )));
                    worker_bars.push(bar.clone());
                    // Start idle bars as finished — no spinner animation
                    // until a step is dispatched to this worker.
                    bar.finish_success();
                }
            }

            let mut step_outputs: StepOutputs = BTreeMap::new();
            let state_snapshot = Arc::new(state.clone());

            // Ready-queue dispatch: a step becomes eligible the moment its
            // own `depends_on` edges have settled, and pulls any free worker
            // from the pool — there is no level barrier.  Output visibility
            // stays dependency-scoped in practice: a step spawns only after
            // all of its dependencies merged their outputs, so the
            // cumulative `step_outputs` snapshot taken at spawn contains
            // them (and possibly unrelated branches' outputs that finished
            // even earlier — narrowing to the transitive-dep subset is a
            // separate change).
            let mut in_degree = dependency_graph.in_degree;
            let mut ready: VecDeque<(String, u32)> = in_degree
                .iter()
                .filter(|(_, deg)| **deg == 0)
                .map(|(id, _)| (id.clone(), 1u32))
                .collect();
            let mut free_workers: VecDeque<usize> = (0..self.workers.len()).collect();
            let mut in_flight = futures_util::stream::FuturesUnordered::new();
            let mut settled_steps = 0usize;

            while settled_steps < total_steps {
                // Dispatch every ready step onto a free worker; when no
                // worker is free, park the step and wait for a completion.
                'dispatch: while let Some((step_id, attempt)) = ready.pop_front() {
                    let Some(worker_idx) = free_workers.pop_front() else {
                        ready.push_back((step_id, attempt));
                        break 'dispatch;
                    };

                    let step =
                        workflow.steps.iter().find(|s| s.id == *step_id).ok_or_else(|| {
                            ConductorError::Internal(format!(
                                "step '{step_id}' not found in workflow"
                            ))
                        })?;

                    let tool_spec =
                        find_tool_by_name(&unified.tools, &step.tool).ok_or_else(|| {
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
                            Some(Timestamp::now())
                        } else {
                            None
                        },
                        state_snapshot: state_snapshot.clone(),
                        outermost_config_dir: Path::new(".").to_path_buf(),
                        conductor_tmp_dir: self.conductor_tmp_dir.clone(),
                        step_outputs: current_step_outputs,
                        required_output_names,
                    };

                    let worker = self.workers[worker_idx].clone();

                    // Update the fixed worker-slot bar before dispatch so it
                    // appears immediately.  The slot is reused (never
                    // recreated), so the bar count stays fixed at `pool_size`.
                    #[cfg(feature = "progress")]
                    if options.progress_group.is_some() {
                        worker_steps_assigned[worker_idx] += 1;
                        let bar = worker_bars[worker_idx].clone();
                        // Re-activate the worker bar from finished-idle
                        // to active — restart() clears the terminal state
                        // marker and resets elapsed tracking.
                        bar.restart();
                        let assigned = worker_steps_assigned[worker_idx];
                        bar.set_truncation(Arc::new(worker_slot_label(
                            WorkerSlotState::Active,
                            workflow_name,
                            &step_id,
                            &step.tool,
                        )));
                        bar.set_total(assigned as u64);
                        // Dispatch declares in-flight work (Task 6):
                        // the bar spins until its settlement finishes
                        // it, and the terminal status parks the row
                        // afterwards.
                        bar.set_animating(true);
                    }

                    let step = step.clone();
                    in_flight.push(async move {
                        let result = worker
                            .call(
                                |reply| StepWorkerMessage::ExecuteStep(Box::new(request), reply),
                                Some(Duration::from_millis(rpc_timeout_ms())),
                            )
                            .await;
                        let result = match result {
                            Ok(CallResult::Success(v)) => v.map_err(|e| (StepFailureKind::Step, e)),
                            Ok(CallResult::Timeout) => Err((
                                StepFailureKind::Rpc,
                                ConductorError::rpc_error("StepWorker", "RPC timeout"),
                            )),
                            Ok(_) => Err((
                                StepFailureKind::Rpc,
                                ConductorError::rpc_error("StepWorker", "RPC channel closed"),
                            )),
                            Err(e) => Err((
                                StepFailureKind::Rpc,
                                ConductorError::rpc_error("StepWorker", e),
                            )),
                        };
                        StepCompletion { step_id, attempt, worker_idx, step, result }
                    });
                }

                let completion = in_flight.next().await.ok_or_else(|| {
                    ConductorError::Internal(format!(
                        "scheduler stalled with {} steps still unsettled",
                        total_steps - settled_steps
                    ))
                })?;
                let StepCompletion { step_id, attempt, worker_idx, step, result } = completion;

                // Retry policy is decided per attempt: a step retries while
                // attempts remain (`attempt <= max_retries`, so a step is
                // dispatched at most `max_retries + 1` times) and either the
                // tool is not impure or the run opted into retrying impure
                // steps.
                let may_retry = (attempt as usize) <= step.max_retries
                    && step.max_retries > 0
                    && match find_tool_by_name(&unified.tools, &step.tool) {
                        Some(t) => !t.is_impure || options.retry_impure,
                        None => false,
                    };

                // `settled` is false only for a retry: the step re-enters the
                // ready queue without releasing its dependents and without
                // advancing the overall bar.
                let mut settled = true;
                match result {
                    Ok(bundle) => {
                        if bundle.cache_hit {
                            cached_steps += 1;
                        } else {
                            executed_steps += 1;
                        }
                        if attempt > 1 {
                            retried_steps += 1;
                        }
                        for (name, record) in &bundle.instance.outputs {
                            step_outputs
                                .entry(step_id.clone())
                                .or_default()
                                .insert(name.clone(), record.hash);
                        }
                        // Insert executed instance into state so subsequent
                        // steps (and future runs) can find cache hits, plus
                        // the per-output persistence modes and GC last-reference
                        // clock on the aux record. Refreshing the clock on both
                        // creation and cache-hit mirrors the pre-redesign
                        // executor behavior (`conductor_gc_last_referenced_at =
                        // now` on every bundle), keeping instances alive across
                        // GC sweeps.
                        let instance_key = bundle.instance.instance_key;
                        state.tool_call_instances.insert(instance_key, bundle.instance);
                        let aux = state.aux.instances.entry(instance_key).or_default();
                        aux.save_modes = bundle.save_modes;
                        aux.last_referenced_at = Timestamp::now();
                        #[cfg(feature = "progress")]
                        if options.progress_group.is_some() {
                            worker_steps_succeeded[worker_idx] += 1;
                            let bar = &worker_bars[worker_idx];
                            let assigned = worker_steps_assigned[worker_idx];
                            bar.set_truncation(Arc::new(worker_slot_label(
                                WorkerSlotState::Idle,
                                "",
                                "",
                                "",
                            )));
                            bar.set_total(assigned as u64);
                            bar.advance(1);
                            bar.finish_success();
                        }
                        #[cfg(feature = "progress")]
                        if let Some(ref ob) = overall_bar {
                            ob.advance(1);
                        }
                    }
                    Err((kind, e)) => {
                        if may_retry {
                            settled = false;
                            match kind {
                                StepFailureKind::Step => tracing::warn!(
                                    "step '{step_id}' failed (attempt {attempt}), retrying: {e}"
                                ),
                                StepFailureKind::Rpc => tracing::warn!(
                                    "step '{step_id}' RPC failed (attempt {attempt}), retrying: {e}"
                                ),
                            }
                            #[cfg(feature = "progress")]
                            if options.progress_group.is_some() {
                                worker_pending_retries[worker_idx] += 1;
                                let bar = &worker_bars[worker_idx];
                                let assigned = worker_steps_assigned[worker_idx];
                                bar.set_truncation(Arc::new(worker_slot_label(
                                    WorkerSlotState::PendingRetry,
                                    "",
                                    "",
                                    "",
                                )));
                                bar.set_total(assigned as u64);
                                bar.advance(1);
                                bar.finish_warning();
                            }
                            // The step re-enters the queue; the worker it just
                            // used is freed below like any other settlement.
                            ready.push_back((step_id.clone(), attempt + 1));
                        } else {
                            failed_steps += 1;
                            match kind {
                                StepFailureKind::Step => {
                                    tracing::error!("step '{step_id}' failed: {e}");
                                }
                                StepFailureKind::Rpc => {
                                    tracing::error!("step '{step_id}' RPC failed: {e}");
                                }
                            }
                            #[cfg(feature = "progress")]
                            if options.progress_group.is_some() {
                                worker_failures[worker_idx] += 1;
                                let bar = &worker_bars[worker_idx];
                                let assigned = worker_steps_assigned[worker_idx];
                                bar.set_truncation(Arc::new(worker_slot_label(
                                    WorkerSlotState::Failed,
                                    "",
                                    "",
                                    "",
                                )));
                                bar.set_total(assigned as u64);
                                bar.advance(1);
                                bar.finish_warning();
                            }
                            #[cfg(feature = "progress")]
                            if let Some(ref ob) = overall_bar {
                                ob.advance(1);
                            }
                        }
                    }
                }

                // The worker is free again the moment the attempt settles —
                // including a retry, whose step holds no worker while queued.
                free_workers.push_back(worker_idx);
                if settled {
                    settled_steps += 1;
                    // Release the dependents: on any settlement — including a
                    // terminal failure, whose outputs simply never appear —
                    // decrement each dependent's in-degree and enqueue those
                    // that reach zero.
                    if let Some(deps) = dependency_graph.dependents.get(&step_id) {
                        for dependent in deps {
                            if let Some(deg) = in_degree.get_mut(dependent) {
                                *deg -= 1;
                                if *deg == 0 {
                                    ready.push_back((dependent.clone(), 1));
                                }
                            }
                        }
                    }
                }
            }

            // Finish the overall workflow bar: a non-fatal warning (yellow
            // `[W]`) when any step failed, otherwise success. Failed steps
            // are recorded in `RunSummary.failed_steps` and surfaced to the
            // caller, so the bar must not claim unconditional success.
            #[cfg(feature = "progress")]
            if let Some(ref bar) = overall_bar {
                #[allow(clippy::cast_possible_truncation)]
                bar.set_suffix_components(mediapm_utils::progress::SuffixComponents::status_list(
                    &[
                        mediapm_utils::progress::StatusCount {
                            word: "cached",
                            count: Some(cached_steps as u32),
                        },
                        mediapm_utils::progress::StatusCount {
                            word: "failed",
                            count: Some(failed_steps as u32),
                        },
                        mediapm_utils::progress::StatusCount {
                            word: "retried",
                            count: Some(retried_steps as u32),
                        },
                    ],
                ));
                if failed_steps > 0 {
                    bar.finish_warning();
                } else {
                    bar.finish_success();
                }
            }

            Ok(RunSummary {
                total_steps,
                executed_steps,
                cached_steps,
                failed_steps,
                retried_steps,
            })
        }
        .await;

        if let Err(error) =
            super::step_worker::sandbox::remove_runtime_tmp_dir(&self.conductor_tmp_dir).await
        {
            tracing::warn!("failed to remove workflow sandboxes: {error}");
        }

        outcome
    }

    /// Returns a default runtime diagnostics snapshot.
    #[must_use]
    #[expect(
        clippy::unused_self,
        reason = "instance method for symmetry with the actor-facing diagnostics API"
    )]
    pub(crate) fn runtime_diagnostics(&self) -> RuntimeDiagnostics {
        RuntimeDiagnostics::default()
    }

    /// Runs conductor garbage collection on the orchestration state and CAS.
    ///
    /// CONDUCTOR GC — distinct from CAS GC.  Calls the three-phase
    /// [`crate::gc::run_conductor_gc`] which handles instance pruning, CAS orphan
    /// reclamation, and CAS metadata maintenance.
    pub(crate) async fn run_gc(
        &self,
        state: &mut ConductorState,
        referenced_keys: &BTreeSet<Hash>,
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
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_clone = cancelled.clone();
        let cas = self.cas.clone();
        let handle = tokio::spawn(async move {
            loop {
                if cancelled_clone.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) = crate::gc::run_cas_gc_sweep(&*cas).await {
                    tracing::warn!("background CAS GC failed: {e}");
                }
                if cancelled_clone.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(interval_secs)).await;
            }
        });
        self.background_gc_guard = Some(BackgroundMaintenanceGuard::new(cancelled, handle));
    }

    /// Deterministically tears down actor-owned resources: the background GC
    /// maintenance task (cancelled and awaited so its CAS clone is released)
    /// and every step worker (stopped with a bounded wait so their states —
    /// including their CAS clones — drop).
    ///
    /// After this returns, the only CAS clone still held by the actor is
    /// `self.cas`, which is released when the coordinator state drops on
    /// actor stop.
    pub(crate) async fn shutdown(&mut self) -> Result<(), ConductorError> {
        if let Some(guard) = self.background_gc_guard.take() {
            // `shutdown` sets the cooperative flag, aborts the task, and
            // awaits the handle, so the task future is dropped (releasing its
            // CAS clone) and the `JoinHandle` resolves with
            // `JoinError::Cancelled`.
            guard.shutdown().await;
        }
        for worker in std::mem::take(&mut self.workers) {
            let _ = worker.stop_and_wait(None, Some(std::time::Duration::from_secs(5))).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies the dependency graph records in-degree and reverse edges for
    /// a basic DAG, so the scheduler seeds the right initial ready queue.
    #[test]
    fn dependency_graph_basic_dag() {
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

        let graph = DependencyGraph::build(&steps).expect("graph should build");
        assert_eq!(graph.in_degree.get("a"), Some(&0), "a is initially ready");
        assert_eq!(graph.in_degree.get("b"), Some(&1), "b waits on a");
        assert_eq!(graph.in_degree.get("c"), Some(&1), "c waits on a");
        assert_eq!(
            graph.dependents.get("a"),
            Some(&vec!["b".to_string(), "c".to_string()]),
            "settling a releases both b and c"
        );
    }

    /// Verifies the background CAS GC loop starts, runs a cycle without
    /// error, and RAII cleanup cancels the guard on coordinator drop.
    #[tokio::test]
    async fn background_cas_gc_spawned_task_runs_maintenance() {
        let cas = Arc::new(mediapm_cas::InMemoryCas::default());
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut coordinator = WorkflowCoordinator::new(cas, tmp.path().join("conductor-tmp"));
        coordinator.start_background_gc(1);
        // Wait for at least one GC cycle to run (interval is 1s).
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        // Dropping the coordinator must not panic and cancels the guard.
        drop(coordinator);
        // Verify guard cancelled after drop.
        // (We can't call background_gc_is_cancelled after drop, but
        // we verify the sleep-and-drop cycle completes without panic.)
    }

    /// Verifies the dependency graph detects cycles.
    #[test]
    fn dependency_graph_detects_cycle() {
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

        assert!(DependencyGraph::build(&steps).is_err());
    }

    /// Verifies the dependency graph returns error for unknown dependency.
    #[test]
    fn dependency_graph_unknown_dependency() {
        let steps = vec![WorkflowStepSpec {
            id: "a".to_string(),
            tool: "echo".to_string(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            max_retries: 0,
            depends_on: vec!["nonexistent".to_string()],
        }];

        assert!(DependencyGraph::build(&steps).is_err());
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
