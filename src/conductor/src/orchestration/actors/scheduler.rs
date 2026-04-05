//! Actor-backed scheduling and diagnostics for conductor workflows.
//!
//! The scheduler actor owns runtime estimates, worker-queue metrics, and trace
//! emission so the execution hub can focus on worker dispatch while the
//! coordinator stays purely workflow-oriented.

use std::collections::{BTreeMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};

use crate::api::{
    RuntimeDiagnostics, SchedulerDiagnostics, SchedulerTraceEvent, SchedulerTraceKind,
    ToolRuntimeEstimate, WorkerQueueDiagnostics,
};
use crate::error::ConductorError;
use crate::model::config::WorkflowStepSpec;
use crate::orchestration::config::{
    DEFAULT_RPC_TIMEOUT_MS, scheduler_ewma_alpha, scheduler_trace_capacity, unknown_step_cost_ms,
};
use crate::orchestration::protocol::{StepCompletionRecord, StepWorkerAssignment};

/// Typed client for the scheduler actor.
#[derive(Debug, Clone)]
pub(super) struct SchedulerClient {
    /// Actor reference used for all scheduling RPC calls.
    actor: ActorRef<SchedulerMessage>,
}

impl SchedulerClient {
    /// Creates a typed client around one scheduler actor reference.
    #[must_use]
    fn new(actor: ActorRef<SchedulerMessage>) -> Self {
        Self { actor }
    }

    /// Plans one workflow level onto the available workers.
    pub(super) async fn plan_level(
        &self,
        workflow_name: &str,
        level_index: usize,
        level: Vec<WorkflowStepSpec>,
        worker_count: usize,
    ) -> Result<Vec<StepWorkerAssignment>, ConductorError> {
        call_t!(
            self.actor,
            SchedulerMessage::PlanLevel,
            DEFAULT_RPC_TIMEOUT_MS,
            workflow_name.to_string(),
            level_index,
            level,
            worker_count
        )
        .map_err(|err| {
            ConductorError::Internal(format!("scheduler plan_level RPC failed: {err}"))
        })?
    }

    /// Records one completed step so estimates, metrics, and traces stay current.
    pub(super) async fn record_completion(
        &self,
        record: StepCompletionRecord,
    ) -> Result<(), ConductorError> {
        call_t!(self.actor, SchedulerMessage::RecordCompletion, DEFAULT_RPC_TIMEOUT_MS, record)
            .map_err(|err| {
                ConductorError::Internal(format!("scheduler record_completion RPC failed: {err}"))
            })?
    }

    /// Returns the latest diagnostics snapshot owned by the scheduler actor.
    pub(super) async fn runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        call_t!(self.actor, SchedulerMessage::GetRuntimeDiagnostics, DEFAULT_RPC_TIMEOUT_MS)
            .map_err(|err| {
                ConductorError::Internal(format!(
                    "scheduler get_runtime_diagnostics RPC failed: {err}"
                ))
            })?
    }
}

/// Requests supported by the scheduler actor.
#[derive(Debug)]
enum SchedulerMessage {
    /// Plans one workflow level and records assignment-side diagnostics.
    PlanLevel(
        String,
        usize,
        Vec<WorkflowStepSpec>,
        usize,
        RpcReplyPort<Result<Vec<StepWorkerAssignment>, ConductorError>>,
    ),
    /// Records one finished step and updates runtime estimates and traces.
    RecordCompletion(StepCompletionRecord, RpcReplyPort<Result<(), ConductorError>>),
    /// Returns the current diagnostics snapshot.
    GetRuntimeDiagnostics(RpcReplyPort<Result<RuntimeDiagnostics, ConductorError>>),
}

/// Actor marker for the scheduler runtime service.
#[derive(Debug, Clone, Copy, Default)]
struct SchedulerActor;

/// Deterministic runtime-estimation state keyed by tool name.
#[derive(Debug, Clone)]
struct SchedulerState {
    /// Exponentially weighted runtime estimates per immutable tool name.
    ewma_by_tool_ms: BTreeMap<String, f64>,
    /// Smoothing factor for new observations.
    ewma_alpha: f64,
    /// Fallback runtime estimate for tools with no history.
    unknown_cost_ms: f64,
}

/// Per-worker queue metrics used for diagnostics snapshots.
#[derive(Debug, Clone, Default)]
struct WorkerQueueMetricsState {
    /// Total steps assigned to the worker across the actor lifetime.
    assigned_steps_total: u64,
    /// Total steps completed by the worker across the actor lifetime.
    completed_steps_total: u64,
    /// Steps currently in flight for the worker.
    in_flight: u64,
    /// Highest observed `in_flight` level for the worker.
    peak_in_flight: u64,
    /// Steps assigned during the most recently planned workflow level.
    last_level_assigned_steps: u64,
    /// Estimated load assigned during the most recently planned workflow level.
    last_level_estimated_load_ms: f64,
    /// Cumulative estimated load assigned to the worker.
    cumulative_estimated_load_ms: f64,
    /// Cumulative observed load completed by the worker.
    cumulative_observed_load_ms: f64,
    /// Number of worker-RPC failures that forced fallback execution.
    rpc_failures_total: u64,
    /// Number of times the worker's assigned step completed via fallback.
    fallback_executions_total: u64,
}

impl WorkerQueueMetricsState {
    /// Records one new step assignment against this worker.
    fn record_level_assignment(&mut self, estimated_ms: f64) {
        self.assigned_steps_total = self.assigned_steps_total.saturating_add(1);
        self.last_level_assigned_steps = self.last_level_assigned_steps.saturating_add(1);
        self.last_level_estimated_load_ms += estimated_ms;
        self.cumulative_estimated_load_ms += estimated_ms;
        self.in_flight = self.in_flight.saturating_add(1);
        self.peak_in_flight = self.peak_in_flight.max(self.in_flight);
    }

    /// Records one step completion and its fallback/RPC-failure status.
    fn record_completion(&mut self, observed_ms: f64, rpc_failed: bool, fallback_used: bool) {
        self.completed_steps_total = self.completed_steps_total.saturating_add(1);
        self.cumulative_observed_load_ms += observed_ms;
        self.in_flight = self.in_flight.saturating_sub(1);
        if rpc_failed {
            self.rpc_failures_total = self.rpc_failures_total.saturating_add(1);
        }
        if fallback_used {
            self.fallback_executions_total = self.fallback_executions_total.saturating_add(1);
        }
    }
}

/// Ring-buffered runtime instrumentation owned by the scheduler actor.
#[derive(Debug, Clone)]
struct RuntimeInstrumentation {
    /// Metrics keyed by worker index.
    worker_metrics: Vec<WorkerQueueMetricsState>,
    /// Recent scheduler trace events in insertion order.
    traces: VecDeque<SchedulerTraceEvent>,
    /// Maximum number of trace events retained in memory.
    trace_capacity: usize,
    /// Monotonic event sequence number.
    trace_sequence: u64,
    /// Total fallback executions caused by worker RPC failures.
    rpc_fallbacks_total: u64,
}

impl Default for RuntimeInstrumentation {
    /// Builds an empty instrumentation buffer using configured defaults.
    fn default() -> Self {
        Self {
            worker_metrics: Vec::new(),
            traces: VecDeque::new(),
            trace_capacity: scheduler_trace_capacity(),
            trace_sequence: 0,
            rpc_fallbacks_total: 0,
        }
    }
}

impl Default for SchedulerState {
    /// Builds scheduler state from configured EWMA defaults.
    fn default() -> Self {
        Self {
            ewma_by_tool_ms: BTreeMap::new(),
            ewma_alpha: scheduler_ewma_alpha(),
            unknown_cost_ms: unknown_step_cost_ms(),
        }
    }
}

impl SchedulerState {
    /// Returns the best current runtime estimate for one tool.
    fn estimate_tool_ms(&self, tool_name: &str) -> f64 {
        self.ewma_by_tool_ms.get(tool_name).copied().unwrap_or(self.unknown_cost_ms)
    }

    /// Incorporates one observed runtime into the EWMA estimate table.
    fn observe_tool_runtime(&mut self, tool_name: &str, observed_ms: f64) -> (Option<f64>, f64) {
        let observed = observed_ms.max(0.001);
        let alpha = self.ewma_alpha;
        let previous = self.ewma_by_tool_ms.get(tool_name).copied();
        let updated = match previous {
            Some(previous) => alpha.mul_add(observed, (1.0 - alpha) * previous),
            None => observed,
        };
        self.ewma_by_tool_ms.insert(tool_name.to_string(), updated);
        (previous, updated)
    }
}

/// Mutable actor-owned scheduling service state.
#[derive(Debug, Clone, Default)]
struct SchedulerService {
    /// EWMA runtime estimator.
    scheduler: SchedulerState,
    /// Trace and queue metrics buffer.
    instrumentation: RuntimeInstrumentation,
    /// Last known worker-pool size used for diagnostics snapshots.
    worker_pool_size: usize,
}

impl SchedulerService {
    /// Ensures metrics storage exists for all active workers.
    fn ensure_worker_metrics(&mut self, worker_count: usize) {
        if self.instrumentation.worker_metrics.len() < worker_count {
            self.instrumentation
                .worker_metrics
                .resize(worker_count, WorkerQueueMetricsState::default());
        }
    }

    /// Starts per-level bookkeeping before assignments are emitted.
    fn begin_level_metrics(&mut self, worker_count: usize) {
        self.worker_pool_size = worker_count;
        self.ensure_worker_metrics(worker_count);
        for metric in self.instrumentation.worker_metrics.iter_mut().take(worker_count) {
            metric.last_level_assigned_steps = 0;
            metric.last_level_estimated_load_ms = 0.0;
        }
    }

    /// Records assignment-side metrics for one worker.
    fn record_assignment_metrics(&mut self, worker_index: usize, estimated_ms: f64) {
        self.ensure_worker_metrics(worker_index + 1);
        if let Some(metric) = self.instrumentation.worker_metrics.get_mut(worker_index) {
            metric.record_level_assignment(estimated_ms.max(0.001));
        }
    }

    /// Records completion-side metrics for one worker.
    fn record_completion_metrics(
        &mut self,
        worker_index: usize,
        observed_ms: f64,
        rpc_failed: bool,
        fallback_used: bool,
    ) {
        self.ensure_worker_metrics(worker_index + 1);
        if let Some(metric) = self.instrumentation.worker_metrics.get_mut(worker_index) {
            metric.record_completion(observed_ms.max(0.001), rpc_failed, fallback_used);
        }
        if fallback_used {
            self.instrumentation.rpc_fallbacks_total =
                self.instrumentation.rpc_fallbacks_total.saturating_add(1);
        }
    }

    /// Appends one trace event while respecting ring-buffer capacity.
    fn push_trace(&mut self, kind: SchedulerTraceKind) {
        if self.instrumentation.trace_capacity == 0 {
            return;
        }

        self.instrumentation.trace_sequence = self.instrumentation.trace_sequence.saturating_add(1);
        let event = SchedulerTraceEvent {
            sequence: self.instrumentation.trace_sequence,
            timestamp_unix_nanos: Self::now_unix_nanos(),
            kind,
        };

        self.instrumentation.traces.push_back(event);
        while self.instrumentation.traces.len() > self.instrumentation.trace_capacity {
            let _ = self.instrumentation.traces.pop_front();
        }
    }

    /// Returns the current wall-clock time in Unix nanoseconds for trace events.
    fn now_unix_nanos() -> u128 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
    }

    /// Plans one level onto workers using deterministic greedy load balancing.
    fn plan_level(
        &mut self,
        workflow_name: &str,
        level_index: usize,
        level: Vec<WorkflowStepSpec>,
        worker_count: usize,
    ) -> Vec<StepWorkerAssignment> {
        self.begin_level_metrics(worker_count);
        self.push_trace(SchedulerTraceKind::LevelPlanned {
            workflow_name: workflow_name.to_string(),
            level_index,
            step_count: level.len(),
            worker_count,
        });

        let mut scheduled = level;
        scheduled.sort_by(|left, right| {
            let left_estimate = self.scheduler.estimate_tool_ms(&left.tool);
            let right_estimate = self.scheduler.estimate_tool_ms(&right.tool);
            right_estimate
                .total_cmp(&left_estimate)
                .then_with(|| left.id.cmp(&right.id))
                .then_with(|| left.tool.cmp(&right.tool))
        });

        let mut loads = vec![0.0_f64; worker_count.max(1)];
        let mut assignments = Vec::with_capacity(scheduled.len());
        for step in scheduled {
            let estimate = self.scheduler.estimate_tool_ms(&step.tool);
            let (worker_index, _) = loads
                .iter()
                .enumerate()
                .min_by(|(_, left), (_, right)| left.total_cmp(right))
                .unwrap_or((0, &0.0));

            self.record_assignment_metrics(worker_index, estimate);
            self.push_trace(SchedulerTraceKind::StepAssigned {
                workflow_name: workflow_name.to_string(),
                level_index,
                step_id: step.id.clone(),
                tool_name: step.tool.clone(),
                worker_index,
                estimated_ms: estimate,
            });
            assignments.push(StepWorkerAssignment { worker_index, step });
            loads[worker_index] += estimate.max(0.001);
        }

        assignments
    }

    /// Records one completed step and updates diagnostics-visible state.
    fn record_completion(&mut self, record: StepCompletionRecord) {
        if record.executed {
            let (previous, new_estimate) =
                self.scheduler.observe_tool_runtime(&record.tool_name, record.observed_ms);
            self.push_trace(SchedulerTraceKind::EwmaUpdated {
                tool_name: record.tool_name.clone(),
                previous_estimate_ms: previous,
                observed_ms: record.observed_ms,
                new_estimate_ms: new_estimate,
            });
        }

        if record.rpc_failed {
            self.push_trace(SchedulerTraceKind::RpcFallback {
                step_id: record.step_id.clone(),
                worker_index: record.worker_index,
                reason: record
                    .rpc_failure_reason
                    .unwrap_or_else(|| "worker_rpc_failed".to_string()),
            });
        }

        self.record_completion_metrics(
            record.worker_index,
            record.observed_ms,
            record.rpc_failed,
            record.fallback_used,
        );
        self.push_trace(SchedulerTraceKind::StepCompleted {
            step_id: record.step_id,
            tool_name: record.tool_name,
            worker_index: record.worker_index,
            executed: record.executed,
            fallback_used: record.fallback_used,
            observed_ms: record.observed_ms,
        });
    }

    /// Builds one diagnostics snapshot from current actor-owned state.
    fn runtime_diagnostics(&self) -> RuntimeDiagnostics {
        let tool_estimates = self
            .scheduler
            .ewma_by_tool_ms
            .iter()
            .map(|(tool_name, estimated_ms)| ToolRuntimeEstimate {
                tool_name: tool_name.clone(),
                estimated_ms: *estimated_ms,
            })
            .collect();

        let workers = self
            .instrumentation
            .worker_metrics
            .iter()
            .enumerate()
            .map(|(worker_index, metric)| WorkerQueueDiagnostics {
                worker_index,
                assigned_steps_total: metric.assigned_steps_total,
                completed_steps_total: metric.completed_steps_total,
                in_flight: metric.in_flight,
                peak_in_flight: metric.peak_in_flight,
                last_level_assigned_steps: metric.last_level_assigned_steps,
                last_level_estimated_load_ms: metric.last_level_estimated_load_ms,
                cumulative_estimated_load_ms: metric.cumulative_estimated_load_ms,
                cumulative_observed_load_ms: metric.cumulative_observed_load_ms,
                rpc_failures_total: metric.rpc_failures_total,
                fallback_executions_total: metric.fallback_executions_total,
            })
            .collect();

        RuntimeDiagnostics {
            worker_pool_size: self.worker_pool_size,
            scheduler: SchedulerDiagnostics {
                ewma_alpha: self.scheduler.ewma_alpha,
                unknown_cost_ms: self.scheduler.unknown_cost_ms,
                tool_estimates,
                rpc_fallbacks_total: self.instrumentation.rpc_fallbacks_total,
            },
            workers,
            recent_traces: self.instrumentation.traces.iter().cloned().collect(),
        }
    }
}

impl Actor for SchedulerActor {
    type Msg = SchedulerMessage;
    type State = SchedulerService;
    type Arguments = ();

    /// Initializes the scheduler actor with default estimation state.
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(SchedulerService::default())
    }

    /// Handles assignment, completion, and diagnostics RPC calls.
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SchedulerMessage::PlanLevel(workflow_name, level_index, level, worker_count, reply) => {
                let _ = reply.send(Ok(state.plan_level(
                    &workflow_name,
                    level_index,
                    level,
                    worker_count,
                )));
            }
            SchedulerMessage::RecordCompletion(record, reply) => {
                state.record_completion(record);
                let _ = reply.send(Ok(()));
            }
            SchedulerMessage::GetRuntimeDiagnostics(reply) => {
                let _ = reply.send(Ok(state.runtime_diagnostics()));
            }
        }
        Ok(())
    }
}

/// Spawns the scheduler actor and returns its typed client.
pub(super) async fn spawn_scheduler_actor() -> Result<SchedulerClient, ConductorError> {
    let (actor_ref, _handle) = Actor::spawn(None, SchedulerActor, ()).await.map_err(|err| {
        ConductorError::Internal(format!("failed spawning scheduler actor: {err}"))
    })?;
    Ok(SchedulerClient::new(actor_ref))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::api::SchedulerTraceKind;
    use crate::model::config::WorkflowStepSpec;

    use super::SchedulerService;

    /// Protects deterministic scheduling order and trace generation for one level.
    #[test]
    fn scheduler_plans_levels_deterministically() {
        let mut scheduler = SchedulerService::default();
        scheduler.scheduler.ewma_by_tool_ms.insert("slow@1".to_string(), 20.0);
        scheduler.scheduler.ewma_by_tool_ms.insert("fast@1".to_string(), 5.0);

        let assignments = scheduler.plan_level(
            "wf",
            0,
            vec![
                WorkflowStepSpec {
                    id: "b".to_string(),
                    tool: "fast@1".to_string(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::new(),
                },
                WorkflowStepSpec {
                    id: "a".to_string(),
                    tool: "slow@1".to_string(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::new(),
                },
            ],
            2,
        );

        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].step.id, "a");
        assert_eq!(assignments[1].step.id, "b");
        assert!(
            scheduler
                .instrumentation
                .traces
                .iter()
                .any(|event| matches!(event.kind, SchedulerTraceKind::LevelPlanned { .. }))
        );
    }
}
