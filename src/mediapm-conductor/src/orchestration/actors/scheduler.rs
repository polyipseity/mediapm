//! Actor-backed scheduling and diagnostics for conductor workflows.
//!
//! The scheduler actor owns runtime estimates, worker-queue metrics, and trace
//! emission so the execution hub can focus on worker dispatch while the
//! coordinator stays purely workflow-oriented.

use std::collections::{BTreeMap, VecDeque};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};

use crate::api::{
    RuntimeDiagnostics, SchedulerDiagnostics, SchedulerTraceEvent, SchedulerTraceKind,
    ToolRuntimeEstimate, WorkerQueueDiagnostics,
};
use crate::error::ConductorError;
use crate::model::config::ImpureTimestamp;
use crate::orchestration::config::{
    rpc_timeout_ms, scheduler_ewma_alpha, scheduler_trace_capacity, unknown_step_cost_ms,
};
use crate::orchestration::protocol::StepCompletionRecord;

/// Typed client for the scheduler actor.
#[derive(Debug, Clone)]
pub(crate) struct SchedulerClient {
    /// Actor reference used for all scheduling RPC calls.
    actor: ActorRef<SchedulerMessage>,
}

impl SchedulerClient {
    /// Creates a typed client around one scheduler actor reference.
    #[must_use]
    fn new(actor: ActorRef<SchedulerMessage>) -> Self {
        Self { actor }
    }

    /// Records one completed step so estimates, metrics, and traces stay current.
    pub(crate) async fn record_completion(
        &self,
        record: StepCompletionRecord,
    ) -> Result<(), ConductorError> {
        call_t!(self.actor, SchedulerMessage::RecordCompletion, rpc_timeout_ms(), record)
            .map_err(|err| ConductorError::rpc_error("scheduler record_completion", err))?
    }

    /// Returns the latest diagnostics snapshot owned by the scheduler actor.
    pub(crate) async fn runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        call_t!(self.actor, SchedulerMessage::GetRuntimeDiagnostics, rpc_timeout_ms())
            .map_err(|err| ConductorError::rpc_error("scheduler get_runtime_diagnostics", err))?
    }
}

/// Requests supported by the scheduler actor.
#[derive(Debug)]
enum SchedulerMessage {
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
}

impl WorkerQueueMetricsState {
    /// Records one step completion.
    fn record_completion(&mut self, observed_ms: f64) {
        self.completed_steps_total = self.completed_steps_total.saturating_add(1);
        self.cumulative_observed_load_ms += observed_ms;
        self.in_flight = self.in_flight.saturating_sub(1);
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
}

impl Default for RuntimeInstrumentation {
    /// Builds an empty instrumentation buffer using configured defaults.
    fn default() -> Self {
        Self {
            worker_metrics: Vec::new(),
            traces: VecDeque::new(),
            trace_capacity: scheduler_trace_capacity(),
            trace_sequence: 0,
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

    /// Records completion-side metrics for one worker.
    fn record_completion_metrics(&mut self, worker_index: usize, observed_ms: f64) {
        self.ensure_worker_metrics(worker_index + 1);
        if let Some(metric) = self.instrumentation.worker_metrics.get_mut(worker_index) {
            metric.record_completion(observed_ms.max(0.001));
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
            timestamp_unix_nanos: ImpureTimestamp::now().as_unix_nanos(),
            kind,
        };

        self.instrumentation.traces.push_back(event);
        while self.instrumentation.traces.len() > self.instrumentation.trace_capacity {
            let _ = self.instrumentation.traces.pop_front();
        }
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

        self.record_completion_metrics(record.worker_index, record.observed_ms);

        // Step-stream dispatch assigns steps implicitly (the execution hub
        // round-robins without calling plan_level), so record assignment-side
        // counters here for diagnostics completeness.
        if let Some(metric) = self.instrumentation.worker_metrics.get_mut(record.worker_index) {
            metric.assigned_steps_total = metric.assigned_steps_total.saturating_add(1);
        }

        self.push_trace(SchedulerTraceKind::StepCompleted {
            step_id: record.step_id,
            tool_name: record.tool_name,
            worker_index: record.worker_index,
            executed: record.executed,
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
            })
            .collect();

        // Fall back to worker_metrics.len() when step-stream dispatch never
        // called begin_level_metrics (which is the only path that sets
        // worker_pool_size).
        let worker_pool_size =
            std::cmp::max(self.worker_pool_size, self.instrumentation.worker_metrics.len());
        RuntimeDiagnostics {
            worker_pool_size,
            scheduler: SchedulerDiagnostics {
                ewma_alpha: self.scheduler.ewma_alpha,
                unknown_cost_ms: self.scheduler.unknown_cost_ms,
                tool_estimates,
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
pub(crate) async fn spawn_scheduler_actor() -> Result<SchedulerClient, ConductorError> {
    let (actor_ref, _handle) = Actor::spawn(None, SchedulerActor, ()).await.map_err(|err| {
        ConductorError::Internal(format!("failed spawning scheduler actor: {err}"))
    })?;
    Ok(SchedulerClient::new(actor_ref))
}
