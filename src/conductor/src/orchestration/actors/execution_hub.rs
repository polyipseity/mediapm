//! Actor-backed workflow-level execution hub.
//!
//! This actor owns the worker pool and scheduler-facing coordination for one
//! conductor runtime. The coordinator sends one topological level at a time,
//! and the execution hub handles assignment, worker RPC fan-out, fallback
//! execution, and diagnostics updates.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use futures_util::future::join_all;
use mediapm_cas::CasApi;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};

use crate::api::{RuntimeDiagnostics, SchedulerDiagnostics};
use crate::error::ConductorError;
use crate::orchestration::config::{
    DEFAULT_RPC_TIMEOUT_MS, default_worker_pool_size, scheduler_ewma_alpha, unknown_step_cost_ms,
};
use crate::orchestration::protocol::{
    LevelExecutionRequest, StepCompletionRecord, StepDispatchOutcome, StepExecutionRequest,
    StepWorkerAssignment, UnifiedNickelDocument,
};

use super::scheduler::{SchedulerClient, spawn_scheduler_actor};
use super::step_worker::{StepWorkerMessage, execute_step_direct, spawn_step_worker_pool};

/// Typed client for the execution hub actor.
#[derive(Debug, Clone)]
pub(in crate::orchestration) struct ExecutionHubClient {
    /// Actor reference used for all execution-hub RPC calls.
    actor: ActorRef<ExecutionHubMessage>,
}

impl ExecutionHubClient {
    /// Creates a typed client around one execution-hub actor reference.
    #[must_use]
    fn new(actor: ActorRef<ExecutionHubMessage>) -> Self {
        Self { actor }
    }

    /// Executes one workflow level and returns the worker-dispatch outcomes in completion order.
    pub(in crate::orchestration) async fn execute_level(
        &self,
        request: LevelExecutionRequest,
    ) -> Result<Vec<StepDispatchOutcome>, ConductorError> {
        call_t!(
            self.actor,
            ExecutionHubMessage::ExecuteLevel,
            DEFAULT_RPC_TIMEOUT_MS,
            Box::new(request)
        )
        .map_err(|err| {
            ConductorError::Internal(format!("execution hub execute_level RPC failed: {err}"))
        })?
    }

    /// Returns runtime diagnostics aggregated by the scheduler actor.
    pub(in crate::orchestration) async fn runtime_diagnostics(
        &self,
    ) -> Result<RuntimeDiagnostics, ConductorError> {
        call_t!(self.actor, ExecutionHubMessage::GetRuntimeDiagnostics, DEFAULT_RPC_TIMEOUT_MS)
            .map_err(|err| {
                ConductorError::Internal(format!(
                    "execution hub get_runtime_diagnostics RPC failed: {err}"
                ))
            })?
    }
}

/// Requests supported by the execution hub actor.
#[derive(Debug)]
enum ExecutionHubMessage {
    /// Executes one topological workflow level using the owned scheduler and worker pool.
    ExecuteLevel(
        Box<LevelExecutionRequest>,
        RpcReplyPort<Result<Vec<StepDispatchOutcome>, ConductorError>>,
    ),
    /// Returns runtime diagnostics derived from the owned scheduler actor.
    GetRuntimeDiagnostics(RpcReplyPort<Result<RuntimeDiagnostics, ConductorError>>),
}

/// Marker actor for workflow-level execution coordination.
#[derive(Debug, Clone, Copy)]
struct ExecutionHubActor<C> {
    /// Type marker for the CAS implementation shared with child actors.
    _phantom: std::marker::PhantomData<C>,
}

impl<C> Default for ExecutionHubActor<C> {
    /// Builds one marker actor with no local fields.
    fn default() -> Self {
        Self { _phantom: std::marker::PhantomData }
    }
}

/// Mutable execution-hub state owned by the actor.
#[derive(Debug, Clone)]
struct ExecutionHubState<C>
where
    C: CasApi,
{
    /// Shared CAS handle passed through to worker actors and fallback execution.
    cas: Arc<C>,
    /// Scheduler actor responsible for assignments and runtime diagnostics.
    scheduler: SchedulerClient,
    /// Worker actors available for step execution.
    workers: Vec<ActorRef<StepWorkerMessage>>,
}

impl<C> ExecutionHubState<C>
where
    C: CasApi + Send + Sync + 'static,
{
    /// Resolves one tool-call concurrency cap from unified tool metadata.
    ///
    /// Returns `None` when the tool allows unlimited concurrent calls.
    fn tool_call_concurrency_limit(
        unified: &UnifiedNickelDocument,
        tool_name: &str,
    ) -> Result<Option<usize>, ConductorError> {
        let tool = unified.tools.get(tool_name).ok_or_else(|| {
            ConductorError::Workflow(format!("unknown tool '{tool_name}' while planning dispatch"))
        })?;

        match tool.max_concurrent_calls {
            -1 => Ok(None),
            value if value > 0 => Ok(Some(value as usize)),
            value => Err(ConductorError::Workflow(format!(
                "tool '{tool_name}' has invalid max_concurrent_calls={value}; expected -1 or positive integer"
            ))),
        }
    }

    /// Selects one dispatch batch that respects each tool's concurrency cap.
    fn select_concurrency_limited_batch(
        pending: &mut VecDeque<(usize, StepWorkerAssignment)>,
        unified: &UnifiedNickelDocument,
    ) -> Result<Vec<(usize, StepWorkerAssignment)>, ConductorError> {
        let mut selected_per_tool: BTreeMap<String, usize> = BTreeMap::new();
        let mut batch = Vec::new();
        let mut retained = VecDeque::new();

        while let Some((assignment_index, assignment)) = pending.pop_front() {
            let tool_name = assignment.step.tool.clone();
            let selected_count = selected_per_tool.get(&tool_name).copied().unwrap_or(0);
            let limit = Self::tool_call_concurrency_limit(unified, &tool_name)?;
            let can_select = match limit {
                None => true,
                Some(max) => selected_count < max,
            };

            if can_select {
                selected_per_tool.insert(tool_name, selected_count.saturating_add(1));
                batch.push((assignment_index, assignment));
            } else {
                retained.push_back((assignment_index, assignment));
            }
        }

        *pending = retained;
        if batch.is_empty() && !pending.is_empty() {
            return Err(ConductorError::Internal(
                "failed selecting any dispatchable step while pending queue is non-empty"
                    .to_string(),
            ));
        }
        Ok(batch)
    }

    /// Executes one workflow level by planning assignments, dispatching workers, and recording completion facts.
    async fn execute_level(
        &self,
        request: LevelExecutionRequest,
    ) -> Result<Vec<StepDispatchOutcome>, ConductorError> {
        let assignments = self
            .scheduler
            .plan_level(
                &request.workflow_name,
                request.level_index,
                request.level,
                self.workers.len(),
            )
            .await?;

        let total_assignments = assignments.len();
        let mut pending: VecDeque<(usize, StepWorkerAssignment)> =
            assignments.into_iter().enumerate().collect();
        let mut outcomes_by_index: Vec<Option<StepDispatchOutcome>> =
            (0..total_assignments).map(|_| None).collect();

        while !pending.is_empty() {
            let dispatch_batch =
                Self::select_concurrency_limited_batch(&mut pending, request.unified.as_ref())?;

            let batch_futures = dispatch_batch.into_iter().map(|(assignment_index, assignment)| {
                let worker_index = assignment.worker_index;
                let step = assignment.step;
                let actor = self.workers[worker_index].clone();
                let step_request = StepExecutionRequest {
                    unified: request.unified.clone(),
                    step: step.clone(),
                    impure_timestamp: request
                        .impure_timestamps
                        .get(&step.id)
                        .cloned()
                        .unwrap_or(None),
                    workflow_name: request.workflow_name.clone(),
                    state_snapshot: request.state_snapshot.clone(),
                    runtime_storage_dir: request.runtime_storage_dir.clone(),
                    outermost_config_dir: request.outermost_config_dir.clone(),
                    step_outputs: request.step_outputs.clone(),
                    required_output_names: request
                        .required_outputs_by_step
                        .get(&step.id)
                        .cloned()
                        .unwrap_or_default(),
                };
                let fallback_request = step_request.clone();
                let cas = self.cas.clone();
                let step_id = step.id.clone();

                async move {
                    let rpc_result = call_t!(
                        actor,
                        StepWorkerMessage::ExecuteStep,
                        DEFAULT_RPC_TIMEOUT_MS,
                        Box::new(step_request)
                    );
                    let outcome = match rpc_result {
                        Ok(worker_reply) => {
                            let mut result = worker_reply?;
                            result.worker_index = worker_index;
                            result.fallback_used = false;
                            Ok(StepDispatchOutcome {
                                result,
                                rpc_failed: false,
                                rpc_failure_reason: None,
                            })
                        }
                        Err(err) => {
                            let rpc_error_text = err.to_string();
                            execute_step_direct(cas, fallback_request)
                                .await
                                .map(|mut result| {
                                    result.worker_index = worker_index;
                                    result.fallback_used = true;
                                    StepDispatchOutcome {
                                        result,
                                        rpc_failed: true,
                                        rpc_failure_reason: Some(rpc_error_text),
                                    }
                                })
                                .map_err(|fallback_err| {
                                    ConductorError::Internal(format!(
                                        "step worker RPC failed for '{step_id}'; fallback execution failed: {fallback_err}"
                                    ))
                                })
                        }
                    }?;

                    Ok::<(usize, StepDispatchOutcome), ConductorError>((assignment_index, outcome))
                }
            });

            for batch_result in join_all(batch_futures).await {
                let (assignment_index, outcome) = batch_result?;
                self.scheduler
                    .record_completion(StepCompletionRecord {
                        step_id: outcome.result.step_id.clone(),
                        tool_name: outcome.result.tool_name.clone(),
                        worker_index: outcome.result.worker_index,
                        executed: outcome.result.executed,
                        fallback_used: outcome.result.fallback_used,
                        observed_ms: outcome.result.elapsed_ms,
                        rpc_failed: outcome.rpc_failed,
                        rpc_failure_reason: outcome.rpc_failure_reason.clone(),
                    })
                    .await?;
                outcomes_by_index[assignment_index] = Some(outcome);
            }
        }

        let mut outcomes = Vec::with_capacity(total_assignments);
        for (index, maybe_outcome) in outcomes_by_index.into_iter().enumerate() {
            let outcome = maybe_outcome.ok_or_else(|| {
                ConductorError::Internal(format!(
                    "missing step dispatch outcome for assignment index {index}"
                ))
            })?;
            outcomes.push(outcome);
        }

        Ok(outcomes)
    }

    /// Returns diagnostics owned by the scheduler actor or a default shell if the scheduler is unavailable.
    async fn runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        self.scheduler.runtime_diagnostics().await.or_else(|_| {
            Ok(RuntimeDiagnostics {
                worker_pool_size: self.workers.len(),
                scheduler: SchedulerDiagnostics {
                    ewma_alpha: scheduler_ewma_alpha(),
                    unknown_cost_ms: unknown_step_cost_ms(),
                    tool_estimates: Vec::new(),
                    rpc_fallbacks_total: 0,
                },
                workers: Vec::new(),
                recent_traces: Vec::new(),
            })
        })
    }
}

impl<C> Actor for ExecutionHubActor<C>
where
    C: CasApi + Send + Sync + 'static,
{
    type Msg = ExecutionHubMessage;
    type State = ExecutionHubState<C>;
    type Arguments = Arc<C>;

    /// Spawns the scheduler actor and step-worker pool owned by this execution hub.
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let scheduler = spawn_scheduler_actor().await.map_err(|err| {
            ActorProcessingErr::from(format!("failed to spawn execution-hub scheduler: {err}"))
        })?;
        let workers = spawn_step_worker_pool(args.clone(), default_worker_pool_size())
            .await
            .map_err(|err| {
                ActorProcessingErr::from(format!("failed to spawn execution-hub workers: {err}"))
            })?;
        Ok(ExecutionHubState { cas: args, scheduler, workers })
    }

    /// Handles level-execution and diagnostics RPC calls.
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ExecutionHubMessage::ExecuteLevel(request, reply) => {
                let _ = reply.send(state.execute_level(*request).await);
            }
            ExecutionHubMessage::GetRuntimeDiagnostics(reply) => {
                let _ = reply.send(state.runtime_diagnostics().await);
            }
        }
        Ok(())
    }
}

/// Spawns the execution hub actor and returns its typed client.
pub(in crate::orchestration) async fn spawn_execution_hub_actor<C>(
    cas: Arc<C>,
) -> Result<ExecutionHubClient, ConductorError>
where
    C: CasApi + Send + Sync + 'static,
{
    let (actor_ref, _handle) =
        Actor::spawn(None, ExecutionHubActor::<C>::default(), cas).await.map_err(|err| {
            ConductorError::Internal(format!("failed spawning execution hub actor: {err}"))
        })?;
    Ok(ExecutionHubClient::new(actor_ref))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    use crate::model::config::{ProcessSpec, ToolInputSpec, ToolOutputSpec, WorkflowStepSpec};
    use crate::orchestration::protocol::{
        StepWorkerAssignment, UnifiedNickelDocument, UnifiedToolSpec,
    };

    use super::ExecutionHubState;

    /// Protects `-1` as the default unlimited concurrency behavior.
    #[test]
    fn select_batch_treats_minus_one_as_unlimited() {
        let unified = test_unified_document(BTreeMap::from([("echo@1.0.0".to_string(), -1)]));
        let mut pending = VecDeque::from([
            (0, assignment("s1", "echo@1.0.0", 0)),
            (1, assignment("s2", "echo@1.0.0", 1)),
        ]);

        let batch =
            ExecutionHubState::<mediapm_cas::InMemoryCas>::select_concurrency_limited_batch(
                &mut pending,
                &unified,
            )
            .expect("batch selection should succeed");

        assert_eq!(batch.len(), 2);
        assert!(pending.is_empty());
    }

    /// Protects per-tool positive concurrency caps when selecting one dispatch batch.
    #[test]
    fn select_batch_honors_per_tool_limit() {
        let unified = test_unified_document(BTreeMap::from([
            ("echo@1.0.0".to_string(), 1),
            ("archive@1.0.0".to_string(), -1),
        ]));
        let mut pending = VecDeque::from([
            (0, assignment("s1", "echo@1.0.0", 0)),
            (1, assignment("s2", "echo@1.0.0", 1)),
            (2, assignment("s3", "archive@1.0.0", 0)),
        ]);

        let batch =
            ExecutionHubState::<mediapm_cas::InMemoryCas>::select_concurrency_limited_batch(
                &mut pending,
                &unified,
            )
            .expect("batch selection should succeed");

        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].0, 0);
        assert_eq!(batch[1].0, 2);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 1);
    }

    /// Protects validation of impossible tool concurrency settings.
    #[test]
    fn invalid_tool_concurrency_value_is_rejected() {
        let unified = test_unified_document(BTreeMap::from([("echo@1.0.0".to_string(), 0)]));
        let mut pending = VecDeque::from([(0, assignment("s1", "echo@1.0.0", 0))]);

        let result =
            ExecutionHubState::<mediapm_cas::InMemoryCas>::select_concurrency_limited_batch(
                &mut pending,
                &unified,
            );

        let message = result.expect_err("invalid concurrency setting should fail").to_string();
        assert!(message.contains("max_concurrent_calls=0"));
    }

    /// Builds one minimal unified document for execution-hub batch-selection tests.
    fn test_unified_document(per_tool_limits: BTreeMap<String, i32>) -> UnifiedNickelDocument {
        let tools = per_tool_limits
            .into_iter()
            .map(|(tool_name, max_concurrent_calls)| {
                (
                    tool_name,
                    UnifiedToolSpec {
                        is_impure: false,
                        max_concurrent_calls,
                        inputs: BTreeMap::<String, ToolInputSpec>::new(),
                        process: ProcessSpec::Builtin {
                            name: "echo".to_string(),
                            version: "1.0.0".to_string(),
                            args: BTreeMap::new(),
                        },
                        outputs: BTreeMap::<String, ToolOutputSpec>::new(),
                        tool_content_map: BTreeMap::new(),
                    },
                )
            })
            .collect();

        UnifiedNickelDocument {
            external_data: BTreeMap::new(),
            tools,
            workflows: BTreeMap::new(),
            tool_content_hashes: BTreeSet::new(),
        }
    }

    /// Builds one deterministic worker assignment fixture.
    fn assignment(step_id: &str, tool_name: &str, worker_index: usize) -> StepWorkerAssignment {
        StepWorkerAssignment {
            worker_index,
            step: WorkflowStepSpec {
                id: step_id.to_string(),
                tool: tool_name.to_string(),
                inputs: BTreeMap::new(),
                depends_on: Vec::new(),
                outputs: BTreeMap::new(),
            },
        }
    }
}
