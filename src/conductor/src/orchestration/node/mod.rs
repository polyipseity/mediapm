//! Top-level conductor node actor and typed client.
//!
//! This module groups the user-facing actor shell in one place: command
//! messages, typed RPC client, actor marker, spawn helper, and the concrete
//! `ractor::Actor` implementation that delegates to the workflow coordinator.

use mediapm_cas::{CasApi, CasMaintenanceApi, Hash};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

use crate::gc::run_cas_gc_sweep;

use crate::api::{RunSummary, RunWorkflowOptions, RuntimeDiagnostics, StateMutationOptions};
use crate::error::ConductorError;
use crate::model::state::OrchestrationState;
use crate::orchestration::config::rpc_timeout_ms;
use crate::orchestration::coordinator::WorkflowCoordinator;

/// Conductor node actor command envelope.
#[derive(Debug)]
pub(super) enum ConductorNodeMessage {
    /// Submits a workflow for background execution, returning a handle ID.
    SubmitWorkflow(
        PathBuf,
        PathBuf,
        Box<RunWorkflowOptions>,
        RpcReplyPort<Result<u64, ConductorError>>,
    ),
    /// Polls a previously submitted workflow by handle ID.
    PollWorkflow(
        u64,
        RpcReplyPort<Result<Option<Result<RunSummary, ConductorError>>, ConductorError>>,
    ),
    /// Returns the current in-memory orchestration-state snapshot.
    GetState(RpcReplyPort<Result<OrchestrationState, ConductorError>>),
    /// Returns runtime diagnostics and scheduler traces.
    GetRuntimeDiagnostics(RpcReplyPort<Result<RuntimeDiagnostics, ConductorError>>),
    /// Loads effective orchestration state resolved from user/machine/state
    /// documents.
    LoadResolvedState(
        PathBuf,
        PathBuf,
        Box<StateMutationOptions>,
        RpcReplyPort<Result<OrchestrationState, ConductorError>>,
    ),
    /// Replaces effective orchestration state and updates only volatile
    /// `state_pointer` + CAS state blob.
    ReplaceResolvedState(
        PathBuf,
        PathBuf,
        Box<OrchestrationState>,
        Box<StateMutationOptions>,
        RpcReplyPort<Result<Hash, ConductorError>>,
    ),
    /// Runs instance GC with an optional TTL override.
    RunGc(Option<u64>, RpcReplyPort<Result<(), ConductorError>>),
}

/// Typed client for interacting with the conductor node actor.
#[derive(Debug, Clone)]
pub struct ConductorActorClient {
    /// Actor reference used for top-level conductor RPC calls.
    actor: ActorRef<ConductorNodeMessage>,
}

impl ConductorActorClient {
    /// Creates a typed client from one node actor reference.
    #[must_use]
    fn new(actor: ActorRef<ConductorNodeMessage>) -> Self {
        Self { actor }
    }

    /// Submits a workflow for background execution, returning a handle ID.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails.
    pub async fn submit_workflow(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<u64, ConductorError> {
        call_t!(
            self.actor,
            ConductorNodeMessage::SubmitWorkflow,
            rpc_timeout_ms(),
            user_ncl.to_path_buf(),
            machine_ncl.to_path_buf(),
            Box::new(options)
        )
        .map_err(|err| {
            ConductorError::Internal(format!("conductor actor submit_workflow RPC failed: {err}"))
        })?
    }

    /// Polls a previously submitted workflow by handle ID.
    ///
    /// Returns `None` if the workflow is still running, `Some(Ok(...))` on
    /// success, or `Some(Err(...))` on failure.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails or the handle ID is
    /// not found.
    pub async fn poll_workflow(
        &self,
        handle_id: u64,
    ) -> Result<Option<Result<RunSummary, ConductorError>>, ConductorError> {
        call_t!(self.actor, ConductorNodeMessage::PollWorkflow, rpc_timeout_ms(), handle_id)
            .map_err(|err| {
                ConductorError::Internal(format!("conductor actor poll_workflow RPC failed: {err}"))
            })?
    }

    /// Polls in a loop until a previously submitted workflow completes.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails, the handle ID is not
    /// found, or the workflow itself failed.
    pub async fn wait_workflow(&self, handle_id: u64) -> Result<RunSummary, ConductorError> {
        loop {
            match self.poll_workflow(handle_id).await? {
                Some(result) => return result,
                None => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
            }
        }
    }

    /// Returns the actor's current in-memory orchestration-state snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails or state retrieval fails
    /// in the coordinator.
    pub async fn get_state(&self) -> Result<OrchestrationState, ConductorError> {
        call_t!(self.actor, ConductorNodeMessage::GetState, rpc_timeout_ms()).map_err(|err| {
            ConductorError::Internal(format!("conductor actor get_state RPC failed: {err}"))
        })?
    }

    /// Returns runtime diagnostics including worker queue metrics and scheduler traces.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails or diagnostics collection
    /// fails in the coordinator.
    pub async fn get_runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        call_t!(self.actor, ConductorNodeMessage::GetRuntimeDiagnostics, rpc_timeout_ms()).map_err(
            |err| {
                ConductorError::Internal(format!(
                    "conductor actor get_runtime_diagnostics RPC failed: {err}"
                ))
            },
        )?
    }

    /// Loads effective orchestration state resolved from user/machine/state
    /// documents.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails or when state loading
    /// fails in the coordinator.
    pub async fn load_resolved_state(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: StateMutationOptions,
    ) -> Result<OrchestrationState, ConductorError> {
        call_t!(
            self.actor,
            ConductorNodeMessage::LoadResolvedState,
            rpc_timeout_ms(),
            user_ncl.to_path_buf(),
            machine_ncl.to_path_buf(),
            Box::new(options)
        )
        .map_err(|err| {
            ConductorError::Internal(format!(
                "conductor actor load_resolved_state RPC failed: {err}"
            ))
        })?
    }

    /// Replaces effective orchestration state and updates only volatile
    /// `state_pointer` + CAS state blob.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails or state replacement
    /// fails in the coordinator.
    pub async fn replace_resolved_state(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        state: OrchestrationState,
        options: StateMutationOptions,
    ) -> Result<Hash, ConductorError> {
        call_t!(
            self.actor,
            ConductorNodeMessage::ReplaceResolvedState,
            rpc_timeout_ms(),
            user_ncl.to_path_buf(),
            machine_ncl.to_path_buf(),
            Box::new(state),
            Box::new(options)
        )
        .map_err(|err| {
            ConductorError::Internal(format!(
                "conductor actor replace_resolved_state RPC failed: {err}"
            ))
        })?
    }

    /// Runs instance GC with an optional TTL override.
    ///
    /// When `ttl_override` is `None`, the state store's configured TTL is used;
    /// if neither is set the call is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails or GC/persistence fails
    /// in the state store.
    pub async fn run_gc(&self, ttl_override: Option<u64>) -> Result<(), ConductorError> {
        call_t!(self.actor, ConductorNodeMessage::RunGc, rpc_timeout_ms(), ttl_override).map_err(
            |err| ConductorError::Internal(format!("conductor actor run_gc RPC failed: {err}")),
        )?
    }
}

/// Actor state wrapping the workflow coordinator with background task tracking.
struct ConductorActorState<C: CasApi + Send + Sync + 'static> {
    /// Core workflow coordinator.
    coordinator: WorkflowCoordinator<C>,
    /// Background workflow tasks keyed by handle ID.
    workflow_handles: HashMap<u64, JoinHandle<Result<RunSummary, ConductorError>>>,
    /// Monotonically increasing handle ID counter.
    next_handle_id: u64,
}

/// Interval in seconds between background GC sweep cycles.
const GC_INTERVAL_SECONDS: u64 = 3600;

/// Marker actor for top-level conductor node command dispatch.
#[derive(Debug, Clone, Copy)]
struct ConductorNodeActor<C> {
    /// Type marker for the CAS implementation shared with the workflow coordinator.
    _phantom: std::marker::PhantomData<C>,
}

impl<C> Default for ConductorNodeActor<C> {
    /// Builds one marker actor with no local fields.
    fn default() -> Self {
        Self { _phantom: std::marker::PhantomData }
    }
}

impl<C> Actor for ConductorNodeActor<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    type Msg = ConductorNodeMessage;
    type State = ConductorActorState<C>;
    type Arguments = Arc<C>;

    /// Initializes the node actor with a workflow coordinator bound to the shared CAS handle.
    ///
    /// Also spawns the background GC loop: compacts the CAS index immediately,
    /// then every [`GC_INTERVAL_SECONDS`] submits a `RunGc(None, …)` message
    /// to itself for full-cycle GC (instance sweep + CAS sweep + compaction).
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        // Spawn background GC loop: compact on startup, then periodic GC.
        let bg_cas = args.clone();
        let bg_self = _myself.clone();
        tokio::spawn(async move {
            // Startup: compact index immediately.
            if let Err(e) = bg_cas.compact_index().await {
                tracing::warn!("background index compaction at startup failed: {e}");
            }
            // Periodic full GC sweep loop.
            loop {
                tokio::time::sleep(Duration::from_secs(GC_INTERVAL_SECONDS)).await;
                let (tx, rx) = tokio::sync::oneshot::channel();
                let reply_port = RpcReplyPort::from(tx);
                if bg_self.send_message(ConductorNodeMessage::RunGc(None, reply_port)).is_err() {
                    tracing::warn!("background GC send failed — actor may have stopped");
                    break;
                }
                match rx.await {
                    Ok(Ok(())) => tracing::debug!("background GC completed successfully"),
                    Ok(Err(e)) => tracing::warn!("background GC failed: {e}"),
                    Err(_) => {
                        tracing::warn!(
                            "background GC reply channel closed — actor may have stopped"
                        );
                        break;
                    }
                }
            }
        });

        Ok(ConductorActorState {
            coordinator: WorkflowCoordinator::new(args),
            workflow_handles: HashMap::new(),
            next_handle_id: 0,
        })
    }

    /// Handles top-level conductor RPC calls by delegating into the workflow coordinator.
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ConductorNodeMessage::SubmitWorkflow(user_ncl, machine_ncl, options, reply) => {
                let handle_id = state.next_handle_id;
                state.next_handle_id += 1;

                // Pre-ensure runtime support so the main coordinator has a
                // state_store that the background task can share.  When the bg
                // coordinator uses the same state_store actor, commit_run
                // directly updates the in-memory current_state — no
                // post-hoc load_resolved_state is needed.
                let main_state_store =
                    if let Err(e) = state.coordinator.ensure_runtime_support().await {
                        tracing::warn!("failed to ensure main coordinator runtime support: {e}");
                        None
                    } else {
                        state.coordinator.state_store()
                    };

                let cas = state.coordinator.cas.clone();
                let join_handle = tokio::spawn(async move {
                    let mut coord = WorkflowCoordinator::new(cas);
                    if let Some(store) = main_state_store {
                        coord.set_state_store(store);
                    }
                    let workflow_result = if *options == RunWorkflowOptions::default() {
                        coord.run_workflow(&user_ncl, &machine_ncl).await
                    } else {
                        coord.run_workflow_with_options(&user_ncl, &machine_ncl, *options).await
                    };
                    workflow_result
                });
                state.workflow_handles.insert(handle_id, join_handle);
                let _ = reply.send(Ok(handle_id));
            }
            ConductorNodeMessage::PollWorkflow(handle_id, reply) => {
                if let Some(handle) = state.workflow_handles.get(&handle_id) {
                    if handle.is_finished() {
                        let handle = state.workflow_handles.remove(&handle_id).unwrap();
                        let result = handle.await;
                        let _ = reply.send(Ok(Some(result.unwrap_or_else(|join_err| {
                            Err(ConductorError::Internal(format!(
                                "workflow background task panicked: {join_err}"
                            )))
                        }))));
                    } else {
                        let _ = reply.send(Ok(None));
                    }
                } else {
                    let _ = reply.send(Err(ConductorError::Internal(format!(
                        "workflow handle {handle_id} not found"
                    ))));
                }
            }
            ConductorNodeMessage::GetState(reply) => {
                let _ = reply.send(state.coordinator.current_state().await);
            }
            ConductorNodeMessage::GetRuntimeDiagnostics(reply) => {
                let _ = reply.send(state.coordinator.runtime_diagnostics().await);
            }
            ConductorNodeMessage::LoadResolvedState(user_ncl, machine_ncl, options, reply) => {
                let _ = reply.send(
                    state
                        .coordinator
                        .load_resolved_state_with_options(&user_ncl, &machine_ncl, *options)
                        .await,
                );
            }
            ConductorNodeMessage::ReplaceResolvedState(
                user_ncl,
                machine_ncl,
                next_state,
                options,
                reply,
            ) => {
                let _ = reply.send(
                    state
                        .coordinator
                        .replace_resolved_state_with_options(
                            &user_ncl,
                            &machine_ncl,
                            *next_state,
                            *options,
                        )
                        .await,
                );
            }
            ConductorNodeMessage::RunGc(ttl_override, reply) => {
                let result = async {
                    // 1. Instance GC (TTL-based pruning of stale instances).
                    state.coordinator.run_gc(ttl_override).await?;

                    // 2. CAS sweep using coordinator's accumulated external_data.
                    let cas = state.coordinator.cas.clone();
                    let external_data = state.coordinator.external_data().clone();
                    let state_pointer = match state.coordinator.state_store() {
                        Some(store) => store.get_state_pointer().await?,
                        None => None,
                    };
                    let current_state = state.coordinator.current_state().await?;
                    run_cas_gc_sweep(cas.as_ref(), &external_data, state_pointer, &current_state)
                        .await?;

                    Ok::<_, ConductorError>(())
                }
                .await;
                let _ = reply.send(result);
            }
        }
        Ok(())
    }
}

/// Spawns a conductor node actor and returns a typed client.
///
/// # Errors
///
/// Returns an error when the node actor cannot be spawned.
pub async fn spawn_conductor_actor<C>(cas: Arc<C>) -> Result<ConductorActorClient, ConductorError>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    let (actor_ref, _join_handle) =
        Actor::spawn(None, ConductorNodeActor::<C>::default(), cas).await.map_err(|err| {
            ConductorError::Internal(format!("failed spawning conductor actor: {err}"))
        })?;
    Ok(ConductorActorClient::new(actor_ref))
}
