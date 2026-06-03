//! Top-level conductor node actor and typed client.
//!
//! This module groups the user-facing actor shell in one place: command
//! messages, typed RPC client, actor marker, spawn helper, and the concrete
//! `ractor::Actor` implementation that delegates to the workflow coordinator.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use mediapm_cas::CasApi;
use mediapm_cas::Hash;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};

use crate::api::{RunSummary, RunWorkflowOptions, RuntimeDiagnostics, StateMutationOptions};
use crate::error::ConductorError;
use crate::model::state::OrchestrationState;
use crate::orchestration::config::DEFAULT_RPC_TIMEOUT_MS;
use crate::orchestration::coordinator::WorkflowCoordinator;

/// Conductor node actor command envelope.
#[derive(Debug)]
pub(super) enum ConductorNodeMessage {
    /// Executes workflows from user/machine config paths plus runtime storage
    /// path options.
    RunWorkflow(
        PathBuf,
        PathBuf,
        Box<RunWorkflowOptions>,
        RpcReplyPort<Result<RunSummary, ConductorError>>,
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

    /// Executes workflows from user/machine config paths plus runtime storage
    /// path options.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails or when workflow
    /// evaluation/execution fails in the coordinator.
    pub async fn run_workflow(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<RunSummary, ConductorError> {
        call_t!(
            self.actor,
            ConductorNodeMessage::RunWorkflow,
            DEFAULT_RPC_TIMEOUT_MS,
            user_ncl.to_path_buf(),
            machine_ncl.to_path_buf(),
            Box::new(options)
        )
        .map_err(|err| {
            ConductorError::Internal(format!("conductor actor run_workflow RPC failed: {err}"))
        })?
    }

    /// Returns the actor's current in-memory orchestration-state snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails or state retrieval fails
    /// in the coordinator.
    pub async fn get_state(&self) -> Result<OrchestrationState, ConductorError> {
        call_t!(self.actor, ConductorNodeMessage::GetState, DEFAULT_RPC_TIMEOUT_MS).map_err(
            |err| ConductorError::Internal(format!("conductor actor get_state RPC failed: {err}")),
        )?
    }

    /// Returns runtime diagnostics including worker queue metrics and scheduler traces.
    ///
    /// # Errors
    ///
    /// Returns an error when actor RPC delivery fails or diagnostics collection
    /// fails in the coordinator.
    pub async fn get_runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        call_t!(self.actor, ConductorNodeMessage::GetRuntimeDiagnostics, DEFAULT_RPC_TIMEOUT_MS)
            .map_err(|err| {
                ConductorError::Internal(format!(
                    "conductor actor get_runtime_diagnostics RPC failed: {err}"
                ))
            })?
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
            DEFAULT_RPC_TIMEOUT_MS,
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
            DEFAULT_RPC_TIMEOUT_MS,
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
        call_t!(self.actor, ConductorNodeMessage::RunGc, DEFAULT_RPC_TIMEOUT_MS, ttl_override)
            .map_err(|err| {
                ConductorError::Internal(format!("conductor actor run_gc RPC failed: {err}"))
            })?
    }
}

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
    C: CasApi + Send + Sync + 'static,
{
    type Msg = ConductorNodeMessage;
    type State = WorkflowCoordinator<C>;
    type Arguments = Arc<C>;

    /// Initializes the node actor with a workflow coordinator bound to the shared CAS handle.
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(WorkflowCoordinator::new(args))
    }

    /// Handles top-level conductor RPC calls by delegating into the workflow coordinator.
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ConductorNodeMessage::RunWorkflow(user_ncl, machine_ncl, options, reply) => {
                let result = if *options == RunWorkflowOptions::default() {
                    state.run_workflow(&user_ncl, &machine_ncl).await
                } else {
                    state.run_workflow_with_options(&user_ncl, &machine_ncl, *options).await
                };
                let _ = reply.send(result);
            }
            ConductorNodeMessage::GetState(reply) => {
                let _ = reply.send(state.current_state().await);
            }
            ConductorNodeMessage::GetRuntimeDiagnostics(reply) => {
                let _ = reply.send(state.runtime_diagnostics().await);
            }
            ConductorNodeMessage::LoadResolvedState(user_ncl, machine_ncl, options, reply) => {
                let _ = reply.send(
                    state.load_resolved_state_with_options(&user_ncl, &machine_ncl, *options).await,
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
                let _ = reply.send(state.run_gc(ttl_override).await);
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
    C: CasApi + Send + Sync + 'static,
{
    let (actor_ref, _join_handle) =
        Actor::spawn(None, ConductorNodeActor::<C>::default(), cas).await.map_err(|err| {
            ConductorError::Internal(format!("failed spawning conductor actor: {err}"))
        })?;
    Ok(ConductorActorClient::new(actor_ref))
}
