//! Top-level conductor node actor and typed client.
//!
//! This module groups the user-facing actor shell in one place: command
//! messages, typed RPC client, actor marker, spawn helper, and the concrete
//! `ractor::Actor` implementation that delegates to the workflow coordinator.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use mediapm_cas::CasApi;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};

use crate::api::{RunSummary, RunWorkflowOptions, RuntimeDiagnostics};
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
        RunWorkflowOptions,
        RpcReplyPort<Result<RunSummary, ConductorError>>,
    ),
    /// Returns the current in-memory orchestration-state snapshot.
    GetState(RpcReplyPort<Result<OrchestrationState, ConductorError>>),
    /// Returns runtime diagnostics and scheduler traces.
    GetRuntimeDiagnostics(RpcReplyPort<Result<RuntimeDiagnostics, ConductorError>>),
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
            options
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
                let result = if options == RunWorkflowOptions::default() {
                    state.run_workflow(&user_ncl, &machine_ncl).await
                } else {
                    state.run_workflow_with_options(&user_ncl, &machine_ncl, options).await
                };
                let _ = reply.send(result);
            }
            ConductorNodeMessage::GetState(reply) => {
                let _ = reply.send(state.current_state().await);
            }
            ConductorNodeMessage::GetRuntimeDiagnostics(reply) => {
                let _ = reply.send(state.runtime_diagnostics().await);
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
