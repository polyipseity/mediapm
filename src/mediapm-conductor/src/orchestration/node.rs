//! Actor-based client for conductor orchestration.
//!
//! [`ConductorActorClient`] wraps the [`WorkflowCoordinator`] behind a
//! `ractor` actor, providing a message-passing interface for workflow
//! execution, diagnostics, and GC operations.

use std::marker::PhantomData;
use std::sync::Arc;

use ractor::rpc::CallResult;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use mediapm_cas::{CasApi, CasMaintenanceApi};

use crate::api::{RunSummary, RunWorkflowOptions, RuntimeDiagnostics};
use crate::error::ConductorError;
use crate::state::OrchestrationState;

use super::coordinator::WorkflowCoordinator;
use super::protocol::UnifiedNickelDocument;

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// Messages accepted by the conductor actor.
#[derive(Debug)]
pub(crate) enum ConductorMessage {
    /// Run a workflow by name.
    RunWorkflow {
        /// Workflow name to execute.
        workflow_name: String,
        /// Run options.
        #[expect(dead_code)]
        options: RunWorkflowOptions,
        /// Unified configuration documents.
        unified: UnifiedNickelDocument,
        /// Current orchestration state (cloned, actor owns its copy).
        state: OrchestrationState,
        /// Reply channel.
        reply: RpcReplyPort<Result<RunSummary, ConductorError>>,
    },
    /// Returns the current runtime diagnostics snapshot.
    GetRuntimeDiagnostics {
        /// Reply channel.
        reply: RpcReplyPort<RuntimeDiagnostics>,
    },
    /// Runs garbage collection on the orchestration state.
    RunGc {
        /// Set of referenced instance keys to retain.
        referenced_keys: std::collections::BTreeSet<String>,
        /// Current orchestration state (cloned, actor owns its copy).
        state: OrchestrationState,
        /// Unified configuration whose hashes protect blobs from reclamation.
        unified: UnifiedNickelDocument,
        /// Reply channel.
        reply: RpcReplyPort<Result<OrchestrationState, ConductorError>>,
    },
}

// ---------------------------------------------------------------------------
// Actor implementation
// ---------------------------------------------------------------------------

/// Actor that owns a [`WorkflowCoordinator`] in its state and handles
/// [`ConductorMessage`] requests.
struct ConductorActor<C: CasApi> {
    /// Phantom marker for CAS type.
    _marker: PhantomData<C>,
}

impl<C: CasApi + CasMaintenanceApi> Default for ConductorActor<C> {
    fn default() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<C> Actor for ConductorActor<C>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    type Msg = ConductorMessage;
    type State = WorkflowCoordinator<C>;
    type Arguments = WorkflowCoordinator<C>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        mut args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        args.start_background_gc(crate::defaults::DEFAULT_CONDUCTOR_GC_INTERVAL_SECONDS);
        Ok(args)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ConductorMessage::RunWorkflow {
                workflow_name,
                options: _,
                unified,
                state: mut ws_state,
                reply,
            } => {
                let summary = state.run_workflow(&workflow_name, &unified, &mut ws_state).await;
                let _ = reply.send(summary);
            }
            ConductorMessage::GetRuntimeDiagnostics { reply } => {
                let diagnostics = state.runtime_diagnostics();
                let _ = reply.send(diagnostics);
            }
            ConductorMessage::RunGc { referenced_keys, state: mut gc_state, unified, reply } => {
                let result = state.run_gc(&mut gc_state, &referenced_keys, &unified).await;
                let _ = reply.send(result.map(|_| gc_state));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Typed client handle to a spawned [`ConductorActor`].
///
/// Clients send messages through this handle and await typed replies.
#[derive(Debug, Clone)]
pub(crate) struct ConductorActorClient {
    /// Reference to the spawned actor.
    actor_ref: ActorRef<ConductorMessage>,
    /// RPC timeout duration.
    rpc_timeout: std::time::Duration,
}

impl ConductorActorClient {
    /// Creates a new client from an existing actor reference.
    #[must_use]
    pub(crate) fn new(actor_ref: ActorRef<ConductorMessage>) -> Self {
        Self {
            rpc_timeout: std::time::Duration::from_millis(super::config::rpc_timeout_ms()),
            actor_ref,
        }
    }

    /// Sends a `RunWorkflow` request and awaits the reply.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Internal`] when the actor is unreachable or
    /// the RPC times out.
    pub(crate) async fn run_workflow(
        &self,
        workflow_name: &str,
        options: RunWorkflowOptions,
        unified: UnifiedNickelDocument,
        state: OrchestrationState,
    ) -> Result<RunSummary, ConductorError> {
        match self
            .actor_ref
            .call(
                |reply| ConductorMessage::RunWorkflow {
                    workflow_name: workflow_name.to_string(),
                    options,
                    unified,
                    state,
                    reply,
                },
                Some(self.rpc_timeout),
            )
            .await
        {
            Ok(CallResult::Success(Ok(summary))) => Ok(summary),
            Ok(CallResult::Success(Err(e))) => Err(e),
            Ok(CallResult::Timeout) => {
                Err(ConductorError::rpc_error("ConductorActor", "RPC timeout"))
            }
            Ok(_) => Err(ConductorError::rpc_error("ConductorActor", "RPC channel closed")),
            Err(e) => Err(ConductorError::rpc_error("ConductorActor", e)),
        }
    }

    /// Sends a `GetRuntimeDiagnostics` request and awaits the reply.
    pub(crate) async fn runtime_diagnostics(&self) -> Result<RuntimeDiagnostics, ConductorError> {
        match self
            .actor_ref
            .call(|reply| ConductorMessage::GetRuntimeDiagnostics { reply }, Some(self.rpc_timeout))
            .await
        {
            Ok(CallResult::Success(diag)) => Ok(diag),
            Ok(CallResult::Timeout) => {
                Err(ConductorError::rpc_error("ConductorActor", "RPC timeout"))
            }
            Ok(_) => Err(ConductorError::rpc_error("ConductorActor", "RPC channel closed")),
            Err(e) => Err(ConductorError::rpc_error("ConductorActor", e)),
        }
    }

    /// Sends a `RunGc` request and awaits the updated orchestration state.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Internal`] when the actor is unreachable or
    /// the RPC times out.
    pub(crate) async fn run_gc(
        &self,
        referenced_keys: std::collections::BTreeSet<String>,
        state: OrchestrationState,
        unified: UnifiedNickelDocument,
    ) -> Result<OrchestrationState, ConductorError> {
        match self
            .actor_ref
            .call(
                |reply| ConductorMessage::RunGc { referenced_keys, state, unified, reply },
                Some(self.rpc_timeout),
            )
            .await
        {
            Ok(CallResult::Success(Ok(gc_state))) => Ok(gc_state),
            Ok(CallResult::Success(Err(e))) => Err(e),
            Ok(CallResult::Timeout) => {
                Err(ConductorError::rpc_error("ConductorActor", "RPC timeout"))
            }
            Ok(_) => Err(ConductorError::rpc_error("ConductorActor", "RPC channel closed")),
            Err(e) => Err(ConductorError::rpc_error("ConductorActor", e)),
        }
    }
}

/// Spawns a new conductor actor and returns a client handle.
///
/// # Errors
///
/// Returns [`ConductorError::Internal`] when actor spawn fails.
pub(crate) async fn spawn_conductor_actor<C>(
    cas: Arc<C>,
) -> Result<ConductorActorClient, ConductorError>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    let coordinator = WorkflowCoordinator::new(cas);
    let (actor_ref, _handle) =
        ractor::spawn_named::<ConductorActor<C>>("conductor".to_string(), coordinator)
            .await
            .map_err(|e| {
                ConductorError::Internal(format!("failed to spawn conductor actor: {e}"))
            })?;

    Ok(ConductorActorClient::new(actor_ref))
}
