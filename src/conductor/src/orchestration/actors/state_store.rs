//! Actor-backed orchestration-state loading and persistence.
//!
//! The state-store actor centralizes CAS-backed state reads, writes, and
//! unsaved-output cleanup. This keeps the coordinator focused on deterministic
//! planning while side effects stay concentrated in one execution-oriented
//! service.

use std::collections::BTreeSet;
use std::sync::Arc;

use mediapm_cas::{CasApi, CasError, Hash};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};

use crate::error::ConductorError;
use crate::model::state::{OrchestrationState, decode_state, encode_state};
use crate::orchestration::config::DEFAULT_RPC_TIMEOUT_MS;
use crate::orchestration::protocol::{CommitStateRequest, UnifiedNickelDocument};

/// Typed client for the state-store actor.
#[derive(Debug, Clone)]
pub(in crate::orchestration) struct StateStoreClient {
    /// Actor reference used for all state-store RPC calls.
    actor: ActorRef<StateStoreMessage>,
}

impl StateStoreClient {
    /// Creates a typed client around one state-store actor reference.
    #[must_use]
    fn new(actor: ActorRef<StateStoreMessage>) -> Self {
        Self { actor }
    }

    /// Returns the actor's current in-memory orchestration-state snapshot.
    pub(in crate::orchestration) async fn current_state(
        &self,
    ) -> Result<OrchestrationState, ConductorError> {
        call_t!(self.actor, StateStoreMessage::GetCurrentState, DEFAULT_RPC_TIMEOUT_MS).map_err(
            |err| {
                ConductorError::Internal(format!("state store get_current_state RPC failed: {err}"))
            },
        )?
    }

    /// Loads a state snapshot from one persisted pointer, or falls back to the current in-memory state.
    pub(in crate::orchestration) async fn load_state_from_pointer(
        &self,
        pointer: Option<Hash>,
    ) -> Result<OrchestrationState, ConductorError> {
        call_t!(
            self.actor,
            StateStoreMessage::LoadStateFromPointer,
            DEFAULT_RPC_TIMEOUT_MS,
            pointer
        )
        .map_err(|err| {
            ConductorError::Internal(format!(
                "state store load_state_from_pointer RPC failed: {err}"
            ))
        })?
    }

    /// Persists a completed workflow run, publishes it as current state, and returns the new state pointer.
    pub(in crate::orchestration) async fn commit_run(
        &self,
        request: CommitStateRequest,
    ) -> Result<Hash, ConductorError> {
        call_t!(self.actor, StateStoreMessage::CommitRun, DEFAULT_RPC_TIMEOUT_MS, Box::new(request))
            .map_err(|err| {
                ConductorError::Internal(format!("state store commit_run RPC failed: {err}"))
            })?
    }
}

/// Requests supported by the state-store actor.
#[derive(Debug)]
enum StateStoreMessage {
    /// Returns the current in-memory orchestration state.
    GetCurrentState(RpcReplyPort<Result<OrchestrationState, ConductorError>>),
    /// Loads a state snapshot from the supplied pointer, if any.
    LoadStateFromPointer(Option<Hash>, RpcReplyPort<Result<OrchestrationState, ConductorError>>),
    /// Persists the next completed run and performs unsaved-output cleanup.
    CommitRun(Box<CommitStateRequest>, RpcReplyPort<Result<Hash, ConductorError>>),
}

/// Marker actor for orchestration-state persistence.
#[derive(Debug, Clone, Copy)]
struct StateStoreActor<C> {
    /// Type marker for the CAS implementation shared with this actor.
    _phantom: std::marker::PhantomData<C>,
}

impl<C> Default for StateStoreActor<C> {
    /// Builds one marker actor with no local fields.
    fn default() -> Self {
        Self { _phantom: std::marker::PhantomData }
    }
}

/// Mutable state-store service owned by the actor.
#[derive(Debug, Clone)]
struct StateStoreService<C>
where
    C: CasApi,
{
    /// Shared CAS handle used for state serialization and cleanup.
    cas: Arc<C>,
    /// Current in-memory orchestration state published to callers.
    current_state: OrchestrationState,
}

impl<C> StateStoreService<C>
where
    C: CasApi + Send + Sync + 'static,
{
    /// Loads a state snapshot from CAS or falls back to the actor's in-memory state.
    async fn load_state_from_pointer(
        &self,
        state_pointer: Option<Hash>,
    ) -> Result<OrchestrationState, ConductorError> {
        if let Some(pointer) = state_pointer {
            match self.cas.get(pointer).await {
                Ok(bytes) => decode_state(&bytes),
                Err(CasError::NotFound(_)) => Ok(self.current_state.clone()),
                Err(other) => Err(ConductorError::Cas(other)),
            }
        } else {
            Ok(self.current_state.clone())
        }
    }

    /// Persists one completed run, updates published state, and deletes unprotected unsaved outputs.
    async fn commit_run(&mut self, request: CommitStateRequest) -> Result<Hash, ConductorError> {
        let current_state_pointer = self.persist_state_blob(&request.next_state).await?;
        self.delete_unsaved_outputs(
            &request.next_state,
            &request.pending_unsaved_hashes,
            &request.unified,
            request.prior_state_pointer,
            current_state_pointer,
        )
        .await?;
        self.current_state = request.next_state;
        Ok(current_state_pointer)
    }

    /// Serializes one orchestration state snapshot into CAS.
    async fn persist_state_blob(&self, state: &OrchestrationState) -> Result<Hash, ConductorError> {
        let encoded = encode_state(state.clone())?;
        self.cas.put(encoded).await.map_err(ConductorError::from)
    }

    /// Deletes unsaved outputs that are no longer referenced by state or merged config.
    async fn delete_unsaved_outputs(
        &self,
        state: &OrchestrationState,
        pending_unsaved_hashes: &BTreeSet<Hash>,
        unified: &UnifiedNickelDocument,
        machine_state_pointer: Option<Hash>,
        current_state_pointer: Hash,
    ) -> Result<(), ConductorError> {
        let mut protected = BTreeSet::new();
        for instance in state.instances.values() {
            for output in instance.outputs.values() {
                if output.persistence.save {
                    protected.insert(output.hash);
                }
            }
        }

        protected.extend(unified.external_data.keys().copied());
        protected.extend(unified.tool_content_hashes.iter().copied());
        protected.insert(current_state_pointer);
        if let Some(pointer) = machine_state_pointer {
            protected.insert(pointer);
        }

        let mut deletion_candidates: BTreeSet<Hash> = pending_unsaved_hashes.clone();
        for instance in state.instances.values() {
            for output in instance.outputs.values() {
                if !output.persistence.save {
                    deletion_candidates.insert(output.hash);
                }
            }
        }

        for candidate in deletion_candidates {
            if protected.contains(&candidate) {
                continue;
            }
            match self.cas.delete(candidate).await {
                Ok(()) => {}
                Err(CasError::NotFound(_)) => {}
                Err(other) => return Err(ConductorError::Cas(other)),
            }
        }

        Ok(())
    }
}

impl<C> Actor for StateStoreActor<C>
where
    C: CasApi + Send + Sync + 'static,
{
    type Msg = StateStoreMessage;
    type State = StateStoreService<C>;
    type Arguments = Arc<C>;

    /// Initializes the actor with the shared CAS handle and an empty in-memory state snapshot.
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(StateStoreService { cas: args, current_state: OrchestrationState::default() })
    }

    /// Handles state loading, current-state queries, and completed-run commits.
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            StateStoreMessage::GetCurrentState(reply) => {
                let _ = reply.send(Ok(state.current_state.clone()));
            }
            StateStoreMessage::LoadStateFromPointer(pointer, reply) => {
                let _ = reply.send(state.load_state_from_pointer(pointer).await);
            }
            StateStoreMessage::CommitRun(request, reply) => {
                let _ = reply.send(state.commit_run(*request).await);
            }
        }
        Ok(())
    }
}

/// Spawns the state-store actor and returns its typed client.
pub(in crate::orchestration) async fn spawn_state_store_actor<C>(
    cas: Arc<C>,
) -> Result<StateStoreClient, ConductorError>
where
    C: CasApi + Send + Sync + 'static,
{
    let (actor_ref, _handle) =
        Actor::spawn(None, StateStoreActor::<C>::default(), cas).await.map_err(|err| {
            ConductorError::Internal(format!("failed spawning state store actor: {err}"))
        })?;
    Ok(StateStoreClient::new(actor_ref))
}
