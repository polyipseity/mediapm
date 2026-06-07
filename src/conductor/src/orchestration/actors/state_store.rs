//! Actor-backed orchestration-state loading and persistence.
//!
//! The state-store actor centralizes CAS-backed state reads, writes, and
//! unsaved-output cleanup. This keeps the coordinator focused on deterministic
//! planning while side effects stay concentrated in one execution-oriented
//! service.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::{CasApi, CasError, Hash};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};

use crate::error::ConductorError;
use crate::model::config::ImpureTimestamp;
use crate::model::state::{OrchestrationState, decode_state, encode_state};
use crate::orchestration::config::rpc_timeout_ms;
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
        call_t!(self.actor, StateStoreMessage::GetCurrentState, rpc_timeout_ms()).map_err(
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
        call_t!(self.actor, StateStoreMessage::LoadStateFromPointer, rpc_timeout_ms(), pointer)
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
        call_t!(self.actor, StateStoreMessage::CommitRun, rpc_timeout_ms(), Box::new(request))
            .map_err(|err| {
                ConductorError::Internal(format!("state store commit_run RPC failed: {err}"))
            })?
    }

    /// Sets the instance GC TTL on the state-store actor (fire-and-forget).
    pub(in crate::orchestration) fn set_instance_ttl(
        &self,
        ttl: Option<u64>,
    ) -> Result<(), ConductorError> {
        self.actor.cast(StateStoreMessage::SetInstanceTtl(ttl)).map_err(|err| {
            ConductorError::Internal(format!("state store set_instance_ttl cast failed: {err}"))
        })
    }

    /// Persists one provided state snapshot and publishes it as current state
    /// without unsaved-output cleanup.
    pub(in crate::orchestration) async fn persist_and_publish_state(
        &self,
        state: OrchestrationState,
    ) -> Result<Hash, ConductorError> {
        call_t!(
            self.actor,
            StateStoreMessage::PersistAndPublishState,
            rpc_timeout_ms(),
            Box::new(state)
        )
        .map_err(|err| {
            ConductorError::Internal(format!(
                "state store persist_and_publish_state RPC failed: {err}"
            ))
        })?
    }

    /// Runs instance GC on the current in-memory state, optionally with a TTL
    /// override. The cleaned state is persisted to CAS and published.
    pub(in crate::orchestration) async fn run_gc(
        &self,
        ttl_override: Option<u64>,
    ) -> Result<(), ConductorError> {
        call_t!(self.actor, StateStoreMessage::RunGc, rpc_timeout_ms(), ttl_override).map_err(
            |err| ConductorError::Internal(format!("state store run_gc RPC failed: {err}")),
        )?
    }

    /// Returns the last persisted state blob CAS pointer, if any.
    #[allow(dead_code)]
    pub(in crate::orchestration) async fn get_state_pointer(
        &self,
    ) -> Result<Option<Hash>, ConductorError> {
        call_t!(self.actor, StateStoreMessage::GetStatePointer, rpc_timeout_ms()).map_err(
            |err| {
                ConductorError::Internal(format!("state store get_state_pointer RPC failed: {err}"))
            },
        )?
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
    /// Persists one external state snapshot and publishes it without cleanup.
    PersistAndPublishState(Box<OrchestrationState>, RpcReplyPort<Result<Hash, ConductorError>>),
    /// Sets the instance TTL for GC pruning.
    SetInstanceTtl(Option<u64>),
    /// Runs instance GC with an optional TTL override.
    RunGc(Option<u64>, RpcReplyPort<Result<(), ConductorError>>),
    /// Returns the last persisted state blob CAS pointer, if any.
    #[allow(dead_code)]
    GetStatePointer(RpcReplyPort<Result<Option<Hash>, ConductorError>>),
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
    /// Optional instance TTL in seconds for GC pruning.
    /// When `None`, instance GC is disabled.
    instance_ttl_seconds: Option<u64>,
    /// Last persisted state blob CAS pointer, if any.
    current_state_pointer: Option<Hash>,
}

impl<C> StateStoreService<C>
where
    C: CasApi + Send + Sync + 'static,
{
    /// Runs instance GC on the in-memory state using the provided TTL override
    /// (or the stored TTL if override is `None`). Persists the cleaned state
    /// to CAS and publishes it.
    async fn run_gc(&mut self, ttl_override: Option<u64>) -> Result<(), ConductorError> {
        let ttl = ttl_override.or(self.instance_ttl_seconds);
        if let Some(ttl_seconds) = ttl {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
            let cutoff = ImpureTimestamp {
                epoch_seconds: now.as_secs().saturating_sub(ttl_seconds),
                subsec_nanos: now.subsec_nanos(),
            };
            self.current_state.gc_instances(cutoff);
            let _pointer = self.persist_state_blob(&self.current_state).await?;
        }
        Ok(())
    }

    /// Loads a state snapshot from CAS or falls back to the actor's in-memory state.
    async fn load_state_from_pointer(
        &self,
        state_pointer: Option<Hash>,
    ) -> Result<OrchestrationState, ConductorError> {
        // Fast path: if pointer matches current in-memory state, skip CAS read/deserialize.
        if state_pointer == self.current_state_pointer {
            return Ok(self.current_state.clone());
        }
        if let Some(pointer) = state_pointer {
            match decode_state(&*self.cas, pointer).await {
                Ok(state) => Ok(state),
                Err(ConductorError::Cas(CasError::NotFound(_))) => Ok(self.current_state.clone()),
                Err(other) => Err(other),
            }
        } else {
            Ok(self.current_state.clone())
        }
    }

    /// Persists one completed run, updates published state, and deletes unprotected unsaved outputs.
    async fn commit_run(
        &mut self,
        mut request: CommitStateRequest,
    ) -> Result<Hash, ConductorError> {
        if let Some(ttl_seconds) = self.instance_ttl_seconds {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
            let cutoff = ImpureTimestamp {
                epoch_seconds: now.as_secs().saturating_sub(ttl_seconds),
                subsec_nanos: now.subsec_nanos(),
            };
            request.next_state.gc_instances(cutoff);
        }
        // Quick skip: if no mutations occurred after GC, return existing pointer
        // without re-persisting or cleaning up unsaved outputs.
        if request.next_state == self.current_state {
            if let Some(pointer) = self.current_state_pointer {
                return Ok(pointer);
            }
        }
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
        self.current_state_pointer = Some(current_state_pointer);
        Ok(current_state_pointer)
    }

    /// Persists one provided state snapshot and publishes it as current state
    /// without GC or unsaved-output cleanup side effects.
    ///
    /// GC is intentionally NOT applied here — it is the responsibility of
    /// `commit_run` (which GCs before persisting) and `run_gc` (which is
    /// explicitly invoked).  Applying GC in this method would create a
    /// double-GC hazard when `load_resolved_state` re-publishes a snapshot
    /// that was already committed.
    async fn persist_and_publish_state(
        &mut self,
        next_state: OrchestrationState,
    ) -> Result<Hash, ConductorError> {
        let pointer = self.persist_state_blob(&next_state).await?;
        self.current_state = next_state;
        self.current_state_pointer = Some(pointer);
        Ok(pointer)
    }

    /// Serializes one orchestration state snapshot into CAS using V2 format.
    async fn persist_state_blob(&self, state: &OrchestrationState) -> Result<Hash, ConductorError> {
        encode_state(&*self.cas, state.clone()).await
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
                if output.persistence.save.should_persist() {
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
                if !output.persistence.save.should_persist() {
                    deletion_candidates.insert(output.hash);
                }
            }
        }

        for candidate in deletion_candidates {
            if protected.contains(&candidate) {
                continue;
            }
            match self.cas.delete(candidate).await {
                Ok(()) | Err(CasError::NotFound(_)) => {}
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
    type Arguments = (Arc<C>, Option<u64>);

    /// Initializes the actor with the shared CAS handle, instance TTL, and an empty in-memory state snapshot.
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(StateStoreService {
            cas: args.0,
            current_state: OrchestrationState::default(),
            instance_ttl_seconds: args.1,
            current_state_pointer: None,
        })
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
            StateStoreMessage::PersistAndPublishState(next_state, reply) => {
                let _ = reply.send(state.persist_and_publish_state(*next_state).await);
            }
            StateStoreMessage::SetInstanceTtl(ttl) => {
                state.instance_ttl_seconds = ttl;
            }
            StateStoreMessage::RunGc(ttl_override, reply) => {
                let _ = reply.send(state.run_gc(ttl_override).await);
            }
            StateStoreMessage::GetStatePointer(reply) => {
                let _ = reply.send(Ok(state.current_state_pointer));
            }
        }
        Ok(())
    }
}

/// Spawns the state-store actor and returns its typed client.
pub(in crate::orchestration) async fn spawn_state_store_actor<C>(
    cas: Arc<C>,
    instance_ttl_seconds: Option<u64>,
) -> Result<StateStoreClient, ConductorError>
where
    C: CasApi + Send + Sync + 'static,
{
    let (actor_ref, _handle) =
        Actor::spawn(None, StateStoreActor::<C>::default(), (cas, instance_ttl_seconds))
            .await
            .map_err(|err| {
                ConductorError::Internal(format!("failed spawning state store actor: {err}"))
            })?;
    Ok(StateStoreClient::new(actor_ref))
}
