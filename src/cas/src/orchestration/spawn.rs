//! Actor spawn and supervision helpers for the CAS orchestration layer.
//!
//! This module centralizes actor startup order and dependency wiring so callers
//! can bootstrap either:
//! - individual actors for focused integration,
//! - or a complete local node stack (`storage + optimizer + index + node`).
//!
//! The helpers return typed clients rather than raw actor references to keep
//! timeout/permit policy consistent at call sites.

use std::sync::Arc;

use ractor::{Actor, ActorRef};

use crate::{CasError, FileSystemCas};

use super::clients::{
    CasNodeActorClient, IndexActorClient, OptimizerActorClient, StorageActorClient,
    default_index_client, default_node_client, default_optimizer_client, default_storage_client,
};
use super::messages::{
    IndexActorMessage, OptimizerActorMessage, StorageActorArgs, StorageActorMessage,
};
use super::state::{CasNodeActor, CasNodeActorState, IndexActor, OptimizerActor, StorageActor};

/// Spawns a storage actor around `cas` using default dependency wiring.
///
/// This is the simplest constructor for storage-only workflows. It does not
/// wire optimizer or index actor dependencies, so post-write maintenance
/// coordination (for example immediate index flush requests) is disabled.
///
/// # Errors
/// Returns [`CasError`] when actor startup fails.
pub async fn spawn_storage_actor(cas: Arc<FileSystemCas>) -> Result<StorageActorClient, CasError> {
    spawn_storage_actor_with_dependencies(cas, None, None).await
}

/// Spawns a storage actor with optional optimizer/index dependencies.
///
/// Use this when the storage actor should coordinate with:
/// - an optimizer actor for disk-pressure maintenance signals, and/or
/// - an index actor for persistence flushes after mutating operations.
///
/// Passing `None` for either dependency keeps the storage actor operational;
/// only that coordination pathway is skipped.
///
/// # Errors
/// Returns [`CasError`] when actor startup fails.
pub async fn spawn_storage_actor_with_dependencies(
    cas: Arc<FileSystemCas>,
    optimizer: Option<ActorRef<OptimizerActorMessage>>,
    index: Option<ActorRef<IndexActorMessage>>,
) -> Result<StorageActorClient, CasError> {
    let args = StorageActorArgs { cas, optimizer, index };
    let (actor, _handle) = Actor::spawn(None, StorageActor, args)
        .await
        .map_err(|err| CasError::actor_rpc("spawning storage actor", err))?;
    Ok(default_storage_client(actor))
}

/// Spawns an optimizer actor around `cas`.
///
/// The returned client exposes maintenance RPCs such as one-shot optimize and
/// prune passes.
///
/// # Errors
/// Returns [`CasError`] when actor startup fails.
pub async fn spawn_optimizer_actor(
    cas: Arc<FileSystemCas>,
) -> Result<OptimizerActorClient, CasError> {
    let (actor, _handle) = Actor::spawn(None, OptimizerActor, cas)
        .await
        .map_err(|err| CasError::actor_rpc("spawning optimizer actor", err))?;
    Ok(default_optimizer_client(actor))
}

/// Spawns an index actor around `cas`.
///
/// The index actor is responsible for durable snapshot flush requests into
/// Redb-backed persistence.
///
/// # Errors
/// Returns [`CasError`] when actor startup fails.
pub async fn spawn_index_actor(cas: Arc<FileSystemCas>) -> Result<IndexActorClient, CasError> {
    let (actor, _handle) = Actor::spawn(None, IndexActor, cas)
        .await
        .map_err(|err| CasError::actor_rpc("spawning index actor", err))?;
    Ok(default_index_client(actor))
}

/// Spawns a node-level command actor from existing actor references.
///
/// This constructor is useful when tests or embedding code want explicit
/// control over actor lifetimes and references before assembling the command
/// façade actor.
///
/// # Errors
/// Returns [`CasError`] when actor startup fails.
pub async fn spawn_cas_node_actor_from_refs(
    storage: ActorRef<StorageActorMessage>,
    optimizer: ActorRef<OptimizerActorMessage>,
    index: ActorRef<IndexActorMessage>,
) -> Result<CasNodeActorClient, CasError> {
    let args = CasNodeActorState { storage, optimizer, index };
    let (actor, _handle) = Actor::spawn(None, CasNodeActor, args)
        .await
        .map_err(|err| CasError::actor_rpc("spawning cas-node actor", err))?;
    Ok(default_node_client(actor))
}

/// Spawns a complete local actor stack (`storage + optimizer + index + node`).
///
/// Startup order is deterministic:
/// 1. optimizer actor,
/// 2. index actor,
/// 3. storage actor (wired to optimizer/index),
/// 4. node command actor.
///
/// The returned node client can execute wire commands and high-level helper
/// calls against the assembled stack.
///
/// # Errors
/// Returns the first [`CasError`] encountered while spawning any actor.
pub async fn spawn_cas_node_actor(cas: Arc<FileSystemCas>) -> Result<CasNodeActorClient, CasError> {
    let optimizer = spawn_optimizer_actor(cas.clone()).await?;
    let index = spawn_index_actor(cas.clone()).await?;
    let storage = spawn_storage_actor_with_dependencies(
        cas,
        Some(optimizer.actor_ref().clone()),
        Some(index.actor_ref().clone()),
    )
    .await?;

    spawn_cas_node_actor_from_refs(
        storage.actor_ref().clone(),
        optimizer.actor_ref().clone(),
        index.actor_ref().clone(),
    )
    .await
}
