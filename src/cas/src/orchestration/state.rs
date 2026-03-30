//! Actor marker/state types for CAS orchestration.
//!
//! The `ractor` runtime separates:
//! - actor *implementations* (zero-sized marker structs used as type anchors),
//! - and actor *state* (runtime data passed to `pre_start`/`handle`).
//!
//! Keeping these definitions in one module helps maintain a clear boundary
//! between message contracts, lifecycle wiring, and behavior implementations.

use std::sync::Arc;

use dashmap::DashMap;
use ractor::ActorRef;
use tokio::sync::Notify;

use crate::{FileSystemCas, Hash};

use super::messages::{
    IndexActorMessage, OptimizerActorMessage, StorageActorArgs, StorageActorMessage,
};

/// Marker actor for storage API calls.
///
/// This zero-sized type exists to bind the [`ractor::Actor`] implementation in
/// `runtime.rs` to a concrete actor identity.
#[derive(Debug, Default, Clone, Copy)]
pub struct StorageActor;

/// Runtime state for [`StorageActor`].
///
/// The state keeps shared CAS services plus optional maintenance dependencies
/// used by write-path orchestration.
#[derive(Clone)]
pub struct StorageActorState {
    /// Shared filesystem-backed CAS service.
    pub(super) cas: Arc<FileSystemCas>,
    /// Optional optimizer actor for pressure-triggered maintenance signaling.
    pub(super) optimizer: Option<ActorRef<OptimizerActorMessage>>,
    /// Optional index actor for durability flush coordination.
    pub(super) index: Option<ActorRef<IndexActorMessage>>,
    /// Per-hash in-flight deduplication map used to collapse concurrent puts.
    pub(super) in_flight: Arc<DashMap<Hash, Arc<Notify>>>,
}

/// Marker actor for optimizer/maintenance calls.
#[derive(Debug, Default, Clone, Copy)]
pub struct OptimizerActor;

/// Marker actor for index persistence writes.
#[derive(Debug, Default, Clone, Copy)]
pub struct IndexActor;

/// Marker actor for node-level command dispatch.
///
/// This actor receives wire-style commands and routes them to storage,
/// optimizer, and index actors.
#[derive(Debug, Default, Clone, Copy)]
pub struct CasNodeActor;

/// Runtime state for [`CasNodeActor`].
#[derive(Clone)]
pub struct CasNodeActorState {
    /// Storage actor reference for put/get/delete/constraint commands.
    pub(super) storage: ActorRef<StorageActorMessage>,
    /// Optimizer actor reference for optimize/prune/toggle commands.
    pub(super) optimizer: ActorRef<OptimizerActorMessage>,
    /// Index actor reference for explicit flush commands.
    pub(super) index: ActorRef<IndexActorMessage>,
}

/// Converts startup argument bundle into storage actor runtime state.
impl From<StorageActorArgs> for StorageActorState {
    fn from(args: StorageActorArgs) -> Self {
        Self {
            cas: args.cas,
            optimizer: args.optimizer,
            index: args.index,
            in_flight: Arc::new(DashMap::new()),
        }
    }
}
