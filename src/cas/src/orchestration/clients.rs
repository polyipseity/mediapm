//! Typed client wrappers for orchestration actors.
//!
//! These clients provide a stable, ergonomic façade over raw actor references
//! by centralizing:
//! - RPC timeout policy,
//! - backpressure limits (for storage requests),
//! - and protocol-level response validation for wire-style commands.

use std::sync::Arc;

use bytes::Bytes;
use ractor::{ActorRef, call_t};
use tokio::sync::Semaphore;

use crate::{CasError, Constraint, Hash, IndexRepairReport, OptimizeReport, PruneReport};

use super::config::{DEFAULT_MAX_INFLIGHT_CLIENT_REQUESTS, DEFAULT_RPC_TIMEOUT_MS};
use super::messages::{
    CasNodeActorMessage, CasWireCommand, CasWireResponse, IndexActorMessage, OptimizerActorMessage,
    StorageActorMessage,
};

/// Typed client for issuing storage actor RPC calls.
///
/// The client enforces a semaphore-based in-flight request cap to avoid
/// unbounded concurrent storage pressure from callers.
#[derive(Clone)]
pub struct StorageActorClient {
    pub(super) actor: ActorRef<StorageActorMessage>,
    pub(super) timeout_ms: u64,
    pub(super) permits: Arc<Semaphore>,
}

impl StorageActorClient {
    /// Stores bytes and returns the canonical content hash.
    ///
    /// A permit is acquired before dispatch; if the semaphore is closed the
    /// operation fails before any actor message is sent.
    ///
    /// # Errors
    /// Returns [`CasError`] when permit acquisition fails, actor RPC transport
    /// fails, or the storage actor returns a domain error.
    pub async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        let _permit = self.permits.acquire().await.map_err(|err| {
            CasError::semaphore_closed("acquiring storage actor permit for put", err)
        })?;
        call_t!(self.actor, StorageActorMessage::Put, self.timeout_ms, data)
            .map_err(|err| CasError::actor_rpc("issuing storage put RPC", err))?
    }

    /// Loads bytes for `hash` through the storage actor.
    ///
    /// # Errors
    /// Returns [`CasError`] when permit acquisition fails, actor RPC transport
    /// fails, or the hash is not available.
    pub async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        let _permit = self.permits.acquire().await.map_err(|err| {
            CasError::semaphore_closed("acquiring storage actor permit for get", err)
        })?;
        call_t!(self.actor, StorageActorMessage::Get, self.timeout_ms, hash)
            .map_err(|err| CasError::actor_rpc("issuing storage get RPC", err))?
    }

    /// Deletes one hash (and required dependent rewrites) via storage actor.
    ///
    /// # Errors
    /// Returns [`CasError`] when permit acquisition fails, actor RPC transport
    /// fails, or deletion invariants reject the request.
    pub async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        let _permit = self.permits.acquire().await.map_err(|err| {
            CasError::semaphore_closed("acquiring storage actor permit for delete", err)
        })?;
        call_t!(self.actor, StorageActorMessage::Delete, self.timeout_ms, hash)
            .map_err(|err| CasError::actor_rpc("issuing storage delete RPC", err))?
    }

    /// Sets explicit base constraints for a target hash.
    ///
    /// # Errors
    /// Returns [`CasError`] when permit acquisition fails, actor RPC transport
    /// fails, or constraint validation fails.
    pub async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError> {
        let _permit = self.permits.acquire().await.map_err(|err| {
            CasError::semaphore_closed("acquiring storage actor permit for set_constraint", err)
        })?;
        call_t!(self.actor, StorageActorMessage::SetConstraint, self.timeout_ms, constraint)
            .map_err(|err| CasError::actor_rpc("issuing storage set_constraint RPC", err))?
    }

    /// Returns effective explicit constraint bases for `target_hash`.
    ///
    /// # Errors
    /// Returns [`CasError`] when permit acquisition fails, actor RPC transport
    /// fails, or the target hash is unknown.
    pub async fn constraint_bases(&self, target_hash: Hash) -> Result<Vec<Hash>, CasError> {
        let _permit = self.permits.acquire().await.map_err(|err| {
            CasError::semaphore_closed("acquiring storage actor permit for constraint_bases", err)
        })?;
        call_t!(self.actor, StorageActorMessage::ConstraintBases, self.timeout_ms, target_hash)
            .map_err(|err| CasError::actor_rpc("issuing storage constraint_bases RPC", err))?
    }

    /// Returns the underlying actor reference.
    ///
    /// This is primarily intended for wiring/composition code.
    pub const fn actor_ref(&self) -> &ActorRef<StorageActorMessage> {
        &self.actor
    }
}

/// Typed client for optimizer actor maintenance RPC calls.
#[derive(Clone)]
pub struct OptimizerActorClient {
    pub(super) actor: ActorRef<OptimizerActorMessage>,
    pub(super) timeout_ms: u64,
}

impl OptimizerActorClient {
    /// Runs a single optimize pass.
    ///
    /// # Errors
    /// Returns [`CasError`] when actor RPC transport fails or the optimizer
    /// reports a maintenance failure.
    pub async fn optimize_once(&self) -> Result<OptimizeReport, CasError> {
        call_t!(self.actor, OptimizerActorMessage::OptimizeOnce, self.timeout_ms)
            .map_err(|err| CasError::actor_rpc("issuing optimizer optimize_once RPC", err))?
    }

    /// Runs a single constraint-prune pass.
    ///
    /// # Errors
    /// Returns [`CasError`] when actor RPC transport fails or prune execution
    /// fails.
    pub async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        call_t!(self.actor, OptimizerActorMessage::PruneConstraints, self.timeout_ms)
            .map_err(|err| CasError::actor_rpc("issuing optimizer prune_constraints RPC", err))?
    }

    /// Toggles max-compression mode on the optimizer actor.
    ///
    /// This is fire-and-forget actor messaging (not RPC).
    ///
    /// # Errors
    /// Returns [`CasError`] if the actor cannot accept the message.
    pub fn set_max_compression_mode(&self, enabled: bool) -> Result<(), CasError> {
        self.actor
            .send_message(OptimizerActorMessage::SetMaxCompressionMode(enabled))
            .map_err(|err| CasError::actor_message("sending optimizer max-compression toggle", err))
    }

    /// Returns the underlying actor reference.
    pub const fn actor_ref(&self) -> &ActorRef<OptimizerActorMessage> {
        &self.actor
    }
}

/// Typed client for index actor persistence RPC calls.
#[derive(Clone)]
pub struct IndexActorClient {
    pub(super) actor: ActorRef<IndexActorMessage>,
    pub(super) timeout_ms: u64,
}

impl IndexActorClient {
    /// Flushes the in-memory index snapshot into Redb.
    ///
    /// # Errors
    /// Returns [`CasError`] when actor RPC transport fails or persistence fails.
    pub async fn flush_snapshot(&self) -> Result<(), CasError> {
        call_t!(self.actor, IndexActorMessage::FlushSnapshot, self.timeout_ms)
            .map_err(|err| CasError::actor_rpc("issuing index flush_snapshot RPC", err))?
    }

    /// Rebuilds durable index metadata from the object store.
    ///
    /// # Errors
    /// Returns [`CasError`] when actor RPC transport fails or index repair fails.
    pub async fn repair_index(&self) -> Result<IndexRepairReport, CasError> {
        call_t!(self.actor, IndexActorMessage::RepairIndex, self.timeout_ms)
            .map_err(|err| CasError::actor_rpc("issuing index repair_index RPC", err))?
    }

    /// Migrates durable index metadata to one target schema marker.
    ///
    /// # Errors
    /// Returns [`CasError`] when actor RPC transport fails or migration fails.
    pub async fn migrate_index_to_version(&self, target_version: u32) -> Result<(), CasError> {
        call_t!(
            self.actor,
            IndexActorMessage::MigrateIndexToVersion,
            self.timeout_ms,
            target_version
        )
        .map_err(|err| CasError::actor_rpc("issuing index migrate_index_to_version RPC", err))?
    }

    /// Returns the underlying actor reference.
    pub const fn actor_ref(&self) -> &ActorRef<IndexActorMessage> {
        &self.actor
    }
}

/// Typed client for node-level wire command dispatch.
///
/// This client is useful for distributed-ready command transport layers because
/// it operates on serializable command/response envelopes.
#[derive(Clone)]
pub struct CasNodeActorClient {
    pub(super) actor: ActorRef<CasNodeActorMessage>,
    pub(super) timeout_ms: u64,
}

impl CasNodeActorClient {
    /// Executes one wire command and returns its wire response.
    ///
    /// # Errors
    /// Returns [`CasError`] when actor RPC transport fails or command execution
    /// fails in downstream actors.
    pub async fn execute(&self, command: CasWireCommand) -> Result<CasWireResponse, CasError> {
        call_t!(self.actor, CasNodeActorMessage::Execute, self.timeout_ms, command)
            .map_err(|err| CasError::actor_rpc("issuing cas-node execute RPC", err))?
    }

    /// Convenience helper that performs a wire `Put` and decodes hash response.
    ///
    /// # Errors
    /// Returns [`CasError`] when command execution fails, response decoding
    /// fails, or protocol response variant mismatches.
    pub async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        match self.execute(CasWireCommand::Put { data: data.to_vec() }).await? {
            CasWireResponse::Hash { hash } => hash.parse().map_err(CasError::from),
            other => {
                Err(CasError::protocol(format!("unexpected response for put command: {other:?}")))
            }
        }
    }

    /// Convenience helper that performs a wire `Get` and decodes byte response.
    ///
    /// # Errors
    /// Returns [`CasError`] when command execution fails or protocol response
    /// variant mismatches.
    pub async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        match self.execute(CasWireCommand::Get { hash: hash.to_string() }).await? {
            CasWireResponse::Bytes { data } => Ok(Bytes::from(data)),
            other => {
                Err(CasError::protocol(format!("unexpected response for get command: {other:?}")))
            }
        }
    }

    /// Convenience helper that performs a wire `Delete`.
    ///
    /// # Errors
    /// Returns [`CasError`] when command execution fails or protocol response
    /// variant mismatches.
    pub async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        match self.execute(CasWireCommand::Delete { hash: hash.to_string() }).await? {
            CasWireResponse::Ack => Ok(()),
            other => Err(CasError::protocol(format!(
                "unexpected response for delete command: {other:?}"
            ))),
        }
    }

    /// Convenience helper that performs a wire `SetConstraint` command.
    ///
    /// # Errors
    /// Returns [`CasError`] when command execution fails or protocol response
    /// variant mismatches.
    pub async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError> {
        let command = CasWireCommand::SetConstraint {
            target_hash: constraint.target_hash.to_string(),
            potential_bases: constraint
                .potential_bases
                .into_iter()
                .map(|hash| hash.to_string())
                .collect(),
        };

        match self.execute(command).await? {
            CasWireResponse::Ack => Ok(()),
            other => Err(CasError::protocol(format!(
                "unexpected response for set_constraint command: {other:?}"
            ))),
        }
    }

    /// Convenience helper that performs a wire `RepairIndex` command.
    ///
    /// # Errors
    /// Returns [`CasError`] when command execution fails or protocol response
    /// variant mismatches.
    pub async fn repair_index(&self) -> Result<IndexRepairReport, CasError> {
        match self.execute(CasWireCommand::RepairIndex).await? {
            CasWireResponse::RepairIndexReport {
                object_rows_rebuilt,
                explicit_constraint_rows_restored,
                scanned_object_files,
                skipped_object_files,
                backup_snapshots_considered,
                constraint_source,
            } => Ok(IndexRepairReport {
                object_rows_rebuilt,
                explicit_constraint_rows_restored,
                scanned_object_files,
                skipped_object_files,
                backup_snapshots_considered,
                constraint_source,
            }),
            other => Err(CasError::protocol(format!(
                "unexpected response for repair_index command: {other:?}"
            ))),
        }
    }

    /// Convenience helper that performs a wire `MigrateIndex` command.
    ///
    /// # Errors
    /// Returns [`CasError`] when command execution fails or protocol response
    /// variant mismatches.
    pub async fn migrate_index_to_version(&self, target_version: u32) -> Result<(), CasError> {
        match self.execute(CasWireCommand::MigrateIndex { target_version }).await? {
            CasWireResponse::Ack => Ok(()),
            other => Err(CasError::protocol(format!(
                "unexpected response for migrate_index_to_version command: {other:?}"
            ))),
        }
    }

    /// Returns the underlying actor reference.
    pub const fn actor_ref(&self) -> &ActorRef<CasNodeActorMessage> {
        &self.actor
    }
}

/// Builds a storage client using repository-default timeout and in-flight limits.
pub(super) fn default_storage_client(actor: ActorRef<StorageActorMessage>) -> StorageActorClient {
    StorageActorClient {
        actor,
        timeout_ms: DEFAULT_RPC_TIMEOUT_MS,
        permits: Arc::new(Semaphore::new(DEFAULT_MAX_INFLIGHT_CLIENT_REQUESTS)),
    }
}

/// Builds an optimizer client with default RPC timeout policy.
pub(super) const fn default_optimizer_client(
    actor: ActorRef<OptimizerActorMessage>,
) -> OptimizerActorClient {
    OptimizerActorClient { actor, timeout_ms: DEFAULT_RPC_TIMEOUT_MS }
}

/// Builds an index client with default RPC timeout policy.
pub(super) const fn default_index_client(actor: ActorRef<IndexActorMessage>) -> IndexActorClient {
    IndexActorClient { actor, timeout_ms: DEFAULT_RPC_TIMEOUT_MS }
}

/// Builds a node client with default RPC timeout policy.
pub(super) const fn default_node_client(
    actor: ActorRef<CasNodeActorMessage>,
) -> CasNodeActorClient {
    CasNodeActorClient { actor, timeout_ms: DEFAULT_RPC_TIMEOUT_MS }
}
