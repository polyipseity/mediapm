//! Actor runtime implementations for CAS orchestration.
//!
//! This module contains the concrete `ractor::Actor` implementations for the
//! marker/state types declared in `state.rs`.
//!
//! Design intent:
//! - keep command routing deterministic,
//! - keep side-effecting work inside dedicated actors,
//! - keep wire-command translation isolated to the node actor.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use fs4::available_space;
use ractor::{Actor, ActorProcessingErr, ActorRef, call_t};
use tracing::error;

use crate::{
    CasApi, CasError, CasMaintenanceApi, Constraint, FileSystemCas, Hash, OptimizeOptions,
};

use super::config::{
    CRITICAL_SPACE_BYTES, DEFAULT_RPC_TIMEOUT_MS, HARD_DISK_PRESSURE_PERCENT,
    SOFT_DISK_PRESSURE_PERCENT,
};
use super::messages::{
    CasNodeActorMessage, CasWireCommand, CasWireResponse, IndexActorMessage, OptimizerActorMessage,
    StorageActorMessage,
};
use super::state::{
    CasNodeActor, CasNodeActorState, IndexActor, OptimizerActor, StorageActor, StorageActorState,
};

/// Storage actor runtime handling object CRUD and constraint RPCs.
impl Actor for StorageActor {
    type Msg = StorageActorMessage;
    type State = StorageActorState;
    type Arguments = super::messages::StorageActorArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args.into())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            StorageActorMessage::Put(data, reply) => {
                let hash = Hash::from_content(&data);

                if let Some(waiting) = state.in_flight.get(&hash) {
                    let notify = waiting.clone();
                    drop(waiting);
                    notify.notified().await;
                    let result = state.cas.get(hash).await.map(|_| hash);
                    if let Err(err) = &result {
                        error!(%hash, error = %err, "storage actor put wait-path failed");
                    }
                    let _ = reply.send(result);
                    return Ok(());
                }

                let notify = Arc::new(tokio::sync::Notify::new());
                state.in_flight.insert(hash, notify.clone());

                let result = handle_storage_put(state, data, hash).await;
                if let Err(err) = &result {
                    error!(%hash, error = %err, "storage actor put failed");
                }

                state.in_flight.remove(&hash);
                notify.notify_waiters();
                let _ = reply.send(result);
            }
            StorageActorMessage::Get(hash, reply) => {
                let result = state.cas.get(hash).await;
                if let Err(err) = &result {
                    error!(%hash, error = %err, "storage actor get failed");
                }
                let _ = reply.send(result);
            }
            StorageActorMessage::Delete(hash, reply) => {
                let result = state.cas.delete(hash).await;
                if result.is_ok()
                    && let Some(index) = &state.index
                {
                    match call_t!(index, IndexActorMessage::FlushSnapshot, DEFAULT_RPC_TIMEOUT_MS) {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            error!(%hash, error = %err, "index flush after delete failed")
                        }
                        Err(err) => {
                            error!(%hash, error = %err, "index actor RPC after delete failed")
                        }
                    }
                }
                if let Err(err) = &result {
                    error!(%hash, error = %err, "storage actor delete failed");
                }
                let _ = reply.send(result);
            }
            StorageActorMessage::SetConstraint(constraint, reply) => {
                let result = state.cas.set_constraint(constraint).await;
                if result.is_ok()
                    && let Some(index) = &state.index
                {
                    match call_t!(index, IndexActorMessage::FlushSnapshot, DEFAULT_RPC_TIMEOUT_MS) {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            error!(error = %err, "index flush after set_constraint failed")
                        }
                        Err(err) => {
                            error!(error = %err, "index actor RPC after set_constraint failed")
                        }
                    }
                }
                if let Err(err) = &result {
                    error!(error = %err, "storage actor set_constraint failed");
                }
                let _ = reply.send(result);
            }
            StorageActorMessage::ConstraintBases(target_hash, reply) => {
                let result = state.cas.get_constraint(target_hash).await.map(|constraint| {
                    constraint
                        .map(|constraint| constraint.potential_bases.into_iter().collect())
                        .unwrap_or_else(Vec::new)
                });
                if let Err(err) = &result {
                    error!(%target_hash, error = %err, "storage actor constraint_bases failed");
                }
                let _ = reply.send(result);
            }
        }
        Ok(())
    }
}

/// Optimizer actor runtime handling maintenance operations.
impl Actor for OptimizerActor {
    type Msg = OptimizerActorMessage;
    type State = Arc<FileSystemCas>;
    type Arguments = Arc<FileSystemCas>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        cas: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(cas)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            OptimizerActorMessage::OptimizeOnce(reply) => {
                let _ = reply.send(state.optimize_once(OptimizeOptions::default()).await);
            }
            OptimizerActorMessage::PruneConstraints(reply) => {
                let _ = reply.send(state.prune_constraints().await);
            }
            OptimizerActorMessage::SetMaxCompressionMode(enabled) => {
                let _ = state.set_max_compression_mode(enabled).await;
            }
        }
        Ok(())
    }
}

/// Index actor runtime handling durability and migration calls.
impl Actor for IndexActor {
    type Msg = IndexActorMessage;
    type State = Arc<FileSystemCas>;
    type Arguments = Arc<FileSystemCas>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        cas: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(cas)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            IndexActorMessage::FlushSnapshot(reply) => {
                let result = persist_snapshot_via_index_actor(state.clone()).await;
                let _ = reply.send(result);
            }
            IndexActorMessage::RepairIndex(reply) => {
                let result = state.repair_index().await;
                let _ = reply.send(result);
            }
            IndexActorMessage::MigrateIndexToVersion(target_version, reply) => {
                let result = state.migrate_index_to_version(target_version).await;
                let _ = reply.send(result);
            }
        }
        Ok(())
    }
}

/// Node actor runtime dispatching wire commands to typed actors.
impl Actor for CasNodeActor {
    type Msg = CasNodeActorMessage;
    type State = CasNodeActorState;
    type Arguments = CasNodeActorState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CasNodeActorMessage::Execute(command, reply) => {
                let command_for_log = command.clone();
                let result = execute_wire_command(state, command).await;
                if let Err(err) = &result {
                    error!(?command_for_log, error = %err, "cas node command failed");
                }
                let _ = reply.send(result);
            }
        }
        Ok(())
    }
}

/// Handles deduplicated `put` execution with disk-pressure policy checks.
///
/// This helper applies pressure policy before writes and ensures the persisted
/// hash matches caller pre-computation.
async fn handle_storage_put(
    state: &StorageActorState,
    data: Bytes,
    expected_hash: Hash,
) -> Result<Hash, CasError> {
    match evaluate_disk_pressure(state.cas.clone()).await? {
        DiskPressure::Hard { available_bytes, cas_size_bytes } => {
            if let Some(optimizer) = &state.optimizer {
                // AGENT_NOTE: Under critical pressure we prune immediately to
                // maximize recovery chance before rejecting writes.
                match call_t!(
                    optimizer,
                    OptimizerActorMessage::PruneConstraints,
                    DEFAULT_RPC_TIMEOUT_MS
                ) {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        error!(error = %err, "optimizer prune failed under hard pressure")
                    }
                    Err(err) => {
                        error!(error = %err, "optimizer prune RPC failed under hard pressure")
                    }
                }
            }
            return Err(CasError::OutOfSpace { available_bytes, cas_size_bytes });
        }
        DiskPressure::Soft => {
            // AGENT_NOTE: Soft pressure enables temporary compression-first mode;
            // normal pressure below restores the configured alpha behavior.
            state.cas.set_max_compression_mode(true).await?;
            if let Some(optimizer) = &state.optimizer {
                let _ = optimizer.send_message(OptimizerActorMessage::SetMaxCompressionMode(true));
                let _ =
                    call_t!(optimizer, OptimizerActorMessage::OptimizeOnce, DEFAULT_RPC_TIMEOUT_MS);
            }
        }
        DiskPressure::Normal => {
            if state.cas.max_compression_mode().await? {
                state.cas.set_max_compression_mode(false).await?;
                if let Some(optimizer) = &state.optimizer {
                    let _ =
                        optimizer.send_message(OptimizerActorMessage::SetMaxCompressionMode(false));
                }
            }
        }
    }

    let hash = state.cas.put(data).await?;
    if hash != expected_hash {
        return Err(CasError::invariant(
            "hash mismatch between dedupe pre-compute and persisted put".to_string(),
        ));
    }

    if let Some(index) = &state.index {
        match call_t!(index, IndexActorMessage::FlushSnapshot, DEFAULT_RPC_TIMEOUT_MS) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => error!(error = %err, "index flush after put failed"),
            Err(err) => error!(error = %err, "index actor RPC after put failed"),
        }
    }

    Ok(hash)
}

/// Executes one wire command through typed actor dependencies.
///
/// Converts command/response wire envelopes into typed actor RPC calls while
/// preserving protocol-shape validation.
async fn execute_wire_command(
    state: &CasNodeActorState,
    command: CasWireCommand,
) -> Result<CasWireResponse, CasError> {
    match command {
        CasWireCommand::Put { data } => {
            let hash = call_t!(
                state.storage,
                StorageActorMessage::Put,
                DEFAULT_RPC_TIMEOUT_MS,
                Bytes::from(data)
            )
            .map_err(|err| CasError::actor_rpc("executing wire Put via storage actor", err))??;
            Ok(CasWireResponse::Hash { hash: hash.to_string() })
        }
        CasWireCommand::Get { hash } => {
            let hash: Hash = hash.parse()?;
            let data =
                call_t!(state.storage, StorageActorMessage::Get, DEFAULT_RPC_TIMEOUT_MS, hash)
                    .map_err(|err| {
                    CasError::actor_rpc("executing wire Get via storage actor", err)
                })??;
            Ok(CasWireResponse::Bytes { data: data.to_vec() })
        }
        CasWireCommand::SetConstraint { target_hash, potential_bases } => {
            let target_hash: Hash = target_hash.parse()?;
            let potential_bases: Vec<Hash> =
                potential_bases.into_iter().map(|hash| hash.parse()).collect::<Result<_, _>>()?;
            let constraint = Constraint {
                target_hash,
                potential_bases: potential_bases.into_iter().collect::<BTreeSet<_>>(),
            };

            call_t!(
                state.storage,
                StorageActorMessage::SetConstraint,
                DEFAULT_RPC_TIMEOUT_MS,
                constraint
            )
            .map_err(|err| {
                CasError::actor_rpc("executing wire SetConstraint via storage actor", err)
            })??;
            Ok(CasWireResponse::Ack)
        }
        CasWireCommand::ConstraintBases { target_hash } => {
            let target_hash: Hash = target_hash.parse()?;
            let bases = call_t!(
                state.storage,
                StorageActorMessage::ConstraintBases,
                DEFAULT_RPC_TIMEOUT_MS,
                target_hash
            )
            .map_err(|err| {
                CasError::actor_rpc("executing wire ConstraintBases via storage actor", err)
            })??;
            Ok(CasWireResponse::Bases {
                hashes: bases.into_iter().map(|hash| hash.to_string()).collect(),
            })
        }
        CasWireCommand::Delete { hash } => {
            let hash: Hash = hash.parse()?;
            call_t!(state.storage, StorageActorMessage::Delete, DEFAULT_RPC_TIMEOUT_MS, hash)
                .map_err(|err| {
                    CasError::actor_rpc("executing wire Delete via storage actor", err)
                })??;
            Ok(CasWireResponse::Ack)
        }
        CasWireCommand::OptimizeOnce => {
            let report = call_t!(
                state.optimizer,
                OptimizerActorMessage::OptimizeOnce,
                DEFAULT_RPC_TIMEOUT_MS
            )
            .map_err(|err| {
                CasError::actor_rpc("executing wire OptimizeOnce via optimizer actor", err)
            })??;
            Ok(CasWireResponse::OptimizeReport { rewritten_objects: report.rewritten_objects })
        }
        CasWireCommand::PruneConstraints => {
            let report = call_t!(
                state.optimizer,
                OptimizerActorMessage::PruneConstraints,
                DEFAULT_RPC_TIMEOUT_MS
            )
            .map_err(|err| {
                CasError::actor_rpc("executing wire PruneConstraints via optimizer actor", err)
            })??;
            Ok(CasWireResponse::PruneReport { removed_candidates: report.removed_candidates })
        }
        CasWireCommand::FlushIndex => {
            call_t!(state.index, IndexActorMessage::FlushSnapshot, DEFAULT_RPC_TIMEOUT_MS)
                .map_err(|err| {
                    CasError::actor_rpc("executing wire FlushIndex via index actor", err)
                })??;
            Ok(CasWireResponse::Ack)
        }
        CasWireCommand::RepairIndex => {
            let report =
                call_t!(state.index, IndexActorMessage::RepairIndex, DEFAULT_RPC_TIMEOUT_MS)
                    .map_err(|err| {
                        CasError::actor_rpc("executing wire RepairIndex via index actor", err)
                    })??;
            Ok(CasWireResponse::RepairIndexReport {
                object_rows_rebuilt: report.object_rows_rebuilt,
                explicit_constraint_rows_restored: report.explicit_constraint_rows_restored,
                scanned_object_files: report.scanned_object_files,
                skipped_object_files: report.skipped_object_files,
                backup_snapshots_considered: report.backup_snapshots_considered,
                constraint_source: report.constraint_source,
            })
        }
        CasWireCommand::MigrateIndex { target_version } => {
            call_t!(
                state.index,
                IndexActorMessage::MigrateIndexToVersion,
                DEFAULT_RPC_TIMEOUT_MS,
                target_version
            )
            .map_err(|err| {
                CasError::actor_rpc("executing wire MigrateIndex via index actor", err)
            })??;
            Ok(CasWireResponse::Ack)
        }
        CasWireCommand::SetMaxCompressionMode { enabled } => {
            state
                .optimizer
                .send_message(OptimizerActorMessage::SetMaxCompressionMode(enabled))
                .map_err(|err| {
                    CasError::actor_message("sending optimizer max-compression toggle", err)
                })?;
            Ok(CasWireResponse::Ack)
        }
    }
}

/// Persists full index snapshot through index-actor call path.
async fn persist_snapshot_via_index_actor(cas: Arc<FileSystemCas>) -> Result<(), CasError> {
    cas.flush_index_snapshot().await
}

/// Coarse disk-pressure classification for write-path policy decisions.
enum DiskPressure {
    /// Healthy free-space ratio.
    Normal,
    /// Low free-space ratio; writes still allowed with compression bias.
    Soft,
    /// Critical free space; writes are rejected.
    Hard { available_bytes: u64, cas_size_bytes: u64 },
}

/// Computes disk-pressure state from free-space and CAS-size metrics.
///
/// Classification thresholds are controlled by orchestration config constants.
async fn evaluate_disk_pressure(cas: Arc<FileSystemCas>) -> Result<DiskPressure, CasError> {
    let root = cas.root_path().to_path_buf();
    let available_bytes = tokio::task::spawn_blocking(move || {
        available_space(&root)
            .map_err(|source| CasError::io("checking available disk space", root.clone(), source))
    })
    .await
    .map_err(|err| CasError::task_join("evaluating disk pressure", err))??;

    let cas_size_bytes = cas.cas_store_size_bytes().await?;
    if cas_size_bytes == 0 {
        return Ok(DiskPressure::Normal);
    }

    let scaled_available = u128::from(available_bytes) * 100;
    let scaled_cas = u128::from(cas_size_bytes);

    if available_bytes <= CRITICAL_SPACE_BYTES
        || scaled_available < scaled_cas * HARD_DISK_PRESSURE_PERCENT
    {
        return Ok(DiskPressure::Hard { available_bytes, cas_size_bytes });
    }
    if scaled_available < scaled_cas * SOFT_DISK_PRESSURE_PERCENT {
        return Ok(DiskPressure::Soft);
    }

    Ok(DiskPressure::Normal)
}
