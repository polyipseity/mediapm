//! Unified actor message envelopes for CAS orchestration.
//!
//! Keeping all message contracts in one module makes it easier for human and
//! AI maintainers to track command flow and evolve actor APIs coherently.

use std::sync::Arc;

use bytes::Bytes;
use ractor::{ActorRef, RpcReplyPort};
use serde::{Deserialize, Serialize};

use crate::{
    CasError, Constraint, FileSystemCas, Hash, IndexRepairConstraintSource, IndexRepairReport,
    OptimizeReport, PruneReport,
};

/// Message set for CAS storage actor operations.
#[derive(Debug)]
pub enum StorageActorMessage {
    /// Stores bytes and returns canonical content hash through reply port.
    ///
    /// The actor may trigger maintenance side effects (for example index flush)
    /// after successful persistence.
    Put(Bytes, RpcReplyPort<Result<Hash, CasError>>),
    /// Retrieves bytes for one hash through reply port.
    Get(Hash, RpcReplyPort<Result<Bytes, CasError>>),
    /// Deletes one hash and any required dependent rewrites.
    Delete(Hash, RpcReplyPort<Result<(), CasError>>),
    /// Replaces explicit optimization constraints for a target hash.
    SetConstraint(Constraint, RpcReplyPort<Result<(), CasError>>),
    /// Returns effective explicit bases for `target_hash`.
    ///
    /// Empty response means there is no explicit row and base choice is
    /// unconstrained by user policy.
    ConstraintBases(Hash, RpcReplyPort<Result<Vec<Hash>, CasError>>),
}

/// Message set for CAS optimizer actor operations.
#[derive(Debug)]
pub enum OptimizerActorMessage {
    /// Runs one optimizer pass and returns rewrite statistics.
    OptimizeOnce(RpcReplyPort<Result<OptimizeReport, CasError>>),
    /// Prunes stale/invalid constraint candidates and returns removal stats.
    PruneConstraints(RpcReplyPort<Result<PruneReport, CasError>>),
    /// Toggles max-compression mode (`alpha = 0`) without RPC reply.
    SetMaxCompressionMode(bool),
}

/// Message set for index actor persistence operations.
#[derive(Debug)]
pub enum IndexActorMessage {
    /// Persists the current in-memory index snapshot to Redb.
    FlushSnapshot(RpcReplyPort<Result<(), CasError>>),
    /// Rebuilds durable index metadata from the object store.
    RepairIndex(RpcReplyPort<Result<IndexRepairReport, CasError>>),
    /// Migrates durable index metadata to one target schema marker.
    MigrateIndexToVersion(u32, RpcReplyPort<Result<(), CasError>>),
}

/// Serializable command envelope for distributed-ready CAS actor orchestration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CasWireCommand {
    /// Store bytes and return canonical hash.
    Put {
        /// Raw payload bytes to persist in CAS.
        data: Vec<u8>,
    },
    /// Load bytes for a hash string accepted by [`Hash::from_str`].
    Get {
        /// Canonical hash string for the object to read.
        hash: String,
    },
    /// Delete bytes/metadata for the given hash string.
    Delete {
        /// Canonical hash string for the object to delete.
        hash: String,
    },
    /// Replace explicit constraint candidates for a target hash.
    SetConstraint {
        /// Canonical hash string whose constraint row should be replaced.
        target_hash: String,
        /// Candidate base hash strings that may be used for optimization.
        potential_bases: Vec<String>,
    },
    /// Read effective explicit constraint bases for a target hash.
    ConstraintBases {
        /// Canonical hash string whose explicit bases should be queried.
        target_hash: String,
    },
    /// Run one optimizer pass.
    OptimizeOnce,
    /// Run one prune pass.
    PruneConstraints,
    /// Flush index snapshot persistence.
    FlushIndex,
    /// Rebuild durable index metadata from the object store.
    RepairIndex,
    /// Migrate durable index metadata to one target schema marker.
    MigrateIndex {
        /// Target schema marker value.
        target_version: u32,
    },
    /// Toggle compression-first mode.
    SetMaxCompressionMode {
        /// `true` enables compression-first mode, `false` restores defaults.
        enabled: bool,
    },
}

/// Serializable response envelope for distributed-ready CAS actor orchestration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CasWireResponse {
    /// Response carrying one canonical hash string.
    Hash {
        /// Canonical hash string result.
        hash: String,
    },
    /// Response carrying raw payload bytes.
    Bytes {
        /// Raw object payload bytes.
        data: Vec<u8>,
    },
    /// Response carrying list of hash strings.
    Bases {
        /// Canonical hash strings representing effective explicit bases.
        hashes: Vec<String>,
    },
    /// Optimizer report response.
    OptimizeReport {
        /// Number of objects rewritten during the optimize pass.
        rewritten_objects: usize,
    },
    /// Prune report response.
    PruneReport {
        /// Number of stale/invalid constraint candidates removed.
        removed_candidates: usize,
    },
    /// Index repair report response.
    RepairIndexReport {
        /// Number of non-empty object rows rebuilt into the index.
        object_rows_rebuilt: usize,
        /// Number of explicit constraint rows restored.
        explicit_constraint_rows_restored: usize,
        /// Number of object files scanned.
        scanned_object_files: usize,
        /// Number of invalid/unrecoverable object files skipped.
        skipped_object_files: usize,
        /// Number of backup snapshots examined.
        backup_snapshots_considered: usize,
        /// Source of restored explicit constraints, if any.
        constraint_source: IndexRepairConstraintSource,
    },
    /// No-payload acknowledgment response.
    Ack,
}

/// Message set for node-level command actor.
#[derive(Debug)]
pub enum CasNodeActorMessage {
    /// Executes one wire command and returns wire response through reply port.
    Execute(CasWireCommand, RpcReplyPort<Result<CasWireResponse, CasError>>),
}

/// Spawn-time arguments for storage actor.
#[derive(Clone)]
pub struct StorageActorArgs {
    /// Shared CAS service used by this storage actor.
    pub cas: Arc<FileSystemCas>,
    /// Optional optimizer actor for pressure-triggered compression signaling.
    pub optimizer: Option<ActorRef<OptimizerActorMessage>>,
    /// Optional index actor for snapshot flush coordination.
    pub index: Option<ActorRef<IndexActorMessage>>,
}

#[cfg(test)]
mod tests {
    use super::{CasWireCommand, CasWireResponse};

    #[test]
    fn cas_wire_command_serializes_and_deserializes_stably() {
        let command = CasWireCommand::SetConstraint {
            target_hash: "blake3:abcdef".to_string(),
            potential_bases: vec!["blake3:123456".to_string(), "blake3:7890ab".to_string()],
        };

        let json = serde_json::to_string(&command).expect("serialize command");
        let decoded: CasWireCommand = serde_json::from_str(&json).expect("deserialize command");

        match decoded {
            CasWireCommand::SetConstraint { target_hash, potential_bases } => {
                assert_eq!(target_hash, "blake3:abcdef");
                assert_eq!(potential_bases.len(), 2);
            }
            other => panic!("unexpected decoded command variant: {other:?}"),
        }
    }

    #[test]
    fn cas_wire_response_serializes_and_deserializes_stably() {
        let response = CasWireResponse::PruneReport { removed_candidates: 7 };

        let json = serde_json::to_string(&response).expect("serialize response");
        let decoded: CasWireResponse = serde_json::from_str(&json).expect("deserialize response");

        match decoded {
            CasWireResponse::PruneReport { removed_candidates } => {
                assert_eq!(removed_candidates, 7);
            }
            other => panic!("unexpected decoded response variant: {other:?}"),
        }
    }
}
