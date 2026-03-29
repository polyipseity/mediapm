//! Phase 1 high-performance unified-delta CAS.
//!
//! This crate implements the core behaviors required by the Phase 1 plan:
//!
//! - Algorithm-tagged content identity (`HashAlgorithm` + 32-byte digest).
//! - Deterministic fan-out object layout:
//!   `{root}/{version}/{algorithm_name}/{h[0:2]}/{h[2:4]}/{h[4..]}`.
//! - Full objects stored as raw data-only files (no headers).
//! - Delta objects stored as `.diff` files with explicit reconstruction metadata.
//! - Persistent constraint/index state with invariant checks.
//! - Incremental optimizer pass that can rewrite stored bases.
//! - Implicit empty-base fallback (empty-only constraints are intentionally not persisted).
//! - Async API contracts suitable for use by higher orchestration layers.

mod api;
mod codec;
mod error;
mod hash;
mod index;
mod orchestration;
mod storage;

pub use api::{
    CasApi, CasByteReader, CasByteStream, CasExistenceBitmap, CasMaintenanceApi, Constraint,
    ConstraintPatch, IndexRepairConstraintSource, IndexRepairReport, ObjectInfo, OptimizeOptions,
    OptimizePriority, OptimizeReport, PruneReport,
};
pub use error::{CasError, HashParseError};
pub use hash::{Hash, HashAlgorithm, empty_content_hash};
pub use orchestration::{
    CasNodeActorClient, CasNodeActorMessage, CasWireCommand, CasWireResponse, IndexActorClient,
    IndexActorMessage, OptimizerActorClient, OptimizerActorMessage, StorageActorArgs,
    StorageActorClient, StorageActorMessage, spawn_cas_node_actor, spawn_cas_node_actor_from_refs,
    spawn_index_actor, spawn_optimizer_actor, spawn_storage_actor,
    spawn_storage_actor_with_dependencies,
};
pub use storage::{
    CasBackendConfig, CasConfig, CasLocatorParseOptions, CasTopologyConstraint,
    CasTopologyEncoding, CasTopologyNode, CasTopologySnapshot, ConfiguredCas, FileSystemCas,
    FileSystemMetrics, FileSystemRecoveryOptions, InMemoryCas, IndexRecoveryMode,
    render_topology_mermaid, render_topology_mermaid_neighborhood, topology_neighborhood_snapshot,
};

pub(crate) use codec::{DeltaPatch, StoredObject};
pub(crate) use index::{
    BatchOperation, CasIndexDb, IndexState, ObjectEncoding, ObjectMeta, ensure_empty_record,
    recalculate_depths,
};

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::{
        CasApi, CasNodeActorClient, CasWireCommand, CasWireResponse, FileSystemCas, Hash,
        InMemoryCas,
    };

    #[test]
    fn public_exports_are_constructible() {
        let _hash = Hash::from_content(b"export-smoke");
        let _command = CasWireCommand::OptimizeOnce;
        let _response = CasWireResponse::Ack;

        let _in_memory = InMemoryCas::new();

        fn _accept_client(_client: Option<CasNodeActorClient>) {}
        _accept_client(None);
    }

    #[tokio::test]
    async fn exported_filesystem_cas_can_roundtrip() {
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open(dir.path()).await.expect("open cas");

        let payload = Bytes::from_static(b"lib-export-roundtrip");
        let hash = cas.put(payload.clone()).await.expect("put payload");
        let restored = cas.get(hash).await.expect("get payload");

        assert_eq!(restored, payload);
    }
}
