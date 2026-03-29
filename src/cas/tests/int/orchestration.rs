//! Integration tests for CAS orchestration actors and wire commands.
//!
//! Verifies actor composition, message routing, and node-level behavior match
//! direct API guarantees.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use mediapm_cas::{
    CasApi, CasWireCommand, CasWireResponse, Constraint, FileSystemCas, Hash, spawn_cas_node_actor,
    spawn_index_actor, spawn_optimizer_actor, spawn_storage_actor,
    spawn_storage_actor_with_dependencies,
};
use tempfile::tempdir;

#[tokio::test]
async fn storage_actor_put_get_roundtrip() {
    let dir = tempdir().expect("tempdir");
    let cas = Arc::new(FileSystemCas::open_for_tests(dir.path()).await.expect("open cas"));

    let storage = spawn_storage_actor(cas.clone()).await.expect("spawn storage actor");

    let payload = Bytes::from_static(b"actor-storage-payload");
    let hash = storage.put(payload.clone()).await.expect("actor put");
    let restored = storage.get(hash).await.expect("actor get");

    assert_eq!(restored, payload);
}

#[tokio::test]
async fn optimizer_actor_runs_optimize_and_prune() {
    let dir = tempdir().expect("tempdir");
    let cas =
        Arc::new(FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas"));

    let base = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
        .await
        .expect("put base");
    let target = cas
        .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
        .await
        .expect("put target");

    cas.set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::from([base]) })
        .await
        .expect("set constraint");

    let optimizer = spawn_optimizer_actor(cas.clone()).await.expect("spawn optimizer actor");

    let optimize = optimizer.optimize_once().await.expect("optimize once");
    let prune = optimizer.prune_constraints().await.expect("prune constraints");

    assert!(optimize.rewritten_objects <= 1);
    assert!(prune.removed_candidates <= 1);
}

#[tokio::test]
async fn storage_actor_with_index_dependency_flushes_redb_tables() {
    let dir = tempdir().expect("tempdir");
    let cas = Arc::new(FileSystemCas::open_for_tests(dir.path()).await.expect("open cas"));

    let index = spawn_index_actor(cas.clone()).await.expect("spawn index actor");
    let storage =
        spawn_storage_actor_with_dependencies(cas.clone(), None, Some(index.actor_ref().clone()))
            .await
            .expect("spawn storage actor with index");

    let hash = storage.put(Bytes::from_static(b"actor-index-flush")).await.expect("actor put");
    let restored = storage.get(hash).await.expect("actor get");
    assert_eq!(restored, Bytes::from_static(b"actor-index-flush"));

    // Also force an explicit flush request through index actor RPC.
    index.flush_snapshot().await.expect("index flush snapshot");

    let db_path = dir.path().join("index.redb");
    let meta = std::fs::metadata(&db_path).expect("index.redb metadata");
    assert!(meta.len() > 0, "expected non-empty redb index at {}", db_path.display());
}

#[tokio::test]
async fn index_actor_and_node_command_can_repair_index() {
    let dir = tempdir().expect("tempdir");
    let cas = Arc::new(FileSystemCas::open_for_tests(dir.path()).await.expect("open cas"));
    let node = spawn_cas_node_actor(cas.clone()).await.expect("spawn node actor");

    let _ = node.put(Bytes::from_static(b"repair-through-node")).await.expect("put object");

    let repair_response = node.execute(CasWireCommand::RepairIndex).await.expect("repair index");
    match repair_response {
        CasWireResponse::RepairIndexReport { object_rows_rebuilt, .. } => {
            assert!(object_rows_rebuilt >= 1);
        }
        other => panic!("unexpected repair response: {other:?}"),
    }
}

#[tokio::test]
async fn index_actor_and_node_command_can_migrate_index() {
    let dir = tempdir().expect("tempdir");
    let cas = Arc::new(FileSystemCas::open_for_tests(dir.path()).await.expect("open cas"));
    let node = spawn_cas_node_actor(cas.clone()).await.expect("spawn node actor");

    let hash = node.put(Bytes::from_static(b"migrate-through-node")).await.expect("put object");

    let migrate_response = node
        .execute(CasWireCommand::MigrateIndex { target_version: 1 })
        .await
        .expect("migrate index command");
    assert!(matches!(migrate_response, CasWireResponse::Ack));

    let restored = node.get(hash).await.expect("get object after migration");
    assert_eq!(restored, Bytes::from_static(b"migrate-through-node"));
}

#[tokio::test]
async fn storage_actor_handles_constraints_via_actor_messages() {
    let dir = tempdir().expect("tempdir");
    let cas = Arc::new(FileSystemCas::open_for_tests(dir.path()).await.expect("open cas"));
    let storage = spawn_storage_actor(cas.clone()).await.expect("spawn storage actor");

    let base = storage.put(Bytes::from_static(b"actor-base")).await.expect("put base");
    let target = storage.put(Bytes::from_static(b"actor-target")).await.expect("put target");

    storage
        .set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::from([base]) })
        .await
        .expect("set constraint through storage actor");

    let bases = storage.constraint_bases(target).await.expect("read effective bases");
    assert!(bases.contains(&base));
}

#[tokio::test]
async fn cas_node_actor_runs_full_command_workflow() {
    let dir = tempdir().expect("tempdir");
    let cas =
        Arc::new(FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas"));
    let node = spawn_cas_node_actor(cas).await.expect("spawn node actor");

    let base = node.put(Bytes::from_static(b"node-base")).await.expect("node put base");
    let target = node.put(Bytes::from_static(b"node-target")).await.expect("node put target");

    node.set_constraint(Constraint {
        target_hash: target,
        potential_bases: BTreeSet::from([base]),
    })
    .await
    .expect("node set constraint");

    let bases_response = node
        .execute(CasWireCommand::ConstraintBases { target_hash: target.to_string() })
        .await
        .expect("constraint bases command");
    match bases_response {
        CasWireResponse::Bases { hashes } => {
            assert!(hashes.iter().any(|hash| hash == &base.to_string()));
        }
        other => panic!("unexpected bases response: {other:?}"),
    }

    let optimize_response =
        node.execute(CasWireCommand::OptimizeOnce).await.expect("optimize command");
    match optimize_response {
        CasWireResponse::OptimizeReport { rewritten_objects } => {
            assert!(rewritten_objects <= 1);
        }
        other => panic!("unexpected optimize response: {other:?}"),
    }

    let restored = node.get(target).await.expect("node get target");
    assert_eq!(restored, Bytes::from_static(b"node-target"));

    node.delete(target).await.expect("node delete target");
    assert!(node.get(target).await.is_err());

    let flush_response =
        node.execute(CasWireCommand::FlushIndex).await.expect("flush index command");
    assert!(matches!(flush_response, CasWireResponse::Ack));
}

#[tokio::test]
async fn cas_node_actor_delete_preserves_delta_descendants() {
    let dir = tempdir().expect("tempdir");
    let cas =
        Arc::new(FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas"));
    let node = spawn_cas_node_actor(cas.clone()).await.expect("spawn node actor");

    let base_payload = vec![b'x'; 8 * 1024];
    let mut target_payload = base_payload.clone();
    target_payload[512] = b'y';

    let base_hash = node.put(Bytes::from(base_payload)).await.expect("node put base");
    let target_hash = Hash::from_content(&target_payload);
    let stored_target =
        node.put(Bytes::from(target_payload.clone())).await.expect("node put target");
    assert_eq!(stored_target, target_hash);

    node.set_constraint(Constraint { target_hash, potential_bases: BTreeSet::from([base_hash]) })
        .await
        .expect("node set target constraint");
    let _ =
        node.execute(CasWireCommand::OptimizeOnce).await.expect("optimize after set_constraint");

    node.delete(base_hash).await.expect("node delete base");

    assert!(node.get(base_hash).await.is_err());
    assert_eq!(
        node.get(target_hash).await.expect("target must remain reconstructible"),
        Bytes::from(target_payload)
    );
}
