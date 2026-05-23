use super::*;

/// Storage-actor workflow: actor-mediated put/get/constraint operations.
///
/// Steps:
/// 1. Open filesystem backend.
/// 2. Spawn storage actor.
/// 3. Put base and target through actor RPC.
/// 4. Set explicit target constraint through actor RPC.
/// 5. Read effective bases via actor API.
/// 6. Retrieve target payload via actor API.
/// 7. Assert payload integrity and base inclusion.
///
/// Edge cases covered:
/// - actor command routing for constraint operations.
#[tokio::test]
async fn storage_actor_roundtrip_and_constraint_bases() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas = Arc::new(FileSystemCas::open_for_tests(root.path()).await.expect("open"));
        let storage = spawn_storage_actor(cas.clone()).await.expect("spawn storage actor");

        let base_payload = synthetic_payload(111, 8192);
        let target_payload = synthetic_payload(112, 8192);
        let base = storage.put(base_payload).await.expect("actor put base");
        let target = storage.put(target_payload.clone()).await.expect("actor put target");

        storage
            .set_constraint(Constraint {
                target_hash: target,
                potential_bases: BTreeSet::from([base]),
            })
            .await
            .expect("actor set constraint");

        let bases = storage.constraint_bases(target).await.expect("actor constraint bases");
        assert!(bases.contains(&base), "actor bases should include explicit base");

        let restored = storage.get(target).await.expect("actor get target");
        assert_eq!(restored, target_payload, "actor get payload mismatch");
    })
    .await;
}

/// Wire-command workflow: full lifecycle over `CasWireCommand`.
///
/// Steps:
/// 1. Open filesystem backend and spawn node actor.
/// 2. Put base and target using raw wire `Put` commands.
/// 3. Set explicit constraint with wire `SetConstraint`.
/// 4. Query bases via `ConstraintBases`.
/// 5. Run `OptimizeOnce` and `PruneConstraints`.
/// 6. Read target via wire `Get` and validate exact bytes.
/// 7. Delete target via wire `Delete`.
/// 8. Confirm subsequent wire `Get` fails for deleted hash.
///
/// Edge cases covered:
/// - wire serialization flow for all primary command variants.
#[tokio::test]
async fn node_actor_wire_full_command_lifecycle() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas =
            Arc::new(FileSystemCas::open_with_alpha_for_tests(root.path(), 0).await.expect("open"));
        let node = spawn_cas_node_actor(cas).await.expect("spawn node actor");

        let base_payload = synthetic_payload(121, 4096);
        let target_payload = synthetic_payload(122, 4096);

        let base_hash = match node
            .execute(CasWireCommand::Put { data: base_payload.to_vec() })
            .await
            .expect("wire put base")
        {
            CasWireResponse::Hash { hash } => hash,
            other => panic!("unexpected base put response: {other:?}"),
        };

        let target_hash = match node
            .execute(CasWireCommand::Put { data: target_payload.to_vec() })
            .await
            .expect("wire put target")
        {
            CasWireResponse::Hash { hash } => hash,
            other => panic!("unexpected target put response: {other:?}"),
        };

        let set = node
            .execute(CasWireCommand::SetConstraint {
                target_hash: target_hash.clone(),
                potential_bases: vec![base_hash.clone()],
            })
            .await
            .expect("wire set constraint");
        assert!(matches!(set, CasWireResponse::Ack));

        let bases = node
            .execute(CasWireCommand::ConstraintBases { target_hash: target_hash.clone() })
            .await
            .expect("wire constraint bases");
        match bases {
            CasWireResponse::Bases { hashes } => {
                assert!(hashes.contains(&base_hash), "wire bases should include explicit base");
            }
            other => panic!("unexpected bases response: {other:?}"),
        }

        let optimize = node.execute(CasWireCommand::OptimizeOnce).await.expect("wire optimize");
        assert!(matches!(optimize, CasWireResponse::OptimizeReport { .. }));

        let prune = node.execute(CasWireCommand::PruneConstraints).await.expect("wire prune");
        assert!(matches!(prune, CasWireResponse::PruneReport { .. }));

        let get = node
            .execute(CasWireCommand::Get { hash: target_hash.clone() })
            .await
            .expect("wire get");
        match get {
            CasWireResponse::Bytes { data } => assert_eq!(data, target_payload.to_vec()),
            other => panic!("unexpected get response: {other:?}"),
        }

        let delete = node
            .execute(CasWireCommand::Delete { hash: target_hash.clone() })
            .await
            .expect("wire delete");
        assert!(matches!(delete, CasWireResponse::Ack));

        let deleted_get = node
            .execute(CasWireCommand::Get { hash: target_hash })
            .await
            .expect_err("wire get for deleted hash should fail");
        assert!(matches!(deleted_get, CasError::NotFound(_)));
    })
    .await;
}

/// Optimizer-actor workflow: optimize+prune under constrained targets.
///
/// Steps:
/// 1. Open filesystem backend.
/// 2. Store one base and multiple constrained targets.
/// 3. Spawn optimizer actor.
/// 4. Invoke optimize and prune via actor API.
/// 5. Assert report bounds.
/// 6. Assert sample targets remain reconstructable.
///
/// Edge cases covered:
/// - actor maintenance workflows with explicit constraints.
#[tokio::test]
async fn optimizer_actor_runs_maintenance_with_constraints() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas =
            Arc::new(FileSystemCas::open_with_alpha_for_tests(root.path(), 0).await.expect("open"));
        let base = cas.put(synthetic_payload(131, 32 * 1024)).await.expect("put base");

        let mut targets = Vec::new();
        for idx in 0..12usize {
            let seed = 132 + u8::try_from(idx % 7).expect("idx % 7 is always less than 7");
            let target_payload = synthetic_payload(seed, 32 * 1024);
            let target = cas.put(target_payload).await.expect("put target");
            cas.set_constraint(Constraint {
                target_hash: target,
                potential_bases: BTreeSet::from([base]),
            })
            .await
            .expect("set constraint");
            targets.push(target);
        }

        let optimizer = spawn_optimizer_actor(cas.clone()).await.expect("spawn optimizer");
        let optimize = optimizer.optimize_once().await.expect("optimize once");
        let prune = optimizer.prune_constraints().await.expect("prune constraints");

        assert!(optimize.rewritten_objects <= targets.len());
        assert!(prune.removed_candidates <= targets.len());

        for hash in targets.iter().step_by(4) {
            let restored = cas.get(*hash).await.expect("sample reconstruct");
            assert_eq!(restored.len(), 32 * 1024);
        }
    })
    .await;
}

/// Index-actor workflow: flush and repair through actor-level APIs.
///
/// Steps:
/// 1. Open filesystem backend.
/// 2. Spawn index actor and storage actor with index dependency.
/// 3. Write payload via storage actor.
/// 4. Force durable flush via index actor.
/// 5. Assert `index.redb` exists and is non-empty.
/// 6. Trigger index repair via index actor.
/// 7. Assert repair report indicates object rows were rebuilt/scanned.
///
/// Edge cases covered:
/// - explicit persistence coordination between storage/index actors.
#[tokio::test]
async fn index_actor_flush_and_repair_workflow() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas = Arc::new(FileSystemCas::open_for_tests(root.path()).await.expect("open"));

        let index = spawn_index_actor(cas.clone()).await.expect("spawn index actor");
        let storage =
            spawn_storage_actor_with_dependencies(cas, None, Some(index.actor_ref().clone()))
                .await
                .expect("spawn storage actor with index dependency");

        let hash = storage.put(synthetic_payload(141, 9000)).await.expect("storage actor put");
        let bytes = storage.get(hash).await.expect("storage actor get");
        assert_eq!(bytes.len(), 9000);

        index.flush_snapshot().await.expect("flush snapshot");
        let db_path = root.path().join("index.redb");
        let meta = std::fs::metadata(&db_path).expect("index.redb metadata");
        assert!(meta.len() > 0, "expected non-empty index.redb");

        let repair = index.repair_index().await.expect("repair index");
        assert!(repair.object_rows_rebuilt >= 1);
    })
    .await;
}

/// Node migration workflow: migrate index schema and preserve payload access.
///
/// Steps:
/// 1. Open filesystem backend and spawn node actor.
/// 2. Store object through node actor API.
/// 3. Issue migrate-index command (`target_version = 1`).
/// 4. Assert migration acknowledgment.
/// 5. Re-read object and verify exact bytes.
/// 6. Issue repair command post-migration.
/// 7. Assert repair report remains sane.
///
/// Edge cases covered:
/// - migration + read-path continuity through node surface.
#[tokio::test]
async fn node_migrate_index_and_payload_survives() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas = Arc::new(FileSystemCas::open_for_tests(root.path()).await.expect("open"));
        let node = spawn_cas_node_actor(cas).await.expect("spawn node actor");

        let payload = synthetic_payload(151, 12 * 1024);
        let hash = node.put(payload.clone()).await.expect("node put");

        let migrated = node
            .execute(CasWireCommand::MigrateIndex { target_version: 1 })
            .await
            .expect("migrate index");
        assert!(matches!(migrated, CasWireResponse::Ack));

        let restored = node.get(hash).await.expect("node get after migration");
        assert_eq!(restored, payload, "payload should survive migration");

        let repair = node.execute(CasWireCommand::RepairIndex).await.expect("repair index");
        assert!(matches!(repair, CasWireResponse::RepairIndexReport { .. }));
    })
    .await;
}

/// Wire-input workflow: malformed hash strings are rejected by node command API.
///
/// Steps:
/// 1. Open filesystem backend and spawn node actor.
/// 2. Execute wire `Get` with malformed hash text.
/// 3. Execute wire `ConstraintBases` with malformed hash text.
/// 4. Assert both calls fail with input/parse-class errors.
///
/// Edge cases covered:
/// - hash parser failure through wire transport paths.
#[tokio::test]
async fn node_wire_commands_reject_malformed_hash_strings() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas = Arc::new(FileSystemCas::open_for_tests(root.path()).await.expect("open"));
        let node = spawn_cas_node_actor(cas).await.expect("spawn node actor");

        let malformed_get = node
            .execute(CasWireCommand::Get { hash: "totally-not-a-hash".to_string() })
            .await
            .expect_err("malformed get hash should fail");
        assert!(
            matches!(malformed_get, CasError::InvalidInput(_) | CasError::HashParse(_)),
            "unexpected error class for malformed get hash: {malformed_get}"
        );

        let malformed_bases = node
            .execute(CasWireCommand::ConstraintBases {
                target_hash: "still-not-a-hash".to_string(),
            })
            .await
            .expect_err("malformed constraint_bases hash should fail");
        assert!(
            matches!(malformed_bases, CasError::InvalidInput(_) | CasError::HashParse(_)),
            "unexpected error class for malformed bases hash: {malformed_bases}"
        );
    })
    .await;
}

/// Control-plane workflow: node-level flush and compression toggle commands acknowledge.
///
/// Steps:
/// 1. Open backend and spawn node actor.
/// 2. Store one payload.
/// 3. Execute `SetMaxCompressionMode(true)`.
/// 4. Execute `FlushIndex`.
/// 5. Execute `SetMaxCompressionMode(false)`.
/// 6. Assert all command responses are `Ack`.
/// 7. Verify stored payload remains readable.
///
/// Edge cases covered:
/// - non-data control commands interleaved with data-path operations.
#[tokio::test]
async fn node_control_plane_commands_ack_and_preserve_data_path() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas = Arc::new(FileSystemCas::open_for_tests(root.path()).await.expect("open"));
        let node = spawn_cas_node_actor(cas).await.expect("spawn node actor");

        let payload = synthetic_payload(207, 3072);
        let hash = node.put(payload.clone()).await.expect("put payload");

        let enable = node
            .execute(CasWireCommand::SetMaxCompressionMode { enabled: true })
            .await
            .expect("enable compression mode");
        assert!(matches!(enable, CasWireResponse::Ack));

        let flush = node.execute(CasWireCommand::FlushIndex).await.expect("flush index");
        assert!(matches!(flush, CasWireResponse::Ack));

        let disable = node
            .execute(CasWireCommand::SetMaxCompressionMode { enabled: false })
            .await
            .expect("disable compression mode");
        assert!(matches!(disable, CasWireResponse::Ack));

        let restored = node.get(hash).await.expect("get payload after control commands");
        assert_eq!(restored, payload);
    })
    .await;
}

/// Storage-actor error workflow: querying explicit bases for missing target fails.
///
/// Steps:
/// 1. Open backend and spawn storage actor.
/// 2. Build a missing target hash.
/// 3. Call `constraint_bases` through actor client.
/// 4. Assert call fails with `NotFound`.
///
/// Edge cases covered:
/// - storage actor path for missing-target constraint queries.
#[tokio::test]
async fn storage_actor_constraint_bases_for_missing_target_returns_not_found() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas = Arc::new(FileSystemCas::open_for_tests(root.path()).await.expect("open"));
        let storage = spawn_storage_actor(cas).await.expect("spawn storage actor");

        let missing = Hash::from_content(b"missing-storage-target");
        let error = storage
            .constraint_bases(missing)
            .await
            .expect_err("constraint_bases on missing target must fail");
        assert!(matches!(error, CasError::NotFound(_)));
    })
    .await;
}
