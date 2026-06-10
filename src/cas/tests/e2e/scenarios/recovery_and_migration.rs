use super::*;

/// Recover-mode workflow: missing primary index rebuilds from objects/backups.
///
/// Edge cases covered:
/// - missing primary index recovery using backup/object-store reconstruction.
#[tokio::test]
async fn recover_mode_rebuilds_missing_primary_index() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let base_payload = synthetic_payload(161, 32 * 1024);
        let target_payload = mutated_payload(base_payload.as_ref(), 1234, b'K');

        let (base, target) = {
            let cas = FileSystemCas::open_with_alpha_and_recovery_for_tests(
                root.path(),
                0,
                FileSystemRecoveryOptions {
                    mode: IndexRecoveryMode::Recover,
                    max_backup_snapshots: 4,
                    wait_for_lock: false,
                },
            )
            .await
            .expect("open with recover mode");

            let base = cas.put(base_payload.clone()).await.expect("put base");
            let target = cas.put(target_payload.clone()).await.expect("put target");
            cas.set_constraint(Constraint {
                target_hash: target,
                potential_bases: BTreeSet::from([base]),
            })
            .await
            .expect("set constraint");
            let _ = cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");
            cas.flush_index_snapshot().await.expect("flush backup snapshot with constraint data");
            (base, target)
        };

        std::fs::remove_file(root.path().join("index.redb")).expect("remove primary index");

        let reopened = FileSystemCas::open_with_alpha_and_recovery_for_tests(
            root.path(),
            0,
            FileSystemRecoveryOptions {
                mode: IndexRecoveryMode::Recover,
                max_backup_snapshots: 4,
                wait_for_lock: false,
            },
        )
        .await
        .expect("reopen recover mode");

        let restored = reopened.get(target).await.expect("get target after recovery");
        assert_eq!(restored, target_payload);

        let row = reopened
            .get_constraint(target)
            .await
            .expect("get constraint")
            .expect("constraint row should be restored");
        assert_eq!(row.potential_bases, BTreeSet::from([base]));

        assert!(reopened.exists(target).await.expect("exists"));
        let info = reopened.info(target).await.expect("info");
        assert_eq!(info.content_len, target_payload.len() as u64);
    })
    .await;
}

/// Strict-mode workflow: missing primary index is rejected when objects exist.
///
/// Edge cases covered:
/// - strict-mode safety policy for missing durable metadata.
#[tokio::test]
async fn strict_mode_rejects_missing_primary_index_when_objects_exist() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        {
            let cas = FileSystemCas::open_for_tests(root.path()).await.expect("open");
            let _ = cas.put(synthetic_payload(171, 10 * 1024)).await.expect("put object");
        }

        std::fs::remove_file(root.path().join("index.redb")).expect("remove index.redb");

        let Err(error) = FileSystemCas::open_with_alpha_and_recovery_for_tests(
            root.path(),
            4,
            FileSystemRecoveryOptions {
                mode: IndexRecoveryMode::Strict,
                max_backup_snapshots: 4,
                wait_for_lock: false,
            },
        )
        .await
        else {
            panic!("strict mode should reject missing primary index");
        };

        assert!(
            error.to_string().contains("reopen with recover mode or run repair_index"),
            "strict-mode error should guide operator"
        );
    })
    .await;
}

/// Recover-mode workflow: corrupted primary index is rebuilt from object store.
///
/// Edge cases covered:
/// - durable index corruption path and automatic rebuild behavior.
#[tokio::test]
async fn corrupt_primary_index_is_rebuilt_from_object_store() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let payload = synthetic_payload(181, 15 * 1024);
        let hash = {
            let cas = FileSystemCas::open_for_tests(root.path()).await.expect("open");
            cas.put(payload.clone()).await.expect("put payload")
        };

        std::fs::write(root.path().join("index.redb"), b"definitely-not-redb")
            .expect("corrupt index file");

        let reopened = FileSystemCas::open_for_tests(root.path()).await.expect("reopen");
        let restored = reopened.get(hash).await.expect("get after recovery");
        assert_eq!(restored, payload);
        assert!(reopened.exists(hash).await.expect("exists after recovery"));
    })
    .await;
}

/// Explicit-repair workflow: removed redb row is auto-healed by `exists()`.
///
/// Edge cases covered:
/// - metadata/data divergence auto-recovery by `exists()` filesystem fallback.
#[tokio::test]
async fn explicit_repair_restores_removed_primary_rows() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let target_payload = synthetic_payload(191, 7000);

        let target = {
            let cas = FileSystemCas::open_for_tests(root.path()).await.expect("open");
            let _control = cas.put(synthetic_payload(192, 7000)).await.expect("put control");
            let target = cas.put(target_payload.clone()).await.expect("put target");
            cas.flush_index_snapshot().await.expect("flush snapshot");
            target
        };

        let db_path = root.path().join("index.redb");
        let db = open_redb_after_shutdown(&db_path).await;
        let write = db.begin_write().expect("begin write");
        {
            let mut table = write.open_table(PRIMARY_INDEX).expect("open primary index");
            let key = target.storage_bytes();
            table.remove(key.as_slice()).expect("remove target row");
        }
        write.commit().expect("commit row deletion");
        drop(db);

        let cas = FileSystemCas::open_for_tests(root.path()).await.expect("reopen");
        // `exists()` auto-heals orphaned files from disk, so it returns true
        // even before an explicit `repair_index` call.
        assert!(cas.exists(target).await.expect("exists auto-heals from disk"));
        assert_eq!(cas.get(target).await.expect("get via object file"), target_payload);

        // Explicit repair is idempotent — rows are already healed.
        let _report = cas.repair_index().await.expect("repair index");
        let info = cas.info(target).await.expect("info after repair");

        assert!(cas.exists(target).await.expect("exists after repair"));
        assert_eq!(info.content_len, 7000);
    })
    .await;
}

/// Retention+migration workflow: backup cap and migration idempotency.
///
/// Edge cases covered:
/// - backup retention limits;
/// - repeated migration against same target version.
#[tokio::test]
async fn backup_retention_and_migration_roundtrip() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");

        let sample_hash = {
            let cas = FileSystemCas::open_with_alpha_and_recovery_for_tests(
                root.path(),
                4,
                FileSystemRecoveryOptions {
                    mode: IndexRecoveryMode::Recover,
                    max_backup_snapshots: 2,
                    wait_for_lock: false,
                },
            )
            .await
            .expect("open with backup retention");

            let mut sample_hash = None;
            for idx in 0..6u8 {
                let hash = cas
                    .put(synthetic_payload(201 + idx, 4096 + usize::from(idx)))
                    .await
                    .expect("put payload");
                cas.flush_index_snapshot()
                    .await
                    .expect("flush backup snapshot for retention rotation");
                if idx == 3 {
                    sample_hash = Some(hash);
                }
            }
            sample_hash.expect("sample hash")
        };

        let backup_count = count_backup_snapshots(root.path());
        assert_eq!(backup_count, 2, "backup retention must keep newest two snapshots");

        let reopened = FileSystemCas::open_for_tests(root.path()).await.expect("reopen");
        reopened.migrate_index_to_version(2).await.expect("migrate to v2");
        reopened
            .migrate_index_to_version(2)
            .await
            .expect("repeat migrate to v2 should be idempotent");

        let restored = reopened.get(sample_hash).await.expect("get sample after migration");
        assert!(restored.len() >= 4096, "sample payload should remain accessible after migration");
    })
    .await;
}

/// Migration validation workflow: unsupported target version is rejected.
///
/// Edge cases covered:
/// - migration guardrails for unsupported schema markers.
#[tokio::test]
async fn unsupported_migration_version_is_rejected_without_data_loss() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_for_tests(root.path()).await.expect("open");
        let payload = synthetic_payload(221, 5000);
        let hash = cas.put(payload.clone()).await.expect("put payload");

        let migration_error = cas
            .migrate_index_to_version(99)
            .await
            .expect_err("unsupported migration target should fail");
        assert!(
            matches!(
                migration_error,
                CasError::InvalidInput(_)
                    | CasError::InvalidConstraint(_)
                    | CasError::Protocol(_)
                    | CasError::CorruptIndex(_)
            ) || migration_error.to_string().contains("unsupported index schema marker"),
            "unexpected migration error class: {migration_error}"
        );

        let restored = cas.get(hash).await.expect("get after failed migration");
        assert_eq!(restored, payload);
    })
    .await;
}

/// Repair idempotency workflow: repeated repair calls remain safe and stable.
///
/// Edge cases covered:
/// - repair-idempotency and repeated maintenance safety.
#[tokio::test]
async fn repair_index_is_idempotent_across_repeated_calls() {
    run_with_15s_timeout(async {
        let root = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_for_tests(root.path()).await.expect("open");
        let payload = synthetic_payload(222, 4096);
        let hash = cas.put(payload.clone()).await.expect("put payload");

        let first = cas.repair_index().await.expect("first repair");
        let second = cas.repair_index().await.expect("second repair");
        assert!(first.object_rows_rebuilt >= 1);
        assert!(second.scanned_object_files >= 1);

        let restored = cas.get(hash).await.expect("get after repeated repairs");
        assert_eq!(restored, payload);
    })
    .await;
}
