//! End-to-end recovery and validation-path tests.
//!
//! Focuses on missing/corrupt index handling, explicit repair flows, backup
//! retention, and rejection behavior for invalid constraints.

use bytes::Bytes;
use mediapm_cas::{
    CasApi, CasMaintenanceApi, Constraint, FileSystemCas, FileSystemRecoveryOptions, Hash,
    IndexRecoveryMode, OptimizeOptions,
};
use redb::{Database, TableDefinition};
use std::collections::BTreeSet;
use tempfile::tempdir;

use super::run_with_15s_timeout;

/// Redb primary-index table used for direct corruption/row-removal setup.
const PRIMARY_INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("primary_index");

/// Opens the redb database with retry to avoid shutdown timing races.
async fn open_redb_after_shutdown(db_path: &std::path::Path) -> Database {
    for _ in 0..30 {
        if let Ok(handle) = Database::open(db_path) {
            return handle;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    Database::open(db_path).expect("open redb index after filesystem actor shutdown")
}

#[tokio::test]
/// Rejects constraints that reference non-existent candidate hashes.
async fn e2e_set_constraint_rejects_missing_candidate_entries() {
    run_with_15s_timeout(async {
        // Arrange
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
        let target = cas.put(Bytes::from_static(b"recovery-target")).await.expect("put target");
        let missing = Hash::from_content(b"missing-candidate");

        let result = cas
            .set_constraint(Constraint {
                target_hash: target,
                potential_bases: BTreeSet::from([missing]),
            })
            .await;

        // Assert
        assert!(result.is_err(), "missing-base constraints must be rejected");
    })
    .await;
}

#[tokio::test]
/// Rebuilds durable index from objects/backups when primary index file is removed.
async fn e2e_missing_primary_index_is_rebuilt_from_objects_and_backups() {
    run_with_15s_timeout(async {
        let dir = tempdir().expect("tempdir");
        let base_payload = Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB");
        let target_payload =
            Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC");

        let (base, target) = {
            let cas =
                FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas");
            let base = cas.put(base_payload.clone()).await.expect("put base");
            let target = cas.put(target_payload.clone()).await.expect("put target");
            cas.set_constraint(Constraint {
                target_hash: target,
                potential_bases: BTreeSet::from([base]),
            })
            .await
            .expect("set constraint");
            let _ = cas.optimize_once(OptimizeOptions::default()).await.expect("optimize once");
            (base, target)
        };

        std::fs::remove_file(dir.path().join("index.redb")).expect("remove primary index");

        let reopened = FileSystemCas::open_for_tests(dir.path()).await.expect("reopen cas");
        let restored = reopened.get(target).await.expect("get target after rebuild");
        let info = reopened.info(target).await.expect("info after rebuild");
        let constraint = reopened
            .get_constraint(target)
            .await
            .expect("get restored constraint")
            .expect("constraint row should be restored from backup snapshots");

        assert_eq!(restored, target_payload);
        assert_eq!(constraint.potential_bases, BTreeSet::from([base]));
        assert!(reopened.exists(target).await.expect("exists after rebuild"));
        assert_eq!(info.content_len, target_payload.len() as u64);
    })
    .await;
}

#[tokio::test]
/// In strict mode, startup fails when index is missing but objects exist.
async fn e2e_strict_mode_rejects_missing_primary_index_when_objects_exist() {
    run_with_15s_timeout(async {
        let dir = tempdir().expect("tempdir");
        {
            let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
            let _ = cas.put(Bytes::from_static(b"strict-mode-object")).await.expect("put object");
        }

        std::fs::remove_file(dir.path().join("index.redb")).expect("remove primary index");

        let Err(error) = FileSystemCas::open_with_alpha_and_recovery_for_tests(
            dir.path(),
            4,
            FileSystemRecoveryOptions {
                mode: IndexRecoveryMode::Strict,
                max_backup_snapshots: 4,
                backup_snapshot_interval_ops: 1,
            },
        )
        .await
        else {
            panic!("strict mode should refuse missing durable index");
        };

        assert!(error.to_string().contains("reopen with recover mode or run repair_index"));
    })
    .await;
}

#[tokio::test]
/// Rebuilds state when primary index file exists but is corrupted.
async fn e2e_corrupt_primary_index_is_rebuilt_from_object_store() {
    run_with_15s_timeout(async {
        let dir = tempdir().expect("tempdir");
        let payload = Bytes::from_static(b"corrupt-primary-rebuild");
        let hash = {
            let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
            cas.put(payload.clone()).await.expect("put object")
        };

        std::fs::write(dir.path().join("index.redb"), b"not-a-redb-file")
            .expect("corrupt primary index file");

        let reopened = FileSystemCas::open_for_tests(dir.path()).await.expect("reopen cas");
        assert_eq!(reopened.get(hash).await.expect("get after corrupt-index recovery"), payload);
        assert!(reopened.exists(hash).await.expect("exists after corrupt-index recovery"));
    })
    .await;
}

#[tokio::test]
/// Repair API repopulates rows that were removed directly from redb.
async fn e2e_explicit_repair_restores_rows_missing_from_primary_index() {
    run_with_15s_timeout(async {
        let dir = tempdir().expect("tempdir");
        let payload = Bytes::from_static(b"repair-missing-row");
        let hash = {
            let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
            let _other =
                cas.put(Bytes::from_static(b"repair-keep-row")).await.expect("put control object");
            let hash = cas.put(payload.clone()).await.expect("put object");
            cas.flush_index_snapshot().await.expect("flush index snapshot");
            hash
        };

        let db_path = dir.path().join("index.redb");
        let db = open_redb_after_shutdown(&db_path).await;
        let write = db.begin_write().expect("begin write txn");
        {
            let mut primary = write.open_table(PRIMARY_INDEX).expect("open primary table");
            let key = hash.storage_bytes();
            primary.remove(key.as_slice()).expect("remove primary row");
        }
        write.commit().expect("commit removed primary row");
        drop(db);

        let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("reopen cas");
        assert!(!cas.exists(hash).await.expect("exists should reflect missing index row"));
        assert_eq!(cas.get(hash).await.expect("get should still read object file"), payload);

        let report = cas.repair_index().await.expect("repair index");
        let info = cas.info(hash).await.expect("info after repair");

        assert!(report.object_rows_rebuilt >= 1);
        assert!(cas.exists(hash).await.expect("exists after repair"));
        assert_eq!(info.content_len, payload.len() as u64);
    })
    .await;
}

#[tokio::test]
/// Backup retention keeps only configured number of most recent snapshots.
async fn e2e_backup_retention_respects_configured_limit() {
    run_with_15s_timeout(async {
        let dir = tempdir().expect("tempdir");
        {
            let cas = FileSystemCas::open_with_alpha_and_recovery_for_tests(
                dir.path(),
                4,
                FileSystemRecoveryOptions {
                    mode: IndexRecoveryMode::Recover,
                    max_backup_snapshots: 2,
                    backup_snapshot_interval_ops: 1,
                },
            )
            .await
            .expect("open cas");

            for index in 0..5u8 {
                let payload = Bytes::from(vec![index; 64]);
                let _ = cas.put(payload).await.expect("put payload to rotate backups");
            }
        }

        let backup_root = dir.path().join("index-backups");
        let backup_files = std::fs::read_dir(&backup_root)
            .expect("read backup directory")
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("postcard"))
            .count();

        assert_eq!(backup_files, 2, "backup retention should keep only the newest two snapshots");
    })
    .await;
}
