//! Integration tests for Redb index persistence side effects.
//!
//! Ensures storage + maintenance workflows leave expected durable index rows.

use bytes::Bytes;
use mediapm_cas::{CasApi, CasMaintenanceApi, Constraint, FileSystemCas, OptimizeOptions};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::BTreeSet;
use std::time::Duration;
use tempfile::tempdir;

/// Redb primary-index table containing object metadata rows.
const PRIMARY_INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("primary_index");
/// Legacy/disabled delta-graph table name used for negative assertion coverage.
const DELTA_GRAPH: TableDefinition<&[u8], &[u8]> = TableDefinition::new("delta_graph");

#[tokio::test]
/// Ensures put/constraint/optimize flows populate expected redb primary rows.
async fn redb_primary_index_is_populated_after_put_and_constraint_workflows() {
    let dir = tempdir().expect("tempdir");
    let cas = FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas");

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

    let _ = cas.optimize_once(OptimizeOptions::default()).await.expect("optimize once");

    drop(cas);

    let db_path = dir.path().join("index.redb");
    let mut db = None;
    for _ in 0..30 {
        match Database::open(&db_path) {
            Ok(handle) => {
                db = Some(handle);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    let db = db.expect("open redb after filesystem actor shutdown");
    let read = db.begin_read().expect("read txn");

    let primary_count = read
        .open_table(PRIMARY_INDEX)
        .expect("open primary index table")
        .iter()
        .expect("iter primary index")
        .count();

    assert!(primary_count >= 2, "expected at least 2 primary index rows");
    assert!(
        read.open_table(DELTA_GRAPH).is_err(),
        "delta graph table should not exist because graph edges are encoded in primary rows"
    );
}
