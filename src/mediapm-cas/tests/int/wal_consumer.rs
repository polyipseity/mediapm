//! `FileSystemCas` WAL-consumption and background-consumer tests.

use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;

use mediapm_cas::api::CasApi;

use crate::common::{artifact_dir, open_file_cas, open_file_cas_with_background};

// ---------------------------------------------------------------------------
// FileSystemCas WAL-consumption regression tests
// ---------------------------------------------------------------------------

/// Verifies that `run_wal_consumer()` on a `FileSystemCas` materializes
/// WAL-only small blobs into the blob store so they are retrievable and
/// have an on-disk blob file.
#[tokio::test]
async fn file_system_cas_wal_consumer_materializes_blob() {
    let cas = open_file_cas().await;

    let data = Bytes::from_static(b"wal-materialize-test");
    let hash = cas.put(data.clone()).await.expect("put");

    // After put, data may still be WAL-only (small blob).
    // Run the WAL consumer to materialize it.
    let consumed = cas.bg_engine().run_wal_consumer().await.expect("run wal consumer");
    assert!(consumed > 0, "WAL consumer must have consumed at least one entry");

    // Data should still be retrievable via get().
    let retrieved = cas.get(hash).await.expect("get after materialization");
    assert_eq!(retrieved, data);
}

/// Verifies that `run_wal_consumer()` processes entries sequentially across
/// multiple put cycles and subsequent calls return 0 (idempotency for that
/// batch).
#[tokio::test]
async fn file_system_cas_wal_consumer_processes_batches() {
    let cas = open_file_cas().await;

    // Put entries in two batches to verify consumer processes all.
    for i in 0..5 {
        let data = Bytes::from(format!("batch-a-{i}"));
        cas.put(data).await.expect("put batch a");
    }
    let consumed1 = cas.bg_engine().run_wal_consumer().await.expect("first wal consumer");
    assert!(consumed1 >= 5, "first consume must process all batch-a entries");

    // Second consume returns 0 (no new entries).
    let consumed2 = cas.bg_engine().run_wal_consumer().await.expect("second wal consumer");
    assert_eq!(consumed2, 0, "second consume must return 0");

    // Put second batch.
    for i in 0..3 {
        let data = Bytes::from(format!("batch-b-{i}"));
        cas.put(data).await.expect("put batch b");
    }
    let consumed3 = cas.bg_engine().run_wal_consumer().await.expect("third wal consumer");
    assert!(consumed3 >= 3, "third consume must process all batch-b entries");

    // All entries from both batches retrievable.
    for i in 0..5 {
        let data = Bytes::from(format!("batch-a-{i}"));
        let hash = mediapm_cas::Hash::from_content(&data);
        let retrieved = cas.get(hash).await.expect("batch-a entry must be retrievable");
        assert_eq!(retrieved, data);
    }
    for i in 0..3 {
        let data = Bytes::from(format!("batch-b-{i}"));
        let hash = mediapm_cas::Hash::from_content(&data);
        let retrieved = cas.get(hash).await.expect("batch-b entry must be retrievable");
        assert_eq!(retrieved, data);
    }
}

/// Verifies that reopening a `FileSystemCas` — putting data, closing,
/// reopening, then calling `run_wal_consumer()` — correctly replays
/// un-consumed entries and advances the checkpoint from where the previous
/// session left off.
#[tokio::test]
async fn file_system_cas_reopen_and_consume_wal() {
    let dir = artifact_dir();

    // First session: open, put data, close (drop cas).
    let hash;
    {
        let cas = mediapm_cas::FileSystemCas::open(dir.path()).await.expect("first open");
        let data = Bytes::from_static(b"reopen-test");
        hash = cas.put(data).await.expect("put");
        // DO NOT consume WAL — close while entries are still in WAL.
    }

    // Second session: reopen, verify data is recoverable via WAL replay.
    {
        let cas = mediapm_cas::FileSystemCas::open(dir.path()).await.expect("second open");
        // `open_with_strategies` calls `rebuild_from_wal`, which loads
        // un-consumed WAL entries into the in-memory metadata store.
        // Data should be retrievable immediately.
        let retrieved = cas.get(hash).await.expect("get after reopen");
        assert_eq!(retrieved, Bytes::from_static(b"reopen-test"));

        // Now consume the WAL to materialize blobs on disk.
        let consumed = cas.bg_engine().run_wal_consumer().await.expect("run wal consumer");
        assert!(consumed > 0, "must consume entries from previous session");
    }
}

/// Verifies that running `run_wal_consumer()` multiple times is idempotent
/// — second call consumes 0 entries.
#[tokio::test]
async fn file_system_cas_wal_consumer_multiple_cycles() {
    let cas = open_file_cas().await;

    cas.put(Bytes::from_static(b"first")).await.expect("put first");
    cas.put(Bytes::from_static(b"second")).await.expect("put second");

    // First consume.
    let consumed1 = cas.bg_engine().run_wal_consumer().await.expect("first wal consumer");
    assert!(consumed1 > 0, "first consume must process entries");

    // Second consume — no new entries, should return 0.
    let consumed2 = cas.bg_engine().run_wal_consumer().await.expect("second wal consumer");
    assert_eq!(consumed2, 0, "second consume must return 0 (no new entries)");
}

// ---------------------------------------------------------------------------
// Background WAL consumer tests
// ---------------------------------------------------------------------------

/// Verifies that the background WAL consumer materializes blobs without
/// an explicit `run_wal_consumer()` call.
#[tokio::test]
async fn file_system_cas_background_task_materializes_blob() {
    let cas = open_file_cas_with_background(Duration::from_millis(100)).await;

    let payload = Bytes::from_static(b"background-test-data");
    let hash = cas.put(payload.clone()).await.expect("put data");
    // Wait for background consumer to materialize the blob.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let retrieved = cas.get(hash).await.expect("get data");
    assert_eq!(retrieved.to_vec(), payload.to_vec(), "background must materialize blob");

    drop(cas);
    // Dropping without panics confirms RAII cleanup works.
}

/// Verifies that the background task survives a re-open cycle (WAL
/// entries from a previous session are consumed on re-open).
#[tokio::test]
async fn file_system_cas_background_task_survives_reopen() {
    let dir = artifact_dir();

    // First session: put data and let background materialize it.
    let cas = mediapm_cas::FileSystemCas::open_with_strategies_and_interval(
        dir.path(),
        vec![],
        Duration::from_millis(100),
    )
    .await
    .expect("open CAS first session");
    let payload = Bytes::from_static(b"reopen-test-data");
    let hash = cas.put(payload.clone()).await.expect("put data");
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(cas);

    // Second session: reopen and verify blob retrievable.
    let cas = mediapm_cas::FileSystemCas::open_with_strategies_and_interval(
        dir.path(),
        vec![],
        Duration::from_millis(100),
    )
    .await
    .expect("open CAS second session");
    let retrieved = cas.get(hash).await.expect("get data after reopen");
    assert_eq!(retrieved.to_vec(), payload.to_vec(), "blob must survive reopen");
}

/// Verifies that the background WAL consumer processes a put+delete pair
/// so the blob is never materialized on disk (the two entries cancel out).
#[tokio::test]
async fn file_system_cas_background_task_deletes_through_wal() {
    let cas = open_file_cas_with_background(Duration::from_millis(100)).await;

    let payload = Bytes::from_static(b"delete-through-wal");
    let hash = cas.put(payload.clone()).await.expect("put data");
    // Delete before the background consumer runs (500ms initial delay).
    tokio::time::sleep(Duration::from_millis(100)).await;
    cas.delete(hash).await.expect("delete data");

    // Wait for background consumer to process both entries.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // The blob should not exist — the net effect of put+delete is zero.
    assert!(cas.get(hash).await.is_err(), "blob must not exist after put+delete via background");
}

/// Verifies that the background maintenance guard's cancelled flag is set
/// when the `FileSystemCas` handle is dropped.
#[tokio::test]
async fn file_system_cas_background_maintenance_guard_cancels_on_drop() {
    let cas = open_file_cas_with_background(Duration::from_millis(100)).await;

    // Clone the cancelled flag from the guard (via test accessor).
    let cancelled = cas.bg_guard_ref().cancelled.clone();
    assert!(!cancelled.load(Ordering::SeqCst), "guard must not be cancelled before drop");

    drop(cas);
    assert!(
        cancelled.load(Ordering::SeqCst),
        "bg_guard must be cancelled after FileSystemCas drop"
    );
}

/// Two WAL consumers draining the same unconsumed range must not collide.
///
/// `FileSystemCas::open` arms a background consumer, and any foreground
/// `flush` / `run_maintenance_cycle` runs on the same engine. Both read the
/// checkpoint before either has advanced it (it only advances at the end of a
/// batch), so without mutual exclusion they replay the *same* entries at the
/// same time. The blob and metadata stores stage every write through a
/// deterministic `<path>.tmp` name, so the two replays fight over one staging
/// file: the first rename consumes it and the second fails with `NotFound`,
/// turning a background task into a foreground error.
///
/// The invariant is that the range is consumed *at most* once, so the two
/// reported counts can never exceed the number of WAL entries. They may fall
/// short of it: the background consumer this store arms at open may claim
/// part of the range, which is the same correct behaviour seen from a third
/// angle.
#[tokio::test]
async fn concurrent_wal_consumers_never_double_consume_a_range() {
    const ENTRIES: u64 = 24;
    let cas = open_file_cas().await;

    for index in 0..ENTRIES {
        cas.put(Bytes::from(format!("concurrent-replay-{index}"))).await.expect("put");
    }

    let (first, second) =
        tokio::join!(cas.bg_engine().run_wal_consumer(), cas.bg_engine().run_wal_consumer());
    let consumed = first.expect("first consumer must not fail")
        + second.expect("second consumer must not fail");
    assert!(
        consumed <= ENTRIES,
        "concurrent consumers consumed {consumed} entries for a {ENTRIES}-entry WAL: \
         the same range was replayed twice"
    );
}
