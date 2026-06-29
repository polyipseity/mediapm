use bytes::Bytes;

use mediapm_cas::api::CasApi;
use mediapm_cas::new_in_memory_cas;

#[tokio::test]
async fn get_after_put_immediate_visibility() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"immediate");
    let hash = cas.put(data.clone()).await.unwrap();
    // Should be visible right away (CasStore writes index + blob directly).
    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn get_after_delete_immediate_miss() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"ephemeral");
    let hash = cas.put(data).await.unwrap();
    cas.delete(hash).await.unwrap();
    // Should be invisible right away (CasStore deletes from index + writes tombstone to WAL).
    assert!(cas.get(hash).await.is_err());
}

#[tokio::test]
async fn concurrent_gets_same_hash() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"concurrent");
    let hash = cas.put(data.clone()).await.unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        let cas_ref = cas.clone();
        let h = hash;
        handles.push(tokio::spawn(async move { cas_ref.get(h).await.unwrap() }));
    }

    for handle in handles {
        let retrieved = handle.await.unwrap();
        assert_eq!(retrieved, data);
    }
}

#[tokio::test]
async fn stat_after_put() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"stat-me");
    let hash = cas.put(data.clone()).await.unwrap();
    let meta = cas.stat(hash).await.unwrap();
    assert_eq!(meta.len, data.len() as u64);
}

#[tokio::test]
async fn stat_missing_fails() {
    let cas = new_in_memory_cas();
    let hash = mediapm_cas::Hash::from_content(b"nope");
    assert!(cas.stat(hash).await.is_err());
}

#[tokio::test]
async fn delete_nonexistent_is_ok() {
    let cas = new_in_memory_cas();
    let hash = mediapm_cas::Hash::from_content(b"ghost");
    // Delete of an object never put should succeed (idempotent).
    cas.delete(hash).await.unwrap();
}

// ---------------------------------------------------------------------------
// Orphan recovery — full blob
// ---------------------------------------------------------------------------

/// get() recovers a full blob whose metadata entry was deleted.
#[tokio::test]
async fn get_recovers_orphan_full_blob() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"orphan-full-get");
    let hash = cas.put(data.clone()).await.unwrap();

    // Flush WAL so check_pending returns NotPresent.
    cas.flush().await.unwrap();
    // Simulate metadata loss.
    cas.simulate_metadata_loss_for_test(hash).await;

    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved, data);
}

/// stat() recovers metadata for an orphan full blob.
#[tokio::test]
async fn stat_recovers_orphan_full_blob() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"orphan-full-stat");
    let hash = cas.put(data.clone()).await.unwrap();

    cas.flush().await.unwrap();
    cas.simulate_metadata_loss_for_test(hash).await;

    let meta = cas.stat(hash).await.unwrap();
    assert_eq!(meta.len, data.len() as u64);
}

/// get_to_writer() recovers an orphan full blob.
#[tokio::test]
async fn get_to_writer_recovers_orphan_full_blob() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"orphan-full-stream");
    let hash = cas.put(data.clone()).await.unwrap();

    cas.flush().await.unwrap();
    cas.simulate_metadata_loss_for_test(hash).await;

    let mut buf = Vec::new();
    cas.get_to_writer(hash, &mut buf).await.unwrap();
    assert_eq!(Bytes::from(buf), data);
}

// ---------------------------------------------------------------------------
// Orphan recovery — edge cases
// ---------------------------------------------------------------------------

/// Normal put/read path is unaffected when there is no metadata loss.
#[tokio::test]
async fn normal_get_unaffected_by_orphan_recovery() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"normal-path");
    let hash = cas.put(data.clone()).await.unwrap();

    // No flush, no metadata loss — immediate visibility.
    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved, data);
}

/// get() on a hash that was never put still returns NotFound after simulating
/// metadata loss (never-put has no blob to recover from).
#[tokio::test]
async fn missing_hash_not_recovered() {
    let cas = new_in_memory_cas();
    let hash = mediapm_cas::Hash::from_content(b"never-put");

    cas.flush().await.unwrap();
    cas.simulate_metadata_loss_for_test(hash).await;

    assert!(cas.get(hash).await.is_err());
}

/// stat() on a never-put hash still returns NotFound.
#[tokio::test]
async fn missing_stat_not_recovered() {
    let cas = new_in_memory_cas();
    let hash = mediapm_cas::Hash::from_content(b"never-put-stat");

    cas.flush().await.unwrap();
    cas.simulate_metadata_loss_for_test(hash).await;

    assert!(cas.stat(hash).await.is_err());
}

/// A WAL tombstone shadows an orphan full blob — orphan recovery must NOT
/// resurrect deleted objects.
#[tokio::test]
async fn tombstone_shadows_orphan_full_blob() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"tombstone-shadow");
    let hash = cas.put(data.clone()).await.unwrap();

    // Flush, then delete (creates WAL tombstone, and with SYNC_MATERIALIZE
    // also removes metadata+blob immediately).
    cas.flush().await.unwrap();
    cas.delete(hash).await.unwrap();

    // Simulate metadata loss (redundant here — already gone).
    cas.simulate_metadata_loss_for_test(hash).await;

    // get should still return NotFound — tombstone in WAL prevents
    // orphan recovery.
    assert!(cas.get(hash).await.is_err());
}

/// stat() is also shadowed by a WAL tombstone.
#[tokio::test]
async fn tombstone_shadows_orphan_stat() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"tombstone-stat");
    let hash = cas.put(data.clone()).await.unwrap();

    cas.flush().await.unwrap();
    cas.delete(hash).await.unwrap();
    cas.simulate_metadata_loss_for_test(hash).await;

    assert!(cas.stat(hash).await.is_err());
}
