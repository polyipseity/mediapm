use bytes::Bytes;

use mediapm_cas::api::CasApi;
use mediapm_cas::new_in_memory_cas;

#[tokio::test]
async fn get_after_put_immediate_visibility() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"immediate");
    let hash = cas.put(data.clone()).await.unwrap();
    // Should be visible right away (hint_state_change populates cache).
    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn get_after_delete_immediate_miss() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"ephemeral");
    let hash = cas.put(data).await.unwrap();
    cas.delete(hash).await.unwrap();
    // Should be invisible right away (hint_state_change populates tombstone).
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
