use bytes::Bytes;

use mediapm_cas::api::CasApi;
use mediapm_cas::new_in_memory_cas;

/// Basic put → get round-trip works.
#[tokio::test]
async fn put_and_get() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"hello cas");
    let hash = cas.put(data.clone()).await.unwrap();

    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved, data);
}

/// Getting a non-existent hash fails.
#[tokio::test]
async fn get_missing_fails() {
    let cas = new_in_memory_cas();
    let hash = mediapm_cas::Hash::from_content(b"nonexistent");
    let result = cas.get(hash).await;
    assert!(result.is_err(), "expected error for missing object");
}

/// put → stat round-trip returns len.
#[tokio::test]
async fn put_and_stat() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"stat test payload");
    let hash = cas.put(data.clone()).await.unwrap();

    let meta = cas.stat(hash).await.unwrap();
    assert_eq!(meta.len, data.len() as u64);
}

/// delete removes an object so subsequent get fails.
#[tokio::test]
async fn put_then_delete() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"ephemeral");
    let hash = cas.put(data).await.unwrap();

    cas.delete(hash).await.unwrap();
    assert!(cas.get(hash).await.is_err(), "get after delete should fail");
}

/// Idempotent put — storing the same content twice yields the same hash.
#[tokio::test]
async fn idempotent_put() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"same content");
    let h1 = cas.put(data.clone()).await.unwrap();
    let h2 = cas.put(data).await.unwrap();
    assert_eq!(h1, h2);
}

/// Multiple puts with different content produce distinct hashes.
#[tokio::test]
async fn distinct_content_distinct_hashes() {
    let cas = new_in_memory_cas();
    let h1 = cas.put(Bytes::from_static(b"alpha")).await.unwrap();
    let h2 = cas.put(Bytes::from_static(b"beta")).await.unwrap();
    assert_ne!(h1, h2);
}

/// put many objects then get each one back.
#[tokio::test]
async fn bulk_put_and_get() {
    let cas = new_in_memory_cas();
    let items: Vec<Bytes> = (0..100).map(|i| Bytes::from(format!("object-{i}"))).collect();
    let mut hashes = Vec::with_capacity(items.len());
    for item in &items {
        hashes.push(cas.put(item.clone()).await.unwrap());
    }

    for (hash, expected) in hashes.into_iter().zip(items.iter()) {
        let retrieved = cas.get(hash).await.unwrap();
        assert_eq!(retrieved, expected.clone());
    }
}

// ---------------------------------------------------------------------------
// Empty hash (Hash::zero()) sentinel tests
// ---------------------------------------------------------------------------

/// Zero hash is a sentinel that always exists as empty content.
#[tokio::test]
async fn zero_hash_is_sentinel() {
    let cas = new_in_memory_cas();
    let zero = mediapm_cas::Hash::zero();
    // It's a valid hash value.
    assert_eq!(zero.as_bytes(), &[0u8; 32]);
    // Not the hash of any real content (including empty).
    assert_ne!(zero, mediapm_cas::Hash::from_content(b""));
    // Always present as empty content.
    let data = cas.get(zero).await.unwrap();
    assert!(data.is_empty(), "get(zero) should return empty bytes");
    let meta = cas.stat(zero).await.unwrap();
    assert_eq!(meta.len, 0, "stat(zero) len should be 0");
    assert_eq!(
        meta.encoding,
        mediapm_cas::ObjectEncoding::Full,
        "stat(zero) encoding should be Full"
    );
    // Deleting zero is harmless (no-op).
    cas.delete(zero).await.unwrap();
    // After delete, zero still exists (no-op delete).
    let meta = cas.stat(zero).await.unwrap();
    assert_eq!(meta.len, 0, "stat(zero) len should still be 0 after delete");
}

/// get(zero) returns empty bytes.
#[tokio::test]
async fn get_zero_returns_empty() {
    let cas = new_in_memory_cas();
    let zero = mediapm_cas::Hash::zero();
    let data = cas.get(zero).await.unwrap();
    assert!(data.is_empty(), "get(zero) should return empty bytes");
}

/// stat(zero) returns ObjectMeta { len: 0, encoding: Full }.
#[tokio::test]
async fn stat_zero_returns_zero_len() {
    let cas = new_in_memory_cas();
    let zero = mediapm_cas::Hash::zero();
    let meta = cas.stat(zero).await.unwrap();
    assert_eq!(meta.len, 0, "stat(zero) len should be 0");
    assert_eq!(
        meta.encoding,
        mediapm_cas::ObjectEncoding::Full,
        "stat(zero) encoding should be Full"
    );
}

/// delete(zero) is a no-op.
#[tokio::test]
async fn delete_zero_is_noop() {
    let cas = new_in_memory_cas();
    let zero = mediapm_cas::Hash::zero();
    // Should not panic or error.
    cas.delete(zero).await.unwrap();
}
