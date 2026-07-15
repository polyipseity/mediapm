use bytes::Bytes;
use std::collections::BTreeSet;

use mediapm_cas::api::{CasApi, ConstraintApi, ConstraintPatch};
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
    let items: Vec<Bytes> = (0..20).map(|i| Bytes::from(format!("object-{i}"))).collect();
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
// Empty-content sentinel tests
// ---------------------------------------------------------------------------

/// Empty-content sentinel (`blake3(b"")`) always exists as empty content.
///
/// - get(empty) → empty bytes
/// - stat(empty) → {len: 0, encoding: Full}
/// - delete(empty) → no-op (sentinel always present)
/// - `set_constraint(empty, …)` → always empty constraints
/// - `get_constraint(empty)` → always empty
/// - `patch_constraint(empty, …)` → no-op
#[tokio::test]
async fn empty_sentinel_is_always_present() {
    let cas = new_in_memory_cas();
    let empty = mediapm_cas::Hash::empty();
    // It's the hash of empty content.
    assert_eq!(empty, mediapm_cas::Hash::from_content(b""));

    // Always present as empty content.
    let data = cas.get(empty).await.unwrap();
    assert!(data.is_empty(), "get(empty) should return empty bytes");
    let meta = cas.stat(empty).await.unwrap();
    assert_eq!(meta.len, 0, "stat(empty) len should be 0");
    assert_eq!(
        meta.encoding,
        mediapm_cas::ObjectEncoding::Full,
        "stat(empty) encoding should be Full"
    );

    // Deleting empty is harmless (no-op).
    cas.delete(empty).await.unwrap();
    // After delete, empty still exists.
    let meta = cas.stat(empty).await.unwrap();
    assert_eq!(meta.len, 0, "stat(empty) len should still be 0 after delete");

    // Constraints on empty are always empty.
    let base = mediapm_cas::Hash::from_content(b"some-base");
    cas.set_constraint(empty, BTreeSet::from([base])).await.unwrap();
    let got = cas.get_constraint(empty).await.unwrap();
    assert!(got.is_empty(), "constraints on empty should always be empty");

    // Patch on empty is also a no-op.
    cas.patch_constraint(
        empty,
        ConstraintPatch {
            add_bases: BTreeSet::from([mediapm_cas::Hash::from_content(b"another")]),
            remove_bases: BTreeSet::new(),
            clear: false,
        },
    )
    .await
    .unwrap();
    let got = cas.get_constraint(empty).await.unwrap();
    assert!(got.is_empty(), "patch_constraint on empty should have no effect");
}
