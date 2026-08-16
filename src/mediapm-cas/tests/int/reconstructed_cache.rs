//! Integration tests for the reconstructed-bytes cache (read path).
//!
//! Exercises the public read path end-to-end: `get()` on delta-encoded
//! objects consults and populates the shared reconstructed-bytes cache,
//! repeated reads hit it, `delete()` invalidates it synchronously, and
//! background maintenance (WAL rematerialization) reuses the same cache
//! instance via the shared [`resolve_full_bytes`](mediapm_cas::storage)
//! choke point.
//!
//! Uses `InMemoryCas` for determinism: no background consumer races.

use bytes::Bytes;

use mediapm_cas::Hash;
use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi, ObjectEncoding};
use mediapm_cas::new_in_memory_cas;

/// Seed the store with 50 filler objects so the reconstructed-bytes cache
/// budget (10 % of store bytes) comfortably admits the 4096-byte test
/// entries: 52 × 4096 = 212 992 store bytes → ~21 299 budget →
/// `max_bytes/4` ≈ 5 324 ≥ 4096 (the per-entry admission limit).
async fn seed_fillers(cas: &impl CasApi) {
    for seed in 0..50u8 {
        cas.put(Bytes::from(vec![seed; 4096])).await.unwrap();
    }
}

/// Base and target content that differ only in `marker`, so the VCDIFF
/// delta is meaningfully smaller than the full content.
fn similar_content_pair(fill: u8, marker: &[u8]) -> (Bytes, Bytes) {
    let base_content = Bytes::from(vec![fill; 4096]);
    let target_content = {
        let mut v = vec![fill; 2048];
        v.extend_from_slice(marker);
        v.extend_from_slice(&vec![fill; 2048 - marker.len()]);
        Bytes::from(v)
    };
    (base_content, target_content)
}

/// Build a delta chain (target ← base) via constraint + maintenance.
async fn build_delta_pair(cas: &(impl CasApi + ConstraintApi + CasMaintenanceApi)) -> (Hash, Hash) {
    let (base_content, target_content) = similar_content_pair(b'B', b"READ_CACHE");
    let base_hash = cas.put(base_content).await.unwrap();
    let target_hash = cas.put(target_content).await.unwrap();
    cas.set_constraint(target_hash, [base_hash].into()).await.unwrap();
    cas.run_maintenance_cycle().await.unwrap();
    (base_hash, target_hash)
}

/// First `get()` on a delta-encoded object is a cache miss that populates
/// the cache; the second is a cache hit served without re-resolving the
/// delta chain.
#[tokio::test]
async fn read_path_consults_and_populates_cache() {
    let cas = new_in_memory_cas();
    seed_fillers(&cas).await;
    let (_base_hash, target_hash) = build_delta_pair(&cas).await;

    // Target is delta-encoded before any user read.
    let meta = cas.stat(target_hash).await.unwrap();
    assert!(matches!(meta.encoding, ObjectEncoding::Delta { .. }));

    // Cache is empty before the first delta resolution; the maintenance
    // budget refresh has already established `max_bytes` (10 % of store).
    let stats = cas.reconstructed_cache_stats().unwrap();
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);
    assert!(stats.max_bytes > 0);

    // First read: miss → budget established → entry inserted.
    assert_eq!(cas.get(target_hash).await.unwrap().len(), 4096);
    let stats = cas.reconstructed_cache_stats().unwrap();
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.cached_bytes, 4096);
    assert!(stats.max_bytes > 0, "budget established lazily on first miss");

    // Second read: cache hit, no new miss or entry.
    assert_eq!(cas.get(target_hash).await.unwrap().len(), 4096);
    let stats = cas.reconstructed_cache_stats().unwrap();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.entries, 1);
}

/// `delete()` invalidates the cached reconstruction synchronously, so the
/// cache can never serve bytes for a deleted object.
#[tokio::test]
async fn read_path_invalidates_on_delete() {
    let cas = new_in_memory_cas();
    seed_fillers(&cas).await;
    let (_base_hash, target_hash) = build_delta_pair(&cas).await;

    // Populate the cache.
    assert_eq!(cas.get(target_hash).await.unwrap().len(), 4096);
    assert_eq!(cas.reconstructed_cache_stats().unwrap().entries, 1);

    // Delete invalidates the cache entry immediately.
    cas.delete(target_hash).await.unwrap();
    assert_eq!(
        cas.reconstructed_cache_stats().unwrap().entries,
        0,
        "delete invalidates the cached reconstruction synchronously",
    );

    // The object is gone from the read path.
    assert!(cas.get(target_hash).await.is_err());

    // Fully materialize the delete; the cache must stay empty.
    cas.run_maintenance_cycle().await.unwrap();
    assert_eq!(cas.reconstructed_cache_stats().unwrap().entries, 0);
}

/// Background maintenance reuses the cache populated by the read path:
/// the optimizer's pre-encoding reconstruction of an already-delta-encoded
/// target is served as a cache hit rather than re-walking the delta chain
/// (read view and bg engine share one cache instance).
#[tokio::test]
async fn optimizer_and_read_path_share_cache() {
    let cas = new_in_memory_cas();
    seed_fillers(&cas).await;
    let (_base_hash, target_hash) = build_delta_pair(&cas).await;

    // Populate the cache via the read path.
    assert_eq!(cas.get(target_hash).await.unwrap().len(), 4096);
    let stats = cas.reconstructed_cache_stats().unwrap();
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.entries, 1);

    // A second maintenance cycle re-encodes the same pair; the optimizer's
    // reconstruction of the delta-encoded target is a cache hit.
    cas.run_maintenance_cycle().await.unwrap();
    let stats = cas.reconstructed_cache_stats().unwrap();
    assert_eq!(stats.hits, 1, "optimizer reconstruction served from cache");
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.entries, 1);

    // Content intact and still delta-encoded.
    let meta = cas.stat(target_hash).await.unwrap();
    assert!(matches!(meta.encoding, ObjectEncoding::Delta { .. }));
    assert_eq!(cas.get(target_hash).await.unwrap().len(), 4096);
    assert_eq!(cas.reconstructed_cache_stats().unwrap().hits, 2);
    assert_eq!(cas.reconstructed_cache_stats().unwrap().misses, 1);
}
