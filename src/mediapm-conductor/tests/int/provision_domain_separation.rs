//! Integration tests that validate provision cache and download cache
//! independence.
//!
//! The two cache tiers (user-level `UserLevelCache` and workspace-scoped
//! `ProvisionCache`) operate at different abstraction layers.  These tests
//! verify that:
//! - They use separate roots and backends.
//! - Pruning one does not corrupt or remove data belonging to the other.

use std::collections::BTreeMap;
use std::sync::Arc;

use mediapm_cas::{FileSystemCas, Hash, InMemoryCas};
use mediapm_conductor::cache_user_level::UserLevelCache;
use mediapm_conductor::provision::ProvisionCache;

/// A store operation on the download cache does not make content available
/// through the provision cache.
///
/// The download cache writes into `<cache_root>/store/` and the provision
/// cache materializes into a separate `<tools_dir>/` tree.  Even when sharing
/// the same CAS backend, the provision cache materialize call requires a
/// content map that points into the shared CAS — but the download cache's
/// logical-key entry is invisible to the provision cache's per-tool metadata.
#[tokio::test]
async fn download_cache_and_provision_cache_use_different_roots() {
    let cache_root = tempfile::tempdir().expect("tempdir for cache");
    let tools_dir = tempfile::tempdir().expect("tempdir for tools");

    // Store a payload in the download cache.
    let (download, _guard) =
        UserLevelCache::open(cache_root.path(), "tools.json", 30 * 24 * 60 * 60)
            .await
            .expect("open download cache");
    download.store_bytes("my-key", b"shared-payload").await;

    // Compute the hash directly from the known payload.
    let hash = Hash::from_content(b"shared-payload");

    // Create a provision cache with an empty InMemoryCas (no data).
    let cas = Arc::new(InMemoryCas::new());
    let provision = ProvisionCache::new(tools_dir.path().to_path_buf(), cas, None);

    // Materializing with a hash that exists in the download cache's
    // FileSystemCas but not in the provision cache's InMemoryCas must fail.
    let content_map = BTreeMap::from([("binary".to_string(), hash)]);
    let result = provision.materialize("my-tool", &content_map).await;
    assert!(
        result.is_err(),
        "provision cache must not find content from download cache's CAS store"
    );
}

/// Pruning the provision cache must not invalidate download cache entries.
///
/// Both caches share the same CAS backend (same `store/` directory) but
/// the provision cache prune only removes expired tool extraction
/// directories, not CAS payload objects.
#[tokio::test]
async fn provision_cache_prune_does_not_affect_download_cache() {
    let root = tempfile::tempdir().expect("tempdir");

    // Open download cache — creates <root>/store/ for CAS.
    let (download, _guard) = UserLevelCache::open(root.path(), "tools.json", 30 * 24 * 60 * 60)
        .await
        .expect("open download cache");
    download.store_bytes("survivor", b"keep-me").await;

    // Compute the hash directly from the known payload.
    let hash = Hash::from_content(b"keep-me");

    // Open provision cache backed by the same FileSystemCas store.
    let store_path = root.path().join("store");
    let cas = Arc::new(FileSystemCas::open(&store_path).await.expect("open FileSystemCas"));
    let tools_dir = root.path().join("tools");
    let provision = ProvisionCache::new(tools_dir, cas, None);

    // Materialize a tool using the shared CAS payload.
    let content_map = BTreeMap::from([("binary".to_string(), hash)]);
    let _provisioned = provision
        .materialize("shared-tool", &content_map)
        .await
        .expect("provision cache materialize must succeed with shared CAS");

    // Prune the provision cache (all entries are fresh, so nothing should
    // be removed, but the prune operation must not touch CAS data).
    provision.prune_expired().await.expect("prune provision cache");

    // Verify the download cache entry is still intact.
    let data = download.lookup_bytes("survivor").await;
    assert_eq!(
        data,
        Some(b"keep-me".to_vec()),
        "download cache entry must survive provision prune"
    );
}
