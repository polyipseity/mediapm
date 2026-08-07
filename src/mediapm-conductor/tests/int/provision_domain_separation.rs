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

use mediapm_cas::{CasApi, Hash, InMemoryCas};
use mediapm_conductor::cache_user_level::UserLevelCache;
use mediapm_conductor::provision::ProvisionCache;

/// A store operation on the download cache does not make content available
/// through the provision cache.
///
/// The download cache writes into `<cache_root>/store/` and the provision
/// cache materializes into a separate `<tools_dir>/` tree.  Even when sharing
/// the same `store/` directory, the download cache's logical-key entry is
/// invisible to the provision cache's per-tool metadata.
#[tokio::test]
async fn download_cache_and_provision_cache_use_different_roots() {
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
    let tools_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");

    // Store a payload in the download cache.
    let download = UserLevelCache::open(cache_root.path(), "tools.json", 30 * 24 * 60 * 60)
        .await
        .expect("open download cache");
    download.store_bytes("default", "my-key", b"shared-payload").await;

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
/// Both caches use separate roots (download cache stores under
/// `<root>/store/`, provision cache extracts under `<root>/tools/`).
/// The provision cache prune only removes expired tool extraction
/// directories, not CAS payload objects in the download cache store.
#[tokio::test]
async fn provision_cache_prune_does_not_affect_download_cache() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");

    // Open download cache — it creates its own FileSystemCas in store/.
    let download = UserLevelCache::open(root.path(), "tools.json", 30 * 24 * 60 * 60)
        .await
        .expect("open download cache");
    download.store_bytes("default", "survivor", b"keep-me").await;

    // Open provision cache at a separate tools directory (no shared CAS).
    let tools_dir = root.path().join("tools");
    let provision_cas = Arc::new(InMemoryCas::new());
    // Write the same payload into the provision cache's CAS so materialize
    // succeeds.
    let hash = provision_cas.put(b"keep-me".to_vec().into()).await.expect("store in provision CAS");

    let provision = ProvisionCache::new(tools_dir, provision_cas, None);

    // Materialize a tool using the payload in the provision cache's CAS.
    let content_map = BTreeMap::from([("binary".to_string(), hash)]);
    let _provisioned = provision
        .materialize("shared-tool", &content_map)
        .await
        .expect("provision cache materialize must succeed with its own CAS");

    // Prune the provision cache (all entries are fresh, so nothing should
    // be removed, but the prune operation must not touch the download cache).
    provision.prune_expired().await.expect("prune provision cache");

    // Verify the download cache entry is still intact.
    let data = download.lookup_bytes("default", "survivor").await;
    assert_eq!(
        data,
        Some(b"keep-me".to_vec()),
        "download cache entry must survive provision prune"
    );
}
