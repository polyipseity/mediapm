//! User-level download cache wrapper.
//!
//! A thin domain wrapper around the generic [`Cache`] that anchors the cache
//! root at a user-level directory (OS cache dir).

use std::path::{Path, PathBuf};

use crate::cache::{Cache, CachePruneReport};
use crate::error::ConductorError;

/// Returns the default user-scoped cache root for conductor standalone
/// invocations.
///
/// Path: `<os-cache-dir>/mediapm-conductor/cache`
#[must_use]
pub fn default_user_download_cache_root() -> Option<PathBuf> {
    dirs::cache_dir().map(|root| root.join("mediapm-conductor").join("cache"))
}

/// Returns the default user-scoped cache root for `mediapm` invocations.
///
/// Path: `<os-cache-dir>/mediapm/cache`
#[must_use]
pub fn default_mediapm_user_download_cache_root() -> Option<PathBuf> {
    dirs::cache_dir().map(|root| root.join("mediapm").join("cache"))
}

/// User-level download cache backed by the generic [`Cache`] engine.
///
/// This type exists so the downloader works with a semantically meaningful
/// domain type.  All methods delegate to the inner [`Cache`].
#[derive(Clone)]
pub struct UserLevelCache(Cache);

impl UserLevelCache {
    /// Opens (or bootstraps) the user-level cache at `root` with the default
    /// TTL (30 days).
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails.
    pub async fn open(root: &Path) -> Result<Self, ConductorError> {
        Cache::open_with_index_file_name_and_ttl(root, "tools.jsonc", 30 * 24 * 60 * 60)
            .await
            .map(Self)
    }

    /// Opens the user-level cache at `root` with a custom TTL and a specific
    /// index file.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails.
    pub async fn open_with_index_file_name_and_ttl(
        root: &Path,
        index_file_name: &str,
        entry_ttl_seconds: u64,
    ) -> Result<Self, ConductorError> {
        Cache::open_with_index_file_name_and_ttl(root, index_file_name, entry_ttl_seconds)
            .await
            .map(Self)
    }

    /// Opens a `tool_metadata.jsonc` cache with 1-day TTL.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails.
    pub async fn open_metadata_cache(root: &Path) -> Result<Self, ConductorError> {
        Self::open_with_index_file_name_and_ttl(root, "tool_metadata.jsonc", 24 * 60 * 60).await
    }

    #[must_use]
    pub async fn lookup_bytes(&self, cache_key: &str) -> Option<Vec<u8>> {
        self.0.lookup_bytes(cache_key).await
    }

    pub async fn store_bytes(&self, cache_key: &str, payload: &[u8]) {
        self.0.store_bytes(cache_key, payload).await;
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.0.entry_count()
    }

    pub fn refresh_last_used(&self, key: &str) {
        self.0.refresh_last_used(key);
    }

    pub async fn prune_expired_entries(&self) -> Result<CachePruneReport, ConductorError> {
        self.0.prune_expired_entries().await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        UserLevelCache, default_mediapm_user_download_cache_root, default_user_download_cache_root,
    };

    /// Protects crate-level cache roots so conductor and mediapm resolve to
    /// distinct base directories with the same flat `cache/` layout.
    #[test]
    fn default_cache_roots_use_flat_cache_layout() {
        let conductor_root = default_user_download_cache_root();
        let mediapm_root = default_mediapm_user_download_cache_root();
        if let (Some(conductor_root), Some(mediapm_root)) = (conductor_root, mediapm_root) {
            assert_ne!(conductor_root, mediapm_root);
            assert!(
                conductor_root.ends_with("cache"),
                "conductor root must end with 'cache', got: {}",
                conductor_root.display()
            );
            assert!(
                mediapm_root.ends_with("cache"),
                "mediapm root must end with 'cache', got: {}",
                mediapm_root.display()
            );
            let conductor_parent = conductor_root.parent().unwrap();
            let mediapm_parent = mediapm_root.parent().unwrap();
            assert_ne!(conductor_parent, mediapm_parent);
            assert!(
                conductor_parent.ends_with("mediapm-conductor"),
                "conductor cache base must be 'mediapm-conductor', got: {}",
                conductor_parent.display()
            );
            assert!(
                mediapm_parent.ends_with("mediapm"),
                "mediapm cache base must be 'mediapm', got: {}",
                mediapm_parent.display()
            );
        }
    }

    /// Protects shared-cache behavior by ensuring key-based round trips return
    /// the original payload bytes.
    #[tokio::test]
    async fn cache_round_trips_bytes_by_logical_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache = UserLevelCache::open(root.path()).await.expect("open cache");
        let payload = b"shared-download-cache".to_vec();
        let key = "test-tool-v1.0.0";
        cache.store_bytes(key, &payload).await;
        let retrieved = cache.lookup_bytes(key).await;
        assert_eq!(retrieved, Some(payload.clone()), "round-trip must return original bytes");
        cache.prune_expired_entries().await.expect("prune should succeed");
        // Immediate prune should not remove fresh entry
        let retrieved_after = cache.lookup_bytes(key).await;
        assert_eq!(retrieved_after, Some(payload), "fresh entry must survive prune");
    }
}
