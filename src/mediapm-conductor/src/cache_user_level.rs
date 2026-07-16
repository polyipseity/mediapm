//! User-level download cache wrapper.
//!
//! A thin newtype wrapper around the generic [`Cache`] that anchors the cache
//! root at a user-level directory (OS cache dir).
//!
//! # Three-tier cache model
//!
//! This crate provides two user-level cache tiers that share a CAS `store/`
//! but have independent index files and TTL policies:
//!
//! 1. **Tool content cache** (`tools.json`, 30-day TTL, last-use-based): stores
//!    raw downloaded bytes for tool binaries. The phase 2 (fetch) consumer calls
//!    `touch()` on cache hit so TTL measures last-download.
//! 2. **Tool metadata cache** (`tool_metadata.json`, 1-day TTL, creation-time-based):
//!    stores version/tag resolution results. The phase 1 (resolve) consumer MUST
//!    NOT call `touch()` on read — TTL is anchored to creation time.
//! 3. **Provision cache** (separate mechanism, see `crate::provision`): manages
//!    extracted tool trees with file locks under `<workspace>/tools/`. This is a
//!    fundamentally different mechanism and never interchangeable with the above
//!    two tiers.
//!
//! The provision cache lives in the `provision` module and is not a `Cache` variant.

use std::ops::Deref;
use std::path::{Path, PathBuf};

use crate::cache::Cache;
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

/// User-level download cache wrapping the generic Cache engine.
///
/// This is a thin newtype. All Cache methods are accessible via Deref.
/// Consumers open separate instances for different cache domains:
///
/// - `open_tool_cache()` — tool binary payloads (30 day TTL, tools.json)
/// - `open_metadata_cache()` — metadata/version-check payloads (1 day TTL,
///   tool_metadata.json)
/// - `open(root, name, ttl)` — fully custom
#[derive(Clone)]
pub struct UserLevelCache(Cache);

impl Deref for UserLevelCache {
    type Target = Cache;
    fn deref(&self) -> &Cache {
        &self.0
    }
}

impl UserLevelCache {
    /// Opens at the default user-level root with tools.json index and 30 day
    /// TTL.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails or the default cache root cannot be determined.
    pub async fn open_tool_cache() -> Result<Self, ConductorError> {
        let root = default_user_download_cache_root().ok_or_else(|| {
            ConductorError::Workflow("could not determine default tool cache root".to_string())
        })?;
        Cache::open_with_index_file_name_and_ttl(&root, "tools.json", 30 * 24 * 60 * 60)
            .await
            .map(Self)
    }

    /// Opens at the default user-level root with tool_metadata.json index and
    /// 1 day TTL.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails or the default cache root cannot be determined.
    pub async fn open_metadata_cache() -> Result<Self, ConductorError> {
        let root = default_user_download_cache_root().ok_or_else(|| {
            ConductorError::Workflow("could not determine default metadata cache root".to_string())
        })?;
        Cache::open_with_index_file_name_and_ttl(&root, "tool_metadata.json", 24 * 60 * 60)
            .await
            .map(Self)
    }

    /// Opens at an explicit root with custom index file and TTL.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails.
    pub async fn open(
        root: &Path,
        index_file_name: &str,
        entry_ttl_seconds: u64,
    ) -> Result<Self, ConductorError> {
        Cache::open_with_index_file_name_and_ttl(root, index_file_name, entry_ttl_seconds)
            .await
            .map(Self)
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
        let cache = UserLevelCache::open(root.path(), "tools.json", 30 * 24 * 60 * 60)
            .await
            .expect("open cache");
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
