//! User-level download cache wrapper.
//!
//! A thin newtype wrapper around the generic [`Cache`] that anchors the cache
//! root at a user-level directory (OS cache dir).

use std::ops::Deref;
use std::path::{Path, PathBuf};

use crate::cache::{Cache, CacheDomainConfig};
use crate::error::ConductorError;

/// Returns the default user-scoped cache root for `mediapm` invocations.
///
/// Path: `<os-cache-dir>/mediapm/cache`
#[must_use]
pub fn default_mediapm_user_download_cache_root() -> Option<PathBuf> {
    dirs::cache_dir().map(|root| root.join("mediapm").join("cache"))
}

/// User-level download cache wrapping the generic Cache engine.
///
/// All Cache methods are accessible via Deref.
#[derive(Clone)]
pub struct UserLevelCache(Cache);

impl Deref for UserLevelCache {
    type Target = Cache;
    fn deref(&self) -> &Cache {
        &self.0
    }
}

impl UserLevelCache {
    /// Wraps an existing [`Cache`] instance as a user-level cache.
    #[must_use]
    pub fn from_cache(cache: Cache) -> Self {
        Self(cache)
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
        Cache::open(
            root,
            &[CacheDomainConfig {
                domain: "default".to_string(),
                index_file_name: index_file_name.to_string(),
                entry_ttl_seconds,
            }],
        )
        .await
        .map(Self)
    }

    /// Opens a user-level cache for the duration of `body` and shuts it down
    /// before returning.
    ///
    /// The cache is opened as a loop-starting cache. After `body` resolves,
    /// [`Cache::shutdown`] is awaited so background maintenance and any
    /// dispatched index writes are quiesced before the cache drops.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when opening the cache fails.
    pub async fn with_open<R, F>(
        root: &Path,
        index_file_name: &str,
        entry_ttl_seconds: u64,
        body: F,
    ) -> Result<R, ConductorError>
    where
        F: for<'a> FnOnce(&'a Self) -> std::pin::Pin<Box<dyn std::future::Future<Output = R> + 'a>>,
    {
        let cache = Self::open(root, index_file_name, entry_ttl_seconds).await?;
        let result = body(&cache).await;
        cache.shutdown().await;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::{UserLevelCache, default_mediapm_user_download_cache_root};

    /// Protects crate-level cache roots so conductor and mediapm resolve to
    /// distinct base directories with the same flat `cache/` layout.
    #[test]
    fn default_cache_roots_use_flat_cache_layout() {
        let mediapm_root = default_mediapm_user_download_cache_root();
        if let Some(mediapm_root) = mediapm_root {
            assert!(
                mediapm_root.ends_with("cache"),
                "mediapm root must end with 'cache', got: {}",
                mediapm_root.display()
            );
            let mediapm_parent = mediapm_root.parent().unwrap();
            assert!(
                mediapm_parent.ends_with("mediapm"),
                "mediapm cache base must be 'mediapm', got: {}",
                mediapm_parent.display()
            );
        }
    }

    /// The scoped user-level helper opens a loop-starting cache, runs the body,
    /// drains on return, and propagates open errors.
    #[tokio::test]
    async fn user_level_cache_with_open_runs_body_drains_and_propagates_open_error() {
        let root = mediapm_utils::temp::cache_dir().expect("cache dir");
        let value = UserLevelCache::with_open(root.path(), "tools.json", 3600, |cache| {
            Box::pin(async move {
                cache.store_bytes("default", "scoped-key", b"scoped").await;
                cache.lookup_bytes("default", "scoped-key").await
            })
        })
        .await
        .expect("with_open");
        assert_eq!(value, Some(b"scoped".to_vec()));

        let store_path = root.path().join("blocked-store");
        std::fs::create_dir_all(&store_path).expect("create dir");
        let blocked = store_path.join("store");
        std::fs::write(&blocked, b"not a directory").expect("create blocking file");
        let result =
            UserLevelCache::with_open(&store_path, "tools.json", 3600, |_cache| Box::pin(async {}))
                .await;
        assert!(result.is_err(), "open failure must propagate from scoped helper");
    }
}
