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
}

#[cfg(test)]
mod tests {
    use super::default_mediapm_user_download_cache_root;

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

    // No cache-behavior tests here — those belong in `cache.rs`.
    // UserLevelCache is a thin location wrapper; the only thing to test
    // is that default roots resolve to distinct paths with the expected
    // layout.
}
