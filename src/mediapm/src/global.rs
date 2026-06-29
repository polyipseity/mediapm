//! User-scoped global directory layout for `mediapm`.
//!
//! This module centralizes where cross-workspace, user-owned `mediapm` cache
//! artifacts live.
//!
//! Cache layout under the resolved root:
//! - `<root>/cache/store/` — CAS payload objects
//! - `<root>/cache/tools.json` — default managed-tool metadata index
//!
//! This user-level managed-download cache is intentionally separate from the
//! workspace conductor tool-content cache (`<mediapm_dir>/tools/` for
//! mediapm-driven runs, `<conductor_dir>/tools/` for standalone conductor).
//! These cache domains must never be treated as interchangeable paths.

use std::path::{Path, PathBuf};

use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;

/// User-agent string sent in HTTP requests by mediapm tools/downloaders.
pub const MEDIAPM_USER_AGENT: &str = concat!("mediapm/", env!("CARGO_PKG_VERSION"));

/// Canonical global directory paths for one user profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaPmGlobalPaths {
    /// Root directory for user-scoped global `mediapm` data.
    pub root_dir: PathBuf,
    /// Root directory for global cache data (`<root_dir>/cache`).
    pub tool_cache_dir: PathBuf,
    /// CAS payload store directory for cache objects.
    pub tool_cache_store_dir: PathBuf,
    /// Default managed-tool JSON index file path.
    pub tool_cache_index: PathBuf,
}

impl MediaPmGlobalPaths {
    /// Builds canonical global paths from one OS cache-base directory.
    ///
    /// The resulting layout is:
    /// - `<base>/mediapm/cache/store/`
    /// - `<base>/mediapm/cache/tools.json`
    #[must_use]
    pub fn from_cache_base_dir(cache_base_dir: impl Into<PathBuf>) -> Self {
        let root_dir = cache_base_dir.into().join("mediapm");
        let tool_cache_dir = root_dir.join("cache");
        Self::from_tool_cache_dir_with_root(tool_cache_dir, root_dir)
    }

    /// Builds canonical global paths from one data-base directory.
    ///
    /// This alias keeps legacy constructor naming.
    #[must_use]
    pub fn from_data_base_dir(data_base_dir: impl Into<PathBuf>) -> Self {
        Self::from_cache_base_dir(data_base_dir)
    }

    /// Builds canonical global paths from one resolved tool-cache root.
    #[must_use]
    pub fn from_tool_cache_dir(tool_cache_dir: impl Into<PathBuf>) -> Self {
        let tool_cache_dir = tool_cache_dir.into();
        let root_dir = infer_root_dir_from_tool_cache_dir(&tool_cache_dir);
        Self::from_tool_cache_dir_with_root(tool_cache_dir, root_dir)
    }

    /// Builds canonical global paths from explicit root and tool-cache paths.
    fn from_tool_cache_dir_with_root(tool_cache_dir: PathBuf, root_dir: PathBuf) -> Self {
        let tool_cache_store_dir = tool_cache_dir.join("store");
        let tool_cache_index = tool_cache_dir.join("tools.json");
        Self { root_dir, tool_cache_dir, tool_cache_store_dir, tool_cache_index }
    }

    /// Resolves default global-directory paths for the current user profile.
    #[must_use]
    pub fn resolve_default() -> Option<Self> {
        default_mediapm_user_download_cache_root().map(Self::from_tool_cache_dir)
    }
}

/// Infers the mediapm global root from a resolved tool-cache path.
///
/// The cache directory is one level below the root: `<root>/cache`.
#[must_use]
fn infer_root_dir_from_tool_cache_dir(tool_cache_dir: &Path) -> PathBuf {
    tool_cache_dir.parent().map_or_else(|| tool_cache_dir.to_path_buf(), Path::to_path_buf)
}

/// Ensures that the global directory layout exists on disk.
///
/// # Errors
///
/// Returns `std::io::Error` if directory creation fails or the global cache
/// root cannot be resolved.
pub fn ensure_global_directory_layout() -> Result<(), std::io::Error> {
    let paths = MediaPmGlobalPaths::resolve_default().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "cannot resolve global cache root")
    })?;
    std::fs::create_dir_all(&paths.tool_cache_store_dir)?;
    Ok(())
}

/// Returns the status of the global tool cache.
///
/// # Errors
///
/// Returns `std::io::Error` if the global cache root cannot be resolved.
pub fn global_tool_cache_status() -> Result<GlobalToolCacheStatus, std::io::Error> {
    let paths = MediaPmGlobalPaths::resolve_default().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "cannot resolve global cache root")
    })?;
    let entry_count = if paths.tool_cache_index.is_file() {
        // TODO: Parse the actual tools.json index to count entries.
        0
    } else {
        0
    };
    Ok(GlobalToolCacheStatus {
        tool_cache_dir: paths.tool_cache_dir,
        store_dir: paths.tool_cache_store_dir,
        index: paths.tool_cache_index,
        entry_count,
    })
}

/// Status of the global tool cache.
///
/// Fields match the PLAN.md specification: each path is a resolved
/// `PathBuf` so callers can inspect existence/readiness via
/// `.is_dir()` / `.is_file()` themselves.
#[derive(Debug, Clone)]
pub struct GlobalToolCacheStatus {
    /// Root cache directory (`<root>/cache`).
    pub tool_cache_dir: PathBuf,
    /// CAS payload subdirectory (`<tool_cache_dir>/store`).
    pub store_dir: PathBuf,
    /// Metadata index file path (`<tool_cache_dir>/tools.json`).
    pub index: PathBuf,
    /// Number of entries in the tool cache index.
    pub entry_count: u64,
}

/// Prunes expired entries from the global tool cache.
///
/// # Errors
///
/// Returns `std::io::Error` if the cache root cannot be resolved.
pub fn global_tool_cache_prune_expired() -> Result<GlobalToolCachePruneSummary, std::io::Error> {
    // TODO: Implement actual TTL-based pruning
    Ok(GlobalToolCachePruneSummary { removed_entries: 0, removed_payloads: 0 })
}

/// Summary of global tool cache pruning.
#[derive(Debug, Clone)]
pub struct GlobalToolCachePruneSummary {
    /// Number of entries removed.
    pub removed_entries: usize,
    /// Number of payload files removed.
    pub removed_payloads: usize,
}

/// Clears the global tool cache entirely.
///
/// # Errors
///
/// Returns `std::io::Error` if the cache cannot be cleared or the root
/// cannot be resolved.
pub fn global_tool_cache_clear() -> Result<(), std::io::Error> {
    let paths = MediaPmGlobalPaths::resolve_default().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "cannot resolve global cache root")
    })?;
    if paths.tool_cache_dir.is_dir() {
        std::fs::remove_dir_all(&paths.tool_cache_dir)?;
    }
    std::fs::create_dir_all(&paths.tool_cache_store_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::MediaPmGlobalPaths;

    #[test]
    fn from_cache_base_dir_uses_flat_cache_layout() {
        let base = PathBuf::from("/tmp/cache-base");
        let paths = MediaPmGlobalPaths::from_cache_base_dir(&base);
        assert_eq!(paths.root_dir, base.join("mediapm"));
        assert_eq!(paths.tool_cache_dir, base.join("mediapm").join("cache"));
        assert_eq!(paths.tool_cache_store_dir, paths.tool_cache_dir.join("store"));
        assert_eq!(paths.tool_cache_index, paths.tool_cache_dir.join("tools.json"));
    }

    #[test]
    fn from_tool_cache_dir_infers_root_dir_one_level_up() {
        let tool_cache_dir = PathBuf::from("/tmp/cache-base/mediapm/cache");
        let paths = MediaPmGlobalPaths::from_tool_cache_dir(&tool_cache_dir);
        assert_eq!(paths.root_dir, PathBuf::from("/tmp/cache-base/mediapm"));
        assert_eq!(paths.tool_cache_dir, tool_cache_dir);
        assert_eq!(paths.tool_cache_store_dir, paths.tool_cache_dir.join("store"));
        assert_eq!(paths.tool_cache_index, paths.tool_cache_dir.join("tools.json"));
    }
}
