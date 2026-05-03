//! User-scoped global directory layout for `mediapm`.
//!
//! This module centralizes where cross-workspace, user-owned `mediapm`
//! cache artifacts live.
//!
//! Cache layout under the resolved root:
//! - `<root>/cache/store/` — CAS payload objects
//! - `<root>/cache/tools.jsonc` — default managed-tool metadata index
//!
//! Root resolution is delegated to shared conductor cache-root policy so
//! `mediapm` runtime provisioning and global cache CLI commands always inspect
//! the same base directory.

use std::path::{Path, PathBuf};

use mediapm_conductor::default_mediapm_user_download_cache_root;

/// Canonical global directory paths for one user profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaPmGlobalPaths {
    /// Root directory for user-scoped global `mediapm` data.
    pub root_dir: PathBuf,
    /// Root directory for global cache data (`<root_dir>/cache`).
    pub tool_cache_dir: PathBuf,
    /// CAS payload store directory for cache objects.
    pub tool_cache_store_dir: PathBuf,
    /// Default managed-tool JSONC index file path.
    pub tool_cache_index_jsonc: PathBuf,
}

impl MediaPmGlobalPaths {
    /// Builds canonical global paths from one OS cache-base directory.
    ///
    /// The resulting layout is:
    /// - `<base>/mediapm/cache/store/`
    /// - `<base>/mediapm/cache/tools.jsonc`
    #[must_use]
    pub fn from_cache_base_dir(cache_base_dir: impl Into<PathBuf>) -> Self {
        let root_dir = cache_base_dir.into().join("mediapm");
        let tool_cache_dir = root_dir.join("cache");

        Self::from_tool_cache_dir_with_root(tool_cache_dir, root_dir)
    }

    /// Builds canonical global paths from one data-base directory.
    ///
    /// This alias keeps legacy constructor naming available while using the
    /// cache-root layout.
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
        let tool_cache_index_jsonc = tool_cache_dir.join("tools.jsonc");

        Self { root_dir, tool_cache_dir, tool_cache_store_dir, tool_cache_index_jsonc }
    }

    /// Resolves default global-directory paths for the current user profile.
    #[must_use]
    pub fn resolve_default() -> Option<Self> {
        default_mediapm_user_download_cache_root().map(Self::from_tool_cache_dir)
    }
}

/// Infers the mediapm global root from one resolved tool-cache path.
///
/// The cache directory is one level below the root: `<root>/cache`.
#[must_use]
fn infer_root_dir_from_tool_cache_dir(tool_cache_dir: &Path) -> PathBuf {
    tool_cache_dir.parent().map_or_else(|| tool_cache_dir.to_path_buf(), Path::to_path_buf)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::MediaPmGlobalPaths;

    /// Ensures derived global paths keep the documented flat cache layout.
    #[test]
    fn from_cache_base_dir_uses_flat_cache_layout() {
        let base = PathBuf::from("/tmp/cache-base");
        let paths = MediaPmGlobalPaths::from_cache_base_dir(&base);

        assert_eq!(paths.root_dir, base.join("mediapm"));
        assert_eq!(paths.tool_cache_dir, base.join("mediapm").join("cache"));
        assert_eq!(paths.tool_cache_store_dir, paths.tool_cache_dir.join("store"));
        assert_eq!(paths.tool_cache_index_jsonc, paths.tool_cache_dir.join("tools.jsonc"));
    }

    /// Ensures tool-cache-root constructor infers root as one level above cache.
    #[test]
    fn from_tool_cache_dir_infers_root_dir_one_level_up() {
        let tool_cache_dir = PathBuf::from("/tmp/cache-base/mediapm/cache");
        let paths = MediaPmGlobalPaths::from_tool_cache_dir(&tool_cache_dir);

        assert_eq!(paths.root_dir, PathBuf::from("/tmp/cache-base/mediapm"));
        assert_eq!(paths.tool_cache_dir, tool_cache_dir);
        assert_eq!(paths.tool_cache_store_dir, paths.tool_cache_dir.join("store"));
        assert_eq!(paths.tool_cache_index_jsonc, paths.tool_cache_dir.join("tools.jsonc"));
    }
}
