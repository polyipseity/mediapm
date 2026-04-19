//! User-scoped global directory layout for `mediapm`.
//!
//! This module centralizes where cross-workspace, user-owned `mediapm`
//! artifacts live. The location is intentionally data-persistent (app-data)
//! rather than cache-only so future global features can share one stable root.

use std::path::PathBuf;

/// Canonical global directory paths for one user profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaPmGlobalPaths {
    /// Root directory for user-scoped global `mediapm` data.
    pub root_dir: PathBuf,
    /// Root directory for global tool-download cache data.
    pub tool_cache_dir: PathBuf,
    /// CAS payload store directory for tool-cache objects.
    pub tool_cache_store_dir: PathBuf,
    /// JSONC index file for cache-key metadata.
    pub tool_cache_index_jsonc: PathBuf,
}

impl MediaPmGlobalPaths {
    /// Builds canonical global paths from one persistent app-data base path.
    #[must_use]
    pub fn from_data_base_dir(data_base_dir: impl Into<PathBuf>) -> Self {
        let root_dir = data_base_dir.into().join("mediapm");
        let tool_cache_dir = root_dir.join("tool-cache");

        Self {
            tool_cache_store_dir: tool_cache_dir.join("store"),
            tool_cache_index_jsonc: tool_cache_dir.join("index.jsonc"),
            root_dir,
            tool_cache_dir,
        }
    }

    /// Resolves default global-directory paths for the current user profile.
    #[must_use]
    pub fn resolve_default() -> Option<Self> {
        default_global_data_base_dir().map(Self::from_data_base_dir)
    }
}

/// Resolves platform-specific persistent app-data base directory.
///
/// Platform policy:
/// - Windows: `%APPDATA%` (roaming profile) with sane fallback,
/// - macOS: `$HOME/Library/Application Support`,
/// - other Unix-like: `$XDG_DATA_HOME` or `$HOME/.local/share`.
#[must_use]
fn default_global_data_base_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        env_path("APPDATA")
            .or_else(|| env_path("USERPROFILE").map(|home| home.join("AppData").join("Roaming")))
    }

    #[cfg(target_os = "macos")]
    {
        env_path("HOME").map(|home| home.join("Library").join("Application Support"))
    }

    #[cfg(all(not(windows), not(target_os = "macos")))]
    {
        env_path("XDG_DATA_HOME")
            .or_else(|| env_path("HOME").map(|home| home.join(".local").join("share")))
    }
}

/// Returns one trimmed environment-variable path when available.
#[must_use]
fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::MediaPmGlobalPaths;

    /// Ensures derived global paths keep the documented tool-cache layout.
    #[test]
    fn from_data_base_dir_uses_global_tool_cache_layout() {
        let base = PathBuf::from("/tmp/app-data");
        let paths = MediaPmGlobalPaths::from_data_base_dir(&base);

        assert_eq!(paths.root_dir, base.join("mediapm"));
        assert_eq!(paths.tool_cache_dir, base.join("mediapm").join("tool-cache"));
        assert_eq!(
            paths.tool_cache_store_dir,
            base.join("mediapm").join("tool-cache").join("store")
        );
        assert_eq!(
            paths.tool_cache_index_jsonc,
            base.join("mediapm").join("tool-cache").join("index.jsonc")
        );
    }
}
