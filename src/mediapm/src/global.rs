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

use mediapm_conductor::cache::{Cache, CacheDomainConfig};
use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
use mediapm_conductor::error::ConductorError;

/// User-agent string sent in HTTP requests by mediapm tools/downloaders.
#[allow(dead_code)]
pub const MEDIAPM_USER_AGENT: &str = concat!("mediapm/", env!("CARGO_PKG_VERSION"));

/// Git hash of the mediapm build, embedded at compile time.
/// Used as the canonical version for builtin tools (media-tagger).
/// Empty (`""`) when git was unavailable at build time.
pub const MEDIAPM_GIT_HASH: &str = env!("MEDIAPM_GIT_HASH");

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

/// Domain configuration for the global tool download cache.
///
/// Mirrors the sync provisioning layout (`conductor_bridge::sync`): a `tools`
/// domain (7-day TTL) plus a `tool_metadata` domain (1-day TTL).
fn global_tool_cache_domains() -> Vec<CacheDomainConfig> {
    vec![
        CacheDomainConfig {
            domain: "tools".to_string(),
            index_file_name: "tools.json".to_string(),
            entry_ttl_seconds: mediapm_conductor::cache::ENTRY_TTL_SECONDS,
        },
        CacheDomainConfig {
            domain: "tool_metadata".to_string(),
            index_file_name: "tool_metadata.json".to_string(),
            entry_ttl_seconds: 24 * 60 * 60,
        },
    ]
}

/// Returns the status of the global tool cache.
///
/// Opens the user-level cache **without** starting the 24-hour background
/// prune loop (a short-lived CLI must not spawn a lingering thread), then
/// reports the real entry counts from each domain index plus the cache
/// root/store/index paths.
///
/// When `cache_root_override` is `Some`, the cache is opened at that root
/// instead of the resolved default (used by hermetic tests).
///
/// # Errors
///
/// Returns [`ConductorError`] if the global cache root cannot be resolved or
/// the cache cannot be opened.
pub async fn global_tool_cache_status(
    cache_root_override: Option<&Path>,
) -> Result<GlobalToolCacheStatus, ConductorError> {
    let paths = match cache_root_override {
        Some(root) => MediaPmGlobalPaths::from_tool_cache_dir(root),
        None => MediaPmGlobalPaths::resolve_default().ok_or_else(|| {
            ConductorError::Workflow("cannot resolve global cache root".to_string())
        })?,
    };
    let cache =
        Cache::open_without_background(&paths.tool_cache_dir, &global_tool_cache_domains()).await?;
    let mut entry_count = 0u64;
    for domain in ["tools", "tool_metadata"] {
        entry_count = entry_count.saturating_add(cache.entry_count(domain) as u64);
    }
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
/// Opens the user-level cache **without** starting the 24-hour background
/// prune loop, then performs an immediate prune (bypassing the automatic
/// cooldown) across both cache domains and reports the removed entry and
/// payload counts.
///
/// When `cache_root_override` is `Some`, the cache is opened at that root
/// instead of the resolved default (used by hermetic tests).
///
/// # Errors
///
/// Returns [`ConductorError`] if the global cache root cannot be resolved or
/// the cache cannot be opened.
pub async fn global_tool_cache_prune_expired(
    cache_root_override: Option<&Path>,
) -> Result<GlobalToolCachePruneSummary, ConductorError> {
    let paths = match cache_root_override {
        Some(root) => MediaPmGlobalPaths::from_tool_cache_dir(root),
        None => MediaPmGlobalPaths::resolve_default().ok_or_else(|| {
            ConductorError::Workflow("cannot resolve global cache root".to_string())
        })?,
    };
    let cache =
        Cache::open_without_background(&paths.tool_cache_dir, &global_tool_cache_domains()).await?;
    let mut removed_entries = 0usize;
    let mut removed_payloads = 0usize;
    for domain in ["tools", "tool_metadata"] {
        let report = cache.prune_expired_immediate(domain).await?;
        removed_entries = removed_entries.saturating_add(report.removed_entries);
        removed_payloads = removed_payloads.saturating_add(report.removed_payloads);
    }
    Ok(GlobalToolCachePruneSummary { removed_entries, removed_payloads })
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
    use std::path::{Path, PathBuf};

    use super::{
        MEDIAPM_GIT_HASH, MediaPmGlobalPaths, global_tool_cache_domains,
        global_tool_cache_prune_expired, global_tool_cache_status,
    };

    use mediapm_conductor::cache::{Cache, CacheDomainConfig};

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

    #[test]
    fn mediapm_git_hash_is_defined() {
        // The constant exists and compiles; it may be "" in environments
        // without .git at build time. Just verify it doesn't panic.
        let _ = MEDIAPM_GIT_HASH;
    }

    #[tokio::test]
    async fn global_tool_cache_status_reports_real_entry_count() {
        let root = mediapm_utils::temp::cache_dir().expect("cache dir");
        // Seed the cache with N entries via the real Cache engine.
        let cache = Cache::open_without_background(
            root.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: mediapm_conductor::cache::ENTRY_TTL_SECONDS,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("open cache");
        cache.store_bytes("tools", "tool-a", b"a").await;
        cache.store_bytes("tools", "tool-b", b"b").await;
        cache.store_bytes("tool_metadata", "meta-a", b"m").await;
        drop(cache);

        let status = global_tool_cache_status(Some(root.path())).await.expect("status");
        assert_eq!(status.entry_count, 3, "status must report the seeded entry count");
    }

    #[tokio::test]
    async fn global_tool_cache_prune_expired_removes_expired_entries() {
        let root = mediapm_utils::temp::cache_dir().expect("cache dir");
        // Seed entries via the real global domains (7d / 1d TTL). Freshly
        // stored entries are NOT expired, so we backdate the on-disk index
        // rows for two of them to simulate staleness before pruning.
        let cache = Cache::open_without_background(root.path(), &global_tool_cache_domains())
            .await
            .expect("open cache");
        cache.store_bytes("tools", "expired-tool", b"x").await;
        cache.store_bytes("tool_metadata", "expired-meta", b"y").await;
        // A fresh entry in the 7-day `tools` domain must survive the prune.
        cache.store_bytes("tools", "fresh-tool", b"z").await;
        drop(cache);

        // Backdate only the target entries so they fall outside the domain
        // TTL window, leaving the fresh entry untouched.
        backdate_index_entries(
            &root.path().join("tools.json"),
            8 * 24 * 60 * 60,
            &["expired-tool"],
        );
        backdate_index_entries(
            &root.path().join("tool_metadata.json"),
            2 * 24 * 60 * 60,
            &["expired-meta"],
        );

        let summary = global_tool_cache_prune_expired(Some(root.path())).await.expect("prune");
        assert!(
            summary.removed_entries >= 2,
            "prune must remove the two expired entries, got {}",
            summary.removed_entries
        );
        assert!(
            summary.removed_payloads >= 2,
            "prune must remove the two unreferenced payloads, got {}",
            summary.removed_payloads
        );

        // After prune, the fresh entry remains.
        let status = global_tool_cache_status(Some(root.path())).await.expect("status");
        assert_eq!(status.entry_count, 1, "fresh entry must survive the prune");
    }

    /// Rewrites an index file so the named entries' `last_access_unix_seconds`
    /// are shifted backwards by `age_seconds`, simulating staleness for prune
    /// tests. Entries not named are left untouched.
    fn backdate_index_entries(index_path: &Path, age_seconds: u64, keys: &[&str]) {
        use std::io::Write;

        let raw = std::fs::read_to_string(index_path).expect("read index");
        let mut value: serde_json::Value = serde_json::from_str(&raw).expect("parse index json");
        let now = mediapm_utils::Timestamp::now().as_unix_secs();
        if let Some(entries) = value.get_mut("entries").and_then(|e| e.as_object_mut()) {
            for key in keys {
                if let Some(entry) = entries.get_mut(*key).and_then(|e| e.as_object_mut()) {
                    entry.insert(
                        "last_access_unix_seconds".to_string(),
                        serde_json::Value::Number((now.saturating_sub(age_seconds)).into()),
                    );
                }
            }
        }
        let rendered = serde_json::to_string_pretty(&value).expect("re-encode index");
        let mut file = std::fs::File::create(index_path).expect("rewrite index");
        file.write_all(rendered.as_bytes()).expect("write index");
    }
}
