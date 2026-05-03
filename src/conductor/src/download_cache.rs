//! Shared user-level tool download cache primitives.
//!
//! This module provides two layers:
//! - namespaced default user-cache roots for crate-level separation,
//! - one CAS-backed logical-key cache engine reusable across crates.
//!
//! The cache layout is intentionally stable and extensible under one
//! user-scoped cache base directory:
//! - `<namespace>/tools/store/` for CAS payload objects,
//! - `<namespace>/tools/*.jsonc` for one or more key-to-hash metadata indexes.
//!
//! Eviction policy is fixed: entries older than 30 days are removed
//! automatically. This keeps cross-workspace reuse effective without
//! unbounded growth.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use serde::{Deserialize, Serialize};

use crate::error::ConductorError;

/// Current on-disk index format marker.
const USER_TOOL_CACHE_INDEX_VERSION: u32 = 1;
/// Default metadata index file used for managed tool payload rows.
const USER_TOOL_CACHE_DEFAULT_INDEX_FILE_NAME: &str = "tools.jsonc";

/// Fixed entry TTL for automatic cache eviction.
pub const USER_TOOL_CACHE_ENTRY_TTL_SECONDS: u64 = 30 * 24 * 60 * 60;

/// Minimum interval between full prune scans.
const USER_TOOL_CACHE_PRUNE_INTERVAL_SECONDS: u64 = 24 * 60 * 60;

/// Minimum interval between persisted access-timestamp updates for unchanged
/// cache keys.
const USER_TOOL_CACHE_TOUCH_PERSIST_INTERVAL_SECONDS: u64 = 5 * 60;

/// User-level cache namespace selector.
///
/// This separates conductor-owned and mediapm-owned cache roots while keeping
/// one shared implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserToolCacheNamespace {
    /// Conductor-managed user cache namespace.
    Conductor,
    /// Mediapm-managed user cache namespace.
    Mediapm,
}

impl UserToolCacheNamespace {
    /// Returns stable directory name used for this namespace.
    #[must_use]
    pub const fn directory_name(self) -> &'static str {
        match self {
            Self::Conductor => "conductor",
            Self::Mediapm => "mediapm",
        }
    }
}

/// Returns whether shared user-level download cache should be used.
///
/// Absent configuration defaults to enabled.
#[must_use]
pub const fn use_user_download_cache_enabled(configured_value: Option<bool>) -> bool {
    match configured_value {
        Some(value) => value,
        None => true,
    }
}

/// Returns default user-scoped global cache root for managed tool downloads.
///
/// This is the conductor namespace default and resolves to:
/// - Windows: `%LOCALAPPDATA%/mediapm/conductor/tools`
/// - macOS: `$HOME/Library/Caches/mediapm/conductor/tools`
/// - Linux/Unix: `$XDG_CACHE_HOME/mediapm/conductor/tools`
///   (fallback `$HOME/.cache/mediapm/conductor/tools`)
#[must_use]
pub fn default_user_download_cache_root() -> Option<PathBuf> {
    default_user_download_cache_root_for(UserToolCacheNamespace::Conductor)
}

/// Returns default user-scoped global cache root for `mediapm` tool downloads.
///
/// Path policy:
/// - Windows: `%LOCALAPPDATA%/mediapm/mediapm/tools`
/// - macOS: `$HOME/Library/Caches/mediapm/mediapm/tools`
/// - Linux/Unix: `$XDG_CACHE_HOME/mediapm/mediapm/tools`
///   (fallback `$HOME/.cache/mediapm/mediapm/tools`)
#[must_use]
pub fn default_mediapm_user_download_cache_root() -> Option<PathBuf> {
    default_user_download_cache_root_for(UserToolCacheNamespace::Mediapm)
}

/// Returns default user-scoped global cache root for one namespace.
#[must_use]
pub fn default_user_download_cache_root_for(namespace: UserToolCacheNamespace) -> Option<PathBuf> {
    dirs::cache_dir().map(|root| root.join("mediapm").join(namespace.directory_name()).join("tools"))
}

/// Summary of one cache-prune operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UserDownloadCachePruneReport {
    /// Number of logical key entries removed from index metadata.
    pub removed_entries: usize,
    /// Number of CAS payload objects removed from `store/`.
    pub removed_payloads: usize,
}

/// User-scoped managed-tool payload cache backed by CAS object storage.
#[derive(Clone)]
pub struct UserDownloadCache {
    /// Shared CAS store that persists cached payload bytes.
    cas: Arc<FileSystemCas>,
    /// Root directory containing `store/` and one or more JSONC index files.
    cache_root: PathBuf,
    /// Path to one JSONC metadata index file.
    index_path: PathBuf,
    /// In-memory index guarded for concurrent downloader worker access.
    index: Arc<Mutex<UserDownloadCacheIndex>>,
}

impl UserDownloadCache {
    /// Opens (or bootstraps) one cache root with default index file.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails.
    pub async fn open(tool_cache_root: &Path) -> Result<Self, ConductorError> {
        Self::open_with_index_file_name(tool_cache_root, USER_TOOL_CACHE_DEFAULT_INDEX_FILE_NAME)
            .await
    }

    /// Opens one cache root and binds this handle to one index file.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails.
    pub async fn open_with_index_file_name(
        tool_cache_root: &Path,
        index_file_name: &str,
    ) -> Result<Self, ConductorError> {
        fs::create_dir_all(tool_cache_root).map_err(|source| ConductorError::Io {
            operation: "creating user download cache root".to_string(),
            path: tool_cache_root.to_path_buf(),
            source,
        })?;

        let store_dir = tool_cache_root.join("store");
        fs::create_dir_all(&store_dir).map_err(|source| ConductorError::Io {
            operation: "creating user download cache store directory".to_string(),
            path: store_dir.clone(),
            source,
        })?;

        let cas = FileSystemCas::open(&store_dir).await.map_err(|source| {
            ConductorError::Workflow(format!(
                "opening user download cache CAS store at '{}' failed: {source}",
                store_dir.display()
            ))
        })?;

        let normalized_index_file_name = normalize_index_file_name(index_file_name);
        let index_path = tool_cache_root.join(normalized_index_file_name);
        let index = load_index_file(&index_path);
        if !index_path.exists() {
            let _ = write_index_file(&index_path, &index);
        }

        Ok(Self {
            cas: Arc::new(cas),
            cache_root: tool_cache_root.to_path_buf(),
            index_path,
            index: Arc::new(Mutex::new(index)),
        })
    }

    /// Looks up cached payload bytes for one logical download key.
    ///
    /// Corrupt or stale key rows are treated as cache misses and cleaned up
    /// lazily so execution can continue without hard failures.
    #[must_use]
    pub async fn lookup_bytes(&self, cache_key: &str) -> Option<Vec<u8>> {
        let entry = {
            let index = self.index.lock().ok()?;
            index.entries.get(cache_key).cloned()
        }?;

        let Ok(hash) = Hash::from_str(entry.hash.trim()) else {
            self.remove_index_entry(cache_key);
            return None;
        };

        let Ok(bytes) = self.cas.get(hash).await else {
            self.remove_index_entry(cache_key);
            return None;
        };

        self.touch_index_entry(cache_key, hash);
        Some(bytes.to_vec())
    }

    /// Stores payload bytes under one logical download key.
    ///
    /// Write failures are intentionally tolerated so provisioning can continue
    /// even when cache persistence is temporarily unavailable.
    pub async fn store_bytes(&self, cache_key: &str, payload: &[u8]) {
        if payload.is_empty() {
            return;
        }

        let Ok(hash) = self.cas.put(payload.to_vec()).await else {
            return;
        };

        self.touch_index_entry(cache_key, hash);
    }

    /// Returns current number of logical cache-key rows in index metadata.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.index.lock().map(|index| index.entries.len()).unwrap_or(0)
    }

    /// Removes expired index rows and their unreferenced CAS payloads.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when index locking or persistence fails.
    pub async fn prune_expired_entries(
        &self,
    ) -> Result<UserDownloadCachePruneReport, ConductorError> {
        let now = now_unix_seconds();
        let cutoff = now.saturating_sub(USER_TOOL_CACHE_ENTRY_TTL_SECONDS);

        let (expired_keys, expired_hashes) = {
            let mut index = self.index.lock().map_err(|_| {
                ConductorError::Internal(
                    "locking user download cache index mutex failed".to_string(),
                )
            })?;

            if now.saturating_sub(index.last_prune_unix_seconds)
                < USER_TOOL_CACHE_PRUNE_INTERVAL_SECONDS
            {
                return Ok(UserDownloadCachePruneReport::default());
            }

            index.last_prune_unix_seconds = now;

            let mut expired_keys = Vec::new();
            let mut expired_hashes = Vec::new();

            for (cache_key, entry) in &index.entries {
                if entry.last_access_unix_seconds <= cutoff {
                    expired_keys.push(cache_key.clone());
                    expired_hashes.push(entry.hash.clone());
                }
            }

            if !expired_keys.is_empty() {
                for key in &expired_keys {
                    index.entries.remove(key);
                }
            }

            write_index_file(&self.index_path, &index)?;

            (expired_keys, expired_hashes)
        };

        if expired_keys.is_empty() {
            return Ok(UserDownloadCachePruneReport::default());
        }

        let active_hash_union = collect_referenced_hashes_from_jsonc_indexes(&self.cache_root);
        let mut removed_payloads = 0usize;
        for hash_text in expired_hashes {
            if active_hash_union.contains(&hash_text) {
                continue;
            }

            let Ok(hash) = Hash::from_str(hash_text.trim()) else {
                continue;
            };

            if self.cas.exists(hash).await.unwrap_or(false) && self.cas.delete(hash).await.is_ok() {
                removed_payloads = removed_payloads.saturating_add(1);
            }
        }

        Ok(UserDownloadCachePruneReport { removed_entries: expired_keys.len(), removed_payloads })
    }

    /// Removes one key row from cache index metadata.
    fn remove_index_entry(&self, cache_key: &str) {
        let Ok(mut index) = self.index.lock() else {
            return;
        };
        if index.entries.remove(cache_key).is_some() {
            let _ = write_index_file(&self.index_path, &index);
        }
    }

    /// Upserts one key row in cache index metadata and bumps access timestamp.
    fn touch_index_entry(&self, cache_key: &str, hash: Hash) {
        let Ok(mut index) = self.index.lock() else {
            return;
        };

        let now = now_unix_seconds();
        let hash_text = hash.to_string();
        let mut should_persist = true;

        if let Some(existing) = index.entries.get_mut(cache_key) {
            let hash_changed = existing.hash != hash_text;
            existing.hash = hash_text;
            let elapsed = now.saturating_sub(existing.last_access_unix_seconds);
            existing.last_access_unix_seconds = now;
            should_persist =
                hash_changed || elapsed >= USER_TOOL_CACHE_TOUCH_PERSIST_INTERVAL_SECONDS;
        } else {
            index.entries.insert(
                cache_key.to_string(),
                UserDownloadCacheIndexEntry { hash: hash_text, last_access_unix_seconds: now },
            );
        }

        if should_persist {
            let _ = write_index_file(&self.index_path, &index);
        }
    }
}

/// Returns a safe JSONC filename for one cache index.
#[must_use]
fn normalize_index_file_name(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return USER_TOOL_CACHE_DEFAULT_INDEX_FILE_NAME.to_string();
    }

    let candidate = Path::new(trimmed)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|name| !name.trim().is_empty())
        .unwrap_or(USER_TOOL_CACHE_DEFAULT_INDEX_FILE_NAME);

    if Path::new(candidate)
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("jsonc"))
    {
        candidate.to_string()
    } else {
        format!("{candidate}.jsonc")
    }
}

/// Persisted cache index file envelope (`*.jsonc`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct UserDownloadCacheIndex {
    /// Envelope version marker.
    version: u32,
    /// Last time a full prune scan completed (Unix seconds).
    #[serde(default)]
    last_prune_unix_seconds: u64,
    /// Cache metadata rows keyed by logical downloader identity key.
    #[serde(default)]
    entries: BTreeMap<String, UserDownloadCacheIndexEntry>,
}

impl Default for UserDownloadCacheIndex {
    fn default() -> Self {
        Self {
            version: USER_TOOL_CACHE_INDEX_VERSION,
            last_prune_unix_seconds: 0,
            entries: BTreeMap::new(),
        }
    }
}

/// One logical cache metadata row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct UserDownloadCacheIndexEntry {
    /// CAS multihash text pointing at payload bytes in `store/`.
    hash: String,
    /// Last successful read/write access timestamp (Unix seconds).
    last_access_unix_seconds: u64,
}

/// Loads one index file from disk, falling back to an empty index when absent
/// or malformed.
#[must_use]
fn load_index_file(index_path: &Path) -> UserDownloadCacheIndex {
    let Ok(raw) = fs::read_to_string(index_path) else {
        return UserDownloadCacheIndex::default();
    };

    let Ok(parsed) = serde_json::from_str::<UserDownloadCacheIndex>(&raw) else {
        return UserDownloadCacheIndex::default();
    };

    if parsed.version == USER_TOOL_CACHE_INDEX_VERSION {
        parsed
    } else {
        UserDownloadCacheIndex::default()
    }
}

/// Collects active hash references from all JSONC index files under one cache
/// root.
///
/// Malformed or version-incompatible index files are ignored so pruning stays
/// best-effort and never blocks tool provisioning.
#[must_use]
fn collect_referenced_hashes_from_jsonc_indexes(cache_root: &Path) -> BTreeSet<String> {
    let mut referenced = BTreeSet::new();

    let Ok(entries) = fs::read_dir(cache_root) else {
        return referenced;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let is_jsonc = path
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|extension| extension.eq_ignore_ascii_case("jsonc"));
        if !is_jsonc {
            continue;
        }

        let index = load_index_file(&path);
        for row in index.entries.values() {
            referenced.insert(row.hash.clone());
        }
    }

    referenced
}

/// Writes one index envelope to disk with replace-on-rename semantics.
fn write_index_file(index_path: &Path, index: &UserDownloadCacheIndex) -> Result<(), ConductorError> {
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
            operation: "creating user download cache index parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let rendered = serde_json::to_string_pretty(index).map_err(|error| {
        ConductorError::Serialization(format!("encoding user download cache index: {error}"))
    })?;
    let temp_path = index_path.with_extension("jsonc.tmp");
    fs::write(&temp_path, format!("{rendered}\n")).map_err(|source| ConductorError::Io {
        operation: "writing temporary user download cache index".to_string(),
        path: temp_path.clone(),
        source,
    })?;
    if index_path.exists() {
        let _ = fs::remove_file(index_path);
    }
    fs::rename(&temp_path, index_path).map_err(|source| ConductorError::Io {
        operation: "replacing user download cache index".to_string(),
        path: index_path.to_path_buf(),
        source,
    })?;

    Ok(())
}

/// Returns current Unix timestamp in seconds.
#[must_use]
fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::str::FromStr;

    use mediapm_cas::{CasApi, Hash};

    use super::{
        USER_TOOL_CACHE_ENTRY_TTL_SECONDS, USER_TOOL_CACHE_INDEX_VERSION, UserDownloadCache,
        UserDownloadCacheIndex, UserDownloadCacheIndexEntry, UserToolCacheNamespace,
        default_mediapm_user_download_cache_root, default_user_download_cache_root,
        default_user_download_cache_root_for, now_unix_seconds, use_user_download_cache_enabled,
    };

    /// Protects default toggle behavior by keeping omitted configuration enabled.
    #[test]
    fn use_user_download_cache_enabled_defaults_to_true() {
        assert!(use_user_download_cache_enabled(None));
        assert!(use_user_download_cache_enabled(Some(true)));
        assert!(!use_user_download_cache_enabled(Some(false)));
    }

    /// Protects crate-level cache separation by ensuring namespace roots differ.
    #[test]
    fn default_cache_roots_are_namespaced_per_crate() {
        let conductor_root = default_user_download_cache_root_for(UserToolCacheNamespace::Conductor);
        let mediapm_root = default_user_download_cache_root_for(UserToolCacheNamespace::Mediapm);

        if let (Some(conductor_root), Some(mediapm_root)) = (conductor_root, mediapm_root) {
            assert_ne!(conductor_root, mediapm_root);
            assert!(conductor_root.ends_with(Path::new("conductor").join("tools")));
            assert!(mediapm_root.ends_with(Path::new("mediapm").join("tools")));
            assert_eq!(default_user_download_cache_root(), Some(conductor_root));
            assert_eq!(default_mediapm_user_download_cache_root(), Some(mediapm_root));
        }
    }

    /// Protects shared-cache behavior by ensuring key-based round trips return
    /// the original payload bytes.
    #[test]
    fn cache_round_trips_bytes_by_logical_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build runtime");

        let cache = runtime.block_on(UserDownloadCache::open(root.path())).expect("open cache");

        let payload = b"shared-download-cache".to_vec();
        runtime.block_on(cache.store_bytes("tool=ffmpeg|version=8.2", &payload));

        assert_eq!(runtime.block_on(cache.lookup_bytes("tool=ffmpeg|version=8.2")), Some(payload));
        assert_eq!(cache.entry_count(), 1);
        assert!(root.path().join("tools.jsonc").exists());
    }

    /// Protects miss semantics so absent keys do not raise hard failures.
    #[test]
    fn cache_lookup_returns_none_for_unknown_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build runtime");

        let cache = runtime.block_on(UserDownloadCache::open(root.path())).expect("open cache");

        assert!(runtime.block_on(cache.lookup_bytes("missing-key")).is_none());
    }

    /// Protects fixed TTL eviction by pruning entries older than 30 days.
    #[test]
    fn prune_expired_entries_removes_old_rows() {
        let root = tempfile::tempdir().expect("tempdir");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build runtime");

        let cache = runtime.block_on(UserDownloadCache::open(root.path())).expect("open cache");
        runtime.block_on(cache.store_bytes("tool=yt-dlp|tag=latest", b"payload-a"));

        {
            let mut index = cache.index.lock().expect("index mutex");
            let stale_timestamp = now_unix_seconds()
                .saturating_sub(USER_TOOL_CACHE_ENTRY_TTL_SECONDS.saturating_add(1));
            if let Some(entry) = index.entries.get_mut("tool=yt-dlp|tag=latest") {
                entry.last_access_unix_seconds = stale_timestamp;
            }
            super::write_index_file(&cache.index_path, &index).expect("write stale index");
        }

        let report = runtime
            .block_on(cache.prune_expired_entries())
            .expect("prune expired entries should succeed");

        assert_eq!(report.removed_entries, 1);
        assert!(runtime.block_on(cache.lookup_bytes("tool=yt-dlp|tag=latest")).is_none());
    }

    /// Protects multi-index cache roots by ensuring payload prune decisions use
    /// hash references from every JSONC index file, not just `tools.jsonc`.
    #[test]
    fn prune_respects_hash_references_from_other_jsonc_indexes() {
        let root = tempfile::tempdir().expect("tempdir");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build runtime");

        let cache = runtime.block_on(UserDownloadCache::open(root.path())).expect("open cache");
        runtime.block_on(cache.store_bytes("tool=yt-dlp|tag=latest", b"payload-a"));

        let payload_hash_text = {
            let index = cache.index.lock().expect("index mutex");
            index.entries.get("tool=yt-dlp|tag=latest").expect("cache entry").hash.clone()
        };

        let secondary_index = UserDownloadCacheIndex {
            version: USER_TOOL_CACHE_INDEX_VERSION,
            last_prune_unix_seconds: 0,
            entries: BTreeMap::from([(
                "secondary=cover-art|id=demo".to_string(),
                UserDownloadCacheIndexEntry {
                    hash: payload_hash_text.clone(),
                    last_access_unix_seconds: now_unix_seconds(),
                },
            )]),
        };
        super::write_index_file(&root.path().join("secondary.jsonc"), &secondary_index)
            .expect("write secondary index");

        {
            let mut index = cache.index.lock().expect("index mutex");
            let stale_timestamp = now_unix_seconds()
                .saturating_sub(USER_TOOL_CACHE_ENTRY_TTL_SECONDS.saturating_add(1));
            if let Some(entry) = index.entries.get_mut("tool=yt-dlp|tag=latest") {
                entry.last_access_unix_seconds = stale_timestamp;
            }
            super::write_index_file(&cache.index_path, &index).expect("write stale index");
        }

        let report = runtime
            .block_on(cache.prune_expired_entries())
            .expect("prune expired entries should succeed");

        assert_eq!(report.removed_entries, 1);
        assert_eq!(report.removed_payloads, 0);

        let payload_hash = Hash::from_str(payload_hash_text.trim()).expect("parse payload hash");
        assert!(runtime.block_on(cache.cas.exists(payload_hash)).expect("exists"));
    }
}
