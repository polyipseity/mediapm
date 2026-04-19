//! Shared global tool-download cache for managed payload bytes.
//!
//! Cache layout is intentionally stable and extensible under one global user
//! directory:
//! - `tool-cache/store/` for CAS payload objects,
//! - `tool-cache/index.jsonc` for key-to-hash metadata.
//!
//! Eviction policy is fixed: entries older than 30 days are removed
//! automatically. This keeps cross-workspace reuse effective without unbounded
//! growth.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use serde::{Deserialize, Serialize};

use crate::error::MediaPmError;
use crate::global::MediaPmGlobalPaths;

/// Current on-disk index format marker.
const TOOL_CACHE_INDEX_VERSION: u32 = 1;

/// Fixed entry TTL for automatic tool-cache eviction.
pub(crate) const TOOL_CACHE_ENTRY_TTL_SECONDS: u64 = 30 * 24 * 60 * 60;

/// Summary of one cache-prune operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ToolCachePruneReport {
    /// Number of logical key entries removed from index metadata.
    pub removed_entries: usize,
    /// Number of CAS payload objects removed from `store/`.
    pub removed_payloads: usize,
}

/// User-scoped managed-tool payload cache backed by CAS object storage.
#[derive(Clone)]
pub(crate) struct ToolDownloadCache {
    /// Shared CAS store that persists cached payload bytes.
    cas: Arc<FileSystemCas>,
    /// Path to one JSONC metadata index file.
    index_path: PathBuf,
    /// In-memory index guarded for concurrent downloader worker access.
    index: Arc<Mutex<ToolCacheIndex>>,
}

impl ToolDownloadCache {
    /// Opens (or bootstraps) one global tool-cache root.
    pub(crate) async fn open(tool_cache_root: &Path) -> Result<Self, MediaPmError> {
        fs::create_dir_all(tool_cache_root).map_err(|source| MediaPmError::Io {
            operation: "creating global tool-cache root".to_string(),
            path: tool_cache_root.to_path_buf(),
            source,
        })?;

        let store_dir = tool_cache_root.join("store");
        fs::create_dir_all(&store_dir).map_err(|source| MediaPmError::Io {
            operation: "creating global tool-cache store directory".to_string(),
            path: store_dir.clone(),
            source,
        })?;

        let cas = FileSystemCas::open(&store_dir).await.map_err(|source| {
            MediaPmError::Workflow(format!(
                "opening global tool-cache CAS store at '{}' failed: {source}",
                store_dir.display()
            ))
        })?;

        let index_path = tool_cache_root.join("index.jsonc");
        let index = load_index_file(&index_path);
        if !index_path.exists() {
            let _ = write_index_file(&index_path, &index);
        }

        Ok(Self { cas: Arc::new(cas), index_path, index: Arc::new(Mutex::new(index)) })
    }

    /// Looks up cached payload bytes for one logical download key.
    ///
    /// Corrupt or stale key rows are treated as cache misses and cleaned up
    /// lazily so download execution can continue without hard failures.
    #[must_use]
    pub(crate) async fn lookup_bytes(&self, cache_key: &str) -> Option<Vec<u8>> {
        let entry = {
            let index = self.index.lock().ok()?;
            index.entries.get(cache_key).cloned()
        }?;

        let Ok(hash) = Hash::from_str(entry.hash.trim()) else {
            self.remove_index_entry(cache_key);
            return None;
        };

        match self.cas.get(hash).await {
            Ok(bytes) => {
                self.touch_index_entry(cache_key, hash);
                Some(bytes.to_vec())
            }
            Err(_) => {
                self.remove_index_entry(cache_key);
                None
            }
        }
    }

    /// Stores payload bytes under one logical download key.
    ///
    /// Write failures are intentionally tolerated so provisioning can continue
    /// even when cache persistence is temporarily unavailable.
    pub(crate) async fn store_bytes(&self, cache_key: &str, payload: &[u8]) {
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
    pub(crate) fn entry_count(&self) -> usize {
        self.index.lock().map(|index| index.entries.len()).unwrap_or(0)
    }

    /// Removes expired index rows and their unreferenced CAS payloads.
    pub(crate) async fn prune_expired_entries(&self) -> Result<ToolCachePruneReport, MediaPmError> {
        let cutoff = now_unix_seconds().saturating_sub(TOOL_CACHE_ENTRY_TTL_SECONDS);

        let (expired_keys, expired_hashes, active_hashes) = {
            let mut index = self.index.lock().map_err(|_| {
                MediaPmError::Workflow("locking global tool-cache index mutex failed".to_string())
            })?;

            let mut expired_keys = Vec::new();
            let mut expired_hashes = Vec::new();
            let mut active_hashes = BTreeSet::new();

            for (cache_key, entry) in &index.entries {
                if entry.last_access_unix_seconds <= cutoff {
                    expired_keys.push(cache_key.clone());
                    expired_hashes.push(entry.hash.clone());
                } else {
                    active_hashes.insert(entry.hash.clone());
                }
            }

            if !expired_keys.is_empty() {
                for key in &expired_keys {
                    index.entries.remove(key);
                }
                let _ = write_index_file(&self.index_path, &index);
            }

            (expired_keys, expired_hashes, active_hashes)
        };

        if expired_keys.is_empty() {
            return Ok(ToolCachePruneReport::default());
        }

        let mut removed_payloads = 0usize;
        for hash_text in expired_hashes {
            if active_hashes.contains(&hash_text) {
                continue;
            }

            let Ok(hash) = Hash::from_str(hash_text.trim()) else {
                continue;
            };

            if self.cas.exists(hash).await.unwrap_or(false) && self.cas.delete(hash).await.is_ok() {
                removed_payloads = removed_payloads.saturating_add(1);
            }
        }

        Ok(ToolCachePruneReport { removed_entries: expired_keys.len(), removed_payloads })
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
        index.entries.insert(
            cache_key.to_string(),
            ToolCacheIndexEntry {
                hash: hash.to_string(),
                last_access_unix_seconds: now_unix_seconds(),
            },
        );
        let _ = write_index_file(&self.index_path, &index);
    }
}

/// Persisted tool-cache index file envelope (`index.jsonc`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ToolCacheIndex {
    /// Envelope version marker.
    version: u32,
    /// Cache metadata rows keyed by logical downloader identity key.
    #[serde(default)]
    entries: BTreeMap<String, ToolCacheIndexEntry>,
}

impl Default for ToolCacheIndex {
    fn default() -> Self {
        Self { version: TOOL_CACHE_INDEX_VERSION, entries: BTreeMap::new() }
    }
}

/// One logical cache metadata row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ToolCacheIndexEntry {
    /// CAS multihash text pointing at payload bytes in `store/`.
    hash: String,
    /// Last successful read/write access timestamp (Unix seconds).
    last_access_unix_seconds: u64,
}

/// Loads one index file from disk, falling back to an empty index when absent
/// or malformed.
#[must_use]
fn load_index_file(index_path: &Path) -> ToolCacheIndex {
    let Ok(raw) = fs::read_to_string(index_path) else {
        return ToolCacheIndex::default();
    };

    let Ok(parsed) = serde_json::from_str::<ToolCacheIndex>(&raw) else {
        return ToolCacheIndex::default();
    };

    if parsed.version == TOOL_CACHE_INDEX_VERSION { parsed } else { ToolCacheIndex::default() }
}

/// Writes one index envelope to disk with replace-on-rename semantics.
fn write_index_file(index_path: &Path, index: &ToolCacheIndex) -> Result<(), MediaPmError> {
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: "creating global tool-cache index parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let rendered = serde_json::to_string_pretty(index).map_err(|error| {
        MediaPmError::Serialization(format!("encoding tool-cache index: {error}"))
    })?;
    let temp_path = index_path.with_extension("jsonc.tmp");
    fs::write(&temp_path, format!("{rendered}\n")).map_err(|source| MediaPmError::Io {
        operation: "writing temporary global tool-cache index".to_string(),
        path: temp_path.clone(),
        source,
    })?;
    if index_path.exists() {
        let _ = fs::remove_file(index_path);
    }
    fs::rename(&temp_path, index_path).map_err(|source| MediaPmError::Io {
        operation: "replacing global tool-cache index".to_string(),
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

/// Returns default global tool-cache root under the persistent global user
/// directory.
#[must_use]
pub(crate) fn default_global_tool_cache_root() -> Option<PathBuf> {
    MediaPmGlobalPaths::resolve_default().map(|paths| paths.tool_cache_dir)
}

#[cfg(test)]
mod tests {
    use super::{TOOL_CACHE_ENTRY_TTL_SECONDS, ToolDownloadCache, now_unix_seconds};

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

        let cache = runtime.block_on(ToolDownloadCache::open(root.path())).expect("open cache");

        let payload = b"shared-download-cache".to_vec();
        runtime.block_on(cache.store_bytes("tool=ffmpeg|version=8.2", &payload));

        assert_eq!(runtime.block_on(cache.lookup_bytes("tool=ffmpeg|version=8.2")), Some(payload));
        assert_eq!(cache.entry_count(), 1);
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

        let cache = runtime.block_on(ToolDownloadCache::open(root.path())).expect("open cache");

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

        let cache = runtime.block_on(ToolDownloadCache::open(root.path())).expect("open cache");
        runtime.block_on(cache.store_bytes("tool=yt-dlp|tag=latest", b"payload-a"));

        {
            let mut index = cache.index.lock().expect("index mutex");
            let stale_timestamp =
                now_unix_seconds().saturating_sub(TOOL_CACHE_ENTRY_TTL_SECONDS.saturating_add(1));
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
}
