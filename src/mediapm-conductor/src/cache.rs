//! CAS-backed logical-key cache engine.
//!
//! Provides a generic CAS-backed cache for reusing payload bytes across
//! conductor subsystems.  The cache layout is always:
//!
//! - `<root>/store/` — CAS payload objects.
//! - `<root>/*.jsonc` — key-to-hash metadata index(es).
//!
//! Where the root is placed determines the effective scope (user-level cache,
//! workspace cache, project cache, etc.).
//!
//! Eviction policy: entries older than 30 days are pruned automatically.
//! The user-level cache wrapper is in [`crate::cache_user_level`].

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use serde::{Deserialize, Serialize};

use crate::error::ConductorError;

/// Current on-disk index format marker.
const INDEX_VERSION: u32 = 1;
/// Default metadata index file name.
const DEFAULT_INDEX_FILE_NAME: &str = "tools.jsonc";

/// Fixed entry TTL for automatic cache eviction (30 days).
pub const ENTRY_TTL_SECONDS: u64 = 30 * 24 * 60 * 60;
/// Minimum interval between full prune scans (24 hours).
const PRUNE_INTERVAL_SECONDS: u64 = 24 * 60 * 60;
/// Minimum interval between persisted access-timestamp updates (5 minutes).
const TOUCH_PERSIST_INTERVAL_SECONDS: u64 = 5 * 60;

/// Summary of one cache-prune operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CachePruneReport {
    /// Number of logical key entries removed from index metadata.
    pub removed_entries: usize,
    /// Number of CAS payload objects removed from `store/`.
    pub removed_payloads: usize,
}

/// Generic CAS-backed logical-key cache.
///
/// This is the core cache engine.  Wrap it in a domain-typed struct (e.g.
/// [`UserLevelCache`](self::user_level::UserLevelCache)) to attach
/// scope-specific constructors and policies.
#[derive(Clone)]
pub struct Cache {
    /// Shared CAS store that persists cached payload bytes.
    cas: Arc<FileSystemCas>,
    /// Path to one JSONC metadata index file.
    index_path: PathBuf,
    /// In-memory index guarded for concurrent worker access.
    index: Arc<Mutex<CacheIndex>>,
    /// Entry TTL in seconds for automatic cache eviction.
    entry_ttl_seconds: u64,
}

impl Cache {
    /// Opens (or bootstraps) one cache root with default index file and default
    /// TTL (30 days).
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails.
    pub async fn open(root: &Path) -> Result<Self, ConductorError> {
        Self::open_with_ttl(root, ENTRY_TTL_SECONDS).await
    }

    /// Opens (or bootstraps) one cache root with a custom TTL and default index
    /// file.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails.
    pub async fn open_with_ttl(
        root: &Path,
        entry_ttl_seconds: u64,
    ) -> Result<Self, ConductorError> {
        Self::open_with_index_file_name_and_ttl(root, DEFAULT_INDEX_FILE_NAME, entry_ttl_seconds)
            .await
    }

    /// Opens one cache root and binds this handle to a specific index file
    /// with the default TTL (30 days).
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when filesystem preparation or CAS opening
    /// fails.
    pub async fn open_with_index_file_name(
        root: &Path,
        index_file_name: &str,
    ) -> Result<Self, ConductorError> {
        Self::open_with_index_file_name_and_ttl(root, index_file_name, ENTRY_TTL_SECONDS).await
    }

    /// Opens one cache root and binds this handle to a specific index file
    /// with a custom TTL.
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
        fs::create_dir_all(root).map_err(|source| ConductorError::Io {
            operation: "creating cache root".to_string(),
            path: root.to_path_buf(),
            source,
        })?;
        let store_dir = root.join("store");
        fs::create_dir_all(&store_dir).map_err(|source| ConductorError::Io {
            operation: "creating cache store directory".to_string(),
            path: store_dir.clone(),
            source,
        })?;
        let cas = FileSystemCas::open(&store_dir).await.map_err(|source| {
            ConductorError::Workflow(format!(
                "opening cache CAS store at '{}' failed: {source}",
                store_dir.display()
            ))
        })?;
        let normalized_index_file_name = normalize_index_file_name(index_file_name);
        let index_path = root.join(normalized_index_file_name);
        let index = load_index_file(&index_path);
        if !index_path.exists() {
            let _ = write_index_file(&index_path, &index);
        }
        Ok(Self {
            cas: Arc::new(cas),
            index_path,
            index: Arc::new(Mutex::new(index)),
            entry_ttl_seconds,
        })
    }

    /// Looks up cached payload bytes for one logical key.
    ///
    /// Corrupt or stale key rows are treated as cache misses and cleaned up
    /// lazily so execution can continue without hard failures.
    #[must_use]
    pub async fn lookup_bytes(&self, key: &str) -> Option<Vec<u8>> {
        let entry = {
            let index = self.index.lock().ok()?;
            index.entries.get(key).cloned()
        }?;
        let Ok(hash) = Hash::from_str(entry.hash.trim()) else {
            self.remove_index_entry(key);
            return None;
        };
        let Ok(bytes) = self.cas.get(hash).await else {
            self.remove_index_entry(key);
            return None;
        };
        self.touch_index_entry(key, hash);
        Some(bytes.to_vec())
    }

    /// Stores payload bytes under one logical key.
    ///
    /// Write failures are intentionally tolerated so callers can continue
    /// even when cache persistence is temporarily unavailable.
    pub async fn store_bytes(&self, key: &str, payload: &[u8]) {
        if payload.is_empty() {
            return;
        }
        let Ok(hash) = self.cas.put(payload.to_vec().into()).await else {
            return;
        };
        self.touch_index_entry(key, hash);
    }

    /// Returns current number of logical cache-key rows in index metadata.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.index.lock().map_or(0, |index| index.entries.len())
    }

    /// Updates `last_access_unix_seconds` for a key without changing its hash.
    pub fn refresh_last_used(&self, key: &str) {
        let Ok(mut index) = self.index.lock() else {
            return;
        };
        if let Some(entry) = index.entries.get_mut(key) {
            entry.last_access_unix_seconds = now_unix_seconds();
        }
    }

    /// Removes expired index rows and their unreferenced CAS payloads.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] when index locking or persistence fails.
    pub async fn prune_expired_entries(&self) -> Result<CachePruneReport, ConductorError> {
        let now = now_unix_seconds();
        let cutoff = now.saturating_sub(self.entry_ttl_seconds);

        let (expired_keys, expired_hashes) = {
            let mut index = self.index.lock().map_err(|_| {
                ConductorError::Internal("locking cache index mutex failed".to_string())
            })?;
            if now.saturating_sub(index.last_prune_unix_seconds) < PRUNE_INTERVAL_SECONDS {
                return Ok(CachePruneReport::default());
            }
            index.last_prune_unix_seconds = now;

            let mut expired_keys = Vec::new();
            let mut expired_hashes = Vec::new();
            for (key, entry) in &index.entries {
                if entry.last_access_unix_seconds <= cutoff {
                    expired_keys.push(key.clone());
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
            return Ok(CachePruneReport::default());
        }

        let active_hash_union = collect_referenced_hashes_from_jsonc_indexes(
            self.index_path.parent().unwrap_or(Path::new("")),
        );
        let mut removed_payloads = 0usize;
        for hash_text in expired_hashes {
            if active_hash_union.contains(&hash_text) {
                continue;
            }
            let Ok(hash) = Hash::from_str(hash_text.trim()) else {
                continue;
            };
            if self.cas.stat(hash).await.is_ok() && self.cas.delete(hash).await.is_ok() {
                removed_payloads = removed_payloads.saturating_add(1);
            }
        }
        Ok(CachePruneReport { removed_entries: expired_keys.len(), removed_payloads })
    }

    /// Removes one key row from cache index metadata.
    fn remove_index_entry(&self, key: &str) {
        let Ok(mut index) = self.index.lock() else {
            return;
        };
        if index.entries.remove(key).is_some() {
            let _ = write_index_file(&self.index_path, &index);
        }
    }

    /// Upserts one key row in cache index metadata and bumps access timestamp.
    fn touch_index_entry(&self, key: &str, hash: Hash) {
        let Ok(mut index) = self.index.lock() else {
            return;
        };
        let now = now_unix_seconds();
        let hash_text = hash.to_string();
        let mut should_persist = true;
        if let Some(existing) = index.entries.get_mut(key) {
            let hash_changed = existing.hash != hash_text;
            existing.hash = hash_text;
            let elapsed = now.saturating_sub(existing.last_access_unix_seconds);
            existing.last_access_unix_seconds = now;
            should_persist = hash_changed || elapsed >= TOUCH_PERSIST_INTERVAL_SECONDS;
        } else {
            index.entries.insert(
                key.to_string(),
                CacheIndexEntry { hash: hash_text, last_access_unix_seconds: now },
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
        return DEFAULT_INDEX_FILE_NAME.to_string();
    }
    let candidate = Path::new(trimmed)
        .file_name()
        .and_then(OsStr::to_str)
        .filter(|name| !name.trim().is_empty())
        .unwrap_or(DEFAULT_INDEX_FILE_NAME);
    if Path::new(candidate)
        .extension()
        .and_then(OsStr::to_str)
        .is_some_and(|ext| ext.eq_ignore_ascii_case("jsonc"))
    {
        candidate.to_string()
    } else {
        format!("{candidate}.jsonc")
    }
}

/// Persisted cache index file envelope (`*.jsonc`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CacheIndex {
    /// Envelope version marker.
    version: u32,
    /// Last time a full prune scan completed (Unix seconds).
    #[serde(default)]
    last_prune_unix_seconds: u64,
    /// Cache metadata rows keyed by logical identity key.
    #[serde(default)]
    entries: BTreeMap<String, CacheIndexEntry>,
}

impl Default for CacheIndex {
    fn default() -> Self {
        Self { version: INDEX_VERSION, last_prune_unix_seconds: 0, entries: BTreeMap::new() }
    }
}

/// One logical cache metadata row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CacheIndexEntry {
    /// CAS multihash text pointing at payload bytes in `store/`.
    hash: String,
    /// Last successful read/write access timestamp (Unix seconds).
    last_access_unix_seconds: u64,
}

/// Loads one index file from disk, falling back to an empty index when absent
/// or malformed.
#[must_use]
fn load_index_file(index_path: &Path) -> CacheIndex {
    let Ok(raw) = fs::read_to_string(index_path) else {
        return CacheIndex::default();
    };
    let Ok(parsed) = serde_json::from_str::<CacheIndex>(&raw) else {
        return CacheIndex::default();
    };
    if parsed.version == INDEX_VERSION { parsed } else { CacheIndex::default() }
}

/// Collects active hash references from all JSONC index files under one cache
/// root.
///
/// Malformed or version-incompatible index files are ignored so pruning stays
/// best-effort and never blocks provisioning.
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
            .and_then(OsStr::to_str)
            .is_some_and(|ext| ext.eq_ignore_ascii_case("jsonc"));
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
fn write_index_file(index_path: &Path, index: &CacheIndex) -> Result<(), ConductorError> {
    let parent = index_path.parent().ok_or_else(|| {
        ConductorError::Workflow(format!(
            "resolving cache index parent directory for '{}' failed",
            index_path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
        operation: "creating cache index parent directory".to_string(),
        path: parent.to_path_buf(),
        source,
    })?;
    let rendered = serde_json::to_string_pretty(index)
        .map_err(|error| ConductorError::Serialization(format!("encoding cache index: {error}")))?;
    let mut temp_file =
        tempfile::NamedTempFile::new_in(parent).map_err(|source| ConductorError::Io {
            operation: "creating temporary cache index file".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    temp_file.write_all(format!("{rendered}\n").as_bytes()).map_err(|source| {
        ConductorError::Io {
            operation: "writing temporary cache index".to_string(),
            path: temp_file.path().to_path_buf(),
            source,
        }
    })?;
    if index_path.exists() {
        let _ = fs::remove_file(index_path);
    }
    temp_file.persist(index_path).map_err(|error| ConductorError::Io {
        operation: "replacing cache index".to_string(),
        path: index_path.to_path_buf(),
        source: error.error,
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
    use super::{Cache, ENTRY_TTL_SECONDS, PRUNE_INTERVAL_SECONDS, TOUCH_PERSIST_INTERVAL_SECONDS};

    /// Verifies the TTL constants are at least one day (no accidental
    /// short-duration defaults).
    #[test]
    #[expect(clippy::assertions_on_constants)]
    fn ttl_constants_are_reasonably_large() {
        assert!(ENTRY_TTL_SECONDS >= 24 * 60 * 60, "ENTRY_TTL_SECONDS should be at least one day");
        assert!(
            PRUNE_INTERVAL_SECONDS >= 60 * 60,
            "PRUNE_INTERVAL_SECONDS should be at least one hour"
        );
        assert!(
            TOUCH_PERSIST_INTERVAL_SECONDS >= 60,
            "TOUCH_PERSIST_INTERVAL_SECONDS should be at least one minute"
        );
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
        let cache = runtime.block_on(Cache::open(root.path())).expect("open cache");
        let payload = b"shared-cache".to_vec();
        let key = "test-tool-v1.0.0";
        runtime.block_on(cache.store_bytes(key, &payload));
        let retrieved = runtime.block_on(cache.lookup_bytes(key));
        assert_eq!(retrieved, Some(payload.clone()), "round-trip must return original bytes");
        runtime.block_on(cache.prune_expired_entries()).expect("prune should succeed");
        // Immediate prune should not remove fresh entry
        let retrieved_after = runtime.block_on(cache.lookup_bytes(key));
        assert_eq!(retrieved_after, Some(payload), "fresh entry must survive prune");
    }
}
