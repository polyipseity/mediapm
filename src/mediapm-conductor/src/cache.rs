//! CAS-backed logical-key cache engine.
//!
//! Provides a generic CAS-backed cache for reusing payload bytes across
//! conductor subsystems.  The cache layout is always:
//!
//! - `<root>/store/` — CAS payload objects.
//! - `<root>/*.json` — key-to-hash metadata index(es).
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::error::ConductorError;

/// Current on-disk index format marker.
const INDEX_VERSION: u32 = 1;
/// Default metadata index file name.
const DEFAULT_INDEX_FILE_NAME: &str = "tools.json";

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
/// [`UserLevelCache`](crate::cache_user_level::UserLevelCache)) to attach
/// scope-specific constructors and policies.
#[derive(Clone)]
pub struct Cache {
    /// Shared CAS store that persists cached payload bytes.
    cas: Arc<FileSystemCas>,
    /// Path to one JSON metadata index file.
    index_path: PathBuf,
    /// In-memory index guarded for concurrent worker access.
    index: Arc<Mutex<CacheIndex>>,
    /// Entry TTL in seconds for automatic cache eviction.
    entry_ttl_seconds: u64,
}

impl Cache {
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
    pub fn touch(&self, key: &str) {
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

        let active_hash_union = collect_referenced_hashes_from_indexes(
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

    /// Start a background task that periodically drains WAL entries via
    /// `cas.bg_engine().run_wal_consumer()` at the given `interval`.
    ///
    /// Returns a [`BackgroundMaintenanceGuard`] that cancels the task on drop.
    ///
    /// Call this on an `Arc<Cache>` when the caller intends to keep the cache
    /// alive for an extended period (e.g. long-running service).  Temporary
    /// or short-lived cache handles typically do not need background
    /// maintenance — the WAL will be consumed on the next open.
    #[must_use]
    pub fn start_background_maintenance(
        self: &Arc<Self>,
        interval: Duration,
    ) -> BackgroundMaintenanceGuard {
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_clone = cancelled.clone();
        let cache = self.clone();
        let handle = tokio::spawn(async move {
            while !cancelled_clone.load(Ordering::Relaxed) {
                tokio::time::sleep(interval).await;
                if cancelled_clone.load(Ordering::Relaxed) {
                    break;
                }
                let _ = cache.cas.bg_engine().run_wal_consumer().await;
            }
        });
        BackgroundMaintenanceGuard { cancelled, handle: Some(handle) }
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

/// Returns a safe JSON filename for one cache index.
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
        .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
    {
        candidate.to_string()
    } else {
        format!("{candidate}.json")
    }
}

/// Persisted cache index file envelope (`*.json`).
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

/// RAII guard that cancels background WAL consumer maintenance on drop.
///
/// Returned by [`Cache::start_background_maintenance`].  While the guard is
/// alive a tokio task periodically calls `cas.bg_engine().run_wal_consumer()`.
/// Dropping the guard aborts the task and releases the `Arc<Cache>` reference.
pub struct BackgroundMaintenanceGuard {
    /// Shared flag checked by the background task loop.
    cancelled: Arc<AtomicBool>,
    /// Handle to the spawned background task, aborted on drop.
    handle: Option<JoinHandle<()>>,
}

impl BackgroundMaintenanceGuard {
    /// Returns `true` if the background task has been cancelled (guard dropped
    /// or [`cancel`](Self::cancel) called explicitly).
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Cancel the background task immediately.  Idempotent.
    pub fn cancel(&mut self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for BackgroundMaintenanceGuard {
    fn drop(&mut self) {
        self.cancel();
    }
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

/// Collects active hash references from all index files under one cache
/// root.
///
/// Malformed or version-incompatible index files are ignored so pruning stays
/// best-effort and never blocks provisioning.
#[must_use]
fn collect_referenced_hashes_from_indexes(cache_root: &Path) -> BTreeSet<String> {
    let mut referenced = BTreeSet::new();
    let Ok(entries) = fs::read_dir(cache_root) else {
        return referenced;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_index = path
            .extension()
            .and_then(OsStr::to_str)
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"));
        if !is_index {
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
impl Cache {
    /// Test-only: returns the last-access timestamp for a cache entry.
    #[must_use]
    pub(crate) fn get_entry_last_access(&self, key: &str) -> Option<u64> {
        self.index.lock().ok()?.entries.get(key).map(|e| e.last_access_unix_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::{Cache, ENTRY_TTL_SECONDS, PRUNE_INTERVAL_SECONDS, TOUCH_PERSIST_INTERVAL_SECONDS};

    // Compile-time assertions: TTL constants must be at least one day/hour/minute.
    const _: () = assert!(ENTRY_TTL_SECONDS >= 24 * 60 * 60);
    const _: () = assert!(PRUNE_INTERVAL_SECONDS >= 60 * 60);
    const _: () = assert!(TOUCH_PERSIST_INTERVAL_SECONDS >= 60);

    /// Protects shared-cache behavior by ensuring key-based round trips return
    /// the original payload bytes.
    #[tokio::test]
    async fn cache_round_trips_bytes_by_logical_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache =
            Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 30 * 24 * 60 * 60)
                .await
                .expect("open cache");
        let payload = b"shared-cache".to_vec();
        let key = "test-tool-v1.0.0";
        cache.store_bytes(key, &payload).await;
        let retrieved = cache.lookup_bytes(key).await;
        assert_eq!(retrieved, Some(payload.clone()), "round-trip must return original bytes");
        cache.prune_expired_entries().await.expect("prune should succeed");
        // Immediate prune should not remove fresh entry
        let retrieved_after = cache.lookup_bytes(key).await;
        assert_eq!(retrieved_after, Some(payload), "fresh entry must survive prune");
    }

    /// Verifies that querying a non-existent key returns None.
    #[tokio::test]
    async fn lookup_bytes_nonexistent_key_returns_none() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache =
            Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 30 * 24 * 60 * 60)
                .await
                .expect("open cache");
        let retrieved = cache.lookup_bytes("no-such-key").await;
        assert!(retrieved.is_none(), "nonexistent key must return None");
    }

    /// Verifies that storing a second value under the same key overwrites the
    /// first and that the new payload is returned on lookup.
    #[tokio::test]
    async fn store_bytes_overwrite_updates_payload() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache =
            Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 30 * 24 * 60 * 60)
                .await
                .expect("open cache");
        let key = "overwrite-key";
        cache.store_bytes(key, b"first-value").await;
        cache.store_bytes(key, b"second-value").await;
        let retrieved = cache.lookup_bytes(key).await;
        assert_eq!(retrieved, Some(b"second-value".to_vec()), "second store must overwrite first");
    }

    /// Verifies that prune removes entries whose TTL has expired (TTL = 0).
    #[tokio::test]
    async fn prune_expired_removes_expired_entries() {
        let root = tempfile::tempdir().expect("tempdir");
        // Use zero TTL so entries expire immediately.
        let cache = Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 0)
            .await
            .expect("open cache");
        let key = "expiring-key";
        cache.store_bytes(key, b"ephemeral").await;
        // Entry was stored with last_access = now; with TTL = 0, cutoff = now,
        // so the entry is eligible for pruning on the first prune call.
        let report = cache.prune_expired_entries().await.expect("prune should succeed");
        assert!(report.removed_entries >= 1, "expired entry must be pruned");
        let retrieved = cache.lookup_bytes(key).await;
        assert!(retrieved.is_none(), "pruned entry must not be retrievable");
    }

    /// Verifies that prune_expired_entries on a fresh empty cache does not
    /// crash or error.
    #[tokio::test]
    async fn prune_on_empty_cache_does_not_crash() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache =
            Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 30 * 24 * 60 * 60)
                .await
                .expect("open cache");
        let report =
            cache.prune_expired_entries().await.expect("prune on empty cache must succeed");
        assert_eq!(report.removed_entries, 0, "no entries in empty cache");
        assert_eq!(report.removed_payloads, 0, "no payloads in empty cache");
    }

    /// Verifies that storing empty bytes is a no-op (entry_count unchanged).
    #[tokio::test]
    async fn store_empty_bytes_does_not_create_entry() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache =
            Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 30 * 24 * 60 * 60)
                .await
                .expect("open cache");
        cache.store_bytes("empty-key", b"").await;
        assert_eq!(cache.entry_count(), 0, "empty payload must not create an entry");
        assert!(cache.lookup_bytes("empty-key").await.is_none(), "empty key must not be findable");
    }

    /// Verifies that `touch()` bumps `last_access` so an entry survives prune.
    #[tokio::test]
    async fn touch_bumps_last_access_and_prevents_prune() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache = Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 1)
            .await
            .expect("open cache");

        cache.store_bytes("key", b"data").await;

        // Wait for TTL to expire (1 second).
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // Touch moves last_access forward to now.
        cache.touch("key");

        // Prune — entry should survive because touch moved last_access
        // past the cutoff (now - 1s).
        let report = cache.prune_expired_entries().await.expect("prune");
        assert_eq!(report.removed_entries, 0, "touched entry must survive prune");
        let retrieved = cache.lookup_bytes("key").await;
        assert_eq!(retrieved, Some(b"data".to_vec()));
    }

    /// Verifies that `lookup_bytes()` does not update `last_access`.
    #[tokio::test]
    async fn lookup_bytes_does_not_bump_last_access() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache =
            Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 30 * 24 * 60 * 60)
                .await
                .expect("open cache");

        cache.store_bytes("key", b"data").await;
        let before = cache.get_entry_last_access("key").expect("entry exists");

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let _ = cache.lookup_bytes("key").await;
        let after = cache.get_entry_last_access("key").expect("entry still exists");
        assert_eq!(before, after, "lookup_bytes must not bump last_access");
    }

    /// Verifies that `touch()` bumps `last_access`.
    #[tokio::test]
    async fn touch_bumps_last_access() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache =
            Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 30 * 24 * 60 * 60)
                .await
                .expect("open cache");

        cache.store_bytes("key", b"data").await;
        let before = cache.get_entry_last_access("key").expect("entry exists");

        // Timestamps are in seconds; sleep 1s to guarantee a different second.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        cache.touch("key");
        let after = cache.get_entry_last_access("key").expect("entry still exists");
        assert!(after > before, "touch must bump last_access");
    }

    /// Verifies that pruning one index does not delete payloads referenced
    /// by another index sharing the same CAS store.
    #[tokio::test]
    async fn prune_cross_index_payload_gc_keeps_shared_references() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache_a = Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 0)
            .await
            .expect("open cache_a");
        let cache_b =
            Cache::open_with_index_file_name_and_ttl(root.path(), "tool_metadata.json", 0)
                .await
                .expect("open cache_b");

        let payload = b"shared-payload".to_vec();
        cache_a.store_bytes("key-a", &payload).await;
        cache_b.store_bytes("key-b", &payload).await;

        // Prune cache_a — key-a entries removed, but payload must survive
        // because cache_b still references the same hash.
        let report = cache_a.prune_expired_entries().await.expect("prune cache_a");
        assert!(report.removed_entries >= 1, "key-a must be pruned");

        // Payload still accessible via cache_b.
        let retrieved = cache_b.lookup_bytes("key-b").await;
        assert_eq!(retrieved, Some(payload), "payload must survive cross-index GC");
    }

    /// Verifies that prune cooldown (24h) prevents re-pruning within the
    /// interval.
    #[tokio::test]
    async fn prune_cooldown_respects_interval() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache = Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 0)
            .await
            .expect("open cache");

        // First prune — removes the immediately-expired entry.
        cache.store_bytes("expiring-key", b"ephemeral").await;
        let report = cache.prune_expired_entries().await.expect("first prune");
        assert!(report.removed_entries >= 1);

        // Store a fresh entry.
        cache.store_bytes("fresh-key", b"fresh").await;

        // Second prune within cooldown — must return empty report.
        let report = cache.prune_expired_entries().await.expect("second prune");
        assert_eq!(report.removed_entries, 0, "cooldown must prevent pruning");
        assert_eq!(report.removed_payloads, 0, "cooldown must prevent payload removal");

        // Fresh entry survives because prune didn't run.
        let retrieved = cache.lookup_bytes("fresh-key").await;
        assert_eq!(retrieved, Some(b"fresh".to_vec()));
    }

    /// Verifies that background maintenance can be started and dropped
    /// without crashing on an empty cache.
    #[tokio::test]
    async fn background_maintenance_does_not_crash_on_empty_cache() {
        use std::sync::Arc;
        use std::time::Duration;

        let root = tempfile::tempdir().expect("tempdir");
        let cache = Arc::new(
            Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 30 * 24 * 60 * 60)
                .await
                .expect("open cache"),
        );

        // Start background maintenance with a 10-minute interval so it never
        // actually fires during the test.
        let guard = cache.start_background_maintenance(Duration::from_secs(600));

        // Verify the guard is alive.
        assert!(!guard.is_cancelled());

        // Drop stops the background task.
        drop(guard);
    }

    /// Verifies that background maintenance guard reports cancelled after
    /// the handle is dropped.
    #[tokio::test]
    async fn background_maintenance_cancels_on_drop() {
        use std::sync::Arc;
        use std::time::Duration;

        let root = tempfile::tempdir().expect("tempdir");
        let cache = Arc::new(
            Cache::open_with_index_file_name_and_ttl(root.path(), "tools.json", 30 * 24 * 60 * 60)
                .await
                .expect("open cache"),
        );

        let guard = cache.start_background_maintenance(Duration::from_secs(600));
        drop(guard);

        // Guard was dropped without panicking — cancellation succeeded.
    }
}
