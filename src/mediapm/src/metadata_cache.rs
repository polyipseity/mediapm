//! Persistent on-disk metadata cache for resolved media metadata.
//!
//! Stores resolved metadata as a JSONC file keyed by BLAKE3 hex strings with
//! timer-based batch persistence and TTL-based entry expiry (1 day of
//! non-usage).

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::MediaPmError;

/// Default TTL for cache entries (1 day of non-usage).
const METADATA_CACHE_ENTRY_TTL_SECONDS: u64 = 86_400;

/// Minimum interval between persisted access-timestamp updates.
const METADATA_CACHE_PERSIST_INTERVAL_SECONDS: u64 = 300;

/// One cached metadata entry with access tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataCacheEntry {
    /// Arbitrary JSON value (entire metadata blob).
    value: serde_json::Value,
    /// Unix epoch seconds of last access (for TTL computation).
    last_access_unix_seconds: u64,
}

/// Persistent on-disk metadata cache keyed by BLAKE3 hex strings.
///
/// Persistence is timer-based: `set()` is in-memory only, and `flush()` writes
/// to disk when the persist interval has elapsed or when dirty entries exist.
/// The cache also flushes on `Drop`.
pub(crate) struct MetadataCache {
    /// In-memory entries guarded for concurrent access.
    entries: Arc<Mutex<BTreeMap<String, MetadataCacheEntry>>>,
    /// Path to `metadata.jsonc`.
    cache_path: PathBuf,
    /// Unix epoch seconds of last full persist.
    last_persist_unix_seconds: Arc<Mutex<u64>>,
    /// Whether there are unsaved changes.
    dirty: Arc<Mutex<bool>>,
}

impl MetadataCache {
    /// Opens (or creates) the metadata cache at `cache_dir/metadata.jsonc`.
    ///
    /// Expired entries are pruned on load. The cache directory is created if it
    /// does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the cache directory cannot be created.
    pub(crate) fn open(cache_dir: &Path) -> Result<Self, MediaPmError> {
        fs::create_dir_all(cache_dir).map_err(|source| MediaPmError::Io {
            operation: "creating metadata cache directory".to_string(),
            path: cache_dir.to_path_buf(),
            source,
        })?;

        let cache_path = cache_dir.join("metadata.jsonc");
        let entries = if cache_path.exists() {
            let content = fs::read_to_string(&cache_path).unwrap_or_default();
            let stripped = strip_jsonc_comments(&content);
            serde_json::from_str::<BTreeMap<String, MetadataCacheEntry>>(&stripped)
                .unwrap_or_default()
        } else {
            BTreeMap::new()
        };

        let now = unix_seconds_now();
        let cache = Self {
            entries: Arc::new(Mutex::new(entries)),
            cache_path,
            last_persist_unix_seconds: Arc::new(Mutex::new(now)),
            dirty: Arc::new(Mutex::new(false)),
        };

        // Prune expired entries on load.
        let pruned = cache.prune_expired();
        if pruned > 0 {
            let _ = cache.flush();
        }

        Ok(cache)
    }

    /// Returns cached metadata value for `key`, or `None` on miss or expiry.
    ///
    /// Updates `last_access_unix_seconds` on hit and marks the cache dirty.
    pub(crate) fn get(&self, key: &str) -> Option<serde_json::Value> {
        let mut entries = self.entries.lock().ok()?;
        let entry = entries.get_mut(key)?;
        let now = unix_seconds_now();

        // Check TTL: if expired, remove and return None.
        if now.saturating_sub(entry.last_access_unix_seconds) > METADATA_CACHE_ENTRY_TTL_SECONDS {
            entries.remove(key);
            if let Ok(mut dirty) = self.dirty.lock() {
                *dirty = true;
            }
            return None;
        }

        entry.last_access_unix_seconds = now;
        if let Ok(mut dirty) = self.dirty.lock() {
            *dirty = true;
        }
        Some(entry.value.clone())
    }

    /// Inserts or updates a cache entry for `key` with the given JSON value.
    ///
    /// This is an in-memory operation only; call `flush()` to persist to disk.
    pub(crate) fn set(&self, key: String, value: serde_json::Value) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(
                key,
                MetadataCacheEntry { value, last_access_unix_seconds: unix_seconds_now() },
            );
        }
        if let Ok(mut dirty) = self.dirty.lock() {
            *dirty = true;
        }
    }

    /// Persists dirty entries to disk if the persist interval has elapsed or
    /// the cache is dirty.
    ///
    /// Expired entries are pruned before writing. Writes use an atomic
    /// tempfile + rename pattern.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when filesystem write or rename fails.
    pub(crate) fn flush(&self) -> Result<(), MediaPmError> {
        let now = unix_seconds_now();
        {
            let last_persist = self.last_persist_unix_seconds.lock().map_err(|_| {
                MediaPmError::Workflow("metadata cache persist lock poisoned".to_string())
            })?;
            let dirty = self.dirty.lock().map_err(|_| {
                MediaPmError::Workflow("metadata cache dirty lock poisoned".to_string())
            })?;

            // Skip flush if not dirty and persist interval hasn't elapsed.
            if !*dirty
                && now.saturating_sub(*last_persist) < METADATA_CACHE_PERSIST_INTERVAL_SECONDS
            {
                return Ok(());
            }
        }

        // Prune expired entries before writing.
        self.prune_expired();

        let entries = self.entries.lock().map_err(|_| {
            MediaPmError::Workflow("metadata cache entries lock poisoned".to_string())
        })?;

        let json = serde_json::to_string_pretty(&*entries).map_err(|source| {
            MediaPmError::Workflow(format!("serializing metadata cache: {source}"))
        })?;

        // Atomic tempfile + rename.
        let temp_path = self.cache_path.with_extension("jsonc.tmp");
        {
            let mut file = fs::File::create(&temp_path).map_err(|source| MediaPmError::Io {
                operation: "creating metadata cache temp file".to_string(),
                path: temp_path.clone(),
                source,
            })?;
            file.write_all(json.as_bytes()).map_err(|source| MediaPmError::Io {
                operation: "writing metadata cache temp file".to_string(),
                path: temp_path.clone(),
                source,
            })?;
            file.sync_all().map_err(|source| MediaPmError::Io {
                operation: "syncing metadata cache temp file".to_string(),
                path: temp_path.clone(),
                source,
            })?;
        }
        fs::rename(&temp_path, &self.cache_path).map_err(|source| MediaPmError::Io {
            operation: "renaming metadata cache temp file".to_string(),
            path: self.cache_path.clone(),
            source,
        })?;

        if let Ok(mut last_persist) = self.last_persist_unix_seconds.lock() {
            *last_persist = now;
        }
        if let Ok(mut dirty) = self.dirty.lock() {
            *dirty = false;
        }

        Ok(())
    }

    /// Removes expired entries and returns the count removed.
    pub(crate) fn prune_expired(&self) -> usize {
        let now = unix_seconds_now();
        let Ok(mut entries) = self.entries.lock() else {
            return 0;
        };

        let before = entries.len();
        entries.retain(|_key, entry| {
            now.saturating_sub(entry.last_access_unix_seconds) <= METADATA_CACHE_ENTRY_TTL_SECONDS
        });
        before - entries.len()
    }

    /// Returns the number of cached entries (used in tests).
    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

impl Drop for MetadataCache {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Returns current unix epoch seconds.
fn unix_seconds_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Strips JSONC comments (`//` and `/* */`) from a string.
fn strip_jsonc_comments(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if i + 1 < chars.len() {
            if chars[i] == '/' && chars[i + 1] == '/' {
                i += 2;
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
                continue;
            }
            if chars[i] == '/' && chars[i + 1] == '*' {
                i += 2;
                while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                    i += 1;
                }
                i += 2;
                continue;
            }
        }
        result.push(chars[i]);
        i += 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_jsonc_comments_removes_line_and_block_comments() {
        let input = "// line comment\n{\"key\": \"value\"} /* block comment */";
        let result = strip_jsonc_comments(input);
        assert_eq!(result, "\n{\"key\": \"value\"} ");
    }

    #[test]
    fn cache_open_and_set_get_round_trips_value() {
        let dir = std::env::temp_dir()
            .join(format!("mediapm-metadata-cache-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let cache = MetadataCache::open(&dir).expect("open should succeed");
        assert_eq!(cache.entry_count(), 0);

        cache.set("test-key".to_string(), serde_json::json!({"hello": "world"}));
        assert_eq!(cache.entry_count(), 1);

        let value = cache.get("test-key");
        assert!(value.is_some());
        assert_eq!(value.unwrap(), serde_json::json!({"hello": "world"}));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cache_persists_and_loads_across_sessions() {
        let dir = std::env::temp_dir()
            .join(format!("mediapm-metadata-cache-persist-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        {
            let cache = MetadataCache::open(&dir).expect("open should succeed");
            cache.set("persist-key".to_string(), serde_json::json!({"persisted": true}));
            cache.flush().expect("flush should succeed");
        }

        {
            let cache = MetadataCache::open(&dir).expect("re-open should succeed");
            let value = cache.get("persist-key");
            assert!(value.is_some());
            assert_eq!(value.unwrap(), serde_json::json!({"persisted": true}));
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prune_expired_removes_stale_entries() {
        let dir =
            std::env::temp_dir().join(format!("mediapm-metadata-cache-ttl-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let cache = MetadataCache::open(&dir).expect("open should succeed");
        let old_time = unix_seconds_now() - METADATA_CACHE_ENTRY_TTL_SECONDS - 1;

        // Manually insert an expired entry.
        {
            let mut entries = cache.entries.lock().unwrap();
            entries.insert(
                "expired-key".to_string(),
                MetadataCacheEntry {
                    value: serde_json::json!("stale"),
                    last_access_unix_seconds: old_time,
                },
            );
        }

        let pruned = cache.prune_expired();
        assert_eq!(pruned, 1);
        assert_eq!(cache.entry_count(), 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_returns_none_for_expired_entry() {
        let dir = std::env::temp_dir()
            .join(format!("mediapm-metadata-cache-get-expired-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let cache = MetadataCache::open(&dir).expect("open should succeed");
        let old_time = unix_seconds_now() - METADATA_CACHE_ENTRY_TTL_SECONDS - 1;

        {
            let mut entries = cache.entries.lock().unwrap();
            entries.insert(
                "stale-key".to_string(),
                MetadataCacheEntry {
                    value: serde_json::json!("stale"),
                    last_access_unix_seconds: old_time,
                },
            );
        }

        // get() should check TTL and return None for expired entries.
        assert!(cache.get("stale-key").is_none());
        assert_eq!(cache.entry_count(), 0);

        let _ = fs::remove_dir_all(&dir);
    }
}
