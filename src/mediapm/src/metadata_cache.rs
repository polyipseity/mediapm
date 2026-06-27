//! Persistent metadata cache with TTL expiry and timer-based persistence.
//!
//! This module provides a simple JSONC-based on-disk cache for resolved
//! metadata values, with configurable TTL and periodic flush semantics.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default TTL for cached metadata entries (1 day in seconds).
const METADATA_CACHE_ENTRY_TTL_SECONDS: u64 = 86_400;

// ---------------------------------------------------------------------------
// Cache entry
// ---------------------------------------------------------------------------

/// One entry in the metadata cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataCacheEntry {
    /// The cached metadata JSON value.
    value: Value,
    /// Unix epoch seconds of the last access time.
    #[serde(rename = "lastAccessUnixSeconds")]
    last_access_unix_seconds: u64,
}

// ---------------------------------------------------------------------------
// MetadataCache
// ---------------------------------------------------------------------------

/// A persistent metadata cache backed by a JSONC file on disk.
///
/// Entries have a configurable TTL after which they are pruned. The cache
/// file is written on [`flush`](Self::flush) when dirty, using an atomic
/// tempfile+rename pattern.
#[derive(Debug)]
pub(crate) struct MetadataCache {
    /// In-memory entries keyed by cache key.
    entries: Arc<Mutex<BTreeMap<String, MetadataCacheEntry>>>,
    /// Path to the cache file on disk.
    cache_path: PathBuf,
    /// Whether the cache has unpersisted modifications.
    dirty: Arc<Mutex<bool>>,
}

impl MetadataCache {
    /// Opens or creates a metadata cache at the given cache directory.
    ///
    /// If the cache file already exists, it is loaded into memory.
    /// Missing or corrupt files start with an empty cache.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn open(cache_dir: &Path) -> Self {
        let cache_path = cache_dir.join("metadata.cache.jsonc");
        let entries = load_cache_file(&cache_path).unwrap_or_default();

        MetadataCache {
            entries: Arc::new(Mutex::new(entries)),
            cache_path,
            dirty: Arc::new(Mutex::new(false)),
        }
    }

    /// Retrieves a cached value by key, or `None` if missing or expired.
    #[must_use]
    pub(crate) fn get(&self, key: &str) -> Option<Value> {
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get(key) {
            let now = unix_seconds_now();
            if now.saturating_sub(entry.last_access_unix_seconds) > METADATA_CACHE_ENTRY_TTL_SECONDS
            {
                // Expired — remove and don't return.
                entries.remove(key);
                *self.dirty.lock().unwrap() = true;
                return None;
            }
            // Update access time.
            let mut entry = entry.clone();
            entry.last_access_unix_seconds = now;
            let val = entry.value.clone();
            entries.insert(key.to_string(), entry);
            Some(val)
        } else {
            None
        }
    }

    /// Inserts or updates a cached value by key.
    pub(crate) fn set(&self, key: String, value: Value) {
        let mut entries = self.entries.lock().unwrap();
        entries.insert(
            key,
            MetadataCacheEntry { value, last_access_unix_seconds: unix_seconds_now() },
        );
        *self.dirty.lock().unwrap() = true;
    }

    /// Persists the cache to disk if dirty.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the cache file cannot be written.
    pub(crate) fn flush(&self) -> Result<(), io::Error> {
        let mut dirty = self.dirty.lock().unwrap();

        if !*dirty {
            return Ok(());
        }

        let entries = self.entries.lock().unwrap();
        let json_bytes = serde_json::to_vec_pretty(&*entries)?;
        // Atomic write: tempfile + rename
        let tmp_path = self.cache_path.with_extension("tmp");
        {
            let mut tmp = tempfile::NamedTempFile::new_in(
                self.cache_path.parent().unwrap_or(Path::new(".")),
            )?;
            tmp.write_all(&json_bytes)?;
            tmp.persist(&tmp_path).map_err(|e| e.error)?;
        }
        fs::rename(&tmp_path, &self.cache_path)?;

        *dirty = false;
        Ok(())
    }

    /// Prunes expired entries from memory.
    #[allow(dead_code)]
    pub(crate) fn prune_expired(&self) {
        let mut entries = self.entries.lock().unwrap();
        let now = unix_seconds_now();
        entries.retain(|_, entry| {
            now.saturating_sub(entry.last_access_unix_seconds) <= METADATA_CACHE_ENTRY_TTL_SECONDS
        });
        *self.dirty.lock().unwrap() = true;
    }

    /// Returns the number of entries in the cache (for testing).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn entry_count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

impl Drop for MetadataCache {
    fn drop(&mut self) {
        if let Ok(true) = self.dirty.lock().map(|d| *d) {
            let _ = self.flush();
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the current Unix epoch seconds.
fn unix_seconds_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// Loads a JSONC cache file, stripping comments before parsing.
#[allow(dead_code)]
fn load_cache_file(path: &Path) -> Result<BTreeMap<String, MetadataCacheEntry>, io::Error> {
    let content = fs::read_to_string(path)?;
    let stripped = strip_jsonc_comments(&content);
    serde_json::from_str(&stripped).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Strips JSONC-style comments (single-line `//` and multi-line `/* */`) from
/// a string. Does not handle comments inside strings.
#[allow(clippy::while_let_on_iterator)]
fn strip_jsonc_comments(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '/' {
            match chars.peek() {
                Some('/') => {
                    // Single-line comment — skip to end of line.
                    while let Some(c) = chars.next() {
                        if c == '\n' {
                            output.push('\n');
                            break;
                        }
                    }
                }
                Some('*') => {
                    // Multi-line comment — skip to */.
                    chars.next();
                    while let Some(c) = chars.next() {
                        if c == '*' && chars.peek() == Some(&'/') {
                            chars.next();
                            break;
                        }
                    }
                }
                _ => output.push(ch),
            }
        } else {
            output.push(ch);
        }
    }
    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Ensures a newly opened cache has zero entries.
    #[test]
    fn open_creates_empty_cache() {
        let dir = TempDir::new().expect("temp dir");
        let cache = MetadataCache::open(dir.path());
        assert_eq!(cache.entry_count(), 0);
    }

    /// Ensures set + get round-trips a value.
    #[test]
    fn set_and_get_round_trips_value() {
        let dir = TempDir::new().expect("temp dir");
        let cache = MetadataCache::open(dir.path());
        let value = serde_json::json!({"title": "Test Video", "artist": "Test Artist"});
        cache.set("test-key".to_string(), value.clone());
        let retrieved = cache.get("test-key");
        assert_eq!(retrieved, Some(value));
    }

    /// Ensures get returns None for missing keys.
    #[test]
    fn get_returns_none_for_missing_key() {
        let dir = TempDir::new().expect("temp dir");
        let cache = MetadataCache::open(dir.path());
        assert!(cache.get("nonexistent").is_none());
    }

    /// Ensures flush writes to disk and can be reloaded.
    #[test]
    fn flush_persists_to_disk() {
        let dir = TempDir::new().expect("temp dir");
        let value = serde_json::json!({"key": "value"});
        {
            let cache = MetadataCache::open(dir.path());
            cache.set("persist-key".to_string(), value);
            cache.flush().expect("flush");
        }
        // Reload from disk.
        let cache2 = MetadataCache::open(dir.path());
        let retrieved = cache2.get("persist-key");
        assert_eq!(retrieved, Some(serde_json::json!({"key": "value"})));
    }

    /// Ensures `strip_jsonc_comments` removes single-line comments.
    #[test]
    fn strip_jsonc_comments_removes_single_line() {
        let input = "{\n  // comment\n  \"key\": \"value\"\n}";
        let output = strip_jsonc_comments(input);
        assert!(!output.contains("// comment"));
        assert!(output.contains("\"key\": \"value\""));
    }

    /// Ensures `strip_jsonc_comments` removes multi-line comments.
    #[test]
    fn strip_jsonc_comments_removes_multi_line() {
        let input = "{\n  /* multi\n     line */\n  \"key\": \"value\"\n}";
        let output = strip_jsonc_comments(input);
        assert!(!output.contains("/* multi"));
        assert!(output.contains("\"key\": \"value\""));
    }
}
