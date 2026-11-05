//! Persistent metadata cache with TTL expiry and timer-based persistence.
//!
//! This module provides a simple JSON-based on-disk cache for resolved
//! metadata values, with configurable TTL and periodic flush semantics.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default TTL for cached metadata entries (1 day in seconds).
const METADATA_CACHE_ENTRY_TTL_SECONDS: u64 = 86_400;

/// Format version this build reads and writes.
///
/// A file carrying any other version, or none at all, is treated as carrying no
/// entries. That costs one probe per key, which is what a missing cache costs,
/// and it keeps numbers a different build wrote from being read as these ones.
/// Version 2 is the `snake_case` access field; version 1 files carry
/// `lastAccessUnixSeconds` and are discarded rather than migrated, because a
/// cache miss costs one probe and a migration would cost a reader for every
/// historical shape.
const CACHE_VERSION: u32 = 2;

/// Name of the file this cache reads and writes.
const CACHE_FILE_NAME: &str = "metadata.json";

/// One entry in the metadata cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataCacheEntry {
    /// The cached metadata JSON value.
    value: Value,
    /// Unix epoch seconds of the last access time.
    last_access_unix_seconds: u64,
}

/// The cache file as it sits on disk.
///
/// Unknown fields are ignored rather than rejected: this is a cache, not a
/// boundary type, and a field a later version adds must not turn a whole file
/// into a parse error.
#[derive(Debug, Serialize, Deserialize)]
struct CacheFile {
    /// Format version the file was written with.
    ///
    /// Required rather than defaulted: a file that names no version carries
    /// nothing this build can read, and a default would adopt it instead.
    version: u32,
    /// Entries by cache key, defaulted so a file carrying only a version reads
    /// as carrying none.
    #[serde(default)]
    entries: BTreeMap<String, MetadataCacheEntry>,
}

/// A persistent metadata cache backed by a JSON file on disk.
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
    /// If the cache file exists and names this build's format version, it is
    /// loaded into memory. Missing files, files naming no version, files naming
    /// another version, and files that fail to parse all start with an empty
    /// cache, which the next write replaces in the current shape. The cache
    /// directory is created on demand so [`flush`](Self::flush) can write its
    /// temp file (a failure to create it leaves a memory-only cache; `Drop` then
    /// skips persistence just as it does for any other write failure).
    ///
    /// Opening has no side effect on the disk: it reads, or it does not.
    #[must_use]
    pub(crate) fn open(cache_dir: &Path) -> Self {
        let _ = fs::create_dir_all(cache_dir);
        let cache_path = cache_dir.join(CACHE_FILE_NAME);
        let cache = MetadataCache {
            entries: Arc::new(Mutex::new(load_cache_file(&cache_path))),
            cache_path,
            dirty: Arc::new(Mutex::new(false)),
        };
        cache.prune_expired();
        cache
    }

    /// Retrieves a cached value by key, or `None` if missing or expired.
    ///
    /// A hit refreshes the entry's access time and, when that time actually
    /// moves, marks the cache dirty so the flush (or the drop) persists the
    /// refresh: the access time is the TTL anchor, and a touch that never
    /// reaches the file lets the next run expire an entry this run just
    /// proved fresh.
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
            // Update access time, and persist the refresh only when it moved:
            // a same-second repeat read changes nothing on disk.
            let mut entry = entry.clone();
            if entry.last_access_unix_seconds != now {
                entry.last_access_unix_seconds = now;
                *self.dirty.lock().unwrap() = true;
            }
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
    /// The bytes are published through a sibling temporary and a rename, so a
    /// reader — this process or the next run — sees the whole cache or the
    /// previous one. A save that cannot finish leaves the previous file in
    /// place, which reads as the entries it holds rather than as none.
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
        let file = CacheFile { version: CACHE_VERSION, entries: entries.clone() };
        let json_bytes = serde_json::to_vec_pretty(&file)?;
        mediapm_utils::fs::write_bytes_atomically(&self.cache_path, &json_bytes)?;

        *dirty = false;
        Ok(())
    }

    /// Prunes expired entries from memory.
    ///
    /// Marks the cache dirty only when at least one entry was removed, so a
    /// freshly loaded cache that is already clean does not trigger a
    /// needless rewrite on drop.
    pub(crate) fn prune_expired(&self) {
        let mut entries = self.entries.lock().unwrap();
        let now = unix_seconds_now();
        let len_before = entries.len();
        entries.retain(|_, entry| {
            now.saturating_sub(entry.last_access_unix_seconds) <= METADATA_CACHE_ENTRY_TTL_SECONDS
        });
        if entries.len() != len_before {
            *self.dirty.lock().unwrap() = true;
        }
    }

    /// Returns the number of entries in the cache (for testing).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn entry_count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

impl Drop for MetadataCache {
    /// Persists whatever the handle still holds unsaved.
    ///
    /// A drop cannot fail a run and cannot retry, so the only useful thing left
    /// is to say that the write was attempted and lost. The cost is bounded: a
    /// cache that misses its flush is rebuilt from the upstream the next time a
    /// key is read, which is the same outcome as a cold cache and never a wrong
    /// answer.
    fn drop(&mut self) {
        if let Ok(true) = self.dirty.lock().map(|d| *d)
            && let Err(error) = self.flush()
        {
            tracing::warn!(
                "metadata cache flush on drop failed, so this run's lookups are lost: {error}"
            );
        }
    }
}

/// Returns the current Unix epoch seconds.
fn unix_seconds_now() -> u64 {
    mediapm_utils::Timestamp::now().as_unix_secs()
}

/// Loads the cache file, answering an empty map for anything this build cannot
/// read.
///
/// A missing file, a file that is not the documented shape, and a file naming
/// another format version all answer the same way, because the caller has one
/// recovery: run the probe again and write the answer in the current shape. A
/// warning separates the cases that leave a file behind unread, so a shape that
/// keeps arriving stays visible in the logs.
fn load_cache_file(path: &Path) -> BTreeMap<String, MetadataCacheEntry> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return BTreeMap::new(),
        Err(error) => {
            tracing::warn!("ignoring unreadable metadata cache '{}': {error}", path.display());
            return BTreeMap::new();
        }
    };
    let file = match serde_json::from_str::<CacheFile>(&content) {
        Ok(file) => file,
        Err(error) => {
            tracing::warn!("ignoring malformed metadata cache '{}': {error}", path.display());
            return BTreeMap::new();
        }
    };
    if file.version != CACHE_VERSION {
        tracing::warn!(
            "ignoring metadata cache '{}': it is version {} and this build reads version \
             {CACHE_VERSION}",
            path.display(),
            file.version
        );
        return BTreeMap::new();
    }
    file.entries
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ensures a newly opened cache has zero entries.
    #[test]
    fn open_creates_empty_cache() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache = MetadataCache::open(dir.path());
        assert_eq!(cache.entry_count(), 0);
    }

    /// Ensures opening a cache on a fresh nested directory creates the
    /// parent directory (so the first flush can persist without an explicit
    /// mkdir at the call site).
    #[test]
    fn metadata_cache_open_creates_dir() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let nested = dir.path().join("nested").join("cache");
        let cache = MetadataCache::open(&nested);
        assert!(nested.is_dir(), "open must create the cache directory");
        assert_eq!(cache.entry_count(), 0);
    }

    /// Ensures opening a cache prunes entries whose last access time is
    /// older than the TTL, while keeping fresh entries.
    ///
    /// The seed goes through the encoder rather than a hand-written map, so the
    /// test reads a file in the shape production writes.
    #[test]
    fn metadata_cache_prune_expired_removes_expired() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let now = unix_seconds_now();
        let stale = now.saturating_sub(METADATA_CACHE_ENTRY_TTL_SECONDS + 1);
        let cache_path = dir.path().join(CACHE_FILE_NAME);
        let seeded = BTreeMap::from([
            (
                "expired".to_string(),
                MetadataCacheEntry {
                    value: serde_json::json!("old"),
                    last_access_unix_seconds: stale,
                },
            ),
            (
                "fresh".to_string(),
                MetadataCacheEntry {
                    value: serde_json::json!("new"),
                    last_access_unix_seconds: now,
                },
            ),
        ]);
        let seeded_file = CacheFile { version: CACHE_VERSION, entries: seeded };
        fs::write(
            &cache_path,
            serde_json::to_string_pretty(&seeded_file).expect("encode seeded cache"),
        )
        .expect("write seeded cache");

        let cache = MetadataCache::open(dir.path());
        assert_eq!(cache.entry_count(), 1, "expired entry must be pruned at open");
        assert!(cache.get("fresh").is_some(), "fresh entry must survive pruning");
        assert!(cache.get("expired").is_none(), "expired entry must be gone");
    }

    /// Ensures set + get round-trips a value.
    #[test]
    fn set_and_get_round_trips_value() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache = MetadataCache::open(dir.path());
        let value = serde_json::json!({"title": "Test Video", "artist": "Test Artist"});
        cache.set("test-key".to_string(), value.clone());
        let retrieved = cache.get("test-key");
        assert_eq!(retrieved, Some(value));
    }

    /// Ensures get returns None for missing keys.
    #[test]
    fn get_returns_none_for_missing_key() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache = MetadataCache::open(dir.path());
        assert!(cache.get("nonexistent").is_none());
    }

    /// Ensures flush writes to disk and can be reloaded.
    #[test]
    fn flush_persists_to_disk() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
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

    #[test]
    fn overwrite_updates_value() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache = MetadataCache::open(dir.path());
        cache.set("k".to_string(), serde_json::json!("v1"));
        cache.set("k".to_string(), serde_json::json!("v2"));
        let retrieved = cache.get("k");
        assert_eq!(retrieved, Some(serde_json::json!("v2")));
    }

    #[test]
    fn large_value_round_trips() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let value = serde_json::json!((0..1000).collect::<Vec<i32>>());
        {
            let cache = MetadataCache::open(dir.path());
            cache.set("large".to_string(), value.clone());
            cache.flush().expect("flush");
        }
        let cache = MetadataCache::open(dir.path());
        let retrieved = cache.get("large");
        assert_eq!(retrieved, Some(value));
    }

    /// The cache file names the shape it carries, so a reader can tell which
    /// build wrote it before trusting a number in it.
    #[test]
    fn metadata_cache_file_carries_its_version() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        {
            let cache = MetadataCache::open(dir.path());
            cache.set(
                "ffprobe:/library/song.mkv".to_string(),
                serde_json::json!({"format": "matroska"}),
            );
            cache.flush().expect("flush");
        }

        let text = fs::read_to_string(dir.path().join(CACHE_FILE_NAME)).expect("read cache file");
        let parsed: Value = serde_json::from_str(&text).expect("cache file is json");
        assert_eq!(
            parsed["version"],
            serde_json::json!(2),
            "the file names the format version it carries: {text}"
        );
        assert_eq!(
            parsed["entries"]["ffprobe:/library/song.mkv"]["value"]["format"],
            serde_json::json!("matroska"),
            "every entry lives under `entries`: {text}"
        );
        let object = parsed.as_object().expect("cache file is an object");
        let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["entries", "version"],
            "the file holds the version and the entries and nothing else: {text}"
        );
    }

    /// A version-1 file is discarded by a version-2 build and rebuilt in the
    /// current shape.
    ///
    /// The camelCase access field belonged to version 1; reading it under a
    /// version-2 name (or a version-1 file under version 2) would attribute
    /// numbers to the wrong scheme. The seed is hand-written in the shape a
    /// version-1 build actually wrote, independent of this build's encoder.
    #[test]
    fn a_version_one_metadata_file_is_discarded_and_rebuilt() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache_path = dir.path().join(CACHE_FILE_NAME);
        let v1_file = serde_json::json!({
            "version": 1,
            "entries": {
                "ffprobe:/library/song.mkv": {
                    "value": {"format": "matroska"},
                    "lastAccessUnixSeconds": unix_seconds_now(),
                }
            }
        });
        fs::write(&cache_path, serde_json::to_string_pretty(&v1_file).expect("encode v1 cache"))
            .expect("write v1 cache");

        let cache = MetadataCache::open(dir.path());
        assert_eq!(
            cache.entry_count(),
            0,
            "a version-1 file carries no entry a version-2 build can read"
        );
        assert!(cache.get("ffprobe:/library/song.mkv").is_none());

        cache.set("ffprobe:/library/fresh.mkv".to_string(), serde_json::json!({"format": "mp4"}));
        cache.flush().expect("flush the rebuild");

        let text = fs::read_to_string(&cache_path).expect("read cache file");
        let parsed: Value = serde_json::from_str(&text).expect("cache file is json");
        assert_eq!(
            parsed["version"],
            serde_json::json!(2),
            "the rebuilt file names the current format version: {text}"
        );
        assert!(
            text.contains("\"last_access_unix_seconds\""),
            "the rebuilt file carries the snake_case access field: {text}"
        );
        assert!(
            !text.contains("lastAccessUnixSeconds"),
            "the rebuilt file no longer carries the version-1 spelling: {text}"
        );
    }

    /// A file that names no version carries nothing this build can read.
    ///
    /// The bare map the cache once held is not a shape with a version, and
    /// reading it as one would adopt numbers whose meaning the file never
    /// stated. It is treated as absent instead, so the next write replaces it
    /// with a file that does name its shape.
    #[test]
    fn a_versionless_metadata_file_is_read_as_empty() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache_path = dir.path().join(CACHE_FILE_NAME);
        let seeded = BTreeMap::from([(
            "ffprobe:/library/song.mkv".to_string(),
            MetadataCacheEntry {
                value: serde_json::json!({"format": "matroska"}),
                last_access_unix_seconds: unix_seconds_now(),
            },
        )]);
        fs::write(&cache_path, serde_json::to_string_pretty(&seeded).expect("encode seeded"))
            .expect("write versionless cache");

        let cache = MetadataCache::open(dir.path());

        assert_eq!(
            cache.entry_count(),
            0,
            "a versionless file carries no entry this build can read"
        );
        assert!(cache.get("ffprobe:/library/song.mkv").is_none());

        cache.set("ffprobe:/library/other.mkv".to_string(), serde_json::json!({"format": "mp4"}));
        cache.flush().expect("flush");
        let reopened = MetadataCache::open(dir.path());
        assert_eq!(
            reopened.get("ffprobe:/library/other.mkv"),
            Some(serde_json::json!({"format": "mp4"})),
            "the write that follows the rebuild leaves a file this build reads"
        );
    }

    /// A file naming a version this build does not read carries nothing.
    #[test]
    fn a_metadata_file_from_an_unknown_version_is_read_as_empty() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache_path = dir.path().join(CACHE_FILE_NAME);
        let foreign = serde_json::json!({
            "version": 99,
            "entries": {
                "ffprobe:/library/song.mkv": {
                    "value": {"format": "matroska"},
                    "last_access_unix_seconds": unix_seconds_now(),
                }
            }
        });
        fs::write(&cache_path, serde_json::to_string_pretty(&foreign).expect("encode foreign"))
            .expect("write foreign cache");

        let cache = MetadataCache::open(dir.path());

        assert_eq!(
            cache.entry_count(),
            0,
            "a file from another version must not be read as this build's entries"
        );
        assert!(cache.get("ffprobe:/library/song.mkv").is_none());
    }

    #[test]
    fn corrupt_file_starts_empty() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache_file = dir.path().join(CACHE_FILE_NAME);
        fs::write(&cache_file, "not valid json").expect("write corrupt file");
        let cache = MetadataCache::open(dir.path());
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn multiple_entries_independent() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache = MetadataCache::open(dir.path());
        for i in 0..5 {
            cache.set(format!("key{i}"), serde_json::json!({"index": i}));
        }
        for i in 0..5 {
            let retrieved = cache.get(&format!("key{i}"));
            assert_eq!(retrieved, Some(serde_json::json!({"index": i})));
        }
    }

    /// A drop that cannot persist says so instead of discarding the loss.
    ///
    /// The write is best effort by nature: a drop cannot fail a run and cannot
    /// retry. Silence is the one disposition it must not choose, because a cache
    /// that never persists is visible only as repeated upstream lookups. A
    /// directory sits where the cache file belongs, which denies the write for
    /// every user including root, so the failure does not depend on the mode the
    /// process runs under.
    #[test]
    fn a_drop_that_cannot_flush_reports_the_loss() {
        use std::sync::{Arc, Mutex};

        /// Collects what the subscriber formats so the test can read it back.
        #[derive(Clone)]
        struct Collected(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for Collected {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("buffer").write(bytes)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join(CACHE_FILE_NAME))
            .expect("a directory where the cache file belongs");
        let cache = MetadataCache::open(dir.path());
        cache.set("unpersistable".to_string(), serde_json::json!({"key": "value"}));

        let collected = Collected(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_writer({
                let sink = collected.clone();
                move || sink.clone()
            })
            .with_max_level(tracing::Level::WARN)
            .finish();
        tracing::subscriber::with_default(subscriber, || drop(cache));

        let logged = String::from_utf8(collected.0.lock().expect("buffer").clone())
            .expect("the subscriber writes UTF-8");
        assert!(
            logged.contains("metadata cache flush on drop failed"),
            "a flush lost at drop names itself rather than leaving the run to look like a cache \
             miss: {logged}"
        );
    }

    /// A read that refreshes an entry's access time has to reach the file.
    ///
    /// The access time is the TTL anchor: a touch left only in memory reads
    /// as the old access time to the next run, which then expires an entry
    /// this run just proved fresh. The seeded time is 100 seconds behind the
    /// touch, so the two are distinguishable at second granularity.
    #[test]
    fn a_read_touch_survives_a_reopen() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache_path = dir.path().join(CACHE_FILE_NAME);
        let seeded_access = unix_seconds_now().saturating_sub(100);
        let seeded = BTreeMap::from([(
            "k".to_string(),
            MetadataCacheEntry {
                value: serde_json::json!("v"),
                last_access_unix_seconds: seeded_access,
            },
        )]);
        let seeded_file = CacheFile { version: CACHE_VERSION, entries: seeded };
        fs::write(
            &cache_path,
            serde_json::to_string_pretty(&seeded_file).expect("encode seeded cache"),
        )
        .expect("write seeded cache");

        {
            let cache = MetadataCache::open(dir.path());
            assert_eq!(
                cache.get("k"),
                Some(serde_json::json!("v")),
                "the seeded entry must answer the read that touches it"
            );
            cache.flush().expect("flush the touch");
        }

        let text = fs::read_to_string(&cache_path).expect("read cache file");
        // Parsed through the struct rather than a field name in this test:
        // the persisted field's spelling is Step 8's change, and this test
        // must hold before and after it.
        let file: CacheFile = serde_json::from_str(&text).expect("cache file decodes");
        let entry = file.entries.get("k").expect("the touched entry is persisted");
        let persisted = entry.last_access_unix_seconds;
        assert!(
            persisted > seeded_access,
            "the touch the read performed must reach the file, so the next run does not expire an \
             entry this run just proved fresh: {text}"
        );
    }

    #[test]
    fn flush_then_set_does_not_lose_new_values() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        {
            let cache = MetadataCache::open(dir.path());
            cache.set("k1".to_string(), serde_json::json!("flushed"));
            cache.flush().expect("first flush");
            cache.set("k2".to_string(), serde_json::json!("unflushed"));
            // Drop flushes again, persisting both.
        }
        let cache = MetadataCache::open(dir.path());
        assert_eq!(cache.get("k1"), Some(serde_json::json!("flushed")));
        assert_eq!(cache.get("k2"), Some(serde_json::json!("unflushed")));
    }
}
