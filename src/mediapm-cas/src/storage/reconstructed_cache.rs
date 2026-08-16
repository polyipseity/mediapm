//! Bounded, time-based cache for reconstructed full bytes.
//!
//! Delta-encoded objects are reconstructed by walking their delta chain,
//! which costs N blob reads. The [`ReconstructedBytesCache`] holds the
//! reconstructed bytes so repeated reads of the same hash skip the walk.
//!
//! Semantics (spec: `src/mediapm-cas/AGENTS.md` §5.6):
//! - Entries expire `ttl` after insertion (measured from insert time, not
//!   access time).
//! - The cache never exceeds `max_bytes` of total entry bytes: when an
//!   insert would exceed the budget, the oldest half of entries (by
//!   insertion time) is evicted, repeated until the new entry fits.
//! - Entries larger than `max_bytes / 4` are skipped (they would thrash
//!   the cache).
//! - The byte budget is established externally from the metadata store
//!   (async — see [`compute_store_bytes`]); until set, `max_bytes` is 0
//!   and inserts are skipped.
//! - `invalidate` removes an entry (used on deletion); a deleted object
//!   must never be served stale cached bytes.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::error::CasError;
use crate::hash::Hash;

use super::metadata_store::MetadataStore;

/// Snapshot of reconstructed-bytes cache statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconstructedCacheStats {
    /// Number of lookups that returned cached bytes.
    pub hits: u64,
    /// Number of lookups that found nothing (unknown or expired hash).
    pub misses: u64,
    /// Number of live entries.
    pub entries: usize,
    /// Total bytes currently held.
    pub cached_bytes: u64,
    /// Current byte budget (0 until established from metadata).
    pub max_bytes: u64,
}

/// Bounded, time-based cache of reconstructed full bytes.
pub struct ReconstructedBytesCache {
    entries: Mutex<HashMap<Hash, (Bytes, Instant)>>,
    cached_bytes: AtomicU64,
    max_bytes: AtomicU64,
    ttl: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl ReconstructedBytesCache {
    /// Create a cache with the given entry TTL. A `Duration::ZERO` TTL
    /// disables caching entirely (lookups always miss, inserts no-op).
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            cached_bytes: AtomicU64::new(0),
            max_bytes: AtomicU64::new(0),
            ttl,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Whether caching is enabled (non-zero TTL).
    fn enabled(&self) -> bool {
        self.ttl > Duration::ZERO
    }

    /// Return cached bytes for `hash`, or `None` on miss/expiry. Counts
    /// hits and misses.
    pub fn get(&self, hash: &Hash) -> Option<Bytes> {
        if !self.enabled() {
            return None;
        }
        let entries = self.entries.lock().unwrap();
        match entries.get(hash) {
            Some((data, inserted_at)) if inserted_at.elapsed() < self.ttl => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(data.clone())
            }
            _ => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert reconstructed bytes for `hash`. No-op when disabled, when
    /// the byte budget is not yet established (`max_bytes == 0`), or when
    /// the entry is disproportionately large (`> max_bytes / 4`).
    pub fn insert(&self, hash: Hash, bytes: Bytes) {
        if !self.enabled() {
            return;
        }
        let entry_size = bytes.len() as u64;
        let max_bytes = self.max_bytes.load(Ordering::Relaxed);
        if max_bytes == 0 || entry_size > max_bytes / 4 {
            return;
        }
        let mut entries = self.entries.lock().unwrap();
        // Evict the oldest half of entries until the new entry fits.
        while self.cached_bytes.load(Ordering::Relaxed) + entry_size > max_bytes {
            if !Self::evict_oldest_half(&mut entries, &self.cached_bytes) {
                break;
            }
        }
        // Replace any existing entry, adjusting the byte total.
        if let Some((old, _)) = entries.insert(hash, (bytes, Instant::now())) {
            self.cached_bytes.fetch_sub(old.len() as u64, Ordering::Relaxed);
        }
        self.cached_bytes.fetch_add(entry_size, Ordering::Relaxed);
    }

    /// Remove `hash` from the cache (used on deletion).
    pub fn invalidate(&self, hash: &Hash) {
        if !self.enabled() {
            return;
        }
        let mut entries = self.entries.lock().unwrap();
        if let Some((old, _)) = entries.remove(hash) {
            self.cached_bytes.fetch_sub(old.len() as u64, Ordering::Relaxed);
        }
    }

    /// Set the byte budget, evicting oldest entries first if the new
    /// budget is smaller than the current cached total.
    pub fn set_max_bytes(&self, bytes: u64) {
        self.max_bytes.store(bytes, Ordering::Relaxed);
        let mut entries = self.entries.lock().unwrap();
        while self.cached_bytes.load(Ordering::Relaxed) > bytes {
            if !Self::evict_oldest_half(&mut entries, &self.cached_bytes) {
                break;
            }
        }
    }

    /// Current byte budget (0 until established from metadata).
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes.load(Ordering::Relaxed)
    }

    /// Snapshot of cache statistics (for tests and observability).
    pub fn stats(&self) -> ReconstructedCacheStats {
        let entries = self.entries.lock().unwrap();
        ReconstructedCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: entries.len(),
            cached_bytes: self.cached_bytes.load(Ordering::Relaxed),
            max_bytes: self.max_bytes.load(Ordering::Relaxed),
        }
    }

    /// Remove the oldest half of entries (by insertion time); returns
    /// `false` when the cache is empty.
    fn evict_oldest_half(
        entries: &mut HashMap<Hash, (Bytes, Instant)>,
        cached_bytes: &AtomicU64,
    ) -> bool {
        if entries.is_empty() {
            return false;
        }
        // Collect owned (Hash, Instant) pairs first so no borrow of
        // `entries` outlives the collect; `remove` needs a mutable borrow.
        let mut by_age: Vec<(Hash, Instant)> = entries.iter().map(|(h, (_, t))| (*h, *t)).collect();
        by_age.sort_by_key(|(_, t)| *t);
        by_age.truncate((by_age.len() / 2).max(1));
        for (hash, _) in by_age {
            if let Some((evicted, _)) = entries.remove(&hash) {
                cached_bytes.fetch_sub(evicted.len() as u64, Ordering::Relaxed);
            }
        }
        true
    }
}

/// Total store bytes on disk: the sum of all metadata entry lengths.
pub(crate) async fn compute_store_bytes<M: MetadataStore>(metadata: &M) -> Result<u64, CasError> {
    let mut total: u64 = 0;
    for hash in metadata.list_hashes().await? {
        if let Some(entry) = metadata.get(&hash).await? {
            total = total.saturating_add(entry.len);
        }
    }
    Ok(total)
}

/// Byte budget for the reconstructed-bytes cache from total store bytes:
/// `CACHE_MAX_FRACTION_OF_TOTAL_SIZE` of the store, floored at 1 byte.
pub(crate) fn budget_from_store_bytes(total_store_bytes: u64) -> u64 {
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let limit =
        (total_store_bytes as f64 * crate::defaults::CACHE_MAX_FRACTION_OF_TOTAL_SIZE) as u64;
    limit.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(seed: u8) -> Hash {
        Hash::from_content(&[seed; 8])
    }

    #[test]
    fn reconstructed_cache_hit_returns_cached_bytes() {
        let cache = ReconstructedBytesCache::new(Duration::from_mins(1));
        cache.set_max_bytes(1024);
        cache.insert(hash(1), Bytes::from(vec![0xAA; 16]));
        assert_eq!(cache.get(&hash(1)), Some(Bytes::from(vec![0xAA; 16])));
        let stats = cache.stats();
        assert_eq!((stats.hits, stats.misses), (1, 0));
    }

    #[test]
    fn reconstructed_cache_miss_returns_none() {
        let cache = ReconstructedBytesCache::new(Duration::from_mins(1));
        cache.set_max_bytes(1024);
        assert_eq!(cache.get(&hash(1)), None);
        let stats = cache.stats();
        assert_eq!((stats.hits, stats.misses), (0, 1));
    }

    #[test]
    fn reconstructed_cache_ttl_expiry_returns_none() {
        let cache = ReconstructedBytesCache::new(Duration::from_millis(10));
        cache.set_max_bytes(1024);
        cache.insert(hash(1), Bytes::from(vec![0xAA; 16]));
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(cache.get(&hash(1)), None, "expired entry is a miss");
    }

    #[test]
    fn reconstructed_cache_ttl_zero_disables() {
        let cache = ReconstructedBytesCache::new(Duration::ZERO);
        cache.set_max_bytes(1024);
        cache.insert(hash(1), Bytes::from(vec![0xAA; 16]));
        assert_eq!(cache.get(&hash(1)), None, "zero TTL never serves entries");
        let stats = cache.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.cached_bytes, 0);
    }

    #[test]
    fn reconstructed_cache_evicts_oldest_first() {
        let cache = ReconstructedBytesCache::new(Duration::from_mins(1));
        cache.set_max_bytes(100);
        cache.insert(hash(1), Bytes::from(vec![0x11; 20]));
        cache.insert(hash(2), Bytes::from(vec![0x22; 20]));
        cache.insert(hash(3), Bytes::from(vec![0x33; 20]));
        cache.insert(hash(4), Bytes::from(vec![0x44; 20]));
        cache.insert(hash(5), Bytes::from(vec![0x55; 20]));
        // 100 + 20 = 120 > 100 → evict the oldest half of 5 (2 entries:
        // h1, h2) → 60 + 20 = 80 ≤ 100.
        cache.insert(hash(6), Bytes::from(vec![0x66; 20]));
        assert_eq!(cache.get(&hash(1)), None, "oldest entry evicted first");
        assert_eq!(cache.get(&hash(2)), None, "second-oldest entry evicted first");
        assert_eq!(cache.get(&hash(3)), Some(Bytes::from(vec![0x33; 20])), "newer entries survive");
        assert_eq!(cache.get(&hash(4)), Some(Bytes::from(vec![0x44; 20])));
        assert_eq!(cache.get(&hash(5)), Some(Bytes::from(vec![0x55; 20])));
        assert_eq!(cache.get(&hash(6)), Some(Bytes::from(vec![0x66; 20])));
        let stats = cache.stats();
        assert!(stats.cached_bytes <= 100, "cached bytes stay within budget");
        assert_eq!(stats.cached_bytes, 80);
        assert_eq!(stats.entries, 4);
    }

    #[test]
    fn reconstructed_cache_budget_is_bytes_not_count() {
        let cache = ReconstructedBytesCache::new(Duration::from_mins(1));
        // Budget of 100 bytes: a count-based budget would fit 5 × 20-byte
        // entries; the byte budget must reject the 6th.
        cache.set_max_bytes(100);
        cache.insert(hash(1), Bytes::from(vec![0x11; 20]));
        cache.insert(hash(2), Bytes::from(vec![0x22; 20]));
        cache.insert(hash(3), Bytes::from(vec![0x33; 20]));
        cache.insert(hash(4), Bytes::from(vec![0x44; 20]));
        cache.insert(hash(5), Bytes::from(vec![0x55; 20]));
        // 100 + 20 = 120 > 100 → evict oldest half of 5 (h1, h2) → 80 ≤ 100.
        cache.insert(hash(6), Bytes::from(vec![0x66; 20]));
        let stats = cache.stats();
        assert!(stats.cached_bytes <= 100, "byte budget respected");
        assert_eq!(stats.cached_bytes, 80);
        assert_eq!(stats.entries, 4);
        assert_eq!(cache.get(&hash(1)), None);
        assert_eq!(cache.get(&hash(2)), None);
        assert_eq!(cache.get(&hash(3)), Some(Bytes::from(vec![0x33; 20])));
        assert_eq!(cache.get(&hash(4)), Some(Bytes::from(vec![0x44; 20])));
        assert_eq!(cache.get(&hash(5)), Some(Bytes::from(vec![0x55; 20])));
        assert_eq!(cache.get(&hash(6)), Some(Bytes::from(vec![0x66; 20])));
    }

    #[test]
    fn reconstructed_cache_set_max_bytes_shrinks_budget() {
        let cache = ReconstructedBytesCache::new(Duration::from_mins(1));
        cache.set_max_bytes(1000);
        cache.insert(hash(1), Bytes::from(vec![0x11; 30]));
        cache.insert(hash(2), Bytes::from(vec![0x22; 30]));
        cache.insert(hash(3), Bytes::from(vec![0x33; 30]));
        // Shrink the budget below the cached total: oldest half evicted.
        cache.set_max_bytes(60);
        let stats = cache.stats();
        assert!(stats.cached_bytes <= 60, "budget shrink evicts oldest entries");
        assert_eq!(cache.get(&hash(1)), None, "oldest entry evicted on shrink");
        assert_eq!(stats.entries, 2);
    }

    #[test]
    fn reconstructed_cache_skip_large_entry() {
        let cache = ReconstructedBytesCache::new(Duration::from_mins(1));
        cache.set_max_bytes(100);
        cache.insert(hash(1), Bytes::from(vec![0x11; 20]));
        // 1000 > 100 / 4 → skipped.
        cache.insert(hash(2), Bytes::from(vec![0x22; 1000]));
        let stats = cache.stats();
        assert_eq!(stats.entries, 1, "large entry not cached");
        assert_eq!(stats.cached_bytes, 20);
        assert_eq!(cache.get(&hash(2)), None);
    }

    #[test]
    fn reconstructed_cache_invalidate_removes_entry() {
        let cache = ReconstructedBytesCache::new(Duration::from_mins(1));
        cache.set_max_bytes(1024);
        cache.insert(hash(1), Bytes::from(vec![0x11; 30]));
        cache.insert(hash(2), Bytes::from(vec![0x22; 30]));
        cache.invalidate(&hash(1));
        assert_eq!(cache.get(&hash(1)), None);
        assert_eq!(cache.get(&hash(2)), Some(Bytes::from(vec![0x22; 30])));
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.cached_bytes, 30);
    }

    #[test]
    fn reconstructed_cache_budget_fraction_of_store_bytes() {
        assert_eq!(budget_from_store_bytes(10_000), 1_000);
        assert_eq!(budget_from_store_bytes(0), 1, "budget floored at 1 byte");
    }
}
