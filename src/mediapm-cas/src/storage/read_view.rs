//! Read-through view — provides coherent reads over cache + ObjectIndex +
//! WAL fallback.
//!
//! The [`ComposedReadView`] implements the three-layer lookup used by
//! [`CasStore`](super::store::CasStore):
//!
//! 1. In-memory cache (fast path, DashMap with TTL eviction)
//! 2. ObjectIndex (persistent committed data)
//! 3. WAL fallback (unconsumed WAL entries for latest in-flight writes)
//!
//! In-flight read dedup prevents redundant WAL scans when multiple
//! concurrent `get()` calls miss cache and ObjectIndex simultaneously.

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

use super::object_index::ObjectIndex;
use super::wal::{PendingState, Wal, WalEntry};

/// How long a cache entry lives before it is considered stale.
const CACHE_TTL: Duration = Duration::from_secs(60);

/// A pending read result that other concurrent readers can wait on.
struct PendingResult {
    done: Notify,
    result: std::sync::OnceLock<Option<Bytes>>,
}

// ---------------------------------------------------------------------------
// ReadView trait
// ---------------------------------------------------------------------------

/// Fast read path backed by materialized storage + mandatory WAL fallback.
#[async_trait]
pub trait ReadView: Send + Sync {
    /// Get bytes. Falls back to WAL if not materialized.
    /// Returns [`CasError::NotFound`] if the hash was never stored or was
    /// deleted.
    async fn get(&self, hash: &Hash) -> Result<Bytes, CasError>;

    /// Get metadata without loading payload bytes.
    async fn stat(&self, hash: &Hash) -> Result<ObjectMeta, CasError>;

    /// Notify the read path of a state change.
    /// `data` is `Some(bytes)` for puts and `None` for deletes.
    /// Best-effort hint for inline caching.
    async fn hint_state_change(&self, hash: Hash, data: Option<Bytes>);

    /// Apply a batch of WAL entries (called by WALConsumer) to refresh
    /// the in-memory cache, so subsequent reads from cache reflect the
    /// materialized state without falling through to the WAL.
    async fn apply_batch(&self, entries: Vec<WalEntry>) -> Result<(), CasError>;
}

/// Metadata about a stored object, re-exported for convenience.
#[doc(inline)]
pub use crate::api::ObjectMeta;

// ---------------------------------------------------------------------------
// ComposedReadView
// ---------------------------------------------------------------------------

/// A read-through cache backed by ObjectIndex + WAL fallback.
///
/// Implements a three-layer lookup:
/// 1. In-memory `DashMap` cache with TTL eviction
/// 2. `ObjectIndex` for committed data
/// 3. `Wal` fallback for entries not yet materialized
///
/// In-flight reads are deduplicated: if two tasks call `get` on the same
/// hash simultaneously, only one performs the ObjectIndex + WAL
/// lookup while the other waits on a [`Notify`].
pub struct ComposedReadView<S: ObjectIndex, J: Wal> {
    /// In-memory cache: hash → (timestamp, cached data).
    /// `None` data means a confirmed NotFound (tombstone cached).
    cache: DashMap<Hash, (Instant, Option<Bytes>)>,
    /// In-flight read dedup: hash → shared pending result.
    pending: DashMap<Hash, Arc<PendingResult>>,
    /// Persistent object index (committed data).
    object_index: S,
    /// WAL for pending-entry fallback.
    wal: J,
}

impl<S: ObjectIndex, J: Wal> ComposedReadView<S, J> {
    /// Create a new view.
    pub fn new(object_index: S, wal: J) -> Self {
        Self { cache: DashMap::new(), pending: DashMap::new(), object_index, wal }
    }

    /// Inner fetch: ObjectIndex → WAL fallback with transparent delta
    /// reconstruction.
    ///
    /// Returns `Ok(Some(data))` if found, `Ok(None)` if confirmed absent.
    ///
    /// Delta-encoded entries are resolved by walking the delta chain
    /// iteratively (not recursively) to avoid Rust async recursion
    /// restrictions. The base lookup bypasses L1 cache, so cache tombstones
    /// from logical deletes do not block reconstruction — base bytes still
    /// exist in ObjectIndex until the WAL consumer physically removes them
    /// (after re-materializing dependents).
    async fn fetch_inner(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        // Zero hash is always present (empty sentinel).
        if *hash == Hash::zero() {
            let empty = Bytes::new();
            self.cache.insert(*hash, (Instant::now(), Some(empty.clone())));
            return Ok(Some(empty));
        }

        // Walk the delta chain iteratively, collecting deltas to apply.
        // Each entry is the V3 envelope bytes for a delta-encoded object.
        let mut chain: Vec<(Hash, Bytes)> = Vec::new();
        let mut current = *hash;

        loop {
            // Physical fetch: ObjectIndex → WAL fallback.
            let (data, encoding): (Bytes, ObjectEncoding) = 'fetch: {
                if let Some(result) = self.object_index.get(&current).await? {
                    break 'fetch result;
                }
                match self.wal.check_pending(&current).await {
                    PendingState::Present(data) => {
                        break 'fetch (data, ObjectEncoding::Full);
                    }
                    PendingState::Tombstone => {
                        self.cache.insert(current, (Instant::now(), None));
                        return Ok(None);
                    }
                    PendingState::NotPresent => {
                        self.cache.insert(current, (Instant::now(), None));
                        return Ok(None);
                    }
                }
            };

            match encoding {
                ObjectEncoding::Full => {
                    // Found the root base. Apply collected deltas in reverse.
                    let result =
                        crate::delta::delta::resolve_delta_chain(data, &mut chain, current)?;
                    self.cache.insert(*hash, (Instant::now(), Some(result.clone())));
                    return Ok(Some(result));
                }
                ObjectEncoding::Delta { base_hash } => {
                    // Guard against self-referential cycles.
                    if current == base_hash {
                        return Err(CasError::CorruptObject {
                            hash: Some(current),
                            details: "delta self-reference detected".into(),
                        });
                    }
                    chain.push((current, data));
                    current = base_hash;
                }
            }
        }
    }
}

#[async_trait]
impl<S: ObjectIndex + Send + Sync, J: Wal + Send + Sync> ReadView for ComposedReadView<S, J> {
    async fn get(&self, hash: &Hash) -> Result<Bytes, CasError> {
        // 1. Check cache (fast path).
        if let Some(entry) = self.cache.get(hash) {
            let (ts, data) = entry.value();
            if ts.elapsed() < CACHE_TTL {
                return match data {
                    Some(bytes) => Ok(bytes.clone()),
                    None => Err(CasError::NotFound(*hash)),
                };
            }
        }

        // 2. In-flight read dedup.
        let pending_result =
            Arc::new(PendingResult { done: Notify::new(), result: std::sync::OnceLock::new() });

        use dashmap::mapref::entry::Entry;
        match self.pending.entry(*hash) {
            Entry::Occupied(e) => {
                // Another thread is already fetching — wait for it.
                let pr = e.get().clone();
                drop(e);
                pr.done.notified().await;
                if let Some(result) = pr.result.get() {
                    match result {
                        Some(bytes) => return Ok(bytes.clone()),
                        None => return Err(CasError::NotFound(*hash)),
                    }
                }
                // No result set yet (shouldn't happen after notify).
                return Err(CasError::NotFound(*hash));
            }
            Entry::Vacant(e) => {
                e.insert(pending_result.clone());
            }
        }

        // 3. Fetch from ObjectIndex → WAL.
        let fetch_result = self.fetch_inner(hash).await;

        // Share result with waiters.
        let shared = match fetch_result {
            Ok(Some(ref data)) => {
                let d = data.clone();
                pending_result.result.set(Some(d)).ok();
                data.clone()
            }
            Ok(None) => {
                pending_result.result.set(None).ok();
                pending_result.done.notify_waiters();
                self.pending.remove(hash);
                return Err(CasError::NotFound(*hash));
            }
            Err(e) => {
                pending_result.result.set(None).ok();
                pending_result.done.notify_waiters();
                self.pending.remove(hash);
                return Err(e);
            }
        };
        pending_result.done.notify_waiters();
        self.pending.remove(hash);

        Ok(shared)
    }

    async fn stat(&self, hash: &Hash) -> Result<ObjectMeta, CasError> {
        // Zero hash is always present.
        if *hash == Hash::zero() {
            return Ok(ObjectMeta { len: 0, encoding: ObjectEncoding::Full });
        }

        // Check ObjectIndex first.
        if let Some(meta) = self.object_index.stat(hash).await? {
            return Ok(meta);
        }

        // WAL fallback: data may only exist as pending Put.
        match self.wal.check_pending(hash).await {
            PendingState::Present(data) => {
                return Ok(ObjectMeta { len: data.len() as u64, encoding: ObjectEncoding::Full });
            }
            PendingState::Tombstone | PendingState::NotPresent => {}
        }

        Err(CasError::NotFound(*hash))
    }

    async fn hint_state_change(&self, hash: Hash, data: Option<Bytes>) {
        self.cache.insert(hash, (Instant::now(), data));
    }

    /// Apply a batch of WAL entries (called by WALConsumer).
    ///
    /// TODO: Wire this into BackgroundEngine so the in-memory cache is
    /// proactively refreshed when WAL entries are consumed. Currently
    /// unused because the cache is updated via `hint_state_change` during
    /// write operations.
    async fn apply_batch(&self, entries: Vec<WalEntry>) -> Result<(), CasError> {
        for entry in entries {
            match entry {
                WalEntry::Put { hash, data } => {
                    self.cache.insert(hash, (Instant::now(), Some(data)));
                }
                WalEntry::Delete { hash } => {
                    self.cache.insert(hash, (Instant::now(), None));
                }
                WalEntry::Constraint { .. } => {
                    // Constraints have no payload — no-op for read view.
                }
            }
        }
        Ok(())
    }
}
