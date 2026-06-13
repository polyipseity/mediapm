//! Read-through view — provides coherent reads over Index + BlobStore +
//! WAL fallback.
//!
//! The [`ComposedReadView`] implements a three-layer lookup used by
//! [`CasStore`](super::store::CasStore):
//!
//! 1. [`Index`](super::index::Index) — metadata (encoding, size, bases).
//! 2. [`BlobStore`](super::blob_store::BlobStore) — payload bytes (full
//!    or delta).
//! 3. WAL fallback — entries not yet materialized into BlobStore/Index.
//!
//! In-flight read dedup prevents redundant blob-store reads when multiple
//! concurrent `get()` calls miss Index simultaneously.

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Notify;

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

use super::blob_store::BlobStore;
use super::index::Index;
use super::wal::{PendingState, Wal, WalEntry};

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

/// A read-through view backed by Index + BlobStore + WAL fallback.
///
/// Implements a three-layer lookup:
/// 1. `Index` for metadata (encoding, size).
/// 2. `BlobStore` for payload bytes.
/// 3. `Wal` fallback for entries not yet materialized.
///
/// In-flight reads are deduplicated: if two tasks call `get` on the same
/// hash simultaneously, only one performs the lookup while the other waits
/// on a [`Notify`].
pub struct ComposedReadView<I: Index, J: Wal, B: BlobStore> {
    /// In-flight read dedup: hash → shared pending result.
    pending: DashMap<Hash, Arc<PendingResult>>,
    /// Metadata index (encoding, size, constraint bases).
    index: I,
    /// WAL for pending-entry fallback.
    wal: J,
    /// Blob store for persistent payload bytes.
    blob_store: B,
}

impl<I: Index, J: Wal, B: BlobStore> ComposedReadView<I, J, B> {
    /// Create a new view.
    pub fn new(index: I, wal: J, blob_store: B) -> Self {
        Self { pending: DashMap::new(), index, wal, blob_store }
    }

    /// Inner fetch: Index + BlobStore → WAL fallback with transparent delta
    /// reconstruction.
    ///
    /// Returns `Ok(Some(data))` if found, `Ok(None)` if confirmed absent.
    ///
    /// Delta-encoded entries are resolved by walking the delta chain
    /// iteratively (not recursively) to avoid Rust async recursion
    /// restrictions.
    async fn fetch_inner(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        // Zero hash is always present (empty sentinel).
        if *hash == Hash::zero() {
            return Ok(Some(Bytes::new()));
        }

        // Check Index for metadata.
        if let Some(entry) = self.index.get(hash).await? {
            match entry.encoding {
                ObjectEncoding::Full => {
                    return self.blob_store.read(hash).await.map(Some);
                }
                ObjectEncoding::Delta { base_hash } => {
                    // Walk the delta chain iteratively.
                    let mut chain: Vec<(Hash, Bytes)> = Vec::new();
                    let mut current = *hash;
                    let mut base = base_hash;

                    loop {
                        if current == base {
                            // Guard against self-referential cycles.
                            return Err(CasError::CorruptObject {
                                hash: Some(current),
                                details: "delta self-reference detected".into(),
                            });
                        }
                        // Read delta envelope from blob store.
                        let delta_data = self.blob_store.read_delta(&current).await?;
                        chain.push((current, delta_data));
                        current = base;

                        // Check the base's encoding in Index.
                        match self.index.get(&current).await? {
                            Some(base_entry) => match base_entry.encoding {
                                ObjectEncoding::Full => {
                                    let base_data = self.blob_store.read(&current).await?;
                                    return crate::delta::delta::resolve_delta_chain(
                                        base_data, &mut chain, current,
                                    )
                                    .map(Some);
                                }
                                ObjectEncoding::Delta { base_hash: next_base } => {
                                    // Continue chain with this base as the new current.
                                    base = next_base;
                                }
                            },
                            None => {
                                return Err(CasError::CorruptObject {
                                    hash: Some(current),
                                    details: format!(
                                        "delta chain: base {current} not found in index"
                                    ),
                                });
                            }
                        }
                    }
                }
            }
        }

        // WAL fallback: data may only exist as pending Put.
        let wal_result = self.wal.check_pending(hash).await;
        match wal_result {
            PendingState::Present(data) => Ok(Some(data)),
            PendingState::Tombstone | PendingState::NotPresent => Ok(None),
        }
    }
}

#[async_trait]
impl<I: Index + Send + Sync, J: Wal + Send + Sync, B: BlobStore + Send + Sync> ReadView
    for ComposedReadView<I, J, B>
{
    async fn get(&self, hash: &Hash) -> Result<Bytes, CasError> {
        // 1. In-flight read dedup.
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

        // 2. Fetch from Index + BlobStore → WAL.
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

        // Check Index first.
        if let Some(entry) = self.index.get(hash).await? {
            return Ok(entry.as_meta());
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

    async fn hint_state_change(&self, _hash: Hash, _data: Option<Bytes>) {
        // Inline cache was removed; this is a no-op.
    }

    /// Apply a batch of WAL entries (called by WALConsumer).
    ///
    /// No-op since the in-memory cache was removed.
    async fn apply_batch(&self, _entries: Vec<WalEntry>) -> Result<(), CasError> {
        Ok(())
    }
}
