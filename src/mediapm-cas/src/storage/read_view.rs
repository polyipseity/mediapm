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
use super::wal::{PendingState, Wal};

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
        // Check Index for metadata.
        let entry = match self.index.get(hash).await? {
            Some(e) => e,
            None => {
                // WAL fallback: data may only exist as pending Put.
                return match self.wal.check_pending(hash).await {
                    PendingState::Present(data) => Ok(Some(data)),
                    PendingState::Tombstone | PendingState::NotPresent => Ok(None),
                };
            }
        };

        // Before reading payload, check the WAL for a pending Delete
        // (tombstone) that hasn't been consumed yet.  This ensures
        // WAL-only (background) deletes are visible immediately.
        match self.wal.check_pending(hash).await {
            PendingState::Tombstone => return Ok(None),
            PendingState::Present(_) | PendingState::NotPresent => {}
        }

        match entry.encoding {
            ObjectEncoding::Full => {
                return self.blob_store.read(hash).await.map(Some);
            }
            ObjectEncoding::Delta { base_hash } => {
                return super::delta_resolve::resolve_delta_chain(
                    hash,
                    base_hash,
                    &self.index,
                    &self.blob_store,
                    "delta self-reference detected",
                    "delta chain: base",
                )
                .await
                .map(Some);
            }
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
        // Check Index first.
        if let Some(entry) = self.index.get(hash).await? {
            // Before returning metadata, check the WAL for a pending Delete
            // (tombstone) that hasn't been consumed yet.
            match self.wal.check_pending(hash).await {
                PendingState::Tombstone => {}
                _ => return Ok(entry.as_meta()),
            }
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
}
