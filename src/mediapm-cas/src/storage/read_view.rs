//! Read-through view — provides coherent reads over Metadata + BlobStore +
//! WAL fallback.
//!
//! The [`ComposedReadView`] implements a three-layer lookup used by
//! [`CasStore`](super::store::CasStore):
//!
//! 1. [`Metadata`](super::metadata::Metadata) — metadata (encoding, size, bases).
//! 2. [`BlobStore`](super::blob_store::BlobStore) — payload bytes (full
//!    or delta).
//! 3. WAL fallback — entries not yet materialized into BlobStore/Metadata.
//!
//! In-flight read dedup prevents redundant blob-store reads when multiple
//! concurrent `get()` calls miss Metadata simultaneously.

use std::collections::HashSet;

use async_trait::async_trait;
use bytes::Bytes;

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

use super::blob_store::BlobStore;
use super::metadata::{Metadata, MetadataEntry};
use super::pending_ops::PendingOps;
use super::wal::{PendingState, Wal};

// ---------------------------------------------------------------------------
// ReadView trait
// ---------------------------------------------------------------------------

/// Fast read path backed by materialized storage + mandatory WAL fallback.
#[async_trait]
pub(crate) trait ReadView: Send + Sync {
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

/// A read-through view backed by Metadata + BlobStore + WAL fallback.
///
/// Implements a three-layer lookup:
/// 1. `Metadata` for metadata (encoding, size).
/// 2. `BlobStore` for payload bytes.
/// 3. `Wal` fallback for entries not yet materialized.
///
/// In-flight reads are deduplicated: if two tasks call `get` on the same
/// hash simultaneously, only one performs the lookup while the other waits
/// for the shared result (see [`PendingOps`]).
pub(crate) struct ComposedReadView<M: Metadata, J: Wal, B: BlobStore> {
    pending: PendingOps,
    metadata: M,
    wal: J,
    blob_store: B,
}

impl<M: Metadata, J: Wal, B: BlobStore> ComposedReadView<M, J, B> {
    /// Create a new view.
    pub fn new(metadata: M, wal: J, blob_store: B) -> Self {
        Self { pending: PendingOps::new(), metadata, wal, blob_store }
    }

    /// Inner fetch: Metadata + BlobStore → WAL fallback with transparent delta
    /// reconstruction.
    ///
    /// Returns `Ok(Some(data))` if found, `Ok(None)` if confirmed absent.
    ///
    /// Delta-encoded entries are resolved by walking the delta chain
    /// iteratively (not recursively) to avoid Rust async recursion
    /// restrictions.
    async fn fetch_inner(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        // Check Metadata for metadata.
        let entry = match self.metadata.get(hash).await? {
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

        resolve_full_bytes(
            hash,
            &entry,
            &self.metadata,
            &self.blob_store,
            "delta self-reference detected",
            "delta chain: base",
        )
        .await
        .map(Some)
    }
}

#[async_trait]
impl<M: Metadata + Send + Sync, J: Wal + Send + Sync, B: BlobStore + Send + Sync> ReadView
    for ComposedReadView<M, J, B>
{
    async fn get(&self, hash: &Hash) -> Result<Bytes, CasError> {
        self.pending
            .execute(*hash, || self.fetch_inner(hash))
            .await?
            .ok_or(CasError::NotFound(*hash))
    }

    async fn stat(&self, hash: &Hash) -> Result<ObjectMeta, CasError> {
        // Check Metadata first.
        if let Some(entry) = self.metadata.get(hash).await? {
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

// ---------------------------------------------------------------------------
// Shared delta-chain resolution
// ---------------------------------------------------------------------------

/// Given a metadata entry, read full bytes — either directly (Full) or by
/// resolving the delta chain (Delta).
pub(super) async fn resolve_full_bytes<M: Metadata, B: BlobStore>(
    hash: &Hash,
    entry: &MetadataEntry,
    metadata: &M,
    blob_store: &B,
    self_ref_msg: &str,
    base_not_found_msg: &str,
) -> Result<Bytes, CasError> {
    match entry.encoding {
        ObjectEncoding::Full => blob_store.read(hash).await,
        ObjectEncoding::Delta { base_hash } => {
            resolve_delta_chain(
                hash,
                base_hash,
                metadata,
                blob_store,
                self_ref_msg,
                base_not_found_msg,
            )
            .await
        }
    }
}

/// Reconstruct full bytes for `hash` by walking its delta chain.
///
/// Callers provide the starting `base_hash` from the object's encoding and
/// context strings for error messages. Returns `Ok(full_bytes)` on success.
pub(super) async fn resolve_delta_chain<M: Metadata, B: BlobStore>(
    hash: &Hash,
    base_hash: Hash,
    metadata: &M,
    blob_store: &B,
    self_ref_msg: &str,
    base_not_found_msg: &str,
) -> Result<Bytes, CasError> {
    let mut chain: Vec<(Hash, Bytes)> = Vec::new();
    let mut current = *hash;
    let mut base = base_hash;
    let mut visited: HashSet<Hash> = HashSet::new();
    visited.insert(*hash);

    loop {
        if current == base {
            return Err(CasError::CorruptObject {
                hash: Some(current),
                details: self_ref_msg.into(),
            });
        }
        // Multi-step cycle detection: A → B → A
        if !visited.insert(base) {
            return Err(CasError::CorruptObject {
                hash: Some(current),
                details: format!("delta chain cycle detected: base {base} already visited"),
            });
        }
        let delta_data = blob_store.read_delta(&current).await?;
        chain.push((current, delta_data));
        current = base;

        match metadata.get(&current).await? {
            Some(base_entry) => match base_entry.encoding {
                ObjectEncoding::Full => {
                    let base_data = blob_store.read(&current).await?;
                    return crate::delta::patch::apply_delta_chain(base_data, &mut chain, current);
                }
                ObjectEncoding::Delta { base_hash: next_base } => {
                    base = next_base;
                }
            },
            None => {
                return Err(CasError::CorruptObject {
                    hash: Some(current),
                    details: format!("{base_not_found_msg}: base {current} not found"),
                });
            }
        }
    }
}
