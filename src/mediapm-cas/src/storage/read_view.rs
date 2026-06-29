//! Read-through view — provides coherent reads over `MetadataStore` + `BlobStore` +
//! WAL fallback.
//!
//! The [`ComposedReadView`] implements a three-layer lookup used by
//! [`CasStore`](super::store::CasStore):
//!
//! 1. [`MetadataStore`](super::metadata_store::MetadataStore) — metadata (encoding, size, bases).
//! 2. [`BlobStore`](super::blob_store::BlobStore) — payload bytes (full
//!    or delta).
//! 3. WAL fallback — entries not yet materialized into BlobStore/MetadataStore.
//!
//! In-flight read dedup prevents redundant blob reads when multiple
//! concurrent `get()` calls miss Metadata simultaneously.

use std::collections::HashSet;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::api::ObjectEncoding;
use crate::defaults;
use crate::delta::object::StoredObject;
use crate::error::CasError;
use crate::hash::Hash;

use super::blob_store::BlobStore;
use super::metadata_store::{MetadataEntry, MetadataStore};
use super::pending_ops::PendingOps;
use super::wal::{PendingState, Wal};

/// Maximum number of delta chain hops before failing with
/// [`CasError::TooLarge`]. Prevents unbounded recursive resolution.
const MAX_DELTA_CHAIN_DEPTH: usize = 5;

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

    /// Write object contents into `writer` directly (streaming).
    /// Returns [`CasError::NotFound`] if the object does not exist.
    /// Returns [`CasError::TooLarge`] if the object exceeds the inline
    /// threshold — callers should fall back to a file-based streaming path.
    async fn get_to_writer(
        &self,
        hash: &Hash,
        writer: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
    ) -> Result<(), CasError> {
        let data = self.get(hash).await?;
        writer.write_all(&data).await?;
        Ok(())
    }

    /// Get metadata without loading payload bytes.
    async fn stat(&self, hash: &Hash) -> Result<ObjectMeta, CasError>;
}

/// Metadata about a stored object, re-exported for convenience.
#[doc(inline)]
pub use crate::api::ObjectMeta;

// ---------------------------------------------------------------------------
// ComposedReadView
// ---------------------------------------------------------------------------

/// A read-through view backed by Metadata + Blob + WAL fallback.
///
/// Implements a three-layer lookup:
/// 1. `MetadataStore` for metadata (encoding, size).
/// 2. `BlobStore` for payload bytes.
/// 3. `Wal` fallback for entries not yet materialized.
///
/// In-flight reads are deduplicated: if two tasks call `get` on the same
/// hash simultaneously, only one performs the lookup while the other waits
/// for the shared result (see [`PendingOps`]).
pub(crate) struct ComposedReadView<M: MetadataStore, J: Wal, B: BlobStore> {
    pending: PendingOps,
    metadata: M,
    wal: J,
    blob: B,
}

impl<M: MetadataStore, J: Wal, B: BlobStore> ComposedReadView<M, J, B> {
    /// Create a new view.
    pub fn new(metadata: M, wal: J, blob: B) -> Self {
        Self { pending: PendingOps::new(), metadata, wal, blob }
    }

    /// Try to recover a blob whose metadata was lost but whose file still
    /// exists in the blob store.
    ///
    /// Returns `Ok(Some(data))` when a full blob is found and its metadata
    /// entry is inserted. Returns `Ok(None)` when a delta blob is found
    /// (metadata inserted, caller should retry resolution) or when no blob
    /// exists at all.
    async fn try_orphan_recovery(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        // Try full blob first (most common case).
        match self.blob.read(hash).await {
            Ok(data) => {
                self.metadata
                    .put(
                        *hash,
                        MetadataEntry { len: data.len() as u64, encoding: ObjectEncoding::Full },
                    )
                    .await?;
                return Ok(Some(data));
            }
            Err(CasError::NotFound(_)) => {} // fall through to delta
            Err(e) => return Err(e),
        }

        // Try delta blob.
        match self.blob.read_delta(hash).await {
            Ok(delta_bytes) => {
                let stored = StoredObject::decode_delta(&delta_bytes)?;
                self.metadata
                    .put(
                        *hash,
                        MetadataEntry {
                            len: stored.state().content_len,
                            encoding: ObjectEncoding::Delta { base_hash: stored.state().base_hash },
                        },
                    )
                    .await?;
                // Precover ancestor chain so resolve_delta_chain succeeds.
                self.precover_delta_chain(*hash, stored.state().base_hash).await?;
                Ok(None)
            }
            Err(CasError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Walk the delta chain forward from `hash`→`base_hash`, recovering
    /// missing metadata for each ancestor. This ensures that
    /// `resolve_delta_chain` won't fail on missing base metadata during
    /// the subsequent call by the read path.
    async fn precover_delta_chain(&self, hash: Hash, mut base_hash: Hash) -> Result<(), CasError> {
        for _ in 0..MAX_DELTA_CHAIN_DEPTH {
            if self.metadata.get(&base_hash).await?.is_some() {
                return Ok(());
            }
            // Full blob?
            if let Ok(data) = self.blob.read(&base_hash).await {
                self.metadata
                    .put(
                        base_hash,
                        MetadataEntry { len: data.len() as u64, encoding: ObjectEncoding::Full },
                    )
                    .await?;
                return Ok(());
            }
            // Delta blob?
            if let Ok(delta_bytes) = self.blob.read_delta(&base_hash).await {
                let stored = StoredObject::decode_delta(&delta_bytes)?;
                self.metadata
                    .put(
                        base_hash,
                        MetadataEntry {
                            len: stored.state().content_len,
                            encoding: ObjectEncoding::Delta { base_hash: stored.state().base_hash },
                        },
                    )
                    .await?;
                base_hash = stored.state().base_hash;
                continue;
            }
            // Neither exists — resolution will produce its own error.
            return Ok(());
        }
        Err(CasError::TooLarge {
            hash,
            size: MAX_DELTA_CHAIN_DEPTH as u64,
            limit: MAX_DELTA_CHAIN_DEPTH as u64,
        })
    }

    /// Inner fetch: Metadata + Blob → WAL fallback with transparent delta
    /// reconstruction.
    ///
    /// Returns `Ok(Some(data))` if found, `Ok(None)` if confirmed absent.
    ///
    /// Delta-encoded entries are resolved by walking the delta chain
    /// iteratively (not recursively) to avoid Rust async recursion
    /// restrictions.
    async fn fetch_inner(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        // Check Metadata for metadata.
        let Some(entry) = self.metadata.get(hash).await? else {
            // WAL fallback: data may only exist as pending Put.
            return match self.wal.check_pending(hash).await {
                PendingState::Present(data) => Ok(Some(data)),
                PendingState::PresentExternal { .. } => {
                    // Large object: immediately materialized to blob +
                    // metadata during put(), so this path shouldn't
                    // normally be hit. Fall through to blob read.
                    Ok(None)
                }
                PendingState::Tombstone => Ok(None),
                PendingState::NotPresent => {
                    // Orphan recovery before giving up.
                    match self.try_orphan_recovery(hash).await? {
                        Some(data) => return Ok(Some(data)),
                        None => {
                            // Possibly delta metadata was inserted — retry resolution.
                            if let Some(entry) = self.metadata.get(hash).await? {
                                return resolve_full_bytes(
                                    hash,
                                    &entry,
                                    &self.metadata,
                                    &self.blob,
                                    "delta self-reference detected",
                                    "delta chain: base",
                                )
                                .await
                                .map(Some);
                            }
                            Ok(None)
                        }
                    }
                }
            };
        };

        // Before reading payload, check the WAL for a pending entry.
        // A pending `Present` means the data was committed to the WAL but
        // hasn't been materialized into Blob+Metadata yet (small blob
        // WAL-only path). Return it directly to avoid a blob-store miss.
        // A pending `Tombstone` means the data was deleted.
        match self.wal.check_pending(hash).await {
            PendingState::Tombstone => return Ok(None),
            PendingState::Present(data) => return Ok(Some(data)),
            PendingState::PresentExternal { .. } | PendingState::NotPresent => {}
        }

        resolve_full_bytes(
            hash,
            &entry,
            &self.metadata,
            &self.blob,
            "delta self-reference detected",
            "delta chain: base",
        )
        .await
        .map(Some)
    }
}

#[async_trait]
impl<M: MetadataStore + Send + Sync, J: Wal + Send + Sync, B: BlobStore + Send + Sync> ReadView
    for ComposedReadView<M, J, B>
{
    async fn get(&self, hash: &Hash) -> Result<Bytes, CasError> {
        // Check size: if object is large, require caller to use
        // get_to_writer (streaming) instead.
        if let Ok(Some(entry)) = self.metadata.get(hash).await
            && entry.len > defaults::WAL_INLINE_THRESHOLD
        {
            return Err(CasError::TooLarge {
                hash: *hash,
                size: entry.len,
                limit: defaults::WAL_INLINE_THRESHOLD,
            });
        } else if let PendingState::PresentExternal { content_len } =
            self.wal.check_pending(hash).await
            && content_len > defaults::WAL_INLINE_THRESHOLD
        {
            return Err(CasError::TooLarge {
                hash: *hash,
                size: content_len,
                limit: defaults::WAL_INLINE_THRESHOLD,
            });
        }

        self.pending
            .execute(*hash, || self.fetch_inner(hash))
            .await?
            .ok_or(CasError::NotFound(*hash))
    }

    async fn get_to_writer(
        &self,
        hash: &Hash,
        writer: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
    ) -> Result<(), CasError> {
        // Streaming path: skip PendingOps dedup (only useful for small
        // objects). Stream directly from Blob when the object is a full
        // encoding; fall back to buffered resolution for deltas.
        let Some(entry) = self.metadata.get(hash).await? else {
            return match self.wal.check_pending(hash).await {
                PendingState::Present(data) => {
                    use tokio::io::AsyncWriteExt;
                    writer.write_all(&data).await?;
                    Ok(())
                }
                PendingState::PresentExternal { .. } => {
                    // Materialized large object — stream from blob.
                    self.blob.read_to_writer(hash, writer).await
                }
                PendingState::Tombstone => Err(CasError::NotFound(*hash)),
                PendingState::NotPresent => {
                    // Orphan recovery before giving up.
                    match self.try_orphan_recovery(hash).await? {
                        Some(data) => {
                            writer.write_all(&data).await?;
                            return Ok(());
                        }
                        None => {
                            // Possibly delta metadata was inserted — retry resolution.
                            if let Some(entry) = self.metadata.get(hash).await? {
                                match entry.encoding {
                                    ObjectEncoding::Full => {
                                        return self.blob.read_to_writer(hash, writer).await;
                                    }
                                    ObjectEncoding::Delta { .. } => {
                                        let bytes = resolve_full_bytes(
                                            hash,
                                            &entry,
                                            &self.metadata,
                                            &self.blob,
                                            "delta self-reference detected",
                                            "delta chain: base",
                                        )
                                        .await?;
                                        writer.write_all(&bytes).await?;
                                        return Ok(());
                                    }
                                }
                            }
                            Err(CasError::NotFound(*hash))
                        }
                    }
                }
            };
        };

        // Before streaming, check WAL for a pending entry.
        // A pending `Present` means the data was committed to the WAL but
        // hasn't been materialized yet — write it directly to the stream.
        // A pending `Tombstone` means the data was deleted.
        match self.wal.check_pending(hash).await {
            PendingState::Tombstone => return Err(CasError::NotFound(*hash)),
            PendingState::Present(data) => {
                use tokio::io::AsyncWriteExt;
                writer.write_all(&data).await?;
                return Ok(());
            }
            PendingState::PresentExternal { .. } | PendingState::NotPresent => {}
        }

        match entry.encoding {
            ObjectEncoding::Full => self.blob.read_to_writer(hash, writer).await,
            ObjectEncoding::Delta { base_hash } => {
                // Delta resolution requires full bytes in memory.
                let bytes = resolve_delta_chain(
                    hash,
                    base_hash,
                    &self.metadata,
                    &self.blob,
                    "delta self-reference detected during get_to_writer",
                    "delta chain: base",
                )
                .await?;
                writer.write_all(&bytes).await?;
                Ok(())
            }
        }
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
            PendingState::PresentExternal { content_len } => {
                return Ok(ObjectMeta { len: content_len, encoding: ObjectEncoding::Full });
            }
            PendingState::Tombstone => {}
            PendingState::NotPresent => {
                // Orphan recovery before giving up.
                if let Some(data) = self.try_orphan_recovery(hash).await? {
                    return Ok(ObjectMeta {
                        len: data.len() as u64,
                        encoding: ObjectEncoding::Full,
                    });
                }
                // Delta metadata may have been inserted.
                if let Some(entry) = self.metadata.get(hash).await? {
                    return Ok(entry.as_meta());
                }
            }
        }

        Err(CasError::NotFound(*hash))
    }
}

// ---------------------------------------------------------------------------
// Shared delta-chain resolution
// ---------------------------------------------------------------------------

/// Given a metadata entry, read full bytes — either directly (Full) or by
/// resolving the delta chain (Delta).
pub(super) async fn resolve_full_bytes<M: MetadataStore, B: BlobStore>(
    hash: &Hash,
    entry: &MetadataEntry,
    metadata: &M,
    blob: &B,
    self_ref_msg: &str,
    base_not_found_msg: &str,
) -> Result<Bytes, CasError> {
    match entry.encoding {
        ObjectEncoding::Full => blob.read(hash).await,
        ObjectEncoding::Delta { base_hash } => {
            resolve_delta_chain(hash, base_hash, metadata, blob, self_ref_msg, base_not_found_msg)
                .await
        }
    }
}

/// Reconstruct full bytes for `hash` by walking its delta chain.
///
/// Callers provide the starting `base_hash` from the object's encoding and
/// context strings for error messages. Returns `Ok(full_bytes)` on success.
pub(super) async fn resolve_delta_chain<M: MetadataStore, B: BlobStore>(
    hash: &Hash,
    base_hash: Hash,
    metadata: &M,
    blob: &B,
    self_ref_msg: &str,
    base_not_found_msg: &str,
) -> Result<Bytes, CasError> {
    let mut chain: Vec<(Hash, Bytes)> = Vec::new();
    let mut current = *hash;
    let mut base = base_hash;
    let mut visited: HashSet<Hash> = HashSet::new();
    visited.insert(*hash);

    loop {
        if chain.len() >= MAX_DELTA_CHAIN_DEPTH {
            return Err(CasError::TooLarge {
                hash: *hash,
                size: chain.len() as u64,
                limit: MAX_DELTA_CHAIN_DEPTH as u64,
            });
        }
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
        let delta_data = blob.read_delta(&current).await?;
        chain.push((current, delta_data));
        current = base;

        match metadata.get(&current).await? {
            Some(base_entry) => match base_entry.encoding {
                ObjectEncoding::Full => {
                    let base_data = blob.read(&current).await?;
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
