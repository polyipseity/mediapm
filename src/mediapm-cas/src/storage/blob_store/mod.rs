//! Blob store — eventual-consistent content-addressed blob storage.
//!
//! Defines the [`BlobStore`] trait and its implementations:
//! - [`FileSystemBlobStore`] — hash-derived directory layout on disk.
//! - [`InMemoryBlobStore`] — ephemeral in-memory map (testing/CI).

mod fs_blob_store;
pub(crate) mod mem_blob_store;
pub(crate) mod versions;

pub use fs_blob_store::FileSystemBlobStore;
pub use mem_blob_store::InMemoryBlobStore;

use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

// ---------------------------------------------------------------------------
// BlobStore trait
// ---------------------------------------------------------------------------

/// Content-addressed blob storage.
///
/// Provides read/write/delete for data indexed by content hash.
/// Full and delta-encoded blobs are stored at different paths
/// (`.diff` suffix for delta) so both can coexist safely during
/// optimizer transitions.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Write data for `hash` with the given encoding.
    ///
    /// - [`ObjectEncoding::Full`]: stored at `<hash-path>`.
    /// - [`ObjectEncoding::Delta`]: stored at `<hash-path>.diff`.
    ///
    /// Atomic via temp-file + rename (for file-system impls).
    async fn write(
        &self,
        hash: Hash,
        encoding: ObjectEncoding,
        data: Bytes,
    ) -> Result<(), CasError>;

    /// Read full blob for `hash`.
    ///
    /// Returns [`CasError::NotFound`] if no full blob exists.
    async fn read(&self, hash: &Hash) -> Result<Bytes, CasError>;

    /// Read delta-encoded blob for `hash`.
    ///
    /// Returns [`CasError::NotFound`] if no delta blob exists.
    async fn read_delta(&self, hash: &Hash) -> Result<Bytes, CasError>;

    /// Delete all blobs for `hash` (both full and delta).
    async fn delete(&self, hash: &Hash) -> Result<(), CasError>;

    /// Delete only a specific encoding of `hash`.
    ///
    /// Used for eventual cleanup during optimization (e.g., removing a
    /// stale `.diff` blob after promoting it to full).
    async fn delete_encoding(&self, hash: Hash, encoding: ObjectEncoding) -> Result<(), CasError> {
        // Default: ignore (in-memory and simple impls).
        let _ = (hash, encoding);
        Ok(())
    }

    /// Return the on-disk path for `hash`'s full blob, if this is a
    /// filesystem-backed store. In-memory stores return `None`.
    fn materialized_path(&self, hash: &Hash) -> Option<PathBuf> {
        let _ = hash;
        None
    }

    /// Whether `put()` should materialize BlobStore + Index synchronously
    /// (write-through), or defer to the WAL consumer (write-back).
    /// InMemory impls return `true`; FileSystem impls return `false`.
    const SYNC_MATERIALIZE: bool = true;
}
