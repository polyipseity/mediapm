//! Blob storage — eventual-consistent content-addressed blob storage.
//!
//! Defines the [`BlobStore`] trait and its implementations:
//! - [`FileSystemBlobStore`] — hash-derived directory layout on disk.
//! - [`InMemoryBlobStore`] — ephemeral in-memory map (testing/CI).

mod fs;
pub(crate) mod mem;
pub(crate) mod versions;

pub use fs::FileSystemBlobStore;
pub use mem::InMemoryBlobStore;

use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};

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

    /// Whether `put()` should materialize [`BlobStore`] + metadata synchronously
    /// (write-through), or defer to the WAL consumer (write-back).
    /// `InMemory` impls return `true`; `FileSystem` impls return `false`.
    const SYNC_MATERIALIZE: bool = true;

    /// Read blob data and write it to an async writer.
    ///
    /// Default impl reads into `Bytes` then writes. Backends override for
    /// zero-copy streaming (e.g. `copy_buf` from a file).
    async fn read_to_writer(
        &self,
        hash: &Hash,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), CasError> {
        use tokio::io::AsyncWriteExt;
        let data = self.read(hash).await?;
        writer.write_all(&data).await.map_err(CasError::Io)?;
        Ok(())
    }

    /// Write blob data from an async reader, computing the content hash
    /// incrementally.
    ///
    /// Default impl buffers the full reader then calls `write()`. Backends
    /// override for streaming writes with hash verification.
    async fn write_from_reader(
        &self,
        hash: Hash,
        encoding: ObjectEncoding,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        content_len: u64,
    ) -> Result<(), CasError> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::with_capacity(usize::try_from(content_len).unwrap_or(usize::MAX));
        reader.read_to_end(&mut buf).await.map_err(CasError::Io)?;
        self.write(hash, encoding, Bytes::from(buf)).await
    }

    /// Write blob data from an async reader, returning the content hash and
    /// length. Useful when the hash is not known upfront (e.g. streaming PUT).
    ///
    /// Default impl buffers the full reader, computes the hash, then calls
    /// `write()`. Backends override for streaming writes with incremental
    /// hashing (avoids double-buffering).
    async fn write_stream(
        &self,
        encoding: ObjectEncoding,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(Hash, u64), CasError> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.map_err(CasError::Io)?;
        let len = buf.len() as u64;
        let hash = Hash::from_content(&buf);
        self.write(hash, encoding, Bytes::from(buf)).await?;
        Ok((hash, len))
    }
}
