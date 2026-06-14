//! Blob store — eventual-consistent content-addressed blob storage.
//!
//! Defines the [`BlobStore`] trait and its implementations:
//! - [`FileSystemBlobStore`] — hash-derived directory layout on disk.
//! - [`InMemoryBlobStore`] — ephemeral in-memory map (testing/CI).

mod fs_blob_store;
pub(crate) mod versions;

pub use fs_blob_store::FileSystemBlobStore;
pub(crate) use versions::hash_to_path;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

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

    /// Check whether any blob exists for `hash`.
    async fn exists(&self, hash: &Hash) -> Result<bool, CasError>;

    /// Whether `put()` should materialize BlobStore + Index synchronously
    /// (write-through), or defer to the WAL consumer (write-back).
    /// InMemory impls return `true`; FileSystem impls return `false`.
    const SYNC_MATERIALIZE: bool = true;
}

// ---------------------------------------------------------------------------
// InMemoryBlobStore
// ---------------------------------------------------------------------------

/// Ephemeral in-memory blob store backed by `DashMap`.
///
/// All data lives in memory. Suitable for testing and ephemeral CAS usage.
/// Ignores the `.diff` path distinction — `read` and `read_delta` both
/// access the same underlying map.
#[derive(Clone, Default)]
pub struct InMemoryBlobStore {
    data: Arc<DashMap<Hash, (Bytes, ObjectEncoding)>>,
}

impl InMemoryBlobStore {
    /// Create an empty blob store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl BlobStore for InMemoryBlobStore {
    async fn write(
        &self,
        hash: Hash,
        _encoding: ObjectEncoding,
        data: Bytes,
    ) -> Result<(), CasError> {
        // In-memory store ignores .diff distinction — encoding is tracked in Index.
        self.data.insert(hash, (data, _encoding));
        Ok(())
    }

    async fn read(&self, hash: &Hash) -> Result<Bytes, CasError> {
        self.data.get(hash).map(|r| r.value().0.clone()).ok_or(CasError::NotFound(*hash))
    }

    async fn read_delta(&self, hash: &Hash) -> Result<Bytes, CasError> {
        // In-memory store: same as read (no separate .diff path).
        self.read(hash).await
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        self.data.remove(hash);
        Ok(())
    }

    async fn exists(&self, hash: &Hash) -> Result<bool, CasError> {
        Ok(self.data.contains_key(hash))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Hash;

    #[tokio::test]
    async fn in_memory_write_read_roundtrip() {
        let store = InMemoryBlobStore::new();
        let data = Bytes::from_static(b"hello blob store");
        let hash = Hash::from_content(&data);

        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();
        let retrieved = store.read(&hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn in_memory_read_missing_returns_not_found() {
        let store = InMemoryBlobStore::new();
        let hash = Hash::from_content(b"missing");
        let result = store.read(&hash).await;
        assert!(matches!(result, Err(CasError::NotFound(h)) if h == hash));
    }

    #[tokio::test]
    async fn in_memory_delete_removes_blob() {
        let store = InMemoryBlobStore::new();
        let data = Bytes::from_static(b"ephemeral");
        let hash = Hash::from_content(&data);

        store.write(hash, ObjectEncoding::Full, data).await.unwrap();
        assert!(store.read(&hash).await.is_ok());
        store.delete(&hash).await.unwrap();
        assert!(store.read(&hash).await.is_err());
    }

    #[tokio::test]
    async fn in_memory_exists_works() {
        let store = InMemoryBlobStore::new();
        let data = Bytes::from_static(b"exists check");
        let hash = Hash::from_content(&data);

        assert!(!store.exists(&hash).await.unwrap());
        store.write(hash, ObjectEncoding::Full, data).await.unwrap();
        assert!(store.exists(&hash).await.unwrap());
    }
}
