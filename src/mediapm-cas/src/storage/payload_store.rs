//! Object store — payload storage backend.
//!
//! Defines the [`ObjectStore`] trait and its in-memory implementation
//! [`InMemoryObjectStore`].

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

use crate::api::{ObjectEncoding, ObjectMeta};
use crate::error::CasError;
use crate::hash::Hash;

// ---------------------------------------------------------------------------
// StoredEntry
// ---------------------------------------------------------------------------

/// A stored object entry with payload and encoding metadata.
#[derive(Debug, Clone)]
pub(crate) struct StoredEntry {
    /// Raw stored bytes (full payload or V3 delta envelope).
    pub data: Bytes,
    /// How the payload is encoded.
    pub encoding: ObjectEncoding,
}

impl StoredEntry {
    /// Create a new stored entry.
    pub(crate) fn new(data: Bytes, encoding: ObjectEncoding) -> Self {
        Self { data, encoding }
    }
}

// ---------------------------------------------------------------------------
// ObjectStore trait
// ---------------------------------------------------------------------------

/// Persistent storage for object payload bytes.
///
/// Pluggable backend that the WAL consumer writes into and the ReadView
/// reads from.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Store bytes for a hash with the given encoding (replaces if exists).
    async fn put(&self, hash: Hash, data: Bytes, encoding: ObjectEncoding) -> Result<(), CasError>;

    /// Retrieve bytes and encoding for a hash. Returns `None` if not found.
    async fn get(&self, hash: &Hash) -> Result<Option<(Bytes, ObjectEncoding)>, CasError>;

    /// Get metadata about a stored object. Returns `None` if not found.
    async fn stat(&self, hash: &Hash) -> Result<Option<ObjectMeta>, CasError>;

    /// Delete an object.
    async fn delete(&self, hash: &Hash) -> Result<(), CasError>;

    /// List all hashes in the store (best-effort).
    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError>;

    /// Return the number of objects.
    fn len(&self) -> usize;

    /// Return `true` if the store is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// InMemoryObjectStore
// ---------------------------------------------------------------------------

/// An [`ObjectStore`] backed by a `DashMap<Hash, StoredEntry>`.
///
/// All data lives in memory. Suitable for testing and ephemeral usage.
#[derive(Clone)]
pub struct InMemoryObjectStore {
    data: Arc<DashMap<Hash, StoredEntry>>,
}

impl InMemoryObjectStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self { data: Arc::new(DashMap::new()) }
    }
}

impl Default for InMemoryObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    async fn put(&self, hash: Hash, data: Bytes, encoding: ObjectEncoding) -> Result<(), CasError> {
        self.data.insert(hash, StoredEntry::new(data, encoding));
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<(Bytes, ObjectEncoding)>, CasError> {
        Ok(self.data.get(hash).map(|v| {
            let entry = v.value();
            (entry.data.clone(), entry.encoding)
        }))
    }

    async fn stat(&self, hash: &Hash) -> Result<Option<ObjectMeta>, CasError> {
        Ok(self.data.get(hash).map(|v| {
            let entry = v.value();
            let payload_len = match entry.encoding {
                ObjectEncoding::Full => entry.data.len() as u64,
                ObjectEncoding::Delta { .. } => {
                    // Decode the V3 envelope to get the original content length.
                    crate::delta::object::StoredObject::decode_delta(&entry.data)
                        .ok()
                        .map(|obj| obj.content_len())
                        .unwrap_or(entry.data.len() as u64)
                }
            };
            ObjectMeta { len: payload_len, encoding: entry.encoding }
        }))
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        self.data.remove(hash);
        Ok(())
    }

    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
        Ok(self.data.iter().map(|r| *r.key()).collect())
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn put_and_get_roundtrip() {
        let store = InMemoryObjectStore::new();
        let data = Bytes::from_static(b"hello world");
        let hash = Hash::from_content(&data);
        store.put(hash, data.clone(), ObjectEncoding::Full).await.unwrap();
        let (retrieved, encoding) = store.get(&hash).await.unwrap().unwrap();
        assert_eq!(retrieved, data);
        assert_eq!(encoding, ObjectEncoding::Full);
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let store = InMemoryObjectStore::new();
        let hash = Hash::from_content(b"missing");
        assert_eq!(store.get(&hash).await.unwrap(), None);
    }

    #[tokio::test]
    async fn put_overwrite_same() {
        let store = InMemoryObjectStore::new();
        let hash = Hash::from_content(b"data");
        store.put(hash, Bytes::from_static(b"old"), ObjectEncoding::Full).await.unwrap();
        store.put(hash, Bytes::from_static(b"new"), ObjectEncoding::Full).await.unwrap();
        let (retrieved, _) = store.get(&hash).await.unwrap().unwrap();
        assert_eq!(retrieved, Bytes::from_static(b"new"));
    }

    #[tokio::test]
    async fn stat_returns_encoding() {
        let store = InMemoryObjectStore::new();
        let data = Bytes::from_static(b"hello world");
        let hash = Hash::from_content(&data);
        store.put(hash, data.clone(), ObjectEncoding::Full).await.unwrap();
        let meta = store.stat(&hash).await.unwrap().unwrap();
        assert_eq!(meta.len, data.len() as u64);
        assert_eq!(meta.encoding, ObjectEncoding::Full);
    }

    #[tokio::test]
    async fn all_hashes_after_puts() {
        let store = InMemoryObjectStore::new();
        let h1 = Hash::from_content(b"a");
        let h2 = Hash::from_content(b"b");
        store.put(h1, Bytes::from_static(b"a"), ObjectEncoding::Full).await.unwrap();
        store.put(h2, Bytes::from_static(b"b"), ObjectEncoding::Full).await.unwrap();
        let mut hashes = store.list_hashes().await.unwrap();
        hashes.sort();
        let mut expected = vec![h1, h2];
        expected.sort();
        assert_eq!(hashes, expected);
    }
}
