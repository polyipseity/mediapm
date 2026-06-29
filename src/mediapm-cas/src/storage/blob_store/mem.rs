//! In-memory blob storage — ephemeral, `DashMap`-backed.
//!
//! All data lives in memory. Suitable for testing and ephemeral CAS usage.
//! Ignores the `.diff` path distinction — `read` and `read_delta` both
//! access the same underlying map.

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

use crate::api::{ObjectEncoding, VerifyTriggerStrategy};
use crate::error::CasError;
use crate::hash::Hash;

use super::BlobStore;

/// Ephemeral in-memory blob storage backed by `DashMap`.
///
/// Verify strategies are accepted but ignored — in-memory data cannot be
/// externally corrupted.
#[derive(Clone)]
pub struct InMemoryBlobStore {
    data: Arc<DashMap<Hash, (Bytes, ObjectEncoding)>>,
    aux: Arc<DashMap<(Hash, String), Bytes>>,
    #[allow(dead_code)]
    verify_strategies: Vec<VerifyTriggerStrategy>,
}

impl Default for InMemoryBlobStore {
    fn default() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            aux: Arc::new(DashMap::new()),
            verify_strategies: Vec::new(),
        }
    }
}

impl InMemoryBlobStore {
    /// Create an empty blob store with no verification.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an empty blob store with the given verify strategies.
    ///
    /// Strategies are accepted for API compatibility but ignored — in-memory
    /// data is always consistent.
    #[must_use]
    pub fn with_strategies(strategies: Vec<VerifyTriggerStrategy>) -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            aux: Arc::new(DashMap::new()),
            verify_strategies: strategies,
        }
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

    async fn write_aux(&self, hash: &Hash, name: &str, data: Bytes) -> Result<(), CasError> {
        self.aux.insert((*hash, name.to_owned()), data);
        Ok(())
    }

    async fn read_aux(&self, hash: &Hash, name: &str) -> Result<Bytes, CasError> {
        self.aux
            .get(&(*hash, name.to_owned()))
            .map(|r| r.value().clone())
            .ok_or(CasError::NotFound(*hash))
    }

    async fn delete_aux(&self, hash: &Hash, name: &str) -> Result<(), CasError> {
        self.aux.remove(&(*hash, name.to_owned()));
        Ok(())
    }

    async fn all_aux(&self, name: &str) -> Result<Vec<Bytes>, CasError> {
        Ok(self.aux.iter().filter(|r| r.key().1 == name).map(|r| r.value().clone()).collect())
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
}
