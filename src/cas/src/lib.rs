//! Phase 1 content-addressed storage (CAS) foundation.
//!
//! This crate defines the core identity and async API contracts for a
//! high-performance, incremental CAS layer. The implementation included here is
//! intentionally minimal and in-memory so later storage/index actors can be
//! added without breaking external callers.

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Fixed-size BLAKE3 hash newtype used for CAS object identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Builds a validated hash from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Computes a hash for arbitrary byte content.
    pub fn from_content(content: &[u8]) -> Self {
        Self(*blake3::hash(content).as_bytes())
    }

    /// Returns the hash bytes for storage fan-out or encoding.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Returns the canonical "empty content" hash.
pub fn empty_content_hash() -> Hash {
    Hash::from_content(&[])
}

/// Phase 1 optimization constraint for a target object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constraint {
    /// Content hash whose base-candidate set is being constrained.
    pub target_hash: Hash,
    /// Potential base hashes, represented as a set-like vector.
    pub potential_bases: Vec<Hash>,
}

/// Errors produced by CAS operations.
#[derive(Debug, Error)]
pub enum CasError {
    /// The requested hash does not exist in the store.
    #[error("object not found: {0:?}")]
    NotFound(Hash),
    /// Constraint requests must contain at least one possible base hash.
    #[error("constraint set must contain at least one potential base")]
    EmptyConstraintSet,
    /// An internal synchronization or state error occurred.
    #[error("internal CAS error: {0}")]
    Internal(String),
}

/// Async API contract for Phase 1 CAS behavior.
#[async_trait]
pub trait CasApi: Send + Sync {
    /// Stores content and returns its canonical content hash.
    async fn put(&self, data: Bytes) -> Result<Hash, CasError>;

    /// Retrieves previously stored content by hash.
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError>;

    /// Updates optimization constraints for a target hash.
    async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError>;
}

/// In-memory CAS implementation used for tests and early integration.
#[derive(Default)]
pub struct InMemoryCas {
    objects: RwLock<HashMap<Hash, Bytes>>,
    constraints: RwLock<HashMap<Hash, Vec<Hash>>>,
}

impl InMemoryCas {
    /// Creates an empty in-memory CAS.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CasApi for InMemoryCas {
    async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        let hash = Hash::from_content(&data);
        let mut objects =
            self.objects.write().map_err(|err| CasError::Internal(err.to_string()))?;
        objects.insert(hash, data);
        Ok(hash)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        let objects = self.objects.read().map_err(|err| CasError::Internal(err.to_string()))?;

        objects.get(&hash).cloned().ok_or(CasError::NotFound(hash))
    }

    async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError> {
        if constraint.potential_bases.is_empty() {
            return Err(CasError::EmptyConstraintSet);
        }

        let mut constraints =
            self.constraints.write().map_err(|err| CasError::Internal(err.to_string()))?;
        constraints.insert(constraint.target_hash, constraint.potential_bases);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{CasApi, Constraint, InMemoryCas, empty_content_hash};

    #[tokio::test]
    async fn roundtrip_put_and_get() {
        let cas = InMemoryCas::new();
        let original = Bytes::from_static(b"hello");

        let hash = cas.put(original.clone()).await.expect("must store");
        let loaded = cas.get(hash).await.expect("must retrieve");

        assert_eq!(loaded, original);
    }

    #[tokio::test]
    async fn constraint_requires_candidates() {
        let cas = InMemoryCas::new();
        let target_hash = cas.put(Bytes::from_static(b"target")).await.expect("must store target");

        let result = cas.set_constraint(Constraint { target_hash, potential_bases: vec![] }).await;

        assert!(result.is_err());
        assert_ne!(empty_content_hash(), target_hash);
    }
}
