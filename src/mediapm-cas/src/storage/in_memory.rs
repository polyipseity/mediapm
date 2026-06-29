//! In-memory CAS — ephemeral store using only memory backends.
//!
//! Composes [`InMemoryWal`](super::wal::InMemoryWal),
//! [`InMemoryMetadataStore`](super::metadata_store::InMemoryMetadataStore), and
//! [`InMemoryBlobStore`](super::blob_store::InMemoryBlobStore) into
//! a fully functional [`CasStore`](super::store::CasStore) that implements
//! all CAS traits without any filesystem persistence.
//!
//! Useful for testing, benchmarks, and short-lived sessions where data does
//! not need to survive process death.

use super::blob_store::InMemoryBlobStore;
use super::metadata_store::{InMemoryMetadataStore, MetadataStore};
use super::store::CasStore;
use super::wal::{InMemoryWal, WalPosition};
use crate::defaults;
use crate::hash::Hash;

/// Fully-assembled in-memory CAS store.
///
/// Wraps [`CasStore`] with the in-memory backend triplet for convenient
/// construction and naming.
#[derive(Clone)]
pub struct InMemoryCas(pub(crate) CasStore<InMemoryWal, InMemoryMetadataStore, InMemoryBlobStore>);

impl InMemoryCas {
    /// Create a new empty in-memory CAS store.
    #[must_use]
    pub fn new() -> Self {
        Self(CasStore::new(
            InMemoryWal::new(),
            InMemoryMetadataStore::new(),
            InMemoryBlobStore::new(),
            WalPosition::ZERO,
            defaults::CACHE_TTL,
        ))
    }
}

impl Default for InMemoryCas {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for InMemoryCas {
    type Target = CasStore<InMemoryWal, InMemoryMetadataStore, InMemoryBlobStore>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Test helper: simulate metadata loss while preserving blobs.
#[doc(hidden)]
impl InMemoryCas {
    /// Remove the metadata entry for `hash`, leaving blob data and the
    /// (already-consumed) WAL intact. Call only after `flush()`.
    pub async fn simulate_metadata_loss_for_test(&self, hash: Hash) {
        self.metadata_store().delete(&hash).await.ok();
    }
}

/// Create a fully functional in-memory CAS store.
///
/// The returned store implements [`CasApi`](crate::api::CasApi),
/// [`ConstraintApi`](crate::api::ConstraintApi), and
/// [`CasMaintenanceApi`](crate::api::CasMaintenanceApi).
#[must_use]
pub fn new_in_memory_cas() -> InMemoryCas {
    InMemoryCas::new()
}
