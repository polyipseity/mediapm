//! In-memory CAS — ephemeral store using only memory backends.
//!
//! Composes [`InMemoryWal`](super::wal::InMemoryWal),
//! [`InMemoryIndex`](super::index::InMemoryIndex), and
//! [`InMemoryBlobStore`](super::blob_store::InMemoryBlobStore) into
//! a fully functional [`CasStore`](super::store::CasStore) that implements
//! all CAS traits without any filesystem persistence.
//!
//! Useful for testing, benchmarks, and short-lived sessions where data does
//! not need to survive process death.

use super::blob_store::InMemoryBlobStore;
use super::index::InMemoryIndex;
use super::store::CasStore;
use super::wal::{InMemoryWal, WalPosition};

/// Fully-assembled in-memory CAS store.
///
/// Wraps [`CasStore`] with the in-memory backend triplet for convenient
/// construction and naming.
#[derive(Clone)]
pub struct InMemoryCas(pub(crate) CasStore<InMemoryWal, InMemoryIndex, InMemoryBlobStore>);

impl InMemoryCas {
    /// Create a new empty in-memory CAS store.
    pub fn new() -> Self {
        Self(CasStore::new(
            InMemoryWal::new(),
            InMemoryIndex::new(),
            InMemoryBlobStore::new(),
            WalPosition::ZERO,
        ))
    }
}

impl std::ops::Deref for InMemoryCas {
    type Target = CasStore<InMemoryWal, InMemoryIndex, InMemoryBlobStore>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Create a fully functional in-memory CAS store.
///
/// The returned store implements [`CasApi`](crate::api::CasApi),
/// [`ConstraintApi`](crate::api::ConstraintApi), and
/// [`CasMaintenanceApi`](crate::api::CasMaintenanceApi).
pub fn new_in_memory_cas() -> InMemoryCas {
    InMemoryCas::new()
}
