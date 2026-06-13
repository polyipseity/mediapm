//! In-memory CAS — ephemeral store using only memory backends.
//!
//! Composes [`InMemoryWal`](super::wal::InMemoryWal),
//! [`InMemoryObjectIndex`](super::object_index::InMemoryObjectIndex), and
//! [`InMemoryMetadataIndex`](super::metadata_index::InMemoryMetadataIndex) into
//! a fully functional [`CasStore`](super::store::CasStore) that implements
//! all CAS traits without any filesystem persistence.
//!
//! Useful for testing, benchmarks, and short-lived sessions where data does
//! not need to survive process death.

use super::metadata_index::InMemoryMetadataIndex;
use super::object_index::InMemoryObjectIndex;
use super::store::CasStore;
use super::wal::InMemoryWal;

/// Fully-assembled in-memory CAS store.
///
/// Wraps [`CasStore`] with the in-memory backend triplet for convenient
/// construction and naming.
#[derive(Clone)]
pub struct InMemoryCas(
    pub(crate) CasStore<InMemoryWal, InMemoryObjectIndex, InMemoryMetadataIndex>,
);

impl InMemoryCas {
    /// Create a new empty in-memory CAS store.
    pub fn new() -> Self {
        Self(CasStore::new(
            InMemoryWal::new(),
            InMemoryObjectIndex::new(),
            InMemoryMetadataIndex::new(),
        ))
    }
}

impl std::ops::Deref for InMemoryCas {
    type Target = CasStore<InMemoryWal, InMemoryObjectIndex, InMemoryMetadataIndex>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl_cas_wrapper_traits!(InMemoryCas);

/// Create a fully functional in-memory CAS store.
///
/// The returned store implements [`CasApi`](crate::api::CasApi),
/// [`ConstraintApi`](crate::api::ConstraintApi), and
/// [`CasMaintenanceApi`](crate::api::CasMaintenanceApi).
pub fn new_in_memory_cas() -> InMemoryCas {
    InMemoryCas::new()
}
