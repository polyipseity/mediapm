//! In-memory CAS — ephemeral store using only memory backends.
//!
//! Composes [`InMemoryWal`](super::wal::InMemoryWal),
//! [`InMemoryMetadata`](super::metadata::InMemoryMetadata), and
//! [`InMemoryBlob`](super::blob::InMemoryBlob) into
//! a fully functional [`CasStore`](super::store::CasStore) that implements
//! all CAS traits without any filesystem persistence.
//!
//! Useful for testing, benchmarks, and short-lived sessions where data does
//! not need to survive process death.

use super::blob::InMemoryBlob;
use super::metadata::InMemoryMetadata;
use super::store::CasStore;
use super::wal::{InMemoryWal, WalPosition};
use crate::defaults;

/// Fully-assembled in-memory CAS store.
///
/// Wraps [`CasStore`] with the in-memory backend triplet for convenient
/// construction and naming.
#[derive(Clone)]
pub struct InMemoryCas(pub(crate) CasStore<InMemoryWal, InMemoryMetadata, InMemoryBlob>);

impl InMemoryCas {
    /// Create a new empty in-memory CAS store.
    pub fn new() -> Self {
        Self(CasStore::new(
            InMemoryWal::new(),
            InMemoryMetadata::new(),
            InMemoryBlob::new(),
            WalPosition::ZERO,
            defaults::CACHE_TTL,
        ))
    }
}

impl std::ops::Deref for InMemoryCas {
    type Target = CasStore<InMemoryWal, InMemoryMetadata, InMemoryBlob>;
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
