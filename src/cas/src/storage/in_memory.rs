//! In-memory CAS — ephemeral store using only memory backends.
//!
//! Composes [`InMemoryJournal`](super::wal::InMemoryJournal),
//! [`InMemoryObjectStore`](super::payload_store::InMemoryObjectStore), and
//! [`InMemoryMetadataStore`](super::meta_store::InMemoryMetadataStore) into
//! a fully functional [`CasStore`](super::store::CasStore) that implements
//! all CAS traits without any filesystem persistence.
//!
//! Useful for testing, benchmarks, and short-lived sessions where data does
//! not need to survive process death.

use super::meta_store::InMemoryMetadataStore;
use super::payload_store::InMemoryObjectStore;
use super::store::CasStore;
use super::wal::InMemoryJournal;

/// Create a fully functional in-memory CAS store.
///
/// The returned store implements [`CasApi`](crate::api::CasApi),
/// [`ConstraintApi`](crate::api::ConstraintApi), and
/// [`CasMaintenanceApi`](crate::api::CasMaintenanceApi).
pub fn new_in_memory_cas() -> CasStore<InMemoryJournal, InMemoryObjectStore, InMemoryMetadataStore>
{
    CasStore::new(InMemoryJournal::new(), InMemoryObjectStore::new(), InMemoryMetadataStore::new())
}
