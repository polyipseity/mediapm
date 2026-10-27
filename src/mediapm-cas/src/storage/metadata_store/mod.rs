//! Metadata trait and types for the CAS storage layer.
//!
//! Provides the [`MetadataStore`] trait (entry, constraint and keep-full marker
//! operations) and the [`MetadataEntry`] type. Implementations:
//!
//! - `InMemoryMetadataStore` — ephemeral, all data in `DashMap`s
//! - `FileSystemMetadataStore` — in-memory with persisted snapshot file

mod fs;
mod mem;
pub(crate) mod versions;

pub(crate) use fs::FileSystemMetadataStore;
pub(crate) use mem::InMemoryMetadataStore;

use async_trait::async_trait;
use std::collections::{BTreeSet, HashSet};

use crate::api::{ObjectEncoding, ObjectMeta};
use crate::error::CasError;
use crate::hash::Hash;

use super::wal::Wal;

/// Metadata for a stored object (payload info only).
///
/// Constraint data is stored separately — see [`MetadataStore::get_constraint`].
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataEntry {
    /// Original payload length (before any encoding).
    pub len: u64,
    /// How the payload is encoded.
    pub encoding: crate::api::ObjectEncoding,
}

impl MetadataEntry {
    /// Convenience: returns `ObjectMeta` from this entry.
    #[must_use]
    pub fn as_meta(&self) -> ObjectMeta {
        ObjectMeta { len: self.len, encoding: self.encoding }
    }
}

/// Unified metadata store — payload metadata, constraint hints and keep-full
/// markers.
///
/// Object metadata (payload size, encoding) is stored via
/// [`MetadataStore::put`]/[`MetadataStore::get`]/[`MetadataStore::delete`]
/// and rebuilt from the WAL on startup. Constraint data is stored
/// independently via
/// [`MetadataStore::set_constraint`]/[`MetadataStore::get_constraint`] —
/// see §8.6 in AGENTS.md.
///
/// Keep-full markers are stored beside those two, through
/// [`MetadataStore::mark_keep_full`]: a marked object keeps its own full encoding
/// and may still serve as another object's delta base.
///
/// In-memory implementations are reconstructed from journal replay on startup.
/// The internal `FileSystemMetadataStore` additionally persists constraints and
/// keep-full markers to disk so they survive WAL trim and process restart.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Store metadata for a hash (replaces existing entry).
    async fn put(&self, hash: Hash, entry: MetadataEntry) -> Result<(), CasError>;

    /// Retrieve metadata for a hash, if any.
    async fn get(&self, hash: &Hash) -> Result<Option<MetadataEntry>, CasError>;

    /// Delete metadata for a hash.
    async fn delete(&self, hash: &Hash) -> Result<(), CasError>;

    /// List all hashes with entries.
    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError>;

    /// Number of entries.
    async fn len(&self) -> usize;

    /// Return `true` if the metadata store is empty.
    async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    // Constraint operations

    /// Record bases for `target` (replaces existing bases, independent of
    /// object metadata).
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError>;

    /// Get bases for `target`. Returns empty set when no constraint exists
    /// (no `Option`).
    async fn get_constraint(&self, target: &Hash) -> Result<BTreeSet<Hash>, CasError>;

    /// List all constraint targets.
    async fn list_targets(&self) -> Result<Vec<Hash>, CasError>;

    /// Remove constraints whose target or any base is not in `live`.
    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError>;

    // Keep-full markers

    /// Record that `hash` must keep its own full encoding.
    ///
    /// The marker is the CAS half of mediapm's `save = "full"`. It is durable
    /// (the filesystem store writes it to a side file a build that does not know
    /// the format ignores) and it is dropped with the object it describes, so a
    /// long-lived store does not accumulate facts about bytes it no longer
    /// holds.
    ///
    /// Marking a hash this store holds no entry for is accepted: the marker is a
    /// statement about how those bytes must be stored, and the store records it
    /// beside the object rather than refusing an order the caller had already
    /// ordered by content.
    async fn mark_keep_full(&self, hash: Hash) -> Result<(), CasError>;

    /// Report whether `hash` was marked keep-full.
    async fn is_keep_full(&self, hash: &Hash) -> Result<bool, CasError>;

    /// List every marked hash.
    ///
    /// The filesystem store uses this to write one directory's markers, and the
    /// maintenance sweep uses it to drop markers whose object is gone.
    async fn list_keep_full(&self) -> Result<Vec<Hash>, CasError>;

    /// List hashes that depend on `hash` as their delta base.
    ///
    /// Default O(N) implementation iterates all entries inline. In-memory
    /// implementations should override with an O(1) reverse-index lookup.
    async fn list_dependents(&self, hash: &Hash) -> Result<Vec<Hash>, CasError> {
        let mut dependents = Vec::new();
        for h in self.list_hashes().await? {
            if let Some(entry) = self.get(&h).await?
                && matches!(entry.encoding, ObjectEncoding::Delta { base_hash } if base_hash == *hash)
            {
                dependents.push(h);
            }
        }
        Ok(dependents)
    }

    /// Rebuild state by replaying the journal.
    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError>;

    /// Whether `put()` should materialize `MetadataStore` + `BlobStore` synchronously
    /// (write-through), or defer to the WAL consumer (write-back).
    /// `InMemory` impls return `true`.
    const SYNC_MATERIALIZE: bool = true;
}
