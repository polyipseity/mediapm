//! Index trait and types for the CAS storage layer.
//!
//! Provides the [`Index`] trait (metadata + constraint operations) and the
//! [`IndexEntry`] type. Implementations:
//!
//! - [`InMemoryIndex`](self::mem_index::InMemoryIndex) — ephemeral, all data in DashMaps
//! - [`FileSystemIndex`](self::fs_index::FileSystemIndex) — in-memory with persisted constraint file

mod fs_index;
mod mem_index;
pub(crate) mod versions;

pub(crate) use fs_index::FileSystemIndex;
pub(crate) use mem_index::InMemoryIndex;

use async_trait::async_trait;
use std::collections::{BTreeSet, HashSet};

use crate::api::{ObjectEncoding, ObjectMeta};
use crate::error::CasError;
use crate::hash::Hash;

use super::wal::Wal;

// ---------------------------------------------------------------------------
// IndexEntry
// ---------------------------------------------------------------------------

/// Metadata for a stored object (payload info only).
///
/// Constraint data is stored separately — see [`Index::get_constraint`].
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    /// Original payload length (before any encoding).
    pub len: u64,
    /// How the payload is encoded.
    pub encoding: crate::api::ObjectEncoding,
}

impl IndexEntry {
    /// Convenience: returns `ObjectMeta` from this entry.
    pub fn as_meta(&self) -> ObjectMeta {
        ObjectMeta { len: self.len, encoding: self.encoding }
    }
}

// ---------------------------------------------------------------------------
// Index trait
// ---------------------------------------------------------------------------

/// Unified metadata index — payload metadata + constraint hints.
///
/// Object metadata (payload size, encoding) is stored via [`put`]/[`get`]/[`delete`]
/// and rebuilt from the WAL on startup. Constraint data is stored independently
/// via [`set_constraint`]/[`get_constraint`] — see §8.6 in AGENTS.md.
///
/// In-memory implementations are reconstructed from journal replay on startup.
/// [`FileSystemIndex`] additionally persists constraints to disk so they survive
/// WAL trim and process restart.
#[async_trait]
pub trait Index: Send + Sync {
    /// Store metadata for a hash (replaces existing entry).
    async fn put(&self, hash: Hash, entry: IndexEntry) -> Result<(), CasError>;

    /// Retrieve metadata for a hash, if any.
    async fn get(&self, hash: &Hash) -> Result<Option<IndexEntry>, CasError>;

    /// Delete metadata for a hash.
    async fn delete(&self, hash: &Hash) -> Result<(), CasError>;

    /// List all hashes with entries.
    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError>;

    /// Number of entries.
    fn len(&self) -> usize;

    /// Return `true` if the index is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // -----------------------------------------------------------------------
    // Constraint operations
    // -----------------------------------------------------------------------

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

    /// List hashes that depend on `hash` as their delta base.
    ///
    /// Default O(N) implementation iterates all entries inline. In-memory
    /// implementations should override with an O(1) reverse-index lookup.
    async fn list_dependents(&self, hash: &Hash) -> Result<Vec<Hash>, CasError> {
        let mut dependents = Vec::new();
        for h in self.list_hashes().await? {
            if let Some(entry) = self.get(&h).await? {
                if matches!(entry.encoding, ObjectEncoding::Delta { base_hash } if base_hash == *hash)
                {
                    dependents.push(h);
                }
            }
        }
        Ok(dependents)
    }

    /// Rebuild state by replaying the journal.
    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError>;

    /// Whether `put()` should materialize Index + BlobStore synchronously
    /// (write-through), or defer to the WAL consumer (write-back).
    /// InMemory impls return `true`.
    const SYNC_MATERIALIZE: bool = true;
}
