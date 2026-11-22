//! Metadata trait and types for the CAS storage layer.
//!
//! Provides the [`MetadataStore`] trait (entry and constraint operations) and
//! the [`MetadataEntry`] type. Implementations:
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

/// What one [`MetadataStore::prune_targets`] walk removed.
///
/// The counts come from the walk that removes them, so a caller never has to
/// diff two listings and guess at the difference.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrunedConstraints {
    /// Constraint entries removed because their target is not in the live set.
    pub targets: usize,
    /// Dead hashes dropped from the base set of a surviving constraint.
    pub bases: usize,
}

/// Unified metadata store — payload metadata and constraint hints.
///
/// Object metadata (payload size, encoding) is stored via
/// [`MetadataStore::put`]/[`MetadataStore::get`]/[`MetadataStore::delete`]
/// and rebuilt from the WAL on startup. Constraint data is stored
/// independently via
/// [`MetadataStore::set_constraint`]/[`MetadataStore::get_constraint`] —
/// see §8.6 in AGENTS.md.
///
/// In-memory implementations are reconstructed from journal replay on startup.
/// The internal `FileSystemMetadataStore` additionally persists constraints to
/// disk so they survive WAL trim and process restart.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Store metadata for a hash (replaces existing entry).
    async fn put(&self, hash: Hash, entry: MetadataEntry) -> Result<(), CasError>;

    /// Retrieve metadata for a hash, if any.
    async fn get(&self, hash: &Hash) -> Result<Option<MetadataEntry>, CasError>;

    /// Delete metadata for a hash, with the constraint that describes those
    /// same bytes.
    ///
    /// The filesystem store flushes the hash's own fan-out directory here
    /// rather than only the directories it believes are occupied, so a
    /// directory left holding nothing loses its snapshot file. That is the gap
    /// [`MetadataStore::prune_targets`] has to close by hand: a prune drops
    /// facts about hashes the caller does not name, so it has to know which
    /// directories those facts lived in, while a delete names its own hash.
    ///
    /// Flushing is not what makes the removal survive a restart, and a caller
    /// must not read it that way. A store's own delete appends to the journal
    /// and returns; this method runs when the consumer drains that entry, so a
    /// store reopened in between has a snapshot that still names the deleted
    /// hash. What makes the removal durable is the reload order: snapshots are
    /// overlaid first and the journal replays over them, so the newer delete
    /// wins. `a_deleted_target_leaves_no_constraint_behind_for_the_next_run`
    /// pins it.
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

    /// Remove a constraint whose target `live` does not hold and drop every
    /// hash `live` does not hold from the bases of a constraint that survives.
    /// A live target whose bases all become dead is pinned to `{Hash::empty()}`
    /// (full) rather than widened to any base.
    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<PrunedConstraints, CasError>;

    /// Rewrite every legacy snapshot this store can see into the current
    /// format, then delete the legacy file. Returns how many were migrated.
    ///
    /// Defaulted to a no-op because only a store with an on-disk snapshot
    /// format has anything to migrate; `FileSystemMetadataStore` overrides it.
    async fn migrate_legacy_snapshots(&self) -> Result<usize, CasError> {
        Ok(0)
    }

    /// List hashes whose every physical form is known to be unsound.
    ///
    /// The default is empty because a store that does not persist a corruption
    /// record has nothing to report; `FileSystemMetadataStore` overrides this
    /// once Task 2 gives it a real set.
    async fn list_corrupted(&self) -> Result<Vec<Hash>, CasError> {
        Ok(Vec::new())
    }

    /// Restore persisted corruption records into this store.
    ///
    /// Semantics are union/extend: the provided hashes are added to any
    /// corruption record already held by this store. This makes per-overlay
    /// rebuild calls across snapshot directories safe, because each directory
    /// contributes its own hashes without replacing another directory's record.
    ///
    /// The default discards the record because a store with no corruption
    /// backing has nowhere to put it.
    async fn restore_corrupted(&self, _hashes: BTreeSet<Hash>) -> Result<(), CasError> {
        Ok(())
    }

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
