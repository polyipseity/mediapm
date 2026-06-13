//! Metadata index — constraint hints.
//!
//! Stores **only** constraint hints — the pairings (target hash → base
//! hashes) that the maintenance pass uses for delta optimization. This is
//! the **only** durable metadata not derivable from the ObjectIndex alone.
//!
//! The implementation is in-memory only. On startup,
//! [`MetadataIndex::rebuild_from_wal`] replays WAL entries to
//! populate the map — the WAL is the single persistent source of truth.

use async_trait::async_trait;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use dashmap::DashMap;

use crate::api::ConstraintPatch;
use crate::error::CasError;
use crate::hash::Hash;

use super::wal::{Wal, WalEntry, WalPosition};

/// Lightweight storage for constraint hints.
///
/// In-memory only — reconstructed from journal replay on startup.
#[async_trait]
pub trait MetadataIndex: Send + Sync {
    /// Record bases for target.
    async fn set(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError>;

    /// Get bases for target, if any.
    async fn get(&self, target: &Hash) -> Result<Option<BTreeSet<Hash>>, CasError>;

    /// Atomically add/remove/clear bases.
    async fn patch(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError>;

    /// Remove all entries for target.
    async fn remove(&self, target: &Hash) -> Result<(), CasError>;

    /// List all constraint targets.
    async fn list_targets(&self) -> Result<Vec<Hash>, CasError>;

    /// Remove constraints whose target or any base is not in `live`.
    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError>;

    /// Rebuild state by replaying the journal.
    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError>;
}

// ---------------------------------------------------------------------------
// InMemoryMetadataIndex
// ---------------------------------------------------------------------------

/// An in-memory [`MetadataIndex`] backed by `Arc<DashMap>`.
///
/// Clones share the same backing data, so all references observe the same
/// constraint state — essential for concurrent access patterns.
#[derive(Clone, Default)]
pub struct InMemoryMetadataIndex {
    data: Arc<DashMap<Hash, BTreeSet<Hash>>>,
}

impl InMemoryMetadataIndex {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MetadataIndex for InMemoryMetadataIndex {
    async fn set(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        self.data.insert(target, bases);
        Ok(())
    }

    async fn get(&self, target: &Hash) -> Result<Option<BTreeSet<Hash>>, CasError> {
        Ok(self.data.get(target).as_deref().cloned())
    }

    async fn patch(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError> {
        let mut entry = self.data.entry(target).or_default();
        if patch.clear {
            entry.clear();
        }
        for h in &patch.add_bases {
            entry.insert(*h);
        }
        for h in &patch.remove_bases {
            entry.remove(h);
        }
        Ok(())
    }

    async fn remove(&self, target: &Hash) -> Result<(), CasError> {
        self.data.remove(target);
        Ok(())
    }

    async fn list_targets(&self) -> Result<Vec<Hash>, CasError> {
        Ok(self.data.iter().map(|r| *r.key()).collect())
    }

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError> {
        self.data.retain(|target, bases| {
            if !live.contains(target) {
                // Entire entry removed: target object was deleted.
                return false;
            }
            // Per-base pruning: keep only live bases.
            // Constraints approach effective constraints (intersection with live hashes).
            bases.retain(|b| live.contains(b));
            // Keep entry even if no bases remain (empty = no effective base =
            // full or any-base delta allowed).
            true
        });
        Ok(())
    }

    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError> {
        let entries = wal.replay_from(WalPosition::ZERO).await;
        for (_, entry) in entries {
            if let WalEntry::Constraint { target, bases } = entry {
                self.data.insert(target, bases);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ConstraintPatch;
    use crate::storage::wal::{InMemoryWal, WalEntry};
    use bytes::Bytes;

    #[tokio::test]
    async fn set_and_get() {
        let store = InMemoryMetadataIndex::new();
        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");
        let bases = BTreeSet::from([base]);
        store.set(target, bases.clone()).await.unwrap();
        assert_eq!(store.get(&target).await.unwrap(), Some(bases));
    }

    #[tokio::test]
    async fn patch_add() {
        let store = InMemoryMetadataIndex::new();
        let target = Hash::from_content(b"t");
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        let patch = ConstraintPatch { add_bases: BTreeSet::from([a, b]), ..Default::default() };
        store.patch(target, patch).await.unwrap();
        let got = store.get(&target).await.unwrap().unwrap();
        assert!(got.contains(&a));
        assert!(got.contains(&b));
    }

    #[tokio::test]
    async fn remove_clears() {
        let store = InMemoryMetadataIndex::new();
        let target = Hash::from_content(b"t");
        store.set(target, BTreeSet::from([Hash::from_content(b"b")])).await.unwrap();
        store.remove(&target).await.unwrap();
        assert_eq!(store.get(&target).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rebuild_from_wal_populates() {
        let journal = InMemoryWal::new();
        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");
        journal
            .append(WalEntry::Put {
                hash: Hash::from_content(b"unrelated"),
                data: Bytes::from_static(b"x"),
            })
            .await
            .unwrap();
        journal
            .append(WalEntry::Constraint { target, bases: BTreeSet::from([base]) })
            .await
            .unwrap();

        let store = InMemoryMetadataIndex::new();
        store.rebuild_from_wal(&journal).await.unwrap();
        assert!(store.get(&target).await.unwrap().is_some());
    }
}
