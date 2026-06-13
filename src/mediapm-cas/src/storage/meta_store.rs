//! Metadata store — constraint hints.
//!
//! Stores **only** constraint hints — the pairings (target hash → base
//! hashes) that the maintenance pass uses for delta optimization. This is
//! the **only** durable metadata not derivable from the ObjectStore alone.
//!
//! The implementation is in-memory only. On startup,
//! [`MetadataStore::rebuild_from_journal`] replays journal entries to
//! populate the map — the WAL is the single persistent source of truth.

use async_trait::async_trait;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::api::ConstraintPatch;
use crate::error::CasError;
use crate::hash::Hash;

use super::wal::{Journal, JournalEntry, JournalPosition};

/// Lightweight storage for constraint hints.
///
/// In-memory only — reconstructed from journal replay on startup.
#[async_trait]
pub trait MetadataStore: Send + Sync {
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
    async fn rebuild_from_journal(&self, journal: &dyn Journal) -> Result<(), CasError>;
}

// ---------------------------------------------------------------------------
// InMemoryMetadataStore
// ---------------------------------------------------------------------------

/// An in-memory [`MetadataStore`] backed by `Arc<RwLock<HashMap>>`.
///
/// Clones share the same backing data, so all references observe the same
/// constraint state — essential for concurrent access patterns.
#[derive(Clone, Default)]
pub struct InMemoryMetadataStore {
    data: Arc<RwLock<HashMap<Hash, BTreeSet<Hash>>>>,
}

impl InMemoryMetadataStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
    async fn set(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        self.data.write().unwrap().insert(target, bases);
        Ok(())
    }

    async fn get(&self, target: &Hash) -> Result<Option<BTreeSet<Hash>>, CasError> {
        Ok(self.data.read().unwrap().get(target).cloned())
    }

    async fn patch(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError> {
        let mut guard = self.data.write().unwrap();
        let entry = guard.entry(target).or_default();
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
        self.data.write().unwrap().remove(target);
        Ok(())
    }

    async fn list_targets(&self) -> Result<Vec<Hash>, CasError> {
        Ok(self.data.read().unwrap().keys().copied().collect())
    }

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError> {
        let mut guard = self.data.write().unwrap();
        guard.retain(|target, bases| {
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

    async fn rebuild_from_journal(&self, journal: &dyn Journal) -> Result<(), CasError> {
        let entries = journal.replay_from(JournalPosition::ZERO).await;
        for (_, entry) in entries {
            if let JournalEntry::Constraint { target, bases } = entry {
                self.data.write().unwrap().insert(target, bases);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ConstraintPatch;
    use crate::storage::wal::InMemoryJournal;
    use bytes::Bytes;

    #[tokio::test]
    async fn set_and_get() {
        let store = InMemoryMetadataStore::new();
        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");
        let bases = BTreeSet::from([base]);
        store.set(target, bases.clone()).await.unwrap();
        assert_eq!(store.get(&target).await.unwrap(), Some(bases));
    }

    #[tokio::test]
    async fn patch_add() {
        let store = InMemoryMetadataStore::new();
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
        let store = InMemoryMetadataStore::new();
        let target = Hash::from_content(b"t");
        store.set(target, BTreeSet::from([Hash::from_content(b"b")])).await.unwrap();
        store.remove(&target).await.unwrap();
        assert_eq!(store.get(&target).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rebuild_from_journal_populates() {
        let journal = InMemoryJournal::new();
        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");
        journal
            .append(JournalEntry::Put {
                hash: Hash::from_content(b"unrelated"),
                data: Bytes::from_static(b"x"),
            })
            .await
            .unwrap();
        journal
            .append(JournalEntry::Constraint { target, bases: BTreeSet::from([base]) })
            .await
            .unwrap();

        let store = InMemoryMetadataStore::new();
        store.rebuild_from_journal(&journal).await.unwrap();
        assert!(store.get(&target).await.unwrap().is_some());
    }
}
