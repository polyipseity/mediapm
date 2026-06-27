//! In-memory metadata — ephemeral, all data in `DashMap`s.
//!
//! Used by [`InMemoryCas`](super::super::in_memory::InMemoryCas) and as the
//! in-memory layer of [`FileSystemMetadataStore`](super::fs::FileSystemMetadataStore).

use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

use super::super::wal::{Wal, WalEntry, WalPosition};
use super::{MetadataEntry, MetadataStore};

/// An in-memory [`MetadataStore`] backed by `Arc<DashMap>` for both entry and
/// constraint data.
///
/// Clones share the same backing data, so all references observe the same
/// state — essential for concurrent access patterns.
///
/// Constraint data is stored in a separate [`DashMap`] from object metadata.
/// See [`MetadataStore::get_constraint`] — returns empty set (no `Option`).
///
/// Delta-base reverse index (`dependents`) is maintained on [`put`](MetadataStore::put)
/// and [`delete`](MetadataStore::delete) so [`list_dependents`](MetadataStore::list_dependents)
/// is O(1) instead of O(N).
#[derive(Clone, Default)]
pub struct InMemoryMetadataStore {
    /// Object metadata (payload size, encoding).
    data: Arc<DashMap<Hash, MetadataEntry>>,
    /// Constraint data (target → bases), independent of metadata.
    constraints: Arc<DashMap<Hash, BTreeSet<Hash>>>,
    /// Reverse index: base hash → dependents (hashes with Delta { `base_hash` }).
    dependents: Arc<DashMap<Hash, Vec<Hash>>>,
}

impl InMemoryMetadataStore {
    /// Create an empty metadata store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
    async fn put(&self, hash: Hash, entry: MetadataEntry) -> Result<(), CasError> {
        // Remove old dependent relation if hash previously had a delta base.
        if let Some(old_entry) = self.data.get(&hash)
            && let ObjectEncoding::Delta { base_hash } = old_entry.encoding
            && let Some(mut list) = self.dependents.get_mut(&base_hash)
        {
            list.retain(|h| *h != hash);
        }
        // Record new dependent relation if this is a delta-encoded entry.
        if let ObjectEncoding::Delta { base_hash } = entry.encoding {
            self.dependents.entry(base_hash).or_default().push(hash);
        }

        self.data.insert(hash, entry);
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<MetadataEntry>, CasError> {
        Ok(self.data.get(hash).as_deref().cloned())
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        // Remove this hash from its base's dependents list.
        if let Some(entry) = self.data.get(hash)
            && let ObjectEncoding::Delta { base_hash } = entry.encoding
            && let Some(mut list) = self.dependents.get_mut(&base_hash)
        {
            list.retain(|h| *h != *hash);
        }
        // Remove this hash as a base from the reverse index.
        self.dependents.remove(hash);

        self.data.remove(hash);
        // Also clean up any associated constraint.
        self.constraints.remove(hash);
        Ok(())
    }

    async fn list_dependents(&self, hash: &Hash) -> Result<Vec<Hash>, CasError> {
        Ok(self.dependents.get(hash).as_deref().cloned().unwrap_or_default())
    }

    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
        Ok(self.data.iter().map(|r| *r.key()).collect())
    }

    async fn len(&self) -> usize {
        self.data.len()
    }

    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        self.constraints.insert(target, bases);
        Ok(())
    }

    async fn get_constraint(&self, target: &Hash) -> Result<BTreeSet<Hash>, CasError> {
        Ok(self.constraints.get(target).as_deref().cloned().unwrap_or_default())
    }

    async fn list_targets(&self) -> Result<Vec<Hash>, CasError> {
        Ok(self.constraints.iter().map(|r| *r.key()).collect())
    }

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError> {
        self.constraints.retain(|target, bases| {
            // Remove entire entry if the target is gone.
            if !live.contains(target) {
                return false;
            }
            // Per-base pruning: keep only live bases.
            bases.retain(|b| live.contains(b));
            true
        });
        Ok(())
    }

    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError> {
        let entries = wal.replay_from(WalPosition::ZERO).await;
        for (_, entry) in entries {
            match entry {
                WalEntry::Put { hash, data } => {
                    self.data.insert(
                        hash,
                        MetadataEntry { len: data.len() as u64, encoding: ObjectEncoding::Full },
                    );
                }
                WalEntry::PutLarge { hash, content_len } => {
                    self.data.insert(
                        hash,
                        MetadataEntry { len: content_len, encoding: ObjectEncoding::Full },
                    );
                }
                WalEntry::Delete { hash } => {
                    self.data.remove(&hash);
                }
                WalEntry::Constraint { target, bases } => {
                    self.constraints.insert(target, bases);
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::InMemoryWal;
    use bytes::Bytes;

    // --- Object metadata tests ---

    #[tokio::test]
    async fn put_and_get_roundtrip() {
        let index = InMemoryMetadataStore::new();
        let hash = Hash::from_content(b"hello world");
        let entry = MetadataEntry { len: 11, encoding: ObjectEncoding::Full };
        index.put(hash, entry.clone()).await.unwrap();
        let retrieved = index.get(&hash).await.unwrap().unwrap();
        assert_eq!(retrieved.len, entry.len);
        assert_eq!(retrieved.encoding, entry.encoding);
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let index = InMemoryMetadataStore::new();
        let hash = Hash::from_content(b"missing");
        assert_eq!(index.get(&hash).await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_removes_entry() {
        let index = InMemoryMetadataStore::new();
        let hash = Hash::from_content(b"delete me");
        index.put(hash, MetadataEntry { len: 1, encoding: ObjectEncoding::Full }).await.unwrap();
        index.delete(&hash).await.unwrap();
        assert_eq!(index.get(&hash).await.unwrap(), None);
    }

    #[tokio::test]
    async fn len_and_is_empty() {
        let index = InMemoryMetadataStore::new();
        assert!(index.is_empty().await);
        let hash = Hash::from_content(b"item");
        index.put(hash, MetadataEntry { len: 1, encoding: ObjectEncoding::Full }).await.unwrap();
        assert_eq!(index.len().await, 1);
    }

    // --- Constraint tests ---

    #[tokio::test]
    async fn set_and_get_constraint() {
        let index = InMemoryMetadataStore::new();
        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");
        let bases = BTreeSet::from([base]);
        index.set_constraint(target, bases.clone()).await.unwrap();
        assert_eq!(index.get_constraint(&target).await.unwrap(), bases);
    }

    #[tokio::test]
    async fn set_get_constraint_roundtrip() {
        let index = InMemoryMetadataStore::new();
        let target = Hash::from_content(b"t");
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        index.set_constraint(target, BTreeSet::from([a, b])).await.unwrap();
        let got = index.get_constraint(&target).await.unwrap();
        assert!(got.contains(&a));
        assert!(got.contains(&b));
    }

    #[tokio::test]
    async fn replace_constraint_clears_previous() {
        let index = InMemoryMetadataStore::new();
        let target = Hash::from_content(b"t");
        index.set_constraint(target, BTreeSet::from([Hash::from_content(b"b")])).await.unwrap();
        // Replacing with empty set clears the constraint.
        index.set_constraint(target, BTreeSet::new()).await.unwrap();
        assert_eq!(index.get_constraint(&target).await.unwrap(), BTreeSet::new());
    }

    #[tokio::test]
    async fn get_constraint_missing_returns_empty() {
        let index = InMemoryMetadataStore::new();
        let target = Hash::from_content(b"missing");
        assert_eq!(index.get_constraint(&target).await.unwrap(), BTreeSet::new());
    }

    #[tokio::test]
    async fn delete_removes_constraint() {
        let index = InMemoryMetadataStore::new();
        let hash = Hash::from_content(b"h");
        let base = Hash::from_content(b"b");
        index.set_constraint(hash, BTreeSet::from([base])).await.unwrap();
        assert!(!index.get_constraint(&hash).await.unwrap().is_empty());
        index.delete(&hash).await.unwrap();
        assert!(index.get_constraint(&hash).await.unwrap().is_empty());
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

        let index = InMemoryMetadataStore::new();
        index.rebuild_from_wal(&journal).await.unwrap();
        assert!(!index.get_constraint(&target).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_targets_returns_constrained_only() {
        let index = InMemoryMetadataStore::new();
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");

        index.put(a, MetadataEntry { len: 1, encoding: ObjectEncoding::Full }).await.unwrap();
        index.set_constraint(b, BTreeSet::from([a])).await.unwrap();

        let targets = index.list_targets().await.unwrap();
        assert_eq!(targets, vec![b]);
    }

    #[tokio::test]
    async fn prune_targets_removes_dead_bases() {
        let index = InMemoryMetadataStore::new();
        let live = Hash::from_content(b"live");
        let dead = Hash::from_content(b"dead");
        let base = Hash::from_content(b"base");

        index.put(live, MetadataEntry { len: 1, encoding: ObjectEncoding::Full }).await.unwrap();
        index.put(dead, MetadataEntry { len: 1, encoding: ObjectEncoding::Full }).await.unwrap();
        index.put(base, MetadataEntry { len: 1, encoding: ObjectEncoding::Full }).await.unwrap();
        index.set_constraint(live, BTreeSet::from([base, dead])).await.unwrap();

        let live_set = HashSet::from([live, base]);
        index.prune_targets(&live_set).await.unwrap();

        // dead target's constraint was removed.
        assert!(index.get_constraint(&dead).await.unwrap().is_empty());

        // live still exists with only 'base' as constraint.
        // prune_targets removes constraint entries for dead targets
        // but does not remove blobs from the store.
        let bases = index.get_constraint(&live).await.unwrap();
        assert_eq!(bases, BTreeSet::from([base]));
    }

    #[tokio::test]
    async fn rebuild_from_wal_puts_constraint() {
        let journal = InMemoryWal::new();
        let hash = Hash::from_content(b"h");
        let base = Hash::from_content(b"b");

        // Put first, then constraint.
        journal.append(WalEntry::Put { hash, data: Bytes::from_static(b"hello") }).await.unwrap();
        journal
            .append(WalEntry::Constraint { target: hash, bases: BTreeSet::from([base]) })
            .await
            .unwrap();

        let index = InMemoryMetadataStore::new();
        index.rebuild_from_wal(&journal).await.unwrap();

        let entry = index.get(&hash).await.unwrap().unwrap();
        assert_eq!(entry.len, 5);
        assert_eq!(entry.encoding, ObjectEncoding::Full);
        let bases = index.get_constraint(&hash).await.unwrap();
        assert_eq!(bases, BTreeSet::from([base]));
    }
}
