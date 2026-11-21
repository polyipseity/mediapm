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
use super::{MetadataEntry, MetadataStore, PrunedConstraints};

/// An in-memory [`MetadataStore`] backed by `Arc<DashMap>` for both entry and
/// constraint data.
///
/// Clones share the same backing data, so all references observe the same
/// state — essential for concurrent access patterns.
///
/// Constraint data is stored in a separate [`DashMap`] from object metadata.
/// See [`MetadataStore::get_constraint`] — returns empty set (no `Option`).
///
/// # Bases-preservation invariant
///
/// [`put`](MetadataStore::put) stores object metadata only and does **not**
/// touch constraint bases. This is by design — `CasStore::put()` writes
/// metadata entries without knowing about constraint state, so constraint
/// bases must never be modified or wiped by `put()`. The BG engine WAL
/// consumer also preserves this separation by handling [`WalEntry::Constraint`]
/// independently from `WalEntry::Put`/`WalEntry::PutLarge`.
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

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<PrunedConstraints, CasError> {
        let mut targets = 0;
        let mut bases = 0;
        self.constraints.retain(|target, live_bases| {
            // Remove entire entry if the target is gone.
            if !live.contains(target) {
                targets += 1;
                return false;
            }
            // Keep the empty sentinel: it is a base the store answers for
            // without holding it, and a target that named only it must stay
            // full. A set that was non-empty and lost every live base becomes
            // `{empty}` instead of being dropped or emptied, because an empty
            // or absent set means "any base" and widening a constraint is not a
            // prune's job. `{empty}` is the safe non-widening representation.
            let before = live_bases.len();
            live_bases.retain(|b| *b == Hash::empty() || live.contains(b));
            bases += before - live_bases.len();
            if before > 0 && live_bases.is_empty() {
                live_bases.insert(Hash::empty());
            }
            true
        });
        Ok(PrunedConstraints { targets, bases })
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
                    // A replay is the same removal a live delete performs, so it
                    // drops the facts about the object as well as the object: a
                    // constraint that outlived its subject is what the live path
                    // already refuses to leave behind.
                    self.data.remove(&hash);
                    self.constraints.remove(&hash);
                }
                WalEntry::Constraint { target, bases } => {
                    self.constraints.insert(target, bases);
                }
            }
        }
        Ok(())
    }
}

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
    async fn prune_targets_counts_every_fact_it_drops() {
        let index = InMemoryMetadataStore::new();
        let live = Hash::from_content(b"live");
        let base = Hash::from_content(b"base");
        let dead_target = Hash::from_content(b"dead-target");
        let dead_base = Hash::from_content(b"dead-base");

        index.put(live, MetadataEntry { len: 1, encoding: ObjectEncoding::Full }).await.unwrap();
        index.put(base, MetadataEntry { len: 1, encoding: ObjectEncoding::Full }).await.unwrap();
        index.set_constraint(live, BTreeSet::from([base, dead_base])).await.unwrap();
        index.set_constraint(dead_target, BTreeSet::new()).await.unwrap();

        let live_set = HashSet::from([live, base]);
        let pruned = index.prune_targets(&live_set).await.unwrap();
        assert_eq!(pruned, PrunedConstraints { targets: 1, bases: 1 });

        // A second pass over the same live set has nothing left to drop, so a
        // count that drifted or double-counted shows here.
        let again = index.prune_targets(&live_set).await.unwrap();
        assert_eq!(again, PrunedConstraints::default());
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

    /// Deleting an object drops the constraint that describes it, so a reopened
    /// store cannot resurrect a base set for a hash it does not hold.
    #[tokio::test]
    async fn a_deleted_object_loses_its_constraint() {
        let index = InMemoryMetadataStore::new();
        let kept = Hash::from_content(b"kept");
        let plain = Hash::from_content(b"plain");

        index.put(kept, MetadataEntry { len: 4, encoding: ObjectEncoding::Full }).await.unwrap();
        index.put(plain, MetadataEntry { len: 5, encoding: ObjectEncoding::Full }).await.unwrap();
        index.set_constraint(kept, BTreeSet::from([Hash::empty()])).await.unwrap();

        assert_eq!(
            index.get_constraint(&kept).await.unwrap(),
            BTreeSet::from([Hash::empty()]),
            "the constraint is recorded"
        );
        assert!(
            index.get_constraint(&plain).await.unwrap().is_empty(),
            "an unconstrained object has no bases"
        );

        index.delete(&kept).await.unwrap();
        assert!(
            index.get_constraint(&kept).await.unwrap().is_empty(),
            "the constraint goes with the object"
        );
    }
}
