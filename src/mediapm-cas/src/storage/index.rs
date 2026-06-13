//! Unified metadata index — combines object metadata and constraint hints.
//!
//! Merges the former [`ObjectIndex`] and [`MetadataIndex`] into a single
//! [`Index`] trait. Each hash maps to one [`IndexEntry`] containing payload
//! size, encoding, and optional constraint bases.
//!
//! The implementation is in-memory only. On startup,
//! [`Index::rebuild_from_wal`] replays WAL entries to populate the map — the
//! WAL is the single persistent source of truth.

use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use crate::api::{ObjectEncoding, ObjectMeta};
use crate::error::CasError;
use crate::hash::Hash;

use super::wal::{Wal, WalEntry, WalPosition};

// ---------------------------------------------------------------------------
// IndexEntry
// ---------------------------------------------------------------------------

/// Metadata for a stored object, combining payload info and optional
/// constraint bases.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    /// Original payload length (before any encoding).
    pub len: u64,
    /// How the payload is encoded.
    pub encoding: ObjectEncoding,
    /// Constraint bases, if any.
    /// `None` means no constraint has been recorded.
    pub bases: Option<BTreeSet<Hash>>,
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
/// In-memory only — reconstructed from journal replay on startup.
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

    /// Record bases for `target` (replaces existing bases, preserves
    /// size/encoding).
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError>;

    /// Get bases for `target`, if any.
    async fn get_constraint(&self, target: &Hash) -> Result<Option<BTreeSet<Hash>>, CasError>;

    /// List all constraint targets.
    async fn list_targets(&self) -> Result<Vec<Hash>, CasError>;

    /// Remove constraints whose target or any base is not in `live`.
    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError>;

    /// Rebuild state by replaying the journal.
    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError>;
}

// ---------------------------------------------------------------------------
// InMemoryIndex
// ---------------------------------------------------------------------------

/// An in-memory [`Index`] backed by `Arc<DashMap>`.
///
/// Clones share the same backing data, so all references observe the same
/// state — essential for concurrent access patterns.
#[derive(Clone, Default)]
pub struct InMemoryIndex {
    data: Arc<DashMap<Hash, IndexEntry>>,
}

impl InMemoryIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Index for InMemoryIndex {
    async fn put(&self, hash: Hash, entry: IndexEntry) -> Result<(), CasError> {
        self.data.insert(hash, entry);
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<IndexEntry>, CasError> {
        Ok(self.data.get(hash).as_deref().cloned())
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        self.data.remove(hash);
        Ok(())
    }

    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
        Ok(self.data.iter().map(|r| *r.key()).collect())
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        // Extract entry from the DashMap Ref guard first so we don't hold
        // the shard read lock while trying to acquire the write lock.
        let mut entry = self.data.get(&target).map(|r| r.value().clone()).unwrap_or(IndexEntry {
            len: 0,
            encoding: ObjectEncoding::Full,
            bases: None,
        });
        entry.bases = Some(bases);
        self.data.insert(target, entry);
        Ok(())
    }

    async fn get_constraint(&self, target: &Hash) -> Result<Option<BTreeSet<Hash>>, CasError> {
        // Clone the entry out of the Ref guard to avoid holding the shard
        // read lock across any subsequent operations.
        Ok(self.data.get(target).map(|r| r.value().clone()).and_then(|e| e.bases))
    }

    async fn list_targets(&self) -> Result<Vec<Hash>, CasError> {
        Ok(self.data.iter().filter(|r| r.value().bases.is_some()).map(|r| *r.key()).collect())
    }

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError> {
        self.data.retain(|target, entry| {
            // Remove entire entry if the target was deleted.
            if !live.contains(target) {
                return false;
            }
            // Per-base pruning: keep only live bases.
            if let Some(bases) = &mut entry.bases {
                bases.retain(|b| live.contains(b));
            }
            true
        });
        Ok(())
    }

    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError> {
        let entries = wal.replay_from(WalPosition::ZERO).await;
        for (_, entry) in entries {
            match entry {
                WalEntry::Put { hash, data } => {
                    let base_entry = self.data.get(&hash).as_deref().cloned();
                    let bases = base_entry.and_then(|e| e.bases);
                    self.data.insert(
                        hash,
                        IndexEntry {
                            len: data.len() as u64,
                            encoding: ObjectEncoding::Full,
                            bases,
                        },
                    );
                }
                WalEntry::Delete { hash } => {
                    self.data.remove(&hash);
                }
                WalEntry::Constraint { target, bases } => {
                    let mut entry =
                        self.data.get(&target).as_deref().cloned().unwrap_or(IndexEntry {
                            len: 0,
                            encoding: ObjectEncoding::Full,
                            bases: None,
                        });
                    entry.bases = Some(bases);
                    self.data.insert(target, entry);
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

    // --- Object metadata tests (from former ObjectIndex tests) ---

    #[tokio::test]
    async fn put_and_get_roundtrip() {
        let index = InMemoryIndex::new();
        let hash = Hash::from_content(b"hello world");
        let entry = IndexEntry { len: 11, encoding: ObjectEncoding::Full, bases: None };
        index.put(hash, entry.clone()).await.unwrap();
        let retrieved = index.get(&hash).await.unwrap().unwrap();
        assert_eq!(retrieved.len, entry.len);
        assert_eq!(retrieved.encoding, entry.encoding);
        assert_eq!(retrieved.bases, None);
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let index = InMemoryIndex::new();
        let hash = Hash::from_content(b"missing");
        assert_eq!(index.get(&hash).await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_removes_entry() {
        let index = InMemoryIndex::new();
        let hash = Hash::from_content(b"delete me");
        index
            .put(hash, IndexEntry { len: 1, encoding: ObjectEncoding::Full, bases: None })
            .await
            .unwrap();
        index.delete(&hash).await.unwrap();
        assert_eq!(index.get(&hash).await.unwrap(), None);
    }

    #[tokio::test]
    async fn len_and_is_empty() {
        let index = InMemoryIndex::new();
        assert!(index.is_empty());
        let hash = Hash::from_content(b"item");
        index
            .put(hash, IndexEntry { len: 1, encoding: ObjectEncoding::Full, bases: None })
            .await
            .unwrap();
        assert_eq!(index.len(), 1);
    }

    // --- Constraint tests (from former MetadataIndex tests) ---

    #[tokio::test]
    async fn set_and_get_constraint() {
        let index = InMemoryIndex::new();
        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");
        let bases = BTreeSet::from([base]);
        index.set_constraint(target, bases.clone()).await.unwrap();
        assert_eq!(index.get_constraint(&target).await.unwrap(), Some(bases));
    }

    #[tokio::test]
    async fn set_get_constraint_roundtrip() {
        let index = InMemoryIndex::new();
        let target = Hash::from_content(b"t");
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        index.set_constraint(target, BTreeSet::from([a, b])).await.unwrap();
        let got = index.get_constraint(&target).await.unwrap().unwrap();
        assert!(got.contains(&a));
        assert!(got.contains(&b));
    }

    #[tokio::test]
    async fn replace_constraint_clears_previous() {
        let index = InMemoryIndex::new();
        let target = Hash::from_content(b"t");
        index.set_constraint(target, BTreeSet::from([Hash::from_content(b"b")])).await.unwrap();
        // Replacing with empty set clears the constraint.
        index.set_constraint(target, BTreeSet::new()).await.unwrap();
        assert_eq!(index.get_constraint(&target).await.unwrap(), Some(BTreeSet::new()));
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

        let index = InMemoryIndex::new();
        index.rebuild_from_wal(&journal).await.unwrap();
        assert!(index.get_constraint(&target).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn put_overwrites_bases() {
        let index = InMemoryIndex::new();
        let hash = Hash::from_content(b"h");
        let base = Hash::from_content(b"b");

        // Set constraint first.
        index.set_constraint(hash, BTreeSet::from([base])).await.unwrap();
        // Then put an object with explicit bases: None.
        index
            .put(hash, IndexEntry { len: 5, encoding: ObjectEncoding::Full, bases: None })
            .await
            .unwrap();

        // Put overwrites constraint bases when bases is explicitly None.
        assert_eq!(index.get_constraint(&hash).await.unwrap(), None);
    }

    #[tokio::test]
    async fn list_targets_returns_constrained_only() {
        let index = InMemoryIndex::new();
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");

        index
            .put(a, IndexEntry { len: 1, encoding: ObjectEncoding::Full, bases: None })
            .await
            .unwrap();
        index.set_constraint(b, BTreeSet::from([a])).await.unwrap();

        let targets = index.list_targets().await.unwrap();
        assert_eq!(targets, vec![b]);
    }

    #[tokio::test]
    async fn prune_targets_removes_dead_entries() {
        let index = InMemoryIndex::new();
        let live = Hash::from_content(b"live");
        let dead = Hash::from_content(b"dead");
        let base = Hash::from_content(b"base");

        index
            .put(live, IndexEntry { len: 1, encoding: ObjectEncoding::Full, bases: None })
            .await
            .unwrap();
        index
            .put(dead, IndexEntry { len: 1, encoding: ObjectEncoding::Full, bases: None })
            .await
            .unwrap();
        index
            .put(base, IndexEntry { len: 1, encoding: ObjectEncoding::Full, bases: None })
            .await
            .unwrap();
        index.set_constraint(live, BTreeSet::from([base, dead])).await.unwrap();

        let live_set = HashSet::from([live, base]);
        index.prune_targets(&live_set).await.unwrap();

        // dead target was removed.
        assert!(index.get(&dead).await.unwrap().is_none());

        // live still exists with only 'base' as constraint.
        let bases = index.get_constraint(&live).await.unwrap().unwrap();
        assert_eq!(bases, BTreeSet::from([base]));
    }

    #[tokio::test]
    async fn rebuild_from_wal_puts_constraint_sets_bases() {
        let journal = InMemoryWal::new();
        let hash = Hash::from_content(b"h");
        let base = Hash::from_content(b"b");

        // Put first, then constraint.
        journal.append(WalEntry::Put { hash, data: Bytes::from_static(b"hello") }).await.unwrap();
        journal
            .append(WalEntry::Constraint { target: hash, bases: BTreeSet::from([base]) })
            .await
            .unwrap();

        let index = InMemoryIndex::new();
        index.rebuild_from_wal(&journal).await.unwrap();

        let entry = index.get(&hash).await.unwrap().unwrap();
        assert_eq!(entry.len, 5);
        assert_eq!(entry.encoding, ObjectEncoding::Full);
        assert_eq!(entry.bases, Some(BTreeSet::from([base])));
    }
}
