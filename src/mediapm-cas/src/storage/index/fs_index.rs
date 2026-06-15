//! Filesystem-backed index — in-memory with persistent snapshot file.
//!
//! [`FileSystemIndex`] wraps an [`InMemoryIndex`](super::mem_index::InMemoryIndex)
//! and persists both index entries (hash → (len, encoding)) and constraint
//! data (target → bases) to a single JSON file so the index survives WAL
//! trim and process restarts.

use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

use super::super::wal::Wal;
use super::mem_index::InMemoryIndex;
use super::versions::{self, FORMAT_VERSION};
use super::{Index, IndexEntry};

/// In-memory index with persistent snapshot (entries + constraints) on disk.
///
/// Delegates all operations to an [`InMemoryIndex`]. Both entries and
/// constraints are flushed to a JSON file after every mutation and loaded
/// during [`rebuild_from_wal`](Index::rebuild_from_wal).
///
/// Flushes are batched via a dirty flag: concurrent mutations coalesce into
/// a single write, avoiding redundant I/O. In the unlikely event of a stale
/// snapshot file (from a racing write), the WAL is the true source of truth
/// and recovers any lost data on process restart.
#[derive(Clone)]
pub struct FileSystemIndex {
    inner: InMemoryIndex,
    constraint_path: PathBuf,
    /// Tracks whether in-memory state has diverged from the on-disk snapshot.
    /// Set `true` by every mutation; cleared by `flush_snapshot`.
    dirty: Arc<AtomicBool>,
}

impl FileSystemIndex {
    /// Create a new `FileSystemIndex` backed by the given path.
    ///
    /// The snapshot file is not loaded until [`rebuild_from_wal`](Index::rebuild_from_wal)
    /// is called.
    pub fn new(constraint_path: PathBuf) -> Self {
        Self {
            inner: InMemoryIndex::new(),
            constraint_path,
            dirty: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Collect all entries from the inner index into a `BTreeMap`.
    async fn collect_entries(&self) -> Result<BTreeMap<Hash, (u64, ObjectEncoding)>, CasError> {
        let mut map = BTreeMap::new();
        for h in self.inner.list_hashes().await? {
            if let Some(entry) = self.inner.get(&h).await? {
                map.insert(h, (entry.len, entry.encoding));
            }
        }
        Ok(map)
    }

    /// Persist all entries + constraints to disk (JSON, V1 format).
    ///
    /// Skips the I/O when no mutations have occurred since the last flush
    /// (dirty-flag batching). The snapshot is always written atomically via
    /// temp+rename.
    async fn flush_snapshot(&self) -> Result<(), CasError> {
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }

        let constraints = {
            let mut map = BTreeMap::<Hash, BTreeSet<Hash>>::new();
            for t in self.inner.list_targets().await? {
                let bases = self.inner.get_constraint(&t).await?;
                map.insert(t, bases);
            }
            map
        };
        let entries = self.collect_entries().await?;

        if constraints.is_empty() && entries.is_empty() {
            if self.constraint_path.exists() {
                tokio::fs::remove_file(&self.constraint_path).await.map_err(CasError::Io)?;
            }
        } else {
            versions::save(&self.constraint_path, &constraints, &entries).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Index for FileSystemIndex {
    const SYNC_MATERIALIZE: bool = false;

    async fn put(&self, hash: Hash, entry: IndexEntry) -> Result<(), CasError> {
        self.inner.put(hash, entry).await?;
        self.dirty.store(true, Ordering::Release);
        self.flush_snapshot().await
    }

    async fn get(&self, hash: &Hash) -> Result<Option<IndexEntry>, CasError> {
        self.inner.get(hash).await
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        self.inner.delete(hash).await?;
        self.dirty.store(true, Ordering::Release);
        self.flush_snapshot().await
    }

    async fn list_dependents(&self, hash: &Hash) -> Result<Vec<Hash>, CasError> {
        self.inner.list_dependents(hash).await
    }

    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
        self.inner.list_hashes().await
    }

    async fn len(&self) -> usize {
        self.inner.len().await
    }

    async fn is_empty(&self) -> bool {
        self.inner.is_empty().await
    }

    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        self.inner.set_constraint(target, bases).await?;
        self.dirty.store(true, Ordering::Release);
        self.flush_snapshot().await
    }

    async fn get_constraint(&self, target: &Hash) -> Result<BTreeSet<Hash>, CasError> {
        self.inner.get_constraint(target).await
    }

    async fn list_targets(&self) -> Result<Vec<Hash>, CasError> {
        self.inner.list_targets().await
    }

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError> {
        self.inner.prune_targets(live).await?;
        self.dirty.store(true, Ordering::Release);
        self.flush_snapshot().await
    }

    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError> {
        self.inner.rebuild_from_wal(wal).await?;

        // Overlay persisted snapshot (entries + constraints survive WAL trim).
        if self.constraint_path.exists() {
            let (persisted_constraints, persisted_entries) =
                versions::load(&self.constraint_path, FORMAT_VERSION).await?;
            for (target, bases) in persisted_constraints {
                self.inner.set_constraint(target, bases).await?;
            }
            for (hash, (len, encoding)) in persisted_entries {
                self.inner.put(hash, IndexEntry { len, encoding }).await?;
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
    use crate::storage::wal::WalEntry;
    use bytes::Bytes;
    use tempfile::tempdir;

    #[tokio::test]
    async fn persists_and_restores_constraint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("constraints.v1.json");
        let index = FileSystemIndex::new(path.clone());

        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");
        index.set_constraint(target, BTreeSet::from([base])).await.unwrap();
        // Already flushed by set_constraint.

        // Create a new index from the same path and rebuild.
        let index2 = FileSystemIndex::new(path.clone());
        let journal = InMemoryWal::new();
        index2.rebuild_from_wal(&journal).await.unwrap();

        assert_eq!(
            index2.get_constraint(&target).await.unwrap(),
            BTreeSet::from([Hash::from_content(b"b")])
        );
    }

    #[tokio::test]
    async fn rebuild_from_wal_merges_persisted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("constraints.v1.json");

        // Create an index, add constraint via WAL + persisted.
        let index = FileSystemIndex::new(path.clone());
        index
            .set_constraint(Hash::from_content(b"p"), BTreeSet::from([Hash::from_content(b"b")]))
            .await
            .unwrap();
        // Already flushed by set_constraint.

        // Now rebuild with a fresh index and a WAL that has a Put + a different constraint.
        let journal = InMemoryWal::new();
        let target1 = Hash::from_content(b"w1");
        journal
            .append(WalEntry::Put { hash: target1, data: Bytes::from_static(b"x") })
            .await
            .unwrap();
        journal
            .append(WalEntry::Constraint {
                target: Hash::from_content(b"w2"),
                bases: BTreeSet::from([Hash::from_content(b"b2")]),
            })
            .await
            .unwrap();

        let index2 = FileSystemIndex::new(path.clone());
        index2.rebuild_from_wal(&journal).await.unwrap();

        // Assert both persisted and WAL constraints survive.
        assert!(!index2.get_constraint(&Hash::from_content(b"p")).await.unwrap().is_empty());
        assert!(!index2.get_constraint(&Hash::from_content(b"w2")).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn no_persist_file_starts_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        let index = FileSystemIndex::new(path);
        let journal = InMemoryWal::new();
        index.rebuild_from_wal(&journal).await.unwrap();
        assert!(index.is_empty().await);
    }

    #[tokio::test]
    async fn prune_also_flushes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("constraints.v1.json");
        let index = FileSystemIndex::new(path.clone());

        let live = Hash::from_content(b"live");
        index.set_constraint(live, BTreeSet::from([Hash::from_content(b"dead")])).await.unwrap();

        // Insert the base into metadata so it's in the index.
        // (prune_targets checks base membership via the HashSet argument, not
        // via the index, so this is fine.)
        index.prune_targets(&HashSet::from([live])).await.unwrap();

        // Reload and verify.
        let index2 = FileSystemIndex::new(path);
        let journal = InMemoryWal::new();
        index2.rebuild_from_wal(&journal).await.unwrap();
        // dead base is gone from the persisted constraint.
        assert_eq!(index2.get_constraint(&live).await.unwrap(), BTreeSet::new());
    }
}
