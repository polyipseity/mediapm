//! Filesystem-backed index — in-memory with persistent constraint file.
//!
//! [`FileSystemIndex`] wraps an [`InMemoryIndex`](super::mem_index::InMemoryIndex)
//! and persists constraint data to a JSON file so it survives WAL trim and
//! process restarts. Object metadata still comes from WAL replay; only
//! constraint information (target → bases) is stored on disk.

use async_trait::async_trait;
use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::error::CasError;
use crate::hash::Hash;

use super::super::wal::Wal;
use super::mem_index::InMemoryIndex;
use super::versions::{self, FORMAT_VERSION};
use super::{Index, IndexEntry};

/// In-memory index with persistent constraint data on disk.
///
/// Delegates all metadata operations to an [`InMemoryIndex`]. Constraint sets
/// are flushed to a JSON file after every runtime [`set_constraint`](Index::set_constraint)
/// call and loaded during [`rebuild_from_wal`](Index::rebuild_from_wal).
#[derive(Clone)]
pub struct FileSystemIndex {
    inner: InMemoryIndex,
    constraint_path: PathBuf,
    /// Suppresses disk writes during `rebuild_from_wal`. Set to `true`
    /// during replay to avoid redundant flushing.
    suppress_persist: Arc<Mutex<bool>>,
    /// Dirty flag: `true` when constraints have been modified since last
    /// flush. Used for batched persistence.
    dirty: Arc<Mutex<bool>>,
}

impl FileSystemIndex {
    /// Create a new `FileSystemIndex` backed by the given path.
    ///
    /// The constraint file is not loaded until [`rebuild_from_wal`](Index::rebuild_from_wal)
    /// is called.
    pub fn new(constraint_path: PathBuf) -> Self {
        Self {
            inner: InMemoryIndex::new(),
            constraint_path,
            suppress_persist: Arc::new(Mutex::new(false)),
            dirty: Arc::new(Mutex::new(false)),
        }
    }

    /// Persist all constraints to disk (JSON, V1 format).
    async fn flush_constraints(&self) -> Result<(), CasError> {
        let targets = self.inner.list_targets().await?;
        let mut map = std::collections::BTreeMap::<Hash, BTreeSet<Hash>>::new();
        for t in targets {
            let bases = self.inner.get_constraint(&t).await?;
            map.insert(t, bases);
        }
        if map.is_empty() {
            if self.constraint_path.exists() {
                tokio::fs::remove_file(&self.constraint_path).await.map_err(CasError::Io)?;
            }
        } else {
            versions::save(&self.constraint_path, &map).await?;
        }
        *self.dirty.lock().unwrap() = false;
        Ok(())
    }

    /// Flush to disk only if constraints have been modified since the
    /// last flush. No-op when already clean.
    pub(crate) async fn flush_if_dirty(&self) -> Result<(), CasError> {
        if *self.dirty.lock().unwrap() {
            self.flush_constraints().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Index for FileSystemIndex {
    const SYNC_MATERIALIZE: bool = false;

    async fn put(&self, hash: Hash, entry: IndexEntry) -> Result<(), CasError> {
        self.inner.put(hash, entry).await
    }

    async fn get(&self, hash: &Hash) -> Result<Option<IndexEntry>, CasError> {
        self.inner.get(hash).await
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        self.inner.delete(hash).await
    }

    async fn list_dependents(&self, hash: &Hash) -> Result<Vec<Hash>, CasError> {
        self.inner.list_dependents(hash).await
    }

    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
        self.inner.list_hashes().await
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        self.inner.set_constraint(target, bases).await?;
        if !*self.suppress_persist.lock().unwrap() {
            *self.dirty.lock().unwrap() = true;
        }
        Ok(())
    }

    async fn get_constraint(&self, target: &Hash) -> Result<BTreeSet<Hash>, CasError> {
        self.inner.get_constraint(target).await
    }

    async fn list_targets(&self) -> Result<Vec<Hash>, CasError> {
        self.inner.list_targets().await
    }

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError> {
        self.inner.prune_targets(live).await?;
        *self.dirty.lock().unwrap() = true;
        self.flush_if_dirty().await
    }

    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError> {
        // 1. Replay WAL — populates metadata and any surviving constraint
        //    entries (pre-trim).
        {
            let mut guard = self.suppress_persist.lock().unwrap();
            *guard = true;
        }
        self.inner.rebuild_from_wal(wal).await?;

        // 2. Overlay persisted constraints (survive WAL trim).
        if self.constraint_path.exists() {
            let persisted = versions::load(&self.constraint_path, FORMAT_VERSION).await?;
            for (target, bases) in persisted {
                self.inner.set_constraint(target, bases).await?;
            }
        }

        {
            let mut guard = self.suppress_persist.lock().unwrap();
            *guard = false;
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
        // Flush so the new index can read the constraint from disk.
        index.flush_if_dirty().await.unwrap();

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
        // Flush so the persisted constraint is readable by a new index.
        index.flush_if_dirty().await.unwrap();

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
        assert!(index.is_empty());
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
