//! Filesystem-backed metadata — in-memory with per-directory persistent snapshots.
//!
//! [`FileSystemMetadataStore`] wraps an [`InMemoryMetadataStore`](super::mem::InMemoryMetadataStore)
//! and persists metadata entries and constraints in per-directory JSON snapshots
//! (one per fan-out directory `v1/blake3/ab/cd/`), using the
//! [`BlobStore`](super::super::blob_store::BlobStore) auxiliary-file API so each
//! mutation flushes only its hash's directory — O(N/D) I/O instead of O(N).
//!
//! Snapshot files use a versioned filename (`metadata-v1.json`) so future format
//! changes are detectable and migratable. See [`LATEST_METADATA_FORMAT`] and
//! [`METADATA_FORMAT_NAMES`].

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::error::CasError;
use crate::hash::Hash;

use super::super::blob_store::BlobStore;
use super::super::blob_store::FileSystemBlobStore;
use super::super::wal::Wal;
use super::mem::InMemoryMetadataStore;
use super::versions;
use super::{MetadataEntry, MetadataStore};

/// The per-directory metadata filename written on flush (latest format).
///
/// When bumping the format, update this name and add the old name to
/// [`METADATA_FORMAT_NAMES`] so old files are still readable on open.
/// Must be the first element of [`METADATA_FORMAT_NAMES`].
const LATEST_METADATA_FORMAT: &str = "metadata-v1.json";

/// All known per-directory metadata filenames, newest-first.
///
/// [`rebuild_from_wal`] scans all of them so both current and legacy
/// format files are loaded during startup. Add old format names here
/// when changing `LATEST_METADATA_FORMAT`.
const METADATA_FORMAT_NAMES: &[&str] = &[LATEST_METADATA_FORMAT];

/// In-memory metadata with per-directory persistent snapshots.
///
/// Delegates all operations to an [`InMemoryMetadataStore`]. After every
/// mutation, only the fan-out directory of the affected hash is flushed to
/// disk as a small JSON file (with a versioned filename like
/// `metadata-v1.json`) via the blob store's aux-file API.
///
/// On startup, [`rebuild_from_wal`](MetadataStore::rebuild_from_wal) replays
/// the WAL then overlays all per-directory snapshots to recover data that
/// survived WAL trim.
#[derive(Clone)]
pub struct FileSystemMetadataStore {
    inner: InMemoryMetadataStore,
    blob_store: FileSystemBlobStore,
}

impl FileSystemMetadataStore {
    /// Create a new `FileSystemMetadataStore` backed by the given blob store.
    ///
    /// The per-directory snapshots are not loaded until [`rebuild_from_wal`](MetadataStore::rebuild_from_wal)
    /// is called.
    pub fn new(blob_store: FileSystemBlobStore) -> Self {
        Self { inner: InMemoryMetadataStore::new(), blob_store }
    }

    /// Flush all entries and constraints whose hash falls into the same fan-out
    /// directory as `hash`. Writes a `metadata-v1.json` aux file for that
    /// directory in the latest format, or removes it if both entries and
    /// constraints are empty.
    ///
    /// After writing the latest format, any older format files in the same
    /// directory are cleaned up (future-proof for format bumps).
    async fn flush_dir(&self, hash: &Hash) -> Result<(), CasError> {
        let prefix = &hash.to_hex()[..4];

        // Collect constraints for targets sharing this prefix.
        let mut constraints = BTreeMap::new();
        for t in self.inner.list_targets().await? {
            if t.to_hex().starts_with(prefix) {
                let bases = self.inner.get_constraint(&t).await?;
                constraints.insert(t, bases);
            }
        }

        // Collect entries for hashes sharing this prefix.
        let mut entries = BTreeMap::new();
        for h in self.inner.list_hashes().await? {
            if h.to_hex().starts_with(prefix)
                && let Some(entry) = self.inner.get(&h).await?
            {
                entries.insert(h, (entry.len, entry.encoding));
            }
        }

        if constraints.is_empty() && entries.is_empty() {
            // Remove all known format files when the directory is empty.
            for name in METADATA_FORMAT_NAMES {
                self.blob_store.delete_aux(hash, name).await?;
            }
        } else {
            let json = versions::save_to_vec(&constraints, &entries)?;
            self.blob_store.write_aux(hash, LATEST_METADATA_FORMAT, Bytes::from(json)).await?;
            // Clean up any legacy format files in this directory.
            for name in METADATA_FORMAT_NAMES {
                if *name != LATEST_METADATA_FORMAT {
                    self.blob_store.delete_aux(hash, name).await?;
                }
            }
        }
        Ok(())
    }

    /// Iterate all targets from `inner`, collect their unique fan-out prefixes,
    /// and flush each directory.
    async fn flush_all_constraint_targets(&self) -> Result<(), CasError> {
        let prefixes: BTreeSet<String> =
            self.inner.list_targets().await?.iter().map(|h| h.to_hex()[..4].to_string()).collect();
        // Also include entry prefixes so that directories with only entries
        // (no constraints) are also flushed.
        let entry_prefixes: BTreeSet<String> =
            self.inner.list_hashes().await?.iter().map(|h| h.to_hex()[..4].to_string()).collect();
        for prefix in prefixes.union(&entry_prefixes) {
            // Construct a hash with the given prefix for path derivation.
            let dummy = Self::dummy_hash(prefix);
            self.flush_dir(&dummy).await?;
        }
        Ok(())
    }

    /// Build a hash whose hex representation starts with the given 4‑character
    /// prefix. Used only for path derivation during bulk flush operations.
    fn dummy_hash(prefix: &str) -> Hash {
        let mut bytes = [0u8; 32];
        if prefix.len() >= 2 {
            bytes[0] = u8::from_str_radix(&prefix[0..2], 16).unwrap_or(0);
        }
        if prefix.len() >= 4 {
            bytes[1] = u8::from_str_radix(&prefix[2..4], 16).unwrap_or(0);
        }
        Hash::from_bytes(bytes)
    }
}

#[async_trait]
impl MetadataStore for FileSystemMetadataStore {
    const SYNC_MATERIALIZE: bool = false;

    async fn put(&self, hash: Hash, entry: MetadataEntry) -> Result<(), CasError> {
        self.inner.put(hash, entry).await?;
        self.flush_dir(&hash).await
    }

    async fn get(&self, hash: &Hash) -> Result<Option<MetadataEntry>, CasError> {
        self.inner.get(hash).await
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        self.inner.delete(hash).await?;
        self.flush_dir(hash).await
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
        self.flush_dir(&target).await
    }

    async fn get_constraint(&self, target: &Hash) -> Result<BTreeSet<Hash>, CasError> {
        self.inner.get_constraint(target).await
    }

    async fn list_targets(&self) -> Result<Vec<Hash>, CasError> {
        self.inner.list_targets().await
    }

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<(), CasError> {
        self.inner.prune_targets(live).await?;
        // Collect all affected prefixes and flush them.
        // The simplest correct approach: flush every non-empty directory.
        // prune_targets is infrequent, so the cost is acceptable.
        self.flush_all_constraint_targets().await
    }

    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError> {
        self.inner.rebuild_from_wal(wal).await?;

        // Overlay per-directory snapshots from all known format names.
        // This transparently handles both current and legacy format files.
        for name in METADATA_FORMAT_NAMES {
            for bytes in self.blob_store.all_aux(name).await? {
                let (persisted_constraints, persisted_entries) = versions::load_from_bytes(&bytes)?;
                for (target, bases) in persisted_constraints {
                    self.inner.set_constraint(target, bases).await?;
                }
                for (hash, (len, encoding)) in persisted_entries {
                    self.inner.put(hash, MetadataEntry { len, encoding }).await?;
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
    use crate::storage::blob_store::FileSystemBlobStore;
    use crate::storage::wal::InMemoryWal;
    use crate::storage::wal::WalEntry;
    use bytes::Bytes;
    use tempfile::tempdir;

    /// Create a [`FileSystemBlobStore`] in a temp directory for testing
    /// [`FileSystemMetadataStore`].
    async fn test_blob_store() -> FileSystemBlobStore {
        let dir = tempdir().unwrap();
        FileSystemBlobStore::create(dir.path().join("blobs"), Vec::new()).await.unwrap()
    }

    #[tokio::test]
    async fn persists_and_restores_constraint() {
        let blob = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob.clone());

        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");
        index.set_constraint(target, BTreeSet::from([base])).await.unwrap();
        // Already flushed by set_constraint.

        // Create a new metadata store from the same blob store and rebuild.
        let index2 = FileSystemMetadataStore::new(blob.clone());
        let journal = InMemoryWal::new();
        index2.rebuild_from_wal(&journal).await.unwrap();

        assert_eq!(
            index2.get_constraint(&target).await.unwrap(),
            BTreeSet::from([Hash::from_content(b"b")])
        );
    }

    #[tokio::test]
    async fn rebuild_from_wal_merges_persisted() {
        let blob = test_blob_store().await;

        // Create a metadata store, add constraint via persisted flush.
        let index = FileSystemMetadataStore::new(blob.clone());
        index
            .set_constraint(Hash::from_content(b"p"), BTreeSet::from([Hash::from_content(b"b")]))
            .await
            .unwrap();
        // Already flushed by set_constraint.

        // Now rebuild with a fresh store and a WAL that has a Put + a different constraint.
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

        let index2 = FileSystemMetadataStore::new(blob.clone());
        index2.rebuild_from_wal(&journal).await.unwrap();

        // Assert both persisted and WAL constraints survive.
        assert!(!index2.get_constraint(&Hash::from_content(b"p")).await.unwrap().is_empty());
        assert!(!index2.get_constraint(&Hash::from_content(b"w2")).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn fresh_blob_store_starts_empty() {
        let blob = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob);
        let journal = InMemoryWal::new();
        index.rebuild_from_wal(&journal).await.unwrap();
        assert!(index.is_empty().await);
    }

    #[tokio::test]
    async fn prune_also_flushes() {
        let blob = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob.clone());

        let live = Hash::from_content(b"live");
        index.set_constraint(live, BTreeSet::from([Hash::from_content(b"dead")])).await.unwrap();

        index.prune_targets(&HashSet::from([live])).await.unwrap();

        // Reload and verify.
        let index2 = FileSystemMetadataStore::new(blob);
        let journal = InMemoryWal::new();
        index2.rebuild_from_wal(&journal).await.unwrap();
        assert_eq!(index2.get_constraint(&live).await.unwrap(), BTreeSet::new());
    }
}
