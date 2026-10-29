//! Filesystem-backed metadata — in-memory with per-directory persistent snapshots.
//!
//! [`FileSystemMetadataStore`] wraps an [`InMemoryMetadataStore`] and persists
//! metadata entries and constraints in per-directory JSON snapshots (one per
//! fan-out directory `v1/blake3/ab/cd/`), using the [`BlobStore`] auxiliary-file
//! API so each mutation flushes only its hash's directory — O(N/D) I/O instead
//! of O(N).
//!
//! Snapshot files use a versioned filename (`metadata-v1.json`) so future format
//! changes are detectable and migratable. Keep-full markers get a second file in
//! the same directory (`keep-full-v1.json`) on their own ladder. This module
//! holds no version knowledge of its own: it consumes the names and codecs from
//! [`versions`], which owns both ladders.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::error::CasError;
use crate::hash::Hash;

use super::super::blob_store::BlobStore;
use super::super::blob_store::FileSystemBlobStore;
use super::super::wal::Wal;
use super::mem::InMemoryMetadataStore;
use super::versions::{
    self, KEEP_FULL_FORMAT_NAMES, LATEST_KEEP_FULL_FORMAT, LATEST_METADATA_FORMAT,
    METADATA_FORMAT_NAMES,
};
use super::{MetadataEntry, MetadataStore, PrunedConstraints};

/// In-memory metadata with per-directory persistent snapshots.
///
/// Delegates all operations to an [`InMemoryMetadataStore`]. After every
/// mutation, only the fan-out directory of the affected hash is flushed to
/// disk as a small JSON file (with a versioned filename like
/// `metadata-v1.json`) via the blob store's aux-file API.
///
/// Keep-full markers are persisted in a second per-directory file
/// (`keep-full-v1.json`, [`LATEST_KEEP_FULL_FORMAT`]) beside the snapshot. That
/// file has its own ladder: a build that does not know the marker format never
/// looks for it, so a store written by a newer build still opens here, and a
/// marker written here is invisible to a build that only reads snapshots.
///
/// On startup, [`rebuild_from_wal`](MetadataStore::rebuild_from_wal) replays
/// the WAL then overlays all per-directory snapshots and marker files to recover
/// data that survived WAL trim.
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

    /// Flush everything whose hash falls into the same fan-out directory as
    /// `hash`: entries, constraints and keep-full markers.
    ///
    /// Writes a `metadata-v1.json` and a `keep-full-v1.json` aux file for that
    /// directory in the latest format of each ladder, and removes either file
    /// when the facts it holds are empty. After writing, any older format files
    /// of the same ladder in the same directory are cleaned up (future-proof for
    /// format bumps).
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

        // Collect keep-full markers for hashes sharing this prefix.
        let mut keep_full = BTreeSet::new();
        for h in self.inner.list_keep_full().await? {
            if h.to_hex().starts_with(prefix) {
                keep_full.insert(h);
            }
        }

        if constraints.is_empty() && entries.is_empty() && keep_full.is_empty() {
            // Remove all known format files of both ladders when the directory
            // holds nothing.
            for name in METADATA_FORMAT_NAMES {
                self.blob_store.delete_aux(hash, name).await?;
            }
            for name in KEEP_FULL_FORMAT_NAMES {
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

            if keep_full.is_empty() {
                for name in KEEP_FULL_FORMAT_NAMES {
                    self.blob_store.delete_aux(hash, name).await?;
                }
            } else {
                let json = versions::save_keep_full_to_vec(&keep_full)?;
                self.blob_store.write_aux(hash, LATEST_KEEP_FULL_FORMAT, Bytes::from(json)).await?;
                for name in KEEP_FULL_FORMAT_NAMES {
                    if *name != LATEST_KEEP_FULL_FORMAT {
                        self.blob_store.delete_aux(hash, name).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Collect the fan-out prefixes of every constraint target and entry.
    ///
    /// A prefix names a fan-out directory, so this is the set of directories
    /// whose snapshot file has something to say. It says nothing about markers:
    /// a caller that has just dropped markers has to add the prefixes they
    /// occupied before it asks, because the markers are already gone from
    /// `inner` by then.
    async fn occupied_prefixes(&self) -> Result<BTreeSet<String>, CasError> {
        let targets: BTreeSet<String> =
            self.inner.list_targets().await?.iter().map(|h| h.to_hex()[..4].to_string()).collect();
        let entries: BTreeSet<String> =
            self.inner.list_hashes().await?.iter().map(|h| h.to_hex()[..4].to_string()).collect();
        Ok(targets.union(&entries).cloned().collect())
    }

    /// Flush each of the given fan-out prefixes.
    async fn flush_prefixes(&self, prefixes: &BTreeSet<String>) -> Result<(), CasError> {
        for prefix in prefixes {
            let representative = Self::prefix_hash(prefix)?;
            self.flush_dir(&representative).await?;
        }
        Ok(())
    }

    /// Builds a hash whose hex representation starts with `prefix`.
    ///
    /// The value is used only to derive the fan-out directory of `prefix`, so
    /// it never reaches the store. Every prefix this module produces is the
    /// first four hex digits of a stored hash, and anything else is a defect in
    /// the caller: reading a malformed prefix as zeros would flush a directory
    /// no fact belongs to and leave the intended one on disk.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::Internal`] when `prefix` is shorter than four
    /// characters or is not hexadecimal.
    fn prefix_hash(prefix: &str) -> Result<Hash, CasError> {
        let digits = prefix.as_bytes();
        let chunks = digits.get(0..2).zip(digits.get(2..4)).ok_or_else(|| {
            CasError::internal(format!("fan-out prefix {prefix:?} is shorter than four hex digits"))
        })?;
        let mut raw = [0u8; 32];
        let (first, second) = chunks;
        for (slot, chunk) in raw.iter_mut().zip([first, second]) {
            let text = std::str::from_utf8(chunk).map_err(|err| {
                CasError::internal(format!("fan-out prefix {prefix:?} is not ASCII: {err}"))
            })?;
            *slot = u8::from_str_radix(text, 16).map_err(|err| {
                CasError::internal(format!("fan-out prefix {prefix:?} is not hexadecimal: {err}"))
            })?;
        }
        Ok(Hash::from_bytes(raw))
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

    async fn mark_keep_full(&self, hash: Hash) -> Result<(), CasError> {
        self.inner.mark_keep_full(hash).await?;
        self.flush_dir(&hash).await
    }

    async fn is_keep_full(&self, hash: &Hash) -> Result<bool, CasError> {
        self.inner.is_keep_full(hash).await
    }

    async fn list_keep_full(&self) -> Result<Vec<Hash>, CasError> {
        self.inner.list_keep_full().await
    }

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<PrunedConstraints, CasError> {
        // The marker prefixes are read before the prune, because the prune is
        // what removes them from `inner`: a directory whose only fact was a
        // marker for an object that no longer exists would otherwise keep the
        // file on disk and resurrect the marker on the next open.
        let marked_before = self.inner.list_keep_full().await?;
        let pruned = self.inner.prune_targets(live).await?;

        let mut prefixes = self.occupied_prefixes().await?;
        prefixes.extend(marked_before.iter().map(|h| h.to_hex()[..4].to_string()));
        // prune_targets is infrequent, so flushing every affected directory is
        // acceptable.
        self.flush_prefixes(&prefixes).await?;
        Ok(pruned)
    }

    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError> {
        self.inner.rebuild_from_wal(wal).await?;

        // Overlay per-directory snapshots from all known format names.
        // This transparently handles both current and legacy format files.
        for name in METADATA_FORMAT_NAMES {
            for bytes in self.blob_store.all_aux(name).await? {
                let (persisted_constraints, persisted_entries) =
                    versions::load_named_from_bytes(name, &bytes)?;
                for (target, bases) in persisted_constraints {
                    self.inner.set_constraint(target, bases).await?;
                }
                for (hash, (len, encoding)) in persisted_entries {
                    self.inner.put(hash, MetadataEntry { len, encoding }).await?;
                }
            }
        }

        // Overlay the keep-full markers. Only the names this build knows are
        // scanned, so a marker file written by a newer build is ignored rather
        // than parsed under this build's rules.
        for name in KEEP_FULL_FORMAT_NAMES {
            for bytes in self.blob_store.all_aux(name).await? {
                for hash in versions::load_keep_full_from_bytes(name, &bytes)? {
                    self.inner.mark_keep_full(hash).await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ObjectEncoding;
    use crate::storage::blob_store::FileSystemBlobStore;
    use crate::storage::wal::InMemoryWal;
    use crate::storage::wal::WalEntry;
    use bytes::Bytes;

    /// Create a [`FileSystemBlobStore`] in a temp directory for testing
    /// [`FileSystemMetadataStore`].
    ///
    /// Returns the `TempDir` alongside the store: the store lazily
    /// `create_dir_all`s its tree on first write, so dropping the guard
    /// before the store does would recreate the tree without RAII
    /// ownership and leak `$TMPDIR/mediapm-artifact-*` dirs.
    async fn test_blob_store() -> (FileSystemBlobStore, tempfile::TempDir) {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let blob = FileSystemBlobStore::create(dir.path().join("blobs"), Vec::new()).await.unwrap();
        (blob, dir)
    }

    #[tokio::test]
    async fn persists_and_restores_constraint() {
        let (blob, _dir) = test_blob_store().await;
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
        let (blob, _dir) = test_blob_store().await;

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
        let (blob, _dir) = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob);
        let journal = InMemoryWal::new();
        index.rebuild_from_wal(&journal).await.unwrap();
        assert!(index.is_empty().await);
    }

    /// Concurrent writes of two entries in one fan-out directory both land.
    ///
    /// A fan-out directory holds one snapshot file, so two entries that share its
    /// prefix write the same target. Without a commit lock the second writer
    /// stages under the name the first is about to rename, and one of them loses
    /// the file: the store reports `ENOENT` for a hash it just accepted. The
    /// materializer and the workflow import hit this on the ordinary path, because
    /// the object they share a directory with is flushed at the same time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_entries_in_one_directory_both_persist() {
        let (blob, _dir) = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob.clone());

        // Two hashes whose fan-out directory is the same one, found rather than
        // assumed so the test keeps colliding if the layout changes.
        let mut first = None;
        let mut pair = None;
        for counter in 0u32.. {
            let hash = Hash::from_content(format!("entry-{counter}").as_bytes());
            match first {
                None => first = Some(hash),
                Some(seen) => {
                    if hash.to_hex()[..4] == seen.to_hex()[..4] {
                        pair = Some((seen, hash));
                        break;
                    }
                }
            }
        }
        let (left, right) = pair.expect("two hashes share a fan-out directory");

        let entry = || MetadataEntry { len: 1, encoding: ObjectEncoding::Full };
        let (left_entry, right_entry) = (entry(), entry());
        let index = std::sync::Arc::new(index);
        let (left_index, right_index) =
            (std::sync::Arc::clone(&index), std::sync::Arc::clone(&index));
        let (left_task, right_task) = tokio::join!(
            tokio::spawn(async move { left_index.put(left, left_entry).await }),
            tokio::spawn(async move { right_index.put(right, right_entry).await }),
        );
        left_task.expect("the writer task runs").expect("the first entry commits");
        right_task.expect("the writer task runs").expect("the second entry commits");

        let reloaded = FileSystemMetadataStore::new(blob);
        reloaded.rebuild_from_wal(&InMemoryWal::new()).await.expect("reload the snapshots");
        assert!(reloaded.get(&left).await.expect("read the first entry").is_some());
        assert!(reloaded.get(&right).await.expect("read the second entry").is_some());
    }

    /// A fan-out prefix is derived from stored hashes, so a malformed one is a
    /// defect in the caller. Reading it as zeros would flush a directory no
    /// fact belongs to.
    #[tokio::test]
    async fn a_malformed_fan_out_prefix_is_refused() {
        let representative =
            FileSystemMetadataStore::prefix_hash("ab12").expect("a hex prefix derives a hash");
        assert_eq!(&representative.to_hex()[..4], "ab12");

        for malformed in ["ab", "zzzz", "a\u{e9}12"] {
            assert!(
                FileSystemMetadataStore::prefix_hash(malformed).is_err(),
                "prefix {malformed:?} must be refused rather than read as zeros",
            );
        }
    }

    #[tokio::test]
    async fn prune_also_flushes() {
        let (blob, _dir) = test_blob_store().await;
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

    /// A marker for an object the store no longer holds leaves no file behind.
    ///
    /// The marker is written into the fan-out directory of the hash it marks,
    /// and the prune rewrites the directories it visited. A marked hash has no
    /// metadata entry, so its directory is not one of the occupied ones: the
    /// flush list has to carry it from the markers read before the prune, or
    /// the file stays and the marker returns on the next open.
    #[tokio::test]
    async fn prune_removes_the_marker_file_of_an_object_that_is_gone() {
        let (blob, _dir) = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob.clone());

        let live = Hash::from_content(b"live entry");
        index.put(live, MetadataEntry { len: 10, encoding: ObjectEncoding::Full }).await.unwrap();

        // A ghost in a fan-out directory of its own, so the marker file is the
        // only reason to rewrite that directory.
        // Bounded: every candidate but one lands outside the live entry's
        // directory, so a thousand tries is an impossible miss rather than a
        // search that insists on terminating.
        let ghost = (0u32..1024)
            .map(|counter| Hash::from_content(format!("ghost-{counter}").as_bytes()))
            .find(|hash| hash.to_hex()[..4] != live.to_hex()[..4])
            .expect("a hash outside the live entry's fan-out directory");
        index.mark_keep_full(ghost).await.unwrap();
        blob.read_aux(&ghost, LATEST_KEEP_FULL_FORMAT)
            .await
            .expect("the marker file is on disk before the prune");

        let pruned = index.prune_targets(&HashSet::from([live])).await.unwrap();
        assert_eq!(pruned.markers, 1, "the marker on a hash the store does not hold is dropped");
        assert!(
            blob.read_aux(&ghost, LATEST_KEEP_FULL_FORMAT).await.is_err(),
            "the marker file must be gone from disk, not only from the in-memory map",
        );

        let reopened = FileSystemMetadataStore::new(blob);
        reopened.rebuild_from_wal(&InMemoryWal::new()).await.unwrap();
        assert!(
            !reopened.is_keep_full(&ghost).await.unwrap(),
            "a marker left on disk comes back on the next open",
        );
    }

    /// A keep-full marker is written to its own file and read back after a
    /// reopen.
    ///
    /// The marker lives in a side file rather than in the metadata snapshot so
    /// that a build which does not know the marker still reads the snapshot.
    /// Persistence is what makes it useful: a marker that dies with the process
    /// would let the next maintenance pass re-encode the object.
    #[tokio::test]
    async fn a_keep_full_marker_survives_a_reopen() {
        let (blob, _dir) = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob.clone());

        let hash = Hash::from_content(b"kept object");
        index.put(hash, MetadataEntry { len: 11, encoding: ObjectEncoding::Full }).await.unwrap();
        index.mark_keep_full(hash).await.unwrap();

        let reopened = FileSystemMetadataStore::new(blob);
        reopened.rebuild_from_wal(&InMemoryWal::new()).await.unwrap();
        assert!(reopened.is_keep_full(&hash).await.unwrap(), "the marker survives a reopen");
    }

    /// A marker file naming a version this build does not know is ignored.
    ///
    /// That is the whole reason the marker is a side file: a newer build may
    /// mark objects in a shape this build cannot read, and the store must still
    /// open and treat nothing as marked rather than refusing to start or
    /// guessing at the bytes.
    #[tokio::test]
    async fn an_unknown_keep_full_version_is_ignored() {
        let (blob, _dir) = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob.clone());

        let hash = Hash::from_content(b"kept by a newer build");
        index.put(hash, MetadataEntry { len: 22, encoding: ObjectEncoding::Full }).await.unwrap();
        blob.write_aux(&hash, "keep-full-v9.json", Bytes::from_static(br#"{"hashes":[]}"#))
            .await
            .unwrap();

        let reopened = FileSystemMetadataStore::new(blob);
        reopened.rebuild_from_wal(&InMemoryWal::new()).await.unwrap();
        assert!(
            reopened.get(&hash).await.unwrap().is_some(),
            "the metadata snapshot still loads beside an unknown marker version"
        );
        assert!(
            !reopened.is_keep_full(&hash).await.unwrap(),
            "an unknown marker version is ignored, not adopted"
        );
    }

    /// Deleting an object drops its keep-full marker, so the marker file cannot
    /// outlive the objects it describes.
    #[tokio::test]
    async fn a_deleted_object_loses_its_keep_full_marker() {
        let (blob, _dir) = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob.clone());

        let hash = Hash::from_content(b"kept then deleted");
        index.put(hash, MetadataEntry { len: 17, encoding: ObjectEncoding::Full }).await.unwrap();
        index.mark_keep_full(hash).await.unwrap();
        index.delete(&hash).await.unwrap();

        assert!(!index.is_keep_full(&hash).await.unwrap(), "the marker goes with the object");
        assert!(
            blob.read_aux(&hash, versions::LATEST_KEEP_FULL_FORMAT).await.is_err(),
            "no marker file is left behind in the deleted object's directory"
        );

        let reopened = FileSystemMetadataStore::new(blob);
        reopened.rebuild_from_wal(&InMemoryWal::new()).await.unwrap();
        assert!(!reopened.is_keep_full(&hash).await.unwrap(), "the marker is not resurrected");
    }
}
