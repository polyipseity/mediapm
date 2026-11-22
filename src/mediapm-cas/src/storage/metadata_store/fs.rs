//! Filesystem-backed metadata — in-memory with per-directory persistent snapshots.
//!
//! [`FileSystemMetadataStore`] wraps an [`InMemoryMetadataStore`] and persists
//! metadata entries and constraints in per-directory JSON snapshots (one per
//! fan-out directory `v1/blake3/ab/cd/`), using the [`BlobStore`] auxiliary-file
//! API so each mutation flushes only its hash's directory — O(N/D) I/O instead
//! of O(N).
//!
//! Snapshot files use a versioned filename (`metadata-v3.json`) so future format
//! changes are detectable and migratable. This module holds no version knowledge
//! of its own: it consumes the names and codecs from [`versions`], which owns the
//! ladder.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

use super::super::blob_store::BlobStore;
use super::super::blob_store::FileSystemBlobStore;
use super::super::wal::Wal;
use super::mem::InMemoryMetadataStore;
use super::versions::{self, LATEST_METADATA_FORMAT, METADATA_FORMAT_NAMES};
use super::{MetadataEntry, MetadataStore, PrunedConstraints};

/// In-memory metadata with per-directory persistent snapshots.
///
/// Delegates all operations to an [`InMemoryMetadataStore`]. After every
/// mutation, only the fan-out directory of the affected hash is flushed to
/// disk as a small JSON file (with a versioned filename like
/// `metadata-v3.json`) via the blob store's aux-file API.
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
    ///
    /// # One store per blob store
    ///
    /// A blob store backs one metadata store, and this is where that is
    /// claimed: a second store built over a clone or any other handle of the
    /// same open blob store returns an error, because each store would install
    /// snapshots holding only the facts it collected. Clone the store instead —
    /// a clone shares the index a snapshot is taken from. A restart opens a new
    /// blob store over the same root, which is a new claim.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::InvalidArgument`] if this blob store already backs a
    /// metadata store.
    pub fn new(blob_store: FileSystemBlobStore) -> Result<Self, CasError> {
        blob_store.claim_metadata_owner()?;
        Ok(Self { inner: InMemoryMetadataStore::new(), blob_store })
    }

    /// Flush everything whose hash falls into the same fan-out directory as
    /// `hash`: entries, constraints, and corruption records.
    ///
    /// Writes a `metadata-v3.json` aux file for that directory in the latest
    /// format, and removes the snapshot file when the facts it holds are empty.
    /// After writing, any older format files in the same directory are cleaned
    /// up (future-proof for format bumps).
    ///
    /// # One critical section per directory
    ///
    /// The facts collected here and the files installed from them are one
    /// critical section for the directory, so an installed snapshot can never
    /// omit an entry that was inserted before it. Collecting outside the
    /// install would let two writers into one directory interleave as
    /// *insert, collect, insert, collect, install, install*: the writer that
    /// installed last would then install the snapshot it took before its
    /// sibling's insert, and that sibling's entry would be gone from the file.
    ///
    /// The lock is taken before any blob-store commit lock and released after
    /// the last write or delete returns, which is this module's only lock order
    /// (`dir` then `file`). A store holds an exclusive `flock`, so the directory
    /// needs no cross-process counterpart.
    ///
    /// The lock itself belongs to the blob store
    /// ([`FileSystemBlobStore::lock_aux_dir`](super::super::blob_store::FileSystemBlobStore::lock_aux_dir)),
    /// because the file is the blob store's: clones of a blob store share the
    /// lock, so the guarantee follows the file rather than the caller.
    ///
    /// What the lock does not do is merge facts: it orders the writers of one
    /// directory, and each installs the facts its own store collected. Two
    /// metadata stores over one blob store would therefore each install a file
    /// holding only their own facts, so [`new`](Self::new) refuses the second
    /// store, and the divergent fact sets this would otherwise order cannot
    /// exist. Clones of one metadata store share the index the snapshot is
    /// taken from, so they hold one fact set between them.
    async fn flush_dir(&self, hash: &Hash, sync: bool) -> Result<(), CasError> {
        let _dir = self.blob_store.lock_aux_dir(hash).await;
        let prefix = &hash.to_hex()[..4];

        let mut snapshot = versions::Snapshot::default();

        // Collect constraints for targets sharing this prefix.
        for t in self.inner.list_targets().await? {
            if t.to_hex().starts_with(prefix) {
                let bases = self.inner.get_constraint(&t).await?;
                snapshot.delta_bases.insert(t, bases);
            }
        }

        // Collect entries for hashes sharing this prefix. Only the length is
        // persisted: which physical form holds the payload is a property of the
        // tree, so the snapshot must not carry a second copy of it.
        for h in self.inner.list_hashes().await? {
            if h.to_hex().starts_with(prefix)
                && let Some(entry) = self.inner.get(&h).await?
            {
                snapshot.lengths.insert(h, entry.len);
            }
        }

        for h in self.inner.list_corrupted().await? {
            if h.to_hex().starts_with(prefix) {
                snapshot.corrupted.insert(h);
            }
        }

        if snapshot.delta_bases.is_empty()
            && snapshot.lengths.is_empty()
            && snapshot.corrupted.is_empty()
        {
            // Remove all known snapshot format files when the directory holds nothing.
            for name in METADATA_FORMAT_NAMES {
                self.blob_store.delete_aux(hash, name).await?;
            }
        } else {
            let json = versions::save_to_vec(&snapshot)?;
            if sync {
                self.blob_store
                    .write_aux_syncing(hash, LATEST_METADATA_FORMAT, Bytes::from(json))
                    .await?;
            } else {
                self.blob_store.write_aux(hash, LATEST_METADATA_FORMAT, Bytes::from(json)).await?;
            }
            // Clean up any legacy format files in this directory.
            for name in METADATA_FORMAT_NAMES {
                if *name != LATEST_METADATA_FORMAT {
                    self.blob_store.delete_aux(hash, name).await?;
                }
            }
        }
        Ok(())
    }

    /// Collect the fan-out prefixes of every constraint target and entry.
    ///
    /// A prefix names a fan-out directory, so this is the set of directories
    /// whose snapshot file has something to say.
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
            self.flush_dir(&representative, false).await?;
        }
        Ok(())
    }

    /// Attach an encoding to every persisted length by asking the tree.
    ///
    /// A snapshot records lengths only, because the physical form is a property
    /// of the tree. Deriving it here is what keeps the index and the bytes from
    /// disagreeing, and an entry whose bytes are gone is dropped rather than
    /// resurrected as an object nothing can read.
    async fn derive_entries(
        &self,
        lengths: BTreeMap<Hash, u64>,
    ) -> Result<BTreeMap<Hash, MetadataEntry>, CasError> {
        let mut derived = BTreeMap::new();
        for (hash, len) in lengths {
            let present = self.blob_store.present_encodings(&hash).await?;
            let encoding = if present.full {
                // Full wins: a both-forms state is a transition in flight and
                // the full form is authoritative while it lasts.
                ObjectEncoding::Full
            } else if present.delta {
                // A `.diff` with no full payload is the only copy, so an
                // envelope that does not decode is a corrupt object naming this
                // hash rather than something to skip: it is the last copy, not a
                // redundant one.
                let envelope = self.blob_store.read_delta(&hash).await?;
                let stored = crate::delta::object::StoredObject::decode_delta(&envelope).map_err(
                    |error| CasError::CorruptObject {
                        hash: Some(hash),
                        details: format!("delta-only object does not decode: {error}"),
                    },
                )?;
                ObjectEncoding::Delta { base_hash: stored.state().base_hash }
            } else {
                continue;
            };
            derived.insert(hash, MetadataEntry { len, encoding });
        }
        Ok(derived)
    }

    /// Rewrite every directory holding a legacy snapshot into the current
    /// format, then delete the legacy file.
    ///
    /// Eager but not on the open path: rewriting every directory during `open`
    /// would make opening proportional to the store, which is what the deferred
    /// WAL consumer exists to avoid. Each directory is rewritten under its own
    /// aux lock, so a concurrent flush of that directory either sees the legacy
    /// file (and rewrites it) or sees the current one.
    async fn migrate_legacy_snapshots(&self) -> Result<usize, CasError> {
        let mut migrated = 0usize;
        for name in METADATA_FORMAT_NAMES {
            if *name == LATEST_METADATA_FORMAT {
                continue;
            }
            for (directory, bytes) in self.blob_store.all_aux_with_dir(name).await? {
                let _dir = self.blob_store.lock_aux_dir_at(directory.clone()).await;
                let snapshot = versions::load_named_from_bytes(name, &bytes)?;
                let entries = self.derive_entries(snapshot.lengths).await?;
                let lengths: BTreeMap<Hash, u64> =
                    entries.iter().map(|(hash, entry)| (*hash, entry.len)).collect();
                let serialized = versions::save_to_vec(&versions::Snapshot {
                    delta_bases: snapshot.delta_bases,
                    lengths,
                    corrupted: BTreeSet::new(),
                })?;
                // Write to the directory the legacy file was found in, even if
                // the snapshot contains cross-directory facts (a malformed but
                // possible legacy state). The directory-derived representative
                // maps to the same fan-out directory.
                let cd = directory.file_name().and_then(|name| name.to_str()).unwrap_or_default();
                let ab = directory
                    .parent()
                    .and_then(|parent| parent.file_name())
                    .and_then(|name| name.to_str())
                    .unwrap_or_default();
                let representative = Self::prefix_hash(&format!("{ab}{cd}"))?;
                self.blob_store
                    .write_aux(&representative, LATEST_METADATA_FORMAT, Bytes::from(serialized))
                    .await?;
                self.blob_store.delete_aux(&representative, name).await?;
                migrated += 1;
            }
        }
        Ok(migrated)
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
        self.flush_dir(&hash, false).await
    }

    async fn get(&self, hash: &Hash) -> Result<Option<MetadataEntry>, CasError> {
        self.inner.get(hash).await
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        self.inner.delete(hash).await?;
        self.flush_dir(hash, false).await
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
        self.flush_dir(&target, false).await
    }

    async fn get_constraint(&self, target: &Hash) -> Result<BTreeSet<Hash>, CasError> {
        self.inner.get_constraint(target).await
    }

    async fn list_targets(&self) -> Result<Vec<Hash>, CasError> {
        self.inner.list_targets().await
    }

    async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<PrunedConstraints, CasError> {
        // Target prefixes are read before the prune, because the prune is what
        // removes them from `inner`: a directory whose only fact was a
        // constraint for a target that no longer exists would otherwise keep
        // its file on disk and resurrect the fact on the next open. A live
        // target whose bases all become dead is pinned to full and remains
        // occupied, so its prefix is flushed through occupied_prefixes.
        let targets_before: Vec<Hash> = self.inner.list_targets().await?;
        let pruned = self.inner.prune_targets(live).await?;

        let mut prefixes = self.occupied_prefixes().await?;
        let targets_after: HashSet<Hash> = self.inner.list_targets().await?.into_iter().collect();
        prefixes.extend(
            targets_before
                .iter()
                .filter(|t| !targets_after.contains(t))
                .map(|h| h.to_hex()[..4].to_string()),
        );
        // prune_targets is infrequent, so flushing every affected directory is
        // acceptable.
        self.flush_prefixes(&prefixes).await?;
        Ok(pruned)
    }

    async fn migrate_legacy_snapshots(&self) -> Result<usize, CasError> {
        Self::migrate_legacy_snapshots(self).await
    }

    async fn mark_corrupted(&self, hash: Hash) -> Result<(), CasError> {
        self.inner.mark_corrupted(hash).await?;
        self.flush_dir(&hash, false).await
    }

    async fn clear_corrupted(&self, hash: &Hash) -> Result<(), CasError> {
        self.inner.clear_corrupted(hash).await?;
        self.flush_dir(hash, false).await
    }

    async fn sync_hash(&self, hash: &Hash) -> Result<(), CasError> {
        self.flush_dir(hash, true).await
    }

    async fn list_corrupted(&self) -> Result<Vec<Hash>, CasError> {
        self.inner.list_corrupted().await
    }

    /// # Ordering: the journal is newer than a snapshot
    ///
    /// A snapshot is a per-directory capture of an older state, and the journal
    /// holds the operations recorded since. Overlaying a snapshot on top of the
    /// journal lets a capture that predates a delete resurrect the fact the
    /// delete removed. That is the ordinary case rather than a race: a delete
    /// only appends to the journal, and the capture is rewritten when the
    /// consumer drains that entry, so a store reopened in between has a stale
    /// snapshot beside a fresh delete.
    ///
    /// The snapshots therefore go in first and the journal replays over them.
    /// Both properties hold that way: a fact the journal never mentions survives
    /// a trim, and a fact the journal removed stays removed.
    async fn rebuild_from_wal(&self, wal: &dyn Wal) -> Result<(), CasError> {
        // Overlay per-directory snapshots from every known format name.
        //
        // Applied oldest-first, newest last, because the names are listed
        // newest-first for readers that want the current format: applying them
        // in list order would let a stale legacy snapshot overwrite a newer
        // current one held in the same directory. Both formats coexist while
        // the sweep catches up, so this order is what keeps a legacy file from
        // being a regression.
        for name in METADATA_FORMAT_NAMES.iter().rev() {
            for (_directory, bytes) in self.blob_store.all_aux_with_dir(name).await? {
                let snapshot = versions::load_named_from_bytes(name, &bytes)?;
                let versions::Snapshot { delta_bases, lengths, corrupted } = snapshot;
                let persisted_entries = self.derive_entries(lengths).await?;
                for (target, bases) in delta_bases {
                    self.inner.set_constraint(target, bases).await?;
                }
                for (hash, entry) in persisted_entries {
                    self.inner.put(hash, entry).await?;
                }
                self.inner.restore_corrupted(corrupted).await?;
            }
        }

        // Replay the journal last, so an operation recorded since a snapshot
        // was captured wins over that snapshot.
        self.inner.rebuild_from_wal(wal).await?;
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

    /// Open a [`FileSystemBlobStore`] over `root` for testing
    /// [`FileSystemMetadataStore`].
    ///
    /// A test that rebuilds a store from disk opens a blob store over the same
    /// root through this, because a blob store backs one metadata store: the
    /// rebuild is a restart, and a restart is a new open rather than a second
    /// store over the first handle.
    async fn test_blob_store_at(root: std::path::PathBuf) -> FileSystemBlobStore {
        FileSystemBlobStore::create(root, Vec::new())
            .await
            .expect("open a blob store over the test root")
    }

    /// Create a [`FileSystemBlobStore`] in a temp directory for testing
    /// [`FileSystemMetadataStore`].
    ///
    /// Returns the `TempDir` alongside the store: the store lazily
    /// `create_dir_all`s its tree on first write, so dropping the guard
    /// before the store does would recreate the tree without RAII
    /// ownership and leak `$TMPDIR/mediapm-artifact-*` dirs.
    async fn test_blob_store() -> (FileSystemBlobStore, mediapm_utils::temp::ManagedTempDir) {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let blob = test_blob_store_at(dir.path().join("blobs")).await;
        (blob, dir)
    }

    #[tokio::test]
    async fn persists_and_restores_constraint() {
        let (blob, _dir) = test_blob_store().await;
        let root = blob.root().to_path_buf();
        let index = FileSystemMetadataStore::new(blob).expect("the metadata store");

        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");
        index.set_constraint(target, BTreeSet::from([base])).await.unwrap();
        // Already flushed by set_constraint.

        // Reopen the blobs and build a new metadata store over them; that is a
        // restart, so it reads whatever the first store left on disk.
        let index2 = FileSystemMetadataStore::new(test_blob_store_at(root).await)
            .expect("a reopened blob store backs a metadata store");
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
        let root = blob.root().to_path_buf();

        // Create a metadata store, add constraint via persisted flush.
        let index = FileSystemMetadataStore::new(blob).expect("the metadata store");
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

        // A restart reopens the blobs under the same root.
        let index2 = FileSystemMetadataStore::new(test_blob_store_at(root).await)
            .expect("a reopened blob store backs a metadata store");
        index2.rebuild_from_wal(&journal).await.unwrap();

        // Assert both persisted and WAL constraints survive.
        assert!(!index2.get_constraint(&Hash::from_content(b"p")).await.unwrap().is_empty());
        assert!(!index2.get_constraint(&Hash::from_content(b"w2")).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn fresh_blob_store_starts_empty() {
        let (blob, _dir) = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob).expect("the metadata store");
        let journal = InMemoryWal::new();
        index.rebuild_from_wal(&journal).await.unwrap();
        assert!(index.is_empty().await);
    }

    /// Two puts into one fan-out directory must both survive in that directory's
    /// snapshot file.
    ///
    /// The file is derived: each writer collects the store's facts and installs
    /// them. Collecting outside the install's critical section lets a writer
    /// install a snapshot taken before its sibling's insert, and the file then
    /// omits the sibling. One pair reproduces that rarely, so the test drives
    /// [`RACE_ROUNDS`] pairs, each in its own directory so a round cannot be
    /// rescued by the previous round's file.
    ///
    /// Both entries are read back through a store rebuilt from disk: the
    /// in-memory layer holds both whatever the file says, so reading it would
    /// pass even when the snapshot on disk dropped one of them.
    ///
    /// The runtime is single-threaded because that is where the window opens
    /// dependably: `write_aux` hands its directory creation and its rename to
    /// the blocking pool (`FileSystemBlobStore::write_aux`), so the two writers'
    /// installs complete in an order that need not match their inserts.
    /// Measured at eleven lost rounds in a hundred while the defect existed.
    #[tokio::test]
    async fn concurrent_entries_in_one_directory_both_persist() {
        /// Rounds needed to reach the window. A pair loses its sibling in about
        /// one round in nine, so a hundred rounds leave a false green at
        /// roughly one run in a hundred thousand.
        const RACE_ROUNDS: usize = 100;

        let (blob, _dir) = test_blob_store().await;
        let root = blob.root().to_path_buf();
        let index = std::sync::Arc::new(
            FileSystemMetadataStore::new(blob.clone()).expect("the metadata store"),
        );
        let entry = || MetadataEntry { len: 1, encoding: ObjectEncoding::Full };

        let mut used: BTreeSet<String> = BTreeSet::new();
        for round in 0..RACE_ROUNDS {
            let (left, right) = fresh_pair(&mut used, round);
            // The snapshot carries lengths only; rebuild derives encoding from the
            // tree. A real entry therefore needs a physical form on disk, or the
            // rebuild drops it as absent by design.
            blob.write(left, ObjectEncoding::Full, Bytes::from_static(b"x"))
                .await
                .expect("left blob");
            blob.write(right, ObjectEncoding::Full, Bytes::from_static(b"y"))
                .await
                .expect("right blob");
            let (left_index, right_index) =
                (std::sync::Arc::clone(&index), std::sync::Arc::clone(&index));
            let (left_entry, right_entry) = (entry(), entry());
            let (left_task, right_task) = tokio::join!(
                tokio::spawn(async move { left_index.put(left, left_entry).await }),
                tokio::spawn(async move { right_index.put(right, right_entry).await }),
            );
            left_task.expect("left put joined").expect("left put");
            right_task.expect("right put joined").expect("right put");

            // Each round reads the directory back through its own open of the
            // blobs, which is the restart the snapshot has to survive.
            let reloaded = FileSystemMetadataStore::new(test_blob_store_at(root.clone()).await)
                .expect("a reopened blob store backs a metadata store");
            reloaded.rebuild_from_wal(&InMemoryWal::new()).await.expect("reload the snapshots");
            assert!(
                reloaded.get(&left).await.expect("read left").is_some(),
                "round {round}: {left:?} lost"
            );
            assert!(
                reloaded.get(&right).await.expect("read right").is_some(),
                "round {round}: {right:?} lost"
            );
        }
    }

    /// Find two hashes that share a fan-out directory and that no earlier round
    /// used.
    ///
    /// Found rather than assumed, so the test keeps colliding if the layout
    /// changes; `used` keeps the rounds from reusing a directory, which would let
    /// a later round pass on an earlier round's snapshot. One pass over the
    /// candidates is enough: a four-hex-digit fan-out collides within a few
    /// hundred, and a scan that followed each candidate until a partner appeared
    /// would cost a full directory of probes per round instead.
    fn fresh_pair(used: &mut BTreeSet<String>, round: usize) -> (Hash, Hash) {
        let mut seen: BTreeMap<String, Hash> = BTreeMap::new();
        for counter in 0u32.. {
            let hash = Hash::from_content(format!("entry-{round}-{counter}").as_bytes());
            let prefix = hash.to_hex()[..4].to_string();
            if used.contains(&prefix) {
                continue;
            }
            if let Some(first) = seen.insert(prefix.clone(), hash) {
                used.insert(prefix);
                return (first, hash);
            }
        }
        unreachable!("two of a few hundred hashes share a four-hex-digit fan-out")
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
        let root = blob.root().to_path_buf();
        let index = FileSystemMetadataStore::new(blob).expect("the metadata store");

        let live = Hash::from_content(b"live");
        index.set_constraint(live, BTreeSet::from([Hash::from_content(b"dead")])).await.unwrap();

        index.prune_targets(&HashSet::from([live])).await.unwrap();

        // Reload and verify.
        let index2 = FileSystemMetadataStore::new(test_blob_store_at(root).await)
            .expect("a reopened blob store backs a metadata store");
        let journal = InMemoryWal::new();
        index2.rebuild_from_wal(&journal).await.unwrap();
        assert_eq!(
            index2.get_constraint(&live).await.unwrap(),
            BTreeSet::from([Hash::empty()]),
            "a pruned-to-empty base set is pinned full on disk, not widened"
        );
    }

    /// A constraint whose target the store never held leaves no file behind.
    ///
    /// The prune flushes the directories of the facts it kept and of the
    /// markers it read before dropping them. A target with neither an entry nor
    /// a marker contributes no prefix to either set, so its directory was never
    /// rewritten and the snapshot on disk outlived the fact inside it. Removal
    /// is what a content-addressed store does to bytes, so the state this test
    /// seeds is one an older build wrote and a live store can still hold; the
    /// prune has to heal it rather than leave it for the next open.
    #[tokio::test]
    async fn prune_removes_the_constraint_file_of_a_target_that_is_gone() {
        let (blob, _dir) = test_blob_store().await;
        let root = blob.root().to_path_buf();
        let index = FileSystemMetadataStore::new(blob.clone()).expect("the metadata store");

        let ghost = Hash::from_content(b"ghost target");
        let base = Hash::from_content(b"ghost base");
        // Seed through the inner store and flush by hand: the outer
        // `set_constraint` names the same directory, so a test that used it
        // would be measuring the seeding rather than the prune.
        index.inner.set_constraint(ghost, BTreeSet::from([base])).await.unwrap();
        index.flush_dir(&ghost, false).await.unwrap();
        assert!(
            blob.read_aux(&ghost, LATEST_METADATA_FORMAT).await.is_ok(),
            "the constraint file must exist before the prune",
        );

        let pruned = index.prune_targets(&HashSet::new()).await.unwrap();
        assert_eq!(pruned.targets, 1, "the constraint's target is not live, so it is dropped");

        assert!(
            blob.read_aux(&ghost, LATEST_METADATA_FORMAT).await.is_err(),
            "the constraint file of a dropped target must be gone from disk",
        );
        let reopened = FileSystemMetadataStore::new(test_blob_store_at(root).await)
            .expect("a reopened blob store backs a metadata store");
        reopened.rebuild_from_wal(&InMemoryWal::new()).await.unwrap();
        assert!(
            reopened.get_constraint(&ghost).await.unwrap().is_empty(),
            "a constraint left on disk comes back on the next open",
        );
    }

    /// A blob store backs one metadata store; the second claim is refused.
    ///
    /// The pair is what a flush would silently break: each store installs only
    /// its own facts, so the last writer drops the other's entry. The claim turns
    /// that into an error at construction instead of a loss at write time.
    #[tokio::test]
    async fn a_blob_store_backs_one_metadata_store() {
        let (blob, _dir) = test_blob_store().await;
        let root = blob.root().to_path_buf();

        FileSystemMetadataStore::new(blob.clone())
            .expect("the first metadata store owns the blobs");

        let second = FileSystemMetadataStore::new(blob.clone());
        let message = match second {
            Ok(_) => panic!("a second metadata store over one blob store must be refused"),
            Err(error) => error.to_string(),
        };
        assert!(
            message.contains("one metadata store"),
            "the refusal names the invariant, got {message:?}"
        );

        // A restart opens a new blob store over the same root, which is a new
        // claim rather than a second store over the first one.
        FileSystemMetadataStore::new(test_blob_store_at(root).await)
            .expect("a reopened blob store backs a metadata store");
    }

    /// Deleting an object drops the constraint that describes it, so a reopened
    /// store cannot resurrect a base set for a hash it does not hold.
    #[tokio::test]
    async fn a_deleted_object_loses_its_constraint() {
        let (blob, _dir) = test_blob_store().await;
        let root = blob.root().to_path_buf();
        let index = FileSystemMetadataStore::new(blob.clone()).expect("the metadata store");

        let hash = Hash::from_content(b"kept then deleted");
        index.put(hash, MetadataEntry { len: 17, encoding: ObjectEncoding::Full }).await.unwrap();
        index.set_constraint(hash, BTreeSet::from([Hash::empty()])).await.unwrap();

        index.delete(&hash).await.unwrap();

        assert_eq!(
            index.get_constraint(&hash).await.unwrap(),
            BTreeSet::new(),
            "the constraint goes with the object"
        );

        let reopened = FileSystemMetadataStore::new(test_blob_store_at(root).await)
            .expect("a reopened blob store backs a metadata store");
        reopened.rebuild_from_wal(&InMemoryWal::new()).await.unwrap();
        assert_eq!(
            reopened.get_constraint(&hash).await.unwrap(),
            BTreeSet::new(),
            "the constraint is not resurrected"
        );
    }

    /// A directory holding a v1 snapshot must be rewritten to the current
    /// format and the v1 file deleted, with every fact intact across a reopen.
    ///
    /// The legacy bytes come from `versions::legacy_snapshot_bytes_for_tests`,
    /// not from a fixture: hand-written bytes would drift from the format the
    /// previous build actually wrote, which is the only format this migration
    /// can ever be asked to read.
    #[tokio::test]
    async fn a_legacy_snapshot_is_migrated_and_deleted() {
        let (blob, dir) = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob.clone()).expect("the metadata store");

        let target = Hash::from_content(b"migration-target");
        let base = Hash::from_content(b"migration-base");
        let entry = Hash::from_content(b"migration-entry");
        index.set_constraint(target, BTreeSet::from([base])).await.unwrap();
        blob.write(entry, ObjectEncoding::Full, Bytes::from_static(b"entry-bytes")).await.unwrap();
        index.put(entry, MetadataEntry { len: 17, encoding: ObjectEncoding::Full }).await.unwrap();

        // What a store written by the previous build holds on disk, in the
        // directory the entry lives in.
        let legacy = versions::legacy_snapshot_bytes_for_tests(
            &BTreeMap::from([(target, BTreeSet::from([base]))]),
            &BTreeMap::from([(entry, (17u64, ObjectEncoding::Full))]),
        );
        blob.write_aux(&entry, "metadata-v1.json", Bytes::from(legacy)).await.unwrap();

        let migrated = index.migrate_legacy_snapshots().await.expect("the sweep runs");
        assert_eq!(migrated, 1, "one directory held a legacy snapshot");
        assert!(
            blob.read_aux(&entry, "metadata-v1.json").await.is_err(),
            "the sweep must delete the legacy snapshot it migrated"
        );
        assert!(
            blob.read_aux(&entry, LATEST_METADATA_FORMAT).await.is_ok(),
            "the sweep must leave a current-format snapshot behind"
        );

        // A restart is a new open of the same root, so the first handle goes.
        drop(index);
        drop(blob);
        let reopened =
            FileSystemMetadataStore::new(test_blob_store_at(dir.path().join("blobs")).await)
                .expect("a reopened blob store backs a metadata store");
        reopened.rebuild_from_wal(&InMemoryWal::new()).await.unwrap();
        assert_eq!(
            reopened.get_constraint(&target).await.unwrap(),
            BTreeSet::from([base]),
            "the migrated snapshot must carry the constraints forward"
        );
        assert_eq!(
            reopened.get(&entry).await.unwrap().map(|entry| entry.len),
            Some(17),
            "the migrated snapshot must carry the entry lengths forward"
        );
    }

    /// A store that is already current has nothing to migrate, and the sweep
    /// must not rewrite a file it did not need to touch.
    #[tokio::test]
    async fn a_current_snapshot_is_not_rewritten() {
        let (blob, _dir) = test_blob_store().await;
        let index = FileSystemMetadataStore::new(blob).expect("the metadata store");
        let target = Hash::from_content(b"current-target");
        index.set_constraint(target, BTreeSet::from([Hash::from_content(b"b")])).await.unwrap();

        assert_eq!(
            index.migrate_legacy_snapshots().await.expect("the sweep runs"),
            0,
            "a store that is already current has nothing to migrate"
        );
    }

    /// Encoding must be derived from the tree at load: a full file wins over a
    /// `.diff`, a `.diff`-only object loads as a delta, and an entry whose
    /// bytes are gone is dropped instead of resurrected as a phantom object.
    #[tokio::test]
    async fn load_derives_encoding_from_the_tree() {
        let (blob, _dir) = test_blob_store().await;
        let full = Hash::from_content(b"derived-full");
        let delta = Hash::from_content(b"derived-delta");
        let base = Hash::from_content(b"derived-base");
        let ghost = Hash::from_content(b"derived-ghost");

        blob.write(full, ObjectEncoding::Full, Bytes::from_static(b"full bytes")).await.unwrap();
        let stored = crate::delta::object::StoredObject::delta(base, 11, b"delta bytes".to_vec());
        blob.write(delta, ObjectEncoding::Delta { base_hash: base }, Bytes::from(stored.encode()))
            .await
            .unwrap();

        // One snapshot naming all three, of which one hash has no bytes at all.
        let index_file = serde_json::json!({
            "constraints": {},
            "entries": {
                full.to_string(): 10u64,
                delta.to_string(): 11u64,
                ghost.to_string(): 5u64,
            },
        });
        blob.write_aux(
            &full,
            "metadata-v2.json",
            Bytes::from(serde_json::to_vec(&index_file).expect("serialize the snapshot")),
        )
        .await
        .unwrap();

        let index = FileSystemMetadataStore::new(blob).expect("the metadata store");
        index.rebuild_from_wal(&InMemoryWal::new()).await.unwrap();

        assert_eq!(
            index.get(&full).await.unwrap(),
            Some(MetadataEntry { len: 10, encoding: ObjectEncoding::Full }),
            "a full file must load as Full"
        );
        assert_eq!(
            index.get(&delta).await.unwrap(),
            Some(MetadataEntry { len: 11, encoding: ObjectEncoding::Delta { base_hash: base } }),
            "a .diff-only object must load as Delta with the header's base"
        );
        assert_eq!(
            index.get(&ghost).await.unwrap(),
            None,
            "an entry whose bytes do not exist must be dropped, not resurrected"
        );
    }
}
