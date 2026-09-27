//! File-system CAS — persistent store using file-based journal, blob
//! store, and file-system-backed index.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::blob_store::{BlobStore, FileSystemBlobStore};
use super::directory_lock::DirectoryLockGuard;
use super::metadata_store::{FileSystemMetadataStore, MetadataStore};
use super::store::CasStore;
use super::wal::FileWal;
use super::wal::Wal;
use crate::api::{CasApi, ObjectEncoding, VerifyTriggerStrategy};
use crate::background::BackgroundMaintenanceGuard;
use crate::defaults;
use crate::error::CasError;
use crate::hash::Hash;
use crate::io_gate::CasIoGate;
use crate::storage::metadata_store::MetadataEntry;

/// Marker for the set of live [`FileSystemCas`] handles.
///
/// Only `FileSystemCas` values hold this, never the stores it owns, so
/// [`Arc::strong_count`] is exactly the number of live handles to the store.
/// That is what lets [`Drop`] tell the last handle from a clone.
#[derive(Debug)]
struct HandleToken;

/// File-system backed CAS store.
///
/// Wraps [`CasStore`] with a [`FileWal`] for WAL persistence, a
/// [`FileSystemBlobStore`] for payload persistence, and a
/// [`FileSystemMetadataStore`] for metadata + constraint lookup with per-
/// directory persistent snapshots alongside blob files.
///
/// Spawns a background WAL consumer on open to periodically materialize
/// WAL entries into blob + metadata.
///
/// # Teardown
///
/// The background consumer holds an owned `Arc` to the store, so it can
/// outlive the caller's handle; dropping the last handle cancels it and closes
/// the store's internal mutation gate, which waits for every already-dispatched
/// file-system mutation and refuses new ones. After the last handle drops, no
/// code path can recreate anything under the store's directory — which is
/// what makes it safe for the owner of that directory (a test's `TempDir`, for
/// example) to remove it straight after the handle. See the crate's `io_gate`
/// module for the contract and why `Drop` can rely on it.
pub struct FileSystemCas {
    store: Arc<CasStore<FileWal, FileSystemMetadataStore, FileSystemBlobStore>>,
    bg_guard: Arc<BackgroundMaintenanceGuard>,
    dir_lock: Arc<DirectoryLockGuard>,
    /// Mutation gate shared with the WAL and blob store.
    io_gate: Arc<CasIoGate>,
    /// Shared by every clone of this handle, and by nothing else.
    handles: Arc<HandleToken>,
}

impl Clone for FileSystemCas {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            bg_guard: self.bg_guard.clone(),
            dir_lock: self.dir_lock.clone(),
            io_gate: Arc::clone(&self.io_gate),
            handles: Arc::clone(&self.handles),
        }
    }
}

impl Drop for FileSystemCas {
    /// Quiesce the store when the **last** handle drops.
    ///
    /// A clone dropping while another handle is alive must not close the
    /// store, so the work is gated on this being the only remaining handle.
    /// Both steps are synchronous: `cancel` requests the abort and closing the
    /// mutation gate waits for the blocking-pool work the aborted task already
    /// dispatched, which abort alone would not cover.
    fn drop(&mut self) {
        if Arc::strong_count(&self.handles) == 1 {
            self.bg_guard.cancel();
            self.io_gate.close();
        }
    }
}

impl std::ops::Deref for FileSystemCas {
    type Target = CasStore<FileWal, FileSystemMetadataStore, FileSystemBlobStore>;
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl FileSystemCas {
    /// Open or create a file-system CAS store at `dir` with the given
    /// verify strategies, spawning a background WAL consumer with the
    /// given interval between cycles.
    ///
    /// The consumer starts after a 500 ms initial delay so fast setup flows
    /// (e.g. tests) finish before the first maintenance cycle races them.
    /// It runs as a deferred background task, not synchronously during open,
    /// so the store is readable immediately after construction.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::LockContention`] if the directory is already
    /// locked by another [`FileSystemCas`] instance. Share the
    /// [`Arc<FileSystemCas>`] between consumers instead of opening multiple.
    pub async fn open_with_strategies_and_interval(
        dir: &Path,
        verify_strategies: Vec<VerifyTriggerStrategy>,
        bg_interval: Duration,
    ) -> Result<Self, CasError> {
        // One gate for the whole store: the WAL, the blob store, and the
        // metadata store (which writes through the blob store) all lease
        // against it, so closing it quiesces every file-system mutation the
        // store can issue without a live handle.
        let io_gate = Arc::new(CasIoGate::new());
        let wal = FileWal::create_gated(
            dir.to_path_buf(),
            FileWal::DEFAULT_MAX_SEGMENT_SIZE,
            Arc::clone(&io_gate),
        )
        .await?;
        let start_pos = wal.consumed_position().await;
        let blob = FileSystemBlobStore::create_gated(
            dir.join("blobs"),
            verify_strategies,
            Arc::clone(&io_gate),
        )
        .await?;
        let metadata = FileSystemMetadataStore::new(blob.clone());
        metadata.rebuild_from_wal(&wal).await?;
        let store = Arc::new(CasStore::new(wal, metadata, blob, start_pos, defaults::CACHE_TTL));

        // Spawn background WAL consumer with the given interval.
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_clone = cancelled.clone();
        let store_clone = store.clone();
        let handle = tokio::spawn(async move {
            // Small initial delay so fast tests can set up before
            // the first maintenance cycle races against them. The
            // run→sleep lifecycle ensures first real maintenance runs
            // promptly after this window.
            tokio::time::sleep(Duration::from_millis(500)).await;
            loop {
                if cancelled_clone.load(Ordering::Relaxed) {
                    break;
                }
                let _ = store_clone.bg_engine().run_wal_consumer().await;
                if cancelled_clone.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(bg_interval).await;
            }
        });
        let guard = BackgroundMaintenanceGuard::new(cancelled, handle);

        // Acquire exclusive directory lock (intra-process then inter-process).
        // The lock is held for the full CAS lifetime via Arc sharing.
        let dir_lock = Arc::new(DirectoryLockGuard::lock(dir).await?);

        Ok(Self {
            store,
            bg_guard: Arc::new(guard),
            dir_lock,
            io_gate,
            handles: Arc::new(HandleToken),
        })
    }

    /// Deterministically stop background maintenance and quiesce the store.
    ///
    /// Awaits the background WAL consumer so it has actually stopped, then
    /// closes the mutation gate so no file-system write it dispatched is still
    /// running. When this returns, the store's directory is inert: nothing can
    /// add to it, so the caller may remove it immediately.
    ///
    /// Dropping the last handle already does both of these things; `close` is
    /// for callers that want the guarantee *before* the handle goes out of
    /// scope, or that want the consumer to be observed as fully stopped rather
    /// than merely aborted. It consumes the handle, so no clone can keep
    /// writing into a store the caller has already retired.
    pub async fn close(self) {
        self.bg_guard.shutdown().await;
        self.io_gate.close();
    }

    /// Open or create a file-system CAS store at `dir` with the given
    /// verify strategies, spawning a background WAL consumer.
    /// # Errors
    ///
    /// Returns [`CasError::LockContention`] if the directory is already
    /// locked by another [`FileSystemCas`] instance.
    ///
    /// Delegates to WAL creation, blob store creation, and metadata rebuild.
    pub async fn open_with_strategies(
        dir: &Path,
        verify_strategies: Vec<VerifyTriggerStrategy>,
    ) -> Result<Self, CasError> {
        Self::open_with_strategies_and_interval(dir, verify_strategies, Duration::from_mins(5))
            .await
    }

    /// Open or create a file-system CAS store at `dir` with no
    /// integrity verification enabled.
    /// # Errors
    ///
    /// Returns [`CasError::LockContention`] if the directory is already
    /// locked by another [`FileSystemCas`] instance.
    ///
    /// Delegates to [`open_with_strategies`](Self::open_with_strategies).
    pub async fn open(dir: &Path) -> Result<Self, CasError> {
        Self::open_with_strategies(dir, Vec::new()).await
    }

    /// Return the on-disk path for a hash's full blob (without `.diff`),
    /// if this store can materialize it. In-memory stores return `None`.
    ///
    /// The caller should verify the path exists before using it for
    /// materialization (e.g., hardlink, symlink, reflink). Returns the path
    /// even when the blob is stored as delta — check `exists` vs the
    /// concrete file.
    #[must_use]
    pub fn object_path_for_hash(&self, hash: Hash) -> Option<PathBuf> {
        self.blob().materialized_path(&hash)
    }

    /// Ensure the blob for `hash` is materialized in the blob store on
    /// disk, even if it was originally committed as a WAL-only small blob.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::NotFound`] if the hash does not exist, or
    /// delegates to blob store and metadata store operations.
    ///
    /// After calling this, [`object_path_for_hash`](Self::object_path_for_hash)
    /// will return a path whose file exists and can be used for
    /// hardlink/symlink/reflink materialization.
    pub async fn ensure_blob_materialized(&self, hash: Hash) -> Result<(), CasError> {
        // Fast path: already materialized in the blob store.
        if self.blob().materialized_path(&hash).is_some_and(|p| p.is_file()) {
            return Ok(());
        }

        // Slow path: read bytes from CAS (WAL fallback handles small
        // blobs) and write them to the blob store + metadata.
        let data = self.get(hash).await?;
        self.blob().write(hash, ObjectEncoding::Full, data.clone()).await?;
        self.metadata_store()
            .put(hash, MetadataEntry { len: data.len() as u64, encoding: ObjectEncoding::Full })
            .await?;
        Ok(())
    }

    /// Test-only: returns a reference to the background maintenance guard.
    #[must_use]
    pub fn bg_guard_ref(&self) -> &Arc<BackgroundMaintenanceGuard> {
        &self.bg_guard
    }
}
