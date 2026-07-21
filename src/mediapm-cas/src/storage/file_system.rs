//! File-system CAS — persistent store using file-based journal, blob
//! store, and file-system-backed index.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::blob_store::{BlobStore, FileSystemBlobStore};
use super::metadata_store::{FileSystemMetadataStore, MetadataStore};
use super::store::CasStore;
use super::wal::FileWal;
use super::wal::Wal;
use crate::api::{CasApi, ObjectEncoding, VerifyTriggerStrategy};
use crate::background::BackgroundMaintenanceGuard;
use crate::defaults;
use crate::error::CasError;
use crate::hash::Hash;
use crate::storage::metadata_store::MetadataEntry;

/// File-system backed CAS store.
///
/// Wraps [`CasStore`] with a [`FileWal`] for WAL persistence, a
/// [`FileSystemBlobStore`] for payload persistence, and a
/// [`FileSystemMetadataStore`] for metadata + constraint lookup with per-
/// directory persistent snapshots alongside blob files.
///
/// Spawns a background WAL consumer on open to periodically materialize
/// WAL entries into blob + metadata.
pub struct FileSystemCas {
    store: Arc<CasStore<FileWal, FileSystemMetadataStore, FileSystemBlobStore>>,
    _bg_guard: Arc<BackgroundMaintenanceGuard>,
}

impl Clone for FileSystemCas {
    fn clone(&self) -> Self {
        Self { store: self.store.clone(), _bg_guard: self._bg_guard.clone() }
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
    /// # Errors
    ///
    /// Delegates to WAL creation, blob store creation, and metadata rebuild.
    pub async fn open_with_strategies_and_interval(
        dir: &Path,
        verify_strategies: Vec<VerifyTriggerStrategy>,
        bg_interval: Duration,
    ) -> Result<Self, CasError> {
        let wal = FileWal::create(dir.to_path_buf()).await?;
        let start_pos = wal.consumed_position().await;
        let blob = FileSystemBlobStore::create(dir.join("blobs"), verify_strategies).await?;
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
        let guard = BackgroundMaintenanceGuard { cancelled, handle: Some(handle) };

        Ok(Self { store, _bg_guard: Arc::new(guard) })
    }

    /// Open or create a file-system CAS store at `dir` with the given
    /// verify strategies, spawning a background WAL consumer.
    ///
    /// # Errors
    ///
    /// Delegates to WAL creation, blob store creation, and metadata rebuild.
    pub async fn open_with_strategies(
        dir: &Path,
        verify_strategies: Vec<VerifyTriggerStrategy>,
    ) -> Result<Self, CasError> {
        Self::open_with_strategies_and_interval(dir, verify_strategies, Duration::from_secs(300))
            .await
    }

    /// Open or create a file-system CAS store at `dir` with no
    /// integrity verification enabled.
    ///
    /// # Errors
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
    #[allow(dead_code)]
    pub fn bg_guard_ref(&self) -> &Arc<BackgroundMaintenanceGuard> {
        &self._bg_guard
    }
}
