//! File-system CAS — persistent store using file-based journal, blob
//! store, and file-system-backed index.

use std::path::{Path, PathBuf};

use super::blob_store::{BlobStore, FileSystemBlobStore};
use super::metadata_store::{FileSystemMetadataStore, MetadataStore};
use super::store::CasStore;
use super::wal::FileWal;
use super::wal::Wal;
use crate::api::VerifyTriggerStrategy;
use crate::defaults;
use crate::error::CasError;
use crate::hash::Hash;

/// File-system backed CAS store.
///
/// Wraps [`CasStore`] with a [`FileWal`] for WAL persistence, a
/// [`FileSystemBlobStore`] for payload persistence, and a
/// [`FileSystemMetadataStore`] for metadata + constraint lookup with persistent
/// snapshot storage at `<dir>/metadata.json`.
#[derive(Clone)]
pub struct FileSystemCas(
    pub(crate) CasStore<FileWal, FileSystemMetadataStore, FileSystemBlobStore>,
);

impl std::ops::Deref for FileSystemCas {
    type Target = CasStore<FileWal, FileSystemMetadataStore, FileSystemBlobStore>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FileSystemCas {
    /// Open or create a file-system CAS store at `dir` with the given
    /// verify strategies.
    pub async fn open_with_strategies(
        dir: &Path,
        verify_strategies: Vec<VerifyTriggerStrategy>,
    ) -> Result<Self, CasError> {
        let wal = FileWal::create(dir.to_path_buf()).await?;
        let start_pos = wal.consumed_position().await;
        let blob = FileSystemBlobStore::create(dir.join("blobs"), verify_strategies).await?;
        let metadata_path = dir.join("metadata.json");
        let metadata = FileSystemMetadataStore::new(metadata_path);
        metadata.rebuild_from_wal(&wal).await?;
        let store = CasStore::new(wal, metadata, blob, start_pos, defaults::CACHE_TTL);
        Ok(Self(store))
    }

    /// Open or create a file-system CAS store at `dir` with no
    /// integrity verification enabled.
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
        self.0.blob().materialized_path(&hash)
    }
}
