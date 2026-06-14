//! File-system CAS — persistent store using file-based journal, blob
//! store, and file-system-backed index.

use std::path::{Path, PathBuf};

use super::blob_store::{FileSystemBlobStore, hash_to_path};
use super::index::{FileSystemIndex, Index};
use super::store::CasStore;
use super::wal::FileWal;
use crate::api::VerifyTriggerStrategy;
use crate::error::CasError;
use crate::hash::Hash;

/// File-system backed CAS store.
///
/// Wraps [`CasStore`] with a [`FileWal`] for WAL persistence, a
/// [`FileSystemBlobStore`] for payload persistence, and a
/// [`FileSystemIndex`] for metadata + constraint lookup with persistent
/// constraint storage at `<dir>/constraints.json`.
#[derive(Clone)]
pub struct FileSystemCas(pub(crate) CasStore<FileWal, FileSystemIndex, FileSystemBlobStore>);

impl std::ops::Deref for FileSystemCas {
    type Target = CasStore<FileWal, FileSystemIndex, FileSystemBlobStore>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl_cas_wrapper_traits!(FileSystemCas);

impl FileSystemCas {
    /// Open or create a file-system CAS store at `dir`.
    ///
    /// Creates the journal directory, checkpoint, blob store root, and
    /// constraint persistence file if they don't exist. Rebuilds the index
    /// from the WAL on open. Constraint data persists at
    /// `<dir>/constraints.json`.
    /// Open or create a file-system CAS store at `dir` with the given
    /// verify strategies.
    pub async fn open_with_strategies(
        dir: &Path,
        verify_strategies: Vec<VerifyTriggerStrategy>,
    ) -> Result<Self, CasError> {
        let wal = FileWal::create(dir.to_path_buf()).await?;
        let blob_store = FileSystemBlobStore::create(dir.join("blobs"), verify_strategies).await?;
        let constraint_path = dir.join("constraints.json");
        let index = FileSystemIndex::new(constraint_path);
        index.rebuild_from_wal(&wal).await?;
        let store = CasStore::new(wal, index, blob_store);
        store.seed_sentinel().await?;
        Ok(Self(store))
    }

    /// Open or create a file-system CAS store at `dir` with no
    /// integrity verification enabled.
    pub async fn open(dir: &Path) -> Result<Self, CasError> {
        Self::open_with_strategies(dir, Vec::new()).await
    }

    /// Return the on-disk path for a hash's full blob (without `.diff`).
    ///
    /// The caller should verify the path exists before using it for
    /// materialization (e.g., hardlink, symlink, reflink). Returns the path
    /// even when the blob is stored as delta — check `exists` vs the
    /// concrete file.
    ///
    /// This is a `FileSystemCas`-only method; in-memory stores do not have
    /// materializable paths.
    pub fn object_path_for_hash(&self, hash: Hash) -> PathBuf {
        let root = self.0.blob_store().root();
        hash_to_path(root, &hash)
    }
}
