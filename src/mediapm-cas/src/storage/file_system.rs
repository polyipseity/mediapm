//! File-system CAS — persistent store using file-based journal, blob
//! store, and in-memory index.

use std::path::{Path, PathBuf};

use super::blob_store::{FileSystemBlobStore, hash_to_path};
use super::index::{InMemoryIndex, Index};
use super::store::CasStore;
use super::wal::FileWal;
use crate::error::CasError;
use crate::hash::Hash;

/// File-system backed CAS store.
///
/// Wraps [`CasStore`] with a [`FileWal`] for WAL persistence, a
/// [`FileSystemBlobStore`] for payload persistence, and an
/// [`InMemoryIndex`] for metadata + constraint lookup.
#[derive(Clone)]
pub struct FileSystemCas(pub(crate) CasStore<FileWal, InMemoryIndex, FileSystemBlobStore>);

impl std::ops::Deref for FileSystemCas {
    type Target = CasStore<FileWal, InMemoryIndex, FileSystemBlobStore>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl_cas_wrapper_traits!(FileSystemCas);

impl FileSystemCas {
    /// Open or create a file-system CAS store at `dir`.
    ///
    /// Creates the journal directory, checkpoint, and blob store root if
    /// they don't exist. Rebuilds the index from the WAL on open.
    pub async fn open(dir: &Path) -> Result<Self, CasError> {
        let wal = FileWal::create(dir.to_path_buf()).await?;
        let blob_store = FileSystemBlobStore::create(dir.join("blobs")).await?;
        let index = InMemoryIndex::new();
        index.rebuild_from_wal(&wal).await?;
        Ok(Self(CasStore::new(wal, index, blob_store)))
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
