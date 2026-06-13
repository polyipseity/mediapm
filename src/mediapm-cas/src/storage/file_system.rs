//! File-system CAS — persistent store using file-based journal with
//! in-memory object/metadata stores.

use std::path::Path;

use super::metadata_index::InMemoryMetadataIndex;
use super::object_index::InMemoryObjectIndex;
use super::store::CasStore;
use super::wal::FileWal;
use crate::error::CasError;

/// File-system backed CAS store.
///
/// Wraps [`CasStore`] with a [`FileWal`] for WAL persistence plus
/// in-memory payload indexing and constraint hints.
#[derive(Clone)]
pub struct FileSystemCas(pub(crate) CasStore<FileWal, InMemoryObjectIndex, InMemoryMetadataIndex>);

impl std::ops::Deref for FileSystemCas {
    type Target = CasStore<FileWal, InMemoryObjectIndex, InMemoryMetadataIndex>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl_cas_wrapper_traits!(FileSystemCas);

impl FileSystemCas {
    /// Open or create a file-system CAS store at `dir`.
    ///
    /// Creates the journal directory and checkpoint if they don't exist.
    pub async fn open(dir: &Path) -> Result<Self, CasError> {
        let wal = FileWal::create(dir.to_path_buf()).await?;
        let object_index = InMemoryObjectIndex::new();
        let metadata_index = InMemoryMetadataIndex::new();
        Ok(Self(CasStore::new(wal, object_index, metadata_index)))
    }
}
