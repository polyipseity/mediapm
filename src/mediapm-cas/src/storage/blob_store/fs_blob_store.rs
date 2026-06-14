//! Filesystem-based blob store implementation.
//!
//! Stores blobs in a hash-derived directory layout.
//! Atomic writes via write-to-temp, then rename.
//! Full blobs and delta blobs live at independent paths (`.diff` suffix).

use async_trait::async_trait;
use bytes::Bytes;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;

use super::BlobStore;
use super::versions::{hash_to_delta_path, hash_to_path};
use crate::api::{ObjectEncoding, VerifyTriggerStrategy};
use crate::error::CasError;
use crate::hash::Hash;

/// Filesystem-backed [`BlobStore`] with hash-derived directory layout.
///
/// ## Storage layout
///
/// ```text
/// <root>/
///   v1/
///     blake3/
///       ab/
///         cd/
///           <remaining>         # full blob
///           <remaining>.diff    # delta envelope
/// ```
///
/// ## Atomicity
///
/// Writes use a temporary file and [`std::fs::rename`] for crash-safe commits.
/// Reads verify the content hash matches the stored hash (integrity check).
///
/// ## Concurrency
///
/// All methods are safe for concurrent access. Directory creation uses
/// `create_dir_all` underneath.
#[derive(Clone, Debug)]
pub struct FileSystemBlobStore {
    root: PathBuf,
    verify_strategies: Vec<VerifyTriggerStrategy>,
}

impl FileSystemBlobStore {
    /// Create a new blob store rooted at `root`.
    ///
    /// The root directory is created if it does not exist.
    pub async fn create(
        root: PathBuf,
        verify_strategies: Vec<VerifyTriggerStrategy>,
    ) -> Result<Self, CasError> {
        fs::create_dir_all(&root).await.map_err(CasError::Io)?;
        Ok(Self { root, verify_strategies })
    }

    /// Convenience: create a store with no integrity verification.
    pub async fn new(root: PathBuf) -> Result<Self, CasError> {
        Self::create(root, Vec::new()).await
    }

    /// Returns `true` when at least one verify-on-read strategy is configured.
    fn should_verify(&self) -> bool {
        !self.verify_strategies.is_empty()
    }

    /// Return the root path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Ensure the parent directory for a hash exists.
    async fn ensure_parent(&self, hash: &Hash) -> Result<(), CasError> {
        let path = hash_to_path(&self.root, hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(CasError::Io)?;
        }
        Ok(())
    }

    /// Write bytes atomically to a path: temp file then rename.
    async fn atomic_write(path: &Path, data: &[u8]) -> Result<(), CasError> {
        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, data).await.map_err(CasError::Io)?;
        fs::rename(&tmp_path, path).await.map_err(CasError::Io)?;
        Ok(())
    }

    /// Verify that the content at `path` hashes to `expected`.
    async fn verify_hash(path: &Path, expected: &Hash) -> Result<(), CasError> {
        let data = fs::read(path).await.map_err(CasError::Io)?;
        let actual = Hash::from_content(&data);
        if actual != *expected {
            return Err(CasError::CorruptObject {
                hash: Some(*expected),
                details: format!(
                    "hash mismatch at {}: expected {expected}, found {actual}",
                    path.display()
                ),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl BlobStore for FileSystemBlobStore {
    const SYNC_MATERIALIZE: bool = false;

    async fn write(
        &self,
        hash: Hash,
        encoding: ObjectEncoding,
        data: Bytes,
    ) -> Result<(), CasError> {
        self.ensure_parent(&hash).await?;
        let path = match encoding {
            ObjectEncoding::Full => hash_to_path(&self.root, &hash),
            ObjectEncoding::Delta { .. } => hash_to_delta_path(&self.root, &hash),
        };
        Self::atomic_write(&path, &data).await?;
        Ok(())
    }

    async fn read(&self, hash: &Hash) -> Result<Bytes, CasError> {
        let path = hash_to_path(&self.root, hash);
        let data = fs::read(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        if self.should_verify() {
            Self::verify_hash(&path, hash).await?;
        }
        Ok(Bytes::from(data))
    }

    async fn read_delta(&self, hash: &Hash) -> Result<Bytes, CasError> {
        let path = hash_to_delta_path(&self.root, hash);
        let data = fs::read(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        if self.should_verify() {
            Self::verify_hash(&path, hash).await?;
        }
        Ok(Bytes::from(data))
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        let full_path = hash_to_path(&self.root, hash);
        let delta_path = hash_to_delta_path(&self.root, hash);

        // Silently ignore NotFound on each path; propagate other errors.
        match fs::remove_file(&full_path).await {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(CasError::Io(e)),
            _ => {}
        }
        match fs::remove_file(&delta_path).await {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(CasError::Io(e)),
            _ => {}
        }
        Ok(())
    }

    async fn exists(&self, hash: &Hash) -> Result<bool, CasError> {
        let full_path = hash_to_path(&self.root, hash);
        let delta_path = hash_to_delta_path(&self.root, hash);
        Ok(fs::try_exists(full_path).await.unwrap_or(false)
            || fs::try_exists(delta_path).await.unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Hash;

    #[tokio::test]
    async fn filesystem_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .unwrap();
        let data = Bytes::from_static(b"hello fs blob store");
        let hash = Hash::from_content(&data);

        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();
        let retrieved = store.read(&hash).await.unwrap();
        assert_eq!(retrieved, data);

        // Verify hash integrity on read.
        let corrupt_path = hash_to_path(dir.path(), &hash);
        fs::write(&corrupt_path, b"corrupted data").await.unwrap();
        let result = store.read(&hash).await;
        assert!(
            matches!(&result, Err(CasError::CorruptObject { hash: Some(h), .. }) if *h == hash),
            "expected CorruptObject for tampered blob, got {result:?}"
        );
    }

    #[tokio::test]
    async fn filesystem_read_missing_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .unwrap();
        let hash = Hash::from_content(b"missing");

        let result = store.read(&hash).await;
        assert!(matches!(result, Err(CasError::NotFound(h)) if h == hash));
    }

    #[tokio::test]
    async fn filesystem_delete_removes_blob() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .unwrap();
        let data = Bytes::from_static(b"delete me");
        let hash = Hash::from_content(&data);

        store.write(hash, ObjectEncoding::Full, data).await.unwrap();
        assert!(store.read(&hash).await.is_ok());
        store.delete(&hash).await.unwrap();
        assert!(store.read(&hash).await.is_err());
    }

    #[tokio::test]
    async fn filesystem_delta_path_works() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .unwrap();

        let base = Hash::from_content(b"base");
        let data = Bytes::from_static(b"delta envelope data");
        let hash = Hash::from_content(&data);

        // Write as delta.
        store.write(hash, ObjectEncoding::Delta { base_hash: base }, data.clone()).await.unwrap();
        let retrieved = store.read_delta(&hash).await.unwrap();
        assert_eq!(retrieved, data);

        // Full path should not exist.
        let result = store.read(&hash).await;
        assert!(result.is_err(), "full path should not exist for delta-only blob");
    }

    #[tokio::test]
    async fn filesystem_delete_removes_both_paths() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .unwrap();

        let base = Hash::from_content(b"base");
        let data = Bytes::from_static(b"dual blob");
        let hash = Hash::from_content(&data);

        // Write both full and delta.
        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();
        store.write(hash, ObjectEncoding::Delta { base_hash: base }, data).await.unwrap();

        assert!(store.exists(&hash).await.unwrap());
        store.delete(&hash).await.unwrap();
        assert!(!store.exists(&hash).await.unwrap());
    }
}
