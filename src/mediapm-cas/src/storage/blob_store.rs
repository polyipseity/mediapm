//! Blob store — eventual-consistent content-addressed blob storage.
//!
//! Defines the [`BlobStore`] trait and its implementations:
//! - [`FileSystemBlobStore`] — hash-derived directory layout on disk.
//! - [`InMemoryBlobStore`] — ephemeral in-memory map (testing/CI).
//!
//! # Path layout
//!
//! Full blobs: `<root>/v1/blake3/ab/cd/<remaining>`
//! Delta blobs: `<root>/v1/blake3/ab/cd/<remaining>.diff`
//!
//! The two-character prefix directories provide fan-out to avoid
//! single-directory bottlenecks on file systems with directory-entry limits.

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

// ---------------------------------------------------------------------------
// BlobStore trait
// ---------------------------------------------------------------------------

/// Content-addressed blob storage.
///
/// Provides read/write/delete for data indexed by content hash.
/// Full and delta-encoded blobs are stored at different paths
/// (`.diff` suffix for delta) so both can coexist safely during
/// optimizer transitions.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Write data for `hash` with the given encoding.
    ///
    /// - [`ObjectEncoding::Full`]: stored at `<hash-path>`.
    /// - [`ObjectEncoding::Delta`]: stored at `<hash-path>.diff`.
    ///
    /// Atomic via temp-file + rename (for file-system impls).
    async fn write(
        &self,
        hash: Hash,
        encoding: ObjectEncoding,
        data: Bytes,
    ) -> Result<(), CasError>;

    /// Read full blob for `hash`.
    ///
    /// Returns [`CasError::NotFound`] if no full blob exists.
    async fn read(&self, hash: &Hash) -> Result<Bytes, CasError>;

    /// Read delta-encoded blob for `hash`.
    ///
    /// Returns [`CasError::NotFound`] if no delta blob exists.
    async fn read_delta(&self, hash: &Hash) -> Result<Bytes, CasError>;

    /// Delete all blobs for `hash` (both full and delta).
    async fn delete(&self, hash: &Hash) -> Result<(), CasError>;

    /// Check whether any blob exists for `hash`.
    async fn exists(&self, hash: &Hash) -> Result<bool, CasError>;
}

// ---------------------------------------------------------------------------
// Hash-to-path helpers
// ---------------------------------------------------------------------------

/// Derive the full-blob path for a hash.
pub(crate) fn hash_to_path(root: &Path, hash: &Hash) -> PathBuf {
    let hex = hash.to_hex();
    root.join("v1").join("blake3").join(&hex[0..2]).join(&hex[2..4]).join(&hex[4..])
}

/// Derive the delta-blob path for a hash (`.diff` suffix).
fn hash_to_delta_path(root: &Path, hash: &Hash) -> PathBuf {
    let mut path = hash_to_path(root, hash);
    let ext = path
        .extension()
        .map(|e| {
            let mut s = e.to_os_string();
            s.push(".diff");
            s
        })
        .unwrap_or_else(|| "diff".into());
    path.set_extension(ext);
    path
}

// ---------------------------------------------------------------------------
// FileSystemBlobStore
// ---------------------------------------------------------------------------

/// Blob store backed by the local file system.
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
#[derive(Clone, Debug)]
pub struct FileSystemBlobStore {
    root: PathBuf,
    verify_on_read: bool,
}

impl FileSystemBlobStore {
    /// Create a new blob store rooted at `root`.
    ///
    /// The root directory is created if it does not exist.
    pub async fn create(root: PathBuf, verify_on_read: bool) -> Result<Self, CasError> {
        tokio::fs::create_dir_all(&root).await.map_err(CasError::Io)?;
        Ok(Self { root, verify_on_read })
    }

    /// Return the root path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Ensure the parent directory for a hash exists.
    async fn ensure_parent(&self, hash: &Hash) -> Result<(), CasError> {
        let path = hash_to_path(&self.root, hash);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(CasError::Io)?;
        }
        Ok(())
    }

    /// Write bytes atomically to a path: temp file then rename.
    async fn atomic_write(path: &Path, data: &[u8]) -> Result<(), CasError> {
        let tmp_path = path.with_extension("tmp");
        tokio::fs::write(&tmp_path, data).await.map_err(CasError::Io)?;
        tokio::fs::rename(&tmp_path, path).await.map_err(CasError::Io)?;
        Ok(())
    }

    /// Verify that the content at `path` hashes to `expected`.
    async fn verify_hash(path: &Path, expected: &Hash) -> Result<(), CasError> {
        let data = tokio::fs::read(path).await.map_err(CasError::Io)?;
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
        let data = tokio::fs::read(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        if self.verify_on_read {
            Self::verify_hash(&path, hash).await?;
        }
        Ok(Bytes::from(data))
    }

    async fn read_delta(&self, hash: &Hash) -> Result<Bytes, CasError> {
        let path = hash_to_delta_path(&self.root, hash);
        let data = tokio::fs::read(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        if self.verify_on_read {
            Self::verify_hash(&path, hash).await?;
        }
        Ok(Bytes::from(data))
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        let full_path = hash_to_path(&self.root, hash);
        let delta_path = hash_to_delta_path(&self.root, hash);
        let _ = tokio::fs::remove_file(&full_path).await;
        let _ = tokio::fs::remove_file(&delta_path).await;
        Ok(())
    }

    async fn exists(&self, hash: &Hash) -> Result<bool, CasError> {
        let full_path = hash_to_path(&self.root, hash);
        let delta_path = hash_to_delta_path(&self.root, hash);
        Ok(tokio::fs::try_exists(full_path).await.unwrap_or(false)
            || tokio::fs::try_exists(delta_path).await.unwrap_or(false))
    }
}

// ---------------------------------------------------------------------------
// InMemoryBlobStore
// ---------------------------------------------------------------------------

/// Ephemeral in-memory blob store backed by `DashMap`.
///
/// All data lives in memory. Suitable for testing and ephemeral CAS usage.
/// Ignores the `.diff` path distinction — `read` and `read_delta` both
/// access the same underlying map.
#[derive(Clone, Default)]
pub struct InMemoryBlobStore {
    data: Arc<DashMap<Hash, (Bytes, ObjectEncoding)>>,
}

impl InMemoryBlobStore {
    /// Create an empty blob store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl BlobStore for InMemoryBlobStore {
    async fn write(
        &self,
        hash: Hash,
        _encoding: ObjectEncoding,
        data: Bytes,
    ) -> Result<(), CasError> {
        // In-memory store ignores .diff distinction — encoding is tracked in Index.
        self.data.insert(hash, (data, _encoding));
        Ok(())
    }

    async fn read(&self, hash: &Hash) -> Result<Bytes, CasError> {
        self.data.get(hash).map(|r| r.value().0.clone()).ok_or(CasError::NotFound(*hash))
    }

    async fn read_delta(&self, hash: &Hash) -> Result<Bytes, CasError> {
        // In-memory store: same as read (no separate .diff path).
        self.read(hash).await
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        self.data.remove(hash);
        Ok(())
    }

    async fn exists(&self, hash: &Hash) -> Result<bool, CasError> {
        Ok(self.data.contains_key(hash))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Hash;

    #[tokio::test]
    async fn in_memory_write_read_roundtrip() {
        let store = InMemoryBlobStore::new();
        let data = Bytes::from_static(b"hello blob store");
        let hash = Hash::from_content(&data);

        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();
        let retrieved = store.read(&hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn in_memory_read_missing_returns_not_found() {
        let store = InMemoryBlobStore::new();
        let hash = Hash::from_content(b"missing");
        let result = store.read(&hash).await;
        assert!(matches!(result, Err(CasError::NotFound(h)) if h == hash));
    }

    #[tokio::test]
    async fn in_memory_delete_removes_blob() {
        let store = InMemoryBlobStore::new();
        let data = Bytes::from_static(b"ephemeral");
        let hash = Hash::from_content(&data);

        store.write(hash, ObjectEncoding::Full, data).await.unwrap();
        assert!(store.read(&hash).await.is_ok());
        store.delete(&hash).await.unwrap();
        assert!(store.read(&hash).await.is_err());
    }

    #[tokio::test]
    async fn in_memory_exists_works() {
        let store = InMemoryBlobStore::new();
        let data = Bytes::from_static(b"exists check");
        let hash = Hash::from_content(&data);

        assert!(!store.exists(&hash).await.unwrap());
        store.write(hash, ObjectEncoding::Full, data).await.unwrap();
        assert!(store.exists(&hash).await.unwrap());
    }

    #[tokio::test]
    async fn hash_path_derivation_is_deterministic() {
        let root = Path::new("/tmp/cas");
        let hash = Hash::from_content(b"test data");
        let hex = hash.to_hex();

        let path = hash_to_path(root, &hash);
        assert_eq!(
            path,
            root.join("v1").join("blake3").join(&hex[0..2]).join(&hex[2..4]).join(&hex[4..])
        );
    }

    #[tokio::test]
    async fn hash_delta_path_ends_with_diff() {
        let root = Path::new("/tmp/cas");
        let hash = Hash::from_content(b"test data");

        let path = hash_to_delta_path(root, &hash);
        assert!(path.to_string_lossy().ends_with(".diff"));
    }

    #[tokio::test]
    async fn file_system_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(dir.path().to_path_buf(), true).await.unwrap();

        let data = Bytes::from_static(b"hello fs blob store");
        let hash = Hash::from_content(&data);

        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();
        let retrieved = store.read(&hash).await.unwrap();
        assert_eq!(retrieved, data);

        // Verify hash integrity on read.
        let corrupt_path = hash_to_path(dir.path(), &hash);
        tokio::fs::write(&corrupt_path, b"corrupted data").await.unwrap();
        let result = store.read(&hash).await;
        assert!(
            matches!(&result, Err(CasError::CorruptObject { hash: Some(h), .. }) if *h == hash),
            "expected CorruptObject for tampered blob, got {result:?}"
        );
    }

    #[tokio::test]
    async fn file_system_delta_path_works() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(dir.path().to_path_buf(), false).await.unwrap();

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
    async fn file_system_delete_removes_both_paths() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(dir.path().to_path_buf(), false).await.unwrap();

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
