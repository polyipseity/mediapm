//! Filesystem-based blob storage implementation.
//!
//! Stores blobs in a hash-derived directory layout.
//! Atomic writes via write-to-temp, then rename.
//! Full blobs and delta blobs live at independent paths (`.diff` suffix).

use async_trait::async_trait;
use bytes::Bytes;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
    /// # Errors
    ///
    /// Returns [`CasError::Io`] if the root directory cannot be created.
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
    ///
    /// # Errors
    ///
    /// Delegates to [`create`](Self::create).
    pub async fn new(root: PathBuf) -> Result<Self, CasError> {
        Self::create(root, Vec::new()).await
    }

    /// Returns `true` when the read-path should verify the content hash
    /// against the stored hash.
    ///
    /// Only [`VerifyTriggerStrategy::Always`] triggers inline verification;
    /// `Modified`, `Sample`, and `Stale` are not yet implemented and
    /// silently treated as off.
    #[must_use]
    fn should_verify(&self) -> bool {
        self.verify_strategies.iter().any(|s| matches!(s, VerifyTriggerStrategy::Always))
    }

    /// Return the root path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Buffer size for streaming I/O.
    fn stream_buffer_size() -> usize {
        crate::defaults::OBJECT_STREAM_BUFFER_SIZE as usize
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
    /// Uses incremental hashing to avoid loading the entire file.
    async fn verify_hash(path: &Path, expected: &Hash) -> Result<(), CasError> {
        let mut file = fs::File::open(path).await.map_err(CasError::Io)?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; Self::stream_buffer_size()];
        loop {
            let n = file.read(&mut buf).await.map_err(CasError::Io)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let actual = Hash::from_bytes(*hasher.finalize().as_bytes());
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

    async fn read_to_writer(
        &self,
        hash: &Hash,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), CasError> {
        let path = hash_to_path(&self.root, hash);
        let mut file = fs::File::open(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        if self.should_verify() {
            // Verify hash while streaming: compute hash incrementally
            let mut hasher = blake3::Hasher::new();
            let mut buf = vec![0u8; Self::stream_buffer_size()];
            loop {
                let n = file.read(&mut buf).await.map_err(CasError::Io)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                writer.write_all(&buf[..n]).await.map_err(CasError::Io)?;
            }
            let actual = Hash::from_bytes(*hasher.finalize().as_bytes());
            if actual != *hash {
                return Err(CasError::CorruptObject {
                    hash: Some(*hash),
                    details: format!(
                        "hash mismatch at {}: expected {hash}, found {actual}",
                        path.display()
                    ),
                });
            }
        } else {
            // No verification: simple copy
            tokio::io::copy_buf(&mut tokio::io::BufReader::new(&mut file), writer)
                .await
                .map_err(CasError::Io)?;
        }
        Ok(())
    }

    async fn write_from_reader(
        &self,
        hash: Hash,
        encoding: ObjectEncoding,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        _content_len: u64,
    ) -> Result<(), CasError> {
        self.ensure_parent(&hash).await?;
        let path = match encoding {
            ObjectEncoding::Full => hash_to_path(&self.root, &hash),
            ObjectEncoding::Delta { .. } => hash_to_delta_path(&self.root, &hash),
        };
        let tmp_path = path.with_extension("tmp");

        // Write to temp file in 64 KiB chunks while incrementally hashing
        let mut tmp_file = fs::File::create(&tmp_path).await.map_err(CasError::Io)?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; Self::stream_buffer_size()];
        loop {
            let n = reader.read(&mut buf).await.map_err(CasError::Io)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            tmp_file.write_all(&buf[..n]).await.map_err(CasError::Io)?;
        }
        tmp_file.flush().await.map_err(CasError::Io)?;
        tmp_file.sync_all().await.map_err(CasError::Io)?;
        drop(tmp_file);

        // Verify content hash
        let computed_hash = Hash::from_bytes(*hasher.finalize().as_bytes());
        if computed_hash != hash {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(CasError::CorruptObject {
                hash: Some(hash),
                details: format!(
                    "content hash mismatch writing {}: expected {hash}, computed {computed_hash}",
                    path.display()
                ),
            });
        }

        // Atomic rename
        fs::rename(&tmp_path, &path).await.map_err(CasError::Io)?;
        Ok(())
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

    async fn delete_encoding(&self, hash: Hash, encoding: ObjectEncoding) -> Result<(), CasError> {
        let path = match encoding {
            ObjectEncoding::Full => hash_to_path(&self.root, &hash),
            ObjectEncoding::Delta { .. } => hash_to_delta_path(&self.root, &hash),
        };
        match fs::remove_file(&path).await {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CasError::Io(e)),
            Ok(()) => Ok(()),
        }
    }

    async fn write_stream(
        &self,
        encoding: ObjectEncoding,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(Hash, u64), CasError> {
        // We don't know the hash yet — write to a temp file while hashing.
        // Use a dedicated temp dir to avoid partial-file collisions.
        static STREAM_COUNTER: AtomicU64 = AtomicU64::new(0);
        let tmp_dir = self.root.join(".tmp");
        fs::create_dir_all(&tmp_dir).await.map_err(CasError::Io)?;
        #[allow(clippy::cast_possible_truncation)]
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let tmp_name = format!("stream-{ts}-{}", STREAM_COUNTER.fetch_add(1, Ordering::Relaxed));
        let tmp_path = tmp_dir.join(tmp_name);

        let mut tmp_file = fs::File::create(&tmp_path).await.map_err(CasError::Io)?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; Self::stream_buffer_size()];
        let mut total: u64 = 0;
        loop {
            let n = reader.read(&mut buf).await.map_err(CasError::Io)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            tmp_file.write_all(&buf[..n]).await.map_err(CasError::Io)?;
            total += n as u64;
        }
        tmp_file.flush().await.map_err(CasError::Io)?;
        tmp_file.sync_all().await.map_err(CasError::Io)?;
        drop(tmp_file);

        let hash = Hash::from_bytes(*hasher.finalize().as_bytes());

        // Rename temp to final path.
        let final_path = match encoding {
            ObjectEncoding::Full => hash_to_path(&self.root, &hash),
            ObjectEncoding::Delta { .. } => hash_to_delta_path(&self.root, &hash),
        };
        self.ensure_parent(&hash).await?;
        fs::rename(&tmp_path, &final_path).await.map_err(CasError::Io)?;
        Ok((hash, total))
    }

    fn materialized_path(&self, hash: &Hash) -> Option<PathBuf> {
        Some(hash_to_path(&self.root, hash))
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

        // Both paths exist before delete.
        assert!(store.read(&hash).await.is_ok());
        assert!(store.read_delta(&hash).await.is_ok());
        store.delete(&hash).await.unwrap();
        // Both paths gone after delete.
        assert!(store.read(&hash).await.is_err());
        assert!(store.read_delta(&hash).await.is_err());
    }

    #[tokio::test]
    async fn filesystem_delete_encoding_removes_specific_encoding() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .unwrap();

        let base = Hash::from_content(b"base");
        let data = Bytes::from_static(b"dual encoding blob");
        let hash = Hash::from_content(&data);

        // Write both full and delta.
        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();
        store.write(hash, ObjectEncoding::Delta { base_hash: base }, data).await.unwrap();

        assert!(store.read(&hash).await.is_ok());
        assert!(store.read_delta(&hash).await.is_ok());

        // Remove only the delta encoding.
        store.delete_encoding(hash, ObjectEncoding::Delta { base_hash: base }).await.unwrap();

        // Full must still be present, delta must be gone.
        assert!(store.read(&hash).await.is_ok(), "full should remain after delta deletion");
        assert!(
            store.read_delta(&hash).await.is_err(),
            "delta should be removed after delete_encoding"
        );
    }

    #[tokio::test]
    async fn filesystem_delete_encoding_missing_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .unwrap();

        let hash = Hash::from_content(b"nonexistent");

        // Deleting encoding for a hash that was never written must succeed.
        store
            .delete_encoding(hash, ObjectEncoding::Full)
            .await
            .expect("delete_encoding on missing hash must succeed");
        store
            .delete_encoding(hash, ObjectEncoding::Delta { base_hash: Hash::from_content(b"x") })
            .await
            .expect("delete_encoding on missing delta must succeed");
    }
}
