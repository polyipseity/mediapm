//! Filesystem-based blob storage implementation.
//!
//! Stores blobs in a hash-derived directory layout.
//! Atomic writes via write-to-temp, then rename.
//! Full blobs and delta blobs live at independent paths (`.diff` suffix).
//!
//! An object file is immutable once it exists: a path derived from the content
//! hash can only ever hold those bytes, and the hierarchy materializer hardlinks
//! outputs straight from these paths, so replacing the inode would detach every
//! link already made from it. The store enforces that on disk as well as by
//! convention: every committed file carries a read-only mode, a staged file has
//! that attribute cleared before it is written, and — on Windows — every write
//! path clears the attribute of a file it is about to replace or remove.
//!
//! POSIX needs no file-level clear: unlinking or renaming a child entry requires
//! write permission on the containing directory, not on the file. Windows
//! refuses both on a read-only file, so that platform clears it first, and
//! `clear_read_only` is a documented no-op everywhere else. Clearing the bit on
//! POSIX would also reach every hardlink of the same inode, which is how a
//! delete through the store could make a materialized library file writable.

use async_trait::async_trait;
use bytes::Bytes;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::BlobStore;
use super::versions::{CURRENT_BLOB_PATH_VERSION, hash_to_delta_path, hash_to_path};
use crate::api::{ObjectEncoding, VerifyTriggerStrategy};
use crate::error::CasError;
use crate::hash::Hash;
use crate::io_gate::CasIoGate;
use crate::verify::VerifyEvaluator;

/// Returns `permissions` with every write bit cleared.
///
/// This is the mode a committed object carries. Clearing all three write bits
/// rather than only the owner's leaves a blob that no user can rewrite without
/// first clearing the attribute, which is exactly what an immutable object is.
fn without_write_bits(mut permissions: std::fs::Permissions) -> std::fs::Permissions {
    permissions.set_readonly(true);
    permissions
}

/// Returns `permissions` with the owner's write bit set.
///
/// The attribute has to be cleared before an object is replaced or removed:
/// Windows refuses both on a read-only file. Only the owner's bit is granted,
/// so the mode a writable store file comes back with stays as restrictive as
/// the umask made it.
#[cfg(unix)]
fn with_owner_write(mut permissions: std::fs::Permissions) -> std::fs::Permissions {
    use std::os::unix::fs::PermissionsExt;

    permissions.set_mode(permissions.mode() | 0o200);
    permissions
}

/// Windows counterpart of the unix `with_owner_write`.
///
/// The read-only bit is a file attribute there rather than a mode bit, so
/// clearing it is the whole operation.
#[cfg(windows)]
fn with_owner_write(mut permissions: std::fs::Permissions) -> std::fs::Permissions {
    permissions.set_readonly(false);
    permissions
}

/// Filesystem-backed [`BlobStore`] with hash-derived directory layout.
///
/// Blobs live at `<root>/v1/blake3/ab/cd/<remaining>` (full) or with a
/// `.diff` suffix (delta envelope). Writes use a temp file + rename for
/// crash-safe commits; reads verify the content hash matches the stored
/// hash. All methods are safe for concurrent access.
///
/// Every step that can create a directory entry runs under a lease from the
/// store's internal mutation gate, so closing that gate quiesces the tree. See
/// the crate's `io_gate` module for the contract and why `Drop` can rely on
/// it.
#[derive(Clone, Debug)]
pub struct FileSystemBlobStore {
    root: PathBuf,
    verify_evaluator: VerifyEvaluator,
    /// Store-lifetime gate. Clones share it, so closing it through any handle
    /// covers the whole store.
    io_gate: Arc<CasIoGate>,
}

impl FileSystemBlobStore {
    /// Create a new blob store rooted at `root`, with its own mutation gate.
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
        Self::create_gated(root, verify_strategies, Arc::new(CasIoGate::new())).await
    }

    /// Create a new blob store rooted at `root`, sharing `io_gate` with the
    /// rest of the owning store.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::Io`] if the root directory cannot be created.
    pub(crate) async fn create_gated(
        root: PathBuf,
        verify_strategies: Vec<VerifyTriggerStrategy>,
        io_gate: Arc<CasIoGate>,
    ) -> Result<Self, CasError> {
        fs::create_dir_all(&root).await.map_err(CasError::Io)?;
        Ok(Self { root, verify_evaluator: VerifyEvaluator::new(&verify_strategies), io_gate })
    }

    /// Convenience: create a store with no integrity verification.
    ///
    /// # Errors
    ///
    /// Delegates to [`create`](Self::create).
    pub async fn new(root: PathBuf) -> Result<Self, CasError> {
        Self::create(root, Vec::new()).await
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
    ///
    /// Leased, because `create_dir_all` is exactly the step that resurrects a
    /// store directory after its owner removed it.
    async fn ensure_parent(&self, hash: &Hash) -> Result<(), CasError> {
        let path = hash_to_path(&self.root, hash);
        let Some(parent) = path.parent().map(Path::to_path_buf) else {
            return Ok(());
        };
        self.io_gate.run(move || std::fs::create_dir_all(&parent)).await.map_err(CasError::Io)
    }

    /// Write bytes atomically to a path: temp file then rename.
    ///
    /// Staging and the rename share one lease so the pair cannot be split by a
    /// gate close: a refused staging write never reaches the rename.
    ///
    /// The staged file is made read-only before it is renamed, so a committed
    /// object is never observable as writable, and the file the rename replaces
    /// has its attribute cleared first on the platform that requires it.
    async fn atomic_write(&self, path: &Path, data: Bytes) -> Result<(), CasError> {
        let tmp_path = path.with_extension("tmp");
        let final_path = path.to_path_buf();
        self.io_gate
            .run(move || {
                Self::clear_staging_read_only(&tmp_path)?;
                std::fs::write(&tmp_path, &data)?;
                let staged = std::fs::metadata(&tmp_path)?.permissions();
                std::fs::set_permissions(&tmp_path, without_write_bits(staged))?;
                match std::fs::metadata(&final_path) {
                    Ok(replaced) => {
                        std::fs::set_permissions(
                            &final_path,
                            with_owner_write(replaced.permissions()),
                        )?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
                std::fs::rename(&tmp_path, &final_path)
            })
            .await
            .map_err(CasError::Io)
    }

    /// Ensures an existing object carries the read-only mode.
    ///
    /// Used by the skip path: an object written by a build that did not set the
    /// attribute is writable, and rewriting its bytes would detach the hardlinks
    /// made from it, so the permission is the only thing that changes.
    async fn enforce_read_only(path: &Path) -> Result<(), CasError> {
        let metadata = fs::metadata(path).await.map_err(CasError::Io)?;
        if metadata.permissions().readonly() {
            return Ok(());
        }
        fs::set_permissions(path, without_write_bits(metadata.permissions()))
            .await
            .map_err(CasError::Io)
    }

    /// Clears the read-only attribute of a staging file before writing it.
    ///
    /// A commit stages under a fixed name, so a write that failed part-way — or
    /// a process that died between making the staged file read-only and renaming
    /// it — leaves a read-only file at that name. Opening it for write truncates
    /// and fails with `EACCES` on POSIX and with a sharing violation on Windows,
    /// which would wedge every later commit for the same hash behind an error
    /// that names nothing useful. Clearing the attribute first is what makes the
    /// fixed staging name reusable.
    ///
    /// Unlike [`Self::clear_read_only`], this runs on every platform: the file is
    /// this operation's own scratch, not a committed object, so it has no
    /// hardlink whose mode a clear could disturb.
    fn clear_staging_read_only(path: &Path) -> io::Result<()> {
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if !metadata.permissions().readonly() {
            return Ok(());
        }
        std::fs::set_permissions(path, with_owner_write(metadata.permissions()))
    }

    /// Clears the read-only attribute so `path` can be replaced or removed.
    ///
    /// A missing file is success: the caller is about to unlink it, and the
    /// unlink reports its own `NotFound` handling. A file that is already
    /// writable is left untouched, so the common path costs one `stat`.
    ///
    /// Windows clears the attribute because it refuses to replace or unlink a
    /// read-only file. POSIX needs only write permission on the containing
    /// directory, so the clear is a no-op there, and that is what keeps a delete
    /// through the store from reaching the mode of a hardlinked library file,
    /// which shares the inode.
    ///
    /// The operation is one `stat` and at most one `set_permissions`, so it runs
    /// inline rather than on the blocking pool: the callers are either already
    /// inside a lease closure or about to issue an await of their own.
    ///
    /// The signature is fallible on every platform because the Windows body is,
    /// and a platform-conditional return type would push the difference into
    /// seven call sites.
    #[cfg_attr(
        not(windows),
        expect(
            clippy::unnecessary_wraps,
            reason = "the fallible signature is shared with the Windows body, where the attribute clear can fail"
        )
    )]
    fn clear_read_only(path: &Path) -> Result<(), CasError> {
        #[cfg(windows)]
        {
            let metadata = match std::fs::metadata(path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(CasError::Io(error)),
            };
            if !metadata.permissions().readonly() {
                return Ok(());
            }
            std::fs::set_permissions(path, with_owner_write(metadata.permissions()))
                .map_err(CasError::Io)
        }

        #[cfg(not(windows))]
        {
            let _ = path;
            Ok(())
        }
    }

    /// Derive the auxiliary-file path for `hash` with the given `name`.
    ///
    /// Auxiliary files live in the hash's fan-out parent directory so they
    /// share the same 2‑level prefix layout as object blobs but without
    /// the trailing hex leaf:
    /// `<root>/<current-layout>/blake3/ab/cd/<name>`, where the layout segment
    /// is [`CURRENT_BLOB_PATH_VERSION`] — a literal here would be a third
    /// statement of which layout this store uses.
    fn aux_path(&self, hash: &Hash, name: &str) -> PathBuf {
        let blob_path = hash_to_path(&self.root, hash);
        // parent is `root/v1/blake3/ab/cd/`
        blob_path.parent().unwrap_or(&self.root).join(name)
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
        // A full-encoding object is immutable once it exists: the path is
        // derived from the hash, so an object already sitting there holds
        // exactly these bytes. Rewriting it would not repair anything — the
        // write-tmp-then-rename commit installs a NEW inode, silently
        // detaching every hardlink already materialized from the old one
        // (the hierarchy materializer hardlinks outputs straight from this
        // path). The WAL consumer re-applies Put entries that a materializer
        // may already have materialized, so an unguarded rewrite here is what
        // breaks those links.
        //
        // Delta envelopes are deliberately excluded: they are a derived
        // encoding the optimizer may legitimately re-encode against a
        // different base, and nothing hardlinks them.
        let already_materialized = matches!(encoding, ObjectEncoding::Full)
            && fs::metadata(&path).await.is_ok_and(|meta| meta.is_file());
        if already_materialized {
            // An object written by a build that did not set the attribute is
            // writable. Its bytes are already correct, so only the mode
            // changes: rewriting them would detach the hardlinks made from it.
            Self::enforce_read_only(&path).await?;
        } else {
            self.atomic_write(&path, data.clone()).await?;
        }
        // Seed mtime for verify-on-read tracking. Runs on the skip path too:
        // the object is present and its mtime is what the verify-on-read
        // strategies compare against, and leaving it unrecorded would make a
        // never-rewritten object look unverified and trigger a re-verify on
        // every read.
        if let Ok(meta) = fs::metadata(&path).await {
            self.verify_evaluator.record_verification(&hash, meta.modified().ok());
        }
        Ok(())
    }

    async fn read(&self, hash: &Hash) -> Result<Bytes, CasError> {
        let path = hash_to_path(&self.root, hash);
        let meta = fs::metadata(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        let mtime = meta.modified().ok();
        let data = fs::read(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        if self.verify_evaluator.should_verify(hash, mtime) {
            Self::verify_hash(&path, hash).await?;
            self.verify_evaluator.record_verification(hash, mtime);
        }
        Ok(Bytes::from(data))
    }

    async fn read_to_writer(
        &self,
        hash: &Hash,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), CasError> {
        let path = hash_to_path(&self.root, hash);
        let meta = fs::metadata(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        let mtime = meta.modified().ok();
        let mut file = fs::File::open(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        if self.verify_evaluator.should_verify(hash, mtime) {
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
            self.verify_evaluator.record_verification(hash, mtime);
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

        // Write to temp file in 64 KiB chunks while incrementally hashing.
        // Only creating the file needs a lease: the writes below go through
        // this open handle, so they cannot recreate a removed tree. The staging
        // name is fixed, so a leftover from an interrupted commit is cleared
        // first or the create would fail on its read-only attribute.
        let tmp_path_for_create = tmp_path.clone();
        let created = self
            .io_gate
            .run(move || {
                Self::clear_staging_read_only(&tmp_path_for_create)?;
                std::fs::File::create(&tmp_path_for_create)
            })
            .await
            .map_err(CasError::Io)?;
        let mut tmp_file = fs::File::from_std(created);
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
            Self::clear_read_only(&tmp_path)?;
            let _ = fs::remove_file(&tmp_path).await;
            return Err(CasError::CorruptObject {
                hash: Some(hash),
                details: format!(
                    "content hash mismatch writing {}: expected {hash}, computed {computed_hash}",
                    path.display()
                ),
            });
        }

        // Atomic rename, after the staged file takes the read-only mode a
        // committed object carries and any file it replaces gives up its own.
        // Neither step creates a directory entry, so neither needs a lease.
        let staged = fs::metadata(&tmp_path).await.map_err(CasError::Io)?.permissions();
        fs::set_permissions(&tmp_path, without_write_bits(staged)).await.map_err(CasError::Io)?;
        Self::clear_read_only(&path)?;
        fs::rename(&tmp_path, &path).await.map_err(CasError::Io)?;
        // Seed mtime for verify-on-read tracking.
        if let Ok(meta) = fs::metadata(&path).await {
            self.verify_evaluator.record_verification(&hash, meta.modified().ok());
        }
        Ok(())
    }

    async fn read_delta(&self, hash: &Hash) -> Result<Bytes, CasError> {
        let path = hash_to_delta_path(&self.root, hash);
        let meta = fs::metadata(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        let mtime = meta.modified().ok();
        let data = fs::read(&path).await.map_err(|_| CasError::NotFound(*hash))?;
        if self.verify_evaluator.should_verify(hash, mtime) {
            Self::verify_hash(&path, hash).await?;
            self.verify_evaluator.record_verification(hash, mtime);
        }
        Ok(Bytes::from(data))
    }

    async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
        let full_path = hash_to_path(&self.root, hash);
        let delta_path = hash_to_delta_path(&self.root, hash);

        // Silently ignore NotFound on each path; propagate other errors.
        Self::clear_read_only(&full_path)?;
        match fs::remove_file(&full_path).await {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(CasError::Io(e)),
            _ => {}
        }
        Self::clear_read_only(&delta_path)?;
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
        Self::clear_read_only(&path)?;
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
        let tmp_dir_for_create = tmp_dir.clone();
        self.io_gate
            .run(move || std::fs::create_dir_all(&tmp_dir_for_create))
            .await
            .map_err(CasError::Io)?;
        let ts = mediapm_utils::Timestamp::now().as_unix_nanos();
        let tmp_name = format!("stream-{ts}-{}", STREAM_COUNTER.fetch_add(1, Ordering::Relaxed));
        let tmp_path = tmp_dir.join(tmp_name);

        let created = self
            .io_gate
            .run({
                let tmp_path = tmp_path.clone();
                move || std::fs::File::create(&tmp_path)
            })
            .await
            .map_err(CasError::Io)?;
        let mut tmp_file = fs::File::from_std(created);
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

        // Rename temp to final path, after the staged file takes the read-only
        // mode a committed object carries and any existing encoding gives up
        // its own so the rename can replace it on Windows.
        let final_path = match encoding {
            ObjectEncoding::Full => hash_to_path(&self.root, &hash),
            ObjectEncoding::Delta { .. } => hash_to_delta_path(&self.root, &hash),
        };
        self.ensure_parent(&hash).await?;
        let staged = fs::metadata(&tmp_path).await.map_err(CasError::Io)?.permissions();
        fs::set_permissions(&tmp_path, without_write_bits(staged)).await.map_err(CasError::Io)?;
        Self::clear_read_only(&final_path)?;
        fs::rename(&tmp_path, &final_path).await.map_err(CasError::Io)?;
        // Seed mtime for verify-on-read tracking.
        if let Ok(meta) = fs::metadata(&final_path).await {
            self.verify_evaluator.record_verification(&hash, meta.modified().ok());
        }
        Ok((hash, total))
    }

    fn materialized_path(&self, hash: &Hash) -> Option<PathBuf> {
        Some(hash_to_path(&self.root, hash))
    }

    async fn write_aux(&self, hash: &Hash, name: &str, data: Bytes) -> Result<(), CasError> {
        if name.contains('/') || name.contains("..") {
            return Err(CasError::InvalidArgument(format!(
                "aux name must not contain '/' or '..': {name:?}"
            )));
        }
        let path = self.aux_path(hash, name);
        if let Some(parent) = path.parent().map(Path::to_path_buf) {
            self.io_gate
                .run(move || std::fs::create_dir_all(&parent))
                .await
                .map_err(CasError::Io)?;
        }
        self.atomic_write(&path, data).await
    }

    async fn read_aux(&self, hash: &Hash, name: &str) -> Result<Bytes, CasError> {
        if name.contains('/') || name.contains("..") {
            return Err(CasError::InvalidArgument(format!(
                "aux name must not contain '/' or '..': {name:?}"
            )));
        }
        let path = self.aux_path(hash, name);
        fs::read(&path).await.map(Bytes::from).map_err(|_| CasError::NotFound(*hash))
    }

    async fn delete_aux(&self, hash: &Hash, name: &str) -> Result<(), CasError> {
        if name.contains('/') || name.contains("..") {
            return Err(CasError::InvalidArgument(format!(
                "aux name must not contain '/' or '..': {name:?}"
            )));
        }
        let path = self.aux_path(hash, name);
        Self::clear_read_only(&path)?;
        match fs::remove_file(&path).await {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CasError::Io(e)),
            Ok(()) => Ok(()),
        }
    }

    async fn all_aux(&self, name: &str) -> Result<Vec<Bytes>, CasError> {
        let fanout_root = self.root.join(CURRENT_BLOB_PATH_VERSION).join("blake3");
        let mut results = Vec::new();

        let mut level1 = match fs::read_dir(&fanout_root).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(CasError::Io(e)),
        };
        while let Some(l1_entry) = level1.next_entry().await.map_err(CasError::Io)? {
            if !l1_entry.file_type().await.map_err(CasError::Io)?.is_dir() {
                continue;
            }
            let mut level2 = fs::read_dir(l1_entry.path()).await.map_err(CasError::Io)?;
            while let Some(l2_entry) = level2.next_entry().await.map_err(CasError::Io)? {
                if !l2_entry.file_type().await.map_err(CasError::Io)?.is_dir() {
                    continue;
                }
                let aux_path = l2_entry.path().join(name);
                match fs::read(&aux_path).await {
                    Ok(data) => results.push(Bytes::from(data)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(CasError::Io(e)),
                }
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Hash;

    #[tokio::test]
    async fn filesystem_write_read_roundtrip() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
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
        tamper(&corrupt_path);
        fs::write(&corrupt_path, b"corrupted data").await.unwrap();
        let result = store.read(&hash).await;
        assert!(
            matches!(&result, Err(CasError::CorruptObject { hash: Some(h), .. }) if *h == hash),
            "expected CorruptObject for tampered blob, got {result:?}"
        );
    }

    #[tokio::test]
    async fn filesystem_read_missing_returns_not_found() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
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
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
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
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
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
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
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
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
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
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
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

    // --- VerifyTriggerStrategy integration tests ---

    #[tokio::test]
    async fn modified_skips_second_read_on_unchanged_mtime() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Modified],
        )
        .await
        .unwrap();

        let data = Bytes::from_static(b"stable content");
        let hash = Hash::from_content(&data);
        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();

        // First read: hash unknown to evaluator, must verify.
        let retrieved = store.read(&hash).await.unwrap();
        assert_eq!(retrieved, data);

        // Second read: mtime unchanged, should skip verification.
        let retrieved = store.read(&hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn modified_catches_tampered_blob() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Modified],
        )
        .await
        .unwrap();

        let original = Bytes::from_static(b"original content");
        let hash = Hash::from_content(&original);
        store.write(hash, ObjectEncoding::Full, original).await.unwrap();

        // Overwrite blob file with different content.
        let path = hash_to_path(dir.path(), &hash);
        // Ensure clocks tick so mtime is distinct.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        tamper(&path);
        fs::write(&path, b"corrupted content").await.unwrap();

        // Read should detect corruption.
        let err = store.read(&hash).await.unwrap_err();
        assert!(
            matches!(err, CasError::CorruptObject { .. }),
            "expected CorruptObject, got {err:?}",
        );
    }

    #[tokio::test]
    async fn sample_triggers_at_denominator_interval() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Sample { denominator: 3 }],
        )
        .await
        .unwrap();

        let data = Bytes::from_static(b"sample test");
        let hash = Hash::from_content(&data);
        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();

        // Read 1: counter 0 → verify. Record happens inside read.
        assert_eq!(store.read(&hash).await.unwrap(), data);
        // Read 2: counter 1 → skip.
        assert_eq!(store.read(&hash).await.unwrap(), data);
        // Read 3: counter 2 → skip.
        assert_eq!(store.read(&hash).await.unwrap(), data);
        // Read 4: counter 3 → verify (3 % 3 == 0).
        assert_eq!(store.read(&hash).await.unwrap(), data);
    }

    #[tokio::test]
    async fn stale_retriggers_after_timeout_expiry() {
        // Use a zero-length timeout so the first read records, second triggers.
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Stale { timeout: std::time::Duration::ZERO }],
        )
        .await
        .unwrap();

        let data = Bytes::from_static(b"stale test");
        let hash = Hash::from_content(&data);
        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();

        // Read 1: hash unknown → verify + record.
        assert_eq!(store.read(&hash).await.unwrap(), data);
        // Read 2: elapsed >= 0 → stale immediately.
        assert_eq!(store.read(&hash).await.unwrap(), data);
    }

    /// Makes a committed object writable so a test can tamper with it.
    ///
    /// Every file the store commits is read-only, so a test that writes into
    /// one has to clear the attribute first — exactly what a replacement write
    /// does before its rename.
    fn tamper(path: &Path) {
        let permissions = std::fs::metadata(path).expect("object metadata").permissions();
        std::fs::set_permissions(path, with_owner_write(permissions))
            .expect("make object writable");
    }

    /// Reports whether a committed file carries no write bit.
    fn is_read_only(path: &Path) -> bool {
        std::fs::metadata(path).expect("object metadata").permissions().readonly()
    }

    /// Reports whether two paths resolve to the same filesystem object.
    ///
    /// Hard-link identity is the CAS materialization contract, so the check
    /// must be identity-based: comparing contents would not notice an object
    /// that was replaced by a byte-identical file, which is exactly the
    /// failure this guards.
    #[cfg(unix)]
    fn is_same_file(left: &Path, right: &Path) -> bool {
        use std::os::unix::fs::MetadataExt;

        match (std::fs::metadata(left), std::fs::metadata(right)) {
            (Ok(left_meta), Ok(right_meta)) => {
                left_meta.dev() == right_meta.dev() && left_meta.ino() == right_meta.ino()
            }
            _ => false,
        }
    }

    /// Windows counterpart of the unix [`is_same_file`], using the volume
    /// serial number plus file index as the object identity.
    ///
    /// The identity is read through `GetFileInformationByHandle` via FFI rather
    /// than through `MetadataExt::volume_serial_number`/`file_index`, because
    /// those methods are still behind the unstable `windows_by_handle` feature
    /// on stable Rust. FFI keeps this callable on every toolchain.
    #[cfg(windows)]
    fn is_same_file(left: &Path, right: &Path) -> bool {
        use std::os::windows::io::AsRawHandle;

        #[repr(C)]
        struct ByHandleFileInformation {
            file_attributes: u32,
            creation_time: u64,
            last_access_time: u64,
            last_write_time: u64,
            volume_serial_number: u32,
            file_size_high: u32,
            file_size_low: u32,
            number_of_links: u32,
            file_index_high: u32,
            file_index_low: u32,
        }

        unsafe extern "system" {
            fn GetFileInformationByHandle(
                h_file: *mut std::ffi::c_void,
                info: *mut ByHandleFileInformation,
            ) -> i32;
        }

        fn file_identity(path: &Path) -> Option<(u32, u64)> {
            let file = std::fs::File::open(path).ok()?;
            let mut info = ByHandleFileInformation {
                file_attributes: 0,
                creation_time: 0,
                last_access_time: 0,
                last_write_time: 0,
                volume_serial_number: 0,
                file_size_high: 0,
                file_size_low: 0,
                number_of_links: 0,
                file_index_high: 0,
                file_index_low: 0,
            };
            let ok =
                unsafe { GetFileInformationByHandle(file.as_raw_handle() as *mut _, &mut info) };
            if ok == 0 {
                return None;
            }
            let file_index = ((info.file_index_high as u64) << 32) | (info.file_index_low as u64);
            Some((info.volume_serial_number, file_index))
        }

        match (file_identity(left), file_identity(right)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }

    /// Regression test for the intermittent
    /// `generate_demo_artifacts_writes_manifest_and_import_metadata` failure
    /// ("materialized output is not hardlinked to CAS object"). Re-writing a
    /// full-encoding object that already exists used to commit through
    /// write-tmp-then-rename, which installs a new inode and silently detaches
    /// every hardlink already made from the object. The WAL consumer performs
    /// exactly such a redundant re-write when it drains a `Put` that a
    /// materializer has already materialized.
    #[tokio::test]
    async fn rewriting_an_existing_full_object_preserves_hardlink_identity() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .unwrap();
        let data = Bytes::from_static(b"immutable object");
        let hash = Hash::from_content(&data);

        store.write(hash, ObjectEncoding::Full, data.clone()).await.unwrap();
        let blob = hash_to_path(dir.path(), &hash);
        let materialized = dir.path().join("materialized-output.bin");
        std::fs::hard_link(&blob, &materialized).unwrap();
        assert!(is_same_file(&blob, &materialized), "hard link setup");

        // Same hash, same bytes: the object is already present and immutable.
        store.write(hash, ObjectEncoding::Full, data).await.unwrap();

        assert!(
            is_same_file(&blob, &materialized),
            "re-writing an existing full object must not replace its inode, because that \
             detaches every hardlink materialized from it"
        );
        assert_eq!(store.read(&hash).await.unwrap(), Bytes::from_static(b"immutable object"));
    }

    /// Delta envelopes stay replaceable: they are a derived encoding the
    /// optimizer may re-derive against a different base, and nothing hardlinks
    /// them, so skipping the rewrite would freeze a stale encoding on disk.
    #[tokio::test]
    async fn rewriting_an_existing_delta_object_replaces_it() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .unwrap();
        let hash = Hash::from_content(b"delta target");
        let base = Hash::from_content(b"delta base");
        let first = Bytes::from_static(b"first delta envelope");
        let second = Bytes::from_static(b"second delta envelope");

        store.write(hash, ObjectEncoding::Delta { base_hash: base }, first.clone()).await.unwrap();
        store.write(hash, ObjectEncoding::Delta { base_hash: base }, second.clone()).await.unwrap();

        let delta_path = hash_to_delta_path(dir.path(), &hash);
        assert_eq!(std::fs::read(&delta_path).unwrap(), second.to_vec());
    }

    /// Every file the store commits is read-only, whichever path wrote it.
    ///
    /// Immutability is the store's own claim, not a side effect of the
    /// materializer's chmod through a hardlinked output: a blob nothing links
    /// to is read-only too, and so is a delta envelope and an auxiliary file.
    #[tokio::test]
    async fn strict_a_stored_blob_is_read_only() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .expect("create store");

        let data = Bytes::from_static(b"immutable object mode");
        let hash = Hash::from_content(&data);
        store.write(hash, ObjectEncoding::Full, data.clone()).await.expect("write full object");
        assert!(
            is_read_only(&hash_to_path(dir.path(), &hash)),
            "a committed full object must be read-only"
        );

        let base = Hash::from_content(b"mode base");
        store
            .write(hash, ObjectEncoding::Delta { base_hash: base }, data)
            .await
            .expect("write delta envelope");
        assert!(
            is_read_only(&hash_to_delta_path(dir.path(), &hash)),
            "a committed delta envelope must be read-only"
        );

        store
            .write_aux(&hash, "materialized-path", Bytes::from_static(b"/tmp/output"))
            .await
            .expect("write aux file");
        let aux = hash_to_path(dir.path(), &hash)
            .parent()
            .expect("object has a fan-out parent")
            .join("materialized-path");
        assert!(is_read_only(&aux), "a committed auxiliary file must be read-only");
    }

    /// A blob written by a build that never set the attribute is repaired by the
    /// skip path, and repairing it must not replace the object: the inode is what
    /// every hardlinked output depends on.
    #[tokio::test]
    async fn regression_an_existing_blob_is_made_read_only() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .expect("create store");

        let data = Bytes::from_static(b"older build wrote this");
        let hash = Hash::from_content(&data);
        store.write(hash, ObjectEncoding::Full, data.clone()).await.expect("write object");
        let blob = hash_to_path(dir.path(), &hash);
        let linked = dir.path().join("materialized-output.bin");
        std::fs::hard_link(&blob, &linked).expect("hard link the object");

        tamper(&blob);
        assert!(!is_read_only(&blob), "the writable premise must hold");

        // The object is already present, so this write takes the skip path.
        store.write(hash, ObjectEncoding::Full, data.clone()).await.expect("re-store object");

        assert!(is_read_only(&blob), "the skip path must restore the read-only mode");
        assert!(
            is_same_file(&blob, &linked),
            "restoring the mode must not replace the object, which would detach its hardlinks"
        );
        assert_eq!(store.read(&hash).await.expect("read object"), data);
    }

    /// A read-only object is still removable through every delete path.
    ///
    /// Windows refuses to unlink a read-only file, so this is the regression pin
    /// for the store's delete paths on both platforms.
    #[tokio::test]
    async fn regression_a_read_only_blob_can_be_removed() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .expect("create store");

        let base = Hash::from_content(b"removal base");
        let data = Bytes::from_static(b"removable object");
        let hash = Hash::from_content(&data);
        store.write(hash, ObjectEncoding::Full, data.clone()).await.expect("write full object");
        store
            .write(hash, ObjectEncoding::Delta { base_hash: base }, data)
            .await
            .expect("write delta object");
        store
            .write_aux(&hash, "materialized-path", Bytes::from_static(b"/tmp/output"))
            .await
            .expect("write aux file");

        let blob = hash_to_path(dir.path(), &hash);
        let delta = hash_to_delta_path(dir.path(), &hash);
        assert!(is_read_only(&blob) && is_read_only(&delta), "both encodings are read-only");

        store
            .delete_encoding(hash, ObjectEncoding::Delta { base_hash: base })
            .await
            .expect("delete_encoding must remove a read-only delta");
        assert!(!delta.exists(), "the delta encoding is gone");

        store.delete(&hash).await.expect("delete must remove a read-only object");
        assert!(!blob.exists(), "the full object is gone");

        store
            .delete_aux(&hash, "materialized-path")
            .await
            .expect("delete_aux must remove a read-only auxiliary file");
        assert!(
            !blob.parent().expect("fan-out parent").join("materialized-path").exists(),
            "the auxiliary file is gone"
        );
    }

    /// A re-encoded delta envelope is replaced successfully and the replacement
    /// carries the same read-only mode.
    #[tokio::test]
    async fn strict_a_rewritten_delta_envelope_is_read_only() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .expect("create store");

        let hash = Hash::from_content(b"re-encoded delta target");
        let base = Hash::from_content(b"re-encoded delta base");
        let first = Bytes::from_static(b"first envelope");
        let second = Bytes::from_static(b"second envelope, longer");

        store
            .write(hash, ObjectEncoding::Delta { base_hash: base }, first)
            .await
            .expect("write first envelope");
        store
            .write(hash, ObjectEncoding::Delta { base_hash: base }, second.clone())
            .await
            .expect("replace the envelope");

        let delta_path = hash_to_delta_path(dir.path(), &hash);
        // Read the envelope directly: a delta envelope is a derived encoding, so
        // its bytes need not hash to the key it is filed under and `read_delta`
        // verifies exactly that relationship.
        assert_eq!(std::fs::read(&delta_path).expect("read envelope"), second.to_vec());
        assert!(is_read_only(&delta_path), "the replacement envelope must be read-only");
    }

    /// The two streaming commit paths take the same mode as the buffered one.
    #[tokio::test]
    async fn strict_a_streamed_blob_is_read_only() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let store = FileSystemBlobStore::create(
            dir.path().to_path_buf(),
            vec![VerifyTriggerStrategy::Always],
        )
        .await
        .expect("create store");

        let payload: &[u8] = b"streamed object mode";
        let mut reader = payload;
        let (hash, length) =
            store.write_stream(ObjectEncoding::Full, &mut reader).await.expect("write from stream");
        assert_eq!(length, payload.len() as u64);
        assert!(
            is_read_only(&hash_to_path(dir.path(), &hash)),
            "a streamed object must be read-only"
        );

        let known = Hash::from_content(b"reader object mode");
        let mut reader: &[u8] = b"reader object mode";
        store
            .write_from_reader(known, ObjectEncoding::Full, &mut reader, 18)
            .await
            .expect("write from reader");
        assert!(
            is_read_only(&hash_to_path(dir.path(), &known)),
            "an object written from a reader must be read-only"
        );
    }

    /// A leftover read-only staging file must not wedge the next commit.
    ///
    /// A commit stages under a fixed name and makes the staged file read-only
    /// before renaming it. A write that fails in that window, or a process that
    /// dies there, leaves a read-only file at the staging name, and opening it
    /// for write truncates and fails: every later commit for the same hash would
    /// fail with an error naming nothing useful. The commit clears the attribute
    /// first, so the name stays reusable.
    #[tokio::test]
    async fn regression_a_read_only_staging_file_does_not_block_the_next_write() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let store = FileSystemBlobStore::create(dir.path().to_path_buf(), Vec::new())
            .await
            .expect("blob store");
        let data = Bytes::from_static(b"payload behind a wedged staging file");
        let hash = Hash::from_content(&data);
        let object_path = hash_to_path(dir.path(), &hash);
        let staging_path = object_path.with_extension("tmp");
        std::fs::create_dir_all(object_path.parent().expect("hash path has a parent"))
            .expect("create fan-out");
        std::fs::write(&staging_path, b"interrupted commit").expect("plant staging file");
        let mut staged = std::fs::metadata(&staging_path).expect("staging metadata").permissions();
        staged.set_readonly(true);
        std::fs::set_permissions(&staging_path, staged).expect("make staging file read-only");
        assert!(
            is_read_only(&staging_path),
            "the fixture has to be read-only, or the commit under test never meets one"
        );

        store.write(hash, ObjectEncoding::Full, data.clone()).await.expect("the next commit");

        assert_eq!(store.read(&hash).await.expect("read back"), data);
        assert!(is_read_only(&object_path), "the committed object must be read-only");
    }

    /// Deleting a hash must not make a hardlinked library file writable.
    ///
    /// The delete path clears the read-only attribute only where the platform
    /// requires it, and POSIX does not: `unlink` needs write permission on the
    /// containing directory. Clearing it anyway would reach the inode a
    /// materialized library file shares with the blob, so a store delete would
    /// silently make the library writable. Windows shares the attribute across
    /// hardlink names and cannot answer how many names an inode has, so this
    /// assertion holds where the mode is per-inode and mutable independently.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_deleted_object_leaves_a_hardlinked_copy_read_only() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let store = FileSystemBlobStore::create(dir.path().to_path_buf(), Vec::new())
            .await
            .expect("blob store");
        let data = Bytes::from_static(b"blob with a library hardlink");
        let hash = Hash::from_content(&data);
        store.write(hash, ObjectEncoding::Full, data.clone()).await.expect("write object");
        let object_path = hash_to_path(dir.path(), &hash);
        let library_path = dir.path().join("library-song.mkv");
        std::fs::hard_link(&object_path, &library_path).expect("hardlink object");

        store.delete(&hash).await.expect("delete object");

        assert!(
            std::fs::symlink_metadata(&object_path).is_err(),
            "the delete has to remove the store's name, or the assertion below proves nothing"
        );
        assert!(
            is_read_only(&library_path),
            "a store delete must not reach the mode of a file that shares the blob's inode"
        );
    }
}
