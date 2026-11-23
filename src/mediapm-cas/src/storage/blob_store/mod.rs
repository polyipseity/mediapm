//! Blob storage — eventual-consistent content-addressed blob storage.
//!
//! Defines the [`BlobStore`] trait and its implementations:
//! - [`FileSystemBlobStore`] — hash-derived directory layout on disk.
//! - [`InMemoryBlobStore`] — ephemeral in-memory map (testing/CI).

mod fs;
pub(crate) mod mem;
pub(crate) mod versions;

pub use fs::FileSystemBlobStore;
pub use mem::InMemoryBlobStore;

use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

/// Which physical forms a hash has on disk.
///
/// The index records intent; this records fact. Both are needed: a both-forms
/// state is legitimate while the optimizer is mid-transition and must not be
/// collapsed during it, so the collapse decides from the two together.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PresentEncodings {
    /// A full payload file exists.
    pub full: bool,
    /// A `.diff` payload file exists.
    pub delta: bool,
}

/// Which physical form of an object a caller is asking about.
///
/// `ObjectEncoding` names how bytes are stored and carries the base a delta
/// applies to; mtime selection only needs the form, so it takes this instead.
/// A caller that holds an encoding projects it, and one that picked a form for
/// path selection states only the form and invents no base hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobForm {
    /// The full payload file.
    Full,
    /// The `.diff` delta envelope.
    Delta,
}

impl From<ObjectEncoding> for BlobForm {
    fn from(encoding: ObjectEncoding) -> Self {
        match encoding {
            ObjectEncoding::Full => Self::Full,
            ObjectEncoding::Delta { .. } => Self::Delta,
        }
    }
}

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

    /// Write a payload, syncing the file and its parent directory first.
    ///
    /// Defaulted to [`Self::write`]: an in-memory store has nothing to sync.
    async fn write_syncing(
        &self,
        hash: Hash,
        encoding: ObjectEncoding,
        data: Bytes,
    ) -> Result<(), CasError> {
        self.write(hash, encoding, data).await
    }

    /// Write an auxiliary file, syncing the file and its parent directory.
    async fn write_aux_syncing(
        &self,
        hash: &Hash,
        name: &str,
        data: Bytes,
    ) -> Result<(), CasError> {
        self.write_aux(hash, name, data).await
    }

    /// Read full blob for `hash`.
    ///
    /// Returns [`CasError::NotFound`] if no full blob exists.
    async fn read(&self, hash: &Hash) -> Result<Bytes, CasError>;

    /// Read delta-encoded blob for `hash`.
    ///
    /// Returns [`CasError::NotFound`] if no delta blob exists.
    ///
    /// A delta envelope is a derived encoding, so its bytes are not the target
    /// hash and cannot be verified against it. A store with verify-on-read
    /// enabled rejects a `.diff` whose envelope or patch payload is tampered
    /// with (a V3 envelope carries `diff_hash`, which decoding validates). A
    /// legacy `.diff` that predates V3 has no integrity field of its own; a
    /// verifying store rewrites it in the current format on first read so the
    /// bytes become verifiable like any other object.
    async fn read_delta(&self, hash: &Hash) -> Result<Bytes, CasError>;

    /// Delete all blobs for `hash` (both full and delta).
    async fn delete(&self, hash: &Hash) -> Result<(), CasError>;

    /// Delete only a specific encoding of `hash`.
    ///
    /// Used for eventual cleanup during optimization (e.g., removing a
    /// stale `.diff` blob after promoting it to full).
    async fn delete_encoding(&self, hash: Hash, encoding: ObjectEncoding) -> Result<(), CasError> {
        // Default: ignore (in-memory and simple impls).
        let _ = (hash, encoding);
        Ok(())
    }

    /// Which physical forms `hash` has.
    ///
    /// The default answers "full or nothing", which is what a store that keeps
    /// one entry per hash can honestly say: the in-memory store ignores the
    /// `.diff` distinction, so it has no second form to report. The file system
    /// separates the forms and overrides this, because for that store the two
    /// files are the fact.
    async fn present_encodings(&self, hash: &Hash) -> Result<PresentEncodings, CasError> {
        match self.read(hash).await {
            Ok(_) => Ok(PresentEncodings { full: true, delta: false }),
            Err(CasError::NotFound(_)) => Ok(PresentEncodings::default()),
            Err(error) => Err(error),
        }
    }

    /// Return the on-disk path for `hash`'s full blob, if this is a
    /// filesystem-backed store. In-memory stores return `None`.
    fn materialized_path(&self, hash: &Hash) -> Option<PathBuf> {
        let _ = hash;
        None
    }

    /// Return the on-disk path for `hash`'s delta blob, if this is a
    /// filesystem-backed store. In-memory stores return `None`.
    ///
    /// Symmetric with [`materialized_path`](Self::materialized_path): it
    /// answers where this form would live, not whether the file exists.
    fn materialized_delta_path(&self, hash: &Hash) -> Option<PathBuf> {
        let _ = hash;
        None
    }

    /// The modification time of the materialized payload for `hash`, if any.
    ///
    /// `form` selects which physical form to stat: a full read cares about the
    /// full file's mtime, a delta read cares about the delta file's. A Modified
    /// verify-on-read strategy keys its comparison on this mtime, so the caller
    /// must pass the form it is about to read; passing the wrong form makes
    /// tamper detection compare against an unrelated file.
    ///
    /// Defaulted to `None`: an in-memory store has no on-disk file whose mtime
    /// a verify-on-read strategy could compare.
    async fn materialized_mtime(
        &self,
        hash: &Hash,
        form: BlobForm,
    ) -> Option<std::time::SystemTime> {
        let _ = (hash, form);
        None
    }

    /// Whether a read of `hash` is scheduled to verify the payload it returns.
    ///
    /// Defaulted to `false`: an in-memory store cannot hold a payload that was
    /// corrupted between its write and its read, so it has nothing to verify and
    /// a caller may stream.
    fn should_verify_on_read(&self, hash: &Hash, mtime: Option<std::time::SystemTime>) -> bool {
        let _ = (hash, mtime);
        false
    }

    /// Whether `put()` should materialize [`BlobStore`] + metadata synchronously
    /// (write-through), or defer to the WAL consumer (write-back).
    /// `InMemory` impls return `true`; `FileSystem` impls return `false`.
    const SYNC_MATERIALIZE: bool = true;

    /// Read blob data and write it to an async writer.
    ///
    /// Default impl reads into `Bytes` then writes. Backends override for
    /// zero-copy streaming (e.g. `copy_buf` from a file).
    async fn read_to_writer(
        &self,
        hash: &Hash,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<(), CasError> {
        use tokio::io::AsyncWriteExt;
        let data = self.read(hash).await?;
        writer.write_all(&data).await.map_err(CasError::Io)?;
        Ok(())
    }

    /// Write blob data from an async reader, computing the content hash
    /// incrementally.
    ///
    /// Default impl buffers the full reader then calls `write()`. Backends
    /// override for streaming writes with hash verification.
    async fn write_from_reader(
        &self,
        hash: Hash,
        encoding: ObjectEncoding,
        reader: &mut (dyn AsyncRead + Send + Unpin),
        content_len: u64,
    ) -> Result<(), CasError> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::with_capacity(usize::try_from(content_len).unwrap_or(usize::MAX));
        reader.read_to_end(&mut buf).await.map_err(CasError::Io)?;
        self.write(hash, encoding, Bytes::from(buf)).await
    }

    /// Write blob data from an async reader, returning the content hash and
    /// length. Useful when the hash is not known upfront (e.g. streaming PUT).
    ///
    /// Default impl buffers the full reader, computes the hash, then calls
    /// `write()`. Backends override for streaming writes with incremental
    /// hashing (avoids double-buffering).
    async fn write_stream(
        &self,
        encoding: ObjectEncoding,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(Hash, u64), CasError> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.map_err(CasError::Io)?;
        let len = buf.len() as u64;
        let hash = Hash::from_content(&buf);
        self.write(hash, encoding, Bytes::from(buf)).await?;
        Ok((hash, len))
    }

    /// Write an auxiliary file into the same fan-out directory tree as `hash`.
    ///
    /// Auxiliary files live alongside object blobs in the hash-derived fan-out
    /// directory (`<root>/v1/blake3/ab/cd/<name>`). They are not part of the
    /// content-addressed CAS — they are side-channel storage for metadata and
    /// other non-content data keyed by a hash's directory locality.
    ///
    /// # Name validation
    ///
    /// Implementations should reject names containing `/` or `..` to prevent
    /// directory traversal.
    async fn write_aux(&self, hash: &Hash, name: &str, data: Bytes) -> Result<(), CasError> {
        let _ = (hash, name, data);
        Ok(())
    }

    /// Read an auxiliary file from `hash`'s fan-out directory.
    ///
    /// Returns [`CasError::NotFound`] when the aux file does not exist.
    async fn read_aux(&self, hash: &Hash, name: &str) -> Result<Bytes, CasError> {
        let _ = (hash, name);
        Err(CasError::NotFound(*hash))
    }

    /// Delete an auxiliary file from `hash`'s fan-out directory.
    ///
    /// Succeeds silently if the aux file does not exist.
    async fn delete_aux(&self, hash: &Hash, name: &str) -> Result<(), CasError> {
        let _ = (hash, name);
        Ok(())
    }

    /// Return contents of all auxiliary files named `name` across all fan-out
    /// directories. Used during startup to rebuild state from per-directory
    /// metadata snapshots.
    ///
    /// Implementations return an empty `Vec` when no such aux files exist.
    async fn all_aux(&self, name: &str) -> Result<Vec<Bytes>, CasError> {
        let _ = name;
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ObjectEncoding;
    use crate::hash::Hash;

    #[test]
    fn an_encoding_projects_to_the_form_it_names() {
        assert_eq!(BlobForm::from(ObjectEncoding::Full), BlobForm::Full);
        assert_eq!(
            BlobForm::from(ObjectEncoding::Delta { base_hash: Hash::from_content(b"base") }),
            BlobForm::Delta
        );
    }
}
