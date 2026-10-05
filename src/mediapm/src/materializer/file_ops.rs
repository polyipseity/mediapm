//! Filesystem materialization helpers: staging, linking, copying, and reflink.
//!
//! The second half of the module answers the question the first half creates.
//! Writing an output leaves a relationship to its CAS object that differs by
//! method, and a re-sync that wants to decline a write has to establish what
//! that relationship is before it can decline anything:
//! [`output_relationship`] for what each method leaves, and
//! [`file_content_matches_hash`] for the one case where only bytes can answer.

use std::fs::Metadata;
use std::io;
use std::path::Path;

use mediapm_cas::{CasApi, FileSystemCas, Hash};

use crate::config::MaterializationMethod;
use crate::error::MediaPmError;

use super::commit::remove_path;

/// Bytes read per chunk while hashing a managed output.
///
/// Managed outputs are media files and a library is large, so the hash streams
/// instead of holding a whole file in memory.
const HASH_CHUNK_BYTES: usize = 64 * 1024;

/// What an output is, as far as the CAS object it was materialized from goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutputRelationship {
    /// The output is the object, or a symlink naming it.
    ///
    /// A hardlink and the object are one inode, so comparing device and inode
    /// settles it with two stat calls and no bytes read. A symlink names the
    /// object's path, so reading the link settles it with one syscall. Neither
    /// check can be wrong about what the output holds, because the output is
    /// the object.
    Linked,
    /// A symlink naming somewhere other than the object.
    ///
    /// Wrong whatever the link happens to resolve to: a managed output is
    /// materialized from the store, so one pointing outside it is not an
    /// output this run can leave alone. Nothing further is worth reading.
    Mislinked,
    /// A regular file that is not the object, which is what a reflink and a
    /// copy leave. A reflink shares blocks but has its own inode, and a copy
    /// has its own inode by definition, so neither says anything about the
    /// bytes in the file. Only comparing the bytes can say.
    Separate,
}

/// Establishes what `target_path` is in relation to the CAS object at
/// `object_path`.
///
/// A path that is neither a file nor a symlink, and an object whose metadata
/// cannot be read, both answer [`OutputRelationship::Separate`], which leaves
/// the caller comparing bytes. That is the direction to err in: the caller
/// rewrites the file to what the document resolved.
pub(super) async fn output_relationship(
    target_path: &Path,
    object_path: &Path,
) -> OutputRelationship {
    let Ok(target_metadata) = tokio::fs::symlink_metadata(target_path).await else {
        return OutputRelationship::Separate;
    };
    if target_metadata.file_type().is_symlink() {
        return match tokio::fs::read_link(target_path).await {
            Ok(link) if link == object_path => OutputRelationship::Linked,
            _ => OutputRelationship::Mislinked,
        };
    }
    if !target_metadata.is_file() {
        return OutputRelationship::Separate;
    }
    let Ok(object_metadata) = tokio::fs::metadata(object_path).await else {
        return OutputRelationship::Separate;
    };
    if describes_one_file(&target_metadata, &object_metadata) {
        OutputRelationship::Linked
    } else {
        OutputRelationship::Separate
    }
}

/// Whether the bytes at `path` hash to `expected`.
///
/// Streams the file, because a managed output is a media file and reading one
/// whole at a time is what makes a verification pass over a library expensive.
/// Every byte an independent-inode method's output holds passes through here,
/// so the cost of this branch is the size of what it checks.
///
/// This is the one place a library output's bytes are read to settle whether
/// it may be left alone, which is why the test-only `read_watch` counts here
/// and nowhere else. A caller that needs to know whether a run read an output
/// at all has no other place to look.
pub(super) async fn file_content_matches_hash(path: &Path, expected: &Hash) -> bool {
    use tokio::io::AsyncReadExt as _;

    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return false;
    };
    #[cfg(test)]
    read_watch::note_hashed_output(path);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; HASH_CHUNK_BYTES];
    loop {
        match file.read(&mut buffer).await {
            Ok(0) => return Hash::from_bytes(*hasher.finalize().as_bytes()) == *expected,
            Ok(read) => {
                hasher.update(&buffer[..read]);
            }
            Err(_) => return false,
        }
    }
}

/// Whether two `stat` results describe one file rather than two that happen to
/// agree on length.
///
/// The platform's own answer to the question: device and inode on unix, volume
/// serial number and file index on Windows. Windows reports both as optional,
/// and a pair it declines to report is not evidence of identity, so that case
/// answers `false` and the caller falls through to comparing content.
fn describes_one_file(left: &Metadata, right: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        left.dev() == right.dev() && left.ino() == right.ino()
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;

        match (left.volume_serial_number(), left.file_index()) {
            (Some(volume), Some(index)) => {
                Some((volume, index)) == (right.volume_serial_number(), right.file_index())
            }
            _ => false,
        }
    }
}

/// Counting the managed outputs a test asked to have their bytes read.
///
/// The count answers one question: did the run open that output to hash it, or
/// did it settle the question some other way. [`file_content_matches_hash`] is
/// the only place a library output's bytes are read to decide whether it may
/// be left alone, so a run that leaves the count at zero for an output it
/// declined to rewrite read none of it. That is the witness the skip cache
/// needs, because a modification time cannot tell a hardlink rewrite from a
/// no-op and a re-read looks like neither.
///
/// Watched paths are whole paths rather than a directory, so tests running
/// beside each other in one process cannot count each other's reads: each
/// holds its own tempdir and names its own outputs. A guard removes its paths
/// when it drops, so the count a later test starts from is the one it left.
#[cfg(test)]
pub(super) mod read_watch {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard, OnceLock};

    /// Watched path to how many times its bytes were read, shared by every
    /// guard in the process.
    fn watched() -> &'static Mutex<BTreeMap<PathBuf, u64>> {
        static WATCHED: OnceLock<Mutex<BTreeMap<PathBuf, u64>>> = OnceLock::new();
        WATCHED.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    /// The watched map, recovering from a guard that panicked while holding
    /// it.
    ///
    /// Recovery rather than propagation, because a poisoned lock must not turn
    /// a read into something no test can observe. A count of zero is the
    /// direction that fails a watched test rather than passing one, so a lost
    /// increment still cannot let a re-read through.
    fn locked() -> MutexGuard<'static, BTreeMap<PathBuf, u64>> {
        watched().lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Count one read of `path`'s bytes, which does nothing unless a guard is
    /// watching that exact path.
    pub(super) fn note_hashed_output(path: &Path) {
        let mut watched = locked();
        if let Some(count) = watched.get_mut(path) {
            *count += 1;
        }
    }

    /// Counts the reads of a fixed set of outputs until it drops.
    ///
    /// Held for as long as the run under observation, so the count covers that
    /// run and nothing else.
    #[derive(Debug)]
    pub(in crate::materializer) struct ReadCountGuard {
        /// The paths this guard watches, kept so it can name which one was
        /// read and so it can unregister them on drop.
        paths: Vec<PathBuf>,
    }

    impl ReadCountGuard {
        /// Start counting the reads of `paths`, each starting from zero.
        pub(in crate::materializer) fn new(paths: impl IntoIterator<Item = PathBuf>) -> Self {
            let paths: Vec<PathBuf> = paths.into_iter().collect();
            let mut watched = locked();
            for path in &paths {
                watched.insert(path.clone(), 0);
            }
            ReadCountGuard { paths }
        }

        /// How many times `path`'s bytes were read while this guard was held.
        ///
        /// Zero for a path this guard does not watch, so a caller that mistyped
        /// a path reads zero rather than a number belonging to something else.
        pub(in crate::materializer) fn hashed(&self, path: &Path) -> u64 {
            if !self.paths.iter().any(|watched| watched == path) {
                return 0;
            }
            locked().get(path).copied().unwrap_or(0)
        }

        /// How many of the watched outputs were read, summed.
        pub(in crate::materializer) fn total_hashed(&self) -> u64 {
            let watched = locked();
            self.paths.iter().filter_map(|path| watched.get(path)).sum()
        }
    }

    impl Drop for ReadCountGuard {
        fn drop(&mut self) {
            let mut watched = locked();
            for path in &self.paths {
                watched.remove(path);
            }
        }
    }
}

/// Removes one destination path if it already exists.
///
/// This helper treats broken symlinks as existing paths and removes them too.
/// Uses `tokio::task::spawn_blocking` to avoid blocking the async executor
/// thread during the recursive readonly-clear and remove operations.
async fn remove_existing_destination_path(path: &Path) -> Result<(), MediaPmError> {
    if tokio::fs::symlink_metadata(path).await.is_ok() {
        let owned = path.to_path_buf();
        tokio::task::spawn_blocking(move || remove_path(&owned)).await.map_err(|e| {
            MediaPmError::Workflow(format!("remove destination path task panicked: {e}"))
        })?
    } else {
        Ok(())
    }
}

/// Creates one filesystem symlink for a regular file using the async tokio
/// runtime API.
#[cfg(unix)]
async fn create_file_symlink_async(source_path: &Path, destination_path: &Path) -> io::Result<()> {
    tokio::fs::symlink(source_path, destination_path).await
}

/// Creates one filesystem symlink for a regular file using the async tokio
/// runtime API.
#[cfg(windows)]
async fn create_file_symlink_async(source_path: &Path, destination_path: &Path) -> io::Result<()> {
    tokio::fs::symlink_file(source_path, destination_path).await
}

/// Attempts reflink/clone (copy-on-write) materialization for one file.
///
/// On Linux, uses the `FICLONE` ioctl (supported on btrfs, XFS, and other
/// copy-on-write-capable filesystems). On macOS, uses `clonefile()` (APFS).
/// On other platforms, reports unsupported and lets ordered fallback proceed.
async fn attempt_reflink_materialization(
    source_path: &Path,
    destination_path: &Path,
) -> io::Result<()> {
    let owned_src = source_path.to_path_buf();
    let owned_dst = destination_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        attempt_reflink_materialization_sync(&owned_src, &owned_dst)
    })
    .await
    .map_err(io::Error::other)?
}

/// Platform-specific reflink implementation for Linux using `FICLONE` ioctl.
#[cfg(target_os = "linux")]
fn attempt_reflink_materialization_sync(
    source_path: &Path,
    destination_path: &Path,
) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let src = std::fs::File::open(source_path)?;
    let dest = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(destination_path)?;

    // SAFETY: FICLONE operates on open file descriptors — the kernel validates
    // both are regular files on a compatible COW filesystem.
    let ret =
        unsafe { libc::ioctl(dest.as_raw_fd(), libc::FICLONE as libc::c_ulong, src.as_raw_fd()) };

    if ret == 0 {
        Ok(())
    } else {
        let err = io::Error::last_os_error();
        // Clean up destination so fallback doesn't see a stale file.
        let _ = std::fs::remove_file(destination_path);
        Err(err)
    }
}

/// Platform-specific reflink implementation for macOS using `clonefile`.
#[cfg(target_os = "macos")]
fn attempt_reflink_materialization_sync(
    source_path: &Path,
    destination_path: &Path,
) -> io::Result<()> {
    use std::ffi::CString;

    let src_c = CString::new(source_path.as_os_str().as_encoded_bytes()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "source path contains null byte")
    })?;
    let dst_c = CString::new(destination_path.as_os_str().as_encoded_bytes()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "destination path contains null byte")
    })?;

    // SAFETY: clonefile is a standard macOS syscall with no memory-safety
    // implications when passed valid C strings.
    let ret = unsafe { libc::clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };

    if ret == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

/// Stub for platforms without native reflink support.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn attempt_reflink_materialization_sync(
    _source_path: &Path,
    _destination_path: &Path,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "reflink materialization is not supported on this build",
    ))
}

/// Attempts one configured materialization method for one destination file.
///
/// All filesystem operations use `tokio::fs` to avoid blocking the async
/// executor thread on potentially slow link, copy, or write I/O.
async fn attempt_materialization_method(
    method: MaterializationMethod,
    cas: &FileSystemCas,
    hash: Hash,
    source_path: Option<&Path>,
    destination_path: &Path,
) -> io::Result<()> {
    match method {
        MaterializationMethod::Hardlink => {
            let source = source_path.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "CAS object file is unavailable for hardlink materialization",
                )
            })?;
            tokio::fs::hard_link(source, destination_path).await
        }
        MaterializationMethod::Symlink => {
            let source = source_path.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "CAS object file is unavailable for symlink materialization",
                )
            })?;
            create_file_symlink_async(source, destination_path).await
        }
        MaterializationMethod::Reflink => {
            let source = source_path.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "CAS object file is unavailable for reflink materialization",
                )
            })?;
            attempt_reflink_materialization(source, destination_path).await
        }
        MaterializationMethod::Copy => {
            if let Some(source) = source_path {
                tokio::fs::copy(source, destination_path).await.map(|_| ())
            } else {
                let dest_file = tokio::fs::File::create(destination_path).await?;
                cas.get_to_writer(hash, dest_file).await.map_err(|error| {
                    io::Error::other(format!(
                        "reading CAS bytes for copy materialization failed: {error}"
                    ))
                })
            }
        }
    }
}

/// Returns a human-readable label for a materialization method.
fn materialization_method_label(method: MaterializationMethod) -> &'static str {
    match method {
        MaterializationMethod::Hardlink => "hardlink",
        MaterializationMethod::Symlink => "symlink",
        MaterializationMethod::Reflink => "reflink",
        MaterializationMethod::Copy => "copy",
    }
}

/// Materializes one managed file from CAS using ordered runtime policy.
pub(super) async fn materialize_file_from_cas_with_order(
    cas: &FileSystemCas,
    hash: Hash,
    destination_path: &Path,
    managed_relative_path: &str,
    methods: &[MaterializationMethod],
    notices: &mut Vec<String>,
) -> Result<(), MediaPmError> {
    let mut failures = Vec::new();

    for (method_index, method) in methods.iter().enumerate() {
        remove_existing_destination_path(destination_path).await?;

        let source_path = if matches!(method, MaterializationMethod::Copy) {
            None
        } else {
            if cas.ensure_blob_materialized(hash).await.is_err() {
                failures.push(format!(
                    "{}: CAS blob store path unavailable for '{hash}'",
                    materialization_method_label(*method)
                ));
                continue;
            }
            cas.object_path_for_hash(hash).filter(|p| p.is_file())
        };

        match attempt_materialization_method(
            *method,
            cas,
            hash,
            source_path.as_deref(),
            destination_path,
        )
        .await
        {
            Ok(()) => {
                if method_index > 0 {
                    notices.push(format!(
                        "hierarchy file '{managed_relative_path}' materialization fell back to '{}'",
                        materialization_method_label(*method)
                    ));
                }
                return Ok(());
            }
            Err(error) => {
                failures.push(format!("{}: {error}", materialization_method_label(*method)));
                let _ = remove_existing_destination_path(destination_path).await;
            }
        }
    }

    Err(MediaPmError::Workflow(format!(
        "materializing hierarchy file '{managed_relative_path}' failed for all configured methods ({})",
        failures.join("; ")
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use bytes::Bytes;
    use mediapm_cas::CasApi;

    #[tokio::test]
    async fn materialize_with_copy_succeeds() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let cas = FileSystemCas::open(&dir.path().join("cas")).await.unwrap();
        let content = b"hello materializer";
        let hash = cas.put(Bytes::from_static(content)).await.unwrap();

        let dest = dir.path().join("output.txt");
        let mut notices = Vec::new();
        materialize_file_from_cas_with_order(
            &cas,
            hash,
            &dest,
            "output.txt",
            &[MaterializationMethod::Copy],
            &mut notices,
        )
        .await
        .unwrap();

        assert!(dest.exists());
        let actual = tokio::fs::read_to_string(&dest).await.unwrap();
        assert_eq!(actual, "hello materializer");
        assert!(notices.is_empty());
    }

    #[tokio::test]
    async fn hardlink_works_for_wal_only_small_blob_after_ensure() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let cas = FileSystemCas::open(&dir.path().join("cas")).await.unwrap();
        let hash = cas.put(Bytes::from_static(b"wal-only-small")).await.unwrap();
        assert!(
            !cas.object_path_for_hash(hash).is_some_and(|p| p.is_file()),
            "small puts should remain WAL-only before materialization ensure"
        );

        let dest = dir.path().join("wal-only.bin");
        let mut notices = Vec::new();
        materialize_file_from_cas_with_order(
            &cas,
            hash,
            &dest,
            "wal-only.bin",
            &[MaterializationMethod::Hardlink],
            &mut notices,
        )
        .await
        .unwrap();

        let source = cas.object_path_for_hash(hash).expect("cas object path");
        assert!(same_file::is_same_file(&source, &dest).expect("same_file check"));
        assert!(notices.is_empty());
    }

    #[tokio::test]
    async fn hardlink_survives_later_wal_consumer_drain_of_the_same_blob() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let cas = FileSystemCas::open(&dir.path().join("cas")).await.unwrap();
        let hash = cas.put(Bytes::from_static(b"drained-after-link")).await.unwrap();
        assert!(
            !cas.object_path_for_hash(hash).is_some_and(|p| p.is_file()),
            "small puts stay WAL-only, so the materializer's ensure writes the blob itself"
        );

        let dest = dir.path().join("drained.bin");
        let mut notices = Vec::new();
        materialize_file_from_cas_with_order(
            &cas,
            hash,
            &dest,
            "drained.bin",
            &[MaterializationMethod::Hardlink],
            &mut notices,
        )
        .await
        .unwrap();
        let source = cas.object_path_for_hash(hash).expect("cas object path");
        assert!(same_file::is_same_file(&source, &dest).expect("same_file check after link"));
        assert!(notices.is_empty(), "hardlink must not fall back to a copying method");

        // The store drains its own WAL in the background, so the same Put can
        // be applied to the blob store long after the materializer hardlinked
        // it. A CAS object is content-addressed and therefore immutable, so
        // that re-application must not replace the blob's inode: doing so
        // silently detaches every hardlink already made from it, and the
        // materialized output keeps pointing at the orphaned inode.
        cas.bg_engine().run_wal_consumer().await.expect("drain wal");

        assert!(
            same_file::is_same_file(&source, &dest).expect("same_file check after wal drain"),
            "materialized output must stay hardlinked to its CAS blob after the store drains the \
             Put that created the blob"
        );
    }

    #[tokio::test]
    async fn hardlink_materialization_succeeds_with_spaces_in_destination_path() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let cas = FileSystemCas::open(&dir.path().join("cas")).await.unwrap();
        let content = b"hardlink-with-spaces";
        let hash = cas.put(Bytes::from_static(content)).await.unwrap();

        let dest = dir
            .path()
            .join("music videos")
            .join("Artist - Title [demo.local.id]")
            .join("Artist - Title [demo.local.id].m4a");
        tokio::fs::create_dir_all(dest.parent().expect("parent")).await.unwrap();
        let mut notices = Vec::new();
        materialize_file_from_cas_with_order(
            &cas,
            hash,
            &dest,
            "music videos/Artist - Title [demo.local.id]/Artist - Title [demo.local.id].m4a",
            &[MaterializationMethod::Hardlink],
            &mut notices,
        )
        .await
        .unwrap();

        let source = cas.object_path_for_hash(hash).expect("cas object path");
        assert!(same_file::is_same_file(&source, &dest).expect("same_file check"));
        assert!(notices.is_empty());
    }

    #[tokio::test]
    async fn concurrent_hardlink_materialization_from_shared_cas() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let cas = Arc::new(FileSystemCas::open(&dir.path().join("cas")).await.unwrap());
        let mut hashes = Vec::new();
        for seed in 0u8..3 {
            hashes
                .push(cas.put(Bytes::from(vec![seed; 16_384])).await.expect("put wal-backed blob"));
        }

        let mut join_set = tokio::task::JoinSet::new();
        for (index, hash) in hashes.into_iter().enumerate() {
            let cas = cas.clone();
            let dest = dir.path().join(format!("parallel-{index}.bin"));
            let relative_path = format!("parallel-{index}.bin");
            join_set.spawn(async move {
                let mut notices = Vec::new();
                materialize_file_from_cas_with_order(
                    &cas,
                    hash,
                    &dest,
                    &relative_path,
                    &[MaterializationMethod::Hardlink],
                    &mut notices,
                )
                .await
                .expect("parallel hardlink materialization");
                (hash, dest)
            });
        }

        while let Some(result) = join_set.join_next().await {
            let (hash, dest) = result.expect("join");
            let source = cas.object_path_for_hash(hash).expect("cas object path");
            assert!(same_file::is_same_file(&source, &dest).expect("same_file check"));
        }
    }

    #[tokio::test]
    async fn hardlink_across_mediapm_store_and_media_dirs() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let store = dir.path().join(".mediapm").join("store");
        let media =
            dir.path().join("media").join("music videos").join("Artist - Title [demo.local.id]");
        tokio::fs::create_dir_all(&media).await.unwrap();
        let cas = FileSystemCas::open(&store).await.unwrap();
        let hash = cas.put(Bytes::from(vec![1u8; 20_000])).await.expect("put wal-backed blob");
        let dest = media.join("Artist - Title [demo.local.id].m4a");

        let mut notices = Vec::new();
        materialize_file_from_cas_with_order(
            &cas,
            hash,
            &dest,
            "music videos/Artist - Title [demo.local.id]/Artist - Title [demo.local.id].m4a",
            &[MaterializationMethod::Hardlink],
            &mut notices,
        )
        .await
        .expect("hardlink across mediapm layout");

        let source = cas.object_path_for_hash(hash).expect("cas object path");
        assert!(same_file::is_same_file(&source, &dest).expect("same_file check"));
        assert!(notices.is_empty());
    }

    #[tokio::test]
    async fn attempt_materialization_copy_without_source_works() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let cas = FileSystemCas::open(&dir.path().join("cas")).await.unwrap();
        let content = b"direct copy";
        let hash = cas.put(Bytes::from_static(content)).await.unwrap();
        let dest = dir.path().join("direct_copy.txt");

        attempt_materialization_method(MaterializationMethod::Copy, &cas, hash, None, &dest)
            .await
            .unwrap();

        assert!(dest.exists());
        let actual = tokio::fs::read_to_string(&dest).await.unwrap();
        assert_eq!(actual, "direct copy");
    }
}
