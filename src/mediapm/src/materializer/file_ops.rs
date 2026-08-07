//! Filesystem materialization helpers: staging, linking, copying, and reflink.

use std::io;
use std::path::Path;

use mediapm_cas::{CasApi, FileSystemCas, Hash};

use crate::config::MaterializationMethod;
use crate::error::MediaPmError;

use super::commit::remove_path;

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
    // Ensure the blob is materialized in the CAS blob store so that
    // filesystem-based methods (hardlink, symlink, reflink) can work.
    // For WAL-only small blobs, this reads the bytes from the WAL and
    // writes them to the blob store + metadata.
    cas.ensure_blob_materialized(hash).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "ensuring CAS blob materialization for '{hash}' failed: {source}"
        ))
    })?;

    let mut failures = Vec::new();

    for (method_index, method) in methods.iter().enumerate() {
        remove_existing_destination_path(destination_path).await?;

        if !matches!(method, MaterializationMethod::Copy) {
            cas.ensure_blob_materialized(hash).await.map_err(|source| {
                MediaPmError::Workflow(format!(
                    "ensuring CAS blob materialization for '{hash}' before '{}' failed: {source}",
                    materialization_method_label(*method),
                ))
            })?;
        }

        // Re-resolve after `ensure_blob_materialized` and before each attempt:
        // background CAS maintenance may rewrite blobs between calls.
        let source_path = cas.object_path_for_hash(hash).filter(|p| p.is_file());

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
