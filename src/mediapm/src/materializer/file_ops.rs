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

/// Attempts reflink/clone materialization for one file.
///
/// Current implementation reports unsupported on this build/runtime and lets
/// ordered fallback proceed to subsequent configured methods.
fn attempt_reflink_materialization(
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
            attempt_reflink_materialization(source, destination_path)
        }
        MaterializationMethod::Copy => {
            if let Some(source) = source_path {
                tokio::fs::copy(source, destination_path).await.map(|_| ())
            } else {
                let bytes = cas.get(hash).await.map_err(|error| {
                    io::Error::other(format!(
                        "reading CAS bytes for copy materialization failed: {error}"
                    ))
                })?;
                tokio::fs::write(destination_path, bytes.as_ref()).await
            }
        }
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
    let source_path = cas.object_path_for_hash(hash);
    let source_path = source_path.is_file().then_some(source_path);
    let mut failures = Vec::new();

    for (method_index, method) in methods.iter().enumerate() {
        remove_existing_destination_path(destination_path).await?;

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
                        method.as_label()
                    ));
                }
                return Ok(());
            }
            Err(error) => {
                failures.push(format!("{}: {error}", method.as_label()));
                let _ = remove_existing_destination_path(destination_path).await;
            }
        }
    }

    Err(MediaPmError::Workflow(format!(
        "materializing hierarchy file '{managed_relative_path}' failed for all configured methods ({})",
        failures.join("; ")
    )))
}
