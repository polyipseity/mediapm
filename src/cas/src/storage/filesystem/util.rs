//! Atomic I/O helper utilities for the filesystem CAS backend.
//!
//! Provides atomic object-file write and read-only permission helpers shared
//! by the object actor and other backend components.

use std::io::Write;
use std::path::{Path, PathBuf};

use tracing::error;

use crate::{CasError, empty_content_hash};

use tokio::fs;

use super::STORAGE_VERSION;
use super::paths::object_path;

/// Ensures the canonical empty object payload exists on disk.
pub(super) async fn bootstrap_empty_object(root: &Path) -> Result<(), CasError> {
    let empty = empty_content_hash();
    let path = object_path(root, empty);
    if fs::try_exists(&path)
        .await
        .map_err(|source| CasError::io("checking empty object", path.clone(), source))?
    {
        return Ok(());
    }

    let staging_root = root.join(STORAGE_VERSION).join("tmp");
    write_object_atomic(&staging_root, &path, &[]).await
}

/// Atomically writes one object file.
pub(super) async fn write_object_atomic(
    staging_root: &Path,
    path: &Path,
    bytes: &[u8],
) -> Result<(), CasError> {
    write_atomic(staging_root.to_path_buf(), path.to_path_buf(), bytes.to_vec()).await
}

/// Atomically writes arbitrary bytes to target path via temp-file rename.
async fn write_atomic(
    staging_root: PathBuf,
    path: PathBuf,
    bytes: Vec<u8>,
) -> Result<(), CasError> {
    tokio::task::spawn_blocking(move || {
        let Some(parent) = path.parent() else {
            return Err(CasError::invalid_input(format!(
                "cannot atomically write path without parent: {}",
                path.display()
            )));
        };

        std::fs::create_dir_all(parent)
            .map_err(|source| CasError::io("creating parent directories", parent, source))?;

        std::fs::create_dir_all(&staging_root).map_err(|source| {
            CasError::io("creating shared staging tmp directory", staging_root.clone(), source)
        })?;

        let mut temp = tempfile::Builder::new()
            .prefix("cas-")
            .suffix(".stage")
            .tempfile_in(&staging_root)
            .map_err(|source| {
                CasError::io("creating named temp file", staging_root.clone(), source)
            })?;

        temp.write_all(&bytes)
            .map_err(|source| CasError::io("writing staged bytes", temp.path(), source))?;
        temp.as_file()
            .sync_all()
            .map_err(|source| CasError::io("syncing staged file", temp.path(), source))?;

        let (staged_file, staged_path) = temp.keep().map_err(|source| {
            let staging_path = source.file.path().to_path_buf();
            CasError::io("materializing staged file path", staging_path, source.error)
        })?;
        drop(staged_file);

        match std::fs::rename(&staged_path, &path) {
            Ok(()) => {
                enforce_file_readonly(&path)?;
                Ok(())
            }
            Err(_first_rename_error) if path.exists() => {
                clear_file_readonly_if_set(&path)?;
                std::fs::remove_file(&path).map_err(|source| {
                    CasError::io(
                        "removing existing target before rename fallback",
                        path.clone(),
                        source,
                    )
                })?;

                std::fs::rename(&staged_path, &path).map_err(|source| {
                    CasError::io("renaming staged file into place", path.clone(), source)
                })?;
                enforce_file_readonly(&path)?;
                Ok(())
            }
            Err(first_rename_error) => {
                let _ = std::fs::remove_file(&staged_path);
                error!(
                    path = %path.display(),
                    source = %first_rename_error,
                    "staged rename failed without existing target"
                );
                Err(CasError::io("renaming staged file into place", path, first_rename_error))
            }
        }
    })
    .await
    .map_err(|err| CasError::task_join("atomically writing filesystem object bytes", err))?
}

/// Marks one object file as read-only after CAS-owned writes complete.
fn enforce_file_readonly(path: &Path) -> Result<(), CasError> {
    let metadata = std::fs::metadata(path).map_err(|source| {
        CasError::io("reading object metadata for readonly enforcement", path, source)
    })?;
    let mut permissions = metadata.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        std::fs::set_permissions(path, permissions).map_err(|source| {
            CasError::io("marking object file readonly after atomic commit", path, source)
        })?;
    }

    Ok(())
}

/// Clears read-only bit on one object file so CAS can replace/remove it.
pub(super) fn clear_file_readonly_if_set(path: &Path) -> Result<(), CasError> {
    let metadata = std::fs::metadata(path).map_err(|source| {
        CasError::io("reading existing object metadata before overwrite", path, source)
    })?;

    let permissions = metadata.permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                let mut writable_permissions = permissions;
                writable_permissions.set_mode(writable_mode);
                std::fs::set_permissions(path, writable_permissions).map_err(|source| {
                    CasError::io("clearing readonly bit before object overwrite", path, source)
                })?;
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed overwrite/delete operations can succeed"
            )]
            {
                let mut writable_permissions = permissions;
                writable_permissions.set_readonly(false);
                std::fs::set_permissions(path, writable_permissions).map_err(|source| {
                    CasError::io("clearing readonly bit before object overwrite", path, source)
                })?;
            }
        }
    }

    Ok(())
}
