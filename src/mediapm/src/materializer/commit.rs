//! Staged-output commit, path removal, readonly enforcement, and timestamp helpers.

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use unicode_normalization::UnicodeNormalization;

use crate::config::HierarchyEntryKind;
use crate::error::MediaPmError;

/// Merges one staged directory into one existing destination directory.
///
/// Existing destination children with the same names are replaced, while
/// unrelated existing children are preserved.
fn merge_staged_directory_into_existing(
    staged_dir: &Path,
    final_dir: &Path,
) -> Result<(), MediaPmError> {
    clear_path_readonly_recursively(final_dir)?;

    for entry in fs::read_dir(staged_dir).map_err(|source| MediaPmError::Io {
        operation: "reading staged directory before merge".to_string(),
        path: staged_dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| MediaPmError::Io {
            operation: "iterating staged directory before merge".to_string(),
            path: staged_dir.to_path_buf(),
            source,
        })?;

        let staged_child = entry.path();
        let final_child = final_dir.join(entry.file_name());

        if final_child.exists() {
            let staged_metadata =
                fs::symlink_metadata(&staged_child).map_err(|source| MediaPmError::Io {
                    operation: "reading staged child metadata before merge".to_string(),
                    path: staged_child.clone(),
                    source,
                })?;
            let final_metadata =
                fs::symlink_metadata(&final_child).map_err(|source| MediaPmError::Io {
                    operation: "reading destination child metadata before merge".to_string(),
                    path: final_child.clone(),
                    source,
                })?;

            if staged_metadata.is_dir() && final_metadata.is_dir() {
                merge_staged_directory_into_existing(&staged_child, &final_child)?;
                continue;
            }

            remove_path(&final_child)?;
        }

        fs::rename(&staged_child, &final_child).map_err(|source| MediaPmError::Io {
            operation: "merging staged directory child".to_string(),
            path: final_child.clone(),
            source,
        })?;
    }

    fs::remove_dir(staged_dir).map_err(|source| MediaPmError::Io {
        operation: "removing emptied staged directory after merge".to_string(),
        path: staged_dir.to_path_buf(),
        source,
    })?;

    Ok(())
}

/// Commits one staged hierarchy output into the final library destination.
///
/// File outputs always replace the previous destination path atomically. For
/// folder outputs, existing destination directories are merged so overlapping
/// hierarchy entries do not delete already-materialized sibling content.
pub(super) fn commit_staged_output(
    staged_path: &Path,
    final_path: &Path,
    entry_kind: HierarchyEntryKind,
) -> Result<(), MediaPmError> {
    if matches!(entry_kind, HierarchyEntryKind::MediaFolder) && final_path.exists() {
        let final_metadata =
            fs::symlink_metadata(final_path).map_err(|source| MediaPmError::Io {
                operation: "reading destination metadata before folder-merge commit".to_string(),
                path: final_path.to_path_buf(),
                source,
            })?;

        if final_metadata.is_dir() {
            merge_staged_directory_into_existing(staged_path, final_path)?;
            ensure_managed_path_readonly(final_path)?;
            return Ok(());
        }

        remove_path(final_path)?;
    } else if final_path.exists() {
        remove_path(final_path)?;
    }

    fs::rename(staged_path, final_path).map_err(|source| MediaPmError::Io {
        operation: "committing staged output via rename".to_string(),
        path: final_path.to_path_buf(),
        source,
    })?;
    ensure_managed_path_readonly(final_path)?;

    Ok(())
}

/// Removes one path recursively when it is a directory, or as one file otherwise.
pub(super) fn remove_path(path: &Path) -> Result<(), MediaPmError> {
    clear_path_readonly_recursively(path)?;

    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before removal".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        fs::remove_dir_all(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale directory".to_string(),
            path: path.to_path_buf(),
            source,
        })
    } else {
        fs::remove_file(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale file".to_string(),
            path: path.to_path_buf(),
            source,
        })
    }
}

/// Marks one managed output path as read-only after successful commit.
///
/// For directory outputs, this recursively marks descendant files/directories
/// read-only. For symlinks, permissions are applied to the resolved target.
fn ensure_managed_path_readonly(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading managed output metadata before readonly enforcement".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading managed output directory before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating managed output directory before readonly enforcement"
                    .to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            ensure_managed_path_readonly(&entry.path())?;
        }
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading managed output permissions before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "marking managed output path readonly".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Clears read-only bit recursively so stale managed paths can be removed.
fn clear_path_readonly_recursively(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before readonly clear".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading directory before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating directory before readonly clear".to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            clear_path_readonly_recursively(&entry.path())?;
        }
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading path permissions before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                permissions.set_mode(writable_mode);
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed overwrite/delete operations can succeed"
            )]
            {
                permissions.set_readonly(false);
            }
        }

        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "clearing readonly bit before managed-path removal".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Validates one relative hierarchy path against mediapm invariants.
pub(super) fn validate_hierarchy_path(relative_path: &str) -> Result<(), MediaPmError> {
    let path = Path::new(relative_path);

    if path.is_absolute() {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path '{relative_path}' must be relative"
        )));
    }

    for component in path.components() {
        let segment = component.as_os_str().to_string_lossy();
        if segment == "." || segment == ".." {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' must not contain '.' or '..' components"
            )));
        }

        if segment.chars().any(is_rejected_char) {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' contains forbidden characters"
            )));
        }

        let nfd = segment.nfd().collect::<String>();
        if nfd != segment {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' is not Unicode NFD normalized"
            )));
        }
    }

    Ok(())
}

/// Returns whether one character is forbidden by cross-platform filename rules.
fn is_rejected_char(ch: char) -> bool {
    matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*')
}

/// Returns current Unix epoch timestamp in seconds.
pub(super) fn now_unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Returns current Unix epoch timestamp in milliseconds.
pub(super) fn unix_epoch_millis() -> u64 {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}
