//! Retain-only and expiry-prune logic.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::ConductorError;

use super::helpers::sanitize_tool_id;
use super::types::Metadata;
use super::{LOCK_FILE_NAME, METADATA_FILE_NAME, TTL_SECONDS, VERSION};

/// Removes all provisioned tool directories under `tools_dir` whose sanitized
/// names are not present in `active_tool_ids`.
///
/// This is the free-function counterpart of [`super::ProvisionCache::retain_only`],
/// usable without constructing a [`super::ProvisionCache`].  Only entries without
/// active locks are removed; in-use entries are preserved even if they are not
/// in the active set.
pub async fn retain_only_tool_dirs<S: std::hash::BuildHasher>(
    tools_dir: PathBuf,
    active_tool_ids: HashSet<String, S>,
) -> Result<(), ConductorError> {
    let active: HashSet<String> =
        active_tool_ids.into_iter().map(|id| sanitize_tool_id(&id)).collect();
    tokio::task::spawn_blocking(move || do_retain_only(&tools_dir, &active)).await.map_err(
        |join_err| ConductorError::Internal(format!("retain-only task panicked: {join_err}")),
    )?
}

/// Removes all entry directories in `tools_dir` whose name is not in
/// `active_sanitized`.
fn do_retain_only(
    tools_dir: &Path,
    active_sanitized: &HashSet<String>,
) -> Result<(), ConductorError> {
    if !tools_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(tools_dir).map_err(|source| ConductorError::Io {
        operation: "enumerating tool-content cache root for retain-only".to_string(),
        path: tools_dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| ConductorError::Io {
            operation: "reading tool-content cache directory entry".to_string(),
            path: tools_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let Ok(dir_name) = entry.file_name().into_string() else {
            continue;
        };
        if !entry.file_type().is_ok_and(|ty| ty.is_dir()) {
            continue;
        }
        if active_sanitized.contains(&dir_name) {
            continue;
        }

        // Try to acquire an exclusive lock non-blockingly.  If the entry is
        // in use, skip it.
        let lock_path = path.join(LOCK_FILE_NAME);
        let Ok(lock_file) = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path)
        else {
            continue;
        };
        if lock_file.try_lock().is_err() {
            continue;
        }
        let _ = fs::remove_dir_all(&path);
    }
    Ok(())
}

/// Removes provisioning entries that have not been used within `TTL_SECONDS`.
pub(crate) fn prune_expired_entries(tools_dir: &Path, now: u64) -> Result<(), ConductorError> {
    if !tools_dir.exists() {
        return Ok(());
    }

    let cutoff = now.saturating_sub(TTL_SECONDS);

    let entries = fs::read_dir(tools_dir).map_err(|source| ConductorError::Io {
        operation: "enumerating tool-content cache root".to_string(),
        path: tools_dir.to_path_buf(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| ConductorError::Io {
            operation: "reading tool-content cache directory entry".to_string(),
            path: tools_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if !entry.file_type().is_ok_and(|ty| ty.is_dir()) {
            continue;
        }

        let metadata_path = path.join(METADATA_FILE_NAME);
        let Ok(raw) = fs::read_to_string(&metadata_path) else {
            continue;
        };
        let Ok(metadata) = serde_json::from_str::<Metadata>(&raw) else {
            continue;
        };
        if metadata.version != VERSION {
            continue;
        }
        if metadata.last_used_unix_seconds > cutoff {
            continue;
        }

        // Try to acquire an exclusive lock non-blockingly.
        let lock_path = path.join(LOCK_FILE_NAME);
        let Ok(lock_file) = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path)
        else {
            continue;
        };
        if lock_file.try_lock().is_err() {
            continue;
        }

        let _ = fs::remove_dir_all(&path);
    }

    Ok(())
}
