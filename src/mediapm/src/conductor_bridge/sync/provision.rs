//! Tool payload provisioning: download, extract, and import to CAS.
//!
//! This module provides [`fetch_and_import_tool_payload`] which handles the
//! full lifecycle for one tool: look up catalog → resolve download plan →
//! download → extract → walk files → CAS import → content map.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use mediapm_cas::CasApi;

use crate::error::MediaPmError;
use crate::tools::catalog::{ARCHIVE_BINARY, current_tool_os, tool_catalog_entry};
use crate::tools::downloader::{
    ToolDownloadCache, extract_archive, fetch_bytes_from_candidates, resolve_download_plan,
};

/// Result of fetching and importing a tool payload into CAS.
#[derive(Debug, Clone)]
pub(super) struct FetchedToolPayload {
    /// Content map: sandbox-relative path → CAS hash hex string.
    pub(super) content_map: BTreeMap<String, String>,
    /// Sandbox-relative path to the main executable.
    pub(super) command_selector: String,
}

/// Fetches a tool payload, extracts it, imports individual files to CAS,
/// and returns the content map + command selector.
///
/// Returns `Ok(None)` when the tool has no catalog entry, is an internal
/// launcher, or has no matching host-OS download action.
pub(super) async fn fetch_and_import_tool_payload(
    cas: &impl CasApi,
    tool_id: &str,
    cache: &ToolDownloadCache,
) -> Result<Option<FetchedToolPayload>, MediaPmError> {
    let Some(entry) = tool_catalog_entry(tool_id) else {
        tracing::warn!("tool {tool_id}: no catalog entry found, skipping provisioning");
        return Ok(None);
    };

    let plan = resolve_download_plan(entry, cache).await.map_err(|e| {
        MediaPmError::Workflow(format!("tool {tool_id}: failed to resolve download plan: {e}"))
    })?;

    if plan.internal_launcher {
        return Ok(None);
    }

    let host_os = current_tool_os();
    let Some(action) = plan.per_os_actions.get(&host_os) else {
        tracing::warn!("tool {tool_id}: no download action for host OS {:?}, skipping", host_os);
        return Ok(None);
    };

    // Download payload (with cache).
    let cache_key = format!("{}_{}", entry.id, entry.latest);
    let bytes = if let Some(cached) = cache.lookup_bytes(&cache_key).await {
        cached
    } else {
        let downloaded = fetch_bytes_from_candidates(&action.urls, None)
            .await
            .map_err(|e| MediaPmError::Workflow(format!("tool {tool_id}: download failed: {e}")))?;
        cache.store_bytes(&cache_key, &downloaded).await;
        downloaded
    };

    // Extract to a temp directory.
    let temp_dir = tempfile::tempdir().map_err(|source| MediaPmError::Io {
        operation: "creating temp directory for tool extraction".to_string(),
        path: PathBuf::new(),
        source,
    })?;
    extract_archive(&bytes, action.archive_format, temp_dir.path())?;

    // For binary format, rename `tool` → `<tool_id>`.
    if action.archive_format == ARCHIVE_BINARY {
        let exe_name = if cfg!(target_os = "windows") { "tool.exe" } else { "tool" };
        let old_path = temp_dir.path().join(exe_name);
        let new_path = temp_dir.path().join(tool_id);
        if old_path.exists() {
            std::fs::rename(&old_path, &new_path).map_err(|source| MediaPmError::Io {
                operation: format!("renaming extracted binary from {exe_name} to {tool_id}"),
                path: old_path,
                source,
            })?;
        }
    }

    // Walk extracted files and import each to CAS.
    let mut content_map: BTreeMap<String, String> = BTreeMap::new();
    walk_and_import_to_cas(cas, temp_dir.path(), temp_dir.path(), &mut content_map).await?;

    // Determine command_selector.
    let command_selector = if action.archive_format == ARCHIVE_BINARY {
        format!("./{tool_id}")
    } else {
        find_command_selector(temp_dir.path(), tool_id).unwrap_or_else(|| format!("./{tool_id}"))
    };

    Ok(Some(FetchedToolPayload { content_map, command_selector }))
}

/// Recursively walks a directory, reads each file, imports it to CAS,
/// and records the mapping `./relative/path → hash_hex` in `content_map`.
async fn walk_and_import_to_cas(
    cas: &impl CasApi,
    root: &Path,
    dir: &Path,
    content_map: &mut BTreeMap<String, String>,
) -> Result<(), MediaPmError> {
    for entry in std::fs::read_dir(dir).map_err(|source| MediaPmError::Io {
        operation: format!("reading directory '{}'", dir.display()),
        path: dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| MediaPmError::Io {
            operation: format!("reading entry in '{}'", dir.display()),
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            Box::pin(walk_and_import_to_cas(cas, root, &path, content_map)).await?;
        } else if path.is_file() {
            let relative = path.strip_prefix(root).unwrap_or(&path);
            let key = format!("./{}", relative.to_string_lossy());
            let file_bytes = std::fs::read(&path).map_err(|source| MediaPmError::Io {
                operation: format!("reading file '{}' for CAS import", path.display()),
                path: path.clone(),
                source,
            })?;
            let hash = cas
                .put(Bytes::from(file_bytes))
                .await
                .map_err(|e| MediaPmError::Workflow(format!("CAS put failed for '{key}': {e}")))?;
            content_map.insert(key, hash.to_hex());
        }
    }
    Ok(())
}

/// Finds the sandbox-relative path to an executable named `tool_id` within
/// the extracted tree. Returns the relative path with `./` prefix.
fn find_command_selector(root: &Path, tool_id: &str) -> Option<String> {
    let target_name =
        if cfg!(target_os = "windows") { format!("{tool_id}.exe") } else { tool_id.to_string() };
    find_file_relative(root, root, &target_name).map(|rel| format!("./{}", rel.to_string_lossy()))
}

/// Recursively searches for a file with the given name, returning its path
/// relative to `root`.
fn find_file_relative(root: &Path, dir: &Path, target: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()? {
        let entry = entry.ok()?;
        let path = entry.path();
        if path.is_dir() {
            if let found @ Some(_) = find_file_relative(root, &path, target) {
                return found;
            }
        } else if path.file_name().and_then(|n| n.to_str()) == Some(target) {
            return path.strip_prefix(root).ok().map(|p| p.to_path_buf());
        }
    }
    None
}
