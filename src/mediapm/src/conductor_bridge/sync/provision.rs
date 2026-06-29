//! Tool payload provisioning: download, extract, and import to CAS.
//!
//! This module provides [`fetch_and_import_tool_payload`] which handles the
//! full lifecycle for one tool: look up catalog → resolve download plan →
//! download → extract → walk files → CAS import → content map.
//!
//! # All-platform download principle
//!
//! Tool payloads are downloaded for **all supported platforms** regardless of
//! the host OS. Each platform's archive is extracted to a separate
//! `{os}/` subdirectory, and every file is imported to CAS with a
//! `./{os}/…` content-map key prefix. The command selector is emitted as a
//! `${context.os == "…" ? ./…/… : …}` template expression so the conductor
//! resolves the correct executable at runtime. The conductor's
//! [`link_to_sandbox`] then skips foreign-platform directories via
//! [`FOREIGN_PLATFORM_DIRS`], materialising only the host-native payload.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use mediapm_cas::CasApi;

use crate::error::MediaPmError;
use crate::tools::catalog::{ARCHIVE_BINARY, tool_catalog_entry};
use crate::tools::downloader::{
    ToolDownloadCache, extract_archive, fetch_bytes_from_candidates, resolve_download_plan,
};

/// Result of fetching and importing a tool payload into CAS.
#[derive(Debug, Clone)]
pub(super) struct FetchedToolPayload {
    /// Content map: sandbox-relative path → CAS hash hex string.
    /// Keys use `./{os}/…` prefixes (e.g. `./linux/sd`, `./windows/sd.exe`)
    /// so the conductor's platform filtering works correctly.
    pub(super) content_map: BTreeMap<String, String>,
    /// Sandbox-relative path to the main executable, emitted as a
    /// `${context.os == "…" ? ./…/… : …}` template expression when multiple
    /// platforms are provisioned.
    pub(super) command_selector: String,
}

/// Fetches a tool payload for **all** platforms, extracts each to a
/// per-OS temp directory, imports files to CAS with `./{os}/` key prefixes,
/// and builds an OS-conditional command-selector template.
///
/// Returns `Ok(None)` when the tool has no catalog entry or is an internal
/// launcher.
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

    if plan.per_os_actions.is_empty() {
        tracing::warn!("tool {tool_id}: no download actions defined, skipping provisioning");
        return Ok(None);
    }

    let temp_root = tempfile::tempdir().map_err(|source| MediaPmError::Io {
        operation: "creating temp directory for tool extraction".to_string(),
        path: PathBuf::new(),
        source,
    })?;

    let mut content_map: BTreeMap<String, String> = BTreeMap::new();
    // Maps OS label → executable path relative to that OS extraction root.
    let mut per_os_exec: BTreeMap<String, String> = BTreeMap::new();

    for (os, action) in &plan.per_os_actions {
        let os_label = os.as_str();

        // ── download (per-OS cache key) ────────────────────────────────
        let cache_key = format!("{}_{}_{}", entry.id, os_label, entry.latest);
        let bytes = if let Some(cached) = cache.lookup_bytes(&cache_key).await {
            cached
        } else {
            let downloaded =
                fetch_bytes_from_candidates(&action.urls, None).await.map_err(|e| {
                    MediaPmError::Workflow(format!(
                        "tool {tool_id}: download failed for {os_label}: {e}"
                    ))
                })?;
            cache.store_bytes(&cache_key, &downloaded).await;
            downloaded
        };

        // ── extract to temp_root/{os_label}/ ───────────────────────────
        let os_dir = temp_root.path().join(os_label);
        std::fs::create_dir_all(&os_dir).map_err(|source| MediaPmError::Io {
            operation: format!("creating temp directory for {os_label} tool extraction"),
            path: os_dir.clone(),
            source,
        })?;
        extract_archive(&bytes, action.archive_format, &os_dir)?;

        // ── binary format: rename `tool` → `{tool_id}` ────────────────
        if action.archive_format == ARCHIVE_BINARY {
            let exe_name = if cfg!(target_os = "windows") { "tool.exe" } else { "tool" };
            let old_path = os_dir.join(exe_name);
            let new_path = os_dir.join(tool_id);
            if old_path.exists() {
                std::fs::rename(&old_path, &new_path).map_err(|source| MediaPmError::Io {
                    operation: format!(
                        "renaming extracted binary from {exe_name} to {tool_id} for {os_label}"
                    ),
                    path: old_path,
                    source,
                })?;
            }
        }

        // ── walk & CAS-import with `./{os_label}/` prefix ──────────────
        walk_dir_and_import_to_cas(cas, &os_dir, &mut content_map, os_label).await?;

        // ── find executable path within this OS subtree ────────────────
        let exec_path = if action.archive_format == ARCHIVE_BINARY {
            tool_id.to_string()
        } else {
            find_os_executable(&os_dir, tool_id).unwrap_or_else(|| tool_id.to_string())
        };
        per_os_exec.insert(os_label.to_string(), exec_path);
    }

    // Build ${context.os == "..." ? ./.../... : ...} template.
    let command_selector = build_os_conditional_selector(&per_os_exec);

    Ok(Some(FetchedToolPayload { content_map, command_selector }))
}

/// Recursively walks `dir`, imports each file to CAS, and records the
/// mapping `./{os_prefix}/relative/path → hash_hex` in `content_map`.
async fn walk_dir_and_import_to_cas(
    cas: &impl CasApi,
    dir: &Path,
    content_map: &mut BTreeMap<String, String>,
    os_prefix: &str,
) -> Result<(), MediaPmError> {
    let mut stack: Vec<PathBuf> = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let mut read_dir = match std::fs::read_dir(&current) {
            Ok(r) => r,
            Err(source) => {
                return Err(MediaPmError::Io {
                    operation: format!("reading directory '{}'", current.display()),
                    path: current,
                    source,
                });
            }
        };
        while let Some(entry) = {
            match read_dir.next() {
                Some(Ok(e)) => Some(e),
                Some(Err(source)) => {
                    return Err(MediaPmError::Io {
                        operation: format!("reading entry in '{}'", current.display()),
                        path: current,
                        source,
                    });
                }
                None => None,
            }
        } {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                let relative = path.strip_prefix(dir).unwrap_or(&path);
                let key = format!("./{}/{}", os_prefix, relative.to_string_lossy());
                let file_bytes = std::fs::read(&path).map_err(|source| MediaPmError::Io {
                    operation: format!("reading file '{}' for CAS import", path.display()),
                    path: path.clone(),
                    source,
                })?;
                let hash = cas.put(Bytes::from(file_bytes)).await.map_err(|e| {
                    MediaPmError::Workflow(format!("CAS put failed for '{key}': {e}"))
                })?;
                content_map.insert(key, hash.to_hex());
            }
        }
    }
    Ok(())
}

/// Searches for an executable named `{tool_id}` or `{tool_id}.exe` inside
/// `os_dir` and returns its path relative to `os_dir`. Returns `None` if
/// neither variant is found.
fn find_os_executable(os_dir: &Path, tool_id: &str) -> Option<String> {
    let candidates = [tool_id.to_string(), format!("{tool_id}.exe")];
    for name in &candidates {
        if let Some(rel) = find_file_relative(os_dir, os_dir, name) {
            return Some(rel.to_string_lossy().to_string());
        }
    }
    None
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

/// Builds a `${context.os == "linux" ? ./linux/sd : context.os == "macos" ? ./macos/sd : ./windows/sd}`
/// template string from the per-OS executable suffix map.
///
/// When only one OS is provisioned the template collapses to a plain path.
fn build_os_conditional_selector(per_os_exec: &BTreeMap<String, String>) -> String {
    if per_os_exec.is_empty() {
        return String::new();
    }
    let mut iter = per_os_exec.iter();
    let (first_os, first_path) = iter.next().expect("non-empty per_os_exec");
    if per_os_exec.len() == 1 {
        return format!("./{first_os}/{first_path}");
    }
    let mut result = format!("${{context.os == \"{first_os}\" ? ./{first_os}/{first_path}");
    for (os, path) in iter.by_ref() {
        result.push_str(&format!(" : context.os == \"{os}\" ? ./{os}/{path}"));
    }
    result.push('}');
    result
}
