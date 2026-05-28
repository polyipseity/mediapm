//! Workspace-local download and extraction helpers for managed media tools.
//!
//! This folder-module keeps provisioning logic focused by splitting catalog
//! resolution, release metadata querying, transfer behavior, and payload
//! materialization into dedicated submodules.

mod cache;
mod github;
mod http;
mod materialize;
mod models;
mod resolve;

#[cfg(test)]
mod tests;

use std::fs;
use std::sync::Arc;

use crate::config::ToolRequirement;
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::tools::catalog::tool_catalog_entry;

pub(crate) use cache::{ToolCachePruneReport, ToolDownloadCache, default_global_tool_cache_root};
pub(crate) use models::{ContentMapSource, ProvisionedToolPayload, ResolvedToolIdentity};

/// Byte-level transfer snapshot emitted while one tool payload downloads.
///
/// The downloader reports progress for the currently active URL candidate
/// only. If fallback moves to a new candidate, progress may restart from
/// `0 / total` for that candidate instead of accumulating bytes from failed
/// attempts.
///
/// `total_bytes` is the active candidate `Content-Length` when provided by
/// the server. When unknown, `total_bytes` is `None` and callers should treat
/// progress as indeterminate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DownloadProgressSnapshot {
    /// Cumulative bytes read so far across all attempted URL candidates.
    pub downloaded_bytes: u64,
    /// Total payload bytes expected across attempted + active candidates,
    /// when known.
    pub total_bytes: Option<u64>,
}

/// Callback invoked as downloader transfer progress advances.
///
/// Callers may use this to update per-tool progress UI while preserving
/// download concurrency.
pub(crate) type DownloadProgressCallback = Arc<dyn Fn(DownloadProgressSnapshot) + Send + Sync>;

/// Stable prefix for `mediapm`-managed immutable tool ids.
const MANAGED_TOOL_ID_PREFIX: &str = "mediapm.tools.";

/// Subdirectory within a tool entry directory where the conductor tool-content
/// cache stores extracted payload content alongside `metadata.json`.
///
/// The conductor wipes the provisioner download install root and re-extracts
/// content here on the first cache-miss run.  When this subdirectory is
/// present, `provision_tool_payload` treats it as the effective root for
/// executable discovery and content-map reconstruction, avoiding an unnecessary
/// re-download on every `tools sync` call after the first `media sync`.
///
/// This mirrors `TOOL_CONTENT_CACHE_PAYLOAD_DIR_NAME` in the conductor crate's
/// `tool_content_cache` module.  Both values must remain in sync.
const CONDUCTOR_TOOL_PAYLOAD_DIR: &str = "payload";

/// Ensures one managed tool payload is provisioned into workspace-local
/// storage and converted into conductor-ready command/content-map metadata.
pub(crate) async fn provision_tool_payload(
    paths: &MediaPmPaths,
    tool_name: &str,
    requirement: &ToolRequirement,
    download_progress: Option<DownloadProgressCallback>,
    download_cache: Option<Arc<ToolDownloadCache>>,
) -> Result<ProvisionedToolPayload, MediaPmError> {
    let entry = tool_catalog_entry(tool_name)?;
    let resolved =
        resolve::resolve_download_plan(&entry, requirement, download_cache.clone()).await?;
    let suffix = resolve::tool_id_suffix_from_identity(&resolved.identity)?;
    let tool_name_id = resolve::sanitize_tool_id_fragment(entry.name);
    let source_id = resolve::sanitize_tool_id_fragment(&resolved.source_identifier);
    let tool_id = format!(
        "{MANAGED_TOOL_ID_PREFIX}{}+{}@{}",
        tool_name_id,
        source_id,
        resolve::sanitize_tool_id_fragment(&suffix)
    );
    let install_root = paths.tools_dir.join(&tool_id);

    if install_root.exists() {
        // When the conductor has already materialised the tool payload into the
        // `payload/` subdirectory (on a previous `media sync` run), treat that
        // subdirectory as the effective root for executable discovery and
        // content-map reconstruction.  The uncompressed ZIP hash of `payload/`
        // is identical to the hash of the original download root (same file
        // names and bytes at the same relative paths), so the conductor
        // cache-hit check continues to pass and no re-download is triggered.
        let payload_dir = install_root.join(CONDUCTOR_TOOL_PAYLOAD_DIR);
        let effective_root =
            if payload_dir.is_dir() { payload_dir.as_path() } else { install_root.as_path() };

        if let Ok(executable_paths) =
            materialize::resolve_executable_paths(&entry, &resolved, effective_root)
            && let Ok(command_selector) = materialize::build_command_selector(&executable_paths)
            && let Ok(content_entries) =
                materialize::collect_materialized_content_entries(&resolved, effective_root)
            && !content_entries.is_empty()
            && materialize::additional_download_sources_present(&entry, &resolved, effective_root)
        {
            return Ok(ProvisionedToolPayload {
                tool_id,
                command_selector,
                content_entries,
                identity: resolved.identity,
                source_label: resolved.source_label,
                source_identifier: resolved.source_identifier,
                catalog: entry,
                warnings: resolved.warnings,
            });
        }

        fs::remove_dir_all(&install_root).map_err(|source| MediaPmError::Io {
            operation: format!("resetting existing tool install directory for '{tool_id}'"),
            path: install_root.clone(),
            source,
        })?;
    }
    fs::create_dir_all(&install_root).map_err(|source| MediaPmError::Io {
        operation: format!("creating tool install directory for '{tool_id}'"),
        path: install_root.clone(),
        source,
    })?;

    materialize::materialize_download_plan(
        &entry,
        &resolved,
        &install_root,
        download_progress,
        download_cache,
    )
    .await?;

    let executable_paths = materialize::resolve_executable_paths(&entry, &resolved, &install_root)?;
    let command_selector = materialize::build_command_selector(&executable_paths)?;
    let content_entries =
        materialize::collect_materialized_content_entries(&resolved, &install_root)?;
    if content_entries.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "tool '{tool_id}' provisioning produced no content-map payload entries"
        )));
    }

    Ok(ProvisionedToolPayload {
        tool_id,
        command_selector,
        content_entries,
        identity: resolved.identity,
        source_label: resolved.source_label,
        source_identifier: resolved.source_identifier,
        catalog: entry,
        warnings: resolved.warnings,
    })
}
