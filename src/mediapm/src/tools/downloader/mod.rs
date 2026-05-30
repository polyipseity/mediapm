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
use std::path::PathBuf;
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

/// Relative user-cache staging directory used while preparing one tool payload.
const USER_SCOPED_PROVISION_STAGING_DIR: &str = "tmp/tool-sync-provision";

/// Ensures one managed tool payload is provisioned into user-scoped staging
/// storage and converted into conductor-ready command/content-map metadata.
///
/// Staging remains necessary because downloader materialization may need to:
/// - expand archives,
/// - discover executable paths,
/// - collect deterministic content-map entries before CAS import.
///
/// When available, staging is rooted under user cache (`<os-cache>/mediapm`) so
/// repeated workspace runs do not churn one workspace-local tmp tree.
/// Workspace tmp remains the fallback when no user cache root is available.
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
    let install_root = provision_install_root(paths, &tool_id);
    if install_root.exists() {
        fs::remove_dir_all(&install_root).map_err(|source| MediaPmError::Io {
            operation: format!("resetting staged tool install directory for '{tool_id}'"),
            path: install_root.clone(),
            source,
        })?;
    }
    fs::create_dir_all(&install_root).map_err(|source| MediaPmError::Io {
        operation: format!("creating staged tool install directory for '{tool_id}'"),
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

/// Resolves the staging install root for one tool payload provisioning run.
#[must_use]
fn provision_install_root(paths: &MediaPmPaths, tool_id: &str) -> PathBuf {
    let user_scoped_root = default_global_tool_cache_root()
        .map(|cache_root| cache_root.join(USER_SCOPED_PROVISION_STAGING_DIR));
    resolve_provision_install_root(paths, tool_id, user_scoped_root)
}

/// Resolves staging install root from one optional user-scoped base directory.
#[must_use]
pub(super) fn resolve_provision_install_root(
    paths: &MediaPmPaths,
    tool_id: &str,
    user_scoped_root: Option<PathBuf>,
) -> PathBuf {
    user_scoped_root
        .unwrap_or_else(|| paths.mediapm_tmp_dir.join("tool-sync-provision"))
        .join(tool_id)
}
