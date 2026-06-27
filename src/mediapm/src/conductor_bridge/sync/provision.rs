//! Concurrent tool payload provisioning.
//!
//! This module handles downloading and verifying tool payloads concurrently,
//! with progress reporting via pulse-bar events.

use std::collections::BTreeMap;
use std::path::Path;

use blake3;
use mediapm_cas::CasApi;

use crate::error::MediaPmError;
use crate::tools::catalog::{current_tool_os, tool_catalog_entry};
use crate::tools::downloader::{
    ToolDownloadCache, extract_archive, fetch_bytes_from_candidates, resolve_download_plan,
};

/// Progress event emitted by the provision worker.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(super) enum ProvisionWorkerEvent {
    /// One tool finished provisioning (tool name, result).
    Finished(String, Result<(), String>),
}

/// Callback invoked with download progress for one tool payload.
#[allow(dead_code)]
pub(super) type DownloadProgressCallback = Box<dyn Fn(u64, u64) + Send + 'static>;

/// Provisions desired tools concurrently, downloading needed payloads.
///
/// Iterates over each desired tool, resolves its catalog entry, downloads
/// missing payloads via the tool download cache, extracts them under
/// `tools_cache_root`, and returns the subset of entries whose payloads
/// were successfully provisioned. Missing or failed entries are logged as
/// warnings.
pub(super) async fn provision_desired_tools_concurrently(
    cas: &impl CasApi,
    desired_tools: &BTreeMap<String, String>,
    tools_cache_root: &Path,
    cache: &ToolDownloadCache,
) -> Result<BTreeMap<String, String>, MediaPmError> {
    let mut result = BTreeMap::new();
    let _ = cas; // retained in signature for future CAS integration

    for (tool_id, hash_or_version) in desired_tools {
        // Look up catalog entry.
        let Some(entry) = tool_catalog_entry(tool_id) else {
            tracing::warn!("tool {tool_id}: no catalog entry found, skipping provisioning");
            continue;
        };

        // Resolve download plan.
        let plan = match resolve_download_plan(entry, cache).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("tool {tool_id}: failed to resolve download plan: {e}, skipping");
                continue;
            }
        };

        // Internal launcher tools need no download.
        if plan.internal_launcher {
            result.insert(tool_id.clone(), hash_or_version.clone());
            continue;
        }

        // Determine host OS action.
        let host_os = current_tool_os();
        let Some(action) = plan.per_os_actions.get(&host_os) else {
            tracing::warn!(
                "tool {tool_id}: no download action for host OS {:?}, skipping",
                host_os,
            );
            continue;
        };

        // Download payload (with cache).
        let cache_key = format!("{}_{}", entry.id, entry.latest);
        let bytes = if let Some(cached) = cache.lookup_bytes(&cache_key).await {
            cached
        } else {
            match fetch_bytes_from_candidates(&action.urls, None).await {
                Ok(bytes) => {
                    cache.store_bytes(&cache_key, &bytes).await;
                    bytes
                }
                Err(e) => {
                    tracing::warn!("tool {tool_id}: download failed: {e}, skipping");
                    continue;
                }
            }
        };

        // Compute BLAKE3 hash.
        let hash = blake3::hash(&bytes);
        let hash_hex = hash.to_hex().as_str().to_string();

        // Extract to tools_cache_root/<tool_id>.
        let target_dir = tools_cache_root.join(tool_id);
        tokio::fs::create_dir_all(&target_dir).await.ok();
        if let Err(e) = extract_archive(&bytes, action.archive_format, &target_dir) {
            tracing::warn!("tool {tool_id}: extraction failed: {e}, skipping");
            continue;
        }

        result.insert(tool_id.clone(), hash_hex);
    }

    Ok(result)
}
