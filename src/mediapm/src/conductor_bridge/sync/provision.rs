//! Tool payload provisioning — thin wrapper around conductor.
//!
//! Delegates to [`mediapm_conductor::tools::provision::fetch_and_import_tool_payload`]
//! which handles the full lifecycle: catalog lookup → download plan → fetch →
//! extract → pack to ZIP → CAS import → content map. This module adapts
//! the mediapm progress-bar API to the conductor's callback-based progress
//! reporting and converts error and result types.

use std::collections::BTreeMap;
use std::sync::Arc;

use mediapm_cas::CasApi;
use mediapm_conductor::tools::provision::fetch_and_import_tool_payload as conductor_fetch;

use crate::error::MediaPmError;
use crate::output::ProgressBarApi;
use crate::tools::catalog::tool_catalog_entry;
use crate::tools::downloader::ToolDownloadCache;

/// Result of fetching and importing a tool payload into CAS.
#[derive(Debug, Clone)]
pub(super) struct FetchedToolPayload {
    /// Content map: sandbox-relative path → CAS hash hex string.
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
/// `progress_handle` is an [`Arc<dyn ProgressBarApi>`] whose message, total,
/// and position are updated per-OS download to show per-tool progress.
///
/// Returns `Ok(None)` when the tool has no catalog entry or is an internal
/// launcher.
pub(super) async fn fetch_and_import_tool_payload(
    cas: &impl CasApi,
    tool_id: &str,
    cache: &ToolDownloadCache,
    progress_handle: Arc<dyn ProgressBarApi>,
) -> Result<Option<FetchedToolPayload>, MediaPmError> {
    let Some(entry) = tool_catalog_entry(tool_id) else {
        tracing::warn!("tool {tool_id}: no catalog entry found, skipping provisioning");
        return Ok(None);
    };

    // Adapt mediapm's Arc<dyn ProgressBarApi> to conductor's Option<DownloadProgressCallback>.
    let progress_cb = {
        let pb = Arc::clone(&progress_handle);
        Some(Arc::new(move |snap: mediapm_conductor::tools::model::DownloadProgressSnapshot| {
            if let Some(total) = snap.total_bytes {
                pb.set_total(total);
            }
            pb.set_position(snap.downloaded_bytes);
        }) as mediapm_conductor::tools::model::DownloadProgressCallback)
    };

    match conductor_fetch(cas, entry, cache, progress_cb).await {
        Ok(Some(result)) => {
            progress_handle.finish();
            Ok(Some(FetchedToolPayload {
                content_map: result.content_map,
                command_selector: result.command_selector,
            }))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(MediaPmError::Workflow(format!("tool {tool_id}: provisioning failed: {e}"))),
    }
}
