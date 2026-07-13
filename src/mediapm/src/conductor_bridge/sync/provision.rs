//! Tool payload provisioning — thin wrapper around conductor's 3-phase pipeline.
//!
//! Delegates to the mediapm [`provider`](crate::tools::provider) module for
//! phase 1 (resolve), then to the conductor's
//! [`fetch_tool_sources`](mediapm_conductor::tools::provider::fetch_tool_sources)
//! for phase 2 (fetch) and
//! [`postprocess_tool_sources`](mediapm_conductor::tools::provider::postprocess_tool_sources)
//! for phase 3 (extract → CAS import → content map). This module adapts
//! the mediapm progress-bar API to the provider's callback-based progress
//! reporting and converts error and result types.

use std::collections::BTreeMap;
use std::sync::Arc;

use mediapm_cas::CasApi;
use mediapm_conductor::tools::provider::{fetch_tool_sources, postprocess_tool_sources};
use mediapm_utils::progress::ProviderProgressCallback;

use crate::error::MediaPmError;
use crate::output::ProgressBarApi;
use crate::tools::downloader::ToolDownloadCache;
use crate::tools::provider;

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
/// Returns `Ok(None)` when the tool has no provider sources (internal
/// launcher, no external download needed).
pub(super) async fn fetch_and_import_tool_payload(
    cas: &impl CasApi,
    tool_id: &str,
    cache: &ToolDownloadCache,
    progress_handle: Arc<dyn ProgressBarApi>,
) -> Result<Option<FetchedToolPayload>, MediaPmError> {
    // Phase 1: Resolve — get source descriptors from the mediapm provider.
    let fetch = provider::resolve_tool_fetch(tool_id, None, None)
        .await
        .map_err(|e| MediaPmError::Workflow(format!("tool {tool_id}: resolve failed: {e}")))?;

    if fetch.sources.is_empty() {
        // No sources to fetch (internal launcher tool like media-tagger).
        return Ok(None);
    }

    // Adapt mediapm's Arc<dyn ProgressBarApi> to ProviderProgressCallback.
    let progress_cb: Option<ProviderProgressCallback> = {
        let pb = Arc::clone(&progress_handle);
        Some(Arc::new(move |snap| {
            pb.set_total(snap.bytes.1);
            pb.set_position(snap.bytes.0);
        }))
    };

    // Phase 2: Fetch — download (or generate) bytes for each source.
    let downloaded = fetch_tool_sources(&fetch, cache, progress_cb.clone())
        .await
        .map_err(|e| MediaPmError::Workflow(format!("tool {tool_id}: fetch failed: {e}")))?;

    // Phase 3: Postprocess — extract archives, repack to uncompressed ZIP,
    // import to CAS, build content map + command selector.
    let result = postprocess_tool_sources(&downloaded, cas, progress_cb)
        .await
        .map_err(|e| MediaPmError::Workflow(format!("tool {tool_id}: postprocess failed: {e}")))?;

    progress_handle.finish();

    Ok(Some(FetchedToolPayload {
        content_map: result.content_map,
        command_selector: result.command_selector,
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mediapm_cas::storage::in_memory::new_in_memory_cas;
    use mediapm_conductor::cache_user_level::UserLevelCache;
    use mediapm_utils::progress::recording::RecordingTrackedHandle;
    use tempfile::TempDir;

    use super::*;

    /// Helper to create the minimal dependencies for tests that exercise
    /// paths not reaching the cache or CAS (unknown tool, no-sources tool).
    async fn test_deps() -> (impl CasApi, ToolDownloadCache, Arc<dyn ProgressBarApi>, TempDir) {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = UserLevelCache::open(tmp.path(), "tools.json", 30 * 24 * 60 * 60)
            .await
            .expect("cache open");
        let progress: Arc<dyn ProgressBarApi> = Arc::new(RecordingTrackedHandle::new(0));
        (cas, cache, progress, tmp)
    }

    #[tokio::test]
    async fn fetch_and_import_rejects_unknown_tool() {
        let (cas, cache, progress, _tmp) = test_deps().await;
        let result =
            fetch_and_import_tool_payload(&cas, "nonexistent-tool", &cache, progress).await;
        assert!(result.is_err(), "unknown tool should return an error");
    }

    #[tokio::test]
    async fn fetch_and_import_media_tagger_returns_none() {
        let (cas, cache, progress, _tmp) = test_deps().await;
        let result = fetch_and_import_tool_payload(&cas, "media-tagger", &cache, progress).await;
        match result {
            Ok(None) => {} // expected: no sources → None
            other => panic!("media-tagger should return Ok(None), got {other:?}"),
        }
    }
}
