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
use mediapm_conductor::tools::provider::{
    ResolvedSource, SourceProducer, fetch_tool_sources, postprocess_tool_sources,
};
use mediapm_utils::progress::ProviderProgressCallback;

use crate::error::MediaPmError;
use crate::output::ProgressGroupApi;
use crate::tools::downloader::ToolDownloadCache;
use crate::tools::provider;

/// Result of fetching and importing a tool payload into CAS.
#[derive(Debug, Clone)]
pub(super) struct FetchedToolPayload {
    /// Content map: sandbox-relative path → CAS hash hex string.
    pub(super) content_map: BTreeMap<String, String>,
    /// Per-OS executable path map (OS label → relative executable path
    /// without OS prefix). Passed to the preset layer to build the command
    /// selector template.
    pub(super) os_exec_paths: BTreeMap<String, String>,
}

/// Fetches a tool payload for **all** platforms, extracts each to a
/// per-OS temp directory, imports files to CAS with `./{os}/` key prefixes,
/// and builds an OS-conditional command-selector template.
///
/// `group` provides 3 phase-agnostic progress bars per tool (resolve, fetch,
/// postprocess). Routes [`ProviderProgressSnapshot`] callbacks to the matching
/// bar by `snap.phase`. Item counters are displayed via `set_prefix`; byte
/// counters drive bar position (`set_position`/`set_total`). The bridge does
/// not interpret the meaning of items or bytes — it only relays the values
/// to the bar.
///
/// After phase 1 (resolve), a HEAD-prefetch step populates
/// [`ResolvedSource.expected_size`] for each `Fetch`-producer source so
/// phase 2 progress bars start with an accurate byte total. Evermeet and
/// getrelease URLs are skipped (dynamic endpoints).
///
/// The resolve bar's position and total are set to `fetch.total_items`
/// before finishing, so it shows the correct count. Phase 2 and 3 bars are
/// created on-demand — one before fetching, one before postprocessing — so
/// bars only appear when their phase actively runs.
///
/// `metadata_cache` is passed to the resolve phase for caching version/tag
/// resolution results. The consumer must NOT call `touch()` on the metadata
/// cache — its TTL is creation-time-based.
///
/// Returns `Ok(None)` when the tool has no provider sources.
pub(super) async fn fetch_and_import_tool_payload(
    cas: &impl CasApi,
    tool_id: &str,
    cache: &ToolDownloadCache,
    metadata_cache: &ToolDownloadCache,
    group: &dyn ProgressGroupApi,
) -> Result<Option<FetchedToolPayload>, MediaPmError> {
    // Track created bars so we can mark them red on error.
    let mut error_bars: Vec<Arc<dyn crate::output::ProgressBarApi>> = Vec::new();

    // Helper to mark all tracked bars as errored before returning Err.
    let finish_error_bars = |bars: &[Arc<dyn crate::output::ProgressBarApi>]| {
        for bar in bars {
            bar.finish_error();
        }
    };

    // Phase 1: Resolve — get source descriptors from the mediapm provider.
    let resolve_bar = group.add_bar(1, &format!("{tool_id} [resolve]"));
    error_bars.push(resolve_bar.clone());
    let mut fetch = match provider::resolve_tool_fetch(tool_id, Some(metadata_cache)).await {
        Ok(fetch) => fetch,
        Err(e) => {
            finish_error_bars(&error_bars);
            return Err(MediaPmError::Workflow(format!("tool {tool_id}: resolve failed: {e}")));
        }
    };
    resolve_bar.set_position(fetch.total_items);
    resolve_bar.set_total(fetch.total_items);
    resolve_bar.finish();

    // Phase 1b: Prefetch expected sizes via HEAD requests.
    prefetch_expected_sizes(&mut fetch.sources).await;

    if fetch.sources.is_empty() {
        // No sources to fetch — return None without error bars since
        // no bars beyond resolve were created.
        return Ok(None);
    }

    let total = fetch.sources.len() as u64;

    // Phase 2: Fetch — download (or generate) bytes for each source.
    let fetch_bar = group.add_bar(total, &format!("{tool_id} [fetch]"));
    error_bars.push(fetch_bar.clone());
    let fetch_bar_cb = fetch_bar.clone();
    let fetch_tool_id = tool_id.to_string();
    let fetch_progress: Option<ProviderProgressCallback> = Some(Arc::new(move |snap| {
        fetch_bar_cb
            .set_prefix(&format!("{fetch_tool_id} [fetch] {}/{}", snap.items.0, snap.items.1));
        fetch_bar_cb.set_position(snap.bytes.0);
        fetch_bar_cb.set_total(snap.bytes.1);
    }));
    let downloaded = match fetch_tool_sources(&fetch, cache, fetch_progress).await {
        Ok(d) => d,
        Err(e) => {
            finish_error_bars(&error_bars);
            return Err(MediaPmError::Workflow(format!("tool {tool_id}: fetch failed: {e}")));
        }
    };
    fetch_bar.finish();

    // Phase 3: Postprocess — extract archives, repack to uncompressed ZIP,
    // import to CAS, build content map + command selector.
    let postprocess_bar = group.add_bar(total, &format!("{tool_id} [process]"));
    error_bars.push(postprocess_bar.clone());
    let postprocess_bar_cb = postprocess_bar.clone();
    let pp_tool_id = tool_id.to_string();
    let pp_progress: Option<ProviderProgressCallback> = Some(Arc::new(move |snap| {
        postprocess_bar_cb
            .set_prefix(&format!("{pp_tool_id} [process] {}/{}", snap.items.0, snap.items.1));
        postprocess_bar_cb.set_position(snap.bytes.0);
        postprocess_bar_cb.set_total(snap.bytes.1);
    }));
    let result = match postprocess_tool_sources(&downloaded, cas, pp_progress).await {
        Ok(r) => r,
        Err(e) => {
            finish_error_bars(&error_bars);
            return Err(MediaPmError::Workflow(format!("tool {tool_id}: postprocess failed: {e}")));
        }
    };
    postprocess_bar.finish();

    Ok(Some(FetchedToolPayload {
        content_map: result.content_map,
        os_exec_paths: result.os_exec_paths,
    }))
}

/// Sends HEAD requests to populate `expected_size` on each `Fetch`-producer
/// source.  Failures are silently ignored — `expected_size` stays `None` and
/// the existing Content-Length fallback in phase 2 applies.
///
/// Evermeet URLs are skipped because they are dynamic endpoints (return a
/// freshly-built zip per request, so HEAD Content-Length wouldn't match the
/// GET response).
async fn prefetch_expected_sizes(sources: &mut [ResolvedSource]) {
    let client = match crate::http_client::shared_http_client() {
        Ok(c) => c,
        Err(_) => return,
    };
    let head_timeout = std::time::Duration::from_secs(10);

    for source in sources.iter_mut() {
        let url = match &source.producer {
            SourceProducer::Fetch { urls } if !urls.is_empty() => &urls[0],
            _ => continue,
        };
        // Skip dynamic endpoints: Evermeet returns a fresh build
        // on every request, so HEAD Content-Length is meaningless.
        if url.contains("evermeet") || url.contains("getrelease") {
            continue;
        }
        let request = client.head(url).timeout(head_timeout).send().await;
        if let Ok(response) = request {
            if response.status().is_success() {
                if let Some(content_length) = response.content_length() {
                    if content_length > 0 {
                        source.expected_size = Some(content_length);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mediapm_cas::storage::in_memory::new_in_memory_cas;
    use mediapm_conductor::cache_user_level::UserLevelCache;
    use mediapm_utils::progress::recording::RecordingProgressTracker;
    use tempfile::TempDir;

    use super::*;

    /// Helper to create the minimal dependencies for tests that exercise
    /// paths not reaching the cache or CAS (unknown tool, no-sources tool).
    async fn test_deps()
    -> (impl CasApi, ToolDownloadCache, ToolDownloadCache, RecordingProgressTracker, TempDir) {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = UserLevelCache::open(tmp.path(), "tools.json", 30 * 24 * 60 * 60)
            .await
            .expect("cache open");
        let metadata_cache = UserLevelCache::open(tmp.path(), "tool_metadata.json", 24 * 60 * 60)
            .await
            .expect("metadata cache open");
        let tracker = RecordingProgressTracker::new();
        (cas, cache, metadata_cache, tracker, tmp)
    }

    #[tokio::test]
    async fn fetch_and_import_rejects_unknown_tool() {
        let (cas, cache, metadata_cache, tracker, _tmp) = test_deps().await;
        let result = fetch_and_import_tool_payload(
            &cas,
            "nonexistent-tool",
            &cache,
            &metadata_cache,
            &tracker,
        )
        .await;
        assert!(result.is_err(), "unknown tool should return an error");
    }

    #[tokio::test]
    async fn fetch_and_import_media_tagger_succeeds() {
        let (cas, cache, metadata_cache, tracker, _tmp) = test_deps().await;
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &metadata_cache, &tracker)
                .await;
        match result {
            Ok(Some(payload)) => {
                // media-tagger now returns 3 GenerateLauncher sources (windows/macos/linux).
                assert_eq!(payload.content_map.len(), 3, "expected 3 content-map entries");
                assert_eq!(payload.os_exec_paths.len(), 3, "expected 3 OS exec paths");
                assert!(
                    payload.content_map.contains_key("windows/media-tagger"),
                    "missing windows/media-tagger in content_map"
                );
                assert!(
                    payload.content_map.contains_key("macos/media-tagger"),
                    "missing macos/media-tagger in content_map"
                );
                assert!(
                    payload.content_map.contains_key("linux/media-tagger"),
                    "missing linux/media-tagger in content_map"
                );
                assert_eq!(
                    payload.os_exec_paths.get("windows"),
                    Some(&"media-tagger".to_string()),
                    "windows exec path mismatch"
                );
                assert_eq!(
                    payload.os_exec_paths.get("macos"),
                    Some(&"media-tagger".to_string()),
                    "macos exec path mismatch"
                );
                assert_eq!(
                    payload.os_exec_paths.get("linux"),
                    Some(&"media-tagger".to_string()),
                    "linux exec path mismatch"
                );
            }
            Ok(None) => panic!("media-tagger should return Ok(Some(...)), got Ok(None)"),
            Err(e) => panic!("media-tagger should succeed, got Err({e:?})"),
        }
    }
}
