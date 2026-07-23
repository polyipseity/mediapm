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
    MAX_LOOKAHEAD, ResolvedSource, ResolvedToolFetch, SourceProducer, fetch_tool_sources,
    postprocess_tool_sources,
};
use mediapm_utils::progress::ProviderProgressCallback;
use tokio::sync::Semaphore;

use crate::error::MediaPmError;
use crate::output::ProgressGroupApi;
use crate::tools::downloader::ToolDownloadCache;
#[cfg(test)]
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
    /// Canonical version used for skip-if-up-to-date logic. Always set;
    /// the type is `String`, not `Option<String>`.
    pub(super) canonical_version: String,
}

/// Outcome of the pre-resolve step that determines whether a tool should be
/// provisioned or skipped.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(super) enum PreResolveOutcome {
    /// Tool should be fetched and imported normally.
    Resolved(ResolvedToolFetch, String),
    /// Tool is already provisioned at the given canonical version (skip).
    Skip {
        /// Tool identifier.
        #[allow(dead_code)]
        name: String,
        /// Canonical version that was already provisioned.
        #[allow(dead_code)]
        version: String,
    },
}

/// Returns `true` if the source producer represents an archive download.
///
/// Archive sources produce compressed payloads that require decompression
/// (e.g., `.zip`, `.tar.gz`, `.tar.xz`). Binary and launcher sources are
/// used as-is. Mirrors the logic in `mediapm-conductor/src/tools/provider/mod.rs`.
fn is_archive_source(producer: &SourceProducer) -> bool {
    match producer {
        SourceProducer::Fetch { urls } => {
            urls.first().map_or(false, |url| infer_archive_format(url).is_some())
        }
        SourceProducer::GenerateLauncher { .. } => false,
    }
}

/// Infers archive format from a URL's file extension.
///
/// Returns `Some(format)` for recognized archive extensions, or `None` for
/// binary/launcher payloads. Mirrors the logic in
/// `mediapm-conductor/src/tools/provider/mod.rs`.
fn infer_archive_format(url: &str) -> Option<&'static str> {
    let url_path = url.split('?').next().unwrap_or(url);
    let filename = url_path.trim_end_matches('/').split('/').next_back().unwrap_or(url_path);
    if filename.ends_with(".tar.xz") {
        Some("tar.xz")
    } else if filename.ends_with(".tar.gz") || filename.ends_with(".tgz") {
        Some("tar.gz")
    } else if filename.ends_with(".zip") || filename == "zip" {
        Some("zip")
    } else {
        None
    }
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
/// The resolve bar shows 1 item (one resolve call).  Fetch bar shows
/// `sources.len()` items (one per source).  Postprocess bar shows the sum
/// of per-source items: archive sources contribute 2 items (decompress +
/// compress), binary/launcher sources contribute 1 item (import).  Phase 2
/// and 3 bars are created on-demand — one before fetching, one before
/// postprocessing — so bars only appear when their phase actively runs.
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
    _metadata_cache: &ToolDownloadCache,
    group: &dyn ProgressGroupApi,
    outcome: PreResolveOutcome,
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
    let (mut fetch, canonical_version) = match outcome {
        PreResolveOutcome::Resolved(f, cv) => (f, cv),
        PreResolveOutcome::Skip { .. } => {
            // Tool is already provisioned at this version — show resolve bar
            // with "skipped" indicator, then return early.
            resolve_bar.set_position(1);
            resolve_bar.set_message("skipped");
            resolve_bar.finish_success();
            return Ok(None);
        }
    };
    // Resolve is a single operation — total stays at 1 from add_bar(1, ...).
    resolve_bar.set_position(1);
    resolve_bar.finish();

    // Phase 1b: Prefetch expected sizes via HEAD requests.
    prefetch_expected_sizes(&mut fetch.sources).await;

    if fetch.sources.is_empty() {
        // No sources to fetch — return None without error bars since
        // no bars beyond resolve were created.
        return Ok(None);
    }

    let total = fetch.sources.len() as u64;

    // Compute total postprocess items: archive sources get 2 (decompress + compress),
    // binary/launcher sources get 1 (import).
    let total_postprocess_items: u64 = fetch
        .sources
        .iter()
        .map(|s| if is_archive_source(&s.producer) { 2u64 } else { 1u64 })
        .sum();

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
    // Set fetch bar RHS message if some sources were cache-served.
    if downloaded.cached_count > 0 {
        fetch_bar.set_message(&format!("cached ({})", downloaded.cached_count));
    }
    fetch_bar.finish();

    // Phase 3: Postprocess — extract archives, repack to uncompressed ZIP,
    // import to CAS, build content map + command selector.
    let postprocess_bar = group.add_bar(total_postprocess_items, &format!("{tool_id} [process]"));
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
        canonical_version,
    }))
}

/// Sends HEAD requests to populate `expected_size` on each `Fetch`-producer
/// source.  Failures are silently ignored — `expected_size` stays `None` and
/// the existing Content-Length fallback in phase 2 applies.
///
/// Evermeet URLs are skipped because they are dynamic endpoints (return a
/// freshly-built zip per request, so HEAD Content-Length wouldn't match the
/// GET response).
///
/// Uses fully concurrent HEAD requests with a semaphore to limit concurrency.
async fn prefetch_expected_sizes(sources: &mut [ResolvedSource]) {
    let client = match crate::http_client::shared_http_client() {
        Ok(c) => c,
        Err(_) => return,
    };
    let head_timeout = std::time::Duration::from_secs(10);
    let semaphore = Arc::new(Semaphore::new(MAX_LOOKAHEAD));

    let tasks: Vec<_> = sources
        .iter()
        .enumerate()
        .filter_map(|(idx, source)| {
            let url = match &source.producer {
                SourceProducer::Fetch { urls } if !urls.is_empty() => &urls[0],
                _ => return None,
            };
            // Skip dynamic endpoints: Evermeet returns a fresh build
            // on every request, so HEAD Content-Length is meaningless.
            if url.contains("evermeet") || url.contains("getrelease") {
                return None;
            }
            let client = client.clone();
            let semaphore = semaphore.clone();
            let url = url.clone();
            Some(async move {
                let _permit = semaphore.acquire().await.expect("semaphore closed");
                let request = client.head(&url).timeout(head_timeout).send().await;
                if let Ok(response) = request {
                    if response.status().is_success() {
                        if let Some(content_length) = response.content_length() {
                            if content_length > 0 {
                                return Some((idx, content_length));
                            }
                        }
                    }
                }
                None
            })
        })
        .collect();

    let results: Vec<Option<(usize, u64)>> = futures_util::future::join_all(tasks).await;
    for result in results {
        if let Some((idx, content_length)) = result {
            sources[idx].expected_size = Some(content_length);
        }
    }
}

#[cfg(test)]
mod tests {
    use mediapm_cas::storage::in_memory::new_in_memory_cas;
    use mediapm_conductor::cache_user_level::UserLevelCache;
    use mediapm_utils::progress::recording::RecordingProgressTracker;
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[tokio::test]
    async fn fetch_and_import_rejects_unknown_tool() {
        let _cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let _cache = UserLevelCache::open(tmp.path(), "tools.json", 30 * 24 * 60 * 60)
            .await
            .expect("cache open");
        let metadata_cache = UserLevelCache::open(tmp.path(), "tool_metadata.json", 24 * 60 * 60)
            .await
            .expect("metadata cache open");
        let _tracker = RecordingProgressTracker::new();
        // Resolution is now handled before fetch_and_import_tool_payload;
        // verify that resolve_tool_fetch rejects unknown tools.
        let resolve_result =
            crate::tools::provider::resolve_tool_fetch("nonexistent-tool", Some(&metadata_cache))
                .await;
        assert!(resolve_result.is_err(), "resolve_tool_fetch should reject unknown tools");
    }

    #[tokio::test]
    async fn fetch_and_import_generate_launcher_succeeds() {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = UserLevelCache::open(tmp.path(), "tools.json", 30 * 24 * 60 * 60)
            .await
            .expect("cache open");
        let metadata_cache = UserLevelCache::open(tmp.path(), "tool_metadata.json", 24 * 60 * 60)
            .await
            .expect("metadata cache open");
        let tracker = RecordingProgressTracker::new();
        let (fetch, canonical) =
            crate::tools::provider::resolve_tool_fetch("media-tagger", Some(&metadata_cache))
                .await
                .unwrap();
        let outcome = PreResolveOutcome::Resolved(fetch, canonical);
        let result = fetch_and_import_tool_payload(
            &cas,
            "media-tagger",
            &cache,
            &metadata_cache,
            &tracker,
            outcome,
        )
        .await;
        match result {
            Ok(Some(payload)) => {
                // GenerateLauncher returns 3 inline sources (windows/macos/linux).
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
                assert!(
                    !payload.canonical_version.is_empty(),
                    "canonical_version should be populated"
                );
            }
            Ok(None) => panic!("media-tagger should return Ok(Some(...)), got Ok(None)"),
            Err(e) => panic!("media-tagger should succeed, got Err({e:?})"),
        }
    }

    #[tokio::test]
    async fn fetch_and_import_ytdlp_full_pipeline() {
        // Full 3-phase pipeline (resolve → fetch → postprocess) for a tool
        // with URL-based Fetch sources. Uses wiremock to serve download
        // payloads and pre-seeds the metadata cache for tag resolution.

        // Start a wiremock server for controlled HTTP responses.
        let mock_server = MockServer::start().await;
        let binaries = vec![
            ("yt-dlp.exe", &b"fake yt-dlp windows binary"[..]),
            ("yt-dlp_macos", &b"fake yt-dlp macos binary"[..]),
            ("yt-dlp_linux", &b"fake yt-dlp linux binary"[..]),
        ];
        for (filename, bytes) in &binaries {
            Mock::given(method("GET"))
                .and(path(&format!("/{filename}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_bytes(bytes.to_vec())
                        .insert_header("Content-Length", bytes.len().to_string()),
                )
                .mount(&mock_server)
                .await;
        }

        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = UserLevelCache::open(tmp.path(), "tools.json", 30 * 24 * 60 * 60)
            .await
            .expect("cache open");
        let metadata_cache = UserLevelCache::open(tmp.path(), "tool_metadata.json", 24 * 60 * 60)
            .await
            .expect("metadata cache open");
        let tracker = RecordingProgressTracker::new();

        // Pre-seed the metadata cache with a stable tag string (no network).
        let tag = "2025.07.15";
        let api_key = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";
        metadata_cache.store_bytes(api_key, tag.as_bytes()).await;

        // Resolve normally — metadata cache returns the pre-seeded tag.
        let (mut fetch, canonical) =
            crate::tools::provider::resolve_tool_fetch("yt-dlp", Some(&metadata_cache))
                .await
                .unwrap();

        // Patch download URLs to point at wiremock (so HEAD prefetch and
        // download hit the local server instead of GitHub).
        for source in &mut fetch.sources {
            if let SourceProducer::Fetch { urls } = &mut source.producer {
                for url in urls.iter_mut() {
                    let filename = url.rsplit('/').next().unwrap_or(url);
                    *url =
                        format!("http://127.0.0.1:{}/{}", mock_server.address().port(), filename);
                }
            }
        }

        let outcome = PreResolveOutcome::Resolved(fetch, canonical);
        let result = fetch_and_import_tool_payload(
            &cas,
            "yt-dlp",
            &cache,
            &metadata_cache,
            &tracker,
            outcome,
        )
        .await;
        let filenames: Vec<&str> = binaries.iter().map(|(n, _)| *n).collect();
        let os_labels = ["windows", "macos", "linux"];
        match result {
            Ok(Some(payload)) => {
                assert_eq!(
                    payload.content_map.len(),
                    3,
                    "expected 3 content-map entries for yt-dlp"
                );
                assert_eq!(payload.os_exec_paths.len(), 3, "expected 3 OS exec paths for yt-dlp");
                for (os, filename) in os_labels.iter().zip(filenames.iter()) {
                    let key = format!("{os}/{filename}");
                    assert!(
                        payload.content_map.contains_key(&key),
                        "missing {key} in content_map for yt-dlp"
                    );
                    assert_eq!(
                        payload.os_exec_paths.get(*os),
                        Some(&filename.to_string()),
                        "{os} exec path mismatch for yt-dlp"
                    );
                }
                assert!(
                    !payload.canonical_version.is_empty(),
                    "canonical_version should be populated"
                );
                assert_eq!(payload.canonical_version, "2025.07.15");
            }
            Ok(None) => panic!("yt-dlp should return Ok(Some(...)), got Ok(None)"),
            Err(e) => panic!("yt-dlp should succeed, got Err({e:?})"),
        }
    }

    #[tokio::test]
    async fn fetch_and_import_with_pre_resolved_canonical_version() {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = UserLevelCache::open(tmp.path(), "tools.json", 30 * 24 * 60 * 60)
            .await
            .expect("cache open");
        let metadata_cache = UserLevelCache::open(tmp.path(), "tool_metadata.json", 24 * 60 * 60)
            .await
            .expect("metadata cache open");
        let tracker = RecordingProgressTracker::new();

        // Use media_tagger's sources as a known ResolvedToolFetch.
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(fetch, "test-canonical".to_string());

        let result = fetch_and_import_tool_payload(
            &cas,
            "media-tagger",
            &cache,
            &metadata_cache,
            &tracker,
            outcome,
        )
        .await;
        match result {
            Ok(Some(payload)) => {
                assert_eq!(
                    payload.canonical_version, "test-canonical",
                    "pre-resolved canonical_version should be threaded through",
                );
                assert_eq!(payload.content_map.len(), 3, "expected 3 content-map entries");
            }
            Ok(None) => panic!("media-tagger should return Ok(Some(...)), got Ok(None)"),
            Err(e) => panic!("media-tagger should succeed, got Err({e:?})"),
        }
    }

    #[test]
    fn infer_archive_format_recognises_zip() {
        assert!(infer_archive_format("https://example.com/file.zip").is_some());
        assert!(infer_archive_format("https://example.com/file.zip?query=1").is_some());
    }

    #[test]
    fn infer_archive_format_recognises_tar_gz() {
        assert!(infer_archive_format("https://example.com/file.tar.gz").is_some());
        assert!(infer_archive_format("https://example.com/file.tgz").is_some());
    }

    #[test]
    fn infer_archive_format_recognises_tar_xz() {
        assert!(infer_archive_format("https://example.com/file.tar.xz").is_some());
    }

    #[test]
    fn infer_archive_format_rejects_binary_urls() {
        assert!(infer_archive_format("https://example.com/ffmpeg-linux-amd64").is_none());
        assert!(infer_archive_format("https://example.com/ffmpeg.exe").is_none());
    }

    #[test]
    fn infer_archive_format_empty_url_returns_none() {
        assert!(infer_archive_format("").is_none());
    }

    #[test]
    fn is_archive_source_fetch_zip_returns_true() {
        let producer =
            SourceProducer::Fetch { urls: vec!["https://example.com/tool.zip".to_string()] };
        assert!(is_archive_source(&producer));
    }

    #[test]
    fn is_archive_source_fetch_binary_returns_false() {
        let producer = SourceProducer::Fetch {
            urls: vec!["https://example.com/tool-linux-amd64".to_string()],
        };
        assert!(!is_archive_source(&producer));
    }

    #[test]
    fn is_archive_source_no_urls_returns_false() {
        let producer = SourceProducer::Fetch { urls: vec![] };
        assert!(!is_archive_source(&producer));
    }

    #[test]
    fn is_archive_source_launcher_returns_false() {
        let producer = SourceProducer::GenerateLauncher { builtin_id: "test".to_string() };
        assert!(!is_archive_source(&producer));
    }

    #[test]
    fn total_postprocess_items_three_sources_three_archives() {
        let fetch = ResolvedToolFetch {
            tool_id: "ffmpeg".to_string(),
            sources: vec![
                ResolvedSource {
                    os: "linux".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/ffmpeg-linux.zip".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                },
                ResolvedSource {
                    os: "macos".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/ffmpeg-macos.zip".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                },
                ResolvedSource {
                    os: "windows".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/ffmpeg-windows.zip".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                },
            ],
        };
        let total: u64 = fetch
            .sources
            .iter()
            .map(|s| if is_archive_source(&s.producer) { 2u64 } else { 1u64 })
            .sum();
        assert_eq!(total, 6, "3 archive sources should produce 6 postprocess items");
    }

    #[test]
    fn total_postprocess_items_mixed_archives_and_binaries() {
        let fetch = ResolvedToolFetch {
            tool_id: "mixed".to_string(),
            sources: vec![
                ResolvedSource {
                    os: "linux".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/tool-linux.zip".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                },
                ResolvedSource {
                    os: "macos".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/tool-macos".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                },
                ResolvedSource {
                    os: "windows".to_string(),
                    producer: SourceProducer::GenerateLauncher { builtin_id: "test".to_string() },
                    expected_size: None,
                    size_hint_bytes: None,
                },
            ],
        };
        let total: u64 = fetch
            .sources
            .iter()
            .map(|s| if is_archive_source(&s.producer) { 2u64 } else { 1u64 })
            .sum();
        // 1 archive (2) + 1 binary (1) + 1 launcher (1) = 4
        assert_eq!(total, 4, "mixed sources should produce correct postprocess total");
    }
}
