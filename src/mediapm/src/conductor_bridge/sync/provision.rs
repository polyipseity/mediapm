//! Tool payload provisioning — thin wrapper around conductor's 3-phase pipeline.
//!
//! Delegates to the mediapm [`provider`](crate::tools::provider) module for
//! phase 1 (resolve), then to the conductor's
//! [`fetch_tool_sources`](mediapm_conductor::tools::provider::fetch_tool_sources)
//! for phase 2 (fetch) and
//! [`process_tool_sources`](mediapm_conductor::tools::provider::process_tool_sources)
//! for phase 3 (extract → CAS import → content map). This module adapts
//! the mediapm progress-bar API to the provider's callback-based progress
//! reporting and converts error and result types.

use std::collections::BTreeMap;
use std::sync::Arc;

use mediapm_cas::CasApi;
use mediapm_conductor::tools::provider::{
    MAX_LOOKAHEAD, ResolvedSource, ResolvedToolFetch, SourceProducer, fetch_tool_sources,
    process_tool_sources,
};
use mediapm_utils::progress::{PrefixComponents, ProviderProgressCallback};
use tokio::sync::Semaphore;

use crate::error::MediaPmError;
use crate::output::ProgressGroupApi;
use crate::tools::downloader::ToolDownloadCache;
#[cfg(test)]
use crate::tools::provider;
#[cfg(test)]
use crate::tools::provider::RecheckPolicy;

/// Result of fetching and importing a tool payload into CAS.
#[derive(Debug, Clone)]
pub(super) struct FetchedToolPayload {
    /// Content map: sandbox-relative path → CAS hash hex string.
    pub(super) content_map: BTreeMap<String, String>,
    /// Per-OS executable path map (OS label → relative executable path
    /// without OS prefix). Passed to the preset layer to build the command
    /// selector template.
    pub(super) os_exec_paths: BTreeMap<String, String>,
    /// Human-readable version string (informational only — has zero semantic
    /// use in state logic). Provider-defined format; no prefix stripping or
    /// normalization is performed.
    pub(super) human_readable_version: String,
    /// Canonical version used for skip-if-up-to-date logic. Always set;
    /// the type is `String`, not `Option<String>`.
    pub(super) canonical_version: String,
}

/// Outcome of the pre-resolve step that determines whether a tool should be
/// provisioned or skipped.
#[derive(Debug, Clone)]
pub(super) enum PreResolveOutcome {
    /// Tool should be fetched and imported normally.
    /// Fields: (ResolvedToolFetch, human_readable_version, canonical_version, metadata_cached, metadata_fetch_count, resolved_tag)
    Resolved(ResolvedToolFetch, String, String, bool, u32, String),
    /// Tool is already provisioned at the given canonical version (skip).
    Skip {
        /// Tool identifier.
        #[allow(dead_code)]
        name: String,
        /// Human-readable version string (informational only).
        #[allow(dead_code)]
        human_readable_version: String,
        /// Canonical version that was already provisioned.
        #[allow(dead_code)]
        version: String,
        /// Whether the version/tag lookups were served from metadata cache.
        metadata_cached: bool,
        /// Number of individual version/tag lookups performed (e.g., ffmpeg = 2, all others = 1).
        metadata_fetch_count: u32,
        /// The git tag resolved from the provider (empty string when no tag).
        #[allow(dead_code)]
        resolved_tag: String,
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
/// `group` provides 3 phase-agnostic progress bars per tool (res, fch,
/// pro). Routes [`ProviderProgressSnapshot`] callbacks to the matching
/// bar by `snap.phase`. Item counters are displayed via `set_prefix_components`; byte
/// counters drive bar position (`set_position`/`set_total`). The bridge does
/// not interpret the meaning of items or bytes — it only relays the values
/// to the bar.
///
/// Bar labels follow the format `{tool_id}{version_suffix} [{phase_abbr}]`
/// where `version_suffix` is ` {human_readable_version}` when non-empty
/// (e.g., `"ffmpeg v7.1 [res]"`) or empty when the version is blank
/// (e.g., `"media-tagger [fch]"`). Phase abbreviations: `[res]`, `[fch]`,
/// `[pro]`.
///
/// After phase 1 (resolve), a HEAD-prefetch step populates
/// [`ResolvedSource.expected_size`] for each `Fetch`-producer source so
/// phase 2 progress bars start with an accurate byte total. Evermeet and
/// getrelease URLs are skipped (dynamic endpoints).
///
/// The resolve bar shows `metadata_fetch_count` items (one per metadata lookup,
/// e.g., ffmpeg has 2: btbn tag + evermeet version). When all metadata lookups
/// were cache hits, the bar shows `"cached (N)"` where N = `metadata_fetch_count`.
/// When the tool is already up-to-date (Skip), the bar shows `"skipped cached (N)"`
/// if cached or `"skipped"` otherwise. Fetch bar shows
/// `sources.len()` items (one per source).  Process bar shows the sum
/// of per-source items: archive sources contribute 2 items (decompress +
/// compress), binary/launcher sources contribute 1 item (import).  Phase 2
/// and 3 bars are created on-demand — one before fetching, one before
/// processing — so bars only appear when their phase actively runs.
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
    let version_for_label = match &outcome {
        PreResolveOutcome::Resolved(_, hr, ..) => hr.as_str(),
        PreResolveOutcome::Skip { human_readable_version, .. } => human_readable_version.as_str(),
    };
    let version_suffix =
        if version_for_label.is_empty() { String::new() } else { format!(" {version_for_label}") };
    let metadata_fetch_count = match &outcome {
        PreResolveOutcome::Resolved(_, _, _, _, count, _) => *count,
        PreResolveOutcome::Skip { metadata_fetch_count, .. } => *metadata_fetch_count,
    };
    let bar_total = metadata_fetch_count;
    let resolve_bar = group.add_bar(bar_total.into(), &format!("{tool_id}{version_suffix} [res]"));
    error_bars.push(resolve_bar.clone());
    let (mut fetch, human_readable_version, canonical_version) = match outcome {
        PreResolveOutcome::Resolved(
            f,
            hr,
            cv,
            metadata_cached,
            metadata_fetch_count,
            _resolved_tag,
        ) => {
            if metadata_cached {
                resolve_bar.set_suffix(&format!("cached ({metadata_fetch_count})"));
            }
            resolve_bar.set_position(bar_total.into());
            resolve_bar.finish();
            (f, hr, cv)
        }
        PreResolveOutcome::Skip {
            human_readable_version: _,
            metadata_cached,
            metadata_fetch_count,
            ..
        } => {
            // Tool is already provisioned at this version — show resolve bar
            // with "skipped" indicator, then return early.
            resolve_bar.set_position(bar_total.into());
            if metadata_cached {
                resolve_bar.set_suffix(&format!("skipped cached ({metadata_fetch_count})"));
            } else {
                resolve_bar.set_suffix("skipped");
            }
            resolve_bar.finish_success();
            return Ok(None);
        }
    };

    // Phase 1b: Prefetch expected sizes via HEAD requests.
    prefetch_expected_sizes(&mut fetch.sources).await;

    if fetch.sources.is_empty() {
        // No sources to fetch — return None without error bars since
        // no bars beyond resolve were created.
        return Ok(None);
    }

    let total = fetch.sources.len() as u64;

    // Compute total process items: archive sources get 2 (decompress + compress),
    // binary/launcher sources get 1 (import).
    let total_process_items: u64 = fetch
        .sources
        .iter()
        .map(|s| if is_archive_source(&s.producer) { 2u64 } else { 1u64 })
        .sum();

    // Phase 2: Fetch — download (or generate) bytes for each source.
    let fetch_bar = group.add_bar(total, &format!("{tool_id}{version_suffix} [fch]"));
    error_bars.push(fetch_bar.clone());
    let fetch_bar_cb = fetch_bar.clone();
    let fetch_tool_id = tool_id.to_string();
    let fetch_version_suffix = version_suffix.clone();
    let fetch_progress: Option<ProviderProgressCallback> = Some(Arc::new(move |snap| {
        fetch_bar_cb.set_prefix_components(PrefixComponents {
            tool_name: fetch_tool_id.clone(),
            version: fetch_version_suffix.trim().to_string(),
            phase: "fch".to_string(),
            count: format!("{}/{}", snap.items.0, snap.items.1),
        });
        fetch_bar_cb.set_position(snap.bytes.0);
        fetch_bar_cb.set_total(snap.bytes.1);
    }));
    let downloaded = match fetch_tool_sources(&fetch, cache, "tools", fetch_progress).await {
        Ok(d) => d,
        Err(e) => {
            finish_error_bars(&error_bars);
            return Err(MediaPmError::Workflow(format!("tool {tool_id}: fetch failed: {e}")));
        }
    };
    // Set fetch bar RHS message if some sources were cache-served.
    if downloaded.cached_count > 0 {
        fetch_bar.set_suffix(&format!("cached ({})", downloaded.cached_count));
    }
    fetch_bar.finish();

    // Phase 3: Process — extract archives, repack to uncompressed ZIP,
    // import to CAS, build content map + command selector.
    // The bar total is set to total_process_items (item count). This is
    // intentional — the byte-level total isn't known until the budget is
    // populated with source byte sizes. The budget starts with item count
    // as the aggregate total and refines to actual payload sizes as each
    // source begins processing.
    let process_bar =
        group.add_bar(total_process_items, &format!("{tool_id}{version_suffix} [pro]"));
    error_bars.push(process_bar.clone());
    let process_bar_cb = process_bar.clone();
    let pp_tool_id = tool_id.to_string();
    let pp_version_suffix = version_suffix.clone();
    let pp_progress: Option<ProviderProgressCallback> = Some(Arc::new(move |snap| {
        process_bar_cb.set_prefix_components(PrefixComponents {
            tool_name: pp_tool_id.clone(),
            version: pp_version_suffix.trim().to_string(),
            phase: "pro".to_string(),
            count: format!("{}/{}", snap.items.0, snap.items.1),
        });
        process_bar_cb.set_position(snap.bytes.0);
        process_bar_cb.set_total(snap.bytes.1);
    }));
    let result = match process_tool_sources(&downloaded, cas, pp_progress).await {
        Ok(r) => r,
        Err(e) => {
            finish_error_bars(&error_bars);
            return Err(MediaPmError::Workflow(format!("tool {tool_id}: process failed: {e}")));
        }
    };
    process_bar.finish();

    Ok(Some(FetchedToolPayload {
        content_map: result.content_map,
        os_exec_paths: result.os_exec_paths,
        human_readable_version,
        canonical_version,
    }))
}

/// # HTTP client policy
///
/// Uses the process-wide shared client from [`crate::http_client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
///
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
    let client = match mediapm_conductor::http::client::shared_http_client() {
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
    use mediapm_conductor::cache::Cache;
    use mediapm_conductor::cache::CacheDomainConfig;
    use mediapm_conductor::cache_user_level::UserLevelCache;
    use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[tokio::test]
    async fn fetch_and_import_rejects_unknown_tool() {
        let _cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let _tracker = RecordingProgressTracker::new();
        // Resolution is now handled before fetch_and_import_tool_payload;
        // verify that resolve_tool_fetch rejects unknown tools.
        let resolve_result = crate::tools::provider::resolve_tool_fetch(
            "nonexistent-tool",
            Some((&*cache, "tool_metadata")),
            RecheckPolicy::default(),
        )
        .await;
        assert!(resolve_result.is_err(), "resolve_tool_fetch should reject unknown tools");
    }

    #[tokio::test]
    async fn fetch_and_import_generate_launcher_succeeds() {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let (
            fetch,
            _human_readable,
            canonical,
            _metadata_cached,
            _metadata_fetch_count,
            _resolved_tag,
        ) = crate::tools::provider::resolve_tool_fetch(
            "media-tagger",
            Some((&*cache, "tool_metadata")),
            RecheckPolicy::default(),
        )
        .await
        .unwrap();
        let outcome =
            PreResolveOutcome::Resolved(fetch, String::new(), canonical, false, 1, String::new());
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
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
        // Full 3-phase pipeline (resolve → fetch → process) for a tool
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
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();

        // Pre-seed the metadata cache with a stable tag+hash (no network).
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag.
        let tag = "2025.07.15";
        let hash = "fdec00e0bf530dc6c3cc7b1dd780e95d9ae460e9";
        let api_key = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";
        cache.store_bytes("tool_metadata", api_key, format!("{tag}\n{hash}").as_bytes()).await;

        // Resolve normally — metadata cache returns the pre-seeded tag.
        let (
            mut fetch,
            _human_readable,
            canonical,
            _metadata_cached,
            _metadata_fetch_count,
            _resolved_tag,
        ) = crate::tools::provider::resolve_tool_fetch(
            "yt-dlp",
            Some((&*cache, "tool_metadata")),
            RecheckPolicy::default(),
        )
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

        let outcome =
            PreResolveOutcome::Resolved(fetch, String::new(), canonical, false, 1, String::new());
        let result = fetch_and_import_tool_payload(&cas, "yt-dlp", &cache, &tracker, outcome).await;
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
                assert_eq!(
                    payload.canonical_version, "fdec00e0bf530dc6c3cc7b1dd780e95d9ae460e9",
                    "canonical version should be the git commit hash",
                );
            }
            Ok(None) => panic!("yt-dlp should return Ok(Some(...)), got Ok(None)"),
            Err(e) => panic!("yt-dlp should succeed, got Err({e:?})"),
        }
    }

    #[tokio::test]
    async fn fetch_and_import_with_pre_resolved_canonical_version() {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();

        // Use media_tagger's sources as a known ResolvedToolFetch.
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            String::new(),
            "test-canonical".to_string(),
            false,
            1,
            String::new(),
        );

        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
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

    // -- Phase 3: metadata_cached / metadata_fetch_count progress bar tests --

    #[tokio::test]
    async fn resolve_bar_shows_cached_when_metadata_cached() {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            String::new(),
            "v1.0.0".to_string(),
            true,
            1,
            String::new(),
        );
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        assert!(
            ops.iter()
                .any(|op| matches!(op, ProgressOp::SetSuffix { suffix } if suffix == "cached (1)")),
            "expected SetSuffix(\"cached (1)\") in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn resolve_bar_shows_total_two_when_metadata_fetch_count_two() {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            String::new(),
            "v1.0.0".to_string(),
            false,
            2,
            String::new(),
        );
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::AddBar { total: 2, .. })),
            "expected AddBar total=2 in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::SetPosition { pos: 2 })),
            "expected SetPosition pos=2 in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn skip_bar_shows_skipped_cached_when_metadata_cached() {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let outcome = PreResolveOutcome::Skip {
            name: "test-tool".to_string(),
            human_readable_version: String::new(),
            version: "v1.0.0".to_string(),
            metadata_cached: true,
            metadata_fetch_count: 1,
            resolved_tag: String::new(),
        };
        let result =
            fetch_and_import_tool_payload(&cas, "test-tool", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "Skip should return Ok");
        assert!(result.unwrap().is_none(), "Skip should return Ok(None)");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::SetSuffix { suffix } if suffix == "skipped cached (1)")),
            "expected SetSuffix(\"skipped cached (1)\") in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(|op| *op == ProgressOp::FinishSuccess),
            "expected FinishSuccess in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn skip_bar_shows_skipped_when_metadata_not_cached() {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let outcome = PreResolveOutcome::Skip {
            name: "test-tool".to_string(),
            human_readable_version: String::new(),
            version: "v1.0.0".to_string(),
            metadata_cached: false,
            metadata_fetch_count: 1,
            resolved_tag: String::new(),
        };
        let result =
            fetch_and_import_tool_payload(&cas, "test-tool", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "Skip should return Ok");
        assert!(result.unwrap().is_none(), "Skip should return Ok(None)");

        let ops = tracker.ops();
        assert!(
            ops.iter()
                .any(|op| matches!(op, ProgressOp::SetSuffix { suffix } if suffix == "skipped")),
            "expected SetSuffix(\"skipped\") in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(|op| *op == ProgressOp::FinishSuccess),
            "expected FinishSuccess in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn resolve_bar_no_cached_message_when_not_cached() {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            String::new(),
            "v1.0.0".to_string(),
            false,
            1,
            String::new(),
        );
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        assert!(
            !ops.iter()
                .any(|op| matches!(op, ProgressOp::SetSuffix { suffix } if suffix == "cached")),
            "unexpected SetSuffix(\"cached\") in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn resolve_bar_zero_metadata_fetch_count_uses_min_one() {
        // Regression: resolve bar with metadata_fetch_count=0 gets total=0
        // (indeterminate bar — set_suffix works correctly after disabled flag
        // was moved out of the total==0 proxy check).
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            String::new(),
            "v1.0.0".to_string(),
            false,
            0,
            String::new(),
        );
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::AddBar { total: 0, .. })),
            "expected AddBar total=0 in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::SetPosition { pos: 0 })),
            "expected SetPosition pos=0 in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn resolve_bar_cached_two_shows_cached_two() {
        // Regression: resolve bar with metadata_cached=true and
        // metadata_fetch_count=2 (e.g., ffmpeg btbn + evermeet) must show
        // "cached (2)" (not bare "cached" without count).
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            String::new(),
            "v1.0.0".to_string(),
            true,
            2,
            String::new(),
        );
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::AddBar { total: 2, .. })),
            "expected AddBar total=2 in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::SetPosition { pos: 2 })),
            "expected SetPosition pos=2 in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter()
                .any(|op| matches!(op, ProgressOp::SetSuffix { suffix } if suffix == "cached (2)")),
            "expected SetSuffix(\"cached (2)\") in ops\ngot: {ops:#?}",
        );
        // Also verify bare "cached" (without count) never appears.
        assert!(
            !ops.iter()
                .any(|op| matches!(op, ProgressOp::SetSuffix { suffix } if suffix == "cached")),
            "unexpected SetSuffix(\"cached\") in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn skip_bar_shows_skipped_cached_two() {
        // Regression: skip bar with metadata_cached=true and
        // metadata_fetch_count=2 must show "skipped cached (2)" and fill the
        // bar to position = total.
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let outcome = PreResolveOutcome::Skip {
            name: "test-tool".to_string(),
            human_readable_version: "v1.0.0".to_string(),
            version: "v1.0.0".to_string(),
            metadata_cached: true,
            metadata_fetch_count: 2,
            resolved_tag: String::new(),
        };
        let result =
            fetch_and_import_tool_payload(&cas, "test-tool", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "Skip should return Ok");
        assert!(result.unwrap().is_none(), "Skip should return Ok(None)");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::AddBar { total: 2, .. })),
            "expected AddBar total=2 in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::SetPosition { pos: 2 })),
            "expected SetPosition pos=2 in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(
                |op| matches!(op, ProgressOp::SetSuffix { suffix } if suffix == "skipped cached (2)")
            ),
            "expected SetSuffix(\"skipped cached (2)\") in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(|op| *op == ProgressOp::FinishSuccess),
            "expected FinishSuccess in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn skip_bar_zero_metadata_fetch_count_uses_min_one() {
        // Regression: skip bar with metadata_fetch_count=0 uses total=0
        // (indeterminate bar — set_suffix works correctly).
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        let outcome = PreResolveOutcome::Skip {
            name: "test-tool".to_string(),
            human_readable_version: String::new(),
            version: "v1.0.0".to_string(),
            metadata_cached: false,
            metadata_fetch_count: 0,
            resolved_tag: String::new(),
        };
        let result =
            fetch_and_import_tool_payload(&cas, "test-tool", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "Skip should return Ok");
        assert!(result.unwrap().is_none(), "Skip should return Ok(None)");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::AddBar { total: 0, .. })),
            "expected AddBar total=0 in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::SetPosition { pos: 0 })),
            "expected SetPosition pos=1 in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter()
                .any(|op| matches!(op, ProgressOp::SetSuffix { suffix } if suffix == "skipped")),
            "expected SetSuffix(\"skipped\") in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.iter().any(|op| *op == ProgressOp::FinishSuccess),
            "expected FinishSuccess in ops\ngot: {ops:#?}",
        );
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
    fn total_process_items_three_sources_three_archives() {
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
        assert_eq!(total, 6, "3 archive sources should produce 6 process items");
    }

    #[test]
    fn total_process_items_mixed_archives_and_binaries() {
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
        assert_eq!(total, 4, "mixed sources should produce correct process total");
    }

    // -- Progress bar label format tests (shortened phases + version) --

    async fn label_setup() -> (impl CasApi, UserLevelCache, RecordingProgressTracker) {
        let cas = new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: 30 * 24 * 60 * 60,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("cache open");
        let cache = UserLevelCache::from_cache(cache);
        let tracker = RecordingProgressTracker::new();
        (cas, cache, tracker)
    }

    #[tokio::test]
    async fn resolve_bar_label_includes_version() {
        let (cas, cache, tracker) = label_setup().await;
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            "v7.1".to_string(),
            "v7.1".to_string(),
            false,
            1,
            String::new(),
        );
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::AddBar { label, .. } if label.contains("v7.1") && label.contains("[res]")
            )),
            "expected AddBar label with 'v7.1' and '[res]' in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn resolve_bar_label_omits_version_when_empty() {
        let (cas, cache, tracker) = label_setup().await;
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            String::new(),
            "v1.0.0".to_string(),
            false,
            1,
            String::new(),
        );
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::AddBar { label, .. } if label == "media-tagger [res]"
            )),
            "expected AddBar label exactly 'media-tagger [res]' in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn skip_bar_label_includes_version() {
        let (cas, cache, tracker) = label_setup().await;
        let outcome = PreResolveOutcome::Skip {
            name: "test-tool".to_string(),
            human_readable_version: "v7.1".to_string(),
            version: "v7.1".to_string(),
            metadata_cached: false,
            metadata_fetch_count: 1,
            resolved_tag: String::new(),
        };
        let result =
            fetch_and_import_tool_payload(&cas, "test-tool", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "Skip should return Ok");
        assert!(result.unwrap().is_none(), "Skip should return Ok(None)");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::AddBar { label, .. } if label.contains("v7.1") && label.contains("[res]")
            )),
            "expected AddBar label with 'v7.1' and '[res]' in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn resolve_bar_label_uses_shortened_phases() {
        let (cas, cache, tracker) = label_setup().await;
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            String::new(),
            "v1.0.0".to_string(),
            false,
            1,
            String::new(),
        );
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        // AddBar labels must use shortened phase names.
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::AddBar { label, .. } if label.contains("[res]")
            )),
            "expected [res] in ops\ngot: {ops:#?}",
        );
        assert!(
            !ops.iter().any(|op| matches!(
                op,
                ProgressOp::AddBar { label, .. } if label.contains("[resolve]")
            )),
            "unexpected [resolve] in ops\ngot: {ops:#?}",
        );
        // Fetch and process bars also use shortened names when present.
        assert!(
            !ops.iter().any(|op| matches!(
                op,
                ProgressOp::AddBar { label, .. } if label.contains("[fetch]")
            )),
            "unexpected [fetch] in ops\ngot: {ops:#?}",
        );
        assert!(
            !ops.iter().any(|op| matches!(
                op,
                ProgressOp::AddBar { label, .. } if label.contains("[process]")
            )),
            "unexpected [process] in ops\ngot: {ops:#?}",
        );
    }
}
