//! Tool payload provisioning — thin wrapper around conductor's 3-phase pipeline.
//!
//! Delegates to the mediapm [`provider`](crate::tools::provider) module for
//! phase 1 (resolve), then to the conductor's [`fetch_tool_sources`]
//! for phase 2 (fetch) and [`process_tool_sources`]
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
use mediapm_utils::progress::{
    PrefixComponents, ProviderProgressCallback, StatusCount, SuffixComponents,
};
use tokio::sync::Semaphore;

use crate::conductor_bridge::sync::{ProvisionFailure, ProvisionPhase};
use crate::error::MediaPmError;
use crate::output::ProgressScreenApi;
use crate::tools::downloader::ToolDownloadCache;
#[cfg(test)]
use crate::tools::provider;
#[cfg(test)]
use crate::tools::provider::RecheckPolicy;
use crate::tools::provider::ResolvedToolMetadata;

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
    /// Resolved upstream git tag, or `None` when the provider has no tag.
    /// Mirrors the provider's why-empty rationale.
    pub(super) resolved_tag: Option<String>,
    /// Resolved upstream version string, or `None` when the provider has no
    /// version.
    pub(super) resolved_version: Option<String>,
    /// Resolved upstream VCS commit hash, or `None` when the provider has no
    /// hash.
    pub(super) resolved_vcs_hash: Option<String>,
    /// Per-candidate fetch failures a later chain member covered, in fetch
    /// order. Threaded to the sync report's warnings so a silently-covered
    /// fallback reaches the operator instead of only the log.
    pub(super) warnings: Vec<String>,
}

/// Outcome of the pre-resolve step that determines whether a tool should be
/// provisioned or skipped.
#[derive(Debug, Clone)]
pub(super) enum PreResolveOutcome {
    /// Tool should be fetched and imported normally.
    Resolved(ResolvedToolFetch, ResolvedToolMetadata),
    /// Tool is already provisioned at the given canonical version (skip).
    // WHY: production code destructures this variant in
    // `fetch_and_import_tool_payload` to render the skipped-tool resolve bar,
    // but nothing in the production call graph constructs it — the reconcile
    // loop's version-matched skip returns `EntryOutcome::Skipped` before it
    // reaches here. Only the unit tests below build a `Skip`, so `dead_code`
    // reports the variant as never constructed even though its match arms are
    // live. Deleting it would mean deleting those arms, i.e. removing the
    // documented skip-bar rendering path.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "matched by production skip-bar rendering but constructed only in tests; the production skip path returns EntryOutcome::Skipped earlier"
        )
    )]
    Skip {
        /// Human-readable version string (informational only).
        human_readable_version: String,
        /// Whether the version/tag lookups were served from metadata cache.
        metadata_cached: bool,
        /// Number of individual metadata lookups performed (e.g., ffmpeg = 5: the
        /// `BtbN` autobuild tag, the martin-riedl macOS-leg probe, and three
        /// jellyfin fallback reads — the repo index plus one pointer file per macOS
        /// arch — while every other tool performs 1).
        metadata_fetch_count: u32,
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
            urls.first().is_some_and(|url| infer_archive_format(url).is_some())
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
    } else if filename.ends_with(".tar.gz")
        || std::path::Path::new(filename)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("tgz"))
    {
        Some("tar.gz")
    } else if std::path::Path::new(filename)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
        || filename == "zip"
    {
        Some("zip")
    } else {
        None
    }
}

/// Display form of a tool version: the `SemVer` version with its build
/// metadata (`+...`) stripped, e.g. `0.0.0+cfa32c3182b` -> `0.0.0`.
///
/// Progress labels use this and nothing else does: provider metadata,
/// `state.json`, and `canonical_version` keep the full string.
#[must_use]
pub(crate) fn display_version(version: &str) -> &str {
    version.split_once('+').map_or(version, |(base, _meta)| base)
}

/// Fetches a tool payload for **all** platforms, extracts each to a
/// per-OS temp directory, imports files to CAS with `./{os}/` key prefixes,
/// and builds an OS-conditional command-selector template.
///
/// `group` provides 3 phase-agnostic progress bars per tool (res, fch,
/// pro). Routes
/// [`mediapm_utils::progress::ProviderProgressSnapshot`] callbacks to the
/// matching bar by `snap.phase`. Item counters are displayed via
/// `set_prefix_components`; byte counters drive bar position
/// (`set_position`/`set_total`). The bridge does not interpret the meaning
/// of items or bytes — it only relays the values to the bar.
///
/// A bar reads as `{tool_id}{version_suffix} [{phase_abbr}]`, with
/// `version_suffix` empty when the tool reports no version (so the row is
/// `media-tagger [fch]`). The three fields reach the renderer as
/// [`PrefixComponents`], so a tool id carrying a space stays one field.
///
/// After phase 1 (resolve), a ranged-`GET` prefetch step populates
/// [`ResolvedSource::expected_size`] for each `Fetch`-producer source so
/// phase 2 progress bars start with an accurate byte total.
///
/// The resolve bar shows `metadata_fetch_count` items (one per metadata lookup:
/// ffmpeg has 5 — the `BtbN` autobuild tag, the martin-riedl build probe, and
/// three jellyfin fallback reads, the repo index plus one pointer file per macOS
/// arch — while every other tool has 1). When all metadata lookups were cache
/// hits, the bar shows `"N cached"` where N = `metadata_fetch_count` (e.g.
/// `5 cached`). When the tool is already up-to-date (Skip), the bar shows
/// `"skipped, N cached"` (with the count when cached, e.g. `skipped, 5 cached`)
/// or `"skipped"` otherwise. Fetch bar shows
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
///
/// Returns [`ProvisionFailure`] tagging the failing phase (`Fetch` or
/// `Process`) so the caller can abort the sync naming the exact stage.
#[expect(
    clippy::too_many_lines,
    reason = "provisioning pipeline orchestrates resolve/fetch/process phases; keeping it in one function preserves the phase ordering invariant"
)]
pub(super) async fn fetch_and_import_tool_payload(
    cas: &impl CasApi,
    tool_id: &str,
    cache: &ToolDownloadCache,
    group: &dyn ProgressScreenApi,
    outcome: PreResolveOutcome,
) -> Result<Option<FetchedToolPayload>, ProvisionFailure> {
    // Track created bars so we can mark them red on error.
    let mut error_bars: Vec<Arc<dyn crate::output::ProgressBarApi>> = Vec::new();

    // Helper to mark the still-active bars as failed before returning the
    // phase-tagged error. A fetch/process failure now aborts the whole sync
    // (fail fast), so the bars are finished red (`finish_error`, `[F]`) to
    // match the fatal outcome rather than the old non-fatal warning. Bars
    // already finished (e.g. resolve/fetch that succeeded before the failing
    // phase) are skipped so they keep their success status instead of being
    // retroactively marked failed.
    let finish_error_bars = |bars: &[Arc<dyn crate::output::ProgressBarApi>]| {
        for bar in bars {
            if !bar.is_finished() {
                bar.finish_error();
            }
        }
    };

    // Phase 1: Resolve — get source descriptors from the mediapm provider.
    let version_for_label = match &outcome {
        PreResolveOutcome::Resolved(_, metadata) => metadata.human_readable_version.as_str(),
        PreResolveOutcome::Skip { human_readable_version, .. } => human_readable_version.as_str(),
    };
    let version_for_label = display_version(version_for_label);
    let version_suffix =
        if version_for_label.is_empty() { String::new() } else { format!(" {version_for_label}") };
    // The fields each phase bar draws, given to the bar rather than rendered
    // into a label and read back out. Phase abbreviations: `[res]`, `[fch]`,
    // `[pro]`.
    let bar_prefix = |phase: &str| PrefixComponents {
        tool_name: tool_id.to_string(),
        version: version_suffix.trim().to_string(),
        phase: phase.to_string(),
        ..Default::default()
    };
    let metadata_fetch_count = match &outcome {
        PreResolveOutcome::Resolved(_, metadata) => metadata.metadata_fetch_count,
        PreResolveOutcome::Skip { metadata_fetch_count, .. } => *metadata_fetch_count,
    };
    let bar_total = metadata_fetch_count;
    let resolve_bar = group.add_bar_with_prefix(bar_total.into(), &bar_prefix("res"));
    error_bars.push(resolve_bar.clone());
    let (
        mut fetch,
        human_readable_version,
        canonical_version,
        resolved_tag,
        resolved_version,
        resolved_vcs_hash,
    ) = match outcome {
        PreResolveOutcome::Resolved(f, metadata) => {
            if metadata.metadata_cached {
                resolve_bar.set_suffix_components(SuffixComponents {
                    custom: SuffixComponents::status_list(&[StatusCount {
                        word: "cached",
                        count: Some(metadata.metadata_fetch_count),
                    }])
                    .custom,
                    ..Default::default()
                });
            }
            resolve_bar.set_position(bar_total.into());
            resolve_bar.finish_success();
            (
                f,
                metadata.human_readable_version,
                metadata.canonical_version,
                metadata.resolved_tag,
                metadata.resolved_version,
                metadata.resolved_vcs_hash,
            )
        }
        PreResolveOutcome::Skip { metadata_cached, metadata_fetch_count, .. } => {
            // Tool is already provisioned at this version — show resolve bar
            // with "skipped" indicator, then return early.
            resolve_bar.set_position(bar_total.into());
            if metadata_cached {
                resolve_bar.set_suffix_components(SuffixComponents {
                    custom: SuffixComponents::status_list(&[
                        StatusCount { word: "skipped", count: None },
                        StatusCount { word: "cached", count: Some(metadata_fetch_count) },
                    ])
                    .custom,
                    ..Default::default()
                });
            } else {
                resolve_bar.set_suffix_components(SuffixComponents {
                    custom: SuffixComponents::status_list(&[StatusCount {
                        word: "skipped",
                        count: None,
                    }])
                    .custom,
                    ..Default::default()
                });
            }
            resolve_bar.finish_success();
            return Ok(None);
        }
    };

    // Phase 1b: Prefetch expected sizes with a one-byte ranged GET (the method
    // the fetch uses — `HEAD` is refused by martin-riedl).
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
    let fetch_bar = group.add_bar_with_prefix(total, &bar_prefix("fch"));
    error_bars.push(fetch_bar.clone());
    let fetch_bar_cb = fetch_bar.clone();
    let fetch_tool_id = tool_id.to_string();
    let fetch_version_suffix = version_suffix.clone();
    let fetch_progress: Option<ProviderProgressCallback> = Some(Arc::new(move |snap| {
        fetch_bar_cb.set_prefix_components(PrefixComponents {
            marker: String::new(),
            tool_name: fetch_tool_id.clone(),
            version: fetch_version_suffix.trim().to_string(),
            phase: "fch".to_string(),
            count: snap.items.0.to_string(),
            total: snap.items.1.to_string(),
        });
        fetch_bar_cb.set_position(snap.bytes.0);
        fetch_bar_cb.set_total(snap.bytes.1);
    }));
    let downloaded = match fetch_tool_sources(&fetch, cache, "tools", fetch_progress).await {
        Ok(d) => d,
        Err(e) => {
            finish_error_bars(&error_bars);
            return Err(ProvisionFailure {
                phase: ProvisionPhase::Fetch,
                source: MediaPmError::Workflow(format!("fetch failed: {e}")),
            });
        }
    };
    // Set fetch bar RHS message if some sources were cache-served.
    if downloaded.cached_count > 0 {
        #[allow(clippy::cast_possible_truncation)]
        fetch_bar.set_suffix_components(SuffixComponents {
            custom: SuffixComponents::status_list(&[StatusCount {
                word: "cached",
                count: Some(downloaded.cached_count as u32),
            }])
            .custom,
            ..Default::default()
        });
    }
    fetch_bar.finish_success();

    // Phase 3: Process — extract archives, repack to uncompressed ZIP,
    // import to CAS, build content map + command selector.
    // The bar total is set to total_process_items (item count). This is
    // intentional — the byte-level total isn't known until the budget is
    // populated with source byte sizes. The budget starts with item count
    // as the aggregate total and refines to actual payload sizes as each
    // source begins processing.
    let process_bar = group.add_bar_with_prefix(total_process_items, &bar_prefix("pro"));
    error_bars.push(process_bar.clone());
    let process_bar_cb = process_bar.clone();
    let pp_tool_id = tool_id.to_string();
    let pp_version_suffix = version_suffix.clone();
    let pp_progress: Option<ProviderProgressCallback> = Some(Arc::new(move |snap| {
        process_bar_cb.set_prefix_components(PrefixComponents {
            marker: String::new(),
            tool_name: pp_tool_id.clone(),
            version: pp_version_suffix.trim().to_string(),
            phase: "pro".to_string(),
            count: snap.items.0.to_string(),
            total: snap.items.1.to_string(),
        });
        process_bar_cb.set_position(snap.bytes.0);
        process_bar_cb.set_total(snap.bytes.1);
    }));
    let result = match process_tool_sources(&downloaded, cas, pp_progress).await {
        Ok(r) => r,
        Err(e) => {
            finish_error_bars(&error_bars);
            return Err(ProvisionFailure {
                phase: ProvisionPhase::Process,
                source: MediaPmError::Workflow(format!("process failed: {e}")),
            });
        }
    };
    process_bar.finish_success();

    // Fetch feedback: a multi-candidate chain can serve a different member
    // than the resolve phase predicted (HTTP failure or `.sha256` mismatch
    // fails a member over), so the canonical version is re-derived from the
    // URLs that actually served before the payload is recorded.
    let canonical_version = crate::tools::provider::canonical_version_after_fetch(
        tool_id,
        &canonical_version,
        &downloaded.entries,
    );

    Ok(Some(FetchedToolPayload {
        content_map: result.content_map,
        os_exec_paths: result.os_exec_paths,
        human_readable_version,
        canonical_version,
        resolved_tag,
        resolved_version,
        resolved_vcs_hash,
        warnings: downloaded.warnings,
    }))
}

/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`](mediapm_conductor::http::client::shared_http_client).
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
///
/// Sends a one-byte ranged `GET` (`Range: bytes=0-0`) to populate
/// `expected_size` on each `Fetch`-producer source. The request carries the
/// method the fetch itself uses, because a `HEAD` probe is not answered
/// everywhere: martin-riedl replies 404 to `HEAD` while serving `GET`, which
/// left its sources with no byte total. The payload total comes from
/// `Content-Range` (`bytes 0-0/<total>`) when the server honours the range and
/// answers `206`, and from `Content-Length` when it ignores the range and
/// returns the whole payload as `200`.
///
/// Failures are silently ignored — `expected_size` stays as it was and the
/// existing Content-Length fallback in phase 2 applies. A transport error, a
/// non-success status, an unparsable `Content-Range`, or a zero size all
/// record nothing.
///
/// Uses fully concurrent requests with a semaphore to limit concurrency.
async fn prefetch_expected_sizes(sources: &mut [ResolvedSource]) {
    let Ok(client) = mediapm_conductor::http::client::shared_http_client() else {
        return;
    };
    let probe_timeout = std::time::Duration::from_secs(10);
    let semaphore = Arc::new(Semaphore::new(MAX_LOOKAHEAD));

    let tasks: Vec<_> = sources
        .iter()
        .enumerate()
        .filter_map(|(idx, source)| {
            let url = match &source.producer {
                SourceProducer::Fetch { urls } if !urls.is_empty() => &urls[0],
                _ => return None,
            };
            let client = client.clone();
            let semaphore = semaphore.clone();
            let url = url.clone();
            Some(async move {
                let _permit = semaphore.acquire().await.expect("semaphore closed");
                // Ask with the method the fetch uses. martin-riedl answers
                // `HEAD` with 404 and serves only `GET`, so a `HEAD` probe
                // recorded nothing for its sources. `Range: bytes=0-0` keeps
                // the probe to one byte; the size comes from `Content-Range`
                // when the server honours the range and from `Content-Length`
                // when it answers with the whole payload instead.
                let request = client
                    .get(&url)
                    .header(reqwest::header::RANGE, "bytes=0-0")
                    .timeout(probe_timeout)
                    .send()
                    .await;
                if let Ok(response) = request
                    && response.status().is_success()
                {
                    if let Some(total) = response
                        .headers()
                        .get(reqwest::header::CONTENT_RANGE)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.rsplit('/').next())
                        .and_then(|total| total.trim().parse::<u64>().ok())
                        && total > 0
                    {
                        return Some((idx, total));
                    }
                    if let Some(content_length) = response.content_length()
                        && content_length > 0
                    {
                        return Some((idx, content_length));
                    }
                }
                None
            })
        })
        .collect();

    let results: Vec<Option<(usize, u64)>> = futures_util::future::join_all(tasks).await;
    for (idx, content_length) in results.into_iter().flatten() {
        sources[idx].expected_size = Some(content_length);
    }
}

#[cfg(test)]
mod tests {
    use mediapm_cas::storage::in_memory::new_in_memory_cas;
    use mediapm_conductor::cache::Cache;
    use mediapm_conductor::cache::CacheDomainConfig;
    use mediapm_conductor::cache::ENTRY_TTL_SECONDS;
    use mediapm_conductor::cache_user_level::UserLevelCache;
    use mediapm_conductor::tools::provider::{VersionSpec, host_arch_label};
    use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::example_isolation::lock_process_env;

    use super::*;

    /// Builds a `ResolvedToolMetadata` for tests where resolved fields are
    /// intentionally absent (matches the default `None` state).
    fn test_metadata(
        human_readable_version: &str,
        canonical_version: &str,
        metadata_cached: bool,
        metadata_fetch_count: u32,
    ) -> ResolvedToolMetadata {
        ResolvedToolMetadata {
            human_readable_version: human_readable_version.to_string(),
            canonical_version: canonical_version.to_string(),
            metadata_cached,
            metadata_fetch_count,
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() reads against the MEDIAPM_GITHUB_API_BASE seam mutation scope"
    )]
    async fn fetch_and_import_rejects_unknown_tool() {
        let _env = lock_process_env();
        let _cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
            &VersionSpec::Latest,
            Some((&*cache, "tool_metadata")),
            RecheckPolicy::default(),
        )
        .await;
        assert!(resolve_result.is_err(), "resolve_tool_fetch should reject unknown tools");
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() reads against the MEDIAPM_GITHUB_API_BASE seam mutation scope"
    )]
    async fn fetch_and_import_generate_launcher_succeeds() {
        let _env = lock_process_env();
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
        let (fetch, metadata) = crate::tools::provider::resolve_tool_fetch(
            "media-tagger",
            &VersionSpec::Latest,
            Some((&*cache, "tool_metadata")),
            RecheckPolicy::default(),
        )
        .await
        .unwrap();
        let outcome = PreResolveOutcome::Resolved(fetch, metadata);
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
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() reads against the MEDIAPM_GITHUB_API_BASE seam mutation scope"
    )]
    async fn fetch_and_import_ytdlp_full_pipeline() {
        let _env = lock_process_env();
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
                .and(path(format!("/{filename}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_bytes(bytes.to_vec())
                        .insert_header("Content-Length", bytes.len().to_string()),
                )
                .mount(&mock_server)
                .await;
        }

        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
        let (mut fetch, metadata) = crate::tools::provider::resolve_tool_fetch(
            "yt-dlp",
            &VersionSpec::Latest,
            Some((&*cache, "tool_metadata")),
            RecheckPolicy::default(),
        )
        .await
        .unwrap();

        // Patch download URLs to point at wiremock (so the prefetch probe and
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

        let outcome = PreResolveOutcome::Resolved(fetch, metadata);
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

    /// Builds a minimal zip archive containing one file, for a Fetch source
    /// whose URL extension marks it as an archive.
    fn test_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use std::io::Write as _;
        use zip::write::SimpleFileOptions;
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();
        for (name, content) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(content).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    /// Drives the full fetch → process → fetch-feedback wiring for ffmpeg and
    /// asserts the redirect build id the fetch observed reaches
    /// [`FetchedToolPayload::canonical_version`].
    ///
    /// The macOS chain's resolve-time leg is deliberately a *different* value
    /// (`resolvetime-obs`) than the build id the redirecting martin-riedl
    /// permalink serves (`1760000000_N-12345-gabcdef`). Only
    /// `canonical_version_after_fetch` (called at the `provision.rs` fetch
    /// site) can replace the resolve-time leg with the fetched one — so
    /// deleting that call leaves the resolve-time value and this test fails.
    /// macOS-only sources keep the fetch on the local wiremock server and off
    /// the real `BtbN` endpoints.
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's martin-riedl/jellyfin seam reads against their mutation scope"
    )]
    async fn fetch_leg_rewrite_reaches_payload_canonical_version() {
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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

        let mock_server = MockServer::start().await;
        let build_id = "1760000000_N-12345-gabcdef";
        let payload_bytes = test_zip(&[("ffmpeg", b"fake-ffmpeg-binary")]);
        // For each macOS arch: the permalink 302-redirects to the versioned
        // download (whose path carries the build id), which serves a real zip.
        // No `.sha256` is mounted, so the sibling probe 404s and the bytes are
        // accepted unverified (the documented "verify when served" rule).
        // The client translates our canonical arch labels to the site's
        // vocabulary (`aarch64` -> `arm64`, `x86_64` -> `amd64`) when it
        // builds martin-riedl URLs, so the mocks must answer on the mapped
        // path rather than the canonical one.
        for url_arch in ["arm64", "amd64"] {
            let permalink = format!("/redirect/latest/macos/{url_arch}/snapshot/ffmpeg.zip");
            let versioned = format!("/download/macos/{url_arch}/{build_id}/ffmpeg.zip");
            Mock::given(method("GET"))
                .and(path(permalink))
                .respond_with(
                    ResponseTemplate::new(302).insert_header("location", versioned.as_str()),
                )
                .mount(&mock_server)
                .await;
            Mock::given(method("GET"))
                .and(path(versioned))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(payload_bytes.clone()))
                .mount(&mock_server)
                .await;
        }

        let env_lock = lock_process_env();
        // SAFETY: the env lock the guard holds scopes these process-global
        // seams to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var("MEDIAPM_MARTIN_RIEDL_BASE", mock_server.uri());
            std::env::set_var("MEDIAPM_JELLYFIN_BASE", mock_server.uri());
        }

        // Build the real ffmpeg source set against the seam, then keep only the
        // macOS dual-arch entries so the fetch never touches BtbN's github.com
        // windows/linux legs.
        // No resolved jellyfin fallback in this test: it drives the
        // martin-riedl primary, which the mock answers first. The resolve-time
        // leg is the same `resolvetime-obs` the metadata below records, so the
        // permalink sources carry the identity this resolve declared. The
        // autobuild tag only keys the BtbN sources, which are retained away
        // below.
        let mut fetch = crate::tools::provider::ffmpeg::sources(
            &VersionSpec::Latest,
            None,
            "resolvetime-obs",
            "autobuild-TEST",
        );
        fetch.sources.retain(|source| source.os == "macos");
        assert_eq!(fetch.sources.len(), 2, "expected both macOS arch sources");

        // Resolve-time metadata carries a DIFFERENT macOS leg than the fetch
        // will observe, so only the fetch-feedback rewrite can produce the
        // asserted value.
        let metadata = test_metadata(
            "autobuild-TEST+resolvetime-obs",
            "autobuild-TEST+resolvetime-obs",
            false,
            0,
        );
        let outcome = PreResolveOutcome::Resolved(fetch, metadata);

        let result = fetch_and_import_tool_payload(&cas, "ffmpeg", &cache, &tracker, outcome).await;

        // SAFETY: same env-lock scope as the sets above.
        unsafe {
            std::env::remove_var("MEDIAPM_MARTIN_RIEDL_BASE");
            std::env::remove_var("MEDIAPM_JELLYFIN_BASE");
        }
        drop(env_lock);

        let payload = result
            .expect("ffmpeg macOS fetch should succeed")
            .expect("ffmpeg should return a payload");

        // The dual-arch rule keys both payloads under arch segments and records
        // the host-arch exec path — confirming this exercised the real chain.
        assert!(
            payload.content_map.contains_key("macos/aarch64/"),
            "dual-arch macOS slot keys the aarch64 payload under an arch segment"
        );
        assert!(
            payload.content_map.contains_key("macos/x86_64/"),
            "dual-arch macOS slot keys the x86_64 payload under an arch segment"
        );
        assert_eq!(
            payload.os_exec_paths.get("macos").map(String::as_str),
            Some(format!("{}/ffmpeg", host_arch_label()).as_str()),
            "os_exec_paths resolves the host-arch macOS payload"
        );

        // THE WIRING UNDER TEST: the build id parsed from the redirect target
        // the fetch actually followed replaces the resolve-time leg.
        assert_eq!(
            payload.canonical_version,
            format!("autobuild-TEST+{build_id}"),
            "the redirect build id the fetch observed must reach the payload's canonical version"
        );
        assert_ne!(
            payload.canonical_version, "autobuild-TEST+resolvetime-obs",
            "the resolve-time leg must have been rewritten away"
        );
    }

    #[tokio::test]
    async fn fetch_and_import_with_pre_resolved_canonical_version() {
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
        let outcome =
            PreResolveOutcome::Resolved(fetch, test_metadata("", "test-canonical", false, 1));

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
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
        let outcome = PreResolveOutcome::Resolved(fetch, test_metadata("", "v1.0.0", true, 1));
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::SetSuffixComponents { components } if components.custom == "1 cached"
            )),
            "expected SetSuffixComponents(custom=1 cached) in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn resolve_bar_shows_total_two_when_metadata_fetch_count_two() {
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
        let outcome = PreResolveOutcome::Resolved(fetch, test_metadata("", "v1.0.0", false, 2));
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
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
            human_readable_version: String::new(),
            metadata_cached: true,
            metadata_fetch_count: 1,
        };
        let result =
            fetch_and_import_tool_payload(&cas, "test-tool", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "Skip should return Ok");
        assert!(result.unwrap().is_none(), "Skip should return Ok(None)");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::SetSuffixComponents { components }
                    if components.custom == "skipped, 1 cached"
            )),
            "expected SetSuffixComponents(custom=skipped, 1 cached) in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.contains(&ProgressOp::FinishSuccess),
            "expected FinishSuccess in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn skip_bar_shows_skipped_when_metadata_not_cached() {
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
            human_readable_version: String::new(),
            metadata_cached: false,
            metadata_fetch_count: 1,
        };
        let result =
            fetch_and_import_tool_payload(&cas, "test-tool", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "Skip should return Ok");
        assert!(result.unwrap().is_none(), "Skip should return Ok(None)");

        let ops = tracker.ops();
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::SetSuffixComponents { components }
                    if components.custom == "skipped"
            )),
            "expected SetSuffixComponents(custom=skipped) in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.contains(&ProgressOp::FinishSuccess),
            "expected FinishSuccess in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn resolve_bar_no_cached_message_when_not_cached() {
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
        let outcome = PreResolveOutcome::Resolved(fetch, test_metadata("", "v1.0.0", false, 1));
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        assert!(
            !ops.iter().any(|op| matches!(
                op,
                ProgressOp::SetSuffixComponents { components } if components.custom == "cached"
            )),
            "unexpected SetSuffixComponents(custom=cached) in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn resolve_bar_zero_metadata_fetch_count_uses_min_one() {
        // Regression: resolve bar with metadata_fetch_count=0 gets total=0
        // (indeterminate bar — set_suffix_components works correctly after
        // disabled flag was moved out of the total==0 proxy check).
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
        let outcome = PreResolveOutcome::Resolved(fetch, test_metadata("", "v1.0.0", false, 0));
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
        // metadata_fetch_count=2 (a two-lookup resolve, as ffmpeg's own count
        // was before the jellyfin fallback reads were added) must show the
        // "2 cached" status list (not bare "cached" without count).
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
        let outcome = PreResolveOutcome::Resolved(fetch, test_metadata("", "v1.0.0", true, 2));
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
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::SetSuffixComponents { components } if components.custom == "2 cached"
            )),
            "expected SetSuffixComponents(custom=2 cached) in ops\ngot: {ops:#?}",
        );
        // Also verify bare "cached" (without count) never appears.
        assert!(
            !ops.iter().any(|op| matches!(
                op,
                ProgressOp::SetSuffixComponents { components } if components.custom == "cached"
            )),
            "unexpected SetSuffixComponents(custom=cached) in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn skip_bar_shows_skipped_cached_two() {
        // Regression: skip bar with metadata_cached=true and
        // metadata_fetch_count=2 must show "skipped, 2 cached" and fill the
        // bar to position = total.
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
            human_readable_version: "v1.0.0".to_string(),
            metadata_cached: true,
            metadata_fetch_count: 2,
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
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::SetSuffixComponents { components }
                    if components.custom == "skipped, 2 cached"
            )),
            "expected SetSuffixComponents(custom=skipped, 2 cached) in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.contains(&ProgressOp::FinishSuccess),
            "expected FinishSuccess in ops\ngot: {ops:#?}",
        );
    }

    #[tokio::test]
    async fn skip_bar_zero_metadata_fetch_count_uses_min_one() {
        // Regression: skip bar with metadata_fetch_count=0 uses total=0
        // (indeterminate bar — set_suffix_components works correctly).
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
            human_readable_version: String::new(),
            metadata_cached: false,
            metadata_fetch_count: 0,
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
            ops.iter().any(|op| matches!(
                op,
                ProgressOp::SetSuffixComponents { components }
                    if components.custom == "skipped"
            )),
            "expected SetSuffixComponents(custom=skipped) in ops\ngot: {ops:#?}",
        );
        assert!(
            ops.contains(&ProgressOp::FinishSuccess),
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
        let producer = SourceProducer::launcher("test");
        assert!(!is_archive_source(&producer));
    }

    #[test]
    fn total_process_items_three_sources_three_archives() {
        let fetch = ResolvedToolFetch {
            tool_id: "ffmpeg".to_string(),
            sources: vec![
                ResolvedSource {
                    os: "linux".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/ffmpeg-linux.zip".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
                },
                ResolvedSource {
                    os: "macos".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/ffmpeg-macos.zip".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
                },
                ResolvedSource {
                    os: "windows".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/ffmpeg-windows.zip".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
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
                    arch: host_arch_label(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/tool-linux.zip".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
                },
                ResolvedSource {
                    os: "macos".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/tool-macos".to_string()],
                    },
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
                },
                ResolvedSource {
                    os: "windows".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::launcher("test"),
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
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

    // -- Expected-size prefetch tests --

    /// Builds one `Fetch`-producer source pointing at `url`, so the prefetch
    /// probe has exactly one candidate to request.
    fn source_with_url(url: &str) -> ResolvedSource {
        ResolvedSource {
            os: "macos".to_string(),
            arch: "aarch64".to_string(),
            producer: SourceProducer::Fetch { urls: vec![url.to_string()] },
            expected_size: None,
            size_hint_bytes: None,
            identity: None,
        }
    }

    /// The prefetch asks with the method the fetch uses. martin-riedl answers
    /// `HEAD` with 404 and serves only `GET`, so a `HEAD`-only probe left its
    /// sources with no byte total and the fetch bar had to wait for the
    /// response. A `206` carries the full size in `Content-Range`, not in
    /// `Content-Length` (which is the one-byte range).
    #[tokio::test]
    async fn prefetch_reads_the_total_from_content_range_when_the_host_rejects_head() {
        use wiremock::matchers::{header, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("HEAD")).respond_with(ResponseTemplate::new(404)).mount(&server).await;
        Mock::given(method("GET"))
            .and(header("range", "bytes=0-0"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("content-range", "bytes 0-0/41943040")
                    .set_body_bytes(b"x".as_slice()),
            )
            .mount(&server)
            .await;

        let mut sources = vec![source_with_url(&format!("{}/ffmpeg.zip", server.uri()))];
        prefetch_expected_sizes(&mut sources).await;

        assert_eq!(
            sources[0].expected_size,
            Some(41_943_040),
            "a 206 answers with the payload total in Content-Range"
        );
    }

    /// A host that ignores the range answers `200`, where `Content-Length` is
    /// the payload total.
    #[tokio::test]
    async fn prefetch_reads_the_total_from_content_length_when_the_range_is_ignored() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0_u8; 2048]))
            .mount(&server)
            .await;

        let mut sources = vec![source_with_url(&format!("{}/deno.zip", server.uri()))];
        prefetch_expected_sizes(&mut sources).await;

        assert_eq!(sources[0].expected_size, Some(2048), "a 200 answers with the payload total");
    }

    /// The prefetch stays best-effort: a refusal records nothing and never
    /// panics, exactly as a failed `HEAD` did.
    #[tokio::test]
    async fn prefetch_records_nothing_for_a_refused_source() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

        let mut sources = vec![source_with_url(&format!("{}/gone.zip", server.uri()))];
        prefetch_expected_sizes(&mut sources).await;

        assert_eq!(sources[0].expected_size, None, "a refusal leaves the size unknown");
    }

    /// A silent failure leaves an already-known size alone, which is the "stays
    /// as it was" half of the contract that seeding `None` cannot observe.
    #[tokio::test]
    async fn prefetch_preserves_a_known_size_when_the_probe_fails() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

        let mut sources = vec![source_with_url(&format!("{}/gone.zip", server.uri()))];
        sources[0].expected_size = Some(999);
        prefetch_expected_sizes(&mut sources).await;

        assert_eq!(
            sources[0].expected_size,
            Some(999),
            "a failed probe must not clear a size recorded earlier"
        );
    }

    // -- Progress bar label format tests (shortened phases + version) --

    async fn label_setup()
    -> (impl CasApi, UserLevelCache, RecordingProgressTracker, tempfile::TempDir) {
        let cas = new_in_memory_cas();
        let tmp = mediapm_utils::temp::cache_dir().expect("temp dir");
        let cache = Cache::open(
            tmp.path(),
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
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
        (cas, cache, tracker, tmp)
    }

    #[tokio::test]
    async fn resolve_bar_label_includes_version() {
        let (cas, cache, tracker, _tmp) = label_setup().await;
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(fetch, test_metadata("v7.1", "v7.1", false, 1));
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
        let (cas, cache, tracker, _tmp) = label_setup().await;
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(fetch, test_metadata("", "v1.0.0", false, 1));
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
        let (cas, cache, tracker, _tmp) = label_setup().await;
        let outcome = PreResolveOutcome::Skip {
            human_readable_version: "v7.1".to_string(),
            metadata_cached: false,
            metadata_fetch_count: 1,
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
        let (cas, cache, tracker, _tmp) = label_setup().await;
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(fetch, test_metadata("", "v1.0.0", false, 1));
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

    /// Progress labels show the `SemVer` version without build metadata (spec R4).
    ///
    /// `media-tagger` reports `0.0.0+<build hash>`; the hash is provider/state
    /// material and eats the prefix budget, which is what made two rows of the
    /// same tool truncate to different lengths.
    #[tokio::test]
    async fn resolve_bar_label_strips_build_metadata() {
        let (cas, cache, tracker, _tmp) = label_setup().await;
        let fetch = provider::media_tagger::sources();
        let outcome = PreResolveOutcome::Resolved(
            fetch,
            test_metadata("0.0.0+cfa32c3182b", "0.0.0+cfa32c3182b", false, 1),
        );
        let result =
            fetch_and_import_tool_payload(&cas, "media-tagger", &cache, &tracker, outcome).await;
        assert!(result.is_ok(), "resolve should succeed");

        let ops = tracker.ops();
        let recorded: Vec<String> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::SetPrefixComponents { version, .. } => Some(version.clone()),
                ProgressOp::AddBar { label, .. } => Some(label.clone()),
                _ => None,
            })
            .collect();
        assert!(
            recorded.iter().any(|r| r.contains("0.0.0")),
            "the label must still carry the version; recorded: {recorded:?}"
        );
        assert!(
            recorded.iter().all(|r| !r.contains("cfa32c3182b")),
            "progress labels must not carry build metadata (spec R4); recorded: {recorded:?}"
        );
    }
}
