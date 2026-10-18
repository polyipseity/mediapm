//! AcoustID fingerprint lookup helpers.

use std::path::Path;

use anyhow::{Context, bail};
use chromaprint::{Algorithm, fingerprint_audio};
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use mediapm_conductor::http::client::shared_http_client;

use super::cover_art::MediaTaggerHttpCache;
use super::util::{normalize_optional_text, resolve_ffmpeg_executable};
use super::{ACOUSTID_API_KEY_ENV, FINGERPRINT_CHANNELS, FINGERPRINT_SAMPLE_RATE};

/// Decoded fingerprint payload plus AcoustID request duration hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FingerprintPayload {
    /// Encoded Chromaprint fingerprint string used by AcoustID lookup.
    pub(super) fingerprint: String,
    /// Rounded audio duration in whole seconds.
    pub(super) duration_seconds: u32,
}

/// Resolves AcoustID API key from CLI override or environment.
pub(super) fn resolve_acoustid_api_key(override_value: Option<&str>) -> Option<String> {
    if let Some(override_value) = override_value {
        if let Some(value) = normalize_optional_text(Some(override_value)) {
            return Some(value);
        }

        // An explicit blank override intentionally disables environment
        // fallback so callers can force the "missing-key" flow.
        return None;
    }

    if let Some(value) = std::env::var_os(ACOUSTID_API_KEY_ENV)
        .and_then(|value| value.into_string().ok())
        .and_then(|value| normalize_optional_text(Some(&value)))
    {
        return Some(value);
    }

    None
}

/// Enforces that AcoustID autodetection has a non-empty API key.
///
/// The lookup path is entered only when callers omit `recording_mbid`.
/// At that point, missing credentials represent a configuration error:
/// runtime would otherwise silently disable autodetection and surprise users.
///
/// In `mediapm sync` executions, the internal media-tagger runs as a managed
/// executable tool under conductor. In that mode, operators typically need to
/// include `ACOUSTID_API_KEY` in `runtime.environment.inherited_env_vars` so the tool
/// subprocess can read it from environment.
pub(super) fn require_acoustid_api_key_for_lookup(
    resolved_key: Option<String>,
) -> anyhow::Result<String> {
    let Some(api_key) = resolved_key else {
        bail!(
            "AcoustID lookup requires a non-empty API key; set --acoustid-api-key or {ACOUSTID_API_KEY_ENV} (for mediapm sync workflows, ensure runtime.environment.inherited_env_vars includes {ACOUSTID_API_KEY_ENV})"
        );
    };

    Ok(api_key)
}

/// Runs `ffmpeg` decode + Chromaprint fingerprint generation for one media file.
pub(super) async fn decode_and_fingerprint_audio(
    input_path: &Path,
) -> anyhow::Result<FingerprintPayload> {
    let ffmpeg_executable = resolve_ffmpeg_executable();
    let output = Command::new(&ffmpeg_executable)
        .arg("-v")
        .arg("error")
        .arg("-probesize")
        .arg("32k")
        .arg("-analyzeduration")
        .arg("0")
        .arg("-i")
        .arg(input_path)
        .arg("-vn")
        .arg("-f")
        .arg("s16le")
        .arg("-ac")
        .arg(FINGERPRINT_CHANNELS.to_string())
        .arg("-ar")
        .arg(FINGERPRINT_SAMPLE_RATE.to_string())
        .arg("-")
        .output()
        .await
        .with_context(|| {
            format!("running '{ffmpeg_executable}' to decode '{}'", input_path.display())
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "ffmpeg decode failed for '{}' with status {}: {stderr}",
            input_path.display(),
            output.status
        );
    }

    if output.stdout.is_empty() {
        bail!("ffmpeg decode for '{}' produced no PCM bytes", input_path.display());
    }

    let samples = output
        .stdout
        .chunks_exact(2)
        .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>();

    if samples.is_empty() {
        bail!("decoded PCM sample stream is empty");
    }

    let fingerprint = fingerprint_audio(
        &samples,
        FINGERPRINT_SAMPLE_RATE,
        FINGERPRINT_CHANNELS,
        Algorithm::default(),
    )
    .context("computing chromaprint fingerprint")?;

    let encoded_fingerprint = fingerprint.encoded().to_string();
    let sample_count = u128::try_from(samples.len()).unwrap_or(u128::MAX);
    let samples_per_second = u128::from(FINGERPRINT_SAMPLE_RATE) * u128::from(FINGERPRINT_CHANNELS);
    let rounded_seconds = ((sample_count + (samples_per_second / 2)) / samples_per_second).max(1);
    let duration_seconds = u32::try_from(rounded_seconds).unwrap_or(u32::MAX);

    Ok(FingerprintPayload { fingerprint: encoded_fingerprint, duration_seconds })
}

/// AcoustID API response payload.
///
/// Cached after a successful lookup, zero-match answers included: a
/// fingerprint the service has never seen is a stable answer, so a repeat
/// lookup skips the network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AcoustIdLookupResponse {
    /// API status string (`ok` on success).
    status: String,
    /// Candidate matches returned by AcoustID.
    #[serde(default)]
    results: Vec<AcoustIdResult>,
}

/// Wire-format query parameters for AcoustID lookup.
#[derive(Debug, Serialize)]
pub(super) struct AcoustIdLookupQuery<'a> {
    /// API key issued by AcoustID.
    client: &'a str,
    /// Requested lookup metadata projections.
    meta: &'a str,
    /// Rounded clip duration in seconds.
    duration: u32,
    /// Encoded Chromaprint fingerprint text.
    fingerprint: &'a str,
    /// Output response format.
    format: &'a str,
}

/// One AcoustID candidate match.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AcoustIdResult {
    /// Match confidence score in `[0, 1]`.
    score: Option<f64>,
    /// Candidate recordings for this fingerprint match.
    #[serde(default)]
    recordings: Vec<AcoustIdRecording>,
}

/// Recording payload returned in AcoustID lookup metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AcoustIdRecording {
    /// MusicBrainz recording identifier.
    id: String,
    /// Optional release candidates linked to this recording.
    #[serde(default)]
    releases: Vec<AcoustIdRelease>,
}

/// Release payload returned in AcoustID lookup metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AcoustIdRelease {
    /// MusicBrainz release identifier.
    id: String,
}

/// Selected AcoustID match values used for downstream fetches.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct AcoustIdMatch {
    /// Selected recording MBID, when available.
    pub(super) recording_mbid: Option<String>,
    /// Selected release MBID, when available.
    pub(super) release_mbid: Option<String>,
}

/// Cache key for one fingerprint lookup at one endpoint.
///
/// The full endpoint URL is keyed as configured: the client dials exactly
/// that URL, so two endpoints differing in scheme or path are different
/// services and get their own rows. The API key stays out of the key;
/// credentials never land in the on-disk index. Duration needs no place
/// here either — it is rounded from the same decode that produced the
/// fingerprint, so one file always yields the same pair.
pub(super) fn acoustid_lookup_cache_key(fingerprint: &str, endpoint: &str) -> String {
    format!("fingerprint={fingerprint}|endpoint={endpoint}")
}

/// Picks the best recording/release pair from one lookup answer.
///
/// Errors only when the service answered with a non-`ok` status. An ok
/// answer with no usable candidates yields an empty match.
fn select_acoustid_match(response: AcoustIdLookupResponse) -> anyhow::Result<AcoustIdMatch> {
    if !response.status.eq_ignore_ascii_case("ok") {
        bail!("AcoustID lookup returned status '{}'", response.status);
    }

    let mut ordered_results = response.results;
    ordered_results.sort_by(|left, right| {
        right
            .score
            .unwrap_or_default()
            .partial_cmp(&left.score.unwrap_or_default())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let Some(best) = ordered_results.into_iter().next() else {
        return Ok(AcoustIdMatch::default());
    };

    let Some(recording) = best.recordings.into_iter().find(|recording| !recording.id.is_empty())
    else {
        return Ok(AcoustIdMatch::default());
    };

    let release_mbid: Option<String> = recording
        .releases
        .into_iter()
        .map(|release| release.id)
        .find(|value| !value.trim().is_empty());

    Ok(AcoustIdMatch { recording_mbid: Some(recording.id), release_mbid })
}

/// Calls the AcoustID lookup API and returns the highest-scoring MBID pair.
///
/// A fresh cached answer for the same fingerprint and endpoint skips the
/// network entirely; a successfully decoded `ok` answer is cached on the way
/// out. Failures — transport, non-2xx, non-`ok` status — propagate and cache
/// nothing.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
pub(super) async fn lookup_acoustid_match(
    endpoint: &str,
    api_key: &str,
    fingerprint: &str,
    duration_seconds: u32,
    cache: &MediaTaggerHttpCache,
) -> anyhow::Result<AcoustIdMatch> {
    let cache_key = acoustid_lookup_cache_key(fingerprint, endpoint);
    if let Some(cached) = cache.read_acoustid_lookup(&cache_key).await
        && cached.is_fresh
    {
        return select_acoustid_match(cached.payload);
    }

    let http_client = shared_http_client()
        .map_err(|error| anyhow::anyhow!("initializing shared HTTP client failed: {error}"))?;

    let payload = http_client
        .get(endpoint)
        .query(&AcoustIdLookupQuery {
            client: api_key,
            // Keep payload lean: only recording/release MBIDs are needed for
            // subsequent MusicBrainz fetches.
            meta: "recordings releases",
            duration: duration_seconds,
            fingerprint,
            format: "json",
        })
        .send()
        .await
        .context("sending AcoustID lookup request")?
        .error_for_status()
        .context("AcoustID lookup returned non-success status")?
        .text()
        .await
        .context("reading AcoustID lookup response body")?;

    let response = serde_json::from_str::<AcoustIdLookupResponse>(&payload)
        .context("decoding AcoustID lookup response JSON")?;

    if response.status.eq_ignore_ascii_case("ok") {
        let _ = cache.write_acoustid_lookup(&cache_key, &response).await;
    }

    select_acoustid_match(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtins::media_tagger::cover_art::{CacheExpiryPolicy, MediaTaggerHttpCache};

    /// One cache plus a wiremock server, the pair every lookup test needs.
    async fn lookup_harness(
        expiry: CacheExpiryPolicy,
    ) -> (MediaTaggerHttpCache, wiremock::MockServer) {
        let server = wiremock::MockServer::start().await;
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(Some(root.path().to_path_buf()), expiry);
        (cache, server)
    }

    /// A canned AcoustID answer naming one recording and release.
    fn recording_answer() -> &'static str {
        r#"{"status":"ok","results":[{"score":0.9,"recordings":[{"id":"rec-1","releases":[{"id":"rel-1"}]}]}]}"#
    }

    async fn request_count(server: &wiremock::MockServer) -> usize {
        server.received_requests().await.expect("request recorder is enabled").len()
    }

    /// A repeat lookup is served from the cache, not the network.
    #[tokio::test]
    async fn a_repeat_lookup_is_served_from_the_cache() {
        let (cache, server) = lookup_harness(CacheExpiryPolicy::from_seconds(60)).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_raw(recording_answer(), "application/json"),
            )
            .mount(&server)
            .await;
        let endpoint = format!("{}/v2/lookup", server.uri());

        let first = lookup_acoustid_match(&endpoint, "key", "fp-1", 12, &cache)
            .await
            .expect("first lookup");
        let second = lookup_acoustid_match(&endpoint, "key", "fp-1", 12, &cache)
            .await
            .expect("repeat lookup");

        assert_eq!(first, second, "the repeat must answer the same match");
        assert_eq!(request_count(&server).await, 1, "the repeat lookup must not reach the network");
    }

    /// A different endpoint URL is a different service: it misses and queries.
    #[tokio::test]
    async fn a_different_endpoint_misses_the_cached_answer() {
        let (cache, server) = lookup_harness(CacheExpiryPolicy::from_seconds(60)).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_raw(recording_answer(), "application/json"),
            )
            .mount(&server)
            .await;
        let first_endpoint = format!("{}/v2/lookup", server.uri());
        let second_endpoint = format!("{}/other/lookup", server.uri());

        lookup_acoustid_match(&first_endpoint, "key", "fp-1", 12, &cache)
            .await
            .expect("first endpoint lookup");
        lookup_acoustid_match(&second_endpoint, "key", "fp-1", 12, &cache)
            .await
            .expect("second endpoint lookup");

        assert_eq!(
            request_count(&server).await,
            2,
            "a fingerprint cached for one endpoint must not satisfy another"
        );
    }

    /// A zero-match answer is a real answer, so it gets cached too.
    #[tokio::test]
    async fn a_zero_match_answer_is_cached() {
        let (cache, server) = lookup_harness(CacheExpiryPolicy::from_seconds(60)).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_raw(r#"{"status":"ok","results":[]}"#, "application/json"),
            )
            .mount(&server)
            .await;
        let endpoint = format!("{}/v2/lookup", server.uri());

        let first = lookup_acoustid_match(&endpoint, "key", "fp-1", 12, &cache)
            .await
            .expect("first lookup");
        let second = lookup_acoustid_match(&endpoint, "key", "fp-1", 12, &cache)
            .await
            .expect("repeat lookup");

        assert_eq!(first, AcoustIdMatch::default(), "no results means no match");
        assert_eq!(second, AcoustIdMatch::default(), "the cached empty answer stands");
        assert_eq!(request_count(&server).await, 1, "the empty answer must be cached");
    }

    /// A failed lookup is an error and caches nothing, so a retry queries again.
    #[tokio::test]
    async fn a_failed_lookup_caches_nothing() {
        let (cache, server) = lookup_harness(CacheExpiryPolicy::from_seconds(60)).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_raw(recording_answer(), "application/json"),
            )
            .mount(&server)
            .await;
        let endpoint = format!("{}/v2/lookup", server.uri());

        let failed = lookup_acoustid_match(&endpoint, "key", "fp-1", 12, &cache).await;
        assert!(failed.is_err(), "a 500 must surface as an error, not a silent empty answer");

        let retried = lookup_acoustid_match(&endpoint, "key", "fp-1", 12, &cache).await;
        assert!(
            retried.as_ref().is_ok_and(|m| m.recording_mbid.as_deref() == Some("rec-1")),
            "the retry must reach the server and take the fresh answer, got {retried:?}"
        );
        assert_eq!(
            request_count(&server).await,
            2,
            "the failure must leave nothing to serve the retry"
        );
    }
}
