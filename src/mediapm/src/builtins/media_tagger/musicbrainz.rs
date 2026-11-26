//! MusicBrainz payload fetching and FFmetadata mapping helpers.
//!
//! Recording and release payloads are fetched over
//! `mediapm_conductor::http::client::shared_http_client()` (shared User-Agent,
//! timeout, and connect policy) by building ws/2 URLs with `url::Url` and
//! deserializing responses into `musicbrainz_rs` entity serde types. Each
//! entity fetch is paced (one request slot per paced attempt), retried by a
//! mediapm-owned policy (production: 6 attempts, 16s max delay, clamped
//! `Retry-After` seconds or exponential backoff `250ms * 2^(attempt-1)` capped
//! with up to 20% subtractive deterministic jitter), and bounded by a 30s
//! per-request timeout floor (`MEDIAPM_HTTP_TIMEOUT_SECONDS`) and a 90s outer
//! recording+release deadline. Retryable outcomes are 408/425/429/5xx and
//! transport errors; other 4xx, URI, and JSON parse failures fail fast. The
//! endpoint scheme is honored end to end while the payload cache key remains
//! host:port-based. Fresh cache hits short-circuit fetches; fetch failures are
//! not masked by stale cache rows.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::Context;
use mediapm_conductor::http::client::shared_http_client;
use musicbrainz_rs::entity::recording::Recording;
use musicbrainz_rs::entity::release::Release;
use parking_lot::Mutex;
use reqwest::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use url::Url;

use super::cover_art::MediaTaggerHttpCache;
use super::util::{
    artist_credit_text, flatten_entity_json, join_unique, resolve_track_position,
    sanitize_metadata_key, truncate_metadata_value,
};

/// User-specific MusicBrainz data extracted from entity responses.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[expect(
    clippy::struct_field_names,
    reason = "user_ prefix distinguishes MusicBrainz user-data fields from generic payload fields"
)]
pub(super) struct MusicBrainzUserData {
    /// User-provided genres as `(name, count)` pairs.
    pub(super) user_genres: Vec<(String, u32)>,
    /// User-provided tags as `(name, count)` pairs.
    pub(super) user_tags: Vec<(String, u32)>,
    /// User rating value, when present.
    pub(super) user_rating: Option<u32>,
    /// User collection IDs the entity belongs to.
    pub(super) user_collections: Vec<String>,
}

/// Recording + optional release payload fetched from MusicBrainz.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MusicBrainzPayload {
    /// Recording entity resolved from MusicBrainz API.
    pub(super) recording: Recording,
    /// Optional release entity resolved from MusicBrainz API.
    pub(super) release: Option<Release>,
    /// User-specific data extracted from the recording response.
    #[serde(default)]
    pub(super) recording_user_data: MusicBrainzUserData,
    /// User-specific data extracted from the release response.
    #[serde(default)]
    pub(super) release_user_data: MusicBrainzUserData,
}

/// Env var naming the per-request timeout in whole seconds, the same name
/// the conductor's shared HTTP client reads (see `src/mediapm-conductor/src/http/client.rs`).
const REQUEST_TIMEOUT_ENV: &str = "MEDIAPM_HTTP_TIMEOUT_SECONDS";

/// How many attempts one recording or release fetch makes before failing.
pub const MUSICBRAINZ_FETCH_ATTEMPTS: u32 = 6;

/// The minimum interval between two `MusicBrainz` requests.
///
/// The web service rate limits by interval rather than by burst, and its
/// limiter answers `503` when callers ignore that. The spacing is enforced on
/// this side because each paced slot corresponds to one API request issued by
/// this transport. Spacing requests is cheaper than retrying them.
const MUSICBRAINZ_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1);

/// Base delay for exponential retry backoff when no server `Retry-After` is
/// provided.
const MUSICBRAINZ_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

/// The next free request slot and the wait until it.
///
/// Pure, so the spacing rule is testable without a clock: `last` is the slot
/// the previous request reserved, `now` the current instant, and the returned
/// instant is the slot this request takes. A caller that arrives after its slot
/// has already passed takes the current instant rather than a slot in the past,
/// which is what keeps the interval from compressing once a queue drains.
fn next_pacing_slot(
    last: Option<Instant>,
    now: Instant,
    interval: Duration,
) -> (Instant, Duration) {
    match last {
        Some(last) => {
            let slot = last + interval;
            (slot.max(now), slot.saturating_duration_since(now))
        }
        None => (now, Duration::ZERO),
    }
}

/// Waits until this process may send another `MusicBrainz` request.
///
/// The slot is reserved before the wait, so concurrent callers queue instead of
/// racing: each takes the next slot and sleeps until it. The wait runs inside the
/// fetch deadline, because one second of interval is small against the whole
/// pair's budget; the retry loop's own backoff is bounded the same way.
async fn pace_musicbrainz_request() {
    static LAST_SLOT: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
    let wait = {
        let mut last = LAST_SLOT.get_or_init(|| Mutex::new(None)).lock();
        let (slot, wait) =
            next_pacing_slot(*last, Instant::now(), MUSICBRAINZ_MIN_REQUEST_INTERVAL);
        *last = Some(slot);
        wait
    };
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }
}

/// Parse the request timeout from the documented env var: whole seconds,
/// floored at the conductor client's documented 30-second floor, defaulting
/// to 30 rather than the conductor's 1800 — a tagging step that stalls on
/// MusicBrainz gives its budget back to the run instead of holding it.
fn parse_musicbrainz_timeout(raw: Option<&str>) -> Duration {
    let trimmed = raw.map(str::trim).filter(|value| !value.is_empty());
    match trimmed.map(str::parse::<u64>) {
        Some(Ok(seconds)) => Duration::from_secs(seconds.max(30)),
        Some(Err(_)) => {
            tracing::warn!(
                "{REQUEST_TIMEOUT_ENV} is not a whole number of seconds; using the 30s default"
            );
            Duration::from_secs(30)
        }
        None => Duration::from_secs(30),
    }
}

/// The env-derived per-request timeout for this path.
fn musicbrainz_timeout() -> Duration {
    parse_musicbrainz_timeout(std::env::var(REQUEST_TIMEOUT_ENV).ok().as_deref())
}

/// The outer wall-clock budget for one recording+release fetch pair.
///
/// The per-request timeout bounds a single HTTP call, but server-controlled
/// `Retry-After` waits and retry backoff can span multiple calls. Wrapping the
/// whole fetch in this deadline bounds pacing, per-entity retry/backoff
/// (including clamped `Retry-After`), and requests for the whole
/// recording+release pair.
pub(super) const MUSICBRAINZ_FETCH_DEADLINE: Duration = Duration::from_secs(90);

/// Retry policy for one MusicBrainz entity fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MusicBrainzRetryPolicy {
    /// Maximum number of attempts for one entity fetch.
    pub(super) attempts: u32,
    /// Upper bound for any retry delay, including a server `Retry-After`.
    pub(super) max_delay: Duration,
}

impl MusicBrainzRetryPolicy {
    /// Production policy: 6 attempts, 16s max delay.
    pub(super) fn production() -> Self {
        Self { attempts: MUSICBRAINZ_FETCH_ATTEMPTS, max_delay: Duration::from_secs(16) }
    }
}

/// Retry delay for one failed attempt (1-indexed attempt number that just failed).
///
/// `retry_after_secs` is the server's `Retry-After` value in whole seconds when
/// present and parseable. When `retry_after_secs` is `Some`, delay is
/// `min(retry_after, max_delay)`. Otherwise exponential backoff
/// `250ms * 2^(attempt-1)` capped at `max_delay`, with up to 20% subtracted as
/// jitter derived from `jitter_seed` (deterministic for a given seed).
pub(super) fn retry_delay(
    attempt: u32,
    policy: MusicBrainzRetryPolicy,
    retry_after_secs: Option<u64>,
    jitter_seed: u64,
) -> Duration {
    if let Some(secs) = retry_after_secs {
        return Duration::from_secs(secs).min(policy.max_delay);
    }
    let base =
        MUSICBRAINZ_RETRY_BASE_DELAY.saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1)));
    let capped = base.min(policy.max_delay);
    let jitter_pct = (jitter_seed % 21) as u32;
    capped.saturating_sub(capped * jitter_pct / 100)
}

/// The host\[:port\] one configured endpoint URL resolves to.
///
/// The scheme is ignored by the cache-key builder, and both the URL builder and
/// the cache-key builder call this, so the row a fetch writes is the row the
/// next fetch with the same shape reads.
///
/// An endpoint that does not parse, or that names no host, fails here — a
/// tagging step pointed at a broken endpoint must surface that broken
/// configuration instead of quietly fetching from somewhere else.
fn musicbrainz_host_for_endpoint(endpoint: &str) -> anyhow::Result<String> {
    let url = Url::parse(endpoint)
        .with_context(|| format!("parsing MusicBrainz endpoint '{endpoint}'"))?;
    let host =
        url.host_str().with_context(|| format!("MusicBrainz endpoint '{endpoint}' has no host"))?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    })
}

/// Every input that changes what one MusicBrainz fetch asks for, bundled so
/// the fetch and its cache row cannot drift apart.
///
/// `write_all_tags` and `release_ars` add annotation/relation includes to
/// the requests, and `endpoint` selects the server. The fetch and the cache
/// key are built from this one value, so adding a field forces the
/// construction site that fills the shape into view instead of leaving a
/// stale-cache hole.
#[derive(Debug, Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "fetch shape mirrors independent MusicBrainz request dimensions"
)]
pub(super) struct MusicBrainzFetchShape {
    /// Whether the extended Picard tag block was requested.
    pub(super) write_all_tags: bool,
    /// Whether recording/release-group level relations were requested.
    pub(super) release_ars: bool,
    /// MusicBrainz endpoint URL.
    pub(super) endpoint: String,
    /// Whether user-genre data should be requested.
    pub(super) include_user_genres: bool,
    /// Whether user-rating data should be requested.
    pub(super) include_user_ratings: bool,
    /// Whether user-collection data should be requested.
    pub(super) include_user_collections: bool,
    /// Whether user-tag data should be requested.
    pub(super) include_user_tags: bool,
    /// Whether genre tags are enabled.
    pub(super) use_genres: bool,
    /// Whether to only use the authenticated user's own genres/tags.
    pub(super) only_my_genres: bool,
}

impl MusicBrainzFetchShape {
    /// The cache key for one recording/release pair under this fetch shape.
    ///
    /// The scheme is deliberately absent: `http://x` and `https://x`
    /// produce equivalent cached rows. The deadline and API credentials are
    /// absent too — one is latency policy, the other must never land in the
    /// on-disk index. User-data flags are included because they change the
    /// requested `inc` set and response shape.
    pub(super) fn cache_key(
        &self,
        recording_mbid: &str,
        release_mbid: Option<&str>,
    ) -> anyhow::Result<String> {
        let release_mbid =
            release_mbid.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("");
        let domain = musicbrainz_host_for_endpoint(&self.endpoint)?;
        Ok(format!(
            "recording={recording_mbid}|release={release_mbid}|all_tags={}|ars={}|endpoint={domain}|user_genres={}|user_ratings={}|user_collections={}|user_tags={}|use_genres={}|only_my_genres={}",
            u8::from(self.write_all_tags),
            u8::from(self.release_ars),
            u8::from(self.include_user_genres),
            u8::from(self.include_user_ratings),
            u8::from(self.include_user_collections),
            u8::from(self.include_user_tags),
            u8::from(self.use_genres),
            u8::from(self.only_my_genres),
        ))
    }
}

/// Returns the normalized base URL for one configured endpoint.
///
/// A trailing slash is ensured so joining `recording/{id}` or `release/{id}`
/// produces `{endpoint}/recording/{id}` regardless of whether the configured
/// endpoint ended with a slash.
fn normalized_endpoint_base(endpoint: &str) -> anyhow::Result<Url> {
    let mut base = Url::parse(endpoint)
        .with_context(|| format!("parsing MusicBrainz endpoint '{endpoint}'"))?;
    if base.host_str().is_none() {
        anyhow::bail!("MusicBrainz endpoint '{endpoint}' has no host");
    }
    if !base.path().ends_with('/') {
        base.path_segments_mut()
            .map_err(|()| {
                anyhow::anyhow!("MusicBrainz endpoint '{endpoint}' cannot be a base URL")
            })?
            .push("");
    }
    Ok(base)
}

/// Builds the entity fetch URL with `fmt=json` and the requested `inc` tokens.
fn entity_url(base: &Url, entity: &str, mbid: &str, includes: &[&str]) -> anyhow::Result<Url> {
    let mut url = base
        .join(&format!("{entity}/{mbid}"))
        .with_context(|| format!("building MusicBrainz {entity} URL for '{mbid}'"))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("fmt", "json");
        pairs.append_pair("inc", &includes.join(" "));
    }
    Ok(url)
}

/// Recording `inc` tokens for the requested fetch shape.
fn recording_includes(shape: &MusicBrainzFetchShape) -> Vec<&'static str> {
    let mut includes = vec!["artists", "releases", "isrcs"];
    if shape.use_genres && !shape.only_my_genres {
        includes.extend(["genres", "tags"]);
    }
    if shape.write_all_tags {
        includes.extend(["annotation", "work-level-rels"]);
    }
    if shape.include_user_genres || (shape.use_genres && shape.only_my_genres) {
        includes.push("user-genres");
    }
    if shape.include_user_ratings {
        includes.push("user-ratings");
    }
    if shape.include_user_tags || (shape.use_genres && shape.only_my_genres) {
        includes.push("user-tags");
    }
    includes
}

/// Release `inc` tokens for the requested fetch shape.
fn release_includes(shape: &MusicBrainzFetchShape) -> Vec<&'static str> {
    let mut includes = vec!["artists", "recordings", "release-groups", "media", "labels", "isrcs"];
    if shape.use_genres && !shape.only_my_genres {
        includes.extend(["genres", "tags"]);
    }
    if shape.write_all_tags {
        includes.push("annotation");
    }
    if shape.write_all_tags || shape.release_ars {
        includes.push("recording-level-rels");
        includes.push("release-group-level-rels");
    }
    if shape.include_user_genres || (shape.use_genres && shape.only_my_genres) {
        includes.push("user-genres");
    }
    if shape.include_user_ratings {
        includes.push("user-ratings");
    }
    if shape.include_user_collections {
        includes.push("user-collections");
    }
    if shape.include_user_tags || (shape.use_genres && shape.only_my_genres) {
        includes.push("user-tags");
    }
    includes
}

/// Returns `Some(seconds)` when the response carries a whole-second `Retry-After`.
fn retry_after_secs(response: &reqwest::Response) -> Option<u64> {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
}

/// Whether an HTTP status warrants a retry.
fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 425 | 429) || status.is_server_error()
}

/// Fetches and deserializes one MusicBrainz entity with per-attempt pacing and
/// the configured retry policy.
async fn fetch_entity<T: DeserializeOwned>(
    client: &Client,
    url: &Url,
    entity_label: &str,
    policy: MusicBrainzRetryPolicy,
    request_timeout: Duration,
    bearer_token: Option<&str>,
) -> anyhow::Result<(T, serde_json::Value)> {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=policy.attempts {
        pace_musicbrainz_request().await;
        let mut request = client.get(url.clone()).timeout(request_timeout);
        if let Some(token) = bearer_token {
            request = request.bearer_auth(token);
        }
        let send_result = request.send().await;
        match send_result {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    let bytes = response.bytes().await.with_context(|| {
                        format!("reading MusicBrainz {entity_label} response body from '{url}'")
                    })?;
                    let value: serde_json::Value =
                        serde_json::from_slice(&bytes).with_context(|| {
                            format!(
                                "deserializing MusicBrainz {entity_label} response from '{url}'"
                            )
                        })?;
                    let typed = serde_json::from_value::<T>(value.clone()).with_context(|| {
                        format!("deserializing MusicBrainz {entity_label} response from '{url}'")
                    })?;
                    return Ok((typed, value));
                }
                if is_retryable_status(status) {
                    let retry_after = retry_after_secs(&response);
                    let error = anyhow::anyhow!(
                        "MusicBrainz {entity_label} request to '{url}' returned retryable status {status}"
                    );
                    if attempt >= policy.attempts {
                        return Err(error);
                    }
                    last_error = Some(error);
                    let delay = retry_delay(attempt, policy, retry_after, u64::from(attempt));
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(anyhow::anyhow!(
                    "MusicBrainz {entity_label} request to '{url}' returned status {status}"
                ));
            }
            Err(error) => {
                let wrapped = anyhow::Error::new(error)
                    .context(format!("requesting MusicBrainz {entity_label} from '{url}'"));
                if attempt >= policy.attempts {
                    return Err(wrapped);
                }
                last_error = Some(wrapped);
                let delay = retry_delay(attempt, policy, None, u64::from(attempt));
                tokio::time::sleep(delay).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!("MusicBrainz {entity_label} request to '{url}' failed without attempts")
    }))
}

/// Extracts user-data fields from a raw MusicBrainz entity JSON value.
fn extract_user_data(value: &serde_json::Value) -> MusicBrainzUserData {
    let user_genres = value
        .get("user-genres")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let name = item.get("name").and_then(serde_json::Value::as_str)?.to_owned();
                    let count = item
                        .get("count")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(1)
                        .try_into()
                        .unwrap_or(u32::MAX);
                    Some((name, count))
                })
                .collect()
        })
        .unwrap_or_default();
    let user_tags = value
        .get("user-tags")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let name = item.get("name").and_then(serde_json::Value::as_str)?.to_owned();
                    let count = item
                        .get("count")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(1)
                        .try_into()
                        .unwrap_or(u32::MAX);
                    Some((name, count))
                })
                .collect()
        })
        .unwrap_or_default();
    let user_rating = value.get("user-rating").and_then(|v| {
        v.get("value")
            .and_then(serde_json::Value::as_f64)
            .and_then(|n| {
                let rounded = n.round();
                if rounded.is_finite() && (0.0..=f64::from(u32::MAX)).contains(&rounded) {
                    #[expect(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "rounded rating is finite and clamped to the u32 range"
                    )]
                    Some(rounded as u32)
                } else {
                    None
                }
            })
            .or_else(|| v.as_u64().and_then(|n| u32::try_from(n).ok()))
    });
    let user_collections = value
        .get("user-collections")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.get("id").and_then(serde_json::Value::as_str).map(ToOwned::to_owned)
                })
                .collect()
        })
        .unwrap_or_default();
    MusicBrainzUserData { user_genres, user_tags, user_rating, user_collections }
}

/// Fetches one recording and, when resolvable, its release.
///
/// Each entity fetch is paced and retried independently according to `policy`.
async fn fetch_pair_with_policy(
    client: &Client,
    shape: &MusicBrainzFetchShape,
    recording_mbid: &str,
    release_mbid: Option<&str>,
    policy: MusicBrainzRetryPolicy,
    bearer_token: Option<&str>,
) -> anyhow::Result<MusicBrainzPayload> {
    let base = normalized_endpoint_base(&shape.endpoint)?;
    let request_timeout = musicbrainz_timeout();

    let recording_url = entity_url(&base, "recording", recording_mbid, &recording_includes(shape))?;
    let (recording, recording_value): (Recording, serde_json::Value) = fetch_entity(
        client,
        &recording_url,
        &format!("recording '{recording_mbid}'"),
        policy,
        request_timeout,
        bearer_token,
    )
    .await
    .with_context(|| format!("fetching MusicBrainz recording '{recording_mbid}'"))?;

    let explicit_release =
        release_mbid.map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned);
    let release_id = explicit_release.or_else(|| {
        recording
            .releases
            .as_ref()
            .and_then(|releases| releases.first())
            .map(|release| release.id.clone())
    });

    let (release, release_value) = if let Some(release_id) = release_id {
        let release_url = entity_url(&base, "release", &release_id, &release_includes(shape))?;
        let (release, release_value): (Release, serde_json::Value) = fetch_entity(
            client,
            &release_url,
            &format!("release '{release_id}'"),
            policy,
            request_timeout,
            bearer_token,
        )
        .await
        .with_context(|| format!("fetching MusicBrainz release '{release_id}'"))?;
        (Some(release), Some(release_value))
    } else {
        (None, None)
    };

    Ok(MusicBrainzPayload {
        recording_user_data: extract_user_data(&recording_value),
        release_user_data: release_value.as_ref().map(extract_user_data).unwrap_or_default(),
        recording,
        release,
    })
}

/// Fetches recording/release payloads through the shared HTTP client.
///
/// The whole recording+release fetch is bounded by `deadline`, which caps
/// pacing, per-entity retry/backoff (including clamped `Retry-After`), and
/// requests for the pair. A fetch failure propagates once the attempts are
/// spent: the caller decides what a failure means for the tagging step, and no
/// stale cache row is served in its place.
pub(super) async fn fetch_musicbrainz_payloads(
    shape: &MusicBrainzFetchShape,
    recording_mbid: &str,
    release_mbid: Option<&str>,
    deadline: Duration,
    cache: &MediaTaggerHttpCache,
    bearer_token: Option<&str>,
) -> anyhow::Result<MusicBrainzPayload> {
    let client = shared_http_client()
        .map_err(|error| anyhow::anyhow!("{error}"))
        .context("acquiring shared HTTP client for MusicBrainz fetch")?;
    fetch_musicbrainz_payloads_on(
        client,
        shape,
        recording_mbid,
        release_mbid,
        deadline,
        cache,
        bearer_token,
    )
    .await
}

/// The fetch itself, against an explicit client and the production retry policy.
pub(super) async fn fetch_musicbrainz_payloads_on(
    client: &Client,
    shape: &MusicBrainzFetchShape,
    recording_mbid: &str,
    release_mbid: Option<&str>,
    deadline: Duration,
    cache: &MediaTaggerHttpCache,
    bearer_token: Option<&str>,
) -> anyhow::Result<MusicBrainzPayload> {
    fetch_musicbrainz_payloads_on_with_policy(
        client,
        shape,
        recording_mbid,
        release_mbid,
        deadline,
        cache,
        MusicBrainzRetryPolicy::production(),
        bearer_token,
    )
    .await
}

/// The fetch itself, against an explicit client and retry policy.
///
/// Tests point it at a local server and a tiny policy without touching
/// production construction. The shape carries both the fetch inputs and the
/// cache key, so a payload is only ever read from or written to the row of the
/// exact shape that produced it.
#[expect(
    clippy::too_many_arguments,
    reason = "bearer token and retry policy are independent fetch dimensions; callers pass one request shape"
)]
pub(super) async fn fetch_musicbrainz_payloads_on_with_policy(
    client: &Client,
    shape: &MusicBrainzFetchShape,
    recording_mbid: &str,
    release_mbid: Option<&str>,
    deadline: Duration,
    cache: &MediaTaggerHttpCache,
    policy: MusicBrainzRetryPolicy,
    bearer_token: Option<&str>,
) -> anyhow::Result<MusicBrainzPayload> {
    let cache_key = shape.cache_key(recording_mbid, release_mbid)?;
    let cache_user_data = bearer_token.is_some()
        || shape.include_user_genres
        || shape.include_user_ratings
        || shape.include_user_collections
        || shape.include_user_tags;
    if !cache_user_data {
        let cached_payload = cache.read_musicbrainz_payload(&cache_key).await;
        if let Some(cached) = cached_payload.as_ref()
            && cached.is_fresh
        {
            return Ok(cached.payload.clone());
        }
    }

    let fetched_payload = tokio::time::timeout(deadline, async {
        fetch_pair_with_policy(client, shape, recording_mbid, release_mbid, policy, bearer_token)
            .await
    })
    .await
    .with_context(|| {
        format!(
            "fetching MusicBrainz entities from '{}' for recording \
             '{recording_mbid}' within {deadline:?}",
            shape.endpoint
        )
    })??;

    if !cache_user_data {
        let _ = cache.write_musicbrainz_payload(&cache_key, &fetched_payload).await;
    }
    Ok(fetched_payload)
}

/// Genre processing configuration for [`build_ffmetadata_map`].
#[derive(Debug, Clone)]
pub(super) struct GenreConfig {
    /// Whether genre output is enabled.
    pub(super) use_genres: bool,
    /// Whether to only use the user's own genres/tags.
    pub(super) only_my_genres: bool,
    /// Maximum number of genres to emit.
    pub(super) max_genres: usize,
    /// Minimum genre usage percentage relative to the top genre.
    pub(super) min_genre_usage: u8,
    /// Newline-delimited genre filter patterns.
    pub(super) genres_filter: String,
    /// Separator for joining genres; empty means multi-value.
    pub(super) join_genres: String,
}

/// Compiles newline-delimited `+include` / `-exclude` genre patterns.
///
/// Patterns are anchored, case-insensitive globs: every character except `*`
/// and `?` is escaped, so regex metacharacters in user configuration stay
/// literal.
fn parse_genre_filter(filter_text: &str) -> (Vec<regex::Regex>, Vec<regex::Regex>) {
    let mut include = Vec::new();
    let mut exclude = Vec::new();
    for line in filter_text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(prefix) = line.chars().next() else { continue };
        if prefix != '+' && prefix != '-' {
            continue;
        }
        let remain = line[prefix.len_utf8()..].trim();
        if remain.is_empty() {
            continue;
        }
        let mut pattern = String::from("^(?:");
        for character in remain.chars() {
            match character {
                '*' => pattern.push_str(".*"),
                '?' => pattern.push('.'),
                _ => pattern.push_str(&regex::escape(&character.to_string())),
            }
        }
        pattern.push_str(")$");
        let Ok(re) = regex::RegexBuilder::new(&pattern).case_insensitive(true).build() else {
            continue;
        };
        if prefix == '+' {
            include.push(re);
        } else {
            exclude.push(re);
        }
    }
    (include, exclude)
}

fn genre_name_matches(name: &str, include: &[regex::Regex], exclude: &[regex::Regex]) -> bool {
    if include.is_empty() && exclude.is_empty() {
        return true;
    }
    for re in include {
        if re.is_match(name) {
            return true;
        }
    }
    if !include.is_empty() {
        return false;
    }
    for re in exclude {
        if re.is_match(name) {
            return false;
        }
    }
    true
}

fn titlecase_genre(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut new_word = true;
    for ch in name.chars() {
        if ch.is_alphanumeric() {
            if new_word {
                for uc in ch.to_uppercase() {
                    out.push(uc);
                }
                new_word = false;
            } else {
                for lc in ch.to_lowercase() {
                    out.push(lc);
                }
            }
        } else {
            out.push(ch);
            new_word = true;
        }
    }
    out
}

/// Computes title-cased genre values for one payload.
///
/// A zero `max_genres` emits nothing, matching Picard's early return. Names are
/// folded to lowercase as they are counted, so `rock` and `Rock` share one
/// count and one limit slot.
fn compute_genre_values(payload: &MusicBrainzPayload, genre_config: &GenreConfig) -> Vec<String> {
    if !genre_config.use_genres || genre_config.max_genres == 0 {
        return Vec::new();
    }
    let mut counts: BTreeMap<String, (String, u32)> = BTreeMap::new();
    let mut add = |name: &str, count: u32| {
        let name = name.trim();
        if name.is_empty() || count == 0 {
            return;
        }
        let key = name.to_lowercase();
        let entry = counts.entry(key).or_insert_with(|| (name.to_string(), 0));
        entry.1 += count;
    };

    if genre_config.only_my_genres {
        for (name, _count) in &payload.recording_user_data.user_genres {
            add(name, 1);
        }
        for (name, _count) in &payload.release_user_data.user_genres {
            add(name, 1);
        }
        for (name, _count) in &payload.recording_user_data.user_tags {
            add(name, 1);
        }
        for (name, _count) in &payload.release_user_data.user_tags {
            add(name, 1);
        }
    } else {
        if let Some(genres) = payload.recording.genres.as_ref() {
            for genre in genres {
                let count = genre.count.unwrap_or(0);
                add(&genre.name, count);
            }
        }
        if let Some(genres) = payload.release.as_ref().and_then(|release| release.genres.as_ref()) {
            for genre in genres {
                let count = genre.count.unwrap_or(0);
                add(&genre.name, count);
            }
        }
        for (name, _count) in &payload.recording_user_data.user_genres {
            add(name, 1);
        }
        for (name, _count) in &payload.release_user_data.user_genres {
            add(name, 1);
        }
    }

    let (include, exclude) = parse_genre_filter(&genre_config.genres_filter);
    let mut filtered: Vec<(String, u32)> = Vec::new();
    for (name, count) in counts.values() {
        if genre_name_matches(name, &include, &exclude) {
            filtered.push((name.clone(), *count));
        }
    }
    if filtered.is_empty() {
        return Vec::new();
    }
    let top_count = filtered.iter().map(|(_, count)| *count).max().unwrap_or(0);
    if top_count == 0 {
        return Vec::new();
    }
    let mut by_threshold: Vec<(String, u32)> = filtered
        .into_iter()
        .filter(|(_, count)| {
            let percent =
                u32::try_from(u64::from(*count) * 100 / u64::from(top_count)).unwrap_or(u32::MAX);
            percent >= u32::from(genre_config.min_genre_usage)
        })
        .collect();
    by_threshold.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    by_threshold.truncate(genre_config.max_genres);
    let mut names: Vec<String> =
        by_threshold.into_iter().map(|(n, _)| titlecase_genre(&n)).collect();
    names.sort();
    names.dedup();
    names
}

/// Converts MusicBrainz payload into FFmetadata key/value map.
#[expect(
    clippy::too_many_lines,
    reason = "maps the full MusicBrainz payload into ffmetadata tags; user-data output is part of the same mapping"
)]
pub(super) fn build_ffmetadata_map(
    payload: &MusicBrainzPayload,
    write_all_tags: bool,
    genre_config: &GenreConfig,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut tags = BTreeMap::new();

    tags.insert("title".to_string(), payload.recording.title.clone());
    tags.insert("musicbrainz_recordingid".to_string(), payload.recording.id.clone());
    tags.insert("musicbrainz_trackid".to_string(), payload.recording.id.clone());

    if let Some(artist) = artist_credit_text(payload.recording.artist_credit.as_deref()) {
        tags.insert("artist".to_string(), artist);
    }

    if let Some(first_release_date) = payload.recording.first_release_date.as_ref() {
        tags.insert("originaldate".to_string(), first_release_date.0.clone());
    }

    if let Some(isrcs) = payload.recording.isrcs.as_ref()
        && !isrcs.is_empty()
    {
        tags.insert("isrc".to_string(), join_unique(isrcs.iter().map(String::as_str)));
    }

    if let Some(release) = payload.release.as_ref() {
        tags.insert("album".to_string(), release.title.clone());
        tags.insert("musicbrainz_albumid".to_string(), release.id.clone());

        if let Some(date) = release.date.as_ref() {
            tags.insert("date".to_string(), date.0.clone());
        }
        if let Some(barcode) = release.barcode.as_ref()
            && !barcode.trim().is_empty()
        {
            tags.insert("barcode".to_string(), barcode.clone());
        }
        if let Some(country) = release.country.as_ref()
            && !country.trim().is_empty()
        {
            tags.insert("releasecountry".to_string(), country.clone());
        }
        if let Some(label_info) = release.label_info.as_ref() {
            let labels = label_info
                .iter()
                .filter_map(|info| info.label.as_ref().map(|label| label.name.as_str()))
                .collect::<Vec<_>>();
            if !labels.is_empty() {
                tags.insert("label".to_string(), join_unique(labels));
            }

            let catalog_numbers = label_info
                .iter()
                .filter_map(|info| info.catalog_number.as_deref())
                .collect::<Vec<_>>();
            if !catalog_numbers.is_empty() {
                tags.insert("catalognumber".to_string(), join_unique(catalog_numbers));
            }
        }

        if let Some(release_group) = release.release_group.as_ref() {
            tags.insert("musicbrainz_releasegroupid".to_string(), release_group.id.clone());
            tags.insert("releasegroup".to_string(), release_group.title.clone());
        }

        if let Some(release_artist) = artist_credit_text(release.artist_credit.as_deref()) {
            tags.insert("albumartist".to_string(), release_artist);
        }

        if let Some(track_info) = resolve_track_position(&payload.recording.id, release) {
            tags.insert("tracknumber".to_string(), track_info.track_number);
            tags.insert("discnumber".to_string(), track_info.disc_number.to_string());
            if let Some(total_tracks) = track_info.total_tracks {
                tags.insert("tracktotal".to_string(), total_tracks.to_string());
                tags.insert("totaltracks".to_string(), total_tracks.to_string());
            }
            if let Some(total_discs) = track_info.total_discs {
                tags.insert("disctotal".to_string(), total_discs.to_string());
                tags.insert("totaldiscs".to_string(), total_discs.to_string());
            }
        }
    }

    if let Some(rating) =
        payload.recording_user_data.user_rating.or(payload.release_user_data.user_rating)
    {
        tags.insert("~rating".to_string(), rating.to_string());
    }
    if !payload.release_user_data.user_collections.is_empty() {
        tags.insert(
            "musicbrainz_collectionid".to_string(),
            join_unique(payload.release_user_data.user_collections.iter().map(String::as_str)),
        );
    }
    if !payload.recording_user_data.user_tags.is_empty()
        || !payload.release_user_data.user_tags.is_empty()
    {
        let mut all_user_tags: Vec<String> = payload
            .recording_user_data
            .user_tags
            .iter()
            .chain(payload.release_user_data.user_tags.iter())
            .map(|(name, _)| name.as_str().to_owned())
            .collect();
        all_user_tags.sort();
        all_user_tags.dedup();
        if !all_user_tags.is_empty() {
            tags.insert(
                "~usertags".to_string(),
                join_unique(all_user_tags.iter().map(String::as_str)),
            );
        }
    }
    let genre_values = compute_genre_values(payload, genre_config);
    if !genre_values.is_empty() && !genre_config.join_genres.is_empty() {
        tags.insert("genre".to_string(), genre_values.join(&genre_config.join_genres));
    }

    if write_all_tags {
        let mut recording_flattened = BTreeMap::new();
        flatten_entity_json(
            "musicbrainz_recording",
            &serde_json::to_value(&payload.recording)?,
            &mut recording_flattened,
        );
        let mut release_flattened = BTreeMap::new();
        if let Some(release) = payload.release.as_ref() {
            flatten_entity_json(
                "musicbrainz_release",
                &serde_json::to_value(release)?,
                &mut release_flattened,
            );
        }

        insert_extended_picard_tags(&mut tags, &recording_flattened, &release_flattened);
    }

    let mut entries: Vec<(String, String)> = tags.into_iter().collect();
    if !genre_values.is_empty() && genre_config.join_genres.is_empty() {
        for value in genre_values {
            entries.push(("genre".to_string(), value));
        }
    }

    Ok(entries)
}

/// Inserts optional Picard-compatible tags from flattened MusicBrainz payloads.
///
/// This helper intentionally emits only known Picard tag keys and rejects the
/// previous broad flatten/alias expansion to avoid non-Picard metadata keys.
pub(super) fn insert_extended_picard_tags(
    tags: &mut BTreeMap<String, String>,
    recording_flattened: &BTreeMap<String, String>,
    release_flattened: &BTreeMap<String, String>,
) {
    for (alias, source_key) in [
        ("albumartistsort", "musicbrainz_release_artist_credit_0_artist_sort_name"),
        ("artistsort", "musicbrainz_recording_artist_credit_0_artist_sort_name"),
        ("artistcredit", "artist"),
        ("albumartistcredit", "albumartist"),
        ("releasestatus", "musicbrainz_release_status"),
        ("releasetype", "musicbrainz_release_release_group_primary_type"),
        ("secondaryreleasetype", "musicbrainz_release_release_group_secondary_types"),
        ("primaryreleasetype", "musicbrainz_release_release_group_primary_type"),
        ("releasegroup", "musicbrainz_release_release_group_title"),
        ("musicbrainz_releasegroupid", "musicbrainz_release_release_group_id"),
        ("musicbrainz_artistid", "musicbrainz_recording_artist_credit_0_artist_id"),
        ("musicbrainz_albumartistid", "musicbrainz_release_artist_credit_0_artist_id"),
        ("recordingtitle", "musicbrainz_recording_title"),
        ("recordingcomment", "musicbrainz_recording_disambiguation"),
        ("releasecomment", "musicbrainz_release_disambiguation"),
        ("releasegroupcomment", "musicbrainz_release_release_group_disambiguation"),
        ("releaselanguage", "musicbrainz_release_text_representation_language"),
        ("script", "musicbrainz_release_text_representation_script"),
        ("releaseannotation", "musicbrainz_release_annotation"),
        ("recordingannotation", "musicbrainz_recording_annotation"),
        ("releasedate", "date"),
        ("releasecountry", "musicbrainz_release_country"),
        ("media", "musicbrainz_release_media_0_format"),
        ("asin", "musicbrainz_release_asin"),
    ] {
        if let Some(value) = tags.get(source_key).cloned().or_else(|| {
            recording_flattened
                .get(source_key)
                .cloned()
                .or_else(|| release_flattened.get(source_key).cloned())
        }) {
            insert_tag_if_absent(tags, alias, &value);
        }
    }
}

/// Inserts one normalized tag key/value when key is absent and value is non-empty.
pub(super) fn insert_tag_if_absent(tags: &mut BTreeMap<String, String>, key: &str, value: &str) {
    if value.trim().is_empty() {
        return;
    }
    tags.entry(sanitize_metadata_key(key)).or_insert_with(|| truncate_metadata_value(value));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtins::media_tagger::cover_art::{CacheExpiryPolicy, MediaTaggerHttpCache};
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn temp_cache() -> (mediapm_utils::temp::ManagedTempDir, MediaTaggerHttpCache) {
        let dir = mediapm_utils::temp::cache_dir().expect("temp cache dir");
        let cache = MediaTaggerHttpCache::new(
            Some(dir.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(60),
        );
        (dir, cache)
    }

    fn recording_mbid() -> &'static str {
        "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae"
    }

    fn test_shape(endpoint: &str) -> MusicBrainzFetchShape {
        MusicBrainzFetchShape {
            write_all_tags: false,
            release_ars: false,
            endpoint: endpoint.to_string(),
            include_user_genres: false,
            include_user_ratings: false,
            include_user_collections: false,
            include_user_tags: false,
            use_genres: false,
            only_my_genres: false,
        }
    }

    #[test]
    fn fetch_attempts_constant_is_six() {
        assert_eq!(MUSICBRAINZ_FETCH_ATTEMPTS, 6);
        assert_eq!(MusicBrainzRetryPolicy::production().attempts, 6);
    }

    #[test]
    fn retry_delay_honors_retry_after_clamped_to_policy_max() {
        let production = MusicBrainzRetryPolicy::production();
        assert_eq!(retry_delay(1, production, Some(30), 0), Duration::from_secs(16));
        assert_eq!(retry_delay(1, production, Some(16), 0), Duration::from_secs(16));
        assert_eq!(retry_delay(1, production, Some(1), 0), Duration::from_secs(1));
        assert_eq!(retry_delay(1, production, Some(0), 0), Duration::from_secs(0));

        let tiny = MusicBrainzRetryPolicy { attempts: 6, max_delay: Duration::from_secs(1) };
        assert_eq!(retry_delay(1, tiny, Some(30), 0), Duration::from_secs(1));
    }

    #[test]
    fn retry_delay_is_exponential_capped_and_deterministically_jittered() {
        let policy = MusicBrainzRetryPolicy::production();
        assert_eq!(retry_delay(1, policy, None, 0), Duration::from_millis(250));
        assert_eq!(retry_delay(2, policy, None, 0), Duration::from_millis(500));
        assert_eq!(retry_delay(3, policy, None, 0), Duration::from_millis(1000));
        assert_eq!(retry_delay(4, policy, None, 0), Duration::from_millis(2000));
        assert_eq!(retry_delay(5, policy, None, 0), Duration::from_millis(4000));
        assert_eq!(retry_delay(6, policy, None, 0), Duration::from_millis(8000));
        assert_eq!(retry_delay(20, policy, None, 0), Duration::from_secs(16));

        for attempt in [1u32, 2, 3, 6, 20] {
            let base = if attempt >= 20 {
                Duration::from_secs(16)
            } else {
                MUSICBRAINZ_RETRY_BASE_DELAY
                    .saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1)))
                    .min(policy.max_delay)
            };
            for seed in 0u64..64 {
                let delay = retry_delay(attempt, policy, None, seed);
                let min_delay = base.saturating_sub(base * 20 / 100);
                assert!(
                    delay <= base,
                    "attempt={attempt} seed={seed} delay={delay:?} > base={base:?}"
                );
                assert!(
                    delay >= min_delay,
                    "attempt={attempt} seed={seed} delay={delay:?} < min={min_delay:?}"
                );
                assert_eq!(delay, retry_delay(attempt, policy, None, seed));
            }
            assert_eq!(retry_delay(attempt, policy, None, 21), base);
            assert_eq!(retry_delay(attempt, policy, None, 20), base.saturating_sub(base / 5));
        }
    }

    #[tokio::test]
    async fn storm_503_fails_after_six_paced_attempts_with_bounded_retry_waits() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/recording/{}", recording_mbid())))
            .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "1"))
            .expect(6)
            .mount(&server)
            .await;

        let (_dir, cache) = temp_cache();
        let client = reqwest::Client::builder().build().expect("client");
        let shape = test_shape(&server.uri());
        let policy = MusicBrainzRetryPolicy { attempts: 6, max_delay: Duration::from_secs(1) };

        let started = Instant::now();
        let result = fetch_musicbrainz_payloads_on_with_policy(
            &client,
            &shape,
            recording_mbid(),
            None,
            Duration::from_secs(90),
            &cache,
            policy,
            None,
        )
        .await;
        let elapsed = started.elapsed();

        assert!(result.is_err(), "storm must fail after exhausting attempts: {result:?}");
        let requests = server.received_requests().await.expect("recorded requests");
        assert_eq!(requests.len(), 6, "expected 6 paced attempts, got {}", requests.len());
        assert!(
            elapsed < Duration::from_secs(30),
            "total retry wait must stay bounded by policy, elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn status_401_and_404_fail_immediately_without_retry() {
        for status in [401u16, 404] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(format!("/recording/{}", recording_mbid())))
                .respond_with(ResponseTemplate::new(status))
                .expect(1)
                .mount(&server)
                .await;

            let (_dir, cache) = temp_cache();
            let client = reqwest::Client::builder().build().expect("client");
            let shape = test_shape(&server.uri());
            let started = Instant::now();
            let result = fetch_musicbrainz_payloads_on_with_policy(
                &client,
                &shape,
                recording_mbid(),
                None,
                Duration::from_secs(30),
                &cache,
                MusicBrainzRetryPolicy::production(),
                None,
            )
            .await;
            let elapsed = started.elapsed();

            assert!(result.is_err(), "status {status} must fail: {result:?}");
            let requests = server.received_requests().await.expect("recorded requests");
            assert_eq!(requests.len(), 1, "status {status} must not retry");
            assert!(
                elapsed < Duration::from_secs(5),
                "status {status} must fail immediately without retry sleeps, elapsed={elapsed:?}"
            );
        }
    }

    #[tokio::test]
    async fn retryable_429_and_5xx_retry_until_attempts_exhausted() {
        for status in [429u16, 503] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(format!("/recording/{}", recording_mbid())))
                .respond_with(ResponseTemplate::new(status))
                .expect(3)
                .mount(&server)
                .await;

            let (_dir, cache) = temp_cache();
            let client = reqwest::Client::builder().build().expect("client");
            let shape = test_shape(&server.uri());
            let policy =
                MusicBrainzRetryPolicy { attempts: 3, max_delay: Duration::from_millis(1) };
            let result = fetch_musicbrainz_payloads_on_with_policy(
                &client,
                &shape,
                recording_mbid(),
                None,
                Duration::from_secs(30),
                &cache,
                policy,
                None,
            )
            .await;

            assert!(
                result.is_err(),
                "retryable status {status} must fail after retries: {result:?}"
            );
            let requests = server.received_requests().await.expect("recorded requests");
            assert_eq!(requests.len(), 3, "status {status} must retry");
        }
    }

    #[tokio::test]
    async fn transport_failures_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let accepts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepts_server = std::sync::Arc::clone(&accepts);
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                accepts_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut buf = [0u8; 1024];
                let _ =
                    tokio::time::timeout(Duration::from_millis(200), socket.read(&mut buf)).await;
                drop(socket);
            }
        });

        let endpoint = format!("http://127.0.0.1:{}", addr.port());
        let url = entity_url(
            &normalized_endpoint_base(&endpoint).expect("base"),
            "recording",
            recording_mbid(),
            &["artists"],
        )
        .expect("entity url");
        let client = reqwest::Client::builder().build().expect("client");
        let policy = MusicBrainzRetryPolicy { attempts: 2, max_delay: Duration::from_millis(1) };

        let result = fetch_entity::<serde_json::Value>(
            &client,
            &url,
            "recording 'transport-test'",
            policy,
            Duration::from_secs(2),
            None,
        )
        .await;

        server.abort();
        let _ = server.await;

        assert!(result.is_err(), "transport failure must fail after retries: {result:?}");
        let message = format!("{:#}", result.unwrap_err());
        assert!(
            message.contains("requesting MusicBrainz recording 'transport-test'"),
            "error should describe the failed request, got: {message}"
        );
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "transport failure must be retried (2 attempts observed)"
        );
    }

    #[test]
    fn request_url_honors_scheme_and_carries_expected_shape() {
        let mbid = recording_mbid();
        let shape = MusicBrainzFetchShape {
            write_all_tags: true,
            release_ars: false,
            endpoint: "https://musicbrainz.example.test".to_string(),

            include_user_genres: false,
            include_user_ratings: false,
            include_user_collections: false,
            include_user_tags: false,
            use_genres: false,
            only_my_genres: false,
        };
        let base = normalized_endpoint_base(&shape.endpoint).expect("base");
        let url = entity_url(&base, "recording", mbid, &recording_includes(&shape)).expect("url");

        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("musicbrainz.example.test"));
        assert_eq!(url.path(), &format!("/recording/{mbid}"));
        let pairs: Vec<(String, String)> =
            url.query_pairs().map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
        assert!(pairs.contains(&("fmt".to_string(), "json".to_string())));
        let inc = pairs
            .iter()
            .find(|(k, _)| k == "inc")
            .map(|(_, v)| v.as_str())
            .expect("inc query param");
        assert_eq!(inc, "artists releases isrcs annotation work-level-rels");

        let http_shape = MusicBrainzFetchShape {
            write_all_tags: false,
            release_ars: false,
            endpoint: "http://musicbrainz.example.test".to_string(),

            include_user_genres: false,
            include_user_ratings: false,
            include_user_collections: false,
            include_user_tags: false,
            use_genres: false,
            only_my_genres: false,
        };
        let http_base = normalized_endpoint_base(&http_shape.endpoint).expect("http base");
        let http_url = entity_url(&http_base, "recording", mbid, &recording_includes(&http_shape))
            .expect("http url");
        assert_eq!(http_url.scheme(), "http");
    }

    #[test]
    fn cache_key_remains_host_port_and_ignores_scheme() {
        let secure_shape = MusicBrainzFetchShape {
            write_all_tags: false,
            release_ars: false,
            endpoint: "https://musicbrainz.example.test:8443/base".to_string(),

            include_user_genres: false,
            include_user_ratings: false,
            include_user_collections: false,
            include_user_tags: false,
            use_genres: false,
            only_my_genres: false,
        };
        let plain_shape = MusicBrainzFetchShape {
            write_all_tags: false,
            release_ars: false,
            endpoint: "http://musicbrainz.example.test:8443/base".to_string(),

            include_user_genres: false,
            include_user_ratings: false,
            include_user_collections: false,
            include_user_tags: false,
            use_genres: false,
            only_my_genres: false,
        };
        let secure_key = secure_shape.cache_key(recording_mbid(), None).expect("https key");
        let plain_key = plain_shape.cache_key(recording_mbid(), None).expect("http key");
        assert_eq!(secure_key, plain_key, "scheme must not affect cache key");
        assert!(
            secure_key.contains("endpoint=musicbrainz.example.test:8443"),
            "cache key must keep host:port: {secure_key}"
        );
        assert!(
            !secure_key.contains("https://") && !secure_key.contains("http://"),
            "cache key must not embed scheme: {secure_key}"
        );

        assert_eq!(
            musicbrainz_host_for_endpoint("https://mb.test:1234/path").expect("host"),
            "mb.test:1234"
        );
        assert_eq!(musicbrainz_host_for_endpoint("http://mb.test/path").expect("host"), "mb.test");
    }

    #[test]
    fn pacing_proof_reflects_one_second_interval_across_fetches() {
        let interval = MUSICBRAINZ_MIN_REQUEST_INTERVAL;
        assert_eq!(interval, Duration::from_secs(1));
        let t0 = Instant::now();

        let (slot1, wait1) = next_pacing_slot(None, t0, interval);
        assert_eq!(slot1, t0);
        assert_eq!(wait1, Duration::ZERO);

        let (slot2, wait2) = next_pacing_slot(Some(slot1), t0, interval);
        assert_eq!(slot2, t0 + interval);
        assert_eq!(wait2, interval);

        let (slot3, wait3) = next_pacing_slot(Some(slot2), t0 + interval, interval);
        assert_eq!(slot3, t0 + interval + interval);
        assert_eq!(wait3, interval);

        let late_arrival = t0 + Duration::from_secs(5);
        let (slot4, wait4) = next_pacing_slot(Some(slot3), late_arrival, interval);
        assert_eq!(slot4, late_arrival);
        assert_eq!(wait4, Duration::ZERO);
    }

    #[tokio::test]
    async fn per_request_timeout_is_applied() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stalling server");
        let addr = listener.local_addr().expect("addr");
        let accept_task = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                tokio::time::sleep(Duration::from_secs(5)).await;
                let _ = socket.shutdown().await;
                let _ = buf;
            }
        });

        let endpoint = format!("http://127.0.0.1:{}", addr.port());
        let url = entity_url(
            &normalized_endpoint_base(&endpoint).expect("base"),
            "recording",
            recording_mbid(),
            &["artists"],
        )
        .expect("entity url");
        let client = reqwest::Client::builder().build().expect("client");
        let policy = MusicBrainzRetryPolicy { attempts: 1, max_delay: Duration::from_millis(1) };

        let started = Instant::now();
        let result = fetch_entity::<serde_json::Value>(
            &client,
            &url,
            "recording 'timeout-test'",
            policy,
            Duration::from_millis(100),
            None,
        )
        .await;
        let elapsed = started.elapsed();

        accept_task.abort();
        assert!(result.is_err(), "stalling server must hit request timeout: {result:?}");
        assert!(
            elapsed < Duration::from_secs(2),
            "request timeout must be applied (short), elapsed={elapsed:?}"
        );
    }

    #[test]
    fn parse_musicbrainz_timeout_clamps_and_defaults() {
        assert_eq!(parse_musicbrainz_timeout(None), Duration::from_secs(30));
        assert_eq!(parse_musicbrainz_timeout(Some("")), Duration::from_secs(30));
        assert_eq!(parse_musicbrainz_timeout(Some("  ")), Duration::from_secs(30));
        assert_eq!(parse_musicbrainz_timeout(Some("not-a-number")), Duration::from_secs(30));
        assert_eq!(parse_musicbrainz_timeout(Some("5")), Duration::from_secs(30));
        assert_eq!(parse_musicbrainz_timeout(Some("30")), Duration::from_secs(30));
        assert_eq!(parse_musicbrainz_timeout(Some("60")), Duration::from_secs(60));
    }

    #[test]
    fn recording_includes_user_data_incs_when_requested() {
        let shape = MusicBrainzFetchShape {
            write_all_tags: false,
            release_ars: false,
            endpoint: "https://musicbrainz.org/ws/2".to_string(),
            include_user_genres: true,
            include_user_ratings: true,
            include_user_collections: true,
            include_user_tags: true,
            use_genres: false,
            only_my_genres: false,
        };
        let incs = recording_includes(&shape);
        assert!(incs.contains(&"user-genres"));
        assert!(incs.contains(&"user-ratings"));
        assert!(incs.contains(&"user-tags"));
        let release_incs = release_includes(&shape);
        assert!(release_incs.contains(&"user-collections"));
        assert!(release_incs.contains(&"user-genres"));
        assert!(release_incs.contains(&"user-ratings"));
        assert!(release_incs.contains(&"user-tags"));
    }

    #[test]
    fn extract_user_data_reads_user_fields() {
        let value = serde_json::json!({
            "user-rating": {"value": 5},
            "user-genres": [{"name": "rock", "count": 2}],
            "user-tags": [{"name": "favorite", "count": 1}],
            "user-collections": [{"id": "coll-1"}],
            "collections": [{"id": "public-coll"}]
        });
        let data = extract_user_data(&value);
        assert_eq!(data.user_rating, Some(5));
        assert_eq!(data.user_genres, vec![("rock".to_string(), 2)]);
        assert_eq!(data.user_tags, vec![("favorite".to_string(), 1)]);
        assert_eq!(data.user_collections, vec!["coll-1".to_string()]);
    }

    #[test]
    fn build_ffmetadata_map_includes_user_data_output() {
        let payload = MusicBrainzPayload {
            recording: serde_json::from_value(serde_json::json!({
                "id": "11111111-1111-1111-1111-111111111111",
                "title": "Song"
            }))
            .expect("recording"),
            release: None,
            recording_user_data: MusicBrainzUserData {
                user_genres: vec![("rock".to_string(), 1)],
                user_tags: vec![("favorite".to_string(), 1)],
                user_rating: Some(4),
                user_collections: vec![],
            },
            release_user_data: MusicBrainzUserData {
                user_genres: vec![],
                user_tags: vec![],
                user_rating: None,
                user_collections: vec!["coll-1".to_string()],
            },
        };
        let tags = build_ffmetadata_map(
            &payload,
            false,
            &GenreConfig {
                use_genres: true,
                only_my_genres: false,
                max_genres: 5,
                min_genre_usage: 90,
                genres_filter: "-seen live\n-favorites\n-fixme\n-owned".to_string(),
                join_genres: String::new(),
            },
        )
        .expect("tags");
        let value = |key: &str| {
            tags.iter().find(|(existing, _)| existing == key).map(|(_, value)| value.as_str())
        };
        let values = |key: &str| {
            tags.iter()
                .filter(|(existing, _)| existing == key)
                .map(|(_, value)| value.as_str())
                .collect::<Vec<_>>()
        };
        assert_eq!(value("~rating"), Some("4"));
        assert_eq!(value("musicbrainz_collectionid"), Some("coll-1"));
        assert_eq!(values("genre"), vec!["Rock"]);
        assert!(value("~usertags").is_some_and(|t| t.contains("favorite")));
    }

    fn genre_payload(
        recording_genres: &serde_json::Value,
        recording_user_genres: Vec<(String, u32)>,
        recording_user_tags: Vec<(String, u32)>,
        release_user_genres: Vec<(String, u32)>,
        release_user_tags: Vec<(String, u32)>,
    ) -> MusicBrainzPayload {
        MusicBrainzPayload {
            recording: serde_json::from_value(serde_json::json!({
                "id": "11111111-1111-1111-1111-111111111111",
                "title": "Song",
                "genres": recording_genres.clone()
            }))
            .expect("recording"),
            release: None,
            recording_user_data: MusicBrainzUserData {
                user_genres: recording_user_genres,
                user_tags: recording_user_tags,
                user_rating: None,
                user_collections: vec![],
            },
            release_user_data: MusicBrainzUserData {
                user_genres: release_user_genres,
                user_tags: release_user_tags,
                user_rating: None,
                user_collections: vec![],
            },
        }
    }

    fn genre_values(tags: &[(String, String)]) -> Vec<&str> {
        tags.iter().filter(|(key, _)| key == "genre").map(|(_, value)| value.as_str()).collect()
    }

    #[test]
    fn build_ffmetadata_map_omits_genre_when_use_genres_is_false() {
        let payload = genre_payload(
            &serde_json::json!([{ "name": "rock", "count": 10 }]),
            vec![("electronic".to_string(), 2)],
            vec![],
            vec![],
            vec![],
        );
        let tags = build_ffmetadata_map(
            &payload,
            false,
            &GenreConfig {
                use_genres: false,
                only_my_genres: false,
                max_genres: 5,
                min_genre_usage: 90,
                genres_filter: "-seen live\n-favorites\n-fixme\n-owned".to_string(),
                join_genres: String::new(),
            },
        )
        .expect("tags");
        assert_eq!(genre_values(&tags), Vec::<&str>::new());
    }

    #[test]
    fn build_ffmetadata_map_emits_joined_genres_when_join_separator_is_set() {
        let payload = genre_payload(
            &serde_json::json!([
                { "name": "rock", "count": 10 },
                { "name": "electronic", "count": 10 }
            ]),
            vec![],
            vec![],
            vec![],
            vec![],
        );
        let tags = build_ffmetadata_map(
            &payload,
            false,
            &GenreConfig {
                use_genres: true,
                only_my_genres: false,
                max_genres: 5,
                min_genre_usage: 90,
                genres_filter: String::new(),
                join_genres: "; ".to_string(),
            },
        )
        .expect("tags");
        assert_eq!(genre_values(&tags), vec!["Electronic; Rock"]);
    }

    #[test]
    fn build_ffmetadata_map_emits_multi_value_genres_when_join_is_empty() {
        let payload = genre_payload(
            &serde_json::json!([
                { "name": "rock", "count": 10 },
                { "name": "electronic", "count": 10 }
            ]),
            vec![],
            vec![],
            vec![],
            vec![],
        );
        let tags = build_ffmetadata_map(
            &payload,
            false,
            &GenreConfig {
                use_genres: true,
                only_my_genres: false,
                max_genres: 5,
                min_genre_usage: 90,
                genres_filter: String::new(),
                join_genres: String::new(),
            },
        )
        .expect("tags");
        assert_eq!(genre_values(&tags), vec!["Electronic", "Rock"]);
    }

    #[test]
    fn build_ffmetadata_map_applies_genre_filter_min_usage_and_max() {
        let payload = genre_payload(
            &serde_json::json!([
                { "name": "rock", "count": 100 },
                { "name": "pop", "count": 95 },
                { "name": "jazz", "count": 80 },
                { "name": "seen live", "count": 100 }
            ]),
            vec![],
            vec![],
            vec![],
            vec![],
        );
        let tags = build_ffmetadata_map(
            &payload,
            false,
            &GenreConfig {
                use_genres: true,
                only_my_genres: false,
                max_genres: 1,
                min_genre_usage: 90,
                genres_filter: "-seen live".to_string(),
                join_genres: String::new(),
            },
        )
        .expect("tags");
        assert_eq!(genre_values(&tags), vec!["Rock"]);
    }

    #[test]
    fn genre_filter_escapes_literal_metacharacters_and_keeps_glob_wildcards() {
        let (literal, _) = parse_genre_filter("+a(b)[c]\\d{2}^$\n+rock+roll\n+pop.lit");
        let literal_match = |name: &str| genre_name_matches(name, &literal, &[]);
        assert!(literal_match("a(b)[c]\\d{2}^$"));
        assert!(!literal_match("abc11"));
        assert!(literal_match("Rock+Roll"), "a literal '+' must stay literal");
        assert!(!literal_match("rockXroll"));
        assert!(literal_match("pop.lit"), "a literal '.' must stay literal");
        assert!(!literal_match("popXlit"));

        let (wildcards, _) = parse_genre_filter("+b?ues\n+rock*");
        let wildcard_match = |name: &str| genre_name_matches(name, &wildcards, &[]);
        assert!(wildcard_match("blues"), "'?' must match one character");
        assert!(!wildcard_match("bues"));
        assert!(wildcard_match("rockabilly"), "'*' must match any suffix");
    }

    #[test]
    fn build_ffmetadata_map_omits_genres_when_max_genres_is_zero() {
        let payload = genre_payload(
            &serde_json::json!([{ "name": "rock", "count": 10 }]),
            vec![],
            vec![],
            vec![],
            vec![],
        );
        let tags = build_ffmetadata_map(
            &payload,
            false,
            &GenreConfig {
                use_genres: true,
                only_my_genres: false,
                max_genres: 0,
                min_genre_usage: 90,
                genres_filter: String::new(),
                join_genres: String::new(),
            },
        )
        .expect("tags");
        assert_eq!(genre_values(&tags), Vec::<&str>::new());
    }

    #[test]
    fn build_ffmetadata_map_deduplicates_genre_case_before_limit() {
        let payload = genre_payload(
            &serde_json::json!([
                { "name": "rock", "count": 100 },
                { "name": "Rock", "count": 90 },
                { "name": "jazz", "count": 60 }
            ]),
            vec![],
            vec![],
            vec![],
            vec![],
        );
        let tags = build_ffmetadata_map(
            &payload,
            false,
            &GenreConfig {
                use_genres: true,
                only_my_genres: false,
                max_genres: 2,
                min_genre_usage: 30,
                genres_filter: String::new(),
                join_genres: String::new(),
            },
        )
        .expect("tags");
        assert_eq!(
            genre_values(&tags),
            vec!["Jazz", "Rock"],
            "case variants must share one slot before the limit is applied",
        );
    }

    #[test]
    fn build_ffmetadata_map_only_my_genres_uses_user_genres_and_tags_with_count_one() {
        let payload = genre_payload(
            &serde_json::json!([{ "name": "public", "count": 100 }]),
            vec![("my-genre".to_string(), 7)],
            vec![("my-tag".to_string(), 3)],
            vec![("release-genre".to_string(), 4)],
            vec![("release-tag".to_string(), 2)],
        );
        let tags = build_ffmetadata_map(
            &payload,
            false,
            &GenreConfig {
                use_genres: true,
                only_my_genres: true,
                max_genres: 5,
                min_genre_usage: 90,
                genres_filter: String::new(),
                join_genres: String::new(),
            },
        )
        .expect("tags");
        assert_eq!(genre_values(&tags), vec!["My-Genre", "My-Tag", "Release-Genre", "Release-Tag"]);
    }
}
