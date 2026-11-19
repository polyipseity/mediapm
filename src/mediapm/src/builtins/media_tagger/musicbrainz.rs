//! MusicBrainz payload fetching and FFmetadata mapping helpers.

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

/// Recording + optional release payload fetched from MusicBrainz.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MusicBrainzPayload {
    /// Recording entity resolved from MusicBrainz API.
    pub(super) recording: Recording,
    /// Optional release entity resolved from MusicBrainz API.
    pub(super) release: Option<Release>,
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
pub(super) struct MusicBrainzFetchShape {
    /// Whether the extended Picard tag block was requested.
    pub(super) write_all_tags: bool,
    /// Whether recording/release-group level relations were requested.
    pub(super) release_ars: bool,
    /// MusicBrainz endpoint URL.
    pub(super) endpoint: String,
}

impl MusicBrainzFetchShape {
    /// The cache key for one recording/release pair under this fetch shape.
    ///
    /// The scheme is deliberately absent: `http://x` and `https://x`
    /// produce equivalent cached rows. The deadline and API credentials are
    /// absent too — one is latency policy, the other must never land in the
    /// on-disk index.
    pub(super) fn cache_key(
        &self,
        recording_mbid: &str,
        release_mbid: Option<&str>,
    ) -> anyhow::Result<String> {
        let release_mbid =
            release_mbid.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("");
        let domain = musicbrainz_host_for_endpoint(&self.endpoint)?;
        Ok(format!(
            "recording={recording_mbid}|release={release_mbid}|all_tags={}|ars={}|endpoint={domain}",
            u8::from(self.write_all_tags),
            u8::from(self.release_ars),
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
            .map_err(|_| anyhow::anyhow!("MusicBrainz endpoint '{endpoint}' cannot be a base URL"))?
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
    let mut includes = vec!["artists", "releases", "isrcs", "genres", "tags"];
    if shape.write_all_tags {
        includes.extend(["annotation", "work-level-rels"]);
    }
    includes
}

/// Release `inc` tokens for the requested fetch shape.
fn release_includes(shape: &MusicBrainzFetchShape) -> Vec<&'static str> {
    let mut includes = vec![
        "artists",
        "recordings",
        "release-groups",
        "media",
        "labels",
        "isrcs",
        "genres",
        "tags",
    ];
    if shape.write_all_tags {
        includes.push("annotation");
    }
    if shape.write_all_tags || shape.release_ars {
        includes.push("recording-level-rels");
        includes.push("release-group-level-rels");
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
) -> anyhow::Result<T> {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=policy.attempts {
        pace_musicbrainz_request().await;
        let send_result = client.get(url.clone()).timeout(request_timeout).send().await;
        match send_result {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    let bytes = response.bytes().await.with_context(|| {
                        format!("reading MusicBrainz {entity_label} response body from '{url}'")
                    })?;
                    return serde_json::from_slice::<T>(&bytes).with_context(|| {
                        format!("deserializing MusicBrainz {entity_label} response from '{url}'")
                    });
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
                    let delay = retry_delay(attempt, policy, retry_after, attempt as u64);
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
                let delay = retry_delay(attempt, policy, None, attempt as u64);
                tokio::time::sleep(delay).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!("MusicBrainz {entity_label} request to '{url}' failed without attempts")
    }))
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
) -> anyhow::Result<MusicBrainzPayload> {
    let base = normalized_endpoint_base(&shape.endpoint)?;
    let request_timeout = musicbrainz_timeout();

    let recording_url = entity_url(&base, "recording", recording_mbid, &recording_includes(shape))?;
    let recording: Recording = fetch_entity(
        client,
        &recording_url,
        &format!("recording '{recording_mbid}'"),
        policy,
        request_timeout,
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

    let release = if let Some(release_id) = release_id {
        let release_url = entity_url(&base, "release", &release_id, &release_includes(shape))?;
        let release: Release = fetch_entity(
            client,
            &release_url,
            &format!("release '{release_id}'"),
            policy,
            request_timeout,
        )
        .await
        .with_context(|| format!("fetching MusicBrainz release '{release_id}'"))?;
        Some(release)
    } else {
        None
    };

    Ok(MusicBrainzPayload { recording, release })
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
) -> anyhow::Result<MusicBrainzPayload> {
    let client = shared_http_client()
        .map_err(|error| anyhow::anyhow!("{error}"))
        .context("acquiring shared HTTP client for MusicBrainz fetch")?;
    fetch_musicbrainz_payloads_on(client, shape, recording_mbid, release_mbid, deadline, cache)
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
) -> anyhow::Result<MusicBrainzPayload> {
    fetch_musicbrainz_payloads_on_with_policy(
        client,
        shape,
        recording_mbid,
        release_mbid,
        deadline,
        cache,
        MusicBrainzRetryPolicy::production(),
    )
    .await
}

/// The fetch itself, against an explicit client and retry policy.
///
/// Tests point it at a local server and a tiny policy without touching
/// production construction. The shape carries both the fetch inputs and the
/// cache key, so a payload is only ever read from or written to the row of the
/// exact shape that produced it.
pub(super) async fn fetch_musicbrainz_payloads_on_with_policy(
    client: &Client,
    shape: &MusicBrainzFetchShape,
    recording_mbid: &str,
    release_mbid: Option<&str>,
    deadline: Duration,
    cache: &MediaTaggerHttpCache,
    policy: MusicBrainzRetryPolicy,
) -> anyhow::Result<MusicBrainzPayload> {
    let cache_key = shape.cache_key(recording_mbid, release_mbid)?;
    let cached_payload = cache.read_musicbrainz_payload(&cache_key).await;
    if let Some(cached) = cached_payload.as_ref()
        && cached.is_fresh
    {
        return Ok(cached.payload.clone());
    }

    let fetched_payload = tokio::time::timeout(deadline, async {
        fetch_pair_with_policy(client, shape, recording_mbid, release_mbid, policy).await
    })
    .await
    .with_context(|| {
        format!(
            "fetching MusicBrainz entities from '{}' for recording \
             '{recording_mbid}' within {deadline:?}",
            shape.endpoint
        )
    })??;

    let _ = cache.write_musicbrainz_payload(&cache_key, &fetched_payload).await;
    Ok(fetched_payload)
}

/// Converts MusicBrainz payload into FFmetadata key/value map.
pub(super) fn build_ffmetadata_map(
    payload: &MusicBrainzPayload,
    write_all_tags: bool,
) -> anyhow::Result<BTreeMap<String, String>> {
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

    if let Some(genres) = payload.recording.genres.as_ref() {
        let genre_values = genres.iter().map(|genre| genre.name.as_str()).collect::<Vec<_>>();
        if !genre_values.is_empty() {
            tags.insert("genre".to_string(), join_unique(genre_values));
        }
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

    Ok(tags)
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
