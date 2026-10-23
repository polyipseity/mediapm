//! MusicBrainz payload fetching and FFmetadata mapping helpers.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::Context;
use musicbrainz_rs::client::MusicBrainzClient;
use musicbrainz_rs::entity::recording::Recording;
use musicbrainz_rs::entity::release::Release;
use musicbrainz_rs::prelude::*;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

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

/// How many times a single MusicBrainz request may be retried before the
/// fetch fails for good.
///
/// `musicbrainz_rs`'s own default is ten retries with a `2n²`-second backoff
/// — minutes of sleeping inside one tagging step. Two retries keep a blip
/// recoverable and a 503 storm terminal.
const MUSICBRAINZ_MAX_RETRIES: u32 = 2;

/// How many times one recording+release pair is attempted.
///
/// A dropped upstream connection arrives as a `502` *response*, which
/// `api_bindium`'s retry loop never sees: it retries a `503` status and
/// transport errors, and hands every other status to the caller. So the retry
/// that makes such a drop survivable has to sit here. The fetch deadline still
/// cuts the whole sequence.
pub const MUSICBRAINZ_FETCH_ATTEMPTS: u32 = 3;

/// The base delay between two attempts of the pair fetch.
///
/// Grows linearly with the attempt number, so three attempts wait 250 ms and
/// then 500 ms: enough to let a dropped connection recover, far short of the
/// per-request timeout a stalled server would otherwise cost.
const MUSICBRAINZ_FETCH_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

/// The minimum interval between two `MusicBrainz` requests.
///
/// The web service rate limits by interval rather than by burst, and its
/// limiter answers `503` when callers ignore that. `api_bindium` carries a
/// `governor` limiter for exactly this interval, but it is compiled out behind
/// a feature `musicbrainz_rs` does not expose, so the spacing is enforced on
/// this side. Spacing requests is cheaper than retrying them.
const MUSICBRAINZ_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1);

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
/// fetch deadline, because the interval is a fraction of a second while the
/// deadline is the whole pair's budget; the retry loop's own backoff is bounded
/// the same way.
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

/// The user agent every request in this path carries.
///
/// MusicBrainz asks callers to identify themselves so it can reach a
/// misbehaving client; `musicbrainz_rs`'s default names the library rather
/// than the application making the call.
fn musicbrainz_user_agent() -> String {
    format!("mediapm/{} (+https://github.com/mediapm/mediapm)", env!("CARGO_PKG_VERSION"))
}

/// The outer wall-clock budget for one recording+release fetch pair.
///
/// The per-request timeout bounds a single HTTP call, but `api_bindium`'s
/// 503 handling sleeps until the server's `Retry-After` instant with no
/// cap — a server-controlled wait no request-level timeout reaches.
/// Wrapping the whole fetch in this deadline cuts that sleep without
/// patching the dependency, and 90 seconds sits above the worst legitimate
/// two-attempt-per-entity time so a slow-but-working fetch still completes.
pub(super) const MUSICBRAINZ_FETCH_DEADLINE: Duration = Duration::from_secs(90);

/// Build an agent that identifies this application and aborts a stalled
/// request at `timeout`.
///
/// The timeout is end-to-end — DNS through reading the response body — so a
/// connection that accepts bytes and then goes quiet cannot hold a tagging
/// step past its budget.
fn bounded_agent(timeout: Duration) -> musicbrainz_rs::api_bindium::ureq::Agent {
    let config = musicbrainz_rs::api_bindium::ureq::config::Config::builder()
        .user_agent(musicbrainz_user_agent())
        .timeout_global(Some(timeout))
        .build();
    musicbrainz_rs::api_bindium::ureq::Agent::new_with_config(config)
}

/// The host\[:port\] one configured endpoint URL resolves to.
///
/// `musicbrainz_rs` appends `/ws/2` itself and always speaks plain HTTP, so
/// only the host (and port, when the endpoint names one) is taken from the
/// configured URL. Both the client builder and the cache-key builder call
/// this, so the row a fetch writes is the row the next fetch with the same
/// shape reads — they cannot parse the endpoint differently.
///
/// An endpoint that does not parse, or that names no host, fails here — a
/// tagging step pointed at a broken endpoint must surface that broken
/// configuration instead of quietly fetching from somewhere else.
fn musicbrainz_host_for_endpoint(endpoint: &str) -> anyhow::Result<String> {
    let url = url::Url::parse(endpoint)
        .with_context(|| format!("parsing MusicBrainz endpoint '{endpoint}'"))?;
    let host =
        url.host_str().with_context(|| format!("MusicBrainz endpoint '{endpoint}' has no host"))?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    })
}

/// The bounded MusicBrainz client for one configured endpoint URL.
///
/// The default `https://musicbrainz.org/ws/2` therefore yields the same
/// `musicbrainz.org` traffic as the crate's stock domain. The per-request
/// timeout, retry cap, and user agent are set exactly as the module's other
/// client construction: this is the fetch path's single client builder.
fn musicbrainz_client_for_endpoint(endpoint: &str) -> anyhow::Result<MusicBrainzClient> {
    let domain = musicbrainz_host_for_endpoint(endpoint)?;

    Ok(MusicBrainzClient::builder()
        .musicbrainz_domain(domain)
        .api_client(
            musicbrainz_rs::api_bindium::ApiClient::builder()
                .agent(bounded_agent(musicbrainz_timeout()))
                .max_retries(MUSICBRAINZ_MAX_RETRIES)
                .build(),
        )
        .build())
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
    /// MusicBrainz endpoint URL whose host\[:port\] selects the fetch domain.
    pub(super) endpoint: String,
}

impl MusicBrainzFetchShape {
    /// The cache key for one recording/release pair under this fetch shape.
    ///
    /// The scheme is deliberately absent: `musicbrainz_rs` speaks plain HTTP
    /// whatever the configured scheme says, so `http://x` and `https://x`
    /// produce identical traffic and share one row. The deadline and API
    /// credentials are absent too — one is latency policy, the other must
    /// never land in the on-disk index.
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

/// Fetches one recording and its release, with no retry of its own.
///
/// The caller owns the retry loop and the deadline, so this stays a single
/// attempt whose error says which entity failed. Each request waits for its own
/// pacing slot, so the interval spaces requests rather than pairs: a pair is two
/// requests, and pacing the pair would let both leave inside one slot and double
/// the rate the server publishes.
async fn fetch_pair_once(
    client: &MusicBrainzClient,
    shape: &MusicBrainzFetchShape,
    recording_mbid: &str,
    release_mbid: Option<&str>,
) -> anyhow::Result<MusicBrainzPayload> {
    let mut recording_fetch_base = Recording::fetch();
    let mut recording_fetch = recording_fetch_base
        .id(recording_mbid)
        .with_artists()
        .with_releases()
        .with_isrcs()
        .with_genres()
        .with_tags();

    if shape.write_all_tags {
        recording_fetch = recording_fetch.with_annotations().with_work_level_relations();
    }

    pace_musicbrainz_request().await;
    let recording = recording_fetch
        .execute_with_client_async(client)
        .await
        .with_context(|| format!("fetching MusicBrainz recording '{recording_mbid}'"))?;

    let release_id = release_mbid.map(ToOwned::to_owned).or_else(|| {
        recording
            .releases
            .as_ref()
            .and_then(|releases| releases.first())
            .map(|release| release.id.clone())
    });

    let release = if let Some(release_id) = release_id {
        let mut release_fetch_base = Release::fetch();
        let mut release_fetch = release_fetch_base
            .id(&release_id)
            .with_artists()
            .with_recordings()
            .with_release_groups()
            .with_media()
            .with_labels()
            .with_isrcs()
            .with_genres()
            .with_tags();

        if shape.write_all_tags {
            release_fetch = release_fetch.with_annotations();
        }
        if shape.write_all_tags || shape.release_ars {
            release_fetch =
                release_fetch.with_recording_level_relations().with_release_group_level_relations();
        }

        Some({
            pace_musicbrainz_request().await;
            release_fetch
                .execute_with_client_async(client)
                .await
                .with_context(|| format!("fetching MusicBrainz release '{release_id}'"))?
        })
    } else {
        None
    };

    Ok(MusicBrainzPayload { recording, release })
}

/// Fetches recording/release payloads through `musicbrainz_rs`, on a client
/// built from `shape.endpoint` rather than the crate's unbounded global.
///
/// The whole recording+release fetch is bounded by `deadline`, which caps the
/// server-controlled `Retry-After` sleep the per-request timeout cannot reach,
/// and a pair that fails is attempted again up to `MUSICBRAINZ_FETCH_ATTEMPTS`
/// times, because the most common failure on this path — a dropped connection —
/// reaches the caller as a response that the dependency's own retry loop never
/// examines. A fetch failure propagates once the attempts are spent: the caller
/// decides what a failure means for the tagging step, and no stale cache row is
/// served in its place.
pub(super) async fn fetch_musicbrainz_payloads(
    shape: &MusicBrainzFetchShape,
    recording_mbid: &str,
    release_mbid: Option<&str>,
    deadline: Duration,
    cache: &MediaTaggerHttpCache,
) -> anyhow::Result<MusicBrainzPayload> {
    let client = musicbrainz_client_for_endpoint(&shape.endpoint)?;
    fetch_musicbrainz_payloads_on(&client, shape, recording_mbid, release_mbid, deadline, cache)
        .await
}

/// The fetch itself, against an explicit client: tests point it at a local
/// wiremock server or a broken endpoint without touching production
/// construction. The shape carries both the fetch inputs and the cache key,
/// so a payload is only ever read from or written to the row of the exact
/// shape that produced it. The endpoint string also rides the deadline
/// error so a stalled fetch names where it stalled.
///
/// Every request waits for its own pacing slot and the whole pair runs inside
/// the deadline, so the deadline is the total budget for one pair including the
/// pacing waits and the retry backoff between attempts.
async fn fetch_musicbrainz_payloads_on(
    client: &MusicBrainzClient,
    shape: &MusicBrainzFetchShape,
    recording_mbid: &str,
    release_mbid: Option<&str>,
    deadline: Duration,
    cache: &MediaTaggerHttpCache,
) -> anyhow::Result<MusicBrainzPayload> {
    let cache_key = shape.cache_key(recording_mbid, release_mbid)?;
    let cached_payload = cache.read_musicbrainz_payload(&cache_key).await;
    if let Some(cached) = cached_payload.as_ref()
        && cached.is_fresh
    {
        return Ok(cached.payload.clone());
    }

    let fetched_payload = tokio::time::timeout(deadline, async {
        let mut attempt = 0;
        loop {
            match fetch_pair_once(client, shape, recording_mbid, release_mbid).await {
                Ok(payload) => break Ok(payload),
                Err(error) => {
                    attempt += 1;
                    if attempt >= MUSICBRAINZ_FETCH_ATTEMPTS {
                        break Err(error);
                    }
                    tracing::warn!(attempt, "retrying a failed MusicBrainz fetch: {error:#}");
                    tokio::time::sleep(MUSICBRAINZ_FETCH_RETRY_BASE_DELAY * attempt).await;
                }
            }
        }
    })
    .await
    .with_context(|| {
        format!(
            "fetching MusicBrainz entities from '{}' for recording \
             '{recording_mbid}' within {deadline:?}",
            shape.endpoint
        )
    })?;
    let fetched_payload = fetched_payload?;

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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    /// The request timeout parses from the documented env var with the
    /// conductor's 30-second floor and this path's tighter default of 30,
    /// so a stalled MusicBrainz response gives the tagging step's budget
    /// back instead of holding it for half an hour.
    #[test]
    fn request_timeout_floors_at_thirty_seconds() {
        assert_eq!(parse_musicbrainz_timeout(None), Duration::from_secs(30));
        assert_eq!(parse_musicbrainz_timeout(Some("10")), Duration::from_secs(30));
        assert_eq!(parse_musicbrainz_timeout(Some("not-a-number")), Duration::from_secs(30));
        assert_eq!(parse_musicbrainz_timeout(Some("120")), Duration::from_mins(2));
    }

    /// The pacer spaces requests by the interval and never waits for a slot
    /// that has already passed.
    ///
    /// The rule is pure, so it is pinned without a clock: the first request
    /// waits for nothing, the next waits out the previous request's slot, and a
    /// caller arriving after its slot has passed waits for nothing rather than
    /// being pushed further into the future.
    #[test]
    fn the_pacer_spaces_requests_by_the_interval() {
        let interval = Duration::from_secs(1);
        let start = Instant::now();
        let (first_slot, first_wait) = next_pacing_slot(None, start, interval);
        assert_eq!(first_wait, Duration::ZERO);
        let (second_slot, second_wait) = next_pacing_slot(Some(first_slot), start, interval);
        assert_eq!(second_wait, interval);
        let (_, third_wait) = next_pacing_slot(Some(second_slot), start + interval * 3, interval);
        assert_eq!(third_wait, Duration::ZERO);
    }

    /// The client the fetch path uses caps its retries and carries a request
    /// timeout: the two settings whose absence let a 503 storm hold a tagging
    /// step for minutes.
    #[test]
    fn musicbrainz_client_is_bounded() {
        let client = musicbrainz_client_for_endpoint("https://musicbrainz.org/ws/2")
            .expect("default endpoint");
        assert_eq!(
            client.api_client.max_retries, MUSICBRAINZ_MAX_RETRIES,
            "a MusicBrainz request may not be retried more than the seam allows"
        );
        let global = client.api_client.agent.config().timeouts().global;
        assert!(
            global.is_some_and(|t| t >= Duration::from_secs(30)),
            "every MusicBrainz request must carry an end-to-end timeout of at least the floor, got {global:?}"
        );
    }

    /// The agent the seam builds aborts a slow response at its own timeout,
    /// proved end to end against a local server that would otherwise hang
    /// the request. The production client builds its agent through this same
    /// constructor with the env-derived timeout.
    #[tokio::test]
    async fn the_bounded_agent_aborts_a_slow_response_at_its_timeout() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let agent = bounded_agent(Duration::from_millis(250));
        let started = Instant::now();
        let result = agent.get(&format!("http://{}/probe", server.address())).call();
        let elapsed = started.elapsed();
        assert!(
            result.is_err(),
            "a 250 ms-bounded agent must not wait out a 5 s response: {result:?}"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "the abort must land at the agent's timeout, not the response's: {elapsed:?}"
        );
    }

    /// A 503 storm stops at the retry cap instead of grinding.
    ///
    /// This is the behavior the task exists for: with the crate's default cap
    /// of 10 the same storm costs ten attempts and up to ~570 seconds of
    /// `2n²` backoff inside one tagging step. `api_bindium`'s `send_async`
    /// loops `while self.tries < client.max_retries`, so the attempts a storm
    /// costs equal the cap — here `MUSICBRAINZ_MAX_RETRIES` requests, then
    /// `MaxRetriesExceeded`.
    #[tokio::test]
    async fn a_503_storm_stops_at_the_retry_cap() {
        use std::sync::Arc;

        use musicbrainz_rs::api_bindium::HTTPVerb;
        use musicbrainz_rs::api_bindium::api_request::ApiRequest;
        use musicbrainz_rs::api_bindium::parsers::text::TextParser;

        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let client = musicbrainz_client_for_endpoint("https://musicbrainz.org/ws/2")
            .expect("default endpoint");
        let mut request: ApiRequest<TextParser> = ApiRequest::builder()
            .uri(
                format!("http://{}/ws/2/recording/test", server.address())
                    .parse::<musicbrainz_rs::api_bindium::ureq::http::Uri>()
                    .expect("locally built wiremock URI parses"),
            )
            .verb(HTTPVerb::Get)
            .parser(Arc::new(TextParser))
            .build();
        let result = request.send_async(&client.api_client).await;
        assert!(result.is_err(), "a capped 503 storm must fail, not hang");

        let received = server.received_requests().await.expect("request recorder is enabled");
        assert_eq!(
            received.len(),
            MUSICBRAINZ_MAX_RETRIES as usize,
            "the storm may cost exactly the configured cap of attempts, not the crate's 10"
        );
    }

    /// A failed fetch surfaces its error even when a stale cache row exists.
    ///
    /// This is the anti-silent-skip contract: the stale row used to be served
    /// in place of the failure, so a tagging step could quietly write outdated
    /// metadata while MusicBrainz was down. The endpoint points at a closed
    /// local port, so the failure is instant and hermetic.
    #[tokio::test]
    async fn a_failed_fetch_with_only_stale_cache_is_an_error() {
        use musicbrainz_rs::api_bindium::ApiClient;
        use musicbrainz_rs::entity::recording::Recording;

        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = super::super::cover_art::MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            super::super::cover_art::CacheExpiryPolicy::from_seconds(1),
        );
        let recording: Recording = serde_json::from_str(
            r#"{"id":"8f3471b5-7e6a-48da-86a9-c1c07a0f47ae","title":"cached"}"#,
        )
        .expect("minimal recording payload");
        let payload = super::MusicBrainzPayload { recording, release: None };
        let shape = super::MusicBrainzFetchShape {
            write_all_tags: false,
            release_ars: false,
            endpoint: "http://127.0.0.1:1/ws/2".to_string(),
        };
        cache
            .write_musicbrainz_payload(
                &shape.cache_key("8f3471b5-7e6a-48da-86a9-c1c07a0f47ae", None).expect("shape key"),
                &payload,
            )
            .await
            .expect("cache write");

        // Let the row age past its one-second budget — measured in whole
        // unix seconds, so this needs more than two seconds of wall clock —
        // before the fetch is attempted.
        tokio::time::sleep(Duration::from_millis(2500)).await;

        // A closed local port: the connection fails instantly, and the
        // client's cap of 1 admits a single try per attempt, so the pair fetch
        // fails after its own attempts rather than on a server-controlled wait.
        let broken = MusicBrainzClient::builder()
            .musicbrainz_domain("127.0.0.1:1".to_string())
            .api_client(
                ApiClient::builder()
                    .agent(bounded_agent(Duration::from_secs(30)))
                    .max_retries(1)
                    .build(),
            )
            .build();

        let fetched = fetch_musicbrainz_payloads_on(
            &broken,
            &shape,
            "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae",
            None,
            Duration::from_secs(30),
            &cache,
        )
        .await;
        let error = fetched.expect_err("a stale row must not stand in for a fetch failure");
        assert!(
            error.to_string().contains("fetching MusicBrainz recording"),
            "the error must be the fetch failure itself, got: {error:#}"
        );
    }

    /// The outer deadline cuts the server-controlled `Retry-After` sleep.
    ///
    /// `api_bindium` sleeps until the server's `Retry-After` instant with no
    /// cap, so a 503 that asks for a one-hour retry would otherwise hold the
    /// tagging step for an hour past the per-request timeout. The sleep is an
    /// await point, so the fetch's own deadline preempts it.
    #[tokio::test]
    async fn a_retry_after_sleep_is_cut_by_the_fetch_deadline() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(503).insert_header("Retry-After", "3600"))
            .mount(&server)
            .await;

        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = super::super::cover_art::MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            super::super::cover_art::CacheExpiryPolicy::from_seconds(60),
        );

        let endpoint = format!("http://{}/ws/2", server.address());
        let client = musicbrainz_client_for_endpoint(&endpoint).expect("local wiremock endpoint");
        let shape = super::MusicBrainzFetchShape {
            write_all_tags: false,
            release_ars: false,
            endpoint: endpoint.clone(),
        };
        let started = Instant::now();
        let fetched = fetch_musicbrainz_payloads_on(
            &client,
            &shape,
            "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae",
            None,
            Duration::from_secs(2),
            &cache,
        )
        .await;
        let elapsed = started.elapsed();

        assert!(fetched.is_err(), "a Retry-After storm must fail, not sleep for an hour");
        assert!(
            elapsed < Duration::from_secs(10),
            "the fetch deadline must cut the server-controlled sleep, took {elapsed:?}"
        );
        let error = fetched.expect_err("the deadline error is inspected below");
        assert!(
            format!("{error:#}").contains(&endpoint),
            "the deadline error must name the endpoint that stalled, got: {error:#}"
        );
    }

    /// The endpoint override extracts the domain the crate builds URLs from,
    /// with an explicit port carried through when the endpoint names one.
    #[test]
    fn musicbrainz_client_for_endpoint_extracts_the_domain() {
        let production = musicbrainz_client_for_endpoint("https://musicbrainz.org/ws/2")
            .expect("production endpoint parses");
        assert_eq!(
            production.musicbrainz_domain, "musicbrainz.org",
            "the default endpoint must keep today's production traffic"
        );

        let local = musicbrainz_client_for_endpoint("http://127.0.0.1:18080/ws/2")
            .expect("local endpoint parses");
        assert_eq!(
            local.musicbrainz_domain, "127.0.0.1:18080",
            "an explicit port must survive into the domain"
        );
    }

    /// An endpoint that does not parse fails with the offending value in the
    /// error instead of silently fetching from the default domain.
    #[test]
    fn musicbrainz_client_for_endpoint_rejects_an_unparseable_endpoint() {
        let error = musicbrainz_client_for_endpoint("not a url")
            .expect_err("an unparseable endpoint must not fall back to a default");
        assert!(
            format!("{error:#}").contains("not a url"),
            "the error must name the endpoint, got: {error:#}"
        );
    }

    /// A payload cached under one fetch shape is invisible to every other shape.
    ///
    /// `write_all_tags`, `release_ars`, and the endpoint all change what the
    /// fetch asks MusicBrainz for, so each combination owns its own row.
    /// The scheme is deliberately not part of the key: `musicbrainz_rs`
    /// speaks plain HTTP whatever the endpoint says, so `http://x` and
    /// `https://x` dial the same server and share one row.
    #[tokio::test]
    async fn payloads_cached_under_one_fetch_shape_are_invisible_to_other_shapes() {
        use musicbrainz_rs::api_bindium::ApiClient;
        use musicbrainz_rs::entity::recording::Recording;

        const RECORDING_MBID: &str = "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae";

        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = super::super::cover_art::MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            super::super::cover_art::CacheExpiryPolicy::from_seconds(60),
        );

        let base = MusicBrainzFetchShape {
            write_all_tags: true,
            release_ars: false,
            endpoint: "http://127.0.0.1:18080/ws/2".to_string(),
        };
        let recording: Recording = serde_json::from_str(
            r#"{"id":"8f3471b5-7e6a-48da-86a9-c1c07a0f47ae","title":"cached"}"#,
        )
        .expect("minimal recording payload");
        let payload = MusicBrainzPayload { recording, release: None };
        cache
            .write_musicbrainz_payload(
                &base.cache_key(RECORDING_MBID, None).expect("base key"),
                &payload,
            )
            .await
            .expect("seed shaped row");

        // A client that fails on connection, so a cache miss surfaces as the
        // fetch error rather than a real request.
        let broken = MusicBrainzClient::builder()
            .musicbrainz_domain("127.0.0.1:1".to_string())
            .api_client(
                ApiClient::builder()
                    .agent(bounded_agent(Duration::from_secs(30)))
                    .max_retries(1)
                    .build(),
            )
            .build();

        for (label, shape) in [
            ("all_tags=false", MusicBrainzFetchShape { write_all_tags: false, ..base.clone() }),
            ("ars=true", MusicBrainzFetchShape { release_ars: true, ..base.clone() }),
            (
                "other host",
                MusicBrainzFetchShape {
                    endpoint: "http://127.0.0.1:18081/ws/2".to_string(),
                    ..base.clone()
                },
            ),
        ] {
            let fetched = fetch_musicbrainz_payloads_on(
                &broken,
                &shape,
                RECORDING_MBID,
                None,
                Duration::from_secs(30),
                &cache,
            )
            .await;
            assert!(
                fetched.is_err(),
                "{label}: a differently-shaped fetch must not read the base shape's row"
            );
        }

        let scheme_variant = MusicBrainzFetchShape {
            endpoint: "https://127.0.0.1:18080/ws/2".to_string(),
            ..base.clone()
        };
        let fetched = fetch_musicbrainz_payloads_on(
            &broken,
            &scheme_variant,
            RECORDING_MBID,
            None,
            Duration::from_secs(30),
            &cache,
        )
        .await
        .expect("scheme-only endpoint variants must share one row");
        assert_eq!(fetched.recording.title, "cached");
    }

    /// A row cached under the pre-shape key is invisible to a shaped fetch.
    ///
    /// Upgrade builds ship with rows written by the old key format; the
    /// shaped fetch must miss them and refetch rather than silently serve a
    /// payload whose fetch shape is unknown. The client fails on connection,
    /// so a miss surfaces as the fetch error.
    #[tokio::test]
    async fn regression_a_pre_shape_cache_key_is_not_read_by_a_shaped_fetch() {
        use musicbrainz_rs::api_bindium::ApiClient;
        use musicbrainz_rs::entity::recording::Recording;

        const RECORDING_MBID: &str = "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae";

        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = super::super::cover_art::MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            super::super::cover_art::CacheExpiryPolicy::from_seconds(60),
        );

        let recording: Recording = serde_json::from_str(
            r#"{"id":"8f3471b5-7e6a-48da-86a9-c1c07a0f47ae","title":"legacy"}"#,
        )
        .expect("minimal recording payload");
        let payload = MusicBrainzPayload { recording, release: None };
        cache
            .write_json_payload(
                "musicbrainz-payloads",
                &format!("recording={RECORDING_MBID}|release="),
                &payload,
            )
            .await
            .expect("seed pre-shape row");

        let broken = MusicBrainzClient::builder()
            .musicbrainz_domain("127.0.0.1:1".to_string())
            .api_client(
                ApiClient::builder()
                    .agent(bounded_agent(Duration::from_secs(30)))
                    .max_retries(1)
                    .build(),
            )
            .build();
        let shape = MusicBrainzFetchShape {
            write_all_tags: false,
            release_ars: false,
            endpoint: "http://127.0.0.1:1/ws/2".to_string(),
        };

        let fetched = fetch_musicbrainz_payloads_on(
            &broken,
            &shape,
            RECORDING_MBID,
            None,
            Duration::from_secs(30),
            &cache,
        )
        .await;
        let error = fetched.expect_err("the pre-shape key must not satisfy a shaped fetch");
        assert!(
            error.to_string().contains("fetching MusicBrainz recording"),
            "the error must be the fetch failure itself, got: {error:#}"
        );
    }
}
