//! Hermetic media-tagger metadata-stage tests over a mocked network.
//!
//! Each test drives the public `run_internal_media_tagger` entry point against
//! local `wiremock` servers, so the metadata fetch, cover-art discovery, and
//! `FFmetadata` write are exercised end to end without touching the real
//! `MusicBrainz` or Cover Art Archive hosts. Both endpoints are injected through
//! options — the same seam production uses — which is the reason these tests
//! can pin the traffic to loopback.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use mediapm::builtins::media_tagger::{
    DEFAULT_ACOUSTID_ENDPOINT, DEFAULT_CA_PROVIDERS, DEFAULT_CAA_APPROVED_ONLY,
    DEFAULT_CAA_ENDPOINT, DEFAULT_CAA_IMAGE_SIZE, DEFAULT_CAA_IMAGE_TYPES,
    DEFAULT_CACHE_EXPIRY_SECONDS, DEFAULT_CLEAR_EXISTING_TAGS, DEFAULT_COVER_ART_SLOT_COUNT,
    DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE, DEFAULT_ENABLE_TAG_SAVING, DEFAULT_PRESERVE_IMAGES,
    DEFAULT_RELEASE_ARS, DEFAULT_SAVE_IMAGES_TO_TAGS, InternalMediaTaggerOptions,
    MUSICBRAINZ_FETCH_ATTEMPTS, run_internal_media_tagger,
};

/// A valid recording MBID the mock server serves and the run requests.
const RECORDING_MBID: &str = "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae";
/// A valid release MBID the recording's embedded release points to.
const RELEASE_MBID: &str = "11111111-1111-1111-1111-111111111111";
/// A valid release-group MBID the release points to.
const GROUP_MBID: &str = "22222222-2222-2222-2222-222222222222";
/// A valid artist MBID the artist credits point to.
const ARTIST_MBID: &str = "33d21b86-dc54-47b2-9258-92d5b42c99a1";

/// Builds base options for one hermetic run: a recording MBID override (which
/// skips the `AcoustID` fingerprint path, so no input media or ffmpeg are
/// needed), strict identification, and no cover art.
///
/// The output sits one directory below `workspace` so the cover-art slot
/// artifacts resolve under the same temporary tree.
fn hermetic_options(
    workspace: &Path,
    musicbrainz_endpoint: &str,
    caa_endpoint: &str,
) -> InternalMediaTaggerOptions {
    InternalMediaTaggerOptions {
        input_path: None,
        output_path: workspace.join("out").join("output.ffmetadata"),
        acoustid_api_key: None,
        acoustid_endpoint: DEFAULT_ACOUSTID_ENDPOINT.to_string(),
        musicbrainz_endpoint: musicbrainz_endpoint.to_string(),
        caa_endpoint: caa_endpoint.to_string(),
        cache_dir: Some(workspace.join("cache")),
        cache_expiry_seconds: DEFAULT_CACHE_EXPIRY_SECONDS,
        strict_identification: true,
        write_all_tags: false,
        write_all_images: false,
        save_images_to_tags: DEFAULT_SAVE_IMAGES_TO_TAGS,
        embed_only_one_front_image: DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE,
        cover_art_slot_count: DEFAULT_COVER_ART_SLOT_COUNT,
        ca_providers: DEFAULT_CA_PROVIDERS.to_string(),
        caa_image_types: DEFAULT_CAA_IMAGE_TYPES.to_string(),
        caa_image_size: DEFAULT_CAA_IMAGE_SIZE.to_string(),
        caa_approved_only: DEFAULT_CAA_APPROVED_ONLY,
        preserve_images: DEFAULT_PRESERVE_IMAGES,
        clear_existing_tags: DEFAULT_CLEAR_EXISTING_TAGS,
        enable_tag_saving: DEFAULT_ENABLE_TAG_SAVING,
        release_ars: DEFAULT_RELEASE_ARS,
        recording_mbid: Some(RECORDING_MBID.to_string()),
        release_mbid: None,
    }
}

/// The artist credit shared by the recording and release payloads.
fn artist_credit() -> serde_json::Value {
    serde_json::json!([
        {"name": "Test Artist", "joinphrase": "", "artist": {
            "id": ARTIST_MBID,
            "name": "Test Artist",
            "sort-name": "Artist, Test"
        }}
    ])
}

/// The recording payload in the real kebab-case ws/2 shape: artist credit,
/// ISRCs, first release date, and one embedded release whose id steers the
/// follow-up release fetch.
fn recording_json() -> serde_json::Value {
    serde_json::json!({
        "id": RECORDING_MBID,
        "title": "Test Recording",
        "artist-credit": artist_credit(),
        "isrcs": ["USUM71300001"],
        "first-release-date": "1987-07-27",
        "releases": [
            {"id": RELEASE_MBID, "title": "Test Album", "first-release-date": "1987-07-27"}
        ]
    })
}

/// The release payload in the real kebab-case ws/2 shape: date, barcode,
/// country, label info, release group, and one medium whose track points back
/// at the recording so the track position resolves.
fn release_json() -> serde_json::Value {
    serde_json::json!({
        "id": RELEASE_MBID,
        "title": "Test Album",
        "date": "1987-07-27",
        "country": "GB",
        "barcode": "0000000000000",
        "artist-credit": artist_credit(),
        "label-info": [
            {"catalog-number": "TEST 1", "label": {
                "id": "44e0cf0e-3f9c-4b0f-9d4a-6e0e0e0e0e0e",
                "name": "Test Label"
            }}
        ],
        "release-group": {"id": GROUP_MBID, "title": "Test Album"},
        "media": [
            {
                "id": "55555555-5555-5555-5555-555555555555",
                "format": "Vinyl",
                "position": 1,
                "track-count": 1,
                "disc-count": 1,
                "track-offset": 0,
                "tracks": [
                    {
                        "id": "66666666-6666-6666-6666-666666666666",
                        "title": "Test Recording",
                        "number": "1",
                        "position": 1,
                        "length": 1000,
                        "recording": {"id": RECORDING_MBID, "title": "Test Recording"}
                    }
                ]
            }
        ]
    })
}

/// Mounts a `MusicBrainz` ws/2 mock that serves the recording and its release.
async fn mount_musicbrainz(server: &wiremock::MockServer) {
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(format!("/ws/2/recording/{RECORDING_MBID}")))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(recording_json()))
        .mount(server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(format!("/ws/2/release/{RELEASE_MBID}")))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(release_json()))
        .mount(server)
        .await;
}

/// Parses an `FFmetadata` document into its global key/value map so assertions
/// read the tags the run actually wrote.
fn parse_ffmetadata(text: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for line in text.lines() {
        if line.starts_with(';') || line.starts_with('[') || line.trim().is_empty() {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            map.insert(key.to_string(), value.to_string());
        }
    }
    map
}

/// The metadata stage honors the configured `MusicBrainz` endpoint and writes a
/// complete `FFmetadata` document from it, without any real network.
///
/// The recording MBID override skips the `AcoustID` fingerprint path, so no input
/// media and no ffmpeg are needed: the run fetches the recording and its first
/// release from the injected endpoint and writes title, artist, recording id,
/// ISRC, and the release date. Both requests must land on the local server,
/// pinning that `musicbrainz_endpoint` really selects the fetch domain rather
/// than silently falling back to the hardcoded musicbrainz.org.
#[tokio::test]
async fn a_recording_mbid_fetches_metadata_through_the_configured_endpoint() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("workspace");
    let musicbrainz = wiremock::MockServer::start().await;
    mount_musicbrainz(&musicbrainz).await;

    let endpoint = format!("{}/ws/2", musicbrainz.uri());
    let options = hermetic_options(workspace.path(), &endpoint, DEFAULT_CAA_ENDPOINT);
    let output_path = options.output_path.clone();

    run_internal_media_tagger(options).await.expect("metadata fetch through the mock endpoint");

    let text = std::fs::read_to_string(&output_path).expect("ffmetadata output written");
    let tags = parse_ffmetadata(&text);
    assert_eq!(tags.get("TITLE").map(String::as_str), Some("Test Recording"));
    assert_eq!(tags.get("ARTIST").map(String::as_str), Some("Test Artist"));
    assert_eq!(tags.get("MUSICBRAINZ_RECORDINGID").map(String::as_str), Some(RECORDING_MBID));
    assert_eq!(tags.get("ISRC").map(String::as_str), Some("USUM71300001"));
    assert_eq!(tags.get("ORIGINALDATE").map(String::as_str), Some("1987-07-27"));
    assert_eq!(tags.get("ALBUM").map(String::as_str), Some("Test Album"));
    assert_eq!(tags.get("DATE").map(String::as_str), Some("1987-07-27"));
    assert_eq!(tags.get("MUSICBRAINZ_ALBUMID").map(String::as_str), Some(RELEASE_MBID));
    assert_eq!(tags.get("TRACKNUMBER").map(String::as_str), Some("1"));

    let paths = musicbrainz
        .received_requests()
        .await
        .expect("request recorder is enabled")
        .iter()
        .map(|request| request.url.path().to_string())
        .collect::<Vec<_>>();
    assert!(paths.contains(&format!("/ws/2/recording/{RECORDING_MBID}")));
    assert!(paths.contains(&format!("/ws/2/release/{RELEASE_MBID}")));
}

/// A `MusicBrainz` server error fails the step loudly and names the endpoint,
/// instead of shipping a silently empty result.
///
/// The ws/2 paths answer a `503` storm, so the bounded client retries its cap
/// and then surfaces the failure. With `strict_identification` enabled the step
/// returns an error rather than quietly writing fallback metadata, and that
/// error names the configured endpoint. The server uses a short `Retry-After`
/// so the retry cycle stays fast; the production-shaped guard that cuts a
/// server-supplied hour-long `Retry-After` at the outer fetch deadline is
/// pinned at the unit level, where that deadline is injectable.
///
/// The bound below is loose because this test pays for the pacing it shares with
/// its siblings in this binary: three attempts of a retried `503`, each waiting
/// out a `Retry-After` and a one-second request slot. It still fails a step that
/// honors a server-controlled stall instead of surfacing the failure.
#[tokio::test]
async fn a_musicbrainz_server_error_fails_the_step_and_names_the_endpoint() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("workspace");
    let musicbrainz = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path_regex("^/ws/2/"))
        .respond_with(wiremock::ResponseTemplate::new(503).insert_header("Retry-After", "1"))
        .mount(&musicbrainz)
        .await;

    let endpoint = format!("{}/ws/2", musicbrainz.uri());
    let options = hermetic_options(workspace.path(), &endpoint, DEFAULT_CAA_ENDPOINT);

    let started = Instant::now();
    let result = run_internal_media_tagger(options).await;
    let elapsed = started.elapsed();

    let error = result.expect_err("a MusicBrainz error must fail the step in strict mode");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("MusicBrainz"),
        "the error should describe the MusicBrainz fetch failure, got: {rendered}"
    );
    let musicbrainz_address = musicbrainz.address().to_string();
    assert!(
        rendered.contains(musicbrainz_address.as_str()),
        "the error must name the failing MusicBrainz endpoint, got: {rendered}"
    );
    let received = musicbrainz.received_requests().await.expect("request recorder is enabled");
    assert!(
        !received.is_empty(),
        "the 503 storm must have actually reached the local server; otherwise the error names the endpoint without the endpoint having answered"
    );
    assert!(
        elapsed < Duration::from_mins(1),
        "the bounded client must surface the failure rather than honor a server-controlled stall: {elapsed:?}"
    );
}

/// A dropped connection is retried instead of failing the step.
///
/// The proxy and the upstream both drop connections under load, and the failure
/// arrives as a `502` *response* — which `api_bindium`'s retry loop never
/// examines, because it retries only a `503` status and transport errors. One
/// `502` followed by a good response must therefore succeed.
///
/// The expiring `502` mock carries priority 1, above the default 5, because
/// wiremock scans its mocks in mounting order and breaks a priority tie by
/// insertion: the recording mock mounted first would otherwise answer every
/// request and the `502` would never be served. Once the mock has matched its
/// one request it stops matching, so the retry reaches the recording mock.
#[tokio::test]
async fn a_musicbrainz_502_is_retried() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("workspace");
    let musicbrainz = wiremock::MockServer::start().await;
    mount_musicbrainz(&musicbrainz).await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path_regex("^/ws/2/"))
        .respond_with(wiremock::ResponseTemplate::new(502))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&musicbrainz)
        .await;

    let endpoint = format!("{}/ws/2", musicbrainz.uri());
    let options = hermetic_options(workspace.path(), &endpoint, DEFAULT_CAA_ENDPOINT);

    let result = run_internal_media_tagger(options).await;

    assert!(result.is_ok(), "one dropped connection must not fail the step: {result:?}");
    let received = musicbrainz.received_requests().await.expect("request recorder is enabled");
    assert_eq!(
        received.len(),
        3,
        "the 502 attempt plus the recording and its release: two would mean the 502 mock never matched",
    );
}

/// A persistent `502` still fails, and is attempted the declared number of
/// times.
///
/// Retrying is bounded: the step surfaces the failure instead of looping, and
/// the attempt count is what bounds it. One request per attempt is the other
/// half of the contract — the dependency's own loop does not retry a `502`, so
/// a count above the declared attempts would mean a retry nobody asked for.
#[tokio::test]
async fn a_persistent_musicbrainz_502_is_surfaced() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("workspace");
    let musicbrainz = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path_regex("^/ws/2/"))
        .respond_with(wiremock::ResponseTemplate::new(502))
        .mount(&musicbrainz)
        .await;

    let endpoint = format!("{}/ws/2", musicbrainz.uri());
    let options = hermetic_options(workspace.path(), &endpoint, DEFAULT_CAA_ENDPOINT);

    let error =
        run_internal_media_tagger(options).await.expect_err("a persistent 502 must fail the step");

    let rendered = format!("{error:#}");
    assert!(rendered.contains("MusicBrainz"), "the error should name the fetch: {rendered}");
    let received = musicbrainz.received_requests().await.expect("request recorder is enabled");
    assert_eq!(
        received.len(),
        MUSICBRAINZ_FETCH_ATTEMPTS as usize,
        "each attempt makes one request, so the count is the bound",
    );
}

/// Cover art is discovered through the configured archive endpoint and its
/// bytes land in the slot artifact the downstream apply stage reads.
///
/// The run enables images, so after the `MusicBrainz` fetch it queries the
/// injected archive endpoint for the release's cover-art index, selects the
/// front image, downloads its bytes, and writes them to the first cover-art
/// slot. Both the archive index request and the image download must reach the
/// local archive server, and the slot file must hold exactly the served bytes.
#[tokio::test]
async fn cover_art_is_discovered_and_downloaded_through_the_configured_archive_endpoint() {
    let image_bytes = b"fake-jpeg-bytes";
    let workspace = mediapm_utils::temp::artifact_dir().expect("workspace");
    let musicbrainz = wiremock::MockServer::start().await;
    mount_musicbrainz(&musicbrainz).await;
    let archive = wiremock::MockServer::start().await;

    let image_url = format!("{}/img/front.jpg", archive.uri());
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(format!("/release/{RELEASE_MBID}")))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "images": [{
                "image": image_url,
                "thumbnails": {"250": format!("{image_url}?thumb"), "1200": format!("{image_url}?large")},
                "types": ["Front"],
                "comment": "",
                "approved": true
            }]
        })))
        .mount(&archive)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/img/front.jpg"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_raw(image_bytes, "image/jpeg"))
        .mount(&archive)
        .await;

    let musicbrainz_endpoint = format!("{}/ws/2", musicbrainz.uri());
    let mut options = hermetic_options(workspace.path(), &musicbrainz_endpoint, &archive.uri());
    options.write_all_images = true;
    options.save_images_to_tags = true;
    options.embed_only_one_front_image = false;
    options.ca_providers = "caa_release".to_string();
    options.caa_image_size = "full".to_string();
    let output_path = options.output_path.clone();

    run_internal_media_tagger(options).await.expect("cover-art run through the mock archive");

    let text = std::fs::read_to_string(&output_path).expect("ffmetadata output written");
    let tags = parse_ffmetadata(&text);
    assert_eq!(
        tags.get("COVERART_URL").map(String::as_str),
        Some(image_url.as_str()),
        "the front image url must be written into the cover-art tag"
    );
    assert_eq!(tags.get("COVERART_MAINTYPE").map(String::as_str), Some("front"));

    let slot = workspace.path().join("coverart").join("coverart-slot-1.bin");
    let slot_bytes = std::fs::read(&slot).expect("first cover-art slot artifact written");
    assert_eq!(slot_bytes, image_bytes, "the slot must hold the exact downloaded image bytes");

    let paths = archive
        .received_requests()
        .await
        .expect("request recorder is enabled")
        .iter()
        .map(|request| request.url.path().to_string())
        .collect::<Vec<_>>();
    assert!(paths.contains(&format!("/release/{RELEASE_MBID}")));
    assert!(paths.contains(&"/img/front.jpg".to_string()));
}

/// A cover-art archive error is a loud hard failure, not a silent skip.
///
/// With images enabled, the archive index endpoint answers `500`. The run must
/// fail rather than quietly continue without cover art, and the error must name
/// the archive endpoint that failed. This pins the no-silent-skip policy at the
/// integration level: a failed image fetch is a missing asset and stops the
/// step, it is not downgraded to a warning or an empty result.
#[tokio::test]
async fn a_cover_art_archive_error_fails_the_step_and_names_the_endpoint() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("workspace");
    let musicbrainz = wiremock::MockServer::start().await;
    mount_musicbrainz(&musicbrainz).await;
    let archive = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(format!("/release/{RELEASE_MBID}")))
        .respond_with(wiremock::ResponseTemplate::new(500))
        .mount(&archive)
        .await;

    let musicbrainz_endpoint = format!("{}/ws/2", musicbrainz.uri());
    let mut options = hermetic_options(workspace.path(), &musicbrainz_endpoint, &archive.uri());
    options.write_all_images = true;
    options.save_images_to_tags = true;
    options.embed_only_one_front_image = false;
    options.ca_providers = "caa_release".to_string();

    let result = run_internal_media_tagger(options).await;

    let error = result.expect_err("a cover-art archive error must fail the step");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("cover-art") || rendered.contains("HTTP 500"),
        "the error should describe the cover-art failure, got: {rendered}"
    );
    let archive_address = archive.address().to_string();
    assert!(
        rendered.contains(archive_address.as_str()),
        "the error must name the failing archive endpoint, got: {rendered}"
    );
}
