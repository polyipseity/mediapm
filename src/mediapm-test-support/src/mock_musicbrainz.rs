//! A local stand-in for the `MusicBrainz` web service.
//!
//! The media tagger is a host builtin mediapm runs in-process, so a hermetic
//! tagging step needs no executable from upstream — only an answer to its two
//! lookups. This mock answers them with the shape the tagger's parser reads,
//! which is the real ws/2 kebab-case JSON: a recording with its artist credit,
//! its ISRCs, its first release date and the release it appears on, and that
//! release with its date, label and track list.
//!
//! The payloads carry the demo's own artist and title, so a tagging run against
//! this mock writes the canonical tags the demo's assertions compare against
//! rather than a stand-in that would need translating. Cover art and `AcoustID`
//! are deliberately absent: the demo binds a recording MBID, which skips the
//! fingerprint lookup, and sets `write_all_images = "false"`, which skips cover
//! art — so neither is reachable, and a route for them would be a shape no run
//! reads.

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The recording MBID a tagging step binds to skip the fingerprint lookup.
pub const RECORDING_MBID: &str = "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae";

/// The release MBID the recording's embedded release points to.
pub const RELEASE_MBID: &str = "11111111-1111-1111-1111-111111111111";

/// The release-group MBID the release points to.
pub const RELEASE_GROUP_MBID: &str = "22222222-2222-2222-2222-222222222222";

/// The artist MBID the artist credits point to.
pub const ARTIST_MBID: &str = "33d21b86-dc54-47b2-9258-92d5b42c99a1";

/// The artist the mock's payloads credit.
///
/// This is the demo's own artist (`DEMO_METADATA_ARTIST` in
/// `src/mediapm/src/demo_hierarchy_spec.rs`), so on the real-upstream path the
/// `ARTIST` tag a tagging run writes is the one the demo asserts. A stub-chain run
/// never reaches that tag: its `ffmpeg` is a copy, so the tagger's metadata
/// document lands in no container and the materialized file keeps the fixture's own
/// tags. What a stub-chain run can prove about this value is that the tagger asked
/// the mock for the recording it binds, not that the answer reached a file.
pub const ARTIST_NAME: &str = "Rick Astley";

/// The recording title the mock reports.
///
/// This is the demo's own title (`DEMO_METADATA_TITLE`), which a tagging run writes
/// and the hierarchy renders into the media's folder name on the real-upstream path.
/// A stub-chain run renders the chain probe's title instead: its `ffmpeg` is a copy,
/// so the tagger's document reaches no container.
pub const RECORDING_TITLE: &str = "Never Gonna Give You Up";

/// The release title the mock reports.
///
/// The release is the single the recording was published on, so its title is
/// the recording's; the tagger writes it as the `ALBUM` tag.
pub const RELEASE_TITLE: &str = "Never Gonna Give You Up";

/// The release date the mock reports, which the tagger writes as `DATE`.
pub const RELEASE_DATE: &str = "1987-07-27";

/// The label the mock reports, which the tagger writes as `LABEL`.
pub const RELEASE_LABEL: &str = "RCA Victor";

/// A `wiremock` server answering the `MusicBrainz` lookups a tagging step makes.
pub struct MockMusicBrainz {
    /// The server every mounted route is attached to.
    server: MockServer,
}

impl MockMusicBrainz {
    /// Starts a server answering the recording and release lookups.
    ///
    /// # Panics
    ///
    /// Panics when the routes cannot be mounted, which no caller can act on.
    pub async fn start() -> Self {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(format!("/ws/2/recording/{RECORDING_MBID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(recording_payload()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/ws/2/release/{RELEASE_MBID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(release_payload()))
            .mount(&server)
            .await;

        Self { server }
    }

    /// The endpoint a tagging step's `musicbrainz_endpoint` option takes.
    ///
    /// The option's value is a ws/2 URL and the tagger takes only its host and
    /// port from it (`musicbrainz_host_for_endpoint` in
    /// `src/mediapm/src/builtins/media_tagger/musicbrainz.rs`), because the
    /// `musicbrainz_rs` client appends `/ws/2` itself.
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("{}/ws/2", self.server.uri())
    }

    /// The underlying server, for a route this type does not mount and for the
    /// request log a caller inspects after a run.
    #[must_use]
    pub fn server(&self) -> &MockServer {
        &self.server
    }
}

/// The artist credit both payloads carry.
fn artist_credit() -> serde_json::Value {
    serde_json::json!([
        {
            "name": ARTIST_NAME,
            "joinphrase": "",
            "artist": {"id": ARTIST_MBID, "name": ARTIST_NAME, "sort-name": "Astley, Rick"},
        }
    ])
}

/// The recording payload in the ws/2 kebab-case shape the tagger parses.
///
/// The embedded release is the one the tagger then fetches by MBID, so a
/// recording without it would leave the run with a recording and no album tags.
fn recording_payload() -> serde_json::Value {
    serde_json::json!({
        "id": RECORDING_MBID,
        "title": RECORDING_TITLE,
        "artist-credit": artist_credit(),
        "isrcs": ["GBARL9300135"],
        "first-release-date": RELEASE_DATE,
        "releases": [
            {"id": RELEASE_MBID, "title": RELEASE_TITLE, "first-release-date": RELEASE_DATE}
        ]
    })
}

/// The release payload in the ws/2 kebab-case shape the tagger parses.
///
/// The track list names the recording, which is how the tagger resolves the
/// track number and the release's total counts.
fn release_payload() -> serde_json::Value {
    serde_json::json!({
        "id": RELEASE_MBID,
        "title": RELEASE_TITLE,
        "date": RELEASE_DATE,
        "country": "GB",
        "barcode": "5012394144777",
        "artist-credit": artist_credit(),
        "release-group": {"id": RELEASE_GROUP_MBID, "title": RELEASE_TITLE},
        "label-info": [
            {
                "catalog-number": "PB 41447",
                "label": {"id": "99999999-9999-9999-9999-999999999999", "name": RELEASE_LABEL}
            }
        ],
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
                        "title": RECORDING_TITLE,
                        "number": "1",
                        "position": 1,
                        "length": 213_000,
                        "recording": {"id": RECORDING_MBID, "title": RECORDING_TITLE}
                    }
                ]
            }
        ]
    })
}
