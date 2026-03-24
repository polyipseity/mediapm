use mediapm::domain::provider::{MusicBrainzQuery, ProviderCandidate};

#[test]
fn query_synthesis_works_from_hints() {
    let query = MusicBrainzQuery {
        query: None,
        artist: Some("Artist".to_owned()),
        title: Some("Title".to_owned()),
        release: Some("Album".to_owned()),
        limit: Some(3),
    };

    let expression = query.effective_query().expect("query should synthesize");
    assert!(expression.contains("artist:\"Artist\""));
    assert!(expression.contains("recording:\"Title\""));
    assert!(expression.contains("release:\"Album\""));
}

#[test]
fn candidate_patch_contains_expected_tag_fields() {
    let candidate = ProviderCandidate {
        provider: "musicbrainz".to_owned(),
        entity_id: Some("rec-1".to_owned()),
        title: Some("Song".to_owned()),
        artist: Some("Artist".to_owned()),
        release: Some("Album".to_owned()),
        score: Some(99.0),
        raw: serde_json::json!({}),
    };

    let (patch, provenance) = candidate.metadata_patch_with_provenance();

    assert_eq!(patch["tags"]["title"], "Song");
    assert_eq!(patch["tags"]["artist"], "Artist");
    assert_eq!(patch["tags"]["album"], "Album");

    assert_eq!(provenance["title"], "provider.musicbrainz");
    assert_eq!(provenance["artist"], "provider.musicbrainz");
    assert_eq!(provenance["album"], "provider.musicbrainz");
}
