use std::fs;

use mediapm::{
    configuration::config::MusicBrainzPolicy,
    domain::provider::MusicBrainzQuery,
    infrastructure::{provider::MusicBrainzProvider, provider::musicbrainz::MusicBrainzHttpProvider, store::WorkspacePaths},
};

#[test]
fn search_uses_fresh_cache_without_network() {
    let workspace = tempfile::tempdir().expect("temp workspace should create");
    let paths = WorkspacePaths::new(workspace.path());
    paths.ensure_store_dirs().expect("store dirs should create");

    let policy = MusicBrainzPolicy {
        base_url: "http://127.0.0.1:9/ws/2".to_owned(),
        cache_ttl_seconds: 3600,
        ..MusicBrainzPolicy::default()
    };

    let mut provider = MusicBrainzHttpProvider::new(&paths, &policy).expect("provider should create");

    let query = MusicBrainzQuery {
        query: Some("recording:\"Song\"".to_owned()),
        artist: None,
        title: None,
        release: None,
        limit: Some(2),
    };

    let cache_key = provider
        .cache_key_for_query(&query)
        .expect("cache key should generate");
    let cache_path = provider.cache_path_for_key(&cache_key);

    let payload = serde_json::json!({
        "fetched_at": "2030-01-01T00:00:00Z",
        "response": {
            "recordings": [{
                "id": "rec-1",
                "title": "Song",
                "score": "95",
                "artist-credit": [{"name": "Artist"}],
                "releases": [{"title": "Album"}]
            }]
        }
    });

    fs::create_dir_all(cache_path.parent().expect("cache parent should exist"))
        .expect("cache parent should create");
    fs::write(
        &cache_path,
        serde_json::to_vec_pretty(&payload).expect("cache payload should serialize"),
    )
    .expect("cache payload should write");

    let result = provider
        .search_recordings(&query)
        .expect("cached query should succeed");

    assert!(result.cache_hit);
    assert_eq!(result.candidates.len(), 1);
    assert_eq!(result.candidates[0].entity_id.as_deref(), Some("rec-1"));
}

#[test]
fn expired_cache_falls_back_to_network_and_errors_when_unreachable() {
    let workspace = tempfile::tempdir().expect("temp workspace should create");
    let paths = WorkspacePaths::new(workspace.path());
    paths.ensure_store_dirs().expect("store dirs should create");

    let policy = MusicBrainzPolicy {
        base_url: "http://127.0.0.1:9/ws/2".to_owned(),
        cache_ttl_seconds: 1,
        timeout_ms: 200,
        min_interval_ms: 0,
        ..MusicBrainzPolicy::default()
    };

    let mut provider = MusicBrainzHttpProvider::new(&paths, &policy).expect("provider should create");

    let query = MusicBrainzQuery {
        query: Some("recording:\"Song\"".to_owned()),
        artist: None,
        title: None,
        release: None,
        limit: Some(2),
    };

    let cache_key = provider
        .cache_key_for_query(&query)
        .expect("cache key should generate");
    let cache_path = provider.cache_path_for_key(&cache_key);

    let stale_payload = serde_json::json!({
        "fetched_at": "2000-01-01T00:00:00Z",
        "response": {"recordings": []}
    });

    fs::create_dir_all(cache_path.parent().expect("cache parent should exist"))
        .expect("cache parent should create");
    fs::write(
        &cache_path,
        serde_json::to_vec_pretty(&stale_payload).expect("cache payload should serialize"),
    )
    .expect("cache payload should write");

    let error = provider
        .search_recordings(&query)
        .expect_err("expired cache should force unreachable network and fail");

    assert!(format!("{error:#}").contains("musicbrainz request failed"));
}
