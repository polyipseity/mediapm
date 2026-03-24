use std::fs;

use serde_json::json;
use tempfile::tempdir;

use mediapm::configuration::config::{
    AppConfig, LinkDecl, Policies, SelectionPreference, SourceDecl, VariantSelection, load_config,
    save_config_pretty,
};

#[test]
fn save_and_load_config_round_trip() {
    let workspace = tempdir().expect("temp workspace should create");
    let config_path = workspace.path().join("mediapm.json");

    let config = AppConfig {
        sources: vec![SourceDecl { uri: "inbox/song.flac".to_owned(), tags: Default::default() }],
        links: vec![LinkDecl {
            path: "library/song.flac".to_owned(),
            from_uri: "inbox/song.flac".to_owned(),
            select: VariantSelection {
                prefer: SelectionPreference::LatestNonLossy,
                variant_hash: None,
            },
        }],
        metadata_overrides: Default::default(),
        provider_queries: Default::default(),
        policies: Policies::default(),
    };

    save_config_pretty(&config_path, &config).expect("config should save");
    let loaded = load_config(&config_path).expect("config should load");

    assert_eq!(loaded.sources.len(), 1);
    assert_eq!(loaded.links.len(), 1);
    assert_eq!(loaded.links[0].select.prefer, SelectionPreference::LatestNonLossy);
}

#[test]
fn rejects_unknown_extension() {
    let workspace = tempdir().expect("temp workspace should create");
    let config_path = workspace.path().join("mediapm.toml");
    fs::write(&config_path, b"anything").expect("config bytes should write");

    let error = load_config(&config_path).expect_err("unknown extension should fail");
    let error_text = format!("{error:#}");

    assert!(error_text.contains("unsupported config extension"));
}

#[test]
fn accepts_json_content_with_ncl_extension() {
    let workspace = tempdir().expect("temp workspace should create");
    let config_path = workspace.path().join("mediapm.ncl");

    let value = json!({
        "sources": [{"uri": "inbox/a.flac"}],
        "links": []
    });

    fs::write(&config_path, serde_json::to_vec_pretty(&value).expect("json should serialize"))
        .expect("ncl config should write");

    let config = load_config(&config_path).expect("json-compatible ncl content should load");
    assert_eq!(config.sources.len(), 1);
    assert!(config.links.is_empty());
}

#[test]
fn deserializes_provider_query_and_musicbrainz_policy_fields() {
    let workspace = tempdir().expect("temp workspace should create");
    let config_path = workspace.path().join("mediapm.json");

    let value = json!({
        "sources": [{"uri": "inbox/song.flac"}],
        "provider_queries": {
            "inbox/song.flac": {
                "artist": "Artist",
                "title": "Song",
                "release": "Album",
                "limit": 3
            }
        },
        "policies": {
            "musicbrainz_enabled": true,
            "musicbrainz": {
                "base_url": "http://127.0.0.1:9/ws/2",
                "user_agent": "mediapm-test",
                "timeout_ms": 50,
                "min_interval_ms": 0,
                "cache_ttl_seconds": 1,
                "max_candidates": 3
            }
        }
    });

    fs::write(&config_path, serde_json::to_vec_pretty(&value).expect("config should serialize"))
        .expect("config should write");

    let config = load_config(&config_path).expect("config should load");
    assert!(config.policies.musicbrainz_enabled);
    assert_eq!(config.policies.musicbrainz.max_candidates, 3);
    assert_eq!(config.provider_queries["inbox/song.flac"].artist.as_deref(), Some("Artist"));
}
