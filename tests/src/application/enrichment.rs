use anyhow::Result;
use serde_json::json;

use mediapm::{
    application::{
        enrichment::apply_musicbrainz_enrichment_with_provider, executor::execute_plan,
        planner::build_plan,
    },
    configuration::config::{AppConfig, MusicBrainzQueryDecl},
    domain::provider::{MusicBrainzQuery, ProviderCandidate, ProviderSearchResult},
    infrastructure::{
        provider::MusicBrainzProvider, store::WorkspacePaths, verify::verify_workspace,
    },
};

struct StubProvider {
    result: ProviderSearchResult,
}

impl MusicBrainzProvider for StubProvider {
    fn search_recordings(&mut self, _query: &MusicBrainzQuery) -> Result<ProviderSearchResult> {
        Ok(self.result.clone())
    }
}

fn setup_workspace_with_sidecar() -> (tempfile::TempDir, WorkspacePaths, AppConfig, String) {
    let workspace = tempfile::tempdir().expect("temp workspace should create");
    let source_file = workspace.path().join("inbox/song.flac");

    std::fs::create_dir_all(source_file.parent().expect("parent should exist"))
        .expect("source directory should create");
    std::fs::write(&source_file, b"seed-audio").expect("source file should write");

    let canonical_uri =
        mediapm::domain::canonical::canonicalize_uri("inbox/song.flac", workspace.path())
            .expect("uri should canonicalize")
            .into_string();

    let mut provider_queries = std::collections::BTreeMap::new();
    provider_queries.insert(
        "inbox/song.flac".to_owned(),
        MusicBrainzQueryDecl {
            query: None,
            artist: Some("Artist".to_owned()),
            title: Some("Song".to_owned()),
            release: Some("Album".to_owned()),
            limit: Some(3),
        },
    );

    let mut metadata_overrides = std::collections::BTreeMap::new();
    metadata_overrides.insert(canonical_uri.clone(), json!({"tags": {"artist": "User Artist"}}));

    let config: AppConfig = serde_json::from_value(json!({
        "sources": [{"uri": "inbox/song.flac"}],
        "links": [],
        "policies": {"musicbrainz_enabled": true},
    }))
    .expect("config should deserialize");

    let mut config = config;
    config.provider_queries = provider_queries;
    config.metadata_overrides = metadata_overrides;

    let paths = WorkspacePaths::new(workspace.path());
    let plan = build_plan(&config, workspace.path()).expect("plan should build");
    execute_plan(&paths, &config, &plan, true).expect("sync should succeed");

    (workspace, paths, config, canonical_uri)
}

#[test]
fn provider_enrichment_applies_patch_with_priority_rules() {
    let (_workspace, paths, config, canonical_uri) = setup_workspace_with_sidecar();

    let mut provider = StubProvider {
        result: ProviderSearchResult {
            candidates: vec![ProviderCandidate {
                provider: "musicbrainz".to_owned(),
                entity_id: Some("rec-1".to_owned()),
                title: Some("Provider Song".to_owned()),
                artist: Some("Provider Artist".to_owned()),
                release: Some("Provider Album".to_owned()),
                score: Some(100.0),
                raw: json!({"id": "rec-1"}),
            }],
            cache_hit: true,
        },
    };

    let summary = apply_musicbrainz_enrichment_with_provider(&paths, &config, &mut provider)
        .expect("enrichment should succeed");

    assert_eq!(summary.queries_declared, 1);
    assert_eq!(summary.queries_attempted, 1);
    assert_eq!(summary.cache_hits, 1);
    assert_eq!(summary.failures, 0);
    assert_eq!(summary.sidecars_updated, 1);

    let sidecar = mediapm::infrastructure::store::read_sidecar(&paths, &canonical_uri)
        .expect("sidecar should read")
        .expect("sidecar should exist");

    let latest = sidecar.variants.last().expect("latest variant should exist");
    assert_eq!(latest.metadata["tags"]["title"], "Provider Song");
    assert_eq!(latest.metadata["tags"]["album"], "Provider Album");
    assert_ne!(latest.metadata["tags"]["artist"], "Provider Artist");

    assert!(sidecar.edits.iter().any(|event| event.operation == "provider_musicbrainz_apply"));

    let verify = verify_workspace(&paths).expect("verify should run");
    assert!(verify.is_clean());
}

#[test]
fn repeated_same_enrichment_is_idempotent() {
    let (_workspace, paths, config, _canonical_uri) = setup_workspace_with_sidecar();

    let base_result = ProviderSearchResult {
        candidates: vec![ProviderCandidate {
            provider: "musicbrainz".to_owned(),
            entity_id: Some("rec-1".to_owned()),
            title: Some("Provider Song".to_owned()),
            artist: Some("Provider Artist".to_owned()),
            release: Some("Provider Album".to_owned()),
            score: Some(100.0),
            raw: json!({"id": "rec-1"}),
        }],
        cache_hit: true,
    };

    let mut provider_a = StubProvider { result: base_result.clone() };
    let first = apply_musicbrainz_enrichment_with_provider(&paths, &config, &mut provider_a)
        .expect("first enrichment should succeed");

    let mut provider_b = StubProvider { result: base_result };
    let second = apply_musicbrainz_enrichment_with_provider(&paths, &config, &mut provider_b)
        .expect("second enrichment should succeed");

    assert_eq!(first.sidecars_updated, 1);
    assert_eq!(second.sidecars_updated, 0);
}
