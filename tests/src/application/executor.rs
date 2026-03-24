use serde_json::json;
use tempfile::tempdir;

use mediapm::{
    application::{executor::execute_plan, planner::build_plan},
    configuration::config::AppConfig,
    infrastructure::store::WorkspacePaths,
};

fn create_basic_workspace() -> (tempfile::TempDir, AppConfig) {
    let workspace = tempdir().expect("temp workspace should create");
    let source_file = workspace.path().join("inbox/song.flac");

    std::fs::create_dir_all(source_file.parent().expect("parent should exist"))
        .expect("source directory should create");
    std::fs::write(&source_file, b"audio-payload").expect("source file should write");

    let config: AppConfig = serde_json::from_value(json!({
        "sources": [{"uri": "inbox/song.flac"}],
        "links": [{"path": "library/song.flac", "from_uri": "inbox/song.flac"}],
        "policies": {"link_methods": ["copy"], "strict_rehash": false, "musicbrainz_enabled": false}
    }))
    .expect("config should deserialize");

    (workspace, config)
}

#[test]
fn dry_run_returns_summary_without_materializing_link() {
    let (workspace, config) = create_basic_workspace();
    let paths = WorkspacePaths::new(workspace.path());
    let plan = build_plan(&config, workspace.path()).expect("plan should build");

    let summary = execute_plan(&paths, &config, &plan, false).expect("dry run should succeed");

    assert_eq!(summary.planned_effects, 2);
    assert_eq!(summary.imports_created, 0);
    assert_eq!(summary.links_created, 0);
    assert_eq!(summary.provider_queries_attempted, 0);
    assert_eq!(summary.provider_cache_hits, 0);
    assert_eq!(summary.provider_sidecars_updated, 0);
    assert_eq!(summary.provider_failures, 0);
    assert!(!workspace.path().join("library/song.flac").exists());
}

#[test]
fn apply_creates_link_and_sidecar_state() {
    let (workspace, config) = create_basic_workspace();
    let paths = WorkspacePaths::new(workspace.path());
    let plan = build_plan(&config, workspace.path()).expect("plan should build");

    let summary = execute_plan(&paths, &config, &plan, true).expect("sync should succeed");

    assert_eq!(summary.imports_created, 1);
    assert_eq!(summary.links_created, 1);
    assert_eq!(summary.provider_queries_attempted, 0);
    assert_eq!(summary.provider_failures, 0);
    assert!(workspace.path().join("library/song.flac").exists());
    assert!(paths.media_dir.exists());
}

#[test]
fn repeated_apply_keeps_imports_idempotent() {
    let (workspace, config) = create_basic_workspace();
    let paths = WorkspacePaths::new(workspace.path());
    let plan = build_plan(&config, workspace.path()).expect("plan should build");

    let _first = execute_plan(&paths, &config, &plan, true).expect("first sync should succeed");
    let second = execute_plan(&paths, &config, &plan, true).expect("second sync should succeed");

    assert_eq!(second.imports_created, 0);
    assert_eq!(second.imports_unchanged, 1);
    assert_eq!(second.provider_queries_attempted, 0);
}
