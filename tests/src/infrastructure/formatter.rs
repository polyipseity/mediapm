use serde_json::json;
use tempfile::tempdir;

use mediapm::{
    application::{executor::execute_plan, planner::build_plan},
    configuration::config::{AppConfig, load_config},
    infrastructure::{formatter::format_workspace, store::WorkspacePaths},
};

#[tokio::test]
async fn format_workspace_rewrites_config_and_sidecars() {
    let workspace = tempdir().expect("temp workspace should create");
    let source_file = workspace.path().join("inbox/song.flac");

    std::fs::create_dir_all(source_file.parent().expect("parent should exist"))
        .expect("source directory should create");
    std::fs::write(&source_file, b"audio-payload").expect("source file should write");

    let config_path = workspace.path().join("mediapm.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec(&json!({
            "sources": [{"uri": "inbox/song.flac"}],
            "links": []
        }))
        .expect("config should serialize"),
    )
    .expect("config should write");

    let config: AppConfig = load_config(&config_path).await.expect("config should load");
    let paths = WorkspacePaths::new(workspace.path());
    let plan = build_plan(&config, workspace.path()).expect("plan should build");
    execute_plan(&paths, &config, &plan, true).await.expect("sync should succeed");

    let report = format_workspace(&paths, &config_path).await.expect("format should succeed");

    assert!(report.config_written);
    assert_eq!(report.sidecars_rewritten, 1);
}
