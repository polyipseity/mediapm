use serde_json::json;
use tempfile::tempdir;

use mediapm::{
    application::{executor::execute_plan, planner::build_plan},
    configuration::config::AppConfig,
    infrastructure::{store::WorkspacePaths, verify::verify_workspace},
};

fn prepare_workspace_with_import() -> (tempfile::TempDir, WorkspacePaths) {
    let workspace = tempdir().expect("temp workspace should create");
    let source_file = workspace.path().join("inbox/song.flac");

    std::fs::create_dir_all(source_file.parent().expect("parent should exist"))
        .expect("source directory should create");
    std::fs::write(&source_file, b"audio-payload").expect("source file should write");

    let config: AppConfig = serde_json::from_value(json!({
        "sources": [{"uri": "inbox/song.flac"}],
        "links": []
    }))
    .expect("config should deserialize");

    let paths = WorkspacePaths::new(workspace.path());
    let plan = build_plan(&config, workspace.path()).expect("plan should build");
    execute_plan(&paths, &config, &plan, true).expect("sync should succeed");

    (workspace, paths)
}

#[test]
fn verify_reports_clean_state_after_sync() {
    let (_workspace, paths) = prepare_workspace_with_import();

    let report = verify_workspace(&paths).expect("verify should run");

    assert!(report.is_clean());
    assert_eq!(report.hash_mismatches.len(), 0);
    assert_eq!(report.missing_objects.len(), 0);
}

#[test]
fn verify_detects_missing_object_after_manual_deletion() {
    let (_workspace, paths) = prepare_workspace_with_import();

    let object = walkdir::WalkDir::new(&paths.objects_dir)
        .into_iter()
        .filter_map(Result::ok)
        .find(|entry| entry.file_type().is_file())
        .expect("object file should exist")
        .path()
        .to_path_buf();

    std::fs::remove_file(&object).expect("object should be removed");

    let report = verify_workspace(&paths).expect("verify should run");

    assert!(!report.is_clean());
    assert_eq!(report.missing_objects.len(), 1);
}
