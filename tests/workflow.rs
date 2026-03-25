//! End-to-end workflow tests for mediapm MVP behaviors.
//!
//! These tests focus on user-visible guarantees:
//! - sync idempotency,
//! - integrity verification,
//! - garbage collection semantics.
//!
//! The intent is to protect behavioral contracts across layers, not just unit
//! implementation details.

use std::fs;

use serde_json::json;
use tempfile::tempdir;

use mediapm::{
    application::{executor::execute_plan, planner::build_plan},
    configuration::config::load_config,
    infrastructure::{gc::gc_workspace, store::WorkspacePaths, verify::verify_workspace},
};

#[tokio::test]
/// Running sync twice with unchanged inputs should be stable and verify clean.
///
/// Why this matters: repeated reconciliation is the normal operational mode for
/// declarative tools. If sync is not idempotent, users lose trust in dry-runs
/// and automation pipelines become noisy or destructive.
async fn sync_is_repeatable_and_verify_passes() {
    let workspace = tempdir().expect("temp workspace should create");
    let workspace_root = workspace.path();

    let source_file = workspace_root.join("inbox/song.flac");
    fs::create_dir_all(source_file.parent().expect("parent should exist"))
        .expect("source dir should create");
    fs::write(&source_file, b"flac-bytes-v1").expect("source file should be written");

    let config_path = workspace_root.join("mediapm.json");
    let config_value = json!({
        "sources": [
            { "uri": "inbox/song.flac" }
        ],
        "links": [
            {
                "path": "library/song.flac",
                "from_uri": "inbox/song.flac",
                "select": { "prefer": "latest_non_lossy" }
            }
        ]
    });
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config_value).expect("config should serialize"),
    )
    .expect("config should be written");

    let config = load_config(&config_path).await.expect("config should load");
    let paths = WorkspacePaths::new(workspace_root);

    let plan_first = build_plan(&config, workspace_root).expect("first plan should build");
    let first_summary =
        execute_plan(&paths, &config, &plan_first, true).await.expect("first sync should succeed");

    assert_eq!(first_summary.imports_created, 1);
    assert_eq!(first_summary.imports_unchanged, 0);
    assert_eq!(first_summary.links_created, 1);

    let linked_file = workspace_root.join("library/song.flac");
    assert!(linked_file.exists());

    let plan_second = build_plan(&config, workspace_root).expect("second plan should build");
    let second_summary = execute_plan(&paths, &config, &plan_second, true)
        .await
        .expect("second sync should succeed");

    assert_eq!(second_summary.imports_created, 0);
    assert_eq!(second_summary.imports_unchanged, 1);
    assert!(second_summary.links_unchanged + second_summary.links_updated >= 1);

    let verify_report = verify_workspace(&paths).await.expect("verify should succeed");
    assert!(verify_report.is_clean(), "verify report should be clean");
}

#[tokio::test]
/// Corrupted object bytes must be detected by verify.
///
/// Why this matters: sidecars store expected content hashes, so verification
/// must detect byte-level drift regardless of how corruption occurred.
async fn verify_detects_object_corruption() {
    let workspace = tempdir().expect("temp workspace should create");
    let workspace_root = workspace.path();

    let source_file = workspace_root.join("inbox/song.flac");
    fs::create_dir_all(source_file.parent().expect("parent should exist"))
        .expect("source dir should create");
    fs::write(&source_file, b"clean-content").expect("source file should be written");

    let config_path = workspace_root.join("mediapm.json");
    let config_value = json!({
        "sources": [
            { "uri": "inbox/song.flac" }
        ]
    });
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config_value).expect("config should serialize"),
    )
    .expect("config should be written");

    let config = load_config(&config_path).await.expect("config should load");
    let paths = WorkspacePaths::new(workspace_root);
    let plan = build_plan(&config, workspace_root).expect("plan should build");
    execute_plan(&paths, &config, &plan, true).await.expect("sync should succeed");

    let mut all_objects = Vec::new();
    for entry in walkdir::WalkDir::new(&paths.objects_dir).into_iter().filter_map(Result::ok) {
        if entry.file_type().is_file() {
            all_objects.push(entry.path().to_path_buf());
        }
    }

    assert_eq!(all_objects.len(), 1);
    fs::write(&all_objects[0], b"corrupted").expect("object should be corrupted for test");

    let verify_report = verify_workspace(&paths).await.expect("verify should run");
    assert!(!verify_report.is_clean(), "verify should fail after corruption");
    assert_eq!(verify_report.hash_mismatches.len(), 1);
}

#[tokio::test]
/// GC should report candidates in dry-run and remove them in apply mode.
///
/// Why this matters: GC is destructive by nature, so users need inspectable
/// candidate reporting before explicit deletion.
async fn gc_finds_and_removes_unreferenced_objects() {
    let workspace = tempdir().expect("temp workspace should create");
    let paths = WorkspacePaths::new(workspace.path());
    paths.ensure_store_dirs().await.expect("store dirs should be created");

    let orphan = paths.objects_dir.join("aa").join("orphan-object");
    fs::create_dir_all(orphan.parent().expect("orphan parent should exist"))
        .expect("orphan directory should create");
    fs::write(&orphan, b"orphan").expect("orphan file should be written");

    let dry_run = gc_workspace(&paths, false).await.expect("gc dry-run should succeed");
    assert_eq!(dry_run.candidate_count, 1);
    assert_eq!(dry_run.removed_count, 0);

    let apply = gc_workspace(&paths, true).await.expect("gc apply should succeed");
    assert_eq!(apply.candidate_count, 1);
    assert_eq!(apply.removed_count, 1);
    assert!(!orphan.exists());
}
