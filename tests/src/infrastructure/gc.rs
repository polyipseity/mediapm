use tempfile::tempdir;

use mediapm::infrastructure::{gc::gc_workspace, store::WorkspacePaths};

#[test]
fn gc_reports_and_removes_orphaned_object() {
    let workspace = tempdir().expect("temp workspace should create");
    let paths = WorkspacePaths::new(workspace.path());
    paths.ensure_store_dirs().expect("store dirs should create");

    let orphan = paths.objects_dir.join("aa/orphan");
    std::fs::create_dir_all(orphan.parent().expect("parent should exist"))
        .expect("orphan parent should create");
    std::fs::write(&orphan, b"orphan-bytes").expect("orphan object should write");

    let dry_run = gc_workspace(&paths, false).expect("gc dry run should succeed");
    assert_eq!(dry_run.candidate_count, 1);
    assert_eq!(dry_run.removed_count, 0);

    let apply = gc_workspace(&paths, true).expect("gc apply should succeed");
    assert_eq!(apply.removed_count, 1);
    assert!(!orphan.exists());
}

#[test]
fn gc_on_empty_workspace_is_noop() {
    let workspace = tempdir().expect("temp workspace should create");
    let paths = WorkspacePaths::new(workspace.path());
    paths.ensure_store_dirs().expect("store dirs should create");

    let report = gc_workspace(&paths, false).expect("gc should succeed");

    assert_eq!(report.referenced_objects, 0);
    assert_eq!(report.candidate_count, 0);
    assert_eq!(report.removed_count, 0);
}
