//! Golden hierarchy layout contract tests.

use mediapm::demo_hierarchy_spec::{
    assert_tree_under, load_demo_hierarchy_golden_document, offline_demo_media_folder_relative,
    online_demo_media_folder_relative,
};
use tempfile::tempdir;

#[test]
fn golden_fixture_paths_match_shared_constants() {
    let golden = load_demo_hierarchy_golden_document();
    assert_eq!(golden.offline.media_folder, offline_demo_media_folder_relative());
    assert_eq!(golden.online.media_folder, online_demo_media_folder_relative());
}

#[test]
fn assert_tree_under_accepts_synthetic_offline_layout() {
    let root = tempdir().expect("tempdir");
    let golden = load_demo_hierarchy_golden_document();
    let hierarchy_root = root.path();

    for relative_path in &golden.offline.required_files {
        let path = hierarchy_root.join(relative_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent dirs");
        }
        std::fs::write(path, b"fixture").expect("write fixture file");
    }

    assert_tree_under(hierarchy_root, &golden.offline).expect("offline golden tree");
}

#[test]
fn assert_tree_under_rejects_missing_online_sidecar_gate() {
    let root = tempdir().expect("tempdir");
    let golden = load_demo_hierarchy_golden_document();
    let hierarchy_root = root.path();
    let media_folder = hierarchy_root.join(&golden.online.media_folder);
    std::fs::create_dir_all(media_folder.join("sidecars")).expect("create sidecars dir");

    let result = assert_tree_under(hierarchy_root, &golden.online);
    assert!(result.is_err(), "missing info.json should fail golden walk");
}
