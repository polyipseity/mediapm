//! Golden hierarchy layout contract tests.
//!
//! Golden JSON encodes exact link basenames for both formats (yt-dlp sidecar vs mediapm root).
//! See `.agents/instructions/demo-hierarchy-golden.instructions.md`.

use mediapm::demo_hierarchy_spec::{
    DEMO_METADATA_ARTIST, DEMO_METADATA_TITLE, ONLINE_DEMO_MEDIA_ID, assert_tree_under,
    demo_media_folder_name, load_demo_hierarchy_golden_document,
    offline_demo_media_folder_relative, online_demo_media_folder_relative,
    online_demo_root_link_relative_path, online_demo_sidecar_link_relative_path,
};

#[test]
fn golden_fixture_paths_match_shared_constants() {
    let golden = load_demo_hierarchy_golden_document();
    assert_eq!(golden.offline.media_folder, offline_demo_media_folder_relative());
    assert_eq!(golden.online.media_folder, online_demo_media_folder_relative());
}

#[test]
fn golden_fixture_link_paths_match_helpers() {
    let golden = load_demo_hierarchy_golden_document();
    for extension in ["url", "webloc", "desktop"] {
        let sidecar_path = online_demo_sidecar_link_relative_path(extension);
        assert!(
            golden.online.required_files.iter().any(|path| path == &sidecar_path),
            "golden online required_files missing sidecar link path '{sidecar_path}'"
        );
        let root_path = online_demo_root_link_relative_path(extension);
        assert!(
            golden.online.required_files.iter().any(|path| path == &root_path),
            "golden online required_files missing root link path '{root_path}'"
        );
    }
}

#[test]
fn assert_tree_under_accepts_synthetic_offline_layout() {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
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
fn assert_tree_under_accepts_synthetic_online_layout() {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let golden = load_demo_hierarchy_golden_document();
    let hierarchy_root = root.path();
    let media_folder = hierarchy_root.join(&golden.online.media_folder);

    for relative_path in &golden.online.required_files {
        let path = hierarchy_root.join(relative_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent dirs");
        }
        std::fs::write(path, b"fixture").expect("write fixture file");
    }

    for relative_dir in &golden.online.required_nonempty_dirs {
        let dir = hierarchy_root.join(relative_dir);
        std::fs::create_dir_all(&dir).expect("create nonempty dir");
        std::fs::write(dir.join("member.txt"), b"fixture").expect("write dir member");
    }

    std::fs::write(
        media_folder.join(format!(
            "{}.thumbnail.jpg",
            demo_media_folder_name(DEMO_METADATA_ARTIST, DEMO_METADATA_TITLE, ONLINE_DEMO_MEDIA_ID)
        )),
        b"jpg",
    )
    .expect("write thumbnail glob match");
    std::fs::write(media_folder.join("folder.webp"), b"webp").expect("write folder glob match");

    assert_tree_under(hierarchy_root, &golden.online).expect("online golden tree");
}

#[test]
fn assert_tree_under_rejects_missing_online_sidecar_gate() {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let golden = load_demo_hierarchy_golden_document();
    let hierarchy_root = root.path();
    let media_folder = hierarchy_root.join(&golden.online.media_folder);
    std::fs::create_dir_all(media_folder.join("sidecars")).expect("create sidecars dir");

    let result = assert_tree_under(hierarchy_root, &golden.online);
    assert!(result.is_err(), "missing info.json should fail golden walk");
}
