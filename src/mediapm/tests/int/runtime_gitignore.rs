//! # Runtime gitignore creation integration tests
//!
//! Tests for `ensure_mediapm_gitignore` — the mediapm-layer `.gitignore`
//! generation that fires at service construction time.

use mediapm::{MediaPmService, load_mediapm_document};

// ---------------------------------------------------------------------------
// Gitignore creation on service construction
// ---------------------------------------------------------------------------

/// Verifies that constructing a `MediaPmService` creates a `.gitignore` in
/// the runtime root with the correct default entries.
#[tokio::test]
async fn gitignore_created_on_service_construction() {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let service = MediaPmService::new_fs_at(root.path()).await.expect("create service");

    let gitignore_path = service.paths().runtime_root.join(".gitignore");
    assert!(gitignore_path.exists(), ".gitignore must exist after service construction");

    let content = std::fs::read_to_string(&gitignore_path).expect("read .gitignore");
    assert!(content.contains("/.env\n"), "must include /.env entry");
    assert!(content.contains("/.env.generated\n"), "must include /.env.generated entry");
    assert!(content.contains("/cache/\n"), "must include /cache/ entry");
    assert!(content.contains("/tools/\n"), "must include /tools/ entry");
}

/// Verifies that the `.gitignore` is not overwritten when the file already
/// exists, preserving any user customizations.
#[tokio::test]
async fn gitignore_preserves_custom_content_on_reconstruction() {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let service = MediaPmService::new_fs_at(root.path()).await.expect("create service");

    let gitignore_path = service.paths().runtime_root.join(".gitignore");
    assert!(gitignore_path.exists(), ".gitignore must exist");

    // Add a custom line to the .gitignore.
    let custom_line = "/custom/\n";
    let existing = std::fs::read_to_string(&gitignore_path).expect("read .gitignore");
    let modified = format!("{existing}{custom_line}");
    std::fs::write(&gitignore_path, &modified).expect("write modified .gitignore");

    // Drop the first service to release the CAS directory lock.
    drop(service);

    // Reconstruct the service — should NOT overwrite the .gitignore.
    let _service2 = MediaPmService::new_fs_at(root.path()).await.expect("re-create service");

    let final_content = std::fs::read_to_string(&gitignore_path).expect("read final .gitignore");
    assert!(
        final_content.contains(custom_line),
        "custom line must be preserved after reconstruction"
    );
}

// ---------------------------------------------------------------------------
// mediapm.ncl bootstrap on service construction
// ---------------------------------------------------------------------------

/// Verifies that constructing a fresh `MediaPmService` bootstraps a default
/// `mediapm.ncl` document on disk, and that it loads back successfully.
#[tokio::test]
async fn mediapm_ncl_bootstrapped_on_fresh_construction() {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let service = MediaPmService::new_fs_at(root.path()).await.expect("create service");

    let ncl_path = service.paths().mediapm_ncl.clone();
    assert!(ncl_path.exists(), "mediapm.ncl must exist after fresh construction");

    // The bootstrapped document must load cleanly as a default document.
    let document = load_mediapm_document(&ncl_path).expect("load bootstrapped mediapm.ncl");
    assert!(document.media.is_empty(), "fresh workspace must have no media sources");
}

/// Verifies that reconstructing a service preserves an existing `mediapm.ncl`
/// (user customizations are never overwritten by the bootstrap).
#[tokio::test]
async fn mediapm_ncl_preserved_on_reconstruction() {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let service = MediaPmService::new_fs_at(root.path()).await.expect("create service");

    let ncl_path = service.paths().mediapm_ncl.clone();
    assert!(ncl_path.exists(), "mediapm.ncl must exist after fresh construction");

    // Append a user comment to the bootstrapped document.
    let mut content = std::fs::read_to_string(&ncl_path).expect("read mediapm.ncl");
    content.push_str("\n// user custom comment\n");
    std::fs::write(&ncl_path, &content).expect("write modified mediapm.ncl");

    // Drop the first service to release the CAS directory lock.
    drop(service);

    // Reconstruct the service — the bootstrap must not touch the existing file.
    let _service2 = MediaPmService::new_fs_at(root.path()).await.expect("re-create service");

    let final_content = std::fs::read_to_string(&ncl_path).expect("read final mediapm.ncl");
    assert!(
        final_content.contains("// user custom comment"),
        "user content must be preserved after reconstruction"
    );
}
