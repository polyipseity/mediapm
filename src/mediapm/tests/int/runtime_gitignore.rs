//! # Runtime gitignore creation integration tests
//!
//! Tests for `ensure_mediapm_gitignore` — the mediapm-layer `.gitignore`
//! generation that fires at service construction time.

use mediapm::MediaPmService;
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Gitignore creation on service construction
// ---------------------------------------------------------------------------

/// Verifies that constructing a `MediaPmService` creates a `.gitignore` in
/// the runtime root with the correct default entries.
#[tokio::test]
async fn gitignore_created_on_service_construction() {
    let root = tempdir().expect("tempdir");
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
    let root = tempdir().expect("tempdir");
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
