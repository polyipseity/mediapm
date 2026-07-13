//! Integration tests for `ProvisionCache::link_to_sandbox` platform filtering.
//!
//! These tests verify two things:
//! 1. The cfg-derived `FOREIGN_PLATFORM_DIRS` constant correctly filters out
//!    non-native platform directories during sandbox linking.
//! 2. The explicit `link_to_sandbox_filtered` API works with caller-specified
//!    foreign dirs.

use mediapm_conductor::provision::{link_to_sandbox, link_to_sandbox_filtered};
use tempfile::tempdir;

/// Helper: creates a payload directory with subdirectories for all three
/// platforms, each containing a marker file.
fn payload_with_all_platforms() -> (tempfile::TempDir, tempfile::TempDir) {
    let payload = tempdir().expect("tempdir for payload");
    let sandbox = tempdir().expect("tempdir for sandbox");

    for os in &["linux", "macos", "windows"] {
        let dir = payload.path().join(os);
        std::fs::create_dir_all(&dir).expect("create platform dir");
        std::fs::write(dir.join("tool"), "content").expect("write marker file");
    }

    (payload, sandbox)
}

/// Verifies that `link_to_sandbox` (which uses `FOREIGN_PLATFORM_DIRS`)
/// preserves only the native OS directory on the current platform.
#[test]
fn link_to_sandbox_preserves_native_platform_only() {
    let (payload, sandbox) = payload_with_all_platforms();

    link_to_sandbox(payload.path(), sandbox.path()).expect("link_to_sandbox should succeed");

    // The current OS's directory is always present; non-native dirs are
    // filtered out by the cfg-dependent FOREIGN_PLATFORM_DIRS constant.
    #[cfg(target_os = "macos")]
    {
        assert!(sandbox.path().join("macos").exists(), "macos absent on macOS");
        assert!(!sandbox.path().join("linux").exists(), "linux present on macOS");
        assert!(!sandbox.path().join("windows").exists(), "windows present on macOS");
    }
    #[cfg(target_os = "linux")]
    {
        assert!(sandbox.path().join("linux").exists(), "linux absent on Linux");
        assert!(!sandbox.path().join("macos").exists(), "macos present on Linux");
        assert!(!sandbox.path().join("windows").exists(), "windows present on Linux");
    }
    #[cfg(target_os = "windows")]
    {
        assert!(sandbox.path().join("windows").exists(), "windows absent on Windows");
        assert!(!sandbox.path().join("linux").exists(), "linux present on Windows");
        assert!(!sandbox.path().join("macos").exists(), "macos present on Windows");
    }
}

/// Verifies that `link_to_sandbox_filtered` with an explicit filter produces
/// the expected result regardless of the host platform.
#[test]
fn link_to_sandbox_explicit_filter_works() {
    let payload = tempdir().expect("tempdir for payload");
    let sandbox = tempdir().expect("tempdir for sandbox");

    for os in &["linux", "windows"] {
        let dir = payload.path().join(os);
        std::fs::create_dir_all(&dir).expect("create platform dir");
        std::fs::write(dir.join("tool"), "content").expect("write marker file");
    }

    // Exclude "linux" explicitly — it should not appear in the sandbox.
    link_to_sandbox_filtered(payload.path(), sandbox.path(), &["linux"])
        .expect("link_to_sandbox_filtered should succeed");

    assert!(!sandbox.path().join("linux").exists(), "linux should be filtered out");
    assert!(sandbox.path().join("windows").exists(), "windows should be present");
}

/// Verifies that an empty payload directory (no platform dirs) produces an
/// empty sandbox without errors.
#[test]
fn link_to_sandbox_empty_payload_creates_empty_sandbox() {
    let payload = tempdir().expect("tempdir for payload");
    let sandbox = tempdir().expect("tempdir for sandbox");

    link_to_sandbox(payload.path(), sandbox.path())
        .expect("link_to_sandbox on empty payload should succeed");

    assert!(sandbox.path().exists(), "sandbox dir should exist");
    assert!(
        sandbox.path().read_dir().expect("read sandbox").next().is_none(),
        "sandbox should be empty",
    );
}

/// Verifies that `link_to_sandbox` returns an error when the payload
/// directory does not exist.
#[test]
fn link_to_sandbox_nonexistent_payload_errors() {
    let payload = tempdir().expect("tempdir for payload");
    let sandbox = tempdir().expect("tempdir for sandbox");
    let nonexistent = payload.path().join("does-not-exist");

    let result = link_to_sandbox(&nonexistent, sandbox.path());

    assert!(result.is_err(), "link_to_sandbox should error on nonexistent payload");
    assert!(result.unwrap_err().contains("does not exist"));
}
