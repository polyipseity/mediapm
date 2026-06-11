//! Tests for shared path utilities.
//!
//! These tests exercise [`crate::path::resolve_path_for_root`],
//! [`crate::path::parse_path_mode`], [`crate::path::absolute_root`], and
//! [`crate::path::normalize_relative_path`] directly.  Equivalent tests were
//! removed from the export, fs, and import builtin crates where they tested
//! the same underlying functions indirectly through builtin-specific APIs.

use tempfile::tempdir;

use crate::StringMap;
use crate::path::{
    PathMode, absolute_root, normalize_relative_path, parse_path_mode, resolve_path_for_root,
};

// ---------------------------------------------------------------------------
// parse_path_mode tests
// ---------------------------------------------------------------------------

/// Verifies default (no key) resolves to Relative.
#[test]
fn parse_path_mode_defaults_to_relative() {
    let mode = parse_path_mode(&StringMap::new(), "test").expect("should succeed");
    assert_eq!(mode, PathMode::Relative);
}

/// Verifies explicit "relative" resolves correctly.
#[test]
fn parse_path_mode_explicit_relative() {
    let params = StringMap::from([("path_mode".to_string(), "relative".to_string())]);
    let mode = parse_path_mode(&params, "test").expect("should succeed");
    assert_eq!(mode, PathMode::Relative);
}

/// Verifies explicit "absolute" resolves correctly.
#[test]
fn parse_path_mode_explicit_absolute() {
    let params = StringMap::from([("path_mode".to_string(), "absolute".to_string())]);
    let mode = parse_path_mode(&params, "test").expect("should succeed");
    assert_eq!(mode, PathMode::Absolute);
}

/// Verifies invalid path_mode values are rejected.
#[test]
fn parse_path_mode_rejects_invalid() {
    let params = StringMap::from([("path_mode".to_string(), "hybrid".to_string())]);
    let err = parse_path_mode(&params, "test").expect_err("should reject invalid");
    assert!(err.contains("hybrid"), "error should mention invalid value: {err}");
}

/// Verifies error includes context.
#[test]
fn parse_path_mode_includes_context() {
    let params = StringMap::from([("path_mode".to_string(), "bad".to_string())]);
    let err = parse_path_mode(&params, "my_ctx").expect_err("should fail");
    assert!(err.contains("my_ctx"), "error should include context: {err}");
}

// ---------------------------------------------------------------------------
// normalize_relative_path tests
// ---------------------------------------------------------------------------

/// Verifies normal relative path is accepted.
#[test]
fn normalize_relative_path_accepts_simple() {
    let result = normalize_relative_path("foo/bar/baz.txt", "test").expect("should succeed");
    assert_eq!(result.to_string_lossy(), "foo/bar/baz.txt");
}

/// Verifies leading `./` is stripped.
#[test]
fn normalize_relative_path_strips_curdir() {
    let result = normalize_relative_path("./foo/bar", "test").expect("should succeed");
    assert_eq!(result.to_string_lossy(), "foo/bar");
}

/// Verifies `./` in the middle is stripped.
#[test]
fn normalize_relative_path_strips_mid_curdir() {
    let result = normalize_relative_path("foo/./bar", "test").expect("should succeed");
    assert_eq!(result.to_string_lossy(), "foo/bar");
}

/// Verifies empty path is rejected.
#[test]
fn normalize_relative_path_rejects_empty() {
    let err = normalize_relative_path("", "test").expect_err("should reject empty");
    assert!(err.contains("non-empty"), "error: {err}");
}

/// Verifies whitespace-only path is rejected.
#[test]
fn normalize_relative_path_rejects_whitespace() {
    let err = normalize_relative_path("   ", "test").expect_err("should reject whitespace");
    assert!(err.contains("non-empty"), "error: {err}");
}

/// Verifies absolute path is rejected.
#[test]
fn normalize_relative_path_rejects_absolute() {
    let err = normalize_relative_path("/foo/bar", "test").expect_err("should reject absolute");
    assert!(err.contains("must be relative"), "error: {err}");
}

/// Verifies parent traversal is rejected.
#[test]
fn normalize_relative_path_rejects_parent() {
    let err =
        normalize_relative_path("../foo", "test").expect_err("should reject parent traversal");
    assert!(err.contains("under root directory"), "error: {err}");
}

/// Verifies double-parent traversal is rejected.
#[test]
fn normalize_relative_path_rejects_double_parent() {
    let err = normalize_relative_path("foo/../../bar", "test")
        .expect_err("should reject double parent traversal");
    assert!(err.contains("under root directory"), "error: {err}");
}

/// Verifies mixed curdir and normal components.
#[test]
fn normalize_relative_path_mixed() {
    let result = normalize_relative_path("./a/./b/./c.txt", "test").expect("should succeed");
    assert_eq!(result.to_string_lossy(), "a/b/c.txt");
}

// ---------------------------------------------------------------------------
// absolute_root tests
// ---------------------------------------------------------------------------

/// Verifies an already-absolute root is returned as-is.
#[test]
fn absolute_root_keeps_absolute() {
    let root = std::path::Path::new("/tmp");
    let result = absolute_root(root, "test").expect("should succeed");
    assert_eq!(result, root);
}

/// Verifies a relative root is joined with cwd.
#[test]
fn absolute_root_resolves_relative() {
    let root = std::path::Path::new(".");
    let result = absolute_root(root, "test").expect("should succeed");
    assert!(result.is_absolute(), "resolved root should be absolute: {result:?}");
}

// ---------------------------------------------------------------------------
// resolve_path_for_root tests
// ---------------------------------------------------------------------------

/// Verifies relative mode rejects absolute path values.
#[test]
fn relative_mode_rejects_absolute_path() {
    let temp = tempdir().expect("tempdir");
    let absolute_path = temp.path().join("out.bin");
    let mode = parse_path_mode(&StringMap::new(), "test").expect("default path_mode");
    assert_eq!(mode, PathMode::Relative);

    let err = resolve_path_for_root(
        temp.path(),
        "test context",
        "path",
        &absolute_path.to_string_lossy(),
        mode,
    )
    .expect_err("relative mode should reject absolute path");
    assert!(err.contains("path_mode='relative'"));
}

/// Verifies relative mode rejects escaping parent traversal.
#[test]
fn relative_mode_rejects_parent_escape() {
    let temp = tempdir().expect("tempdir");
    let mode =
        parse_path_mode(&StringMap::new(), "test").expect("default path_mode should succeed");

    let err = resolve_path_for_root(temp.path(), "test context", "path", "../escape.txt", mode)
        .expect_err("relative mode should reject parent escape");
    assert!(err.contains("must stay under root directory"));
}

/// Verifies absolute mode accepts explicit absolute source paths.
#[test]
fn absolute_mode_accepts_absolute_path() {
    let temp = tempdir().expect("tempdir");
    let absolute_path = temp.path().join("abs").join("payload.bin");

    let resolved = resolve_path_for_root(
        temp.path(),
        "test context",
        "path",
        &absolute_path.to_string_lossy(),
        PathMode::Absolute,
    )
    .expect("absolute mode should accept absolute path");
    assert_eq!(resolved, absolute_path);
}

/// Verifies absolute mode rejects relative paths.
#[test]
fn absolute_mode_rejects_relative_path() {
    let temp = tempdir().expect("tempdir");
    let err = resolve_path_for_root(
        temp.path(),
        "test context",
        "path",
        "relative.txt",
        PathMode::Absolute,
    )
    .expect_err("absolute mode should reject relative path");
    assert!(err.contains("path_mode='absolute'"));
}

/// Verifies relative mode resolves a normal relative path under root.
#[test]
fn relative_mode_resolves_normal_path() {
    let temp = tempdir().expect("tempdir");
    let resolved =
        resolve_path_for_root(temp.path(), "test", "path", "foo/bar.txt", PathMode::Relative)
            .expect("should resolve");
    assert_eq!(resolved, temp.path().join("foo/bar.txt"));
}

/// Verifies relative mode strips `./` prefix.
#[test]
fn relative_mode_strips_curdir() {
    let temp = tempdir().expect("tempdir");
    let resolved =
        resolve_path_for_root(temp.path(), "test", "path", "./foo/bar.txt", PathMode::Relative)
            .expect("should resolve");
    assert_eq!(resolved, temp.path().join("foo/bar.txt"));
}
