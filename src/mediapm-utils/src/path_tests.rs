//! Tests for shared path utilities.
//!
//! These tests exercise [`crate::path::resolve_path_for_root`] and
//! [`crate::path::parse_path_mode`] directly.  Equivalent tests were removed
//! from the export, fs, and import builtin crates where they tested the same
//! underlying functions indirectly through builtin-specific APIs.

use tempfile::tempdir;

use crate::StringMap;
use crate::path::{PathMode, parse_path_mode, resolve_path_for_root};

/// Verifies relative mode rejects absolute path values.
#[test]
fn relative_mode_rejects_absolute_path() {
    let temp = tempdir().expect("tempdir");
    let absolute_path = temp.path().join("out.bin");
    let mode =
        parse_path_mode(&StringMap::new(), "test").expect("default path_mode should succeed");
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
