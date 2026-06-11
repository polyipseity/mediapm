//! Shared path-resolution utilities for builtin crates that accept file paths.

use std::path::{Component, Path, PathBuf};

use crate::StringMap;

/// Path-resolution mode for builtin path arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathMode {
    /// Resolve paths under the configured root directory.
    Relative,
    /// Treat paths as explicit absolute host paths.
    Absolute,
}

/// Parses and validates path-mode selector from string-map params.
///
/// Defaults to `Relative` when no `path_mode` key is present.
///
/// # Errors
///
/// Returns an error when `path_mode` is not `relative` or `absolute`.
pub fn parse_path_mode(params: &StringMap, context: &str) -> Result<PathMode, String> {
    match params.get("path_mode").map_or("relative", String::as_str) {
        "relative" => Ok(PathMode::Relative),
        "absolute" => Ok(PathMode::Absolute),
        other => {
            Err(format!("{context} path_mode must be 'relative' or 'absolute', got '{other}'"))
        }
    }
}

/// Resolves one path against root + path-mode semantics.
///
/// In relative mode, the candidate is normalized (parent-dir traversal
/// rejected) and joined with the absolute root. In absolute mode, the
/// candidate must already be absolute.
///
/// # Errors
///
/// Returns an error when path-mode validation fails, path resolution fails,
/// or a relative path escapes the root directory.
pub fn resolve_path_for_root(
    root_dir: &Path,
    context: &str,
    field: &str,
    candidate: &str,
    mode: PathMode,
) -> Result<PathBuf, String> {
    match mode {
        PathMode::Relative => {
            if Path::new(candidate).is_absolute() {
                return Err(format!(
                    "{context} with path_mode='relative' requires relative '{field}'"
                ));
            }
            let root = absolute_root(root_dir, context)?;
            let normalized = normalize_relative_path(candidate, context)?;
            Ok(root.join(normalized))
        }
        PathMode::Absolute => {
            let parsed = Path::new(candidate);
            if !parsed.is_absolute() {
                return Err(format!(
                    "{context} with path_mode='absolute' requires absolute '{field}'"
                ));
            }
            Ok(parsed.to_path_buf())
        }
    }
}

/// Resolves one root directory into an absolute filesystem path.
///
/// Returns the root as-is if it is already absolute; otherwise joins it with
/// the process current working directory.
///
/// # Errors
///
/// Returns an error when the current directory cannot be determined.
pub fn absolute_root(root: &Path, context: &str) -> Result<PathBuf, String> {
    if root.is_absolute() {
        return Ok(root.to_path_buf());
    }

    std::env::current_dir()
        .map(|cwd| cwd.join(root))
        .map_err(|err| format!("resolving current directory for {context} root failed: {err}"))
}

/// Normalizes one relative path and rejects escaping components.
///
/// Strips `.` components and rejects `..`, absolute root, and prefix
/// components. The result is always a non-empty relative path.
///
/// # Errors
///
/// Returns an error when the path is empty, absolute, or contains parent-dir
/// traversal or other escaping components.
pub fn normalize_relative_path(candidate: &str, context: &str) -> Result<PathBuf, String> {
    if candidate.trim().is_empty() {
        return Err(format!("{context} must be non-empty"));
    }

    let parsed = Path::new(candidate);
    if parsed.is_absolute() {
        return Err(format!("{context} must be relative"));
    }

    let mut normalized = PathBuf::new();
    for component in parsed.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!("{context} must stay under root directory"));
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err(format!("{context} must contain at least one path component"));
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::{
        PathMode, absolute_root, normalize_relative_path, parse_path_mode, resolve_path_for_root,
    };
    use crate::StringMap;
    use tempfile::tempdir;

    // -----------------------------------------------------------------------
    // parse_path_mode tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_path_mode_defaults_to_relative() {
        let mode = parse_path_mode(&StringMap::new(), "test").expect("should succeed");
        assert_eq!(mode, PathMode::Relative);
    }

    #[test]
    fn parse_path_mode_explicit_relative() {
        let params = StringMap::from([("path_mode".to_string(), "relative".to_string())]);
        let mode = parse_path_mode(&params, "test").expect("should succeed");
        assert_eq!(mode, PathMode::Relative);
    }

    #[test]
    fn parse_path_mode_explicit_absolute() {
        let params = StringMap::from([("path_mode".to_string(), "absolute".to_string())]);
        let mode = parse_path_mode(&params, "test").expect("should succeed");
        assert_eq!(mode, PathMode::Absolute);
    }

    #[test]
    fn parse_path_mode_rejects_invalid() {
        let params = StringMap::from([("path_mode".to_string(), "hybrid".to_string())]);
        let err = parse_path_mode(&params, "test").expect_err("should reject invalid");
        assert!(err.contains("hybrid"), "error should mention invalid value: {err}");
    }

    #[test]
    fn parse_path_mode_includes_context() {
        let params = StringMap::from([("path_mode".to_string(), "bad".to_string())]);
        let err = parse_path_mode(&params, "my_ctx").expect_err("should fail");
        assert!(err.contains("my_ctx"), "error should include context: {err}");
    }

    // -----------------------------------------------------------------------
    // normalize_relative_path tests
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_relative_path_accepts_simple() {
        let result = normalize_relative_path("foo/bar/baz.txt", "test").expect("should succeed");
        assert_eq!(result.to_string_lossy(), "foo/bar/baz.txt");
    }

    #[test]
    fn normalize_relative_path_strips_curdir() {
        let result = normalize_relative_path("./foo/bar", "test").expect("should succeed");
        assert_eq!(result.to_string_lossy(), "foo/bar");
    }

    #[test]
    fn normalize_relative_path_strips_mid_curdir() {
        let result = normalize_relative_path("foo/./bar", "test").expect("should succeed");
        assert_eq!(result.to_string_lossy(), "foo/bar");
    }

    #[test]
    fn normalize_relative_path_rejects_empty() {
        let err = normalize_relative_path("", "test").expect_err("should reject empty");
        assert!(err.contains("non-empty"), "error: {err}");
    }

    #[test]
    fn normalize_relative_path_rejects_whitespace() {
        let err = normalize_relative_path("   ", "test").expect_err("should reject whitespace");
        assert!(err.contains("non-empty"), "error: {err}");
    }

    #[test]
    fn normalize_relative_path_rejects_absolute() {
        let err = normalize_relative_path("/foo/bar", "test").expect_err("should reject absolute");
        assert!(err.contains("must be relative"), "error: {err}");
    }

    #[test]
    fn normalize_relative_path_rejects_parent() {
        let err =
            normalize_relative_path("../foo", "test").expect_err("should reject parent traversal");
        assert!(err.contains("under root directory"), "error: {err}");
    }

    #[test]
    fn normalize_relative_path_rejects_double_parent() {
        let err = normalize_relative_path("foo/../../bar", "test")
            .expect_err("should reject double parent traversal");
        assert!(err.contains("under root directory"), "error: {err}");
    }

    #[test]
    fn normalize_relative_path_mixed() {
        let result = normalize_relative_path("./a/./b/./c.txt", "test").expect("should succeed");
        assert_eq!(result.to_string_lossy(), "a/b/c.txt");
    }

    // -----------------------------------------------------------------------
    // absolute_root tests
    // -----------------------------------------------------------------------

    #[test]
    fn absolute_root_keeps_absolute() {
        let root = std::path::Path::new("/tmp");
        let result = absolute_root(root, "test").expect("should succeed");
        assert_eq!(result, root);
    }

    #[test]
    fn absolute_root_resolves_relative() {
        let root = std::path::Path::new(".");
        let result = absolute_root(root, "test").expect("should succeed");
        assert!(result.is_absolute(), "resolved root should be absolute: {result:?}");
    }

    // -----------------------------------------------------------------------
    // resolve_path_for_root tests
    // -----------------------------------------------------------------------

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

    #[test]
    fn relative_mode_rejects_parent_escape() {
        let temp = tempdir().expect("tempdir");
        let mode =
            parse_path_mode(&StringMap::new(), "test").expect("default path_mode should succeed");

        let err = resolve_path_for_root(temp.path(), "test context", "path", "../escape.txt", mode)
            .expect_err("relative mode should reject parent escape");
        assert!(err.contains("must stay under root directory"));
    }

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

    #[test]
    fn relative_mode_resolves_normal_path() {
        let temp = tempdir().expect("tempdir");
        let resolved =
            resolve_path_for_root(temp.path(), "test", "path", "foo/bar.txt", PathMode::Relative)
                .expect("should resolve");
        assert_eq!(resolved, temp.path().join("foo/bar.txt"));
    }

    #[test]
    fn relative_mode_strips_curdir() {
        let temp = tempdir().expect("tempdir");
        let resolved =
            resolve_path_for_root(temp.path(), "test", "path", "./foo/bar.txt", PathMode::Relative)
                .expect("should resolve");
        assert_eq!(resolved, temp.path().join("foo/bar.txt"));
    }
}
