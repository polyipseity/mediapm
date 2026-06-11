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
