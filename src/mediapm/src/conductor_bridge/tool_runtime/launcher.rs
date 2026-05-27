//! Binary resolution helpers for the internal media-tagger launcher.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;

pub(super) fn resolve_media_tagger_launcher_binary_path(
    paths: &MediaPmPaths,
) -> Result<PathBuf, MediaPmError> {
    if let Some(candidate) = resolve_workspace_mediapm_binary(&paths.root_dir) {
        return Ok(candidate);
    }

    let current_exe = std::env::current_exe().map_err(|error| {
        MediaPmError::Workflow(format!(
            "failed to resolve current process executable while preparing internal media-tagger launcher env: {error}"
        ))
    })?;

    if executable_file_stem_eq_ignore_ascii_case(&current_exe, "mediapm") {
        return Ok(current_exe);
    }

    if let Some(profile_adjacent_binary) =
        resolve_profile_adjacent_mediapm_binary_for_example(&current_exe)?
    {
        return Ok(profile_adjacent_binary);
    }

    if let Some(from_env) = std::env::var_os("CARGO_BIN_EXE_mediapm") {
        let candidate = PathBuf::from(from_env);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    let binary_name = if cfg!(windows) { "mediapm.exe" } else { "mediapm" };
    for ancestor in current_exe.ancestors().skip(1).take(6) {
        let candidate = ancestor.join(binary_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(MediaPmError::Workflow(format!(
        "failed to resolve mediapm executable path for internal media-tagger launcher; current executable was '{}'",
        current_exe.display()
    )))
}

/// Resolves a profile-adjacent `mediapm` binary when running from examples.
///
/// Returns `Ok(None)` when the current executable is not an example binary or
/// when no workspace root can be inferred for build fallback.
pub(super) fn resolve_profile_adjacent_mediapm_binary_for_example(
    current_exe: &Path,
) -> Result<Option<PathBuf>, MediaPmError> {
    let binary_name = if cfg!(windows) { "mediapm.exe" } else { "mediapm" };

    let examples_dir = current_exe.parent();
    if !examples_dir.is_some_and(|dir| {
        dir.file_name()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|name| name.eq_ignore_ascii_case("examples"))
    }) {
        return Ok(None);
    }

    let Some(profile_dir) = examples_dir.and_then(Path::parent) else {
        return Ok(None);
    };

    if !profile_dir.file_name().and_then(std::ffi::OsStr::to_str).is_some_and(|profile_name| {
        profile_name.eq_ignore_ascii_case("debug") || profile_name.eq_ignore_ascii_case("release")
    }) {
        return Ok(None);
    }

    let candidate = profile_dir.join(binary_name);
    if candidate.is_file() {
        return Ok(Some(candidate));
    }

    let Some(target_dir) = profile_dir.parent() else {
        return Ok(None);
    };
    let Some(workspace_root) = find_workspace_root_for_target_dir(target_dir) else {
        return Ok(None);
    };

    build_workspace_mediapm_binary(&workspace_root, target_dir)?;
    if candidate.is_file() {
        return Ok(Some(candidate));
    }

    Ok(None)
}

/// Finds the nearest ancestor that appears to be a Cargo workspace root.
#[must_use]
pub(super) fn find_workspace_root_for_target_dir(target_dir: &Path) -> Option<PathBuf> {
    target_dir
        .ancestors()
        .find_map(|ancestor| ancestor.join("Cargo.toml").is_file().then(|| ancestor.to_path_buf()))
}

/// Builds `mediapm` binary into one specific target directory.
pub(super) fn build_workspace_mediapm_binary(
    workspace_root: &Path,
    target_dir: &Path,
) -> Result<(), MediaPmError> {
    let output = Command::new("cargo")
        .arg("build")
        .arg("--package")
        .arg("mediapm")
        .arg("--bin")
        .arg("mediapm")
        .arg("--target-dir")
        .arg(target_dir)
        .current_dir(workspace_root)
        .output()
        .map_err(|error| {
            MediaPmError::Workflow(format!(
                "failed to execute cargo build while resolving internal media-tagger launcher binary at '{}': {error}",
                workspace_root.display()
            ))
        })?;

    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(MediaPmError::Workflow(format!(
        "failed to build mediapm binary for internal media-tagger launcher (workspace='{}', target_dir='{}', status={}):\nstdout:\n{}\nstderr:\n{}",
        workspace_root.display(),
        target_dir.display(),
        output.status,
        stdout.trim(),
        stderr.trim(),
    )))
}

/// Resolves a workspace-local `target/<profile>/mediapm` executable path.
#[must_use]
pub(super) fn resolve_workspace_mediapm_binary(root_dir: &Path) -> Option<PathBuf> {
    let binary_name = if cfg!(windows) { "mediapm.exe" } else { "mediapm" };

    for profile in ["debug", "release"] {
        let candidate = root_dir.join("target").join(profile).join(binary_name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    None
}

/// Returns true when executable filename stem matches expected text.
pub(super) fn executable_file_stem_eq_ignore_ascii_case(path: &Path, expected_stem: &str) -> bool {
    path.file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|stem| stem.eq_ignore_ascii_case(expected_stem))
}
