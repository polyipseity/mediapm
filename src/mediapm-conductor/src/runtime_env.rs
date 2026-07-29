//! Runtime `.env` management for conductor execution contexts.
//!
//! Conductor supports loading zero or more dotenv files in specified order.
//! The default convention uses two colocated files under the resolved
//! `runtime_storage_paths.conductor_dir` root:
//! - `.env`: user-authored environment variables,
//! - `.env.generated`: machine-generated runtime variables.
//!
//! Loaded variable names are returned so callers can inherit them into
//! executable tool environments.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::ConductorError;

/// User-authored runtime dotenv file name under one conductor directory root.
pub const RUNTIME_DOTENV_FILE_NAME: &str = ".env";
/// Machine-generated runtime dotenv file name under one conductor directory root.
pub const RUNTIME_DOTENV_GENERATED_FILE_NAME: &str = ".env.generated";

/// Canonical generated `.env` template for conductor runtime roots.
const RUNTIME_DOTENV_TEMPLATE: &str = concat!(
    "# conductor runtime environment variables\n",
    "#\n",
    "# User-authored values go here. This file is loaded automatically by\n",
    "# conductor before state/workflow operations.\n",
    "#\n",
    "# Core mediapm/conductor runtime knobs:\n",
    "# MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS=120\n",
    "# MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS=300\n",
    "# MEDIAPM_HTTP_TIMEOUT_SECONDS=180\n",
    "#\n",
    "# Media metadata enrichment (AcoustID -> MusicBrainz):\n",
    "# ACOUSTID_API_KEY=replace-me\n",
    "#\n",
    "# Optional demo/example timeout override:\n",
    "# MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS=180\n",
    "#\n",
    "# Optional manual override for internal media-tagger ffmpeg path\n",
    "# (normally generated automatically into .env.generated):\n",
    "# MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN=\n",
    "#\n",
    "# Optional manual override for yt-dlp companion paths\n",
    "# (normally generated automatically into .env.generated):\n",
    "# MEDIAPM_YT_DLP_FFMPEG_LOCATION=\n",
    "# MEDIAPM_YT_DLP_JS_RUNTIMES=\n",
    "#\n",
    "# Optional proxy passthrough for downloader tools:\n",
    "# HTTP_PROXY=http://127.0.0.1:7890\n",
    "# HTTPS_PROXY=http://127.0.0.1:7890\n",
    "# NO_PROXY=localhost,127.0.0.1\n",
);

/// Canonical generated `.env.generated` template for conductor runtime roots.
const RUNTIME_DOTENV_GENERATED_TEMPLATE: &str = concat!(
    "# conductor generated runtime variables\n",
    "#\n",
    "# This file is managed by tooling. Manual edits may be overwritten.\n",
);

/// Returns the canonical `.env` path for one conductor runtime root.
#[must_use]
pub fn runtime_dotenv_path(conductor_dir: &Path) -> PathBuf {
    conductor_dir.join(RUNTIME_DOTENV_FILE_NAME)
}

/// Returns the canonical `.env.generated` path for one conductor runtime root.
#[must_use]
pub fn runtime_generated_dotenv_path(conductor_dir: &Path) -> PathBuf {
    conductor_dir.join(RUNTIME_DOTENV_GENERATED_FILE_NAME)
}

/// Ensures runtime dotenv template files exist.
///
/// # Errors
///
/// Returns [`ConductorError`] when directory creation or file writes fail.
pub fn ensure_runtime_env_files(conductor_dir: &Path) -> Result<(), ConductorError> {
    fs::create_dir_all(conductor_dir).map_err(|source| ConductorError::Io {
        operation: "creating conductor runtime environment directory".to_string(),
        path: conductor_dir.to_path_buf(),
        source,
    })?;

    let dotenv_path = runtime_dotenv_path(conductor_dir);
    if !dotenv_path.exists() {
        fs::write(&dotenv_path, RUNTIME_DOTENV_TEMPLATE.as_bytes()).map_err(|source| {
            ConductorError::Io {
                operation: "writing conductor runtime .env template".to_string(),
                path: dotenv_path.clone(),
                source,
            }
        })?;
    }

    let generated_dotenv_path = runtime_generated_dotenv_path(conductor_dir);
    if !generated_dotenv_path.exists() {
        fs::write(&generated_dotenv_path, RUNTIME_DOTENV_GENERATED_TEMPLATE.as_bytes()).map_err(
            |source| ConductorError::Io {
                operation: "writing conductor runtime .env.generated template".to_string(),
                path: generated_dotenv_path.clone(),
                source,
            },
        )?;
    }

    Ok(())
}

/// Loads conductor runtime dotenv files in specified order.
///
/// Each file name is resolved relative to `conductor_dir`. Later files
/// override earlier values. An empty list loads nothing (returns empty names).
/// Returned names preserve declaration order with case-insensitive
/// de-duplication.
///
/// # Errors
///
/// Returns [`ConductorError`] when any dotenv file cannot be read or parsed.
pub fn load_runtime_env_files(
    conductor_dir: &Path,
    file_names: &[&str],
) -> Result<Vec<String>, ConductorError> {
    let mut inherited_names = Vec::new();

    for file_name in file_names {
        let path = conductor_dir.join(file_name);
        let names = read_dotenv_variable_names(&path)?;
        for name in &names {
            append_unique_env_var_name(&mut inherited_names, name);
        }

        if path.exists() {
            dotenvy::from_path_override(&path).map_err(|source| {
                ConductorError::Workflow(format!(
                    "loading conductor runtime dotenv file '{}' failed: {source}",
                    path.display()
                ))
            })?;
        }
    }

    Ok(inherited_names)
}

/// Returns the default env file names in load order.
#[must_use]
pub fn default_runtime_env_file_names() -> &'static [&'static str] {
    &[RUNTIME_DOTENV_FILE_NAME, RUNTIME_DOTENV_GENERATED_FILE_NAME]
}

/// Reads dotenv variable names from one file without mutating process env.
fn read_dotenv_variable_names(path: &Path) -> Result<Vec<String>, ConductorError> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut names = Vec::new();
    let iter = dotenvy::from_path_iter(path).map_err(|source| {
        ConductorError::Workflow(format!(
            "parsing conductor runtime dotenv file '{}' failed: {source}",
            path.display()
        ))
    })?;

    for entry in iter {
        let (name, _value) = entry.map_err(|source| {
            ConductorError::Workflow(format!(
                "parsing conductor runtime dotenv assignment in '{}' failed: {source}",
                path.display()
            ))
        })?;
        append_unique_env_var_name(&mut names, &name);
    }

    Ok(names)
}

/// Appends one env-var name with trimming and case-insensitive de-duplication.
fn append_unique_env_var_name(target: &mut Vec<String>, raw_name: &str) {
    let trimmed = raw_name.trim();
    if trimmed.is_empty() {
        return;
    }

    if target.iter().any(|existing| existing.eq_ignore_ascii_case(trimmed)) {
        return;
    }

    target.push(trimmed.to_string());
}

/// Canonical conductor `.gitignore` content for the runtime root.
///
/// Created at startup to keep generated dotenv files out of version control.
pub const CONDUCTOR_GITIGNORE_CONTENT: &str = concat!("/.env\n", "/.env.generated\n");

/// Canonical `# @generated` header for `.env.generated` files.
///
/// Used by `write_generated_dotenv()` when overwriting the generated env file.
pub const RUNTIME_DOTENV_GENERATED_HEADER: &str = concat!(
    "# @generated\n",
    "# Managed runtime environment variables.\n",
    "# Do not edit manually; values may be rewritten during sync.\n\n",
);

/// Ensures a `.gitignore` exists in the conductor runtime root.
///
/// Creates the file with [`CONDUCTOR_GITIGNORE_CONTENT`] if it does not
/// already exist. Existing files are never overwritten.
///
/// # Errors
///
/// Returns [`ConductorError`] when directory creation or file writes fail.
pub fn ensure_runtime_gitignore(conductor_dir: &Path) -> Result<(), ConductorError> {
    let gitignore_path = conductor_dir.join(".gitignore");
    if gitignore_path.exists() {
        return Ok(());
    }
    fs::write(&gitignore_path, CONDUCTOR_GITIGNORE_CONTENT.as_bytes()).map_err(|source| {
        ConductorError::Io {
            operation: "writing conductor runtime .gitignore".to_string(),
            path: gitignore_path,
            source,
        }
    })?;
    Ok(())
}

/// Appends extra entries to the conductor runtime root `.gitignore`.
///
/// Skips entries already present in the file (simple string-contains dedup).
/// The `extra` string is appended as-is if not already contained.
///
/// # Errors
///
/// Returns [`ConductorError`] when file reads or writes fail.
pub fn extend_runtime_gitignore(conductor_dir: &Path, extra: &str) -> Result<(), ConductorError> {
    let gitignore_path = conductor_dir.join(".gitignore");
    // Ensure the base file exists first.
    ensure_runtime_gitignore(conductor_dir)?;
    let content = fs::read_to_string(&gitignore_path).map_err(|source| ConductorError::Io {
        operation: "reading conductor runtime .gitignore".to_string(),
        path: gitignore_path.clone(),
        source,
    })?;
    if content.contains(extra) {
        return Ok(());
    }
    let new_content = format!("{content}{extra}");
    fs::write(&gitignore_path, new_content.as_bytes()).map_err(|source| ConductorError::Io {
        operation: "extending conductor runtime .gitignore".to_string(),
        path: gitignore_path,
        source,
    })?;
    Ok(())
}

/// Discovers the project root by walking up from the current working directory.
///
/// Looks for a `conductor.ncl` (or `mediapm.ncl`) file to determine the
/// project boundary. Falls back to the current working directory when no
/// marker is found.
///
/// # Errors
///
/// Returns [`ConductorError`] when the current working directory cannot be
/// resolved.
pub fn discover_project_root() -> Result<PathBuf, ConductorError> {
    let cwd = std::env::current_dir().map_err(|source| ConductorError::Io {
        operation: "resolving current working directory".to_string(),
        path: PathBuf::from("."),
        source,
    })?;

    let mut current = Some(cwd.as_path());
    while let Some(dir) = current {
        if dir.join("conductor.ncl").exists() || dir.join("mediapm.ncl").exists() {
            return Ok(dir.to_path_buf());
        }
        current = dir.parent();
    }

    // Fallback: return the original cwd
    Ok(cwd)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn ensure_runtime_gitignore_creates_file() {
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");

        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");
        let gitignore_path = conductor_dir.join(".gitignore");
        assert!(gitignore_path.exists(), ".gitignore should exist");
        let content = fs::read_to_string(&gitignore_path).expect("read .gitignore");
        assert!(content.contains("/.env"), ".gitignore should contain /.env\ncontent:\n{content}");
        assert!(
            content.contains("/.env.generated"),
            ".gitignore should contain /.env.generated\ncontent:\n{content}"
        );
    }

    #[test]
    fn ensure_runtime_gitignore_no_overwrite() {
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");
        let gitignore_path = conductor_dir.join(".gitignore");
        fs::write(&gitignore_path, "# custom\n").expect("write custom .gitignore");

        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");
        let content = fs::read_to_string(&gitignore_path).expect("read .gitignore");
        assert_eq!(content, "# custom\n", "custom .gitignore should be preserved");
    }

    #[test]
    fn extend_runtime_gitignore_appends() {
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");

        // First create the base gitignore.
        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");
        // Then extend with extra entries.
        extend_runtime_gitignore(&conductor_dir, "/cache/\n/tools/\n")
            .expect("extend_runtime_gitignore");

        let content =
            fs::read_to_string(&conductor_dir.join(".gitignore")).expect("read .gitignore");
        assert!(
            content.contains("/cache/"),
            ".gitignore should contain /cache/\ncontent:\n{content}"
        );
        assert!(
            content.contains("/tools/"),
            ".gitignore should contain /tools/\ncontent:\n{content}"
        );
        assert!(
            content.contains("/.env"),
            ".gitignore should still contain original entries\ncontent:\n{content}"
        );
    }

    #[test]
    fn extend_runtime_gitignore_no_duplicate() {
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");

        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");
        extend_runtime_gitignore(&conductor_dir, "/cache/\n/tools/\n")
            .expect("extend_runtime_gitignore first");
        extend_runtime_gitignore(&conductor_dir, "/cache/\n/tools/\n")
            .expect("extend_runtime_gitignore second");

        let content =
            fs::read_to_string(&conductor_dir.join(".gitignore")).expect("read .gitignore");
        // /cache/ and /tools/ should appear exactly once each.
        let cache_count = content.matches("/cache/").count();
        let tools_count = content.matches("/tools/").count();
        assert_eq!(
            cache_count, 1,
            "/cache/ should appear exactly once, got {cache_count}\ncontent:\n{content}"
        );
        assert_eq!(
            tools_count, 1,
            "/tools/ should appear exactly once, got {tools_count}\ncontent:\n{content}"
        );
    }

    #[test]
    fn ensure_runtime_gitignore_wired_in_cli() {
        // This test validates that the CLI startup path creates the .gitignore.
        // We simulate the startup by calling the same functions ensure_conductor calls.
        let dir = tempdir().expect("tempdir");
        let conductor_dir = dir.path().join(".conductor");
        fs::create_dir_all(&conductor_dir).expect("create conductor dir");

        // Call the same sequence as ensure_conductor.
        ensure_runtime_env_files(&conductor_dir).expect("ensure_runtime_env_files");
        ensure_runtime_gitignore(&conductor_dir).expect("ensure_runtime_gitignore");

        let gitignore_path = conductor_dir.join(".gitignore");
        assert!(gitignore_path.exists(), ".gitignore should exist after startup sequence");
        let content = fs::read_to_string(&gitignore_path).expect("read .gitignore");
        assert!(content.contains("/.env"), ".gitignore should contain /.env\ncontent:\n{content}");
        assert!(
            content.contains("/.env.generated"),
            ".gitignore should contain /.env.generated\ncontent:\n{content}"
        );

        // Ensure the .env files exist too.
        assert!(
            conductor_dir.join(".env").exists(),
            ".env should exist after ensure_runtime_env_files"
        );
        assert!(
            conductor_dir.join(".env.generated").exists(),
            ".env.generated should exist after ensure_runtime_env_files"
        );
    }

    #[test]
    fn runtime_dotenv_generated_header_is_valid() {
        let header = RUNTIME_DOTENV_GENERATED_HEADER;
        assert!(
            header.starts_with("# @generated"),
            "header should start with # @generated\ngot:\n{header}"
        );
        assert!(!header.is_empty(), "header should not be empty");
    }
}
