//! Runtime `.env` management for conductor execution contexts.
//!
//! Conductor uses two colocated dotenv files under the resolved
//! `runtime_storage_paths.conductor_dir` root:
//! - `.env`: user-authored environment variables,
//! - `.env.generated`: machine-generated runtime variables.
//!
//! Both files are loaded before workflow/state commands execute, and all loaded
//! variable names are returned so callers can inherit them into executable tool
//! environments.

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
    "# MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS=180\n",
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

/// Canonical colocated `.gitignore` content for conductor runtime dotenv files.
const RUNTIME_DOTENV_GITIGNORE: &str =
    concat!("/.env\n", "/.env.generated\n", "/cache/\n", "/store/index-backups/\n", "/tools/\n");

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

/// Ensures runtime dotenv files and colocated `.gitignore` exist.
///
/// # Errors
///
/// Returns [`ConductorError`] when directory creation, file reads, or writes
/// fail.
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

    ensure_runtime_gitignore(&conductor_dir.join(".gitignore"))?;

    Ok(())
}

/// Ensures runtime `.gitignore` contains the canonical generated entries.
fn ensure_runtime_gitignore(path: &Path) -> Result<(), ConductorError> {
    let existing = if path.exists() {
        fs::read_to_string(path).map_err(|source| ConductorError::Io {
            operation: "reading conductor runtime dotenv gitignore".to_string(),
            path: path.to_path_buf(),
            source,
        })?
    } else {
        String::new()
    };

    let rendered = merge_runtime_gitignore(&existing);
    if path.exists() && rendered == existing {
        return Ok(());
    }

    fs::write(path, rendered.as_bytes()).map_err(|source| ConductorError::Io {
        operation: "writing conductor runtime dotenv gitignore".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

/// Merges canonical runtime `.gitignore` entries into existing file content.
#[must_use]
fn merge_runtime_gitignore(existing: &str) -> String {
    let mut lines = existing.lines().map(ToString::to_string).collect::<Vec<_>>();
    let required_lines =
        RUNTIME_DOTENV_GITIGNORE.lines().filter(|line| !line.trim().is_empty()).collect::<Vec<_>>();

    for required in required_lines {
        if lines.iter().any(|line| line.trim() == required) {
            continue;
        }
        lines.push(required.to_string());
    }

    if lines.is_empty() { String::new() } else { format!("{}\n", lines.join("\n")) }
}

/// Loads conductor runtime dotenv files and returns inherited env-var names.
///
/// Load order is `.env` then `.env.generated`; later files override earlier
/// values. Returned names preserve declaration order with case-insensitive
/// de-duplication.
///
/// # Errors
///
/// Returns [`ConductorError`] when dotenv files cannot be read or parsed.
pub fn load_runtime_env_files(conductor_dir: &Path) -> Result<Vec<String>, ConductorError> {
    ensure_runtime_env_files(conductor_dir)?;

    let dotenv_path = runtime_dotenv_path(conductor_dir);
    let generated_dotenv_path = runtime_generated_dotenv_path(conductor_dir);

    let mut inherited_names = read_dotenv_variable_names(&dotenv_path)?;
    let generated_names = read_dotenv_variable_names(&generated_dotenv_path)?;
    append_unique_env_var_names(&mut inherited_names, &generated_names);

    if dotenv_path.exists() {
        dotenvy::from_path_override(&dotenv_path).map_err(|source| {
            ConductorError::Workflow(format!(
                "loading conductor runtime dotenv file '{}' failed: {source}",
                dotenv_path.display()
            ))
        })?;
    }

    if generated_dotenv_path.exists() {
        dotenvy::from_path_override(&generated_dotenv_path).map_err(|source| {
            ConductorError::Workflow(format!(
                "loading conductor runtime dotenv file '{}' failed: {source}",
                generated_dotenv_path.display()
            ))
        })?;
    }

    Ok(inherited_names)
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

/// Appends env-var names with case-insensitive de-duplication.
fn append_unique_env_var_names(target: &mut Vec<String>, source: &[String]) {
    for name in source {
        append_unique_env_var_name(target, name);
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{ensure_runtime_env_files, merge_runtime_gitignore};

    /// Protects generated runtime `.gitignore` defaults so runtime cache and
    /// generated env files stay out of version control by default.
    #[test]
    fn merge_runtime_gitignore_adds_runtime_cache_entries() {
        let merged = merge_runtime_gitignore("/.env\n");

        assert!(merged.contains("/.env\n"));
        assert!(merged.contains("/.env.generated\n"));
        assert!(merged.contains("/cache/\n"));
        assert!(merged.contains("/store/index-backups/\n"));
        assert!(merged.contains("/tools/\n"));
        assert_eq!(merged, "/.env\n/.env.generated\n/cache/\n/store/index-backups/\n/tools/\n");
    }

    /// Protects no-overwrite behavior by preserving existing custom ignore
    /// lines while appending any missing generated runtime entries.
    #[test]
    fn ensure_runtime_env_files_preserves_existing_gitignore_content() {
        let workspace = tempdir().expect("tempdir");
        let runtime_dir = workspace.path().join(".conductor");
        std::fs::create_dir_all(&runtime_dir).expect("create runtime dir");
        let gitignore_path = runtime_dir.join(".gitignore");
        std::fs::write(&gitignore_path, "/custom/\n/.env\n").expect("seed gitignore");

        ensure_runtime_env_files(&runtime_dir).expect("ensure runtime env files");

        let rendered = std::fs::read_to_string(&gitignore_path).expect("read gitignore");
        assert!(rendered.contains("/custom/\n"));
        assert!(rendered.contains("/.env\n"));
        assert!(rendered.contains("/.env.generated\n"));
        assert!(rendered.contains("/cache/\n"));
        assert!(rendered.contains("/store/index-backups/\n"));
        assert!(rendered.contains("/tools/\n"));
    }
}
