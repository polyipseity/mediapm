//! Tool import helpers for the conductor CLI.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mediapm_cas::Hash;

use crate::error::ConductorError;

/// Collects file list for tool import from a file or directory recursively.
pub(crate) fn collect_tool_files(path: &Path) -> Result<Vec<PathBuf>, ConductorError> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }

    if !path.is_dir() {
        return Err(ConductorError::Workflow(format!(
            "tool import path '{}' does not exist",
            path.display()
        )));
    }

    let mut files = Vec::new();
    collect_files_recursive(path, &mut files)?;
    if files.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool import directory '{}' is empty",
            path.display()
        )));
    }
    Ok(files)
}

fn collect_files_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), ConductorError> {
    for entry in
        std::fs::read_dir(dir).map_err(|e| ConductorError::io("reading directory", dir, e))?
    {
        let entry = entry.map_err(|e| ConductorError::io("reading directory entry", dir, e))?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursive(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
}

/// Resolves executable process name for a tool import.
pub(crate) fn resolve_import_process_name(
    import_path: &Path,
    process_name: Option<&str>,
    fallback_relative_file: Option<&str>,
) -> Result<String, ConductorError> {
    if let Some(explicit) = process_name {
        if explicit.trim().is_empty() {
            return Err(ConductorError::Workflow(
                "--process-name cannot be empty when provided".to_string(),
            ));
        }
        return Ok(explicit.to_string());
    }

    if import_path.is_file() {
        let relative = fallback_relative_file.ok_or_else(|| {
            ConductorError::Workflow(
                "tool import expected at least one file when deriving process name".to_string(),
            )
        })?;
        return Ok(relative.to_string());
    }

    Err(ConductorError::Workflow(
        "tool import from a directory must specify --process-name".to_string(),
    ))
}

/// Checks whether a command binary exists on the system.
///
/// For absolute/relative paths, checks file existence. For bare command names,
/// searches `PATH` directories.
#[must_use]
pub(crate) fn check_binary_exists(cmd: &str) -> bool {
    if cmd.contains(std::path::MAIN_SEPARATOR) || cmd.contains('/') {
        std::path::Path::new(cmd).exists()
    } else {
        std::env::var_os("PATH")
            .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(cmd).exists()))
            .unwrap_or(false)
    }
}

/// Imports a directory tree into a content map by hashing each file into CAS.
///
/// Returns the content map (relative_path → hash) and the total number of
/// imported files.  Files whose names appear in `skip_names` are excluded.
pub(crate) async fn import_directory_to_content_map<C: mediapm_cas::CasApi>(
    cas: &C,
    dir: &Path,
    skip_names: &[&str],
) -> Result<(BTreeMap<String, Hash>, usize), ConductorError> {
    let files = collect_tool_files(dir)?;
    let mut content_map = BTreeMap::new();
    let mut count = 0usize;

    for file_path in &files {
        let relative = file_path.strip_prefix(dir).unwrap_or(file_path);
        let relative_str = relative.to_string_lossy().to_string();

        if skip_names
            .iter()
            .any(|n| relative_str == *n || file_path.file_name().map_or(false, |f| f == *n))
        {
            continue;
        }

        let bytes = tokio::fs::read(file_path)
            .await
            .map_err(|e| ConductorError::io("reading tool file", file_path, e))?;
        let hash = cas.put(bytes.into()).await?;
        content_map.insert(relative_str, hash);
        count += 1;
    }

    Ok((content_map, count))
}
