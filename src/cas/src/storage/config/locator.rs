//! Locator parsing and filesystem-root validation helpers for CAS config.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::CasError;

/// Converts a locator root path into an absolute path.
///
/// Relative paths are resolved against the current process working directory
/// at parse time so downstream filesystem operations do not depend on later
/// working-directory changes.
///
/// # Errors
/// Returns [`CasError`] when reading the current working directory fails.
pub(super) fn normalize_locator_path(path: PathBuf) -> Result<PathBuf, CasError> {
    if path.is_absolute() {
        return Ok(path);
    }

    let cwd = std::env::current_dir().map_err(|source| {
        CasError::io("resolving current directory for relative locator", PathBuf::from("."), source)
    })?;
    Ok(cwd.join(path))
}

/// Expands environment-variable placeholders inside a CAS locator string.
///
/// Supported forms:
/// - Unix style `$NAME`
/// - Unix brace style `${NAME}`
/// - Windows style `%NAME%`
///
/// Variables must be present in the process environment; unresolved variables
/// are treated as invalid user input.
///
/// # Errors
/// Returns [`CasError::InvalidInput`](crate::CasError::InvalidInput) for
/// unterminated placeholders or missing environment variables.
pub(super) fn expand_locator_env(locator: &str) -> Result<String, CasError> {
    let bytes = locator.as_bytes();
    let mut out = String::with_capacity(locator.len());
    let mut idx = 0usize;

    while idx < bytes.len() {
        match bytes[idx] {
            b'$' => {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'{' {
                    let start = idx + 2;
                    let mut end = start;
                    while end < bytes.len() && bytes[end] != b'}' {
                        end += 1;
                    }
                    if end >= bytes.len() {
                        return Err(CasError::invalid_input(format!(
                            "unterminated environment variable expression in locator '{locator}'"
                        )));
                    }
                    let name = &locator[start..end];
                    out.push_str(&std::env::var(name).map_err(|_| {
                        CasError::invalid_input(format!(
                            "environment variable '{name}' referenced by locator is not set"
                        ))
                    })?);
                    idx = end + 1;
                } else {
                    let start = idx + 1;
                    let mut end = start;
                    while end < bytes.len()
                        && ((bytes[end] as char).is_ascii_alphanumeric() || bytes[end] == b'_')
                    {
                        end += 1;
                    }

                    if end == start {
                        out.push('$');
                        idx += 1;
                    } else {
                        let name = &locator[start..end];
                        out.push_str(&std::env::var(name).map_err(|_| {
                            CasError::invalid_input(format!(
                                "environment variable '{name}' referenced by locator is not set"
                            ))
                        })?);
                        idx = end;
                    }
                }
            }
            b'%' => {
                let start = idx + 1;
                let mut end = start;
                while end < bytes.len() && bytes[end] != b'%' {
                    end += 1;
                }
                if end < bytes.len() {
                    let name = &locator[start..end];
                    out.push_str(&std::env::var(name).map_err(|_| {
                        CasError::invalid_input(format!(
                            "environment variable '{name}' referenced by locator is not set"
                        ))
                    })?);
                    idx = end + 1;
                } else {
                    out.push('%');
                    idx += 1;
                }
            }
            byte => {
                out.push(byte as char);
                idx += 1;
            }
        }
    }

    Ok(out)
}

/// Verifies that a filesystem-backed locator root is usable for writes.
///
/// The check performs an end-to-end probe (`create -> write -> fsync ->
/// remove`) under `root` so callers fail fast when the location is
/// read-only, inaccessible, or misconfigured.
///
/// # Errors
/// Returns [`CasError`] when the root path is not a directory or any probe
/// file operation fails.
pub(super) fn validate_filesystem_root_writable(root: &Path) -> Result<(), CasError> {
    if root.exists() && !root.is_dir() {
        return Err(CasError::invalid_input(format!(
            "filesystem locator root must be a directory: {}",
            root.display()
        )));
    }

    std::fs::create_dir_all(root)
        .map_err(|source| CasError::io("creating filesystem locator root", root, source))?;

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let probe = root.join(format!(".cas-write-probe-{}-{nonce}", std::process::id()));

    let mut file = std::fs::OpenOptions::new().create_new(true).write(true).open(&probe).map_err(
        |source| CasError::io("creating filesystem locator write probe", &probe, source),
    )?;
    file.write_all(b"probe")
        .map_err(|source| CasError::io("writing filesystem locator write probe", &probe, source))?;
    file.sync_all()
        .map_err(|source| CasError::io("syncing filesystem locator write probe", &probe, source))?;
    drop(file);

    std::fs::remove_file(&probe).map_err(|source| {
        CasError::io("removing filesystem locator write probe", &probe, source)
    })?;

    Ok(())
}
