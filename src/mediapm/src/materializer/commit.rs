//! Path validation, readonly enforcement, and filesystem helpers.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use unicode_normalization::UnicodeNormalization;

use crate::config::hierarchy_types::SanitizeNamesConfig;
use crate::error::MediaPmError;

/// Removes one path recursively when it is a directory, or as one file otherwise.
pub(super) fn remove_path(path: &Path) -> Result<(), MediaPmError> {
    clear_path_readonly_recursively(path)?;

    // On Unix, removing a child entry requires write permission on the parent
    // directory. The parent directory is usually already writable, but may be
    // read-only when it is itself a managed output that was marked read-only by
    // `ensure_managed_path_readonly()` during a previous materialization cycle
    // (e.g. a hierarchy folder node containing stale file entries).
    clear_directory_writable(path)?;

    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before removal".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        fs::remove_dir_all(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale directory".to_string(),
            path: path.to_path_buf(),
            source,
        })
    } else {
        fs::remove_file(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale file".to_string(),
            path: path.to_path_buf(),
            source,
        })
    }
}

/// Marks one managed output path as read-only after successful materialization.
///
/// For directory outputs, this recursively marks descendant files/directories
/// read-only. For symlinks, permissions are applied to the resolved target.
pub(super) fn ensure_managed_path_readonly(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading managed output metadata before readonly enforcement".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading managed output directory before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating managed output directory before readonly enforcement"
                    .to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            ensure_managed_path_readonly(&entry.path())?;
        }
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading managed output permissions before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "marking managed output path readonly".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Clears read-only bit recursively so stale managed paths can be removed.
///
/// On BSD platforms (macOS, FreeBSD, etc.) this also clears the user/system
/// immutable flags (`UF_IMMUTABLE` / `SF_IMMUTABLE` / `uchg` / `schg`) which
/// prevent file deletion independently of Unix permission bits.
fn clear_path_readonly_recursively(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before readonly clear".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading directory before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating directory before readonly clear".to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            clear_path_readonly_recursively(&entry.path())?;
        }
    }

    // On BSD platforms (macOS, FreeBSD, etc.), clear immutable file flags
    // that prevent deletion independently of Unix permission bits.
    // These flags can be inherited from tool outputs, set by backup software,
    // or applied manually by the user.
    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        clear_bsd_immutable_flags(path)?;
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading path permissions before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                permissions.set_mode(writable_mode);
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed overwrite/delete operations can succeed"
            )]
            {
                permissions.set_readonly(false);
            }
        }

        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "clearing readonly bit before managed-path removal".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Clears BSD immutable flags (`UF_IMMUTABLE` / `SF_IMMUTABLE`) on the given
/// path so the file can be removed.
///
/// Uses `stat` + `chflags` (both following symlinks) for consistency with the
/// `fs::metadata` / `fs::set_permissions` calls elsewhere in this function.
/// If `stat` fails (e.g. the path no longer exists), this is treated as a
/// no-op and the subsequent permission check will surface any relevant error.
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn clear_bsd_immutable_flags(path: &Path) -> Result<(), MediaPmError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| MediaPmError::Workflow("path contains null byte for chflags".to_string()))?;

    // Read current flags via stat (follows symlinks, matching fs::metadata behavior).
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::stat(c_path.as_ptr(), &raw mut st) } != 0 {
        // Path may not exist; let the caller's fs::metadata surface the error.
        return Ok(());
    }

    let immutable_mask = libc::UF_IMMUTABLE | libc::SF_IMMUTABLE;
    if (st.st_flags as u32) & immutable_mask != 0 {
        let new_flags = (st.st_flags as u32) & !immutable_mask;
        if unsafe { libc::chflags(c_path.as_ptr(), new_flags) } != 0 {
            let err = std::io::Error::last_os_error();
            return Err(MediaPmError::Io {
                operation: "clearing immutable flags before managed-path removal".to_string(),
                path: path.to_path_buf(),
                source: err,
            });
        }
    }

    Ok(())
}

/// Ensures the parent directory of `path` is writable so the child entry can
/// be removed.
///
/// On Unix, unlinking or renaming a child requires write permission on the
/// containing directory. Managed directory outputs may be read-only when they
/// are themselves part of a previously materialized hierarchy tree. This
/// helper clears the readonly bit and BSD immutable flags on the parent
/// (without recursing into sibling entries).
fn clear_directory_writable(path: &Path) -> Result<(), MediaPmError> {
    let Some(parent) = path.parent() else { return Ok(()) };

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        clear_bsd_immutable_flags(parent)?;
    }

    let Ok(metadata) = fs::metadata(parent) else { return Ok(()) };
    let mut permissions = metadata.permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                permissions.set_mode(writable_mode);
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed delete operations can succeed"
            )]
            {
                permissions.set_readonly(false);
            }
        }

        fs::set_permissions(parent, permissions).map_err(|source| MediaPmError::Io {
            operation: "clearing readonly bit on parent directory before removal".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Applies a reserved-character replacement map to a single path component.
///
/// This operates on individual characters within one path component, not on
/// a joined path string, so `/` and `\` within a component are properly
/// replaced rather than consumed as structural separators.
#[must_use]
#[allow(dead_code)]
pub(super) fn sanitize_path_component(
    component: &str,
    replacements: &BTreeMap<char, char>,
) -> String {
    component.chars().map(|ch| replacements.get(&ch).copied().unwrap_or(ch)).collect()
}

/// Checks that source path components are NFD-normalized before resolution.
///
/// This is the first NFD check — applied at the config level before any
/// template placeholders are resolved.
#[allow(dead_code)]
pub(super) fn check_nfd_source(components: &[String]) -> Result<(), MediaPmError> {
    for component in components {
        let component_nfd = component.nfd().collect::<String>();
        if component_nfd != *component {
            return Err(MediaPmError::Workflow(format!(
                "source path component '{component}' must be NFD-normalized"
            )));
        }
    }
    Ok(())
}

/// Validates resolved and sanitized path components against mediapm invariants.
///
/// Rules per component:
/// - Must not be empty
/// - Must not be `.` or `..`
/// - Must not contain forbidden characters (`<`, `>`, `:`, `"`, `|`, `?`, `*`, `/`, `\`)
/// - Must be Unicode NFD normalized (with a distinct message from the source check)
///
/// Returns the validated components (consume-then-return for pipeline chaining).
#[allow(dead_code)]
pub(super) fn validate_components(components: &[String]) -> Result<Vec<String>, MediaPmError> {
    for component in components {
        if component.is_empty() {
            return Err(MediaPmError::Workflow(
                "hierarchy path component must not be empty".to_string(),
            ));
        }
        if component == "." || component == ".." {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path component '{component}' must not be '.' or '..'"
            )));
        }
        if component.chars().any(is_rejected_char) {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path component '{component}' contains forbidden characters"
            )));
        }
        let component_nfd = component.nfd().collect::<String>();
        if component_nfd != *component {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path component '{component}' is not NFD-normalized"
            )));
        }
    }
    Ok(components.to_vec())
}

/// Applies NFD normalization, optional reserved-character sanitization, and
/// strict validation to resolved hierarchy path components.
///
/// Pipeline order:
/// 1. NFD normalize each component.
/// 2. If `sanitize_names` is enabled, replace reserved characters using the
///    effective replacement map (runtime defaults merged with any per-entry
///    custom overrides).
/// 3. Validate all components (NFD, non-empty, no `.`/`..`, no reserved chars).
///
/// # Errors
///
/// Delegates to [`validate_components`] when any component fails validation.
#[allow(dead_code)]
pub(super) fn sanitize_and_validate_components(
    components: &[String],
    sanitize_names: &SanitizeNamesConfig,
    default_replacements: &BTreeMap<char, char>,
) -> Result<Vec<String>, MediaPmError> {
    let mut resolved = components.to_vec();
    for component in &mut resolved {
        *component = component.nfd().collect::<String>();
    }
    if matches!(sanitize_names, SanitizeNamesConfig::Enabled | SanitizeNamesConfig::Custom(..)) {
        let effective = match sanitize_names {
            SanitizeNamesConfig::Custom(custom) => {
                let mut map = default_replacements.clone();
                map.extend(custom.clone());
                map
            }
            _ => default_replacements.clone(),
        };
        for component in &mut resolved {
            *component = sanitize_path_component(component, &effective);
        }
    }
    validate_components(&resolved)
}

/// Returns whether one character is forbidden by cross-platform filename rules.
#[allow(dead_code)]
fn is_rejected_char(ch: char) -> bool {
    matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*' | '/' | '\\')
}

/// Returns current Unix epoch timestamp in seconds.
#[allow(dead_code)]
pub(super) fn now_unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}
