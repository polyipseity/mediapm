//! Prefixed temporary directories for mediapm tests, examples, and runtime sandboxes.
//!
//! All mediapm-owned temp paths under `$TMPDIR` use `mediapm-{role}-{unique}` naming so
//! `scripts/clean-mediapm-temp.sh` can remove orphans. See
//! `.agents/instructions/example-temp-isolation.instructions.md`.

use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

/// Prefix for example/test workspace artifact roots (`mediapm-artifact-XXXXXX`).
pub const ARTIFACT_PREFIX: &str = "mediapm-artifact-";

/// Prefix for isolated user-level download cache roots (`mediapm-cache-XXXXXX`).
pub const CACHE_PREFIX: &str = "mediapm-cache-";

/// Prefix for per-workspace conductor runtime roots (`mediapm-runtime-{16hex}`).
pub const RUNTIME_PREFIX: &str = "mediapm-runtime-";

/// Creates a unique artifact workspace directory under `$TMPDIR`.
///
/// # Errors
///
/// Returns [`io::Error`] when the directory cannot be created.
pub fn artifact_dir() -> io::Result<tempfile::TempDir> {
    tempfile::Builder::new().prefix(ARTIFACT_PREFIX).tempdir()
}

/// Creates a unique user-level download cache directory under `$TMPDIR`.
///
/// # Errors
///
/// Returns [`io::Error`] when the directory cannot be created.
pub fn cache_dir() -> io::Result<tempfile::TempDir> {
    tempfile::Builder::new().prefix(CACHE_PREFIX).tempdir()
}

/// Returns the stable conductor/mediapm runtime tmp root for one workspace.
#[must_use]
pub fn runtime_dir_for_workspace(workspace_root: &Path) -> PathBuf {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    workspace_root.hash(&mut hasher);
    let key = format!("{:016x}", hasher.finish());
    std::env::temp_dir().join(format!("{RUNTIME_PREFIX}{key}"))
}

/// Returns true when the final path component uses a mediapm temp role prefix.
#[must_use]
pub fn is_managed_path(path: &Path) -> bool {
    path.file_name().and_then(|name| name.to_str()).is_some_and(|name| {
        name.starts_with(ARTIFACT_PREFIX)
            || name.starts_with(CACHE_PREFIX)
            || name.starts_with(RUNTIME_PREFIX)
    })
}

/// Removes a directory tree with readonly-bit clearing and short retries.
///
/// # Errors
///
/// Returns the last [`io::Error`] when removal fails after all attempts.
pub fn remove_dir_all_with_retry(path: &Path) -> io::Result<()> {
    const ATTEMPTS: usize = 6;
    const BACKOFF_MS: u64 = 40;

    if !path.exists() {
        return Ok(());
    }

    let mut last_error: Option<io::Error> = None;

    for attempt in 0..ATTEMPTS {
        match fs::remove_dir_all(path) {
            Ok(()) => return Ok(()),
            Err(error) => {
                let retryable = error.kind() == io::ErrorKind::PermissionDenied
                    || error.raw_os_error() == Some(32);
                last_error = Some(error);
                if !retryable || attempt + 1 == ATTEMPTS {
                    break;
                }
                clear_readonly_bits_recursively(path);
                thread::sleep(Duration::from_millis(BACKOFF_MS));
            }
        }
    }

    match last_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[expect(
    clippy::permissions_set_readonly_false,
    reason = "cleanup retries must clear readonly flags on artifacts so repeated demo runs can remove prior trees"
)]
fn clear_readonly_bits_recursively(path: &Path) {
    if !path.exists() {
        return;
    }

    let mut stack = vec![path.to_path_buf()];
    while let Some(next) = stack.pop() {
        if let Ok(metadata) = fs::metadata(&next) {
            let mut permissions = metadata.permissions();
            if permissions.readonly() {
                permissions.set_readonly(false);
                let _ = fs::set_permissions(&next, permissions);
            }
        }

        if next.is_dir()
            && let Ok(entries) = fs::read_dir(&next)
        {
            for entry in entries.flatten() {
                stack.push(entry.path());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_dir_uses_role_prefix() {
        let dir = artifact_dir().expect("artifact dir");
        let name = dir.path().file_name().and_then(|n| n.to_str()).expect("file name");
        assert!(name.starts_with(ARTIFACT_PREFIX));
        assert!(is_managed_path(dir.path()));
    }

    #[test]
    fn cache_dir_uses_role_prefix() {
        let dir = cache_dir().expect("cache dir");
        let name = dir.path().file_name().and_then(|n| n.to_str()).expect("file name");
        assert!(name.starts_with(CACHE_PREFIX));
    }

    #[test]
    fn runtime_dir_for_workspace_is_stable_and_prefixed() {
        let root = PathBuf::from("/tmp/mediapm-workspace-fixture");
        let first = runtime_dir_for_workspace(&root);
        let second = runtime_dir_for_workspace(&root);
        assert_eq!(first, second);
        let name = first.file_name().and_then(|n| n.to_str()).expect("file name");
        assert!(name.starts_with(RUNTIME_PREFIX));
    }

    #[test]
    fn is_managed_path_rejects_unrelated_temp_children() {
        let unrelated = PathBuf::from("/tmp/.tmpUnrelated");
        assert!(!is_managed_path(&unrelated));
    }
}
