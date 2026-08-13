//! Prefixed temporary directories for mediapm tests, examples, and runtime sandboxes.
//!
//! Every mediapm-owned temp path under `$TMPDIR` uses a single tracked prefix `mediapm-`
//! followed by a role suffix (`mediapm-artifact-{unique}`, `mediapm-cache-{unique}`,
//! `mediapm-runtime-{16hex}`), so `scripts/clean-mediapm-temp.sh` (POSIX) or
//! `scripts/clean-mediapm-temp.ps1` (Windows) can remove orphans with one `mediapm-*` glob.
//! See `.agents/instructions/temp-directory-spec.instructions.md` for the canonical spec
//! (naming contract, janitor contract, regression gates, authoring rules) and
//! `.agents/instructions/example-temp-isolation.instructions.md` for example/test wiring.

use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

/// Single prefix for every mediapm-owned temp root (`mediapm-{role}-{unique}`).
///
/// The janitor globs (`mediapm-*`) and `is_managed_path` are derived from this
/// constant; no other prefix is tracked.
pub const MEDIAPM_TEMP_PREFIX: &str = "mediapm-";

/// Creates a unique artifact workspace directory under `$TMPDIR`.
///
/// # Errors
///
/// Returns [`io::Error`] when the directory cannot be created.
pub fn artifact_dir() -> io::Result<tempfile::TempDir> {
    tempfile::Builder::new().prefix(&format!("{MEDIAPM_TEMP_PREFIX}artifact-")).tempdir()
}

/// Creates a unique user-level download cache directory under `$TMPDIR`.
///
/// # Errors
///
/// Returns [`io::Error`] when the directory cannot be created.
pub fn cache_dir() -> io::Result<tempfile::TempDir> {
    tempfile::Builder::new().prefix(&format!("{MEDIAPM_TEMP_PREFIX}cache-")).tempdir()
}

/// Returns the stable conductor/mediapm runtime tmp root for one workspace.
#[must_use]
pub fn runtime_dir_for_workspace(workspace_root: &Path) -> PathBuf {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    workspace_root.hash(&mut hasher);
    let key = format!("{:016x}", hasher.finish());
    std::env::temp_dir().join(format!("{MEDIAPM_TEMP_PREFIX}runtime-{key}"))
}

/// Returns true when the final path component uses the mediapm temp prefix.
#[must_use]
pub fn is_managed_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(MEDIAPM_TEMP_PREFIX))
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
                let retryable = is_retryable_os_error(&error);
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

/// Returns true for OS errors where a short retry may succeed.
///
/// `remove_dir_all` on Unix can transiently fail with `EBUSY`, `EPERM`, or
/// `EACCES` when a lingering background task still holds a handle into the
/// tree (e.g. a spawned maintenance task racing test teardown); retrying
/// after a short backoff clears those. On Windows the classic transient is
/// `ERROR_SHARING_VIOLATION` (raw code 32). macOS raw 32 is `EPIPE`, which
/// `remove_dir_all` never produces, so it is deliberately NOT retryable on
/// Unix — classifying it so would mask a real failure.
fn is_retryable_os_error(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::PermissionDenied {
        return true;
    }
    let Some(code) = error.raw_os_error() else {
        return false;
    };
    #[cfg(unix)]
    {
        // POSIX errno values shared by macOS and Linux.
        const EBUSY: i32 = 16;
        const EPERM: i32 = 1;
        const EACCES: i32 = 13;
        code == EBUSY || code == EPERM || code == EACCES
    }
    #[cfg(windows)]
    {
        code == 32 // ERROR_SHARING_VIOLATION
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
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn artifact_dir_uses_role_prefix() {
        let dir = artifact_dir().expect("artifact dir");
        let name = dir.path().file_name().and_then(|n| n.to_str()).expect("file name");
        assert!(name.starts_with(MEDIAPM_TEMP_PREFIX));
        assert!(is_managed_path(dir.path()));
    }

    #[test]
    fn cache_dir_uses_role_prefix() {
        let dir = cache_dir().expect("cache dir");
        let name = dir.path().file_name().and_then(|n| n.to_str()).expect("file name");
        assert!(name.starts_with(MEDIAPM_TEMP_PREFIX));
    }

    #[test]
    fn runtime_dir_for_workspace_is_stable_and_prefixed() {
        let root = PathBuf::from("/tmp/mediapm-workspace-fixture");
        let first = runtime_dir_for_workspace(&root);
        let second = runtime_dir_for_workspace(&root);
        assert_eq!(first, second);
        let name = first.file_name().and_then(|n| n.to_str()).expect("file name");
        assert!(name.starts_with(MEDIAPM_TEMP_PREFIX));
    }

    #[test]
    fn is_managed_path_rejects_unrelated_temp_children() {
        let unrelated = PathBuf::from("/tmp/.tmpUnrelated");
        assert!(!is_managed_path(&unrelated));
    }

    #[test]
    fn remove_retry_classifies_permission_denied() {
        assert!(is_retryable_os_error(&io::Error::from(io::ErrorKind::PermissionDenied)));
    }

    #[cfg(unix)]
    #[test]
    fn remove_retry_classifies_unix_transient_errors() {
        // EBUSY / EPERM / EACCES are transient remove_dir_all failures on
        // macOS and Linux (lingering handles racing teardown).
        for code in [16, 1, 13] {
            assert!(
                is_retryable_os_error(&io::Error::from_raw_os_error(code)),
                "raw OS error {code} must be classified retryable"
            );
        }
        // macOS raw 32 is EPIPE; remove_dir_all never produces it, so it
        // must NOT be retryable (retrying would mask a real failure).
        assert!(!is_retryable_os_error(&io::Error::from_raw_os_error(32)));
    }

    #[cfg(windows)]
    #[test]
    fn remove_retry_classifies_sharing_violation() {
        assert!(is_retryable_os_error(&io::Error::from_raw_os_error(32)));
    }

    /// Collects every `mediapm-*` glob stem (trailing `*`) from a janitor
    /// script. The only glob form is `mediapm-*`, so each occurrence of
    /// `mediapm-` immediately followed by `*` contributes the stem
    /// `mediapm-`. Path mentions (e.g. `src/mediapm-utils/src/temp.rs`
    /// comments) have no trailing `*` and are excluded.
    fn mediapm_glob_stems(script: &str) -> BTreeSet<&str> {
        let mut stems = BTreeSet::new();
        let mut offset = 0;
        while let Some(relative) = script[offset..].find("mediapm-") {
            let start = offset + relative;
            let after = start + "mediapm-".len();
            if script[after..].starts_with('*') {
                stems.insert(&script[start..after]);
            }
            offset = after;
        }
        stems
    }

    #[test]
    fn janitor_glob_stems_match_prefix_contract() {
        // The janitor scripts cannot import the Rust constant, so they
        // hardcode the `mediapm-*` glob; this test pins it to the single
        // source of truth (`temp.rs`) so the glob set drifting from
        // `mediapm-` in either janitor fails the suite.
        let bash = include_str!("../../../scripts/clean-mediapm-temp.sh");
        let powershell = include_str!("../../../scripts/clean-mediapm-temp.ps1");
        let expected = BTreeSet::from([MEDIAPM_TEMP_PREFIX]);
        assert_eq!(
            mediapm_glob_stems(bash),
            expected,
            "bash janitor glob set must match the temp.rs prefix contract"
        );
        assert_eq!(
            mediapm_glob_stems(powershell),
            expected,
            "PowerShell janitor glob set must match the temp.rs prefix contract"
        );
    }
}
