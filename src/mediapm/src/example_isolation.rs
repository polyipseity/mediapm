//! Isolation helpers for `mediapm` examples and examples-as-tests.
//!
//! Sets `MEDIAPM_EXAMPLE_*` env overrides to [`mediapm_utils::temp`] directories.
//! See `.agents/instructions/example-temp-isolation.instructions.md`.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

pub use mediapm_utils::temp::{ARTIFACT_PREFIX, CACHE_PREFIX, remove_dir_all_with_retry};

/// Env var overriding the example artifact root (workspace directory the example mutates).
pub const ARTIFACT_ROOT_ENV: &str = "MEDIAPM_EXAMPLE_ARTIFACT_ROOT";

/// Env var overriding the user-level tool download cache root for example runs.
pub const CACHE_ROOT_ENV: &str = "MEDIAPM_EXAMPLE_CACHE_ROOT";

/// Default example cache root — a hermetic sibling of the artifact root used
/// when [`CACHE_ROOT_ENV`] is unset, so bare `cargo run --example` never
/// touches the real OS user cache. Lives under the artifact root, so it is
/// wiped together with it on each run reset.
#[must_use]
pub fn default_example_cache_root(artifact_root: &Path) -> PathBuf {
    artifact_root.join("cache")
}

/// RAII guard pointing example env vars at unique prefixed tempdirs for one test run.
pub struct IsolatedExampleRoots {
    artifact_dir: tempfile::TempDir,
    cache_dir: Option<tempfile::TempDir>,
    previous_artifact_root: Option<OsString>,
    previous_cache_root: Option<OsString>,
}

impl IsolatedExampleRoots {
    /// Isolates artifact and user-level tool-download cache roots.
    #[must_use]
    pub fn with_cache() -> Self {
        let artifact_dir = mediapm_utils::temp::artifact_dir().expect("create temp artifact root");
        let cache_dir = mediapm_utils::temp::cache_dir().expect("create temp cache root");
        let previous_artifact_root = std::env::var_os(ARTIFACT_ROOT_ENV);
        let previous_cache_root = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: test/example guard scopes env overrides to this struct's lifetime.
        unsafe {
            std::env::set_var(ARTIFACT_ROOT_ENV, artifact_dir.path());
            std::env::set_var(CACHE_ROOT_ENV, cache_dir.path());
        }
        Self {
            artifact_dir,
            cache_dir: Some(cache_dir),
            previous_artifact_root,
            previous_cache_root,
        }
    }

    /// Isolates only the artifact root (no cache override).
    #[must_use]
    pub fn artifact_only() -> Self {
        let artifact_dir = mediapm_utils::temp::artifact_dir().expect("create temp artifact root");
        let previous_artifact_root = std::env::var_os(ARTIFACT_ROOT_ENV);
        // SAFETY: test/example guard scopes env overrides to this struct's lifetime.
        unsafe {
            std::env::set_var(ARTIFACT_ROOT_ENV, artifact_dir.path());
        }
        Self {
            artifact_dir,
            cache_dir: None,
            previous_artifact_root,
            previous_cache_root: std::env::var_os(CACHE_ROOT_ENV),
        }
    }

    /// Isolated artifact root path (the effective workspace / artifact directory).
    #[must_use]
    pub fn artifact_root_path(&self) -> &Path {
        self.artifact_dir.path()
    }

    /// Isolated user-level download cache path when [`Self::with_cache`] was used.
    #[must_use]
    pub fn cache_root_path(&self) -> Option<&Path> {
        self.cache_dir.as_ref().map(|dir| dir.path())
    }
}

impl Drop for IsolatedExampleRoots {
    fn drop(&mut self) {
        // SAFETY: restore prior env values before deleting tempdirs.
        unsafe {
            match &self.previous_artifact_root {
                Some(value) => std::env::set_var(ARTIFACT_ROOT_ENV, value),
                None => std::env::remove_var(ARTIFACT_ROOT_ENV),
            }
            match &self.previous_cache_root {
                Some(value) => std::env::set_var(CACHE_ROOT_ENV, value),
                None => std::env::remove_var(CACHE_ROOT_ENV),
            }
        }

        let _ = remove_dir_all_with_retry(self.artifact_dir.path());
        if let Some(cache_dir) = &self.cache_dir {
            let _ = remove_dir_all_with_retry(cache_dir.path());
        }
    }
}

/// Creates a prefixed artifact tempdir for share-violation fallbacks during manual runs.
///
/// # Errors
///
/// Returns [`std::io::Error`] when the directory cannot be created.
pub fn isolated_artifact_dir() -> std::io::Result<(tempfile::TempDir, PathBuf)> {
    let dir = mediapm_utils::temp::artifact_dir()?;
    let path = dir.path().to_path_buf();
    Ok((dir, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_example_cache_root_is_artifact_sibling() {
        assert_eq!(
            default_example_cache_root(Path::new("/tmp/example-artifact")),
            PathBuf::from("/tmp/example-artifact/cache")
        );
    }

    #[test]
    fn isolated_roots_restore_env_and_cleanup() {
        let previous = std::env::var_os(ARTIFACT_ROOT_ENV);
        let _guard = IsolatedExampleRoots::with_cache();
        assert!(std::env::var_os(ARTIFACT_ROOT_ENV).is_some());
        assert!(std::env::var_os(CACHE_ROOT_ENV).is_some());
        drop(_guard);
        match &previous {
            Some(value) => assert_eq!(std::env::var_os(ARTIFACT_ROOT_ENV), Some(value.clone())),
            None => assert!(std::env::var_os(ARTIFACT_ROOT_ENV).is_none()),
        }
    }
}
