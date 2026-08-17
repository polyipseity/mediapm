//! Isolation helpers for `mediapm` examples and examples-as-tests.
//!
//! Sets `MEDIAPM_EXAMPLE_*` env overrides to [`mediapm_utils::temp`] directories.
//! See `.agents/instructions/example-temp-isolation.instructions.md`.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

pub use mediapm_utils::temp::remove_dir_all_with_retry;

/// Env var overriding the example artifact root (workspace directory the example mutates).
pub const ARTIFACT_ROOT_ENV: &str = "MEDIAPM_EXAMPLE_ARTIFACT_ROOT";

/// Env var overriding the user-level tool download cache root for example runs.
pub const CACHE_ROOT_ENV: &str = "MEDIAPM_EXAMPLE_CACHE_ROOT";

/// Env var gating the Level 3 full online-sync path shared by the online demo
/// (`mediapm_demo_online`) and the non-example `online_sync_post_sync_dump`
/// regression test. The offline demo (`mediapm_demo`) is deterministic and has
/// no Level 3 gate.
///
/// Semantics differ by consumer (the 3-level model): the online demo `main()`
/// runs full sync unless explicitly disabled (`false|0|no|off`); the
/// regression test runs only on an enabled token (`1|true|yes|on`).
pub const RUN_ONLINE_SYNC_ENV: &str = "MEDIAPM_RUN_ONLINE_SYNC";

/// Process-wide lock serializing `MEDIAPM_EXAMPLE_*` env mutation.
///
/// Examples-as-tests run embedded in one process and can execute in parallel
/// (cargo test threads; nextest runs one process per test). Every
/// [`IsolatedExampleRoots`] guard holds this lock for its whole lifetime, and
/// tests that mutate env directly must hold it via [`lock_process_env`], so an
/// env read inside a test can never observe another test's overrides or a
/// guard's half-restored state.
static ENV_LOCK: LazyLock<parking_lot::Mutex<()>> = LazyLock::new(|| parking_lot::Mutex::new(()));

/// Acquires the process-wide env lock for direct env mutation outside a guard.
pub fn lock_process_env() -> parking_lot::MutexGuard<'static, ()> {
    ENV_LOCK.lock()
}

/// Example user-level tool download cache root.
///
/// Honors [`CACHE_ROOT_ENV`] (set by [`IsolatedExampleRoots::with_cache`] for
/// hermetic examples-as-test runs) and otherwise resolves the real persistent
/// OS user-level cache via
/// [`mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root`],
/// so explicit `cargo run --example` runs share the cache with regular mediapm
/// syncs and persist downloaded tools across runs.
///
/// # Panics
///
/// Panics when the env override is unset and the OS cache directory cannot be
/// determined (mirrors the production sync error).
#[must_use]
pub fn user_level_cache_root() -> PathBuf {
    std::env::var_os(CACHE_ROOT_ENV).map_or_else(
        || {
            mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root()
                .expect("could not determine default user-level tool cache root")
        },
        PathBuf::from,
    )
}

/// Whether the example runs against an isolated (hermetic) cache root.
///
/// True exactly when [`CACHE_ROOT_ENV`] is present, i.e. inside
/// examples-as-tests using [`IsolatedExampleRoots::with_cache`]. Explicit
/// `cargo run --example` runs (env unset) resolve `false`, meaning the real
/// user-level cache is in use.
#[must_use]
pub fn uses_isolated_cache_root() -> bool {
    std::env::var_os(CACHE_ROOT_ENV).is_some()
}

/// Detects a CI environment from standard CI variables.
///
/// Used by the 3-level run model: Level 1 skips nondeterministic network paths
/// automatically in CI. Mirrors the pattern previously duplicated in
/// `mediapm_demo_online.rs` and `online_sync_post_sync_dump.rs`.
#[must_use]
pub fn ci_mode_detected() -> bool {
    std::env::var("CI")
        .is_ok_and(|v| !v.to_ascii_lowercase().is_empty() && v != "0" && v != "false")
        || std::env::var("GITHUB_ACTIONS").is_ok()
        || std::env::var("GITLAB_CI").is_ok()
        || std::env::var("CIRCLECI").is_ok()
        || std::env::var("TRAVIS").is_ok()
        || std::env::var("BUILDKITE").is_ok()
        || std::env::var("DRONE").is_ok()
}

/// Level 3 opt-in for the non-example `online_sync_post_sync_dump` regression
/// test. Enabled tokens: `1|true|yes|on`. Anything else (unset/disabled/unknown)
/// returns `false` (Level 2 skip). Independent of the `large-tests` Cargo feature.
#[must_use]
pub fn run_online_sync_enabled() -> bool {
    std::env::var(RUN_ONLINE_SYNC_ENV)
        .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

/// Strict parser for the online demo `main()` Level 3 override.
///
/// - `None`/empty → `Ok(true)` (full sync; direct `cargo run --example` = Level 3).
/// - enabled token (`1|true|yes|on`) → `Ok(true)`.
/// - disabled token (`false|0|no|off`) → `Ok(false)` (reduced mode).
/// - unknown token → `Err(...)`.
///
/// Fail-fast on unknown tokens preserves the prior `run_sync_override_rejects_invalid_tokens`
/// contract.
pub fn parse_run_online_sync_override(value: Option<&str>) -> Result<bool, String> {
    let Some(raw) = value else {
        return Ok(true);
    };

    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(true);
    }

    if matches!(normalized.as_str(), "true" | "1" | "yes" | "on") {
        return Ok(true);
    }

    if matches!(normalized.as_str(), "false" | "0" | "no" | "off") {
        return Ok(false);
    }

    Err(format!(
        "{RUN_ONLINE_SYNC_ENV} only accepts enabled (true/1/yes/on) or disabled (false/0/no/off) values; got '{normalized}'"
    ))
}

/// RAII guard pointing example env vars at unique prefixed tempdirs for one test run.
pub struct IsolatedExampleRoots {
    artifact_dir: tempfile::TempDir,
    cache_dir: Option<tempfile::TempDir>,
    previous_artifact_root: Option<OsString>,
    previous_cache_root: Option<OsString>,
    // Declared last so it drops last: env restore and tempdir cleanup in
    // `Drop` run while the process-wide env lock is still held.
    _env_lock: parking_lot::MutexGuard<'static, ()>,
}

impl IsolatedExampleRoots {
    /// Isolates artifact and user-level tool-download cache roots.
    ///
    /// # Panics
    ///
    /// Panics when the prefixed artifact or cache tempdir cannot be created.
    #[must_use]
    pub fn with_cache() -> Self {
        let env_lock = lock_process_env();
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
            _env_lock: env_lock,
        }
    }

    /// Isolates only the artifact root (no cache override).
    ///
    /// # Panics
    ///
    /// Panics when the prefixed artifact tempdir cannot be created.
    #[must_use]
    pub fn artifact_only() -> Self {
        let env_lock = lock_process_env();
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
            _env_lock: env_lock,
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
        self.cache_dir.as_ref().map(tempfile::TempDir::path)
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

    /// Restores `CACHE_ROOT_ENV` to its prior value; every env-mutating test
    /// below holds [`lock_process_env`] for its whole body.
    fn restore_cache_root_env(previous: Option<OsString>) {
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            match previous {
                Some(value) => std::env::set_var(CACHE_ROOT_ENV, value),
                None => std::env::remove_var(CACHE_ROOT_ENV),
            }
        }
    }

    #[test]
    fn user_level_cache_root_unset_uses_real_user_cache() {
        let _lock = lock_process_env();
        let previous = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            std::env::remove_var(CACHE_ROOT_ENV);
        }
        let expected =
            mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root()
                .expect("could not determine default user-level tool cache root");
        assert_eq!(user_level_cache_root(), expected);
        restore_cache_root_env(previous);
    }

    #[test]
    fn user_level_cache_root_override_wins() {
        let _lock = lock_process_env();
        let previous = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            std::env::set_var(CACHE_ROOT_ENV, "/tmp/example-cache-override");
        }
        assert_eq!(user_level_cache_root(), PathBuf::from("/tmp/example-cache-override"));
        restore_cache_root_env(previous);
    }

    #[test]
    fn uses_isolated_cache_root_true_when_env_set() {
        let _lock = lock_process_env();
        let previous = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            std::env::set_var(CACHE_ROOT_ENV, "/tmp/example-cache-override");
        }
        assert!(uses_isolated_cache_root());
        restore_cache_root_env(previous);
    }

    #[test]
    fn uses_isolated_cache_root_false_when_unset() {
        let _lock = lock_process_env();
        let previous = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            std::env::remove_var(CACHE_ROOT_ENV);
        }
        assert!(!uses_isolated_cache_root());
        restore_cache_root_env(previous);
    }

    #[test]
    fn isolated_roots_restore_env_and_cleanup() {
        let previous = std::env::var_os(ARTIFACT_ROOT_ENV);
        let guard = IsolatedExampleRoots::with_cache();
        assert!(std::env::var_os(ARTIFACT_ROOT_ENV).is_some());
        assert!(std::env::var_os(CACHE_ROOT_ENV).is_some());
        drop(guard);
        match &previous {
            Some(value) => assert_eq!(std::env::var_os(ARTIFACT_ROOT_ENV), Some(value.clone())),
            None => assert!(std::env::var_os(ARTIFACT_ROOT_ENV).is_none()),
        }
    }
}
