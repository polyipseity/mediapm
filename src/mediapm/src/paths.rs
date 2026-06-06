//! Filesystem path layout helpers for `mediapm` state.
//!
//! This module centralizes where `mediapm` keeps user policy, conductor-facing
//! Nickel documents, machine-managed state, and staging directories.
//! Keeping path decisions in one place prevents accidental drift between CLI,
//! sync service, and tests.

use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use crate::config::MediaRuntimeStorage;

/// Canonical path bundle for one `mediapm` workspace root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaPmPaths {
    /// Root directory that contains all `mediapm` files.
    pub root_dir: PathBuf,
    /// User-edited `mediapm` policy/config Nickel document.
    pub mediapm_ncl: PathBuf,
    /// Conductor user Nickel document used by mediapm orchestration.
    pub conductor_user_ncl: PathBuf,
    /// Conductor machine Nickel document used by mediapm orchestration.
    pub conductor_machine_ncl: PathBuf,
    /// Conductor volatile runtime state document path used by mediapm orchestration.
    pub conductor_state_config: PathBuf,
    /// Conductor execution-sandbox temporary directory used by mediapm orchestration.
    pub conductor_tmp_dir: PathBuf,
    /// Conductor schema export directory used by mediapm orchestration.
    pub conductor_schema_dir: PathBuf,
    /// `mediapm` machine-managed state document (`state.ncl`).
    pub mediapm_state_ncl: PathBuf,
    /// Runtime dotenv file used to load local credential overrides.
    pub env_file: PathBuf,
    /// Machine-generated runtime dotenv file (written by tooling, not users).
    pub env_generated_file: PathBuf,
    /// Runtime root for `mediapm`-owned state under the workspace root.
    pub runtime_root: PathBuf,
    /// Embedded Nickel schema export directory for `mediapm.ncl` contracts.
    ///
    /// `None` means schema export is disabled by runtime policy.
    pub schema_export_dir: Option<PathBuf>,
    /// Temporary staging directory used by atomic sync.
    pub mediapm_tmp_dir: PathBuf,
    /// Materialized media library root.
    pub hierarchy_root_dir: PathBuf,
    /// Tool-content unpack cache generated under the workspace runtime root.
    pub tools_dir: PathBuf,
}

impl MediaPmPaths {
    /// Computes canonical mediapm paths from one workspace root.
    #[must_use]
    pub fn from_root(root_dir: impl Into<PathBuf>) -> Self {
        let root_dir = root_dir.into();
        let runtime_root = root_dir.join(".mediapm");
        let tmp_dir = default_runtime_tmp_dir(&root_dir);

        Self {
            mediapm_ncl: root_dir.join("mediapm.ncl"),
            conductor_user_ncl: root_dir.join("mediapm.conductor.ncl"),
            conductor_machine_ncl: root_dir.join("mediapm.conductor.machine.ncl"),
            conductor_state_config: runtime_root.join("state.conductor.ncl"),
            conductor_tmp_dir: tmp_dir.clone(),
            conductor_schema_dir: runtime_root.join("config").join("conductor"),
            mediapm_state_ncl: runtime_root.join("state.ncl"),
            env_file: runtime_root.join(".env"),
            env_generated_file: runtime_root.join(".env.generated"),
            schema_export_dir: Some(runtime_root.join("config").join("mediapm")),
            mediapm_tmp_dir: tmp_dir,
            hierarchy_root_dir: root_dir.clone(),
            tools_dir: runtime_root.join("tools"),
            runtime_root,
            root_dir,
        }
    }

    /// Returns the default path layout rooted at the current working directory.
    #[must_use]
    pub fn from_current_dir() -> Self {
        Self::from_root(Path::new("."))
    }

    /// Returns the runtime-scoped cache root (`<runtime>/cache`).
    #[must_use]
    pub fn workspace_cache_dir(&self) -> PathBuf {
        self.runtime_root.join("cache")
    }

    /// Returns the runtime-scoped shared cache store (`<runtime>/cache/store`).
    #[must_use]
    pub fn workspace_cache_store_dir(&self) -> PathBuf {
        self.workspace_cache_dir().join("store")
    }

    /// Returns the managed yt-dlp cache directory (`<runtime>/cache/yt-dlp`).
    #[must_use]
    pub fn workspace_yt_dlp_cache_dir(&self) -> PathBuf {
        self.workspace_cache_dir().join("yt-dlp")
    }

    /// Returns the managed media-tagger cache root.
    ///
    /// Media-tagger uses the shared cache layout under this root:
    /// - `<cache>/store/` for CAS payload objects,
    /// - `<cache>/media-tagger.jsonc` for metadata index rows.
    ///
    /// Keeping media-tagger on this root avoids dedicated per-tool directories
    /// under `store/` and aligns runtime-local cache behavior with user-global
    /// cache semantics.
    #[must_use]
    pub fn workspace_media_tagger_cache_dir(&self) -> PathBuf {
        self.workspace_cache_dir()
    }

    /// Resolves runtime storage overrides from `mediapm.ncl`.
    ///
    /// Resolution rules:
    /// - `runtime.mediapm_dir` is resolved relative to the outermost
    ///   `mediapm.ncl` directory when provided as a relative path,
    /// - `runtime.hierarchy_root_dir` is resolved relative to the outermost
    ///   `mediapm.ncl` directory when provided as a relative path,
    /// - `runtime.conductor_config` and
    ///   `runtime.conductor_machine_config` resolve relative to the
    ///   outermost `mediapm.ncl` directory when provided as relative paths,
    /// - `runtime.conductor_state_config` resolves relative to the outermost
    ///   `mediapm.ncl` directory when provided as a relative path,
    /// - `runtime.conductor_schema_dir` resolves relative to the effective
    ///   workspace directory when provided as a relative path and defaults to
    ///   `<runtime.mediapm_dir>/config/conductor`,
    /// - `runtime.media_state_config` resolves relative to the outermost
    ///   `mediapm.ncl` directory when provided as a relative path,
    /// - `runtime.env_file` resolves relative to the outermost
    ///   `mediapm.ncl` directory when provided as a relative path,
    /// - omitted fields keep defaults of config root for library output and
    ///   an OS-provided temp directory for runtime staging.
    #[must_use]
    pub fn with_runtime_storage(&self, runtime_storage: &MediaRuntimeStorage) -> Self {
        let config_dir = self.mediapm_ncl.parent().unwrap_or(&self.root_dir);

        let runtime_root = runtime_storage
            .mediapm_dir
            .as_deref()
            .map_or_else(|| self.runtime_root.clone(), |raw| resolve_path(config_dir, raw));

        let library_dir = runtime_storage
            .hierarchy_root_dir
            .as_deref()
            .map_or_else(|| config_dir.to_path_buf(), |raw| resolve_path(config_dir, raw));

        let tmp_dir = default_runtime_tmp_dir(&self.root_dir);

        let conductor_user_ncl = runtime_storage
            .conductor_config
            .as_deref()
            .map_or_else(|| self.conductor_user_ncl.clone(), |raw| resolve_path(config_dir, raw));

        let conductor_machine_ncl =
            runtime_storage.conductor_machine_config.as_deref().map_or_else(
                || self.conductor_machine_ncl.clone(),
                |raw| resolve_path(config_dir, raw),
            );

        let conductor_state_config = runtime_storage.conductor_state_config.as_deref().map_or_else(
            || runtime_root.join("state.conductor.ncl"),
            |raw| resolve_path(config_dir, raw),
        );

        let conductor_schema_dir = runtime_storage.conductor_schema_dir.as_deref().map_or_else(
            || runtime_root.join("config").join("conductor"),
            |raw| resolve_path(config_dir, raw),
        );

        let mediapm_state_ncl = runtime_storage
            .media_state_config
            .as_deref()
            .map_or_else(|| runtime_root.join("state.ncl"), |raw| resolve_path(config_dir, raw));

        let env_file = runtime_storage
            .env_file
            .as_deref()
            .map_or_else(|| runtime_root.join(".env"), |raw| resolve_path(config_dir, raw));

        let env_generated_file = runtime_storage.env_generated_file.as_deref().map_or_else(
            || runtime_root.join(".env.generated"),
            |raw| resolve_path(config_dir, raw),
        );

        let schema_export_dir = match &runtime_storage.mediapm_schema_dir {
            None => Some(runtime_root.join("config").join("mediapm")),
            Some(None) => None,
            Some(Some(raw)) => Some(resolve_path(config_dir, raw)),
        };

        Self {
            root_dir: self.root_dir.clone(),
            mediapm_ncl: self.mediapm_ncl.clone(),
            conductor_user_ncl,
            conductor_machine_ncl,
            conductor_state_config,
            conductor_tmp_dir: tmp_dir.clone(),
            conductor_schema_dir,
            mediapm_state_ncl,
            env_file,
            env_generated_file,
            schema_export_dir,
            runtime_root: runtime_root.clone(),
            mediapm_tmp_dir: tmp_dir,
            hierarchy_root_dir: library_dir,
            tools_dir: runtime_root.join("tools"),
        }
    }
}

/// Returns an OS-backed temporary directory unique to this workspace.
///
/// Uses the workspace root path to derive a deterministic subdirectory name
/// under the system temp dir, ensuring concurrent runs on different workspaces
/// never collide on staging paths.
#[must_use]
fn default_runtime_tmp_dir(root_dir: &Path) -> PathBuf {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    root_dir.hash(&mut hasher);
    let key = format!("{:016x}", hasher.finish());
    std::env::temp_dir().join(format!("mediapm-{key}"))
}

/// Resolves one path value against a base directory unless it is absolute.
#[must_use]
fn resolve_path(base_dir: &Path, raw: &str) -> PathBuf {
    let candidate = PathBuf::from(raw);
    if candidate.is_absolute() { candidate } else { base_dir.join(candidate) }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::config::MediaRuntimeStorage;

    use super::{MediaPmPaths, default_runtime_tmp_dir};

    /// Ensures default runtime paths keep state under `.mediapm` while tmp
    /// uses OS-backed temp storage with per-workspace unique path and library
    /// remains config-rooted.
    #[test]
    fn default_paths_use_dot_mediapm_root() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(root.path());
        let expected_tmp_dir = default_runtime_tmp_dir(root.path());

        assert_eq!(paths.runtime_root, root.path().join(".mediapm"));
        assert_eq!(paths.hierarchy_root_dir, root.path());
        assert_eq!(paths.mediapm_tmp_dir, expected_tmp_dir.clone());
        assert_eq!(
            paths.conductor_state_config,
            root.path().join(".mediapm").join("state.conductor.ncl")
        );
        assert_eq!(paths.conductor_tmp_dir, expected_tmp_dir);
        assert_eq!(
            paths.conductor_schema_dir,
            root.path().join(".mediapm").join("config").join("conductor")
        );
        assert_eq!(paths.mediapm_state_ncl, root.path().join(".mediapm").join("state.ncl"));
        assert_eq!(paths.env_file, root.path().join(".mediapm").join(".env"));
        assert_eq!(paths.env_generated_file, root.path().join(".mediapm").join(".env.generated"));
        assert_eq!(
            paths.schema_export_dir,
            Some(root.path().join(".mediapm").join("config").join("mediapm"))
        );
        assert_eq!(paths.workspace_cache_dir(), root.path().join(".mediapm").join("cache"));
        assert_eq!(
            paths.workspace_cache_store_dir(),
            root.path().join(".mediapm").join("cache").join("store")
        );
        assert_eq!(
            paths.workspace_yt_dlp_cache_dir(),
            root.path().join(".mediapm").join("cache").join("yt-dlp")
        );
        assert_eq!(
            paths.workspace_media_tagger_cache_dir(),
            root.path().join(".mediapm").join("cache")
        );
        assert_eq!(paths.tools_dir, root.path().join(".mediapm").join("tools"));
    }

    /// Ensures `hierarchy_root_dir` remains config-root relative while
    /// `mediapm_tmp_dir`
    /// remains runtime-root relative.
    #[test]
    fn runtime_storage_overrides_resolve_with_split_roots() {
        let root = tempfile::tempdir().expect("tempdir");
        let base = MediaPmPaths::from_root(root.path());
        let runtime_storage = MediaRuntimeStorage {
            mediapm_dir: Some(".mediapm-runtime".to_string()),
            hierarchy_root_dir: Some("library-custom".to_string()),
            materialization_preference_order: None,
            conductor_config: Some("configs/custom.conductor.ncl".to_string()),
            conductor_machine_config: Some("configs/custom.conductor.machine.ncl".to_string()),
            conductor_state_config: Some("state/custom.state.ncl".to_string()),
            conductor_schema_dir: Some("schemas/conductor".to_string()),
            inherited_env_vars: None,
            instance_ttl_seconds: None,
            media_state_config: Some("state/custom.state.mediapm.ncl".to_string()),
            env_file: Some("state/custom.env".to_string()),
            env_generated_file: None,
            mediapm_schema_dir: Some(Some("schemas/mediapm".to_string())),
            profiler_enabled: None,
            verify_materialization: None,
            path_sanitization: None,
            verify_on_read: None,
            verify_on_read_sample_denominator: None,
            verify_on_read_stale_timeout_secs: None,
            reconstructed_bytes_cache_ttl_secs: None,
        };

        let resolved = base.with_runtime_storage(&runtime_storage);
        let expected_tmp_dir = default_runtime_tmp_dir(root.path());

        assert_eq!(resolved.runtime_root, root.path().join(".mediapm-runtime"));
        assert_eq!(resolved.hierarchy_root_dir, root.path().join("library-custom"));
        assert_eq!(resolved.mediapm_tmp_dir, expected_tmp_dir.clone());
        assert_eq!(resolved.conductor_tmp_dir, expected_tmp_dir);
        assert_eq!(resolved.conductor_schema_dir, root.path().join("schemas/conductor"));
        assert_eq!(
            resolved.conductor_user_ncl,
            root.path().join("configs").join("custom.conductor.ncl")
        );
        assert_eq!(
            resolved.conductor_machine_ncl,
            root.path().join("configs").join("custom.conductor.machine.ncl")
        );
        assert_eq!(
            resolved.conductor_state_config,
            root.path().join("state").join("custom.state.ncl")
        );
        assert_eq!(
            resolved.mediapm_state_ncl,
            root.path().join("state").join("custom.state.mediapm.ncl")
        );
        assert_eq!(resolved.env_file, root.path().join("state").join("custom.env"));
        assert_eq!(resolved.schema_export_dir, Some(root.path().join("schemas/mediapm")));
        assert_eq!(resolved.tools_dir, root.path().join(".mediapm-runtime").join("tools"));
    }

    /// Ensures overriding only `mediapm_dir` relocates all runtime-dependent
    /// paths while leaving config-rooted paths (conductor config files,
    /// hierarchy root) at their defaults.
    #[test]
    fn mediapm_dir_override_relocates_all_runtime_dependent_paths() {
        let root = tempfile::tempdir().expect("tempdir");
        let base = MediaPmPaths::from_root(root.path());
        let runtime_storage = MediaRuntimeStorage {
            mediapm_dir: Some(".custom-mediapm".to_string()),
            ..MediaRuntimeStorage::default()
        };

        let resolved = base.with_runtime_storage(&runtime_storage);
        let expected_tmp_dir = default_runtime_tmp_dir(root.path());

        // runtime-root paths use the overridden mediapm_dir
        let expected_runtime_root = root.path().join(".custom-mediapm");
        assert_eq!(resolved.runtime_root, expected_runtime_root);
        assert_eq!(resolved.tools_dir, expected_runtime_root.join("tools"));
        assert_eq!(
            resolved.conductor_state_config,
            expected_runtime_root.join("state.conductor.ncl")
        );
        assert_eq!(resolved.mediapm_state_ncl, expected_runtime_root.join("state.ncl"));
        assert_eq!(resolved.env_file, expected_runtime_root.join(".env"));
        assert_eq!(resolved.env_generated_file, expected_runtime_root.join(".env.generated"));
        assert_eq!(
            resolved.conductor_schema_dir,
            expected_runtime_root.join("config").join("conductor")
        );
        assert_eq!(
            resolved.schema_export_dir,
            Some(expected_runtime_root.join("config").join("mediapm"))
        );
        assert_eq!(resolved.workspace_cache_dir(), expected_runtime_root.join("cache"));
        assert_eq!(
            resolved.workspace_cache_store_dir(),
            expected_runtime_root.join("cache").join("store")
        );

        // config-rooted paths remain at defaults
        assert_eq!(resolved.mediapm_ncl, root.path().join("mediapm.ncl"));
        assert_eq!(resolved.conductor_user_ncl, root.path().join("mediapm.conductor.ncl"));
        assert_eq!(
            resolved.conductor_machine_ncl,
            root.path().join("mediapm.conductor.machine.ncl")
        );
        assert_eq!(resolved.hierarchy_root_dir, root.path());

        // tmp dirs unchanged
        assert_eq!(resolved.mediapm_tmp_dir, expected_tmp_dir);
        assert_eq!(resolved.conductor_tmp_dir, expected_tmp_dir);
    }

    /// Ensures an absolute `mediapm_dir` path is used as-is without
    /// resolving against the config directory.
    #[test]
    fn mediapm_dir_absolute_resolves_correctly() {
        let root = tempfile::tempdir().expect("tempdir");
        let base = MediaPmPaths::from_root(root.path());
        let runtime_storage = MediaRuntimeStorage {
            mediapm_dir: Some("/custom/absolute/mediapm".to_string()),
            ..MediaRuntimeStorage::default()
        };

        let resolved = base.with_runtime_storage(&runtime_storage);

        assert_eq!(resolved.runtime_root, PathBuf::from("/custom/absolute/mediapm"));
        assert_eq!(resolved.tools_dir, PathBuf::from("/custom/absolute/mediapm").join("tools"));
        assert_eq!(
            resolved.conductor_state_config,
            PathBuf::from("/custom/absolute/mediapm").join("state.conductor.ncl")
        );
    }

    /// Ensures `mediapm_dir = None` preserves the default `.mediapm`
    /// `runtime_root` even when other runtime-storage fields are overridden.
    #[test]
    fn mediapm_dir_none_preserves_default_runtime_root_with_other_overrides() {
        let root = tempfile::tempdir().expect("tempdir");
        let base = MediaPmPaths::from_root(root.path());
        let runtime_storage = MediaRuntimeStorage {
            mediapm_dir: None,
            conductor_state_config: Some("overridden/state.conductor.ncl".to_string()),
            env_file: Some("overridden/.env".to_string()),
            ..MediaRuntimeStorage::default()
        };

        let resolved = base.with_runtime_storage(&runtime_storage);

        // runtime_root stays as the .mediapm default
        assert_eq!(resolved.runtime_root, root.path().join(".mediapm"));
        assert_eq!(resolved.tools_dir, root.path().join(".mediapm").join("tools"));

        // overridden fields take effect
        assert_eq!(
            resolved.conductor_state_config,
            root.path().join("overridden").join("state.conductor.ncl")
        );
        assert_eq!(resolved.env_file, root.path().join("overridden").join(".env"));

        // non-overridden fields stay at defaults
        assert_eq!(resolved.mediapm_state_ncl, root.path().join(".mediapm").join("state.ncl"));
        assert_eq!(
            resolved.conductor_schema_dir,
            root.path().join(".mediapm").join("config").join("conductor")
        );
    }
}
