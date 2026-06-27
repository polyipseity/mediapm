//! Filesystem path layout helpers for `mediapm` state.
//!
//! This module centralizes where `mediapm` keeps user policy, conductor-facing
//! Nickel documents, machine-managed state, and staging directories.

use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

/// Path-override parameters resolved from `mediapm.ncl` runtime storage.
///
/// Optional fields represent config-level overrides; absent fields fall back
/// to defaults computed by [`MediaPmPaths`].
pub struct MediaPmPathOverrides {
    /// Override for `mediapm_dir`.
    pub mediapm_dir: Option<PathBuf>,
    /// Override for `hierarchy_root_dir`.
    pub hierarchy_root_dir: Option<PathBuf>,
    /// Override for conductor user config path.
    pub conductor_config: Option<PathBuf>,
    /// Override for conductor machine/generated config path.
    pub conductor_generated_config: Option<PathBuf>,
    /// Override for conductor state config path.
    pub conductor_state_config: Option<PathBuf>,
    /// Override for conductor schema dir.
    pub conductor_schema_dir: Option<PathBuf>,
    /// Override for mediapm state config path.
    pub media_state_config: Option<PathBuf>,
    /// Override for env file path.
    pub env_file: Option<PathBuf>,
    /// Override for env generated file path.
    pub env_generated_file: Option<PathBuf>,
    /// Override for mediapm schema export dir (`None` = disable export).
    #[allow(clippy::option_option)]
    pub mediapm_schema_dir: Option<Option<PathBuf>>,
}

/// Canonical path bundle for one `mediapm` workspace root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaPmPaths {
    /// Root directory that contains all `mediapm` files.
    pub root_dir: PathBuf,
    /// User-edited `mediapm` policy/config Nickel document.
    pub mediapm_ncl: PathBuf,
    /// Conductor user Nickel document used by mediapm orchestration.
    pub conductor_user_ncl: PathBuf,
    /// Conductor machine/generated Nickel document.
    pub conductor_machine_ncl: PathBuf,
    /// Conductor volatile runtime state document path.
    pub conductor_state_config: PathBuf,
    /// Conductor execution-sandbox temporary directory.
    pub conductor_tmp_dir: PathBuf,
    /// Conductor schema export directory.
    pub conductor_schema_dir: PathBuf,
    /// `mediapm` machine-managed state document (`state.ncl`).
    pub mediapm_state_ncl: PathBuf,
    /// Runtime dotenv file for local credential overrides.
    pub env_file: PathBuf,
    /// Machine-generated runtime dotenv file.
    pub env_generated_file: PathBuf,
    /// Runtime root for `mediapm`-owned state under the workspace.
    pub runtime_root: PathBuf,
    /// Embedded Nickel schema export directory for `mediapm.ncl` contracts.
    /// `None` means schema export is disabled.
    pub schema_export_dir: Option<PathBuf>,
    /// Temporary staging directory used by atomic sync.
    pub mediapm_tmp_dir: PathBuf,
    /// Materialized media library root.
    pub hierarchy_root_dir: PathBuf,
    /// Tool-content unpack cache under the workspace runtime root.
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
    #[must_use]
    pub fn workspace_media_tagger_cache_dir(&self) -> PathBuf {
        self.workspace_cache_dir()
    }

    /// Returns the mediapm metadata cache root (`<runtime>/cache/mediapm`).
    #[must_use]
    pub fn workspace_mediapm_cache_dir(&self) -> PathBuf {
        self.workspace_cache_dir().join("mediapm")
    }

    /// Applies path overrides from `mediapm.ncl` runtime storage.
    ///
    /// Resolution rules:
    /// - `mediapm_dir` is resolved relative to the outermost `mediapm.ncl`
    ///   directory when provided as a relative path,
    /// - `hierarchy_root_dir` resolves relative to the outermost
    ///   `mediapm.ncl` directory,
    /// - conductor config paths resolve relative to the outermost
    ///   `mediapm.ncl` directory,
    /// - omitted fields keep defaults.
    #[must_use]
    pub fn with_overrides(&self, overrides: &MediaPmPathOverrides) -> Self {
        let config_dir = self.mediapm_ncl.parent().unwrap_or(&self.root_dir);

        let runtime_root = overrides
            .mediapm_dir
            .clone()
            .map_or_else(|| self.runtime_root.clone(), |raw| resolve_path(config_dir, raw));

        let hierarchy_root_dir = overrides
            .hierarchy_root_dir
            .clone()
            .map_or_else(|| config_dir.to_path_buf(), |raw| resolve_path(config_dir, raw));

        let tmp_dir = default_runtime_tmp_dir(&self.root_dir);

        let conductor_user_ncl = overrides
            .conductor_config
            .clone()
            .map_or_else(|| self.conductor_user_ncl.clone(), |raw| resolve_path(config_dir, raw));

        let conductor_machine_ncl = overrides.conductor_generated_config.clone().map_or_else(
            || self.conductor_machine_ncl.clone(),
            |raw| resolve_path(config_dir, raw),
        );

        let conductor_state_config = overrides.conductor_state_config.clone().map_or_else(
            || runtime_root.join("state.conductor.ncl"),
            |raw| resolve_path(config_dir, raw),
        );

        let conductor_schema_dir = overrides.conductor_schema_dir.clone().map_or_else(
            || runtime_root.join("config").join("conductor"),
            |raw| resolve_path(config_dir, raw),
        );

        let mediapm_state_ncl = overrides
            .media_state_config
            .clone()
            .map_or_else(|| runtime_root.join("state.ncl"), |raw| resolve_path(config_dir, raw));

        let env_file = overrides
            .env_file
            .clone()
            .map_or_else(|| runtime_root.join(".env"), |raw| resolve_path(config_dir, raw));

        let env_generated_file = overrides.env_generated_file.clone().map_or_else(
            || runtime_root.join(".env.generated"),
            |raw| resolve_path(config_dir, raw),
        );

        let schema_export_dir = match &overrides.mediapm_schema_dir {
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
            hierarchy_root_dir,
            tools_dir: runtime_root.join("tools"),
        }
    }
}

/// Returns an OS-backed temporary directory unique to this workspace.
#[must_use]
fn default_runtime_tmp_dir(root_dir: &Path) -> PathBuf {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    root_dir.hash(&mut hasher);
    let key = format!("{:016x}", hasher.finish());
    std::env::temp_dir().join(format!("mediapm-{key}"))
}

/// Resolves one path value against a base directory unless it is absolute.
#[must_use]
fn resolve_path(base_dir: &Path, raw: impl AsRef<Path>) -> PathBuf {
    let raw = raw.as_ref();
    if raw.is_absolute() { raw.to_path_buf() } else { base_dir.join(raw) }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{MediaPmPathOverrides, MediaPmPaths, default_runtime_tmp_dir};

    #[test]
    fn default_paths_use_dot_mediapm_root() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(root.path());
        let expected_tmp_dir = default_runtime_tmp_dir(root.path());

        assert_eq!(paths.runtime_root, root.path().join(".mediapm"));
        assert_eq!(paths.hierarchy_root_dir, root.path());
        assert_eq!(paths.mediapm_tmp_dir, expected_tmp_dir);
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
        assert_eq!(paths.tools_dir, root.path().join(".mediapm").join("tools"));
    }

    #[test]
    fn overrides_resolve_with_split_roots() {
        let root = tempfile::tempdir().expect("tempdir");
        let base = MediaPmPaths::from_root(root.path());
        let overrides = MediaPmPathOverrides {
            mediapm_dir: Some(PathBuf::from(".mediapm-runtime")),
            hierarchy_root_dir: Some(PathBuf::from("library-custom")),
            conductor_config: Some(PathBuf::from("configs/custom.conductor.ncl")),
            conductor_generated_config: Some(PathBuf::from("configs/custom.conductor.machine.ncl")),
            conductor_state_config: Some(PathBuf::from("state/custom.state.ncl")),
            conductor_schema_dir: Some(PathBuf::from("schemas/conductor")),
            media_state_config: Some(PathBuf::from("state/custom.state.mediapm.ncl")),
            env_file: Some(PathBuf::from("state/custom.env")),
            env_generated_file: None,
            mediapm_schema_dir: Some(Some(PathBuf::from("schemas/mediapm"))),
        };

        let resolved = base.with_overrides(&overrides);
        let expected_tmp_dir = default_runtime_tmp_dir(root.path());

        assert_eq!(resolved.runtime_root, root.path().join(".mediapm-runtime"));
        assert_eq!(resolved.hierarchy_root_dir, root.path().join("library-custom"));
        assert_eq!(resolved.mediapm_tmp_dir, expected_tmp_dir);
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

        let resolved_no_schema =
            MediaPmPaths::from_root(root.path()).with_overrides(&MediaPmPathOverrides {
                mediapm_schema_dir: Some(None),
                ..MediaPmPathOverrides {
                    mediapm_dir: None,
                    hierarchy_root_dir: None,
                    conductor_config: None,
                    conductor_generated_config: None,
                    conductor_state_config: None,
                    conductor_schema_dir: None,
                    media_state_config: None,
                    env_file: None,
                    env_generated_file: None,
                    mediapm_schema_dir: Some(None),
                }
            });
        assert_eq!(resolved_no_schema.schema_export_dir, None);
    }
}
