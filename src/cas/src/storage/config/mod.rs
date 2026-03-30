//! Storage backend configuration and runtime backend delegation.

use std::collections::BTreeSet;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use url::Url;

mod locator;
use locator::{expand_locator_env, normalize_locator_path, validate_filesystem_root_writable};

use crate::{
    CasApi, CasByteReader, CasByteStream, CasError, CasMaintenanceApi, Constraint, ConstraintPatch,
    FileSystemCas, Hash, InMemoryCas, IndexRepairReport, ObjectInfo, OptimizeOptions,
    OptimizeReport, PruneReport,
};

/// Default optimizer depth penalty for filesystem backend.
const DEFAULT_FILESYSTEM_ALPHA: u64 = 4;
/// Default maximum retained index backup snapshots.
const DEFAULT_INDEX_BACKUP_SNAPSHOT_LIMIT: usize = 4;
/// Default mutation-batch interval between backup snapshots.
const DEFAULT_INDEX_BACKUP_BATCH_INTERVAL_OPS: usize = 8;

/// Startup policy to apply when the primary filesystem index is missing or corrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexRecoveryMode {
    /// Fail startup when non-empty object files exist but the primary index cannot be trusted.
    Strict,
    /// Rebuild object metadata from disk and restore constraints from backups when possible.
    #[default]
    Recover,
}

/// Recovery and backup settings for the filesystem CAS backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSystemRecoveryOptions {
    /// Startup policy for missing/corrupt durable index state.
    pub mode: IndexRecoveryMode,
    /// Maximum number of backup snapshots to retain.
    ///
    /// Set to `0` to disable backup snapshot creation entirely.
    pub max_backup_snapshots: usize,
    /// Number of successful incremental index mutation batches between backup snapshots.
    ///
    /// Set to `1` to preserve per-mutation backup behavior.
    /// Values below `1` are normalized to `1` by filesystem runtime code.
    pub backup_snapshot_interval_ops: usize,
}

/// Default recovery profile used by filesystem backend constructors.
impl Default for FileSystemRecoveryOptions {
    fn default() -> Self {
        Self {
            mode: IndexRecoveryMode::Recover,
            max_backup_snapshots: DEFAULT_INDEX_BACKUP_SNAPSHOT_LIMIT,
            backup_snapshot_interval_ops: DEFAULT_INDEX_BACKUP_BATCH_INTERVAL_OPS,
        }
    }
}

/// Controls how locator strings are interpreted when opening CAS backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CasLocatorParseOptions {
    /// When `true`, locator strings without an explicit scheme are interpreted
    /// as filesystem paths. When `false`, a scheme is required.
    pub allow_plain_filesystem_path: bool,
}

/// Backend choice and initialization parameters for opening CAS storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasBackendConfig {
    /// In-memory backend, primarily for tests and ephemeral usage.
    InMemory,
    /// Filesystem backend rooted at `root` using optimizer penalty `alpha`.
    FileSystem {
        /// Root directory for persisted CAS objects and index database.
        root: PathBuf,
        /// Depth penalty coefficient used by optimizer scoring.
        alpha: u64,
    },
}

/// Locator parsing and backend-variant construction helpers.
impl CasBackendConfig {
    /// Constructs validated filesystem backend config with default alpha.
    fn filesystem(root: PathBuf) -> Result<Self, CasError> {
        validate_filesystem_root_writable(&root)?;
        Ok(Self::FileSystem { root, alpha: DEFAULT_FILESYSTEM_ALPHA })
    }

    /// Builds backend configuration from a locator string.
    ///
    /// Supported forms:
    /// - `cas://memory`
    /// - `cas:///absolute/path`
    /// - `cas://./relative/path`
    /// - `file:///absolute/path`
    /// - `file:./relative/path`
    ///
    /// Environment variables are expanded in locator strings using
    /// `$NAME`, `${NAME}`, or `%NAME%` forms.
    pub fn from_locator(locator: &str) -> Result<Self, CasError> {
        Self::from_locator_with_options(locator, CasLocatorParseOptions::default())
    }

    /// Builds backend configuration from a locator string with explicit parse options.
    pub fn from_locator_with_options(
        locator: &str,
        options: CasLocatorParseOptions,
    ) -> Result<Self, CasError> {
        if locator.trim().is_empty() {
            return Err(CasError::invalid_input("CAS locator cannot be empty"));
        }
        let locator = expand_locator_env(locator.trim())?;

        if let Some(rest) = locator.strip_prefix("cas://") {
            if rest.eq_ignore_ascii_case("memory") {
                return Ok(Self::InMemory);
            }

            if rest.trim().is_empty() {
                return Err(CasError::invalid_input(
                    "CAS locator scheme requires a backend target after cas://",
                ));
            }

            return Self::filesystem(normalize_locator_path(PathBuf::from(rest))?);
        }

        if let Some(relative_path) = locator.strip_prefix("file:")
            && !relative_path.starts_with("//")
        {
            return Self::filesystem(normalize_locator_path(PathBuf::from(relative_path))?);
        }

        if locator.contains("://") {
            let parsed = Url::parse(&locator).map_err(|err| {
                CasError::invalid_input(format!("invalid CAS locator URL '{locator}': {err}"))
            })?;

            if parsed.scheme() == "file" {
                let file_path = parsed.to_file_path().map_err(|_| {
                    CasError::invalid_input(format!(
                        "invalid file locator '{locator}': must resolve to a local filesystem path"
                    ))
                })?;
                return Self::filesystem(normalize_locator_path(file_path)?);
            }

            return Err(CasError::invalid_input(format!(
                "unsupported CAS locator scheme '{}': use cas://memory, cas:///path, or file:///path",
                parsed.scheme()
            )));
        }

        if options.allow_plain_filesystem_path {
            return Self::filesystem(normalize_locator_path(PathBuf::from(locator))?);
        }

        Err(CasError::invalid_input(
            "plain filesystem locators are disabled; use cas:///path or file:///path, or enable allow_plain_filesystem_path",
        ))
    }
}

/// Open-configuration wrapper for CAS backend creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasConfig {
    backend: CasBackendConfig,
    filesystem_recovery: FileSystemRecoveryOptions,
}

/// Builder-style constructors and open methods for configured backends.
impl CasConfig {
    /// Creates configuration for in-memory backend.
    pub const fn in_memory() -> Self {
        Self {
            backend: CasBackendConfig::InMemory,
            filesystem_recovery: FileSystemRecoveryOptions {
                mode: IndexRecoveryMode::Recover,
                max_backup_snapshots: DEFAULT_INDEX_BACKUP_SNAPSHOT_LIMIT,
                backup_snapshot_interval_ops: DEFAULT_INDEX_BACKUP_BATCH_INTERVAL_OPS,
            },
        }
    }

    /// Creates configuration for filesystem backend with default alpha (`4`).
    pub fn filesystem(root: impl Into<PathBuf>) -> Self {
        Self {
            backend: CasBackendConfig::FileSystem {
                root: root.into(),
                alpha: DEFAULT_FILESYSTEM_ALPHA,
            },
            filesystem_recovery: FileSystemRecoveryOptions::default(),
        }
    }

    /// Creates configuration for filesystem backend with explicit alpha.
    pub fn filesystem_with_alpha(root: impl Into<PathBuf>, alpha: u64) -> Self {
        Self {
            backend: CasBackendConfig::FileSystem { root: root.into(), alpha },
            filesystem_recovery: FileSystemRecoveryOptions::default(),
        }
    }

    /// Creates configuration for filesystem backend with explicit alpha and recovery settings.
    pub fn filesystem_with_alpha_and_recovery(
        root: impl Into<PathBuf>,
        alpha: u64,
        filesystem_recovery: FileSystemRecoveryOptions,
    ) -> Self {
        Self {
            backend: CasBackendConfig::FileSystem { root: root.into(), alpha },
            filesystem_recovery,
        }
    }

    /// Parses a configuration from a locator string.
    pub fn from_locator(locator: &str) -> Result<Self, CasError> {
        Ok(Self {
            backend: CasBackendConfig::from_locator_with_options(
                locator,
                CasLocatorParseOptions::default(),
            )?,
            filesystem_recovery: FileSystemRecoveryOptions::default(),
        })
    }

    /// Parses a configuration from a locator string using explicit parse options.
    pub fn from_locator_with_options(
        locator: &str,
        options: CasLocatorParseOptions,
    ) -> Result<Self, CasError> {
        Ok(Self {
            backend: CasBackendConfig::from_locator_with_options(locator, options)?,
            filesystem_recovery: FileSystemRecoveryOptions::default(),
        })
    }

    /// Overrides filesystem recovery behavior for this configuration.
    #[must_use]
    pub fn with_filesystem_recovery(
        mut self,
        filesystem_recovery: FileSystemRecoveryOptions,
    ) -> Self {
        self.filesystem_recovery = filesystem_recovery;
        self
    }

    /// Opens the configured backend.
    pub async fn open(self) -> Result<ConfiguredCas, CasError> {
        match self.backend {
            CasBackendConfig::InMemory => Ok(ConfiguredCas::InMemory(InMemoryCas::new())),
            CasBackendConfig::FileSystem { root, alpha } => {
                let cas = FileSystemCas::open_with_alpha_and_recovery(
                    root,
                    alpha,
                    self.filesystem_recovery,
                )
                .await?;
                Ok(ConfiguredCas::FileSystem(cas))
            }
        }
    }
}

/// Runtime CAS backend selected from [`CasConfig`].
pub enum ConfiguredCas {
    /// In-memory backend variant.
    InMemory(InMemoryCas),
    /// Filesystem backend variant.
    FileSystem(FileSystemCas),
}

/// Delegates one backend operation across configured runtime variants.
///
/// This macro intentionally keeps trait impls concise while preserving one
/// exhaustiveness point for backend-variant dispatch.
macro_rules! delegate_configured_backend {
    ($backend:expr, |$cas:ident| $operation:expr) => {
        match $backend {
            ConfiguredCas::InMemory($cas) => $operation,
            ConfiguredCas::FileSystem($cas) => $operation,
        }
    };
}

#[async_trait]
/// Unified [`CasApi`] dispatch over configured backend variants.
impl CasApi for ConfiguredCas {
    async fn exists(&self, hash: Hash) -> Result<bool, CasError> {
        delegate_configured_backend!(self, |cas| cas.exists(hash).await)
    }

    async fn put<D>(&self, data: D) -> Result<Hash, CasError>
    where
        D: TryInto<Bytes> + Send,
        D::Error: std::fmt::Display + Send,
    {
        delegate_configured_backend!(self, |cas| cas.put(data).await)
    }

    async fn put_with_constraints<D>(
        &self,
        data: D,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError>
    where
        D: TryInto<Bytes> + Send,
        D::Error: std::fmt::Display + Send,
    {
        delegate_configured_backend!(self, |cas| cas.put_with_constraints(data, bases).await)
    }

    async fn put_stream(&self, reader: CasByteReader) -> Result<Hash, CasError> {
        delegate_configured_backend!(self, |cas| cas.put_stream(reader).await)
    }

    async fn put_stream_with_constraints(
        &self,
        reader: CasByteReader,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError> {
        delegate_configured_backend!(self, |cas| cas
            .put_stream_with_constraints(reader, bases)
            .await)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        delegate_configured_backend!(self, |cas| cas.get(hash).await)
    }

    async fn get_stream(&self, hash: Hash) -> Result<CasByteStream, CasError> {
        delegate_configured_backend!(self, |cas| cas.get_stream(hash).await)
    }

    async fn info(&self, hash: Hash) -> Result<ObjectInfo, CasError> {
        delegate_configured_backend!(self, |cas| cas.info(hash).await)
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        delegate_configured_backend!(self, |cas| cas.delete(hash).await)
    }

    async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError> {
        delegate_configured_backend!(self, |cas| cas.set_constraint(constraint).await)
    }

    async fn patch_constraint(
        &self,
        target_hash: Hash,
        patch: ConstraintPatch,
    ) -> Result<Option<Constraint>, CasError> {
        delegate_configured_backend!(self, |cas| cas.patch_constraint(target_hash, patch).await)
    }

    async fn get_constraint(&self, hash: Hash) -> Result<Option<Constraint>, CasError> {
        delegate_configured_backend!(self, |cas| cas.get_constraint(hash).await)
    }
}

#[async_trait]
/// Unified maintenance dispatch over configured backend variants.
impl CasMaintenanceApi for ConfiguredCas {
    async fn optimize_once(&self, options: OptimizeOptions) -> Result<OptimizeReport, CasError> {
        delegate_configured_backend!(self, |cas| cas.optimize_once(options).await)
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        delegate_configured_backend!(self, |cas| cas.prune_constraints().await)
    }

    async fn repair_index(&self) -> Result<IndexRepairReport, CasError> {
        delegate_configured_backend!(self, |cas| cas.repair_index().await)
    }

    async fn migrate_index_to_version(&self, target_version: u32) -> Result<(), CasError> {
        delegate_configured_backend!(self, |cas| cas.migrate_index_to_version(target_version).await)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use url::Url;

    use super::{
        CasBackendConfig, CasConfig, CasLocatorParseOptions, ConfiguredCas,
        FileSystemRecoveryOptions, IndexRecoveryMode,
    };

    #[test]
    fn locator_parser_requires_explicit_scheme_by_default() {
        let memory = CasBackendConfig::from_locator("cas://memory").expect("memory locator");
        assert_eq!(memory, CasBackendConfig::InMemory);

        let file_dir = tempfile::tempdir().expect("tempdir");
        let root_url = Url::from_directory_path(file_dir.path())
            .expect("directory path must map to file:// URL");
        let fs_from_file_url = CasBackendConfig::from_locator(root_url.as_str())
            .expect("file url locator must resolve to filesystem backend");
        assert!(matches!(fs_from_file_url, CasBackendConfig::FileSystem { .. }));

        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("cas-repo");

        let plain = CasBackendConfig::from_locator(root.to_str().expect("utf-8 path"));
        assert!(plain.is_err(), "plain paths should be rejected unless explicitly allowed");

        let fs = CasBackendConfig::from_locator_with_options(
            root.to_str().expect("utf-8 path"),
            CasLocatorParseOptions { allow_plain_filesystem_path: true },
        )
        .expect("plain path locator with explicit fallback option");
        match fs {
            CasBackendConfig::FileSystem { root, alpha } => {
                assert!(root.ends_with("cas-repo"));
                assert_eq!(alpha, 4);
            }
            CasBackendConfig::InMemory => panic!("expected filesystem config"),
        }
    }

    #[test]
    fn locator_parser_resolves_relative_paths_to_current_directory() {
        let cfg =
            CasBackendConfig::from_locator("cas://./target/cas-rel").expect("relative cas locator");

        let CasBackendConfig::FileSystem { root, .. } = cfg else {
            panic!("expected filesystem backend");
        };

        let cwd = std::env::current_dir().expect("current_dir");
        assert!(root.starts_with(cwd));
        assert!(root.ends_with("target\\cas-rel") || root.ends_with("target/cas-rel"));
    }

    #[test]
    fn locator_parser_expands_environment_variables() {
        let (name, value) = std::env::vars()
            .find(|(_, value)| !value.is_empty())
            .expect("at least one non-empty environment variable");

        let locator = format!("cas://${{{name}}}/mediapm-cas-repo");
        let cfg = CasBackendConfig::from_locator(&locator).expect("env-expanded cas locator");
        let CasBackendConfig::FileSystem { root, .. } = cfg else {
            panic!("expected filesystem backend");
        };

        assert!(
            root.to_string_lossy().contains(&value),
            "expanded locator root should include source environment value"
        );
        assert!(root.to_string_lossy().contains("mediapm-cas-repo"));
    }

    #[tokio::test]
    async fn config_open_builds_selected_backend() {
        let memory = CasConfig::in_memory().open().await.expect("open in-memory cas");
        assert!(matches!(memory, ConfiguredCas::InMemory(_)));

        let dir = tempdir().expect("tempdir");
        let filesystem = CasConfig::filesystem(dir.path()).open().await.expect("open fs cas");
        assert!(matches!(filesystem, ConfiguredCas::FileSystem(_)));
    }

    #[test]
    fn config_can_override_filesystem_recovery() {
        let cfg = CasConfig::filesystem_with_alpha_and_recovery(
            std::path::PathBuf::from("repo"),
            7,
            FileSystemRecoveryOptions {
                mode: IndexRecoveryMode::Strict,
                max_backup_snapshots: 9,
                backup_snapshot_interval_ops: 3,
            },
        );

        assert!(matches!(cfg.backend, CasBackendConfig::FileSystem { alpha: 7, .. }));
        assert_eq!(cfg.filesystem_recovery.mode, IndexRecoveryMode::Strict);
        assert_eq!(cfg.filesystem_recovery.max_backup_snapshots, 9);
        assert_eq!(cfg.filesystem_recovery.backup_snapshot_interval_ops, 3);
    }
}
