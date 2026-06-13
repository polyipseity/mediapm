//! CAS configuration — storage locators, integrity settings, and the
//! [`ConfiguredCas`] dispatcher enum.

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;

use crate::api::{
    CasApi, CasMaintenanceApi, ConstraintApi, ConstraintPatch, IndexRepairReport, ObjectMeta,
    OptimizeReport, PruneReport,
};
use crate::error::CasError;
use crate::hash::Hash;
use crate::storage::file_system::FileSystemCas;
use crate::storage::in_memory::InMemoryCas;

// ---------------------------------------------------------------------------
// VerifyTriggerStrategy
// ---------------------------------------------------------------------------

/// Strategy for triggering CAS integrity verification on read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyTriggerStrategy {
    /// Verify every object.
    Always,
    /// Verify only if metadata suggests modification.
    Modified,
    /// Verify a 1-in-N sample of objects.
    Sample { denominator: u32 },
    /// Verify only if the cache entry is older than a threshold.
    Stale { timeout: Duration },
}

// ---------------------------------------------------------------------------
// CasIntegrityConfig
// ---------------------------------------------------------------------------

/// Configuration for CAS integrity verification.
///
/// No [`Default`] impl — default values are pushed to boundary callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasIntegrityConfig {
    /// Ordered list of trigger strategies.
    pub verify_on_read: Vec<VerifyTriggerStrategy>,
    /// TTL for cache of reconstructed delta bytes.
    pub reconstructed_bytes_cache_ttl: Duration,
}

// ---------------------------------------------------------------------------
// CasStorageLocator
// ---------------------------------------------------------------------------

/// Resolved storage location for a CAS backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasStorageLocator {
    /// In-memory (ephemeral) store.
    InMemory,
    /// File-system-backed store at the given path.
    FileSystem { path: PathBuf },
}

// ---------------------------------------------------------------------------
// CasLocatorParseOptions
// ---------------------------------------------------------------------------

/// Options for parsing CAS locator strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasLocatorParseOptions {
    /// When true, a plain filesystem path (not prefixed with a scheme) is
    /// accepted as a [`CasStorageLocator::FileSystem`].
    pub allow_plain_filesystem_path: bool,
}

// ---------------------------------------------------------------------------
// CasConfig
// ---------------------------------------------------------------------------

/// A single CAS configuration object.
///
/// All fields are required; default values are pushed to boundary callers.
/// No [`Default`] impl — construct explicitly or use
/// [`from_locator_with_options`](Self::from_locator_with_options).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasConfig {
    /// Which storage backend to use.
    pub storage_locator: CasStorageLocator,
    /// Integrity verification settings.
    pub integrity: CasIntegrityConfig,
}

impl CasConfig {
    /// Parse a locator string into a [`CasConfig`].
    ///
    /// Recognized schemes:
    /// - `"memory"` → [`CasStorageLocator::InMemory`]
    /// - A filesystem path (when `opts.allow_plain_filesystem_path` is true)
    ///   → [`CasStorageLocator::FileSystem`]
    pub fn from_locator_with_options(
        locator: &str,
        opts: CasLocatorParseOptions,
    ) -> Result<Self, CasError> {
        if locator == "memory" {
            return Ok(Self {
                storage_locator: CasStorageLocator::InMemory,
                integrity: CasIntegrityConfig {
                    verify_on_read: Vec::new(),
                    reconstructed_bytes_cache_ttl: Duration::from_secs(300),
                },
            });
        }

        if opts.allow_plain_filesystem_path && !locator.is_empty() && !locator.contains("://") {
            return Ok(Self {
                storage_locator: CasStorageLocator::FileSystem {
                    path: Path::new(locator).to_path_buf(),
                },
                integrity: CasIntegrityConfig {
                    verify_on_read: Vec::new(),
                    reconstructed_bytes_cache_ttl: Duration::from_secs(300),
                },
            });
        }

        Err(CasError::InvalidArgument(format!("unsupported CAS locator: {locator}")))
    }

    /// Open the configured CAS backend.
    pub async fn open(&self) -> Result<ConfiguredCas, CasError> {
        match &self.storage_locator {
            CasStorageLocator::InMemory => Ok(ConfiguredCas::InMemory(InMemoryCas::new())),
            CasStorageLocator::FileSystem { path } => {
                Ok(ConfiguredCas::FileSystem(FileSystemCas::open(path).await?))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ConfiguredCas
// ---------------------------------------------------------------------------

/// Enum over all configured CAS backends.
///
/// Implements [`CasApi`], [`CasMaintenanceApi`], and [`ConstraintApi`] by
/// forwarding to the inner variant.
#[derive(Clone)]
pub enum ConfiguredCas {
    /// In-memory (ephemeral) backend.
    InMemory(InMemoryCas),
    /// File-system-backed backend.
    FileSystem(FileSystemCas),
}

/// Forwards a method call to each variant of [`ConfiguredCas`].
macro_rules! forward {
    ($self:ident.$method:ident($($arg:ident),*).await) => {
        match $self {
            Self::InMemory(cas) => cas.0.$method($($arg),*).await,
            Self::FileSystem(cas) => cas.0.$method($($arg),*).await,
        }
    };
}

#[async_trait]
impl CasApi for ConfiguredCas {
    async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        forward!(self.put(data).await)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        forward!(self.get(hash).await)
    }

    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError> {
        forward!(self.stat(hash).await)
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        forward!(self.delete(hash).await)
    }
}

#[async_trait]
impl CasMaintenanceApi for ConfiguredCas {
    async fn optimize_once(&self) -> Result<OptimizeReport, CasError> {
        forward!(self.optimize_once().await)
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        forward!(self.prune_constraints().await)
    }

    async fn list_all_hashes(&self) -> Result<Vec<Hash>, CasError> {
        forward!(self.list_all_hashes().await)
    }

    async fn repair_index(&self) -> Result<IndexRepairReport, CasError> {
        forward!(self.repair_index().await)
    }
}

#[async_trait]
impl ConstraintApi for ConfiguredCas {
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        forward!(self.set_constraint(target, bases).await)
    }

    async fn get_constraint(&self, target: Hash) -> Result<Option<BTreeSet<Hash>>, CasError> {
        forward!(self.get_constraint(target).await)
    }

    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError> {
        forward!(self.patch_constraint(target, patch).await)
    }
}
