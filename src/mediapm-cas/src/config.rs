//! CAS configuration — storage locators, integrity settings, and the
//! [`ConfiguredCas`] dispatcher enum.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::ops::Deref;

use crate::api::{
    CasApi, CasMaintenanceApi, ConstraintApi, ConstraintPatch, ObjectMeta, OptimizeReport,
    PruneReport,
};
use crate::error::CasError;
use crate::hash::Hash;
use crate::storage::file_system::FileSystemCas;
use crate::storage::in_memory::InMemoryCas;

// ---------------------------------------------------------------------------
// CasIntegrityConfig
// ---------------------------------------------------------------------------

/// Configuration for CAS integrity verification.
///
/// No [`Default`] impl — default values are pushed to boundary callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasIntegrityConfig {
    /// Ordered list of trigger strategies.
    pub verify_on_read: Vec<crate::api::VerifyTriggerStrategy>,
}

impl CasIntegrityConfig {
    /// Returns `true` when at least one verify-on-read strategy is
    /// configured.
    pub fn should_verify_on_read(&self) -> bool {
        !self.verify_on_read.is_empty()
    }
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
        integrity: CasIntegrityConfig,
    ) -> Result<Self, CasError> {
        if locator == "memory" {
            return Ok(Self { storage_locator: CasStorageLocator::InMemory, integrity });
        }

        if opts.allow_plain_filesystem_path && !locator.is_empty() && !locator.contains("://") {
            return Ok(Self {
                storage_locator: CasStorageLocator::FileSystem {
                    path: Path::new(locator).to_path_buf(),
                },
                integrity,
            });
        }

        Err(CasError::InvalidArgument(format!("unsupported CAS locator: {locator}")))
    }

    /// Parse a locator string with default options and empty integrity config.
    pub fn from_locator(locator: &str) -> Result<Self, CasError> {
        Self::from_locator_with_options(
            locator,
            CasLocatorParseOptions { allow_plain_filesystem_path: true },
            CasIntegrityConfig { verify_on_read: Vec::new() },
        )
    }

    /// Open the configured CAS backend.
    pub async fn open(&self) -> Result<ConfiguredCas, CasError> {
        match &self.storage_locator {
            CasStorageLocator::InMemory => Ok(ConfiguredCas::InMemory(InMemoryCas::new())),
            CasStorageLocator::FileSystem { path } => Ok(ConfiguredCas::FileSystem(
                FileSystemCas::open_with_strategies(path, self.integrity.verify_on_read.clone())
                    .await?,
            )),
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
            Self::InMemory(cas) => cas.deref().$method($($arg),*).await,
            Self::FileSystem(cas) => cas.deref().$method($($arg),*).await,
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
    async fn run_maintenance_cycle(&self) -> Result<OptimizeReport, CasError> {
        forward!(self.run_maintenance_cycle().await)
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        forward!(self.prune_constraints().await)
    }

    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
        forward!(self.list_hashes().await)
    }
}

#[async_trait]
impl ConstraintApi for ConfiguredCas {
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        forward!(self.set_constraint(target, bases).await)
    }

    async fn get_constraint(&self, target: Hash) -> Result<BTreeSet<Hash>, CasError> {
        forward!(self.get_constraint(target).await)
    }

    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError> {
        forward!(self.patch_constraint(target, patch).await)
    }
}
