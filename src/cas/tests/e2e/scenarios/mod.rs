//! Domain-grouped end-to-end scenario suite for Phase 1 CAS.
//!
//! This module converts long-form user scenarios into executable tests,
//! grouped by workflow domain so each file stays focused and maintainable.
//!
//! ## Domain grouping
//!
//! - Ingest and stream workflows
//! - Constraint and validation workflows
//! - Delete and reconstructability workflows
//! - Orchestration and wire-command workflows
//! - Recovery and migration workflows
//!
//! ## Backend policy
//!
//! Unless a scenario explicitly depends on durable index files/actors,
//! scenarios execute against both exposed storage backends:
//! [`FileSystemCas`] and [`InMemoryCas`].

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use mediapm_cas::{
    CasApi, CasByteReader, CasByteStream, CasError, CasMaintenanceApi, CasWireCommand,
    CasWireResponse, Constraint, ConstraintPatch, FileSystemCas, FileSystemRecoveryOptions, Hash,
    InMemoryCas, IndexRecoveryMode, OptimizeOptions, spawn_cas_node_actor, spawn_index_actor,
    spawn_optimizer_actor, spawn_storage_actor, spawn_storage_actor_with_dependencies,
};
use redb::{Database, TableDefinition};
use tempfile::{TempDir, tempdir};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

use super::run_with_15s_timeout;

mod constraints_and_validation;
mod delete_and_reconstructability;
mod ingest_and_stream;
mod orchestration_and_wire;
mod recovery_and_migration;

/// Redb table used by recovery scenarios that intentionally remove index rows.
const PRIMARY_INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("primary_index");

/// Global lock for tests that assert coarse runtime budgets.
///
/// Serializing budget-sensitive tests reduces scheduler noise and keeps
/// quantitative checks reproducible across slower CI environments.
fn budget_sensitive_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Supported backend kinds for scenario parity coverage.
#[derive(Debug, Clone, Copy)]
enum BackendKind {
    /// Filesystem-backed backend with durable index/files.
    FileSystem,
    /// In-memory backend for fast semantic parity checks.
    InMemory,
}

/// Helper exposing deterministic backend iteration order.
impl BackendKind {
    /// Returns all backend kinds covered by parity scenarios.
    const fn all() -> [Self; 2] {
        [Self::FileSystem, Self::InMemory]
    }

    /// Human-readable backend label used in assertion diagnostics.
    const fn label(self) -> &'static str {
        match self {
            Self::FileSystem => "filesystem",
            Self::InMemory => "in-memory",
        }
    }
}

/// Test harness unifying operations across storage backends.
///
/// The enum keeps temporary directories alive for filesystem scenarios while
/// still allowing compact cross-backend scenario code.
enum BackendHarness {
    /// Filesystem backend variant.
    FileSystem {
        /// CAS handle under test.
        cas: FileSystemCas,
        /// Temporary root retained for full test lifetime.
        _root: TempDir,
    },
    /// In-memory backend variant.
    InMemory {
        /// CAS handle under test.
        cas: InMemoryCas,
    },
}

/// Backend harness constructors and operation adapters.
impl BackendHarness {
    /// Creates one backend harness for the selected kind.
    async fn new(kind: BackendKind) -> Self {
        match kind {
            BackendKind::FileSystem => {
                let root = tempdir().expect("tempdir");
                let cas =
                    FileSystemCas::open_with_alpha_for_tests(root.path(), 0).await.expect("open");
                Self::FileSystem { cas, _root: root }
            }
            BackendKind::InMemory => Self::InMemory { cas: InMemoryCas::new() },
        }
    }

    /// Returns user-facing label for diagnostics.
    fn label(&self) -> &'static str {
        match self {
            Self::FileSystem { .. } => BackendKind::FileSystem.label(),
            Self::InMemory { .. } => BackendKind::InMemory.label(),
        }
    }

    /// Delegates [`CasApi::put`] for the active backend.
    async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.put(data).await,
            Self::InMemory { cas } => cas.put(data).await,
        }
    }

    /// Delegates [`CasApi::put_with_constraints`] for the active backend.
    async fn put_with_constraints(
        &self,
        data: Bytes,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.put_with_constraints(data, bases).await,
            Self::InMemory { cas } => cas.put_with_constraints(data, bases).await,
        }
    }

    /// Delegates [`CasApi::put_stream`] using a repeat-byte reader.
    async fn put_stream_repeat(&self, byte: u8, len: usize) -> Result<Hash, CasError> {
        let reader: CasByteReader = Box::new(tokio::io::repeat(byte).take(len as u64));
        match self {
            Self::FileSystem { cas, .. } => cas.put_stream(reader).await,
            Self::InMemory { cas } => cas.put_stream(reader).await,
        }
    }

    /// Delegates [`CasApi::put_stream_with_constraints`] with repeat-byte input.
    async fn put_stream_repeat_with_constraints(
        &self,
        byte: u8,
        len: usize,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError> {
        let reader: CasByteReader = Box::new(tokio::io::repeat(byte).take(len as u64));
        match self {
            Self::FileSystem { cas, .. } => cas.put_stream_with_constraints(reader, bases).await,
            Self::InMemory { cas } => cas.put_stream_with_constraints(reader, bases).await,
        }
    }

    /// Delegates [`CasApi::exists`] for the active backend.
    async fn exists(&self, hash: Hash) -> Result<bool, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.exists(hash).await,
            Self::InMemory { cas } => cas.exists(hash).await,
        }
    }

    /// Delegates [`CasApi::exists_many`] for the active backend.
    async fn exists_many(&self, hashes: Vec<Hash>) -> Result<Vec<bool>, CasError> {
        let bitmap = match self {
            Self::FileSystem { cas, .. } => cas.exists_many(hashes).await,
            Self::InMemory { cas } => cas.exists_many(hashes).await,
        }?;
        Ok(bitmap.iter().by_vals().collect())
    }

    /// Delegates [`CasApi::get`] for the active backend.
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.get(hash).await,
            Self::InMemory { cas } => cas.get(hash).await,
        }
    }

    /// Delegates [`CasApi::get_stream`] and collects bytes for comparison.
    async fn get_stream_bytes(&self, hash: Hash) -> Result<Bytes, CasError> {
        let stream = match self {
            Self::FileSystem { cas, .. } => cas.get_stream(hash).await,
            Self::InMemory { cas } => cas.get_stream(hash).await,
        }?;
        collect_stream(stream).await
    }

    /// Delegates [`CasApi::get_many`] for the active backend.
    async fn get_many(&self, hashes: Vec<Hash>) -> Result<Vec<(Hash, Bytes)>, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.get_many(hashes).await,
            Self::InMemory { cas } => cas.get_many(hashes).await,
        }
    }

    /// Delegates [`CasApi::info`] for the active backend.
    async fn info(&self, hash: Hash) -> Result<mediapm_cas::ObjectInfo, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.info(hash).await,
            Self::InMemory { cas } => cas.info(hash).await,
        }
    }

    /// Delegates [`CasApi::info_many`] for the active backend.
    async fn info_many(
        &self,
        hashes: Vec<Hash>,
    ) -> Result<Vec<(Hash, mediapm_cas::ObjectInfo)>, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.info_many(hashes).await,
            Self::InMemory { cas } => cas.info_many(hashes).await,
        }
    }

    /// Delegates [`CasApi::delete`] for the active backend.
    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.delete(hash).await,
            Self::InMemory { cas } => cas.delete(hash).await,
        }
    }

    /// Delegates [`CasApi::delete_many`] for the active backend.
    async fn delete_many(&self, hashes: Vec<Hash>) -> Result<(), CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.delete_many(hashes).await,
            Self::InMemory { cas } => cas.delete_many(hashes).await,
        }
    }

    /// Delegates [`CasApi::set_constraint`] for the active backend.
    async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.set_constraint(constraint).await,
            Self::InMemory { cas } => cas.set_constraint(constraint).await,
        }
    }

    /// Delegates [`CasApi::patch_constraint`] for the active backend.
    async fn patch_constraint(
        &self,
        target_hash: Hash,
        patch: ConstraintPatch,
    ) -> Result<Option<Constraint>, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.patch_constraint(target_hash, patch).await,
            Self::InMemory { cas } => cas.patch_constraint(target_hash, patch).await,
        }
    }

    /// Delegates [`CasApi::get_constraint`] for the active backend.
    async fn get_constraint(&self, hash: Hash) -> Result<Option<Constraint>, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.get_constraint(hash).await,
            Self::InMemory { cas } => cas.get_constraint(hash).await,
        }
    }

    /// Delegates [`CasApi::get_constraint_many`] for the active backend.
    async fn get_constraint_many(
        &self,
        hashes: Vec<Hash>,
    ) -> Result<Vec<(Hash, Option<Constraint>)>, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.get_constraint_many(hashes).await,
            Self::InMemory { cas } => cas.get_constraint_many(hashes).await,
        }
    }

    /// Delegates [`CasMaintenanceApi::optimize_once`] for the active backend.
    async fn optimize_once(
        &self,
        options: OptimizeOptions,
    ) -> Result<mediapm_cas::OptimizeReport, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.optimize_once(options).await,
            Self::InMemory { cas } => cas.optimize_once(options).await,
        }
    }

    /// Delegates [`CasMaintenanceApi::prune_constraints`] for the active backend.
    async fn prune_constraints(&self) -> Result<mediapm_cas::PruneReport, CasError> {
        match self {
            Self::FileSystem { cas, .. } => cas.prune_constraints().await,
            Self::InMemory { cas } => cas.prune_constraints().await,
        }
    }
}

/// Collects an output byte stream into one contiguous buffer for assertions.
async fn collect_stream(stream: CasByteStream) -> Result<Bytes, CasError> {
    let mut out = Vec::new();
    tokio::pin!(stream);
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(Bytes::from(out))
}

/// Creates deterministic synthetic payload bytes with realistic object sizes.
fn synthetic_payload(seed: u8, len: usize) -> Bytes {
    let mut payload = vec![0u8; len];
    for (idx, byte) in payload.iter_mut().enumerate() {
        let bump = u8::try_from(idx % 251).expect("idx modulo 251 fits into u8");
        *byte = seed.wrapping_add(bump);
    }
    Bytes::from(payload)
}

/// Produces a target payload by mutating one byte in a base payload.
fn mutated_payload(base: &[u8], at: usize, value: u8) -> Bytes {
    let mut next = base.to_vec();
    next[at] = value;
    Bytes::from(next)
}

/// Asserts elapsed wall-clock time is within a coarse reproducible budget.
fn assert_budget(elapsed: Duration, max: Duration, context: &str) {
    assert!(elapsed <= max, "{context} exceeded coarse budget: elapsed={elapsed:?} max={max:?}");
}

/// Counts retained backup snapshot files at `<root>/index-backups`.
fn count_backup_snapshots(root: &std::path::Path) -> usize {
    let backup_root = root.join("index-backups");
    std::fs::read_dir(&backup_root)
        .expect("read backup directory")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("postcard"))
        .count()
}

/// Opens redb with short retries to avoid actor-shutdown races.
async fn open_redb_after_shutdown(db_path: &std::path::Path) -> Database {
    for _ in 0..30 {
        if let Ok(handle) = Database::open(db_path) {
            return handle;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Database::open(db_path).expect("open redb after shutdown")
}
