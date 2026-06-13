//! Public CAS traits and types.
//!
//! # Architecture
//!
//! - [`CasApi`] — minimal 4-method contract (`put`/`get`/`stat`/`delete`).
//!   No `exists`, no `info` — TOCTOU discouraged.
//! - [`CasApiStreaming`] — extension trait with blanket impl, provides
//!   stream-based put/get.
//! - [`ConstraintApi`] — separate trait for constraint hints.
//! - [`CasMaintenanceApi`] — maintenance operations (GC, optimization, etc.).

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeSet, HashSet};

use crate::error::CasError;
use crate::hash::Hash;

// ---------------------------------------------------------------------------
// ObjectMeta and ObjectEncoding
// ---------------------------------------------------------------------------

/// Metadata about a stored object, returned by [`CasApi::stat`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Original payload length.
    pub len: u64,
    /// How the object is encoded.
    pub encoding: ObjectEncoding,
}

/// Encoding of an object payload.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ObjectEncoding {
    /// Full (unencoded) payload.
    Full,
    /// Delta-encoded against a base hash.
    Delta { base_hash: Hash },
}

// ---------------------------------------------------------------------------
// CasApi — minimal public contract
// ---------------------------------------------------------------------------

/// Minimal public CAS contract with intuitive postcondition guarantees.
///
/// Everything else (constraints, streaming, materialize, batch) is built on
/// top of these four methods.
///
/// # TOCTOU discouraged
///
/// There is no standalone `exists()` method. Use `get()` (returns
/// [`CasError::NotFound`] on miss) or `stat()` (returns
/// [`CasError::NotFound`] on miss). Both give an authoritative answer in one
/// operation, removing the temptation to check-then-act separately.
///
/// # Guarantees
///
/// All ordering guarantees apply **within a single thread of execution**
/// (one async task). Across threads, concurrent operations are commutative
/// where possible, but no cross-thread ordering is promised.
///
/// ## Write-then-read (instant)
/// After `put(data)` returns `Ok`, `get(hash)` returns the data and
/// `stat(hash)` returns the correct metadata immediately.
///
/// ## Delete-then-get / Delete-then-stat (instant)
/// After `delete(hash)` returns `Ok`, `get(hash)` and `stat(hash)` return
/// `NotFound` immediately.
///
/// ## Idempotency
/// - `put(data)` twice with the same data is a no-op.
/// - `delete(hash)` twice is a no-op.
///
/// ## Crash survival
/// After any method returns `Ok`, the effect survives process death.
#[async_trait]
pub trait CasApi: Send + Sync {
    /// Store bytes, return the canonical content-addressed hash.
    async fn put(&self, data: Bytes) -> Result<Hash, CasError>;

    /// Retrieve bytes by hash.
    ///
    /// Returns [`CasError::NotFound`] if the object does not exist.
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError>;

    /// Get metadata for an object.
    ///
    /// Returns [`CasError::NotFound`] if the object does not exist.
    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError>;

    /// Delete an object by hash. Idempotent.
    async fn delete(&self, hash: Hash) -> Result<(), CasError>;
}

// ---------------------------------------------------------------------------
// CasApiStreaming — extension trait
// ---------------------------------------------------------------------------

/// Streaming I/O — built atop [`CasApi`] with default buffer-through impls.
///
/// Backends that can stream directly (e.g. file descriptors) should override
/// for zero-copy paths.
#[async_trait]
pub trait CasApiStreaming: CasApi {
    /// Read from an unbuffered reader, store contents, return hash.
    async fn put_stream<R: tokio::io::AsyncRead + Send + Unpin>(
        &self,
        mut reader: R,
    ) -> Result<Hash, CasError> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        self.put(Bytes::from(buf)).await
    }

    /// Retrieve bytes and write to a writer.
    async fn get_stream<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        hash: Hash,
        mut writer: W,
    ) -> Result<(), CasError> {
        use tokio::io::AsyncWriteExt;
        let data = self.get(hash).await?;
        writer.write_all(&data).await?;
        Ok(())
    }
}

// Blanket impl: every CasApi automatically provides streaming methods.
impl<T: CasApi> CasApiStreaming for T {}

// ---------------------------------------------------------------------------
// ConstraintApi — delta-compression hints
// ---------------------------------------------------------------------------

/// Manages compression-hint constraints (target → base hashes).
///
/// Constraints are **non-binding hints**: setting one records the intent
/// that `target` may compress well against `bases`. The system never blocks
/// on constraint satisfaction.
#[async_trait]
pub trait ConstraintApi: Send + Sync {
    /// Record that `target` may compress well against `bases`.
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError>;

    /// Retrieve the bases recorded for `target`, if any.
    async fn get_constraint(&self, target: Hash) -> Result<Option<BTreeSet<Hash>>, CasError>;

    /// Atomically modify the bases for `target`.
    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError>;

    /// Compute effective bases: intersection of stored constraint bases
    /// with `live` hashes.
    ///
    /// A base that doesn't exist in the store cannot be used for delta
    /// reconstruction, so it is excluded from the effective set.
    async fn effective_bases(
        &self,
        target: Hash,
        live: &HashSet<Hash>,
    ) -> Result<BTreeSet<Hash>, CasError> {
        let bases = match self.get_constraint(target).await? {
            Some(bases) => bases,
            None => return Ok(BTreeSet::new()),
        };
        Ok(bases.into_iter().filter(|b| live.contains(b)).collect())
    }
}

/// Describes an atomic modification to a constraint's base set.
#[derive(Debug, Clone, Default)]
pub struct ConstraintPatch {
    /// Bases to add.
    pub add_bases: BTreeSet<Hash>,
    /// Bases to remove.
    pub remove_bases: BTreeSet<Hash>,
    /// If true, clear all existing bases before applying adds/removes.
    pub clear: bool,
}

// ---------------------------------------------------------------------------
// CasMaintenanceApi — background / maintenance operations
// ---------------------------------------------------------------------------

/// Maintenance operations (GC, optimization, index repair).
///
/// These are infrequent, potentially expensive operations. They are exposed
/// as direct async methods that the caller invokes at an appropriate time
/// (e.g. from a background task or during idle periods).
#[async_trait]
pub trait CasMaintenanceApi: Send + Sync {
    /// Run one round of optimization: drain the WAL consumer and run
    /// combined GC + optimizer.
    async fn optimize_once(&self) -> Result<OptimizeReport, CasError>;

    /// Remove constraints whose target or bases no longer exist.
    async fn prune_constraints(&self) -> Result<PruneReport, CasError>;

    /// Prune orphaned constraint metadata entries.
    async fn gc_sweep(&self) -> Result<GcSweepReport, CasError>;

    /// List all hashes currently in the store (best-effort).
    async fn list_all_hashes(&self) -> Result<Vec<Hash>, CasError>;

    /// Repair any index inconsistencies.
    async fn repair_index(&self) -> Result<IndexRepairReport, CasError>;
}

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

/// Result of [`CasMaintenanceApi::optimize_once`].
#[derive(Debug, Clone, Default)]
pub struct OptimizeReport {
    /// Number of WAL entries consumed.
    pub wal_entries_consumed: usize,
    /// Whether maintenance work was done.
    pub maintenance_done: bool,
}

/// Result of [`CasMaintenanceApi::prune_constraints`].
#[derive(Debug, Clone, Default)]
pub struct PruneReport {
    /// Number of constraint entries removed.
    pub removed: usize,
}

/// Result of [`CasMaintenanceApi::gc_sweep`].
///
/// GC does not delete objects — it only prunes constraint metadata entries so
/// they approach effective constraints (intersection of stored bases with live
/// hashes). Per-base pruning removes individual dead bases rather than whole
/// entries. Objects are only removed by explicit [`CasApi::delete`].
#[derive(Debug, Clone, Default)]
pub struct GcSweepReport {
    /// Number of constraint entries removed (those whose target was deleted).
    pub deleted: usize,
}

/// Result of [`CasMaintenanceApi::repair_index`].
#[derive(Debug, Clone, Default)]
pub struct IndexRepairReport {
    /// Number of inconsistencies fixed.
    pub fixed: usize,
}
