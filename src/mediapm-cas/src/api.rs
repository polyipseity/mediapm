//! Public CAS traits and types.
//!
//! # Architecture
//!
//! - [`CasApi`] — minimal 5-method contract (`put`/`get`/`stat`/`delete`/`flush`).
//!   No `exists`, no `info` — TOCTOU discouraged.
//! - [`CasApiStreaming`] — extension trait with blanket impl, provides
//!   stream-based put/get.
//! - [`ConstraintApi`] — separate trait for constraint hints.
//! - [`CasMaintenanceApi`] — maintenance operations (GC, optimization, etc.).

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;

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
    Stale { timeout: std::time::Duration },
}

// ---------------------------------------------------------------------------
// CasApi — minimal public contract
// ---------------------------------------------------------------------------

/// Minimal public CAS contract with intuitive postcondition guarantees.
///
/// Everything else (constraints, streaming, materialize, batch) is built on
/// top of these five methods.
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

    /// Materialize all committed WAL entries into the backing blob and
    /// metadata stores, making them visible to future `get`/`stat` calls.
    ///
    /// Returns the number of WAL entries consumed. No-op for backends that
    /// use write-through semantics (e.g. [`InMemoryCas`]) or for callers
    /// that never use write-back CAS stores.
    ///
    /// # Guarantees
    ///
    /// After `flush()` returns `Ok`, all `put`/`delete` calls that
    /// completed before the flush are durably materialized.
    async fn flush(&self) -> Result<u64, CasError> {
        Ok(0)
    }
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

    /// Retrieve the bases recorded for `target`. Returns an empty set when
    /// no constraint exists (no `Option` wrapper).
    async fn get_constraint(&self, target: Hash) -> Result<BTreeSet<Hash>, CasError>;

    /// Atomically modify the bases for `target`.
    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError>;
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
    /// Run one round of maintenance: drain the WAL consumer and run
    /// combined GC + optimizer.
    async fn run_maintenance_cycle(&self) -> Result<OptimizeReport, CasError>;

    /// Remove constraints whose target or bases no longer exist.
    async fn prune_constraints(&self) -> Result<PruneReport, CasError>;

    /// List all hashes currently in the store (best-effort).
    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError>;
}

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

/// Result of [`CasMaintenanceApi::run_maintenance_cycle`].
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
