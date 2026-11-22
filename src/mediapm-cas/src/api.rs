//! Public CAS traits and types.
//!
//! - [`CasApi`] — core contract (`put`/`get`/`stat`/`delete`/`flush`) plus
//!   streaming variants `put_stream`/`get_to_writer`. No `exists`, no `info`
//!   — TOCTOU discouraged.
//! - [`ConstraintApi`] — separate trait for constraint hints.
//! - [`CasMaintenanceApi`] — maintenance operations (GC, optimization, etc.).

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;

use crate::error::CasError;
use crate::hash::Hash;

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

    /// Retrieve bytes by hash. Default impl buffers into memory via
    /// `get_to_writer()`. Prefer `get_to_writer()` for large objects.
    ///
    /// Returns [`CasError::NotFound`] if the object does not exist.
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        let mut buf = Vec::new();
        self.get_to_writer(hash, &mut buf).await?;
        Ok(Bytes::from(buf))
    }

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
    /// use write-through semantics (e.g. [`InMemoryCas`](crate::InMemoryCas)) or for callers
    /// that never use write-back CAS stores.
    ///
    /// # Guarantees
    ///
    /// After `flush()` returns `Ok`, all `put`/`delete` calls that
    /// completed before the flush are durably materialized.
    async fn flush(&self) -> Result<u64, CasError> {
        Ok(0)
    }

    /// Read from an async reader, store contents, return hash.
    ///
    /// Default impl buffers the reader then calls [`put`](Self::put).
    /// Override for zero-copy paths.
    async fn put_stream<R: tokio::io::AsyncRead + Send + Unpin>(
        &self,
        mut reader: R,
    ) -> Result<Hash, CasError>
    where
        Self: Sized,
    {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        self.put(Bytes::from(buf)).await
    }

    /// Retrieve bytes by hash and write to an async writer.
    ///
    /// Returns [`CasError::NotFound`] if the object does not exist.
    async fn get_to_writer<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        hash: Hash,
        writer: W,
    ) -> Result<(), CasError>;
}

/// How durable a constraint's side effects must be when the call returns.
///
/// A hard constraint can force the store to change a payload file (today only a
/// promotion to full), and every constraint changes the index. Those are
/// independent facts, so they are independent flags rather than one
/// "be durable" switch.
///
/// A hand-rolled byte newtype rather than a flags crate: the workspace
/// deliberately carries no `bitflags` dependency, and two flags do not warrant
/// one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CasDurability(u8);

impl CasDurability {
    /// Neither the payload files nor the index are synced. The constraint is
    /// journaled, which is what survives a process crash.
    pub const NONE: Self = Self(0);
    /// Sync every payload file the call changes, before returning.
    pub const STORE: Self = Self(1 << 0);
    /// Sync the metadata snapshot the call changes, before returning.
    pub const METADATA: Self = Self(1 << 1);

    /// Whether every flag in `other` is set in `self`.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// The union of two durability requests.
    #[must_use]
    pub const fn with(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOr for CasDurability {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        self.with(other)
    }
}

/// Manages compression-hint constraints (target to base hashes).
///
/// A constraint answers two independent questions: which objects a target may be
/// compressed against (a non-empty base set restricts the choice, an empty or
/// absent set leaves it open), and whether the target ends up stored full (only
/// when the chosen base is the empty object). Storing a target full is therefore
/// `set_constraint(target, {empty})`, not a marker of its own.
#[async_trait]
pub trait ConstraintApi: Send + Sync {
    /// Record a constraint, and state how durable its side effects must be.
    ///
    /// Identical to [`Self::set_constraint`] except for [`CasDurability`]: a
    /// caller that needs the bytes on disk when this returns asks for
    /// `STORE`, and one that needs the index entry too asks for
    /// `STORE | METADATA`.
    ///
    /// # Errors
    ///
    /// The same preconditions as [`Self::set_constraint`].
    async fn set_constraint_with(
        &self,
        target: Hash,
        bases: BTreeSet<Hash>,
        durability: CasDurability,
    ) -> Result<(), CasError>;

    /// Record that `target` may compress well against `bases`.
    ///
    /// # Errors
    ///
    /// A constraint is a recipe for rebuilding the target's bytes, so the store
    /// requires them: a target it does not hold is refused with
    /// [`CasError::InvalidArgument`], naming the hash. "Held" means the read path
    /// answers for the target, which includes bytes still pending in the journal
    /// and bytes recovered from an orphan scan, so the ordinary sequence of a
    /// put followed by a constraint on it is accepted without waiting for a
    /// maintenance pass.
    ///
    /// `bases` must be distinct and must not contain `target` itself.
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        self.set_constraint_with(target, bases, CasDurability::default()).await
    }

    /// Retrieve the bases recorded for `target`. Returns an empty set when
    /// no constraint exists (no `Option` wrapper).
    async fn get_constraint(&self, target: Hash) -> Result<BTreeSet<Hash>, CasError>;

    /// Atomically modify the bases for `target`.
    ///
    /// # Errors
    ///
    /// The same precondition [`Self::set_constraint`] applies, because this
    /// records a constraint by another name: a target the store does not hold is
    /// refused with [`CasError::InvalidArgument`] naming the hash.
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

    /// Remove a constraint whose target the store no longer holds and drop
    /// dead hashes inside the bases of a constraint that survives.
    ///
    /// The two are different edits, so [`PruneReport`] counts them separately:
    /// a dead base does not remove the constraint it sat in.
    async fn prune_constraints(&self) -> Result<PruneReport, CasError>;

    /// List all hashes currently in the store (best-effort).
    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError>;

    /// List hashes whose every physical form is known to be unsound.
    async fn list_corrupted_objects(&self) -> Result<Vec<Hash>, CasError>;
}

/// Result of [`CasMaintenanceApi::run_maintenance_cycle`].
#[derive(Debug, Clone, Default)]
pub struct OptimizeReport {
    /// Number of WAL entries consumed.
    pub wal_entries_consumed: usize,
    /// Whether maintenance work was done.
    pub maintenance_done: bool,
    /// Number of hashes currently recorded as having no sound physical form.
    ///
    /// A count rather than a list because a maintenance report answers "is there
    /// anything to fix", and the list is what the explicit inspection command is
    /// for.
    pub corrupted_objects: usize,
}

/// Result of [`CasMaintenanceApi::prune_constraints`].
///
/// Each count names one kind of fact the prune removes. A constraint whose
/// target is gone and a dead hash inside the surviving bases of a constraint
/// are different edits, and reporting only the first says zero while the store
/// changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneReport {
    /// Number of constraint entries removed because their target is gone.
    pub targets_removed: usize,
    /// Number of dead hashes dropped from the base sets of live constraints.
    pub bases_removed: usize,
}
