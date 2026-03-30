//! Public API contracts for Phase 1 CAS.
//!
//! This module is the main semantic boundary between the CAS implementation and
//! higher layers such as Conductor or the Phase 3 CLI surface.
//!
//! The public types here intentionally focus on:
//! - deterministic behavior,
//! - explicit lifecycle semantics,
//! - low-copy data movement where practical,
//! - stream-based escape hatches for very large payloads.
//!
//! ## Implementation notes
//!
//! - [`Hash`] is passed by value throughout the API because it is a small,
//!   copyable identity type and does not benefit from reference indirection.
//! - Constraint state distinguishes between **no explicit constraint row** and
//!   **an explicit non-empty candidate set**. The former means unrestricted
//!   base selection; the latter means the backend must choose from that set.
//! - Stream-oriented methods exist to keep the API usable for multi-gigabyte
//!   payloads, even if some initial backends internally buffer before writing.

use async_trait::async_trait;
use bitvec::vec::BitVec;
use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::pin::Pin;
use std::time::Duration;

use futures_core::Stream;
use tokio::io::AsyncRead;

use crate::{CasError, Hash};

/// Boxed async byte reader used by streaming put APIs.
///
/// This is a type-erased reader so callers can hand the CAS any owned async
/// input source without the trait itself becoming generic over a reader type.
/// The bounds are intentional:
///
/// - [`AsyncRead`] means the source can be consumed incrementally by the
///   backend.
/// - [`Unpin`] keeps usage simple for backends that want to call helper methods
///   like `read_to_end` without additional pin-projection machinery.
/// - [`Send`] allows the reader to cross task boundaries when implementations
///   offload work onto async tasks or blocking workers.
/// - `'static` ensures the boxed reader does not borrow from a stack frame that
///   may disappear before asynchronous processing completes.
///
/// In short: this alias trades a small amount of dynamic dispatch for a much
/// simpler and more backend-friendly streaming API surface.
pub type CasByteReader = Box<dyn AsyncRead + Unpin + Send + 'static>;

/// Boxed byte stream used by streaming get APIs.
///
/// Like [`CasByteReader`], this alias uses type erasure to keep the trait
/// object-safe and easy to implement across multiple backends.
/// The bounds are intentional:
///
/// - [`Stream`] allows backends to yield chunks incrementally instead of
///   materializing the full object in memory.
/// - each item is `Result<Bytes, CasError>` so transport/storage failures can
///   surface mid-stream with full CAS error context.
/// - [`Bytes`] is used as the chunk type because it is cheap to clone/slice and
///   works well for both buffered and zero-copy-ish implementations.
/// - [`Pin<Box<...>>`] gives callers a stable, heap-allocated stream object even
///   when the concrete stream type is deeply nested or self-referential.
/// - [`Send`] and `'static` allow implementations to hand the stream across task
///   boundaries safely.
///
/// This shape is slightly more verbose than a concrete stream type, but it
/// keeps the public API stable while giving implementations room to evolve.
pub type CasByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, CasError>> + Send + 'static>>;

/// Bit-packed existence map where each bit corresponds to one input hash.
///
/// Bit positions are 1:1 with the input ordering supplied to
/// [`CasApi::exists_many`].
///
/// This keeps large existence batches compact: one bit per answer instead of a
/// full byte-or-bool payload per item.
pub type CasExistenceBitmap = BitVec;

/// Phase 1 optimization constraint for a target object.
///
/// A constraint is an **explicit narrowing** of valid base choices for one
/// already-stored target object. If no explicit constraint row exists, the
/// backend is free to choose any valid base, including the empty-content hash
/// which semantically corresponds to full-object storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constraint {
    /// Content hash whose base-candidate set is being constrained.
    pub target_hash: Hash,
    /// Potential base hashes for the target.
    ///
    /// Ordering is deterministic and values are unique.
    pub potential_bases: BTreeSet<Hash>,
}

/// Incremental mutation for an existing explicit constraint row.
///
/// This avoids the classic read-modify-write race that would otherwise happen
/// if callers had to fetch a whole [`Constraint`], edit it locally, and then
/// overwrite it with [`CasApi::set_constraint`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintPatch {
    /// Candidate bases to add.
    pub add_bases: BTreeSet<Hash>,
    /// Candidate bases to remove.
    pub remove_bases: BTreeSet<Hash>,
    /// Clear existing explicit candidates before applying add/remove sets.
    pub clear_existing: bool,
}

/// Cheap metadata for one stored object without payload fetch.
///
/// This is intended for scheduling, preflight checks, and UI/reporting code
/// that needs object facts without paying the cost of reconstruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectInfo {
    /// Reconstructed logical length.
    pub content_len: u64,
    /// Stored payload length (raw full bytes or encoded diff bytes).
    pub payload_len: u64,
    /// Whether object is persisted as delta payload.
    pub is_delta: bool,
    /// Base hash for delta objects.
    pub base_hash: Option<Hash>,
}

/// Summary of one optimizer pass.
///
/// The report is intentionally small and cheap to serialize so it can be used
/// in actor replies, logs, and CLI output without additional shaping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptimizeReport {
    /// Number of stored objects rewritten to new bases/encodings.
    pub rewritten_objects: usize,
}

/// Summary of one prune pass.
///
/// This describes structural cleanup of explicit constraint metadata only; it
/// does not imply object payload deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneReport {
    /// Number of removed dangling candidate-base references.
    pub removed_candidates: usize,
}

/// Source used to restore explicit constraint rows during an index repair.
///
/// Object metadata is always reconstructed from the object store itself. This
/// enum reports where any recovered explicit constraint rows came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexRepairConstraintSource {
    /// No explicit constraints were restored.
    None,
    /// Explicit constraints were restored from the current in-memory index state.
    InMemoryIndex,
    /// Explicit constraints were restored from the newest valid backup snapshot.
    BackupSnapshot,
}

/// Summary of one durable index repair or rebuild pass.
///
/// The filesystem backend uses this report for both explicit operator-triggered
/// repair flows and automatic startup recovery when the primary index is
/// missing or corrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRepairReport {
    /// Number of non-empty object rows rebuilt into the index.
    pub object_rows_rebuilt: usize,
    /// Number of explicit constraint rows restored after filtering invalid references.
    pub explicit_constraint_rows_restored: usize,
    /// Number of object files scanned while rebuilding metadata.
    pub scanned_object_files: usize,
    /// Number of object files skipped because they were invalid or unrecoverable.
    pub skipped_object_files: usize,
    /// Number of backup snapshots examined while looking for recoverable constraints.
    pub backup_snapshots_considered: usize,
    /// Source of restored explicit constraints, if any.
    pub constraint_source: IndexRepairConstraintSource,
}

/// Priority hint for one optimizer pass.
///
/// This is a scheduling hint, not a hard guarantee. Backends may currently use
/// it only for heuristics/logging and evolve toward stronger QoS later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizePriority {
    /// Best-effort background maintenance.
    Background,
    /// Foreground maintenance requested by caller.
    Foreground,
}

/// Configuration for one optimizer pass.
///
/// This allows callers to bound maintenance work so optimization does not
/// starve foreground ingestion or retrieval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptimizeOptions {
    /// Optional cap on number of objects to rewrite in this pass.
    pub max_rewrites: Option<usize>,
    /// Optional wall-clock timeout for one pass.
    pub timeout: Option<Duration>,
    /// Caller priority hint.
    pub priority: OptimizePriority,
}

/// Default optimizer options favor background best-effort maintenance.
impl Default for OptimizeOptions {
    fn default() -> Self {
        Self { max_rewrites: None, timeout: None, priority: OptimizePriority::Background }
    }
}

/// Async API contract for Phase 1 CAS behavior.
///
/// Implementations may differ internally (filesystem, in-memory, remote, or
/// actor-mediated), but they must preserve the same observable semantics:
/// identity stability, reconstructability, and constraint correctness.
#[async_trait]
pub trait CasApi: Send + Sync {
    /// Checks whether one hash exists without loading payload bytes.
    ///
    /// # Errors
    /// Returns [`CasError`] when lookup fails.
    ///
    /// # Performance
    /// O(1) index lookup in steady state.
    async fn exists(&self, hash: Hash) -> Result<bool, CasError>;

    /// Checks existence for many hashes in one logical call.
    ///
    /// # Errors
    /// Returns [`CasError`] if any lookup fails.
    ///
    /// Output vector is 1:1 with input order.
    ///
    /// # Performance
    /// Default implementation runs lookups concurrently and writes each result
    /// directly into a pre-sized bitmap, avoiding sort overhead for very large
    /// batches.
    async fn exists_many(&self, hashes: Vec<Hash>) -> Result<CasExistenceBitmap, CasError> {
        let total = hashes.len();
        let mut pending: FuturesUnordered<_> = hashes
            .into_iter()
            .enumerate()
            .map(|(idx, hash)| async move { (idx, self.exists(hash).await) })
            .collect();

        let mut bitmap = BitVec::repeat(false, total);
        let mut completed = 0usize;
        while let Some((idx, exists)) = pending.next().await {
            bitmap.set(idx, exists?);
            completed = completed.saturating_add(1);
        }
        debug_assert_eq!(completed, total, "all existence checks should complete exactly once");
        Ok(bitmap)
    }

    /// Stores content and returns its canonical content hash.
    ///
    /// Implementations must guarantee that subsequent [`Self::get`] calls using
    /// the returned hash reconstruct the exact same bytes.
    ///
    /// # Errors
    /// Returns [`CasError`] when storage, hashing, or persistence fails.
    ///
    /// # Performance
    /// O(data length) for hashing plus backend-specific persistence overhead.
    async fn put<D>(&self, data: D) -> Result<Hash, CasError>
    where
        D: TryInto<Bytes> + Send,
        D::Error: std::fmt::Display + Send;

    /// Stores content and atomically attaches explicit constraints.
    ///
    /// This is the preferred ingestion path when caller already knows candidate
    /// bases, because it avoids a second round-trip and second index write.
    ///
    /// # Errors
    /// Returns [`CasError::NotFound`] when any non-empty candidate base is missing.
    /// Returns [`CasError`] for storage/index failures.
    ///
    /// # Performance
    /// O(data length + candidate count) plus backend persistence overhead.
    async fn put_with_constraints<D>(
        &self,
        data: D,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError>
    where
        D: TryInto<Bytes> + Send,
        D::Error: std::fmt::Display + Send;

    /// Stores content from a stream-oriented reader.
    ///
    /// # Errors
    /// Returns [`CasError`] when stream ingestion, hashing, or persistence fails.
    ///
    /// # Performance
    /// Backend-defined; intended to reduce peak memory for large payloads.
    async fn put_stream(&self, reader: CasByteReader) -> Result<Hash, CasError>;

    /// Stores content from a stream and atomically attaches explicit constraints.
    ///
    /// This is the stream-oriented companion to [`Self::put_with_constraints`].
    /// It exists so callers handling very large payloads can still provide
    /// known candidate bases during initial ingestion instead of requiring a
    /// second metadata call.
    ///
    /// # Errors
    /// Returns [`CasError::NotFound`] when any non-empty candidate base is missing.
    /// Returns [`CasError`] for stream/storage/index failures.
    ///
    /// # Performance
    /// Backend-defined. Implementations may use this hook to optimize base
    /// selection and persistence in one ingestion path.
    async fn put_stream_with_constraints(
        &self,
        reader: CasByteReader,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError>;

    /// Retrieves previously stored content by hash.
    ///
    /// Returns [`CasError::NotFound`] if the hash is unknown and no reconstructable
    /// object chain exists.
    ///
    /// # Errors
    /// Returns [`CasError`] when lookup or reconstruction fails.
    ///
    /// # Performance
    /// O(chain depth) reconstruction for delta objects; O(1) for full objects.
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError>;

    /// Retrieves content as a stream.
    ///
    /// # Errors
    /// Returns [`CasError`] when lookup or stream setup fails.
    ///
    /// # Performance
    /// Backend-defined; may avoid full-buffer materialization.
    async fn get_stream(&self, hash: Hash) -> Result<CasByteStream, CasError>;

    /// Retrieves many objects in one logical call.
    ///
    /// # Errors
    /// Returns [`CasError`] if any object lookup fails.
    ///
    /// # Performance
    /// Default implementation runs fetches concurrently and writes each result
    /// directly into a pre-sized output buffer, avoiding sort overhead for
    /// very large batches.
    async fn get_many(&self, hashes: Vec<Hash>) -> Result<Vec<(Hash, Bytes)>, CasError> {
        let total = hashes.len();
        let mut pending: FuturesUnordered<_> = hashes
            .into_iter()
            .enumerate()
            .map(|(idx, hash)| async move {
                let result = self.get(hash).await;
                (idx, hash, result)
            })
            .collect();

        let mut ordered: Vec<Option<(Hash, Bytes)>> = vec![None; total];
        let mut completed = 0usize;
        while let Some((idx, hash, bytes)) = pending.next().await {
            ordered[idx] = Some((hash, bytes?));
            completed = completed.saturating_add(1);
        }
        debug_assert_eq!(completed, total, "all get_many fetches should complete exactly once");

        ordered
            .into_iter()
            .map(|entry| {
                entry.ok_or_else(|| {
                    CasError::invariant("get_many completion buffer was not fully populated")
                })
            })
            .collect()
    }

    /// Returns object metadata without loading full payload bytes.
    ///
    /// # Errors
    /// Returns [`CasError::NotFound`] when `hash` is unknown.
    ///
    /// # Performance
    /// O(1) index lookup.
    async fn info(&self, hash: Hash) -> Result<ObjectInfo, CasError>;

    /// Returns object metadata for many hashes in one logical call.
    ///
    /// Output vector is 1:1 with input order.
    ///
    /// # Errors
    /// Returns [`CasError`] if any per-hash metadata lookup fails.
    ///
    /// # Performance
    /// Default implementation runs lookups concurrently and writes each result
    /// directly into a pre-sized output buffer, avoiding sort overhead for
    /// very large batches.
    async fn info_many(&self, hashes: Vec<Hash>) -> Result<Vec<(Hash, ObjectInfo)>, CasError> {
        let total = hashes.len();
        let mut pending: FuturesUnordered<_> = hashes
            .into_iter()
            .enumerate()
            .map(|(idx, hash)| async move {
                let result = self.info(hash).await;
                (idx, hash, result)
            })
            .collect();

        let mut ordered: Vec<Option<(Hash, ObjectInfo)>> = vec![None; total];
        let mut completed = 0usize;
        while let Some((idx, hash, info)) = pending.next().await {
            ordered[idx] = Some((hash, info?));
            completed = completed.saturating_add(1);
        }
        debug_assert_eq!(completed, total, "all info_many lookups should complete exactly once");

        ordered
            .into_iter()
            .map(|entry| {
                entry.ok_or_else(|| {
                    CasError::invariant("info_many completion buffer was not fully populated")
                })
            })
            .collect()
    }

    /// Deletes one object while preserving reconstructability of descendants.
    ///
    /// Implementations must rewrite or rebase dependent objects as needed so
    /// remaining objects stay reconstructible while still respecting existing
    /// explicit constraints after removing references to the deleted hash.
    ///
    /// Implementations must also prune any explicit constraint row where this
    /// hash is the target.
    ///
    /// # Errors
    /// Returns [`CasError::NotFound`] when hash does not exist.
    /// Returns [`CasError::InvalidConstraint`] when descendants cannot be
    /// preserved under remaining constraints.
    /// Returns [`CasError`] for storage/index rewrite failures.
    ///
    /// # Performance
    /// O(number of direct dependents × candidate count + index maintenance).
    async fn delete(&self, hash: Hash) -> Result<(), CasError>;

    /// Deletes many objects in one logical call.
    ///
    /// Default behavior is deterministic and sequential: hashes are deleted in
    /// input order. Backends can override this for true batch/delete-set
    /// optimizations (for example, lock amortization or remote bulk-delete
    /// primitives).
    ///
    /// # Errors
    /// Returns the first [`CasError`] produced by [`Self::delete`].
    ///
    /// # Performance
    /// Default implementation is O(number of hashes × delete cost).
    async fn delete_many(&self, hashes: Vec<Hash>) -> Result<(), CasError> {
        for hash in hashes {
            self.delete(hash).await?;
        }
        Ok(())
    }

    /// Sets optimization constraints for an existing target hash.
    ///
    /// Constraint semantics:
    /// - no explicit row (or an empty candidate set after pruning) means
    ///   unrestricted base choice (`put`/optimizer can consider any object,
    ///   including empty hash for full storage);
    /// - non-empty explicit set means base must be one of these hashes.
    ///
    /// # Errors
    /// Returns [`CasError::NotFound`] when `constraint.target_hash` is missing.
    /// Returns [`CasError::NotFound`] when a non-empty base is missing.
    /// Returns [`CasError::InvalidConstraint`] for self-reference.
    /// Returns [`CasError`] for persistence failures.
    ///
    /// # Performance
    /// O(number of provided bases) for normalization and deduplication.
    async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError>;

    /// Incrementally mutates explicit constraints for an existing target hash.
    ///
    /// `patch.remove_bases` is applied before `patch.add_bases`.
    ///
    /// # Errors
    /// Returns [`CasError::NotFound`] when target is missing.
    /// Returns [`CasError::NotFound`] when any newly added non-empty base is missing.
    /// Returns [`CasError::InvalidConstraint`] when target would reference itself.
    ///
    /// # Performance
    /// O(current bases + patch sizes) for merge and validation.
    async fn patch_constraint(
        &self,
        target_hash: Hash,
        patch: ConstraintPatch,
    ) -> Result<Option<Constraint>, CasError>;

    /// Reads current explicit constraint row for one hash.
    ///
    /// `None` means unrestricted base selection.
    ///
    /// # Errors
    /// Returns [`CasError::NotFound`] when target hash is missing.
    ///
    /// # Performance
    /// O(1) index lookup.
    async fn get_constraint(&self, hash: Hash) -> Result<Option<Constraint>, CasError>;

    /// Reads explicit constraint rows for many hashes in one logical call.
    ///
    /// Output vector is 1:1 with input order. `None` still means unrestricted
    /// base selection for that corresponding target hash.
    ///
    /// # Errors
    /// Returns [`CasError`] if any per-hash lookup fails.
    ///
    /// # Performance
    /// Default implementation runs lookups concurrently and writes each result
    /// directly into a pre-sized output buffer.
    async fn get_constraint_many(
        &self,
        hashes: Vec<Hash>,
    ) -> Result<Vec<(Hash, Option<Constraint>)>, CasError> {
        let total = hashes.len();
        let mut pending: FuturesUnordered<_> = hashes
            .into_iter()
            .enumerate()
            .map(|(idx, hash)| async move {
                let result = self.get_constraint(hash).await;
                (idx, hash, result)
            })
            .collect();

        let mut ordered: Vec<Option<(Hash, Option<Constraint>)>> = vec![None; total];
        let mut completed = 0usize;
        while let Some((idx, hash, constraint)) = pending.next().await {
            ordered[idx] = Some((hash, constraint?));
            completed = completed.saturating_add(1);
        }
        debug_assert_eq!(
            completed, total,
            "all get_constraint_many lookups should complete exactly once"
        );

        ordered
            .into_iter()
            .map(|entry| {
                entry.ok_or_else(|| {
                    CasError::invariant(
                        "get_constraint_many completion buffer was not fully populated",
                    )
                })
            })
            .collect()
    }
}

/// Additional maintenance APIs for optimizer/pruning lifecycle.
///
/// These operations are intentionally separated from the core read/write API so
/// callers can choose whether maintenance runs inline, in a background actor,
/// or under explicit operator control.
#[async_trait]
pub trait CasMaintenanceApi: Send + Sync {
    /// Executes one incremental optimization pass.
    ///
    /// The operation may rewrite object encodings and base references but must
    /// preserve reconstructability of all objects.
    ///
    /// # Errors
    /// Returns [`CasError`] when candidate evaluation or rewrite fails.
    ///
    /// # Performance
    /// O(number of constraints × candidate count) for one pass.
    async fn optimize_once(&self, options: OptimizeOptions) -> Result<OptimizeReport, CasError>;

    /// Executes one pass with default options.
    ///
    /// # Errors
    /// Returns [`CasError`] when optimization fails.
    ///
    /// # Performance
    /// Equivalent to [`Self::optimize_once`] with [`OptimizeOptions::default`].
    async fn optimize_once_default(&self) -> Result<OptimizeReport, CasError> {
        self.optimize_once(OptimizeOptions::default()).await
    }

    /// Removes dangling base candidates from constraints.
    ///
    /// This operation must:
    /// 1) remove explicit constraints whose target no longer exists,
    /// 2) remove deleted candidate bases from remaining constraints.
    ///
    /// # Errors
    /// Returns [`CasError`] when state validation or persistence fails.
    ///
    /// # Performance
    /// O(total number of explicit constraint candidates).
    async fn prune_constraints(&self) -> Result<PruneReport, CasError>;

    /// Rebuilds durable index metadata from persisted object files.
    ///
    /// Filesystem-backed implementations use the on-disk object store as the
    /// authoritative source for object metadata and attempt to restore explicit
    /// constraints from either the current in-memory index state or the newest
    /// valid backup snapshot.
    ///
    /// Backends without a durable on-disk index may implement this as a no-op.
    ///
    /// # Errors
    /// Returns [`CasError`] when scanning object files, validating recovered
    /// content, or persisting the repaired index fails.
    async fn repair_index(&self) -> Result<IndexRepairReport, CasError>;

    /// Migrates durable index storage to one target schema marker.
    ///
    /// Migration is whole-index and backend-controlled; reads may be blocked
    /// until migration completes to preserve consistency.
    ///
    /// # Errors
    /// Returns [`CasError`] when target version is unsupported or migration
    /// read/write flow fails.
    async fn migrate_index_to_version(&self, target_version: u32) -> Result<(), CasError>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::Hash;

    use super::{
        Constraint, IndexRepairConstraintSource, IndexRepairReport, OptimizeReport, PruneReport,
    };

    #[test]
    fn constraint_is_constructible_with_expected_fields() {
        let target = Hash::from_content(b"target");
        let base = Hash::from_content(b"base");

        let constraint =
            Constraint { target_hash: target, potential_bases: BTreeSet::from([base]) };

        assert_eq!(constraint.target_hash, target);
        assert_eq!(constraint.potential_bases, BTreeSet::from([base]));
    }

    #[test]
    fn maintenance_reports_preserve_counts() {
        let optimize = OptimizeReport { rewritten_objects: 2 };
        let prune = PruneReport { removed_candidates: 3 };
        let repair = IndexRepairReport {
            object_rows_rebuilt: 4,
            explicit_constraint_rows_restored: 1,
            scanned_object_files: 5,
            skipped_object_files: 0,
            backup_snapshots_considered: 2,
            constraint_source: IndexRepairConstraintSource::BackupSnapshot,
        };

        assert_eq!(optimize.rewritten_objects, 2);
        assert_eq!(prune.removed_candidates, 3);
        assert_eq!(repair.object_rows_rebuilt, 4);
        assert_eq!(repair.explicit_constraint_rows_restored, 1);
        assert_eq!(repair.backup_snapshots_considered, 2);
        assert_eq!(repair.constraint_source, IndexRepairConstraintSource::BackupSnapshot);
    }
}
