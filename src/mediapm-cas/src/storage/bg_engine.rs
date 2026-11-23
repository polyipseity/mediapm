//! Background engine — WAL consumer + maintenance orchestrator.
//!
//! Drives two background tasks: the WAL consumer drains pending WAL entries
//! into the [`BlobStore`] and [`MetadataStore`], then trims them; maintenance
//! runs combined GC + optimizer, pruning constraint metadata toward effective
//! constraints (stored bases ∩ live hashes) and evaluating delta-compression
//! opportunities. GC never deletes objects — only explicit `delete()` does —
//! it just prunes orphaned constraint bases individually.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;

use crate::api::ObjectEncoding;
use crate::defaults;
use crate::delta::object::StoredObject;
use crate::delta::patch::DeltaPatch;
use crate::error::CasError;
use crate::hash::Hash;

use super::blob_store::BlobStore;
use super::metadata_store::{MetadataEntry, MetadataStore};
use super::read_view::ReadView;
use super::reconstructed_cache::{
    ReconstructedBytesCache, budget_from_store_bytes, compute_store_bytes,
};
use super::wal::{Wal, WalEntry, WalPosition};

/// Re-export so the cache statistics type is nameable by external callers
/// of [`BackgroundEngine::reconstructed_cache_stats`].
pub use super::reconstructed_cache::ReconstructedCacheStats;

/// The facts one maintenance pass reports to its caller.
///
/// `corrupted_objects` is the count after the pass, not the number the pass
/// newly recorded, so a caller that asks "is anything still corrupt?" reads one
/// field instead of diffing two snapshots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MaintenanceOutcome {
    /// Whether the pass changed stored state.
    pub(crate) did_work: bool,
    /// Hashes currently recorded as having no sound physical form.
    pub(crate) corrupted_objects: usize,
}

/// Background engine driving WAL consumption and maintenance.
pub struct BackgroundEngine<J: Wal, M: MetadataStore, B: BlobStore> {
    wal: J,
    metadata: M,
    blob: B,
    read_view: Arc<dyn ReadView>,
    checkpoint: AtomicU64,
    /// Serializes WAL replay across every consumer sharing this store.
    ///
    /// The background consumer spawned by [`FileSystemCas`](super::file_system::FileSystemCas)
    /// and a foreground [`CasStore::flush`](super::store::CasStore::flush)
    /// or `run_maintenance_cycle` can be scheduled at the same instant, and
    /// both would otherwise replay the *same* entry range: the checkpoint is
    /// only advanced at the end of a batch, so neither sees the other's
    /// progress. Two concurrent replays are not merely redundant — they
    /// collide, because [`BlobStore::write`] and the metadata snapshot share
    /// a deterministic `<path>.tmp` staging name, so one replay's rename
    /// consumes the staging file the other is about to rename and the loser
    /// fails with `NotFound`. See [`run_wal_consumer`](Self::run_wal_consumer).
    ///
    /// Shared through [`Arc`] so [`Clone`] copies participate in the same
    /// critical section rather than each running an independent replay.
    consume_lock: Arc<tokio::sync::Mutex<()>>,
    cancelled: Arc<AtomicBool>,
    /// Shared reconstructed-bytes cache (see `reconstructed_cache` module;
    /// spec `src/mediapm-cas/AGENTS.md` §5.6). `None` when caching is
    /// disabled (zero TTL). Shared with the read view via `Arc`.
    reconstructed_cache: Option<Arc<ReconstructedBytesCache>>,
}

impl<J: Wal, M: MetadataStore, B: BlobStore> BackgroundEngine<J, M, B> {
    /// Create a new engine, checkpointing at `start_pos`.
    ///
    /// `reconstructed_cache` is the store-wide reconstructed-bytes cache;
    /// pass `None` to disable caching.
    pub(crate) fn new(
        wal: J,
        metadata: M,
        blob: B,
        start_pos: WalPosition,
        read_view: Arc<dyn ReadView>,
        reconstructed_cache: Option<Arc<ReconstructedBytesCache>>,
    ) -> Self {
        Self {
            wal,
            metadata,
            blob,
            read_view,
            checkpoint: AtomicU64::new(start_pos.as_u64()),
            consume_lock: Arc::new(tokio::sync::Mutex::new(())),
            cancelled: Arc::new(AtomicBool::new(false)),
            reconstructed_cache,
        }
    }

    /// Drain the WAL consumer once: drain WAL entries into Blob +
    /// Metadata, advancing checkpoint after each entry.
    ///
    /// Replay is exclusive per store: the call holds `consume_lock`
    /// for its whole body, so a background consumer and a foreground
    /// `flush` / `run_maintenance_cycle` drain the same range one after the
    /// other instead of interleaving. The second caller observes the
    /// advanced checkpoint and returns `0` — replaying an already-consumed
    /// range is unnecessary work, and while entries are individually
    /// idempotent the *batch* is not: a `Put` replayed after maintenance
    /// has advanced the same hash to a delta encoding would otherwise
    /// regress that newer state.
    ///
    /// The lock is a leaf guard: it must not be held across any call that
    /// re-enters this method, because [`tokio::sync::Mutex`] is not
    /// reentrant. `run_maintenance` and
    /// [`drain_all`](Self::drain_all) call this method and therefore must
    /// acquire the lock through it only, never directly.
    ///
    /// # Errors
    ///
    /// Delegates to the WAL and metadata store operations.
    ///
    /// Returns the number of entries consumed.
    pub async fn run_wal_consumer(&self) -> Result<u64, CasError> {
        let _replay_guard = self.consume_lock.lock().await;
        let committed = self.wal.committed_position().await;
        let ckpt = WalPosition::from_u64(self.checkpoint.load(Ordering::SeqCst));

        // Checkpoint stores the next position to consume. Skip when
        // committed hasn't reached the next unconsumed position yet,
        // which correctly handles the first entry at position 0.
        if committed.next() <= ckpt {
            return Ok(0);
        }

        // Replay from checkpoint (inclusive) one segment at a time.
        // Processing entries per-segment bounds memory usage: at most one
        // sealed segment (~75 MB) is loaded at a time, avoiding the
        // accumulation of all WAL entries in a single Vec.
        let boundaries = self.wal.segment_boundaries(ckpt).await;
        let mut total_consumed = 0u64;
        for (seg_start, seg_end) in &boundaries {
            if self.is_cancelled() {
                break;
            }
            // Use max(seg_start, ckpt) as the effective start to avoid
            // re-processing entries that were already consumed in a
            // previous invocation.
            let effective_start = std::cmp::max(*seg_start, ckpt);
            let entries = self.wal.replay_range(effective_start, *seg_end).await;
            if entries.is_empty() {
                continue;
            }
            for (pos, entry) in &entries {
                if self.is_cancelled() {
                    break;
                }
                match entry {
                    WalEntry::Put { hash, data } => {
                        // Write payload to Blob as Full. Idempotent: a
                        // full-encoding object is immutable once present, so
                        // the blob store skips an existing one.
                        self.blob.write(*hash, ObjectEncoding::Full, data.clone()).await?;
                        // A `Put` entry is materialized at most once. The
                        // WAL carries a per-entry position but no per-object
                        // generation, so "the entry is absent" is the only
                        // monotonicity signal available: content addressing
                        // makes `len` identical for an entry that is already
                        // materialized, and the only field a later event can
                        // have advanced is `encoding` (maintenance rewriting
                        // the object as a delta against one of its bases).
                        // Writing `Full` unconditionally would regress that
                        // newer state, so an existing entry is left alone —
                        // which also keeps a replay from resurrecting
                        // constraint bases that `prune_targets` dropped.
                        if self.metadata.get(hash).await?.is_none() {
                            // Preserve existing constraint bases, if any.
                            let existing_bases = self.metadata.get_constraint(hash).await?;
                            self.metadata
                                .put(
                                    *hash,
                                    MetadataEntry {
                                        len: data.len() as u64,
                                        encoding: ObjectEncoding::Full,
                                    },
                                )
                                .await?;
                            // Re-apply constraint bases (constraint is stored separately
                            // from metadata, so we must explicitly set it after put).
                            if !existing_bases.is_empty() {
                                self.metadata.set_constraint(*hash, existing_bases).await?;
                            }
                        }
                    }
                    WalEntry::PutLarge { hash, content_len: _ } => {
                        // Large objects are immediately materialized during
                        // put(), so the WAL consumer just advances checkpoint
                        // and trims. The payload is already in Blob + Metadata.
                        // Preserve existing constraint bases, if any.
                        let existing_bases = self.metadata.get_constraint(hash).await?;
                        if !existing_bases.is_empty() {
                            self.metadata.set_constraint(*hash, existing_bases).await?;
                        }
                    }
                    WalEntry::Delete { hash } => {
                        // Empty-content sentinel is indelible; skip deletion.
                        if *hash == Hash::empty() {
                            continue;
                        }
                        // Drop any cached reconstruction immediately — the
                        // cache must never serve bytes for a deleted object.
                        if let Some(cache) = &self.reconstructed_cache {
                            cache.invalidate(hash);
                        }
                        // Before physical deletion, re-materialize any deltas
                        // that depend on this hash as their base. This prevents
                        // dangling-delta reconstruction failure.
                        self.rematerialize_deltas_for(hash).await?;
                        self.blob.delete(hash).await?;
                        self.metadata.delete(hash).await?;
                    }
                    WalEntry::Constraint { target, bases } => {
                        self.metadata.set_constraint(*target, bases.clone()).await?;
                    }
                }
                // Advance checkpoint to the next position after this entry.
                self.checkpoint.store(pos.next().as_u64(), Ordering::SeqCst);
            }
            // Trim the segment after processing.
            if let Some((last_pos, _)) = entries.last() {
                self.wal.trim(*last_pos).await?;
            }
            total_consumed += entries.len() as u64;
        }

        Ok(total_consumed)
    }

    /// Re-materialize all delta-encoded objects that depend on `hash` as
    /// their base. After this call, each dependent is stored as
    /// [`ObjectEncoding::Full`] so it remains reachable after `hash` is
    /// physically removed from the Blob and Metadata.
    ///
    /// This is called by the WAL consumer **before** physically deleting
    /// `hash`.
    async fn rematerialize_deltas_for(&self, hash: &Hash) -> Result<(), CasError> {
        let dependents = self.metadata.list_dependents(hash).await?;

        for dep_hash in dependents {
            if self.is_cancelled() {
                break;
            }

            // Read delta envelope from blob store.
            let delta_data = self.blob.read_delta(&dep_hash).await?;
            // Base bytes are still in Blob (not yet deleted).
            // Use read_full_bytes so delta-encoded bases are reconstructed.
            let base_bytes = self.read_full_bytes(hash).await?.ok_or(CasError::NotFound(*hash))?;

            let stored_obj =
                StoredObject::decode_delta(&delta_data).map_err(|e| CasError::CorruptObject {
                    hash: Some(dep_hash),
                    details: format!("failed to decode delta envelope for re-materialization: {e}"),
                })?;
            let vcdiff = stored_obj.payload();
            let patch = crate::delta::patch::DeltaPatch::decode(vcdiff);
            let result = patch.apply(&base_bytes, dep_hash, dep_hash, *hash).map_err(|e| {
                CasError::CorruptObject {
                    hash: Some(dep_hash),
                    details: format!("delta apply failed during re-materialization: {e}"),
                }
            })?;

            // Store as Full, replacing the delta-encoded entry.
            let result_bytes = Bytes::from(result);
            self.blob.write(dep_hash, ObjectEncoding::Full, result_bytes.clone()).await?;
            // Clean up the stale .diff blob since it's now promoted to Full.
            self.blob.delete_encoding(dep_hash, ObjectEncoding::Delta { base_hash: *hash }).await?;
            // Preserve constraint bases.
            let existing_bases = self.metadata.get_constraint(&dep_hash).await?;
            self.metadata
                .put(
                    dep_hash,
                    MetadataEntry {
                        len: result_bytes.len() as u64,
                        encoding: ObjectEncoding::Full,
                    },
                )
                .await?;
            if !existing_bases.is_empty() {
                self.metadata.set_constraint(dep_hash, existing_bases).await?;
            }
        }

        Ok(())
    }

    /// Remove a target's full payload once its delta payload reconstructs it.
    ///
    /// The delta was written immediately before this call, so the full payload
    /// is a second copy of bytes the delta already holds. The reconstruction
    /// here is the tripwire that keeps that claim honest: when the stored delta
    /// does not reconstruct the target within the verified length, nothing is
    /// deleted and the caller's own read-back guard falls back to full.
    async fn reclaim_full_after_delta_verified(
        &self,
        target: Hash,
        _base: Hash,
        verified_len: u64,
    ) -> Result<(), CasError> {
        let reconstructs = self.read_full_bytes(&target).await?.is_some_and(|bytes| {
            bytes.len() as u64 == verified_len
                && blake3::hash(&bytes).as_bytes() == target.as_bytes()
        });
        if !reconstructs {
            return Ok(());
        }
        self.blob.delete_encoding(target, ObjectEncoding::Full).await
    }

    /// Converge every live target that holds both physical forms.
    ///
    /// Two forms are legitimate only while a transition is in flight, and the
    /// recorded intent says which form survives:
    ///
    /// - intent `Full` with a `.diff` present: the delta is the leftover, so it
    ///   is deleted. A full payload that no longer validates is rewritten from
    ///   the delta first, because deleting the delta first would discard the
    ///   only sound copy.
    /// - intent `Delta` with a full payload present: the full payload is the
    ///   leftover, and it is deleted only when the delta reconstructs the
    ///   target, because deleting it is what would otherwise lose the object.
    ///
    /// This runs under the caller's `consume_lock`, which is the same exclusion
    /// the optimizer's own writes take, so a both-forms state observed here is
    /// abandoned rather than mid-transition. The synchronous promotion in
    /// `set_constraint` finishes its own transition under the same lock before
    /// returning, so it cannot be caught half-written.
    ///
    /// Both branches read through `verified_form_bytes`, which builds a
    /// throwaway entry with `len: 0`: `resolve_full_bytes` dispatches on
    /// `encoding` and never consults `len`, and the hash comparison is what
    /// decides soundness, so a recorded length would be a second claim to keep
    /// in sync for nothing.
    ///
    /// A form that reads but does not hash to the target, or whose own
    /// integrity check fails, is reported as absent rather than as corrupt,
    /// because this is the pass that decides which copy to keep: the other copy
    /// may still be sound, so a hard error would abort a collapse that can
    /// succeed.
    async fn verified_form_bytes(
        &self,
        target: &Hash,
        encoding: ObjectEncoding,
    ) -> Result<Option<Bytes>, CasError> {
        let entry = MetadataEntry { len: 0, encoding };
        match super::read_view::resolve_full_bytes(
            target,
            &entry,
            &self.metadata,
            &self.blob,
            self.reconstructed_cache.as_deref(),
            "delta self-reference detected while collapsing blob forms",
            "delta chain: base",
        )
        .await
        {
            Ok(bytes) if blake3::hash(&bytes).as_bytes() == target.as_bytes() => Ok(Some(bytes)),
            // A form that is missing, reads but hashes wrong, or fails its own
            // integrity check is unsound, and the other form may still be
            // sound, so neither is an error here. Anything else (I/O, an
            // over-long chain) is not a statement about this one copy and
            // propagates.
            Ok(_) | Err(CasError::NotFound(_) | CasError::CorruptObject { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn collapse_blob_forms(&self) -> Result<bool, CasError> {
        let mut did_work = false;
        let mut targets = self.metadata.list_hashes().await?;
        targets.sort_unstable();
        for target in targets {
            if self.is_cancelled() {
                break;
            }
            let Some(entry) = self.metadata.get(&target).await? else {
                continue;
            };
            let present = self.blob.present_encodings(&target).await?;
            if !(present.full && present.delta) {
                continue;
            }
            match entry.encoding {
                ObjectEncoding::Full => {
                    // Intent is full, so the delta is the leftover. The full
                    // payload is rewritten from it first when it no longer
                    // verifies, because deleting the delta would then discard
                    // the only sound copy.
                    let base_hash = match self.blob.read_delta(&target).await {
                        Ok(envelope) => StoredObject::decode_delta(&envelope)
                            .map(|stored| stored.state().base_hash)?,
                        Err(CasError::NotFound(_)) => continue,
                        Err(error) => return Err(error),
                    };
                    if self.verified_form_bytes(&target, ObjectEncoding::Full).await?.is_none() {
                        let Some(repaired) = self
                            .verified_form_bytes(&target, ObjectEncoding::Delta { base_hash })
                            .await?
                        else {
                            tracing::warn!(
                                hash = %target,
                                "both physical forms fail to verify; recording the hash as corrupt and leaving both in place for manual repair"
                            );
                            self.metadata.mark_corrupted(target).await?;
                            did_work = true;
                            continue;
                        };
                        let repaired_len = repaired.len() as u64;
                        self.blob.write(target, ObjectEncoding::Full, repaired).await?;
                        self.metadata
                            .put(
                                target,
                                MetadataEntry { len: repaired_len, encoding: ObjectEncoding::Full },
                            )
                            .await?;
                        self.metadata.clear_corrupted(&target).await?;
                    }
                    self.metadata.clear_corrupted(&target).await?;
                    self.blob.delete_encoding(target, ObjectEncoding::Delta { base_hash }).await?;
                    did_work = true;
                }
                ObjectEncoding::Delta { base_hash } => {
                    if self
                        .verified_form_bytes(&target, ObjectEncoding::Delta { base_hash })
                        .await?
                        .is_some()
                    {
                        // Intent is delta, so the full payload is the leftover.
                        self.blob.delete_encoding(target, ObjectEncoding::Full).await?;
                        self.metadata.clear_corrupted(&target).await?;
                    } else if self
                        .verified_form_bytes(&target, ObjectEncoding::Full)
                        .await?
                        .is_some()
                    {
                        // The delta is the unsound copy: drop it and record the
                        // full payload the tree still holds, which is the repair
                        // the owner of a corrupt delta wants.
                        self.blob
                            .delete_encoding(target, ObjectEncoding::Delta { base_hash })
                            .await?;
                        self.metadata
                            .put(
                                target,
                                MetadataEntry { len: entry.len, encoding: ObjectEncoding::Full },
                            )
                            .await?;
                        self.metadata.clear_corrupted(&target).await?;
                    } else {
                        tracing::warn!(
                            hash = %target,
                            "both physical forms fail to verify; recording the hash as corrupt and leaving both in place for manual repair"
                        );
                        self.metadata.mark_corrupted(target).await?;
                        did_work = true;
                        continue;
                    }
                    did_work = true;
                }
            }
        }
        Ok(did_work)
    }

    /// Report whether storing `target` as a delta against `base` would create a
    /// delta chain cycle or an excessively deep chain.
    ///
    /// A cycle would corrupt reads, and a deep chain would hit the read-path
    /// resolution limit, so the optimizer skips any candidate that would form
    /// either.
    async fn would_form_cycle(&self, target: &Hash, base: Hash) -> Result<bool, CasError> {
        let mut current = base;
        let mut depth = 0usize;
        let mut seen = HashSet::new();
        loop {
            if current == *target {
                return Ok(true);
            }
            if !seen.insert(current) {
                return Ok(true);
            }
            let Some(entry) = self.metadata.get(&current).await? else {
                return Ok(false);
            };
            match entry.encoding {
                ObjectEncoding::Delta { base_hash } => {
                    depth += 1;
                    // Keep the resulting target chain within the read-path
                    // depth limit (MAX_DELTA_CHAIN_DEPTH). A base chain with
                    // `depth` delta edges means the target would become
                    // `depth + 1` edges deep.
                    if depth >= super::read_view::MAX_DELTA_CHAIN_DEPTH {
                        return Ok(true);
                    }
                    current = base_hash;
                }
                ObjectEncoding::Full => return Ok(false),
            }
        }
    }

    /// Clear a corruption record whose sound form a read already served.
    ///
    /// The bytes are verified and in hand, so a bookkeeping write that fails must
    /// not turn a successful read into a failure. The record that survives is the
    /// collapse pass's work, and that pass still propagates its own failures.
    async fn clear_corruption_record_best_effort(&self, hash: &Hash) {
        if let Err(error) = self.metadata.clear_corrupted(hash).await {
            tracing::warn!(
                %hash,
                %error,
                "served a sound form but could not clear the corruption record; \
                 the next collapse pass will retry it"
            );
        }
    }

    /// Reconstruct the full (reconstructed) bytes for a hash by walking
    /// any delta chain present in the Metadata + Blob.
    ///
    /// Consults and populates the shared reconstructed-bytes cache via
    /// [`resolve_full_bytes`](super::read_view::resolve_full_bytes), so
    /// repeated delta-chain walks during maintenance and rematerialization
    /// are served from the cache (spec: `src/mediapm-cas/AGENTS.md` §5.6).
    ///
    /// Returns `None` if the hash does not exist in the store. Clearing a
    /// corruption record on this path is best-effort because the bytes were
    /// already served; the collapse pass remains the authority that must not
    /// swallow a bookkeeping failure.
    async fn read_full_bytes(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        let Some(entry) = self.metadata.get(hash).await? else {
            return Ok(None);
        };

        let result = match super::read_view::resolve_full_bytes(
            hash,
            &entry,
            &self.metadata,
            &self.blob,
            self.reconstructed_cache.as_deref(),
            "delta self-reference detected during optimizer reconstruction",
            "delta chain: base",
        )
        .await
        {
            Ok(bytes) => Some(bytes),
            Err(CasError::NotFound(_)) => None,
            Err(_) => {
                // Fallback: if a full blob still exists, prefer it over a
                // corrupt delta chain and repair the metadata so later reads
                // do not hit the same corruption.
                if let Ok(full_bytes) = self.blob.read(hash).await {
                    let _ = self.blob.delete_encoding(*hash, entry.encoding).await;
                    let _ = self
                        .metadata
                        .put(
                            *hash,
                            MetadataEntry {
                                len: full_bytes.len() as u64,
                                encoding: ObjectEncoding::Full,
                            },
                        )
                        .await;
                    self.clear_corruption_record_best_effort(hash).await;
                    Some(full_bytes)
                } else {
                    None
                }
            }
        };

        Ok(result)
    }

    /// Run maintenance: optimizer + blob-form collapse + constraint pruning.
    ///
    /// 1. **Optimizer**: for each live object, select candidate bases from its
    ///    constraint (an absent or empty base set means any live object except
    ///    the empty object; a non-empty set restricts to its named live
    ///    members). The empty object named as a base means "store full". For a
    ///    non-empty real base, compute a VCDIFF delta and keep it only when it
    ///    is smaller than the full payload, skipping candidates that would form
    ///    a cycle or an excessively deep delta chain.
    /// 2. **Blob-form collapse**: converge every live target that holds both
    ///    physical forms toward its recorded intent, so a transition that
    ///    produced two copies cannot persist across intervals.
    /// 3. **Constraint pruning**: per-base prune so each entry converges
    ///    toward its effective constraint set (intersection of stored bases
    ///    with live hashes), pinning a live target whose bases all became dead
    ///    to full instead of widening it. Only prunes metadata, never objects.
    ///
    /// # Errors
    ///
    /// Delegates to WAL consumer, metadata store, and blob store operations.
    ///
    /// Returns a [`MaintenanceOutcome`] describing the work done and the number
    /// of hashes currently recorded as corrupt.
    #[expect(
        clippy::too_many_lines,
        reason = "optimizer and constraint-pruning phases share live-set state and cancellation checks; splitting would widen borrow plumbing without clarifying either phase"
    )]
    pub(crate) async fn run_maintenance(&self) -> Result<MaintenanceOutcome, CasError> {
        // Drain WAL first so we have a consistent view.
        self.run_wal_consumer().await?;

        // Refresh the reconstructed-bytes cache budget from current store
        // bytes. Best-effort: on metadata failure keep the previous budget —
        // the cache is an optimization, never a correctness requirement for
        // maintenance.
        if let Some(cache) = &self.reconstructed_cache
            && let Ok(total) = compute_store_bytes(&self.metadata).await
        {
            cache.set_max_bytes(budget_from_store_bytes(total));
        }

        let mut did_work = false;

        if self.is_cancelled() {
            return Ok(MaintenanceOutcome {
                did_work,
                corrupted_objects: self.metadata.list_corrupted().await?.len(),
            });
        }

        // === Phase 1: Optimizer ===
        // Build the live set once — it is reused for both the optimizer and
        // the pruning step that follows.
        let live: HashSet<Hash> = self.metadata.list_hashes().await?.into_iter().collect();
        let mut targets: Vec<Hash> = live.iter().copied().collect();
        targets.sort_unstable();
        for target in &targets {
            if self.is_cancelled() {
                break;
            }

            // Concern 1: which bases may this target be measured against? An
            // absent or empty base set leaves the choice open; a non-empty one
            // restricts it to the named live objects.
            let stored = self.metadata.get_constraint(target).await?;
            let mut candidates: Vec<Hash> = if stored.is_empty() {
                live.iter().copied().filter(|h| *h != *target && *h != Hash::empty()).collect()
            } else {
                stored
                    .iter()
                    .filter(|b| **b == Hash::empty() || live.contains(*b))
                    .copied()
                    .collect()
            };
            if candidates.is_empty() {
                continue;
            }
            // An unconstrained target picks from a HashSet iteration, so sort
            // for a stable base choice across runs.
            if stored.is_empty() {
                candidates.sort_unstable();
            }

            // Concern 2: the chosen base alone decides the mode. The empty
            // object named as a base means "store this full", so it is answered
            // without measurement; that is also why an unconstrained target
            // never reaches this branch, since the empty object is filtered out
            // of the open set above. A target that is already delta-encoded
            // must be rematerialized to full when the empty object is chosen.
            if candidates.iter().any(|candidate| *candidate == Hash::empty()) {
                let entry = self.metadata.get(target).await?;
                if let Some(entry) = entry
                    && let ObjectEncoding::Delta { base_hash } = entry.encoding
                {
                    let Some(target_bytes) = self.read_full_bytes(target).await? else {
                        continue;
                    };
                    self.blob.write(*target, ObjectEncoding::Full, target_bytes.clone()).await?;
                    self.blob.delete_encoding(*target, ObjectEncoding::Delta { base_hash }).await?;
                    let existing_bases = self.metadata.get_constraint(target).await?;
                    self.metadata
                        .put(
                            *target,
                            MetadataEntry {
                                len: target_bytes.len() as u64,
                                encoding: ObjectEncoding::Full,
                            },
                        )
                        .await?;
                    if !existing_bases.is_empty() {
                        self.metadata.set_constraint(*target, existing_bases).await?;
                    }
                    did_work = true;
                }
                continue;
            }

            let mut best_base = None;
            for candidate in &candidates {
                if !self.would_form_cycle(target, *candidate).await? {
                    best_base = Some(*candidate);
                    break;
                }
            }
            let Some(best_base) = best_base else {
                continue;
            };
            // Already optimal: the target is delta-encoded against the base this
            // pass would choose, and the stored payload still reconstructs it.
            // Rewriting it would produce the same bytes and a second both-forms
            // window, so the pass must report no work instead of churning on
            // every interval.
            if let Some(entry) = self.metadata.get(target).await?
                && let ObjectEncoding::Delta { base_hash } = entry.encoding
                && base_hash == best_base
                && self
                    .read_full_bytes(target)
                    .await?
                    .is_some_and(|bytes| blake3::hash(&bytes).as_bytes() == target.as_bytes())
            {
                continue;
            }
            // Reconstruct full bytes for target and base.
            let Some(target_bytes) = self.read_full_bytes(target).await? else {
                continue;
            };
            let Some(base_bytes) = self.read_full_bytes(&best_base).await? else {
                continue;
            };

            // Skip delta compression for large objects (> 16 MiB).
            // VCDIFF operates in-memory, so multi-GB objects would
            // defeat the purpose of streaming.
            if target_bytes.len() as u64 > defaults::DELTA_THRESHOLD {
                continue;
            }

            // Compute VCDIFF delta from base to target.
            let patch = DeltaPatch::diff(&base_bytes, &target_bytes)?;
            let delta_payload = patch.encode();

            // Only store delta if it is meaningfully smaller than the full
            // payload. Otherwise keep the full encoding.
            if (delta_payload.len() as u64) < target_bytes.len() as u64 {
                let stored_delta = StoredObject::delta(
                    best_base,
                    target_bytes.len() as u64,
                    delta_payload.to_vec(),
                );
                let envelope = Bytes::from(stored_delta.encode());
                // Verify the encoded envelope decodes and roundtrips as the
                // read path will. Skip the delta if the decoder disagrees.
                let verify_obj = crate::delta::object::StoredObject::decode_delta(&envelope)
                    .map_err(|e| crate::CasError::CorruptObject {
                        hash: Some(*target),
                        details: format!("delta envelope self-check: {e}"),
                    })?;
                let verify_patch = DeltaPatch::decode(verify_obj.payload());
                let verified = verify_patch
                    .apply(&base_bytes, *target, *target, best_base)
                    .is_ok_and(|b| blake3::hash(&b).as_bytes() == target.as_bytes());
                if !verified {
                    continue;
                }
                self.blob
                    .write(*target, ObjectEncoding::Delta { base_hash: best_base }, envelope)
                    .await?;
                // Reclaim the full payload now that a verified delta holds the
                // same bytes. The read-back inside the reclaim is what makes
                // deleting it safe: if the stored delta cannot reconstruct the
                // target, nothing is deleted.
                self.reclaim_full_after_delta_verified(
                    *target,
                    best_base,
                    target_bytes.len() as u64,
                )
                .await?;
                // Preserve constraint bases.
                let existing_bases = self.metadata.get_constraint(target).await?;
                self.metadata
                    .put(
                        *target,
                        MetadataEntry {
                            len: target_bytes.len() as u64,
                            encoding: ObjectEncoding::Delta { base_hash: best_base },
                        },
                    )
                    .await?;
                if !existing_bases.is_empty() {
                    self.metadata.set_constraint(*target, existing_bases.clone()).await?;
                }
                // Read-back guard: some delta/base combinations decode
                // incorrectly later in the same maintenance cycle. If the stored
                // delta cannot be reconstructed to the target hash, revert to
                // full.
                let roundtrip_ok = self
                    .read_full_bytes(target)
                    .await?
                    .is_some_and(|bytes| blake3::hash(&bytes).as_bytes() == target.as_bytes());
                if !roundtrip_ok {
                    self.blob.write(*target, ObjectEncoding::Full, target_bytes.clone()).await?;
                    self.blob
                        .delete_encoding(*target, ObjectEncoding::Delta { base_hash: best_base })
                        .await?;
                    self.metadata
                        .put(
                            *target,
                            MetadataEntry {
                                len: target_bytes.len() as u64,
                                encoding: ObjectEncoding::Full,
                            },
                        )
                        .await?;
                    if !existing_bases.is_empty() {
                        self.metadata.set_constraint(*target, existing_bases.clone()).await?;
                    }
                    continue;
                }
                did_work = true;
            }
        }

        // === Phase 2: Blob-form collapse ===
        // Both physical forms of one object are legitimate only while a
        // transition is in flight. Converging them here, under the same
        // `consume_lock` the optimizer writes under, is what keeps a store from
        // carrying two copies indefinitely.
        if self.collapse_blob_forms().await? {
            did_work = true;
        }
        let corrupted_objects = self.metadata.list_corrupted().await?.len();

        // === Phase 3: Constraint pruning ===
        // Prune dead bases from constraint entries. The live set from Phase 1
        // is still valid — the optimizer only changes encodings, not existence.
        let pruned = self.metadata.prune_targets(&live).await?;
        if pruned.targets > 0 || pruned.bases > 0 {
            did_work = true;
        }

        Ok(MaintenanceOutcome { did_work, corrupted_objects })
    }

    /// Run both WAL consumer and maintenance until nothing remains to do.
    ///
    /// # Errors
    ///
    /// Delegates to [`run_wal_consumer`](Self::run_wal_consumer) and
    /// `run_maintenance`.
    pub async fn drain_all(&self) -> Result<(), CasError> {
        self.run_wal_consumer().await?;
        self.run_maintenance().await?;
        Ok(())
    }

    /// Request cancellation of background work.
    pub fn request_cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Check if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// The serialization guard every maintenance pass holds.
    ///
    /// Exposed to the store so a state transition that must not interleave with
    /// a maintenance pass takes the same exclusion the pass does, instead of
    /// introducing a second lock that would have to be ordered against it.
    pub(crate) async fn consume_lock(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.consume_lock).lock_owned().await
    }

    /// Return the current checkpoint position.
    pub fn checkpoint_position(&self) -> WalPosition {
        WalPosition::from_u64(self.checkpoint.load(Ordering::SeqCst))
    }

    /// Statistics for the reconstructed-bytes cache, or `None` when the
    /// cache is disabled (zero TTL). Primarily for tests and observability.
    pub fn reconstructed_cache_stats(&self) -> Option<ReconstructedCacheStats> {
        self.reconstructed_cache.as_ref().map(|cache| cache.stats())
    }

    /// Access to the shared reconstructed-bytes cache, used by
    /// [`CasStore::delete`](super::store::CasStore) to invalidate entries
    /// synchronously on deletion.
    pub(crate) fn reconstructed_cache(&self) -> Option<&Arc<ReconstructedBytesCache>> {
        self.reconstructed_cache.as_ref()
    }
}

impl<J: Wal, M: MetadataStore, B: BlobStore> Clone for BackgroundEngine<J, M, B>
where
    J: Clone,
    M: Clone,
    B: Clone,
{
    fn clone(&self) -> Self {
        Self {
            wal: self.wal.clone(),
            metadata: self.metadata.clone(),
            blob: self.blob.clone(),
            read_view: self.read_view.clone(),
            checkpoint: AtomicU64::new(self.checkpoint.load(Ordering::SeqCst)),
            consume_lock: self.consume_lock.clone(),
            cancelled: self.cancelled.clone(),
            reconstructed_cache: self.reconstructed_cache.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Regression tests for WAL replay invariants.
    //!
    //! These pin the two properties `run_wal_consumer` must hold when a
    //! replay is driven by a source other than the engine's own checkpoint
    //! (a restart from an un-checkpointed position, a cloned engine, or a
    //! background consumer that overlaps a foreground `flush`).

    use std::collections::{BTreeSet, HashSet};
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::api::{CasApi, CasMaintenanceApi, ConstraintApi, ObjectEncoding};
    use crate::storage::blob_store::{BlobStore, InMemoryBlobStore};
    use crate::storage::in_memory::InMemoryCas;
    use crate::storage::metadata_store::{
        InMemoryMetadataStore, MetadataEntry, MetadataStore, PrunedConstraints,
    };
    use crate::storage::store::CasStore;
    use crate::storage::wal::InMemoryWal;

    /// Store type assembled from fresh in-memory backends, exposed so tests
    /// can rebuild a second [`CasStore`] over the *same* shared components.
    type MemStore = CasStore<InMemoryWal, InMemoryMetadataStore, InMemoryBlobStore>;

    /// Builds a store plus clones of the three shared backends. Every
    /// in-memory backend is `Arc`-backed, so the clones observe the same WAL
    /// and the same metadata — that sharing is what makes a second engine
    /// over them a faithful stand-in for a restart.
    fn shared_mem_store() -> (MemStore, InMemoryWal, InMemoryMetadataStore, InMemoryBlobStore) {
        let wal = InMemoryWal::new();
        let metadata = InMemoryMetadataStore::new();
        let blob = InMemoryBlobStore::new();
        let store = CasStore::new(
            wal.clone(),
            metadata.clone(),
            blob.clone(),
            WalPosition::ZERO,
            Duration::ZERO,
        );
        (store, wal, metadata, blob)
    }

    /// Two contents where the target is a small edit of the base, so the
    /// optimizer's VCDIFF is meaningfully smaller and the target is
    /// genuinely rewritten as a delta.
    fn similar_content_pair(fill: u8, marker: &[u8]) -> (Bytes, Bytes) {
        let base = Bytes::from(vec![fill; 4096]);
        let mut target = vec![fill; 2048];
        target.extend_from_slice(marker);
        target.extend_from_slice(&vec![fill; 2048 - marker.len()]);
        (base, Bytes::from(target))
    }

    /// Replaying a `Put` must not regress an encoding a later event advanced.
    ///
    /// A `Put` entry is materialized once, but nothing in the WAL makes its
    /// application exclusive: the same hash can be committed twice (identical
    /// content yields the same hash, so two `put()` calls append two `Put`
    /// entries), and a background consumer can replay an entry whose earlier
    /// application has since been overtaken by the optimizer. Writing the
    /// metadata entry unconditionally reset a delta-encoded object back to
    /// `Full`, discarding the optimizer's work and racing a concurrent
    /// `stat`. Replay is therefore required to be idempotent on the metadata
    /// side: content addressing makes `len` identical for an entry that is
    /// already present, and the only field a later event can have advanced is
    /// `encoding`, so "the entry is absent" is the correct condition for
    /// materializing.
    #[tokio::test]
    async fn put_replay_preserves_a_newer_delta_encoding() {
        let (store, wal, metadata, _blob) = shared_mem_store();
        let cas = InMemoryCas(store);
        let (base_content, target_content) = similar_content_pair(b'B', b"WAL_REPLAY");

        let base = cas.put(base_content).await.unwrap();
        let target = cas.put(target_content.clone()).await.unwrap();
        cas.set_constraint(target, [base].into()).await.unwrap();
        cas.run_maintenance_cycle().await.unwrap();

        let entry = metadata.get(&target).await.unwrap().expect("target must be materialized");
        assert!(
            matches!(entry.encoding, ObjectEncoding::Delta { .. }),
            "precondition: optimizer must have rewritten the target as a delta, got {:?}",
            entry.encoding
        );

        // A second `Put` entry for the same hash, as a re-commit of identical
        // content appends. Nothing about the content changed, so the
        // delta encoding the optimizer already wrote must survive.
        wal.append(WalEntry::Put { hash: target, data: target_content }).await.unwrap();
        let consumed = cas.0.bg_engine().run_wal_consumer().await.unwrap();
        assert_eq!(consumed, 1, "the appended Put entry must be the one replayed");

        let after = metadata.get(&target).await.unwrap().expect("target still materialized");
        assert!(
            matches!(after.encoding, ObjectEncoding::Delta { .. }),
            "replaying an applied Put regressed the encoding to {:?}",
            after.encoding
        );
        assert_eq!(after.len, entry.len, "replay must not change the recorded length");
    }

    /// Two consumers sharing one store must not each consume the same range.
    ///
    /// Replay batches are not independent: overlapping consumers race on the
    /// shared `<path>.tmp` staging files (so one replay's rename deletes the
    /// file the other is about to rename, failing it with `NotFound`) and on
    /// the checkpoint (which only advances at the end of a batch, so neither
    /// sees the other's progress). The background consumer spawned by
    /// `FileSystemCas::open` and a foreground `run_maintenance_cycle` are
    /// scheduled on the *same* engine, which is what this joins.
    /// [`BackgroundEngine::consume_lock`] makes the range exclusive, so the
    /// reported entry counts of two concurrent runs sum to at most the number
    /// of entries in the WAL.
    #[tokio::test]
    async fn concurrent_consumers_never_double_consume_a_range() {
        let (store, _wal, _metadata, _blob) = shared_mem_store();
        let cas = InMemoryCas(store);
        for index in 0..8u8 {
            cas.put(Bytes::from(vec![index; 128])).await.unwrap();
        }

        let (first, second) = tokio::join!(
            cas.0.bg_engine().run_wal_consumer(),
            cas.0.bg_engine().run_wal_consumer()
        );
        let consumed = first.expect("first consumer") + second.expect("second consumer");
        assert!(consumed > 0, "at least one consumer must drain the WAL");
        assert!(
            consumed <= 8,
            "entry counts {consumed} exceed the 8 WAL entries: the same range was consumed twice"
        );
    }

    /// A corruption record is part of the maintenance outcome, so a pass that
    /// clears it reports the new count instead of a stale one.
    #[tokio::test]
    async fn maintenance_reports_corrupted_count_and_clear_transition() {
        let (store, _wal, metadata, _blob) = shared_mem_store();
        let cas = InMemoryCas(store);
        let hash = cas.put(Bytes::from_static(b"corrupt-me")).await.unwrap();
        cas.0.bg_engine().run_wal_consumer().await.unwrap();

        metadata.mark_corrupted(hash).await.unwrap();
        let marked = cas.0.bg_engine().run_maintenance().await.unwrap();
        assert_eq!(marked.corrupted_objects, 1);

        metadata.clear_corrupted(&hash).await.unwrap();
        let cleared = cas.0.bg_engine().run_maintenance().await.unwrap();
        assert_eq!(cleared.corrupted_objects, 0);
    }

    /// Metadata wrapper that fails corruption-record clearing, so tests can
    /// exercise best-effort clear paths.
    #[derive(Clone)]
    struct FailClearMetadata<M> {
        inner: M,
    }

    #[async_trait::async_trait]
    impl<M: MetadataStore> MetadataStore for FailClearMetadata<M> {
        async fn put(&self, hash: Hash, entry: MetadataEntry) -> Result<(), CasError> {
            self.inner.put(hash, entry).await
        }

        async fn get(&self, hash: &Hash) -> Result<Option<MetadataEntry>, CasError> {
            self.inner.get(hash).await
        }

        async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
            self.inner.delete(hash).await
        }

        async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
            self.inner.list_hashes().await
        }

        async fn len(&self) -> usize {
            self.inner.len().await
        }

        async fn set_constraint(
            &self,
            target: Hash,
            bases: BTreeSet<Hash>,
        ) -> Result<(), CasError> {
            self.inner.set_constraint(target, bases).await
        }

        async fn get_constraint(&self, target: &Hash) -> Result<BTreeSet<Hash>, CasError> {
            self.inner.get_constraint(target).await
        }

        async fn list_targets(&self) -> Result<Vec<Hash>, CasError> {
            self.inner.list_targets().await
        }

        async fn prune_targets(&self, live: &HashSet<Hash>) -> Result<PrunedConstraints, CasError> {
            self.inner.prune_targets(live).await
        }

        async fn clear_corrupted(&self, _hash: &Hash) -> Result<(), CasError> {
            Err(CasError::internal("simulated clear_corrupted failure"))
        }

        async fn mark_corrupted(&self, hash: Hash) -> Result<(), CasError> {
            self.inner.mark_corrupted(hash).await
        }

        async fn list_corrupted(&self) -> Result<Vec<Hash>, CasError> {
            self.inner.list_corrupted().await
        }

        async fn sync_hash(&self, hash: &Hash) -> Result<(), CasError> {
            self.inner.sync_hash(hash).await
        }

        async fn set_constraint_syncing(
            &self,
            target: Hash,
            bases: BTreeSet<Hash>,
        ) -> Result<(), CasError> {
            self.inner.set_constraint_syncing(target, bases).await
        }

        async fn rebuild_from_wal(
            &self,
            wal: &dyn crate::storage::wal::Wal,
        ) -> Result<(), CasError> {
            self.inner.rebuild_from_wal(wal).await
        }
    }

    /// A `read_full_bytes` recovery that serves bytes from the full-blob fallback
    /// must not fail if clearing the corruption record fails. The bytes are
    /// already in hand; the clear is best-effort and the collapse pass remains
    /// authoritative.
    #[tokio::test]
    async fn read_full_bytes_recovery_clear_failure_is_best_effort() {
        #[derive(Clone)]
        struct SplitBlobStore {
            full: InMemoryBlobStore,
            delta: InMemoryBlobStore,
        }

        impl SplitBlobStore {
            fn new() -> Self {
                Self { full: InMemoryBlobStore::new(), delta: InMemoryBlobStore::new() }
            }
        }

        #[async_trait::async_trait]
        impl BlobStore for SplitBlobStore {
            async fn write(
                &self,
                hash: Hash,
                encoding: ObjectEncoding,
                data: Bytes,
            ) -> Result<(), CasError> {
                match encoding {
                    ObjectEncoding::Full => self.full.write(hash, encoding, data).await,
                    ObjectEncoding::Delta { .. } => self.delta.write(hash, encoding, data).await,
                }
            }

            async fn read(&self, hash: &Hash) -> Result<Bytes, CasError> {
                self.full.read(hash).await
            }

            async fn read_delta(&self, hash: &Hash) -> Result<Bytes, CasError> {
                self.delta.read_delta(hash).await
            }

            async fn delete(&self, hash: &Hash) -> Result<(), CasError> {
                self.full.delete(hash).await?;
                self.delta.delete(hash).await
            }
        }

        let wal = InMemoryWal::new();
        let inner_meta = InMemoryMetadataStore::new();
        let blob = SplitBlobStore::new();
        let metadata = FailClearMetadata { inner: inner_meta.clone() };
        let store =
            CasStore::new(wal.clone(), metadata, blob.clone(), WalPosition::ZERO, Duration::ZERO);

        let (base_bytes, target_bytes) = similar_content_pair(b'R', b"CLEAR");
        let base = store.put(base_bytes).await.unwrap();
        let target = store.put(target_bytes.clone()).await.unwrap();
        store.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
        store.set_constraint(target, BTreeSet::from([base])).await.unwrap();
        store.run_maintenance_cycle().await.unwrap();

        let entry = inner_meta.get(&target).await.unwrap().expect("target metadata");
        let ObjectEncoding::Delta { base_hash } = entry.encoding else {
            panic!("the setup requires the target to be delta-encoded");
        };

        // Corrupt the delta payload but keep the correct full blob present so
        // read_full_bytes takes the full-blob fallback path.
        blob.write(target, ObjectEncoding::Full, target_bytes.clone()).await.unwrap();
        blob.write(target, ObjectEncoding::Delta { base_hash }, Bytes::from_static(b"garbage"))
            .await
            .unwrap();

        inner_meta.mark_corrupted(target).await.unwrap();

        let recovered = store
            .bg_engine()
            .read_full_bytes(&target)
            .await
            .expect("best-effort clear failure must not fail the recovery")
            .expect("the full fallback blob must be returned");

        assert_eq!(
            recovered, target_bytes,
            "the recovery must return the verified full bytes despite the clear failure"
        );
        assert_eq!(
            inner_meta.list_corrupted().await.unwrap(),
            vec![target],
            "a best-effort clear failure must leave the corruption record for the collapse pass"
        );
    }
}
