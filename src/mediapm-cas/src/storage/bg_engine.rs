//! Background engine — WAL consumer + maintenance orchestrator.
//!
//! Drives two background tasks:
//!
//! - **WAL consumer** — drains pending WAL entries into the [`BlobStore`] and
//!   [`MetadataStore`], then trims them from the WAL.
//! - **Maintenance** — combined GC + Optimizer: prunes constraint metadata to
//!   approach effective constraints (intersection of stored bases with live
//!   hashes) and evaluates delta-compression opportunities.
//!
//! GC never deletes objects — objects are only removed by explicit `delete()`
//! operations materialized by the WAL consumer. GC prunes constraint metadata
//! entries so orphaned bases (for deleted objects) are removed individually,
//! not all-or-nothing.

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

/// Background engine driving WAL consumption and maintenance.
pub struct BackgroundEngine<J: Wal, M: MetadataStore, B: BlobStore> {
    wal: J,
    metadata: M,
    blob: B,
    read_view: Arc<dyn ReadView>,
    checkpoint: AtomicU64,
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
            cancelled: Arc::new(AtomicBool::new(false)),
            reconstructed_cache,
        }
    }

    /// Drain the WAL consumer once: drain WAL entries into Blob +
    /// Metadata, advancing checkpoint after each entry.
    ///
    /// # Errors
    ///
    /// Delegates to the WAL and metadata store operations.
    ///
    /// Returns the number of entries consumed.
    pub async fn run_wal_consumer(&self) -> Result<u64, CasError> {
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
                        // Write payload to Blob as Full.
                        self.blob.write(*hash, ObjectEncoding::Full, data.clone()).await?;
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

    /// Select the best base for delta compression of `target`.
    ///
    /// Currently picks the first effective base. Future optimizations may
    /// evaluate multiple candidates (e.g., smallest VCDIFF, lowest chain depth).
    fn select_best_base<'a>(_target: &Hash, effective: &[&'a Hash]) -> Option<&'a Hash> {
        // TODO: evaluate all candidates and pick optimal base
        effective.first().copied()
    }

    /// Reconstruct the full (reconstructed) bytes for a hash by walking
    /// any delta chain present in the Metadata + Blob.
    ///
    /// Consults and populates the shared reconstructed-bytes cache via
    /// [`resolve_full_bytes`](super::read_view::resolve_full_bytes), so
    /// repeated delta-chain walks during maintenance and rematerialization
    /// are served from the cache (spec: `src/mediapm-cas/AGENTS.md` §5.6).
    ///
    /// Returns `None` if the hash does not exist in the store.
    async fn read_full_bytes(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        let Some(entry) = self.metadata.get(hash).await? else {
            return Ok(None);
        };

        let result = super::read_view::resolve_full_bytes(
            hash,
            &entry,
            &self.metadata,
            &self.blob,
            self.reconstructed_cache.as_deref(),
            "delta self-reference detected during optimizer reconstruction",
            "delta chain: base",
        )
        .await
        .map(Some)
        .or_else(|e| match e {
            CasError::NotFound(_) => Ok(None),
            other => Err(other),
        })?;

        Ok(result)
    }

    /// Run maintenance: optimizer + constraint pruning.
    ///
    /// 1. **Optimizer**: build constraint map from Metadata, attempt delta
    ///    rewrites. Computes VCDIFF delta for each constraint and stores the
    ///    delta-encoded result if it is smaller than the full payload.
    /// 2. **Constraint pruning**: per-base prune so each entry converges
    ///    toward its effective constraint set (intersection of stored bases
    ///    with live hashes). Only prunes metadata, never objects.
    ///
    /// # Errors
    ///
    /// Delegates to WAL consumer, metadata store, and blob store operations.
    ///
    /// Returns `true` if any work was done.
    pub async fn run_maintenance(&self) -> Result<bool, CasError> {
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
            return Ok(did_work);
        }

        // === Phase 1: Optimizer ===
        // Build the live set once — it is reused for both the optimizer and
        // the pruning step that follows.
        let live: HashSet<Hash> = self.metadata.list_hashes().await?.into_iter().collect();
        let targets = self.metadata.list_targets().await?;
        for target in &targets {
            if self.is_cancelled() {
                break;
            }
            let bases = self.metadata.get_constraint(target).await?;
            if !bases.is_empty() {
                // Effective bases: intersection of stored bases with live
                // hashes. Dead bases cannot be used for delta reconstruction.
                let effective: Vec<&Hash> = bases.iter().filter(|b| live.contains(b)).collect();

                if let Some(best_base) = Self::select_best_base(target, &effective) {
                    // Reconstruct full bytes for target and base.
                    let Some(target_bytes) = self.read_full_bytes(target).await? else {
                        continue;
                    };
                    let Some(base_bytes) = self.read_full_bytes(best_base).await? else {
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

                    // Only store delta if it is meaningfully smaller than
                    // the full payload. Otherwise keep the full encoding.
                    if (delta_payload.len() as u64) < target_bytes.len() as u64 {
                        let stored = StoredObject::delta(
                            *best_base,
                            target_bytes.len() as u64,
                            delta_payload.to_vec(),
                        );
                        let envelope = Bytes::from(stored.encode());
                        self.blob
                            .write(
                                *target,
                                ObjectEncoding::Delta { base_hash: *best_base },
                                envelope,
                            )
                            .await?;
                        // Preserve constraint bases.
                        let existing_bases = self.metadata.get_constraint(target).await?;
                        self.metadata
                            .put(
                                *target,
                                MetadataEntry {
                                    len: target_bytes.len() as u64,
                                    encoding: ObjectEncoding::Delta { base_hash: *best_base },
                                },
                            )
                            .await?;
                        if !existing_bases.is_empty() {
                            self.metadata.set_constraint(*target, existing_bases).await?;
                        }
                        did_work = true;
                    }
                }
            }
        }

        // === Phase 2: Constraint pruning ===
        // Prune dead bases from constraint entries. The live set from Phase 1
        // is still valid — the optimizer only changes encodings, not existence.
        let before = self.metadata.list_targets().await?.len();
        self.metadata.prune_targets(&live).await?;
        let after = self.metadata.list_targets().await?.len();
        if after < before {
            did_work = true;
        }

        Ok(did_work)
    }

    /// Run both WAL consumer and maintenance until nothing remains to do.
    ///
    /// # Errors
    ///
    /// Delegates to [`run_wal_consumer`](Self::run_wal_consumer) and
    /// [`run_maintenance`](Self::run_maintenance).
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
            cancelled: self.cancelled.clone(),
            reconstructed_cache: self.reconstructed_cache.clone(),
        }
    }
}
