//! Background engine — WAL consumer + maintenance orchestrator.
//!
//! Drives two background tasks:
//!
//! - **WAL consumer** — drains pending WAL entries into the BlobStore and
//!   Index, then trims them from the WAL.
//! - **Maintenance** — combined GC + Optimizer: prunes constraint metadata to
//!   approach effective constraints (intersection of stored bases with live
//!   hashes) and evaluates delta-compression opportunities.
//!
//! GC never deletes objects — objects are only removed by explicit `delete()`
//! operations materialized by the WAL consumer. GC prunes constraint metadata
//! entries so orphaned bases (for deleted objects) are removed individually,
//! not all-or-nothing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::api::ObjectEncoding;
use crate::delta::object::StoredObject;
use crate::delta::patch::DeltaPatch;
use crate::error::CasError;
use crate::hash::Hash;

use super::blob_store::BlobStore;
use super::index::{Index, IndexEntry};
use super::read_view::ReadView;
use super::wal::{Wal, WalEntry, WalPosition};

/// Background engine driving WAL consumption and maintenance.
pub struct BackgroundEngine<J: Wal, I: Index, B: BlobStore> {
    wal: J,
    index: I,
    blob_store: B,
    read_view: Arc<dyn ReadView>,
    checkpoint: AtomicU64,
    cancelled: Arc<AtomicBool>,
    /// Cache for reconstructed full bytes (avoids repeated delta-chain walks).
    /// Shared across clones via `Arc`.
    reconstructed_cache: Arc<Mutex<HashMap<Hash, (Bytes, Instant)>>>,
    /// TTL for cache entries (default 60s). Pushed to constructor boundary.
    reconstructed_cache_ttl: Duration,
}

impl<J: Wal, I: Index, B: BlobStore> BackgroundEngine<J, I, B> {
    /// Create a new engine, checkpointing at `start_pos`.
    ///
    /// `cache_ttl` controls how long reconstructed full bytes remain cached
    /// (default 60s). Pass `Duration::ZERO` to disable caching.
    pub(crate) fn new(
        wal: J,
        index: I,
        blob_store: B,
        start_pos: WalPosition,
        read_view: Arc<dyn ReadView>,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            wal,
            index,
            blob_store,
            read_view,
            checkpoint: AtomicU64::new(start_pos.as_u64()),
            cancelled: Arc::new(AtomicBool::new(false)),
            reconstructed_cache: Arc::new(Mutex::new(HashMap::new())),
            reconstructed_cache_ttl: cache_ttl,
        }
    }

    /// Drain the WAL consumer once: drain WAL entries into BlobStore +
    /// Index, advancing checkpoint after each entry.
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

        // Replay from checkpoint (inclusive). Re-processing already-consumed
        // entries is safe because puts and deletes are idempotent.
        let entries = self.wal.replay_from(ckpt).await;
        if entries.is_empty() {
            return Ok(0);
        }

        for (pos, entry) in &entries {
            if self.is_cancelled() {
                break;
            }
            match entry {
                WalEntry::Put { hash, data } => {
                    // Write payload to BlobStore as Full.
                    self.blob_store.write(*hash, ObjectEncoding::Full, data.clone()).await?;
                    // Preserve existing constraint bases, if any.
                    let existing_bases = self.index.get_constraint(hash).await?;
                    self.index
                        .put(
                            *hash,
                            IndexEntry { len: data.len() as u64, encoding: ObjectEncoding::Full },
                        )
                        .await?;
                    // Re-apply constraint bases (constraint is stored separately
                    // from metadata, so we must explicitly set it after put).
                    if !existing_bases.is_empty() {
                        self.index.set_constraint(*hash, existing_bases).await?;
                    }
                }
                WalEntry::Delete { hash } => {
                    // Empty-content sentinel is indelible; skip deletion.
                    if *hash == Hash::empty() {
                        continue;
                    }
                    // Before physical deletion, re-materialize any deltas
                    // that depend on this hash as their base. This prevents
                    // dangling-delta reconstruction failure.
                    self.rematerialize_deltas_for(hash).await?;
                    self.blob_store.delete(hash).await?;
                    self.index.delete(hash).await?;
                }
                WalEntry::Constraint { target, bases } => {
                    self.index.set_constraint(*target, bases.clone()).await?;
                }
            }
            // Advance checkpoint to the next position after this entry.
            self.checkpoint.store(pos.next().as_u64(), Ordering::SeqCst);
        }

        // Trim up to the last processed position.
        if let Some((last_pos, _)) = entries.last() {
            self.wal.trim(*last_pos).await?;
        }

        Ok(entries.len() as u64)
    }

    /// Re-materialize all delta-encoded objects that depend on `hash` as
    /// their base. After this call, each dependent is stored as
    /// [`ObjectEncoding::Full`] so it remains reachable after `hash` is
    /// physically removed from the BlobStore and Index.
    ///
    /// This is called by the WAL consumer **before** physically deleting
    /// `hash`.
    async fn rematerialize_deltas_for(&self, hash: &Hash) -> Result<(), CasError> {
        let dependents = self.index.list_dependents(hash).await?;

        for dep_hash in dependents {
            if self.is_cancelled() {
                break;
            }

            // Read delta envelope from blob store.
            let delta_data = self.blob_store.read_delta(&dep_hash).await?;
            // Base bytes are still in BlobStore (not yet deleted).
            // Use read_full_bytes so delta-encoded bases are reconstructed.
            let base_bytes =
                self.read_full_bytes(hash).await?.ok_or_else(|| CasError::NotFound(*hash))?;

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
            self.blob_store.write(dep_hash, ObjectEncoding::Full, result_bytes.clone()).await?;
            // Clean up the stale .diff blob since it's now promoted to Full.
            self.blob_store
                .delete_encoding(dep_hash, ObjectEncoding::Delta { base_hash: *hash })
                .await?;
            // Preserve constraint bases.
            let existing_bases = self.index.get_constraint(&dep_hash).await?;
            self.index
                .put(
                    dep_hash,
                    IndexEntry { len: result_bytes.len() as u64, encoding: ObjectEncoding::Full },
                )
                .await?;
            if !existing_bases.is_empty() {
                self.index.set_constraint(dep_hash, existing_bases).await?;
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
    /// any delta chain present in the Index + BlobStore.
    ///
    /// Uses an internal time-based cache (TTL configurable via constructor)
    /// to avoid repeated delta-chain walks during maintenance cycles.
    ///
    /// Returns `None` if the hash does not exist in the store.
    async fn read_full_bytes(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        // Check the time-based cache first.
        if self.reconstructed_cache_ttl > Duration::ZERO {
            let cache = self.reconstructed_cache.lock().unwrap();
            if let Some((cached_bytes, expiry)) = cache.get(hash) {
                if expiry.elapsed() < self.reconstructed_cache_ttl {
                    return Ok(Some(cached_bytes.clone()));
                }
            }
            // Don't hold the lock while doing I/O below.
            drop(cache);
        }

        let Some(entry) = self.index.get(hash).await? else {
            return Ok(None);
        };

        let result = super::read_view::resolve_full_bytes(
            hash,
            &entry,
            &self.index,
            &self.blob_store,
            "delta self-reference detected during optimizer reconstruction",
            "delta chain: base",
        )
        .await
        .map(Some)
        .or_else(|e| match e {
            CasError::NotFound(_) => Ok(None),
            other => Err(other),
        })?;

        // Cache the result if TTL is non-zero.
        if let Some(ref bytes) = result {
            if self.reconstructed_cache_ttl > Duration::ZERO {
                let mut cache = self.reconstructed_cache.lock().unwrap();
                cache.insert(*hash, (bytes.clone(), Instant::now()));
            }
        }

        Ok(result)
    }

    /// Run maintenance: optimizer + constraint pruning.
    ///
    /// 1. **Optimizer**: build constraint map from Index, attempt delta
    ///    rewrites. Computes VCDIFF delta for each constraint and stores the
    ///    delta-encoded result if it is smaller than the full payload.
    /// 2. **Constraint pruning**: per-base prune so each entry converges
    ///    toward its effective constraint set (intersection of stored bases
    ///    with live hashes). Only prunes metadata, never objects.
    ///
    /// Returns `true` if any work was done.
    pub async fn run_maintenance(&self) -> Result<bool, CasError> {
        // Drain WAL first so we have a consistent view.
        self.run_wal_consumer().await?;

        let mut did_work = false;

        if self.is_cancelled() {
            return Ok(did_work);
        }

        // === Phase 1: Optimizer ===
        // Build the live set once — it is reused for both the optimizer and
        // the pruning step that follows.
        let live: HashSet<Hash> = self.index.list_hashes().await?.into_iter().collect();
        let targets = self.index.list_targets().await?;
        for target in &targets {
            if self.is_cancelled() {
                break;
            }
            let bases = self.index.get_constraint(target).await?;
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
                        self.blob_store
                            .write(
                                *target,
                                ObjectEncoding::Delta { base_hash: *best_base },
                                envelope,
                            )
                            .await?;
                        // Preserve constraint bases.
                        let existing_bases = self.index.get_constraint(target).await?;
                        self.index
                            .put(
                                *target,
                                IndexEntry {
                                    len: target_bytes.len() as u64,
                                    encoding: ObjectEncoding::Delta { base_hash: *best_base },
                                },
                            )
                            .await?;
                        if !existing_bases.is_empty() {
                            self.index.set_constraint(*target, existing_bases).await?;
                        }
                        did_work = true;
                    }
                }
            }
        }

        // === Phase 2: Constraint pruning ===
        // Prune dead bases from constraint entries. The live set from Phase 1
        // is still valid — the optimizer only changes encodings, not existence.
        let before = self.index.list_targets().await?.len();
        self.index.prune_targets(&live).await?;
        let after = self.index.list_targets().await?.len();
        if after < before {
            did_work = true;
        }

        Ok(did_work)
    }

    /// Run both WAL consumer and maintenance until nothing remains to do.
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
}

impl<J: Wal, I: Index, B: BlobStore> Clone for BackgroundEngine<J, I, B>
where
    J: Clone,
    I: Clone,
    B: Clone,
{
    fn clone(&self) -> Self {
        Self {
            wal: self.wal.clone(),
            index: self.index.clone(),
            blob_store: self.blob_store.clone(),
            read_view: self.read_view.clone(),
            checkpoint: AtomicU64::new(self.checkpoint.load(Ordering::SeqCst)),
            cancelled: self.cancelled.clone(),
            reconstructed_cache: self.reconstructed_cache.clone(),
            reconstructed_cache_ttl: self.reconstructed_cache_ttl,
        }
    }
}
