//! Background engine — WAL consumer + maintenance orchestrator.
//!
//! Drives two background tasks:
//!
//! - **WAL consumer** — drains pending WAL entries into the ObjectIndex,
//!   then trims them from the WAL.
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
use crate::delta::delta::DeltaPatch;
use crate::delta::object::StoredObject;
use crate::error::CasError;
use crate::hash::Hash;

use super::metadata_index::MetadataIndex;
use super::object_index::ObjectIndex;
use super::read_view::ReadView;
use super::wal::{Wal, WalEntry, WalPosition};

/// Background engine driving WAL consumption and maintenance.
pub struct BackgroundEngine<J: Wal, S: ObjectIndex, M: MetadataIndex> {
    wal: J,
    object_index: S,
    metadata_index: M,
    read_view: Arc<dyn ReadView>,
    checkpoint: AtomicU64,
    cancelled: Arc<AtomicBool>,
}

impl<J: Wal, S: ObjectIndex, M: MetadataIndex> BackgroundEngine<J, S, M> {
    /// Create a new engine, checkpointing at `start_pos`.
    pub fn new(
        wal: J,
        object_index: S,
        metadata_index: M,
        start_pos: WalPosition,
        read_view: Arc<dyn ReadView>,
    ) -> Self {
        Self {
            wal,
            object_index,
            metadata_index,
            read_view,
            checkpoint: AtomicU64::new(start_pos.as_u64()),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Drain the WAL consumer once: drain WAL entries into ObjectIndex,
    /// advancing checkpoint after each entry.
    ///
    /// Returns the number of entries consumed.
    pub async fn run_wal_consumer(&self) -> Result<u64, CasError> {
        let committed = self.wal.committed_position().await;
        let ckpt = WalPosition::from_u64(self.checkpoint.load(Ordering::SeqCst));

        if committed <= ckpt {
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
                    self.object_index.put(*hash, data.clone(), ObjectEncoding::Full).await?;
                }
                WalEntry::Delete { hash } => {
                    // Before physical deletion, re-materialize any deltas
                    // that depend on this hash as their base. This prevents
                    // dangling-delta reconstruction failure.
                    self.rematerialize_deltas_for(hash).await?;
                    self.object_index.delete(hash).await?;
                }
                WalEntry::Constraint { target, bases } => {
                    self.metadata_index.set(*target, bases.clone()).await?;
                }
            }
            // Advance checkpoint after each entry (incremental).
            self.checkpoint.store(pos.as_u64(), Ordering::SeqCst);
        }

        // Trim up to the last processed position.
        if let Some((last_pos, _)) = entries.last() {
            self.wal.trim(*last_pos).await?;
        }

        // Refresh the read-view cache with processed entries so
        // concurrent readers see materialized state.
        self.read_view.apply_batch(entries.iter().map(|(_, e)| e.clone()).collect()).await?;

        Ok(entries.len() as u64)
    }

    /// Re-materialize all delta-encoded objects that depend on `hash` as
    /// their base. After this call, each dependent is stored as
    /// [`ObjectEncoding::Full`] so it remains reachable after `hash` is
    /// physically removed from the ObjectIndex.
    ///
    /// This is called by the WAL consumer **before** physically deleting
    /// `hash`.
    async fn rematerialize_deltas_for(&self, hash: &Hash) -> Result<(), CasError> {
        let dependents: Vec<Hash> = self
            .object_index
            .list_hashes()
            .await?
            .into_iter()
            .filter(|h| {
                // Don't re-check the hash being deleted.
                h != hash
            })
            .collect();

        for dep_hash in dependents {
            if self.is_cancelled() {
                break;
            }
            // Get the dependent's current entry (delta or full).
            let Some((data, encoding)) = self.object_index.get(&dep_hash).await? else {
                continue;
            };
            let ObjectEncoding::Delta { base_hash } = encoding else {
                continue; // Not a delta, nothing to re-materialize.
            };
            if base_hash != *hash {
                continue; // Depends on a different base.
            }

            // Reconstruct original bytes.
            let stored_obj =
                StoredObject::decode_delta(&data).map_err(|e| CasError::CorruptObject {
                    hash: Some(dep_hash),
                    details: format!("failed to decode delta envelope for re-materialization: {e}"),
                })?;
            // Base bytes are still in ObjectIndex (not yet deleted).
            let Some((base_bytes, _)) = self.object_index.get(&base_hash).await? else {
                return Err(CasError::CorruptObject {
                    hash: Some(dep_hash),
                    details: format!(
                        "delta base {base_hash} missing during re-materialization of {dep_hash}"
                    ),
                });
            };
            let vcdiff = stored_obj.payload();
            let patch = crate::delta::delta::DeltaPatch::decode(vcdiff);
            let result = patch.apply(&base_bytes, dep_hash, dep_hash, base_hash).map_err(|e| {
                CasError::CorruptObject {
                    hash: Some(dep_hash),
                    details: format!("delta apply failed during re-materialization: {e}"),
                }
            })?;

            // Store as Full, replacing the delta-encoded entry.
            self.object_index.put(dep_hash, Bytes::from(result), ObjectEncoding::Full).await?;
        }

        Ok(())
    }

    /// Reconstruct the full (reconstructed) bytes for a hash by walking
    /// any delta chain present in the ObjectIndex.
    ///
    /// Returns `None` if the hash does not exist in the store.
    async fn read_full_bytes(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        let mut chain: Vec<(Hash, Bytes)> = Vec::new();
        let mut current = *hash;

        loop {
            let Some((data, encoding)) = self.object_index.get(&current).await? else {
                return Ok(None);
            };

            match encoding {
                ObjectEncoding::Full => {
                    // Found the base. Apply collected deltas in reverse.
                    return Ok(Some(crate::delta::delta::resolve_delta_chain(
                        data, &mut chain, current,
                    )?));
                }
                ObjectEncoding::Delta { base_hash } => {
                    if current == base_hash {
                        return Err(CasError::CorruptObject {
                            hash: Some(current),
                            details: "delta self-reference detected during \
                                      optimizer reconstruction"
                                .into(),
                        });
                    }
                    chain.push((current, data));
                    current = base_hash;
                }
            }
        }
    }

    /// Run maintenance: optimizer + constraint pruning.
    ///
    /// 1. **Optimizer**: build constraint map from MetadataIndex, attempt
    ///    delta rewrites. Skips zero-hash targets (sentinel).
    ///    Computes VCDIFF delta for each constraint and stores the
    ///    delta-encoded result if it is smaller than the full payload.
    /// 2. **Constraint pruning**: per-base prune so each entry converges
    ///    toward its effective constraint set (intersection of stored bases
    ///    with live hashes). Never deletes objects — only prunes metadata.
    ///
    /// GC does NOT delete objects — objects are only removed by explicit
    /// `delete()` operations (materialized by the WAL consumer). Constraints
    /// are delta-compression hints and have no bearing on object liveness.
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
        let live: HashSet<Hash> = self.object_index.list_hashes().await?.into_iter().collect();
        let targets = self.metadata_index.list_targets().await?;
        for target in &targets {
            if self.is_cancelled() {
                break;
            }
            // Skip zero hash (sentinel, never materialized).
            if *target == Hash::zero() {
                continue;
            }

            if let Some(bases) = self.metadata_index.get(target).await? {
                // Effective bases: intersection of stored bases with live
                // hashes. Dead bases cannot be used for delta reconstruction.
                let effective: Vec<&Hash> = bases.iter().filter(|b| live.contains(b)).collect();

                if let Some(best_base) = effective.first() {
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
                            **best_base,
                            target_bytes.len() as u64,
                            delta_payload.to_vec(),
                        );
                        let envelope = stored.encode();
                        self.object_index
                            .put(
                                *target,
                                Bytes::from(envelope),
                                ObjectEncoding::Delta { base_hash: **best_base },
                            )
                            .await?;
                        did_work = true;
                    }
                }
            }
        }

        // === Phase 2: Prune constraints to approach effective constraints ===
        // GC never deletes objects — objects are only removed by explicit
        // Delete operations (materialized by the WAL consumer). What GC does
        // is prune constraint metadata: per-base pruning removes individual
        // dead bases so each constraint entry converges toward the effective
        // constraint set (intersection of stored bases with live hashes).
        // Constraints are delta-compression hints and have no bearing on
        // object liveness.
        let live: HashSet<Hash> = self.object_index.list_hashes().await?.into_iter().collect();
        let before = self.metadata_index.list_targets().await?.len();
        self.metadata_index.prune_targets(&live).await?;
        let after = self.metadata_index.list_targets().await?.len();
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

impl<J: Wal, S: ObjectIndex, M: MetadataIndex> Clone for BackgroundEngine<J, S, M>
where
    J: Clone,
    S: Clone,
    M: Clone,
{
    fn clone(&self) -> Self {
        Self {
            wal: self.wal.clone(),
            object_index: self.object_index.clone(),
            metadata_index: self.metadata_index.clone(),
            read_view: self.read_view.clone(),
            checkpoint: AtomicU64::new(self.checkpoint.load(Ordering::SeqCst)),
            cancelled: self.cancelled.clone(),
        }
    }
}
