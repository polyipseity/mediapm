//! Background engine — WAL consumer + maintenance orchestrator.
//!
//! Drives two background tasks:
//!
//! - **WAL consumer** — drains pending journal entries into the ObjectStore,
//!   then trims them from the journal.
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

use super::meta_store::MetadataStore;
use super::payload_store::ObjectStore;
use super::wal::{Journal, JournalEntry, JournalPosition};

/// Background engine driving WAL consumption and maintenance.
pub struct BackgroundEngine<J: Journal, S: ObjectStore, M: MetadataStore> {
    journal: J,
    object_store: S,
    meta_store: M,
    checkpoint: AtomicU64,
    cancelled: Arc<AtomicBool>,
    generation: AtomicU64,
}

impl<J: Journal, S: ObjectStore, M: MetadataStore> BackgroundEngine<J, S, M> {
    /// Create a new engine, checkpointing at `start_pos`.
    pub fn new(journal: J, object_store: S, meta_store: M, start_pos: JournalPosition) -> Self {
        Self {
            journal,
            object_store,
            meta_store,
            checkpoint: AtomicU64::new(start_pos.as_u64()),
            cancelled: Arc::new(AtomicBool::new(false)),
            generation: AtomicU64::new(0),
        }
    }

    /// Increment the generation counter (called after each `put`).
    pub fn increment_generation(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Return the current generation value.
    pub fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Drain the WAL consumer once: drain journal entries into ObjectStore,
    /// advancing checkpoint after each entry.
    ///
    /// Returns `true` if any work was done.
    pub async fn run_wal_consumer(&self) -> Result<bool, CasError> {
        let committed = self.journal.committed_position().await;
        let ckpt = JournalPosition::from_u64(self.checkpoint.load(Ordering::SeqCst));

        if committed <= ckpt {
            return Ok(false);
        }

        // Replay from checkpoint (inclusive). Re-processing already-consumed
        // entries is safe because puts and deletes are idempotent.
        let entries = self.journal.replay_from(ckpt).await;
        if entries.is_empty() {
            return Ok(false);
        }

        for (pos, entry) in &entries {
            if self.is_cancelled() {
                break;
            }
            match entry {
                JournalEntry::Put { hash, data } => {
                    self.object_store.put(*hash, data.clone(), ObjectEncoding::Full).await?;
                }
                JournalEntry::Delete { hash } => {
                    // Before physical deletion, re-materialize any deltas
                    // that depend on this hash as their base. This prevents
                    // dangling-delta reconstruction failure.
                    self.rematerialize_deltas_for(hash).await?;
                    self.object_store.delete(hash).await?;
                }
                JournalEntry::Constraint { target, bases } => {
                    self.meta_store.set(*target, bases.clone()).await?;
                }
            }
            // Advance checkpoint after each entry (incremental).
            self.checkpoint.store(pos.as_u64(), Ordering::SeqCst);
        }

        // Trim up to the last processed position.
        if let Some((last_pos, _)) = entries.last() {
            self.journal.trim(*last_pos).await?;
        }

        Ok(true)
    }

    /// Re-materialize all delta-encoded objects that depend on `hash` as
    /// their base. After this call, each dependent is stored as
    /// [`ObjectEncoding::Full`] so it remains reachable after `hash` is
    /// physically removed from the ObjectStore.
    ///
    /// This is called by the WAL consumer **before** physically deleting
    /// `hash`.
    async fn rematerialize_deltas_for(&self, hash: &Hash) -> Result<(), CasError> {
        let dependents: Vec<Hash> = self
            .object_store
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
            let Some((data, encoding)) = self.object_store.get(&dep_hash).await? else {
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
            // Base bytes are still in ObjectStore (not yet deleted).
            let Some((base_bytes, _)) = self.object_store.get(&base_hash).await? else {
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
            self.object_store.put(dep_hash, Bytes::from(result), ObjectEncoding::Full).await?;
        }

        Ok(())
    }

    /// Reconstruct the full (reconstructed) bytes for a hash by walking
    /// any delta chain present in the ObjectStore.
    ///
    /// Returns `None` if the hash does not exist in the store.
    async fn read_full_bytes(&self, hash: &Hash) -> Result<Option<Bytes>, CasError> {
        let mut chain: Vec<(Hash, Bytes)> = Vec::new();
        let mut current = *hash;

        loop {
            let Some((data, encoding)) = self.object_store.get(&current).await? else {
                return Ok(None);
            };

            match encoding {
                ObjectEncoding::Full => {
                    // Found the base. Apply collected deltas in reverse.
                    let mut result = data;
                    while let Some((dep_hash, dep_data)) = chain.pop() {
                        let stored_obj = StoredObject::decode_delta(&dep_data).map_err(|e| {
                            CasError::CorruptObject {
                                hash: Some(dep_hash),
                                details: format!(
                                    "failed to decode delta envelope during \
                                     optimizer reconstruction: {e}",
                                ),
                            }
                        })?;
                        let vcdiff = stored_obj.payload();
                        let patch = DeltaPatch::decode(vcdiff);
                        result = Bytes::from(
                            patch.apply(&result, dep_hash, dep_hash, current).map_err(|e| {
                                CasError::CorruptObject {
                                    hash: Some(dep_hash),
                                    details: format!(
                                        "failed to apply delta during \
                                         optimizer reconstruction: {e}",
                                    ),
                                }
                            })?,
                        );
                        current = dep_hash;
                    }
                    return Ok(Some(result));
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
    /// 1. **Optimizer**: build constraint map from MetadataStore, attempt
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
        let live: HashSet<Hash> = self.object_store.list_hashes().await?.into_iter().collect();
        let targets = self.meta_store.list_targets().await?;
        for target in &targets {
            if self.is_cancelled() {
                break;
            }
            // Skip zero hash (sentinel, never materialized).
            if *target == Hash::zero() {
                continue;
            }

            if let Some(bases) = self.meta_store.get(target).await? {
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
                        self.object_store
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
        let live: HashSet<Hash> = self.object_store.list_hashes().await?.into_iter().collect();
        let before = self.meta_store.list_targets().await?.len();
        self.meta_store.prune_targets(&live).await?;
        let after = self.meta_store.list_targets().await?.len();
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
    pub fn checkpoint_position(&self) -> JournalPosition {
        JournalPosition::from_u64(self.checkpoint.load(Ordering::SeqCst))
    }
}

impl<J: Journal, S: ObjectStore, M: MetadataStore> Clone for BackgroundEngine<J, S, M>
where
    J: Clone,
    S: Clone,
    M: Clone,
{
    fn clone(&self) -> Self {
        Self {
            journal: self.journal.clone(),
            object_store: self.object_store.clone(),
            meta_store: self.meta_store.clone(),
            checkpoint: AtomicU64::new(self.checkpoint.load(Ordering::SeqCst)),
            cancelled: self.cancelled.clone(),
            generation: AtomicU64::new(self.generation.load(Ordering::SeqCst)),
        }
    }
}
