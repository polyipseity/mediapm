//! Composed CAS store — the primary handle tying together journal, read
//! view, object store, metadata store, and background engine.
//!
//! # Architecture
//!
//! ```text
//! +--------------------------------------------------+
//! |                   CasStore                        |
//! |  +----------+  +-----------+  +---------------+   |
//! |  | Journal  |  | ReadView  |  | ObjectStore   |   |
//! |  | (WAL)    |  | (cache +  |  | (payloads)    |   |
//! |  |          |  |  store +  |  |               |   |
//! |  |          |  |  journal) |  |               |   |
//! |  +----------+  +-----------+  +---------------+   |
//! |  | MetaStore|  | BgEngine  |  |               |   |
//! |  |(constraints)|(consumer+ |  |               |   |
//! |  |          |  | maint)    |  |               |   |
//! |  +----------+  +-----------+  +---------------+   |
//! +--------------------------------------------------+
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::collections::HashSet;

use crate::api::{
    CasApi, CasMaintenanceApi, ConstraintApi, ConstraintPatch, GcSweepReport, IndexRepairReport,
    ObjectMeta, OptimizeReport, PruneReport,
};
use crate::error::CasError;
use crate::hash::Hash;

use super::bg_engine::BackgroundEngine;
use super::meta_store::MetadataStore;
use super::payload_store::ObjectStore;
use super::read_view::{ComposedReadView, ReadView};
use super::wal::{Journal, JournalEntry, JournalPosition};

/// Composed CAS store — primary handle for all CAS operations.
pub struct CasStore<J: Journal, S: ObjectStore, M: MetadataStore> {
    journal: J,
    object_store: S,
    meta_store: M,
    read_view: ComposedReadView<S, J>,
    bg_engine: BackgroundEngine<J, S, M>,
}

impl<J: Journal + Clone, S: ObjectStore + Clone, M: MetadataStore + Clone> Clone
    for CasStore<J, S, M>
{
    fn clone(&self) -> Self {
        Self {
            journal: self.journal.clone(),
            object_store: self.object_store.clone(),
            meta_store: self.meta_store.clone(),
            read_view: ComposedReadView::new(self.object_store.clone(), self.journal.clone()),
            bg_engine: self.bg_engine.clone(),
        }
    }
}

impl<J: Journal + Clone, S: ObjectStore + Clone, M: MetadataStore + Clone> CasStore<J, S, M> {
    /// Create a new composed store.
    pub fn new(journal: J, object_store: S, meta_store: M) -> Self {
        let read_view = ComposedReadView::new(object_store.clone(), journal.clone());
        let bg_engine = BackgroundEngine::new(
            journal.clone(),
            object_store.clone(),
            meta_store.clone(),
            JournalPosition::ZERO,
        );
        Self { journal, object_store, meta_store, read_view, bg_engine }
    }

    /// Rebuild metadata store from journal (for recovery after restart).
    pub async fn rebuild_meta_from_journal(&self) -> Result<(), CasError> {
        self.meta_store.rebuild_from_journal(&self.journal).await
    }

    /// Return a reference to the background engine.
    pub fn bg_engine(&self) -> &BackgroundEngine<J, S, M> {
        &self.bg_engine
    }
}

// ---------------------------------------------------------------------------
// CasApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Journal, S: ObjectStore, M: MetadataStore> CasApi for CasStore<J, S, M> {
    async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        let hash = Hash::from_content(&data);
        // Empty hash is always valid but never stored.
        if hash == Hash::zero() {
            return Ok(hash);
        }
        // Append to journal (the crash-safe commitment).
        self.journal.append(JournalEntry::Put { hash, data: data.clone() }).await?;
        // Increment generation for GC coordination.
        self.bg_engine.increment_generation();
        // Update the read-view cache so a subsequent get sees the data.
        self.read_view.hint_state_change(hash, Some(data)).await;
        Ok(hash)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        self.read_view.get(&hash).await
    }

    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError> {
        self.read_view.stat(&hash).await
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        // Deleting zero hash is a no-op (zero is never stored).
        if hash == Hash::zero() {
            return Ok(());
        }
        self.journal.append(JournalEntry::Delete { hash }).await?;
        // Update cache so subsequent reads miss.
        self.read_view.hint_state_change(hash, None).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConstraintApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Journal, S: ObjectStore, M: MetadataStore> ConstraintApi for CasStore<J, S, M> {
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        // Validate: bases must be distinct and not include target.
        if bases.contains(&target) {
            return Err(CasError::InvalidArgument(
                "constraint target cannot be its own base".into(),
            ));
        }

        // Journal the constraint first.
        self.journal.append(JournalEntry::Constraint { target, bases: bases.clone() }).await?;

        // Then update the meta store.
        self.meta_store.set(target, bases).await
    }

    async fn get_constraint(&self, target: Hash) -> Result<Option<BTreeSet<Hash>>, CasError> {
        self.meta_store.get(&target).await
    }

    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError> {
        // Read current state.
        let mut bases = self.meta_store.get(&target).await?.unwrap_or_default();

        if patch.clear {
            bases.clear();
        }
        for add in &patch.add_bases {
            if *add == target {
                return Err(CasError::InvalidArgument(
                    "constraint target cannot be its own base".into(),
                ));
            }
            bases.insert(*add);
        }
        for remove in &patch.remove_bases {
            bases.remove(remove);
        }

        // Journal the full updated constraint.
        self.journal.append(JournalEntry::Constraint { target, bases: bases.clone() }).await?;
        self.meta_store.set(target, bases).await
    }
}

// ---------------------------------------------------------------------------
// CasMaintenanceApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Journal, S: ObjectStore, M: MetadataStore> CasMaintenanceApi for CasStore<J, S, M> {
    async fn optimize_once(&self) -> Result<OptimizeReport, CasError> {
        let wal_done = self.bg_engine.run_wal_consumer().await?;
        let maint_done = self.bg_engine.run_maintenance().await?;
        Ok(OptimizeReport {
            wal_entries_consumed: if wal_done { 1 } else { 0 },
            maintenance_done: maint_done,
        })
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        let all_hashes: HashSet<Hash> =
            self.object_store.list_hashes().await?.into_iter().collect();
        let targets = self.meta_store.list_targets().await?;
        let initial_count = targets.len();
        self.meta_store.prune_targets(&all_hashes).await?;
        let final_count = self.meta_store.list_targets().await?.len();
        Ok(PruneReport { removed: initial_count.saturating_sub(final_count) })
    }

    async fn gc_sweep(&self) -> Result<GcSweepReport, CasError> {
        // Drain WAL first for a consistent view of the object store.
        self.bg_engine.run_wal_consumer().await?;

        // Prune constraint entries whose targets or bases no longer exist.
        // GC never deletes objects — objects are only removed by explicit
        // delete() operations materialized by the WAL consumer.
        // Constraints are delta-compression hints, NOT liveness indicators.
        let all_hashes: HashSet<Hash> =
            self.object_store.list_hashes().await?.into_iter().collect();
        let targets = self.meta_store.list_targets().await?;
        let initial_count = targets.len();
        self.meta_store.prune_targets(&all_hashes).await?;
        let final_count = self.meta_store.list_targets().await?.len();

        Ok(GcSweepReport { deleted: initial_count.saturating_sub(final_count) })
    }

    async fn list_all_hashes(&self) -> Result<Vec<Hash>, CasError> {
        self.object_store.list_hashes().await
    }

    async fn repair_index(&self) -> Result<IndexRepairReport, CasError> {
        // For in-memory backends, index is always consistent.
        // File-based implementations will override.
        Ok(IndexRepairReport { fixed: 0 })
    }
}
