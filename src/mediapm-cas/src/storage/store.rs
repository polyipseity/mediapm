//! Composed CAS store — the primary handle tying together WAL, read
//! view, object index, metadata index, and background engine.
//!
//! # Architecture
//!
//! ```text
//! +--------------------------------------------------+
//! |                   CasStore                        |
//! |  +----------+  +-----------+  +---------------+   |
//! |  | Wal      |  | ReadView  |  | ObjectIndex   |   |
//! |  | (WAL)    |  | (cache +  |  | (payloads)    |   |
//! |  |          |  |  index +  |  |               |   |
//! |  |          |  |  wal)     |  |               |   |
//! |  +----------+  +-----------+  +---------------+   |
//! |  | MetaIdx  |  | BgEngine  |  |               |   |
//! |  |(constraints)|(consumer+ |  |               |   |
//! |  |          |  | maint)    |  |               |   |
//! |  +----------+  +-----------+  +---------------+   |
//! +--------------------------------------------------+
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::sync::Arc;

use crate::api::{
    CasApi, CasMaintenanceApi, ConstraintApi, ConstraintPatch, IndexRepairReport, ObjectMeta,
    OptimizeReport, PruneReport,
};
use crate::error::CasError;
use crate::hash::Hash;

use super::bg_engine::BackgroundEngine;
use super::metadata_index::MetadataIndex;
use super::object_index::ObjectIndex;
use super::read_view::{ComposedReadView, ReadView};
use super::wal::{Wal, WalEntry, WalPosition};

/// Composed CAS store — primary handle for all CAS operations.
pub struct CasStore<J: Wal, S: ObjectIndex, M: MetadataIndex> {
    wal: J,
    object_index: S,
    metadata_index: M,
    read_view: Arc<dyn ReadView>,
    bg_engine: BackgroundEngine<J, S, M>,
}

impl<J: Wal + Clone, S: ObjectIndex + Clone, M: MetadataIndex + Clone> Clone for CasStore<J, S, M> {
    fn clone(&self) -> Self {
        Self {
            wal: self.wal.clone(),
            object_index: self.object_index.clone(),
            metadata_index: self.metadata_index.clone(),
            read_view: self.read_view.clone(),
            bg_engine: self.bg_engine.clone(),
        }
    }
}

impl<J: Wal + Clone, S: ObjectIndex + Clone, M: MetadataIndex + Clone> CasStore<J, S, M> {
    /// Create a new composed store.
    pub fn new(wal: J, object_index: S, metadata_index: M) -> Self
    where
        J: 'static,
        S: 'static,
    {
        let read_view: Arc<dyn ReadView> =
            Arc::new(ComposedReadView::new(object_index.clone(), wal.clone()));
        let bg_engine = BackgroundEngine::new(
            wal.clone(),
            object_index.clone(),
            metadata_index.clone(),
            WalPosition::ZERO,
            read_view.clone(),
        );
        Self { wal, object_index, metadata_index, read_view, bg_engine }
    }

    /// Rebuild metadata index from WAL (for recovery after restart).
    pub async fn rebuild_metadata_from_wal(&self) -> Result<(), CasError> {
        self.metadata_index.rebuild_from_wal(&self.wal).await
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
impl<J: Wal, S: ObjectIndex, M: MetadataIndex> CasApi for CasStore<J, S, M> {
    async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        let hash = Hash::from_content(&data);
        // Empty hash is always valid but never stored.
        if hash == Hash::zero() {
            return Ok(hash);
        }
        // Append to WAL (the crash-safe commitment).
        self.wal.append(WalEntry::Put { hash, data: data.clone() }).await?;
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
        self.wal.append(WalEntry::Delete { hash }).await?;
        // Update cache so subsequent reads miss.
        self.read_view.hint_state_change(hash, None).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConstraintApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Wal, S: ObjectIndex, M: MetadataIndex> ConstraintApi for CasStore<J, S, M> {
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        // Validate: bases must be distinct and not include target.
        if bases.contains(&target) {
            return Err(CasError::InvalidArgument(
                "constraint target cannot be its own base".into(),
            ));
        }

        // Write the constraint to WAL first.
        self.wal.append(WalEntry::Constraint { target, bases: bases.clone() }).await?;

        // Then update the metadata index.
        self.metadata_index.set(target, bases).await
    }

    async fn get_constraint(&self, target: Hash) -> Result<Option<BTreeSet<Hash>>, CasError> {
        self.metadata_index.get(&target).await
    }

    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError> {
        // Read current state.
        let mut bases = self.metadata_index.get(&target).await?.unwrap_or_default();

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

        // Write the full updated constraint to WAL.
        self.wal.append(WalEntry::Constraint { target, bases: bases.clone() }).await?;
        self.metadata_index.set(target, bases).await
    }
}

// ---------------------------------------------------------------------------
// CasMaintenanceApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Wal, S: ObjectIndex, M: MetadataIndex> CasMaintenanceApi for CasStore<J, S, M> {
    async fn optimize_once(&self) -> Result<OptimizeReport, CasError> {
        let count = self.bg_engine.run_wal_consumer().await? as usize;
        let maint_done = self.bg_engine.run_maintenance().await?;
        Ok(OptimizeReport { wal_entries_consumed: count, maintenance_done: maint_done })
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        let all_hashes: HashSet<Hash> =
            self.object_index.list_hashes().await?.into_iter().collect();
        let targets = self.metadata_index.list_targets().await?;
        let initial_count = targets.len();
        self.metadata_index.prune_targets(&all_hashes).await?;
        let final_count = self.metadata_index.list_targets().await?.len();
        Ok(PruneReport { removed: initial_count.saturating_sub(final_count) })
    }

    async fn list_all_hashes(&self) -> Result<Vec<Hash>, CasError> {
        self.object_index.list_hashes().await
    }

    async fn repair_index(&self) -> Result<IndexRepairReport, CasError> {
        // For in-memory backends, index is always consistent.
        // File-based implementations will override.
        // TODO(mediapm-cas#file-repair): Implement for FileSystemCas —
        // verify that FileWal entries have corresponding ObjectIndex entries
        // and vice versa, removing orphaned entries.
        Ok(IndexRepairReport { fixed: 0 })
    }
}
