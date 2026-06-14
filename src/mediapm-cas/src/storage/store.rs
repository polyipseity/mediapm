//! Composed CAS store — the primary handle tying together WAL, read
//! view, Index, BlobStore, and background engine.
//!
//! # Architecture
//!
//! ```text
//! +--------------------------------------------------+
//! |                   CasStore                        |
//! |  +----------+  +-----------+  +---------------+   |
//! |  | Wal      |  | ReadView  |  | Index         |   |
//! |  | (WAL)    |  | (index +  |  | (metadata +   |   |
//! |  |          |  |  blob +   |  |  constraints) |   |
//! |  |          |  |  wal)     |  |               |   |
//! |  +----------+  +-----------+  +---------------+   |
//! |  | BlobStor |  | BgEngine  |  |               |   |
//! |  | (payload)|  |(consumer+ |  |               |   |
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
    CasApi, CasMaintenanceApi, ConstraintApi, ConstraintPatch, ObjectEncoding, ObjectMeta,
    OptimizeReport, PruneReport,
};
use crate::error::CasError;
use crate::hash::Hash;

use super::bg_engine::BackgroundEngine;
use super::blob_store::BlobStore;
use super::index::{Index, IndexEntry};
use super::read_view::{ComposedReadView, ReadView};
use super::wal::{Wal, WalEntry, WalPosition};

/// Composed CAS store — primary handle for all CAS operations.
pub struct CasStore<J: Wal, I: Index, B: BlobStore> {
    wal: J,
    index: I,
    blob_store: B,
    read_view: Arc<dyn ReadView>,
    bg_engine: BackgroundEngine<J, I, B>,
}

impl<J: Wal + Clone, I: Index + Clone, B: BlobStore + Clone> Clone for CasStore<J, I, B> {
    fn clone(&self) -> Self {
        Self {
            wal: self.wal.clone(),
            index: self.index.clone(),
            blob_store: self.blob_store.clone(),
            read_view: self.read_view.clone(),
            bg_engine: self.bg_engine.clone(),
        }
    }
}

impl<J: Wal + Clone, I: Index + Clone, B: BlobStore + Clone> CasStore<J, I, B> {
    /// Create a new composed store.
    pub fn new(wal: J, index: I, blob_store: B) -> Self
    where
        J: 'static,
        I: 'static,
        B: 'static,
    {
        let read_view: Arc<dyn ReadView> =
            Arc::new(ComposedReadView::new(index.clone(), wal.clone(), blob_store.clone()));
        let bg_engine = BackgroundEngine::new(
            wal.clone(),
            index.clone(),
            blob_store.clone(),
            WalPosition::ZERO,
            read_view.clone(),
        );
        Self { wal, index, blob_store, read_view, bg_engine }
    }

    /// Rebuild index from WAL (for recovery after restart).
    pub async fn rebuild_index_from_wal(&self) -> Result<(), CasError> {
        self.index.rebuild_from_wal(&self.wal).await
    }

    /// Return a reference to the blob store.
    pub(crate) fn blob_store(&self) -> &B {
        &self.blob_store
    }

    /// Return a reference to the background engine.
    pub fn bg_engine(&self) -> &BackgroundEngine<J, I, B> {
        &self.bg_engine
    }
}

// ---------------------------------------------------------------------------
// CasApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Wal, I: Index, B: BlobStore> CasApi for CasStore<J, I, B> {
    async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        let hash = Hash::from_content(&data);
        // Zero hash is always valid but never stored.
        if hash == Hash::zero() {
            return Ok(hash);
        }
        // Append to WAL (the crash-safe commitment).
        self.wal.append(WalEntry::Put { hash, data: data.clone() }).await?;
        // Materialize BlobStore + Index immediately (write-through) when
        // both backends prefer synchronous materialization.  Otherwise
        // defer to the WAL consumer (write-back).
        if B::SYNC_MATERIALIZE && I::SYNC_MATERIALIZE {
            self.blob_store.write(hash, ObjectEncoding::Full, data.clone()).await?;
            self.index
                .put(hash, IndexEntry { len: data.len() as u64, encoding: ObjectEncoding::Full })
                .await?;
        }
        // The ReadView L3 WAL fallback handles visibility for write-back
        // entries until the consumer materializes them.
        Ok(hash)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        // Zero hash is always present (empty sentinel).
        if hash == Hash::zero() {
            return Ok(Bytes::new());
        }
        self.read_view.get(&hash).await
    }

    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError> {
        // Zero hash is always present (empty sentinel).
        if hash == Hash::zero() {
            return Ok(ObjectMeta { len: 0, encoding: ObjectEncoding::Full });
        }
        self.read_view.stat(&hash).await
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        // Deleting zero hash is a no-op (zero is never stored).
        if hash == Hash::zero() {
            return Ok(());
        }
        self.wal.append(WalEntry::Delete { hash }).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConstraintApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Wal, I: Index, B: BlobStore> ConstraintApi for CasStore<J, I, B> {
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        // Validate: bases must be distinct and not include target.
        if bases.contains(&target) {
            return Err(CasError::InvalidArgument(
                "constraint target cannot be its own base".into(),
            ));
        }

        // Write the constraint to WAL first.
        self.wal.append(WalEntry::Constraint { target, bases: bases.clone() }).await?;

        // Then update the index.
        self.index.set_constraint(target, bases).await
    }

    async fn get_constraint(&self, target: Hash) -> Result<BTreeSet<Hash>, CasError> {
        self.index.get_constraint(&target).await
    }

    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError> {
        // Read current state.
        let mut bases = self.index.get_constraint(&target).await?;

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
        self.index.set_constraint(target, bases).await
    }
}

// ---------------------------------------------------------------------------
// CasMaintenanceApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Wal, I: Index, B: BlobStore> CasMaintenanceApi for CasStore<J, I, B> {
    async fn run_maintenance_cycle(&self) -> Result<OptimizeReport, CasError> {
        let count = self.bg_engine.run_wal_consumer().await? as usize;
        let maint_done = self.bg_engine.run_maintenance().await?;
        Ok(OptimizeReport { wal_entries_consumed: count, maintenance_done: maint_done })
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        let all_hashes: HashSet<Hash> = self.index.list_hashes().await?.into_iter().collect();
        let targets = self.index.list_targets().await?;
        let initial_count = targets.len();
        self.index.prune_targets(&all_hashes).await?;
        let final_count = self.index.list_targets().await?.len();
        Ok(PruneReport { removed: initial_count.saturating_sub(final_count) })
    }

    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
        self.index.list_hashes().await
    }
}
