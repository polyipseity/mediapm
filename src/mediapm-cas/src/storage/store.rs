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
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use crate::api::{
    CasApi, CasMaintenanceApi, ConstraintApi, ConstraintPatch, ObjectEncoding, ObjectMeta,
    OptimizeReport, PruneReport,
};
use crate::error::CasError;
use crate::hash::Hash;

use super::bg_engine::BackgroundEngine;
use super::blob_store::BlobStore;
use super::metadata::{Metadata, MetadataEntry};
use super::read_view::{ComposedReadView, ReadView};
use super::wal::{Wal, WalEntry, WalPosition};

/// Composed CAS store — primary handle for all CAS operations.
pub struct CasStore<J: Wal, M: Metadata, B: BlobStore> {
    wal: J,
    metadata: M,
    blob_store: B,
    read_view: Arc<dyn ReadView>,
    bg_engine: BackgroundEngine<J, M, B>,
}

impl<J: Wal + Clone, M: Metadata + Clone, B: BlobStore + Clone> Clone for CasStore<J, M, B> {
    fn clone(&self) -> Self {
        Self {
            wal: self.wal.clone(),
            metadata: self.metadata.clone(),
            blob_store: self.blob_store.clone(),
            read_view: self.read_view.clone(),
            bg_engine: self.bg_engine.clone(),
        }
    }
}

impl<J: Wal + Clone, M: Metadata + Clone, B: BlobStore + Clone> CasStore<J, M, B> {
    /// Create a new composed store.  `start_pos` tells the background
    /// engine which WAL position to begin consuming from (e.g., the
    /// last checkpoint on restart).
    ///
    /// The reconstructed-bytes cache uses a 60-second TTL by default.
    pub fn new(wal: J, metadata: M, blob_store: B, start_pos: WalPosition) -> Self
    where
        J: 'static,
        M: 'static,
        B: 'static,
    {
        let read_view: Arc<dyn ReadView> =
            Arc::new(ComposedReadView::new(metadata.clone(), wal.clone(), blob_store.clone()));
        let bg_engine = BackgroundEngine::new(
            wal.clone(),
            metadata.clone(),
            blob_store.clone(),
            start_pos,
            read_view.clone(),
            Duration::from_secs(60),
        );
        Self { wal, metadata, blob_store, read_view, bg_engine }
    }

    /// Rebuild metadata from WAL (for recovery after restart).
    pub async fn rebuild_index_from_wal(&self) -> Result<(), CasError> {
        self.metadata.rebuild_from_wal(&self.wal).await
    }

    /// Return a reference to the blob store.
    pub(crate) fn blob_store(&self) -> &B {
        &self.blob_store
    }

    /// Return a reference to the background engine.
    pub fn bg_engine(&self) -> &BackgroundEngine<J, M, B> {
        &self.bg_engine
    }
}

// ---------------------------------------------------------------------------
// CasApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Wal, M: Metadata, B: BlobStore> CasApi for CasStore<J, M, B> {
    async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        let hash = Hash::from_content(&data);
        // Append to WAL (the crash-safe commitment).
        self.wal.append(WalEntry::Put { hash, data: data.clone() }).await?;
        // Materialize BlobStore + Index immediately (write-through) when
        // both backends prefer synchronous materialization.  Otherwise
        // defer to the WAL consumer (write-back).
        if B::SYNC_MATERIALIZE && M::SYNC_MATERIALIZE {
            self.blob_store.write(hash, ObjectEncoding::Full, data.clone()).await?;
            self.metadata
                .put(hash, MetadataEntry { len: data.len() as u64, encoding: ObjectEncoding::Full })
                .await?;
        }
        // The ReadView L3 WAL fallback handles visibility for write-back
        // entries until the consumer materializes them.
        Ok(hash)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        // Empty-content sentinel is always immediately available.
        if hash == Hash::empty() {
            return Ok(Bytes::new());
        }
        self.read_view.get(&hash).await
    }

    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError> {
        // Empty-content sentinel is always immediately available.
        if hash == Hash::empty() {
            return Ok(ObjectMeta { len: 0, encoding: ObjectEncoding::Full });
        }
        self.read_view.stat(&hash).await
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        // Empty-content sentinel is indelible.
        if hash == Hash::empty() {
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
impl<J: Wal, M: Metadata, B: BlobStore> ConstraintApi for CasStore<J, M, B> {
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        // Sentinel always has empty constraints.
        if target == Hash::empty() {
            return Ok(());
        }
        // Validate: bases must be distinct and not include target.
        if bases.contains(&target) {
            return Err(CasError::InvalidArgument(
                "constraint target cannot be its own base".into(),
            ));
        }

        // Write the constraint to WAL first (crash-safe commitment).
        self.wal.append(WalEntry::Constraint { target, bases: bases.clone() }).await?;

        // Materialize Index immediately when SYNC_MATERIALIZE is true.
        // Otherwise, the WAL consumer (BackgroundEngine) will apply it.
        if M::SYNC_MATERIALIZE {
            self.metadata.set_constraint(target, bases).await?;
        }
        Ok(())
    }

    async fn get_constraint(&self, target: Hash) -> Result<BTreeSet<Hash>, CasError> {
        // Sentinel always has empty constraints.
        if target == Hash::empty() {
            return Ok(BTreeSet::new());
        }
        // Check the Index first (committed state).
        let result = self.metadata.get_constraint(&target).await?;
        if !result.is_empty() {
            return Ok(result);
        }
        // WAL fallback: check for a pending Constraint entry that hasn't
        // been consumed yet (write-back mode).
        match self.wal.check_pending_constraint(&target).await {
            Some(bases) => Ok(bases),
            None => Ok(result), // whatever the index returned (possibly empty)
        }
    }

    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError> {
        // Sentinel always has empty constraints.
        if target == Hash::empty() {
            return Ok(());
        }
        // Read current state (considers WAL fallback).
        let mut bases = self.get_constraint(target).await?;

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

        // Write the full updated constraint to WAL first.
        self.wal.append(WalEntry::Constraint { target, bases: bases.clone() }).await?;

        // Materialize Index immediately when SYNC_MATERIALIZE is true.
        if M::SYNC_MATERIALIZE {
            self.metadata.set_constraint(target, bases).await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CasMaintenanceApi impl
// ---------------------------------------------------------------------------

#[async_trait]
impl<J: Wal, M: Metadata, B: BlobStore> CasMaintenanceApi for CasStore<J, M, B> {
    async fn run_maintenance_cycle(&self) -> Result<OptimizeReport, CasError> {
        let count = self.bg_engine.run_wal_consumer().await? as usize;
        let maint_done = self.bg_engine.run_maintenance().await?;
        Ok(OptimizeReport { wal_entries_consumed: count, maintenance_done: maint_done })
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        let all_hashes: HashSet<Hash> = self.metadata.list_hashes().await?.into_iter().collect();
        let targets = self.metadata.list_targets().await?;
        let initial_count = targets.len();
        self.metadata.prune_targets(&all_hashes).await?;
        let final_count = self.metadata.list_targets().await?.len();
        Ok(PruneReport { removed: initial_count.saturating_sub(final_count) })
    }

    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
        self.metadata.list_hashes().await
    }
}

// ---------------------------------------------------------------------------
// Blanket impls for Deref-to-CasStore wrappers
//
// InMemoryCas and FileSystemCas wrap CasStore<...> with Deref and
// automatically implement all CAS traits through these blanket impls.
// ---------------------------------------------------------------------------

#[async_trait]
impl<T, J: Wal, M: Metadata, B: BlobStore> CasApi for T
where
    T: Deref<Target = CasStore<J, M, B>> + Send + Sync,
{
    async fn put(&self, data: Bytes) -> Result<Hash, CasError> {
        self.deref().put(data).await
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        self.deref().get(hash).await
    }

    async fn stat(&self, hash: Hash) -> Result<ObjectMeta, CasError> {
        self.deref().stat(hash).await
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        self.deref().delete(hash).await
    }
}

#[async_trait]
impl<T, J: Wal, M: Metadata, B: BlobStore> ConstraintApi for T
where
    T: Deref<Target = CasStore<J, M, B>> + Send + Sync,
{
    async fn set_constraint(&self, target: Hash, bases: BTreeSet<Hash>) -> Result<(), CasError> {
        self.deref().set_constraint(target, bases).await
    }

    async fn get_constraint(&self, target: Hash) -> Result<BTreeSet<Hash>, CasError> {
        self.deref().get_constraint(target).await
    }

    async fn patch_constraint(&self, target: Hash, patch: ConstraintPatch) -> Result<(), CasError> {
        self.deref().patch_constraint(target, patch).await
    }
}

#[async_trait]
impl<T, J: Wal, M: Metadata, B: BlobStore> CasMaintenanceApi for T
where
    T: Deref<Target = CasStore<J, M, B>> + Send + Sync,
{
    async fn run_maintenance_cycle(&self) -> Result<OptimizeReport, CasError> {
        self.deref().run_maintenance_cycle().await
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        self.deref().prune_constraints().await
    }

    async fn list_hashes(&self) -> Result<Vec<Hash>, CasError> {
        self.deref().list_hashes().await
    }
}
