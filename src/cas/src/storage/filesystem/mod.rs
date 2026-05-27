//! Filesystem-backed CAS implementation.
//!
//! This backend persists object payloads and index metadata with atomic-write
//! semantics and supports incremental optimization/pruning maintenance flows.
//!
//! ## On-disk layout
//!
//! Object fan-out paths are rooted under:
//! `/<root>/<storage-version>/<algorithm>/<h[0..2]>/<h[2..4]>/<h[4..]>`
//! where `<h...>` is the hash hex body.
//!
//! Storage-kind determination intentionally uses filename extension:
//! - full object: `<hash>` (no extension, raw payload only)
//! - delta object: `<hash>.diff` (structured diff payload)
//!
//! Constraint semantics:
//! - no explicit row means unrestricted base choice,
//! - explicit non-empty row means base must be chosen from that set.
//!
//! ## Safety and durability
//!
//! Writes use staged temp files + rename to preserve atomic update behavior for
//! both object payloads and index snapshots.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream;
use ractor::ActorStatus;

use crate::storage::{
    CasTopologySnapshot, FileSystemRecoveryOptions, render_topology_mermaid,
    render_topology_mermaid_neighborhood,
};
use crate::{
    CasApi, CasByteReader, CasByteStream, CasError, CasExistenceBitmap, CasMaintenanceApi,
    Constraint, ConstraintPatch, Hash, IndexRepairReport, ObjectInfo, OptimizeOptions,
    OptimizeReport, PruneReport, empty_content_hash,
};

/// Filesystem object-store layout version segment.
pub(crate) const STORAGE_VERSION: &str = "v1";

/// Timeout for object-actor request/response RPC calls.
const FILESYSTEM_OBJECT_ACTOR_RPC_TIMEOUT_MS: u64 = 8_000;
/// Candidate cap for unconstrained base selection.
const FILESYSTEM_UNRESTRICTED_CANDIDATE_LIMIT: usize = 32;
/// Default rewrite budget per optimize pass.
const FILESYSTEM_DEFAULT_OPTIMIZE_MAX_REWRITES: usize = 24;
/// Concurrency for candidate scoring tasks.
const FILESYSTEM_CANDIDATE_EVAL_CONCURRENCY: usize = 8;
/// Minimum full-object size for mmap-backed reads.
pub(super) const FILESYSTEM_MMAP_MIN_BYTES: u64 = 64 * 1024;
/// Chunk size used when buffering streamed writes.
const FILESYSTEM_STREAM_READ_CHUNK_BYTES: usize = 32 * 1024;
/// Max retained reusable stream buffers.
const FILESYSTEM_STREAM_BUFFER_POOL_MAX_BUFFERS: usize = 128;
/// Inline capacity hint for small hash collections.
const FILESYSTEM_SMALL_INLINE_HASHES: usize = 8;
mod actor;
mod maintenance;
mod metrics;
mod paths;
mod recovery;
mod state;
mod util;

use self::state::FileSystemState;
pub use metrics::FileSystemMetrics;
use paths::{diff_object_path, object_path};

const FILESYSTEM_DEFAULT_DROP_GRACE_PERIOD: Duration = Duration::from_secs(2);
/// Short actor-shutdown grace period for test constructors.
const FILESYSTEM_TEST_DROP_GRACE_PERIOD: Duration = Duration::from_millis(25);
/// On-disk CAS implementation with mutation-focused actor coordination.
///
/// Read paths execute directly against shared CAS state for concurrency.
/// A dedicated object actor is retained only for mutation/accounting tasks
/// that must coordinate with active memory-map leases.
pub struct FileSystemCas {
    root: PathBuf,
    state: Arc<FileSystemState>,
    drop_grace_period: Duration,
}

/// Stops object actor on drop and escalates to kill on timeout.
impl Drop for FileSystemCas {
    fn drop(&mut self) {
        self.state.object_actor.stop(Some("filesystem cas dropped".to_string()));
        let started = Instant::now();
        while self.state.object_actor.get_status() != ActorStatus::Stopped
            && started.elapsed() < self.drop_grace_period
        {
            std::thread::sleep(Duration::from_millis(1));
        }

        if self.state.object_actor.get_status() != ActorStatus::Stopped {
            self.state.object_actor.kill();
        }
    }
}

/// Public constructor/helpers and visualization/maintenance utilities.
impl FileSystemCas {
    /// Opens (or initializes) a CAS repository rooted at `root`.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when directory initialization, durable index
    /// loading/recovery, or object-actor startup fails.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, CasError> {
        Self::open_with_alpha_and_recovery(root, 4, FileSystemRecoveryOptions::default()).await
    }

    /// Opens a CAS repository with test-oriented drop behavior.
    ///
    /// This keeps normal production constructors unchanged while allowing tests
    /// to opt into a shorter actor-shutdown grace window.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] under the same failure conditions as [`Self::open`].
    pub async fn open_for_tests(root: impl AsRef<Path>) -> Result<Self, CasError> {
        Self::open_with_alpha_and_recovery_for_tests(root, 4, FileSystemRecoveryOptions::default())
            .await
    }

    /// Opens a CAS repository with an explicit optimizer depth penalty.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when backend initialization or startup recovery
    /// cannot complete.
    pub async fn open_with_alpha(root: impl AsRef<Path>, alpha: u64) -> Result<Self, CasError> {
        Self::open_with_alpha_and_recovery(root, alpha, FileSystemRecoveryOptions::default()).await
    }

    /// Opens a CAS repository with explicit optimizer and recovery settings.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when filesystem/bootstrap/index startup flows fail.
    pub async fn open_with_alpha_and_recovery(
        root: impl AsRef<Path>,
        alpha: u64,
        recovery: FileSystemRecoveryOptions,
    ) -> Result<Self, CasError> {
        Self::open_with_alpha_and_drop_grace_period(
            root,
            alpha,
            recovery,
            FILESYSTEM_DEFAULT_DROP_GRACE_PERIOD,
        )
        .await
    }

    /// Opens a CAS repository with test-oriented drop behavior and explicit
    /// optimizer depth penalty.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] under the same failure conditions as
    /// [`Self::open_with_alpha_and_recovery`].
    pub async fn open_with_alpha_for_tests(
        root: impl AsRef<Path>,
        alpha: u64,
    ) -> Result<Self, CasError> {
        Self::open_with_alpha_and_recovery_for_tests(
            root,
            alpha,
            FileSystemRecoveryOptions::default(),
        )
        .await
    }

    /// Opens a CAS repository with test-oriented drop behavior and explicit recovery settings.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when startup initialization/recovery fails.
    pub async fn open_with_alpha_and_recovery_for_tests(
        root: impl AsRef<Path>,
        alpha: u64,
        recovery: FileSystemRecoveryOptions,
    ) -> Result<Self, CasError> {
        Self::open_with_alpha_and_drop_grace_period(
            root,
            alpha,
            recovery,
            FILESYSTEM_TEST_DROP_GRACE_PERIOD,
        )
        .await
    }

    async fn open_with_alpha_and_drop_grace_period(
        root: impl AsRef<Path>,
        alpha: u64,
        recovery: FileSystemRecoveryOptions,
        drop_grace_period: Duration,
    ) -> Result<Self, CasError> {
        let root = root.as_ref().to_path_buf();
        let state =
            Arc::new(FileSystemState::open_with_alpha_and_recovery(&root, alpha, recovery).await?);
        Ok(Self { root, state, drop_grace_period })
    }

    /// Returns the canonical full-data object file path for `hash`.
    #[must_use]
    pub fn object_path_for_hash(&self, hash: Hash) -> PathBuf {
        object_path(&self.root, hash)
    }

    /// Returns the canonical `.diff` path for `hash`.
    #[must_use]
    pub fn diff_path_for_hash(&self, hash: Hash) -> PathBuf {
        diff_object_path(&self.root, hash)
    }

    /// Returns CAS root path.
    #[must_use]
    pub fn root_path(&self) -> &Path {
        &self.root
    }

    /// Enables/disables max-compression mode (`alpha = 0` when enabled).
    ///
    /// # Errors
    ///
    /// This API is intentionally fallible for parity with other runtime
    /// toggles; the current implementation does not produce an error.
    #[expect(
        clippy::unused_async,
        reason = "kept async for API-shape consistency with neighboring fallible runtime helpers"
    )]
    pub async fn set_max_compression_mode(&self, enabled: bool) -> Result<(), CasError> {
        self.state.set_max_compression_mode(enabled);
        Ok(())
    }

    /// Reports current max-compression mode flag.
    ///
    /// # Errors
    ///
    /// This API is intentionally fallible for consistency; the current
    /// implementation does not produce an error.
    #[expect(
        clippy::unused_async,
        reason = "kept async for API-shape consistency with neighboring fallible runtime helpers"
    )]
    pub async fn max_compression_mode(&self) -> Result<bool, CasError> {
        Ok(self.state.max_compression_mode())
    }

    /// Computes total CAS payload bytes currently stored on disk.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when requesting the object actor's tracked size
    /// fails (for example, actor RPC timeout/failure).
    pub async fn cas_store_size_bytes(&self) -> Result<u64, CasError> {
        self.state.cas_store_size_bytes().await
    }

    /// Returns runtime observability counters for this filesystem backend.
    ///
    /// # Errors
    ///
    /// This API is intentionally fallible for consistency; the current
    /// implementation does not produce an error.
    #[expect(
        clippy::unused_async,
        reason = "kept async for API-shape consistency with neighboring fallible runtime helpers"
    )]
    pub async fn metrics(&self) -> Result<FileSystemMetrics, CasError> {
        Ok(self.state.metrics())
    }

    /// Returns topology snapshot suitable for graph visualization.
    ///
    /// # Errors
    ///
    /// This API is intentionally fallible for consistency; the current
    /// implementation does not produce an error.
    #[expect(
        clippy::unused_async,
        reason = "kept async for API-shape consistency with neighboring fallible runtime helpers"
    )]
    pub async fn topology_snapshot(
        &self,
        include_empty: bool,
    ) -> Result<CasTopologySnapshot, CasError> {
        Ok(self.state.topology_snapshot(include_empty))
    }

    /// Renders current topology as Mermaid flowchart markup.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when topology snapshot collection fails.
    pub async fn visualize_mermaid(&self, include_empty: bool) -> Result<String, CasError> {
        let snapshot = self.topology_snapshot(include_empty).await?;
        Ok(render_topology_mermaid(&snapshot))
    }

    /// Renders a depth-limited Mermaid topology view around one target hash.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when topology snapshot collection fails.
    pub async fn visualize_mermaid_neighborhood(
        &self,
        target_hash: Hash,
        max_steps: u32,
        include_empty: bool,
    ) -> Result<String, CasError> {
        let snapshot = self.topology_snapshot(include_empty).await?;
        Ok(render_topology_mermaid_neighborhood(&snapshot, target_hash, max_steps))
    }

    /// Returns current explicit constraint bases for `target_hash`.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::NotFound`] when `target_hash` does not exist.
    #[expect(
        clippy::unused_async,
        reason = "kept async for API-shape consistency with neighboring fallible runtime helpers"
    )]
    pub async fn constraint_bases(&self, target_hash: Hash) -> Result<Vec<Hash>, CasError> {
        self.state.constraint_bases(target_hash)
    }

    /// Flushes in-memory index snapshot to redb.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when durable snapshot persistence fails.
    pub async fn flush_index_snapshot(&self) -> Result<(), CasError> {
        self.state.persist_index_snapshot().await
    }

    /// Rebuilds durable index metadata from the object store and persists it.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when object-store scanning, validation, or
    /// snapshot persistence fails.
    pub async fn repair_index(&self) -> Result<IndexRepairReport, CasError> {
        self.state.repair_index_from_object_store().await
    }

    /// Migrates durable index storage to one target schema marker.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] when migration or post-migration load/publish
    /// operations fail.
    pub async fn migrate_index_to_version(&self, target_version: u32) -> Result<(), CasError> {
        self.state.migrate_index_to_version(target_version).await
    }

    /// Reports whether an optimize run is currently active.
    ///
    /// # Errors
    ///
    /// This API is intentionally fallible for consistency; the current
    /// implementation does not produce an error.
    #[expect(
        clippy::unused_async,
        reason = "kept async for API-shape consistency with neighboring fallible runtime helpers"
    )]
    pub async fn optimize_in_progress(&self) -> Result<bool, CasError> {
        Ok(self.state.optimize_in_progress.load(Ordering::Acquire))
    }
}

#[async_trait]
/// Delegates [`CasApi`] operations into shared filesystem runtime state.
impl CasApi for FileSystemCas {
    async fn exists(&self, hash: Hash) -> Result<bool, CasError> {
        self.state.exists(hash).await
    }

    async fn exists_many(&self, hashes: Vec<Hash>) -> Result<CasExistenceBitmap, CasError> {
        self.state.exists_many(hashes).await
    }

    async fn put<D>(&self, data: D) -> Result<Hash, CasError>
    where
        D: TryInto<Bytes> + Send,
        D::Error: std::fmt::Display + Send,
    {
        let data: Bytes = data.try_into().map_err(|err| {
            CasError::invalid_input(format!("failed to convert input into bytes: {err}"))
        })?;

        self.state.put(data).await
    }

    async fn put_with_constraints<D>(
        &self,
        data: D,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError>
    where
        D: TryInto<Bytes> + Send,
        D::Error: std::fmt::Display + Send,
    {
        let hash = self.put(data).await?;
        self.set_constraint(Constraint { target_hash: hash, potential_bases: bases }).await?;
        Ok(hash)
    }

    async fn put_stream(&self, reader: CasByteReader) -> Result<Hash, CasError> {
        self.state.put_stream(reader).await
    }

    async fn put_stream_with_constraints(
        &self,
        reader: CasByteReader,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError> {
        let hash = self.put_stream(reader).await?;
        self.set_constraint(Constraint { target_hash: hash, potential_bases: bases }).await?;
        Ok(hash)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        self.state.get(hash).await
    }

    async fn get_stream(&self, hash: Hash) -> Result<CasByteStream, CasError> {
        if hash == empty_content_hash() {
            return Ok(Box::pin(stream::once(async { Ok(Bytes::new()) })));
        }

        let bytes = self.get(hash).await?;
        Ok(Box::pin(stream::once(async move { Ok(bytes) })))
    }

    async fn info(&self, hash: Hash) -> Result<ObjectInfo, CasError> {
        self.state.info(hash).await
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        self.state.delete(hash).await
    }

    async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError> {
        self.state.set_constraint(constraint).await
    }

    async fn patch_constraint(
        &self,
        target_hash: Hash,
        patch: ConstraintPatch,
    ) -> Result<Option<Constraint>, CasError> {
        self.state.patch_constraint(target_hash, patch).await
    }

    async fn get_constraint(&self, hash: Hash) -> Result<Option<Constraint>, CasError> {
        self.state.get_constraint(hash).await
    }
}

#[async_trait]
/// Delegates maintenance APIs into filesystem runtime state.
impl CasMaintenanceApi for FileSystemCas {
    async fn optimize_once(&self, options: OptimizeOptions) -> Result<OptimizeReport, CasError> {
        if self
            .state
            .optimize_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(CasError::invalid_input("optimize_once is already running"));
        }

        let result = self.state.optimize_once(options).await;
        self.state.optimize_in_progress.store(false, Ordering::Release);
        result
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        self.state.prune_constraints().await
    }

    async fn repair_index(&self) -> Result<IndexRepairReport, CasError> {
        FileSystemCas::repair_index(self).await
    }

    async fn migrate_index_to_version(&self, target_version: u32) -> Result<(), CasError> {
        FileSystemCas::migrate_index_to_version(self, target_version).await
    }
}

#[cfg(test)]
mod tests;
