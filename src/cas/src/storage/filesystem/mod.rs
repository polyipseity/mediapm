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

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use ractor::{Actor, ActorRef, ActorStatus, call_t};
use smallvec::SmallVec;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tracing::{Span, error, info, instrument};

use crate::index::{DELTA_PROMOTION_DEPTH, MAX_DELTA_DEPTH, resolve_object_depth};
use crate::storage::{
    CasTopologyConstraint, CasTopologyEncoding, CasTopologyNode, CasTopologySnapshot,
    FileSystemRecoveryOptions, StreamBufferPool, is_unconstrained_constraint_row,
    normalize_explicit_constraint_set, render_topology_mermaid,
    render_topology_mermaid_neighborhood, validate_constraint_target_not_in_bases,
};
use crate::{
    BatchOperation, CasApi, CasByteReader, CasByteStream, CasError, CasExistenceBitmap, CasIndexDb,
    CasMaintenanceApi, Constraint, ConstraintPatch, DeltaPatch, Hash, HashAlgorithm,
    IndexRepairReport, IndexState, ObjectEncoding, ObjectInfo, ObjectMeta, OptimizeOptions,
    OptimizeReport, PruneReport, StoredObject, empty_content_hash, ensure_empty_record,
    recalculate_depths,
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

use actor::{
    ActiveMmapRegistry, FileObjectActor, FileObjectActorMessage, FileObjectActorState,
    read_full_object_bytes_mmap,
};
pub use metrics::FileSystemMetrics;
use metrics::{FileSystemMetricsState, ObjectActorRpcScope};
use paths::{diff_object_path, object_path};

/// Shared filesystem CAS backend state.
struct FileSystemState {
    /// CAS filesystem root used for direct stateless read-path operations.
    root: PathBuf,
    /// Depth penalty coefficient used by optimizer scoring.
    alpha: u64,
    /// Runtime toggle for compression-first optimizer mode (`alpha = 0`).
    max_compression_mode: AtomicBool,
    /// In-memory index state guarded by a reader/writer lock.
    index: RwLock<IndexState>,
    /// Best-effort process-local cache of reconstructed object bytes.
    content_cache: Mutex<HashMap<Hash, Bytes>>,
    /// Reusable pooled stream buffers for incremental ingestion.
    stream_buffer_pool: Arc<StreamBufferPool>,
    /// Redb persistence handle used for incremental index flushes.
    index_db: CasIndexDb,
    /// Startup recovery and backup retention settings.
    recovery: FileSystemRecoveryOptions,
    /// Number of incremental mutation batches persisted since process start.
    backup_batch_counter: AtomicU64,
    /// In-process observability counters for filesystem CAS operations.
    metrics: FileSystemMetricsState,
    /// Tracks whether an optimize run is currently active.
    optimize_in_progress: AtomicBool,
    /// Shared mmap lease registry coordinating read mmap lifetimes with mutations.
    active_mmaps: Arc<ActiveMmapRegistry>,
    /// Dedicated object I/O actor for on-disk object operations.
    object_actor: ActorRef<FileObjectActorMessage>,
}

/// Internal filesystem backend runtime operations and helpers.
impl FileSystemState {
    /// Opens a CAS repository with an explicit optimizer depth penalty.
    pub async fn open_with_alpha_and_recovery(
        root: impl AsRef<Path>,
        alpha: u64,
        recovery: FileSystemRecoveryOptions,
    ) -> Result<Self, CasError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join(STORAGE_VERSION).join(empty_content_hash().algorithm_name()))
            .await
            .map_err(|source| CasError::io("creating objects root", root.clone(), source))?;

        bootstrap_empty_object(&root).await?;

        let (redb_index, mut index, recovery_report) =
            recovery::load_or_recover_primary_index(&root, &recovery)?;

        if let Some(report) = recovery_report {
            info!(
                object_rows_rebuilt = report.object_rows_rebuilt,
                explicit_constraint_rows_restored = report.explicit_constraint_rows_restored,
                scanned_object_files = report.scanned_object_files,
                skipped_object_files = report.skipped_object_files,
                backup_snapshots_considered = report.backup_snapshots_considered,
                ?report.constraint_source,
                "filesystem CAS recovered durable index state during startup"
            );
        }

        ensure_empty_record(&mut index);
        index.rebuild_constraint_reverse();
        index.rebuild_delta_reverse();

        let active_mmaps = Arc::new(ActiveMmapRegistry::default());

        let (object_actor, _handle) = Actor::spawn(
            None,
            FileObjectActor,
            FileObjectActorState {
                root: root.clone(),
                total_store_size: 0,
                active_mmaps: active_mmaps.clone(),
            },
        )
        .await
        .map_err(|err| CasError::actor_rpc("spawning file object actor", err))?;

        let cas = Self {
            root: root.clone(),
            alpha,
            max_compression_mode: AtomicBool::new(false),
            index: RwLock::new(index),
            content_cache: Mutex::new(HashMap::new()),
            stream_buffer_pool: StreamBufferPool::new(
                FILESYSTEM_STREAM_READ_CHUNK_BYTES,
                FILESYSTEM_STREAM_BUFFER_POOL_MAX_BUFFERS,
            ),
            index_db: redb_index,
            recovery,
            backup_batch_counter: AtomicU64::new(0),
            metrics: FileSystemMetricsState::default(),
            optimize_in_progress: AtomicBool::new(false),
            active_mmaps,
            object_actor,
        };

        cas.repair_index_file_invariant().await?;
        cas.persist_index_snapshot().await?;
        Ok(cas)
    }

    /// Enables/disables max-compression mode (`alpha = 0` when enabled).
    pub fn set_max_compression_mode(&self, enabled: bool) {
        self.max_compression_mode.store(enabled, Ordering::Relaxed);
    }

    /// Reports current max-compression mode flag.
    pub fn max_compression_mode(&self) -> bool {
        self.max_compression_mode.load(Ordering::Relaxed)
    }

    /// Acquires a shared read guard over runtime index state.
    fn lock_index_read(&self, _operation: &str) -> RwLockReadGuard<'_, IndexState> {
        self.index.read()
    }

    /// Acquires an exclusive write guard over runtime index state.
    fn lock_index_write(&self, _operation: &str) -> RwLockWriteGuard<'_, IndexState> {
        self.index.write()
    }

    /// Acquires content-byte cache lock for cache mutation/read operations.
    fn lock_content_cache(&self, _operation: &str) -> MutexGuard<'_, HashMap<Hash, Bytes>> {
        self.content_cache.lock()
    }

    /// Removes one hash from in-process reconstructed-byte cache.
    fn invalidate_cached_object_bytes(&self, hash: Hash) {
        let mut cache = self.lock_content_cache("invalidating object-byte cache");
        cache.remove(&hash);
    }

    /// Converts ratio inputs to `f64` for observability reporting.
    ///
    /// The metrics API intentionally exposes a floating-point compression ratio.
    /// Precision loss for very large counters is acceptable in this diagnostic
    /// view and does not affect correctness-critical planning/state logic.
    #[expect(
        clippy::cast_precision_loss,
        reason = "this ratio is diagnostic-only telemetry; minor floating-point precision loss does not affect correctness"
    )]
    fn ratio_as_f64(numerator: u64, denominator: u64) -> f64 {
        numerator as f64 / denominator as f64
    }

    /// Increments cache-hit metric counter.
    fn record_cache_hit(&self) {
        self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Accumulates delta compression accounting counters.
    fn record_delta_compression(&self, payload_len: u64, content_len: u64) {
        self.metrics.delta_payload_bytes.fetch_add(payload_len, Ordering::Relaxed);
        self.metrics.delta_content_bytes.fetch_add(content_len, Ordering::Relaxed);
    }

    /// Adds elapsed optimize runtime to cumulative runtime metrics.
    fn record_optimizer_runtime_ms(&self, runtime_ms: u64) {
        self.metrics.optimizer_runtime_ms.fetch_add(runtime_ms, Ordering::Relaxed);
    }

    /// Opens one object-actor RPC scope and updates inflight/peak counters.
    fn object_actor_rpc_scope(&self) -> ObjectActorRpcScope<'_> {
        let inflight = self.metrics.object_actor_inflight.fetch_add(1, Ordering::AcqRel) + 1;
        let mut peak = self.metrics.object_actor_inflight_peak.load(Ordering::Acquire);
        while inflight > peak {
            match self.metrics.object_actor_inflight_peak.compare_exchange_weak(
                peak,
                inflight,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => peak = current,
            }
        }

        ObjectActorRpcScope::new(&self.metrics)
    }

    /// Materializes immutable metrics snapshot from atomic counters.
    fn metrics_snapshot(&self) -> FileSystemMetrics {
        let cache_hits = self.metrics.cache_hits.load(Ordering::Relaxed);
        let delta_payload = self.metrics.delta_payload_bytes.load(Ordering::Relaxed);
        let delta_content = self.metrics.delta_content_bytes.load(Ordering::Relaxed);
        let optimizer_runtime_ms = self.metrics.optimizer_runtime_ms.load(Ordering::Relaxed);
        let object_actor_inflight = self.metrics.object_actor_inflight.load(Ordering::Relaxed);
        let object_actor_inflight_peak =
            self.metrics.object_actor_inflight_peak.load(Ordering::Relaxed);
        let object_actor_rpc_calls = self.metrics.object_actor_rpc_calls.load(Ordering::Relaxed);
        let object_actor_rpc_wait_ms =
            self.metrics.object_actor_rpc_wait_ms.load(Ordering::Relaxed);

        let delta_compression_ratio =
            if delta_content == 0 { 1.0 } else { Self::ratio_as_f64(delta_payload, delta_content) };

        FileSystemMetrics {
            cache_hits,
            delta_compression_ratio,
            optimizer_runtime_ms,
            object_actor_inflight,
            object_actor_inflight_peak,
            object_actor_rpc_calls,
            object_actor_rpc_wait_ms,
        }
    }

    /// Returns current filesystem observability counters.
    pub fn metrics(&self) -> FileSystemMetrics {
        self.metrics_snapshot()
    }

    /// Builds topology snapshot from current index state.
    ///
    /// When `include_empty` is false, the implicit empty-content node and any
    /// edges that reference it are omitted.
    fn topology_snapshot(&self, include_empty: bool) -> CasTopologySnapshot {
        let index = self.lock_index_read("building topology snapshot");

        let mut nodes: Vec<CasTopologyNode> = index
            .objects
            .iter()
            .filter_map(|(hash, meta)| {
                if !include_empty && *hash == empty_content_hash() {
                    return None;
                }

                let encoding = match meta.encoding() {
                    ObjectEncoding::Full => CasTopologyEncoding::Full,
                    ObjectEncoding::Delta { base_hash } => CasTopologyEncoding::Delta { base_hash },
                };

                Some(CasTopologyNode {
                    hash: *hash,
                    content_len: meta.content_len,
                    payload_len: meta.payload_len,
                    depth: meta.depth(),
                    encoding,
                })
            })
            .collect();
        nodes.sort_by_key(|node| node.hash);

        let mut constraints: Vec<CasTopologyConstraint> = index
            .constraints
            .iter()
            .filter_map(|(target_hash, bases)| {
                if !include_empty && *target_hash == empty_content_hash() {
                    return None;
                }

                let mut filtered: Vec<Hash> = bases
                    .iter()
                    .copied()
                    .filter(|base| include_empty || *base != empty_content_hash())
                    .collect();
                filtered.sort_unstable();
                if filtered.is_empty() {
                    return None;
                }

                Some(CasTopologyConstraint { target_hash: *target_hash, bases: filtered })
            })
            .collect();
        constraints.sort_by_key(|row| row.target_hash);

        CasTopologySnapshot { include_empty, nodes, constraints }
    }

    /// Reads bytes from cache or underlying storage, then caches result.
    async fn get_cached_object_bytes(&self, hash: Hash) -> Result<Bytes, CasError> {
        if hash == empty_content_hash() {
            return Ok(Bytes::new());
        }

        if let Some(cached) = {
            let cache = self.lock_content_cache("reading object-byte cache");
            cache.get(&hash).cloned()
        } {
            self.record_cache_hit();
            return Ok(cached);
        }

        let bytes = self.get(hash).await?;
        {
            let mut cache = self.lock_content_cache("writing object-byte cache");
            cache.insert(hash, bytes.clone());
        }
        Ok(bytes)
    }

    /// Attempts mmap-backed read for a full-object file.
    async fn read_full_object_mmap(&self, hash: Hash) -> Result<Option<Bytes>, CasError> {
        let full_path = object_path(&self.root, hash);
        if !fs::try_exists(&full_path).await.map_err(|source| {
            CasError::io("checking full object mmap source", &full_path, source)
        })? {
            return Ok(None);
        }

        let bytes = read_full_object_bytes_mmap(hash, full_path, self.active_mmaps.clone()).await?;
        Ok(Some(bytes))
    }

    /// Computes total CAS payload bytes currently stored on disk.
    pub async fn cas_store_size_bytes(&self) -> Result<u64, CasError> {
        let _rpc_scope = self.object_actor_rpc_scope();
        call_t!(
            self.object_actor,
            FileObjectActorMessage::CasStoreSizeBytes,
            FILESYSTEM_OBJECT_ACTOR_RPC_TIMEOUT_MS
        )
        .map_err(|err| CasError::actor_rpc("requesting object actor CAS store size", err))?
    }

    /// Returns current explicit constraint bases for `target_hash`.
    pub fn constraint_bases(&self, target_hash: Hash) -> Result<Vec<Hash>, CasError> {
        let index = self.lock_index_read("reading explicit constraint bases");

        if !index.objects.contains_key(&target_hash) {
            return Err(CasError::NotFound(target_hash));
        }

        let explicit = index.constraints.get(&target_hash).cloned().unwrap_or_default();
        Ok(normalize_explicit_constraint_set(explicit).unwrap_or_default().into_iter().collect())
    }

    /// Builds heuristic base candidates when no explicit constraints exist.
    ///
    /// Candidates are ranked by content-length proximity, then depth/payload,
    /// and always include the canonical empty-content base.
    fn unconstrained_candidate_bases_for_target(
        index: &IndexState,
        target_hash: Hash,
        target_content_len: u64,
    ) -> BTreeSet<Hash> {
        let mut ranked: Vec<(u64, u32, u64, Hash)> = index
            .objects
            .iter()
            .filter_map(|(candidate, meta)| {
                if *candidate == target_hash || *candidate == empty_content_hash() {
                    return None;
                }
                Some((
                    meta.content_len.abs_diff(target_content_len),
                    meta.depth(),
                    meta.payload_len,
                    *candidate,
                ))
            })
            .collect();
        ranked.sort_unstable();

        let mut selected = BTreeSet::from([empty_content_hash()]);
        for (_, _, _, candidate) in ranked.into_iter().take(FILESYSTEM_UNRESTRICTED_CANDIDATE_LIMIT)
        {
            selected.insert(candidate);
        }
        selected
    }

    /// Returns candidate bases honoring explicit constraints when present.
    fn candidate_bases_for_target(
        index: &IndexState,
        target_hash: Hash,
        target_content_len: u64,
    ) -> BTreeSet<Hash> {
        match index.constraints.get(&target_hash) {
            Some(explicit) => {
                normalize_explicit_constraint_set(explicit.clone()).unwrap_or_else(|| {
                    Self::unconstrained_candidate_bases_for_target(
                        index,
                        target_hash,
                        target_content_len,
                    )
                })
            }
            _ => Self::unconstrained_candidate_bases_for_target(
                index,
                target_hash,
                target_content_len,
            ),
        }
    }

    /// Repairs dangling index rows for missing on-disk object variants.
    async fn repair_index_file_invariant(&self) -> Result<(), CasError> {
        // AGENT_NOTE: Hard invariant for operability and maintenance:
        // if a hash exists in redb index state, at least one corresponding
        // on-disk object file (`<hash>` or `<hash>.diff`) must exist.
        let candidates: Vec<Hash> = {
            let index = self.lock_index_read("collecting index/file invariant candidates");
            index.objects.keys().copied().filter(|hash| *hash != empty_content_hash()).collect()
        };

        let mut missing = BTreeSet::new();
        for hash in candidates {
            let exists = self.object_variant_exists(hash).await?;

            if !exists {
                missing.insert(hash);
            }
        }

        if missing.is_empty() {
            return Ok(());
        }

        error!(
            missing_objects = missing.len(),
            "repairing index/file invariant by dropping dangling index rows"
        );

        {
            let mut index = self.lock_index_write("repairing index/file invariant");
            for hash in &missing {
                index.objects.remove(hash);
                index.constraints.remove(hash);
            }
            index.constraints.retain(|_, bases| {
                for hash in &missing {
                    bases.remove(hash);
                }
                !is_unconstrained_constraint_row(bases)
            });
            index.rebuild_constraint_reverse();
            recalculate_depths(&mut index)?;
        }

        Ok(())
    }

    /// Persists full index snapshot to primary store and backup snapshot.
    async fn persist_index_snapshot(&self) -> Result<(), CasError> {
        let state_snapshot = {
            let index = self.lock_index_read("snapshotting full index state for redb persistence");
            index.clone()
        };
        let index_db = self.index_db.clone();
        let root = self.root.clone();
        let max_backup_snapshots = self.recovery.max_backup_snapshots;

        tokio::task::spawn_blocking(move || {
            index_db.persist_state(&state_snapshot)?;
            recovery::write_backup_snapshot(&root, &state_snapshot, max_backup_snapshots)
        })
        .await
        .map_err(|err| CasError::task_join("persisting full index snapshot to redb", err))??;
        Ok(())
    }

    /// Returns whether current batch counter requires periodic backup write.
    fn should_write_backup_after_batch(&self) -> bool {
        if self.recovery.max_backup_snapshots == 0 {
            return false;
        }

        let interval = self.recovery.backup_snapshot_interval_ops.max(1) as u64;
        let next = self.backup_batch_counter.fetch_add(1, Ordering::AcqRel).saturating_add(1);
        next.is_multiple_of(interval)
    }

    /// Writes backup snapshot without mutating primary index tables.
    async fn write_backup_snapshot_only(&self) -> Result<(), CasError> {
        if self.recovery.max_backup_snapshots == 0 {
            return Ok(());
        }

        let state_snapshot = {
            let index = self.lock_index_read("snapshotting full index state for backup snapshot");
            index.clone()
        };
        let root = self.root.clone();
        let max_backup_snapshots = self.recovery.max_backup_snapshots;

        tokio::task::spawn_blocking(move || {
            recovery::write_backup_snapshot(&root, &state_snapshot, max_backup_snapshots)
        })
        .await
        .map_err(|err| CasError::task_join("persisting periodic index backup snapshot", err))??;
        Ok(())
    }

    /// Persists incremental index operations and optional backup snapshots.
    async fn persist_index_batch(&self, operations: Vec<BatchOperation>) -> Result<(), CasError> {
        if operations.is_empty() {
            return Ok(());
        }

        let force_backup_for_constraints = operations
            .iter()
            .any(|operation| matches!(operation, BatchOperation::SetConstraintBases { .. }));

        let index_db = self.index_db.clone();
        tokio::task::spawn_blocking(move || index_db.persist_batch(operations)).await.map_err(
            |err| CasError::task_join("persisting incremental index batch to redb", err),
        )??;

        if force_backup_for_constraints || self.should_write_backup_after_batch() {
            self.write_backup_snapshot_only().await?;
        }

        Ok(())
    }

    /// Rebuilds index state from object store and publishes repaired state.
    async fn repair_index_from_object_store(&self) -> Result<IndexRepairReport, CasError> {
        let current_constraints = {
            let index = self.lock_index_read("snapshotting explicit constraints for repair");
            index.constraints.clone()
        };
        let seed = recovery::choose_constraint_seed(&self.root, Some(current_constraints))?;
        let recovered = recovery::rebuild_index_from_object_store(&self.root, &seed)?;

        {
            let mut index = self.lock_index_write("publishing repaired index state");
            *index = recovered.state;
        }

        self.persist_index_snapshot().await?;
        Ok(recovered.report)
    }

    /// Migrates durable index schema and publishes migrated runtime state.
    async fn migrate_index_to_version(&self, target_version: u32) -> Result<(), CasError> {
        let index_db = self.index_db.clone();
        let mut migrated_state = tokio::task::spawn_blocking(move || {
            index_db.migrate_to_version(target_version)?;
            index_db.load_state()
        })
        .await
        .map_err(|err| CasError::task_join("migrating durable index schema", err))??;

        {
            let mut index = self.lock_index_write("publishing migrated index state");
            migrated_state.rebuild_delta_reverse();
            *index = migrated_state;
        }

        Ok(())
    }

    /// Loads and decodes one stored object file by hash.
    async fn read_stored_object(&self, hash: Hash) -> Result<StoredObject, CasError> {
        let diff_path = diff_object_path(&self.root, hash);
        if fs::try_exists(&diff_path).await.map_err(|source| {
            CasError::io("checking diff object existence", diff_path.clone(), source)
        })? {
            let data = fs::read(&diff_path)
                .await
                .map_err(|source| CasError::io("reading diff object", diff_path, source))?;
            return StoredObject::decode_delta(&data);
        }

        let full_path = object_path(&self.root, hash);
        if fs::try_exists(&full_path).await.map_err(|source| {
            CasError::io("checking full object existence", full_path.clone(), source)
        })? {
            let payload = fs::read(&full_path)
                .await
                .map_err(|source| CasError::io("reading full object", full_path, source))?;
            return Ok(StoredObject::full(payload));
        }

        Err(CasError::NotFound(hash))
    }

    /// Returns whether full or delta object file exists for a hash.
    async fn object_variant_exists(&self, hash: Hash) -> Result<bool, CasError> {
        let full_path = object_path(&self.root, hash);
        let diff_path = diff_object_path(&self.root, hash);

        let has_full = fs::try_exists(&full_path).await.map_err(|source| {
            CasError::io("checking indexed full object existence", full_path, source)
        })?;
        let has_diff = fs::try_exists(&diff_path).await.map_err(|source| {
            CasError::io("checking indexed diff object existence", diff_path, source)
        })?;

        Ok(has_full || has_diff)
    }

    /// Deletes both full/delta object variants and invalidates cache entry.
    async fn delete_object_files(&self, hash: Hash) -> Result<(), CasError> {
        let _rpc_scope = self.object_actor_rpc_scope();
        call_t!(
            self.object_actor,
            FileObjectActorMessage::DeleteObjectFiles,
            FILESYSTEM_OBJECT_ACTOR_RPC_TIMEOUT_MS,
            hash
        )
        .map_err(|err| CasError::actor_rpc("deleting object files via object actor", err))??;
        self.invalidate_cached_object_bytes(hash);
        Ok(())
    }

    /// Calculates optimization score for currently stored object metadata.
    fn current_score(&self, meta: &ObjectMeta) -> u128 {
        u128::from(meta.payload_len) + u128::from(self.effective_alpha()) * u128::from(meta.depth())
    }

    /// Returns effective optimizer alpha considering max-compression mode.
    fn effective_alpha(&self) -> u64 {
        if self.max_compression_mode.load(Ordering::Relaxed) { 0 } else { self.alpha }
    }

    /// Returns whether following `start` base-chain reaches `needle`.
    fn base_chain_reaches(index: &IndexState, start: Hash, needle: Hash) -> Result<bool, CasError> {
        let mut current = start;
        let mut visited = BTreeSet::new();

        loop {
            if current == needle {
                return Ok(true);
            }
            if current == empty_content_hash() {
                return Ok(false);
            }
            if !visited.insert(current) {
                return Err(CasError::CycleDetected {
                    target: start,
                    detail: format!("cycle encountered while scanning base chain from {start}"),
                });
            }

            let meta = index.objects.get(&current).ok_or_else(|| {
                CasError::corrupt_index(format!("missing metadata for base {current}"))
            })?;

            match meta.encoding() {
                ObjectEncoding::Full => return Ok(false),
                ObjectEncoding::Delta { base_hash } => {
                    current = base_hash;
                }
            }
        }
    }

    /// Evaluates one potential storage/base plan for a target object.
    async fn evaluate_candidate(
        &self,
        target_hash: Hash,
        target: &[u8],
        candidate_base: Hash,
        index: &IndexState,
    ) -> Result<Option<CandidatePlan>, CasError> {
        if candidate_base != empty_content_hash() && !index.objects.contains_key(&candidate_base) {
            return Ok(None);
        }

        if candidate_base == target_hash {
            return Ok(None);
        }

        if candidate_base != empty_content_hash()
            && Self::base_chain_reaches(index, candidate_base, target_hash)?
        {
            return Ok(None);
        }

        if candidate_base == empty_content_hash() {
            return Ok(Some(self.full_candidate_plan(target_hash, target)));
        }

        let base_bytes = self.get_cached_object_bytes(candidate_base).await?;
        let patch = DeltaPatch::diff(&base_bytes, target)?;
        let payload = patch.encode().to_vec();
        let base_depth =
            index.objects.get(&candidate_base).ok_or(CasError::NotFound(candidate_base))?.depth();
        let depth = base_depth.saturating_add(1);

        if depth >= DELTA_PROMOTION_DEPTH {
            return Ok(Some(self.full_candidate_plan(target_hash, target)));
        }

        let score = u128::from(payload.len() as u64)
            + u128::from(self.effective_alpha()) * u128::from(depth);

        Ok(Some(CandidatePlan {
            object: StoredObject::delta(candidate_base, target.len() as u64, payload),
            base_hash: candidate_base,
            depth,
            score,
        }))
    }

    /// Builds a full-object candidate plan for one target payload.
    fn full_candidate_plan(&self, target_hash: Hash, target: &[u8]) -> CandidatePlan {
        let depth = u32::from(target_hash != empty_content_hash());
        let payload = target.to_vec();
        let score = u128::from(payload.len() as u64)
            + u128::from(self.effective_alpha()) * u128::from(depth);
        CandidatePlan {
            object: StoredObject::full(payload),
            base_hash: empty_content_hash(),
            depth,
            score,
        }
    }

    /// Checks for pre-existing on-disk object and heals missing index metadata.
    async fn check_disk_collision_and_heal_if_needed(
        &self,
        hash: Hash,
        candidate_len: u64,
    ) -> Result<bool, CasError> {
        let has_variant = self.object_variant_exists(hash).await?;

        if !has_variant {
            return Ok(false);
        }

        let object = self.read_stored_object(hash).await?;
        ensure_no_length_collision(hash, object.content_len(), candidate_len)?;

        {
            let mut index = self.lock_index_write("healing index row from existing disk object");
            let seeded = match object.base_hash() {
                Some(base_hash) => {
                    ObjectMeta::delta(object.payload_len(), object.content_len(), 0, base_hash)
                }
                None => ObjectMeta::full(object.payload_len(), object.content_len(), 0),
            };
            index.objects.entry(hash).or_insert(seeded);
            recalculate_depths(&mut index)?;
        }

        self.persist_index_snapshot().await?;
        Ok(true)
    }

    /// Returns whether `candidate` should replace `current_best`.
    fn candidate_plan_is_better(candidate: &CandidatePlan, current_best: &CandidatePlan) -> bool {
        (
            candidate.score,
            candidate.object.payload().len(),
            candidate.depth,
            candidate.base_hash.code(),
            candidate.base_hash.size(),
            candidate.base_hash.digest(),
        ) < (
            current_best.score,
            current_best.object.payload().len(),
            current_best.depth,
            current_best.base_hash.code(),
            current_best.base_hash.size(),
            current_best.base_hash.digest(),
        )
    }

    /// Picks the best object storage plan from candidate bases.
    ///
    /// The planner evaluates candidates in deterministic order and chooses the
    /// minimum tuple `(score, payload_len, depth, base_identity)`.
    async fn select_best_candidate_plan(
        &self,
        target_hash: Hash,
        target: &[u8],
        index_snapshot: &IndexState,
        candidates: BTreeSet<Hash>,
    ) -> Result<Option<CandidatePlan>, CasError> {
        let mut best: Option<CandidatePlan> = None;
        let mut evaluations = stream::iter(candidates.into_iter().map(|candidate| async move {
            self.evaluate_candidate(target_hash, target, candidate, index_snapshot).await
        }))
        .buffer_unordered(FILESYSTEM_CANDIDATE_EVAL_CONCURRENCY);

        while let Some(plan_result) = evaluations.next().await {
            let plan = plan_result?;
            if let Some(plan) = plan {
                let should_replace = match &best {
                    None => true,
                    Some(previous) => Self::candidate_plan_is_better(&plan, previous),
                };
                if should_replace {
                    best = Some(plan);
                }
            }
        }

        Ok(best)
    }

    /// Persists one object representation and cleans the stale counterpart path.
    async fn persist_object_variant(
        &self,
        hash: Hash,
        object: &StoredObject,
    ) -> Result<(), CasError> {
        let _rpc_scope = self.object_actor_rpc_scope();
        call_t!(
            self.object_actor,
            FileObjectActorMessage::PersistObjectVariant,
            FILESYSTEM_OBJECT_ACTOR_RPC_TIMEOUT_MS,
            hash,
            object.clone()
        )
        .map_err(|err| CasError::actor_rpc("persisting object variant via object actor", err))??;
        if let StoredObject::Delta { state } = object {
            self.record_delta_compression(state.payload.len() as u64, state.content_len);
        }
        self.invalidate_cached_object_bytes(hash);
        Ok(())
    }

    /// Derives index metadata from one persisted object plan.
    fn meta_for_object(object: &StoredObject, depth: u32) -> ObjectMeta {
        match object.base_hash() {
            Some(base_hash) => {
                ObjectMeta::delta(object.payload_len(), object.content_len(), depth, base_hash)
            }
            None => ObjectMeta::full(object.payload_len(), object.content_len(), depth),
        }
    }

    /// Returns whether `base_hash` currently has direct delta dependents.
    fn has_dependents(index: &IndexState, base_hash: Hash) -> bool {
        index.delta_reverse.get(&base_hash).is_some_and(|children| !children.is_empty())
    }

    /// Adds one reverse delta edge (`base_hash -> child_hash`) if absent.
    fn reverse_delta_add(index: &mut IndexState, base_hash: Hash, child_hash: Hash) {
        let children = index.delta_reverse.entry(base_hash).or_default();
        if !children.contains(&child_hash) {
            children.push(child_hash);
            children.sort_unstable();
        }
    }

    /// Removes one reverse delta edge and drops empty edge rows.
    fn reverse_delta_remove(index: &mut IndexState, base_hash: Hash, child_hash: Hash) {
        let Some(children) = index.delta_reverse.get_mut(&base_hash) else {
            return;
        };

        if let Some(position) = children.iter().position(|candidate| *candidate == child_hash) {
            children.remove(position);
        }
        if children.is_empty() {
            index.delta_reverse.remove(&base_hash);
        }
    }

    /// Recomputes descendant depths rooted at changed objects.
    ///
    /// This bounded traversal avoids full recomputation unless invariant checks
    /// fail or traversal appears cyclic.
    fn rebuild_descendant_depths_local(
        index: &mut IndexState,
        roots: &BTreeSet<Hash>,
    ) -> Result<(), CasError> {
        if roots.is_empty() {
            return Ok(());
        }

        let mut queue: VecDeque<Hash> = roots.iter().copied().collect();
        let mut processed = 0usize;
        let max_processed = index.objects.len().saturating_mul(2).max(1);

        while let Some(base_hash) = queue.pop_front() {
            processed = processed.saturating_add(1);
            if processed > max_processed {
                return Err(CasError::CycleDetected {
                    target: base_hash,
                    detail: "localized descendant depth recompute exceeded bounded traversal; falling back to full index recompute".to_string(),
                });
            }

            let base_depth =
                index.objects.get(&base_hash).map(ObjectMeta::depth).ok_or_else(|| {
                    CasError::corrupt_index(format!(
                        "localized depth recompute missing base metadata for {base_hash}"
                    ))
                })?;

            let children = index.delta_reverse.get(&base_hash).cloned().unwrap_or_default();
            for child_hash in children {
                let child_meta = index.objects.get_mut(&child_hash).ok_or_else(|| {
                    CasError::corrupt_index(format!(
                        "localized depth recompute missing child metadata for {child_hash}"
                    ))
                })?;

                let ObjectEncoding::Delta { base_hash: stored_base } = child_meta.encoding() else {
                    return Err(CasError::corrupt_index(format!(
                        "localized depth recompute expected delta child {child_hash} under base {base_hash}"
                    )));
                };

                if stored_base != base_hash {
                    return Err(CasError::corrupt_index(format!(
                        "localized depth recompute found stale reverse edge: child={child_hash}, reverse_base={base_hash}, meta_base={stored_base}"
                    )));
                }

                let next_depth = base_depth.checked_add(1).ok_or_else(|| {
                    CasError::corrupt_index(format!(
                        "localized depth recompute overflow while resolving child={child_hash} from base={base_hash}"
                    ))
                })?;
                if next_depth > MAX_DELTA_DEPTH {
                    return Err(CasError::corrupt_index(format!(
                        "localized depth recompute exceeded maximum depth for child={child_hash}: depth={next_depth}, max={MAX_DELTA_DEPTH}"
                    )));
                }

                if child_meta.depth() != next_depth {
                    child_meta.set_depth(next_depth);
                    queue.push_back(child_hash);
                }
            }
        }

        Ok(())
    }

    /// Recomputes descendant depths, falling back to full graph recomputation.
    fn recompute_descendant_depths_with_fallback(
        index: &mut IndexState,
        roots: &BTreeSet<Hash>,
    ) -> Result<(), CasError> {
        if roots.is_empty() {
            return Ok(());
        }

        if Self::rebuild_descendant_depths_local(index, roots).is_ok() {
            return Ok(());
        }

        index.rebuild_delta_reverse();
        recalculate_depths(index)
    }

    /// Synchronizes reverse delta edges around one object metadata update.
    fn sync_delta_reverse_for_meta_update(
        index: &mut IndexState,
        hash: Hash,
        previous: Option<ObjectMeta>,
        next: ObjectMeta,
    ) {
        if let Some(previous) = previous
            && let ObjectEncoding::Delta { base_hash } = previous.encoding()
        {
            Self::reverse_delta_remove(index, base_hash, hash);
        }

        if let ObjectEncoding::Delta { base_hash } = next.encoding() {
            Self::reverse_delta_add(index, base_hash, hash);
        }
    }

    /// Writes or overwrites one object metadata row using an incremental depth
    /// update when possible, with full recalculation only if dependents may be
    /// affected by a depth/base change.
    fn upsert_object_meta(
        index: &mut IndexState,
        hash: Hash,
        object: &StoredObject,
    ) -> Result<u32, CasError> {
        let encoding = match object.base_hash() {
            Some(base_hash) => ObjectEncoding::Delta { base_hash },
            None => ObjectEncoding::Full,
        };

        let depth = resolve_object_depth(index, hash, encoding)?;

        let next = Self::meta_for_object(object, depth);
        let previous = index.objects.insert(hash, next);
        Self::sync_delta_reverse_for_meta_update(index, hash, previous, next);

        let requires_descendant_recompute = previous.is_some_and(|old| {
            let depth_or_base_changed = old.depth() != next.depth()
                || old.base_hash() != next.base_hash()
                || old.is_full() != next.is_full();

            depth_or_base_changed && Self::has_dependents(index, hash)
        });

        if requires_descendant_recompute {
            let roots = BTreeSet::from([hash]);
            Self::recompute_descendant_depths_with_fallback(index, &roots)?;
        }

        Ok(depth)
    }

    /// Validates collision invariants and reports whether put should insert data.
    ///
    /// Returns `Ok(false)` when object already exists with matching content
    /// length and no further write is required.
    async fn ensure_put_target_needs_insert(
        &self,
        target_hash: Hash,
        candidate_len: u64,
    ) -> Result<bool, CasError> {
        let existing_meta = {
            let index = self.lock_index_read("checking existing target metadata before put");
            index.objects.get(&target_hash).copied()
        };

        if let Some(meta) = existing_meta {
            ensure_no_length_collision(target_hash, meta.content_len, candidate_len)?;
            return Ok(false);
        }

        if self.check_disk_collision_and_heal_if_needed(target_hash, candidate_len).await? {
            return Ok(false);
        }

        Ok(true)
    }

    /// Persists one new object as a full payload (hot put path).
    async fn put_new_full_object(&self, target_hash: Hash, data: Bytes) -> Result<Hash, CasError> {
        let candidate_len = data.len() as u64;
        if !self.ensure_put_target_needs_insert(target_hash, candidate_len).await? {
            return Ok(target_hash);
        }

        let full = StoredObject::full(data.to_vec());
        self.persist_object_variant(target_hash, &full).await?;

        let resolved_depth = {
            let mut index = self.lock_index_write("updating index after hot full put");
            Self::upsert_object_meta(&mut index, target_hash, &full)?
        };

        self.persist_index_batch(vec![BatchOperation::UpsertObject {
            hash: target_hash,
            meta: Self::meta_for_object(&full, resolved_depth),
        }])
        .await?;
        Ok(target_hash)
    }

    /// Collects hashes that currently reference `base_hash` as their delta base.
    fn direct_dependents(
        snapshot: &IndexState,
        base_hash: Hash,
    ) -> SmallVec<[Hash; FILESYSTEM_SMALL_INLINE_HASHES]> {
        snapshot
            .delta_reverse
            .get(&base_hash)
            .map(|children| children.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Removes one object metadata row and verifies reverse-index cleanliness.
    fn remove_object_meta(index: &mut IndexState, hash: Hash) -> Result<(), CasError> {
        let removed = index.objects.remove(&hash).ok_or(CasError::NotFound(hash))?;

        if let ObjectEncoding::Delta { base_hash } = removed.encoding() {
            Self::reverse_delta_remove(index, base_hash, hash);
        }

        let dangling_children = index.delta_reverse.remove(&hash).unwrap_or_default();
        if !dangling_children.is_empty() {
            return Err(CasError::corrupt_index(format!(
                "cannot remove object metadata for {hash} while reverse delta map still contains {} direct dependents",
                dangling_children.len()
            )));
        }

        Ok(())
    }

    /// Adds one reverse explicit-constraint edge (`base -> target`) if absent.
    fn reverse_link_add(index: &mut IndexState, base_hash: Hash, target_hash: Hash) {
        let targets = index.constraint_reverse.entry(base_hash).or_default();
        if !targets.contains(&target_hash) {
            targets.push(target_hash);
            targets.sort_unstable();
        }
    }

    /// Removes one reverse explicit-constraint edge and prunes empty rows.
    fn reverse_link_remove(index: &mut IndexState, base_hash: Hash, target_hash: Hash) {
        let Some(targets) = index.constraint_reverse.get_mut(&base_hash) else {
            return;
        };

        if let Some(index) = targets.iter().position(|candidate| *candidate == target_hash) {
            targets.remove(index);
        }
        if targets.is_empty() {
            index.constraint_reverse.remove(&base_hash);
        }
    }

    /// Applies patch semantics to an existing explicit constraint candidate set.
    fn merge_constraint_patch(
        existing: Option<&BTreeSet<Hash>>,
        patch: ConstraintPatch,
    ) -> BTreeSet<Hash> {
        let mut merged = if patch.clear_existing {
            BTreeSet::new()
        } else {
            existing.cloned().unwrap_or_default()
        };

        for base in patch.remove_bases {
            merged.remove(&base);
        }
        for base in patch.add_bases {
            merged.insert(base);
        }

        merged
    }

    /// Writes one normalized explicit constraint row and maintains reverse index.
    ///
    /// Returns the persisted explicit set, or `None` when row normalizes to
    /// unconstrained semantics and is removed.
    fn set_constraint_row_optic(
        index: &mut IndexState,
        target_hash: Hash,
        candidate_bases: BTreeSet<Hash>,
    ) -> Option<BTreeSet<Hash>> {
        let previous = index.constraints.get(&target_hash).cloned().unwrap_or_default();
        let next = normalize_explicit_constraint_set(candidate_bases).unwrap_or_default();

        if next.is_empty() {
            index.constraints.remove(&target_hash);
        } else {
            index.constraints.insert(target_hash, next.clone());
        }

        for base in previous.difference(&next) {
            Self::reverse_link_remove(index, *base, target_hash);
        }
        for base in next.difference(&previous) {
            Self::reverse_link_add(index, *base, target_hash);
        }

        normalize_explicit_constraint_set(next)
    }

    /// Removes a hash from explicit constraints as target and candidate.
    fn remove_constraint_references(index: &mut IndexState, hash: Hash) {
        if let Some(previous_target_bases) = index.constraints.remove(&hash) {
            for base in previous_target_bases {
                Self::reverse_link_remove(index, base, hash);
            }
        }

        let affected_targets = index.constraint_reverse.remove(&hash).unwrap_or_default();
        for target_hash in affected_targets {
            let Some(previous_bases) = index.constraints.get(&target_hash).cloned() else {
                continue;
            };

            let mut next_bases = previous_bases;
            next_bases.remove(&hash);
            let _ = Self::set_constraint_row_optic(index, target_hash, next_bases);
        }
    }

    /// Computes incremental persistence operations from `before -> after` state.
    fn index_diff_operations(before: &IndexState, after: &IndexState) -> Vec<BatchOperation> {
        let mut operations = Vec::new();

        for (hash, meta) in &after.objects {
            if *hash == empty_content_hash() {
                continue;
            }

            if before.objects.get(hash) != Some(meta) {
                operations.push(BatchOperation::UpsertObject { hash: *hash, meta: *meta });
            }
        }

        for hash in before
            .objects
            .keys()
            .copied()
            .filter(|hash| *hash != empty_content_hash() && !after.objects.contains_key(hash))
        {
            operations.push(BatchOperation::DeleteObject { hash });
        }

        let mut targets = BTreeSet::new();
        targets.extend(before.constraints.keys().copied());
        targets.extend(after.constraints.keys().copied());

        for target_hash in targets {
            if target_hash == empty_content_hash() || !after.objects.contains_key(&target_hash) {
                continue;
            }

            let before_bases = before.constraints.get(&target_hash).cloned().unwrap_or_default();
            let after_bases = after.constraints.get(&target_hash).cloned().unwrap_or_default();
            if before_bases != after_bases {
                operations
                    .push(BatchOperation::SetConstraintBases { target_hash, bases: after_bases });
            }
        }

        operations
    }

    /// Plans all dependent rewrites needed before deleting `deleted_hash`.
    async fn plan_dependent_rewrites(
        &self,
        deleted_hash: Hash,
        dependents: &[Hash],
        projected: &mut IndexState,
    ) -> Result<Vec<(Hash, CandidatePlan)>, CasError> {
        let mut rewritten_plans: Vec<(Hash, CandidatePlan)> = Vec::with_capacity(dependents.len());

        for dependent in dependents {
            let bytes = self.get(*dependent).await?;

            let mut candidates =
                Self::candidate_bases_for_target(projected, *dependent, bytes.len() as u64);
            candidates.remove(&deleted_hash);
            candidates.remove(dependent);

            let chosen = self
                .select_best_candidate_plan(*dependent, &bytes, projected, candidates)
                .await?
                .ok_or_else(|| {
                    CasError::invalid_constraint(format!(
                        "cannot preserve dependent object {dependent} after deleting {deleted_hash} under remaining constraints"
                    ))
                })?;

            let _ = Self::upsert_object_meta(projected, *dependent, &chosen.object)?;
            rewritten_plans.push((*dependent, chosen));
        }

        Ok(rewritten_plans)
    }

    /// Persists rewritten dependent objects to disk.
    async fn persist_rewritten_dependents(
        &self,
        rewritten_plans: &[(Hash, CandidatePlan)],
    ) -> Result<(), CasError> {
        for (dependent, plan) in rewritten_plans {
            self.persist_object_variant(*dependent, &plan.object).await?;
        }

        Ok(())
    }

    /// Optimizes one target hash and returns whether a rewrite was applied.
    #[instrument(name = "filesystem.optimize_target_if_beneficial", skip(self), fields(target_hash = %target))]
    async fn optimize_target_if_beneficial(&self, target: Hash) -> Result<bool, CasError> {
        let (exists, current_meta, snapshot) = {
            let index =
                self.lock_index_read("snapshotting index for optimize_target_if_beneficial");
            let exists = index.objects.contains_key(&target);
            let current_meta = index.objects.get(&target).copied();
            (exists, current_meta, index.clone())
        };

        if !exists {
            return Ok(false);
        }

        let target_bytes = self.get(target).await?;
        let current_meta = current_meta.ok_or(CasError::NotFound(target))?;
        let mut all_candidates =
            Self::candidate_bases_for_target(&snapshot, target, current_meta.content_len);
        all_candidates.remove(&target);

        let Some(best) = self
            .select_best_candidate_plan(target, &target_bytes, &snapshot, all_candidates)
            .await?
        else {
            return Ok(false);
        };

        if best.score >= self.current_score(&current_meta) {
            return Ok(false);
        }

        self.persist_object_variant(target, &best.object).await?;

        let resolved_depth = {
            let mut index = self.lock_index_write("updating index after optimize target rewrite");
            Self::upsert_object_meta(&mut index, target, &best.object)?
        };

        self.persist_index_batch(vec![BatchOperation::UpsertObject {
            hash: target,
            meta: Self::meta_for_object(&best.object, resolved_depth),
        }])
        .await?;

        Ok(true)
    }
}

#[async_trait]
/// Core CAS API implementation over shared filesystem backend state.
impl CasApi for FileSystemState {
    async fn exists(&self, hash: Hash) -> Result<bool, CasError> {
        if hash == empty_content_hash() {
            return Ok(true);
        }

        let index = self.lock_index_read("checking hash existence");
        Ok(index.objects.contains_key(&hash))
    }

    async fn exists_many(&self, hashes: Vec<Hash>) -> Result<CasExistenceBitmap, CasError> {
        if hashes.is_empty() {
            return Ok(CasExistenceBitmap::new());
        }

        let index_db = self.index_db.clone();
        let flags = tokio::task::spawn_blocking(move || index_db.contains_hashes_fast(&hashes))
            .await
            .map_err(|err| {
                CasError::task_join("checking hash batch existence via redb bloom prefilter", err)
            })??;

        Ok(flags.into_iter().collect())
    }

    #[instrument(
        name = "filesystem.put",
        skip(self, data),
        fields(target_hash = tracing::field::Empty, payload_len = tracing::field::Empty)
    )]
    async fn put<D>(&self, data: D) -> Result<Hash, CasError>
    where
        D: TryInto<Bytes> + Send,
        D::Error: std::fmt::Display + Send,
    {
        let data: Bytes = data.try_into().map_err(|err| {
            CasError::invalid_input(format!("failed to convert input into bytes: {err}"))
        })?;
        let target_hash = Hash::from_content(&data);
        let span = Span::current();
        span.record("target_hash", tracing::field::display(target_hash));
        span.record("payload_len", data.len());
        if target_hash == empty_content_hash() {
            return Ok(target_hash);
        }

        self.put_new_full_object(target_hash, data).await
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
        if hash != empty_content_hash() {
            let _ = self.optimize_target_if_beneficial(hash).await?;
        }
        Ok(hash)
    }

    async fn put_stream(&self, mut reader: CasByteReader) -> Result<Hash, CasError> {
        let mut data = BytesMut::with_capacity(FILESYSTEM_STREAM_READ_CHUNK_BYTES);
        let mut chunk = self.stream_buffer_pool.lease();
        loop {
            chunk.clear();
            let read = reader.read_buf(&mut *chunk).await.map_err(|err| {
                CasError::stream_io("reading source stream during put_stream", err)
            })?;
            if read == 0 {
                break;
            }
            data.extend_from_slice(chunk.as_ref());
        }

        let target_hash = Hash::from_content_with_algorithm(HashAlgorithm::Blake3, data.as_ref());
        if target_hash == empty_content_hash() {
            return Ok(target_hash);
        }

        self.put_new_full_object(target_hash, data.freeze()).await
    }

    async fn put_stream_with_constraints(
        &self,
        reader: CasByteReader,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError> {
        let hash = self.put_stream(reader).await?;
        self.set_constraint(Constraint { target_hash: hash, potential_bases: bases }).await?;
        if hash != empty_content_hash() {
            let _ = self.optimize_target_if_beneficial(hash).await?;
        }
        Ok(hash)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        if hash == empty_content_hash() {
            return Ok(Bytes::new());
        }

        let mut current = hash;
        let mut visited = HashSet::new();
        let mut patch_payloads: Vec<Cow<'static, [u8]>> = Vec::new();
        let mut expected_len: Option<u64> = None;

        loop {
            if !visited.insert(current) {
                return Err(CasError::CycleDetected {
                    target: hash,
                    detail: format!("loop encountered at {current}"),
                });
            }

            if let Some(mapped_full) = self.read_full_object_mmap(current).await? {
                if expected_len.is_none() {
                    expected_len = Some(mapped_full.len() as u64);
                }

                if patch_payloads.is_empty() {
                    ensure_reconstructed_size(hash, expected_len, mapped_full.len())?;
                    ensure_reconstructed_hash(hash, mapped_full.as_ref(), "mmap full-object read")?;
                    return Ok(mapped_full);
                }

                let data = apply_delta_patch_stack(mapped_full.to_vec(), &mut patch_payloads)?;
                ensure_reconstructed_size(hash, expected_len, data.len())?;
                ensure_reconstructed_hash(hash, data.as_slice(), "mmap+delta reconstruction")?;

                return Ok(Bytes::from(data));
            }

            let object = self.read_stored_object(current).await?;
            if expected_len.is_none() {
                expected_len = Some(object.content_len());
            }
            match object {
                StoredObject::Full { payload } => {
                    let data = apply_delta_patch_stack(payload, &mut patch_payloads)?;
                    ensure_reconstructed_size(hash, expected_len, data.len())?;
                    ensure_reconstructed_hash(hash, data.as_slice(), "full/delta reconstruction")?;
                    return Ok(Bytes::from(data));
                }
                StoredObject::Delta { state } => {
                    patch_payloads.push(state.payload);
                    current = state.base_hash;
                }
            }
        }
    }

    async fn get_stream(&self, hash: Hash) -> Result<CasByteStream, CasError> {
        // Integrity-first stream path: emit one verified chunk.
        let bytes = self.get(hash).await?;
        Ok(Box::pin(stream::once(async move { Ok(bytes) })))
    }

    async fn info(&self, hash: Hash) -> Result<ObjectInfo, CasError> {
        if hash == empty_content_hash() {
            return Ok(ObjectInfo {
                content_len: 0,
                payload_len: 0,
                is_delta: false,
                base_hash: None,
            });
        }

        let index = self.lock_index_read("reading object info");
        let meta = index.objects.get(&hash).ok_or(CasError::NotFound(hash))?;
        let (is_delta, base_hash) = match meta.encoding() {
            ObjectEncoding::Full => (false, None),
            ObjectEncoding::Delta { base_hash } => (true, Some(base_hash)),
        };
        Ok(ObjectInfo {
            content_len: meta.content_len,
            payload_len: meta.payload_len,
            is_delta,
            base_hash,
        })
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        if hash == empty_content_hash() {
            return Err(CasError::invalid_constraint(
                "cannot delete implicit empty-content root".to_string(),
            ));
        }

        let snapshot = {
            let index = self.lock_index_read("capturing delete snapshot");
            if !index.objects.contains_key(&hash) {
                return Err(CasError::NotFound(hash));
            }
            index.clone()
        };

        let dependents = Self::direct_dependents(&snapshot, hash);
        let mut projected = snapshot.clone();
        Self::remove_constraint_references(&mut projected, hash);

        let rewritten_plans =
            self.plan_dependent_rewrites(hash, &dependents, &mut projected).await?;

        let rewritten_roots: BTreeSet<Hash> =
            rewritten_plans.iter().map(|(dependent, _)| *dependent).collect();

        Self::remove_object_meta(&mut projected, hash)?;
        Self::recompute_descendant_depths_with_fallback(&mut projected, &rewritten_roots)?;
        let index_operations = Self::index_diff_operations(&snapshot, &projected);

        self.persist_rewritten_dependents(&rewritten_plans).await?;

        self.delete_object_files(hash).await?;

        {
            let mut index = self.lock_index_write("publishing projected index after delete");
            *index = projected;
        }

        self.persist_index_batch(index_operations).await
    }

    async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError> {
        let set: BTreeSet<Hash> = constraint.potential_bases;
        validate_constraint_target_not_in_bases(constraint.target_hash, &set)?;

        let persisted_bases = {
            let mut index = self.lock_index_write("setting explicit constraint row");
            if !index.objects.contains_key(&constraint.target_hash) {
                return Err(CasError::NotFound(constraint.target_hash));
            }

            for base in &set {
                if *base != empty_content_hash() && !index.objects.contains_key(base) {
                    return Err(CasError::NotFound(*base));
                }
            }

            Self::set_constraint_row_optic(&mut index, constraint.target_hash, set)
                .unwrap_or_default()
        };

        self.persist_index_batch(vec![BatchOperation::SetConstraintBases {
            target_hash: constraint.target_hash,
            bases: persisted_bases,
        }])
        .await
    }

    async fn patch_constraint(
        &self,
        target_hash: Hash,
        patch: ConstraintPatch,
    ) -> Result<Option<Constraint>, CasError> {
        {
            let index = self.lock_index_read("validating constraint patch target and bases");
            if !index.objects.contains_key(&target_hash) {
                return Err(CasError::NotFound(target_hash));
            }
            for base in &patch.add_bases {
                if *base != empty_content_hash() && !index.objects.contains_key(base) {
                    return Err(CasError::NotFound(*base));
                }
            }
        }

        let merged = {
            let mut index = self.lock_index_write("applying constraint patch");

            let merged = Self::merge_constraint_patch(index.constraints.get(&target_hash), patch);

            validate_constraint_target_not_in_bases(target_hash, &merged)?;

            Self::set_constraint_row_optic(&mut index, target_hash, merged)
        };

        self.persist_index_batch(vec![BatchOperation::SetConstraintBases {
            target_hash,
            bases: merged.clone().unwrap_or_default(),
        }])
        .await?;

        Ok(merged.map(|potential_bases| Constraint { target_hash, potential_bases }))
    }

    async fn get_constraint(&self, hash: Hash) -> Result<Option<Constraint>, CasError> {
        let index = self.lock_index_read("reading constraint row");
        if !index.objects.contains_key(&hash) {
            return Err(CasError::NotFound(hash));
        }

        Ok(index.constraints.get(&hash).and_then(|potential_bases| {
            normalize_explicit_constraint_set(potential_bases.clone())
                .map(|explicit| Constraint { target_hash: hash, potential_bases: explicit })
        }))
    }
}

/// Default actor-shutdown grace period for production constructors.
const FILESYSTEM_DEFAULT_DROP_GRACE_PERIOD: Duration = Duration::from_secs(2);
/// Short actor-shutdown grace period for test constructors.
const FILESYSTEM_TEST_DROP_GRACE_PERIOD: Duration = Duration::from_millis(25);
/// On-disk Phase 1 CAS with mutation-focused actor coordination.
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

#[derive(Debug, Clone)]
/// Candidate rewrite plan evaluated by optimizer scoring logic.
struct CandidatePlan {
    /// Candidate object representation.
    object: StoredObject,
    /// Candidate base hash.
    base_hash: Hash,
    /// Candidate reconstruction depth.
    depth: u32,
    /// Candidate optimization score.
    score: u128,
}

/// Ensures the canonical empty object payload exists on disk.
async fn bootstrap_empty_object(root: &Path) -> Result<(), CasError> {
    let empty = empty_content_hash();
    let path = object_path(root, empty);
    if fs::try_exists(&path)
        .await
        .map_err(|source| CasError::io("checking empty object", path.clone(), source))?
    {
        return Ok(());
    }

    let staging_root = root.join(STORAGE_VERSION).join("tmp");
    write_object_atomic(&staging_root, &path, &[]).await
}

/// Atomically writes one object file.
async fn write_object_atomic(
    staging_root: &Path,
    path: &Path,
    bytes: &[u8],
) -> Result<(), CasError> {
    write_atomic(staging_root.to_path_buf(), path.to_path_buf(), bytes.to_vec()).await
}

/// Atomically writes arbitrary bytes to target path via temp-file rename.
async fn write_atomic(
    staging_root: PathBuf,
    path: PathBuf,
    bytes: Vec<u8>,
) -> Result<(), CasError> {
    tokio::task::spawn_blocking(move || {
        let Some(parent) = path.parent() else {
            return Err(CasError::invalid_input(format!(
                "cannot atomically write path without parent: {}",
                path.display()
            )));
        };

        std::fs::create_dir_all(parent)
            .map_err(|source| CasError::io("creating parent directories", parent, source))?;

        std::fs::create_dir_all(&staging_root).map_err(|source| {
            CasError::io("creating shared staging tmp directory", staging_root.clone(), source)
        })?;

        let mut temp = tempfile::Builder::new()
            .prefix("cas-")
            .suffix(".stage")
            .tempfile_in(&staging_root)
            .map_err(|source| {
                CasError::io("creating named temp file", staging_root.clone(), source)
            })?;

        temp.write_all(&bytes)
            .map_err(|source| CasError::io("writing staged bytes", temp.path(), source))?;
        temp.as_file()
            .sync_all()
            .map_err(|source| CasError::io("syncing staged file", temp.path(), source))?;

        let (staged_file, staged_path) = temp.keep().map_err(|source| {
            let staging_path = source.file.path().to_path_buf();
            CasError::io("materializing staged file path", staging_path, source.error)
        })?;
        drop(staged_file);

        match std::fs::rename(&staged_path, &path) {
            Ok(()) => {
                enforce_file_readonly(&path)?;
                Ok(())
            }
            Err(_first_rename_error) if path.exists() => {
                clear_file_readonly_if_set(&path)?;
                std::fs::remove_file(&path).map_err(|source| {
                    CasError::io(
                        "removing existing target before rename fallback",
                        path.clone(),
                        source,
                    )
                })?;

                std::fs::rename(&staged_path, &path).map_err(|source| {
                    CasError::io("renaming staged file into place", path.clone(), source)
                })?;
                enforce_file_readonly(&path)?;
                Ok(())
            }
            Err(first_rename_error) => {
                let _ = std::fs::remove_file(&staged_path);
                error!(
                    path = %path.display(),
                    source = %first_rename_error,
                    "staged rename failed without existing target"
                );
                Err(CasError::io("renaming staged file into place", path, first_rename_error))
            }
        }
    })
    .await
    .map_err(|err| CasError::task_join("atomically writing filesystem object bytes", err))?
}

/// Marks one object file as read-only after CAS-owned writes complete.
fn enforce_file_readonly(path: &Path) -> Result<(), CasError> {
    let metadata = std::fs::metadata(path).map_err(|source| {
        CasError::io("reading object metadata for readonly enforcement", path, source)
    })?;
    let mut permissions = metadata.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        std::fs::set_permissions(path, permissions).map_err(|source| {
            CasError::io("marking object file readonly after atomic commit", path, source)
        })?;
    }

    Ok(())
}

/// Clears read-only bit on one object file so CAS can replace/remove it.
fn clear_file_readonly_if_set(path: &Path) -> Result<(), CasError> {
    let metadata = std::fs::metadata(path).map_err(|source| {
        CasError::io("reading existing object metadata before overwrite", path, source)
    })?;

    let permissions = metadata.permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                let mut writable_permissions = permissions;
                writable_permissions.set_mode(writable_mode);
                std::fs::set_permissions(path, writable_permissions).map_err(|source| {
                    CasError::io("clearing readonly bit before object overwrite", path, source)
                })?;
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed overwrite/delete operations can succeed"
            )]
            {
                let mut writable_permissions = permissions;
                writable_permissions.set_readonly(false);
                std::fs::set_permissions(path, writable_permissions).map_err(|source| {
                    CasError::io("clearing readonly bit before object overwrite", path, source)
                })?;
            }
        }
    }

    Ok(())
}

/// Applies hash-collision length guard for existing hash keys.
const fn ensure_no_length_collision(
    hash: Hash,
    existing_len: u64,
    candidate_len: u64,
) -> Result<(), CasError> {
    // STICKY SAFETY NOTE (DO NOT REMOVE):
    // We always compare lengths when a hash key already exists. If lengths differ,
    // we fail fast to surface probable hash-collision/consistency issues while
    // preserving storage atomicity guarantees. If two different contents of the
    // same length ever collide, we intentionally do not handle that case here and
    // assume it does not happen in practice.
    if existing_len != candidate_len {
        return Err(CasError::HashCollisionLengthMismatch { hash, existing_len, candidate_len });
    }
    Ok(())
}

/// Applies stacked delta payloads to a full-object base payload.
///
/// `patch_payloads` is consumed from the end so callers can push base-to-leaf
/// deltas during traversal and replay them in reconstruction order.
fn apply_delta_patch_stack(
    mut base_payload: Vec<u8>,
    patch_payloads: &mut Vec<Cow<'static, [u8]>>,
) -> Result<Vec<u8>, CasError> {
    while let Some(patch_payload) = patch_payloads.pop() {
        let patch = DeltaPatch::decode(patch_payload.as_ref())?;
        base_payload = patch.apply(&base_payload)?;
    }

    Ok(base_payload)
}

/// Validates reconstructed payload length against optional expected size.
fn ensure_reconstructed_size(
    hash: Hash,
    expected_len: Option<u64>,
    actual_len: usize,
) -> Result<(), CasError> {
    if let Some(expected) = expected_len
        && actual_len as u64 != expected
    {
        return Err(CasError::corrupt_object(format!(
            "reconstructed size mismatch for {hash}: expected {expected}, got {actual_len}",
        )));
    }

    Ok(())
}

/// Validates reconstructed payload hash against expected object hash.
fn ensure_reconstructed_hash(
    expected_hash: Hash,
    content: &[u8],
    operation: &str,
) -> Result<(), CasError> {
    let actual = Hash::from_content(content);
    if actual != expected_hash {
        return Err(CasError::corrupt_object(format!(
            "hash mismatch while {operation}: expected {expected_hash}, got {actual}"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use bytes::Bytes;
    use futures_util::StreamExt;
    use tempfile::{TempDir, tempdir};
    use tokio::io::AsyncReadExt;

    use super::FileSystemCas;
    use crate::{
        CasApi, CasError, CasMaintenanceApi, Constraint, ConstraintPatch,
        FileSystemRecoveryOptions, Hash, HashAlgorithm, IndexRecoveryMode, ObjectMeta,
        OptimizeOptions, empty_content_hash,
    };

    async fn open_temp_filesystem_cas() -> (TempDir, FileSystemCas) {
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("cas should open");
        (dir, cas)
    }

    fn count_backup_snapshots(root: &std::path::Path) -> usize {
        let backup_root = root.join("index-backups");
        let Ok(entries) = std::fs::read_dir(backup_root) else {
            return 0;
        };

        entries
            .flatten()
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.starts_with("index-backup-") && name.ends_with(".postcard"))
            })
            .filter(|is_backup| *is_backup)
            .count()
    }

    #[tokio::test]
    async fn constructors_expose_explicit_drop_grace_policies() {
        let dir = tempdir().expect("tempdir");

        let production = FileSystemCas::open(dir.path()).await.expect("open production cas");
        assert_eq!(production.drop_grace_period, Duration::from_secs(2));
        drop(production);

        let test = FileSystemCas::open_for_tests(dir.path()).await.expect("open test cas");
        assert_eq!(test.drop_grace_period, Duration::from_millis(25));
    }

    #[tokio::test]
    async fn open_bootstraps_index_and_empty_object() {
        let (dir, cas) = open_temp_filesystem_cas().await;

        assert!(dir.path().join("index.redb").is_file());
        let empty_path = cas.object_path_for_hash(empty_content_hash());
        assert!(empty_path.is_file());

        let empty_payload = cas.get(empty_content_hash()).await.expect("get empty payload");
        assert!(empty_payload.is_empty(), "empty content hash must resolve to empty bytes");
    }

    #[tokio::test]
    async fn filesystem_backup_snapshot_interval_batches_mutation_backups() {
        let dir = tempdir().expect("tempdir");
        let recovery = FileSystemRecoveryOptions {
            mode: IndexRecoveryMode::Recover,
            max_backup_snapshots: 16,
            backup_snapshot_interval_ops: 3,
        };
        let cas = FileSystemCas::open_with_alpha_and_recovery_for_tests(dir.path(), 4, recovery)
            .await
            .expect("open fs cas with interval recovery settings");

        let initial = count_backup_snapshots(dir.path());
        assert_eq!(initial, 1, "open should persist one initial backup snapshot");

        cas.put(Bytes::from_static(b"interval-a")).await.expect("put #1");
        assert_eq!(count_backup_snapshots(dir.path()), initial);

        cas.put(Bytes::from_static(b"interval-b")).await.expect("put #2");
        assert_eq!(count_backup_snapshots(dir.path()), initial);

        cas.put(Bytes::from_static(b"interval-c")).await.expect("put #3");
        assert_eq!(count_backup_snapshots(dir.path()), initial + 1);
    }

    #[test]
    fn localized_depth_recompute_updates_transitive_descendants() {
        let base = Hash::from_content(b"depth-base");
        let pivot = Hash::from_content(b"depth-pivot");
        let child = Hash::from_content(b"depth-child");
        let grandchild = Hash::from_content(b"depth-grandchild");

        let mut index = crate::IndexState::default();
        crate::ensure_empty_record(&mut index);
        index.objects.insert(base, ObjectMeta::full(4, 4, 1));
        index.objects.insert(pivot, ObjectMeta::full(4, 4, 1));
        index.objects.insert(child, ObjectMeta::delta(2, 4, 2, pivot));
        index.objects.insert(grandchild, ObjectMeta::delta(2, 4, 3, child));
        index.rebuild_delta_reverse();

        let next_pivot = ObjectMeta::delta(2, 4, 2, base);
        let previous_pivot = index.objects.insert(pivot, next_pivot);
        super::FileSystemState::sync_delta_reverse_for_meta_update(
            &mut index,
            pivot,
            previous_pivot,
            next_pivot,
        );

        let roots = BTreeSet::from([pivot]);
        super::FileSystemState::recompute_descendant_depths_with_fallback(&mut index, &roots)
            .expect("localized depth recompute should succeed");

        assert_eq!(index.objects.get(&pivot).expect("pivot meta").depth(), 2);
        assert_eq!(index.objects.get(&child).expect("child meta").depth(), 3);
        assert_eq!(index.objects.get(&grandchild).expect("grandchild meta").depth(), 4);
    }

    #[tokio::test]
    async fn filesystem_put_get_delete_lifecycle() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let payload = Bytes::from_static(b"fs-delete");
        let hash = cas.put(payload.clone()).await.expect("put payload");
        assert_eq!(cas.get(hash).await.expect("get payload"), payload);

        cas.delete(hash).await.expect("delete payload");
        let missing = cas.get(hash).await;
        assert!(missing.is_err(), "deleted payload should not be retrievable");
    }

    #[tokio::test]
    async fn filesystem_uses_single_shared_staging_tmp_directory() {
        let (dir, cas) = open_temp_filesystem_cas().await;

        cas.put(Bytes::from_static(b"staging tmp regression payload")).await.expect("put payload");

        let storage_root = dir.path().join(super::STORAGE_VERSION);
        let expected_tmp = storage_root.join("tmp");
        assert!(expected_tmp.exists(), "shared staging tmp should exist under storage root");

        let mut stack = vec![storage_root.clone()];
        let mut tmp_dirs = Vec::new();
        while let Some(path) = stack.pop() {
            for entry in std::fs::read_dir(&path).expect("read_dir") {
                let entry = entry.expect("dir entry");
                let child = entry.path();
                if child.is_dir() {
                    if child.file_name().is_some_and(|name| name == "tmp") {
                        tmp_dirs.push(child.clone());
                    }
                    stack.push(child);
                }
            }
        }

        assert_eq!(tmp_dirs, vec![expected_tmp]);
    }

    #[tokio::test]
    async fn filesystem_set_constraint_rejects_missing_base() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let target =
            cas.put(Bytes::from_static(b"target")).await.expect("target store should succeed");
        let missing = Hash::from_content(b"missing");
        let result = cas
            .set_constraint(Constraint {
                target_hash: target,
                potential_bases: BTreeSet::from([missing]),
            })
            .await;

        assert!(result.is_err(), "missing-base constraints must be rejected");
        assert_ne!(empty_content_hash(), target);
    }

    #[tokio::test]
    async fn filesystem_delete_preserves_delta_descendants_via_rewrite() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let base_payload = vec![b'a'; 8 * 1024];
        let mut target_payload = base_payload.clone();
        target_payload[2_048] = b'z';

        let base_hash = cas.put(Bytes::from(base_payload)).await.expect("put base");
        let target_hash = Hash::from_content(&target_payload);
        let stored_target = cas.put(Bytes::from(target_payload.clone())).await.expect("put target");
        assert_eq!(stored_target, target_hash);

        cas.set_constraint(Constraint {
            target_hash,
            potential_bases: BTreeSet::from([base_hash]),
        })
        .await
        .expect("set target constraint");

        cas.delete(base_hash).await.expect("delete base while preserving descendants");

        assert!(cas.get(base_hash).await.is_err());
        assert_eq!(
            cas.get(target_hash).await.expect("target must remain reconstructible"),
            Bytes::from(target_payload)
        );

        let target_constraint =
            cas.get_constraint(target_hash).await.expect("get target constraint after base delete");
        if let Some(constraint) = target_constraint {
            assert!(
                !constraint.potential_bases.contains(&base_hash),
                "rewritten descendant constraint must not retain deleted base"
            );
        }
    }

    #[tokio::test]
    async fn filesystem_exists_many_returns_ordered_bitset() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let a = cas.put(Bytes::from_static(b"a")).await.expect("put a");
        let b = cas.put(Bytes::from_static(b"b")).await.expect("put b");
        let missing = Hash::from_content(b"missing");

        let flags = cas.exists_many(vec![a, missing, b]).await.expect("exists_many");

        assert_eq!(flags.iter().by_vals().collect::<Vec<_>>(), vec![true, false, true]);
    }

    #[tokio::test]
    async fn filesystem_patch_constraint_add_remove_and_clear() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let target = cas.put(Bytes::from_static(b"target")).await.expect("put target");
        let base_a = cas.put(Bytes::from_static(b"base-a")).await.expect("put base_a");
        let base_b = cas.put(Bytes::from_static(b"base-b")).await.expect("put base_b");

        cas.set_constraint(Constraint {
            target_hash: target,
            potential_bases: BTreeSet::from([base_a]),
        })
        .await
        .expect("set initial constraint");

        let patched = cas
            .patch_constraint(
                target,
                ConstraintPatch {
                    add_bases: BTreeSet::from([base_b]),
                    remove_bases: BTreeSet::from([base_a]),
                    clear_existing: false,
                },
            )
            .await
            .expect("patch constraint");

        assert_eq!(
            patched,
            Some(Constraint { target_hash: target, potential_bases: BTreeSet::from([base_b]) })
        );

        let cleared = cas
            .patch_constraint(
                target,
                ConstraintPatch {
                    add_bases: BTreeSet::new(),
                    remove_bases: BTreeSet::new(),
                    clear_existing: true,
                },
            )
            .await
            .expect("clear explicit constraint");

        assert!(cleared.is_none());
        assert!(cas.get_constraint(target).await.expect("get constraint").is_none());
    }

    #[tokio::test]
    async fn filesystem_delete_respects_patched_constraint_reverse_links() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let base_a = cas.put(Bytes::from_static(b"base-a")).await.expect("put base_a");
        let base_b = cas.put(Bytes::from_static(b"base-b")).await.expect("put base_b");
        let target_a = cas.put(Bytes::from_static(b"target-a")).await.expect("put target_a");
        let target_b = cas.put(Bytes::from_static(b"target-b")).await.expect("put target_b");

        cas.set_constraint(Constraint {
            target_hash: target_a,
            potential_bases: BTreeSet::from([base_a]),
        })
        .await
        .expect("set target_a constraint");
        cas.set_constraint(Constraint {
            target_hash: target_b,
            potential_bases: BTreeSet::from([base_a]),
        })
        .await
        .expect("set target_b constraint");

        cas.patch_constraint(
            target_a,
            ConstraintPatch {
                add_bases: BTreeSet::from([base_b]),
                remove_bases: BTreeSet::from([base_a]),
                clear_existing: false,
            },
        )
        .await
        .expect("patch target_a constraint");

        cas.delete(base_a).await.expect("delete base_a");

        assert_eq!(
            cas.get_constraint(target_a).await.expect("target_a constraint"),
            Some(Constraint { target_hash: target_a, potential_bases: BTreeSet::from([base_b]) })
        );
        assert!(
            cas.get_constraint(target_b).await.expect("target_b constraint").is_none(),
            "target_b should become unconstrained after deleting its only base"
        );
    }

    #[tokio::test]
    async fn filesystem_put_stream_with_constraints_sets_constraint() {
        let (_dir, cas) = open_temp_filesystem_cas().await;
        let base = cas.put(Bytes::from_static(b"stream-base")).await.expect("put base");

        let reader = Box::new(tokio::io::repeat(b'y').take(5));
        let hash = cas
            .put_stream_with_constraints(reader, BTreeSet::from([base]))
            .await
            .expect("put_stream_with_constraints");

        let bytes = cas.get(hash).await.expect("get streamed object");
        assert_eq!(bytes, Bytes::from_static(b"yyyyy"));
        assert_eq!(
            cas.get_constraint(hash).await.expect("get constraint row"),
            Some(Constraint { target_hash: hash, potential_bases: BTreeSet::from([base]) })
        );
    }

    #[tokio::test]
    async fn filesystem_put_stream_hash_matches_multihash_identity() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let len = (super::FILESYSTEM_STREAM_READ_CHUNK_BYTES * 2) + 257;
        let payload = vec![b'z'; len];
        let expected = Hash::from_content_with_algorithm(HashAlgorithm::Blake3, &payload);

        let reader = Box::new(tokio::io::repeat(b'z').take(len as u64));
        let hash = cas.put_stream(reader).await.expect("put_stream");

        assert_eq!(hash, expected);
        assert_eq!(cas.get(hash).await.expect("get streamed payload").len(), len);
    }

    #[tokio::test]
    async fn filesystem_info_many_returns_ordered_metadata() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let a = cas.put(Bytes::from_static(b"aa")).await.expect("put a");
        let b = cas.put(Bytes::from_static(b"bbb")).await.expect("put b");

        let infos = cas.info_many(vec![b, a]).await.expect("info_many");

        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].0, b);
        assert_eq!(infos[0].1.content_len, 3);
        assert_eq!(infos[1].0, a);
        assert_eq!(infos[1].1.content_len, 2);
    }

    #[tokio::test]
    async fn filesystem_get_constraint_many_returns_ordered_rows() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let base = cas.put(Bytes::from_static(b"base")).await.expect("put base");
        let constrained =
            cas.put(Bytes::from_static(b"constrained")).await.expect("put constrained");
        let unrestricted = cas.put(Bytes::from_static(b"free")).await.expect("put unrestricted");

        cas.set_constraint(Constraint {
            target_hash: constrained,
            potential_bases: BTreeSet::from([base]),
        })
        .await
        .expect("set constraint");

        let rows = cas
            .get_constraint_many(vec![unrestricted, constrained])
            .await
            .expect("get_constraint_many");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (unrestricted, None));
        assert_eq!(
            rows[1],
            (
                constrained,
                Some(Constraint {
                    target_hash: constrained,
                    potential_bases: BTreeSet::from([base])
                }),
            )
        );
    }

    #[tokio::test]
    async fn filesystem_delete_many_removes_all_hashes() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let a = cas.put(Bytes::from_static(b"a")).await.expect("put a");
        let b = cas.put(Bytes::from_static(b"b")).await.expect("put b");

        cas.delete_many(vec![a, b]).await.expect("delete_many");

        assert!(cas.get(a).await.is_err());
        assert!(cas.get(b).await.is_err());
    }

    #[tokio::test]
    async fn filesystem_put_defaults_to_full_hot_path() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let hash = cas
            .put(Bytes::from_static(b"hot-path-full-object"))
            .await
            .expect("put object on hot path");

        assert!(cas.object_path_for_hash(hash).is_file());
        assert!(!cas.diff_path_for_hash(hash).exists());
    }

    /// Protects readonly enforcement for persisted full-object payload files.
    #[tokio::test]
    async fn filesystem_put_marks_object_file_readonly() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let hash = cas.put(Bytes::from_static(b"readonly-object")).await.expect("put object");
        let object_path = cas.object_path_for_hash(hash);
        let metadata = std::fs::metadata(&object_path).expect("object metadata");

        assert!(metadata.permissions().readonly());
    }

    #[tokio::test]
    async fn filesystem_optimize_once_rewrites_unconstrained_objects() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let base = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
            .await
            .expect("put base");
        let target = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
            .await
            .expect("put target");

        let report = cas
            .optimize_once(OptimizeOptions::default())
            .await
            .expect("optimize unconstrained objects");

        assert!(report.rewritten_objects >= 1, "expected at least one rewrite from optimizer");
        assert_eq!(cas.get(base).await.expect("reconstruct base").len(), 48);
        assert_eq!(cas.get(target).await.expect("reconstruct target").len(), 48);
    }

    #[tokio::test]
    async fn filesystem_get_stream_round_trips_large_full_payload() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let mmap_min_bytes = usize::try_from(super::FILESYSTEM_MMAP_MIN_BYTES).unwrap_or(64 * 1024);
        let payload = vec![b'm'; mmap_min_bytes + 4096];
        let hash = cas.put(Bytes::from(payload.clone())).await.expect("put large payload");

        let mut stream = cas.get_stream(hash).await.expect("get_stream large full payload");
        let mut restored = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("stream chunk");
            restored.extend_from_slice(&chunk);
        }

        assert_eq!(restored, payload);
    }

    #[tokio::test]
    async fn filesystem_get_detects_payload_corruption() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let original = Bytes::from_static(b"integrity-check-target");
        let hash = cas.put(original).await.expect("put payload");

        let object_path = cas.object_path_for_hash(hash);
        super::clear_file_readonly_if_set(&object_path)
            .expect("clear readonly flag before intentional corruption");
        tokio::fs::write(&object_path, b"mutated-bytes").await.expect("mutate object bytes");

        let err = cas.get(hash).await.expect_err("corrupt payload must fail verification");
        assert!(matches!(err, CasError::CorruptObject(_)));
    }

    #[tokio::test]
    async fn filesystem_metrics_expose_cache_hits_and_optimizer_runtime() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let base =
            cas.put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaa")).await.expect("put base");
        let target =
            cas.put(Bytes::from_static(b"aaaaaaaaaaaaaaaabbbbbbbb")).await.expect("put target");

        cas.set_constraint(Constraint {
            target_hash: target,
            potential_bases: BTreeSet::from([base]),
        })
        .await
        .expect("set optimization constraint");

        cas.optimize_once(OptimizeOptions::default()).await.expect("optimize #1");
        cas.optimize_once(OptimizeOptions::default()).await.expect("optimize #2");

        let metrics = cas.metrics().await.expect("read metrics snapshot");
        assert!(metrics.cache_hits > 0);
        assert!(metrics.optimizer_runtime_ms > 0);
        assert!(metrics.delta_compression_ratio.is_finite());
    }

    #[tokio::test]
    async fn filesystem_topology_snapshot_captures_nodes_delta_edges_and_constraints() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let base = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
            .await
            .expect("put base");
        let target = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
            .await
            .expect("put target");

        cas.set_constraint(Constraint {
            target_hash: target,
            potential_bases: BTreeSet::from([base]),
        })
        .await
        .expect("set explicit constraint");
        cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");

        let snapshot = cas.topology_snapshot(false).await.expect("topology snapshot");
        assert!(snapshot.nodes.iter().all(|node| node.hash != empty_content_hash()));

        let target_node =
            snapshot.nodes.iter().find(|node| node.hash == target).expect("target node must exist");
        match target_node.encoding {
            crate::storage::CasTopologyEncoding::Delta { base_hash } => assert_eq!(base_hash, base),
            crate::storage::CasTopologyEncoding::Full => {
                panic!("expected optimized target to be encoded as delta")
            }
        }

        let row = snapshot
            .constraints
            .iter()
            .find(|row| row.target_hash == target)
            .expect("constraint row for target");
        assert_eq!(row.bases, vec![base]);
    }

    #[tokio::test]
    async fn filesystem_visualize_mermaid_emits_graph_nodes_and_edges() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let base = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
            .await
            .expect("put base");
        let target = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
            .await
            .expect("put target");

        cas.set_constraint(Constraint {
            target_hash: target,
            potential_bases: BTreeSet::from([base]),
        })
        .await
        .expect("set explicit constraint");
        cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");

        let mermaid = cas.visualize_mermaid(false).await.expect("render mermaid");
        assert!(mermaid.contains("flowchart TD"));
        assert!(mermaid.contains(&format!("n{}", target.to_hex())));
        assert!(mermaid.contains(&format!("n{}", base.to_hex())));
        assert!(mermaid.contains("-->|base|"));
        assert!(mermaid.contains("-.->|allowed|"));
    }

    #[tokio::test]
    async fn filesystem_visualize_mermaid_neighborhood_limits_distance() {
        let (_dir, cas) = open_temp_filesystem_cas().await;

        let base = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
            .await
            .expect("put base");
        let mid = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC"))
            .await
            .expect("put mid");
        let tip = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaDDDDDDDDDDDDDDDD"))
            .await
            .expect("put tip");

        cas.set_constraint(Constraint {
            target_hash: mid,
            potential_bases: BTreeSet::from([base]),
        })
        .await
        .expect("set constraint mid");
        cas.set_constraint(Constraint { target_hash: tip, potential_bases: BTreeSet::from([mid]) })
            .await
            .expect("set constraint tip");

        cas.optimize_once(OptimizeOptions::default())
            .await
            .expect("optimize neighborhood topology");

        let limited =
            cas.visualize_mermaid_neighborhood(base, 1, false).await.expect("render neighborhood");

        assert!(limited.contains(&format!("n{}", base.to_hex())));
        assert!(limited.contains(&format!("n{}", mid.to_hex())));
        assert!(
            !limited.contains(&format!("n{}", tip.to_hex())),
            "depth-1 neighborhood should exclude distance-2 nodes"
        );
    }
}
