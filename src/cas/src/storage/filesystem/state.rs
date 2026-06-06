//! Shared filesystem CAS runtime state.
//!
//! This module owns [`FileSystemState`], the core in-memory state shared
//! between all API paths, plus scoring types and pure utility functions used
//! exclusively by the state implementation.
//!
//! # Module structure note
//!
//! This file intentionally remains as a single module despite exceeding 1 700
//! lines. The entire public surface is the `impl FileSystemState` block (plus
//! the `impl CasApi for FileSystemState` trait implementation), every method
//! of which takes `&self` or `&mut self`. Rust does not allow `impl` blocks
//! to span multiple files without the non-idiomatic `include!()` macro, and
//! the handful of standalone helper functions at the bottom (< 80 lines) are
//! too small to justify a separate sibling file. Keep this file whole.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use ractor::{Actor, ActorRef, call_t};
use rand::Rng;
use smallvec::SmallVec;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tracing::{Span, error, info, instrument, warn};

use crate::index::{DELTA_PROMOTION_DEPTH, MAX_DELTA_DEPTH, resolve_object_depth};
use crate::storage::{
    CasIntegrityConfig, CasTopologyConstraint, CasTopologyEncoding, CasTopologyNode,
    CasTopologySnapshot, FileSystemRecoveryOptions, StreamBufferPool, VerifyTriggerStrategy,
    chain::check_no_cycle, is_unconstrained_constraint_row, normalize_explicit_constraint_set,
    validate_constraint_target_not_in_bases,
};
use crate::{
    BatchOperation, CasApi, CasByteReader, CasByteStream, CasError, CasExistenceBitmap, CasIndexDb,
    Constraint, ConstraintBatchOp, ConstraintPatch, DeltaPatch, Hash, HashAlgorithm,
    IndexRepairReport, IndexState, ObjectEncoding, ObjectInfo, ObjectMeta, StoredObject,
    empty_content_hash, ensure_empty_record, recalculate_depths,
};

use super::actor::{
    ActiveMmapRegistry, FileObjectActor, FileObjectActorMessage, FileObjectActorState,
    read_full_object_bytes_mmap,
};
use super::metrics::FileSystemMetrics;
use super::metrics::{FileSystemMetricsState, ObjectActorRpcScope};
use super::paths::{diff_object_path, object_path};
use super::recovery;
use super::util::bootstrap_empty_object;
use super::{
    FILESYSTEM_CANDIDATE_EVAL_CONCURRENCY, FILESYSTEM_OBJECT_ACTOR_RPC_TIMEOUT_MS,
    FILESYSTEM_SMALL_INLINE_HASHES, FILESYSTEM_STREAM_BUFFER_POOL_MAX_BUFFERS,
    FILESYSTEM_STREAM_READ_CHUNK_BYTES, FILESYSTEM_UNRESTRICTED_CANDIDATE_LIMIT, STORAGE_VERSION,
};

/// Shared filesystem CAS backend state.
pub(super) struct FileSystemState {
    /// CAS filesystem root used for direct stateless read-path operations.
    root: PathBuf,
    /// Depth penalty coefficient used by optimizer scoring.
    alpha: u64,
    /// Runtime toggle for compression-first optimizer mode (`alpha = 0`).
    max_compression_mode: AtomicBool,
    /// In-memory index state guarded by a reader/writer lock.
    index: RwLock<IndexState>,
    /// Process-local TTL cache of reconstructed object bytes. Entries
    /// carry a timestamp for TTL expiry; stale entries are re-fetched
    /// and re-verified from storage.
    reconstructed_bytes_cache: Mutex<HashMap<Hash, (Instant, Bytes)>>,
    /// Reusable pooled stream buffers for incremental ingestion.
    stream_buffer_pool: Arc<StreamBufferPool>,
    /// Redb persistence handle used for incremental index flushes.
    index_db: CasIndexDb,
    /// Startup recovery and backup retention settings.
    recovery: FileSystemRecoveryOptions,
    /// Integrity verification policy for read-path hash checks.
    integrity: CasIntegrityConfig,

    /// In-process observability counters for filesystem CAS operations.
    metrics: FileSystemMetricsState,
    /// Tracks whether an optimize run is currently active.
    pub(super) optimize_in_progress: AtomicBool,
    /// Shared mmap lease registry coordinating read mmap lifetimes with mutations.
    active_mmaps: Arc<ActiveMmapRegistry>,
    /// Dedicated object I/O actor for on-disk object operations.
    pub(super) object_actor: ActorRef<FileObjectActorMessage>,
    /// Lock file handle held for the duration of this process's exclusive
    /// access to the filesystem store. Dropped on state drop to release.
    #[expect(dead_code)]
    lock_file: Option<std::fs::File>,
}

/// Internal filesystem backend runtime operations and helpers.
impl FileSystemState {
    /// Opens a CAS repository with an explicit optimizer depth penalty and optional
    /// integrity verification policy.
    pub async fn open_with_alpha_and_recovery(
        root: impl AsRef<Path>,
        alpha: u64,
        recovery: FileSystemRecoveryOptions,
        integrity: CasIntegrityConfig,
    ) -> Result<Self, CasError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join(STORAGE_VERSION).join(empty_content_hash().algorithm_name()))
            .await
            .map_err(|source| CasError::io("creating objects root", root.clone(), source))?;

        bootstrap_empty_object(&root).await?;

        let lock_path = root.join("lock");
        let lock_file =
            acquire_filesystem_lock(root.clone(), &lock_path, recovery.wait_for_lock).await?;

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
            reconstructed_bytes_cache: Mutex::new(HashMap::new()),
            stream_buffer_pool: StreamBufferPool::new(
                FILESYSTEM_STREAM_READ_CHUNK_BYTES,
                FILESYSTEM_STREAM_BUFFER_POOL_MAX_BUFFERS,
            ),
            index_db: redb_index,
            recovery,
            integrity,

            metrics: FileSystemMetricsState::default(),
            optimize_in_progress: AtomicBool::new(false),
            active_mmaps,
            object_actor,
            lock_file,
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
    pub(super) fn lock_index_read(&self, _operation: &str) -> RwLockReadGuard<'_, IndexState> {
        self.index.read()
    }

    /// Acquires an exclusive write guard over runtime index state.
    pub(super) fn lock_index_write(&self, _operation: &str) -> RwLockWriteGuard<'_, IndexState> {
        self.index.write()
    }

    fn lock_reconstructed_cache(
        &self,
        _operation: &str,
    ) -> MutexGuard<'_, HashMap<Hash, (Instant, Bytes)>> {
        self.reconstructed_bytes_cache.lock()
    }

    /// Removes one hash from in-process reconstructed-byte cache.
    fn invalidate_cached_object_bytes(&self, hash: Hash) {
        let mut cache = self.lock_reconstructed_cache("invalidating reconstructed-bytes cache");
        cache.remove(&hash);
    }

    /// Returns the integrity verification policy for read-path hash checks.
    #[allow(dead_code)]
    pub(super) fn integrity(&self) -> &CasIntegrityConfig {
        &self.integrity
    }

    /// Returns the current unix epoch timestamp in seconds.
    fn now_unix() -> i64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs().cast_signed())
    }

    /// Checks whether a hash can skip full BLAKE3 re-verification based on
    /// the configured integrity strategy.
    fn can_skip_verification(&self, hash: Hash) -> bool {
        let config = &self.integrity;
        if config.verify_on_read.is_empty() {
            return true;
        }

        let index_guard = self.index.read();
        let meta = index_guard.objects.get(&hash);
        let verify_time = meta.map_or(0, ObjectMeta::verify_time);
        drop(index_guard);

        for strategy in &config.verify_on_read {
            match strategy {
                VerifyTriggerStrategy::Always => return false,
                VerifyTriggerStrategy::Modified => {
                    // Check mtime of on-disk object file.
                    let path = object_path(&self.root, hash);
                    if let Ok(metadata) = std::fs::metadata(&path)
                        && let Ok(mtime) = metadata.modified().map(|t| {
                            t.duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                .cast_signed()
                        })
                        && mtime > verify_time
                    {
                        return false;
                    }
                }
                VerifyTriggerStrategy::Sample { denominator } => {
                    if *denominator > 0 && rand::rng().random_range(0..*denominator) == 0 {
                        return false;
                    }
                }
                VerifyTriggerStrategy::Stale { timeout } => {
                    let now = Self::now_unix();
                    if verify_time == 0
                        || now.saturating_sub(verify_time).cast_unsigned() >= timeout.as_secs()
                    {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Records a successful verification in the object metadata.
    fn record_verified(&self, hash: Hash) {
        let now = Self::now_unix();
        let mut index = self.index.write();
        if let Some(meta) = index.objects.get_mut(&hash) {
            meta.set_verify_time(now);
        }
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
    pub(super) fn record_optimizer_runtime_ms(&self, runtime_ms: u64) {
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
    pub(super) fn metrics_snapshot(&self) -> FileSystemMetrics {
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
    pub(super) fn topology_snapshot(&self, include_empty: bool) -> CasTopologySnapshot {
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
            let cache = self.lock_reconstructed_cache("reading reconstructed-bytes cache");
            cache.get(&hash).and_then(|(cached_at, bytes)| {
                if cached_at.elapsed() < self.integrity.reconstructed_bytes_cache_ttl {
                    Some(bytes.clone())
                } else {
                    None
                }
            })
        } {
            self.record_cache_hit();
            return Ok(cached);
        }

        let bytes = self.get(hash).await?;
        {
            let mut cache = self.lock_reconstructed_cache("writing reconstructed-bytes cache");
            cache.insert(hash, (Instant::now(), bytes.clone()));
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
    pub(super) async fn persist_index_snapshot(&self) -> Result<(), CasError> {
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

    /// Persists incremental index operations.
    async fn persist_index_batch(&self, operations: Vec<BatchOperation>) -> Result<(), CasError> {
        if operations.is_empty() {
            return Ok(());
        }

        let index_db = self.index_db.clone();
        tokio::task::spawn_blocking(move || index_db.persist_batch(operations)).await.map_err(
            |err| CasError::task_join("persisting incremental index batch to redb", err),
        )??;

        Ok(())
    }

    /// Rebuilds index state from object store and publishes repaired state.
    pub(super) async fn repair_index_from_object_store(
        &self,
    ) -> Result<IndexRepairReport, CasError> {
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
    pub(super) async fn migrate_index_to_version(
        &self,
        target_version: u32,
    ) -> Result<(), CasError> {
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
        // Cycle safety pre-check via shared helper before the reachability walk.
        check_no_cycle(start, |h| {
            let meta = index
                .objects
                .get(&h)
                .ok_or_else(|| CasError::corrupt_index(format!("missing metadata for base {h}")))?;
            match meta.encoding() {
                ObjectEncoding::Full => Ok(None),
                ObjectEncoding::Delta { base_hash } => Ok(Some(base_hash)),
            }
        })?;

        let mut current = start;
        loop {
            if current == needle {
                return Ok(true);
            }
            if current == empty_content_hash() {
                return Ok(false);
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
    pub(super) fn recompute_descendant_depths_with_fallback(
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
    pub(super) fn sync_delta_reverse_for_meta_update(
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

        let mut meta = Self::meta_for_object(&full, resolved_depth);
        meta.set_verify_time(Self::now_unix());
        self.persist_index_batch(vec![BatchOperation::UpsertObject { hash: target_hash, meta }])
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
    pub(super) async fn optimize_target_if_beneficial(
        &self,
        target: Hash,
    ) -> Result<bool, CasError> {
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

        let mut meta = Self::meta_for_object(&best.object, resolved_depth);
        meta.set_verify_time(Self::now_unix());
        self.persist_index_batch(vec![BatchOperation::UpsertObject { hash: target, meta }]).await?;

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
        // Cycle detection via local visited set (async reads preclude shared helpers in `chain.rs`).
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
                    if !self.can_skip_verification(hash) {
                        ensure_reconstructed_hash(
                            hash,
                            mapped_full.as_ref(),
                            "mmap full-object read",
                        )?;
                        self.record_verified(hash);
                    }
                    return Ok(mapped_full);
                }

                let data = apply_delta_patch_stack(mapped_full.to_vec(), &mut patch_payloads)?;
                ensure_reconstructed_size(hash, expected_len, data.len())?;
                if !self.can_skip_verification(hash) {
                    ensure_reconstructed_hash(hash, data.as_slice(), "mmap+delta reconstruction")?;
                    self.record_verified(hash);
                }

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
                    if !self.can_skip_verification(hash) {
                        ensure_reconstructed_hash(
                            hash,
                            data.as_slice(),
                            "full/delta reconstruction",
                        )?;
                        self.record_verified(hash);
                    }
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
        if hash == empty_content_hash() {
            return Ok(Box::pin(stream::once(async move { Ok(Bytes::new()) })));
        }

        // Fast path: stream full object files directly from disk in chunks.
        // This avoids loading large objects entirely into memory.
        let full_path = object_path(&self.root, hash);
        match try_open_and_stream_full_object(&full_path).await {
            Ok(Some(stream)) => return Ok(stream),
            Ok(None) => { /* full object file doesn't exist; reconstruct from delta chain */ }
            Err(e) => return Err(e),
        }

        // Fallback: reconstruct from delta chain (loads full object into memory).
        let bytes = self.get(hash).await?;
        Ok(Box::pin(stream::once(async move { Ok(bytes) })))
    }

    async fn materialize_to_path(&self, hash: Hash, dest: PathBuf) -> Result<(), CasError> {
        if hash == empty_content_hash() {
            tokio::fs::write(&dest, []).await.map_err(|err| {
                CasError::io("materialize_to_path: write empty content", &dest, err)
            })?;
            return Ok(());
        }

        // Fast path: copy full object file directly (kernel-level zero-copy),
        // then make the destination writable so tools can modify the file.
        let full_path = object_path(&self.root, hash);
        match fs::try_exists(&full_path).await {
            Ok(true) => {
                fs::copy(&full_path, &dest).await.map_err(|err| {
                    CasError::io("materialize_to_path: copy full object file", &dest, err)
                })?;
                // CAS store files are read-only; ensure the destination is
                // writable so sandbox tools (e.g. rsgain) can modify it.
                let metadata = std::fs::metadata(&dest).map_err(|source| {
                    CasError::io("materialize_to_path: read destination metadata", &dest, source)
                })?;
                let mut permissions = metadata.permissions();
                if permissions.readonly() {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let mode = permissions.mode();
                        permissions.set_mode(mode | 0o200);
                    }
                    #[cfg(not(unix))]
                    {
                        permissions.set_readonly(false);
                    }
                    std::fs::set_permissions(&dest, permissions).map_err(|source| {
                        CasError::io(
                            "materialize_to_path: make destination writable",
                            &dest,
                            source,
                        )
                    })?;
                }
                return Ok(());
            }
            Ok(false) => { /* fall through to default */ }
            Err(err) => {
                return Err(CasError::io(
                    "materialize_to_path: check object existence",
                    &full_path,
                    err,
                ));
            }
        }

        // Fallback: reconstruct from delta chain and then write.
        let data = self.get(hash).await?;
        tokio::fs::write(&dest, data).await.map_err(|err| {
            CasError::io("materialize_to_path: write reconstructed content", &dest, err)
        })?;
        Ok(())
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
        let before;
        let after_set;
        let add_bases;
        let remove_bases;
        {
            let mut index = self.lock_index_write("applying constraint patch");
            if !index.objects.contains_key(&target_hash) {
                return Err(CasError::NotFound(target_hash));
            }
            for base in &patch.add_bases {
                if *base != empty_content_hash() && !index.objects.contains_key(base) {
                    return Err(CasError::NotFound(*base));
                }
            }

            before = index.constraints.get(&target_hash).cloned().unwrap_or_default();
            let merged = Self::merge_constraint_patch(Some(&before), patch);
            validate_constraint_target_not_in_bases(target_hash, &merged)?;

            let after_optic = Self::set_constraint_row_optic(&mut index, target_hash, merged);
            after_set = after_optic.clone().unwrap_or_default();

            add_bases = after_set.difference(&before).copied().collect::<Vec<_>>();
            remove_bases = before.difference(&after_set).copied().collect::<Vec<_>>();
        }

        if add_bases.is_empty() && remove_bases.is_empty() {
            // Nothing changed — no persistence needed.
            return Ok(if after_set.is_empty() {
                None
            } else {
                Some(Constraint { target_hash, potential_bases: after_set })
            });
        }

        self.persist_index_batch(vec![BatchOperation::PatchConstraintBases {
            target_hash,
            add_bases,
            remove_bases,
        }])
        .await?;

        Ok(if after_set.is_empty() {
            None
        } else {
            Some(Constraint { target_hash, potential_bases: after_set })
        })
    }

    /// Applies a batch of constraint mutations in a single index-write and
    /// persistence call.
    ///
    /// Each op in the batch is independently validated before the batch is
    /// committed. If any op fails validation, the entire batch is rejected
    /// and no state is persisted.
    async fn set_constraint_batch(&self, batch: Vec<ConstraintBatchOp>) -> Result<(), CasError> {
        #[derive(Debug, Clone)]
        enum PersistRow {
            /// Replace the entire constraint set (from [`ConstraintBatchOp::Set`]).
            Set { target_hash: Hash, bases: BTreeSet<Hash> },
            /// Apply an incremental delta (from [`ConstraintBatchOp::Patch`]).
            Patch { target_hash: Hash, add_bases: Vec<Hash>, remove_bases: Vec<Hash> },
        }

        let mut persisted_rows: Vec<PersistRow> = Vec::with_capacity(batch.len());

        {
            let mut index = self.lock_index_write("applying batched constraint mutations");

            for op in &batch {
                match op {
                    ConstraintBatchOp::Set { target_hash, potential_bases } => {
                        validate_constraint_target_not_in_bases(*target_hash, potential_bases)?;
                        if !index.objects.contains_key(target_hash) {
                            return Err(CasError::NotFound(*target_hash));
                        }
                        for base in potential_bases {
                            if *base != empty_content_hash() && !index.objects.contains_key(base) {
                                return Err(CasError::NotFound(*base));
                            }
                        }

                        let bases = Self::set_constraint_row_optic(
                            &mut index,
                            *target_hash,
                            potential_bases.clone(),
                        )
                        .unwrap_or_default();
                        persisted_rows.push(PersistRow::Set { target_hash: *target_hash, bases });
                    }
                    ConstraintBatchOp::Patch { target_hash, patch } => {
                        if !index.objects.contains_key(target_hash) {
                            return Err(CasError::NotFound(*target_hash));
                        }
                        for base in &patch.add_bases {
                            if *base != empty_content_hash() && !index.objects.contains_key(base) {
                                return Err(CasError::NotFound(*base));
                            }
                        }

                        let before =
                            index.constraints.get(target_hash).cloned().unwrap_or_default();
                        let merged = Self::merge_constraint_patch(Some(&before), patch.clone());
                        validate_constraint_target_not_in_bases(*target_hash, &merged)?;

                        // Update in-memory state via the full row optic (computes
                        // reverse-link diffs internally).
                        let after =
                            Self::set_constraint_row_optic(&mut index, *target_hash, merged)
                                .unwrap_or_default();

                        // Compute the delta for persistence — only the changed
                        // bases need to be written, not the entire set.
                        let add_bases: Vec<Hash> = after.difference(&before).copied().collect();
                        let remove_bases: Vec<Hash> = before.difference(&after).copied().collect();
                        persisted_rows.push(PersistRow::Patch {
                            target_hash: *target_hash,
                            add_bases,
                            remove_bases,
                        });
                    }
                }
            }
        }

        let operations: Vec<BatchOperation> = persisted_rows
            .into_iter()
            .map(|row| match row {
                PersistRow::Set { target_hash, bases } => {
                    BatchOperation::SetConstraintBases { target_hash, bases }
                }
                PersistRow::Patch { target_hash, add_bases, remove_bases } => {
                    BatchOperation::PatchConstraintBases { target_hash, add_bases, remove_bases }
                }
            })
            .collect();

        self.persist_index_batch(operations).await
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
        let patch = DeltaPatch::decode(patch_payload.as_ref());
        base_payload = patch.apply(&base_payload)?;
    }

    Ok(base_payload)
}

/// Attempts to open a full object file and produce a chunked streaming reader.
///
/// Returns `Ok(None)` when `path` does not exist (e.g. it is a delta-chain
/// object, or the object is simply absent). Small files (≤ 256 KiB) are read
/// entirely into a single chunk to amortize async-read overhead.
async fn try_open_and_stream_full_object(path: &Path) -> Result<Option<CasByteStream>, CasError> {
    const SMALL_OBJECT_BYTES: u64 = 256 * 1024;
    // Check existence first to avoid signalling a spurious IO error for a
    // legitimate delta-object lookup.
    match fs::try_exists(path).await {
        Ok(true) => { /* proceed */ }
        Ok(false) => return Ok(None),
        Err(source) => {
            return Err(CasError::io(
                "checking full object file existence for streaming",
                path,
                source,
            ));
        }
    }
    let file = fs::File::open(path)
        .await
        .map_err(|source| CasError::io("opening full object for streaming", path, source))?;
    let file_len = file
        .metadata()
        .await
        .map_err(|source| CasError::io("getting full object metadata for streaming", path, source))?
        .len();

    if file_len <= SMALL_OBJECT_BYTES {
        // Read entirely into one chunk to avoid tiny-async-read overhead.
        drop(file); // release the fd before the independent fs::read call
        let bytes = fs::read(path).await.map_err(|source| {
            CasError::io("reading small full object for streaming", path, source)
        })?;
        return Ok(Some(Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) }))));
    }

    // Chunked stream for larger objects — 256 KiB per chunk.
    Ok(Some(chunked_full_object_stream(file, file_len, path)))
}

/// Produces a chunked [`CasByteStream`] that reads a large full-object file in
/// fixed-size blocks, yielding each block as a [`Bytes`] slice.
///
/// The stream signals the end of the file by returning `None` (i.e. the stream
/// itself, not an error).
fn chunked_full_object_stream(file: fs::File, file_len: u64, path: &Path) -> CasByteStream {
    const CHUNK_BYTES: u64 = 256 * 1024;
    let path = path.to_path_buf();

    Box::pin(stream::unfold((file, 0u64), move |(mut file, pos)| {
        let path = path.clone();
        async move {
            if pos >= file_len {
                return None;
            }
            let remaining = file_len - pos;
            #[allow(clippy::cast_possible_truncation)]
            let to_read = std::cmp::min(CHUNK_BYTES, remaining) as usize;
            let mut buf = vec![0u8; to_read];
            match file.read_exact(&mut buf).await {
                Ok(_n) => {
                    let bytes = Bytes::from(buf);
                    let new_pos = pos + to_read as u64;
                    Some((Ok(bytes), (file, new_pos)))
                }
                Err(source) => {
                    let err = CasError::io("reading chunk from full object stream", &path, source);
                    // Advance pos past the remaining length so the stream
                    // terminates after this error.
                    Some((Err(err), (file, file_len)))
                }
            }
        }
    }))
}

/// Fast O(1) length pre-check before full hash verification.
///
/// Strictly redundant with the subsequent hash check (any corruption that
/// changes the size also changes the hash), but provides cheap early
/// rejection before the hash computation.
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

/// Terminal integrity proof: recomputes `Hash::from_content()` on the
/// reconstructed payload and compares against the expected hash.
///
/// This is the definitive integrity guarantee for reconstructed objects.
/// The O(1) size check via [`ensure_reconstructed_size`] runs first as a
/// fast pre-filter but is strictly redundant with this hash check.
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

/// Acquire an exclusive filesystem lock with stale PID detection.
///
/// On Unix, `flock` is released by the kernel when the owning process
/// exits, so stale locks should not normally occur. However, edge
/// cases (force-kill, system crash, macOS-specific behavior) can
/// leave a lock file behind whose `flock` has been released but whose
/// content identifies a now-dead PID.
async fn acquire_filesystem_lock(
    root: PathBuf,
    lock_path: &Path,
    wait_for_lock: bool,
) -> Result<Option<std::fs::File>, CasError> {
    match std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
    {
        Ok(mut file) => {
            // Check for stale lock — a lock file whose owning PID is
            // no longer alive.
            if let Some(stale_pid) = check_stale_lock(lock_path)? {
                warn!(
                    path = %lock_path.display(),
                    stale_pid,
                    "lock file appears stale (owner process no longer \
                     exists), breaking stale lock"
                );
                drop(file);
                fs::remove_file(lock_path)
                    .await
                    .map_err(|e| CasError::io("removing stale lock file", lock_path, e))?;
                file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(false)
                    .open(lock_path)
                    .map_err(|e| {
                        CasError::io("re-opening lock file after stale break", lock_path, e)
                    })?;
            }

            if wait_for_lock {
                file.lock().map_err(|e| CasError::io("acquire filesystem lock", lock_path, e))?;
            } else {
                match file.try_lock() {
                    Ok(()) => {}
                    Err(std::fs::TryLockError::WouldBlock) => {
                        return Err(CasError::StoreLocked { root });
                    }
                    Err(std::fs::TryLockError::Error(e)) => {
                        return Err(CasError::io("acquire filesystem lock", lock_path, e));
                    }
                }
            }

            // Write our PID to the lock file so subsequent processes
            // can detect stale locks on future startups.
            if let Err(e) = file
                .set_len(0)
                .and_then(|()| file.write_all(std::process::id().to_string().as_bytes()))
            {
                warn!("failed to write PID to lock file: {e}");
            }

            Ok(Some(file))
        }
        Err(e) => Err(CasError::io("open lock file", lock_path, e)),
    }
}

/// Checks whether the lock file at `path` contains a PID belonging to a
/// process that is no longer alive.
///
/// Returns `Ok(Some(stale_pid))` if a stale lock was detected,
/// `Ok(None)` if the lock is either not present, empty, or held by a
/// still-running process.
fn check_stale_lock(path: &Path) -> Result<Option<u32>, CasError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(CasError::io("reading lock file", path, e)),
    };

    let pid_str = content.trim();
    if pid_str.is_empty() {
        // No PID written; cannot determine staleness, assume active.
        return Ok(None);
    }

    let pid: u32 = match pid_str.parse() {
        Ok(p) => p,
        Err(_) => {
            // Lock file contains unparseable content; treat as not stale
            // (backward compatibility with old 0-byte lock files).
            return Ok(None);
        }
    };

    if pid == 0 || !is_pid_alive(pid) {
        return Ok(Some(pid));
    }

    Ok(None)
}

/// Returns `true` if a process with the given PID is still running, using
/// the POSIX `kill` syscall with signal 0 (which probes existence without
/// sending a signal).
#[cfg(unix)]
fn is_pid_alive(pid: u32) -> bool {
    // SAFETY: `kill(pid, 0)` is a signal-safe POSIX syscall that probes
    // process existence. It is safe even for invalid PIDs — the kernel
    // returns ESRCH.
    let result = unsafe { libc::kill(pid.cast_signed(), 0) };
    match result {
        0 => true,
        -1 => {
            let err = ::std::io::Error::last_os_error();
            err.raw_os_error() != Some(libc::ESRCH) // ESRCH = no such process
        }
        // Unexpected return value; conservatively assume alive.
        _ => true,
    }
}

/// On non-Unix platforms, conservatively assume every PID is alive since
/// we cannot use the POSIX `kill` syscall.
#[cfg(not(unix))]
fn is_pid_alive(_pid: u32) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::FileSystemState;
    use crate::{Hash, IndexState, ObjectMeta, ensure_empty_record};

    #[test]
    fn localized_depth_recompute_updates_transitive_descendants() {
        let base = Hash::from_content(b"depth-base");
        let pivot = Hash::from_content(b"depth-pivot");
        let child = Hash::from_content(b"depth-child");
        let grandchild = Hash::from_content(b"depth-grandchild");

        let mut index = IndexState::default();
        ensure_empty_record(&mut index);
        index.objects.insert(base, ObjectMeta::full(4, 4, 1));
        index.objects.insert(pivot, ObjectMeta::full(4, 4, 1));
        index.objects.insert(child, ObjectMeta::delta(2, 4, 2, pivot));
        index.objects.insert(grandchild, ObjectMeta::delta(2, 4, 3, child));
        index.rebuild_delta_reverse();

        let next_pivot = ObjectMeta::delta(2, 4, 2, base);
        let previous_pivot = index.objects.insert(pivot, next_pivot);
        FileSystemState::sync_delta_reverse_for_meta_update(
            &mut index,
            pivot,
            previous_pivot,
            next_pivot,
        );

        let roots = BTreeSet::from([pivot]);
        FileSystemState::recompute_descendant_depths_with_fallback(&mut index, &roots)
            .expect("localized depth recompute should succeed");

        assert_eq!(index.objects.get(&pivot).expect("pivot meta").depth(), 2);
        assert_eq!(index.objects.get(&child).expect("child meta").depth(), 3);
        assert_eq!(index.objects.get(&grandchild).expect("grandchild meta").depth(), 4);
    }
}
