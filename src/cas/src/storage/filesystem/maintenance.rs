//! Maintenance operations for the filesystem-backed CAS backend.
//!
//! This module isolates optimize/prune flows from hot put/get code paths.

use std::collections::{BTreeSet, HashSet};
use std::time::Instant;

use async_trait::async_trait;
use tracing::{info, instrument};

use crate::api::CasApi;
use crate::storage::is_unconstrained_constraint_row;
use crate::{
    CasError, CasMaintenanceApi, CompactReport, GcSweepReport, Hash, IndexRepairReport,
    OptimizeOptions, OptimizeReport, PruneReport, empty_content_hash,
};

use super::{FILESYSTEM_DEFAULT_OPTIMIZE_MAX_REWRITES, FileSystemState};

#[async_trait]
/// Maintenance trait implementation for shared filesystem runtime state.
impl CasMaintenanceApi for FileSystemState {
    #[allow(unreachable_code, unused_variables)]
    #[instrument(name = "filesystem.optimize_once", skip(self, options))]
    async fn optimize_once(&self, options: OptimizeOptions) -> Result<OptimizeReport, CasError> {
        // TODO(#XXX): Re-enable optimize_once after fixing the O(n×m) scaling
        // issue. The current implementation does a full object scan per call
        // to unconstrained_candidate_bases_for_target (up to 24× the full
        // object set). For small repos it's wasted work; for large repos it
        // causes visible hangs. The optimizer needs a bounded scan or an
        // incremental approach before re-enabling.
        //
        // Tests disabled alongside this (search for optimize_once TODO ref):
        // - codec/object.rs: optimized_delta_is_stored_in_diff_extension_file
        // - storage/filesystem/mod.rs: filesystem_optimize_once_rewrites_unconstrained_objects
        // - storage/filesystem/mod.rs: filesystem_metrics_expose_cache_hits_and_optimizer_runtime
        // - storage/filesystem/mod.rs: filesystem_topology_snapshot_captures_nodes_delta_edges_and_constraints
        // - storage/filesystem/mod.rs: filesystem_visualize_mermaid_emits_graph_nodes_and_edges
        // - tests/int/storage_format.rs: diff_objects_use_dot_diff_extension_and_raw_path_absent
        // - tests/int/visualize.rs: visualize_mermaid_contains_graph_edges
        //
        // When re-enabling, remove #[ignore] from all of the above and remove
        // this whole early-return block.
        //
        // Tracked by: temporary disable during GC improvements — remove this
        // return and the associated comment once the scaling fix lands.
        return Ok(OptimizeReport { rewritten_objects: 0 });

        let optimize_started = Instant::now();
        let (mut constrained_targets, mut unconstrained_targets) = {
            let index = self.lock_index_read("collecting optimize targets");

            let constrained: BTreeSet<Hash> = index
                .constraints
                .keys()
                .copied()
                .filter(|hash| *hash != empty_content_hash())
                .collect();

            let unconstrained: Vec<Hash> = index
                .objects
                .keys()
                .copied()
                .filter(|hash| *hash != empty_content_hash() && !constrained.contains(hash))
                .collect();

            (constrained.into_iter().collect::<Vec<_>>(), unconstrained)
        };
        constrained_targets.sort_unstable();
        unconstrained_targets.sort_unstable();

        let mut targets = constrained_targets;
        targets.extend(unconstrained_targets);

        if targets.is_empty() {
            let runtime_ms =
                u64::try_from(optimize_started.elapsed().as_millis().max(1)).unwrap_or(u64::MAX);
            self.record_optimizer_runtime_ms(runtime_ms);
            return Ok(OptimizeReport { rewritten_objects: 0 });
        }

        let mut rewritten = 0usize;
        let started = Instant::now();
        let max_rewrites = options.max_rewrites.unwrap_or(FILESYSTEM_DEFAULT_OPTIMIZE_MAX_REWRITES);
        for target in targets {
            if rewritten >= max_rewrites {
                break;
            }
            if let Some(timeout) = options.timeout
                && started.elapsed() >= timeout
            {
                break;
            }
            if self.optimize_target_if_beneficial(target).await? {
                rewritten += 1;
            }
        }

        let runtime_ms =
            u64::try_from(optimize_started.elapsed().as_millis().max(1)).unwrap_or(u64::MAX);
        self.record_optimizer_runtime_ms(runtime_ms);

        let metrics = self.metrics_snapshot();
        info!(
            rewritten_objects = rewritten,
            optimizer_runtime_ms = runtime_ms,
            cumulative_optimizer_runtime_ms = metrics.optimizer_runtime_ms,
            cache_hits = metrics.cache_hits,
            delta_compression_ratio = metrics.delta_compression_ratio,
            "filesystem optimize_once completed"
        );

        Ok(OptimizeReport { rewritten_objects: rewritten })
    }

    /// Removes stale explicit constraint candidates and rows.
    ///
    /// Pruning rules:
    /// - drop rows whose target no longer exists,
    /// - drop candidate bases that no longer exist (except implicit empty base),
    /// - drop rows that become unconstrained after filtering.
    ///
    /// The in-memory reverse-constraint index is rebuilt after pruning and the
    /// resulting snapshot is persisted to durable storage.
    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        let mut removed = 0usize;
        {
            let mut index = self.lock_index_write("pruning constraint rows");

            let existing: HashSet<Hash> = index.objects.keys().copied().collect();
            index.constraints.retain(|target, bases| {
                if !existing.contains(target) {
                    removed += bases.len();
                    return false;
                }
                let before = bases.len();
                bases.retain(|candidate| {
                    *candidate == empty_content_hash() || existing.contains(candidate)
                });
                removed += before.saturating_sub(bases.len());
                !is_unconstrained_constraint_row(bases)
            });
            index.rebuild_constraint_reverse();
        }

        self.persist_index_snapshot().await?;
        Ok(PruneReport { removed_candidates: removed })
    }

    async fn list_all_hashes(&self) -> Result<Vec<Hash>, CasError> {
        let index = self.lock_index_read("listing all hashes for GC sweep");
        let hashes: Vec<Hash> = index.objects.keys().copied().collect();
        Ok(hashes)
    }

    async fn gc_sweep(&self, roots: &BTreeSet<Hash>) -> Result<GcSweepReport, CasError> {
        let all_hashes = self.list_all_hashes().await?;
        let total_objects = all_hashes.len();
        // Exclude recently-written hashes from the sweep set to avoid
        // pruning objects that were committed by a concurrent writer but
        // whose hash may not have reached the durable index yet.
        let recently_written: Vec<Hash> = self.recently_written.iter().map(|e| *e).collect();
        self.recently_written.clear();
        let sweep_set: Vec<Hash> = all_hashes
            .into_iter()
            .filter(|hash| {
                *hash != empty_content_hash()
                    && !roots.contains(hash)
                    && !recently_written.contains(hash)
            })
            .collect();
        let deleted_count = sweep_set.len();

        if !sweep_set.is_empty() {
            tracing::info!(
                sweeping = sweep_set.len(),
                total = total_objects,
                "GC sweep: deleting unreachable objects"
            );
            self.delete_many(sweep_set).await?;
            tracing::info!("GC sweep: deletion complete");
        } else {
            tracing::info!(total = total_objects, "GC sweep: no unreachable objects to delete");
        }

        Ok(GcSweepReport { deleted_count, total_objects })
    }

    /// Rebuilds durable index metadata from object-store state.
    ///
    /// This delegates to filesystem recovery logic and persists the rebuilt
    /// index snapshot on success.
    async fn repair_index(&self) -> Result<IndexRepairReport, CasError> {
        self.repair_index_from_object_store().await
    }

    /// Migrates durable index metadata to one target schema marker.
    ///
    /// Migration is performed by the shared index persistence layer and then
    /// published back into runtime index state.
    async fn migrate_index_to_version(&self, target_version: u32) -> Result<(), CasError> {
        self.migrate_index_to_version(target_version).await
    }

    async fn compact_index(&self) -> Result<CompactReport, CasError> {
        self.compact_index().await
    }
}
