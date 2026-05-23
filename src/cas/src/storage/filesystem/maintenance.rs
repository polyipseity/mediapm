//! Maintenance operations for the filesystem-backed CAS backend.
//!
//! This module isolates optimize/prune flows from hot put/get code paths.

use std::collections::{BTreeSet, HashSet};
use std::time::Instant;

use async_trait::async_trait;
use tracing::{info, instrument};

use crate::storage::is_unconstrained_constraint_row;
use crate::{
    CasError, CasMaintenanceApi, Hash, IndexRepairReport, OptimizeOptions, OptimizeReport,
    PruneReport, empty_content_hash,
};

use super::{FILESYSTEM_DEFAULT_OPTIMIZE_MAX_REWRITES, FileSystemState};

#[async_trait]
/// Maintenance trait implementation for shared filesystem runtime state.
impl CasMaintenanceApi for FileSystemState {
    #[instrument(name = "filesystem.optimize_once", skip(self, options))]
    async fn optimize_once(&self, options: OptimizeOptions) -> Result<OptimizeReport, CasError> {
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
}
