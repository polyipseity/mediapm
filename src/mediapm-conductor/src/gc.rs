//! Garbage collection orchestration for conductor.
//!
//! Conductor GC is a three-phase process covering state-instance pruning, CAS
//! orphan reclamation, and CAS metadata maintenance.  It is **distinct** from
//! CAS-internal GC — callers run both as appropriate.

use std::collections::BTreeSet;

use mediapm_cas::{CasApi, CasMaintenanceApi, Hash, PruneReport};
use tracing;

use crate::error::ConductorError;
use crate::orchestration::protocol::UnifiedNickelDocument;

use crate::model::state::OrchestrationState;

/// Aggregate report from one full conductor GC cycle.
#[derive(Debug, Clone, Default)]
pub struct ConductorGcReport {
    /// Number of tool-call instances evicted by TTL grace period.
    pub instances_removed: usize,
    /// Number of orphan CAS blobs deleted.
    pub orphans_removed: usize,
    /// Result of the final CAS maintenance / constraint-pruning step.
    pub prune_report: PruneReport,
}

/// Runs full conductor garbage collection: instance pruning, CAS orphan
/// deletion, and CAS maintenance.
///
/// This is the one-stop function called by background GC loops and CLI.
///
/// # Phases
///
/// 1. **Instance GC** — evict stale tool-call instances from
///    [`OrchestrationState`] using the TTL grace period.
/// 2. **Root-set / orphan reclamation** — collect all referenced CAS hashes
///    from surviving instances and the unified config, list all CAS hashes,
///    delete the orphans.
/// 3. **CAS maintenance** — delegate to [`run_cas_gc_sweep`] for constraint
///    pruning and WAL consumption.
///
/// # Errors
///
/// Returns [`ConductorError::Cas`] on any CAS operation failure.
pub(crate) async fn run_conductor_gc<C>(
    cas: &C,
    state: &mut OrchestrationState,
    unified: &UnifiedNickelDocument,
    referenced_keys: &BTreeSet<String>,
    ttl_seconds: u64,
) -> Result<ConductorGcReport, ConductorError>
where
    C: CasApi + CasMaintenanceApi,
{
    // ---- Phase 1: Instance-level GC (TTL-based) ----
    tracing::info!("conductor GC phase 1/3: instance pruning (ttl={ttl_seconds}s)");
    let instance_count_before = state.tool_call_instances.len();
    state.run_conductor_gc(referenced_keys, ttl_seconds);
    let instances_removed = instance_count_before.saturating_sub(state.tool_call_instances.len());

    // ---- Phase 2: Root-set collection + CAS orphan deletion ----
    tracing::info!("conductor GC phase 2/3: root-set collection and orphan reclamation");
    let mut root_set: BTreeSet<Hash> = BTreeSet::new();

    // Hashes referenced by surviving tool-call instance outputs.
    for instance in state.tool_call_instances.values() {
        for output in &instance.outputs {
            root_set.insert(output.hash);
        }
    }

    // External-data policy keys are CAS hashes that must be preserved.
    root_set.extend(unified.external_data_policies.keys().copied());

    // Tool-content-map hashes referenced by the config.
    root_set.extend(unified.tool_content_hashes.iter().copied());

    let all_hashes: BTreeSet<Hash> =
        cas.list_hashes().await.map_err(ConductorError::Cas)?.into_iter().collect();

    let orphans: Vec<&Hash> = all_hashes.difference(&root_set).collect();
    let orphans_removed = orphans.len();

    for hash in &orphans {
        // CONDUCTOR GC: deleting orphan CAS blob {hash}
        cas.delete(**hash).await.map_err(ConductorError::Cas)?;
    }

    // Flush pending WAL entries (deletes) so the maintenance step sees a
    // consistent state.
    cas.flush().await.map_err(ConductorError::Cas)?;

    // ---- Phase 3: CAS maintenance (optimize + prune constraints) ----
    tracing::info!("conductor GC phase 3/3: CAS maintenance");
    let prune_report = run_cas_gc_sweep(cas).await?;

    Ok(ConductorGcReport { instances_removed, orphans_removed, prune_report })
}

/// Runs full CAS maintenance: optimize, prune constraints.
///
/// This is the one-stop function called by background GC loops and CLI. It
/// does **not** touch instance GC or orphan reclamation — callers should run
/// [`run_conductor_gc`] for the full cycle.
///
/// # Errors
///
/// Returns [`ConductorError::Cas`] when any maintenance operation fails.
pub async fn run_cas_gc_sweep<C>(cas: &C) -> Result<PruneReport, ConductorError>
where
    C: CasApi + CasMaintenanceApi,
{
    tracing::info!("CAS GC phase 1/2: run_maintenance_cycle");
    cas.run_maintenance_cycle().await.map_err(ConductorError::Cas)?;
    tracing::info!("CAS GC phase 2/2: prune_constraints");
    let report = cas.prune_constraints().await.map_err(ConductorError::Cas)?;
    tracing::info!("CAS GC phase 2/2: complete (removed={})", report.removed);
    Ok(report)
}
