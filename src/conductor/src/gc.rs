//! GC root computation and CAS sweep orchestration for conductor maintenance.
//!
//! The entry point [`compute_gc_roots`] collects all CAS hashes referenced by
//! current conductor configuration and state so a downstream sweep can
//! distinguish live objects from unreachable blobs.
//!
//! [`run_cas_gc_sweep`] composes root computation with a CAS sweep + index
//! compaction step — this is the decoupled function used by the background
//! GC loop in the node actor and by the CLI.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::{CasApi, CasMaintenanceApi, GcSweepReport, Hash, OptimizeOptions};
use tracing;

use crate::error::ConductorError;
use crate::model::config::ExternalContentRef;
use crate::model::state::OrchestrationState;

/// Computes the full set of protected CAS hashes for GC root computation.
///
/// # Invariant
///
/// `content_map ⊆ external_data` is enforced at decode time by
/// `vet_latest_envelope()`, so this function iterates only `external_data`
/// keys (plus the state pointer and instance I/O) — `content_map` iteration
/// is redundant and omitted.
#[must_use]
pub fn compute_gc_roots(
    user_external_data: &BTreeMap<Hash, ExternalContentRef>,
    machine_external_data: &BTreeMap<Hash, ExternalContentRef>,
    state_pointer: Option<Hash>,
    state: &OrchestrationState,
) -> BTreeSet<Hash> {
    compute_gc_roots_from_keys(
        user_external_data.keys().copied().chain(machine_external_data.keys().copied()),
        state_pointer,
        state,
    )
}

/// Shared GC-root-computation body used by both public entry points.
fn compute_gc_roots_from_keys(
    external_data_keys: impl Iterator<Item = Hash>,
    state_pointer: Option<Hash>,
    state: &OrchestrationState,
) -> BTreeSet<Hash> {
    let mut roots = BTreeSet::new();

    // All external-data keys (covers content_map per the invariant above).
    roots.extend(external_data_keys);

    // State pointer hash, if one exists.
    if let Some(hash) = state_pointer {
        roots.insert(hash);
    }

    // Input and output hashes from every tool-call instance.
    for instance in state.instances.values() {
        roots.extend(instance.outputs.values().map(|o| o.hash));
        roots.extend(instance.inputs.values().map(|i| i.hash));
    }

    // Instance blob CAS hashes (V2 envelope refs) — these are the hashes of
    // the encoded per-instance blobs stored in CAS, separate from the
    // instance I/O payload hashes already covered above.
    roots.extend(state.instance_blob_hashes.iter().copied());

    roots
}

/// Runs full CAS maintenance: optimize, prune constraints, GC sweep, and
/// index compaction — using `state.external_data` as GC roots.
///
/// This is the one-stop function called by the background GC loop (node
/// actor), the `RunGc` handler, and the CLI. It does **not** touch instance
/// GC — callers must run instance GC separately via
/// [`StateStoreClient::run_gc`].
///
/// # Decoupling invariant
///
/// This function owns only CAS maintenance operations. Callers are
/// responsible for:
/// - Instance GC (`state_store.run_gc(…)`)
/// - Providing the state pointer and state (whose `external_data` field
///   supplies root hashes)
/// - Error handling at the orchestration boundary
///
/// # Errors
///
/// Returns [`ConductorError::Cas`] when any maintenance operation fails.
pub async fn run_cas_gc_sweep<C>(
    cas: &C,
    state_pointer: Option<Hash>,
    state: &OrchestrationState,
) -> Result<GcSweepReport, ConductorError>
where
    C: CasApi + CasMaintenanceApi,
{
    tracing::info!("GC phase 1/5: optimize_once");
    cas.optimize_once(OptimizeOptions::default()).await.map_err(ConductorError::Cas)?;
    tracing::info!("GC phase 2/5: prune_constraints");
    cas.prune_constraints().await.map_err(ConductorError::Cas)?;
    let roots =
        compute_gc_roots_from_keys(state.external_data.keys().copied(), state_pointer, state);
    tracing::info!("GC phase 3/5: gc_sweep");
    let report = cas.gc_sweep(&roots).await.map_err(ConductorError::Cas)?;
    tracing::info!("GC phase 4/5: compact_index");
    cas.compact_index().await.map_err(ConductorError::Cas)?;
    tracing::info!("GC phase 5/5: complete (deleted={})", report.deleted_count);
    Ok(report)
}
