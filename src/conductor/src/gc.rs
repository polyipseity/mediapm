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

use mediapm_cas::{CasApi, CasMaintenanceApi, Hash};

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
    let mut roots = BTreeSet::new();

    // All external-data keys (covers content_map per the invariant above).
    roots.extend(user_external_data.keys().copied());
    roots.extend(machine_external_data.keys().copied());

    // State pointer hash, if one exists.
    if let Some(hash) = state_pointer {
        roots.insert(hash);
    }

    // Input and output hashes from every tool-call instance.
    for instance in state.instances.values() {
        roots.extend(instance.outputs.values().map(|o| o.hash));
        roots.extend(instance.inputs.values().map(|i| i.hash));
    }

    roots
}

/// Computes GC roots from a unified `external_data` map (merged user+machine).
///
/// Has the same semantics as [`compute_gc_roots`] but takes a single already-
/// merged map instead of separate user and machine maps. This is the variant
/// used by the background GC loop which works from the coordinator's
/// accumulated `external_data` set.
#[must_use]
pub fn compute_gc_roots_from_unified(
    external_data: &BTreeMap<Hash, ExternalContentRef>,
    state_pointer: Option<Hash>,
    state: &OrchestrationState,
) -> BTreeSet<Hash> {
    let mut roots = BTreeSet::new();

    // All external-data keys (covers content_map per the invariant above).
    roots.extend(external_data.keys().copied());

    // State pointer hash, if one exists.
    if let Some(hash) = state_pointer {
        roots.insert(hash);
    }

    // Input and output hashes from every tool-call instance.
    for instance in state.instances.values() {
        roots.extend(instance.outputs.values().map(|o| o.hash));
        roots.extend(instance.inputs.values().map(|i| i.hash));
    }

    roots
}

/// Runs a CAS sweep + index compaction cycle using unified `external_data` as
/// GC roots.
///
/// This is the decoupled function called by the background GC loop (node
/// actor) and the CLI. It does **not** touch instance GC — callers must run
/// instance GC separately via [`StateStoreClient::run_gc`].
///
/// # Decoupling invariant
///
/// This function owns only the CAS sweep + compact concern. Callers are
/// responsible for:
/// - Instance GC (`state_store.run_gc(…)`)
/// - Providing the current `external_data` set, state pointer, and state
/// - Error handling at the orchestration boundary
///
/// # Errors
///
/// Returns [`ConductorError::Cas`] when sweep or compaction fails.
pub async fn run_cas_gc_sweep<C>(
    cas: &C,
    external_data: &BTreeMap<Hash, ExternalContentRef>,
    state_pointer: Option<Hash>,
    state: &OrchestrationState,
) -> Result<(), ConductorError>
where
    C: CasApi + CasMaintenanceApi,
{
    let roots = compute_gc_roots_from_unified(external_data, state_pointer, state);
    cas.gc_sweep(&roots).await.map_err(ConductorError::Cas)?;
    cas.compact_index().await.map_err(ConductorError::Cas)?;
    Ok(())
}
