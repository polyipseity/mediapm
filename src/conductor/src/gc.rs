//! GC root computation for CAS sweep operations.
//!
//! The entry point [`compute_gc_roots`] collects all CAS hashes referenced by
//! current conductor configuration and state so a downstream sweep can
//! distinguish live objects from unreachable blobs.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;

use crate::model::config::ExternalContentRef;
use crate::model::state::OrchestrationState;

/// Computes the full set of protected CAS hashes for GC root computation.
///
/// # Invariant
///
/// `content_map ⊆ external_data` is enforced at decode time by
/// `vet_latest_envelope()`, so this function iterates only `external_data`
/// keys (plus the state pointer and instance I/O) — content_map iteration
/// is redundant and omitted.
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
