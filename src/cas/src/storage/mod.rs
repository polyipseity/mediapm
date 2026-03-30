//! CAS storage backends and configuration surface.

use std::collections::BTreeSet;

use crate::{CasError, Hash};

mod buffer_pool;
mod config;
mod filesystem;
mod in_memory;
mod visualization;

pub(crate) use buffer_pool::StreamBufferPool;

/// Canonical validation message for self-referential constraint rows.
pub(crate) const CONSTRAINT_SELF_BASE_MESSAGE: &str =
    "target hash cannot be its own base candidate";

/// Validates that a constraint row does not include its own target as base.
///
/// # Errors
/// Returns [`CasError::InvalidConstraint`] when `target_hash` appears in
/// `potential_bases`.
pub(crate) fn validate_constraint_target_not_in_bases(
    target_hash: Hash,
    potential_bases: &BTreeSet<Hash>,
) -> Result<(), CasError> {
    if potential_bases.contains(&target_hash) {
        return Err(CasError::invalid_constraint(CONSTRAINT_SELF_BASE_MESSAGE.to_string()));
    }

    Ok(())
}

/// Returns `true` when a constraint row is semantically unconstrained.
///
/// The current model treats an empty explicit base set as "no explicit row".
pub(crate) fn is_unconstrained_constraint_row(bases: &BTreeSet<Hash>) -> bool {
    bases.is_empty()
}

/// Normalizes explicit constraint rows by dropping unconstrained empty sets.
///
/// Returns `Some(bases)` when the set has at least one explicit candidate,
/// otherwise `None` to indicate implicit unconstrained semantics.
pub(crate) fn normalize_explicit_constraint_set(bases: BTreeSet<Hash>) -> Option<BTreeSet<Hash>> {
    (!is_unconstrained_constraint_row(&bases)).then_some(bases)
}

/// Storage backend configuration and runtime backend delegation.
pub use config::{
    CasBackendConfig, CasConfig, CasLocatorParseOptions, ConfiguredCas, FileSystemRecoveryOptions,
    IndexRecoveryMode,
};
/// Filesystem-backed persistent CAS implementation and observability metrics.
pub use filesystem::{FileSystemCas, FileSystemMetrics};
/// In-memory CAS implementation for tests and lightweight integration.
pub use in_memory::InMemoryCas;
/// Topology visualization data model and renderers.
pub use visualization::{
    CasTopologyConstraint, CasTopologyEncoding, CasTopologyNode, CasTopologySnapshot,
    render_topology_mermaid, render_topology_mermaid_neighborhood, topology_neighborhood_snapshot,
};
