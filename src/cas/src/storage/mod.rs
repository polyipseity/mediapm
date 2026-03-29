//! CAS storage backends and configuration surface.

use std::collections::BTreeSet;

use crate::{CasError, Hash};

mod buffer_pool;
mod config;
mod filesystem;
mod in_memory;
mod visualization;

pub(crate) use buffer_pool::StreamBufferPool;

pub(crate) const CONSTRAINT_SELF_BASE_MESSAGE: &str =
    "target hash cannot be its own base candidate";

pub(crate) fn validate_constraint_target_not_in_bases(
    target_hash: Hash,
    potential_bases: &BTreeSet<Hash>,
) -> Result<(), CasError> {
    if potential_bases.contains(&target_hash) {
        return Err(CasError::invalid_constraint(CONSTRAINT_SELF_BASE_MESSAGE.to_string()));
    }

    Ok(())
}

pub(crate) fn is_unconstrained_constraint_row(bases: &BTreeSet<Hash>) -> bool {
    bases.is_empty()
}

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
