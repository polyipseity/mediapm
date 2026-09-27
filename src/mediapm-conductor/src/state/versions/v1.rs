//! V1 wire format for orchestration state persistence.
//!
//! This module owns the V1 envelope and instance-ref types.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`
//!   beyond the resolved runtime model it is declared to bridge.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.
//! - Files outside `versions/` must reach versioned symbols only through
//!   `versions/mod.rs`, never through a direct `versions::vX` path.
//!
//! ## Known deviation: `aux: AuxData`
//!
//! This envelope types `aux` as the *current* unversioned [`AuxData`] rather
//! than a version-local `AuxDataV1`. That is a boundary violation, and it is
//! deliberate until the V1-era aux shape is known: the tree does not record
//! what a V1 envelope's aux block looked like before the Hash-keyed instance
//! redesign, and guessing a shape would risk making a real V1 document
//! unreadable. Correcting it needs the historical shape, not a decision.
//! Until then, do not "fix" this by mirroring today's `AuxData` into a local
//! struct: that would freeze today's shape under a version name and make the
//! drift permanent instead of visible.

use std::collections::BTreeMap;

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use crate::state::AuxData;

/// V1 schema version marker.
pub(crate) const ORCHESTRATION_STATE_VERSION_V1: u32 = 1;

/// Returns whether `marker` matches V1.
#[must_use]
pub(crate) const fn is_orchestration_state_version_v1(marker: u32) -> bool {
    marker == ORCHESTRATION_STATE_VERSION_V1
}

/// V1 instance reference (hash-only, no inline data).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InstanceRefV1 {
    /// CAS hash of the serialized instance.
    pub hash: Hash,
}

/// V1 orchestration state envelope stored in CAS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct OrchestrationStateEnvelopeV1 {
    /// Schema version marker.
    pub version: u32,
    /// Instance store (key → CAS hash reference).
    pub instances: BTreeMap<String, InstanceRefV1>,
    /// Auxiliary metadata.
    pub aux: AuxData,
}
