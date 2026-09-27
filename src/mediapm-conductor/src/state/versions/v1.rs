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
//! ## `aux: AuxData` is the historical V1 shape
//!
//! This envelope types `aux` as [`AuxData`] rather than a version-local
//! `AuxDataV1`, and that is confirmed correct, not a pending deviation:
//! `AuxData` as it is typed today *is* the shape a V1 envelope's aux block
//! always had. V2 introduced the version-local `AuxDataV2` because V2 really
//! did change the wire shape; V1 predates that change, so the runtime type
//! and the V1 wire shape coincide.
//!
//! Re-typing this field into a version-local copy would be wrong on both
//! counts: it would duplicate a type that has no V2-era variant, and it would
//! invite a future edit of [`AuxData`] to silently rewrite the V1 wire format.

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
