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
//! ## `aux` is a version-local copy of the historical V1 shape
//!
//! This envelope types `aux` as [`AuxDataV1`]. It used to reuse the runtime
//! `AuxData` on the argument that the two coincided, and that argument has now
//! failed in exactly the way it predicted: the runtime block dropped the
//! instance counter and the per-instance clock map, so reusing it would have
//! silently redefined the V1 wire format and refused to read the files it was
//! meant to migrate. A reader for an old format keeps its own copy of that
//! format's shape; the runtime type is free to move.
//!
//! Every field of [`AuxDataV1`] decodes from the JSON the old writers emitted,
//! because the timestamp newtype serializes as bare Unix nanoseconds, which is
//! what those files held.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use mediapm_utils::Timestamp;
use serde::{Deserialize, Serialize};

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

/// V1 per-instance auxiliary metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct InstanceAuxV1 {
    /// Conductor GC last-reference clock, in Unix nanoseconds.
    #[serde(default)]
    pub last_referenced_at: Timestamp,
}

/// V1 auxiliary metadata block.
///
/// The counter and the per-instance clock map are carried even though the V1→V2
/// migration reads only the counter: they are what the format held, and a reader
/// that dropped them would decode a V1 document into a different document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct AuxDataV1 {
    /// Monotonic tool call instance counter.
    #[serde(default)]
    pub tool_call_instance_counter: u64,
    /// Conductor GC reference clock, in Unix nanoseconds.
    #[serde(default)]
    pub conductor_gc_epoch: Timestamp,
    /// Per-instance aux metadata keyed by instance key.
    #[serde(default)]
    pub instances: BTreeMap<Hash, InstanceAuxV1>,
    /// Instance keys each workflow's latest successful run produced or hit.
    #[serde(default)]
    pub live_instances: BTreeMap<String, BTreeSet<Hash>>,
}

/// V1 orchestration state envelope stored in CAS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct OrchestrationStateEnvelopeV1 {
    /// Schema version marker.
    pub version: u32,
    /// Instance store (key → CAS hash reference).
    pub instances: BTreeMap<String, InstanceRefV1>,
    /// Auxiliary metadata, in the shape V1 wrote it.
    pub aux: AuxDataV1,
}
