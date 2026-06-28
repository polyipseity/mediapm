//! V1 wire format for orchestration state persistence.
//!
//! This module owns the V1 envelope and instance-ref types. It must not import
//! unversioned runtime state from `super::super`.

use std::collections::BTreeMap;

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use crate::state::AuxData;

/// V1 schema version marker.
pub(crate) const ORCHESTRATION_STATE_VERSION_V1: u32 = 1;

/// Returns whether `marker` matches V1.
#[must_use]
#[expect(dead_code)]
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
