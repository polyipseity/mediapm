//! V2 wire format for orchestration state persistence.
//!
//! V2 is the inline JSON-based format (no CAS envelope). The entire state
//! is serialized as a single JSON object with a `version` marker.
//!
//! This module owns the V2 version marker, wire-format types, and the
//! V1→V2 migration function (migration from vX to vX+1 always belongs to
//! the vX+1 module). It must not import unversioned runtime state from
//! `super::super`.

use std::collections::BTreeMap;

use mediapm_cas::{CasApi, Hash};
use serde::{Deserialize, Serialize};

use super::v1;
use crate::error::ConductorError;

/// V2 schema version marker.
pub(crate) const ORCHESTRATION_STATE_VERSION_V2: u32 = 2;

/// Returns whether `marker` matches V2.
#[must_use]
pub(crate) const fn is_orchestration_state_version_v2(marker: u32) -> bool {
    marker == ORCHESTRATION_STATE_VERSION_V2
}

// ---------------------------------------------------------------------------
// V2 wire format types
// ---------------------------------------------------------------------------
// These mirror the runtime `OrchestrationState` struct but live in the
// version module so the version boundary is explicit.  The `mod.rs` bridge
// converts between V2 wire types and the unversioned runtime representation.

/// V2 persistence save-mode (serde round-trips identically to runtime
/// [`super::super::OutputSaveMode`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum OutputSaveModeV2 {
    #[serde(rename = "Unsaved")]
    Unsaved,
    #[serde(rename = "Saved")]
    Saved,
    #[serde(rename = "Full")]
    Full,
}

/// V2 resolved input key-value pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResolvedInputV2 {
    pub key: String,
    pub value: String,
}

/// V2 output reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OutputRefV2 {
    pub name: String,
    pub hash: Hash,
    pub save_mode: OutputSaveModeV2,
}

/// V2 tool-call instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ToolCallInstanceV2 {
    pub instance_key: String,
    pub tool_id: String,
    pub inputs: Vec<ResolvedInputV2>,
    pub outputs: Vec<OutputRefV2>,
    pub worker_index: usize,
    pub executed: bool,
    pub rematerialized: bool,
    /// Conductor GC last-referenced-at clock.
    #[serde(default)]
    pub conductor_gc_last_referenced_at: ImpureTimestampV2,
}

/// V2 impure timestamp (nanoseconds since Unix epoch, matching runtime
/// [`ImpureTimestamp`](crate::config::ImpureTimestamp) wire repr).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImpureTimestampV2(
    /// Nanoseconds since Unix epoch.
    pub u64,
);

/// V2 auxiliary metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AuxDataV2 {
    pub tool_call_instance_counter: u64,
    pub conductor_gc_epoch: ImpureTimestampV2,
}

impl Default for AuxDataV2 {
    fn default() -> Self {
        Self { tool_call_instance_counter: 0, conductor_gc_epoch: ImpureTimestampV2(0) }
    }
}

/// V2 inline orchestration state (plain JSON, no CAS envelope).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct OrchestrationStateV2 {
    pub version: u32,
    pub tool_call_instances: BTreeMap<String, ToolCallInstanceV2>,
    pub aux: AuxDataV2,
}

// ---------------------------------------------------------------------------
// V1 → V2 migration
// ---------------------------------------------------------------------------
// Migration from vX to vX+1 lives in the vX+1 module.  This function
// converts a CAS-backed V1 envelope into a V2 inline state by fetching
// each instance blob from CAS and assembling the flat JSON payload.

/// Migrates a V1 CAS-backed envelope into V2 inline representation.
///
/// Each instance reference in the V1 envelope is resolved through CAS,
/// deserialized as a V2 instance, and collected into a flat
/// `OrchestrationStateV2` with the V2 version marker.
#[allow(clippy::cast_possible_truncation)]
pub(crate) async fn migrate_v1_to_v2<C: CasApi>(
    cas: &C,
    envelope: v1::OrchestrationStateEnvelopeV1,
) -> Result<OrchestrationStateV2, ConductorError> {
    let mut instances = BTreeMap::new();
    for (key, instance_ref) in envelope.instances {
        let instance_bytes = cas.get(instance_ref.hash).await?;
        let instance: ToolCallInstanceV2 = serde_json::from_slice(&instance_bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        instances.insert(key, instance);
    }

    Ok(OrchestrationStateV2 {
        version: ORCHESTRATION_STATE_VERSION_V2,
        tool_call_instances: instances,
        aux: AuxDataV2 {
            tool_call_instance_counter: envelope.aux.tool_call_instance_counter,
            conductor_gc_epoch: ImpureTimestampV2(
                envelope.aux.conductor_gc_epoch.as_unix_nanos() as u64
            ),
        },
    })
}
