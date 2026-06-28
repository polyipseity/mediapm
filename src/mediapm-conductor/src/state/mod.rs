//! Runtime orchestration state (volatile + CAS-persisted).
//!
//! The orchestration state tracks every tool-call instance, its persistence
//! status, and the auxiliary metadata needed for GC and diagnostics.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use crate::config::ImpureTimestamp;

pub mod versions;

/// Current orchestration-state schema version.
///
/// Must be bumped when the persisted JSON layout changes. Backward
/// compatibility is handled via the `state/versions/` module.
pub(crate) const STATE_VERSION: u32 = 2;

/// Persistence status for one output within a tool-call instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputSaveMode {
    /// Output has not been persisted to CAS.
    Unsaved,
    /// Output has been persisted to CAS.
    Saved,
    /// Output was persisted with full-data preference.
    Full,
}

/// Flags controlling output persistence behavior for one tool call instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistenceFlags {
    /// Whether outputs are saved to CAS (`true`) or kept only in volatile
    /// state (`false`).
    pub save: bool,
    /// Whether to force full data persistence instead of hash-only.
    #[serde(default)]
    pub force_full: bool,
}

/// Merges two `PersistenceFlags` into one: `save` uses AND, `force_full` uses OR.
#[must_use]
pub fn merge_persistence_flags(a: PersistenceFlags, b: PersistenceFlags) -> PersistenceFlags {
    PersistenceFlags { save: a.save && b.save, force_full: a.force_full || b.force_full }
}

/// A resolved input key-value pair for a tool-call instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedInput {
    /// Input key name.
    pub key: String,
    /// Resolved string value (may be a CAS hash reference or literal).
    pub value: String,
}

/// Reference to one persisted tool-step output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRef {
    /// Logical output name.
    pub name: String,
    /// CAS hash of the output content.
    pub hash: Hash,
    /// Persistence mode for this output.
    pub save_mode: OutputSaveMode,
}

/// A completed tool-call instance with its resolved inputs and outputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallInstance {
    /// Unique tool call instance key derived from tool id + resolved inputs + timestamp.
    pub instance_key: String,
    /// Tool identifier (`name@version`).
    pub tool_id: String,
    /// Resolved input values.
    pub inputs: Vec<ResolvedInput>,
    /// Produced output references.
    pub outputs: Vec<OutputRef>,
    /// Worker index that executed this tool call instance.
    pub worker_index: usize,
    /// Whether this tool call instance was executed (vs. cache-hit reuse).
    pub executed: bool,
    /// Whether this tool call instance was rematerialized from cache.
    pub rematerialized: bool,
    /// Conductor GC timestamp: refreshed to `aux.conductor_gc_epoch`
    /// whenever this instance is referenced during step execution.
    /// Used for grace-period comparisons during GC sweep.
    #[serde(default = "default_impure_timestamp_zero")]
    pub conductor_gc_last_referenced_at: ImpureTimestamp,
}

fn default_impure_timestamp_zero() -> ImpureTimestamp {
    ImpureTimestamp::default()
}

/// Auxiliary metadata attached to the orchestration state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuxData {
    /// Monotonic tool call instance counter.
    pub tool_call_instance_counter: u64,
    /// Conductor GC reference clock — updated to [`now()`] on every state
    /// commit. Used by `run_conductor_gc()` for grace-period comparisons
    /// and CAS blob reclamation. Distinct from CAS GC.
    pub conductor_gc_epoch: ImpureTimestamp,
}

/// Full orchestration state snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrchestrationState {
    /// Schema version marker.
    pub version: u32,
    /// Declared tool call instance store (`instance_key` → instance).
    pub tool_call_instances: BTreeMap<String, ToolCallInstance>,
    /// Auxiliary metadata.
    pub aux: AuxData,
}

impl OrchestrationState {
    /// Creates an empty initial state with the current version marker.
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            version: STATE_VERSION,
            tool_call_instances: BTreeMap::new(),
            aux: AuxData::default(),
        }
    }

    /// Runs conductor GC on instances: refreshes `conductor_gc_last_referenced_at`
    /// for referenced instances, evicts unreferenced instances past the TTL
    /// grace period, then updates `conductor_gc_epoch`.
    ///
    /// This is CONDUCTOR GC — prunes stale tool call instances and reclaims
    /// unreachable CAS blobs. Distinct from CAS GC which is a separate
    /// mechanism.
    pub fn run_conductor_gc(&mut self, referenced_keys: &BTreeSet<String>, ttl_seconds: u64) {
        let ttl = std::time::Duration::from_secs(ttl_seconds);
        let epoch = self.aux.conductor_gc_epoch;
        let epoch_nanos = epoch.as_unix_nanos();

        self.tool_call_instances.retain(|key, instance| {
            if referenced_keys.contains(key) {
                instance.conductor_gc_last_referenced_at = epoch;
                true
            } else {
                let last_ref = instance.conductor_gc_last_referenced_at.as_unix_nanos();
                // Evict if last reference was more than TTL ago
                let deadline = last_ref.saturating_add(ttl.as_nanos());
                deadline >= epoch_nanos
            }
        });
        self.aux.conductor_gc_epoch = ImpureTimestamp::now();
    }
}

impl Default for OrchestrationState {
    fn default() -> Self {
        Self::new_empty()
    }
}
