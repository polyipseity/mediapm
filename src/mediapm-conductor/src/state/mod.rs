//! Runtime orchestration state (volatile + CAS-persisted).
//!
//! The orchestration state tracks every tool-call instance, its persistence
//! status, and the auxiliary metadata needed for GC and diagnostics.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use mediapm_utils::Timestamp;
use serde::{Deserialize, Serialize};

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

/// A content-addressed value with its determinism classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashedValueRecord {
    /// CAS content hash of the recorded value.
    pub hash: Hash,
    /// Whether the value is deterministic (participates in instance keys).
    pub deterministic: bool,
}

/// A completed tool-call instance: the runtime artifact of one tool call.
///
/// Records the effective command argv, execution environment, materialized
/// inputs, and captured outputs. Values are content-addressed (never stored
/// literally); only deterministic records participate in the instance key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallInstance {
    /// Content-addressed instance key (see `state::versions::derive_instance_key_v2`).
    pub instance_key: Hash,
    /// Tool call identifier: the unified tools-catalog map key (e.g.
    /// `"ffmpeg@v1"`).
    pub tool_call_id: String,
    /// Whether the tool is impure (impure instances embed `executed_at` in
    /// the key, so every run creates a fresh entry).
    pub impure: bool,
    /// Execution timestamp recorded at run time. Never part of pure keys.
    pub executed_at: Timestamp,
    /// Effective command argv recorded at execution time, in order. Values
    /// are content-addressed; deterministic records participate in the key.
    pub command_args: Vec<HashedValueRecord>,
    /// Execution environment variables (minimal set: non-empty config values
    /// only). Values are hashed, never stored literally.
    pub env_vars: BTreeMap<String, HashedValueRecord>,
    /// Materialized tool-content inputs keyed by sandbox-relative path.
    pub materialized_inputs: BTreeMap<String, HashedValueRecord>,
    /// Captured output records keyed by output name.
    pub outputs: BTreeMap<String, HashedValueRecord>,
}

/// Per-instance auxiliary metadata (save modes + GC lifecycle), kept off the
/// instance record so cache probes and key derivation stay lean.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InstanceAux {
    /// Per-output persistence modes keyed by output name.
    pub save_modes: BTreeMap<String, OutputSaveMode>,
    /// Conductor GC last-reference clock: refreshed whenever the instance is
    /// referenced during step execution or retained by GC.
    pub last_referenced_at: Timestamp,
}

/// Auxiliary metadata attached to the orchestration state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuxData {
    /// Monotonic tool call instance counter.
    pub tool_call_instance_counter: u64,
    /// Conductor GC reference clock — updated to [`Timestamp::now()`] on every
    /// state commit. Used by `run_conductor_gc()` for grace-period comparisons
    /// and CAS blob reclamation. Distinct from CAS GC.
    pub conductor_gc_epoch: Timestamp,
    /// Per-instance aux metadata keyed by instance key.
    pub instances: BTreeMap<Hash, InstanceAux>,
}

/// Full orchestration state snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrchestrationState {
    /// Schema version marker.
    pub version: u32,
    /// Declared tool call instance store (`instance_key` → instance).
    pub tool_call_instances: BTreeMap<Hash, ToolCallInstance>,
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

    /// Runs conductor GC on instances: refreshes
    /// `aux.instances[key].last_referenced_at` for referenced instances,
    /// evicts unreferenced instances past the TTL grace period, then updates
    /// `conductor_gc_epoch`.
    ///
    /// This is CONDUCTOR GC — prunes stale tool call instances and reclaims
    /// unreachable CAS blobs. Distinct from CAS GC which is a separate
    /// mechanism.
    pub fn run_conductor_gc(&mut self, referenced_keys: &BTreeSet<Hash>, ttl_seconds: u64) {
        let ttl_nanos = ttl_seconds.saturating_mul(1_000_000_000);
        let epoch = self.aux.conductor_gc_epoch;
        let epoch_nanos = epoch.as_unix_nanos();
        let aux = &mut self.aux;

        self.tool_call_instances.retain(|key, _instance| {
            if referenced_keys.contains(key) {
                // Referenced instances survive and get their last-reference
                // clock refreshed to the current GC epoch.
                if let Some(entry) = aux.instances.get_mut(key) {
                    entry.last_referenced_at = epoch;
                }
                true
            } else {
                let last_ref = aux
                    .instances
                    .get(key)
                    .map_or(0, |entry| entry.last_referenced_at.as_unix_nanos());
                // Evict if last reference was more than TTL ago.
                let deadline = last_ref.saturating_add(ttl_nanos);
                deadline >= epoch_nanos
            }
        });
        // Drop aux entries for evicted instances so the map never grows stale
        // keys.
        let live: BTreeSet<Hash> = self.tool_call_instances.keys().copied().collect();
        aux.instances.retain(|key, _| live.contains(key));
        aux.conductor_gc_epoch = Timestamp::now();
    }
}

impl Default for OrchestrationState {
    fn default() -> Self {
        Self::new_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_persistence_flags_save_requires_both() {
        assert_eq!(
            merge_persistence_flags(
                PersistenceFlags { save: true, force_full: false },
                PersistenceFlags { save: true, force_full: false },
            ),
            PersistenceFlags { save: true, force_full: false },
        );
        assert_eq!(
            merge_persistence_flags(
                PersistenceFlags { save: false, force_full: false },
                PersistenceFlags { save: true, force_full: false },
            ),
            PersistenceFlags { save: false, force_full: false },
        );
    }

    #[test]
    fn merge_persistence_flags_force_full_uses_or() {
        assert_eq!(
            merge_persistence_flags(
                PersistenceFlags { save: false, force_full: false },
                PersistenceFlags { save: false, force_full: false },
            ),
            PersistenceFlags { save: false, force_full: false },
        );
        assert_eq!(
            merge_persistence_flags(
                PersistenceFlags { save: false, force_full: true },
                PersistenceFlags { save: false, force_full: false },
            ),
            PersistenceFlags { save: false, force_full: true },
        );
    }

    #[test]
    fn merge_persistence_flags_default_merge() {
        let flags =
            merge_persistence_flags(PersistenceFlags::default(), PersistenceFlags::default());
        assert!(!flags.save);
        assert!(!flags.force_full);
    }
}
