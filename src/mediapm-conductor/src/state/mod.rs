//! Runtime orchestration state (volatile + CAS-persisted).
//!
//! The orchestration state tracks every tool-call instance, the captured
//! outputs each instance recorded together with the capture spec that produced
//! them, and the auxiliary metadata needed for GC and diagnostics.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use mediapm_utils::Timestamp;
use serde::{Deserialize, Serialize};

pub mod versions;

/// Current orchestration-state schema version.
///
/// Bumped when the persisted JSON layout changes in a way older or newer
/// readers cannot absorb. An additive, serde-defaulted provenance field is
/// not such a change: an older file decodes with an empty value and a newer
/// file still decodes in an older reader, so the version stays put. That
/// forward readability holds only because no state wire type denies unknown
/// fields; a wire type that gained `#[serde(deny_unknown_fields)]` would end
/// the carve-out.
///
/// Readability is all the carve-out buys, so it is not round-trip safety: a
/// reader that predates `step_ids` decodes it as an unknown field, drops it
/// when it next writes the document, and leaves a state file whose schema
/// version still says this value while its provenance is gone. A build that
/// may rewrite this document therefore has to know every field it carries,
/// and a change an older writer would lose that way is what a bump is for.
/// Backward compatibility is handled via the `state/versions/` module.
pub(crate) const STATE_VERSION: u32 = 3;

/// Persistence policy for a CAS-referenced blob declared as external data.
///
/// Captured outputs carry their persistence decision on the capture spec
/// recorded beside them, so this mode speaks only for blobs the config names
/// directly: it says how a hash that is not part of any tool content map is
/// retained and persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputSaveMode {
    /// The blob is not persisted to CAS.
    Unsaved,
    /// The blob is persisted to CAS.
    Saved,
    /// The blob is persisted with full-data preference.
    Full,
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

/// One captured output: its content hash and the spec that produced it.
///
/// The spec travels with the bytes because an output is addressed by name, and
/// the name alone does not say what was captured: `content` may be a file read
/// or a folder archive, strict or tolerant of emptiness, and a reusing step
/// must be able to tell whether the bytes it finds were produced the way it
/// would produce them. `None` is the migration's "recorded before specs were
/// stored" sentinel. No declared [`crate::config::OutputCaptureSpec`] equals
/// `None`, so an entry in that state never satisfies a capture request that
/// names a spec — the step-worker probe compares each required name's recorded
/// spec and treats `None` as a miss — and the step re-executes and re-records
/// it once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedOutput {
    /// CAS content hash of the captured bytes.
    pub hash: Hash,
    /// The capture spec that produced this entry, or `None` when the record
    /// predates spec storage.
    pub capture: Option<crate::config::OutputCaptureSpec>,
}

/// A completed tool-call instance: the runtime artifact of one tool call.
///
/// Records the effective command argv, execution environment, materialized
/// inputs, and captured outputs. Values are content-addressed (never stored
/// literally); only deterministic records participate in the instance key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallInstance {
    /// Content-addressed instance key (see
    /// [`crate::instance_key::derive_instance_key`]).
    pub instance_key: Hash,
    /// Tool call identifier: the unified tools-catalog map key (e.g.
    /// `"ffmpeg@v1"`).
    pub tool_call_id: String,
    /// Whether the tool is impure.
    ///
    /// Stored as provenance: it says how the tool was classified when the
    /// instance was recorded. Nothing reads it to decide behaviour — the retry
    /// gate consults the tool spec's `is_impure`, not this field — and it never
    /// alters the instance key, because a key derives from content alone.
    pub impure: bool,
    /// Execution timestamp recorded at run time. Provenance only; it is never
    /// part of any instance key, impure or pure.
    pub executed_at: Timestamp,
    /// Steps that have served this instance, by step id.
    ///
    /// Provenance only, never key material. It names the step that recorded
    /// the instance and every later step that reused it, which is what lets
    /// the demo fixture be re-keyed without re-deriving a historical key
    /// scheme: regeneration pairs an instance with the step that claims it.
    /// A cache hit records the reusing step here, and an id the set already
    /// holds is left as it is, so a steady-state sync persists the same
    /// record.
    #[serde(default)]
    pub step_ids: BTreeSet<String>,
    /// Effective command argv recorded at execution time, in order. Values
    /// are content-addressed; deterministic records participate in the key.
    pub command_args: Vec<HashedValueRecord>,
    /// Execution environment variables (minimal set: non-empty config values
    /// only). Values are hashed, never stored literally.
    pub env_vars: BTreeMap<String, HashedValueRecord>,
    /// Materialized tool-content inputs keyed by sandbox-relative path.
    pub materialized_inputs: BTreeMap<String, HashedValueRecord>,
    /// Captured output records keyed by output name, each carrying the spec
    /// that produced it.
    pub outputs: BTreeMap<String, CapturedOutput>,
}

/// Per-instance auxiliary metadata, kept off the instance record so cache
/// probes and key derivation stay lean.
///
/// Persistence is not here: a save mode belongs to the capture spec recorded
/// with the entry it describes, not to the instance, so replacing one output
/// never speaks for another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InstanceAux {
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
pub struct ConductorState {
    /// Schema version marker.
    pub version: u32,
    /// Declared tool call instance store (`instance_key` → instance).
    pub tool_call_instances: BTreeMap<Hash, ToolCallInstance>,
    /// Auxiliary metadata.
    pub aux: AuxData,
}

impl ConductorState {
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

impl Default for ConductorState {
    fn default() -> Self {
        Self::new_empty()
    }
}

/// Re-derives a tool-call instance key from the same components recorded at
/// execution time.
///
/// Builtin tools store resolved deterministic input hashes in the key rather
/// than `command_args`, so mediapm materialization uses this helper to match
/// persisted instances. Every step's key derives from content alone: the
/// recorded `executed_at` timestamp never participates, so an impure step's
/// stored instance keeps matching across runs until its key material changes
/// (or an explicit `force_rerun` bypasses the cache for one run).
///
/// Captures are not parameters: an instance's captured outputs are records
/// addressed by name, and a step reads the names it binds out of the instance
/// it found. Two steps running the same work therefore share this key however
/// differently they declare what they record, which is what lets one execution
/// serve both.
#[must_use]
pub fn derive_tool_call_instance_key(
    tool_call_id: &str,
    deterministic_input_hashes: &[Hash],
    deterministic_materialized_input_hashes: &[Hash],
) -> Hash {
    crate::instance_key::derive_instance_key(
        tool_call_id,
        deterministic_input_hashes,
        deterministic_materialized_input_hashes,
    )
}
