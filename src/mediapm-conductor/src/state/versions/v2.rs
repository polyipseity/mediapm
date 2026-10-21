//! V2 wire format for orchestration state persistence.
//!
//! V2 is the inline JSON-based format (no CAS envelope). The entire state
//! is serialized as a single JSON object with a `version` marker.
//!
//! This module owns the V2 version marker, wire-format types, the
//! content-addressed instance-key derivation, and the V1→V2 migration
//! function (migration from vX to vX+1 always belongs to the vX+1 module).
//! It must not import unversioned runtime state from `super::super`.
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

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::{CasApi, Hash};
use serde::{Deserialize, Serialize};

use super::v1;
use crate::error::ConductorError;

/// V2 schema version marker.
pub(crate) const CONDUCTOR_STATE_VERSION_V2: u32 = 2;

/// Returns whether `marker` matches V2.
#[must_use]
pub(crate) const fn is_conductor_state_version_v2(marker: u32) -> bool {
    marker == CONDUCTOR_STATE_VERSION_V2
}

/// Domain separator for tool-call-instance keys (v2 scheme, third revision).
///
/// Bumping this string invalidates every previously derived instance key.
/// The revisions advance in place while the state schema stays at v2 (the
/// wire fields persist as provenance), so the string carries the key-scheme
/// revision rather than the schema version. Old persisted keys miss once
/// and re-execute one time after the bump.
///
/// - `.1` removed wall-clock time from impure keys.
/// - `.2` added the step's capture set, so two steps that run the same
///   command but record different files no longer share a key.
const INSTANCE_KEY_DOMAIN_V2: &str = "mediapm/conductor/tool call instances/v2.2";

// ---------------------------------------------------------------------------
// V2 wire format types
// ---------------------------------------------------------------------------
// These mirror the runtime `ConductorState` struct but live in the
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

/// V2 impure timestamp (nanoseconds since Unix epoch, matching runtime
/// [`mediapm_utils::Timestamp`] wire repr).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImpureTimestampV2(
    /// Nanoseconds since Unix epoch.
    pub u64,
);

/// V2 content-addressed value record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HashedValueRecordV2 {
    /// CAS content hash of the recorded value.
    pub hash: Hash,
    /// Whether the value is deterministic (participates in instance keys).
    pub deterministic: bool,
}

/// V2 tool-call instance.
///
/// Records the runtime artifact of one tool call: effective command argv,
/// execution environment, materialized inputs, and captured outputs. Values
/// are content-addressed (never stored literally); only deterministic
/// records participate in the instance key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ToolCallInstanceV2 {
    /// Content-addressed instance key (see [`derive_instance_key_v2`]).
    pub instance_key: Hash,
    /// Tool call identifier: the unified tools-catalog map key (e.g.
    /// `"ffmpeg@v1"`).
    pub tool_call_id: String,
    /// Whether the tool is impure.
    ///
    /// Impure steps depend on externals not captured in their inputs. The
    /// flag records that fact (it drives `retry_impure` retry semantics and
    /// `last_impure_sync_at` provenance) but never alters the instance key:
    /// time never invalidates an instance. Re-execution follows changed key
    /// material or the run-scoped explicit `force_rerun` invalidation.
    pub impure: bool,
    /// Execution timestamp recorded at run time. Provenance only; never part
    /// of any instance key.
    pub executed_at: ImpureTimestampV2,
    /// Steps that recorded this instance, by step id.
    ///
    /// Provenance only: it is never hashed into the instance key, and
    /// `#[serde(default)]` keeps state written before the field existed
    /// readable. The demo fixture's key regeneration pairs through this set
    /// instead of re-deriving historical key schemes.
    #[serde(default)]
    pub step_ids: BTreeSet<String>,
    /// Effective command argv recorded at execution time, in order.
    pub command_args: Vec<HashedValueRecordV2>,
    /// Execution environment variables (minimal set: non-empty config values
    /// only). Values are hashed, never stored literally.
    pub env_vars: BTreeMap<String, HashedValueRecordV2>,
    /// Materialized tool-content inputs keyed by sandbox-relative path.
    pub materialized_inputs: BTreeMap<String, HashedValueRecordV2>,
    /// Captured output records keyed by output name.
    pub outputs: BTreeMap<String, HashedValueRecordV2>,
}

/// V2 per-instance auxiliary metadata (save modes + GC lifecycle).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InstanceAuxV2 {
    /// Per-output persistence modes keyed by output name.
    pub save_modes: BTreeMap<String, OutputSaveModeV2>,
    /// Conductor GC last-reference clock.
    pub last_referenced_at: ImpureTimestampV2,
}

impl Default for InstanceAuxV2 {
    fn default() -> Self {
        Self { save_modes: BTreeMap::new(), last_referenced_at: ImpureTimestampV2(0) }
    }
}

/// V2 auxiliary metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AuxDataV2 {
    pub tool_call_instance_counter: u64,
    pub conductor_gc_epoch: ImpureTimestampV2,
    /// Per-instance aux metadata keyed by instance key.
    pub instances: BTreeMap<Hash, InstanceAuxV2>,
}

impl Default for AuxDataV2 {
    fn default() -> Self {
        Self {
            tool_call_instance_counter: 0,
            conductor_gc_epoch: ImpureTimestampV2(0),
            instances: BTreeMap::new(),
        }
    }
}

/// V2 inline orchestration state (plain JSON, no CAS envelope).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ConductorStateV2 {
    pub version: u32,
    pub tool_call_instances: BTreeMap<Hash, ToolCallInstanceV2>,
    pub aux: AuxDataV2,
}

// ---------------------------------------------------------------------------
// Instance-key derivation (v2 scheme)
// ---------------------------------------------------------------------------

/// Derives a content-addressed tool-call-instance key.
///
/// The key is the composite hash of, in order:
/// 1. the versioned domain separator,
/// 2. the tool call identifier (unified tools-catalog map key),
/// 3. the step's capture set (see [`crate::step_output_specs_hash`]),
/// 4. deterministic command args in order,
/// 5. deterministic environment records sorted by name,
/// 6. deterministic materialized inputs sorted by path.
///
/// Every step derives its key from content alone — time never participates,
/// pure and impure alike (`impure_keys_ignore_time`; the sync-level pin is
/// `an_immediate_second_sync_executes_nothing_and_rematerializes_nothing`).
/// The `executed_at` timestamp stays on the instance as provenance; an
/// impure step re-executes only when its key material changes or the run
/// sets the explicit `force_rerun` invalidation.
///
/// Non-deterministic records are stored on the instance but excluded here so
/// an env-dependent run still re-executes instead of colliding on stale
/// content. Two steps that differ in neither command nor captures still
/// deduplicate onto one key.
#[must_use]
pub(crate) fn derive_instance_key_v2(
    tool_call_id: &str,
    output_specs_hash: Hash,
    deterministic_command_args: &[Hash],
    deterministic_env_vars: &[Hash],
    deterministic_materialized_inputs: &[Hash],
) -> Hash {
    let mut parts = Vec::with_capacity(
        3 + deterministic_command_args.len()
            + deterministic_env_vars.len()
            + deterministic_materialized_inputs.len(),
    );
    parts.push(Hash::from_content(INSTANCE_KEY_DOMAIN_V2.as_bytes()));
    parts.push(Hash::from_content(tool_call_id.as_bytes()));
    parts.push(output_specs_hash);
    parts.extend_from_slice(deterministic_command_args);
    parts.extend_from_slice(deterministic_env_vars);
    parts.extend_from_slice(deterministic_materialized_inputs);
    Hash::composite(&parts)
}

// ---------------------------------------------------------------------------
// V1 → V2 migration
// ---------------------------------------------------------------------------

// Migration from vX to vX+1 lives in the vX+1 module.  This function
// converts a CAS-backed V1 envelope into a V2 inline state by fetching
// each instance blob from CAS and assembling the flat JSON payload.

/// Legacy V2 resolved input key-value pair (pre-redesign shape).
///
/// Accept-and-discard: the redesigned V2 instance carries no resolved-input
/// record, so a legacy blob must parse with these fields present and then
/// drop them. [`legacy_instance_into_v2`] never reads them, which is what the
/// migration's documented one-way guarantee means.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyResolvedInputV2 {
    key: String,
    value: String,
}

/// Legacy V2 output reference (pre-redesign shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyOutputRefV2 {
    name: String,
    hash: Hash,
    save_mode: OutputSaveModeV2,
}

/// Legacy V2 tool-call instance wire shape (pre-redesign).
///
/// Old stored v2 instances used string keys and carried inputs/outputs as
/// vectors with per-output save modes and an inline GC clock. Only used to
/// decode V1-envelope instance blobs during migration; the redesign
/// converts these records into the Hash-keyed shape with aux metadata.
///
/// ## Accept-and-discard fields
///
/// `inputs` (and through it `LegacyResolvedInputV2`'s `key`/`value`),
/// `worker_index`, `executed`, and `rematerialized` are parsed so a legacy
/// CAS blob still deserializes, and then dropped. The V2 redesign replaced
/// them with values a legacy blob cannot supply: `materialized_inputs` and
/// `env_vars` (empty), `executed_at` (zero), output materialized flags
/// (rebuilt from the legacy per-output save modes), and `step_ids` (empty — a
/// legacy blob records no step provenance).
/// [`legacy_instance_into_v2`] is where that substitution happens, and it is
/// the documented one-way guarantee of this migration — nothing the new schema
/// can use is lost, and nothing is read back out of these fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyToolCallInstanceV2 {
    instance_key: String,
    tool_id: String,
    inputs: Vec<LegacyResolvedInputV2>,
    outputs: Vec<LegacyOutputRefV2>,
    worker_index: usize,
    executed: bool,
    rematerialized: bool,
    /// Conductor GC last-referenced-at clock.
    #[serde(default)]
    conductor_gc_last_referenced_at: ImpureTimestampV2,
}

/// Converts a legacy (pre-redesign) V2 instance into the new shape.
///
/// The legacy string key is parsed as a content hash (plain hex, or
/// re-derived from the string if unparseable), outputs convert into
/// content-addressed records (determinism unknown → treated as
/// deterministic), and per-output save modes plus the GC clock move into
/// per-instance aux metadata. Arity and env/materialized-input records do
/// not exist in the legacy format.
fn legacy_instance_into_v2(
    legacy: LegacyToolCallInstanceV2,
) -> (Hash, ToolCallInstanceV2, InstanceAuxV2) {
    let instance_key = legacy
        .instance_key
        .parse::<Hash>()
        .unwrap_or_else(|_| Hash::from_content(legacy.instance_key.as_bytes()));
    let mut outputs = BTreeMap::new();
    let mut save_modes = BTreeMap::new();
    for output in legacy.outputs {
        save_modes.insert(output.name.clone(), output.save_mode);
        outputs.insert(output.name, HashedValueRecordV2 { hash: output.hash, deterministic: true });
    }
    let instance = ToolCallInstanceV2 {
        instance_key,
        tool_call_id: legacy.tool_id,
        impure: false,
        executed_at: ImpureTimestampV2(0),
        step_ids: BTreeSet::new(),
        command_args: Vec::new(),
        env_vars: BTreeMap::new(),
        materialized_inputs: BTreeMap::new(),
        outputs,
    };
    let aux =
        InstanceAuxV2 { save_modes, last_referenced_at: legacy.conductor_gc_last_referenced_at };
    (instance_key, instance, aux)
}

/// Migrates a V1 CAS-backed envelope into V2 inline representation.
///
/// Each instance reference in the V1 envelope is resolved through CAS,
/// deserialized as a legacy V2 instance, and collected into a flat
/// `ConductorStateV2` with the V2 version marker. Instances convert into
/// the redesigned Hash-keyed shape; the aux counter and GC epoch carry over
/// from the envelope.
pub(crate) async fn migrate_v1_to_v2<C: CasApi>(
    cas: &C,
    envelope: v1::OrchestrationStateEnvelopeV1,
) -> Result<ConductorStateV2, ConductorError> {
    let mut instances = BTreeMap::new();
    let mut instance_aux = BTreeMap::new();
    for (_key, instance_ref) in envelope.instances {
        let instance_bytes = cas.get(instance_ref.hash).await?;
        let legacy: LegacyToolCallInstanceV2 = serde_json::from_slice(&instance_bytes)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        let (instance_key, instance, aux) = legacy_instance_into_v2(legacy);
        instances.insert(instance_key, instance);
        instance_aux.insert(instance_key, aux);
    }

    Ok(ConductorStateV2 {
        version: CONDUCTOR_STATE_VERSION_V2,
        tool_call_instances: instances,
        aux: AuxDataV2 {
            tool_call_instance_counter: envelope.aux.tool_call_instance_counter,
            conductor_gc_epoch: ImpureTimestampV2(envelope.aux.conductor_gc_epoch.as_unix_nanos()),
            instances: instance_aux,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mediapm_utils::Timestamp;

    /// Stands in for a capture-set hash in the derivation tests.
    ///
    /// The value only has to be stable: what these tests isolate is the
    /// composite around the capture term, and the term's own encoding is
    /// pinned by `instance_key::tests` where the encoder lives.
    fn captures() -> Hash {
        Hash::from_content(b"capture-set")
    }

    /// Deriving twice with identical inputs yields identical keys.
    #[test]
    fn derive_instance_key_v2_is_deterministic() {
        let captures = captures();
        let a = derive_instance_key_v2("echo@v1", captures, &[], &[], &[]);
        let b = derive_instance_key_v2("echo@v1", captures, &[], &[], &[]);
        assert_eq!(a, b);
    }

    /// Time never participates in any instance key, impure or pure.
    ///
    /// Earlier key schemes embedded `executed_at` for impure steps, which
    /// made every run mint a fresh instance and re-execute the step. The
    /// timestamp parameters are gone from the signature, so time-independence
    /// is compile-enforced at this level; this test pins the contract through
    /// the public `derive_tool_call_instance_key` wrapper (the API the
    /// mediapm materializer matches through) and names the behavioral pin:
    /// `an_immediate_second_sync_executes_nothing_and_rematerializes_nothing`.
    #[test]
    fn impure_keys_ignore_time() {
        let args = [Hash::from_content(b"arg")];
        let captures = captures();
        let first = crate::state::derive_tool_call_instance_key("tool", captures, &args, &[], &[]);
        let second = crate::state::derive_tool_call_instance_key("tool", captures, &args, &[], &[]);
        assert_eq!(
            first, second,
            "identical material must derive identical keys regardless of wall-clock time",
        );
    }

    /// The versioned domain prefix participates in the key.
    #[test]
    fn derive_instance_key_v2_domain_prefix() {
        let captures = captures();
        let with_domain = derive_instance_key_v2("tool", captures, &[], &[], &[]);
        let without_domain = Hash::composite(&[Hash::from_content(b"tool")]);
        assert_ne!(with_domain, without_domain);
    }

    /// Arg order matters; deterministic env/input hashes participate.
    #[test]
    fn derive_instance_key_v2_args_env_inputs_ordered() {
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        let captures = captures();
        assert_ne!(
            derive_instance_key_v2("t", captures, &[a, b], &[], &[]),
            derive_instance_key_v2("t", captures, &[b, a], &[], &[]),
            "command arg order must matter",
        );
        assert_ne!(
            derive_instance_key_v2("t", captures, &[], &[], &[]),
            derive_instance_key_v2("t", captures, &[], &[a], &[]),
            "deterministic env records must participate",
        );
        assert_ne!(
            derive_instance_key_v2("t", captures, &[], &[], &[]),
            derive_instance_key_v2("t", captures, &[], &[], &[b]),
            "deterministic materialized inputs must participate",
        );
    }

    /// The key is exactly `composite(domain, id, captures, args, env, inputs)`.
    ///
    /// Env is excluded in practice because the executor always passes an empty
    /// env slice, and no time component exists at all (the pre-revision scheme
    /// embedded `executed_at` for impure steps). The capture term is included
    /// because an instance claims the outputs it captured. Pinning the exact
    /// component list guards all of it: adding a component, dropping the
    /// capture term, keying env, or reintroducing a time term breaks this test.
    #[test]
    fn derive_instance_key_v2_composite_components_exact() {
        let a = Hash::from_content(b"arg");
        let captures = captures();
        let key = derive_instance_key_v2("t", captures, &[a], &[], &[]);
        let expected = Hash::composite(&[
            Hash::from_content(INSTANCE_KEY_DOMAIN_V2.as_bytes()),
            Hash::from_content(b"t"),
            captures,
            a,
        ]);
        assert_eq!(key, expected);
    }

    /// A state file written before `step_ids` existed still loads, and the
    /// field is provenance rather than required data.
    #[test]
    fn recorded_instance_without_step_ids_decodes_with_an_empty_set() {
        let json = r#"{
            "instance_key": "blake3:0000000000000000000000000000000000000000000000000000000000000001",
            "tool_call_id": "echo",
            "impure": false,
            "executed_at": 0,
            "command_args": [],
            "env_vars": {},
            "materialized_inputs": {},
            "outputs": {}
        }"#;
        let instance: ToolCallInstanceV2 =
            serde_json::from_str(json).expect("legacy instance decodes");
        assert!(instance.step_ids.is_empty());
    }

    /// R7: a v1 envelope migrates into the redesigned v2 state, including
    /// per-instance aux metadata.
    #[tokio::test]
    async fn migrate_v1_to_v2_converts_legacy_instances() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let legacy = LegacyToolCallInstanceV2 {
            instance_key: Hash::from_content(b"legacy-key").to_hex(),
            tool_id: "echo@v1".to_string(),
            inputs: Vec::new(),
            outputs: vec![LegacyOutputRefV2 {
                name: "stdout".to_string(),
                hash: Hash::from_content(b"out"),
                save_mode: OutputSaveModeV2::Saved,
            }],
            worker_index: 0,
            executed: true,
            rematerialized: false,
            conductor_gc_last_referenced_at: ImpureTimestampV2(42),
        };
        let blob = serde_json::to_vec(&legacy).unwrap();
        let hash = cas.put(bytes::Bytes::from(blob)).await.unwrap();
        let envelope = v1::OrchestrationStateEnvelopeV1 {
            version: v1::ORCHESTRATION_STATE_VERSION_V1,
            instances: BTreeMap::from([("legacy-key".to_string(), v1::InstanceRefV1 { hash })]),
            aux: crate::state::AuxData {
                tool_call_instance_counter: 7,
                conductor_gc_epoch: Timestamp::from_unix_nanos(99),
                instances: BTreeMap::new(),
            },
        };
        let state = migrate_v1_to_v2(&cas, envelope).await.unwrap();
        assert_eq!(state.version, CONDUCTOR_STATE_VERSION_V2);
        assert_eq!(state.tool_call_instances.len(), 1);
        let (key, instance) = state.tool_call_instances.iter().next().unwrap();
        assert_eq!(instance.tool_call_id, "echo@v1");
        assert_eq!(instance.outputs.len(), 1);
        assert_eq!(instance.outputs["stdout"].hash, Hash::from_content(b"out"));
        let aux = &state.aux.instances[key];
        assert_eq!(aux.save_modes["stdout"], OutputSaveModeV2::Saved);
        assert_eq!(aux.last_referenced_at.0, 42);
        assert_eq!(state.aux.tool_call_instance_counter, 7);
        assert_eq!(state.aux.conductor_gc_epoch.0, 99);
    }
}
