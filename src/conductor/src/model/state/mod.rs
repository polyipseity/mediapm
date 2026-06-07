//! Runtime orchestration-state model.
//!
//! Runtime structs in this module are version-agnostic. Persisted representation
//! is handled by `versions/` modules and bridged through fp-library optics.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use mediapm_cas::{CasApi, Hash};
use serde::{Deserialize, Serialize};

use crate::error::ConductorError;
use crate::model::config::{ImpureTimestamp, ToolSpec};

pub(crate) mod versions;

/// Effective persistence mode for one captured output.
///
/// Ordering is intentional and semantic:
/// - `Unsaved < Saved < Full`.
///
/// This allows merge behavior to be expressed as a simple maximum across
/// equivalent callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum OutputSaveMode {
    /// Output can be dropped after execution when no equivalent caller requires
    /// persistence.
    Unsaved,
    /// Output is persisted with regular incremental behavior.
    #[default]
    Saved,
    /// Output is persisted and treated as full-data preferred.
    Full,
}

impl OutputSaveMode {
    /// Returns whether this mode keeps output bytes persisted.
    #[must_use]
    pub const fn should_persist(self) -> bool {
        !matches!(self, Self::Unsaved)
    }

    /// Returns whether this mode requires full-data persistence hints.
    #[must_use]
    pub const fn prefers_full(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// User/machine merged persistence flags for one output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceFlags {
    /// Effective tri-state persistence mode.
    pub save: OutputSaveMode,
}

impl Default for PersistenceFlags {
    fn default() -> Self {
        Self { save: OutputSaveMode::Saved }
    }
}

/// Merges persistence flags from multiple equivalent tool-call references.
///
/// Invariants:
/// - merge uses maximum ordering over `OutputSaveMode` where
///   `Unsaved < Saved < Full`.
#[must_use]
pub fn merge_persistence_flags(
    flags: impl IntoIterator<Item = PersistenceFlags>,
) -> PersistenceFlags {
    let mut merged = OutputSaveMode::Unsaved;
    let mut seen = false;

    for flag in flags {
        merged = merged.max(flag.save);
        seen = true;
    }

    if seen { PersistenceFlags { save: merged } } else { PersistenceFlags::default() }
}

/// Fully resolved input vector item used in deterministic instance keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedInput {
    /// CAS hash identity for this resolved input payload.
    pub hash: Hash,
    /// Plain content consumed by the current in-memory invocation.
    ///
    /// This field is runtime-only execution cache and is intentionally omitted
    /// from persisted orchestration-state snapshots.
    #[serde(default, skip_serializing, skip_deserializing)]
    pub plain_content: Bytes,
    /// Optional list-of-strings runtime payload.
    ///
    /// This field is runtime-only and omitted from persisted state snapshots.
    /// It is populated for executable inputs declared with
    /// `ToolInputKind::StringList` so command rendering can expand standalone
    /// unpack tokens into multiple argv entries.
    #[serde(default, skip_serializing, skip_deserializing)]
    pub string_list: Option<Vec<String>>,
}

impl ResolvedInput {
    /// Builds one runtime input from in-memory bytes.
    ///
    /// This helper computes the deterministic hash identity directly from the
    /// provided content and is intended for tests and transient runtime values.
    #[must_use]
    pub fn from_plain_content(plain_content: Bytes) -> Self {
        Self { hash: Hash::from_content(plain_content.as_ref()), plain_content, string_list: None }
    }

    /// Builds one runtime input from an existing CAS hash.
    ///
    /// Persisted state decoding typically uses this constructor because
    /// snapshots record only hash identities.
    #[must_use]
    pub fn from_hash(hash: Hash) -> Self {
        Self { hash, plain_content: Bytes::new(), string_list: None }
    }

    /// Builds one runtime list input from ordered string values.
    ///
    /// Hash identity is derived from canonical JSON encoding of the full list.
    ///
    /// # Errors
    ///
    /// Returns an error when the ordered list cannot be serialized into its
    /// canonical JSON byte representation.
    pub fn from_string_list(string_list: Vec<String>) -> Result<Self, ConductorError> {
        let plain_content_vec = serde_json::to_vec(&string_list)
            .map_err(|err| ConductorError::Serialization(err.to_string()))?;
        Ok(Self {
            hash: Hash::from_content(plain_content_vec.as_slice()),
            plain_content: Bytes::from(plain_content_vec),
            string_list: Some(string_list),
        })
    }
}

/// Hash-only reference to a resolved step input.
///
/// Intentionally excludes `plain_content` and `string_list` — this type is
/// used inside `ToolCallInstance` so that runtime content bytes cannot
/// accidentally be retained across the state boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedInputKey {
    /// CAS hash identity for this resolved input payload.
    pub hash: Hash,
}

impl From<ResolvedInput> for ResolvedInputKey {
    fn from(input: ResolvedInput) -> Self {
        Self { hash: input.hash }
    }
}

/// Output map entry for an executed instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRef {
    /// CAS hash for this output value.
    ///
    /// When `allow_empty_capture` is `true`, this hash is the blake3 hash of
    /// zero bytes, representing an empty capture rather than real content.
    pub hash: Hash,
    /// Effective merged persistence policy for this output.
    ///
    /// This value is persisted after deduplicating equivalent tool-call
    /// instances and combining duplicate caller output policies via
    /// [`merge_persistence_flags`].
    pub persistence: PersistenceFlags,
    /// Whether this output was captured as intentionally empty via the
    /// tool output spec's `allow_empty = true` policy.
    ///
    /// When `true`, the hash is the empty-bytes hash and the output must not
    /// be used as a step input. Downstream steps that reference this output
    /// receive a workflow error at input-resolution time.
    ///
    /// Defaults to `false` for backward-compatibility with persisted state
    /// written before this field was introduced.
    #[serde(default)]
    pub allow_empty_capture: bool,
}

/// State record for one deterministic tool-call instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallInstance {
    /// Immutable tool map key used by the workflow step.
    pub tool_name: String,
    /// Metadata used to identify tool code and behavior.
    ///
    /// Persistence semantics:
    /// - executable metadata remains shape-compatible with reusable config
    ///   `ToolSpec`,
    /// - builtin metadata is normalized to identity-only
    ///   (`kind`/`name`/`version`) when persisted.
    ///
    /// Runtime still uses `ToolSpec` for ergonomic internal handling.
    pub metadata: ToolSpec,
    /// Optional machine-managed timestamp for impure calls.
    ///
    /// Stored outside `metadata` so metadata remains byte-identical to tool
    /// config declarations.
    #[serde(default)]
    pub impure_timestamp: Option<ImpureTimestamp>,
    /// Resolved inputs participating in cache identity.
    ///
    /// Uses [`ResolvedInputKey`] (hash-only) so that runtime content bytes
    /// cannot accidentally be retained across the state boundary. Full
    /// [`ResolvedInput`] values live only in the hot execution path
    /// (`step_worker`).
    pub inputs: BTreeMap<String, ResolvedInputKey>,
    /// Captured output CAS refs and effective persistence policies.
    pub outputs: BTreeMap<String, OutputRef>,
}

/// Runtime auxiliary metadata for one tool-call instance.
///
/// `last_unreachable` is guaranteed non-null at the type level — the decode
/// path injects `now()` for any instance that lacks a value, and all
/// runtime construction paths provide a value directly. This eliminates
/// `None`-checking from GC and other runtime logic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuxData {
    /// When this instance was determined unreachable (start of its TTL clock
    /// for GC eviction). Set at creation time or when GC first notices an
    /// unreferenced instance without an aux entry.
    pub last_unreachable: ImpureTimestamp,
}

/// Immutable orchestration-state value stored as a CAS blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrchestrationState {
    /// Explicit runtime schema version marker.
    ///
    /// This mirrors the persisted envelope marker so tooling can perform
    /// explicit optics/migration orchestration against in-memory snapshots.
    pub version: u32,
    /// Deterministic instance table keyed by derived instance key.
    #[serde(default)]
    pub instances: BTreeMap<String, ToolCallInstance>,
    /// Envelope-level auxiliary metadata keyed by instance key.
    #[serde(default)]
    pub aux: BTreeMap<String, AuxData>,
    /// Instance keys referenced by the current planning pass.
    ///
    /// Populated during planning by the coordinator. Instances in this set
    /// are never evicted by GC regardless of `last_unreachable`.
    #[serde(default, skip_serializing, skip_deserializing)]
    pub referenced_instance_keys: HashSet<String>,
}

impl Default for OrchestrationState {
    fn default() -> Self {
        Self {
            version: versions::latest_state_version(),
            instances: BTreeMap::new(),
            aux: BTreeMap::new(),
            referenced_instance_keys: HashSet::new(),
        }
    }
}

impl OrchestrationState {
    /// Garbage-collects instances not referenced by the current planning pass.
    ///
    /// ## Semantics
    ///
    /// 1. **Reachability is primary**: instances in `referenced_instance_keys`
    ///    are never evicted regardless of their `last_unreachable` value.
    /// 2. **Unreferenced instances** without `last_unreachable` get it initialised
    ///    to `now` (TTL clock starts when GC first notices them).
    /// 3. **Unreferenced instances** whose `last_unreachable < cutoff` are evicted,
    ///    along with their `aux` metadata.
    /// 4. **Unreferenced instances** with `last_unreachable >= cutoff` (or `None`
    ///    before this pass runs) are preserved until a subsequent pass.
    pub fn gc_instances(&mut self, cutoff: ImpureTimestamp) {
        let now = ImpureTimestamp::now();
        // Phase 1: initialise aux for unreferenced instances that lack an
        // entry, so the TTL clock starts ticking from this GC pass onward.
        for key in self.instances.keys() {
            if self.referenced_instance_keys.contains(key) {
                continue;
            }
            self.aux.entry(key.clone()).or_insert(AuxData { last_unreachable: now });
        }
        // Phase 2: evict unreferenced instances past the cutoff.
        let evict_keys: Vec<String> = self
            .instances
            .keys()
            .filter(|key| {
                if self.referenced_instance_keys.contains(*key) {
                    return false;
                }
                self.aux.get(*key).is_some_and(|a| a.last_unreachable < cutoff)
            })
            .cloned()
            .collect();
        for key in &evict_keys {
            self.instances.remove(key);
            self.aux.remove(key);
        }
    }
}

/// Converts runtime orchestration state into V2 wire-envelope JSON without CAS.
///
/// Uses V2 types directly via optics to serialize inline instance data.
/// This is the non-CAS path used by CLI export (state files with inline
/// instance payloads).
///
/// # Errors
///
/// Returns an error when V2 type conversion or JSON serialization fails.
pub fn persisted_state_json_value(
    state: &OrchestrationState,
) -> Result<serde_json::Value, ConductorError> {
    let mut instances_json = BTreeMap::new();
    for (key, instance) in &state.instances {
        let v2_instance = versions::v2::tool_call_instance_v2_iso().to(instance.clone());
        let instance_value = serde_json::to_value(&v2_instance)
            .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        instances_json.insert(key.clone(), instance_value);
    }
    let aux_json = state
        .aux
        .iter()
        .map(|(key, aux)| {
            let v2_aux = versions::v2::aux_data_v2_iso().to(aux.clone());
            let value = serde_json::to_value(&v2_aux)
                .map_err(|e| ConductorError::Serialization(e.to_string()))?;
            Ok((key.clone(), value))
        })
        .collect::<Result<BTreeMap<_, _>, ConductorError>>()?;
    Ok(serde_json::json!({
        "version": versions::latest_state_version(),
        "instances": instances_json,
        "aux": aux_json,
    }))
}

/// Parses a V2 inline orchestration-state JSON slice into a runtime state.
///
/// The input must be in the V2 inline format (instances serialized as full
/// `ToolCallInstanceV2` values, not CAS refs). This is the non-CAS file
/// format produced by [`persisted_state_json_value`].
///
/// # Errors
///
/// Returns an error when JSON deserialization or V2 instance conversion
/// fails.
pub fn decode_state_from_slice(bytes: &[u8]) -> Result<OrchestrationState, ConductorError> {
    let json: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| ConductorError::Serialization(e.to_string()))?;
    let instances_json =
        json.get("instances").and_then(serde_json::Value::as_object).ok_or_else(|| {
            ConductorError::Serialization("missing 'instances' field in state JSON".to_string())
        })?;
    let mut instances = BTreeMap::new();
    for (key, instance_value) in instances_json {
        let v2_instance: versions::v2::ToolCallInstanceV2 =
            serde_json::from_value(instance_value.clone())
                .map_err(|e| ConductorError::Serialization(e.to_string()))?;
        let runtime_instance = versions::v2::tool_call_instance_v2_iso().from(v2_instance);
        instances.insert(key.clone(), runtime_instance);
    }
    let version = json
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(versions::latest_state_version() as u64) as u32;
    let mut aux = json
        .get("aux")
        .and_then(serde_json::Value::as_object)
        .map(|aux_obj| {
            aux_obj
                .iter()
                .map(|(key, aux_value)| {
                    let v2_aux: versions::v2::AuxDataV2 = serde_json::from_value(aux_value.clone())
                        .map_err(|e| ConductorError::Serialization(e.to_string()))?;
                    let runtime_aux = versions::v2::aux_data_v2_iso().from(v2_aux);
                    Ok((key.clone(), runtime_aux))
                })
                .collect::<Result<BTreeMap<_, _>, ConductorError>>()
        })
        .transpose()?
        .unwrap_or_default();

    // Ensure every instance has an aux entry. The bridge above injects
    // now() for any last_unreachable: None in the input, so only completely
    // missing entries need handling here.
    let now = ImpureTimestamp::now();
    for key in instances.keys() {
        aux.entry(key.clone()).or_insert(AuxData { last_unreachable: now });
    }

    Ok(OrchestrationState { version, instances, aux, referenced_instance_keys: HashSet::new() })
}

/// Renders runtime orchestration state as pretty persisted wire-envelope JSON.
///
/// This is equivalent to `serde_json::to_string_pretty` over
/// [`persisted_state_json_value`].
///
/// # Errors
///
/// Returns an error when persisted-state projection fails or when pretty JSON
/// serialization fails.
pub fn persisted_state_json_pretty(state: &OrchestrationState) -> Result<String, ConductorError> {
    let json = persisted_state_json_value(state)?;
    serde_json::to_string_pretty(&json)
        .map_err(|err| ConductorError::Serialization(err.to_string()))
}

/// Encodes orchestration state using V2 CAS-backed persistence.
///
/// Each instance is individually CAS-stored; the envelope hash is returned.
///
/// # Errors
///
/// Returns an error when instance encoding, CAS put, or envelope
/// serialization fails.
pub async fn encode_state<C: CasApi>(
    cas: &C,
    state: OrchestrationState,
) -> Result<Hash, ConductorError> {
    versions::encode_state(cas, state).await
}

/// Decodes orchestration state from a CAS-backed V2 envelope pointer.
///
/// Reads envelope and individual instance blobs from CAS.
///
/// # Errors
///
/// Returns an error when CAS get, envelope deserialization, or instance
/// decode fails.
pub async fn decode_state<C: CasApi>(
    cas: &C,
    pointer: Hash,
) -> Result<OrchestrationState, ConductorError> {
    versions::decode_state(cas, pointer).await
}
