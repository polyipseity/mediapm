//! Runtime orchestration-state model.
//!
//! Runtime structs in this module are version-agnostic. Persisted representation
//! is handled by `versions/` modules and bridged through fp-library optics.

use std::collections::BTreeMap;

use mediapm_cas::Hash;
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
    pub plain_content: Vec<u8>,
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
    pub fn from_plain_content(plain_content: Vec<u8>) -> Self {
        Self {
            hash: Hash::from_content(plain_content.as_slice()),
            plain_content,
            string_list: None,
        }
    }

    /// Builds one runtime input from an existing CAS hash.
    ///
    /// Persisted state decoding typically uses this constructor because
    /// snapshots record only hash identities.
    #[must_use]
    pub fn from_hash(hash: Hash) -> Self {
        Self { hash, plain_content: Vec::new(), string_list: None }
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
        let plain_content = serde_json::to_vec(&string_list)
            .map_err(|err| ConductorError::Serialization(err.to_string()))?;
        Ok(Self {
            hash: Hash::from_content(plain_content.as_slice()),
            plain_content,
            string_list: Some(string_list),
        })
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
    pub inputs: BTreeMap<String, ResolvedInput>,
    /// Captured output CAS refs and effective persistence policies.
    pub outputs: BTreeMap<String, OutputRef>,
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
}

impl Default for OrchestrationState {
    fn default() -> Self {
        Self { version: versions::latest_state_version(), instances: BTreeMap::new() }
    }
}

/// Converts runtime orchestration state into persisted wire-envelope JSON.
///
/// This helper centralizes projection into persistence shape so callers can
/// inspect or serialize state exactly as it is stored:
/// - top-level explicit numeric `version`,
/// - deterministic `instances` table,
/// - builtin metadata normalized to identity-only
///   (`kind`/`name`/`version`),
/// - resolved inputs persisted as hash identities only.
///
/// The runtime state is cloned to preserve ownership expectations for callers
/// that still need the original value after serialization.
///
/// # Errors
///
/// Returns an error when state envelope encoding fails or when the encoded
/// payload cannot be parsed back into JSON.
pub fn persisted_state_json_value(
    state: &OrchestrationState,
) -> Result<serde_json::Value, ConductorError> {
    let encoded = encode_state(state.clone())?;
    serde_json::from_slice(&encoded).map_err(|err| ConductorError::Serialization(err.to_string()))
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

/// Encodes orchestration state with latest persistence version envelope.
///
/// # Errors
///
/// Returns an error when the runtime state cannot be converted into or
/// serialized as the latest persistence envelope.
pub fn encode_state(state: OrchestrationState) -> Result<Vec<u8>, ConductorError> {
    versions::encode_state(state)
}

/// Decodes orchestration state from versioned persistence bytes.
///
/// # Errors
///
/// Returns an error when version dispatch, migration, or envelope
/// deserialization fails.
pub fn decode_state(bytes: &[u8]) -> Result<OrchestrationState, ConductorError> {
    versions::decode_state(bytes)
}
