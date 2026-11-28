//! V7 wire format for orchestration state persistence.
//!
//! V7 keeps the V6 record layout but marks key-participating digests as
//! conditionally keyed: `command_args`, `hash_ck`, `step_instances` values, and
//! `tool_call_instances` keys may be either ordinary CAS hashes or secret-keyed
//! digests. `_nk` fields and `materialized_inputs` remain unkeyed CAS hashes.
//! The new `instance_key_secret_verifier` is explicit `null` for migrated
//! records.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.
//! - Files outside `versions/` must reach versioned symbols only through
//!   `versions/mod.rs`, never through a direct `versions::vX` path.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use mediapm_cas::Hash;
use serde::{Deserialize, Deserializer, Serialize};

use super::v6;
use crate::keyed_hash::{KEYED_HASH_PREFIX, KeyedHash};

/// V7 schema version marker.
pub(crate) const CONDUCTOR_STATE_VERSION_V7: u32 = 7;

/// Returns whether `marker` matches V7.
#[must_use]
pub(crate) const fn is_conductor_state_version_v7(marker: u32) -> bool {
    marker == CONDUCTOR_STATE_VERSION_V7
}

/// V7 timestamp (nanoseconds since Unix epoch).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct WireTimestampV7(
    /// Nanoseconds since Unix epoch.
    pub u64,
);

/// V7 env var provenance tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EnvVarSourceV7 {
    /// The document fixed the value as a non-empty literal.
    Literal,
    /// The host supplied the value (or the document left it empty/inherited).
    Inherited,
}

/// V7 digest that may be keyed or unkeyed, serialized in canonical text form.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub(crate) struct DerivedHashV7(
    /// Canonical wire text: a `blake3:` or `blake3-keyed:` digest.
    String,
);

impl DerivedHashV7 {
    /// Wraps an ordinary CAS hash as an unkeyed digest.
    #[must_use]
    pub(crate) fn from_hash(hash: Hash) -> Self {
        Self(hash.to_string())
    }

    /// Wraps a secret-keyed digest in its wire form.
    #[must_use]
    pub(crate) fn from_keyed(hash: KeyedHash) -> Self {
        Self(hash.to_string())
    }

    /// Returns the canonical wire text of this digest.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for DerivedHashV7 {
    type Err = String;

    /// Parses either digest mode, using the keyed prefix to disambiguate.
    ///
    /// # Errors
    ///
    /// Returns the underlying parse error for foreign prefixes or malformed
    /// digests.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.starts_with(KEYED_HASH_PREFIX) {
            let keyed = value.parse::<KeyedHash>()?;
            Ok(Self(keyed.to_string()))
        } else {
            let unkeyed = value.parse::<Hash>().map_err(|err| err.to_string())?;
            Ok(Self(unkeyed.to_string()))
        }
    }
}

impl<'de> Deserialize<'de> for DerivedHashV7 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// Non-secret verifier for the instance key secret, or an explicit `null`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WireKeyedHashV7(#[serde(with = "wire_keyed_hash")] String);

mod wire_keyed_hash {
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::keyed_hash::KeyedHash;

    /// Writes the verifier's canonical keyed-digest text.
    pub(super) fn serialize<S: Serializer>(value: &str, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(value)
    }

    /// Reads and validates the verifier's keyed-digest text.
    ///
    /// # Errors
    ///
    /// Returns the `KeyedHash` parse error for malformed or unkeyed text.
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<String, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse::<KeyedHash>().map_err(serde::de::Error::custom)?;
        Ok(raw)
    }
}

impl WireKeyedHashV7 {
    /// Wraps a verifier digest in its wire form.
    #[must_use]
    pub(crate) fn from_keyed(value: KeyedHash) -> Self {
        Self(value.to_string())
    }

    /// Parses the validated wire text back into a keyed digest.
    ///
    /// # Errors
    ///
    /// Returns the `KeyedHash` parse error if the text no longer parses.
    pub(crate) fn keyed_hash(&self) -> Result<KeyedHash, String> {
        self.0.parse()
    }
}

/// V7 env var record: conditionally keyed hash and optional provenance tag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EnvVarEntryV7 {
    /// Hash of the env var value (key material for literal entries).
    pub hash_ck: DerivedHashV7,
    /// Provenance of the value, or `null` for migrated records with unknown
    /// provenance.
    pub source_nk: Option<EnvVarSourceV7>,
}

/// V7 captured output: capture token, nullable unkeyed hash, retention fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapturedOutputV7 {
    /// Capture token/kind string (for example `file:downloads/out.mkv`,
    /// `folder_flat:downloads/x`).
    pub capture: String,
    /// CAS content hash, or `null` when an allowed-empty capture matched
    /// nothing.
    pub hash_nk: Option<Hash>,
    /// Whether the bytes are retained as a saved fact.
    pub saved_nk: bool,
}

/// V7 step reference, qualified by workflow.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StepRefV7 {
    /// Workflow the step belongs to.
    #[serde(default)]
    pub workflow_id: String,
    /// Step id inside that workflow.
    pub step_id: String,
}

/// V7 tool-call instance record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolCallInstanceV7 {
    /// Version whose derivation produced the stored key.
    pub key_version: u32,
    /// Highest key version attempted for migration checks.
    pub attempted_key_version_nk: u32,
    /// Logical tool-call identifier used for instance key derivation.
    pub tool_call_id: String,
    /// Effective command argv / key material digests in order.
    pub command_args: Vec<DerivedHashV7>,
    /// Execution environment variables, hashed, with optional provenance.
    pub env_vars_ck: BTreeMap<String, EnvVarEntryV7>,
    /// Inputs staged into the sandbox, keyed by destination (never keyed).
    pub materialized_inputs: BTreeMap<String, Hash>,
    /// Captured outputs keyed by output name.
    pub outputs: BTreeMap<String, CapturedOutputV7>,
    /// Whether the tool is impure (never key material).
    pub impure_nk: bool,
    /// When the instance was invalidated, or explicit `null`.
    pub invalidated_at_nk: Option<WireTimestampV7>,
    /// Execution timestamp (never key material).
    pub executed_at_nk: WireTimestampV7,
    /// GC last-reference timestamp (never key material).
    pub last_referenced_at_nk: WireTimestampV7,
    /// Steps that have served this instance (never key material).
    #[serde(default)]
    pub step_ids_nk: BTreeSet<StepRefV7>,
}

/// V7 orchestration state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConductorStateV7 {
    /// Schema version marker.
    pub version: u32,
    /// Conductor GC reference clock (nanoseconds since Unix epoch).
    pub conductor_gc_ran_at: u64,
    /// Workflow -> step -> instance digest map: sole GC seed and lookup map.
    #[serde(default)]
    pub step_instances: BTreeMap<String, BTreeMap<String, DerivedHashV7>>,
    /// Recorded tool-call instances keyed by instance digest.
    pub tool_call_instances: BTreeMap<DerivedHashV7, ToolCallInstanceV7>,
    /// Non-secret verifier for the instance key secret, or explicit `null`.
    pub instance_key_secret_verifier: Option<WireKeyedHashV7>,
}

/// Migrates a V6 state into the V7 wire shape without introducing a secret.
pub(crate) fn from_v6_into_v7(state: v6::ConductorStateV6) -> ConductorStateV7 {
    let step_instances = state
        .step_instances
        .into_iter()
        .map(|(workflow, steps)| {
            let steps = steps
                .into_iter()
                .map(|(step, key)| (step, DerivedHashV7::from_hash(key)))
                .collect();
            (workflow, steps)
        })
        .collect();
    let tool_call_instances = state
        .tool_call_instances
        .into_iter()
        .map(|(key, instance)| (DerivedHashV7::from_hash(key), instance_into_v7(instance)))
        .collect();
    ConductorStateV7 {
        version: CONDUCTOR_STATE_VERSION_V7,
        conductor_gc_ran_at: state.conductor_gc_ran_at,
        step_instances,
        tool_call_instances,
        instance_key_secret_verifier: None,
    }
}

/// Converts one V6 instance into V7, keeping all unkeyed facts unchanged.
fn instance_into_v7(instance: v6::ToolCallInstanceV6) -> ToolCallInstanceV7 {
    let command_args = instance.command_args.into_iter().map(DerivedHashV7::from_hash).collect();
    let env_vars_ck = instance
        .env_vars_ck
        .into_iter()
        .map(|(name, entry)| {
            (
                name,
                EnvVarEntryV7 {
                    hash_ck: DerivedHashV7::from_hash(entry.hash_ck),
                    source_nk: entry.source_nk.map(Into::into),
                },
            )
        })
        .collect();
    let outputs = instance
        .outputs
        .into_iter()
        .map(|(name, output)| {
            (
                name,
                CapturedOutputV7 {
                    capture: output.capture,
                    hash_nk: output.hash_nk,
                    saved_nk: output.saved_nk,
                },
            )
        })
        .collect();
    let step_ids_nk = instance
        .step_ids_nk
        .into_iter()
        .map(|step| StepRefV7 { workflow_id: step.workflow_id, step_id: step.step_id })
        .collect();

    ToolCallInstanceV7 {
        key_version: instance.key_version,
        attempted_key_version_nk: instance.attempted_key_version_nk,
        tool_call_id: instance.tool_call_id,
        command_args,
        env_vars_ck,
        materialized_inputs: instance.materialized_inputs,
        outputs,
        impure_nk: instance.impure_nk,
        invalidated_at_nk: instance.invalidated_at_nk.map(|stamp| WireTimestampV7(stamp.0)),
        executed_at_nk: WireTimestampV7(instance.executed_at_nk.0),
        last_referenced_at_nk: WireTimestampV7(instance.last_referenced_at_nk.0),
        step_ids_nk,
    }
}

impl From<v6::EnvVarSourceV6> for EnvVarSourceV7 {
    fn from(source: v6::EnvVarSourceV6) -> Self {
        match source {
            v6::EnvVarSourceV6::Literal => Self::Literal,
            v6::EnvVarSourceV6::Inherited => Self::Inherited,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_maps_key_material_and_keeps_unkeyed_facts() {
        let key = Hash::from_content(b"key");
        let state = v6::ConductorStateV6 {
            version: v6::CONDUCTOR_STATE_VERSION_V6,
            conductor_gc_ran_at: 9,
            step_instances: BTreeMap::from([(
                "w".to_string(),
                BTreeMap::from([("s".to_string(), key)]),
            )]),
            tool_call_instances: BTreeMap::from([(
                key,
                v6::ToolCallInstanceV6 {
                    key_version: 7,
                    attempted_key_version_nk: 7,
                    tool_call_id: "t".to_string(),
                    command_args: vec![Hash::from_content(b"arg")],
                    env_vars_ck: BTreeMap::from([(
                        "K".to_string(),
                        v6::EnvVarEntryV6 {
                            hash_ck: Hash::from_content(b"env"),
                            source_nk: Some(v6::EnvVarSourceV6::Literal),
                        },
                    )]),
                    materialized_inputs: BTreeMap::from([(
                        "dest".to_string(),
                        Hash::from_content(b"staged"),
                    )]),
                    outputs: BTreeMap::from([(
                        "o".to_string(),
                        v6::CapturedOutputV6 {
                            capture: "file:out".to_string(),
                            hash_nk: Some(Hash::from_content(b"out")),
                            saved_nk: true,
                        },
                    )]),
                    impure_nk: false,
                    invalidated_at_nk: Some(v6::WireTimestampV6(11)),
                    executed_at_nk: v6::WireTimestampV6(5),
                    last_referenced_at_nk: v6::WireTimestampV6(42),
                    step_ids_nk: BTreeSet::from([v6::StepRefV6 {
                        workflow_id: "w".to_string(),
                        step_id: "s".to_string(),
                    }]),
                },
            )]),
        };

        let migrated = from_v6_into_v7(state);
        let instance = &migrated.tool_call_instances[&DerivedHashV7::from_hash(key)];

        assert_eq!(migrated.version, CONDUCTOR_STATE_VERSION_V7);
        assert_eq!(migrated.conductor_gc_ran_at, 9);
        assert!(migrated.instance_key_secret_verifier.is_none());
        assert_eq!(migrated.step_instances["w"]["s"], DerivedHashV7::from_hash(key));
        assert_eq!(
            instance.command_args,
            vec![DerivedHashV7::from_hash(Hash::from_content(b"arg"))]
        );
        assert_eq!(
            instance.env_vars_ck["K"].hash_ck,
            DerivedHashV7::from_hash(Hash::from_content(b"env"))
        );
        assert_eq!(instance.materialized_inputs["dest"], Hash::from_content(b"staged"));
        assert_eq!(instance.outputs["o"].capture, "file:out");
        assert_eq!(instance.outputs["o"].hash_nk, Some(Hash::from_content(b"out")));
        assert_eq!(instance.invalidated_at_nk, Some(WireTimestampV7(11)));
        assert_eq!(instance.step_ids_nk.iter().next().unwrap().step_id, "s");
    }

    #[test]
    fn derived_hash_v7_accepts_both_digest_modes_and_rejects_a_foreign_prefix() {
        let unkeyed = Hash::from_content(b"plain");
        let keyed = format!("{KEYED_HASH_PREFIX}{}", "0".repeat(64));

        let parsed_unkeyed = unkeyed.to_string().parse::<DerivedHashV7>().expect("unkeyed parses");
        let parsed_keyed = keyed.parse::<DerivedHashV7>().expect("keyed parses");

        assert_eq!(parsed_unkeyed, DerivedHashV7::from_hash(unkeyed));
        assert_eq!(parsed_keyed.as_str(), keyed);
        assert!("sha256:deadbeef".parse::<DerivedHashV7>().is_err());
    }
}
