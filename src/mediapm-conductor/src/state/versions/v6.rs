//! V6 wire format for orchestration state persistence.
//!
//! V6 flattens and renames conductor state for explicit key participation and
//! retention facts:
//!
//! - `aux` is flattened away: `conductor_gc_epoch` becomes top-level
//!   `conductor_gc_ran_at`, and `live_instances` is dropped.
//! - `step_instances` (workflow -> step -> instance key) is the sole GC seed and
//!   lookup map.
//! - Instance fields are renamed to mark key participation: bare/keyed
//!   (`key_version`, `tool_call_id`, `command_args`, `materialized_inputs`,
//!   `outputs` names and capture tokens), `_ck` conditionally keyed
//!   (`env_vars_ck`, `hash_ck` for literal entries), `_nk` never keyed
//!   (`attempted_key_version_nk`, `hash_nk`, `saved_nk`, `source_nk`,
//!   `impure_nk`, `invalidated_at_nk`, `executed_at_nk`,
//!   `last_referenced_at_nk`, `step_ids_nk`).
//! - Outputs are flattened: `{ capture, hash_nk, saved_nk }`. `hash_nk: null`
//!   means an allowed-empty capture matched nothing.
//! - Env entries become `{ hash_ck, source_nk }`; migrated records write explicit
//!   `source_nk: null`.
//! - Nullable facts are always serialized as explicit `null`.
//! - Each instance records `key_version` and `attempted_key_version_nk`; migrated
//!   pre-v6 records carry 7.
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

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use super::v5;

/// V6 schema version marker.
pub(crate) const CONDUCTOR_STATE_VERSION_V6: u32 = 6;

/// Legacy instance key version recorded for pre-v6 migrated instances.
pub(crate) const MIGRATED_INSTANCE_KEY_VERSION_V6: u32 = 7;

/// Returns whether `marker` matches V6.
#[must_use]
pub(crate) const fn is_conductor_state_version_v6(marker: u32) -> bool {
    marker == CONDUCTOR_STATE_VERSION_V6
}

/// V6 timestamp (nanoseconds since Unix epoch).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct WireTimestampV6(
    /// Nanoseconds since Unix epoch.
    pub u64,
);

/// V6 env var provenance tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EnvVarSourceV6 {
    /// The document fixed the value as a non-empty literal.
    Literal,
    /// The host supplied the value (or the document left it empty/inherited).
    Inherited,
}

/// V6 env var record: hashed value and optional provenance tag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EnvVarEntryV6 {
    /// Hash of the env var value (key material for literal entries).
    pub hash_ck: Hash,
    /// Provenance of the value, or `null` for migrated records with unknown
    /// provenance.
    pub source_nk: Option<EnvVarSourceV6>,
}

/// V6 captured output: capture token, nullable hash fact, and retention fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapturedOutputV6 {
    /// Capture token/kind string (for example `file:downloads/out.mkv`,
    /// `folder_flat:downloads/x`).
    pub capture: String,
    /// CAS content hash, or `null` when an allowed-empty capture matched
    /// nothing.
    pub hash_nk: Option<Hash>,
    /// Whether the bytes are retained as a saved fact.
    pub saved_nk: bool,
}

/// V6 step reference, qualified by workflow.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StepRefV6 {
    /// Workflow the step belongs to.
    #[serde(default)]
    pub workflow_id: String,
    /// Step id inside that workflow.
    pub step_id: String,
}

/// V6 tool-call instance record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolCallInstanceV6 {
    /// Version whose derivation produced the stored key.
    pub key_version: u32,
    /// Highest key version attempted for migration checks.
    pub attempted_key_version_nk: u32,
    /// Tool call identifier: unified tools-catalog map key.
    pub tool_call_id: String,
    /// Effective command argv / key material hashes in order.
    pub command_args: Vec<Hash>,
    /// Execution environment variables, hashed, with optional provenance.
    pub env_vars_ck: BTreeMap<String, EnvVarEntryV6>,
    /// Inputs staged into the sandbox, keyed by destination.
    pub materialized_inputs: BTreeMap<String, Hash>,
    /// Captured outputs keyed by output name.
    pub outputs: BTreeMap<String, CapturedOutputV6>,
    /// Whether the tool is impure (never key material).
    pub impure_nk: bool,
    /// When the instance was invalidated, or explicit `null`.
    pub invalidated_at_nk: Option<WireTimestampV6>,
    /// Execution timestamp (never key material).
    pub executed_at_nk: WireTimestampV6,
    /// GC last-reference timestamp (never key material).
    pub last_referenced_at_nk: WireTimestampV6,
    /// Steps that have served this instance (never key material).
    #[serde(default)]
    pub step_ids_nk: BTreeSet<StepRefV6>,
}

/// V6 orchestration state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConductorStateV6 {
    /// Schema version marker.
    pub version: u32,
    /// Conductor GC reference clock (nanoseconds since Unix epoch).
    pub conductor_gc_ran_at: u64,
    /// Workflow -> step -> instance key map: sole GC seed and lookup map.
    #[serde(default)]
    pub step_instances: BTreeMap<String, BTreeMap<String, Hash>>,
    /// Recorded tool-call instances keyed by instance key.
    pub tool_call_instances: BTreeMap<Hash, ToolCallInstanceV6>,
}

/// Migrates a V5 state into the V6 wire shape.
pub(crate) fn from_v5_into_v6(state: v5::ConductorStateV5) -> ConductorStateV6 {
    let tool_call_instances: BTreeMap<Hash, ToolCallInstanceV6> = state
        .tool_call_instances
        .into_iter()
        .map(|(key, instance)| (key, instance_into_v6(instance)))
        .collect();

    let step_instances = backfill_step_instances(&tool_call_instances);

    ConductorStateV6 {
        version: CONDUCTOR_STATE_VERSION_V6,
        conductor_gc_ran_at: state.aux.conductor_gc_epoch.0,
        step_instances,
        tool_call_instances,
    }
}

/// Converts one V5 instance into V6.
fn instance_into_v6(instance: v5::ToolCallInstanceV5) -> ToolCallInstanceV6 {
    let outputs = instance
        .outputs
        .into_iter()
        .filter_map(|(name, output)| output_into_v6(output).map(|converted| (name, converted)))
        .collect();

    let env_vars_ck = instance
        .env_vars
        .into_iter()
        .map(|(name, hash_ck)| (name, EnvVarEntryV6 { hash_ck, source_nk: None }))
        .collect();

    let step_ids_nk = instance
        .step_ids
        .into_iter()
        .map(|step| StepRefV6 { workflow_id: step.workflow_id, step_id: step.step_id })
        .collect();

    ToolCallInstanceV6 {
        key_version: MIGRATED_INSTANCE_KEY_VERSION_V6,
        attempted_key_version_nk: MIGRATED_INSTANCE_KEY_VERSION_V6,
        tool_call_id: instance.tool_call_id,
        command_args: instance.command_args,
        env_vars_ck,
        materialized_inputs: instance.materialized_inputs,
        outputs,
        impure_nk: instance.impure,
        invalidated_at_nk: instance.invalidated_at.map(|stamp| WireTimestampV6(stamp.0)),
        executed_at_nk: WireTimestampV6(instance.executed_at.0),
        last_referenced_at_nk: WireTimestampV6(instance.last_referenced_at.0),
        step_ids_nk,
    }
}

/// Converts a V5 captured output into V6, dropping sentinel `capture: None`.
fn output_into_v6(output: v5::CapturedOutputV5) -> Option<CapturedOutputV6> {
    let spec = output.capture?;
    Some(CapturedOutputV6 {
        capture: capture_token_v5(&spec),
        hash_nk: Some(output.hash),
        saved_nk: match spec.save {
            v5::SaveModeV5::False => false,
            v5::SaveModeV5::True | v5::SaveModeV5::Full => true,
        },
    })
}

/// Renders the v5 capture spec's token/kind string.
fn capture_token_v5(spec: &v5::OutputCaptureSpecV5) -> String {
    if let Some(rest) = spec.capture.strip_prefix("folder:") {
        if spec.include_topmost_folder {
            format!("folder:{rest}")
        } else {
            format!("folder_flat:{rest}")
        }
    } else if let Some(rest) = spec.capture.strip_prefix("folder_regex:") {
        if spec.include_topmost_folder {
            format!("folder_regex:{rest}")
        } else {
            format!("folder_flat_regex:{rest}")
        }
    } else {
        spec.capture.clone()
    }
}

/// Backfills `step_instances` from migrated instances.
///
/// For each `(workflow_id, step_id)` claimed by instances, selects the instance
/// key with highest `last_referenced_at_nk`, then `executed_at_nk`, then key
/// order.
fn backfill_step_instances(
    instances: &BTreeMap<Hash, ToolCallInstanceV6>,
) -> BTreeMap<String, BTreeMap<String, Hash>> {
    let mut best: BTreeMap<(String, String), (WireTimestampV6, WireTimestampV6, Hash)> =
        BTreeMap::new();

    for (&key, instance) in instances {
        for step in &instance.step_ids_nk {
            let candidate = (
                step.workflow_id.clone(),
                step.step_id.clone(),
                instance.last_referenced_at_nk,
                instance.executed_at_nk,
                key,
            );
            let entry_key = (step.workflow_id.clone(), step.step_id.clone());
            let replace = match best.get(&entry_key) {
                None => true,
                Some(existing) => {
                    (candidate.2, candidate.3, candidate.4) > (existing.0, existing.1, existing.2)
                }
            };
            if replace {
                best.insert(entry_key, (candidate.2, candidate.3, candidate.4));
            }
        }
    }

    let mut step_instances: BTreeMap<String, BTreeMap<String, Hash>> = BTreeMap::new();
    for ((workflow_id, step_id), (_, _, key)) in best {
        step_instances.entry(workflow_id).or_default().insert(step_id, key);
    }
    step_instances
}

#[cfg(test)]
mod tests {
    use super::*;
    use mediapm_cas::Hash;

    fn v5_spec(
        name: &str,
        capture: &str,
        save: v5::SaveModeV5,
        include_topmost_folder: bool,
    ) -> v5::OutputCaptureSpecV5 {
        v5::OutputCaptureSpecV5 {
            name: name.to_string(),
            capture: capture.to_string(),
            save,
            allow_empty: false,
            include_topmost_folder,
        }
    }

    #[test]
    fn migration_backfills_step_instances_and_drops_none_captures() {
        let key_a = Hash::from_content(b"a");
        let key_b = Hash::from_content(b"b");
        let state = v5::ConductorStateV5 {
            version: 5,
            tool_call_instances: BTreeMap::from([
                (
                    key_a,
                    v5::ToolCallInstanceV5 {
                        tool_call_id: "echo@v1".to_string(),
                        impure: false,
                        invalidated_at: None,
                        executed_at: v5::WireTimestampV5(10),
                        last_referenced_at: v5::WireTimestampV5(20),
                        step_ids: BTreeSet::from([v5::StepRefV5 {
                            workflow_id: "w".to_string(),
                            step_id: "s".to_string(),
                        }]),
                        command_args: vec![Hash::from_content(b"arg")],
                        env_vars: BTreeMap::from([("K".to_string(), Hash::from_content(b"v"))]),
                        materialized_inputs: BTreeMap::new(),
                        outputs: BTreeMap::from([
                            (
                                "out".to_string(),
                                v5::CapturedOutputV5 {
                                    hash: Hash::from_content(b"out"),
                                    capture: Some(v5_spec(
                                        "out",
                                        "file:out",
                                        v5::SaveModeV5::True,
                                        true,
                                    )),
                                },
                            ),
                            (
                                "sentinel".to_string(),
                                v5::CapturedOutputV5 {
                                    hash: Hash::from_content(b"sentinel"),
                                    capture: None,
                                },
                            ),
                        ]),
                    },
                ),
                (
                    key_b,
                    v5::ToolCallInstanceV5 {
                        tool_call_id: "echo@v1".to_string(),
                        impure: true,
                        invalidated_at: Some(v5::WireTimestampV5(99)),
                        executed_at: v5::WireTimestampV5(30),
                        last_referenced_at: v5::WireTimestampV5(40),
                        step_ids: BTreeSet::from([v5::StepRefV5 {
                            workflow_id: "w".to_string(),
                            step_id: "s".to_string(),
                        }]),
                        command_args: Vec::new(),
                        env_vars: BTreeMap::new(),
                        materialized_inputs: BTreeMap::new(),
                        outputs: BTreeMap::new(),
                    },
                ),
            ]),
            aux: v5::AuxDataV5 {
                conductor_gc_epoch: v5::WireTimestampV5(123),
                live_instances: BTreeMap::from([("w".to_string(), BTreeSet::from([key_a]))]),
            },
        };

        let migrated = from_v5_into_v6(state);
        assert_eq!(migrated.version, CONDUCTOR_STATE_VERSION_V6);
        assert_eq!(migrated.conductor_gc_ran_at, 123);
        assert_eq!(migrated.step_instances["w"]["s"], key_b, "highest last_referenced_at wins");
        let a = &migrated.tool_call_instances[&key_a];
        assert_eq!(a.key_version, 7);
        assert_eq!(a.attempted_key_version_nk, 7);
        assert_eq!(a.env_vars_ck["K"].source_nk, None);
        assert!(a.outputs.contains_key("out"));
        assert!(!a.outputs.contains_key("sentinel"));
        assert_eq!(a.outputs["out"].hash_nk, Some(Hash::from_content(b"out")));
        assert!(a.outputs["out"].saved_nk);
        let b = &migrated.tool_call_instances[&key_b];
        assert_eq!(b.invalidated_at_nk, Some(WireTimestampV6(99)));
    }

    #[test]
    fn migration_maps_save_modes_and_folder_tokens() {
        let key = Hash::from_content(b"k");
        let state = v5::ConductorStateV5 {
            version: 5,
            tool_call_instances: BTreeMap::from([(
                key,
                v5::ToolCallInstanceV5 {
                    tool_call_id: "t".to_string(),
                    impure: false,
                    invalidated_at: None,
                    executed_at: v5::WireTimestampV5(1),
                    last_referenced_at: v5::WireTimestampV5(1),
                    step_ids: BTreeSet::new(),
                    command_args: Vec::new(),
                    env_vars: BTreeMap::new(),
                    materialized_inputs: BTreeMap::new(),
                    outputs: BTreeMap::from([
                        (
                            "a".to_string(),
                            v5::CapturedOutputV5 {
                                hash: Hash::from_content(b"a"),
                                capture: Some(v5_spec(
                                    "a",
                                    "folder:downloads",
                                    v5::SaveModeV5::False,
                                    false,
                                )),
                            },
                        ),
                        (
                            "b".to_string(),
                            v5::CapturedOutputV5 {
                                hash: Hash::from_content(b"b"),
                                capture: Some(v5_spec(
                                    "b",
                                    "folder:downloads",
                                    v5::SaveModeV5::Full,
                                    true,
                                )),
                            },
                        ),
                        (
                            "c".to_string(),
                            v5::CapturedOutputV5 {
                                hash: Hash::from_content(b"c"),
                                capture: Some(v5_spec(
                                    "c",
                                    "folder_regex:^x$",
                                    v5::SaveModeV5::True,
                                    false,
                                )),
                            },
                        ),
                    ]),
                },
            )]),
            aux: v5::AuxDataV5::default(),
        };
        let migrated = from_v5_into_v6(state);
        let out = &migrated.tool_call_instances[&key].outputs;
        assert_eq!(out["a"].capture, "folder_flat:downloads");
        assert!(!out["a"].saved_nk);
        assert_eq!(out["b"].capture, "folder:downloads");
        assert!(out["b"].saved_nk);
        assert_eq!(out["c"].capture, "folder_flat_regex:^x$");
        assert!(out["c"].saved_nk);
    }

    #[test]
    fn nullable_facts_serialize_explicitly() {
        let instance = ToolCallInstanceV6 {
            key_version: 7,
            attempted_key_version_nk: 7,
            tool_call_id: "t".to_string(),
            command_args: Vec::new(),
            env_vars_ck: BTreeMap::from([(
                "K".to_string(),
                EnvVarEntryV6 { hash_ck: Hash::from_content(b"v"), source_nk: None },
            )]),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::from([(
                "o".to_string(),
                CapturedOutputV6 { capture: "stdout".to_string(), hash_nk: None, saved_nk: false },
            )]),
            impure_nk: false,
            invalidated_at_nk: None,
            executed_at_nk: WireTimestampV6(1),
            last_referenced_at_nk: WireTimestampV6(2),
            step_ids_nk: BTreeSet::new(),
        };
        let json = serde_json::to_value(&instance).expect("serialize");
        assert_eq!(json["invalidated_at_nk"], serde_json::Value::Null);
        assert_eq!(json["outputs"]["o"]["hash_nk"], serde_json::Value::Null);
        assert_eq!(json["env_vars_ck"]["K"]["source_nk"], serde_json::Value::Null);
    }

    #[test]
    fn v6_rejects_v5_only_fields() {
        let err = serde_json::from_str::<ConductorStateV6>(
            r#"{"version":6,"conductor_gc_ran_at":0,"step_instances":{},"tool_call_instances":{},"aux":{}}"#,
        )
        .expect_err("aux must be rejected");
        assert!(err.to_string().contains("aux") || err.to_string().contains("unknown"));
    }
}
