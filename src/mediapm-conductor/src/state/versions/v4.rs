//! V4 wire format for orchestration state persistence.
//!
//! V4 adds two fields, and both exist to make a decision travel with the record
//! it is about instead of with a global queue:
//!
//! - [`ToolCallInstanceV4::invalidated_at`] — when an operator last marked a
//!   recorded instance as not to be reused. Only the step-worker probe reads it
//!   on the execution path, which is what makes explicit invalidation hard to
//!   overlook: the probe is the gate every reuse of recorded work passes
//!   through. Materialization deliberately ignores it, because it executes
//!   nothing and refusing a marked record would delete bytes the re-execution
//!   has not yet replaced.
//! - [`AuxDataV4::live_instances`] — the instance keys each workflow's most
//!   recent successful run produced or hit, which is the seed of the GC
//!   reachability walk.
//!
//! This module owns the V4 version marker, the V4 wire types, and the
//! V3→V4 migration. It must not import unversioned runtime state from
//! `super::super`; the bridge to runtime types lives in [`super`].
//!
//! Every V4 wire type that can carry a field denies unknown fields, matching
//! V3: a document this build wrote is the only V4 document it reads, and a
//! field it cannot name is either a newer build's or a hand edit, so decoding
//! it away would let the next save delete it. Two V4 wire types cannot carry
//! the attribute at all: [`SaveModeV4`] is a fieldless enum, and
//! [`ImpureTimestampV4`] is a one-field tuple newtype, so serde gives neither a
//! map to reject unknown keys in. Their *containing* structs are strict, which
//! is where an unknown key would actually appear.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`
//!   beyond what the version-local types are declared to bridge.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.
//! - Files outside `versions/` must reach versioned symbols only through
//!   `versions/mod.rs`, never through a direct `versions::vX` path.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use super::v3;

/// V4 schema version marker.
pub(crate) const CONDUCTOR_STATE_VERSION_V4: u32 = 4;

/// Returns whether `marker` matches V4.
#[must_use]
pub(crate) const fn is_conductor_state_version_v4(marker: u32) -> bool {
    marker == CONDUCTOR_STATE_VERSION_V4
}

// ---------------------------------------------------------------------------
// V4 wire format types
// ---------------------------------------------------------------------------
// These mirror the runtime `ConductorState` family but stay version-local, so
// an older version's shape can never be reshaped by a later edit. The
// `mod.rs` bridge converts between V4 wire types and the runtime types.

/// V4 persistence mode for one capture spec.
///
/// Mirrors the runtime `SaveMode` spelling (`"false"`, `"true"`, `"full"`) so a
/// spec written by one representation decodes into the other unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SaveModeV4 {
    /// Do not persist this output.
    False,
    /// Persist this output normally.
    #[default]
    True,
    /// Force full persistence even when empty or the step fails.
    Full,
}

/// Returns whether `mode` is the default mode, which the wire omits.
///
/// serde resolves `skip_serializing_if` to a predicate taking the field by
/// reference, so the reference is the interface rather than an oversight.
#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if invokes the predicate with &T"
)]
const fn is_save_mode_true_v4(mode: &SaveModeV4) -> bool {
    matches!(mode, SaveModeV4::True)
}

/// V4 capture specification for one output name.
///
/// The shape is the runtime `OutputCaptureSpec` as persisted with the entry
/// that recorded it. `save` is omitted when it is the default, matching the
/// config document's own serialization so a spec has one spelling everywhere.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputCaptureSpecV4 {
    /// Logical output name.
    pub name: String,
    /// Capture source selector (`stdout`, `stderr`, `process_code`,
    /// `file:<path>`, `file_regex:<pattern>`, `folder:<path>`,
    /// `folder_regex:<pattern>`).
    pub capture: String,
    /// The persistence intent recorded for this output.
    ///
    /// Written and read as provenance: the capture phase stores every output
    /// whatever this says, so it is never the reason bytes are or are not kept.
    #[serde(default, skip_serializing_if = "is_save_mode_true_v4")]
    pub save: SaveModeV4,
    /// Whether an empty capture result is acceptable.
    #[serde(default)]
    pub allow_empty: bool,
    /// Whether `folder:` and `folder_regex:` listings include the topmost
    /// folder name.
    #[serde(default = "default_include_topmost_folder_v4")]
    pub include_topmost_folder: bool,
}

/// Default for [`OutputCaptureSpecV4::include_topmost_folder`].
const fn default_include_topmost_folder_v4() -> bool {
    true
}

/// V4 impure timestamp (nanoseconds since Unix epoch).
///
/// The same one-field newtype V3 used: a tuple struct has no map for serde to
/// reject unknown keys in, so the strictness lives on the structs that contain
/// it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImpureTimestampV4(
    /// Nanoseconds since Unix epoch.
    pub u64,
);

/// V4 content-addressed value record for argv, env and materialized inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HashedValueRecordV4 {
    /// CAS content hash of the recorded value.
    pub hash: Hash,
    /// Whether the value is deterministic (participates in instance keys).
    pub deterministic: bool,
}

/// V4 captured output: content hash plus the spec that produced it.
///
/// `capture: None` is the migration's "spec unknown" sentinel: an entry
/// carried over from V2 recorded no spec, and no declared spec equals `None`,
/// so such an entry never satisfies a capture request that names a spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapturedOutputV4 {
    /// CAS content hash of the captured bytes.
    pub hash: Hash,
    /// The capture spec that produced this entry, or `None` when the record
    /// predates spec storage.
    pub capture: Option<OutputCaptureSpecV4>,
}

/// V4 tool-call instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolCallInstanceV4 {
    /// Content-addressed instance key.
    pub instance_key: Hash,
    /// Tool call identifier: the unified tools-catalog map key.
    pub tool_call_id: String,
    /// Whether the tool is impure.
    ///
    /// Settlement reads it to decide whether a differing artifact for one
    /// capture identity replaces the stored entry or is divergence, so it is
    /// behaviour and not only provenance. It is never key material.
    pub impure: bool,
    /// When an operator last invalidated this record, or `None` while its bytes
    /// may be reused.
    ///
    /// Omitted from the wire when absent, and defaulted on read, so a V3
    /// document migrates to `None` and a record written before any invalidation
    /// stays byte-identical. The consumer is the step-worker probe alone; see
    /// the runtime field's documentation for why that is the whole mechanism.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalidated_at: Option<ImpureTimestampV4>,
    /// Execution timestamp. Provenance only; never key material.
    pub executed_at: ImpureTimestampV4,
    /// Steps that have served this instance, by step id.
    ///
    /// Provenance only and `#[serde(default)]`. A V2 document that predates
    /// the field is read by [`super::v2::ToolCallInstanceV2`], whose own
    /// `#[serde(default)]` on the same field supplies the empty set the
    /// migration then carries across; the default on this struct is what keeps
    /// a record written before the field readable.
    #[serde(default)]
    pub step_ids: BTreeSet<String>,
    /// Effective command argv recorded at execution time, in order.
    pub command_args: Vec<HashedValueRecordV4>,
    /// Execution environment variables, hashed.
    pub env_vars: BTreeMap<String, HashedValueRecordV4>,
    /// Materialized tool-content inputs keyed by sandbox-relative path.
    pub materialized_inputs: BTreeMap<String, HashedValueRecordV4>,
    /// Captured output records keyed by output name.
    pub outputs: BTreeMap<String, CapturedOutputV4>,
}

/// V4 per-instance auxiliary metadata: the GC reference clock only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstanceAuxV4 {
    /// Conductor GC last-reference clock.
    pub last_referenced_at: ImpureTimestampV4,
}

impl Default for InstanceAuxV4 {
    fn default() -> Self {
        Self { last_referenced_at: ImpureTimestampV4(0) }
    }
}

/// V4 auxiliary metadata: the GC clock, the instance counter and the GC seed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuxDataV4 {
    /// Monotonic counter the instance keys are derived against; carried over
    /// unchanged from V2 so a migration does not shift any key.
    pub tool_call_instance_counter: u64,
    /// Conductor GC epoch: the clock reference ages are measured from.
    pub conductor_gc_epoch: ImpureTimestampV4,
    /// Per-instance aux metadata keyed by instance key.
    pub instances: BTreeMap<Hash, InstanceAuxV4>,
    /// Instance keys each workflow's latest successful run produced or hit,
    /// keyed by workflow id: the GC reachability seed.
    #[serde(default)]
    pub live_instances: BTreeMap<String, BTreeSet<Hash>>,
}

impl Default for AuxDataV4 {
    fn default() -> Self {
        Self {
            tool_call_instance_counter: 0,
            conductor_gc_epoch: ImpureTimestampV4(0),
            instances: BTreeMap::new(),
            live_instances: BTreeMap::new(),
        }
    }
}

/// V4 orchestration state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConductorStateV4 {
    /// Schema version marker; always [`CONDUCTOR_STATE_VERSION_V4`] when this
    /// build writes, and the dispatch key a reader selects on.
    pub version: u32,
    /// Recorded tool-call instances keyed by instance key.
    pub tool_call_instances: BTreeMap<Hash, ToolCallInstanceV4>,
    /// Auxiliary metadata: the GC clock, the reference clock and the GC seed.
    pub aux: AuxDataV4,
}

// ---------------------------------------------------------------------------
// V3 → V4 migration
// ---------------------------------------------------------------------------

/// Migrates a V3 state into the V4 wire shape.
///
/// Both new fields migrate to their empty value, and the two directions differ
/// in what that means. `invalidated_at: None` is exactly right — no operator has
/// invalidated anything in a document written before the marker existed, and
/// inventing a timestamp would re-execute work nobody asked to re-run.
/// `live_instances` being empty is a *pending* seed rather than a claim that
/// nothing is live: the GC walk treats "this workflow has no recorded seed" as
/// "evict nothing for it", so a migrated document keeps every instance until
/// that workflow next runs successfully and records its real seed.
pub(crate) fn from_v3_into_v4(state: v3::ConductorStateV3) -> ConductorStateV4 {
    ConductorStateV4 {
        version: CONDUCTOR_STATE_VERSION_V4,
        tool_call_instances: state
            .tool_call_instances
            .into_iter()
            .map(|(key, instance)| (key, instance_into_v4(instance)))
            .collect(),
        aux: AuxDataV4 {
            tool_call_instance_counter: state.aux.tool_call_instance_counter,
            conductor_gc_epoch: ImpureTimestampV4(state.aux.conductor_gc_epoch.0),
            instances: state
                .aux
                .instances
                .into_iter()
                .map(|(key, aux)| {
                    (
                        key,
                        InstanceAuxV4 {
                            last_referenced_at: ImpureTimestampV4(aux.last_referenced_at.0),
                        },
                    )
                })
                .collect(),
            live_instances: BTreeMap::new(),
        },
    }
}

/// Converts one V3 instance, adding the absent invalidation marker.
fn instance_into_v4(instance: v3::ToolCallInstanceV3) -> ToolCallInstanceV4 {
    ToolCallInstanceV4 {
        instance_key: instance.instance_key,
        tool_call_id: instance.tool_call_id,
        impure: instance.impure,
        invalidated_at: None,
        executed_at: ImpureTimestampV4(instance.executed_at.0),
        step_ids: instance.step_ids,
        command_args: instance
            .command_args
            .into_iter()
            .map(|record| HashedValueRecordV4 {
                hash: record.hash,
                deterministic: record.deterministic,
            })
            .collect(),
        env_vars: instance
            .env_vars
            .into_iter()
            .map(|(name, record)| {
                (
                    name,
                    HashedValueRecordV4 { hash: record.hash, deterministic: record.deterministic },
                )
            })
            .collect(),
        materialized_inputs: instance
            .materialized_inputs
            .into_iter()
            .map(|(path, record)| {
                (
                    path,
                    HashedValueRecordV4 { hash: record.hash, deterministic: record.deterministic },
                )
            })
            .collect(),
        outputs: instance
            .outputs
            .into_iter()
            .map(|(name, record)| {
                (
                    name,
                    CapturedOutputV4 {
                        hash: record.hash,
                        capture: record.capture.map(spec_into_v4),
                    },
                )
            })
            .collect(),
    }
}

/// Converts one V3 capture spec into its V4 spelling.
fn spec_into_v4(spec: v3::OutputCaptureSpecV3) -> OutputCaptureSpecV4 {
    OutputCaptureSpecV4 {
        name: spec.name,
        capture: spec.capture,
        save: match spec.save {
            v3::SaveModeV3::False => SaveModeV4::False,
            v3::SaveModeV3::True => SaveModeV4::True,
            v3::SaveModeV3::Full => SaveModeV4::Full,
        },
        allow_empty: spec.allow_empty,
        include_topmost_folder: spec.include_topmost_folder,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const INSTANCE_KEY: &str =
        "blake3:0000000000000000000000000000000000000000000000000000000000000002";
    const OUTPUT_HASH: &str =
        "blake3:0000000000000000000000000000000000000000000000000000000000000001";

    /// A V3 document migrates to V4 with both new fields empty.
    ///
    /// `invalidated_at: None` is a fact (nobody invalidated anything before the
    /// marker existed) while the empty seed is a promise the GC walk keeps: it
    /// evicts nothing for a workflow it has no seed for, so a migrated document
    /// neither re-executes work nor loses records on the first sweep.
    #[test]
    fn v3_state_migrates_to_v4_with_defaults() {
        let json = format!(
            r#"{{
                "version": 3,
                "tool_call_instances": {{
                    "{INSTANCE_KEY}": {{
                        "instance_key": "{INSTANCE_KEY}",
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 7,
                        "step_ids": ["step-0-echo"],
                        "command_args": [],
                        "env_vars": {{}},
                        "materialized_inputs": {{}},
                        "outputs": {{
                            "content": {{ "hash": "{OUTPUT_HASH}", "capture": null }}
                        }}
                    }}
                }},
                "aux": {{
                    "tool_call_instance_counter": 2,
                    "conductor_gc_epoch": 9,
                    "instances": {{}}
                }}
            }}"#
        );
        let v3_state: v3::ConductorStateV3 =
            serde_json::from_str(&json).expect("the v3 fixture decodes as v3");
        let v4_state = from_v3_into_v4(v3_state);

        assert_eq!(v4_state.version, CONDUCTOR_STATE_VERSION_V4);
        assert!(v4_state.aux.live_instances.is_empty());
        let instance = v4_state.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.invalidated_at, None);
        assert_eq!(instance.executed_at, ImpureTimestampV4(7));
        assert_eq!(v4_state.aux.conductor_gc_epoch, ImpureTimestampV4(9));
    }

    /// A V4 document may omit both new fields, and omitting them means "not
    /// invalidated" and "no seed recorded yet".
    #[test]
    fn a_v4_document_without_the_new_fields_decodes_to_their_defaults() {
        let json = format!(
            r#"{{
                "version": 4,
                "tool_call_instances": {{
                    "{INSTANCE_KEY}": {{
                        "instance_key": "{INSTANCE_KEY}",
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 0,
                        "command_args": [],
                        "env_vars": {{}},
                        "materialized_inputs": {{}},
                        "outputs": {{}}
                    }}
                }},
                "aux": {{
                    "tool_call_instance_counter": 0,
                    "conductor_gc_epoch": 0,
                    "instances": {{}}
                }}
            }}"#
        );
        let state: ConductorStateV4 =
            serde_json::from_str(&json).expect("a v4 document without the new fields decodes");

        assert_eq!(
            state.tool_call_instances.values().next().expect("one instance").invalidated_at,
            None,
        );
        assert!(state.aux.live_instances.is_empty());
    }

    /// A field this version cannot name is rejected, not decoded away.
    ///
    /// The alternative is silent data loss: the field is dropped on read and
    /// the next save writes the document back without it.
    #[test]
    fn a_v4_document_with_an_unknown_field_is_rejected() {
        let json = r#"{
                "version": 4,
                "also_unknown": true,
                "tool_call_instances": {},
                "aux": {
                    "tool_call_instance_counter": 0,
                    "conductor_gc_epoch": 0,
                    "instances": {}
                }
            }"#;
        let error = serde_json::from_str::<ConductorStateV4>(json)
            .expect_err("an unknown field must not be ignored");

        assert!(
            error.to_string().contains("also_unknown"),
            "the diagnostic must name the field, got: {error}",
        );
    }

    /// The marker field is omitted while absent, so a document with no
    /// invalidation serializes exactly as V3 did apart from the version number.
    #[test]
    fn an_absent_marker_is_omitted_from_the_wire() {
        let state = ConductorStateV4 {
            version: CONDUCTOR_STATE_VERSION_V4,
            tool_call_instances: BTreeMap::from([(
                Hash::from_content(b"key"),
                ToolCallInstanceV4 {
                    instance_key: Hash::from_content(b"key"),
                    tool_call_id: "echo@v1".to_string(),
                    impure: false,
                    invalidated_at: None,
                    executed_at: ImpureTimestampV4(0),
                    step_ids: BTreeSet::new(),
                    command_args: Vec::new(),
                    env_vars: BTreeMap::new(),
                    materialized_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                },
            )]),
            aux: AuxDataV4::default(),
        };
        let value = serde_json::to_value(&state).expect("v4 state serializes");
        let instance = value["tool_call_instances"]
            .as_object()
            .expect("instances map")
            .values()
            .next()
            .expect("one instance");

        assert!(
            instance.get("invalidated_at").is_none(),
            "an absent marker must not be written as null",
        );
        assert_eq!(value["aux"]["live_instances"], json!({}));
    }

    /// The seed survives the wire with its keys, so a GC pass after a restart
    /// sees the same reachability it saw before.
    #[test]
    fn the_live_seed_round_trips() {
        let key = Hash::from_content(b"seed");
        let state = ConductorStateV4 {
            version: CONDUCTOR_STATE_VERSION_V4,
            tool_call_instances: BTreeMap::new(),
            aux: AuxDataV4 {
                tool_call_instance_counter: 0,
                conductor_gc_epoch: ImpureTimestampV4(0),
                instances: BTreeMap::new(),
                live_instances: BTreeMap::from([("default".to_string(), BTreeSet::from([key]))]),
            },
        };
        let encoded = serde_json::to_vec(&state).expect("serializes");
        let decoded: ConductorStateV4 = serde_json::from_slice(&encoded).expect("decodes");

        assert_eq!(decoded.aux.live_instances.get("default"), Some(&BTreeSet::from([key])));
    }
}
