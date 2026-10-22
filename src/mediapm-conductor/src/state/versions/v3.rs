//! V3 wire format for orchestration state persistence.
//!
//! V3 replaces the per-instance `save_modes` map with a capture spec recorded
//! beside every captured output: an output is addressed by name, and the entry
//! says which [`OutputCaptureSpecV3`] produced it (selector, save mode,
//! `allow_empty`, `include_topmost_folder`). The per-instance aux record keeps
//! only the GC reference clock, because persistence is now a property of the
//! entry rather than of the instance.
//!
//! This module owns the V3 version marker, the V3 wire types, and the
//! V2→V3 migration. It must not import unversioned runtime state from
//! `super::super`; the bridge to runtime types lives in [`super`].
//!
//! Every V3 wire type denies unknown fields. A document this build wrote is the
//! only V3 document it reads, and a field it cannot name is either a newer
//! build's or a hand edit: decoding it away would let the next save delete it.
//! [`super::v2`] deliberately keeps its lenient shape, because it is read-only
//! history whose writers have already shipped.
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

use super::v2;

/// V3 schema version marker.
pub(crate) const CONDUCTOR_STATE_VERSION_V3: u32 = 3;

/// Returns whether `marker` matches V3.
#[must_use]
pub(crate) const fn is_conductor_state_version_v3(marker: u32) -> bool {
    marker == CONDUCTOR_STATE_VERSION_V3
}

// ---------------------------------------------------------------------------
// V3 wire format types
// ---------------------------------------------------------------------------
// These mirror the runtime `ConductorState` family but stay version-local, so
// an older version's shape can never be reshaped by a later edit. The
// `mod.rs` bridge converts between V3 wire types and the runtime types.

/// V3 persistence mode for one capture spec.
///
/// Mirrors the runtime `SaveMode` spelling (`"false"`, `"true"`, `"full"`) so a
/// spec written by one representation decodes into the other unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SaveModeV3 {
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
const fn is_save_mode_true_v3(mode: &SaveModeV3) -> bool {
    matches!(mode, SaveModeV3::True)
}

/// V3 capture specification for one output name.
///
/// The shape is the runtime `OutputCaptureSpec` as persisted with the entry
/// that recorded it. `save` is omitted when it is the default, matching the
/// config document's own serialization so a spec has one spelling everywhere.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputCaptureSpecV3 {
    /// Logical output name.
    pub name: String,
    /// Capture source selector (`stdout`, `stderr`, `process_code`,
    /// `file:<path>`, `file_regex:<pattern>`, `folder:<path>`,
    /// `folder_regex:<pattern>`).
    pub capture: String,
    /// Whether to persist this output to CAS.
    #[serde(default, skip_serializing_if = "is_save_mode_true_v3")]
    pub save: SaveModeV3,
    /// Whether an empty capture result is acceptable.
    #[serde(default)]
    pub allow_empty: bool,
    /// Whether `folder:` and `folder_regex:` listings include the topmost
    /// folder name.
    #[serde(default = "default_include_topmost_folder_v3")]
    pub include_topmost_folder: bool,
}

/// Default for [`OutputCaptureSpecV3::include_topmost_folder`].
const fn default_include_topmost_folder_v3() -> bool {
    true
}

/// V3 impure timestamp (nanoseconds since Unix epoch).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImpureTimestampV3(
    /// Nanoseconds since Unix epoch.
    pub u64,
);

/// V3 content-addressed value record for argv, env and materialized inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HashedValueRecordV3 {
    /// CAS content hash of the recorded value.
    pub hash: Hash,
    /// Whether the value is deterministic (participates in instance keys).
    pub deterministic: bool,
}

/// V3 captured output: content hash plus the spec that produced it.
///
/// `capture: None` is the migration's "spec unknown" sentinel: an entry
/// carried over from V2 recorded no spec, and no declared spec equals `None`,
/// so such an entry never satisfies a capture request that names a spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapturedOutputV3 {
    /// CAS content hash of the captured bytes.
    pub hash: Hash,
    /// The capture spec that produced this entry, or `None` when the record
    /// predates spec storage.
    pub capture: Option<OutputCaptureSpecV3>,
}

/// V3 tool-call instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolCallInstanceV3 {
    /// Content-addressed instance key.
    pub instance_key: Hash,
    /// Tool call identifier: the unified tools-catalog map key.
    pub tool_call_id: String,
    /// Whether the tool is impure. Provenance only; never key material.
    pub impure: bool,
    /// Execution timestamp. Provenance only; never key material.
    pub executed_at: ImpureTimestampV3,
    /// Steps that have served this instance, by step id.
    ///
    /// Provenance only and `#[serde(default)]`. A V2 document that predates
    /// the field is read by [`super::v2::ToolCallInstanceV2`], whose own
    /// `#[serde(default)]` on the same field supplies the empty set the
    /// migration then carries across; the default on this struct is what keeps
    /// a V3 record written before the field readable.
    #[serde(default)]
    pub step_ids: BTreeSet<String>,
    /// Effective command argv recorded at execution time, in order.
    pub command_args: Vec<HashedValueRecordV3>,
    /// Execution environment variables, hashed.
    pub env_vars: BTreeMap<String, HashedValueRecordV3>,
    /// Materialized tool-content inputs keyed by sandbox-relative path.
    pub materialized_inputs: BTreeMap<String, HashedValueRecordV3>,
    /// Captured output records keyed by output name.
    pub outputs: BTreeMap<String, CapturedOutputV3>,
}

/// V3 per-instance auxiliary metadata: the GC reference clock only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstanceAuxV3 {
    /// Conductor GC last-reference clock.
    pub last_referenced_at: ImpureTimestampV3,
}

impl Default for InstanceAuxV3 {
    fn default() -> Self {
        Self { last_referenced_at: ImpureTimestampV3(0) }
    }
}

/// V3 auxiliary metadata: the GC clock and the instance-key counter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuxDataV3 {
    /// Monotonic counter the instance keys are derived against; carried over
    /// unchanged from V2 so a migration does not shift any key.
    pub tool_call_instance_counter: u64,
    /// Conductor GC epoch: the clock reference ages are measured from.
    pub conductor_gc_epoch: ImpureTimestampV3,
    /// Per-instance aux metadata keyed by instance key.
    pub instances: BTreeMap<Hash, InstanceAuxV3>,
}

impl Default for AuxDataV3 {
    fn default() -> Self {
        Self {
            tool_call_instance_counter: 0,
            conductor_gc_epoch: ImpureTimestampV3(0),
            instances: BTreeMap::new(),
        }
    }
}

/// V3 inline orchestration state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConductorStateV3 {
    /// Schema version marker; always [`CONDUCTOR_STATE_VERSION_V3`] when this
    /// build writes, and the dispatch key a reader selects on.
    pub version: u32,
    /// Recorded tool-call instances keyed by instance key.
    pub tool_call_instances: BTreeMap<Hash, ToolCallInstanceV3>,
    /// Auxiliary metadata: the GC clock and the per-instance reference clock.
    pub aux: AuxDataV3,
}

// ---------------------------------------------------------------------------
// V2 → V3 migration
// ---------------------------------------------------------------------------

/// Migrates a V2 inline state into the V3 wire shape.
///
/// Every V2 output becomes a [`CapturedOutputV3`] with `capture: None`, and the
/// V2 per-output save mode is dropped. The migration is one-way by design: V2
/// recorded the save mode of the last run that touched an output, which is a
/// capture-time instruction rather than a property of the recorded bytes, so
/// V3 neither resurrects it as a synthesized spec nor carries it forward on the
/// next write. What survives is the content hash, the instance identity,
/// the provenance set and the GC clock; what is lost is a persistence decision
/// that the next run re-derives from the config it captures under.
///
/// The `None` sentinel is what keeps that loss from being silently wrong: a
/// step whose declared spec no longer matches an entry treats the entry as
/// unstored and re-executes once, re-recording the bytes under the spec it
/// actually used.
pub(crate) fn from_v2_into_v3(state: v2::ConductorStateV2) -> ConductorStateV3 {
    ConductorStateV3 {
        version: CONDUCTOR_STATE_VERSION_V3,
        tool_call_instances: state
            .tool_call_instances
            .into_iter()
            .map(|(key, instance)| (key, instance_into_v3(instance)))
            .collect(),
        aux: AuxDataV3 {
            tool_call_instance_counter: state.aux.tool_call_instance_counter,
            conductor_gc_epoch: ImpureTimestampV3(state.aux.conductor_gc_epoch.0),
            instances: state
                .aux
                .instances
                .into_iter()
                // The V2 `save_modes` map is intentionally not read: dropping it
                // is the migration's documented one-way guarantee.
                .map(|(key, aux)| {
                    (
                        key,
                        InstanceAuxV3 {
                            last_referenced_at: ImpureTimestampV3(aux.last_referenced_at.0),
                        },
                    )
                })
                .collect(),
        },
    }
}

/// Converts one V2 instance, mapping each output to an unknown-spec entry.
fn instance_into_v3(instance: v2::ToolCallInstanceV2) -> ToolCallInstanceV3 {
    ToolCallInstanceV3 {
        instance_key: instance.instance_key,
        tool_call_id: instance.tool_call_id,
        impure: instance.impure,
        executed_at: ImpureTimestampV3(instance.executed_at.0),
        step_ids: instance.step_ids,
        command_args: instance
            .command_args
            .into_iter()
            .map(|record| HashedValueRecordV3 {
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
                    HashedValueRecordV3 { hash: record.hash, deterministic: record.deterministic },
                )
            })
            .collect(),
        materialized_inputs: instance
            .materialized_inputs
            .into_iter()
            .map(|(path, record)| {
                (
                    path,
                    HashedValueRecordV3 { hash: record.hash, deterministic: record.deterministic },
                )
            })
            .collect(),
        outputs: instance
            .outputs
            .into_iter()
            .map(|(name, record)| (name, CapturedOutputV3 { hash: record.hash, capture: None }))
            .collect(),
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

    /// The `capture` field is the change under test: an entry recorded before
    /// specs were stored carries `null`, and decoding it must produce the
    /// "spec unknown" sentinel rather than an empty spec that could later be
    /// mistaken for a declared one.
    #[test]
    fn a_null_capture_decodes_to_the_unknown_spec_sentinel() {
        let json = format!(
            r#"{{
                "version": 3,
                "tool_call_instances": {{
                    "{INSTANCE_KEY}": {{
                        "instance_key": "{INSTANCE_KEY}",
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 0,
                        "command_args": [],
                        "env_vars": {{}},
                        "materialized_inputs": {{}},
                        "outputs": {{
                            "content": {{ "hash": "{OUTPUT_HASH}", "capture": null }}
                        }}
                    }}
                }},
                "aux": {{
                    "tool_call_instance_counter": 0,
                    "conductor_gc_epoch": 0,
                    "instances": {{}}
                }}
            }}"#
        );
        let state: ConductorStateV3 = serde_json::from_str(&json).expect("v3 document decodes");
        let instance = state.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.outputs["content"].capture, None);
    }

    /// A V2 document migrates to V3 with every output recorded as spec-unknown
    /// and its per-output save mode left behind.
    ///
    /// The migration is one-way on purpose: a V2 save mode is the persistence
    /// decision of the run that last touched the output, not a property of the
    /// recorded bytes. V3 therefore neither synthesizes a capture spec from it
    /// nor writes it back — the next run derives persistence from the config it
    /// captures under, and the `None` sentinel makes any step that names a spec
    /// re-execute once instead of reusing an entry it cannot vouch for.
    ///
    /// The re-encode half is what proves the shape actually changed: a reader
    /// that migrated in memory while the writer still emitted V2 would pass a
    /// decode-only assertion and resurrect the old aux record on the next save.
    #[test]
    fn a_v2_document_migrates_to_v3_without_resurrecting_its_save_modes() {
        let json = format!(
            r#"{{
                "version": 2,
                "tool_call_instances": {{
                    "{INSTANCE_KEY}": {{
                        "instance_key": "{INSTANCE_KEY}",
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 0,
                        "command_args": [],
                        "env_vars": {{}},
                        "materialized_inputs": {{}},
                        "outputs": {{
                            "content": {{ "hash": "{OUTPUT_HASH}", "deterministic": true }}
                        }}
                    }}
                }},
                "aux": {{
                    "tool_call_instance_counter": 0,
                    "conductor_gc_epoch": 0,
                    "instances": {{
                        "{INSTANCE_KEY}": {{
                            "save_modes": {{ "content": "Saved" }},
                            "last_referenced_at": 7
                        }}
                    }}
                }}
            }}"#
        );

        let state = super::super::decode_state_json(json.as_bytes()).expect("the v2 file migrates");
        assert_eq!(state.version, 3, "a migrated document declares the current schema version");
        let instance = state.tool_call_instances.values().next().expect("one instance");
        assert_eq!(
            instance.outputs["content"].capture, None,
            "a v2 entry recorded no spec, so it must carry the unknown-spec sentinel",
        );
        let aux =
            state.aux.instances.values().next().expect("the aux record survives the migration");
        assert_eq!(aux.last_referenced_at.as_unix_nanos(), 7);

        let re_encoded =
            super::super::encode_state_json(&state).expect("the migrated state encodes");
        let value: serde_json::Value = serde_json::from_slice(&re_encoded).expect("valid JSON");
        assert_eq!(value["version"], json!(3));
        let encoded_aux = value["aux"]["instances"]
            .as_object()
            .and_then(|instances| instances.values().next())
            .and_then(serde_json::Value::as_object)
            .expect("one encoded aux record");
        assert!(
            encoded_aux.get("save_modes").is_none(),
            "a v2 save mode must not reappear in the v3 document: {encoded_aux:?}",
        );
        let encoded_instance = value["tool_call_instances"]
            .as_object()
            .and_then(|instances| instances.values().next())
            .expect("one encoded instance");
        assert_eq!(
            encoded_instance["outputs"]["content"]["capture"],
            serde_json::Value::Null,
            "an unknown spec is written as null, never as a synthesized spec",
        );
    }

    /// The wire shape must be a fixed point: what this build writes is what it
    /// reads back, byte for byte, with the capture spec intact. A spec field
    /// that changed serde spelling or defaulting would break the fixed point
    /// while still decoding into an equal-looking value.
    #[test]
    fn a_full_capture_spec_round_trips_byte_identically() {
        let spec = OutputCaptureSpecV3 {
            name: "content".to_string(),
            capture: "file:out.mkv".to_string(),
            save: SaveModeV3::False,
            allow_empty: false,
            include_topmost_folder: false,
        };
        let state = ConductorStateV3 {
            version: CONDUCTOR_STATE_VERSION_V3,
            tool_call_instances: BTreeMap::from([(
                Hash::from_content(b"instance"),
                ToolCallInstanceV3 {
                    instance_key: Hash::from_content(b"instance"),
                    tool_call_id: "ffmpeg@v1".to_string(),
                    impure: false,
                    executed_at: ImpureTimestampV3(5),
                    step_ids: BTreeSet::from(["step-0".to_string()]),
                    command_args: Vec::new(),
                    env_vars: BTreeMap::new(),
                    materialized_inputs: BTreeMap::new(),
                    outputs: BTreeMap::from([(
                        "content".to_string(),
                        CapturedOutputV3 {
                            hash: Hash::from_content(b"bytes"),
                            capture: Some(spec.clone()),
                        },
                    )]),
                },
            )]),
            aux: AuxDataV3 {
                tool_call_instance_counter: 0,
                conductor_gc_epoch: ImpureTimestampV3(0),
                instances: BTreeMap::from([(
                    Hash::from_content(b"instance"),
                    InstanceAuxV3 { last_referenced_at: ImpureTimestampV3(7) },
                )]),
            },
        };

        let first = serde_json::to_vec_pretty(&state).expect("encode v3");
        let decoded: ConductorStateV3 = serde_json::from_slice(&first).expect("decode v3");
        assert_eq!(decoded, state, "the decoded document must equal the encoded one");
        let second = serde_json::to_vec_pretty(&decoded).expect("re-encode v3");
        assert_eq!(second, first, "the wire shape must be a byte-identical fixed point");

        let instance = decoded.tool_call_instances.values().next().expect("one instance");
        assert_eq!(instance.outputs["content"].capture.as_ref(), Some(&spec));
    }

    /// An unknown field in a V3 document is rejected, naming the field.
    ///
    /// The state file is machine-managed and rewritten wholesale, so a field
    /// this build does not know is either a newer build's or a hand edit.
    /// Ignoring it would decode a document whose content the reader cannot
    /// carry forward, and the next save would delete what it never understood.
    #[test]
    fn a_v3_document_with_an_unknown_field_is_rejected() {
        // A literal, not a `format!` template: the document carries no
        // interpolated identity, and the field under test is the extra key.
        let json = r#"{
            "version": 3,
            "also_unknown": 1,
            "tool_call_instances": {},
            "aux": {
                "tool_call_instance_counter": 0,
                "conductor_gc_epoch": 0,
                "instances": {}
            }
        }"#;

        let error = serde_json::from_str::<ConductorStateV3>(json)
            .expect_err("an unknown top-level field must not decode away");
        let message = error.to_string();
        assert!(
            message.contains("also_unknown"),
            "the error must name the field it rejected: {message}",
        );
    }

    /// A V2 document that recorded no outputs migrates to an empty V3 output
    /// map.
    ///
    /// The migration maps entries one at a time, so the empty case is the one
    /// no per-entry assertion reaches: a reader that assumed it could take
    /// "the first entry" would pass every other migration test and still drop
    /// an instance's identity here.
    #[test]
    fn a_v2_document_with_no_outputs_migrates_to_an_empty_map() {
        let json = format!(
            r#"{{
                "version": 2,
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

        let state = super::super::decode_state_json(json.as_bytes()).expect("the v2 file migrates");
        let instance = state.tool_call_instances.values().next().expect("one instance");
        assert!(
            instance.outputs.is_empty(),
            "an instance that recorded no outputs must migrate with none: {:?}",
            instance.outputs,
        );
    }

    /// A V3 document that omits `step_ids` decodes with an empty set rather than
    /// failing.
    ///
    /// V3's own wire type keeps the field `#[serde(default)]`: `step_ids` is
    /// provenance, so a document written without it — an older writer's record, or
    /// a hand-edited state — still describes a usable instance. Dropping the
    /// attribute would make V3 decoding refuse a document it can otherwise read,
    /// and no other V3 test would notice because they all write the field.
    #[test]
    fn a_v3_document_without_step_ids_decodes_to_an_empty_set() {
        let json = format!(
            r#"{{
                "version": 3,
                "tool_call_instances": {{
                    "{INSTANCE_KEY}": {{
                        "instance_key": "{INSTANCE_KEY}",
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 0,
                        "command_args": [],
                        "env_vars": {{}},
                        "materialized_inputs": {{}},
                        "outputs": {{
                            "content": {{ "hash": "{OUTPUT_HASH}", "capture": null }}
                        }}
                    }}
                }},
                "aux": {{
                    "tool_call_instance_counter": 0,
                    "conductor_gc_epoch": 0,
                    "instances": {{}}
                }}
            }}"#
        );

        let state = super::super::decode_state_json(json.as_bytes())
            .expect("a v3 document predating step_ids still decodes");
        let instance = state.tool_call_instances.values().next().expect("one instance");
        assert!(
            instance.step_ids.is_empty(),
            "an instance recorded before reuse was tracked names no step: {:?}",
            instance.step_ids,
        );
    }

    /// A capture spec whose `save` is the default `True` omits the field on the
    /// wire and decodes back to `True`.
    ///
    /// `save` is the only field of a spec that a step may set on a
    /// tool-declared name, so its omission has to mean "persist" rather than
    /// "not recorded": a decoder that read the absent field as `False` would
    /// silently stop persisting every default-saved capture.
    #[test]
    fn a_spec_with_the_default_save_mode_round_trips_without_writing_it() {
        let spec = OutputCaptureSpecV3 {
            name: "content".to_string(),
            capture: "file:out.mkv".to_string(),
            save: SaveModeV3::True,
            allow_empty: true,
            include_topmost_folder: true,
        };
        let state = ConductorStateV3 {
            version: CONDUCTOR_STATE_VERSION_V3,
            tool_call_instances: BTreeMap::from([(
                Hash::from_content(b"instance"),
                ToolCallInstanceV3 {
                    instance_key: Hash::from_content(b"instance"),
                    tool_call_id: "ffmpeg@v1".to_string(),
                    impure: false,
                    executed_at: ImpureTimestampV3(5),
                    step_ids: BTreeSet::from(["step-0".to_string()]),
                    command_args: Vec::new(),
                    env_vars: BTreeMap::new(),
                    materialized_inputs: BTreeMap::new(),
                    outputs: BTreeMap::from([(
                        "content".to_string(),
                        CapturedOutputV3 {
                            hash: Hash::from_content(b"bytes"),
                            capture: Some(spec),
                        },
                    )]),
                },
            )]),
            aux: AuxDataV3::default(),
        };

        let encoded = serde_json::to_value(&state).expect("encode v3 as a JSON value");
        let encoded_spec = encoded["tool_call_instances"]
            .as_object()
            .and_then(|instances| instances.values().next())
            .map(|instance| &instance["outputs"]["content"]["capture"])
            .expect("one encoded capture spec");
        assert!(
            encoded_spec.get("save").is_none(),
            "the default save mode must be omitted rather than written: {encoded_spec:?}",
        );

        let decoded: ConductorStateV3 = serde_json::from_value(encoded).expect("decode v3");
        let instance = decoded.tool_call_instances.values().next().expect("one instance");
        let capture =
            instance.outputs["content"].capture.as_ref().expect("the spec survives the round trip");
        assert_eq!(capture.save, SaveModeV3::True, "an omitted save means persist");
    }

    /// Save modes live on each captured entry now, so the per-instance aux
    /// record must carry only the GC clock. A stray `save_modes` key would
    /// mean the old mirroring survived the schema change.
    #[test]
    fn the_encoded_aux_record_carries_no_save_modes() {
        let state = ConductorStateV3 {
            version: CONDUCTOR_STATE_VERSION_V3,
            tool_call_instances: BTreeMap::new(),
            aux: AuxDataV3 {
                tool_call_instance_counter: 0,
                conductor_gc_epoch: ImpureTimestampV3(0),
                instances: BTreeMap::from([(
                    Hash::from_content(b"instance"),
                    InstanceAuxV3 { last_referenced_at: ImpureTimestampV3(7) },
                )]),
            },
        };
        let value: serde_json::Value =
            serde_json::to_value(&state).expect("encode v3 as a JSON value");
        let aux = value["aux"]["instances"]
            .as_object()
            .and_then(|instances| instances.values().next())
            .and_then(serde_json::Value::as_object)
            .expect("one aux record");
        assert!(aux.get("save_modes").is_none(), "aux must not mirror save modes: {aux:?}");
        assert_eq!(aux.get("last_referenced_at"), Some(&json!(7)));
    }
}
