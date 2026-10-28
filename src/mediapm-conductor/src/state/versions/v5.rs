//! V5 wire format for orchestration state persistence.
//!
//! V5 makes the record's map key its only identity and moves the facts that
//! describe a record onto the record:
//!
//! - [`ConductorStateV5::tool_call_instances`] — the map key is the instance
//!   key. V4 also wrote the key into each record, so one document held two
//!   copies of an identity and a hand edit could make them disagree. There is
//!   no `instance_key` field here to disagree with.
//! - [`ToolCallInstanceV5::last_referenced_at`] — the GC reference clock, moved
//!   off `aux.instances`. An instance and the clock its eviction is measured
//!   against are now written and deleted as one value.
//! - [`ToolCallInstanceV5::step_ids`] — a [`StepRefV5`] names the workflow as
//!   well as the step, because step ids are unique only inside a workflow and
//!   a bare id reached work another workflow recorded.
//! - [`ToolCallInstanceV5::command_args`] — hashes alone, in argv order. They
//!   are the key material, so the recorded argv and the key composition are one
//!   list; V4's per-argument `deterministic` flag is gone.
//! - `aux` keeps the GC epoch and the per-workflow GC seed, and nothing else:
//!   the instance counter was read by no consumer, and the per-instance map is
//!   now a field on the instance.
//!
//! This module owns the V5 version marker, the V5 wire types, and the
//! V4→V5 migration. It must not import unversioned runtime state from
//! `super::super`; the bridge to runtime types lives in [`super`].
//!
//! Every V5 wire type that can carry a field denies unknown fields, matching
//! V4: a document this build wrote is the only V5 document it reads, and a
//! field it cannot name is either a newer build's or a hand edit, so decoding
//! it away would let the next save delete it. Two V5 wire types cannot carry
//! the attribute at all: [`SaveModeV5`] is a fieldless enum, and
//! [`WireTimestampV5`] is a one-field tuple newtype, so serde gives neither a
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

use super::v4;

/// V5 schema version marker.
pub(crate) const CONDUCTOR_STATE_VERSION_V5: u32 = 5;

/// Returns whether `marker` matches V5.
#[must_use]
pub(crate) const fn is_conductor_state_version_v5(marker: u32) -> bool {
    marker == CONDUCTOR_STATE_VERSION_V5
}

// ---------------------------------------------------------------------------
// V5 wire format types
// ---------------------------------------------------------------------------
// These mirror the runtime `ConductorState` family but stay version-local, so
// an older version's shape can never be reshaped by a later edit. The
// `mod.rs` bridge converts between V5 wire types and the runtime types.

/// V5 persistence mode for one capture spec.
///
/// Mirrors the runtime `SaveMode` spelling (`"false"`, `"true"`, `"full"`) so a
/// spec written by one representation decodes into the other unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SaveModeV5 {
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
const fn is_save_mode_true_v5(mode: &SaveModeV5) -> bool {
    matches!(mode, SaveModeV5::True)
}

/// V5 capture specification for one output name.
///
/// The shape is the runtime `OutputCaptureSpec` as persisted with the entry
/// that recorded it. `save` is omitted when it is the default, matching the
/// config document's own serialization so a spec has one spelling everywhere.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputCaptureSpecV5 {
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
    #[serde(default, skip_serializing_if = "is_save_mode_true_v5")]
    pub save: SaveModeV5,
    /// Whether an empty capture result is acceptable.
    #[serde(default)]
    pub allow_empty: bool,
    /// Whether `folder:` and `folder_regex:` listings include the topmost
    /// folder name.
    #[serde(default = "default_include_topmost_folder_v5")]
    pub include_topmost_folder: bool,
}

/// Default for [`OutputCaptureSpecV5::include_topmost_folder`].
const fn default_include_topmost_folder_v5() -> bool {
    true
}

/// V5 timestamp (nanoseconds since Unix epoch).
///
/// A tuple struct has no map for serde to reject unknown keys in, so the
/// strictness lives on the structs that contain it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WireTimestampV5(
    /// Nanoseconds since Unix epoch.
    pub u64,
);

/// One step, named by the workflow it belongs to and the id it has inside it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StepRefV5 {
    /// Workflow the step belongs to.
    ///
    /// Empty on a ref migrated from V4, whose wire shape could not name one.
    #[serde(default)]
    pub workflow_id: String,
    /// Step id inside that workflow.
    pub step_id: String,
}

/// V5 captured output: content hash plus the spec that produced it.
///
/// `capture: None` is the migration's "spec unknown" sentinel: an entry
/// carried over from V2 recorded no spec, and no declared spec equals `None`,
/// so such an entry never satisfies a capture request that names a spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapturedOutputV5 {
    /// CAS content hash of the captured bytes.
    pub hash: Hash,
    /// The capture spec that produced this entry, or `None` when the record
    /// predates spec storage.
    pub capture: Option<OutputCaptureSpecV5>,
}

/// V5 tool-call instance, filed under its own content-addressed key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolCallInstanceV5 {
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
    /// Omitted from the wire when absent, so a record written before any
    /// invalidation stays byte-identical. The consumer is the step-worker probe
    /// alone; see the runtime field's documentation for why that is the whole
    /// mechanism.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalidated_at: Option<WireTimestampV5>,
    /// Execution timestamp. Provenance only; never key material.
    pub executed_at: WireTimestampV5,
    /// Conductor GC last-reference clock.
    ///
    /// Required: every writer sets it, so a document that omits it is foreign
    /// or damaged rather than old, and defaulting it would age the record out on
    /// the next pass.
    pub last_referenced_at: WireTimestampV5,
    /// Steps that have served this instance, qualified by workflow.
    ///
    /// `#[serde(default)]` so a record that predates the field — which only a
    /// migrated document can be — reads as naming no step rather than failing.
    #[serde(default)]
    pub step_ids: BTreeSet<StepRefV5>,
    /// Effective command argv recorded at execution time, in order.
    pub command_args: Vec<Hash>,
    /// Execution environment variables, hashed.
    pub env_vars: BTreeMap<String, Hash>,
    /// Inputs the step staged into its sandbox, keyed by sandbox-relative
    /// destination.
    pub materialized_inputs: BTreeMap<String, Hash>,
    /// Captured output records keyed by output name.
    pub outputs: BTreeMap<String, CapturedOutputV5>,
}

/// V5 auxiliary metadata: the GC epoch and the GC seed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuxDataV5 {
    /// Conductor GC epoch: the clock reference ages are measured from.
    pub conductor_gc_epoch: WireTimestampV5,
    /// Instance keys each workflow's latest successful run produced or hit,
    /// keyed by workflow id: the GC reachability seed.
    #[serde(default)]
    pub live_instances: BTreeMap<String, BTreeSet<Hash>>,
}

impl Default for AuxDataV5 {
    fn default() -> Self {
        Self { conductor_gc_epoch: WireTimestampV5(0), live_instances: BTreeMap::new() }
    }
}

/// V5 orchestration state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConductorStateV5 {
    /// Schema version marker; always [`CONDUCTOR_STATE_VERSION_V5`] when this
    /// build writes, and the dispatch key a reader selects on.
    pub version: u32,
    /// Recorded tool-call instances, each filed under its own instance key.
    pub tool_call_instances: BTreeMap<Hash, ToolCallInstanceV5>,
    /// Auxiliary metadata: the GC epoch and the GC seed.
    pub aux: AuxDataV5,
}

// ---------------------------------------------------------------------------
// V4 → V5 migration
// ---------------------------------------------------------------------------

/// Migrates a V4 state into the V5 wire shape.
///
/// Three judgement calls, each erring toward re-execution rather than reuse:
///
/// - The V4 `instance_key` field is **dropped without comparison**. V4 wrote it
///   equal to the map key by construction, so a mismatch can only be a hand
///   edit, and failing a load over a cache entry's redundancy is worse than
///   ignoring it: the map key is what every reader uses.
/// - `aux.instances[key].last_referenced_at` moves onto the record, and a
///   record with no aux entry gets [`WireTimestampV5::default`] — the epoch,
///   which is exactly what the V4 reader's `map_or(0, …)` treated a missing
///   entry as. The eviction semantics are therefore unchanged.
/// - A V4 `step_id` becomes [`StepRefV5::default`]'s workflow plus the id: a ref
///   naming no workflow. It matches no invalidation request, which always names
///   a workflow, so a migrated record cannot be marked for work another
///   workflow recorded. The record gains a qualified ref the next time the step
///   serves it, by executing or by hitting it.
pub(crate) fn from_v4_into_v5(state: v4::ConductorStateV4) -> ConductorStateV5 {
    let v4::ConductorStateV4 { tool_call_instances, aux, .. } = state;
    let mut aux_instances = aux.instances;
    let tool_call_instances = tool_call_instances
        .into_iter()
        .map(|(key, instance)| {
            let last_referenced_at = aux_instances
                .remove(&key)
                .map_or(WireTimestampV5(0), |entry| WireTimestampV5(entry.last_referenced_at.0));
            (key, instance_into_v5(instance, last_referenced_at))
        })
        .collect();
    ConductorStateV5 {
        version: CONDUCTOR_STATE_VERSION_V5,
        tool_call_instances,
        aux: AuxDataV5 {
            conductor_gc_epoch: WireTimestampV5(aux.conductor_gc_epoch.0),
            live_instances: aux.live_instances,
        },
    }
}

/// Converts one V4 instance into its V5 spelling.
fn instance_into_v5(
    instance: v4::ToolCallInstanceV4,
    last_referenced_at: WireTimestampV5,
) -> ToolCallInstanceV5 {
    ToolCallInstanceV5 {
        tool_call_id: instance.tool_call_id,
        impure: instance.impure,
        invalidated_at: instance.invalidated_at.map(|stamp| WireTimestampV5(stamp.0)),
        executed_at: WireTimestampV5(instance.executed_at.0),
        last_referenced_at,
        step_ids: instance
            .step_ids
            .into_iter()
            .map(|step_id| StepRefV5 { workflow_id: String::new(), step_id })
            .collect(),
        command_args: instance.command_args.into_iter().map(|record| record.hash).collect(),
        env_vars: instance.env_vars.into_iter().map(|(name, record)| (name, record.hash)).collect(),
        materialized_inputs: instance
            .materialized_inputs
            .into_iter()
            .map(|(path, record)| (path, record.hash))
            .collect(),
        outputs: instance.outputs.into_iter().map(|(name, record)| (name, record.into())).collect(),
    }
}

impl From<v4::CapturedOutputV4> for CapturedOutputV5 {
    fn from(output: v4::CapturedOutputV4) -> Self {
        Self { hash: output.hash, capture: output.capture.map(Into::into) }
    }
}

impl From<v4::OutputCaptureSpecV4> for OutputCaptureSpecV5 {
    fn from(spec: v4::OutputCaptureSpecV4) -> Self {
        Self {
            name: spec.name,
            capture: spec.capture,
            save: match spec.save {
                v4::SaveModeV4::False => SaveModeV5::False,
                v4::SaveModeV4::True => SaveModeV5::True,
                v4::SaveModeV4::Full => SaveModeV5::Full,
            },
            allow_empty: spec.allow_empty,
            include_topmost_folder: spec.include_topmost_folder,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INSTANCE_KEY: &str =
        "blake3:0000000000000000000000000000000000000000000000000000000000000002";
    const ARG_HASH: &str =
        "blake3:0000000000000000000000000000000000000000000000000000000000000003";

    /// A V5 document with no `instance_key` field decodes, because the key it
    /// would carry is the map key.
    #[test]
    fn a_v5_document_decodes_without_an_instance_key_field() {
        let json = format!(
            r#"{{
                "version": 5,
                "tool_call_instances": {{
                    "{INSTANCE_KEY}": {{
                        "tool_call_id": "echo@v1",
                        "impure": false,
                        "executed_at": 7,
                        "last_referenced_at": 11,
                        "step_ids": [{{ "workflow_id": "default", "step_id": "step-0-echo" }}],
                        "command_args": ["{ARG_HASH}"],
                        "env_vars": {{}},
                        "materialized_inputs": {{}},
                        "outputs": {{}}
                    }}
                }},
                "aux": {{ "conductor_gc_epoch": 9 }}
            }}"#
        );

        let state: ConductorStateV5 = serde_json::from_str(&json).expect("a v5 document decodes");

        assert_eq!(state.version, CONDUCTOR_STATE_VERSION_V5);
        let instance = &state.tool_call_instances[&INSTANCE_KEY.parse().expect("hash")];
        assert_eq!(instance.last_referenced_at, WireTimestampV5(11));
        assert_eq!(
            instance.step_ids,
            BTreeSet::from([StepRefV5 {
                workflow_id: "default".to_string(),
                step_id: "step-0-echo".to_string(),
            }]),
        );
        assert_eq!(instance.command_args.len(), 1);
        assert!(state.aux.live_instances.is_empty());
    }

    /// A V4-only field this version cannot name is rejected, not decoded away.
    ///
    /// The alternative is silent data loss: the field is dropped on read and
    /// the next save writes the document back without it.
    #[test]
    fn a_v5_document_with_a_v4_only_field_is_rejected() {
        let json = r#"{
                "version": 5,
                "tool_call_instances": {},
                "aux": { "conductor_gc_epoch": 0, "tool_call_instance_counter": 0 }
            }"#;
        let error = serde_json::from_str::<ConductorStateV5>(json)
            .expect_err("an unknown field must not be ignored");

        assert!(
            error.to_string().contains("tool_call_instance_counter"),
            "the diagnostic must name the field, got: {error}",
        );
    }

    /// The GC seed survives the wire with its keys, so a pass after a restart
    /// sees the same reachability it saw before.
    #[test]
    fn the_live_seed_round_trips() {
        let key: Hash = INSTANCE_KEY.parse().expect("hash");
        let state = ConductorStateV5 {
            version: CONDUCTOR_STATE_VERSION_V5,
            tool_call_instances: BTreeMap::new(),
            aux: AuxDataV5 {
                conductor_gc_epoch: WireTimestampV5(0),
                live_instances: BTreeMap::from([("default".to_string(), BTreeSet::from([key]))]),
            },
        };
        let encoded = serde_json::to_vec(&state).expect("serializes");
        let decoded: ConductorStateV5 = serde_json::from_slice(&encoded).expect("decodes");

        assert_eq!(decoded.aux.live_instances.get("default"), Some(&BTreeSet::from([key])));
    }
}
