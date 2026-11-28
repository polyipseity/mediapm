//! Runtime orchestration state (volatile + CAS-persisted).
//!
//! The orchestration state tracks every tool-call instance, the captured
//! outputs each instance recorded together with the capture spec that produced
//! them, and the auxiliary metadata needed for GC and diagnostics.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use mediapm_utils::Timestamp;
use serde::{Deserialize, Serialize};

use crate::derived_hash::DerivedHash;
use crate::error::ConductorError;
use crate::keyed_hash::KeyedHash;

pub mod versions;

/// Current orchestration-state schema version.
pub(crate) const STATE_VERSION: u32 = 7;

/// Persistence policy for a CAS-referenced blob declared as external data.
///
/// Captured outputs carry their retention fact as `saved_nk`; this mode remains
/// for blobs the config names directly in `external_data`.
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

/// One step, named by the workflow it belongs to and the id it has inside it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StepRef {
    /// Workflow the step belongs to.
    #[serde(default)]
    pub workflow_id: String,
    /// Step id inside that workflow.
    pub step_id: String,
}

/// Provenance tag for an environment variable entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvVarSource {
    /// The document fixed the value as a non-empty literal.
    Literal,
    /// The host supplied the value (or the document left it empty/inherited).
    Inherited,
}

/// A recorded environment variable: hashed value and optional provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvVarEntry {
    /// Key-participating hash of the env var value.
    pub hash_ck: DerivedHash,
    /// Provenance tag, or `None` when unknown (migrated/pre-provenance).
    pub source_nk: Option<EnvVarSource>,
}

/// One captured output: capture token, nullable hash fact, and retention fact.
///
/// `allow_empty` is not part of the claim identity; a changed answer is caught
/// by the merge's presence check, not by the instance key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedOutput {
    /// Capture token/kind string (for example `file:downloads/out.mkv`).
    pub capture: String,
    /// CAS content hash, or `None` when an allowed-empty capture matched
    /// nothing.
    pub hash_nk: Option<Hash>,
    /// Whether the bytes are retained as a saved fact.
    pub saved_nk: bool,
}

/// A completed tool-call instance: the runtime artifact of one tool call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallInstance {
    /// Version whose derivation produced the stored key.
    pub key_version: u32,
    /// Highest key version attempted for migration checks.
    pub attempted_key_version_nk: u32,
    /// Logical tool-call identifier used for instance key derivation and
    /// reuse. This is the step's tool name, not the provisioned catalog map
    /// key, so a tool payload update does not change step identity.
    pub tool_call_id: String,
    /// Effective command argv / key material hashes in order.
    pub command_args: Vec<DerivedHash>,
    /// Execution environment variables (hashed), with optional provenance.
    pub env_vars_ck: BTreeMap<String, EnvVarEntry>,
    /// Inputs staged into the sandbox, keyed by destination.
    pub materialized_inputs: BTreeMap<String, Hash>,
    /// Captured output records keyed by output name.
    pub outputs: BTreeMap<String, CapturedOutput>,
    /// Whether the tool is impure (never key material).
    pub impure_nk: bool,
    /// When an operator last invalidated this instance, or explicit `None`.
    pub invalidated_at_nk: Option<Timestamp>,
    /// Execution timestamp recorded at run time (never key material).
    pub executed_at_nk: Timestamp,
    /// Conductor GC last-reference clock (never key material).
    pub last_referenced_at_nk: Timestamp,
    /// Steps that have served this instance (never key material).
    #[serde(default)]
    pub step_ids_nk: BTreeSet<StepRef>,
}

/// Full orchestration state snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConductorState {
    /// Schema version marker.
    pub version: u32,
    /// Conductor GC reference clock.
    pub conductor_gc_ran_at: Timestamp,
    /// Workflow -> step -> instance key map: sole GC seed and lookup map.
    #[serde(default)]
    pub step_instances: BTreeMap<String, BTreeMap<String, DerivedHash>>,
    /// Recorded tool-call instances keyed by instance key.
    pub tool_call_instances: BTreeMap<DerivedHash, ToolCallInstance>,
    /// Non-secret verifier for the conductor-owned instance key secret.
    ///
    /// `None` is serialized as explicit `null` and means no secret was used.
    pub instance_key_secret_verifier: Option<KeyedHash>,
}

impl ConductorState {
    /// Creates an empty initial state with the current version marker.
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            version: STATE_VERSION,
            conductor_gc_ran_at: Timestamp::default(),
            step_instances: BTreeMap::new(),
            tool_call_instances: BTreeMap::new(),
            instance_key_secret_verifier: None,
        }
    }

    /// Runs conductor GC on instances using the provided reachable key set.
    ///
    /// `reachable_keys` must be derived by the caller (T5 owns the full
    /// seed/closure/prune algorithm). This method performs the mechanical TTL
    /// retention/refreshed-clock behavior against the current v7 state shape.
    pub fn run_conductor_gc(&mut self, reachable_keys: &BTreeSet<DerivedHash>, ttl_seconds: u64) {
        let ttl_nanos = ttl_seconds.saturating_mul(1_000_000_000);
        let epoch = self.conductor_gc_ran_at;
        let epoch_nanos = epoch.as_unix_nanos();

        self.tool_call_instances.retain(|key, instance| {
            if reachable_keys.contains(key) {
                instance.last_referenced_at_nk = epoch;
                true
            } else {
                let last_ref = instance.last_referenced_at_nk.as_unix_nanos();
                let deadline = last_ref.saturating_add(ttl_nanos);
                deadline >= epoch_nanos
            }
        });
        self.conductor_gc_ran_at = Timestamp::now();
    }
}

impl Default for ConductorState {
    fn default() -> Self {
        Self::new_empty()
    }
}

/// Re-derives a tool-call instance key from command material alone.
///
/// Routes through the active v9 scheme in the caller's digest mode; callers with
/// the full work material should derive through `StepWorkMaterial::key_at`, which
/// also includes output tokens and staged inputs.
#[must_use]
pub fn derive_tool_call_instance_key(
    tool_call_id: &str,
    key_material: &[DerivedHash],
    mode: &crate::instance_key_secret::ResolvedSecretMode,
) -> DerivedHash {
    crate::instance_key::derive_instance_key(tool_call_id, key_material, mode.clone())
}

/// The step and work a settlement is merging for.
#[derive(Clone, Copy)]
pub(crate) struct SettlingStep<'a> {
    /// Id of the step whose settlement is merging.
    pub(crate) step_id: &'a str,
    /// Whether the work is impure.
    pub(crate) impure: bool,
}

/// What one fresh capture does to the entry the instance already holds.
enum MergeDecision {
    /// Insert a fresh entry.
    Insert,
    /// Replace the stored entry.
    Replace,
    /// Keep the stored entry.
    Keep,
    /// Pure work diverged for one capture identity.
    Diverge {
        /// Hash the instance recorded, or `None` when the capture was absent.
        stored: Option<Hash>,
        /// Hash the re-execution produced, or `None` when the capture was absent.
        produced: Option<Hash>,
    },
}

/// Merges one execution's captured outputs into the entries an instance already
/// holds.
///
/// Mechanical v6 adaptation: compares capture tokens (state no longer stores
/// `allow_empty`/spec objects), treats `hash_nk: None` as an empty-capture fact,
/// and preserves existing union/diagnostic-stream/impure-divergence behavior as
/// much as feasible.
///
/// `allow_empty` is not part of the claim identity; a changed answer is caught
/// by this presence check, not by the instance key.
pub(crate) fn merge_captured_outputs(
    stored: &BTreeMap<String, CapturedOutput>,
    declared: &BTreeMap<String, crate::config::OutputCaptureSpec>,
    fresh: BTreeMap<String, CapturedOutput>,
    settling: SettlingStep<'_>,
) -> Result<BTreeMap<String, CapturedOutput>, ConductorError> {
    let mut merged = stored.clone();
    for (name, spec) in declared {
        if fresh.contains_key(name) {
            continue;
        }
        let same_identity =
            merged.get(name).is_some_and(|stored| stored.capture == spec.capture.to_token());
        if same_identity {
            merged.remove(name);
        }
    }
    for (name, incoming) in fresh {
        let decision = match merged.get(&name) {
            None => MergeDecision::Insert,
            Some(existing) => {
                if existing.capture != incoming.capture {
                    MergeDecision::Replace
                } else if existing.hash_nk == incoming.hash_nk
                    || is_diagnostic_stream_token(&existing.capture)
                {
                    MergeDecision::Keep
                } else if settling.impure {
                    MergeDecision::Replace
                } else {
                    MergeDecision::Diverge { stored: existing.hash_nk, produced: incoming.hash_nk }
                }
            }
        };
        match decision {
            MergeDecision::Insert | MergeDecision::Replace => {
                merged.insert(name, incoming);
            }
            MergeDecision::Keep => {}
            MergeDecision::Diverge { stored, produced } => {
                let message = match (stored, produced) {
                    (Some(stored), Some(produced)) => format!(
                        "step '{}' captured output '{name}' as {stored} and a re-execution of the \
                         same work produced {produced}; identical work must produce identical \
                         artifacts, so a differing record means the tool is non-deterministic or two \
                         different executions were recorded under one instance key",
                        settling.step_id,
                    ),
                    (Some(stored), None) => format!(
                        "step '{}' captured output '{name}' as {stored} on the stored run and absent on \
                         re-execution; identical work must produce identical artifacts, so a differing \
                         record means the tool is non-deterministic or two different executions were \
                         recorded under one instance key",
                        settling.step_id,
                    ),
                    (None, Some(produced)) => format!(
                        "step '{}' captured output '{name}' as absent on the stored run and {produced} on \
                         re-execution; identical work must produce identical artifacts, so a differing \
                         record means the tool is non-deterministic or two different executions were \
                         recorded under one instance key",
                        settling.step_id,
                    ),
                    (None, None) => unreachable!(
                        "identical absent captures take the equality path and never diverge"
                    ),
                };
                return Err(ConductorError::Workflow(message));
            }
        }
    }
    Ok(merged)
}

/// Returns whether a capture token reads a diagnostic stream.
fn is_diagnostic_stream_token(capture: &str) -> bool {
    matches!(capture, "stdout" | "stderr")
}

#[cfg(test)]
mod tests {
    use super::{
        CapturedOutput, ConductorState, EnvVarEntry, STATE_VERSION, SettlingStep, ToolCallInstance,
        merge_captured_outputs,
    };
    use crate::config::{OutputCaptureSpec, SaveMode};
    use crate::derived_hash::DerivedHash;
    use mediapm_cas::Hash;
    use mediapm_utils::Timestamp;
    use std::collections::{BTreeMap, BTreeSet};

    fn pure_settlement() -> SettlingStep<'static> {
        SettlingStep { step_id: "s1", impure: false }
    }

    fn spec(name: &str, selector: &str, save: SaveMode) -> OutputCaptureSpec {
        OutputCaptureSpec {
            name: name.to_string(),
            capture: crate::config::CaptureKind::parse_token(selector)
                .expect("valid capture token"),
            save,
            ..OutputCaptureSpec::default()
        }
    }

    fn captured(name: &str, selector: &str, bytes: &[u8], saved_nk: bool) -> CapturedOutput {
        CapturedOutput {
            capture: spec(name, selector, SaveMode::True).capture.to_token(),
            hash_nk: Some(Hash::from_content(bytes)),
            saved_nk,
        }
    }

    fn captured_absent(name: &str, selector: &str) -> CapturedOutput {
        CapturedOutput {
            capture: spec(name, selector, SaveMode::True).capture.to_token(),
            hash_nk: None,
            saved_nk: false,
        }
    }

    fn declared_of(
        fresh: &BTreeMap<String, CapturedOutput>,
    ) -> BTreeMap<String, OutputCaptureSpec> {
        fresh
            .keys()
            .map(|name| (name.clone(), spec(name, &fresh[name].capture, SaveMode::True)))
            .collect()
    }

    #[test]
    fn a_fresh_name_is_inserted_beside_the_stored_entries() {
        let stored = BTreeMap::from([
            ("content".to_string(), captured("content", "stdout", b"A", true)),
            ("sibling_only".to_string(), captured("sibling_only", "stdout", b"S", true)),
        ]);
        let fresh = BTreeMap::from([
            ("content".to_string(), captured("content", "stdout", b"A", true)),
            ("thumbnail".to_string(), captured("thumbnail", "stderr", b"B", true)),
        ]);

        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
                .expect("identical bytes are not a conflict");

        assert_eq!(
            merged.keys().cloned().collect::<Vec<_>>(),
            vec!["content".to_string(), "sibling_only".to_string(), "thumbnail".to_string()],
        );
        assert_eq!(merged["thumbnail"].hash_nk, Some(Hash::from_content(b"B")));
        assert_eq!(merged["sibling_only"], stored["sibling_only"]);
    }

    #[test]
    fn an_identical_record_is_a_no_op() {
        let stored = BTreeMap::from([
            ("content".to_string(), captured("content", "stdout", b"A", true)),
            ("sibling_only".to_string(), captured("sibling_only", "stdout", b"S", true)),
        ]);
        let fresh =
            BTreeMap::from([("content".to_string(), captured("content", "stdout", b"A", false))]);
        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
                .expect("an identical record cannot conflict");

        assert_eq!(merged, stored, "re-capturing the same bytes keeps the stored entry");
    }

    #[test]
    fn a_differing_diagnostic_stream_keeps_the_stored_entry() {
        let stored = BTreeMap::from([
            ("stdout".to_string(), captured("stdout", "stdout", b"first run chatter", true)),
            ("stderr".to_string(), captured("stderr", "stderr", b"first run warnings", true)),
        ]);
        let fresh = BTreeMap::from([
            ("stdout".to_string(), captured("stdout", "stdout", b"second run chatter", true)),
            ("stderr".to_string(), captured("stderr", "stderr", b"second run warnings", true)),
        ]);

        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
                .expect("diagnostic chatter cannot contradict the work");

        assert_eq!(merged, stored);
    }

    #[test]
    fn different_process_codes_under_one_identity_are_rejected() {
        let stored = BTreeMap::from([(
            "process_code".to_string(),
            captured("process_code", "process_code", b"0", true),
        )]);
        let fresh = BTreeMap::from([(
            "process_code".to_string(),
            captured("process_code", "process_code", b"1", true),
        )]);

        let error = merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
            .expect_err("an exit code is an outcome, so a change is divergence");

        assert!(error.to_string().contains("'process_code'"), "{error}");
    }

    #[test]
    fn a_changed_capture_identity_replaces_only_that_entry() {
        let stored = BTreeMap::from([
            ("content".to_string(), captured("content", "stdout", b"A", true)),
            ("thumbnail".to_string(), captured("thumbnail", "stdout", b"B", true)),
        ]);
        let fresh =
            BTreeMap::from([("content".to_string(), captured("content", "stderr", b"C", true))]);

        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
                .expect("an identity change is allowed");

        assert_eq!(merged["content"].capture, "stderr");
        assert_eq!(merged["content"].hash_nk, Some(Hash::from_content(b"C")));
        assert_eq!(merged["thumbnail"], stored["thumbnail"]);
    }

    #[test]
    fn different_bytes_under_one_identity_are_rejected() {
        let stored = BTreeMap::from([(
            "content".to_string(),
            captured("content", "file:a.bin", b"A", true),
        )]);
        let fresh = BTreeMap::from([(
            "content".to_string(),
            captured("content", "file:a.bin", b"C", true),
        )]);

        let error = merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
            .expect_err("identical work that produced different bytes must be rejected");
        let message = error.to_string();

        assert!(message.contains("'content'"), "{message}");
        assert!(message.contains(&Hash::from_content(b"A").to_string()), "{message}");
        assert!(message.contains(&Hash::from_content(b"C").to_string()), "{message}");
    }

    #[test]
    fn a_pure_reexecution_that_loses_an_artifact_is_a_divergence() {
        let stored = BTreeMap::from([(
            "content".to_string(),
            captured("content", "file:a.bin", b"A", true),
        )]);
        let fresh =
            BTreeMap::from([("content".to_string(), captured_absent("content", "file:a.bin"))]);

        let error = merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
            .expect_err("pure work cannot produce a hash and then nothing under the same token");
        let message = error.to_string();

        assert!(message.contains("'s1'"), "{message}");
        assert!(message.contains("'content'"), "{message}");
        assert!(message.contains(&Hash::from_content(b"A").to_string()), "{message}");
        assert!(message.contains("re-execution") && message.contains("absent"), "{message}");
    }

    #[test]
    fn a_pure_reexecution_that_gains_an_artifact_is_a_divergence() {
        let stored =
            BTreeMap::from([("content".to_string(), captured_absent("content", "file:a.bin"))]);
        let fresh = BTreeMap::from([(
            "content".to_string(),
            captured("content", "file:a.bin", b"A", true),
        )]);

        let error = merge_captured_outputs(&stored, &declared_of(&fresh), fresh, pure_settlement())
            .expect_err("pure work cannot produce nothing and then a hash under the same token");
        let message = error.to_string();

        assert!(message.contains("'s1'"), "{message}");
        assert!(message.contains("'content'"), "{message}");
        assert!(message.contains(&Hash::from_content(b"A").to_string()), "{message}");
        assert!(message.contains("stored") && message.contains("absent"), "{message}");
    }

    #[test]
    fn an_empty_fresh_map_leaves_the_stored_map_untouched() {
        let stored =
            BTreeMap::from([("content".to_string(), captured("content", "stdout", b"A", true))]);
        let merged =
            merge_captured_outputs(&stored, &BTreeMap::new(), BTreeMap::new(), pure_settlement())
                .expect("an empty capture set cannot conflict");
        assert_eq!(merged, stored);
    }

    #[test]
    fn identical_bytes_under_a_changed_identity_are_still_replaced() {
        let stored =
            BTreeMap::from([("content".to_string(), captured("content", "stdout", b"A", true))]);
        let fresh =
            BTreeMap::from([("content".to_string(), captured("content", "stderr", b"A", true))]);
        let merged =
            merge_captured_outputs(&stored, &declared_of(&fresh), fresh.clone(), pure_settlement())
                .expect("a changed capture identity is always allowed");
        assert_eq!(merged, fresh);
    }

    #[test]
    fn a_declared_name_the_execution_did_not_produce_is_dropped() {
        let stored =
            BTreeMap::from([("gamma".to_string(), captured("gamma", "file:g.txt", b"G", true))]);
        let declared =
            BTreeMap::from([("gamma".to_string(), spec("gamma", "file:g.txt", SaveMode::True))]);
        let merged = merge_captured_outputs(&stored, &declared, BTreeMap::new(), pure_settlement())
            .expect("a capture that matched nothing is not a conflict");
        assert!(merged.is_empty(), "{merged:?}");
    }

    #[test]
    fn a_name_the_execution_never_declares_is_never_dropped() {
        let stored = BTreeMap::from([
            ("alpha".to_string(), captured("alpha", "stdout", b"A", true)),
            ("sibling_only".to_string(), captured("sibling_only", "stdout", b"S", true)),
        ]);
        let declared =
            BTreeMap::from([("alpha".to_string(), spec("alpha", "stdout", SaveMode::True))]);
        let fresh =
            BTreeMap::from([("alpha".to_string(), captured("alpha", "stdout", b"A", true))]);
        let merged = merge_captured_outputs(&stored, &declared, fresh, pure_settlement())
            .expect("an undeclared name cannot be a conflict");
        assert_eq!(merged, stored);
    }

    #[test]
    fn an_impure_re_execution_replaces_a_differing_artifact() {
        let stored = BTreeMap::from([(
            "content".to_string(),
            captured("content", "file:out.bin", b"A", true),
        )]);
        let fresh = BTreeMap::from([(
            "content".to_string(),
            captured("content", "file:out.bin", b"C", true),
        )]);
        let merged = merge_captured_outputs(
            &stored,
            &declared_of(&fresh),
            fresh.clone(),
            SettlingStep { step_id: "render", impure: true },
        )
        .expect("an impure tool's re-execution cannot contradict its own bytes");
        assert_eq!(merged, fresh);
    }

    #[test]
    fn an_impure_re_execution_of_identical_bytes_is_a_no_op() {
        let stored = BTreeMap::from([
            ("content".to_string(), captured("content", "file:out.bin", b"A", true)),
            ("sibling_only".to_string(), captured("sibling_only", "stdout", b"S", true)),
        ]);
        let fresh = BTreeMap::from([(
            "content".to_string(),
            captured("content", "file:out.bin", b"A", false),
        )]);
        let merged = merge_captured_outputs(
            &stored,
            &declared_of(&fresh),
            fresh,
            SettlingStep { step_id: "render", impure: true },
        )
        .expect("identical bytes cannot conflict, impure or not");
        assert_eq!(merged, stored);
    }

    #[test]
    fn the_divergence_error_names_the_settling_step() {
        let stored = BTreeMap::from([(
            "content".to_string(),
            captured("content", "file:a.bin", b"A", true),
        )]);
        let fresh = BTreeMap::from([(
            "content".to_string(),
            captured("content", "file:a.bin", b"C", true),
        )]);
        let error = merge_captured_outputs(
            &stored,
            &declared_of(&fresh),
            fresh,
            SettlingStep { step_id: "transcode", impure: false },
        )
        .expect_err("a pure tool cannot produce two different artifacts for one input set");
        assert!(error.to_string().contains("'transcode'"), "{error}");
    }

    #[test]
    fn a_pass_stamps_the_epoch_on_every_instance_it_keeps() {
        let key = DerivedHash::Unkeyed(Hash::from_content(b"instance"));
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            key,
            ToolCallInstance {
                key_version: 8,
                attempted_key_version_nk: 8,
                tool_call_id: "echo@v1".to_string(),
                command_args: Vec::new(),
                env_vars_ck: BTreeMap::<String, EnvVarEntry>::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                impure_nk: false,
                invalidated_at_nk: None,
                executed_at_nk: Timestamp::default(),
                last_referenced_at_nk: Timestamp::from_unix_nanos(7),
                step_ids_nk: BTreeSet::new(),
            },
        );
        state.conductor_gc_ran_at = Timestamp::from_unix_nanos(42);

        state.run_conductor_gc(&BTreeSet::from([key]), 0);

        let instance = state.tool_call_instances.get(&key).expect("a reachable key survives");
        assert_eq!(instance.last_referenced_at_nk, Timestamp::from_unix_nanos(42));
    }

    #[test]
    fn state_version_is_v7() {
        assert_eq!(STATE_VERSION, 7);
        assert_eq!(ConductorState::new_empty().version, 7);
    }
}
