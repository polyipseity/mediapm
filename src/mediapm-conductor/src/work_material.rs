//! The material that identifies one step's work.
//!
//! A tool-call instance key is content-addressed over the tool call identity and
//! the key material the command names. [`StepWorkMaterial`] is that material as
//! one value: the argv or input terms, the capture token of every output name
//! the step can produce, and the inputs its selectors stage.
//!
//! Two parties derive instance keys from it, and both must derive the same one:
//! the step executor, from the values it is about to run with, and mediapm's
//! materializer, from the document alone. Building the material here, instead of
//! at each call site, is what keeps those derivations from drifting: the
//! output-spec merge, the implicit stream captures and the predicted staged
//! inputs have one definition.
//!
//! The material is input, never result: captured output hashes, staged paths,
//! timestamps and environment values are recorded on the instance and are not
//! part of the key.

use std::collections::BTreeMap;

use mediapm_cas::Hash;

use crate::config::OutputCaptureSpec;
use crate::derived_hash::DerivedHash;
use crate::instance_key::{
    V9KeyMaterial, derive_instance_key_for_version, key_material_for_tool,
    predicted_materialized_inputs,
};
use crate::instance_key_secret::ResolvedSecretMode;
use crate::orchestration::step_worker::capture::implicit_stream_specs;
use crate::state::ToolCallInstance;

/// Everything that identifies one step's work beyond its tool call id.
///
/// The three maps are exactly the terms [`crate::instance_key::derive_instance_key_for_version`]
/// reads: `command_args` for every version, and the output tokens and staged
/// inputs for v8 and v9. Command terms carry their own digest mode; staged
/// hashes stay ordinary CAS hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepWorkMaterial {
    /// Key material in order, one derived hash per key-material part: a
    /// builtin's declared inputs by name, or a command's parts in argv order.
    pub command_args: Vec<DerivedHash>,
    /// Capture token per output name this step can produce.
    pub output_tokens: BTreeMap<String, String>,
    /// Inputs the command's selectors stage, keyed by sandbox-relative
    /// destination.
    pub materialized_inputs: BTreeMap<String, Hash>,
}

impl StepWorkMaterial {
    /// Builds the material of one step from its tool, its declared outputs and
    /// its resolved input values.
    ///
    /// `command_parts`, `literal_env_vars` and `tool_outputs` are the tool's: the
    /// first two are what [`key_material_for_tool`] keys, and the third holds the
    /// capture defaults a step declaration merges into. `merged_bindings` is the
    /// step's bindings with the tool's default inputs filled in, exactly the map
    /// key derivation reads. `input_values` is each binding's resolved bytes, the
    /// same map the executor hands the template engine and the materializer
    /// reconstructs from the document.
    ///
    /// The producible set is the merged declarations plus the stream captures
    /// every execution records, so the predicted v8 output terms cover the names
    /// an instance is guaranteed to hold.
    #[must_use]
    pub fn for_step(
        command_parts: &[String],
        literal_env_vars: &BTreeMap<String, String>,
        tool_outputs: &BTreeMap<String, OutputCaptureSpec>,
        step_outputs: &BTreeMap<String, OutputCaptureSpec>,
        merged_bindings: &BTreeMap<String, String>,
        input_values: &BTreeMap<String, Vec<u8>>,
        mode: &ResolvedSecretMode,
    ) -> Self {
        let merged = merge_output_specs(tool_outputs, step_outputs);
        let producible = producible_output_specs(&merged);
        Self {
            command_args: key_material_for_tool(
                command_parts,
                input_values,
                merged_bindings,
                literal_env_vars,
                mode,
            ),
            output_tokens: output_tokens(&producible),
            materialized_inputs: predicted_materialized_inputs(
                command_parts,
                input_values,
                literal_env_vars,
            ),
        }
    }

    /// Derives this material's instance key for one recorded key version.
    ///
    /// The version decides the derivation, and the material supplies every term
    /// any known version needs: v7 derives from unkeyed `command_args` alone,
    /// v8 and v9 also read the output tokens and staged inputs. `None` means the
    /// version has no derivation this crate can evaluate.
    #[must_use]
    pub fn key_at(
        &self,
        tool_call_id: &str,
        version: u32,
        mode: &ResolvedSecretMode,
    ) -> Option<DerivedHash> {
        derive_instance_key_for_version(
            version,
            tool_call_id,
            &self.command_args,
            Some(&V9KeyMaterial {
                output_tokens: &self.output_tokens,
                materialized_inputs: &self.materialized_inputs,
            }),
            mode,
        )
    }
}

/// Returns whether a recorded instance re-derives from `material` to
/// `stored_key`.
///
/// The record's own `key_version` decides which derivation applies, so a v7
/// record and a v8 record both verify against the same material while the two
/// versions coexist. Pinning one version would reject every record of the other,
/// which is why verification follows the version the record was filed under.
///
/// An unknown recorded version has no derivation, so the record cannot be
/// verified from any material and never matches.
#[must_use]
pub fn instance_matches_work(
    stored_key: DerivedHash,
    instance: &ToolCallInstance,
    material: &StepWorkMaterial,
    mode: &ResolvedSecretMode,
) -> bool {
    material.key_at(&instance.tool_call_id, instance.key_version, mode) == Some(stored_key)
}

/// Merges tool-level output specs (defaults) with step-level output specs.
///
/// The tool owns the meaning of every artifact name it declares: its `capture`
/// is kept, and a step declaring the same name contributes its `save` and
/// `allow_empty`. A step's full declaration is authoritative only for a name
/// the tool does not declare, and a name only the tool declares is kept
/// unchanged.
///
/// The reason is that one instance is shared by every step running the same
/// work, and a name-addressed record is reusable only when the capture beside it
/// is the one the reusing step would have produced. The artifact layout is a
/// property of the tool (the sandbox path a capture reads follows from how the
/// tool writes it), so a step that could re-select a tool-declared name would
/// let two steps disagree about bytes one execution produced, and each would
/// re-execute to replace the other's record forever.
///
/// Tolerance and persistence are step-local decisions: whether this step's use
/// of the name permits an empty match, and whether its materialization keeps the
/// family, do not change which bytes the capture reads. That is also why the
/// probe compares capture identity rather than the whole spec
/// (`lookup_instance` and `satisfies_requirements` in the step worker's `cache`
/// module implement the probe and requirement logic, and v8 keys include capture
/// tokens): `save` is recorded beside the entry for
/// provenance, but a reused entry is byte-for-byte the same artifact whether the
/// reusing step persists it or not, so letting it decide a miss would put two
/// steps with different persistence on a forever-re-executing loop.
///
/// Every entry's `name` is set to the map key it is stored under. Capture records
/// `spec.name`, while the probe, the dependents' references and the materializer
/// address the key; a declaration whose name disagrees with its key would record
/// a name nothing can bind, and the step would re-execute on every sync. The key
/// is the identity, so the key wins.
///
/// The result is what the capture phase records and what a probe hit must
/// satisfy, so one instance holds every name a step with this work can bind.
/// Only the names and their capture tokens are key material, and only in v8: the
/// hashes a capture produces never enter a key.
#[must_use]
pub fn merge_output_specs(
    tool_outputs: &BTreeMap<String, OutputCaptureSpec>,
    step_outputs: &BTreeMap<String, OutputCaptureSpec>,
) -> BTreeMap<String, OutputCaptureSpec> {
    let mut merged = tool_outputs.clone();
    for (name, step_spec) in step_outputs {
        match merged.get_mut(name) {
            Some(tool_spec) => {
                tool_spec.save = step_spec.save;
                tool_spec.allow_empty = step_spec.allow_empty;
            }
            None => {
                merged.insert(name.clone(), step_spec.clone());
            }
        }
    }
    for (name, spec) in &mut merged {
        if spec.name != *name {
            spec.name.clone_from(name);
        }
    }
    merged
}

/// Names the fields of `step` that `tool` overrides for one output.
///
/// A step may restate a tool-declared name for any reason, so silence here would
/// leave a declaration that has no effect looking as though it had one: the tool
/// keeps `capture`, while the step's `save` and `allow_empty` win for that name.
/// Comparing values rather than presence is deliberate: a step that repeats the
/// tool's own selector is declaring what already holds and deserves no warning,
/// while one that names a different selector is stating an intent that is being
/// discarded.
///
/// The order is fixed so the diagnostic reads the same way every run.
///
/// `name` is not in the list. The merge rewrites it to the map key, but that is
/// normalization rather than a discarded intent: a key identifies its entry and a
/// spec's own `name` restates it, so a step whose `name` disagrees with its key
/// has a malformed spec, not an unfulfilled declaration.
#[must_use]
pub fn discarded_step_fields(
    tool: &OutputCaptureSpec,
    step: &OutputCaptureSpec,
) -> Vec<&'static str> {
    let mut discarded = Vec::new();
    if step.capture != tool.capture {
        discarded.push("capture");
    }
    discarded
}

/// Warns for every step-declared output name whose declaration the tool
/// overrides.
///
/// [`merge_output_specs`] is pure and silent, so this is the one place the
/// diagnostic lives. It fires once per discarded name, in step-declaration order,
/// with the same wording whatever caller merged: a step whose intent was
/// overridden should read the same whether the executor or a test merged it.
pub fn warn_discarded_step_fields(
    tool_outputs: &BTreeMap<String, OutputCaptureSpec>,
    step_outputs: &BTreeMap<String, OutputCaptureSpec>,
) {
    for (name, step_spec) in step_outputs {
        let Some(tool_spec) = tool_outputs.get(name) else {
            continue;
        };
        let discarded = discarded_step_fields(tool_spec, step_spec);
        if !discarded.is_empty() {
            tracing::warn!(
                "output '{name}' is declared by the tool, so the step's {} {} ignored; \
                 the tool's capture applies, and the step's allow_empty and save are taken \
                 from the step",
                discarded.join(", "),
                if discarded.len() == 1 { "is" } else { "are" },
            );
        }
    }
}

/// Returns every output name this step's work can produce, with the spec that
/// produces it.
///
/// The merged declarations are not the whole set: `stdout`, `stderr` and
/// `process_code` are recorded by every execution (see `implicit_stream_specs`
/// in the capture phase), so a dependent referencing one of them is asking for a
/// name the instance holds even though neither the tool nor the step declared
/// it. A merged-set-only filter would therefore skip the CAS check for exactly
/// the names every execution writes, so the check would be missing where the
/// recorded entry is guaranteed to exist.
///
/// A declared entry wins over the implicit one for its name, matching the
/// capture phase, which records a declared `stdout` under the declared spec and
/// skips the implicit spec.
#[must_use]
pub fn producible_output_specs(
    merged_outputs: &BTreeMap<String, OutputCaptureSpec>,
) -> BTreeMap<String, OutputCaptureSpec> {
    let mut producible = merged_outputs.clone();
    for (name, spec) in implicit_stream_specs() {
        producible.entry(name).or_insert(spec);
    }
    producible
}

/// Returns the capture token of every producible output name.
///
/// These are the v8 output-spec terms: the name and the token it is captured
/// under, in name order. The captured hashes are results and never enter.
#[must_use]
pub fn output_tokens(producible: &BTreeMap<String, OutputCaptureSpec>) -> BTreeMap<String, String> {
    producible.iter().map(|(name, spec)| (name.clone(), spec.capture.to_token())).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mediapm_cas::Hash;
    use mediapm_utils::Timestamp;

    use super::{StepWorkMaterial, instance_matches_work};
    use crate::config::documents::tool_literal_env_vars;
    use crate::config::{
        CaptureKind, OutputCaptureSpec, SaveMode, ToolKindSpec, ToolSpec, WorkflowStepSpec,
    };
    use crate::derived_hash::DerivedHash;
    use crate::instance_key::{
        INSTANCE_KEY_VERSION, V7_INSTANCE_KEY_VERSION, derive_instance_key_v7,
    };
    use crate::instance_key_secret::ResolvedSecretMode;
    use crate::state::ToolCallInstance;

    /// The command a tool runs, empty for a builtin.
    ///
    /// This mirrors how the document side reads a tool's command, so the tests
    /// build the material from the same shape a materializer holds.
    fn command_parts(tool: &ToolSpec) -> &[String] {
        match &tool.kind {
            ToolKindSpec::Executable { command, .. } => command,
            ToolKindSpec::Builtin { .. } => &[],
        }
    }

    /// A managed tool that stages one input through a `:file(...)` selector and
    /// declares one output, plus the step that runs it.
    ///
    /// The tool default input is the one the merge fills in for the step, so the
    /// fixture covers the `merged_bindings` path rather than a step that spells
    /// every binding out.
    fn managed_tool_and_step() -> (ToolSpec, WorkflowStepSpec) {
        let tool = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec![
                    "ffmpeg".to_string(),
                    "-i".to_string(),
                    "${inputs.source:file(inputs/source.mkv)}".to_string(),
                    "out.mkv".to_string(),
                ],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            name: "ffmpeg".to_string(),
            default_inputs: BTreeMap::from([(
                "format".to_string(),
                crate::config::InputBinding::String("mkv".to_string()),
            )]),
            outputs: BTreeMap::from([(
                "video".to_string(),
                OutputCaptureSpec {
                    name: "video".to_string(),
                    capture: CaptureKind::parse_token("file:out.mkv").expect("valid capture"),
                    save: SaveMode::True,
                    allow_empty: false,
                },
            )]),
            ..ToolSpec::default()
        };
        let step = WorkflowStepSpec {
            id: "step-0-transcode".to_string(),
            tool: "ffmpeg".to_string(),
            inputs: BTreeMap::from([(
                "source".to_string(),
                "${step_output.step-0-probe.primary}".to_string(),
            )]),
            outputs: BTreeMap::new(),
            runtime: Some(crate::config::StepRuntime { max_retries: Some(0) }),
            depends_on: vec!["step-0-probe".to_string()],
        };
        (tool, step)
    }

    /// Builds the step's material the way the executor does: the step's bindings
    /// with the tool's default inputs filled in, and each binding's resolved
    /// bytes.
    fn material_for(
        tool: &ToolSpec,
        step: &WorkflowStepSpec,
        source_hash: Hash,
    ) -> StepWorkMaterial {
        let mut merged_bindings = step.inputs.clone();
        for (name, binding) in &tool.default_inputs {
            merged_bindings
                .entry(name.clone())
                .or_insert_with(|| crate::instance_key::input_binding_to_string(binding));
        }
        let input_values = BTreeMap::from([
            ("source".to_string(), source_hash.to_string().into_bytes()),
            ("format".to_string(), b"mkv".to_vec()),
        ]);
        StepWorkMaterial::for_step(
            command_parts(tool),
            &tool_literal_env_vars(tool),
            &tool.outputs,
            &step.outputs,
            &merged_bindings,
            &input_values,
            &ResolvedSecretMode::Unkeyed,
        )
    }

    /// A recorded instance of `tool_call_id`, filed under `key_version`.
    ///
    /// Only the identity fields matter to matching, so the rest is the empty
    /// record an execution with no environment and no outputs would leave.
    fn recorded_instance(tool_call_id: &str, key_version: u32) -> ToolCallInstance {
        ToolCallInstance {
            key_version,
            attempted_key_version_nk: key_version,
            tool_call_id: tool_call_id.to_string(),
            command_args: Vec::new(),
            env_vars_ck: BTreeMap::new(),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            impure_nk: false,
            invalidated_at_nk: None,
            executed_at_nk: Timestamp::default(),
            last_referenced_at_nk: Timestamp::from_unix_nanos(0),
            step_ids_nk: BTreeSet::new(),
        }
    }

    /// A v8 record matches the material that produced it.
    ///
    /// This is the regression the repair is about: a v8 key is not a v7 key, so
    /// verification pinned to v7 rejects the record even though its stored key
    /// re-derives exactly.
    #[test]
    fn a_v9_record_matches_the_material_that_produced_it() {
        let (tool, step) = managed_tool_and_step();
        let material = material_for(&tool, &step, Hash::from_content(b"source-bytes"));
        let tool_call_id = step.tool.clone();
        let stored_key = material
            .key_at(&tool_call_id, INSTANCE_KEY_VERSION, &ResolvedSecretMode::Unkeyed)
            .expect("a v9 derivation is defined for complete material");
        let instance = recorded_instance(&tool_call_id, INSTANCE_KEY_VERSION);

        assert!(
            instance_matches_work(stored_key, &instance, &material, &ResolvedSecretMode::Unkeyed),
            "the key a v9 record stores must re-derive from the material that produced it",
        );
        assert_ne!(
            derive_instance_key_v7(
                &tool_call_id,
                &material
                    .command_args
                    .iter()
                    .filter_map(|value| value.as_unkeyed())
                    .collect::<Vec<_>>(),
            ),
            stored_key,
            "a v7 derivation of the same command args must differ, which is the old miss",
        );
    }

    /// A v7 record matches at its own version.
    ///
    /// Migration has not reached every stored record, and one that still carries
    /// v7 keys the command args alone, so the same material has to verify it.
    #[test]
    fn a_v7_record_matches_at_its_own_version() {
        let (tool, step) = managed_tool_and_step();
        let material = material_for(&tool, &step, Hash::from_content(b"source-bytes"));
        let tool_call_id = step.tool.clone();
        let stored_key = derive_instance_key_v7(
            &tool_call_id,
            &material
                .command_args
                .iter()
                .filter_map(|value| value.as_unkeyed())
                .collect::<Vec<_>>(),
        );
        let instance = recorded_instance(&tool_call_id, V7_INSTANCE_KEY_VERSION);

        assert_eq!(
            material.key_at(&tool_call_id, V7_INSTANCE_KEY_VERSION, &ResolvedSecretMode::Unkeyed),
            Some(DerivedHash::from(stored_key)),
            "a v7 key derives from command args alone",
        );
        assert!(
            instance_matches_work(
                DerivedHash::from(stored_key),
                &instance,
                &material,
                &ResolvedSecretMode::Unkeyed
            ),
            "a v7 record must verify against the material that reproduces its key",
        );
    }

    /// An unknown recorded version never matches.
    ///
    /// The derivation cannot be evaluated for a version this crate does not know,
    /// so no material can reproduce that record's key and the record stays
    /// unverifiable rather than matching by accident.
    #[test]
    fn an_unknown_recorded_version_never_matches() {
        let (tool, step) = managed_tool_and_step();
        let material = material_for(&tool, &step, Hash::from_content(b"source-bytes"));
        let tool_call_id = step.tool.clone();
        let stored_key = material
            .key_at(&tool_call_id, INSTANCE_KEY_VERSION, &ResolvedSecretMode::Unkeyed)
            .expect("a v9 derivation is defined for complete material");
        let instance = recorded_instance(&tool_call_id, 99);

        assert_eq!(
            material.key_at(&tool_call_id, 99, &ResolvedSecretMode::Unkeyed),
            None,
            "version 99 has no derivation"
        );
        assert!(
            !instance_matches_work(stored_key, &instance, &material, &ResolvedSecretMode::Unkeyed),
            "a record of an unknown key version cannot be verified",
        );
    }

    /// The output tokens cover the implicit streams, and a declared name wins.
    ///
    /// Every execution records `stdout`, `stderr` and `process_code` even when
    /// neither the tool nor the step declares them, so the v8 terms include them.
    /// A declared name keeps the spec it was declared with, as the capture phase
    /// records it.
    #[test]
    fn the_output_tokens_cover_the_implicit_streams() {
        let (tool, mut step) = managed_tool_and_step();
        step.outputs = BTreeMap::from([(
            "stdout".to_string(),
            OutputCaptureSpec {
                name: "stdout".to_string(),
                capture: CaptureKind::parse_token("file:console.txt").expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        )]);
        let material = material_for(&tool, &step, Hash::from_content(b"source-bytes"));

        assert_eq!(
            material.output_tokens.get("stdout").map(String::as_str),
            Some("file:console.txt"),
            "a declared name keeps its own capture token",
        );
        for implicit in ["stderr", "process_code"] {
            assert_eq!(
                material.output_tokens.get(implicit).map(String::as_str),
                Some(implicit),
                "the implicit stream capture '{implicit}' is a v8 output term",
            );
        }
        assert_eq!(
            material.output_tokens.get("video").map(String::as_str),
            Some("file:out.mkv"),
            "a tool-declared output keeps the tool's capture token",
        );
    }
}
