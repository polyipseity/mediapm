//! Cache-probe logic: whether a recorded tool-call instance can serve a step.
//!
//! Instance keys are derived in [`crate::instance_key`], from inputs alone. This
//! module answers the question that key opens: an instance exists under it, so
//! may the step that asked reuse it? The answer is a pure function of recorded
//! state — what the step binds, and how each recorded entry was captured — and
//! the executor pairs it with its own CAS presence check.

use std::collections::BTreeMap;

use mediapm_cas::Hash;

use crate::config::OutputCaptureSpec;
use crate::orchestration::protocol::ConductorState;
use crate::state::ToolCallInstance;

/// Returns whether a recorded capture names the same bytes a required capture
/// names.
///
/// The comparison itself is [`OutputCaptureSpec::same_capture_identity`], so the
/// probe, the settlement union and the same-run conflict check cannot disagree
/// about what "the same capture" means: the selector plus the two settings that
/// decide which bytes are read (`allow_empty`, `include_topmost_folder`). `save`
/// is a persistence instruction rather than a property of the bytes, so two
/// steps that share an instance and disagree only about keeping the artifact do
/// not invalidate each other; `name` is the map key on both sides, so the
/// lookup has already used it.
fn capture_identity_matches(recorded: &OutputCaptureSpec, required: &OutputCaptureSpec) -> bool {
    recorded.same_capture_identity(required)
}

/// Returns the cached instance for `instance_key` when it can serve this step.
///
/// "Can serve" means the instance exists, carries no invalidation marker, and
/// records every required name under the required capture identity. `required`
/// is the names this step binds — its dependents' references unioned with its
/// own declarations — mapped to the spec the probed step's *producible* set
/// gives each one: the merged tool-plus-step declarations plus the implicit
/// streams every execution records (`stdout`, `stderr`, `process_code`). A name
/// outside even that set never appears here, because no instance of this work
/// can hold it; the executor filters the requirement through the producible set
/// before probing.
///
/// The marker is checked before the names, and on every path: an empty
/// requirement accepts any instance under the key, so a check that ran after
/// (or inside) the name loop would leave a step that binds no output able to
/// reuse work an operator explicitly invalidated. Nothing else in the run
/// consults the marker, which is why this is the whole of the mechanism rather
/// than one gate among several.
///
/// Equality is on capture identity, not on the whole spec and not on the name
/// alone: `content` read from a file and `content` read from a folder are
/// different bytes, so a name recorded another way must not satisfy this step,
/// while `save` differs harmlessly because it changes no bytes (see
/// [`capture_identity_matches`]). A recorded `capture: None` is the v2
/// migration's "spec unknown" sentinel and never equals a declared spec, so a
/// migrated instance re-executes once and re-records under the spec it used.
///
/// The check is pure and synchronous; whether the store still holds the bytes
/// those names point at is a separate fact the executor asks the CAS about.
pub(super) fn probe_cache(
    instance_key: &Hash,
    state: &ConductorState,
    required: &BTreeMap<String, OutputCaptureSpec>,
) -> Option<ToolCallInstance> {
    let instance = state.tool_call_instances.get(instance_key)?;
    if instance.invalidated_at.is_some() {
        return None;
    }
    let satisfies_requirements = required.iter().all(|(name, spec)| {
        instance
            .outputs
            .get(name)
            .and_then(|record| record.capture.as_ref())
            .is_some_and(|recorded| capture_identity_matches(recorded, spec))
    });
    satisfies_requirements.then(|| instance.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SaveMode;
    use crate::state::{CapturedOutput, ToolCallInstance};
    use mediapm_utils::Timestamp;
    use std::collections::BTreeSet;

    /// Builds a capture spec under `name` with every optional field at its
    /// documented default, so a test varies only the selector.
    fn spec(name: &str, capture: &str) -> OutputCaptureSpec {
        OutputCaptureSpec {
            name: name.to_string(),
            capture: capture.to_string(),
            ..OutputCaptureSpec::default()
        }
    }

    /// Builds a one-instance state keyed by `key`, whose outputs are `outputs`.
    fn state_with(key: Hash, outputs: BTreeMap<String, CapturedOutput>) -> ConductorState {
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            key,
            ToolCallInstance {
                instance_key: key,
                tool_call_id: "echo@v1".to_string(),
                impure: false,
                invalidated_at: None,
                executed_at: Timestamp::default(),
                step_ids: BTreeSet::new(),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs,
            },
        );
        state
    }

    /// A required name recorded under the required spec is a hit.
    #[test]
    fn a_required_name_with_an_equal_spec_is_a_hit() {
        let key = Hash::from_content(b"instance");
        let content = spec("content", "stdout");
        let state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput {
                    hash: Hash::from_content(b"bytes"),
                    capture: Some(content.clone()),
                },
            )]),
        );

        assert!(
            probe_cache(&key, &state, &BTreeMap::from([("content".to_string(), content)]))
                .is_some(),
            "an entry recorded under the required spec must serve the step",
        );
    }

    /// The same name captured another way is a miss.
    ///
    /// `content` read from a file and `content` read from a folder are not the
    /// same bytes, so the spec — not the name — decides reuse.
    #[test]
    fn a_required_name_recorded_with_a_different_spec_is_a_miss() {
        let key = Hash::from_content(b"instance");
        let state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput {
                    hash: Hash::from_content(b"bytes"),
                    capture: Some(spec("content", "file:out.mkv")),
                },
            )]),
        );

        assert!(
            probe_cache(
                &key,
                &state,
                &BTreeMap::from([("content".to_string(), spec("content", "folder:outputs"))]),
            )
            .is_none(),
            "a name recorded under a different spec must be a miss",
        );
    }

    /// An instance an operator invalidated cannot serve a step, however well
    /// its recorded names match.
    ///
    /// The marker is the whole of explicit invalidation and this probe is its
    /// only reader, so a probe that ignored it would make every invalidation
    /// command a no-op that reports success.
    #[test]
    fn an_invalidated_instance_is_a_miss() {
        let key = Hash::from_content(b"instance");
        let content = spec("content", "stdout");
        let mut state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput {
                    hash: Hash::from_content(b"bytes"),
                    capture: Some(content.clone()),
                },
            )]),
        );
        state.tool_call_instances.get_mut(&key).expect("seeded instance").invalidated_at =
            Some(Timestamp::from_unix_nanos(1));

        assert!(
            probe_cache(&key, &state, &BTreeMap::from([("content".to_string(), content)]))
                .is_none(),
            "a marked instance must not serve a step",
        );
    }

    /// A marked instance is a miss even when the step binds nothing by name.
    ///
    /// An empty requirement accepts any instance under the key, which would
    /// otherwise be the one path around the marker: a step whose outputs are
    /// all derived from a sibling's would reuse invalidated work.
    #[test]
    fn an_invalidated_instance_is_a_miss_with_an_empty_requirement() {
        let key = Hash::from_content(b"instance");
        let mut state = state_with(key, BTreeMap::new());
        state.tool_call_instances.get_mut(&key).expect("seeded instance").invalidated_at =
            Some(Timestamp::from_unix_nanos(1));

        assert!(
            probe_cache(&key, &state, &BTreeMap::new()).is_none(),
            "an empty requirement must not accept a marked instance",
        );
    }

    /// An entry migrated from v2 records no spec, so it never matches.
    #[test]
    fn a_required_name_recorded_without_a_spec_never_matches() {
        let key = Hash::from_content(b"instance");
        let state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput { hash: Hash::from_content(b"bytes"), capture: None },
            )]),
        );

        assert!(
            probe_cache(
                &key,
                &state,
                &BTreeMap::from([("content".to_string(), spec("content", "stdout"))]),
            )
            .is_none(),
            "the unknown-spec sentinel must never satisfy a declared spec",
        );
    }

    /// A required name the instance does not record is a miss.
    #[test]
    fn a_missing_required_name_is_a_miss() {
        let key = Hash::from_content(b"instance");
        let state = state_with(key, BTreeMap::new());

        assert!(
            probe_cache(
                &key,
                &state,
                &BTreeMap::from([("content".to_string(), spec("content", "stdout"))]),
            )
            .is_none(),
            "a step that binds a name the instance never recorded cannot reuse it",
        );
    }

    /// A recorded `allow_empty` that disagrees with the required one is a miss.
    ///
    /// The flag decides whether a capture that matched nothing is recorded as
    /// an empty artifact or as no artifact at all, so the two settings describe
    /// different bytes for the same selector. An entry recorded under the other
    /// setting cannot answer this step's request.
    #[test]
    fn a_required_name_recorded_with_a_different_allow_empty_is_a_miss() {
        let key = Hash::from_content(b"instance");
        let recorded =
            OutputCaptureSpec { allow_empty: true, ..spec("content", "folder_regex:.*") };
        let required =
            OutputCaptureSpec { allow_empty: false, ..spec("content", "folder_regex:.*") };
        let state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput { hash: Hash::from_content(b"bytes"), capture: Some(recorded) },
            )]),
        );

        assert!(
            probe_cache(&key, &state, &BTreeMap::from([("content".to_string(), required)]))
                .is_none(),
            "a name recorded under a different `allow_empty` must be a miss",
        );
    }

    /// A recorded `include_topmost_folder` that disagrees with the required one
    /// is a miss.
    ///
    /// The flag rewrites the paths a folder capture lists, so the same folder
    /// read with it on and off produces different listings — and therefore
    /// different bytes under the same name.
    #[test]
    fn a_required_name_recorded_with_a_different_include_topmost_folder_is_a_miss() {
        let key = Hash::from_content(b"instance");
        let recorded = OutputCaptureSpec {
            include_topmost_folder: false,
            ..spec("content", "folder:outputs")
        };
        let required =
            OutputCaptureSpec { include_topmost_folder: true, ..spec("content", "folder:outputs") };
        let state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput { hash: Hash::from_content(b"bytes"), capture: Some(recorded) },
            )]),
        );

        assert!(
            probe_cache(&key, &state, &BTreeMap::from([("content".to_string(), required)]))
                .is_none(),
            "a name recorded under a different `include_topmost_folder` must be a miss",
        );
    }

    /// A recorded entry whose only difference is `save` still serves the step.
    ///
    /// Persistence is the field the merge ruling leaves to the step, so two steps
    /// sharing an instance can legitimately disagree about it. If the probe
    /// compared the whole spec, each would reject the entry the other recorded and
    /// re-execute to replace it, forever — while the bytes it hands downstream are
    /// the same either way. Both directions are checked because neither side is
    /// privileged: the recorded entry may persist what the step drops, or drop
    /// what the step persists.
    #[test]
    fn a_save_only_difference_still_satisfies_the_probe() {
        let persisted = OutputCaptureSpec { save: SaveMode::True, ..spec("content", "stdout") };
        let transient = OutputCaptureSpec { save: SaveMode::False, ..spec("content", "stdout") };
        let hash = Hash::from_content(b"bytes");

        let recorded_persisted = state_with(
            Hash::from_content(b"instance-1"),
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput { hash, capture: Some(persisted.clone()) },
            )]),
        );
        let recorded_transient = state_with(
            Hash::from_content(b"instance-2"),
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput { hash, capture: Some(transient.clone()) },
            )]),
        );

        assert!(
            probe_cache(
                &Hash::from_content(b"instance-1"),
                &recorded_persisted,
                &BTreeMap::from([("content".to_string(), transient)]),
            )
            .is_some(),
            "a step that drops a persistence decision must reuse the bytes that were persisted",
        );
        assert!(
            probe_cache(
                &Hash::from_content(b"instance-2"),
                &recorded_transient,
                &BTreeMap::from([("content".to_string(), persisted)]),
            )
            .is_some(),
            "a step that persists an artifact must reuse bytes recorded transiently",
        );
    }

    /// An instance that is not recorded under the key is a miss.
    #[test]
    fn an_unknown_key_is_a_miss() {
        let state = ConductorState::new_empty();

        assert!(
            probe_cache(
                &Hash::from_content(b"instance"),
                &state,
                &BTreeMap::from([("content".to_string(), spec("content", "stdout"))]),
            )
            .is_none(),
            "the probe must not invent an instance",
        );
    }

    /// A step that binds no output by name accepts the instance under its key.
    #[test]
    fn an_empty_requirement_accepts_the_instance() {
        let key = Hash::from_content(b"instance");
        let state = state_with(key, BTreeMap::new());

        assert!(
            probe_cache(&key, &state, &BTreeMap::new()).is_some(),
            "nothing to require means nothing to refuse",
        );
    }
}

#[cfg(feature = "proptest")]
#[cfg(test)]
mod proptests {
    use super::*;
    use crate::instance_key::derive_instance_key;
    use crate::state::ResolvedInput;
    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;

    impl proptest::arbitrary::Arbitrary for ResolvedInput {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;
        /// Generates literal-binding inputs only.
        ///
        /// Production drops non-deterministic bindings from the key material
        /// (`is_deterministic_binding_source` rejects `${env.`, `context.os`,
        /// `:file(` and `:folder(`), and [`arg_hashes`] now routes through that
        /// same production helper. An unconstrained `any::<String>()` can emit
        /// a value containing `${env.`, which would be filtered out; two
        /// distinct input lists could then both filter to empty and the
        /// `different_inputs_produce_different_keys` property would fail on a
        /// rejection, not on a real collision. Restricting the generators to
        /// literal shapes keeps the filter a no-op here so the properties test
        /// what they claim to test.
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            (
                proptest::string::string_regex("[a-z_]{0,6}").expect("literal input names"),
                proptest::string::string_regex("[A-Za-z0-9]{0,8}").expect("literal input values"),
            )
                .prop_map(|(key, value)| ResolvedInput { key, value })
                .boxed()
        }
    }

    /// Maps resolved inputs to instance-key hashes through the production
    /// helper, so the guard tracks the real recipe instead of duplicating it.
    ///
    /// Duplicating the composite by hand left the properties green while a
    /// flipped determinism filter in production would collapse every key; the
    /// generator above keeps every generated binding deterministic, so the
    /// production filter is exercised without rejecting any case.
    fn arg_hashes(inputs: &[ResolvedInput]) -> Vec<Hash> {
        inputs
            .iter()
            .filter_map(|input| {
                crate::instance_key::instance_key_hash_from_resolved_value_bytes(
                    &input.key,
                    &input.value,
                    input.value.as_bytes(),
                )
            })
            .collect()
    }

    proptest! {
        #[test]
        fn different_inputs_produce_different_keys(
            inputs1 in proptest::collection::vec(any::<ResolvedInput>(), 1..5),
            inputs2 in proptest::collection::vec(any::<ResolvedInput>(), 1..5),
        ) {
            prop_assume!(inputs1 != inputs2);
            let args1 = arg_hashes(&inputs1);
            let args2 = arg_hashes(&inputs2);
            let key1 = derive_instance_key("test", &args1, &[]);
            let key2 = derive_instance_key("test", &args2, &[]);
            prop_assert_ne!(key1, key2);
        }

        #[test]
        fn same_inputs_produce_same_keys(
            inputs in proptest::collection::vec(any::<ResolvedInput>(), 0..10),
        ) {
            let args = arg_hashes(&inputs);
            let key1 = derive_instance_key("test", &args, &[]);
            let key2 = derive_instance_key("test", &args, &[]);
            prop_assert_eq!(key1, key2);
        }
    }

    /// The same inputs derive the same key across wall-clock time, impure or
    /// not: the pre-revision scheme embedded `executed_at` for impure steps
    /// (covered by `impure_keys_ignore_time` in the key-derivation module);
    /// this pins the executor-level callers see the same contract.
    #[test]
    fn impure_timestamps_do_not_change_the_key() {
        let args = arg_hashes(&[ResolvedInput {
            key: "url".to_string(),
            value: "https://example.com".to_string(),
        }]);
        let key1 = derive_instance_key("test", &args, &[]);
        let key2 = derive_instance_key("test", &args, &[]);
        assert_eq!(key1, key2);
    }
}
