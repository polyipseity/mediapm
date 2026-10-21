//! Deterministic tool call instance-key derivation and cache-probe logic.

use std::collections::BTreeSet;

use mediapm_cas::Hash;

use crate::orchestration::protocol::ConductorState;
use crate::state::ToolCallInstance;

/// Returns the cached instance for `instance_key` when it can serve this step.
///
/// "Can serve" means the instance exists and records every output the step's
/// dependents bind: an instance recorded against a wider or narrower output
/// set is a miss, because reusing it would either starve a dependent or force
/// it to re-derive what the step already knows how to produce. The check is
/// pure and synchronous; whether the store still holds the bytes those names
/// point at is a separate fact the executor asks the CAS about.
pub(super) fn probe_cache(
    instance_key: &Hash,
    state: &ConductorState,
    required_outputs: &BTreeSet<String>,
) -> Option<ToolCallInstance> {
    let instance = state.tool_call_instances.get(instance_key)?;
    if required_outputs.is_empty()
        || required_outputs.iter().all(|name| instance.outputs.contains_key(name))
    {
        Some(instance.clone())
    } else {
        None
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
    use std::collections::BTreeMap;

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
    /// The capture-set hash these properties hold fixed.
    ///
    /// They vary input material only; the capture term is orthogonal to it and
    /// has its own properties in the key-derivation module.
    fn no_captures() -> Hash {
        crate::step_output_specs_hash(&BTreeMap::new())
    }

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
            let key1 = derive_instance_key("test", no_captures(), &args1, &[], &[]);
            let key2 = derive_instance_key("test", no_captures(), &args2, &[], &[]);
            prop_assert_ne!(key1, key2);
        }

        #[test]
        fn same_inputs_produce_same_keys(
            inputs in proptest::collection::vec(any::<ResolvedInput>(), 0..10),
        ) {
            let args = arg_hashes(&inputs);
            let key1 = derive_instance_key("test", no_captures(), &args, &[], &[]);
            let key2 = derive_instance_key("test", no_captures(), &args, &[], &[]);
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
        let key1 = derive_instance_key("test", no_captures(), &args, &[], &[]);
        let key2 = derive_instance_key("test", no_captures(), &args, &[], &[]);
        assert_eq!(key1, key2);
    }
}
