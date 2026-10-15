//! Deterministic tool call instance-key derivation and cache-probe logic.

use std::collections::BTreeSet;

use mediapm_cas::Hash;

use crate::orchestration::protocol::ConductorState;
use crate::state::ToolCallInstance;

/// Checks whether a cached tool call instance exists with all required outputs.
pub(super) fn probe_cache(
    instance_key: &Hash,
    state: &ConductorState,
    required_outputs: &BTreeSet<String>,
) -> (bool, Option<ToolCallInstance>) {
    if let Some(instance) = state.tool_call_instances.get(instance_key) {
        // Check that all required outputs exist.
        if required_outputs.is_empty()
            || required_outputs.iter().all(|name| instance.outputs.contains_key(name))
        {
            return (true, Some(instance.clone()));
        }
    }
    (false, None)
}

#[cfg(feature = "proptest")]
#[cfg(test)]
mod proptests {
    use super::*;
    use crate::state::ResolvedInput;
    use crate::state::versions::derive_instance_key_v2;
    use mediapm_utils::Timestamp;
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
            let key1 = derive_instance_key_v2("test", false, 0, &args1, &[], &[]);
            let key2 = derive_instance_key_v2("test", false, 0, &args2, &[], &[]);
            prop_assert_ne!(key1, key2);
        }

        #[test]
        fn same_inputs_produce_same_keys(
            inputs in proptest::collection::vec(any::<ResolvedInput>(), 0..10),
        ) {
            let args = arg_hashes(&inputs);
            let key1 = derive_instance_key_v2("test", false, 0, &args, &[], &[]);
            let key2 = derive_instance_key_v2("test", false, 0, &args, &[], &[]);
            prop_assert_eq!(key1, key2);
        }
    }

    #[test]
    fn different_impure_timestamps_produce_different_keys() {
        let args = arg_hashes(&[ResolvedInput {
            key: "url".to_string(),
            value: "https://example.com".to_string(),
        }]);
        let key1 = derive_instance_key_v2(
            "test",
            true,
            Timestamp::from_unix_nanos(0).as_unix_nanos(),
            &args,
            &[],
            &[],
        );
        let key2 = derive_instance_key_v2(
            "test",
            true,
            Timestamp::from_unix_nanos(1).as_unix_nanos(),
            &args,
            &[],
            &[],
        );
        assert_ne!(key1, key2);
    }
}
