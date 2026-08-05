//! Deterministic tool call instance-key derivation and cache-probe logic.

use std::collections::BTreeSet;

use mediapm_cas::Hash;

use crate::orchestration::protocol::OrchestrationState;
use crate::state::ToolCallInstance;

/// Checks whether a cached tool call instance exists with all required outputs.
pub(super) fn probe_cache(
    instance_key: &Hash,
    state: &OrchestrationState,
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
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            (any::<String>(), any::<String>())
                .prop_map(|(key, value)| ResolvedInput { key, value })
                .boxed()
        }
    }

    /// Maps resolved inputs to deterministic command-arg hashes (the v2 key
    /// takes content hashes, not raw values).
    fn arg_hashes(inputs: &[ResolvedInput]) -> Vec<Hash> {
        inputs.iter().map(|i| Hash::from_content(i.value.as_bytes())).collect()
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
