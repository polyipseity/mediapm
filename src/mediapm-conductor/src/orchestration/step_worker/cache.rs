//! Deterministic tool call instance-key derivation and cache-probe logic.

use std::collections::BTreeSet;

use crate::config::ImpureTimestamp;
use crate::orchestration::protocol::OrchestrationState;
use crate::state::{ResolvedInput, ToolCallInstance};

/// Derives a deterministic tool call instance key from tool + inputs + optional impure timestamp.
pub(super) fn derive_instance_key(
    tool_id: &str,
    inputs: &[ResolvedInput],
    impure_timestamp: Option<ImpureTimestamp>,
) -> String {
    use blake3;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"instance-key-v2");

    // Include tool identity so different tools with identical inputs produce different keys.
    hasher.update(tool_id.as_bytes());
    hasher.update(b"\0");

    for input in inputs {
        hasher.update(input.key.as_bytes());
        hasher.update(b"\0");
        hasher.update(input.value.as_bytes());
        hasher.update(b"\0");
    }

    if let Some(ts) = impure_timestamp {
        hasher.update(b"impure:");
        hasher.update(&ts.as_unix_nanos().to_le_bytes());
    }

    hasher.finalize().to_string()
}

/// Checks whether a cached tool call instance exists with all required outputs.
pub(super) fn probe_cache(
    instance_key: &str,
    state: &OrchestrationState,
    required_outputs: &BTreeSet<String>,
) -> (bool, Option<ToolCallInstance>) {
    if let Some(instance) = state.tool_call_instances.get(instance_key) {
        // Check that all required outputs exist.
        if required_outputs.is_empty()
            || required_outputs.iter().all(|name| instance.outputs.iter().any(|o| &o.name == name))
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
    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;

    impl proptest::arbitrary::Arbitrary for ResolvedInput {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;
        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            (any::<String>(), any::<String>())
                .prop_map(|(key, value)| ResolvedInput { key, value })
                .boxed()
        }
    }

    proptest! {
        #[test]
        fn different_inputs_produce_different_keys(
            inputs1 in proptest::collection::vec(any::<ResolvedInput>(), 1..5),
            inputs2 in proptest::collection::vec(any::<ResolvedInput>(), 1..5),
        ) {
            prop_assume!(inputs1 != inputs2);
            let key1 = derive_instance_key("test", &inputs1, None);
            let key2 = derive_instance_key("test", &inputs2, None);
            prop_assert_ne!(key1, key2);
        }

        #[test]
        fn same_inputs_produce_same_keys(
            inputs in proptest::collection::vec(any::<ResolvedInput>(), 0..10),
        ) {
            let key1 = derive_instance_key("test", &inputs, None);
            let key2 = derive_instance_key("test", &inputs, None);
            prop_assert_eq!(key1, key2);
        }
    }

    #[test]
    fn different_impure_timestamps_produce_different_keys() {
        let inputs = vec![ResolvedInput {
            key: "url".to_string(),
            value: "https://example.com".to_string(),
        }];
        let key1 = derive_instance_key("test", &inputs, Some(ImpureTimestamp::from_unix_nanos(0)));
        let key2 = derive_instance_key("test", &inputs, Some(ImpureTimestamp::from_unix_nanos(1)));
        assert_ne!(key1, key2);
    }
}
