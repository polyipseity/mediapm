//! Property tests for in-memory CAS round-trip correctness.
//!
//! Randomized payload generation validates that put/get preserves byte identity
//! across many content sizes.

use bytes::Bytes;
use mediapm_cas::{CasApi, InMemoryCas};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn prop_in_memory_put_get_roundtrip_for_many_payloads(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..1024), 1..48)
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime build");

        runtime.block_on(async move {
            let cas = InMemoryCas::new();
            for payload in payloads {
                let original = Bytes::from(payload.clone());
                let hash = cas.put(original.clone()).await.expect("put payload");
                let restored = cas.get(hash).await.expect("get payload");
                assert_eq!(restored, original);
            }
        });
    }
}
