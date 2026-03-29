//! Property tests for delta-chain reconstructability.
//!
//! Confirms both in-memory and filesystem backends reconstruct all constrained
//! chain nodes back to their original payload bytes.

use std::collections::BTreeSet;

use bytes::Bytes;
use mediapm_cas::{CasApi, FileSystemCas, InMemoryCas};
use proptest::prelude::*;
use tempfile::tempdir;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    #[test]
    fn prop_in_memory_delta_chain_roundtrip(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..256), 2..24)
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime build");

        runtime.block_on(async move {
            let cas = InMemoryCas::new();

            let mut previous = None;
            let mut expected = Vec::new();
            for payload in payloads {
                let bytes = Bytes::from(payload);
                let hash = if let Some(previous_hash) = previous {
                    cas.put_with_constraints(bytes.clone(), BTreeSet::from([previous_hash]))
                        .await
                        .expect("put constrained payload")
                } else {
                    cas.put(bytes.clone()).await.expect("put first payload")
                };

                expected.push((hash, bytes));
                previous = Some(hash);
            }

            for (hash, bytes) in expected {
                let restored = cas.get(hash).await.expect("reconstruct constrained chain payload");
                assert_eq!(restored, bytes);
            }
        });
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    #[test]
    fn prop_filesystem_delta_chain_roundtrip(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..256), 2..12)
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime build");

        runtime.block_on(async move {
            let temp = tempdir().expect("tempdir");
            let cas = FileSystemCas::open_for_tests(temp.path()).await.expect("open cas");

            let mut previous = None;
            let mut expected = Vec::new();
            for payload in payloads {
                let bytes = Bytes::from(payload);
                let hash = if let Some(previous_hash) = previous {
                    cas.put_with_constraints(bytes.clone(), BTreeSet::from([previous_hash]))
                        .await
                        .expect("put constrained payload")
                } else {
                    cas.put(bytes.clone()).await.expect("put first payload")
                };

                expected.push((hash, bytes));
                previous = Some(hash);
            }

            for (hash, bytes) in expected {
                let restored = cas.get(hash).await.expect("reconstruct constrained chain payload");
                assert_eq!(restored, bytes);
            }
        });
    }
}
