//! Property tests for filesystem durability across reopen cycles.
//!
//! Randomized payloads ensure persisted objects remain readable after closing
//! and reopening the filesystem backend.

use bytes::Bytes;
use mediapm_cas::{CasApi, FileSystemCas};
use proptest::prelude::*;
use tempfile::tempdir;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    #[test]
    /// Files persisted in filesystem backend must remain readable after reopen.
    fn prop_filesystem_roundtrip_survives_reopen(payload in prop::collection::vec(any::<u8>(), 0..4096)) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime build");

        runtime.block_on(async move {
            let temp = tempdir().expect("tempdir");
            let hash = {
                let cas = FileSystemCas::open_for_tests(temp.path()).await.expect("open cas");
                cas.put(Bytes::from(payload.clone())).await.expect("put payload")
            };

            let reopened = FileSystemCas::open_for_tests(temp.path()).await.expect("reopen cas");
            let restored = reopened.get(hash).await.expect("get payload");
            assert_eq!(restored, Bytes::from(payload));
        });
    }
}
