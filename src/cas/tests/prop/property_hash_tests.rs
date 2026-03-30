//! Property tests for hash encoding/decoding stability.
//!
//! Guards string and storage-byte round-trips plus the expected BLAKE3
//! multihash prefix shape.

use std::str::FromStr;

use mediapm_cas::Hash;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    /// Hash string/storage encodings must round-trip to identical hash values.
    fn prop_hash_string_and_storage_roundtrip(payload in prop::collection::vec(any::<u8>(), 0..4096)) {
        let hash = Hash::from_content(&payload);

        let text = hash.to_string();
        let parsed_from_text = Hash::from_str(&text).expect("string parse");
        prop_assert_eq!(parsed_from_text, hash);

        let storage = hash.to_storage_bytes();
        let parsed_from_storage = Hash::from_storage_bytes(&storage).expect("storage parse");
        prop_assert_eq!(parsed_from_storage, hash);
    }

    #[test]
    /// Blake3 multihash storage prefix bytes must remain stable.
    fn prop_blake3_multihash_prefix_is_stable(payload in prop::collection::vec(any::<u8>(), 0..2048)) {
        let hash = Hash::from_content(&payload);
        let storage = hash.to_storage_bytes();

        // blake3-256 multicodec code varint + digest-size varint
        prop_assert_eq!(storage[0], 0x1e);
        prop_assert_eq!(storage[1], 0x20);
        prop_assert_eq!(storage.len(), 34);
    }
}
