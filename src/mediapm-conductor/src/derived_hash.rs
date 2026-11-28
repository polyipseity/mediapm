//! Persisted digest that may be keyed or unkeyed.
//!
//! Only conductor instance-key participates in this type. CAS blobs continue
//! to use `mediapm_cas::Hash` directly.

use std::fmt;
use std::str::FromStr;

use mediapm_cas::Hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::keyed_hash::{KEYED_HASH_PREFIX, KeyedHash};

/// A digest that participates in instance identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DerivedHash {
    /// An ordinary unkeyed BLAKE3 digest.
    Unkeyed(Hash),
    /// A secret-keyed BLAKE3 digest.
    Keyed(KeyedHash),
}

impl DerivedHash {
    /// Returns the CAS hash when this digest is unkeyed.
    ///
    /// Keyed digests have no CAS-hash representation; callers that require a
    /// CAS hash must reject them rather than discarding the keying.
    #[must_use]
    pub const fn as_unkeyed(self) -> Option<Hash> {
        match self {
            Self::Unkeyed(hash) => Some(hash),
            Self::Keyed(_) => None,
        }
    }
}

impl fmt::Display for DerivedHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unkeyed(value) => value.fmt(formatter),
            Self::Keyed(value) => value.fmt(formatter),
        }
    }
}

impl From<Hash> for DerivedHash {
    fn from(value: Hash) -> Self {
        Self::Unkeyed(value)
    }
}

impl From<KeyedHash> for DerivedHash {
    fn from(value: KeyedHash) -> Self {
        Self::Keyed(value)
    }
}

impl Serialize for DerivedHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Unkeyed(value) => value.serialize(serializer),
            Self::Keyed(value) => value.serialize(serializer),
        }
    }
}

impl FromStr for DerivedHash {
    type Err = String;

    /// Parses either digest mode, using the keyed prefix to disambiguate.
    ///
    /// # Errors
    ///
    /// Returns the underlying parse error for a malformed digest.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.starts_with(KEYED_HASH_PREFIX) {
            value.parse::<KeyedHash>().map(Self::Keyed)
        } else {
            value.parse::<Hash>().map(Self::Unkeyed).map_err(|err| err.to_string())
        }
    }
}

impl<'de> Deserialize<'de> for DerivedHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance_key_secret::InstanceKeySecret;
    use crate::keyed_hash::KeyedHash;

    #[test]
    fn derived_hash_round_trips_keyed_and_unkeyed() {
        let plain = DerivedHash::Unkeyed(Hash::from_content(b"plain"));
        let keyed = DerivedHash::Keyed(KeyedHash::hash(
            &InstanceKeySecret::from_bytes_for_test([3; 32]),
            "test",
            b"payload",
        ));
        for value in [plain, keyed] {
            let encoded = serde_json::to_string(&value).expect("derived hash encodes");
            let decoded =
                serde_json::from_str::<DerivedHash>(&encoded).expect("derived hash decodes");
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn derived_hash_rejects_foreign_prefix() {
        let parsed = serde_json::from_str::<DerivedHash>("\"sha256:deadbeef\"");
        assert!(parsed.is_err());
    }
}
