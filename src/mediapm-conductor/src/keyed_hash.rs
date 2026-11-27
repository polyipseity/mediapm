//! Keyed BLAKE3 digest used only for conductor-owned secret mode.
//!
//! A keyed digest is not a CAS content hash. It has its own textual prefix so
//! persisted state never blurs the two modes.

use std::fmt;
use std::str::FromStr;

use mediapm_cas::Hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::instance_key_secret::InstanceKeySecret;

/// Wire prefix for keyed BLAKE3 digests.
pub const KEYED_HASH_PREFIX: &str = "blake3-keyed:";

/// A 32-byte BLAKE3 digest computed with a conductor secret key.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyedHash([u8; 32]);

/// One tagged input to a keyed composite digest.
#[derive(Clone, Copy)]
pub enum KeyedHashPart<'a> {
    /// A digest that was itself keyed.
    Keyed(&'a KeyedHash),
    /// An ordinary CAS hash, tagged so it cannot be confused with keyed bytes.
    Unkeyed(&'a Hash),
}

impl KeyedHash {
    /// Hashes `bytes` with `secret` and a hardcoded purpose string.
    #[must_use]
    pub fn hash(secret: &InstanceKeySecret, purpose: &'static str, bytes: &[u8]) -> Self {
        let mut keyed = blake3::Hasher::new_keyed(secret.key_bytes());
        keyed.update(purpose.as_bytes());
        keyed.update(&[0]);
        keyed.update(bytes);
        Self(*keyed.finalize().as_bytes())
    }

    /// Hashes tagged parts in order with a fixed composite purpose.
    #[must_use]
    pub fn composite(parts: &[KeyedHashPart<'_>]) -> Self {
        let mut keyed = blake3::Hasher::new();
        keyed.update(b"mediapm/conductor/keyed hash composite/v1");
        for part in parts {
            match part {
                KeyedHashPart::Keyed(value) => {
                    keyed.update(&[1]);
                    keyed.update(value.0.as_slice());
                }
                KeyedHashPart::Unkeyed(value) => {
                    keyed.update(&[2]);
                    keyed.update(value.as_bytes());
                }
            }
        }
        Self(*keyed.finalize().as_bytes())
    }

    /// Returns the lowercase hex digest without its prefix.
    #[must_use]
    pub fn as_hex(self) -> String {
        crate::hex_codec::encode(&self.0)
    }

    /// Returns the raw digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for KeyedHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{KEYED_HASH_PREFIX}{}", crate::hex_codec::encode(&self.0))
    }
}

impl fmt::Debug for KeyedHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<keyed digest>")
    }
}

impl FromStr for KeyedHash {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let hex_part = value
            .strip_prefix(KEYED_HASH_PREFIX)
            .ok_or_else(|| format!("keyed hash must start with '{KEYED_HASH_PREFIX}'"))?;
        if hex_part.len() != 64
            || !hex_part.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("keyed hash must be 64 lowercase hex characters".to_owned());
        }
        let bytes = crate::hex_codec::decode(hex_part)?;
        let fixed: [u8; 32] =
            bytes.try_into().map_err(|_| "keyed hash must decode to 32 bytes".to_owned())?;
        Ok(Self(fixed))
    }
}

impl Serialize for KeyedHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for KeyedHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret() -> InstanceKeySecret {
        let bytes = [7u8; 32];
        InstanceKeySecret::from_bytes_for_test(bytes)
    }

    #[test]
    fn keyed_hash_uses_distinct_prefix() {
        let value = KeyedHash::hash(&secret(), "test/purpose", b"payload");
        let encoded = serde_json::to_string(&value).expect("keyed hash encodes");
        assert_eq!(encoded, format!("\"{KEYED_HASH_PREFIX}{}\"", value.as_hex()));
        assert!(!encoded.contains("\"blake3:"));
    }

    #[test]
    fn keyed_hash_is_deterministic_and_purpose_separated() {
        let secret = secret();
        let a = KeyedHash::hash(&secret, "purpose-a", b"payload");
        let b = KeyedHash::hash(&secret, "purpose-a", b"payload");
        let c = KeyedHash::hash(&secret, "purpose-b", b"payload");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn keyed_hash_rejects_unkeyed_wire_text() {
        let parsed = serde_json::from_str::<KeyedHash>(
            "\"blake3:0000000000000000000000000000000000000000000000000000000000000000\"",
        );
        assert!(parsed.is_err(), "an ordinary CAS hash is not a keyed hash");
    }
}
