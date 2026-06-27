//! Content-addressed hash (blake3, 32 bytes).
//!
//! Storage encoding uses the [`multihash`] crate for proper unsigned-varint
//! encoding: `[code: varint(0x1e)][digest_len: varint(0x20)][digest: 32 bytes]`.

use multihash::Multihash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

pub const HASH_SIZE: usize = 32;

/// Multihash code for blake3.
const BLAKE3_MULTICODEC: u64 = 0x1e;

/// Errors parsing a hash from encoded multihash bytes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HashParseError {
    /// Invalid multihash encoding.
    #[error("invalid multihash encoding: {0}")]
    Multihash(String),
}

/// A content-addressed hash (blake3, 32 bytes).
///
/// Derives [`Ord`] so hashes can be used in sorted collections. The ordering
/// is lexicographic on the underlying bytes — meaningful for set membership
/// and tree structures but not for content priority.
///
/// Serializes as a human-readable string `"blake3:hexdigest"` (e.g.
/// `"blake3:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"`)
/// for compatibility with Nickel config/state documents.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Hash([u8; HASH_SIZE]);

impl Hash {
    /// Compute the hash of arbitrary bytes.
    #[must_use]
    pub fn from_content(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    /// Construct from raw 32-byte array.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; HASH_SIZE]) -> Self {
        Self(bytes)
    }

    /// Return a reference to the raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; HASH_SIZE] {
        &self.0
    }

    /// Hex-encode the hash.
    #[must_use]
    pub fn to_hex(&self) -> String {
        blake3::Hash::from(self.0).to_hex().to_string()
    }

    /// Return the hash of empty content (`blake3(b"")`). A well-known sentinel:
    /// always present in the store (seeded on init) and protected from deletion.
    #[must_use]
    pub const fn empty() -> Self {
        // Precomputed blake3-256 hash of the empty byte string.
        const EMPTY_HASH_BYTES: [u8; HASH_SIZE] = [
            0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc,
            0xc9, 0x49, 0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7, 0xcc, 0x9a, 0x93, 0xca,
            0xe4, 0x1f, 0x32, 0x62,
        ];
        Self(EMPTY_HASH_BYTES)
    }

    /// Composite hash from a sequence of hashes.
    ///
    /// Produces `blake3(h₁.as_bytes() ‖ h₂.as_bytes() ‖ …)`. Deterministic —
    /// same sequence always produces same composite hash.
    ///
    /// This is used by [`Conductor`] and [`MediaPM`] for [`StringList`] identity where
    /// element hashes are already stored as individual CAS objects.
    #[must_use]
    pub fn composite(hashes: &[Hash]) -> Self {
        let mut inner = blake3::Hasher::new();
        for h in hashes {
            inner.update(h.as_bytes());
        }
        Self(*inner.finalize().as_bytes())
    }

    /// Return the raw digest bytes (same as [`as_bytes`](Self::as_bytes)).
    #[must_use]
    pub fn digest(&self) -> &[u8] {
        &self.0
    }

    /// Return the multihash codec code (blake3 = `0x1e`).
    #[must_use]
    pub fn code(&self) -> u64 {
        BLAKE3_MULTICODEC
    }

    /// Return the multihash digest length in bytes (always 32 for blake3-256).
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn size(&self) -> u8 {
        HASH_SIZE as u8
    }

    /// Encode the hash as multihash storage bytes using the official [`multihash`] crate.
    ///
    /// # Panics
    ///
    /// Panics if the blake3-256 digest does not fit in a [`Multihash<32>`].
    /// This is a programming error; the digest is always 32 bytes.
    ///
    /// Output uses proper unsigned-varint encoding: `[code: varint(0x1e)][digest_len: varint(0x20)][digest: 32 bytes]`.
    #[must_use]
    pub fn storage_bytes(&self) -> Vec<u8> {
        Multihash::<32>::wrap(BLAKE3_MULTICODEC, &self.0)
            .expect("blake3-256 digest always fits in Multihash<32>")
            .to_bytes()
    }

    /// Parse a hash from multihash-encoded bytes, returning the hash and bytes consumed.
    ///
    /// # Errors
    ///
    /// Returns [`HashParseError::Multihash`] if the bytes do not contain a
    /// valid multihash or the codec is not blake3.
    ///
    /// Uses [`Multihash::read`] instead of [`Multihash::from_bytes`] because the multihash
    /// may be embedded in a larger buffer with trailing data.
    pub fn from_storage_bytes_with_len(bytes: &[u8]) -> Result<(Self, usize), HashParseError> {
        let mut cursor = bytes;
        let mh = Multihash::<32>::read(&mut cursor)
            .map_err(|e| HashParseError::Multihash(e.to_string()))?;
        if mh.code() != BLAKE3_MULTICODEC {
            return Err(HashParseError::Multihash(format!(
                "expected blake3 code {BLAKE3_MULTICODEC:#x}, got {:#x}",
                mh.code()
            )));
        }
        let consumed = bytes.len() - cursor.len();
        let mut arr = [0u8; HASH_SIZE];
        arr.copy_from_slice(mh.digest());
        Ok((Self(arr), consumed))
    }
}

impl Serialize for Hash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("blake3:{}", self.to_hex()))
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let hex = s.strip_prefix("blake3:").ok_or_else(|| {
            serde::de::Error::custom(format!("expected hash in format 'blake3:hex', got '{s}'"))
        })?;
        let bytes = blake3::Hash::from_hex(hex)
            .map_err(|e| serde::de::Error::custom(format!("invalid hex digest: {e}")))?;
        Ok(Self(*bytes.as_bytes()))
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", self.to_hex())
    }
}

impl fmt::Display for Hash {
    /// Returns the canonical human-readable format `"blake3:hexdigest"`,
    /// matching the [`Serialize`](Serialize) representation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "blake3:{}", self.to_hex())
    }
}

impl From<[u8; HASH_SIZE]> for Hash {
    fn from(bytes: [u8; HASH_SIZE]) -> Self {
        Self(bytes)
    }
}

impl From<Hash> for [u8; HASH_SIZE] {
    fn from(hash: Hash) -> Self {
        hash.0
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl FromStr for Hash {
    type Err = HashParseError;

    /// Parse a hash from a string.
    ///
    /// Accepts either:
    /// - `"blake3:hexdigest"` (the canonical human-readable serialization)
    /// - plain 64-char lowercase hex (as produced by [`to_hex`](Hash::to_hex))
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Strip optional "blake3:" prefix used in config/state serialization.
        let hex = s.strip_prefix("blake3:").unwrap_or(s);
        let bytes =
            blake3::Hash::from_hex(hex).map_err(|e| HashParseError::Multihash(e.to_string()))?;
        Ok(Self(*bytes.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_content_roundtrip() {
        let data = b"hello world";
        let hash = Hash::from_content(data);
        let hex = hash.to_hex();
        assert_eq!(hex.len(), 64);
        assert_ne!(hex, "0".repeat(64));
        // Same content → same hash
        assert_eq!(Hash::from_content(data), hash);
    }

    #[test]
    fn different_content_different_hash() {
        assert_ne!(Hash::from_content(b"foo"), Hash::from_content(b"bar"),);
    }

    #[test]
    fn empty_equals_hash_of_empty_content() {
        let z = Hash::empty();
        assert_eq!(z, Hash::from_content(b""));
    }

    #[test]
    fn empty_has_expected_hex() {
        assert_eq!(
            Hash::empty().to_hex(),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
    }

    #[test]
    fn from_into_array() {
        let arr = [42u8; 32];
        let h = Hash::from(arr);
        let back: [u8; 32] = h.into();
        assert_eq!(arr, back);
    }
}
