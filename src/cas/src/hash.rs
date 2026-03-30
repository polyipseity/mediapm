//! Hash identity model backed by the multihash standard.
//!
//! This module intentionally uses multiformats-compatible encoding so
//! algorithm code and digest size are self-describing and future-friendly.

use std::fmt::{Display, Formatter};
use std::io::Cursor;
use std::str::FromStr;
use std::sync::OnceLock;

use multihash::Multihash as RawMultihash;
use multihash_derive::MultihashDigest as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::HashParseError;

/// Fixed multihash backing capacity used by this crate.
pub(crate) const MULTIHASH_BUFFER_SIZE: usize = 64;
/// Digest width for Blake3-256.
const BLAKE3_DIGEST_SIZE: usize = 32;
/// Stable algorithm token used in textual hash representation.
const BLAKE3_NAME: &str = "blake3";
/// Encoded multihash storage width for current algorithm set.
const STORAGE_BYTES_LEN: usize = 2 + BLAKE3_DIGEST_SIZE;

/// `blake3` hasher wrapper for `multihash_derive` code table generation.
#[derive(Default)]
struct Blake3MultihashHasher {
    /// Incremental Blake3 state.
    inner: blake3::Hasher,
    /// Reusable digest output buffer required by trait API.
    digest: [u8; BLAKE3_DIGEST_SIZE],
}

/// `multihash_derive` hasher adapter implementation for Blake3.
///
/// This adapter is intentionally tiny and allocation-free:
/// - `update` forwards bytes into incremental Blake3 state,
/// - `finalize` writes digest bytes into a reusable fixed array,
/// - `reset` restores initial state for reuse in code-table calls.
impl multihash_derive::Hasher for Blake3MultihashHasher {
    fn update(&mut self, input: &[u8]) {
        self.inner.update(input);
    }

    fn finalize(&mut self) -> &[u8] {
        self.digest = *self.inner.finalize().as_bytes();
        &self.digest
    }

    fn reset(&mut self) {
        self.inner = blake3::Hasher::new();
        self.digest = [0u8; BLAKE3_DIGEST_SIZE];
    }
}

/// Supported content-hash algorithms.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    multihash_derive::MultihashDigest,
)]
#[mh(alloc_size = 64)]
/// Canonical algorithm enum consumed by parse/format/hash constructors.
pub enum HashAlgorithm {
    /// BLAKE3-256 using multicodec code `0x1e`.
    #[default]
    #[mh(code = 0x1e, hasher = Blake3MultihashHasher)]
    Blake3,
}

/// Algorithm-name/code conversion helpers used by parse/format paths.
impl HashAlgorithm {
    /// Stable multicodec algorithm code.
    #[inline]
    #[must_use]
    pub fn code(self) -> u64 {
        self.into()
    }

    /// Stable human-readable algorithm name.
    #[inline]
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Blake3 => BLAKE3_NAME,
        }
    }

    /// Resolves an algorithm from multicodec code.
    pub fn from_code(code: u64) -> Result<Self, HashParseError> {
        Self::try_from(code).map_err(|_| HashParseError::UnknownAlgorithmCode(code))
    }

    /// Resolves an algorithm from name.
    pub fn from_name(name: &str) -> Result<Self, HashParseError> {
        if name.eq_ignore_ascii_case(BLAKE3_NAME) {
            Ok(Self::Blake3)
        } else {
            Err(HashParseError::UnknownAlgorithmName(name.to_string()))
        }
    }
}

/// Algorithm-tagged content hash using multihash standard encoding.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hash {
    inner: RawMultihash<MULTIHASH_BUFFER_SIZE>,
}

/// Core constructors, parsers, and format/encoding helpers for [`Hash`].
impl Hash {
    /// Builds a hash from default algorithm digest bytes.
    #[must_use]
    pub fn from_bytes(digest: [u8; BLAKE3_DIGEST_SIZE]) -> Self {
        Self::from_parts(HashAlgorithm::default(), digest)
    }

    /// Builds a hash from algorithm and digest bytes.
    #[must_use]
    pub fn from_parts(algorithm: HashAlgorithm, digest: [u8; BLAKE3_DIGEST_SIZE]) -> Self {
        Self::from_digest_slice(algorithm, &digest)
            .expect("digest length and algorithm mapping must be valid")
    }

    /// Builds a hash from encoded multihash bytes.
    pub fn from_storage_bytes(bytes: &[u8]) -> Result<Self, HashParseError> {
        let inner = RawMultihash::<MULTIHASH_BUFFER_SIZE>::from_bytes(bytes)
            .map_err(|err| HashParseError::Multihash(err.to_string()))?;
        Self::from_multihash(inner)
    }

    /// Encodes storage bytes into an existing fixed-capacity output buffer.
    ///
    /// # Performance
    /// This method performs no heap allocation.
    pub fn write_storage_bytes(self, out: &mut [u8; STORAGE_BYTES_LEN]) {
        let encoded_len = self.inner.encoded_len();
        debug_assert!(
            encoded_len == STORAGE_BYTES_LEN,
            "encoded multihash length {} did not match fixed length {}",
            encoded_len,
            STORAGE_BYTES_LEN
        );

        let mut cursor = Cursor::new(&mut out[..]);
        let written = self
            .inner
            .write(&mut cursor)
            .expect("writing multihash into fixed stack buffer should not fail");
        debug_assert_eq!(written, STORAGE_BYTES_LEN, "multihash write length should be fixed");
    }

    /// Serializes hash into allocation-free encoded storage bytes.
    ///
    /// # Performance
    /// Uses stack-backed fixed-capacity storage and avoids heap allocation.
    #[must_use]
    pub fn storage_bytes(self) -> [u8; STORAGE_BYTES_LEN] {
        let mut bytes = [0u8; STORAGE_BYTES_LEN];
        self.write_storage_bytes(&mut bytes);
        bytes
    }

    /// Serializes hash into multihash bytes `[code-varint][size-varint][digest]`.
    ///
    /// This compatibility helper allocates; prefer [`Self::storage_bytes`] when
    /// allocation-free serialization is needed.
    #[must_use]
    pub fn to_storage_bytes(self) -> Vec<u8> {
        self.storage_bytes().to_vec()
    }

    /// Returns lowercase hex representation of full multihash storage bytes.
    #[must_use]
    pub fn to_storage_hex(self) -> String {
        let mut out = String::with_capacity(STORAGE_BYTES_LEN * 2);
        self.write_storage_hex(&mut out).expect("writing to String should not fail");
        out
    }

    /// Writes lowercase multihash storage hex into an existing formatter/writer.
    pub fn write_storage_hex(&self, writer: &mut impl std::fmt::Write) -> std::fmt::Result {
        let storage = self.storage_bytes();
        write_hex_into(writer, &storage)
    }

    /// Computes a hash for arbitrary byte content using default algorithm.
    #[must_use]
    pub fn from_content(content: &[u8]) -> Self {
        Self::from_content_with_algorithm(HashAlgorithm::default(), content)
    }

    /// Computes a hash for arbitrary byte content using the selected algorithm.
    #[must_use]
    pub fn from_content_with_algorithm(algorithm: HashAlgorithm, content: &[u8]) -> Self {
        let inner = algorithm.digest(content);
        Self::from_multihash(inner)
            .expect("digest from derive-generated codetable must satisfy algorithm invariants")
    }

    /// Returns the algorithm tag.
    #[inline]
    #[must_use]
    pub fn algorithm(self) -> HashAlgorithm {
        HashAlgorithm::from_code(self.inner.code())
            .expect("Hash contains an unknown algorithm code; constructors must validate")
    }

    /// Returns the stable algorithm name.
    #[inline]
    #[must_use]
    pub fn algorithm_name(self) -> &'static str {
        self.algorithm().name()
    }

    /// Returns digest size in bytes.
    #[inline]
    #[must_use]
    pub const fn size(self) -> u8 {
        self.inner.size()
    }

    /// Returns digest bytes.
    #[inline]
    #[must_use]
    pub fn digest(&self) -> &[u8] {
        self.inner.digest()
    }

    /// Returns digest bytes as 32-byte array for Blake3.
    #[must_use]
    pub fn as_digest_bytes(&self) -> &[u8; BLAKE3_DIGEST_SIZE] {
        self.digest().try_into().expect("blake3 digest size must be 32 bytes")
    }

    /// Compatibility alias for digest bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; BLAKE3_DIGEST_SIZE] {
        self.as_digest_bytes()
    }

    /// Returns lowercase hex representation of digest bytes only.
    #[must_use]
    pub fn to_hex(self) -> String {
        let mut out = String::with_capacity(self.digest().len() * 2);
        self.write_hex(&mut out).expect("writing to String should not fail");
        out
    }

    /// Writes lowercase digest hex directly into an existing formatter/writer.
    ///
    /// # Performance
    /// Allocation-free for callers that provide an existing output buffer.
    pub fn write_hex(&self, writer: &mut impl std::fmt::Write) -> std::fmt::Result {
        write_hex_into(writer, self.digest())
    }

    /// Returns underlying multihash code.
    #[inline]
    #[must_use]
    pub const fn code(self) -> u64 {
        self.inner.code()
    }

    /// Returns a reference to the underlying multihash value.
    #[must_use]
    pub const fn as_multihash(&self) -> &RawMultihash<MULTIHASH_BUFFER_SIZE> {
        &self.inner
    }

    /// Validates and wraps one raw multihash into crate-level [`Hash`].
    ///
    /// Validation rules:
    /// - algorithm code must map to a supported [`HashAlgorithm`],
    /// - digest width must match the algorithm's expected fixed size.
    fn from_multihash(inner: RawMultihash<MULTIHASH_BUFFER_SIZE>) -> Result<Self, HashParseError> {
        let algorithm = HashAlgorithm::from_code(inner.code())?;
        let expected_size = BLAKE3_DIGEST_SIZE;
        let got = inner.size() as usize;
        if got != expected_size {
            return Err(HashParseError::InvalidDigestSize {
                algorithm: algorithm.name(),
                expected: expected_size,
                got,
            });
        }

        Ok(Self { inner })
    }

    /// Creates a Hash from a raw multihash value, validating the algorithm and digest size.
    ///
    /// This is useful for low-level codec operations that need to construct hashes
    /// from pre-validated multihash values.
    pub fn try_from_multihash(
        inner: RawMultihash<MULTIHASH_BUFFER_SIZE>,
    ) -> Result<Self, HashParseError> {
        Self::from_multihash(inner)
    }

    /// Returns the number of bytes this hash occupies when encoded.
    ///
    /// This accounts for the varint-encoded code and size prefix.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        self.inner.encoded_len()
    }

    /// Parses a hash from encoded multihash bytes, returning the consumed byte count.
    ///
    /// This is useful when parsing formats where the exact byte length must be determined
    /// from the varint-encoded prefix.
    pub fn from_storage_bytes_with_len(bytes: &[u8]) -> Result<(Self, usize), HashParseError> {
        let mut cursor = Cursor::new(bytes);
        RawMultihash::<MULTIHASH_BUFFER_SIZE>::read(&mut cursor)
            .map_err(|e| HashParseError::Multihash(e.to_string()))?;

        let consumed =
            usize::try_from(cursor.position()).expect("cursor position should fit in usize");

        let hash = Self::from_storage_bytes(&bytes[..consumed])?;
        Ok((hash, consumed))
    }

    /// Validates digest width for `algorithm` then wraps with multihash tag.
    fn from_digest_slice(algorithm: HashAlgorithm, digest: &[u8]) -> Result<Self, HashParseError> {
        let expected = BLAKE3_DIGEST_SIZE;
        if digest.len() != expected {
            return Err(HashParseError::InvalidDigestSize {
                algorithm: algorithm.name(),
                expected,
                got: digest.len(),
            });
        }

        let inner =
            algorithm.wrap(digest).map_err(|err| HashParseError::Multihash(err.to_string()))?;
        Self::from_multihash(inner)
    }
}

/// Debug formatter that prints algorithm identity and digest hex.
impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Hash {{ algorithm: \"{}\", code: 0x{:x}, hex: \"",
            self.algorithm_name(),
            self.code()
        )?;
        self.write_hex(f)?;
        write!(f, "\" }}")
    }
}

/// User-facing stable textual formatter: `<algorithm-name>:<digest-hex>`.
impl Display for Hash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:", self.algorithm_name())?;
        self.write_hex(f)
    }
}

/// Parses textual hash forms accepted by this crate.
///
/// Supported algorithm token forms:
/// - stable name (`blake3`),
/// - decimal multicodec code,
/// - hexadecimal multicodec code (`0x...`).
impl FromStr for Hash {
    type Err = HashParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut parts = input.splitn(2, ':');
        let algo_part =
            parts.next().ok_or_else(|| HashParseError::InvalidFormat(input.to_string()))?.trim();
        let hex_part =
            parts.next().ok_or_else(|| HashParseError::InvalidFormat(input.to_string()))?.trim();

        if algo_part.is_empty() || hex_part.is_empty() {
            return Err(HashParseError::InvalidFormat(input.to_string()));
        }

        let algorithm = parse_algorithm_token(algo_part, input)?;
        let digest = decode_hex_to_array::<BLAKE3_DIGEST_SIZE>(hex_part)?;
        Ok(Self::from_parts(algorithm, digest))
    }
}

/// Serde string serializer using [`Display`] canonical representation.
impl Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Serde string deserializer using [`FromStr`] parsing rules.
impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Hash::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Returns the canonical "empty content" hash for the default algorithm.
#[must_use]
pub fn empty_content_hash() -> Hash {
    static EMPTY_HASH: OnceLock<Hash> = OnceLock::new();
    *EMPTY_HASH.get_or_init(|| Hash::from_content(&[]))
}

/// Parses algorithm token from textual, decimal-code, or hexadecimal-code forms.
fn parse_algorithm_token(
    algo_part: &str,
    original_input: &str,
) -> Result<HashAlgorithm, HashParseError> {
    let hex_code = algo_part.strip_prefix("0x").or_else(|| algo_part.strip_prefix("0X"));
    if let Some(stripped) = hex_code {
        let code = u64::from_str_radix(stripped, 16)
            .map_err(|_| HashParseError::InvalidFormat(original_input.to_string()))?;
        return HashAlgorithm::from_code(code);
    }

    if algo_part.chars().all(|ch| ch.is_ascii_digit()) {
        let code = algo_part
            .parse::<u64>()
            .map_err(|_| HashParseError::InvalidFormat(original_input.to_string()))?;
        return HashAlgorithm::from_code(code);
    }

    HashAlgorithm::from_name(algo_part)
}

/// Decodes lowercase/uppercase hexadecimal text into a fixed-size byte array.
fn decode_hex_to_array<const N: usize>(input: &str) -> Result<[u8; N], HashParseError> {
    let expected_hex_len = N * 2;
    if input.len() != expected_hex_len {
        return Err(HashParseError::InvalidHexLength {
            expected: expected_hex_len,
            got: input.len(),
        });
    }

    let mut out = [0u8; N];
    for (index, chunk) in input.as_bytes().chunks_exact(2).enumerate() {
        let hi = from_hex_nibble(chunk[0])?;
        let lo = from_hex_nibble(chunk[1])?;
        out[index] = (hi << 4) | lo;
    }
    Ok(out)
}

/// Decodes one hexadecimal nibble byte into its numeric value.
const fn from_hex_nibble(byte: u8) -> Result<u8, HashParseError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(HashParseError::InvalidHexCharacter(byte as char)),
    }
}

/// Writes bytes as lowercase hexadecimal text.
fn write_hex_into(writer: &mut impl std::fmt::Write, bytes: &[u8]) -> std::fmt::Result {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        writer.write_char(TABLE[(byte >> 4) as usize] as char)?;
        writer.write_char(TABLE[(byte & 0x0f) as usize] as char)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{Hash, HashAlgorithm};

    #[test]
    fn hash_storage_bytes_match_multihash_blake3_layout() {
        let hash = Hash::from_content(b"layout");

        let storage = hash.to_storage_bytes();

        assert_eq!(storage[0], 0x1e);
        assert_eq!(storage[1], 0x20);
        assert_eq!(storage.len(), 34);
        assert_eq!(&storage[2..], hash.as_digest_bytes());
    }

    #[test]
    fn hash_storage_hex_is_self_describing_multihash() {
        let hash = Hash::from_content(b"storage-hex-layout");
        let storage_hex = hash.to_storage_hex();

        assert_eq!(
            storage_hex.len(),
            68,
            "34-byte multihash storage should render as 68 hex chars"
        );
        assert!(
            storage_hex.starts_with("1e20"),
            "blake3 multihash storage encoding should start with code+len prefix 0x1e 0x20"
        );
    }

    #[test]
    fn hash_parsing_supports_name_decimal_and_hex_algorithm_forms() {
        let hash = Hash::from_content(b"parse");

        let by_name = Hash::from_str(&format!("blake3:{}", hash.to_hex())).expect("name parse");
        let by_decimal =
            Hash::from_str(&format!("{}:{}", HashAlgorithm::Blake3.code(), hash.to_hex()))
                .expect("decimal code parse");
        let by_hex = Hash::from_str(&format!("0x1e:{}", hash.to_hex())).expect("hex parse");

        assert_eq!(by_name, hash);
        assert_eq!(by_decimal, hash);
        assert_eq!(by_hex, hash);
    }

    #[test]
    fn hash_parsing_accepts_case_insensitive_algorithm_name() {
        let hash = Hash::from_content(b"parse-case-insensitive");

        let upper = Hash::from_str(&format!("BLAKE3:{}", hash.to_hex())).expect("uppercase name");
        let mixed = Hash::from_str(&format!("BlAkE3:{}", hash.to_hex())).expect("mixed name");

        assert_eq!(upper, hash);
        assert_eq!(mixed, hash);
    }

    #[test]
    fn hash_parsing_supports_uppercase_hex_prefix_for_algorithm_code() {
        let hash = Hash::from_content(b"parse-hex-prefix");

        let parsed =
            Hash::from_str(&format!("0X1e:{}", hash.to_hex())).expect("0X-prefixed code parse");

        assert_eq!(parsed, hash);
    }

    #[test]
    fn hash_parsing_trims_algorithm_and_digest_segments() {
        let hash = Hash::from_content(b"parse-trimmed");

        let parsed =
            Hash::from_str(&format!("  blake3  :  {}  ", hash.to_hex())).expect("trimmed parse");

        assert_eq!(parsed, hash);
    }

    #[test]
    fn hash_parsing_rejects_empty_algorithm_or_digest_segments() {
        let hash = Hash::from_content(b"parse-empty-segments");

        let missing_algorithm = Hash::from_str(&format!(":{}", hash.to_hex()));
        let missing_digest = Hash::from_str("blake3:");
        let blank_segments = Hash::from_str("   :   ");

        assert!(missing_algorithm.is_err());
        assert!(missing_digest.is_err());
        assert!(blank_segments.is_err());
    }

    #[test]
    fn hash_rejects_plain_hex_without_algorithm_prefix() {
        let hash = Hash::from_content(b"plain-hex");

        let result = Hash::from_str(&hash.to_hex());

        assert!(result.is_err());
    }

    #[test]
    fn hash_storage_bytes_matches_allocating_compat_path() {
        let hash = Hash::from_content(b"storage-bytes");

        let stack = hash.storage_bytes();
        let heap = hash.to_storage_bytes();

        assert_eq!(stack.as_slice(), heap.as_slice());
    }

    #[test]
    fn hash_write_hex_matches_to_hex() {
        let hash = Hash::from_content(b"hex-writer");
        let mut output = String::new();

        hash.write_hex(&mut output).expect("write hex");

        assert_eq!(output, hash.to_hex());
    }
}
