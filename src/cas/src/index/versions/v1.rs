//! V1 index row model using fixed-width POD headers and multimap constraints.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version isomorphism/migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `index/versions/mod.rs`.

use std::mem::{size_of, size_of_val};

use bytemuck::{Pod, Zeroable, bytes_of, pod_read_unaligned};
use redb::{MultimapTableDefinition, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::{CasError, Hash};

/// Bit flag indicating "full object" encoding in packed `depth_and_tag`.
const OBJECT_META_FULL_FLAG: u32 = 1 << 31;
/// Bit mask selecting packed depth bits in `depth_and_tag`.
const OBJECT_META_DEPTH_MASK: u32 = !OBJECT_META_FULL_FLAG;

/// Serde bridge for fixed-width base-hash storage bytes.
mod base_storage_serde {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    use super::HASH_STORAGE_KEY_BYTES;

    /// Serializes fixed-width base-storage bytes as an opaque byte sequence.
    pub(super) fn serialize<S>(
        value: &[u8; HASH_STORAGE_KEY_BYTES],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(value)
    }

    /// Deserializes base-storage bytes and enforces exact fixed width.
    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<[u8; HASH_STORAGE_KEY_BYTES], D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        let len = bytes.len();
        bytes.try_into().map_err(|_: Vec<u8>| {
            D::Error::custom(format!(
                "expected {HASH_STORAGE_KEY_BYTES} base-storage bytes, got {len}"
            ))
        })
    }
}

/// Fixed-width multihash key size for the current hash algorithm set.
pub(crate) const HASH_STORAGE_KEY_BYTES: usize = 34;

/// Current redb schema version for active index storage.
pub(crate) const INDEX_SCHEMA_VERSION: u32 = 1;

/// Returns the active V1 schema marker.
#[must_use]
pub(crate) const fn schema_version_v1() -> u32 {
    INDEX_SCHEMA_VERSION
}

/// Returns whether one persisted schema marker belongs to V1.
#[must_use]
pub(crate) const fn is_schema_version_v1(marker: u32) -> bool {
    marker == INDEX_SCHEMA_VERSION
}

/// Fixed-width bloom header byte size for V1 payload encoding.
///
/// Layout is:
/// - bytes `0..4`: little-endian `word_count: u32`
/// - bytes `4..12`: little-endian `bit_len: u64`
pub(crate) const BLOOM_PAYLOAD_HEADER_BYTES_V1: usize = size_of::<u32>() + size_of::<u64>();

/// Metadata table for persisted bloom state.
///
/// `single key -> encoded bloom payload`.
pub(crate) const BLOOM_STATE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bloom_state");

/// V1 bloom payload key in the bloom-state table.
const BLOOM_STATE_KEY_V1: &[u8] = b"hash_bloom";

/// Primary object metadata table.
///
/// `Hash multihash storage bytes (u8[34]) -> PrimaryHeaderV1` bytes.
pub(crate) const PRIMARY_INDEX: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("primary_index");

/// Explicit constraint candidates table.
///
/// `target hash multihash bytes (u8[34]) -> base hash multihash bytes (u8[34])`.
pub(crate) const PRIMARY_CONSTRAINTS: MultimapTableDefinition<&[u8], &[u8]> =
    MultimapTableDefinition::new("primary_constraints");

/// Opens V1 primary-object table for a read transaction.
pub(crate) fn open_primary_table_read_v1(
    read: &redb::ReadTransaction,
) -> Result<redb::ReadOnlyTable<&'static [u8], &'static [u8]>, CasError> {
    read.open_table(PRIMARY_INDEX).map_err(CasError::redb)
}

/// Opens V1 primary-object table for a write transaction.
pub(crate) fn open_primary_table_write_v1(
    write: &redb::WriteTransaction,
) -> Result<redb::Table<'_, &'static [u8], &'static [u8]>, CasError> {
    write.open_table(PRIMARY_INDEX).map_err(CasError::redb)
}

/// Opens V1 constraints table for a read transaction.
pub(crate) fn open_constraints_table_read_v1(
    read: &redb::ReadTransaction,
) -> Result<redb::ReadOnlyMultimapTable<&'static [u8], &'static [u8]>, CasError> {
    read.open_multimap_table(PRIMARY_CONSTRAINTS).map_err(CasError::redb)
}

/// Opens V1 constraints table for a write transaction.
pub(crate) fn open_constraints_table_write_v1(
    write: &redb::WriteTransaction,
) -> Result<redb::MultimapTable<'_, &'static [u8], &'static [u8]>, CasError> {
    write.open_multimap_table(PRIMARY_CONSTRAINTS).map_err(CasError::redb)
}

/// Creates all V1 data tables except the fixed schema metadata table.
pub(crate) fn initialize_data_tables_v1(write: &redb::WriteTransaction) -> Result<(), CasError> {
    let _ = write.open_table(PRIMARY_INDEX).map_err(CasError::redb)?;
    let _ = write.open_multimap_table(PRIMARY_CONSTRAINTS).map_err(CasError::redb)?;
    let _ = write.open_table(BLOOM_STATE).map_err(CasError::redb)?;
    Ok(())
}

/// Reads V1 bloom payload bytes from persisted bloom table.
pub(crate) fn read_bloom_payload_from_table_v1(
    read: &redb::ReadTransaction,
) -> Result<Option<Vec<u8>>, CasError> {
    let bloom_table = read.open_table(BLOOM_STATE).map_err(CasError::redb)?;
    let Some(payload) = bloom_table.get(BLOOM_STATE_KEY_V1).map_err(CasError::redb)? else {
        return Ok(None);
    };

    Ok(Some(payload.value().to_vec()))
}

/// Writes V1 bloom payload bytes to persisted bloom table.
pub(crate) fn write_bloom_payload_to_table_v1(
    write: &redb::WriteTransaction,
    payload: &[u8],
) -> Result<(), CasError> {
    let mut bloom_table = write.open_table(BLOOM_STATE).map_err(CasError::redb)?;
    bloom_table.insert(BLOOM_STATE_KEY_V1, payload).map_err(CasError::redb)?;
    Ok(())
}

/// Fixed-width header row for one object metadata entry.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub(crate) struct PrimaryHeaderV1 {
    payload_len: u64,
    content_len: u64,
    depth: u32,
    flags: u8,
    _reserved: [u8; 3],
    base_storage_head: [u8; 32],
    base_storage_tail: [u8; 2],
    _padding: [u8; 6],
}

/// Header encode/decode and validation utilities for V1 primary rows.
impl PrimaryHeaderV1 {
    const FLAG_FULL: u8 = 1;

    /// Encodes V1 metadata into fixed-width header bytes.
    pub(crate) fn encode(
        meta: ObjectMetaV1,
        max_delta_depth: u32,
    ) -> Result<[u8; PRIMARY_HEADER_BYTES], CasError> {
        let depth = meta.depth();
        if depth > max_delta_depth {
            return Err(CasError::corrupt_index(format!(
                "runtime object depth exceeds configured limit: depth={depth}, max={max_delta_depth}"
            )));
        }

        let (flags, base_storage) = if meta.is_full() {
            (Self::FLAG_FULL, [0u8; HASH_STORAGE_KEY_BYTES])
        } else {
            let base_hash = meta.base_hash().ok_or_else(|| {
                CasError::corrupt_index(
                    "delta object metadata missing base hash while encoding primary header",
                )
            })?;
            (0, base_hash.storage_bytes())
        };

        let mut base_storage_head = [0u8; 32];
        base_storage_head.copy_from_slice(&base_storage[..32]);
        let mut base_storage_tail = [0u8; 2];
        base_storage_tail.copy_from_slice(&base_storage[32..]);

        let header = Self {
            payload_len: meta.payload_len,
            content_len: meta.content_len,
            depth,
            flags,
            _reserved: [0u8; 3],
            base_storage_head,
            base_storage_tail,
            _padding: [0u8; 6],
        };

        let mut out = [0u8; PRIMARY_HEADER_BYTES];
        out.copy_from_slice(bytes_of(&header));
        Ok(out)
    }

    /// Decodes one fixed-width header row.
    pub(crate) fn decode(bytes: &[u8], max_delta_depth: u32) -> Result<Self, CasError> {
        if bytes.len() != PRIMARY_HEADER_BYTES {
            return Err(CasError::corrupt_index(format!(
                "invalid primary header width: expected {PRIMARY_HEADER_BYTES}, got {}",
                bytes.len()
            )));
        }

        let header = pod_read_unaligned::<Self>(bytes);
        header.validate(max_delta_depth)?;
        Ok(header)
    }

    /// Converts this header into V1 object metadata.
    pub(crate) fn to_object_meta_v1(self) -> ObjectMetaV1 {
        if self.flags & Self::FLAG_FULL != 0 {
            ObjectMetaV1::full(self.payload_len, self.content_len, self.depth)
        } else {
            let mut base_storage = [0u8; HASH_STORAGE_KEY_BYTES];
            base_storage[..32].copy_from_slice(&self.base_storage_head);
            base_storage[32..].copy_from_slice(&self.base_storage_tail);

            let base_hash = Hash::from_storage_bytes(&base_storage)
                .expect("persisted delta base hash must decode from multihash storage bytes");
            ObjectMetaV1::delta(self.payload_len, self.content_len, self.depth, base_hash)
        }
    }

    /// Validates decoded header invariants against runtime depth constraints.
    fn validate(self, max_delta_depth: u32) -> Result<(), CasError> {
        if self.depth > max_delta_depth {
            return Err(CasError::corrupt_index(format!(
                "persisted object depth exceeds configured limit: depth={}, max={max_delta_depth}",
                self.depth,
            )));
        }

        if self.flags & !Self::FLAG_FULL != 0 {
            return Err(CasError::corrupt_index(format!(
                "persisted header contains unsupported flag bits: 0b{:08b}",
                self.flags
            )));
        }

        Ok(())
    }
}

/// Fixed-width encoded header byte length.
pub(crate) const PRIMARY_HEADER_BYTES: usize = size_of::<PrimaryHeaderV1>();

/// Encodes one hash into the fixed-width primary-key representation.
#[must_use]
pub(crate) fn index_key_from_hash(hash: Hash) -> [u8; HASH_STORAGE_KEY_BYTES] {
    hash.storage_bytes()
}

/// Decodes one fixed-width primary key back into a hash.
///
/// # Errors
/// Returns [`CasError::CorruptIndex`] if `key` is not exactly
/// [`HASH_STORAGE_KEY_BYTES`] bytes.
pub(crate) fn hash_from_index_key(key: &[u8]) -> Result<Hash, CasError> {
    let encoded: [u8; HASH_STORAGE_KEY_BYTES] = key.try_into().map_err(|_| {
        CasError::corrupt_index(format!(
            "invalid primary-index key length: expected {HASH_STORAGE_KEY_BYTES}, got {}",
            key.len()
        ))
    })?;

    Hash::from_storage_bytes(&encoded).map_err(CasError::from)
}

/// Decodes persisted bloom payload bytes using V1 layout.
///
/// Returns `(bit_len, raw_words_bytes)` where `raw_words_bytes` is a borrowed
/// slice over packed little-endian `u64` words.
pub(crate) fn decode_bloom_payload_v1(bytes: &[u8]) -> Result<(usize, &[u8]), CasError> {
    if bytes.len() < BLOOM_PAYLOAD_HEADER_BYTES_V1 {
        return Err(CasError::corrupt_index(format!(
            "persisted bloom payload too short: expected at least {BLOOM_PAYLOAD_HEADER_BYTES_V1}, got {}",
            bytes.len()
        )));
    }

    let word_count = u32::from_le_bytes(bytes[0..4].try_into().expect("slice width checked"));
    let bit_len = u64::from_le_bytes(bytes[4..12].try_into().expect("slice width checked"));

    let bit_len = usize::try_from(bit_len).map_err(|_| {
        CasError::corrupt_index(format!(
            "persisted bloom bit length {bit_len} does not fit platform usize"
        ))
    })?;
    if bit_len == 0 || !bit_len.is_power_of_two() {
        return Err(CasError::corrupt_index(format!(
            "persisted bloom filter has invalid bit length: {bit_len}"
        )));
    }

    let raw_words = &bytes[BLOOM_PAYLOAD_HEADER_BYTES_V1..];
    let expected_raw_bytes =
        (word_count as usize).checked_mul(size_of::<u64>()).ok_or_else(|| {
            CasError::corrupt_index("persisted bloom payload width overflow while decoding")
        })?;

    if raw_words.len() != expected_raw_bytes {
        return Err(CasError::corrupt_index(format!(
            "persisted bloom payload word width mismatch: expected {expected_raw_bytes}, got {}",
            raw_words.len()
        )));
    }

    Ok((bit_len, raw_words))
}

/// Encodes bloom payload bytes using V1 layout.
pub(crate) fn encode_bloom_payload_v1(
    bit_len: usize,
    raw_words: &[u64],
) -> Result<Vec<u8>, CasError> {
    if bit_len == 0 || !bit_len.is_power_of_two() {
        return Err(CasError::corrupt_index(format!(
            "cannot encode bloom payload with invalid bit length: {bit_len}"
        )));
    }

    let word_count = u32::try_from(raw_words.len()).map_err(|_| {
        CasError::corrupt_index(format!(
            "cannot encode bloom payload: word count {} exceeds u32::MAX",
            raw_words.len()
        ))
    })?;

    let bit_len_u64 = u64::try_from(bit_len).map_err(|_| {
        CasError::corrupt_index(format!(
            "cannot encode bloom payload: bit length {bit_len} exceeds u64::MAX"
        ))
    })?;

    let mut out = Vec::with_capacity(BLOOM_PAYLOAD_HEADER_BYTES_V1 + size_of_val(raw_words));
    out.extend_from_slice(&word_count.to_le_bytes());
    out.extend_from_slice(&bit_len_u64.to_le_bytes());
    for word in raw_words {
        out.extend_from_slice(&word.to_le_bytes());
    }
    Ok(out)
}

/// V1 in-memory metadata for one stored object entry.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ObjectMetaV1 {
    /// Stored payload size.
    pub(crate) payload_len: u64,
    /// Logical content length.
    pub(crate) content_len: u64,
    /// Packed encoding tag + depth (`bit31 => full-data flag`, lower bits => depth).
    depth_and_tag: u32,
    /// Base-hash multihash storage bytes for delta encoding (`[0; 34]` for full-data objects).
    #[serde(with = "base_storage_serde")]
    base_storage: [u8; HASH_STORAGE_KEY_BYTES],
}

/// Constructors/accessors for packed V1 object metadata.
impl ObjectMetaV1 {
    /// Constructs full-data object metadata.
    #[must_use]
    pub(crate) const fn full(payload_len: u64, content_len: u64, depth: u32) -> Self {
        Self {
            payload_len,
            content_len,
            depth_and_tag: OBJECT_META_FULL_FLAG | (depth & OBJECT_META_DEPTH_MASK),
            base_storage: [0u8; HASH_STORAGE_KEY_BYTES],
        }
    }

    /// Constructs delta object metadata.
    #[must_use]
    pub(crate) fn delta(payload_len: u64, content_len: u64, depth: u32, base_hash: Hash) -> Self {
        Self {
            payload_len,
            content_len,
            depth_and_tag: depth & OBJECT_META_DEPTH_MASK,
            base_storage: base_hash.storage_bytes(),
        }
    }

    /// Returns whether this object is encoded as full-data bytes.
    #[must_use]
    pub(crate) const fn is_full(&self) -> bool {
        self.depth_and_tag & OBJECT_META_FULL_FLAG != 0
    }

    /// Returns reconstruction depth.
    #[must_use]
    pub(crate) const fn depth(&self) -> u32 {
        self.depth_and_tag & OBJECT_META_DEPTH_MASK
    }

    /// Returns base hash for delta-encoded objects.
    #[must_use]
    pub(crate) fn base_hash(&self) -> Option<Hash> {
        (!self.is_full()).then(|| {
            Hash::from_storage_bytes(&self.base_storage)
                .expect("delta object metadata must store valid multihash-encoded base hash")
        })
    }
}
