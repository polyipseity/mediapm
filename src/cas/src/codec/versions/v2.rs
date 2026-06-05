//! V2 binary wire format for delta objects.
//!
//! V2 is V1 without the 4-byte CRC32 checksum field. All new writes use V2.
//! V1 format is kept for reading legacy objects.
//!
//! Layout:
//! `magic_with_embedded_version[8] | content_len[8] | payload_len[8] | base_hash[...] | payload[...]`
//!
//! Hash encoding/decoding is delegated to `rust-multihash` through
//! [`crate::Hash`] helpers. No manual varint parsing is performed here.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version isomorphism/migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `codec/versions/mod.rs`.

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use std::borrow::Cow;
use zerocopy::little_endian::U64 as Le64;
use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::codec::versions::{Migrate, migrate_with_version_state};
use crate::{CasError, Hash, HashParseError};

/// Magic marker for diff-file integrity and versioning sanity checks.
///
/// The `MD` prefix stands for **Media Delta**; the final two bytes are a
/// little-endian `u16` version.
///
/// DO NOT REMOVE: In the fat future when there are more than 65535 versions,
/// we use 0 for both bytes to represent the version, and we will at that time
/// find a better way to represent the version.
pub(crate) const DIFF_STORAGE_MAGIC: &[u8; 8] = b"MDCASD\x02\x00";

/// Version-local delta state for V2 wire semantics.
///
/// Keep this type self-contained inside `versions/` so `v2.rs` never depends
/// on unversioned runtime structs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeltaStateV2<'a> {
    /// Base hash used for reconstruction.
    pub(crate) base_hash: Hash,
    /// Reconstructed logical content length.
    pub(crate) content_len: u64,
    /// Encoded patch payload (VCDIFF bytes).
    pub(crate) payload: Cow<'a, [u8]>,
}

/// Parses a multihash from bytes, returning both hash and consumed byte count.
fn parse_multihash_from_bytes(bytes: &[u8]) -> Result<(Hash, usize), HashParseError> {
    Hash::from_storage_bytes_with_len(bytes)
}

/// On-disk V2 envelope model.
#[derive(Debug, Clone)]
pub(crate) struct V2Envelope<'a> {
    /// Base hash (variable length, parsed as multihash bytes).
    pub(crate) base_hash: Hash,
    /// Reconstructed content length.
    pub(crate) content_len: u64,
    /// Encoded payload length.
    pub(crate) payload_len: u64,
    /// VCDIFF payload bytes.
    pub(crate) payload: Cow<'a, [u8]>,
}

/// Fixed-size metadata block at the start of V2 envelope.
#[derive(FromBytes, KnownLayout, Immutable, Debug)]
#[repr(C, packed)]
struct V2Metadata {
    _magic: [u8; 8],
    content_len: Le64,
    payload_len: Le64,
}

/// Fixed-size metadata constants for V2 envelope parsing.
impl V2Metadata {
    const SIZE: usize = std::mem::size_of::<V2Metadata>();
}

/// Minimum bytes for a potentially valid V2 envelope.
///
/// Minimal multihash is 3 bytes: code-varint(1) + size-varint(1) + digest(1).
const V2_MIN_SIZE: usize = V2Metadata::SIZE + 3;

/// Parse/validate/encode helpers for V2 delta envelopes.
impl<'a> V2Envelope<'a> {
    /// Parses V2 envelope bytes into structured fields.
    ///
    /// The caller must already validate magic-embedded-version dispatch.
    pub(crate) fn parse(bytes: &'a [u8]) -> Result<V2Envelope<'a>, CasError> {
        if bytes.len() < V2_MIN_SIZE {
            return Err(CasError::corrupt_object(
                "delta envelope: buffer too short for V2 minimum",
            ));
        }

        let (metadata, _) = V2Metadata::ref_from_prefix(bytes)
            .map_err(|_| CasError::corrupt_object("delta envelope: invalid metadata"))?;

        let content_len = metadata.content_len.get();
        let payload_len = metadata.payload_len.get();

        let hash_start = V2Metadata::SIZE;
        let (base_hash, hash_bytes_len) = parse_multihash_from_bytes(&bytes[hash_start..])
            .map_err(|e| CasError::corrupt_object(e.to_string()))?;
        let hash_end = hash_start
            .checked_add(hash_bytes_len)
            .ok_or_else(|| CasError::corrupt_object("delta envelope: hash bounds overflow"))?;

        let payload_start = hash_end;
        let payload_len_usize = usize::try_from(payload_len).map_err(|_| {
            CasError::corrupt_object(format!(
                "delta envelope: payload_len {payload_len} exceeds platform usize"
            ))
        })?;
        let payload_end = payload_start
            .checked_add(payload_len_usize)
            .ok_or_else(|| CasError::corrupt_object("delta envelope: payload bounds overflow"))?;

        if bytes.len() < payload_end {
            return Err(CasError::corrupt_object(format!(
                "delta envelope: buffer too short for payload (need {}, have {})",
                payload_end,
                bytes.len()
            )));
        }

        if bytes.len() != payload_end {
            return Err(CasError::corrupt_object(format!(
                "delta envelope: trailing bytes after payload (expected {}, have {})",
                payload_end,
                bytes.len()
            )));
        }

        let payload = Cow::Borrowed(&bytes[payload_start..payload_end]);

        Ok(V2Envelope { base_hash, content_len, payload_len, payload })
    }

    /// Validates envelope consistency (payload length).
    pub(crate) fn validate(&self) -> Result<(), CasError> {
        let actual_payload_len =
            u64::try_from(self.payload.len()).expect("payload length exceeds u64::MAX");
        if self.payload_len != actual_payload_len {
            return Err(CasError::corrupt_object(format!(
                "delta envelope: payload_len mismatch (field {}, actual {})",
                self.payload_len, actual_payload_len
            )));
        }
        Ok(())
    }

    /// Encodes V2 envelope to bytes.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let base_hash_bytes = self.base_hash.storage_bytes();
        let capacity = V2Metadata::SIZE + base_hash_bytes.len() + self.payload.len();

        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(DIFF_STORAGE_MAGIC);
        bytes.extend_from_slice(&self.content_len.to_le_bytes());
        bytes.extend_from_slice(&self.payload_len.to_le_bytes());
        bytes.extend_from_slice(&base_hash_bytes);
        bytes.extend_from_slice(self.payload.as_ref());
        bytes
    }
}

/// Formal bidirectional optic between V2 envelope and `DeltaStateV2`.
pub(crate) fn delta_state_v2_iso<'a>() -> IsoPrime<'a, RcBrand, V2Envelope<'a>, DeltaStateV2<'a>> {
    IsoPrime::new(
        |envelope: V2Envelope<'a>| DeltaStateV2 {
            base_hash: envelope.base_hash,
            content_len: envelope.content_len,
            payload: envelope.payload,
        },
        |state: DeltaStateV2<'a>| {
            let payload_len =
                u64::try_from(state.payload.len()).expect("payload length exceeds u64::MAX");

            V2Envelope {
                base_hash: state.base_hash,
                content_len: state.content_len,
                payload_len,
                payload: state.payload,
            }
        },
    )
}

/// Placeholder migration implementation proving optic composition path.
impl<'a> Migrate<V2Envelope<'a>> for V2Envelope<'a> {
    fn migrate(self) -> V2Envelope<'a> {
        let iso = delta_state_v2_iso();
        migrate_with_version_state(self, &iso, &iso)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rejects_truncated_payload() {
        let base_hash = Hash::from_content(b"base");
        let state =
            DeltaStateV2 { base_hash, content_len: 7, payload: Cow::Owned(vec![10, 11, 12, 13]) };
        let envelope = delta_state_v2_iso().to(state);
        let mut bytes = envelope.encode();
        bytes.pop();

        let error =
            V2Envelope::parse(&bytes).expect_err("truncated payload length must fail parsing");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[test]
    fn parse_borrows_payload_slice() {
        let base_hash = Hash::from_content(b"base");
        let state =
            DeltaStateV2 { base_hash, content_len: 7, payload: Cow::Owned(vec![10, 11, 12, 13]) };
        let bytes = delta_state_v2_iso().to(state).encode();

        let envelope = V2Envelope::parse(&bytes).expect("valid v2 bytes should parse");
        assert!(matches!(envelope.payload, Cow::Borrowed(_)));
    }

    #[test]
    fn parse_rejects_truncated_base_hash() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(DIFF_STORAGE_MAGIC);
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&[0x1e, 0x20]);

        let error =
            V2Envelope::parse(&bytes).expect_err("truncated multihash bytes must fail parsing");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[test]
    fn parse_rejects_trailing_bytes_after_payload() {
        let base_hash = Hash::from_content(b"base");
        let state =
            DeltaStateV2 { base_hash, content_len: 7, payload: Cow::Owned(vec![10, 11, 12, 13]) };
        let envelope = delta_state_v2_iso().to(state);
        let mut bytes = envelope.encode();
        bytes.push(0x00);

        let error =
            V2Envelope::parse(&bytes).expect_err("trailing bytes after payload must fail parsing");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[test]
    fn validate_rejects_payload_len_mismatch() {
        let base_hash = Hash::from_content(b"base");
        let state =
            DeltaStateV2 { base_hash, content_len: 7, payload: Cow::Owned(vec![10, 11, 12, 13]) };
        let mut envelope = delta_state_v2_iso().to(state);
        envelope.payload_len += 1;

        let error = envelope.validate().expect_err("payload_len mismatch must fail validation");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[test]
    fn v2_iso_roundtrip_preserves_delta_state() {
        let base_hash = Hash::from_content(b"base");
        let state =
            DeltaStateV2 { base_hash, content_len: 42, payload: Cow::Owned(vec![1, 2, 3, 4]) };
        let iso = delta_state_v2_iso();

        let envelope = iso.to(state.clone());
        envelope.validate().expect("envelope should be valid");

        let restored = iso.from(envelope);

        assert_eq!(restored, state);
    }
}
