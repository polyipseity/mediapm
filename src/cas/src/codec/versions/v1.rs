//! V1 binary wire format for delta objects (legacy, read-only).
//!
//! V1 includes a 4-byte CRC32 checksum. All new writes use [V2](super::v2).
//! V1 is kept for reading legacy objects only; the checksum field is parsed
//! for wire-format compatibility but is no longer verified.
//!
//! Layout:
//! `magic_with_embedded_version[8] | content_len[8] | payload_len[8] | checksum[4] | base_hash[...] | payload[...]`
//!
//! Hash encoding/decoding is delegated to `rust-multihash` through
//! [`crate::Hash`] helpers. No manual varint parsing is performed here.
#![allow(dead_code)]
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
use zerocopy::little_endian::{U32 as Le32, U64 as Le64};
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
pub(crate) const DIFF_STORAGE_MAGIC: &[u8; 8] = b"MDCASD\x01\x00";

/// Version-local delta state for V1 wire semantics.
///
/// Keep this type self-contained inside `versions/` so `v1.rs` never depends
/// on unversioned runtime structs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeltaStateV1<'a> {
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

/// On-disk V1 envelope model.
#[derive(Debug, Clone)]
pub(crate) struct V1Envelope<'a> {
    /// Base hash (variable length, parsed as multihash bytes).
    pub(crate) base_hash: Hash,
    /// Reconstructed content length.
    pub(crate) content_len: u64,
    /// Encoded payload length.
    pub(crate) payload_len: u64,
    /// Envelope checksum.
    pub(crate) checksum: u32,
    /// VCDIFF payload bytes.
    pub(crate) payload: Cow<'a, [u8]>,
}

/// Fixed-size metadata block at the start of V1 envelope.
#[derive(FromBytes, KnownLayout, Immutable, Debug)]
#[repr(C, packed)]
struct V1Metadata {
    _magic: [u8; 8],
    content_len: Le64,
    payload_len: Le64,
    checksum: Le32,
}

/// Fixed-size metadata constants for V1 envelope parsing.
impl V1Metadata {
    const SIZE: usize = std::mem::size_of::<V1Metadata>();
}

/// Minimum bytes for a potentially valid V1 envelope.
///
/// Minimal multihash is 3 bytes: code-varint(1) + size-varint(1) + digest(1).
const V1_MIN_SIZE: usize = V1Metadata::SIZE + 3;

/// Parse/validate/encode helpers for V1 delta envelopes.
impl<'a> V1Envelope<'a> {
    /// Parses V1 envelope bytes into structured fields.
    ///
    /// The caller must already validate magic-embedded-version dispatch.
    pub(crate) fn parse(bytes: &'a [u8]) -> Result<V1Envelope<'a>, CasError> {
        if bytes.len() < V1_MIN_SIZE {
            return Err(CasError::corrupt_object(
                "delta envelope: buffer too short for V1 minimum",
            ));
        }

        let (metadata, _) = V1Metadata::ref_from_prefix(bytes)
            .map_err(|_| CasError::corrupt_object("delta envelope: invalid metadata"))?;

        let content_len = metadata.content_len.get();
        let payload_len = metadata.payload_len.get();
        let checksum = metadata.checksum.get();

        let hash_start = V1Metadata::SIZE;
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

        Ok(V1Envelope { base_hash, content_len, payload_len, checksum, payload })
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

    /// Encodes V1 envelope to bytes.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let base_hash_bytes = self.base_hash.storage_bytes();
        let capacity = V1Metadata::SIZE + base_hash_bytes.len() + self.payload.len();

        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(DIFF_STORAGE_MAGIC);
        bytes.extend_from_slice(&self.content_len.to_le_bytes());
        bytes.extend_from_slice(&self.payload_len.to_le_bytes());
        bytes.extend_from_slice(&self.checksum.to_le_bytes());
        bytes.extend_from_slice(&base_hash_bytes);
        bytes.extend_from_slice(self.payload.as_ref());
        bytes
    }
}

/// Formal bidirectional optic between V1 envelope and `DeltaState`.
pub(crate) fn delta_state_v1_iso<'a>() -> IsoPrime<'a, RcBrand, V1Envelope<'a>, DeltaStateV1<'a>> {
    IsoPrime::new(
        |envelope: V1Envelope<'a>| DeltaStateV1 {
            base_hash: envelope.base_hash,
            content_len: envelope.content_len,
            payload: envelope.payload,
        },
        |state: DeltaStateV1<'a>| {
            let payload_len =
                u64::try_from(state.payload.len()).expect("payload length exceeds u64::MAX");

            V1Envelope {
                base_hash: state.base_hash,
                content_len: state.content_len,
                payload_len,
                checksum: 0,
                payload: state.payload,
            }
        },
    )
}

/// Placeholder migration implementation proving optic composition path.
///
/// V1→V2 migration is handled in `versions/mod.rs decode_envelope_for_version`.
/// This identity migration is used when V1 stays as V1 (no-op).
impl<'a> Migrate<V1Envelope<'a>> for V1Envelope<'a> {
    fn migrate(self) -> V1Envelope<'a> {
        let iso = delta_state_v1_iso();
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
            DeltaStateV1 { base_hash, content_len: 7, payload: Cow::Owned(vec![10, 11, 12, 13]) };
        let envelope = delta_state_v1_iso().to(state);
        let mut bytes = envelope.encode();
        bytes.pop();

        let error =
            V1Envelope::parse(&bytes).expect_err("truncated payload length must fail parsing");
        assert!(matches!(error, CasError::CorruptObject { .. }));
    }

    #[test]
    fn parse_borrows_payload_slice() {
        let base_hash = Hash::from_content(b"base");
        let state =
            DeltaStateV1 { base_hash, content_len: 7, payload: Cow::Owned(vec![10, 11, 12, 13]) };
        let bytes = delta_state_v1_iso().to(state).encode();

        let envelope = V1Envelope::parse(&bytes).expect("valid v1 bytes should parse");
        assert!(matches!(envelope.payload, Cow::Borrowed(_)));
    }

    #[test]
    fn parse_rejects_truncated_base_hash() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(DIFF_STORAGE_MAGIC);
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&[0x1e, 0x20]);

        let error =
            V1Envelope::parse(&bytes).expect_err("truncated multihash bytes must fail parsing");
        assert!(matches!(error, CasError::CorruptObject { .. }));
    }

    #[test]
    fn parse_rejects_trailing_bytes_after_payload() {
        let base_hash = Hash::from_content(b"base");
        let state =
            DeltaStateV1 { base_hash, content_len: 7, payload: Cow::Owned(vec![10, 11, 12, 13]) };
        let envelope = delta_state_v1_iso().to(state);
        let mut bytes = envelope.encode();
        bytes.push(0x00);

        let error =
            V1Envelope::parse(&bytes).expect_err("trailing bytes after payload must fail parsing");
        assert!(matches!(error, CasError::CorruptObject { .. }));
    }

    #[test]
    fn validate_rejects_payload_len_mismatch() {
        let base_hash = Hash::from_content(b"base");
        let state =
            DeltaStateV1 { base_hash, content_len: 7, payload: Cow::Owned(vec![10, 11, 12, 13]) };
        let mut envelope = delta_state_v1_iso().to(state);
        envelope.payload_len += 1;

        let error = envelope.validate().expect_err("payload_len mismatch must fail validation");
        assert!(matches!(error, CasError::CorruptObject { .. }));
    }

    #[test]
    fn v1_iso_roundtrip_preserves_delta_state() {
        let base_hash = Hash::from_content(b"base");
        let state =
            DeltaStateV1 { base_hash, content_len: 42, payload: Cow::Owned(vec![1, 2, 3, 4]) };
        let iso = delta_state_v1_iso();

        let envelope = iso.to(state.clone());
        envelope.validate().expect("checksum should be valid");

        let restored = iso.from(envelope);

        assert_eq!(restored, state);
    }
}
