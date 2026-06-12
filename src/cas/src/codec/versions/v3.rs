//! V3 binary wire format for delta objects.
//!
//! V3 is V2 with a 32-byte blake3 hash of the diff payload appended before
//! the base hash. The `diff_hash` provides independent integrity verification
//! of the VCDIFF payload without needing the base object — enabling offline
//! validation and safe caching of diff payloads.
//!
//! `diff_hash = blake3(payload)`.
//!
//! Layout:
//! `magic_with_embedded_version[8] | content_len[8] | payload_len[8] | diff_hash[32] | base_hash[...] | payload[...]`
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

use crate::codec::versions::v2::DeltaStateV2;
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
pub(crate) const DIFF_STORAGE_MAGIC: &[u8; 8] = b"MDCASD\x03\x00";

/// Version-local delta state for V3 wire semantics.
///
/// Keep this type self-contained inside `versions/` so `v3.rs` never depends
/// on unversioned runtime structs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeltaStateV3<'a> {
    /// Base hash used for reconstruction.
    pub(crate) base_hash: Hash,
    /// Reconstructed logical content length.
    pub(crate) content_len: u64,
    /// blake3 hash of the diff payload (for independent integrity verification).
    pub(crate) diff_hash: [u8; 32],
    /// Encoded patch payload (VCDIFF bytes).
    pub(crate) payload: Cow<'a, [u8]>,
}

/// Parses a multihash from bytes, returning both hash and consumed byte count.
fn parse_multihash_from_bytes(bytes: &[u8]) -> Result<(Hash, usize), HashParseError> {
    Hash::from_storage_bytes_with_len(bytes)
}

/// On-disk V3 envelope model.
#[derive(Debug, Clone)]
pub(crate) struct V3Envelope<'a> {
    /// Base hash (variable length, parsed as multihash bytes).
    pub(crate) base_hash: Hash,
    /// Reconstructed content length.
    pub(crate) content_len: u64,
    /// Encoded payload length.
    pub(crate) payload_len: u64,
    /// blake3 hash of the diff payload.
    pub(crate) diff_hash: [u8; 32],
    /// VCDIFF payload bytes.
    pub(crate) payload: Cow<'a, [u8]>,
}

/// Fixed-size metadata block at the start of V3 envelope.
#[derive(FromBytes, KnownLayout, Immutable, Debug)]
#[repr(C, packed)]
struct V3Metadata {
    _magic: [u8; 8],
    content_len: Le64,
    payload_len: Le64,
    diff_hash: [u8; 32],
}

/// Fixed-size metadata constants for V3 envelope parsing.
impl V3Metadata {
    const SIZE: usize = std::mem::size_of::<V3Metadata>();
}

/// Minimum bytes for a potentially valid V3 envelope.
///
/// Minimal multihash is 3 bytes: code-varint(1) + size-varint(1) + digest(1).
const V3_MIN_SIZE: usize = V3Metadata::SIZE + 3;

/// Parse/validate/encode helpers for V3 delta envelopes.
impl<'a> V3Envelope<'a> {
    /// Parses V3 envelope bytes into structured fields.
    ///
    /// The caller must already validate magic-embedded-version dispatch.
    pub(crate) fn parse(bytes: &'a [u8]) -> Result<V3Envelope<'a>, CasError> {
        if bytes.len() < V3_MIN_SIZE {
            return Err(CasError::corrupt_object(
                "delta envelope: buffer too short for V3 minimum",
            ));
        }

        // SAFETY: size check above guarantees at least V3Metadata fits.
        let meta = V3Metadata::ref_from_bytes(&bytes[..V3Metadata::SIZE])
            .map_err(|_| CasError::corrupt_object("delta envelope: V3 metadata alignment error"))?;

        let content_len = meta.content_len.get();
        let payload_len = meta.payload_len.get();
        let diff_hash = meta.diff_hash;

        let header_size = V3Metadata::SIZE;
        let remaining = &bytes[header_size..];

        let base_hash_len = remaining.len().saturating_sub(payload_len as usize);
        let (base_hash_bytes, payload) = remaining.split_at(base_hash_len);

        let base_hash = parse_multihash_from_bytes(base_hash_bytes)
            .map_err(|e| CasError::corrupt_object(format!("delta envelope: V3 base_hash: {e}")))?
            .0;

        Ok(V3Envelope {
            base_hash,
            content_len,
            payload_len,
            diff_hash,
            payload: Cow::Borrowed(payload),
        })
    }

    /// Validates the diff_hash: `blake3(payload) == diff_hash`.
    pub(crate) fn validate(&self) -> Result<(), CasError> {
        let computed = blake3::hash(&self.payload);
        if *computed.as_bytes() != self.diff_hash {
            return Err(CasError::corrupt_object(
                "delta envelope: V3 diff_hash mismatch — payload corrupted or tampered",
            ));
        }
        Ok(())
    }

    /// Encodes this envelope into bytes.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            V3Metadata::SIZE + self.base_hash.storage_bytes().len() + self.payload.len(),
        );
        buf.extend_from_slice(DIFF_STORAGE_MAGIC);
        buf.extend_from_slice(&self.content_len.to_le_bytes());
        buf.extend_from_slice(&self.payload_len.to_le_bytes());
        buf.extend_from_slice(&self.diff_hash);
        buf.extend_from_slice(&self.base_hash.storage_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Builds an envelope from content parameters, computing diff_hash internally.
    #[allow(dead_code)] // used in test code only
    pub(crate) fn from_parts(base_hash: Hash, content_len: u64, payload: Vec<u8>) -> Self {
        let payload_len = payload.len() as u64;
        let diff_hash = *blake3::hash(&payload).as_bytes();
        V3Envelope { base_hash, content_len, payload_len, diff_hash, payload: Cow::Owned(payload) }
    }
}

/// IsoPrime bridge: V3Envelope <-> DeltaStateV3.
pub(crate) fn delta_state_v3_iso<'a>() -> IsoPrime<'a, RcBrand, V3Envelope<'a>, DeltaStateV3<'a>> {
    IsoPrime::new(
        |envelope: V3Envelope<'a>| DeltaStateV3 {
            base_hash: envelope.base_hash,
            content_len: envelope.content_len,
            diff_hash: envelope.diff_hash,
            payload: envelope.payload,
        },
        |state: DeltaStateV3<'a>| V3Envelope {
            base_hash: state.base_hash,
            content_len: state.content_len,
            payload_len: state.payload.len() as u64,
            diff_hash: state.diff_hash,
            payload: state.payload,
        },
    )
}

/// V2 → V3 migration: preserves base_hash, content_len, payload; computes
/// diff_hash from payload.
impl<'a> From<DeltaStateV2<'a>> for DeltaStateV3<'a> {
    fn from(v2: DeltaStateV2<'a>) -> Self {
        let diff_hash = *blake3::hash(&v2.payload).as_bytes();
        DeltaStateV3 {
            base_hash: v2.base_hash,
            content_len: v2.content_len,
            diff_hash,
            payload: v2.payload,
        }
    }
}

/// Identity migration for V3V3 (optic composition placeholder).
///
/// Matches the pattern established by V2 — the Migrate impl exists so the
/// generic `decode_delta_state_borrowed` dispatch can call `.migrate()`
/// uniformly on `latest::Envelope`.
impl<'a> Migrate<V3Envelope<'a>> for V3Envelope<'a> {
    fn migrate(self) -> V3Envelope<'a> {
        let iso = delta_state_v3_iso();
        migrate_with_version_state(self, &iso, &iso)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v3_roundtrip_preserves_all_fields() {
        let base_hash = Hash::from_content(b"base data");
        let payload = b"vcdiff payload bytes".to_vec();
        let content_len = 42;

        let envelope = V3Envelope::from_parts(base_hash, content_len, payload.clone());
        assert_eq!(envelope.content_len, content_len);
        assert_eq!(envelope.base_hash, base_hash);
        assert_eq!(*envelope.payload, payload);

        let encoded = envelope.encode();
        let parsed = V3Envelope::parse(&encoded).expect("parse should succeed");
        assert_eq!(parsed.content_len, content_len);
        assert_eq!(parsed.base_hash, base_hash);
        assert_eq!(parsed.payload, payload);
        assert_eq!(parsed.diff_hash, envelope.diff_hash);
    }

    #[test]
    fn v3_validate_accepts_correct_diff_hash() {
        let base_hash = Hash::from_content(b"base");
        let payload = b"valid diff payload".to_vec();
        let envelope = V3Envelope::from_parts(base_hash, 100, payload);
        envelope.validate().expect("diff_hash should match payload");
    }

    #[test]
    fn v3_validate_rejects_tampered_payload() {
        let base_hash = Hash::from_content(b"base");
        let payload = b"original payload".to_vec();
        let mut envelope = V3Envelope::from_parts(base_hash, 100, payload);

        // Tamper the payload bytes directly
        match &mut envelope.payload {
            Cow::Owned(ref mut p) => p[0] ^= 0xFF,
            Cow::Borrowed(_) => unreachable!(),
        }

        let result = envelope.validate();
        assert!(result.is_err(), "validate should reject tampered payload");
    }

    #[test]
    fn v2_to_v3_migration_computes_diff_hash() {
        use crate::codec::versions::v2::DeltaStateV2;

        let v2 = DeltaStateV2 {
            base_hash: Hash::from_content(b"base"),
            content_len: 50,
            payload: Cow::Owned(b"some diff".to_vec()),
        };

        let v3 = DeltaStateV3::from(v2);
        let expected_hash = *blake3::hash(b"some diff").as_bytes();
        assert_eq!(v3.diff_hash, expected_hash);
        assert_eq!(v3.content_len, 50);
        assert_eq!(&*v3.payload, b"some diff");
    }
}
