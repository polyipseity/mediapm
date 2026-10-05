//! V1 binary wire format for journal segments and checkpoints.
//!
//! V1 is the initial journal format. Each journal segment carries an 8-byte
//! header (`CASJNL` + version 1), followed by len-prefixed entries. The
//! checkpoint file carries a `CASCKP` header + last position + integrity hash.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version isomorphism/migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.

use std::collections::BTreeSet;

use bytes::Bytes;

use crate::error::CasError;
use crate::hash::Hash;

/// V1 journal entry — mirrors the unversioned `WalEntry` but is self-contained within
/// `versions/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalEntryV1 {
    /// Store data under hash.
    Put { hash: Hash, data: Bytes },
    /// Logically delete hash.
    Delete { hash: Hash },
    /// Set delta-compression hints.
    Constraint { target: Hash, bases: BTreeSet<Hash> },
}

/// V1 checkpoint state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CheckpointV1 {
    /// Last fully-consumed journal position.
    pub(crate) last_position: u64,
}

/// Magic prefix for V1 journal segment files.
///
/// Byte-identical to [`super::v2::JOURNAL_MAGIC`]. The magic deliberately does
/// not discriminate journal formats; the 2-byte version field in the segment
/// header does, and that is what the dispatcher routes on.
///
/// Only the round-trip test needs this constant: production never writes a V1
/// segment, so it writes V2's. It is kept so the test can assert the two
/// magics are byte-identical rather than assuming it.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "production writes V2 segments only; V1's own magic is retained so the round-trip test can build a real V1 segment and assert the two magics match"
    )
)]
pub(crate) const JOURNAL_MAGIC: [u8; 6] = *b"CASJNL";

/// V1 journal segment format version.
///
/// This is the value V1 writes into the 8-byte segment header, and the value
/// [`super::decode_header`] hands back so the dispatcher routes the segment's
/// entries to [`WalEntryV1::decode`]. The magic prefix is deliberately not
/// repeated here: it is identical across versions and cannot discriminate.
pub(crate) const JOURNAL_VERSION: u16 = 1;

/// Magic prefix for checkpoint files.
pub(crate) const CHECKPOINT_MAGIC: [u8; 6] = *b"CASCKP";

/// Maximum supported journal segment format version.
pub(crate) const MAX_JOURNAL_VERSION: u16 = 1;

// Each entry:
//   [pos: 8-byte LE u64]
//   [hash: 32 bytes]
//   [op_type: 1 byte] — 0=Put, 1=Delete, 2=Constraint
//   [payload_len: 4-byte LE u32]
//   [payload: payload_len bytes]
//
// Payload per op_type:
//   Put:        data bytes (the raw content)
//   Delete:     (empty)
//   Constraint: base_count(4-byte LE u32) + base_hashes(32 bytes each)

impl WalEntryV1 {
    /// Encode a journal entry into bytes at the given position.
    ///
    /// Only reached from the round-trip test and the V1 → V2 migration proof:
    /// the running system writes [`super::v2::JOURNAL_VERSION`] segments, so
    /// no V1 segment is ever produced. See the round-trip test in `mod.rs`.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the running system writes V2 segments; the V1 encoder exists so the V1 round-trip test can produce a V1 journal to decode"
        )
    )]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "payload lengths are bounded by the in-memory Bytes the caller already holds; a payload over u32::MAX cannot be represented in this on-disk layout, which is a documented limit of the V1 format"
    )]
    pub(crate) fn encode(&self, pos: u64) -> Vec<u8> {
        match self {
            WalEntryV1::Put { hash, data } => {
                let payload = data.as_ref();
                let total = 8 + 34 + 1 + 4 + payload.len();
                let mut buf = Vec::with_capacity(total);
                buf.extend_from_slice(&pos.to_le_bytes());
                buf.extend_from_slice(&hash.storage_bytes());
                buf.push(0); // op_type Put
                buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                buf.extend_from_slice(payload);
                buf
            }
            WalEntryV1::Delete { hash } => {
                let mut buf = Vec::with_capacity(8 + 34 + 1 + 4);
                buf.extend_from_slice(&pos.to_le_bytes());
                buf.extend_from_slice(&hash.storage_bytes());
                buf.push(1); // op_type Delete
                buf.extend_from_slice(&0u32.to_le_bytes()); // payload_len = 0
                buf
            }
            WalEntryV1::Constraint { target, bases } => {
                // Payload: base_count(4) + base_hashes(34 each, multihash-encoded)
                let payload_len = 4 + bases.len() * 34;
                let total = 8 + 34 + 1 + 4 + payload_len;
                let mut buf = Vec::with_capacity(total);
                buf.extend_from_slice(&pos.to_le_bytes());
                buf.extend_from_slice(&target.storage_bytes());
                buf.push(2); // op_type Constraint
                buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
                buf.extend_from_slice(&(bases.len() as u32).to_le_bytes());
                for base in bases {
                    buf.extend_from_slice(&base.storage_bytes());
                }
                buf
            }
        }
    }

    /// Decode a single V1 journal entry from bytes.
    ///
    /// Returns `(entry, position, bytes_consumed)`. A V1 entry has no
    /// `PutLarge` variant, so `op_type == 3` is rejected here; that rejection
    /// is the only observable difference between the V1 and V2 entry decoders,
    /// and the segment header's version field is what routes to this one.
    pub(crate) fn decode(buf: &[u8]) -> Result<(Self, u64, usize), CasError> {
        if buf.len() < 8 + 34 + 1 + 4 {
            return Err(CasError::corrupt_object(
                "journal entry: buffer too short for multihash entry (need 47)",
            ));
        }

        let pos =
            u64::from_le_bytes(buf[..8].try_into().map_err(|_| {
                CasError::corrupt_object("journal entry: failed to parse position")
            })?);

        let (hash, hash_bytes) = Hash::from_storage_bytes_with_len(&buf[8..]).map_err(|e| {
            CasError::corrupt_object(format!("journal entry: invalid multihash hash: {e}"))
        })?;

        let op_type_offset = 8 + hash_bytes;
        let op_type = buf[op_type_offset];

        let payload_len_offset = op_type_offset + 1;
        let payload_len =
            u32::from_le_bytes(buf[payload_len_offset..payload_len_offset + 4].try_into().map_err(
                |_| CasError::corrupt_object("journal entry: failed to parse payload_len"),
            )?) as usize;

        let total = payload_len_offset + 4 + payload_len;
        if buf.len() < total {
            return Err(CasError::corrupt_object("journal entry: payload truncated"));
        }

        let payload = &buf[payload_len_offset + 4..total];

        let entry = match op_type {
            0 => {
                // Put
                WalEntryV1::Put { hash, data: Bytes::copy_from_slice(payload) }
            }
            1 => {
                // Delete
                if !payload.is_empty() {
                    return Err(CasError::corrupt_object(
                        "journal entry: Delete with non-empty payload",
                    ));
                }
                WalEntryV1::Delete { hash }
            }
            2 => {
                // Constraint
                // Base hashes are multihash-encoded (34 bytes each)
                const BASE_HASH_MH_SIZE: usize = 34;
                if payload.len() < 4 {
                    return Err(CasError::corrupt_object(
                        "journal entry: Constraint payload too short",
                    ));
                }
                let base_count = u32::from_le_bytes(payload[..4].try_into().map_err(|_| {
                    CasError::corrupt_object("journal entry: failed to parse base_count")
                })?) as usize;
                let expected_payload = 4 + base_count * BASE_HASH_MH_SIZE;
                if payload.len() < expected_payload {
                    return Err(CasError::corrupt_object(format!(
                        "journal entry: Constraint payload too short: \
                         need {expected_payload}, have {}",
                        payload.len()
                    )));
                }
                let mut bases = BTreeSet::new();
                for i in 0..base_count {
                    let offset = 4 + i * BASE_HASH_MH_SIZE;
                    let (base, _) =
                        Hash::from_storage_bytes_with_len(&payload[offset..]).map_err(|e| {
                            CasError::corrupt_object(format!(
                                "journal entry: invalid constraint base hash: {e}"
                            ))
                        })?;
                    bases.insert(base);
                }
                WalEntryV1::Constraint { target: hash, bases }
            }
            _ => {
                return Err(CasError::corrupt_object(format!(
                    "journal entry: unknown op_type {op_type}"
                )));
            }
        };

        Ok((entry, pos, total))
    }
}

// Checkpoint file layout:
//   [header: 8 bytes (magic "CASCKP" + version)]
//   [last_position: 8-byte LE u64]
//   [integrity_hash: 32 bytes (blake3 of header + last_position)]

impl CheckpointV1 {
    /// Encode a checkpoint file (header + body).
    pub(crate) fn encode(last_position: u64) -> Vec<u8> {
        let header = super::encode_header(CHECKPOINT_MAGIC, JOURNAL_VERSION);
        let last_pos_bytes = last_position.to_le_bytes();
        let mut buf = Vec::with_capacity(8 + 8 + 32);
        buf.extend_from_slice(&header);
        buf.extend_from_slice(&last_pos_bytes);
        // Integrity hash: blake3 of header + last_position
        let integrity = blake3::hash(&buf);
        buf.extend_from_slice(integrity.as_bytes());
        buf
    }

    /// Decode and verify a checkpoint file.
    ///
    /// Returns the last consumed position.
    pub(crate) fn decode(buf: &[u8]) -> Result<u64, CasError> {
        if buf.len() < 8 + 8 + 32 {
            return Err(CasError::corrupt_object("checkpoint: file too short"));
        }

        // Verify header
        let mut header = [0u8; 8];
        header.copy_from_slice(&buf[..8]);
        super::decode_header(header, CHECKPOINT_MAGIC, MAX_JOURNAL_VERSION)?;

        // Verify integrity hash
        let body_end = 8 + 8; // header + last_position
        let stored_hash = &buf[body_end..body_end + 32];
        let computed = blake3::hash(&buf[..body_end]);
        if computed.as_bytes() != stored_hash {
            return Err(CasError::corrupt_object("checkpoint: integrity hash mismatch"));
        }

        let pos =
            u64::from_le_bytes(buf[8..16].try_into().map_err(|_| {
                CasError::corrupt_object("checkpoint: failed to parse last_position")
            })?);
        Ok(pos)
    }
}
