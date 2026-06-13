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

// ---------------------------------------------------------------------------
// Version-specific types
// ---------------------------------------------------------------------------

/// V1 journal entry — mirrors [`WalEntry`] but is self-contained within
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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic prefix for journal segment files.
pub(crate) const JOURNAL_MAGIC: &[u8; 6] = b"CASJNL";
/// Current journal segment format version.
pub(crate) const JOURNAL_VERSION: u16 = 1;

/// Magic prefix for checkpoint files.
pub(crate) const CHECKPOINT_MAGIC: &[u8; 6] = b"CASCKP";
/// Current checkpoint format version.
pub(crate) const CHECKPOINT_VERSION: u16 = 1;

/// Maximum supported journal segment format version.
pub(crate) const MAX_JOURNAL_VERSION: u16 = 1;
/// Maximum supported checkpoint format version.
pub(crate) const MAX_CHECKPOINT_VERSION: u16 = 1;

// ---------------------------------------------------------------------------
// Entry encoding / decoding
// ---------------------------------------------------------------------------
//
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

    /// Decode a single journal entry from bytes.
    ///
    /// Returns `(entry, position, bytes_consumed)`.
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
                if payload.len() < 4 {
                    return Err(CasError::corrupt_object(
                        "journal entry: Constraint payload too short",
                    ));
                }
                let base_count = u32::from_le_bytes(payload[..4].try_into().map_err(|_| {
                    CasError::corrupt_object("journal entry: failed to parse base_count")
                })?) as usize;
                // Base hashes are multihash-encoded (34 bytes each)
                const BASE_HASH_MH_SIZE: usize = 34;
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

// ---------------------------------------------------------------------------
// Checkpoint encoding / decoding (version 1)
// ---------------------------------------------------------------------------
//
// Checkpoint file layout:
//   [header: 8 bytes (magic "CASCKP" + version)]
//   [last_position: 8-byte LE u64]
//   [integrity_hash: 32 bytes (blake3 of header + last_position)]

impl CheckpointV1 {
    /// Encode a checkpoint file (header + body).
    pub(crate) fn encode(last_position: u64) -> Vec<u8> {
        let header = encode_header(CHECKPOINT_MAGIC, CHECKPOINT_VERSION);
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
        decode_header(&header, CHECKPOINT_MAGIC, MAX_CHECKPOINT_VERSION)?;

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

// ---------------------------------------------------------------------------
// Header helpers
// ---------------------------------------------------------------------------

/// Encode an 8-byte header: 6-byte magic + 2-byte LE version.
pub(crate) fn encode_header(magic: &[u8; 6], version: u16) -> [u8; 8] {
    let mut buf = [0u8; 8];
    buf[..6].copy_from_slice(magic);
    buf[6..8].copy_from_slice(&version.to_le_bytes());
    buf
}

/// Decode and validate an 8-byte header.
///
/// Returns the decoded version on success.
pub(crate) fn decode_header(
    buf: &[u8; 8],
    expected_magic: &[u8; 6],
    max_version: u16,
) -> Result<u16, CasError> {
    if &buf[..6] != expected_magic {
        return Err(CasError::corrupt_object(format!(
            "expected magic {expected_magic:02x?}, got {:02x?}",
            &buf[..6]
        )));
    }
    let version = u16::from_le_bytes([buf[6], buf[7]]);
    if version == 0 || version > max_version {
        return Err(CasError::corrupt_object(format!(
            "unsupported version {version} (max {max_version})"
        )));
    }
    Ok(version)
}
