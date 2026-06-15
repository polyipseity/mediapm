//! V2 binary wire format for journal segments.
//!
//! Difference from V1: adds `PutLarge` entry type (op_type=3) for large
//! objects whose payload is stored externally (immediately materialized to
//! the blob store). `PutLarge` stores only `hash` + `content_len` with no
//! inline data.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module
//!   for version-to-version migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.

use std::collections::BTreeSet;

use bytes::Bytes;

use crate::error::CasError;
use crate::hash::Hash;

// ---------------------------------------------------------------------------
// Version-specific types
// ---------------------------------------------------------------------------

/// V2 journal entry — adds `PutLarge` variant vs. V1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalEntryV2 {
    /// Store data under hash (inlined).
    Put { hash: Hash, data: Bytes },
    /// Store large data (payload in blob store, not inlined).
    PutLarge { hash: Hash, content_len: u64 },
    /// Logically delete hash.
    Delete { hash: Hash },
    /// Set delta-compression hints.
    Constraint { target: Hash, bases: BTreeSet<Hash> },
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic prefix for journal segment files (same as V1).
pub(crate) const JOURNAL_MAGIC: &[u8; 6] = b"CASJNL";
/// Current journal segment format version.
pub(crate) const JOURNAL_VERSION: u16 = 2;

/// Maximum supported journal segment format version.
pub(crate) const MAX_JOURNAL_VERSION: u16 = 2;

// ---------------------------------------------------------------------------
// Entry encoding / decoding
// ---------------------------------------------------------------------------
//
// Each entry:
//   [pos: 8-byte LE u64]
//   [hash: 32 bytes]
//   [op_type: 1 byte] — 0=Put, 1=Delete, 2=Constraint, 3=PutLarge
//   [payload_len: 4-byte LE u32]
//   [payload: payload_len bytes]
//
// Payload per op_type:
//   Put:        data bytes (raw content)
//   PutLarge:   content_len (8-byte LE u64)
//   Delete:     (empty)
//   Constraint: base_count(4-byte LE u32) + base_hashes(34 bytes each)

impl WalEntryV2 {
    /// Encode a journal entry into bytes at the given position.
    pub(crate) fn encode(&self, pos: u64) -> Vec<u8> {
        match self {
            WalEntryV2::Put { hash, data } => {
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
            WalEntryV2::PutLarge { hash, content_len } => {
                let payload_len = 8;
                let total = 8 + 34 + 1 + 4 + payload_len;
                let mut buf = Vec::with_capacity(total);
                buf.extend_from_slice(&pos.to_le_bytes());
                buf.extend_from_slice(&hash.storage_bytes());
                buf.push(3); // op_type PutLarge
                buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
                buf.extend_from_slice(&content_len.to_le_bytes());
                buf
            }
            WalEntryV2::Delete { hash } => {
                let mut buf = Vec::with_capacity(8 + 34 + 1 + 4);
                buf.extend_from_slice(&pos.to_le_bytes());
                buf.extend_from_slice(&hash.storage_bytes());
                buf.push(1); // op_type Delete
                buf.extend_from_slice(&0u32.to_le_bytes()); // payload_len = 0
                buf
            }
            WalEntryV2::Constraint { target, bases } => {
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
                WalEntryV2::Put { hash, data: Bytes::copy_from_slice(payload) }
            }
            1 => {
                // Delete
                if !payload.is_empty() {
                    return Err(CasError::corrupt_object(
                        "journal entry: Delete with non-empty payload",
                    ));
                }
                WalEntryV2::Delete { hash }
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
                WalEntryV2::Constraint { target: hash, bases }
            }
            3 => {
                // PutLarge
                if payload.len() < 8 {
                    return Err(CasError::corrupt_object(
                        "journal entry: PutLarge payload too short",
                    ));
                }
                let content_len = u64::from_le_bytes(payload[..8].try_into().map_err(|_| {
                    CasError::corrupt_object("journal entry: failed to parse content_len")
                })?);
                WalEntryV2::PutLarge { hash, content_len }
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
