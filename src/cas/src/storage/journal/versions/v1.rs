//! V1 on-disk format for journal segments and checkpoint files.
//!
//! ## Journal segment format (`CASJNL` + `\x01\x00`)
//!
//! ```text
//! [magic: 6 bytes "CASJNL"][version: 2-byte LE u16 1]
//! [entry_count: 4-byte LE u32]
//! [entry_1][entry_2]...[entry_N]
//!
//! Each entry:
//!   [op_type: 1 byte (0=Put, 1=Delete, 2=Constraint)]
//!   [hash: 34 bytes (multihash storage bytes)]
//!   [payload_len: 4-byte LE u32]
//!   [payload: payload_len bytes]  // omitted for Delete
//!
//! For Constraint entries, payload is:
//!   [base_count: 4-byte LE u32]
//!   [base_1: 34 bytes]...[base_N: 34 bytes]
//! ```
//!
//! ## Checkpoint format (`CASCKP` + `\x01\x00`)
//!
//! ```text
//! [magic: 6 bytes "CASCKP"][version: 2-byte LE u16 1]
//! [position: 8-byte LE u64]
//! [integrity_hash: 34 bytes (multihash of checkpoint content)]
//! ```
//!
//! ## Invariants
//!
//! - All multihash-length fields use fixed `34` bytes (blake3-256).
//! - Entries are strictly append-only; no in-place mutation.
//! - Segment files are created with read-write permissions and closed
//!   atomically (tempfile + rename) on rotation.

use std::collections::BTreeSet;
use std::io::Read;

use crate::error::CasError;
use crate::hash::{Hash, STORAGE_BYTES_LEN};
use crate::storage::journal::JournalEntry;

/// Magic prefix for journal segment files.
pub(crate) const JOURNAL_MAGIC: &[u8; 6] = b"CASJNL";
/// Magic prefix for checkpoint files.
pub(crate) const CHECKPOINT_MAGIC: &[u8; 6] = b"CASCKP";
/// Version tag appended after magic.
pub(crate) const VERSION_TAG: [u8; 2] = [0x01, 0x00];
/// Total header size: magic (6) + version (2) + entry_count (4) = 12 bytes.
pub(crate) const SEGMENT_HEADER_LEN: u32 = 12;
/// Checkpoint file size: magic (6) + version (2) + position (8) + hash (34) = 50 bytes.
pub(crate) const CHECKPOINT_LEN: u64 = 50;
/// Op-type byte value for Put entries.
const OP_PUT: u8 = 0;
/// Op-type byte value for Delete entries.
const OP_DELETE: u8 = 1;
/// Op-type byte value for Constraint entries.
const OP_CONSTRAINT: u8 = 2;

/// Encodes a single journal entry to its V1 wire format.
pub(crate) fn encode_entry(entry: &JournalEntry, buf: &mut Vec<u8>) {
    match entry {
        JournalEntry::Put { hash, data } => {
            buf.push(OP_PUT);
            buf.extend_from_slice(&hash.storage_bytes());
            let len = data.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(data);
        }
        JournalEntry::Delete { hash } => {
            buf.push(OP_DELETE);
            buf.extend_from_slice(&hash.storage_bytes());
            // payload_len = 0
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        JournalEntry::Constraint { target, bases } => {
            buf.push(OP_CONSTRAINT);
            buf.extend_from_slice(&target.storage_bytes());
            let base_count = bases.len() as u32;
            // payload = [base_count: 4] [base_1: 34]...
            let payload_len = 4 + base_count as usize * STORAGE_BYTES_LEN;
            buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
            buf.extend_from_slice(&base_count.to_le_bytes());
            for base in bases {
                buf.extend_from_slice(&base.storage_bytes());
            }
        }
    }
}

/// Decodes a single journal entry from a byte slice reader.
/// Returns `None` if there are not enough bytes for a complete entry.
pub(crate) fn decode_entry<R: Read>(reader: &mut R) -> Result<Option<JournalEntry>, CasError> {
    let mut op_buf = [0u8; 1];
    if reader.read_exact(&mut op_buf).is_err() {
        return Ok(None); // EOF / incomplete
    }
    let op_type = op_buf[0];

    let mut hash_buf = [0u8; STORAGE_BYTES_LEN];
    reader.read_exact(&mut hash_buf).map_err(|e| CasError::Io {
        operation: "read journal entry hash".into(),
        path: "(journal segment)".into(),
        source: e,
    })?;
    let hash = Hash::from_storage_bytes(&hash_buf).map_err(|e| CasError::CorruptObject {
        target: None,
        current: None,
        base: None,
        detail: format!("journal entry: hash decode failed: {e}"),
    })?;

    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).map_err(|e| CasError::Io {
        operation: "read journal entry payload_len".into(),
        path: "(journal segment)".into(),
        source: e,
    })?;
    let payload_len = u32::from_le_bytes(len_buf) as usize;

    match op_type {
        OP_PUT => {
            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                reader.read_exact(&mut payload).map_err(|e| CasError::Io {
                    operation: "read journal entry payload".into(),
                    path: "(journal segment)".into(),
                    source: e,
                })?;
            }
            Ok(Some(JournalEntry::Put { hash, data: payload.into() }))
        }
        OP_DELETE => Ok(Some(JournalEntry::Delete { hash })),
        OP_CONSTRAINT => {
            let mut count_buf = [0u8; 4];
            reader.read_exact(&mut count_buf).map_err(|e| CasError::Io {
                operation: "read constraint base count".into(),
                path: "(journal segment)".into(),
                source: e,
            })?;
            let base_count = u32::from_le_bytes(count_buf) as usize;
            let mut bases = BTreeSet::new();
            for _ in 0..base_count {
                let mut base_buf = [0u8; STORAGE_BYTES_LEN];
                reader.read_exact(&mut base_buf).map_err(|e| CasError::Io {
                    operation: "read constraint base hash".into(),
                    path: "(journal segment)".into(),
                    source: e,
                })?;
                bases.insert(Hash::from_storage_bytes(&base_buf).map_err(|e| {
                    CasError::CorruptObject {
                        target: None,
                        current: None,
                        base: None,
                        detail: format!("journal entry: constraint base hash decode failed: {e}"),
                    }
                })?);
            }
            Ok(Some(JournalEntry::Constraint { target: hash, bases }))
        }
        _ => Err(CasError::CorruptObject {
            target: None,
            current: None,
            base: None,
            detail: format!("unknown journal op_type: {op_type}"),
        }),
    }
}

/// Encodes a V1 checkpoint file.
pub(crate) fn encode_checkpoint(position: u64, integrity_hash: &Hash) -> Vec<u8> {
    let mut buf = Vec::with_capacity(CHECKPOINT_LEN as usize);
    buf.extend_from_slice(CHECKPOINT_MAGIC);
    buf.extend_from_slice(&VERSION_TAG);
    buf.extend_from_slice(&position.to_le_bytes());
    buf.extend_from_slice(&integrity_hash.storage_bytes());
    buf
}

/// Decodes a V1 checkpoint file.
pub(crate) fn decode_checkpoint(data: &[u8]) -> Result<(u64, Hash), CasError> {
    if data.len() < CHECKPOINT_LEN as usize {
        return Err(CasError::CorruptObject {
            target: None,
            current: None,
            base: None,
            detail: format!("checkpoint too short: {} < {}", data.len(), CHECKPOINT_LEN),
        });
    }
    let magic = &data[..6];
    if magic != CHECKPOINT_MAGIC {
        return Err(CasError::CorruptObject {
            target: None,
            current: None,
            base: None,
            detail: "checkpoint magic mismatch".into(),
        });
    }
    let version = u16::from_le_bytes([data[6], data[7]]);
    if version != 1 {
        return Err(CasError::CorruptObject {
            target: None,
            current: None,
            base: None,
            detail: format!("unsupported checkpoint version: {version}"),
        });
    }
    let position = u64::from_le_bytes(data[8..16].try_into().unwrap());
    let mut hash_buf = [0u8; STORAGE_BYTES_LEN];
    hash_buf.copy_from_slice(&data[16..16 + STORAGE_BYTES_LEN]);
    let hash = Hash::from_storage_bytes(&hash_buf).map_err(|e| CasError::CorruptObject {
        target: None,
        current: None,
        base: None,
        detail: format!("checkpoint: hash decode failed: {e}"),
    })?;
    Ok((position, hash))
}
