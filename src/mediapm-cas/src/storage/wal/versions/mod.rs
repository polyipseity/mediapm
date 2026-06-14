//! Versioned binary wire formats for journal and checkpoint artifacts.
//!
//! The long-lived functional core is [`Journal`](super::super::wal::Journal).
//! Each wire version owns:
//! - its exact byte layout,
//! - parse/validate/encode behavior,
//! - direct `From` conversions to/from version-specific state types.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version migration.
//! - This `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside `versions/` must interact with versioned envelopes only
//!   through this `mod.rs`, never through direct `versions::vX` imports.
//! - Do not directly re-export `versions::vX` structs/types from this module.
//!   Expose unversioned APIs here and keep versioned internals encapsulated.

pub(crate) mod v1;

use crate::error::CasError;

use super::{WalEntry, WalPosition};

// ---------------------------------------------------------------------------
// Re-exported constants
// ---------------------------------------------------------------------------

/// Header size for all on-disk artifacts: 6-byte magic + 2-byte version.
pub(crate) const HEADER_LEN: usize = 8;

/// Magic prefix for journal segment files.
pub(crate) const JOURNAL_MAGIC: &[u8; 6] = v1::JOURNAL_MAGIC;
/// Current journal segment format version.
pub(crate) const JOURNAL_VERSION: u16 = v1::JOURNAL_VERSION;

/// Maximum supported journal segment format version.
pub(crate) const MAX_JOURNAL_VERSION: u16 = v1::MAX_JOURNAL_VERSION;

// ---------------------------------------------------------------------------
// Re-exported header helpers
// ---------------------------------------------------------------------------

pub(crate) use v1::{decode_header, encode_header};

// ---------------------------------------------------------------------------
// Entry encode/decode (bridge between unversioned WalEntry and V1 types)
// ---------------------------------------------------------------------------

/// Encode a journal entry at the given position.
pub(crate) fn encode_entry(entry: &WalEntry, pos: WalPosition) -> Vec<u8> {
    entry_to_v1(entry).encode(pos.as_u64())
}

/// Decode a single journal entry from bytes.
///
/// Returns `(entry, position, bytes_consumed)`.
pub(crate) fn decode_entry(buf: &[u8]) -> Result<(WalEntry, WalPosition, usize), CasError> {
    let (v1_entry, pos_u64, consumed) = v1::WalEntryV1::decode(buf)?;
    let entry = entry_from_v1(&v1_entry);
    Ok((entry, WalPosition::from_u64(pos_u64), consumed))
}

/// Decode all journal entries from a buffer.
pub(crate) fn decode_entries(buf: &[u8]) -> Result<Vec<(WalPosition, WalEntry)>, CasError> {
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset < buf.len() {
        let (entry, pos, consumed) = decode_entry(&buf[offset..])?;
        entries.push((pos, entry));
        offset += consumed;
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// Checkpoint encode/decode
// ---------------------------------------------------------------------------

/// Encode a checkpoint file for the given position.
pub(crate) fn encode_checkpoint(pos: WalPosition) -> Vec<u8> {
    v1::CheckpointV1::encode(pos.as_u64())
}

/// Decode a checkpoint file, returning the last consumed position.
pub(crate) fn decode_checkpoint(buf: &[u8]) -> Result<WalPosition, CasError> {
    let pos_u64 = v1::CheckpointV1::decode(buf)?;
    Ok(WalPosition::from_u64(pos_u64))
}

// ---------------------------------------------------------------------------
// Bridge conversions between versioned and unversioned entry types
// ---------------------------------------------------------------------------

fn entry_to_v1(entry: &WalEntry) -> v1::WalEntryV1 {
    match entry {
        WalEntry::Put { hash, data } => v1::WalEntryV1::Put { hash: *hash, data: data.clone() },
        WalEntry::Delete { hash } => v1::WalEntryV1::Delete { hash: *hash },
        WalEntry::Constraint { target, bases } => {
            v1::WalEntryV1::Constraint { target: *target, bases: bases.clone() }
        }
    }
}

fn entry_from_v1(entry: &v1::WalEntryV1) -> WalEntry {
    match entry {
        v1::WalEntryV1::Put { hash, data } => WalEntry::Put { hash: *hash, data: data.clone() },
        v1::WalEntryV1::Delete { hash } => WalEntry::Delete { hash: *hash },
        v1::WalEntryV1::Constraint { target, bases } => {
            WalEntry::Constraint { target: *target, bases: bases.clone() }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bytes::Bytes;

    use super::super::{WalEntry, WalPosition};
    use crate::hash::Hash;

    use super::*;

    #[test]
    fn header_roundtrip() {
        let header = encode_header(JOURNAL_MAGIC, 1);
        let version = decode_header(&header, JOURNAL_MAGIC, 1).unwrap();
        assert_eq!(version, 1);
    }

    #[test]
    fn header_rejects_wrong_magic() {
        let header = encode_header(v1::CHECKPOINT_MAGIC, 1);
        assert!(decode_header(&header, JOURNAL_MAGIC, 1).is_err());
    }

    #[test]
    fn header_rejects_unknown_version() {
        let mut header = encode_header(JOURNAL_MAGIC, 99);
        // decode_header rejects > max_version
        assert!(decode_header(&header, JOURNAL_MAGIC, 1).is_err());
        // Also reject version 0
        header = encode_header(JOURNAL_MAGIC, 0);
        assert!(decode_header(&header, JOURNAL_MAGIC, 1).is_err());
    }

    #[test]
    fn entry_roundtrip_put() {
        let data = Bytes::from_static(b"hello world");
        let hash = Hash::from_content(&data);
        let entry = WalEntry::Put { hash, data: data.clone() };
        let pos = WalPosition::from_u64(42);

        let encoded = encode_entry(&entry, pos);
        let (decoded, decoded_pos, consumed) = decode_entry(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded_pos, pos);
        match decoded {
            WalEntry::Put { hash: h, data: d } => {
                assert_eq!(h, hash);
                assert_eq!(d, data);
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn entry_roundtrip_delete() {
        let hash = Hash::from_content(b"delete-me");
        let entry = WalEntry::Delete { hash };
        let pos = WalPosition::from_u64(7);

        let encoded = encode_entry(&entry, pos);
        let (decoded, decoded_pos, consumed) = decode_entry(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded_pos, pos);
        assert!(matches!(decoded, WalEntry::Delete { hash: h } if h == hash));
    }

    #[test]
    fn entry_roundtrip_constraint() {
        let target = Hash::from_content(b"target");
        let bases: BTreeSet<_> =
            [b"base1", b"base2", b"base3"].iter().map(|b| Hash::from_content(*b)).collect();
        let entry = WalEntry::Constraint { target, bases: bases.clone() };
        let pos = WalPosition::from_u64(99);

        let encoded = encode_entry(&entry, pos);
        let (decoded, decoded_pos, consumed) = decode_entry(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded_pos, pos);
        match decoded {
            WalEntry::Constraint { target: t, bases: bs } => {
                assert_eq!(t, target);
                assert_eq!(bs, bases);
            }
            _ => panic!("expected Constraint"),
        }
    }

    #[test]
    fn checkpoint_roundtrip() {
        let pos = WalPosition::from_u64(12345);
        let encoded = encode_checkpoint(pos);
        let decoded = decode_checkpoint(&encoded).unwrap();
        assert_eq!(decoded, pos);
    }

    #[test]
    fn checkpoint_rejects_corrupt() {
        let pos = WalPosition::from_u64(42);
        let mut encoded = encode_checkpoint(pos);
        // Corrupt the integrity hash
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;
        assert!(decode_checkpoint(&encoded).is_err());
    }

    #[test]
    fn decode_entries_multiple() {
        let h1 = Hash::from_content(b"a");
        let h2 = Hash::from_content(b"b");
        let entries = vec![
            (WalPosition::from_u64(1), WalEntry::Put { hash: h1, data: Bytes::from_static(b"a") }),
            (WalPosition::from_u64(2), WalEntry::Delete { hash: h2 }),
        ];

        let mut encoded = Vec::new();
        for (pos, entry) in &entries {
            encoded.extend_from_slice(&encode_entry(entry, *pos));
        }

        let decoded = decode_entries(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, WalPosition::from_u64(1));
        assert_eq!(decoded[1].0, WalPosition::from_u64(2));
    }
}
