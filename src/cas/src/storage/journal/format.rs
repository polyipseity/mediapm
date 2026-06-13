//! On-disk format definitions for journal and checkpoint files.
//!
//! Thin compatibility shim that re-exports the versioned format layer
//! from `versions/`. All encode/decode/constant definitions live in
//! version-specific `versions/vX.rs` files, bridged through
//! `versions/mod.rs`.

// TODO(phase8): remove when journal is wired into storage backends.
#![allow(dead_code)]

pub(crate) use super::versions::{
    HEADER_LEN, JOURNAL_MAGIC, JOURNAL_VERSION, MAX_JOURNAL_VERSION, decode_checkpoint,
    decode_entries, decode_entry, decode_header, encode_checkpoint, encode_entry, encode_header,
};

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bytes::Bytes;

    use crate::hash::Hash;
    use crate::storage::journal::versions::CHECKPOINT_MAGIC;
    use crate::storage::wal::{JournalEntry, JournalPosition};

    use super::*;

    #[test]
    fn header_roundtrip() {
        let header = encode_header(JOURNAL_MAGIC, 1);
        let version = decode_header(&header, JOURNAL_MAGIC, 1).unwrap();
        assert_eq!(version, 1);
    }

    #[test]
    fn header_rejects_wrong_magic() {
        let header = encode_header(CHECKPOINT_MAGIC, 1);
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
        let entry = JournalEntry::Put { hash, data: data.clone() };
        let pos = JournalPosition::from_u64(42);

        let encoded = encode_entry(&entry, pos);
        let (decoded, decoded_pos, consumed) = decode_entry(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded_pos, pos);
        match decoded {
            JournalEntry::Put { hash: h, data: d } => {
                assert_eq!(h, hash);
                assert_eq!(d, data);
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn entry_roundtrip_delete() {
        let hash = Hash::from_content(b"delete-me");
        let entry = JournalEntry::Delete { hash };
        let pos = JournalPosition::from_u64(7);

        let encoded = encode_entry(&entry, pos);
        let (decoded, decoded_pos, consumed) = decode_entry(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded_pos, pos);
        assert!(matches!(decoded, JournalEntry::Delete { hash: h } if h == hash));
    }

    #[test]
    fn entry_roundtrip_constraint() {
        let target = Hash::from_content(b"target");
        let bases: BTreeSet<_> =
            [b"base1", b"base2", b"base3"].iter().map(|b| Hash::from_content(*b)).collect();
        let entry = JournalEntry::Constraint { target, bases: bases.clone() };
        let pos = JournalPosition::from_u64(99);

        let encoded = encode_entry(&entry, pos);
        let (decoded, decoded_pos, consumed) = decode_entry(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded_pos, pos);
        match decoded {
            JournalEntry::Constraint { target: t, bases: bs } => {
                assert_eq!(t, target);
                assert_eq!(bs, bases);
            }
            _ => panic!("expected Constraint"),
        }
    }

    #[test]
    fn checkpoint_roundtrip() {
        let pos = JournalPosition::from_u64(12345);
        let encoded = encode_checkpoint(pos);
        let decoded = decode_checkpoint(&encoded).unwrap();
        assert_eq!(decoded, pos);
    }

    #[test]
    fn checkpoint_rejects_corrupt() {
        let pos = JournalPosition::from_u64(42);
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
            (
                JournalPosition::from_u64(1),
                JournalEntry::Put { hash: h1, data: Bytes::from_static(b"a") },
            ),
            (JournalPosition::from_u64(2), JournalEntry::Delete { hash: h2 }),
        ];

        let mut encoded = Vec::new();
        for (pos, entry) in &entries {
            encoded.extend_from_slice(&encode_entry(entry, *pos));
        }

        let decoded = decode_entries(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, JournalPosition::from_u64(1));
        assert_eq!(decoded[1].0, JournalPosition::from_u64(2));
    }
}
