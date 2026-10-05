//! Versioned binary wire formats for journal and checkpoint artifacts.
//!
//! The long-lived functional core is the [`Wal`](super::Wal) trait.
//! Each wire version owns its exact byte layout, parse/validate/encode
//! behavior, and `From` conversions to/from version-specific state types.
//! `vX.rs` files must never import unversioned structs outside `versions/`,
//! may reference only the most recent previous version (for migration), and
//! this `mod.rs` is the only place where latest version state is bridged to
//! unversioned runtime state.
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

mod v1;
mod v2;

use crate::error::CasError;

use super::{WalEntry, WalPosition};

use std::iter;

/// Header size for all on-disk artifacts: 6-byte magic + 2-byte version.
pub(crate) const HEADER_LEN: usize = 8;

/// Magic prefix for journal segment files.
///
/// The magic is identical across every journal version, so it cannot
/// discriminate formats on its own. The 2-byte version field that follows it
/// can, and [`decode_header`] returns it for dispatch.
pub(crate) const JOURNAL_MAGIC: [u8; 6] = v2::JOURNAL_MAGIC;
/// Current journal segment format version (the version this build writes).
pub(crate) const JOURNAL_VERSION: u16 = v2::JOURNAL_VERSION;

/// Maximum supported journal segment format version.
///
/// Every version up to this one stays readable. Raising it without keeping the
/// lower read paths alive would make existing journals unreadable.
pub(crate) const MAX_JOURNAL_VERSION: u16 = v2::MAX_JOURNAL_VERSION;

/// Encode an 8-byte header: 6-byte magic + 2-byte LE version.
///
/// The header codec is shared infrastructure, not a version's property: both
/// journal versions and the checkpoint format use the identical layout, so it
/// lives here rather than in any single `vX.rs`.
pub(crate) fn encode_header(magic: [u8; 6], version: u16) -> [u8; 8] {
    let mut buf = [0u8; 8];
    buf[..6].copy_from_slice(&magic);
    buf[6..8].copy_from_slice(&version.to_le_bytes());
    buf
}

/// Decode and validate an 8-byte header.
///
/// Returns the decoded version, which is the journal format's discriminator:
/// callers pass it to [`decode_entry`] so each segment is parsed with the
/// codec that actually wrote it.
pub(crate) fn decode_header(
    buf: [u8; 8],
    expected_magic: [u8; 6],
    max_version: u16,
) -> Result<u16, CasError> {
    if buf[..6] != expected_magic {
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

/// Encode a journal entry at the given position.
///
/// Always writes [`JOURNAL_VERSION`]: this build emits one format, and older
/// formats are read-only.
pub(crate) fn encode_entry(entry: &WalEntry, pos: WalPosition) -> Vec<u8> {
    entry_to_v2(entry).encode(pos.as_u64())
}

/// Decode a single journal entry from bytes, routing on the segment's version.
///
/// `version` is the value [`decode_header`] read from the segment's 8-byte
/// header. Both codecs share a byte layout for every variant V1 can express,
/// so the version is what decides which decoder runs: a V1 segment must not be
/// parsed by the V2 decoder, whose extra `PutLarge` opcode would silently
/// accept a byte sequence V1 could never have produced.
///
/// Returns `(entry, position, bytes_consumed)`.
///
/// # Errors
///
/// Returns [`CasError::InvalidArgument`] when `version` names a journal format
/// this build cannot read, and the codec's own corruption error when the
/// entry bytes do not match that format.
pub(crate) fn decode_entry(
    buf: &[u8],
    version: u16,
) -> Result<(WalEntry, WalPosition, usize), CasError> {
    match version {
        1 => {
            let (v1_entry, pos_u64, consumed) = v1::WalEntryV1::decode(buf)?;
            let entry = entry_from_v1(&v1_entry);
            Ok((entry, WalPosition::from_u64(pos_u64), consumed))
        }
        2 => {
            let (v2_entry, pos_u64, consumed) = v2::WalEntryV2::decode(buf)?;
            let entry = entry_from_v2(&v2_entry);
            Ok((entry, WalPosition::from_u64(pos_u64), consumed))
        }
        other => Err(CasError::InvalidArgument(format!(
            "unsupported journal entry format version {other} (max {MAX_JOURNAL_VERSION})"
        ))),
    }
}

/// Decode all journal entries from a buffer, routing on the segment's version.
pub(crate) fn decode_entries(
    buf: &[u8],
    version: u16,
) -> Result<Vec<(WalPosition, WalEntry)>, CasError> {
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset < buf.len() {
        let (entry, pos, consumed) = decode_entry(&buf[offset..], version)?;
        entries.push((pos, entry));
        offset += consumed;
    }
    Ok(entries)
}

/// Iterate over journal entries in a buffer without allocating a Vec.
///
/// Yields `(position, entry)` tuples from the wire format. Returns
/// `CasError::CorruptObject` on malformed data. After yielding an error,
/// the iterator terminates (does not try to resync).
pub(crate) fn decode_entries_streaming(
    buf: &[u8],
    version: u16,
) -> impl Iterator<Item = Result<(WalPosition, WalEntry), CasError>> + '_ {
    let mut offset = 0;
    iter::from_fn(move || {
        if offset >= buf.len() {
            return None;
        }
        match decode_entry(&buf[offset..], version) {
            Ok((entry, pos, consumed)) => {
                offset += consumed;
                Some(Ok((pos, entry)))
            }
            Err(e) => {
                offset = buf.len(); // prevent infinite loop
                Some(Err(e))
            }
        }
    })
}

/// Encode a checkpoint file for the given position.
pub(crate) fn encode_checkpoint(pos: WalPosition) -> Vec<u8> {
    v1::CheckpointV1::encode(pos.as_u64())
}

/// Decode a checkpoint file, returning the last consumed position.
pub(crate) fn decode_checkpoint(buf: &[u8]) -> Result<WalPosition, CasError> {
    let pos_u64 = v1::CheckpointV1::decode(buf)?;
    Ok(WalPosition::from_u64(pos_u64))
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

fn entry_to_v2(entry: &WalEntry) -> v2::WalEntryV2 {
    match entry {
        WalEntry::Put { hash, data } => v2::WalEntryV2::Put { hash: *hash, data: data.clone() },
        WalEntry::PutLarge { hash, content_len } => {
            v2::WalEntryV2::PutLarge { hash: *hash, content_len: *content_len }
        }
        WalEntry::Delete { hash } => v2::WalEntryV2::Delete { hash: *hash },
        WalEntry::Constraint { target, bases } => {
            v2::WalEntryV2::Constraint { target: *target, bases: bases.clone() }
        }
    }
}

fn entry_from_v2(entry: &v2::WalEntryV2) -> WalEntry {
    match entry {
        v2::WalEntryV2::Put { hash, data } => WalEntry::Put { hash: *hash, data: data.clone() },
        v2::WalEntryV2::PutLarge { hash, content_len } => {
            WalEntry::PutLarge { hash: *hash, content_len: *content_len }
        }
        v2::WalEntryV2::Delete { hash } => WalEntry::Delete { hash: *hash },
        v2::WalEntryV2::Constraint { target, bases } => {
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
        let version = decode_header(header, JOURNAL_MAGIC, 1).unwrap();
        assert_eq!(version, 1);
    }

    /// Builds a byte-exact V1-format journal segment: V1 header followed by
    /// the V1 encodings of the given entries.
    fn v1_segment(entries: &[(u64, v1::WalEntryV1)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_header(v1::JOURNAL_MAGIC, v1::JOURNAL_VERSION));
        for (pos, entry) in entries {
            buf.extend_from_slice(&entry.encode(*pos));
        }
        buf
    }

    /// Every journal version this build can read must survive dispatch to the
    /// unversioned `WalEntry`. This is the ladder's readability contract, and
    /// it is the only proof that a V1 segment written by an older build still
    /// parses after V2 became the write format.
    #[test]
    fn v1_journal_round_trips_through_dispatch() {
        let data = Bytes::from_static(b"v1 payload");
        let put_hash = Hash::from_content(&data);
        let del_hash = Hash::from_content(b"absent");
        let target = Hash::from_content(b"target");
        let base = Hash::from_content(b"base");

        let entries = vec![
            (10u64, v1::WalEntryV1::Put { hash: put_hash, data: data.clone() }),
            (11, v1::WalEntryV1::Delete { hash: del_hash }),
            (12, v1::WalEntryV1::Constraint { target, bases: BTreeSet::from([base]) }),
        ];
        let segment = v1_segment(&entries);

        // The header alone says which codec must run.
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(&segment[..HEADER_LEN]);
        let version = decode_header(header, JOURNAL_MAGIC, MAX_JOURNAL_VERSION)
            .expect("a V1 segment header must validate against the shared magic");
        assert_eq!(version, v1::JOURNAL_VERSION);

        let decoded = decode_entries(&segment[HEADER_LEN..], version)
            .expect("V1 entries must dispatch-decode");
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].0, WalPosition::from_u64(10));
        assert!(
            matches!(
                &decoded[0].1,
                WalEntry::Put { hash, data: d } if *hash == put_hash && d == &data
            ),
            "entry 0 must dispatch to a Put carrying the original payload: {:?}",
            decoded[0].1
        );
        assert!(
            matches!(&decoded[1].1, WalEntry::Delete { hash } if *hash == del_hash),
            "entry 1 must dispatch to a Delete: {:?}",
            decoded[1].1
        );
        assert!(
            matches!(
                &decoded[2].1,
                WalEntry::Constraint { target: t, bases } if *t == target && *bases == BTreeSet::from([base])
            ),
            "entry 2 must dispatch to a Constraint: {:?}",
            decoded[2].1
        );
    }

    /// A V1 journal's entries must decode identically through the streaming
    /// decoder, so both read paths agree on old data.
    #[test]
    fn v1_journal_round_trips_through_streaming_dispatch() {
        let data = Bytes::from_static(b"streaming v1");
        let hash = Hash::from_content(&data);
        let segment = v1_segment(&[(1u64, v1::WalEntryV1::Put { hash, data: data.clone() })]);

        let streaming: Vec<_> =
            decode_entries_streaming(&segment[HEADER_LEN..], v1::JOURNAL_VERSION).collect();
        assert_eq!(streaming.len(), 1);
        let (pos, entry) = streaming[0].as_ref().expect("entry must decode");
        assert_eq!(*pos, WalPosition::from_u64(1));
        assert!(
            matches!(
                entry,
                WalEntry::Put { hash: h, data: d } if *h == hash && d == &data
            ),
            "streaming dispatch must yield the same Put as the batch decoder: {entry:?}"
        );
    }

    /// The V1 -> V2 entry migration is total and lossless: every variant V1
    /// can express maps onto a V2 entry with identical content. V1 cannot
    /// produce `PutLarge`, so no information is dropped anywhere.
    #[test]
    fn v1_to_v2_entry_migration_is_total_and_lossless() {
        let data = Bytes::from_static(b"migrate me");
        let put = Hash::from_content(&data);
        let del = Hash::from_content(b"gone");
        let target = Hash::from_content(b"t");
        let base = Hash::from_content(b"b");

        let pairs = [
            (
                v1::WalEntryV1::Put { hash: put, data: data.clone() },
                v2::WalEntryV2::Put { hash: put, data },
            ),
            (v1::WalEntryV1::Delete { hash: del }, v2::WalEntryV2::Delete { hash: del }),
            (
                v1::WalEntryV1::Constraint { target, bases: BTreeSet::from([base]) },
                v2::WalEntryV2::Constraint { target, bases: BTreeSet::from([base]) },
            ),
        ];

        for (v1_entry, expected) in pairs {
            let migrated: v2::WalEntryV2 = v1_entry.clone().into();
            assert_eq!(migrated, expected);
        }
    }

    /// The header version, not the magic, is the discriminator. Both journal
    /// versions share the magic, so a V1 and a V2 segment differ only in the
    /// two version bytes. If that ever stops being true, dispatch is wrong.
    #[test]
    fn journal_versions_share_magic_and_differ_only_in_version() {
        assert_eq!(v1::JOURNAL_MAGIC, v2::JOURNAL_MAGIC, "magic must not discriminate");
        let v1_header = encode_header(v1::JOURNAL_MAGIC, v1::JOURNAL_VERSION);
        let v2_header = encode_header(v2::JOURNAL_MAGIC, v2::JOURNAL_VERSION);
        assert_eq!(v1_header[..6], v2_header[..6], "the 6 magic bytes must match");
        assert_ne!(v1_header[6..8], v2_header[6..8], "the 2 version bytes must differ");
    }

    /// An unreadable version must be rejected by dispatch rather than guessed.
    #[test]
    fn dispatch_rejects_a_version_beyond_the_supported_range() {
        let err = decode_entry(&[0u8; 64], v2::JOURNAL_VERSION + 1)
            .expect_err("a version above MAX_JOURNAL_VERSION must not decode");
        assert!(
            err.to_string().contains("unsupported journal entry format version"),
            "error must name the unsupported version: {err}"
        );
    }

    /// The V1 decoder must reject `op_type == 3` (`PutLarge`), which only V2
    /// can emit. This is the observable difference that makes routing on the
    /// header version meaningful rather than decorative.
    #[test]
    fn v1_decoder_rejects_the_v2_only_put_large_opcode() {
        let hash = Hash::from_content(b"large");
        let put_large = v2::WalEntryV2::PutLarge { hash, content_len: 42 };
        let bytes = put_large.encode(1);

        assert!(
            v1::WalEntryV1::decode(&bytes).is_err(),
            "V1 has no PutLarge variant and must reject op_type 3"
        );
        assert!(v2::WalEntryV2::decode(&bytes).is_ok(), "V2 must accept its own PutLarge opcode");
    }

    #[test]
    fn header_rejects_wrong_magic() {
        let header = encode_header(v1::CHECKPOINT_MAGIC, 1);
        assert!(decode_header(header, JOURNAL_MAGIC, 1).is_err());
    }

    #[test]
    fn header_rejects_unknown_version() {
        let mut header = encode_header(JOURNAL_MAGIC, 99);
        // decode_header rejects > max_version
        assert!(decode_header(header, JOURNAL_MAGIC, 1).is_err());
        // Also reject version 0
        header = encode_header(JOURNAL_MAGIC, 0);
        assert!(decode_header(header, JOURNAL_MAGIC, 1).is_err());
    }

    #[test]
    fn entry_roundtrip_put() {
        let data = Bytes::from_static(b"hello world");
        let hash = Hash::from_content(&data);
        let entry = WalEntry::Put { hash, data: data.clone() };
        let pos = WalPosition::from_u64(42);

        let encoded = encode_entry(&entry, pos);
        let (decoded, decoded_pos, consumed) = decode_entry(&encoded, JOURNAL_VERSION).unwrap();
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
        let (decoded, decoded_pos, consumed) = decode_entry(&encoded, JOURNAL_VERSION).unwrap();
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
        let (decoded, decoded_pos, consumed) = decode_entry(&encoded, JOURNAL_VERSION).unwrap();
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

        let decoded = decode_entries(&encoded, JOURNAL_VERSION).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, WalPosition::from_u64(1));
        assert_eq!(decoded[1].0, WalPosition::from_u64(2));
    }

    #[test]
    fn streaming_decoder_matches_batch_decoder() {
        let h1 = Hash::from_content(b"data1");
        let h2 = Hash::from_content(b"data2");
        let target = Hash::from_content(b"constraint-target");
        let entries = vec![
            (
                WalPosition::from_u64(10),
                WalEntry::Put { hash: h1, data: Bytes::from_static(b"data1") },
            ),
            (WalPosition::from_u64(20), WalEntry::Delete { hash: h2 }),
            (
                WalPosition::from_u64(30),
                WalEntry::Constraint {
                    target,
                    bases: std::iter::once(Hash::from_content(b"base")).collect(),
                },
            ),
        ];

        let mut encoded = Vec::new();
        for (pos, entry) in &entries {
            encoded.extend_from_slice(&encode_entry(entry, *pos));
        }

        let batch: Vec<(WalPosition, WalEntry)> =
            decode_entries(&encoded, JOURNAL_VERSION).unwrap();
        let streaming: Vec<Result<(WalPosition, WalEntry), CasError>> =
            decode_entries_streaming(&encoded, JOURNAL_VERSION).collect();

        assert_eq!(streaming.len(), batch.len());
        for (s, b) in streaming.iter().zip(batch.iter()) {
            let s = s.as_ref().unwrap();
            assert_eq!(s.0, b.0, "position mismatch");
            // Compare entries via re-encoding (WalEntry does not implement PartialEq)
            let s_encoded = encode_entry(&s.1, s.0);
            let b_encoded = encode_entry(&b.1, b.0);
            assert_eq!(s_encoded, b_encoded, "entry mismatch at position {:?}", s.0);
        }
    }

    #[test]
    fn streaming_decoder_rejects_corrupt_data() {
        let hash = Hash::from_content(b"good");
        let good = encode_entry(
            &WalEntry::Put { hash, data: Bytes::from_static(b"good") },
            WalPosition::from_u64(1),
        );
        // truncated entry (incomplete header)
        let corrupt = good[..good.len() - 3].to_vec();

        let mut buf = Vec::new();
        buf.extend_from_slice(&good);
        buf.extend_from_slice(&corrupt);

        let mut iter = decode_entries_streaming(&buf, JOURNAL_VERSION);
        assert!(iter.next().unwrap().is_ok());
        assert!(iter.next().unwrap().is_err());
        assert!(iter.next().is_none());
    }

    #[test]
    fn streaming_decoder_empty_buffer() {
        let results: Vec<Result<(WalPosition, WalEntry), CasError>> =
            decode_entries_streaming(b"", JOURNAL_VERSION).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn streaming_decoder_single_entry() {
        let hash = Hash::from_content(b"single");
        let entry = WalEntry::Put { hash, data: Bytes::from_static(b"single") };
        let encoded = encode_entry(&entry, WalPosition::from_u64(1));

        let mut iter = decode_entries_streaming(&encoded, JOURNAL_VERSION);
        let result = iter.next().unwrap();
        assert!(result.is_ok());
        let (pos, decoded) = result.unwrap();
        assert_eq!(pos, WalPosition::from_u64(1));
        match decoded {
            WalEntry::Put { hash, data } => {
                assert_eq!(hash, Hash::from_content(b"single"));
                assert_eq!(data, Bytes::from_static(b"single"));
            }
            _ => panic!("expected a Put entry"),
        }
        assert!(iter.next().is_none());
    }
}
