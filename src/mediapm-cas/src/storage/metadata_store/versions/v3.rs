//! V3 persistence format for the CAS metadata store.
//!
//! V3 renames the v2 fields `constraints` and `entries` to `delta_bases` and
//! `lengths`, and adds a `corrupted` set. The rename is the format change; the
//! corruption record is the new fact. Both are carried in one snapshot because
//! a reader that finds a corrupt hash should not have to rediscover it by
//! probing the tree.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version isomorphism/migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::CasError;
use crate::hash::Hash;

use super::Snapshot;

/// On-disk representation of a V3 persistence file.
///
/// `corrupted` defaults on read so a v3 file written before a corruption was
/// ever recorded still loads, and `deny_unknown_fields` keeps a misspelled or
/// future field from being silently ignored.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotFile {
    /// Constraint target to its candidate bases.
    delta_bases: BTreeMap<String, Vec<String>>,
    /// Payload length per hash.
    lengths: BTreeMap<String, u64>,
    /// Hashes whose every physical form fails verification.
    #[serde(default)]
    corrupted: BTreeSet<String>,
}

/// Parse V3 snapshot data from raw bytes.
///
/// Returns `None` when the data is empty, which every version treats as an
/// absent snapshot rather than an empty one.
pub(crate) fn parse_v3_snapshot(data: &[u8]) -> Result<Option<Snapshot>, CasError> {
    if data.is_empty() {
        return Ok(None);
    }
    let file: SnapshotFile = serde_json::from_slice(data).map_err(|e| CasError::CorruptObject {
        hash: None,
        details: format!("failed to parse a v3 snapshot file: {e}"),
    })?;
    let mut corrupted = BTreeSet::new();
    for hex in &file.corrupted {
        let hash: Hash = hex.parse().map_err(|_| CasError::CorruptObject {
            hash: None,
            details: format!("invalid hash in a v3 snapshot file's corrupted set: {hex}"),
        })?;
        corrupted.insert(hash);
    }
    let lengths: BTreeMap<Hash, u64> = file
        .lengths
        .into_iter()
        .map(|(hex, len)| hex.parse::<Hash>().map(|hash| (hash, len)))
        .collect::<Result<_, _>>()
        .map_err(|_| CasError::CorruptObject {
            hash: None,
            details: "invalid hash in a v3 snapshot file's lengths".into(),
        })?;
    let (delta_bases, lengths) = super::decode_snapshot_body(file.delta_bases, lengths)?;
    Ok(Some(Snapshot { delta_bases, lengths, corrupted }))
}

/// Serialize V3 snapshot data to a `Vec<u8>`.
pub(crate) fn serialize_v3_snapshot(snapshot: &Snapshot) -> Result<Vec<u8>, CasError> {
    let mut file = SnapshotFile {
        delta_bases: BTreeMap::new(),
        lengths: BTreeMap::new(),
        corrupted: BTreeSet::new(),
    };
    for (target, bases) in &snapshot.delta_bases {
        file.delta_bases
            .insert(target.to_string(), bases.iter().map(ToString::to_string).collect());
    }
    for (hash, len) in &snapshot.lengths {
        file.lengths.insert(hash.to_string(), *len);
    }
    for hash in &snapshot.corrupted {
        file.corrupted.insert(hash.to_string());
    }
    serde_json::to_vec_pretty(&file).map_err(|e| CasError::Io(std::io::Error::other(e)))
}

/// Decode a V3 snapshot into the current model.
///
/// V3 is the current format, so this is the parser rather than a migration.
pub(crate) fn load_v3_snapshot(data: &[u8]) -> Result<Option<Snapshot>, CasError> {
    parse_v3_snapshot(data)
}

/// Migrate a V2 snapshot into this version's shape.
///
/// V2 has no corruption record. The set is empty rather than guessed, because
/// inventing corruption facts is worse than missing them.
pub(crate) fn migrate_v2_to_v3(snapshot: super::v2::SnapshotV2) -> Snapshot {
    Snapshot {
        delta_bases: snapshot.constraints,
        lengths: snapshot.entries,
        corrupted: BTreeSet::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The v2 codec renamed two fields and added one. The migration must move
    /// the facts and invent nothing: a v2 file cannot know about corruption, so
    /// the set it migrates to is empty rather than guessed.
    #[test]
    fn migrating_v2_renames_the_fields_and_records_no_corruption() {
        let target = Hash::from_content(b"target");
        let base = Hash::from_content(b"base");
        let entry = Hash::from_content(b"entry");
        let migrated = migrate_v2_to_v3(super::super::v2::SnapshotV2 {
            constraints: BTreeMap::from([(target, BTreeSet::from([base]))]),
            entries: BTreeMap::from([(entry, 4u64)]),
        });
        assert_eq!(migrated.delta_bases, BTreeMap::from([(target, BTreeSet::from([base]))]));
        assert_eq!(migrated.lengths, BTreeMap::from([(entry, 4u64)]));
        assert!(migrated.corrupted.is_empty());
    }

    /// V3 round-trips every field through its own codec.
    #[test]
    fn v3_roundtrip_preserves_all_fields() {
        let target = Hash::from_content(b"target");
        let base = Hash::from_content(b"base");
        let entry = Hash::from_content(b"entry");
        let corrupt = Hash::from_content(b"corrupt");
        let snapshot = Snapshot {
            delta_bases: BTreeMap::from([(target, BTreeSet::from([base]))]),
            lengths: BTreeMap::from([(entry, 42u64)]),
            corrupted: BTreeSet::from([corrupt]),
        };

        let bytes = serialize_v3_snapshot(&snapshot).expect("serialize");
        let read = parse_v3_snapshot(&bytes).expect("parse").expect("a snapshot, not an absence");
        assert_eq!(read, snapshot);
    }

    #[test]
    fn an_empty_v3_snapshot_is_an_absence() {
        assert!(parse_v3_snapshot(b"").expect("empty input must not error").is_none());
    }
}
