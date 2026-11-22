//! V2 persistence format for the CAS metadata store.
//!
//! The format is identified by the filename (`metadata-v2.json`), not by an
//! internal version field. Compared with V1 it drops the per-entry `encoding`:
//! which physical form an object has is a property of the tree (`<hash>` or
//! `<hash>.diff`), so persisting it here would be a second copy that can
//! disagree with the bytes on disk.
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

use super::SnapshotData;

/// On-disk representation of a constraint entry.
#[derive(serde::Serialize, serde::Deserialize)]
struct ConstraintEntry {
    /// Hex-encoded base hashes.
    bases: Vec<String>,
}

/// On-disk representation of a V2 persistence file.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotFile {
    /// Map from hex-encoded target hash to constraint entry.
    constraints: BTreeMap<String, ConstraintEntry>,
    /// Index entries keyed by hex-encoded hash. Old files without this field
    /// still load correctly.
    #[serde(default)]
    entries: BTreeMap<String, u64>,
}

/// Parse V2 snapshot data from raw bytes.
///
/// Returns `None` when the data is empty, which every version treats as an
/// absent snapshot rather than an empty one. An entry is a bare payload length,
/// so a file carrying V1's `{len, encoding}` object fails here as a type error
/// instead of being read with the extra fact silently ignored.
pub(crate) fn parse_v2_snapshot(data: &[u8]) -> Result<Option<SnapshotData>, CasError> {
    if data.is_empty() {
        return Ok(None);
    }
    let file: SnapshotFile = serde_json::from_slice(data).map_err(|e| CasError::CorruptObject {
        hash: None,
        details: format!("failed to parse a v2 snapshot file: {e}"),
    })?;
    let constraints: BTreeMap<String, Vec<String>> =
        file.constraints.into_iter().map(|(target_hex, entry)| (target_hex, entry.bases)).collect();
    let entries: BTreeMap<Hash, u64> = file
        .entries
        .into_iter()
        .map(|(hex, len)| hex.parse::<Hash>().map(|hash| (hash, len)))
        .collect::<Result<_, _>>()
        .map_err(|_| CasError::CorruptObject {
            hash: None,
            details: "invalid hash in a v2 snapshot file's entries".into(),
        })?;
    super::decode_snapshot_body(constraints, entries).map(Some)
}

/// Serialize V2 snapshot data to a `Vec<u8>`.
pub(crate) fn serialize_v2_snapshot(
    constraints: &BTreeMap<Hash, BTreeSet<Hash>>,
    entries: &BTreeMap<Hash, u64>,
) -> Result<Vec<u8>, CasError> {
    let mut file = SnapshotFile { constraints: BTreeMap::new(), entries: BTreeMap::new() };
    for (target, bases) in constraints {
        file.constraints.insert(
            target.to_string(),
            ConstraintEntry { bases: bases.iter().map(ToString::to_string).collect() },
        );
    }
    for (hash, len) in entries {
        file.entries.insert(hash.to_string(), *len);
    }
    serde_json::to_vec_pretty(&file).map_err(|e| CasError::Io(std::io::Error::other(e)))
}

/// Decode a V2 snapshot into the current model.
///
/// V2 is the current format, so this is the parser rather than a migration.
/// The ladder reaches V2 through this one seam and never through
/// [`parse_v2_snapshot`] directly, so every version has the same shape of
/// loader.
pub(crate) fn load_v2_snapshot(data: &[u8]) -> Result<Option<SnapshotData>, CasError> {
    parse_v2_snapshot(data)
}

/// Migrate a V1 snapshot into this version's shape.
///
/// The migration is a loss by design: `encoding` has no counterpart here
/// because which physical form an object has is read from the tree. Dropping it
/// is the point of V2, so nothing is invented to replace it.
///
/// This lives in `v2.rs` rather than `v1.rs` because V2 owns the contract the
/// result must satisfy, and it is the only place allowed to know both shapes.
pub(crate) fn migrate_v1_to_v2(snapshot: super::v1::SnapshotV1) -> SnapshotData {
    let entries: BTreeMap<Hash, u64> =
        snapshot.entries.into_iter().map(|(hash, (len, _encoding))| (hash, len)).collect();
    (snapshot.constraints, entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_roundtrip_preserves_constraints_and_lengths() {
        let target = Hash::from_content(b"target");
        let base = Hash::from_content(b"base");
        let entry = Hash::from_content(b"entry");
        let constraints = BTreeMap::from([(target, BTreeSet::from([base]))]);
        let entries = BTreeMap::from([(entry, 42u64)]);

        let bytes = serialize_v2_snapshot(&constraints, &entries).expect("serialize");
        let (read_constraints, read_entries) =
            parse_v2_snapshot(&bytes).expect("parse").expect("a snapshot, not an absence");
        assert_eq!(read_constraints, constraints);
        assert_eq!(read_entries, entries);
    }

    #[test]
    fn an_empty_v2_snapshot_is_an_absence() {
        assert!(parse_v2_snapshot(b"").expect("empty input must not error").is_none());
    }

    /// The migration's only job is to drop the encoding and keep every length,
    /// so a V1 entry set that mixes forms must survive it unchanged otherwise.
    #[test]
    fn migrating_v1_keeps_every_length_and_drops_the_encoding() {
        let full = Hash::from_content(b"full");
        let base = Hash::from_content(b"delta-base");
        let target = Hash::from_content(b"target");
        let snapshot = super::super::v1::SnapshotV1 {
            constraints: BTreeMap::from([(target, BTreeSet::from([base]))]),
            entries: BTreeMap::from([
                (full, (7u64, crate::api::ObjectEncoding::Full)),
                (base, (9u64, crate::api::ObjectEncoding::Delta { base_hash: base })),
            ]),
        };

        let (constraints, entries) = migrate_v1_to_v2(snapshot);

        assert_eq!(constraints, BTreeMap::from([(target, BTreeSet::from([base]))]));
        assert_eq!(entries, BTreeMap::from([(full, 7u64), (base, 9u64)]));
    }
}
