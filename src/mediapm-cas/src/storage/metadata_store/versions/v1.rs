//! V1 persistence format for the CAS metadata store.
//!
//! Stores constraints and metadata entries together in a JSON structure
//! (serialized to/from `Vec<u8>`). The format is identified by the
//! filename (`metadata-v1.json`), not by an internal version field.
//! The `entries` field uses `#[serde(default)]` so old files
//! (constraints-only) remain loadable.
//!
//! This module owns only the V1 codec (`parse_v1_snapshot` /
//! `serialize_v1_snapshot`) and the V1-local model a parse produces. The V1 →
//! V2 rewrite lives in [`v2`](super::v2), because V2 owns the contract its own
//! result has to satisfy.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version isomorphism/migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.

use std::collections::{BTreeMap, BTreeSet};

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

/// On-disk representation of a constraint entry.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConstraintEntry {
    /// Hex-encoded base hashes.
    bases: Vec<String>,
}

/// On-disk representation of a metadata entry.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryData {
    len: u64,
    /// `"full"` or `"delta:<hex_base_hash>"`
    encoding: String,
}

/// On-disk representation of the V1 persistence file.
///
/// The format is identified by the filename (`metadata-v1.json`), not
/// by an internal version field.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotFile {
    /// Map from hex-encoded target hash → constraint entry.
    constraints: BTreeMap<String, ConstraintEntry>,
    /// Index entries (hex-encoded hash → entry data).
    /// Old files without this field still load correctly.
    #[serde(default)]
    entries: BTreeMap<String, EntryData>,
}

/// A snapshot decoded from the V1 format, before migration.
///
/// V1 records an encoding per entry and the current model derives it from the
/// tree, so this type exists only so the migration sees both shapes. It is
/// never returned to a caller outside this `versions` directory.
pub(crate) struct SnapshotV1 {
    /// Constraints: target to its bases.
    pub(crate) constraints: BTreeMap<Hash, BTreeSet<Hash>>,
    /// Entries: hash to (payload length, encoding).
    pub(crate) entries: BTreeMap<Hash, (u64, ObjectEncoding)>,
}

/// Parse V1 snapshot data from raw bytes into the V1-local model.
///
/// Returns `None` if the data is empty (no snapshot file). The result still
/// carries each entry's encoded form; dropping it is the migration's job, not
/// the parser's, because a V1 reader must keep reading V1 files as written.
pub(crate) fn parse_v1_snapshot(data: &[u8]) -> Result<Option<SnapshotV1>, CasError> {
    if data.is_empty() {
        return Ok(None);
    }

    let file: SnapshotFile = serde_json::from_slice(data).map_err(|e| CasError::CorruptObject {
        hash: None,
        details: format!("failed to parse snapshot file: {e}"),
    })?;

    // --- Constraints ---
    let mut constraints = BTreeMap::new();
    for (target_hex, entry) in &file.constraints {
        let target: Hash = target_hex.parse().map_err(|_| CasError::CorruptObject {
            hash: None,
            details: format!("invalid target hash in snapshot file: {target_hex}"),
        })?;
        let bases: BTreeSet<Hash> = entry.bases.iter().filter_map(|h| h.parse().ok()).collect();
        if bases.len() != entry.bases.len() {
            return Err(CasError::CorruptObject {
                hash: None,
                details: "invalid base hash in snapshot file".into(),
            });
        }
        constraints.insert(target, bases);
    }

    // --- Entries ---
    let mut entries = BTreeMap::new();
    for (hash_hex, data) in &file.entries {
        let hash: Hash = hash_hex.parse().map_err(|_| CasError::CorruptObject {
            hash: None,
            details: format!("invalid hash in snapshot file entries: {hash_hex}"),
        })?;
        let encoding = match data.encoding.as_str() {
            "full" => ObjectEncoding::Full,
            s if s.starts_with("delta:") => {
                let base: Hash = s[6..].parse().map_err(|_| CasError::CorruptObject {
                    hash: None,
                    details: format!("invalid delta base hash in snapshot file: {}", &s[6..]),
                })?;
                ObjectEncoding::Delta { base_hash: base }
            }
            other => {
                return Err(CasError::CorruptObject {
                    hash: None,
                    details: format!("unknown encoding in snapshot file: {other}"),
                });
            }
        };
        entries.insert(hash, (data.len, encoding));
    }

    Ok(Some(SnapshotV1 { constraints, entries }))
}

/// Serialize V1 snapshot (constraints + entries) to a `Vec<u8>`.
///
/// Nothing writes V1 in production after the V2 bump. The writer stays because
/// it is what the V1 → V2 migration is tested against: a hand-written fixture
/// would drift from the bytes the V1 format actually produced, and those bytes
/// are the only input the migration can ever be asked to read.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "nothing writes V1 in production after the V2 bump; the writer exists so the \
                  V1 to V2 migration is tested against bytes the V1 format actually produced \
                  instead of a hand-written fixture that could drift from it"
    )
)]
pub(crate) fn serialize_v1_snapshot(
    constraints: &BTreeMap<Hash, BTreeSet<Hash>>,
    entries: &BTreeMap<Hash, (u64, ObjectEncoding)>,
) -> Result<Vec<u8>, CasError> {
    let mut file = SnapshotFile { constraints: BTreeMap::new(), entries: BTreeMap::new() };

    for (target, bases) in constraints {
        let entry = ConstraintEntry { bases: bases.iter().map(ToString::to_string).collect() };
        file.constraints.insert(target.to_string(), entry);
    }

    for (hash, (len, encoding)) in entries {
        let encoding_str = match encoding {
            ObjectEncoding::Full => "full".to_string(),
            ObjectEncoding::Delta { base_hash } => format!("delta:{base_hash}"),
        };
        file.entries.insert(hash.to_string(), EntryData { len: *len, encoding: encoding_str });
    }

    serde_json::to_vec_pretty(&file).map_err(|e| CasError::Io(std::io::Error::other(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The v1 parser and writer must keep describing v1 files exactly, so a
    /// store written by an older build is still recovered. The `encoding`
    /// field is part of that contract even though the current model drops it.
    #[test]
    fn v1_bytes_keep_their_encoding_field() {
        let entry = Hash::from_content(b"entry");
        let bytes = serialize_v1_snapshot(
            &BTreeMap::new(),
            &BTreeMap::from([(entry, (3u64, ObjectEncoding::Full))]),
        )
        .expect("serialize");
        let text = String::from_utf8(bytes).expect("UTF-8 JSON");
        assert!(
            text.contains("\"encoding\": \"full\""),
            "the v1 writer must keep emitting the field a v1 reader expects: {text}"
        );
    }

    #[test]
    fn regression_v1_snapshot_rejects_unknown_field() {
        let raw = r#"{
        "constraints": {},
        "entries": {},
        "unexpected": true
    }"#;
        let err = serde_json::from_str::<SnapshotFile>(raw)
            .err()
            .expect("v1 snapshot should reject unknown fields")
            .to_string();
        assert!(
            err.contains("unknown field"),
            "v1 snapshot must reject unknown fields, got: {err}"
        );
    }
}
