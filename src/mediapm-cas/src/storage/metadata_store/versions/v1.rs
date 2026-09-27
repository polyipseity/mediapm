//! V1 persistence format for the CAS metadata store.
//!
//! Stores constraints and metadata entries together in a JSON structure
//! (serialized to/from `Vec<u8>`). The format is identified by the
//! filename (`metadata-v1.json`), not by an internal version field.
//! The `entries` field uses `#[serde(default)]` so old files
//! (constraints-only) remain loadable.
//!
//! This module owns two things: the V1 codec (`parse_v1_snapshot` /
//! `serialize_v1_snapshot`) and the V1 → current migration hook
//! (`migrate_v1_to_current`) that a future `v2.rs` would replace.
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

use super::SnapshotData;

/// On-disk representation of a constraint entry.
#[derive(serde::Serialize, serde::Deserialize)]
struct ConstraintEntry {
    /// Hex-encoded base hashes.
    bases: Vec<String>,
}

/// On-disk representation of a metadata entry.
#[derive(serde::Serialize, serde::Deserialize)]
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
struct SnapshotFile {
    /// Map from hex-encoded target hash → constraint entry.
    constraints: BTreeMap<String, ConstraintEntry>,
    /// Index entries (hex-encoded hash → entry data).
    /// Old files without this field still load correctly.
    #[serde(default)]
    entries: BTreeMap<String, EntryData>,
}

/// Parse V1 snapshot data from raw bytes.
///
/// Returns `None` if the data is empty (no snapshot file).
pub(crate) fn parse_v1_snapshot(data: &[u8]) -> Result<Option<SnapshotData>, CasError> {
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

    Ok(Some((constraints, entries)))
}

/// Serialize V1 snapshot (constraints + entries) to a `Vec<u8>`.
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

/// Migrate a snapshot decoded from the V1 format into the current snapshot model.
///
/// V1 is the newest format this build implements, so the migration is the
/// identity today. The hook exists so the ladder has exactly one place where
/// "carry an old snapshot forward" is written: a V2 cannot change the V1
/// struct (the V1 loader must keep reading V1 files byte-for-byte as they
/// were written), so the difference between the two shapes has to live in a
/// function that sees both.
///
/// ## What a real V1 → V2 migration would have to do
///
/// - **Renamed, re-typed, or regrouped fields**: read [`SnapshotFile`] and emit
///   the V2 struct. This is the only function allowed to know both shapes.
/// - **New required data with no V1 source**: the snapshot cannot supply it,
///   and inventing a value would fabricate metadata that later verification
///   would contradict. The field must be optional, or the file must be
///   re-derived from the WAL
///   (`MetadataStore::rebuild_from_wal` replays the same facts) before this
///   migration runs.
/// - **Dropped or narrowed data**: the caller deletes the superseded file once
///   this returns `Ok`, which is why
///   [`METADATA_FORMAT_NAMES`](super::METADATA_FORMAT_NAMES) keeps the old
///   name readable for exactly as long as the migration may still need it.
/// - **A version this build does not know**: unreachable from here.
///   [`migrate_snapshot_to_current`](super::migrate_snapshot_to_current)
///   rejects an unregistered version before it reaches this function, so a
///   `metadata-v2.json` written by a newer build is an error on this build
///   rather than a V1 parse.
///
/// # Errors
///
/// Cannot fail while V1 is the current format; the signature is fallible
/// because the rewrite a V2 introduces is not, and the dispatch in
/// `versions/mod.rs` calls every version through the one fallible seam rather
/// than special-casing the newest.
#[expect(
    clippy::unnecessary_wraps,
    reason = "the migration seam must be fallible before it is fallible in fact: a V2 rewrite \
              that can reject a snapshot it cannot rewrite would otherwise force a signature \
              change on every caller at exactly the moment the ladder is being extended"
)]
pub(crate) fn migrate_v1_to_current(snapshot: SnapshotData) -> Result<SnapshotData, CasError> {
    Ok(snapshot)
}
