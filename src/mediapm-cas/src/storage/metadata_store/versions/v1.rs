//! V1 persistence format for the CAS metadata store.
//!
//! Stores constraints and metadata entries together in a versioned JSON
//! structure (serialized to/from `Vec<u8>`). The `entries` field uses
//! `#[serde(default)]` so old files (constraints-only) remain loadable.

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
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotFile {
    version: u32,
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

    if file.version != 1 {
        return Err(CasError::CorruptObject {
            hash: None,
            details: format!("unsupported snapshot file version: {}", file.version),
        });
    }

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
    let mut file =
        SnapshotFile { version: 1, constraints: BTreeMap::new(), entries: BTreeMap::new() };

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
