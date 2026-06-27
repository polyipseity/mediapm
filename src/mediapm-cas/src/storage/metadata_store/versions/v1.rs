//! V1 persistence format for the CAS metadata store.
//!
//! Stores constraints and metadata entries together in a single versioned
//! JSON file. The `entries` field uses `#[serde(default)]` so old files
//! (constraints-only) remain loadable.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

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

/// Load snapshot (constraints + entries) from a V1 file.
///
/// Returns empty maps if the file does not exist.
pub(crate) fn load(path: &Path) -> Result<SnapshotData, CasError> {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((BTreeMap::new(), BTreeMap::new()));
        }
        Err(e) => return Err(CasError::Io(e)),
    };

    let file: SnapshotFile =
        serde_json::from_slice(&data).map_err(|e| CasError::CorruptObject {
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

    Ok((constraints, entries))
}

/// Save snapshot (constraints + entries) to a V1 file.
///
/// Creates parent directories if needed. Writes atomically via temp+rename.
pub(crate) fn save(
    path: &Path,
    constraints: &BTreeMap<Hash, BTreeSet<Hash>>,
    entries: &BTreeMap<Hash, (u64, ObjectEncoding)>,
) -> Result<(), CasError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(CasError::Io)?;
    }

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

    // Atomic write via temp+rename.
    let tmp_path = path.with_extension("tmp");
    let json =
        serde_json::to_vec_pretty(&file).map_err(|e| CasError::Io(std::io::Error::other(e)))?;
    std::fs::write(&tmp_path, &json).map_err(CasError::Io)?;
    std::fs::rename(&tmp_path, path).map_err(CasError::Io)?;
    Ok(())
}
