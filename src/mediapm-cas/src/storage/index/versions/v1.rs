//! V1 persistence format for the CAS index's constraint data.
//!
//! Stores the constraint map as a versioned JSON file, keyed by hex-encoded
//! target hash to a sorted list of hex-encoded base hashes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::error::CasError;
use crate::hash::Hash;

/// On-disk representation of a constraint entry.
#[derive(serde::Serialize, serde::Deserialize)]
struct ConstraintEntry {
    /// Hex-encoded base hashes.
    bases: Vec<String>,
}

/// On-disk representation of the V1 constraint file.
#[derive(serde::Serialize, serde::Deserialize)]
struct ConstraintFile {
    version: u32,
    /// Map from hex-encoded target hash → constraint entry.
    constraints: BTreeMap<String, ConstraintEntry>,
}

/// Load constraints from a V1 file.
///
/// Returns an empty map if the file does not exist.
pub(crate) fn load(path: &Path) -> Result<BTreeMap<Hash, BTreeSet<Hash>>, CasError> {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(e) => return Err(CasError::Io(e)),
    };

    let file: ConstraintFile =
        serde_json::from_slice(&data).map_err(|e| CasError::CorruptObject {
            hash: None,
            details: format!("failed to parse constraint file: {e}"),
        })?;

    if file.version != 1 {
        return Err(CasError::CorruptObject {
            hash: None,
            details: format!("unsupported constraint file version: {}", file.version),
        });
    }

    let mut result = BTreeMap::new();
    for (target_hex, entry) in file.constraints {
        let target: Hash = target_hex.parse().map_err(|_| CasError::CorruptObject {
            hash: None,
            details: format!("invalid target hash in constraint file: {target_hex}"),
        })?;
        let bases: BTreeSet<Hash> = entry.bases.iter().filter_map(|h| h.parse().ok()).collect();
        if bases.len() != entry.bases.len() {
            return Err(CasError::CorruptObject {
                hash: None,
                details: "invalid base hash in constraint file".into(),
            });
        }
        result.insert(target, bases);
    }
    Ok(result)
}

/// Save constraints to a V1 file.
///
/// Creates parent directories if needed. Writes atomically via temp+rename.
pub(crate) fn save(
    path: &Path,
    constraints: &BTreeMap<Hash, BTreeSet<Hash>>,
) -> Result<(), CasError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(CasError::Io)?;
    }

    let mut file = ConstraintFile { version: 1, constraints: BTreeMap::new() };

    for (target, bases) in constraints {
        let entry = ConstraintEntry { bases: bases.iter().map(|h| h.to_string()).collect() };
        file.constraints.insert(target.to_string(), entry);
    }

    // Atomic write via temp+rename.
    let tmp_path = path.with_extension("tmp");
    let json = serde_json::to_vec_pretty(&file)
        .map_err(|e| CasError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
    std::fs::write(&tmp_path, &json).map_err(CasError::Io)?;
    std::fs::rename(&tmp_path, path).map_err(CasError::Io)?;
    Ok(())
}
