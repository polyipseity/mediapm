//! Versioned persistence for index constraint data.
//!
//! Currently only V1 is supported. The module dispatches to the correct
//! version handler based on the file format version marker.
//!
//! Functions are async (wrapping blocking I/O via `spawn_blocking`) so callers
//! stay in the async runtime.

mod v1;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::error::CasError;
use crate::hash::Hash;

/// The current format version used when saving new files.
pub(crate) const FORMAT_VERSION: u32 = 1;

/// Load constraint data from `path` in the given `version` format.
///
/// Returns an empty map if the file doesn't exist.
pub(crate) async fn load(
    path: &Path,
    version: u32,
) -> Result<BTreeMap<Hash, BTreeSet<Hash>>, CasError> {
    let owned = path.to_owned();
    tokio::task::spawn_blocking(move || match_version(version, &owned))
        .await
        .map_err(|e| CasError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
}

fn match_version(version: u32, path: &Path) -> Result<BTreeMap<Hash, BTreeSet<Hash>>, CasError> {
    match version {
        1 => v1::load(path),
        v => Err(CasError::CorruptObject {
            hash: None,
            details: format!("unsupported constraint file version: {v}"),
        }),
    }
}

/// Save constraint data to `path` in the current format (V1).
pub(crate) async fn save(
    path: &Path,
    constraints: &BTreeMap<Hash, BTreeSet<Hash>>,
) -> Result<(), CasError> {
    let owned = path.to_owned();
    let clone = constraints.clone();
    tokio::task::spawn_blocking(move || v1::save(&owned, &clone))
        .await
        .map_err(|e| CasError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
}
