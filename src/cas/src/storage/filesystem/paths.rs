//! Pure path derivation helpers for filesystem object storage.
//!
//! These helpers are intentionally side-effect free and deterministic so path
//! derivation behavior is consistent across write/read/repair code paths.

use std::path::{Path, PathBuf};

use crate::Hash;

use super::STORAGE_VERSION;

/// Returns canonical fan-out object file path for a hash.
///
/// Layout: `<root>/<storage-version>/<algorithm>/<h0h1>/<h2h3>/<remaining-hex>`.
pub(super) fn object_path(root: &Path, hash: Hash) -> PathBuf {
    let hex = hash.to_hex();
    let algorithm = hash.algorithm_name();
    let first = &hex[0..2];
    let second = &hex[2..4];
    let rest = &hex[4..];
    root.join(STORAGE_VERSION).join(algorithm).join(first).join(second).join(rest)
}

/// Returns canonical `.diff` object path for a hash.
///
/// This path is identical to [`object_path`] except for the `.diff` extension,
/// which denotes a delta-envelope payload.
pub(super) fn diff_object_path(root: &Path, hash: Hash) -> PathBuf {
    let mut path = object_path(root, hash);
    path.set_extension("diff");
    path
}
