//! Pure path derivation helpers for filesystem object storage.

use std::path::{Path, PathBuf};

use crate::Hash;

use super::STORAGE_VERSION;

/// Returns canonical fan-out object file path for a hash.
pub(super) fn object_path(root: &Path, hash: Hash) -> PathBuf {
    let hex = hash.to_hex();
    let algorithm = hash.algorithm_name();
    let first = &hex[0..2];
    let second = &hex[2..4];
    let rest = &hex[4..];
    root.join(STORAGE_VERSION).join(algorithm).join(first).join(second).join(rest)
}

/// Returns canonical `.diff` object path for a hash.
pub(super) fn diff_object_path(root: &Path, hash: Hash) -> PathBuf {
    let mut path = object_path(root, hash);
    path.set_extension("diff");
    path
}
