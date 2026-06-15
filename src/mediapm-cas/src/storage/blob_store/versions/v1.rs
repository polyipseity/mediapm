//! V1 directory layout for blob storage.
//!
//! V1 uses a hash-derived fan-out tree rooted at `<root>/v1/blake3/ab/cd/<hex>`.
//! Full blobs are stored at the leaf path; delta blobs use a `.diff` suffix.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version isomorphism.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.

use std::path::{Path, PathBuf};

use crate::hash::Hash;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The path version segment used in blob storage directory layout.
pub(crate) const BLOB_PATH_VERSION: &str = "v1";

// ---------------------------------------------------------------------------
// Path derivation
// ---------------------------------------------------------------------------

/// Derive the full-blob path for a hash using the V1 layout.
pub(crate) fn hash_to_path(root: &Path, hash: &Hash) -> PathBuf {
    let hex = hash.to_hex();
    root.join(BLOB_PATH_VERSION).join("blake3").join(&hex[0..2]).join(&hex[2..4]).join(&hex[4..])
}

/// Derive the delta-blob path for a hash (`.diff` suffix) using the V1 layout.
pub(crate) fn hash_to_delta_path(root: &Path, hash: &Hash) -> PathBuf {
    let mut path = hash_to_path(root, hash);
    let ext = path
        .extension()
        .map(|e| {
            let mut s = e.to_os_string();
            s.push(".diff");
            s
        })
        .unwrap_or_else(|| "diff".into());
    path.set_extension(ext);
    path
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn hash_path_derivation_is_deterministic() {
        let root = Path::new("/tmp/cas");
        let hash = Hash::from_content(b"test data");
        let hex = hash.to_hex();

        let path = hash_to_path(root, &hash);
        assert_eq!(
            path,
            root.join("v1").join("blake3").join(&hex[0..2]).join(&hex[2..4]).join(&hex[4..])
        );
    }

    #[test]
    fn hash_delta_path_ends_with_diff() {
        let root = Path::new("/tmp/cas");
        let hash = Hash::from_content(b"test data");

        let path = hash_to_delta_path(root, &hash);
        assert!(path.to_string_lossy().ends_with(".diff"));
    }
}
