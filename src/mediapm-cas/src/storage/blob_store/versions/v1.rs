//! V1 directory layout for blob storage.
//!
//! V1 uses a hash-derived fan-out tree rooted at `<root>/v1/blake3/ab/cd/<hex>`.
//! Full blobs are stored at the leaf path; delta blobs use a `.diff` suffix.
//!
//! This module owns the V1 codec (the two path derivations) and the V1 → current
//! migration hook (`migrate_v1_layout_to_current`) that a future `v2.rs` would
//! replace.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version isomorphism.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::error::CasError;
use crate::hash::Hash;

use super::LayoutMigration;

/// The path version segment used in blob storage directory layout.
pub(crate) const BLOB_PATH_VERSION: &str = "v1";

/// Derive the full-blob path for a hash using the V1 layout.
pub(crate) fn hash_to_path(root: &Path, hash: &Hash) -> PathBuf {
    let hex = hash.to_hex();
    root.join(BLOB_PATH_VERSION).join("blake3").join(&hex[0..2]).join(&hex[2..4]).join(&hex[4..])
}

/// Derive the delta-blob path for a hash (`.diff` suffix) using the V1 layout.
pub(crate) fn hash_to_delta_path(root: &Path, hash: &Hash) -> PathBuf {
    let mut path = hash_to_path(root, hash);
    let ext = path.extension().map_or(OsString::from("diff"), |e| {
        let mut s = e.to_os_string();
        s.push(".diff");
        s
    });
    path.set_extension(ext);
    path
}

/// Migrate a store tree written under the V1 layout to the current layout.
///
/// V1 is the newest layout this build implements, so the migration is the
/// identity: the tree is already where the current layout reads it. The hook
/// exists so the ladder has exactly one place where "carry an old tree
/// forward" is written, and it reports which of the two cases it is in
/// ([`LayoutMigration::AlreadyCurrent`]) rather than leaving the caller to
/// assume.
///
/// ## What a real V1 → V2 layout migration would have to rewrite
///
/// A layout is a naming scheme, not a format: the bytes do not change, so
/// there is nothing to re-encode. What moves is the tree, and the migration has
/// to cover every file the layout put somewhere, not just the blobs a read
/// happens to touch:
///
/// - **Every full blob**: each `<root>/v1/blake3/<ab>/<cd>/<rest>` moves to
///   the new fan-out. Enumerating by hash is not an option — the migration does
///   not know which hashes exist — so it walks the tree.
/// - **Every delta blob**: `.diff` suffixed leaves, whose new location a new
///   layout may place independently of the full blobs.
/// - **Auxiliary files**: `write_aux` puts per-hash sidecars (the metadata
///   snapshots) in the fan-out *parent* directory, so they move with their
///   fan-out and a migration that only moves leaves strands them.
/// - **The version directory itself**: the old `v1/` tree has to remain, or be
///   removed atomically, or a crash mid-migration leaves two layouts the reader
///   cannot choose between. A resumable migration must be able to tell a
///   half-moved tree from a whole one, which is why the current-layout segment
///   is the registry's single source of truth and not a literal at the call
///   site.
/// - **Timing**: no concurrent access. The blob store holds an exclusive lock
///   on its root, so a migration runs with the store closed; running it against
///   a live store would race the readers that are deriving paths from the
///   layout this migration is replacing.
///
/// # Errors
///
/// Cannot fail while V1 is the current layout; the signature is fallible
/// because the tree rewrite a V2 introduces is not.
#[expect(
    clippy::unnecessary_wraps,
    reason = "the migration seam must be fallible before it is fallible in fact: rewriting a tree \
              in place can fail on I/O, and a signature change at the moment the ladder grows a \
              rung would reach every caller of the migration entry point"
)]
pub(crate) fn migrate_v1_layout_to_current(root: &Path) -> Result<LayoutMigration, CasError> {
    Ok(LayoutMigration::AlreadyCurrent { root: root.to_path_buf() })
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
