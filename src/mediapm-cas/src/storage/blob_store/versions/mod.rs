//! Version dispatch for blob store directory layout.
//!
//! Each on-disk version owns its path layout. This module provides
//! version-aware path derivation and bridges versioned internals to the
//! unversioned [`BlobStore`](super::super::BlobStore) runtime. `vX.rs` files
//! must never import unversioned structs outside `versions/`, and this
//! `mod.rs` is the only place where latest version state is bridged to
//! unversioned runtime state.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version migration.
//! - This `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside `versions/` must interact with versioned envelopes only
//!   through this `mod.rs`, never through direct `versions::vX` imports.
//! - Do not directly re-export `versions::vX` structs/types from this module.
//!   Expose unversioned APIs here and keep versioned internals encapsulated.

mod v1;

use std::path::{Path, PathBuf};

use crate::hash::Hash;

/// Current blob-store path layout version.
///
/// This ladder has exactly one rung, and that is not an accident of
/// incomplete work: a blob store's on-disk discriminator is the `v<N>` path
/// segment (see [`v1::BLOB_PATH_VERSION`]), not anything in the file
/// contents, and nothing in the tree ever reads a version marker back. The
/// layout is therefore a compile-time choice, not a runtime dispatch on
/// observed data.
///
/// A second rung means a `v2.rs` owning a different fan-out scheme plus a new
/// arm in [`layout_for_version`] — not editing this constant alone. The
/// `current_layout_version_has_a_registered_arm` test in this module fails if
/// the constant is bumped without one, so the two cannot drift apart.
pub(crate) const BLOB_LAYOUT_VERSION: u32 = 1;

/// Every blob layout version this build implements, ascending.
///
/// Read by the registry's own tests, which is what keeps this constant and
/// the [`layout_for_version`] match from drifting apart; production dispatch
/// resolves the single current version directly.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the dispatch resolves BLOB_LAYOUT_VERSION directly; this list exists so the registry's tests can prove every implemented layout is registered"
    )
)]
pub(crate) const SUPPORTED_BLOB_LAYOUT_VERSIONS: &[u32] = &[1];

/// Resolves one layout version to its full-blob path derivation.
fn layout_for_version(version: u32) -> fn(&Path, &Hash) -> PathBuf {
    match version {
        1 => v1::hash_to_path,
        // Unreachable while `SUPPORTED_BLOB_LAYOUT_VERSIONS` and this match
        // agree, which `current_layout_version_has_a_registered_arm` proves.
        // A version outside that set means the caller asked for a layout this
        // build does not implement; deriving a path under the wrong layout
        // would silently write blobs a future reader cannot find, so halting
        // is the correct response to a violated invariant.
        other => panic!("unsupported blob layout version {other}"),
    }
}

/// Derive the full-blob path for a hash under the current layout.
///
/// Dispatches on [`BLOB_LAYOUT_VERSION`], so a future layout is a new arm in
/// [`layout_for_version`] plus a new `vN.rs`, and this function keeps its
/// signature.
pub(crate) fn hash_to_path(root: &Path, hash: &Hash) -> PathBuf {
    layout_for_version(BLOB_LAYOUT_VERSION)(root, hash)
}

/// Derive the delta-blob path for a hash (`.diff` suffix) under the current
/// layout.
pub(crate) fn hash_to_delta_path(root: &Path, hash: &Hash) -> PathBuf {
    v1::hash_to_delta_path(root, hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The current layout version must be one this build actually implements.
    /// Bumping the constant without adding a layout arm would make every blob
    /// path halt at run time; this fails at test time instead.
    #[test]
    fn current_layout_version_has_a_registered_arm() {
        assert!(
            SUPPORTED_BLOB_LAYOUT_VERSIONS.contains(&BLOB_LAYOUT_VERSION),
            "BLOB_LAYOUT_VERSION {BLOB_LAYOUT_VERSION} has no entry in \
             SUPPORTED_BLOB_LAYOUT_VERSIONS {SUPPORTED_BLOB_LAYOUT_VERSIONS:?}"
        );
        // Resolving must not hit the halting arm.
        let root = Path::new("/tmp/cas");
        let hash = Hash::from_content(b"registered");
        assert_eq!(layout_for_version(BLOB_LAYOUT_VERSION)(root, &hash), hash_to_path(root, &hash));
    }

    /// Every implemented layout must derive a path carrying its own version
    /// segment. This is the layout ladder's round-trip proof: a store written
    /// under version N is reachable by deriving its path from version N, and a
    /// version with no path segment would collide with every other version.
    #[test]
    fn every_supported_layout_derives_a_version_tagged_path() {
        let root = Path::new("/tmp/cas");
        let hash = Hash::from_content(b"layout ladder");

        for &version in SUPPORTED_BLOB_LAYOUT_VERSIONS {
            let path = layout_for_version(version)(root, &hash);
            let segment = format!("v{version}");
            assert!(
                path.components().any(|c| c.as_os_str() == std::ffi::OsStr::new(&segment)),
                "path {path:?} must carry its own {segment} layout segment, otherwise two \
                 layouts would share one directory"
            );
        }
    }
}
