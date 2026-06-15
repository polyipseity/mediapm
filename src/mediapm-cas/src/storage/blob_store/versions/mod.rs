//! Version dispatch for blob store directory layout.
//!
//! Each on-disk version owns its path layout. This module provides
//! version-aware path derivation and bridges versioned internals to the
//! unversioned [`BlobStore`](super::super::BlobStore) runtime.
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

pub(crate) mod v1;

use std::path::{Path, PathBuf};

use crate::hash::Hash;

// ---------------------------------------------------------------------------
// Path derivation (version-aware dispatch)
// ---------------------------------------------------------------------------

/// Derive the full-blob path for a hash.
///
/// Currently dispatches to the V1 layout; future versions may add a version
/// match here.
pub(crate) fn hash_to_path(root: &Path, hash: &Hash) -> PathBuf {
    v1::hash_to_path(root, hash)
}

/// Derive the delta-blob path for a hash (`.diff` suffix).
///
/// Currently dispatches to the V1 layout; future versions may add a version
/// match here.
pub(crate) fn hash_to_delta_path(root: &Path, hash: &Hash) -> PathBuf {
    v1::hash_to_delta_path(root, hash)
}
