//! Version dispatch for blob store directory layout.
//!
//! Each on-disk version owns its path layout. This module keeps the ladder in
//! one registry and provides the version-aware path derivation, the on-disk
//! version resolver, and the migration entry point that bridges versioned
//! internals to the unversioned [`BlobStore`](super::BlobStore) runtime.
//! `vX.rs` files must never import unversioned structs outside `versions/`, and
//! this `mod.rs` is the only place where latest version state is bridged to
//! unversioned runtime state.
//!
//! ## Why the version is not read back at run time
//!
//! A blob store's on-disk discriminator is the `v<N>` path segment, not
//! anything in the file contents, and nothing in the tree ever reads a version
//! marker back. The layout is therefore a compile-time choice: the newest
//! registered rung is the one this build writes, and path derivation cannot
//! fail because no input can make it. The one place a version arrives from
//! outside the program — a marker found on disk — is
//! [`resolve_stored_layout`], and that is where the fallibility lives.
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

use crate::error::CasError;
use crate::hash::Hash;

/// Derives a full-blob path under one layout version.
type LayoutPathFn = fn(&Path, &Hash) -> PathBuf;

/// Derives a delta-blob path under one layout version.
type DeltaPathFn = fn(&Path, &Hash) -> PathBuf;

/// Migrates a store tree from one layout version to the current one.
type LayoutMigrationFn = fn(&Path) -> Result<LayoutMigration, CasError>;

/// What a layout migration did to a store tree.
///
/// The two cases are not a forecast: a migration that only maps a stored
/// version onto the current one leaves the tree where it is, while a migration
/// that moves files has to say so, because the caller has to know whether the
/// old tree still exists afterwards.
#[derive(Debug)]
#[expect(
    dead_code,
    reason = "`Rewritten` is unreachable while the ladder has one rung: no layout differs from \
              the current one, so no migration moves files. The variant is part of the seam a \
              future `v2.rs` rewrites"
)]
pub(crate) enum LayoutMigration {
    /// The tree was already in the current layout. The root is returned
    /// unchanged and nothing on disk moved.
    AlreadyCurrent {
        /// Root of the store, which holds the current layout.
        root: PathBuf,
    },
    /// The tree was rewritten from an older layout into the current one.
    Rewritten {
        /// Root of the store, now holding the current layout.
        root: PathBuf,
    },
}

/// One rung of the blob-store layout ladder.
struct LayoutRegistration {
    /// Numeric layout version, ascending down the registry.
    number: u32,
    /// The `v<N>` path segment that appears in this layout's tree.
    path_segment: &'static str,
    /// Derives a full-blob path under this layout.
    hash_to_path: LayoutPathFn,
    /// Derives a delta-blob path under this layout.
    hash_to_delta_path: DeltaPathFn,
    /// Migrates this layout's tree to the current layout.
    migrate_to_current: LayoutMigrationFn,
}

/// Every blob layout version this build implements, ascending; newest last.
///
/// This registry is the single source of truth for the ladder. The layout this
/// build writes, the on-disk segment it uses, and the migration each old layout
/// needs are all read from it, so a new rung is one entry here plus its
/// `vN.rs` — not an independent constant that can drift from the dispatch.
const REGISTERED_LAYOUTS: &[LayoutRegistration] = &[LayoutRegistration {
    number: 1,
    path_segment: v1::BLOB_PATH_VERSION,
    hash_to_path: v1::hash_to_path,
    hash_to_delta_path: v1::hash_to_delta_path,
    migrate_to_current: v1::migrate_v1_layout_to_current,
}];

/// The newest registered layout: the one this build reads and writes.
const NEWEST_LAYOUT: &LayoutRegistration = &REGISTERED_LAYOUTS[REGISTERED_LAYOUTS.len() - 1];

/// The on-disk path segment of the current blob layout.
///
/// This is the version name every caller that touches the tree layout needs,
/// and it is derived from the registry's newest rung rather than written out
/// again, so the segment a path is built from and the layout that builds it are
/// the same fact.
pub(crate) const CURRENT_BLOB_PATH_VERSION: &str = NEWEST_LAYOUT.path_segment;

/// Looks up a registered layout by its on-disk path segment.
///
/// The error names the marker that was found and lists every `(version, path
/// segment)` this build reads. A caller that derived a path under the wrong
/// layout would not find the blobs it was looking for, and the resulting
/// `NotFound` would say nothing about why — a wrong-format read is worse than a
/// wrong-format error.
///
/// Nothing reads a layout marker off disk today, which is why the marker is a
/// compile-time choice and [`hash_to_path`] needs no fallibility; the two
/// entry points below are the seam a future second rung plugs into.
fn registered_layout_by_segment(stored: &str) -> Result<&'static LayoutRegistration, CasError> {
    REGISTERED_LAYOUTS.iter().find(|layout| layout.path_segment == stored).ok_or_else(|| {
        let known: Vec<(u32, &'static str)> =
            REGISTERED_LAYOUTS.iter().map(|layout| (layout.number, layout.path_segment)).collect();
        CasError::InvalidArgument(format!(
            "unknown blob store layout version {stored:?}; this build reads (version, path \
             segment) {known:?}; a store written by another build needs a migration before this \
             build can read it"
        ))
    })
}

/// Map an on-disk layout version marker to the current layout's path segment.
///
/// A store written under any layout this build knows is read under the current
/// one: older layouts keep their files where they are, and this says which
/// layout answers for them. An unknown marker is an error rather than a
/// default, because defaulting to the current segment would derive a path under
/// a layout the store was never written with — a miss that looks exactly like a
/// missing blob.
///
/// # Errors
///
/// Returns [`CasError::InvalidArgument`] naming `stored` when this build
/// implements no such layout.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "infrastructure awaiting its caller: nothing reads a layout marker off disk \
                  today, so the on-disk entry point has no production caller. The registry's \
                  tests exercise it against V1, which is the whole proof available without a \
                  second rung"
    )
)]
pub(crate) fn resolve_stored_layout(stored: &str) -> Result<&'static str, CasError> {
    registered_layout_by_segment(stored).map(|_| CURRENT_BLOB_PATH_VERSION)
}

/// Migrate a store tree written under layout `stored` into the current layout.
///
/// This is the write-side counterpart of [`resolve_stored_layout`]: the
/// resolver answers "which layout reads this?", this answers "make this tree
/// readable by the current layout".
///
/// # Errors
///
/// Returns [`CasError::InvalidArgument`] naming `stored` when this build
/// implements no such layout, and whatever the per-layout migration reports.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "infrastructure awaiting its caller: a migration is triggered by opening a \
                  store whose tree is under a layout this build does not write, and no such \
                  layout exists yet. The registry's tests exercise it against V1"
    )
)]
pub(crate) fn migrate_stored_layout(
    root: &Path,
    stored: &str,
) -> Result<LayoutMigration, CasError> {
    (registered_layout_by_segment(stored)?.migrate_to_current)(root)
}

/// Derive the full-blob path for a hash under the current layout.
///
/// Infallible, deliberately. The layout is [`NEWEST_LAYOUT`], a compile-time
/// constant, so there is no input that could make the derivation fail: a
/// `Result` here would be a fallible signature over an infallible operation,
/// and it would push an error branch into the hot path that runs on every blob
/// get, put, and stat. The fallibility that *is* real — a layout marker read
/// from disk that this build does not know — lives in
/// [`resolve_stored_layout`], which is called once per store, not once per
/// hash.
pub(crate) fn hash_to_path(root: &Path, hash: &Hash) -> PathBuf {
    (NEWEST_LAYOUT.hash_to_path)(root, hash)
}

/// Derive the delta-blob path for a hash (`.diff` suffix) under the current
/// layout.
///
/// Infallible for the same reason as [`hash_to_path`]. The delta derivation is
/// dispatched through the registry rather than pinned to `v1`, because a layout
/// is free to place delta blobs somewhere else from its full blobs, and a
/// second rung that did so would otherwise be read under V1's rule.
pub(crate) fn hash_to_delta_path(root: &Path, hash: &Hash) -> PathBuf {
    (NEWEST_LAYOUT.hash_to_delta_path)(root, hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registry's newest entry must be the layout the production paths use.
    /// If the two ever diverge, every blob this build writes goes somewhere the
    /// tests here call "current" and nowhere the code does.
    #[test]
    fn current_layout_is_the_newest_registered_rung() {
        assert_eq!(
            NEWEST_LAYOUT.number,
            *REGISTERED_LAYOUTS.last().map(|layout| &layout.number).expect("a registered layout"),
            "the layout this build writes must be the newest rung in the registry"
        );
        assert_eq!(CURRENT_BLOB_PATH_VERSION, NEWEST_LAYOUT.path_segment);
        assert!(
            NEWEST_LAYOUT.path_segment.starts_with('v'),
            "a layout path segment must be versioned, or two layouts would share a directory"
        );
    }

    /// Every registered layout must derive a path carrying its own version
    /// segment, and must route its delta paths through its own derivation.
    /// This is the layout ladder's round-trip proof: a store written under
    /// version N is reachable by deriving its path from version N, and a
    /// version with no path segment would collide with every other version.
    #[test]
    fn every_supported_layout_derives_a_version_tagged_path() {
        let root = Path::new("/tmp/cas");
        let hash = Hash::from_content(b"layout ladder");

        for layout in REGISTERED_LAYOUTS {
            let path = (layout.hash_to_path)(root, &hash);
            let segment = format!("v{}", layout.number);
            assert!(
                path.components().any(|c| c.as_os_str() == std::ffi::OsStr::new(&segment)),
                "path {path:?} must carry its own {segment} layout segment, otherwise two \
                 layouts would share one directory"
            );
            let delta = (layout.hash_to_delta_path)(root, &hash);
            assert_eq!(
                delta.parent(),
                path.parent(),
                "layout {} derives its delta path outside the fan-out directory it derives its \
                 full path into",
                layout.number
            );
        }
    }

    /// The current layout's paths must still be exactly the V1 fan-out every
    /// store on disk was written with.
    ///
    /// The expected paths are spelled out as literals instead of being derived
    /// from `v1::hash_to_path`, so a refactor that quietly moved a segment — or
    /// that made the registry's newest rung derive something other than V1 —
    /// fails here instead of orphaning every existing blob. This is the test
    /// that would catch a "harmless" path-derivation change.
    #[test]
    fn current_layout_paths_are_the_v1_fanout() {
        let root = Path::new("/tmp/cas");
        let hash = Hash::from_content(b"layout regression");
        let hex = hash.to_hex();

        let expected_full =
            root.join("v1").join("blake3").join(&hex[0..2]).join(&hex[2..4]).join(&hex[4..]);
        assert_eq!(hash_to_path(root, &hash), expected_full);
        assert_eq!(
            hash_to_delta_path(root, &hash),
            root.join("v1")
                .join("blake3")
                .join(&hex[0..2])
                .join(&hex[2..4])
                .join(format!("{}.diff", &hex[4..]))
        );
    }

    /// The current layout's marker must resolve to the current layout: a store
    /// this build wrote is readable without any migration step.
    #[test]
    fn resolve_stored_layout_maps_the_current_version_to_itself() {
        assert_eq!(
            resolve_stored_layout(CURRENT_BLOB_PATH_VERSION).expect("the current layout resolves"),
            CURRENT_BLOB_PATH_VERSION
        );
        for layout in REGISTERED_LAYOUTS {
            assert_eq!(
                resolve_stored_layout(layout.path_segment)
                    .expect("a layout this build implements resolves"),
                CURRENT_BLOB_PATH_VERSION,
                "a store written under a layout this build knows must be read under the current \
                 one, not under the layout it was written with"
            );
        }
    }

    /// An unknown marker must be an error that names the marker, never a silent
    /// resolve to the current layout. This is the failure mode a second rung
    /// introduces: a store written by a newer build, opened by this one, whose
    /// blobs would otherwise be looked up under a fan-out that does not exist.
    #[test]
    fn resolve_stored_layout_rejects_and_names_an_unknown_version() {
        for stored in ["v0", "v2", "v99", "", "1", "vv1", "V1"] {
            let err = resolve_stored_layout(stored)
                .expect_err("an unknown layout must not resolve to a layout");
            let message = err.to_string();
            assert!(
                message.contains(&format!("{stored:?}")),
                "the error must name the marker it found so the caller can act on it: {message}"
            );
        }
    }

    /// The migration entry point must report a store written under the current
    /// layout as already current, and must reject an unknown marker through the
    /// same resolver the read path uses.
    #[test]
    fn migrate_stored_layout_reports_a_current_store_and_rejects_an_unknown_one() {
        let root = Path::new("/tmp/cas");
        let migrated = migrate_stored_layout(root, CURRENT_BLOB_PATH_VERSION)
            .expect("the current layout needs no migration");
        assert!(
            matches!(&migrated, LayoutMigration::AlreadyCurrent { root: migrated_root } if migrated_root == root),
            "a store already in the current layout must be reported as unchanged, got {migrated:?}"
        );

        let err = migrate_stored_layout(root, "v7")
            .expect_err("an unknown layout must not be migrated as if it were known");
        assert!(
            err.to_string().contains("\"v7\""),
            "the migration error must name the marker it was given: {err}"
        );
    }
}
