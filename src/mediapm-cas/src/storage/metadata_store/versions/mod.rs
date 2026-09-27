//! Versioned persistence for metadata constraint data and entries.
//!
//! Currently only V1 is supported.
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

use std::collections::{BTreeMap, BTreeSet};

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

/// Return type for snapshot load functions: (constraints, entries).
pub(crate) type SnapshotData = (
    BTreeMap<Hash, BTreeSet<Hash>>,        // constraints: target → bases
    BTreeMap<Hash, (u64, ObjectEncoding)>, // entries: hash → (len, encoding)
);

/// Current metadata snapshot format version.
///
/// This ladder has exactly one rung, and the rungs are *not* interchangeable
/// in the way a wire format's are: the V1 snapshot carries no version field
/// inside the document. Its version lives entirely in the filename
/// (`metadata-v1.json`), which is what makes an old snapshot readable without
/// any in-band marker. A second format is therefore a second filename, listed
/// in the caller's `METADATA_FORMAT_NAMES`, plus a new `vN.rs` and a new arm
/// in [`snapshot_loader_for_version`].
pub(crate) const SNAPSHOT_VERSION: u32 = 1;

/// Every snapshot format version this build can read, ascending.
///
/// Reading a version outside this set would mean applying a parser to bytes it
/// was never written for, so the loader rejects it rather than guessing.
pub(crate) const SUPPORTED_SNAPSHOT_VERSIONS: &[u32] = &[1];

/// Extracts the format version from a snapshot filename.
///
/// The filename is the version marker: `metadata-v<N>.json` yields `N`. A name
/// that does not match the pattern, or whose `N` this build cannot read, is an
/// error — a snapshot whose version cannot be identified must not be parsed
/// under some other version's rules.
fn snapshot_version_from_name(name: &str) -> Result<u32, CasError> {
    let version = name
        .strip_prefix("metadata-v")
        .and_then(|rest| rest.strip_suffix(".json"))
        .and_then(|digits| digits.parse::<u32>().ok())
        .ok_or_else(|| {
            CasError::InvalidArgument(format!(
                "metadata snapshot filename {name:?} does not name a format version \
                 (expected metadata-v<N>.json)"
            ))
        })?;

    if !SUPPORTED_SNAPSHOT_VERSIONS.contains(&version) {
        return Err(CasError::InvalidArgument(format!(
            "unsupported metadata snapshot version {version} in {name:?} (supported: \
             {SUPPORTED_SNAPSHOT_VERSIONS:?})"
        )));
    }
    Ok(version)
}

/// A snapshot parser for one format version.
///
/// Takes the raw snapshot bytes and yields `None` when the input is empty,
/// which every version treats as an absent snapshot rather than an error.
type SnapshotLoader = fn(&[u8]) -> Result<Option<SnapshotData>, CasError>;

/// Resolves one snapshot version to its parser.
fn snapshot_loader_for_version(version: u32) -> Result<SnapshotLoader, CasError> {
    match version {
        1 => Ok(v1::parse_v1_snapshot),
        other => Err(CasError::InvalidArgument(format!(
            "unsupported metadata snapshot version {other} (supported: \
             {SUPPORTED_SNAPSHOT_VERSIONS:?})"
        ))),
    }
}

/// Parse snapshot data from the raw bytes of a named snapshot file.
///
/// `name` is the snapshot's filename and is the only version marker the format
/// carries, so it selects the parser. Dispatching on it (rather than assuming
/// the newest) is what keeps a legacy snapshot file readable after the format
/// is bumped.
///
/// # Errors
///
/// Returns [`CasError::InvalidArgument`] when `name` does not encode a
/// readable format version, and the parser's own error when the bytes do not
/// match that format.
pub(crate) fn load_named_from_bytes(name: &str, data: &[u8]) -> Result<SnapshotData, CasError> {
    let loader = snapshot_loader_for_version(snapshot_version_from_name(name)?)?;
    loader(data).map(Option::unwrap_or_default)
}

/// Serializes a snapshot to `Vec<u8>` in the current format.
///
/// Dispatches on [`SNAPSHOT_VERSION`] for symmetry with the read path, so
/// writing and reading agree on which format is current.
pub(crate) fn save_to_vec(
    constraints: &BTreeMap<Hash, BTreeSet<Hash>>,
    entries: &BTreeMap<Hash, (u64, ObjectEncoding)>,
) -> Result<Vec<u8>, CasError> {
    match SNAPSHOT_VERSION {
        1 => v1::serialize_v1_snapshot(constraints, entries),
        // Unreachable while `SUPPORTED_SNAPSHOT_VERSIONS` and
        // `snapshot_loader_for_version` agree with this match, which
        // `every_supported_snapshot_version_has_a_parser` proves. Serializing
        // under an unimplemented format would write bytes no reader of this
        // build could parse, so halting is the correct response.
        other => panic!("unsupported metadata snapshot version {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The current version must be one this build implements, and every
    /// supported version must have a parser arm. Bumping the constant without
    /// adding a parser would make every save halt or mis-parse.
    #[test]
    fn every_supported_snapshot_version_has_a_parser() {
        assert!(SUPPORTED_SNAPSHOT_VERSIONS.contains(&SNAPSHOT_VERSION));
        for &version in SUPPORTED_SNAPSHOT_VERSIONS {
            assert!(
                snapshot_loader_for_version(version).is_ok(),
                "snapshot version {version} has no parser arm"
            );
        }
    }

    /// A representative current-format snapshot must round-trip: serialize
    /// through the current writer, then read back through dispatch, and
    /// recover the same constraints and entries.
    #[test]
    fn current_snapshot_round_trips_through_dispatch() {
        let target = Hash::from_content(b"target");
        let base = Hash::from_content(b"base");
        let entry = Hash::from_content(b"entry");
        let constraints = BTreeMap::from([(target, BTreeSet::from([base]))]);
        let entries = BTreeMap::from([(entry, (11u64, ObjectEncoding::Full))]);

        let bytes = save_to_vec(&constraints, &entries).expect("serialize current snapshot");
        let name = "metadata-v1.json";

        let version =
            snapshot_version_from_name(name).expect("the current filename must name a version");
        assert_eq!(version, SNAPSHOT_VERSION);

        let (read_constraints, read_entries) =
            load_named_from_bytes(name, &bytes).expect("dispatch must read the snapshot back");
        assert_eq!(read_constraints, constraints);
        assert_eq!(read_entries, entries);

        // The writer and the dispatching reader must agree on the current
        // version, or every save would produce a file no read path selects.
        assert_eq!(version, SNAPSHOT_VERSION);
    }

    /// A filename that does not name a readable format must be rejected, not
    /// parsed under the current version's rules.
    #[test]
    fn unidentifiable_snapshot_filename_is_rejected() {
        for bad in ["metadata.json", "metadata-vX.json", "other-v1.json", "metadata-v1.bin"] {
            let err = snapshot_version_from_name(bad)
                .expect_err("an unidentifiable filename must not resolve to a version");
            assert!(
                err.to_string().contains("metadata snapshot"),
                "error for {bad:?} must say what was wrong: {err}"
            );
        }
        assert!(snapshot_version_from_name("metadata-v2.json").is_err());
        assert!(snapshot_version_from_name("metadata-v0.json").is_err());
    }

    /// Empty input is an absent snapshot, not a parse failure, in every
    /// readable version.
    #[test]
    fn empty_snapshot_bytes_are_an_absent_snapshot_in_every_version() {
        for &version in SUPPORTED_SNAPSHOT_VERSIONS {
            let loader = snapshot_loader_for_version(version).expect("supported version");
            assert!(
                loader(b"").expect("empty input must not error").is_none(),
                "version {version} must treat empty input as an absent snapshot, not an empty one"
            );
        }
    }
}
