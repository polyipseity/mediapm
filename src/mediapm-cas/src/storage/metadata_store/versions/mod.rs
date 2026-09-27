//! Version dispatch for the metadata store's per-directory snapshots.
//!
//! Currently only V1 is supported.
//!
//! A V1 snapshot carries no version field inside the document: its version
//! lives entirely in its filename (`metadata-v1.json`). This module therefore
//! owns *both* halves of the ladder — the filenames and the codecs — because
//! splitting them is how a `metadata-v2.json` written by one module gets
//! handed to a V1 parser by another. [`fs`](super::fs) consumes the names; it
//! holds no version knowledge of its own.
//!
//! ## What adding a format costs
//!
//! One new `vN.rs` (codec plus its `migrate_vN_to_current`), one new arm in
//! [`snapshot_loader_for_version`] and one in [`migrate_snapshot_to_current`],
//! and one new entry in [`METADATA_FORMAT_NAMES`]. Nothing in the caller
//! changes, and an unreadable version is an error rather than a wrong-format
//! parse.
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

/// The per-directory snapshot filename this build writes.
///
/// Owned here rather than by the file that lays the snapshots out, because the
/// filename *is* the version marker: a constant owned by the layout module and
/// a codec owned by this module could disagree, and nothing would notice until
/// a reader dispatched on the wrong parser.
pub(crate) const LATEST_METADATA_FORMAT: &str = "metadata-v1.json";

/// Every per-directory metadata filename this build can read, newest first.
///
/// [`rebuild_from_wal`](super::FileSystemMetadataStore) scans all of them, so
/// a file written under a previous format is still recovered after the format
/// is bumped. The first element is [`LATEST_METADATA_FORMAT`], which
/// `latest_metadata_format_names_the_current_snapshot_version` pins to
/// [`SNAPSHOT_VERSION`].
pub(crate) const METADATA_FORMAT_NAMES: &[&str] = &[LATEST_METADATA_FORMAT];

/// Current metadata snapshot format version.
///
/// This ladder has exactly one rung, and the rungs are *not* interchangeable
/// in the way a wire format's are: the V1 snapshot carries no version field
/// inside the document. Its version lives entirely in the filename
/// (`metadata-v1.json`), which is what makes an old snapshot readable without
/// any in-band marker. A second format is therefore a second filename, listed
/// in [`METADATA_FORMAT_NAMES`], plus a new `vN.rs` and new dispatch arms.
pub(crate) const SNAPSHOT_VERSION: u32 = 1;

/// Every snapshot format version this build can read, ascending.
///
/// Reading a version outside this set would mean applying a parser to bytes it
/// was never written for, so the loader rejects it rather than guessing.
pub(crate) const SUPPORTED_SNAPSHOT_VERSIONS: &[u32] = &[1];

/// The single error every unknown-version path funnels through.
///
/// Naming the version that was found is the point: a caller holding
/// `metadata-v2.json` must be able to tell "this build does not know that
/// format" from "that file is corrupt", and a message listing only the
/// expected set gives it nothing to act on.
fn unsupported_version_error(version: u32, context: &str) -> CasError {
    CasError::InvalidArgument(format!(
        "unsupported metadata snapshot version {version}{context} (supported: \
         {SUPPORTED_SNAPSHOT_VERSIONS:?})"
    ))
}

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

    if SUPPORTED_SNAPSHOT_VERSIONS.contains(&version) {
        return Ok(version);
    }
    Err(unsupported_version_error(version, &format!(" in snapshot file {name:?}")))
}

/// A snapshot parser for one format version.
///
/// Takes the raw snapshot bytes and yields `None` when the input is empty,
/// which every version treats as an absent snapshot rather than an error.
type SnapshotLoader = fn(&[u8]) -> Result<Option<SnapshotData>, CasError>;

/// Resolves one snapshot version to its parser.
///
/// The only place a version selects a codec. An unregistered version is an
/// error: falling back to the newest (or oldest) parser would read the bytes
/// under rules they were not written for and report a parse failure for a file
/// that is perfectly valid in a format this build simply lacks.
fn snapshot_loader_for_version(version: u32) -> Result<SnapshotLoader, CasError> {
    match version {
        1 => Ok(v1::parse_v1_snapshot),
        other => Err(unsupported_version_error(other, "")),
    }
}

/// Carries a snapshot read under `version` into the current snapshot model.
///
/// The read half of the ladder is two steps — decode in the version's own
/// format, then migrate forward — and this function is the seam between them.
/// Today it is the identity for every version, because V1 is the newest; a
/// future `v2.rs` owns the rewrite from V1, and this match is the only place
/// that has to learn about it.
fn migrate_snapshot_to_current(
    version: u32,
    snapshot: SnapshotData,
) -> Result<SnapshotData, CasError> {
    match version {
        1 => v1::migrate_v1_to_current(snapshot),
        other => Err(unsupported_version_error(other, "")),
    }
}

/// Decode and migrate a snapshot of one known version.
fn load_snapshot_for_version(version: u32, data: &[u8]) -> Result<SnapshotData, CasError> {
    let decoded = snapshot_loader_for_version(version)?(data)?.unwrap_or_default();
    migrate_snapshot_to_current(version, decoded)
}

/// Parse snapshot data from the raw bytes of a named snapshot file.
///
/// `name` is the snapshot's filename and is the only version marker the format
/// carries, so it selects the parser. Dispatching on it (rather than assuming
/// the newest) is what keeps a legacy snapshot file readable after the format
/// is bumped — and what makes an unrecognised name an error instead of a
/// best-effort parse under whatever this build happens to implement.
///
/// # Errors
///
/// Returns [`CasError::InvalidArgument`] when `name` does not encode a
/// readable format version, and the parser's own error when the bytes do not
/// match the format that version defines.
pub(crate) fn load_named_from_bytes(name: &str, data: &[u8]) -> Result<SnapshotData, CasError> {
    load_snapshot_for_version(snapshot_version_from_name(name)?, data)
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

    use std::path::{Path, PathBuf};

    /// The non-removable marker every `versions/` file must carry.
    const VERSIONS_POLICY_GUARD_MARKER: &str = "//! ## DO NOT REMOVE: versions policy guard";

    /// The storage version ladders this workstream owns, relative to the
    /// crate's own `src` directory.
    const STORAGE_LADDERS: &[&str] =
        &["storage/metadata_store/versions", "storage/blob_store/versions"];

    /// Collects `.rs` files under `dir`, recursively.
    fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read source dir") {
            let path = entry.expect("read source dir entry").path();
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    /// Returns `true` when `path` sits anywhere below a directory named
    /// `versions`.
    fn is_inside_a_versions_dir(path: &Path) -> bool {
        path.components()
            .any(|component| matches!(component, std::path::Component::Normal(name) if name == "versions"))
    }

    /// Returns `true` when `name` is a version module name: `v` plus digits
    /// (`v1`), or `v_` plus a lowercase word (`v_latest`).
    ///
    /// The whole name has to match, so the unversioned siblings a `versions/`
    /// directory legitimately contains (`validate_v1_document`, `v1beta`) are
    /// not mistaken for version modules.
    fn is_version_module_name(name: &str) -> bool {
        let Some(tail) = name.strip_prefix('v') else {
            return false;
        };
        if !tail.is_empty() && tail.bytes().all(|byte| byte.is_ascii_digit()) {
            return true;
        }
        match tail.strip_prefix('_') {
            Some(word) => !word.is_empty() && word.bytes().all(|byte| byte.is_ascii_lowercase()),
            None => false,
        }
    }

    /// Returns the version module a line names through a `versions::` path, if
    /// any.
    ///
    /// Both spellings count. `use super::versions::v1::Thing;` is the direct
    /// form. `use super::versions::{v1, save_to_vec};` is the one a plain
    /// "does the line contain `versions::v`" check misses — and it is the
    /// spelling an author reaches for first, because the unversioned entry
    /// points have to be imported too. A detector that only caught the direct
    /// form would let the more natural violation through.
    fn version_module_named_by(line: &str) -> Option<String> {
        const PREFIX: &str = "versions::";
        let mut rest = line;
        while let Some(at) = rest.find(PREFIX) {
            let starts_a_path = at == 0
                || !rest[..at]
                    .chars()
                    .next_back()
                    .is_some_and(|ch| ch.is_alphanumeric() || ch == '_');
            let after = &rest[at + PREFIX.len()..];
            rest = after;
            if !starts_a_path {
                continue;
            }
            if let Some(list) = after.strip_prefix('{') {
                let close = list.find('}')?;
                if let Some(found) = list[..close]
                    .split(',')
                    .map(str::trim)
                    .find(|item| is_version_module_name(item))
                {
                    return Some(found.to_string());
                }
            } else {
                let name: String =
                    after.chars().take_while(|ch| ch.is_alphanumeric() || *ch == '_').collect();
                if is_version_module_name(&name) {
                    return Some(name);
                }
            }
        }
        None
    }

    /// Both storage ladders must keep the non-removable versions policy guard,
    /// and no file in this crate may name a version module from outside a
    /// `versions/` directory.
    ///
    /// Two limits are worth stating rather than hiding. First, the ladder list
    /// is explicit: it names the two ladders this workstream owns, and the
    /// workspace-wide scan in `delta/versions/mod_policy_guard.rs` is what
    /// covers every other `versions/` directory, `storage/wal/versions/`
    /// included. The assertion that each listed directory still exists is what
    /// turns a moved or deleted ladder into a failure here instead of a
    /// silently smaller scan. Second, the cross-boundary half skips lines whose
    /// first non-space characters are `//`, which covers the `//!` and `///`
    /// prose this crate uses to restate the rule; a violation hidden in a
    /// block comment or a string literal is the workspace scan's job, because
    /// it projects those away properly.
    #[test]
    fn storage_version_ladders_keep_the_versions_policy_guard() {
        let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");

        let mut guarded = Vec::new();
        for ladder in STORAGE_LADDERS {
            let dir = src_dir.join(ladder);
            assert!(dir.is_dir(), "version ladder '{}' must exist", dir.display());
            for entry in std::fs::read_dir(&dir).expect("read versions dir") {
                let path = entry.expect("read versions dir entry").path();
                if path.extension().is_none_or(|ext| ext != "rs") {
                    continue;
                }
                let content = std::fs::read_to_string(&path).expect("read versions file");
                // The production portion only: this very assertion text lives
                // below the split, so scanning the whole file would let the
                // guard satisfy itself.
                let production = content
                    .split_once("#[cfg(test)]")
                    .map_or(content.as_str(), |(production, _tests)| production);
                assert!(
                    production.contains(VERSIONS_POLICY_GUARD_MARKER),
                    "{} must carry the non-removable versions policy guard docstring",
                    path.display()
                );
                guarded.push(path);
            }
        }
        assert!(!guarded.is_empty(), "the two storage ladders produced no files to guard");

        let mut files = Vec::new();
        collect_rs_files(&src_dir, &mut files);
        let mut violations = Vec::new();
        for path in files {
            if is_inside_a_versions_dir(&path) {
                continue;
            }
            let content = std::fs::read_to_string(&path).expect("read source file");
            for (index, line) in content.lines().enumerate() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                if let Some(reference) = version_module_named_by(line) {
                    violations.push(format!("{}:{}: names {reference}", path.display(), index + 1));
                }
            }
        }
        assert!(
            violations.is_empty(),
            "these lines name a version module from outside a `versions/` directory, which the \
             versions boundary policy forbids; route through the unversioned entry points in \
             each `versions/mod.rs` instead:\n{}",
            violations.join("\n")
        );
    }

    /// The current version must be one this build implements, and every
    /// supported version must have both a parser arm and a migration arm.
    /// Bumping a constant without adding them would make a read halt or
    /// mis-parse.
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

    /// Every readable version must also have a migration arm, so a snapshot
    /// cannot decode successfully and then fail to be carried forward. An
    /// absent snapshot exercises the same arm as a present one.
    #[test]
    fn every_supported_snapshot_version_has_a_migration_arm() {
        for &version in SUPPORTED_SNAPSHOT_VERSIONS {
            let empty: SnapshotData = (BTreeMap::new(), BTreeMap::new());
            assert!(
                migrate_snapshot_to_current(version, empty.clone()).is_ok(),
                "snapshot version {version} has no migration arm"
            );
        }
    }

    /// The filename this build writes and the version the codec dispatches on
    /// are the same fact stated twice; nothing but a test keeps them together.
    #[test]
    fn latest_metadata_format_names_the_current_snapshot_version() {
        assert_eq!(
            snapshot_version_from_name(LATEST_METADATA_FORMAT)
                .expect("LATEST_METADATA_FORMAT must name a version this build can read"),
            SNAPSHOT_VERSION
        );
        assert_eq!(
            METADATA_FORMAT_NAMES.first(),
            Some(&LATEST_METADATA_FORMAT),
            "the newest readable name must be the one this build writes, or \
             rebuild_from_wal would never find the files it just wrote"
        );
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

        let (read_constraints, read_entries) =
            load_named_from_bytes(LATEST_METADATA_FORMAT, &bytes)
                .expect("dispatch must read the snapshot back");
        assert_eq!(read_constraints, constraints);
        assert_eq!(read_entries, entries);
    }

    /// A filename naming a version this build does not implement must be
    /// rejected, and the error must name the version that was found.
    ///
    /// The bytes handed in are a *valid* V1 snapshot, so the test proves the
    /// version check runs before any parser does: an implementation that fell
    /// back to the newest parser would parse these bytes successfully and
    /// return `Ok`, which is exactly the silent mis-parse this guards against.
    #[test]
    fn unknown_snapshot_version_is_rejected_and_named() {
        let target = Hash::from_content(b"target");
        let constraints = BTreeMap::from([(target, BTreeSet::from([Hash::from_content(b"base")]))]);
        let valid_v1_bytes =
            save_to_vec(&constraints, &BTreeMap::new()).expect("serialize a valid V1 snapshot");

        for version in [0u32, 2, 7, 999] {
            let name = format!("metadata-v{version}.json");
            let err = load_named_from_bytes(&name, &valid_v1_bytes)
                .expect_err("an unknown version must not be parsed under a known version's rules");
            let message = err.to_string();
            assert!(
                message.contains(&version.to_string()),
                "the error must name the version it found, so the caller can act on it: \
                 {message}"
            );
            assert!(
                !matches!(err, CasError::CorruptObject { .. }),
                "an unknown version is not a corrupt file; falling through to a parser would \
                 mislabel it"
            );
        }
    }

    /// The V1 → current migration must be lossless: decoding a V1 snapshot,
    /// migrating it, and re-serializing must reproduce the same bytes the V1
    /// writer produced. A migration that dropped a field, reordered a map, or
    /// rewrote an encoding string would break this.
    #[test]
    fn migrating_a_v1_snapshot_reproduces_its_bytes() {
        let target = Hash::from_content(b"target");
        let delta = Hash::from_content(b"delta-base");
        let constraints = BTreeMap::from([(target, BTreeSet::from([Hash::from_content(b"base")]))]);
        let entries = BTreeMap::from([
            (Hash::from_content(b"full"), (7u64, ObjectEncoding::Full)),
            (delta, (9u64, ObjectEncoding::Delta { base_hash: delta })),
        ]);

        let original = save_to_vec(&constraints, &entries).expect("serialize a V1 snapshot");
        let decoded = load_named_from_bytes(LATEST_METADATA_FORMAT, &original)
            .expect("decode the V1 snapshot");
        let migrated =
            v1::migrate_v1_to_current(decoded).expect("migrate V1 to the current version");
        assert_eq!(migrated.0, constraints, "constraints must survive the migration");
        assert_eq!(migrated.1, entries, "entries must survive the migration");

        let rewritten =
            save_to_vec(&migrated.0, &migrated.1).expect("re-serialize the migrated data");
        assert_eq!(
            rewritten, original,
            "a V1 snapshot migrated into the current model must serialize back to the same \
             bytes, or the migration is not the identity it claims to be"
        );
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
