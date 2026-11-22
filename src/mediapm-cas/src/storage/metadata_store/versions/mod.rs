//! Version dispatch for the metadata store's per-directory snapshots.
//!
//! V1, V2, and V3 are supported. No snapshot carries a version field inside
//! the document: the version lives entirely in the filename
//! (`metadata-v3.json`). This module therefore owns *both* halves of the
//! ladder — the filenames and the codecs — because splitting them is how a
//! `metadata-v3.json` written by one module gets handed to a V2 parser by
//! another. [`fs`](super::fs) consumes the names; it holds no version
//! knowledge of its own.
//!
//! ## What adding a format costs
//!
//! One new `vN.rs` exporting `parse_vN_snapshot`, `serialize_vN_snapshot` and
//! a `load_vN_snapshot` that parses in its own format and migrates into the
//! current model; one new arm in [`snapshot_loader_for_version`]; one new arm
//! in [`save_to_vec`]; and one new entry in [`METADATA_FORMAT_NAMES`].
//!
//! The read half is one seam per version rather than a parse seam plus a
//! migration seam. Each version's `load_vN_snapshot` returns the *current*
//! model, so no caller can parse a file under one version's rules and then
//! forget to carry it forward — a second seam that only ever calls the first
//! is indirection, and it is exactly the place where a `metadata-v2.json`
//! reaches a V1 parser.
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
mod v2;
mod v3;

use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

/// One fan-out directory's persisted facts.
///
/// The three fields are the whole snapshot. `lengths` is a payload length per
/// hash and nothing else: which physical form holds the bytes is read from the
/// tree at load. `delta_bases` names the bases a delta may be compressed
/// against. `corrupted` names the hashes whose every physical form is unsound,
/// so a reader that finds one does not have to rediscover it by probing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Snapshot {
    /// Constraint target to its candidate bases.
    pub(crate) delta_bases: BTreeMap<Hash, BTreeSet<Hash>>,
    /// Payload length per hash.
    pub(crate) lengths: BTreeMap<Hash, u64>,
    /// Hashes whose every physical form fails verification.
    pub(crate) corrupted: BTreeSet<Hash>,
}

/// The per-directory snapshot filename this build writes.
///
/// Owned here rather than by the file that lays the snapshots out, because the
/// filename *is* the version marker: a constant owned by the layout module and
/// a codec owned by this module could disagree, and nothing would notice until
/// a reader dispatched on the wrong parser.
pub(crate) const LATEST_METADATA_FORMAT: &str = "metadata-v3.json";

/// Every per-directory metadata filename this build can read, newest first.
///
/// [`rebuild_from_wal`](super::FileSystemMetadataStore) scans all of them, so
/// a file written under a previous format is still recovered after the format
/// is bumped. The first element is [`LATEST_METADATA_FORMAT`], which
/// `latest_metadata_format_names_the_current_snapshot_version` pins to
/// [`SNAPSHOT_VERSION`].
///
/// The order is the contract, not an incidental detail: a reader that overlays
/// every name has to apply them oldest-first so a stale legacy file cannot win
/// over a newer current one in the same directory while both formats coexist.
/// Listing the newest first is what makes that iteration a `.rev()` rather
/// than a magic constant in the caller.
pub(crate) const METADATA_FORMAT_NAMES: &[&str] =
    &["metadata-v3.json", "metadata-v2.json", "metadata-v1.json"];

/// Current metadata snapshot format version.
///
/// The rungs are *not* interchangeable in the way a wire format's are: a
/// snapshot carries no version field inside the document. Its version lives
/// entirely in the filename (`metadata-vN.json`), which is what makes an old
/// snapshot readable without any in-band marker. A second format is therefore a
/// second filename, listed in [`METADATA_FORMAT_NAMES`], plus a new `vN.rs` and
/// new dispatch arms.
pub(crate) const SNAPSHOT_VERSION: u32 = 3;

/// Every snapshot format version this build can read, ascending.
///
/// Reading a version outside this set would mean applying a parser to bytes it
/// was never written for, so the loader rejects it rather than guessing.
pub(crate) const SUPPORTED_SNAPSHOT_VERSIONS: &[u32] = &[1, 2, 3];

/// The single error every unknown-version path funnels through.
///
/// Naming the version that was found is the point: a caller holding
/// `metadata-v3.json` must be able to tell "this build does not know that
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

/// A snapshot loader for one format version: parse in that version's own
/// format, then carry the result into the current model.
///
/// Takes the raw snapshot bytes and yields `None` when the input is empty,
/// which every version treats as an absent snapshot rather than an error.
type SnapshotLoader = fn(&[u8]) -> Result<Option<Snapshot>, CasError>;

/// Read a V1 snapshot and carry it into the current model.
///
/// V1 migrates to V2 in `v2`, V2 migrates to V3 in `v3`, and this module owns
/// the chain because each version module may reference only its immediate
/// predecessor.
fn load_snapshot_v1(data: &[u8]) -> Result<Option<Snapshot>, CasError> {
    Ok(v1::parse_v1_snapshot(data)?.map(|v1| v3::migrate_v2_to_v3(v2::migrate_v1_to_v2(v1))))
}

/// Read a V2 snapshot and carry it into the current model.
///
/// V2 parses its own shape; V3 owns the rewrite into the current model. The
/// bridge lives here so `v2` does not reference a forward version.
fn load_snapshot_v2(data: &[u8]) -> Result<Option<Snapshot>, CasError> {
    Ok(v2::parse_v2_snapshot(data)?.map(v3::migrate_v2_to_v3))
}

/// Resolves one snapshot version to its loader.
///
/// The only place a version selects a codec. An unregistered version is an
/// error: falling back to the newest (or oldest) loader would read the bytes
/// under rules they were not written for and report a parse failure for a file
/// that is perfectly valid in a format this build simply lacks.
fn snapshot_loader_for_version(version: u32) -> Result<SnapshotLoader, CasError> {
    match version {
        1 => Ok(load_snapshot_v1),
        2 => Ok(load_snapshot_v2),
        3 => Ok(v3::load_v3_snapshot),
        other => Err(unsupported_version_error(other, "")),
    }
}

/// Decode a snapshot of one known version.
fn load_snapshot_for_version(version: u32, data: &[u8]) -> Result<Snapshot, CasError> {
    Ok(snapshot_loader_for_version(version)?(data)?.unwrap_or_default())
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
pub(crate) fn load_named_from_bytes(name: &str, data: &[u8]) -> Result<Snapshot, CasError> {
    load_snapshot_for_version(snapshot_version_from_name(name)?, data)
}

/// Serializes a snapshot to `Vec<u8>` in the current format.
///
/// Dispatches on [`SNAPSHOT_VERSION`] for symmetry with the read path, so
/// writing and reading agree on which format is current.
pub(crate) fn save_to_vec(snapshot: &Snapshot) -> Result<Vec<u8>, CasError> {
    match SNAPSHOT_VERSION {
        3 => v3::serialize_v3_snapshot(snapshot),
        // Unreachable while `SUPPORTED_SNAPSHOT_VERSIONS` and
        // `snapshot_loader_for_version` agree with this match, which
        // `every_supported_snapshot_version_has_a_parser` proves. Serializing
        // under an unimplemented format would write bytes no reader of this
        // build could parse, so halting is the correct response.
        other => panic!("unsupported metadata snapshot version {other}"),
    }
}

/// Serialize a snapshot in the V1 format.
///
/// Test-only. A test outside `versions/` must be able to seed bytes the previous
/// build could have written, and the versions boundary forbids it from naming
/// `v1` directly; this routes the one call it needs.
#[cfg(test)]
pub(crate) fn legacy_snapshot_bytes_for_tests(
    constraints: &BTreeMap<Hash, BTreeSet<Hash>>,
    entries: &BTreeMap<Hash, (u64, ObjectEncoding)>,
) -> Vec<u8> {
    v1::serialize_v1_snapshot(constraints, entries).expect("serialize a V1 snapshot")
}

/// The two-map body shared by every snapshot codec: delta bases and lengths.
///
/// Corruption is version-specific, so the shared decoder returns only the
/// fields every format carries.
pub(crate) type SnapshotBody = (BTreeMap<Hash, BTreeSet<Hash>>, BTreeMap<Hash, u64>);

/// Decode constraint hex keys into typed maps and pass through lengths.
///
/// Each codec decodes its own length keys because the on-disk shape differs by
/// version; the constraint body has the same hex-keyed shape across formats, so
/// its decoding and error messages live here once.
pub(crate) fn decode_snapshot_body(
    constraints: BTreeMap<String, Vec<String>>,
    entries: BTreeMap<Hash, u64>,
) -> Result<SnapshotBody, CasError> {
    let mut decoded = BTreeMap::new();
    for (target_hex, base_hexes) in constraints {
        let target: Hash = target_hex.parse().map_err(|_| CasError::CorruptObject {
            hash: None,
            details: format!("invalid target hash in snapshot file: {target_hex}"),
        })?;
        let bases: BTreeSet<Hash> = base_hexes.iter().filter_map(|h| h.parse().ok()).collect();
        if bases.len() != base_hexes.len() {
            return Err(CasError::CorruptObject {
                hash: None,
                details: "invalid base hash in snapshot file".into(),
            });
        }
        decoded.insert(target, bases);
    }
    Ok((decoded, entries))
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
    /// supported version must resolve to a loader. Bumping a constant without
    /// adding a loader would make a read halt.
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

    /// A current-format snapshot round-trips through dispatch.
    #[test]
    fn current_snapshot_round_trips_through_dispatch() {
        let target = Hash::from_content(b"target");
        let base = Hash::from_content(b"base");
        let entry = Hash::from_content(b"entry");
        let snapshot = Snapshot {
            delta_bases: BTreeMap::from([(target, BTreeSet::from([base]))]),
            lengths: BTreeMap::from([(entry, 11u64)]),
            corrupted: BTreeSet::new(),
        };

        let bytes = save_to_vec(&snapshot).expect("serialize current snapshot");
        let read = load_named_from_bytes(LATEST_METADATA_FORMAT, &bytes)
            .expect("dispatch must read the snapshot back");
        assert_eq!(read, snapshot);
    }

    /// The current format carries exactly three facts per directory: the delta
    /// bases, the payload lengths, and which hashes are known corrupt.
    #[test]
    fn a_v3_snapshot_records_lengths_delta_bases_and_corruption() {
        let target = Hash::from_content(b"target");
        let base = Hash::from_content(b"base");
        let entry = Hash::from_content(b"entry");
        let corrupt = Hash::from_content(b"corrupt");
        let snapshot = Snapshot {
            delta_bases: BTreeMap::from([(target, BTreeSet::from([base]))]),
            lengths: BTreeMap::from([(entry, 11u64)]),
            corrupted: BTreeSet::from([corrupt]),
        };

        let bytes = save_to_vec(&snapshot).expect("serialize a v3 snapshot");
        let text = String::from_utf8(bytes.clone()).expect("a snapshot is UTF-8 JSON");
        for field in ["\"delta_bases\"", "\"lengths\"", "\"corrupted\""] {
            assert!(text.contains(field), "a v3 snapshot must name {field}: {text}");
        }
        for gone in ["\"constraints\"", "\"entries\""] {
            assert!(!text.contains(gone), "v3 renamed {gone} away: {text}");
        }

        let read = load_named_from_bytes(LATEST_METADATA_FORMAT, &bytes)
            .expect("dispatch must read the snapshot back");
        assert_eq!(read, snapshot, "a v3 snapshot must round-trip every field");
    }

    /// A v2 file must keep loading after the rename, and its facts must arrive
    /// under the new names with no corruption recorded.
    #[test]
    fn a_v2_snapshot_loads_into_the_v3_model() {
        let target = Hash::from_content(b"target");
        let base = Hash::from_content(b"base");
        let entry = Hash::from_content(b"entry");
        let snapshot = Snapshot {
            delta_bases: BTreeMap::from([(target, BTreeSet::from([base]))]),
            lengths: BTreeMap::from([(entry, 9u64)]),
            corrupted: BTreeSet::new(),
        };
        let v2_bytes = v2::serialize_v2_snapshot(&snapshot.delta_bases, &snapshot.lengths)
            .expect("serialize a v2 snapshot");

        let read = load_named_from_bytes("metadata-v2.json", &v2_bytes)
            .expect("a v2 snapshot must still load");
        assert_eq!(read.delta_bases, snapshot.delta_bases);
        assert_eq!(read.lengths, snapshot.lengths);
        assert!(read.corrupted.is_empty(), "v2 has no corruption record to migrate");
    }

    /// `corrupted` is a set of hashes with no clock beside them. A timestamp
    /// nothing reads is a field waiting to disagree with the tree.
    #[test]
    fn a_v3_snapshot_stores_corruption_as_bare_hashes() {
        let corrupt = Hash::from_content(b"corrupt");
        let bytes = save_to_vec(&Snapshot {
            delta_bases: BTreeMap::new(),
            lengths: BTreeMap::new(),
            corrupted: BTreeSet::from([corrupt]),
        })
        .expect("serialize a v3 snapshot");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("a snapshot is JSON");
        assert_eq!(
            value["corrupted"],
            serde_json::json!([corrupt.to_string()]),
            "a corruption record is an array of hex hashes and nothing else"
        );
    }

    /// The current format must carry exactly one fact per entry: its length.
    /// `encoding` is derived from which file exists, so persisting it would be
    /// a second copy that can disagree with the tree.
    #[test]
    fn a_v3_snapshot_records_lengths_and_no_encoding() {
        let entry = Hash::from_content(b"entry");
        let bytes = save_to_vec(&Snapshot {
            delta_bases: BTreeMap::new(),
            lengths: BTreeMap::from([(entry, 11u64)]),
            corrupted: BTreeSet::new(),
        })
        .expect("serialize a v3 snapshot");
        let text = String::from_utf8(bytes).expect("a snapshot is UTF-8 JSON");
        assert!(
            !text.contains("encoding"),
            "a v3 snapshot must not persist an encoding, the tree already states it: {text}"
        );
        assert!(
            text.contains(&entry.to_string()),
            "the snapshot must still name the entry's hash: {text}"
        );
    }

    /// A v2 entry is a bare length; the old object shape (which carried
    /// `encoding`) must be rejected as a type error, not parsed.
    #[test]
    fn a_v2_snapshot_rejects_object_shaped_entries() {
        let hash = Hash::from_content(b"entry").to_string();
        let bytes = serde_json::to_vec(&serde_json::json!({
            "constraints": {},
            "entries": { hash: { "len": 1, "encoding": "full" } }
        }))
        .expect("serialize a v2-shaped fixture");
        let err = load_named_from_bytes("metadata-v2.json", &bytes)
            .expect_err("a v2 object-shaped entry must not parse as a bare length");
        let message = err.to_string();
        assert!(
            message.contains("invalid type")
                || message.contains("expected u64")
                || message.contains("expected integer"),
            "the error must name the type mismatch, got: {message}"
        );
    }

    /// A V1 file must still load after the format is bumped, and its encoding
    /// must be dropped rather than carried into the current model.
    #[test]
    fn a_v1_snapshot_loads_into_the_current_model_without_its_encoding() {
        let target = Hash::from_content(b"target");
        let full = Hash::from_content(b"full");
        let delta = Hash::from_content(b"delta-base");
        let constraints = BTreeMap::from([(target, BTreeSet::from([Hash::from_content(b"base")]))]);
        let entries = BTreeMap::from([
            (full, (7u64, ObjectEncoding::Full)),
            (delta, (9u64, ObjectEncoding::Delta { base_hash: delta })),
        ]);

        let v1_bytes = v1::serialize_v1_snapshot(&constraints, &entries)
            .expect("serialize a v1 snapshot that still records encodings");
        let read = load_named_from_bytes("metadata-v1.json", &v1_bytes)
            .expect("a v1 snapshot must load into the current model");

        assert_eq!(read.delta_bases, constraints, "constraints survive the migration");
        assert_eq!(
            read.lengths,
            BTreeMap::from([(full, 7u64), (delta, 9u64)]),
            "the migration keeps every length and drops every encoding"
        );
        assert!(read.corrupted.is_empty(), "v1 has no corruption record to migrate");
    }

    /// The format list is read newest-first and applied oldest-first, so its
    /// order is the contract rather than an incidental detail.
    #[test]
    fn the_format_list_is_newest_first() {
        assert_eq!(
            METADATA_FORMAT_NAMES,
            &["metadata-v3.json", "metadata-v2.json", "metadata-v1.json"],
            "readers iterate this list newest-first and apply it oldest-first"
        );
    }

    /// Every supported version must have a loader arm, and a snapshot written
    /// under any supported version must load through dispatch with its facts
    /// intact. This is the migration arm's proof.
    #[test]
    fn every_supported_snapshot_version_loads_its_own_bytes() {
        assert!(SUPPORTED_SNAPSHOT_VERSIONS.contains(&SNAPSHOT_VERSION));
        for &version in SUPPORTED_SNAPSHOT_VERSIONS {
            assert!(
                snapshot_loader_for_version(version).is_ok(),
                "snapshot version {version} has no loader arm"
            );
        }

        let target = Hash::from_content(b"target");
        let entry = Hash::from_content(b"entry");
        let constraints = BTreeMap::from([(target, BTreeSet::from([Hash::from_content(b"base")]))]);
        let lengths = BTreeMap::from([(entry, 5u64)]);

        let v3_bytes = save_to_vec(&Snapshot {
            delta_bases: constraints.clone(),
            lengths: lengths.clone(),
            corrupted: BTreeSet::new(),
        })
        .expect("serialize a v3 snapshot");
        let v2_bytes =
            v2::serialize_v2_snapshot(&constraints, &lengths).expect("serialize a v2 snapshot");
        let v1_bytes = v1::serialize_v1_snapshot(
            &constraints,
            &BTreeMap::from([(entry, (5u64, ObjectEncoding::Full))]),
        )
        .expect("serialize a v1 snapshot");

        for (version, bytes) in [(3u32, v3_bytes), (2u32, v2_bytes), (1u32, v1_bytes)] {
            let name = format!("metadata-v{version}.json");
            let read = load_named_from_bytes(&name, &bytes).expect("a supported version must load");
            assert_eq!(read.delta_bases, constraints, "version {version} lost its constraints");
            assert_eq!(read.lengths, lengths, "version {version} lost its entries");
            assert!(read.corrupted.is_empty(), "version {version} invented corruption");
        }
    }

    /// The test-only V1 writer must produce bytes the ladder reads back, or a
    /// test outside this directory would seed a legacy file no reader accepts.
    #[test]
    fn the_test_only_legacy_writer_produces_a_readable_snapshot() {
        let entry = Hash::from_content(b"entry");
        let bytes = legacy_snapshot_bytes_for_tests(
            &BTreeMap::new(),
            &BTreeMap::from([(entry, (3u64, ObjectEncoding::Full))]),
        );
        let read = load_named_from_bytes("metadata-v1.json", &bytes)
            .expect("the seeded legacy bytes must load through dispatch");
        assert_eq!(read.lengths, BTreeMap::from([(entry, 3u64)]));
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
        let valid_v1_bytes = save_to_vec(&Snapshot {
            delta_bases: constraints,
            lengths: BTreeMap::new(),
            corrupted: BTreeSet::new(),
        })
        .expect("serialize a valid V3 snapshot");

        for version in [0u32, 4, 7, 999] {
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
