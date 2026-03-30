//! Versioned binary wire-format envelopes for CAS delta objects.
//!
//! The long-lived functional core is [`crate::codec::object::DeltaState`].
//! Each wire version owns:
//! - its exact byte layout,
//! - parse/validate/encode behavior,
//! - one `IsoPrime` bridge to/from `DeltaState`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version isomorphism/migration.
//! - This `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside `codec/versions/` and `index/versions/` must interact with
//!   versioned envelopes only through each folder's `versions/mod.rs`, never
//!   through direct `versions::vX` imports.
//! - Do not directly re-export `versions::vX` structs/types from this module.
//!   Expose unversioned APIs here and keep versioned internals encapsulated.

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;

use crate::CasError;
use crate::codec::object::DeltaState;

pub(crate) mod v1;

/// Prefix bytes required to dispatch a delta envelope: magic_with_version[8].
const ENVELOPE_PREFIX_LEN: usize = 8;

/// Stable family prefix; trailing two bytes in magic encode little-endian `u16` version.
const DIFF_STORAGE_FAMILY_PREFIX: &[u8; 6] = b"MDCASD";

/// Latest-version bindings.
///
/// Keep all explicit latest-version (`vX`) references centralized in this
/// module so version bumps are less error-prone.
// BEGIN latest-version bindings
mod latest {
    use super::v1;
    use crate::codec::object::DeltaState;
    use fp_library::brands::RcBrand;
    use fp_library::types::optics::IsoPrime;

    pub(super) const WIRE_VERSION: u16 = 1;
    pub(super) type Envelope<'a> = v1::V1Envelope<'a>;
    pub(super) type State<'a> = v1::DeltaStateV1<'a>;

    pub(super) fn runtime_iso<'a>() -> IsoPrime<'a, RcBrand, State<'a>, DeltaState<'a>> {
        IsoPrime::new(
            |latest: State<'a>| DeltaState {
                base_hash: latest.base_hash,
                content_len: latest.content_len,
                payload: latest.payload,
            },
            |runtime: DeltaState<'a>| State {
                base_hash: runtime.base_hash,
                content_len: runtime.content_len,
                payload: runtime.payload,
            },
        )
    }

    pub(super) fn version_iso<'a>() -> IsoPrime<'a, RcBrand, Envelope<'a>, State<'a>> {
        v1::delta_state_v1_iso()
    }
}
// END latest-version bindings

/// Decodes and validates the embedded wire version from envelope magic bytes.
fn decode_magic_embedded_version(bytes: &[u8]) -> Result<u16, CasError> {
    if &bytes[..DIFF_STORAGE_FAMILY_PREFIX.len()] != DIFF_STORAGE_FAMILY_PREFIX {
        return Err(CasError::corrupt_object("delta envelope: magic mismatch"));
    }

    let version = u16::from_le_bytes([bytes[6], bytes[7]]);
    if version == 0 {
        return Err(CasError::corrupt_object(
            "delta envelope: embedded version 0 is reserved for a future >65535-version scheme",
        ));
    }

    Ok(version)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Internal dispatched delta envelope layout marker.
enum DeltaEnvelopeVersion {
    V1,
}

/// Returns the latest supported wire envelope version marker.
#[must_use]
pub(crate) const fn latest_delta_wire_version() -> u16 {
    latest::WIRE_VERSION
}

/// ## DO NOT REMOVE: latest-first version dispatch guard
///
/// Version checks shoud always start checking from the latest version to ensure performance.
fn dispatch_delta_wire_version(version: u16) -> Result<DeltaEnvelopeVersion, CasError> {
    if version == latest_delta_wire_version() {
        return Ok(DeltaEnvelopeVersion::V1);
    }

    Err(CasError::corrupt_object(format!("delta envelope: unsupported version {}", version)))
}

/// Optic-based migration trait between envelope versions.
///
/// Implementors must migrate by composing version optics through `DeltaState`
/// rather than rewriting or re-compressing payload bytes.
pub(crate) trait Migrate<To> {
    /// Migrates `self` to `To` using optic composition.
    fn migrate(self) -> To;
}

/// Shared migration helper using `old_iso.view` then `new_iso.review`.
pub(crate) fn migrate_with_version_state<'a, From, To, State>(
    from: From,
    from_iso: &IsoPrime<'a, RcBrand, From, State>,
    to_iso: &IsoPrime<'a, RcBrand, To, State>,
) -> To {
    let state = from_iso.from(from);
    to_iso.to(state)
}

/// Isomorphism between latest version-local state and unversioned runtime state.
pub(crate) fn latest_delta_state_iso<'a>()
-> IsoPrime<'a, RcBrand, latest::State<'a>, DeltaState<'a>> {
    latest::runtime_iso()
}

/// Decodes versioned `.diff` bytes into version-agnostic [`DeltaState`], borrowing payload bytes.
///
/// This function is the only version-dispatch entry point. It peeks at
/// `magic_with_embedded_version` and delegates to the matching version module.
pub(crate) fn decode_delta_state_borrowed<'a>(bytes: &'a [u8]) -> Result<DeltaState<'a>, CasError> {
    if bytes.len() < ENVELOPE_PREFIX_LEN {
        return Err(CasError::corrupt_object(
            "delta envelope: buffer too short for magic-with-version prefix",
        ));
    }

    let version = decode_magic_embedded_version(bytes)?;
    let envelope = decode_envelope_for_version(bytes, version)?;
    let migrated = migrate_envelope_to_version(envelope, version, latest_delta_wire_version())?;
    let latest_version_state = latest::version_iso().from(migrated);
    Ok(latest_delta_state_iso().from(latest_version_state))
}

/// Parses one delta envelope according to one already-dispatched wire version.
fn decode_envelope_for_version<'a>(
    bytes: &'a [u8],
    version: u16,
) -> Result<latest::Envelope<'a>, CasError> {
    match dispatch_delta_wire_version(version)? {
        DeltaEnvelopeVersion::V1 => {
            let envelope = v1::V1Envelope::parse(bytes)?;
            envelope.validate()?;
            Ok(envelope)
        }
    }
}

/// Migrates one parsed envelope from one wire version to one target wire version.
///
/// This is intentionally the central migration gateway used by decode paths.
pub(crate) fn migrate_envelope_to_version<'a>(
    envelope: latest::Envelope<'a>,
    from_version: u16,
    target_version: u16,
) -> Result<latest::Envelope<'a>, CasError> {
    let from_layout = dispatch_delta_wire_version(from_version)?;
    let to_layout = dispatch_delta_wire_version(target_version)?;

    match (from_layout, to_layout) {
        (DeltaEnvelopeVersion::V1, DeltaEnvelopeVersion::V1) => Ok(envelope.migrate()),
    }
}

/// Decodes versioned `.diff` bytes into owned [`DeltaState`].
///
/// This convenience wrapper preserves the existing `StoredObject` ownership model.
pub(crate) fn decode_delta_state(bytes: &[u8]) -> Result<DeltaState<'static>, CasError> {
    Ok(decode_delta_state_borrowed(bytes)?.into_owned())
}

/// Encodes unversioned runtime delta state using the latest wire version.
pub(crate) fn encode_delta_state<'a>(state: DeltaState<'a>) -> Vec<u8> {
    let latest_version_state = latest_delta_state_iso().to(state);
    let envelope = latest::version_iso().to(latest_version_state);
    envelope.encode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::Hash;

    /// Collects `vN.rs` files and parsed numeric version markers in ascending order.
    fn collect_versioned_files(dir: &Path) -> Vec<(PathBuf, u32)> {
        let mut files = fs::read_dir(dir)
            .unwrap_or_else(|err| panic!("failed to read versions dir '{}': {err}", dir.display()))
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                if !path.is_file() {
                    return None;
                }

                let file_name = path.file_name()?.to_string_lossy();
                let version = file_name
                    .strip_prefix('v')
                    .and_then(|tail| tail.strip_suffix(".rs"))
                    .and_then(|digits| digits.parse::<u32>().ok())?;
                Some((path, version))
            })
            .collect::<Vec<_>>();
        files.sort_unstable_by_key(|(_, version)| *version);
        files
    }

    /// Extracts `::vN::` style module references from source text.
    fn extract_version_module_refs(content: &str) -> Vec<u32> {
        let bytes = content.as_bytes();
        let mut refs = Vec::new();
        let mut idx = 0usize;

        while idx + 3 < bytes.len() {
            if bytes[idx] != b'v' {
                idx += 1;
                continue;
            }

            let mut end = idx + 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }

            let looks_like_path_segment = end > idx + 1
                && end + 1 < bytes.len()
                && bytes[end] == b':'
                && bytes[end + 1] == b':'
                && idx >= 2
                && bytes[idx - 1] == b':'
                && bytes[idx - 2] == b':';

            if looks_like_path_segment && let Ok(version) = content[idx + 1..end].parse::<u32>() {
                refs.push(version);
            }

            idx = end;
        }

        refs
    }

    /// Recursively collects Rust source files under `dir`.
    fn collect_rs_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir)
            .unwrap_or_else(|err| panic!("failed to read source dir '{}': {err}", dir.display()))
        {
            let entry = entry.unwrap_or_else(|err| {
                panic!("failed reading source dir entry in '{}': {err}", dir.display())
            });
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files_recursive(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    /// Returns `true` when `path` is inside one of the versions directories.
    fn path_is_inside_versions_dir(path: &Path) -> bool {
        let normalized = path.to_string_lossy().replace('\\', "/");
        normalized.contains("/codec/versions/") || normalized.contains("/index/versions/")
    }

    /// Detects direct `versions::vN` path usage in non-versions files.
    fn has_direct_versions_vx_path(content: &str) -> bool {
        let bytes = content.as_bytes();
        let needle = b"versions::v";
        let mut idx = 0usize;

        while idx + needle.len() < bytes.len() {
            let Some(rel) = bytes[idx..].windows(needle.len()).position(|w| w == needle) else {
                break;
            };
            let pos = idx + rel;
            let mut end = pos + needle.len();
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }

            if end > pos + needle.len() {
                let boundary_ok = end == bytes.len()
                    || matches!(
                        bytes[end],
                        b':' | b';' | b',' | b' ' | b'\n' | b'\r' | b'{' | b'(' | b')'
                    );
                if boundary_ok {
                    return true;
                }
            }

            idx = end;
        }

        false
    }

    /// Detects leaked versioned type tokens outside version boundaries.
    fn has_known_versioned_type_leak(content: &str) -> bool {
        const LEAKED_TOKENS: &[&str] =
            &["IndexStateV", "ObjectMetaV", "PrimaryHeaderV", "V1Envelope", "DeltaStateV"];
        LEAKED_TOKENS.iter().any(|token| content.contains(token))
    }

    #[test]
    /// Verifies latest-version encoded payloads decode through dispatch path.
    fn decode_dispatches_v1_and_restores_state() {
        let state = DeltaState {
            base_hash: Hash::from_content(b"base"),
            content_len: 12,
            payload: Cow::Owned(vec![1, 2, 3, 4]),
        };

        let bytes = encode_delta_state(state.clone());
        let restored =
            decode_delta_state_borrowed(&bytes).expect("v1 payload should decode via dispatcher");

        assert_eq!(restored, state);
    }

    #[test]
    /// Verifies dispatch rejects payloads with invalid family magic.
    fn decode_rejects_bad_magic() {
        let state = DeltaState {
            base_hash: Hash::from_content(b"base"),
            content_len: 12,
            payload: Cow::Owned(vec![1, 2, 3, 4]),
        };

        let mut bytes = encode_delta_state(state);
        bytes[0] ^= 0xFF;

        let error = decode_delta_state_borrowed(&bytes)
            .expect_err("bad magic must fail dispatcher prefix validation");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[test]
    /// Verifies owned decode wrapper preserves decoded runtime state.
    fn decode_owned_wrapper_restores_state() {
        let state = DeltaState {
            base_hash: Hash::from_content(b"base"),
            content_len: 12,
            payload: Cow::Owned(vec![1, 2, 3, 4]),
        };

        let bytes = encode_delta_state(state.clone());
        let restored = decode_delta_state(&bytes).expect("owned wrapper should decode payload");

        assert_eq!(restored, state);
    }

    #[test]
    /// Verifies dispatcher rejects unknown/unsupported wire versions.
    fn decode_rejects_unsupported_version() {
        let state = DeltaState {
            base_hash: Hash::from_content(b"base"),
            content_len: 12,
            payload: Cow::Owned(vec![1, 2, 3, 4]),
        };

        let mut bytes = encode_delta_state(state);
        bytes[6] = 2;
        bytes[7] = 0;

        let error =
            decode_delta_state(&bytes).expect_err("unknown envelope version must fail dispatcher");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[test]
    /// Enforces version-folder policy guard and vN reference boundaries.
    fn versioned_files_keep_policy_guard_and_boundary_rules() {
        let versions_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/codec/versions");
        let files = collect_versioned_files(&versions_dir);
        assert!(
            !files.is_empty(),
            "expected at least one versioned file under {}",
            versions_dir.display()
        );

        for (path, version) in files {
            let content = fs::read_to_string(&path).unwrap_or_else(|err| {
                panic!("failed reading versioned file '{}': {err}", path.display())
            });

            assert!(
                content.contains("## DO NOT REMOVE: versions policy guard"),
                "{} must include the non-removable versions policy guard docstring",
                path.display()
            );

            assert!(
                !content.contains("use crate::codec::object::")
                    && !content.contains("crate::codec::object::DeltaState"),
                "{} must not import unversioned codec runtime structs directly",
                path.display()
            );

            let referenced_versions = extract_version_module_refs(&content);
            for referenced in referenced_versions {
                if referenced == version {
                    continue;
                }

                if version == 1 {
                    panic!(
                        "{} (v1) must not reference any other version module; found v{}",
                        path.display(),
                        referenced
                    );
                }

                assert_eq!(
                    referenced,
                    version - 1,
                    "{} (v{}) may reference only v{}; found v{}",
                    path.display(),
                    version,
                    version - 1,
                    referenced
                );
            }
        }
    }

    #[test]
    /// Ensures non-version files never directly import `versions::vN` symbols.
    fn non_versions_files_never_import_versions_vx_directly() {
        let src_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut rs_files = Vec::new();
        collect_rs_files_recursive(&src_dir, &mut rs_files);

        for path in rs_files {
            if path_is_inside_versions_dir(&path) {
                continue;
            }

            let content = fs::read_to_string(&path).unwrap_or_else(|err| {
                panic!("failed reading source file '{}': {err}", path.display())
            });

            assert!(
                !has_direct_versions_vx_path(&content),
                "{} must not directly import or reference versions::vX; route through versions/mod.rs",
                path.display()
            );

            assert!(
                !has_known_versioned_type_leak(&content),
                "{} must not depend on leaked versioned type names; keep non-versions files on unversioned runtime models",
                path.display()
            );
        }
    }

    #[test]
    /// Ensures this module does not directly re-export version-specific symbols.
    fn versions_mod_must_not_reexport_versioned_symbols() {
        let mod_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/codec/versions/mod.rs");
        let content = fs::read_to_string(&mod_file)
            .unwrap_or_else(|err| panic!("failed reading '{}': {err}", mod_file.display()));
        let has_direct_reexport = content.lines().any(|line| {
            let line = line.trim_start();
            line.starts_with("pub(crate) use v") || line.starts_with("pub use v")
        });

        assert!(
            !has_direct_reexport,
            "{} must not directly re-export vX symbols; expose unversioned APIs instead",
            mod_file.display()
        );
    }

    #[test]
    /// Ensures latest-first dispatch performance guard text remains present.
    fn versions_mod_keeps_latest_first_dispatch_docstring() {
        let mod_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/codec/versions/mod.rs");
        let content = fs::read_to_string(&mod_file)
            .unwrap_or_else(|err| panic!("failed reading '{}': {err}", mod_file.display()));

        assert!(
            content.contains("Version checks shoud always start checking from the latest version to ensure performance."),
            "{} must keep the latest-first version dispatch performance guard docstring",
            mod_file.display()
        );
    }

    #[test]
    /// Ensures latest-version binding block remains centralized in one section.
    fn versions_mod_centralizes_latest_bindings_block() {
        let mod_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/codec/versions/mod.rs");
        let full_content = fs::read_to_string(&mod_file)
            .unwrap_or_else(|err| panic!("failed reading '{}': {err}", mod_file.display()));
        let content = full_content
            .split("\n#[cfg(test)]")
            .next()
            .expect("split always yields at least one segment");

        let begin = "// BEGIN latest-version bindings";
        let end = "// END latest-version bindings";
        let Some(begin_idx) = content.find(begin) else {
            panic!(
                "{} must include '{}' marker to centralize latest bindings",
                mod_file.display(),
                begin
            );
        };
        let Some(end_idx) = content.find(end) else {
            panic!(
                "{} must include '{}' marker to centralize latest bindings",
                mod_file.display(),
                end
            );
        };
        assert!(
            begin_idx < end_idx,
            "{} latest bindings markers are out of order",
            mod_file.display()
        );

        let mut outside = String::with_capacity(content.len());
        outside.push_str(&content[..begin_idx]);
        outside.push_str(&content[end_idx + end.len()..]);

        assert!(
            content.contains("pub(crate) const fn latest_delta_wire_version() -> u16")
                && content.contains("latest::WIRE_VERSION"),
            "{} must centralize latest wire-version access through latest bindings",
            mod_file.display()
        );

        let allowed_functions = ["decode_envelope_for_version", "migrate_envelope_to_version"];
        let mut current_fn: Option<String> = None;
        for line in outside.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("fn ") || trimmed.starts_with("pub(crate) fn ") {
                let after_kw = if let Some(rest) = trimmed.strip_prefix("pub(crate) fn ") {
                    rest
                } else if let Some(rest) = trimmed.strip_prefix("fn ") {
                    rest
                } else {
                    continue;
                };

                let name = after_kw
                    .split('(')
                    .next()
                    .unwrap_or_default()
                    .split('<')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                current_fn = Some(name);
            }

            if trimmed.contains("v1::") {
                let fn_name = current_fn.as_deref().unwrap_or("<module>");
                assert!(
                    allowed_functions.contains(&fn_name),
                    "{} must use direct v1:: references only in dispatch-conditioned functions; found in {}: {}",
                    mod_file.display(),
                    fn_name,
                    trimmed
                );
            }
        }
    }
}
