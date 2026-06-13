//! Versioned binary wire-format envelopes for CAS delta objects.
//!
//! The long-lived functional core is [`crate::delta::object::DeltaState`].
//! Each wire version owns:
//! - its exact byte layout,
//! - parse/validate/encode behavior,
//! - direct `From` conversions to/from version-specific state types.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version migration.
//! - This `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside `delta/versions/` and `index/versions/` must interact with
//!   versioned envelopes only through each folder's `versions/mod.rs`, never
//!   through direct `versions::vX` imports.
//! - Do not directly re-export `versions::vX` structs/types from this module.
//!   Expose unversioned APIs here and keep versioned internals encapsulated.

use crate::CasError;
use crate::delta::object::DeltaState;

pub(crate) mod v1;
pub(crate) mod v2;
pub(crate) mod v3;

/// Prefix bytes required to dispatch a delta envelope: `magic_with_version`[8].
const ENVELOPE_PREFIX_LEN: usize = 8;

/// Stable family prefix; trailing two bytes in magic encode little-endian `u16` version.
const DIFF_STORAGE_FAMILY_PREFIX: &[u8; 6] = b"MDCASD";

/// Latest supported wire version.
#[expect(dead_code, reason = "available for external migration use")]
pub(crate) const LATEST_WIRE_VERSION: u16 = 3;

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

/// ## DO NOT REMOVE: latest-first version dispatch guard
///
/// Version checks shoud always start checking from the latest version to ensure performance.
fn dispatch_delta_wire_version(version: u16) -> Result<(), CasError> {
    match version {
        3 | 2 | 1 => Ok(()),
        _ => {
            Err(CasError::corrupt_object(format!("delta envelope: unsupported version {version}")))
        }
    }
}

/// Decodes versioned `.diff` bytes into version-agnostic [`DeltaState`].
///
/// This function is the only version-dispatch entry point. It peeks at
/// `magic_with_embedded_version` and dispatches to the matching version parser,
/// migrating older formats forward to the latest state representation via `From` impls.
pub(crate) fn decode_delta_state(bytes: &[u8]) -> Result<DeltaState, CasError> {
    if bytes.len() < ENVELOPE_PREFIX_LEN {
        return Err(CasError::corrupt_object(
            "delta envelope: buffer too short for magic-with-version prefix",
        ));
    }

    let version = decode_magic_embedded_version(bytes)?;
    dispatch_delta_wire_version(version)?;

    match version {
        3 => {
            let envelope = v3::V3Envelope::parse(bytes)?;
            envelope.validate()?;
            let state = v3::DeltaStateV3::from(envelope);
            Ok(DeltaState {
                base_hash: state.base_hash,
                content_len: state.content_len,
                payload: state.payload,
            })
        }
        2 => {
            let envelope = v2::V2Envelope::parse(bytes)?;
            envelope.validate()?;
            let state_v2 = v2::DeltaStateV2::from(envelope);
            let state_v3 = v3::DeltaStateV3::from(state_v2);
            Ok(DeltaState {
                base_hash: state_v3.base_hash,
                content_len: state_v3.content_len,
                payload: state_v3.payload,
            })
        }
        1 => {
            let envelope = v1::V1Envelope::parse(bytes)?;
            envelope.validate()?;
            let state_v1 = v1::DeltaStateV1::from(envelope);
            let state_v2 = v2::DeltaStateV2::from(state_v1);
            let state_v3 = v3::DeltaStateV3::from(state_v2);
            Ok(DeltaState {
                base_hash: state_v3.base_hash,
                content_len: state_v3.content_len,
                payload: state_v3.payload,
            })
        }
        _ => unreachable!("dispatch_delta_wire_version validated the version"),
    }
}

/// Encodes unversioned runtime delta state using the latest wire version.
pub(crate) fn encode_delta_state(state: DeltaState) -> Vec<u8> {
    let state_v3 = v3::DeltaStateV3 {
        base_hash: state.base_hash,
        content_len: state.content_len,
        diff_hash: *blake3::hash(&state.payload).as_bytes(),
        payload: state.payload,
    };
    let envelope = v3::V3Envelope::from(state_v3);
    envelope.encode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::Hash;

    /// Collects `vN.rs` files and parsed numeric version markers in ascending order.
    fn collect_versioned_files(dir: &Path) -> Vec<(PathBuf, u32)> {
        let mut files = fs::read_dir(dir)
            .unwrap_or_else(|err| panic!("failed to read versions dir '{}': {err}", dir.display()))
            .filter_map(std::result::Result::ok)
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
        normalized.contains("/delta/versions/") || normalized.contains("/index/versions/")
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
        const LEAKED_TOKENS: &[&str] = &[
            "IndexStateV",
            "ObjectMetaV",
            "PrimaryHeaderV",
            "V1Envelope",
            "V2Envelope",
            "DeltaStateV",
        ];
        LEAKED_TOKENS.iter().any(|token| content.contains(token))
    }

    #[test]
    /// Verifies latest-version encoded payloads decode through dispatch path.
    fn decode_latest_roundtrip_restores_state() {
        let state = DeltaState {
            base_hash: Hash::from_content(b"base"),
            content_len: 12,
            payload: vec![1, 2, 3, 4],
        };

        let bytes = encode_delta_state(state.clone());
        let restored = decode_delta_state(&bytes).expect("v1 payload should decode via dispatcher");

        assert_eq!(restored, state);
    }

    #[test]
    /// Verifies V1 wire format bytes decode through V1→V2 migration dispatch.
    fn decode_dispatches_v1_and_restores_state() {
        let v1_envelope = v1::V1Envelope {
            base_hash: Hash::from_content(b"base"),
            content_len: 12,
            payload_len: 4,
            checksum: 0,
            payload: vec![1, 2, 3, 4],
        };

        let v1_bytes = v1_envelope.encode();
        let restored = decode_delta_state(&v1_bytes)
            .expect("V1 payload should decode and migrate via dispatcher");

        assert_eq!(
            restored,
            DeltaState {
                base_hash: v1_envelope.base_hash,
                content_len: v1_envelope.content_len,
                payload: v1_envelope.payload,
            }
        );
    }

    #[test]
    /// Verifies dispatch rejects payloads with invalid family magic.
    fn decode_rejects_bad_magic() {
        let state = DeltaState {
            base_hash: Hash::from_content(b"base"),
            content_len: 12,
            payload: vec![1, 2, 3, 4],
        };

        let mut bytes = encode_delta_state(state);
        bytes[0] ^= 0xFF;

        let error = decode_delta_state(&bytes)
            .expect_err("bad magic must fail dispatcher prefix validation");
        assert!(matches!(error, CasError::CorruptObject { .. }));
    }

    #[test]
    /// Verifies dispatcher rejects unknown/unsupported wire versions.
    fn decode_rejects_unsupported_version() {
        let state = DeltaState {
            base_hash: Hash::from_content(b"base"),
            content_len: 12,
            payload: vec![1, 2, 3, 4],
        };

        let mut bytes = encode_delta_state(state);
        // Use version 99 (well beyond latest=3) to ensure rejection
        bytes[6] = 99;
        bytes[7] = 0;

        let error =
            decode_delta_state(&bytes).expect_err("unknown envelope version must fail dispatcher");
        assert!(matches!(error, CasError::CorruptObject { .. }));
    }

    #[test]
    /// Enforces version-folder policy guard and vN reference boundaries.
    fn versioned_files_keep_policy_guard_and_boundary_rules() {
        let versions_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/delta/versions");
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

            let referenced_versions = extract_version_module_refs(&content);
            for referenced in referenced_versions {
                if referenced == version {
                    continue;
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
        let mod_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/delta/versions/mod.rs");
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
        let mod_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/delta/versions/mod.rs");
        let content = fs::read_to_string(&mod_file)
            .unwrap_or_else(|err| panic!("failed reading '{}': {err}", mod_file.display()));

        assert!(
            content.contains("Version checks shoud always start checking from the latest version to ensure performance."),
            "{} must keep the latest-first version dispatch performance guard docstring",
            mod_file.display()
        );
    }
}
