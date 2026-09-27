//! Structural policy tests for the `delta/versions/` folder.
//!
//! These tests read the folder's own source files and assert the versioning
//! rules hold: the non-removable guard marker is present, a `vX.rs` file
//! references only `v(X-1)`, non-`versions/` files never name a `versions::vX`
//! path, no versioned type leaks past the boundary, and `mod.rs` neither
//! re-exports `vX` symbols nor loses its latest-first dispatch note.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version migration.
//! - `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside `versions/` must interact with versioned envelopes only
//!   through `mod.rs`, never through direct `versions::vX` imports.
//! - Do not directly re-export `versions::vX` structs/types. Expose
//!   unversioned APIs and keep versioned internals encapsulated.
//!
//! This file carries the guard marker because the guard applies to every file
//! in a `versions/` directory, not only to the ones with production code. It
//! restates `mod.rs`'s guard; it does not replace it.

use std::fs;
use std::path::{Path, PathBuf};

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

        let content = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed reading source file '{}': {err}", path.display()));

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

    // Search the production portion only: the asserted literal below lives in
    // this same file, so scanning the whole file would let the assertion
    // satisfy itself and the guard could never fail.
    let (production, _tests) = content
        .split_once("#[cfg(test)]")
        .unwrap_or_else(|| panic!("'{}' must contain a #[cfg(test)] module", mod_file.display()));

    assert!(
        production.contains("Version checks should always start checking from the latest version to ensure performance."),
        "{} must keep the latest-first version dispatch performance guard docstring",
        mod_file.display()
    );
}

#[test]
/// Enforces the non-removable versions policy guard in every `versions/`
/// directory under this crate's `src/`.
///
/// `versioned_files_keep_policy_guard_and_boundary_rules` pins the guard for
/// `src/delta/versions/` only, so the same deletion under `storage/wal/`,
/// `storage/blob_store/` or `storage/metadata_store/` stays invisible to that test.
/// This test discovers the directories instead of naming them, so a new
/// `versions/` directory under this crate's `src/` is covered as soon as it exists.
/// The scan root stays crate-local on purpose: covering the repo-wide marker
/// policy would need a workspace-wide scan root and a marker-name decision first.
fn every_versions_dir_keeps_policy_guard_docstring() {
    let src_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut rs_files = Vec::new();
    collect_rs_files_recursive(&src_dir, &mut rs_files);

    let mut guarded = Vec::new();
    let mut discovered = Vec::new();
    for path in &rs_files {
        let Some(versions_dir) = path.parent() else {
            continue;
        };
        if versions_dir.file_name().and_then(|name| name.to_str()) != Some("versions") {
            continue;
        }
        // The parent module of `versions/` (`delta`, `wal`, `blob_store`, ...).
        let Some(owner) =
            versions_dir.parent().and_then(|dir| dir.file_name()).and_then(|name| name.to_str())
        else {
            continue;
        };
        if !discovered.iter().any(|dir| dir == owner) {
            discovered.push(owner.to_string());
        }
        guarded.push(path.clone());
    }

    assert!(!guarded.is_empty(), "no versions/ files found under {}", src_dir.display());
    // A walker that silently stops descending would otherwise pass on an empty
    // set, which is exactly how the guard-marker drift went unnoticed.
    for expected in ["blob_store", "delta", "metadata_store", "wal"] {
        assert!(
            discovered.iter().any(|dir| dir == expected),
            "versions/ directory '{expected}' was not discovered; discovered: {discovered:?}"
        );
    }

    let mut missing = Vec::new();
    for path in guarded {
        let content = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed reading '{}': {err}", path.display()));
        // Search the production portion only: this test lives inside
        // `delta/versions/mod.rs`, so scanning that whole file would let the
        // asserted literal satisfy itself and the guard could never fail.
        let production = content
            .split_once("#[cfg(test)]")
            .map_or(content.as_str(), |(production, _tests)| production);
        if !production.contains("//! ## DO NOT REMOVE: versions policy guard") {
            missing.push(path);
        }
    }
    assert!(
        missing.is_empty(),
        "these files must include the non-removable versions policy guard docstring: {missing:#?}"
    );
}
