//! Structural policy tests for the `delta/versions/` folder.
//!
//! These tests read the folder's own source files and assert the versioning
//! rules hold: the non-removable guard marker is present, a `vX.rs` file
//! references only `v(X-1)`, non-`versions/` files never name a `versions::vX`
//! path, no versioned type leaks past the boundary, and `mod.rs` neither
//! re-exports `vX` symbols nor loses its latest-first dispatch note.
//!
//! The guard-marker check reaches every workspace member, not just this crate:
//! the scan root and member set are derived from the workspace manifest so a new
//! member is covered without editing this file.
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

/// Recursively collects Rust source files under `dir`, skipping directories
/// whose name is in `excluded`.
fn collect_rs_files_recursive_excluding(dir: &Path, excluded: &[&str], out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("failed to read source dir '{}': {err}", dir.display()))
    {
        let entry = entry.unwrap_or_else(|err| {
            panic!("failed reading source dir entry in '{}': {err}", dir.display())
        });
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().and_then(|name| name.to_str());
            if name.is_some_and(|name| excluded.contains(&name)) {
                continue;
            }
            collect_rs_files_recursive_excluding(&path, excluded, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Recursively collects Rust source files under `dir`.
fn collect_rs_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    collect_rs_files_recursive_excluding(dir, &[], out);
}

/// Directories never worth scanning: build output and VCS metadata.
const SCAN_EXCLUDED_DIRS: &[&str] = &["target", ".git"];

/// The single non-removable marker literal every `versions/` file must carry in
/// its production portion.
///
/// One marker name for the whole workspace is the decision this test rests on.
/// `//! ## DO NOT REMOVE: versions policy guard` is what all 23 `versions/`
/// files already carry verbatim, and a scan that accepted more than one spelling
/// (bare `##`, `////` block form, a per-crate variant) would let a file satisfy
/// the policy with the wrong marker. A single literal is also what makes the
/// negative control meaningful: delete this exact line anywhere and the suite goes
/// red.
const VERSIONS_POLICY_GUARD_MARKER: &str = "//! ## DO NOT REMOVE: versions policy guard";

/// Locates the workspace root by walking up from this crate's manifest dir.
///
/// The scan root is derived, never configured: the first ancestor whose
/// `Cargo.toml` declares a `[workspace]` table is the root, so the test follows
/// a moved or renamed workspace without edits. Anything else would have to be
/// a hardcoded path that rots.
fn find_workspace_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let manifest = dir.join("Cargo.toml");
        if manifest.is_file()
            && let Ok(text) = fs::read_to_string(&manifest)
            && text.lines().any(|line| line.trim() == "[workspace]")
        {
            return dir;
        }
        assert!(
            dir.pop(),
            "no ancestor of '{}' declares a [workspace] table",
            env!("CARGO_MANIFEST_DIR")
        );
    }
}

/// Extracts the body of the `[table]` TOML table: its lines up to the next
/// table header.
fn toml_table_body<'a>(manifest: &'a str, table: &str) -> &'a str {
    let header = format!("[{table}]");
    let start = manifest
        .lines()
        .position(|line| line.trim() == header)
        .unwrap_or_else(|| panic!("workspace manifest has no `{header}` table"));
    let rest = &manifest[start + 1..];
    let end_line = rest
        .lines()
        .position(|line| {
            let trimmed = line.trim();
            trimmed.starts_with('[') && trimmed.ends_with(']')
        })
        .unwrap_or_else(|| rest.lines().count());
    let byte_end: usize = rest.lines().take(end_line).map(|line| line.len() + 1).sum();
    &rest[..byte_end]
}

/// Parses a TOML array body into its quoted string elements, ignoring `#`
/// comments and supporting escapes.
fn parse_toml_string_array(body: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut chars = body.chars();
    let mut current = String::new();
    let mut in_string = false;

    while let Some(ch) = chars.next() {
        if in_string {
            match ch {
                '"' => {
                    in_string = false;
                    values.push(std::mem::take(&mut current));
                }
                '\\' => {
                    if let Some(escaped) = chars.next() {
                        current.push(escaped);
                    }
                }
                other => current.push(other),
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '#' => {
                for skipped in chars.by_ref() {
                    if skipped == '\n' {
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    assert!(!in_string, "unterminated string in TOML array: {body}");
    values
}

/// Returns the workspace member crate roots declared by the root manifest.
///
/// Two declaration sources are unioned, matching Cargo's own membership rules:
/// the `members` list of the `[workspace]` table, and every `path = "…"` entry
/// anywhere in the manifest (a path dependency inside the workspace directory is
/// automatically a member, whether or not it is repeated in `members`).
///
/// Glob and brace patterns are rejected rather than ignored: a pattern this
/// parser cannot expand would silently under-cover exactly the crates the test
/// exists to protect, and a loud failure is the honest outcome.
///
/// Scope: only the root manifest is read. A crate that becomes a member solely
/// through a *member's* path dependency is not claimed here, so
/// `workspace_member_set_matches_the_filesystem` fails loudly in that case and
/// the derivation gets extended, rather than the scan quietly narrowing.
fn workspace_member_roots(workspace_root: &Path) -> Vec<PathBuf> {
    let manifest_path = workspace_root.join("Cargo.toml");
    let manifest = fs::read_to_string(&manifest_path)
        .unwrap_or_else(|err| panic!("failed reading '{}': {err}", manifest_path.display()));

    let members_body = toml_table_body(&manifest, "workspace");
    let members_start = members_body
        .lines()
        .position(|line| {
            let trimmed = line.trim();
            trimmed.starts_with("members") && trimmed.contains('=')
        })
        .map_or_else(
            || panic!("workspace manifest has no `members` key"),
            |index| {
                let offset: usize =
                    members_body.lines().take(index).map(|line| line.len() + 1).sum();
                &members_body[offset..]
            },
        );
    let array_start = members_start
        .find('[')
        .unwrap_or_else(|| panic!("`members` in '{}' is not an array", manifest_path.display()));
    let array_end = members_start[array_start..].find(']').map_or_else(
        || panic!("`members` array in '{}' is unterminated", manifest_path.display()),
        |offset| array_start + offset,
    );
    let mut declared = parse_toml_string_array(&members_start[array_start + 1..array_end]);

    // Auto-included path dependencies: `name = { path = "…" }` entries.
    for (_, tail) in manifest.match_indices("path") {
        let tail = &tail["path".len()..];
        let Some(tail) = tail.trim_start().strip_prefix('=') else {
            continue;
        };
        if let Some(value) =
            parse_toml_string_array(&format!("\"{}\"", tail.trim())).into_iter().next()
        {
            declared.push(value);
        }
    }

    assert!(!declared.is_empty(), "no workspace members parsed from '{}'", manifest_path.display());

    let mut roots: Vec<PathBuf> = declared
        .iter()
        .map(|entry| {
            assert!(
                !entry.contains(['*', '?', '{']),
                "member pattern '{entry}' in '{}' needs glob support in \
                 workspace_member_roots; refusing to under-cover it silently",
                manifest_path.display()
            );
            workspace_root.join(entry)
        })
        .collect();
    roots.sort();
    roots.dedup();
    roots
}

/// Returns the directories of every `Cargo.toml` under `root`, excluding build
/// output and VCS metadata.
fn discovered_manifest_dirs(root: &Path) -> Vec<PathBuf> {
    let mut manifests = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("failed to read '{}': {err}", dir.display()))
        {
            let path = entry
                .unwrap_or_else(|err| panic!("failed reading entry in '{}': {err}", dir.display()))
                .path();
            if !path.is_dir() {
                continue;
            }
            let name = path.file_name().and_then(|name| name.to_str());
            if name.is_some_and(|name| SCAN_EXCLUDED_DIRS.contains(&name)) {
                continue;
            }
            if path.join("Cargo.toml").is_file() {
                manifests.push(path.clone());
            }
            stack.push(path);
        }
    }
    manifests.sort();
    manifests
}

/// Returns `true` when `path` sits directly inside a directory named `versions`.
fn is_versions_dir_member(path: &Path) -> bool {
    path.parent().and_then(Path::file_name).and_then(|name| name.to_str()) == Some("versions")
}

/// Returns `true` when `path` sits anywhere below a directory named
/// `versions`.
///
/// Structural, not string-matched against a hardcoded crate prefix: a `store`
/// that grows its own `versions/` folder, or a member that moves its ladder
/// from `delta/versions/` to `config/versions/`, is covered without editing
/// this file. A file that merely has `versions` in its own name is not
/// affected, because only a *path component* counts.
fn path_is_inside_versions_dir(path: &Path) -> bool {
    path.components().any(
        |component| matches!(component, std::path::Component::Normal(name) if name == "versions"),
    )
}

/// Returns `true` when `byte` can appear inside a Rust identifier.
fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// Returns `true` when `byte` cannot continue an identifier, so the token
/// ending at `index` is a whole path segment.
fn is_segment_end(bytes: &[u8], index: usize) -> bool {
    !bytes.get(index).copied().is_some_and(is_identifier_byte)
}

/// Returns `Some(count)` when a raw string literal starts at `start`, and
/// `count` is the number of `#` padding its terminator.
///
/// Raw strings carry no escapes, so they are scanned separately from ordinary
/// strings; folding them into the escaped-string case would stop at the first
/// `"` inside the body and then scan the remainder as code.
fn raw_string_padding(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'r') || start > 0 && is_identifier_byte(bytes[start - 1]) {
        return None;
    }
    let mut index = start + 1;
    let mut padding = 0usize;
    while bytes.get(index) == Some(&b'#') {
        padding += 1;
        index += 1;
    }
    (bytes.get(index) == Some(&b'"')).then_some(padding)
}

/// Returns the byte length of the character literal starting at `start`, or
/// `None` when the `'` at `start` opens a lifetime or a block label.
///
/// Getting this wrong in the permissive direction is the one way the
/// code-only projection could hide a real violation: treating `&'a str` as a
/// character literal would consume everything up to the next `'` in the file
/// and scan none of it. Only the three unambiguous literal shapes are claimed.
fn character_literal_len(bytes: &[u8], start: usize) -> Option<usize> {
    match bytes.get(start + 1)? {
        // `'\n'`, `'\\'`, `'\''`: an escape body is exactly two bytes.
        b'\\' => (bytes.get(start + 3) == Some(&b'\'')).then_some(4),
        // `'x'`: a single-ASCII-byte body.
        body if body.is_ascii() => (bytes.get(start + 2) == Some(&b'\'')).then_some(3),
        // A multi-byte scalar cannot be shape-tested byte-wise, so decline
        // and leave the `'` in the code stream. A character literal can never
        // contain a module path, so declining costs no coverage.
        _ => None,
    }
}

/// Projects a Rust source file onto its **code-only** text.
///
/// Every line comment (which covers `//`, `///`, and `//!` in one rule), block
/// comment (nestable, so `/**` and `/*!` are covered too), string literal,
/// raw string, byte string, and character literal is replaced by a space;
/// newlines are copied through so the projection stays line-aligned with the
/// original.
///
/// This is the entire prose-versus-code distinction, and it is deliberately
/// one rule instead of a list of files, spellings, or markers to skip. The
/// versions policy is about *referencing* a version module from Rust code, and
/// text inside a comment or a string literal cannot reference anything. The
/// converse also holds: nothing in code is exempt, so a version path in a
/// `use`, a type annotation, or a fully-qualified expression is a violation
/// wherever it appears in any crate. A skip-list would rot silently the first
/// time a new file restates the rule in prose; this projection cannot.
fn code_only_projection(content: &str) -> String {
    let bytes = content.as_bytes();
    let mut out = String::with_capacity(content.len());
    let mut index = 0usize;

    while index < bytes.len() {
        let byte = bytes[index];

        if byte == b'\n' {
            out.push('\n');
            index += 1;
            continue;
        }

        if let Some(padding) = raw_string_padding(bytes, index) {
            let body_start = index + 1 + padding + 1;
            let mut cursor = body_start;
            while cursor < bytes.len() {
                let terminates = bytes[cursor] == b'"'
                    && (cursor + padding).saturating_add(1) <= bytes.len()
                    && bytes[cursor + 1..cursor + 1 + padding].iter().all(|byte| *byte == b'#');
                if terminates {
                    cursor += 1 + padding;
                    break;
                }
                out.push(if bytes[cursor] == b'\n' { '\n' } else { ' ' });
                cursor += 1;
            }
            for _ in index..cursor {
                out.push(' ');
            }
            index = cursor;
            continue;
        }

        let literal_len = match byte {
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                let end = bytes[index..]
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(bytes.len(), |offset| index + offset);
                Some(end - index)
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                let mut depth = 0usize;
                let mut cursor = index;
                while cursor < bytes.len() {
                    if bytes[cursor] == b'/' && bytes.get(cursor + 1) == Some(&b'*') {
                        depth += 1;
                        cursor += 2;
                        continue;
                    }
                    if bytes[cursor] == b'*' && bytes.get(cursor + 1) == Some(&b'/') {
                        depth -= 1;
                        cursor += 2;
                        if depth == 0 {
                            break;
                        }
                        continue;
                    }
                    cursor += 1;
                }
                Some(cursor - index)
            }
            b'"' => {
                let mut cursor = index + 1;
                while cursor < bytes.len() {
                    if bytes[cursor] == b'\\' {
                        cursor += 2;
                        continue;
                    }
                    if bytes[cursor] == b'"' || bytes[cursor] == b'\n' {
                        break;
                    }
                    cursor += 1;
                }
                Some(cursor - index + usize::from(bytes.get(cursor) == Some(&b'"')))
            }
            b'\'' => character_literal_len(bytes, index),
            _ => None,
        };

        if let Some(len) = literal_len {
            for offset in 0..len {
                out.push(if bytes[index + offset] == b'\n' { '\n' } else { ' ' });
            }
            index += len;
        } else {
            out.push(byte as char);
            index += 1;
        }
    }

    out
}

/// Returns the end offset of a version module segment starting at `start`, or
/// `None` when the segment is not one.
///
/// A version module is `v` plus decimal digits (`v1`, `v2`, `v3`) or `v_`
/// plus a lowercase word (`v_latest`). That shape covers every version module
/// in the workspace and any `_`-suffixed sibling added later, while refusing
/// the unversioned siblings the same directories legitimately contain
/// (`versions::mod`, `versions::merge`, `versions::mod_policy_guard`) and
/// unrelated identifiers that merely begin with `v` (`versions::validate`).
fn version_module_segment_end(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'v') {
        return None;
    }
    let mut index = start + 1;

    let digits = index;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    if index > digits {
        return is_segment_end(bytes, index).then_some(index);
    }

    if bytes.get(index) == Some(&b'_') {
        index += 1;
        let word = index;
        while bytes.get(index).is_some_and(u8::is_ascii_lowercase) {
            index += 1;
        }
        if index > word {
            return is_segment_end(bytes, index).then_some(index);
        }
    }

    None
}

/// Returns every `versions::<version-module>` path appearing in the
/// code-only projection `code`, as `(byte offset, matched text)` pairs.
///
/// All occurrences are reported, not just the first, because the guard exists
/// to hand a reviewer an actionable list; a single reference per file would
/// hide a second leak in the same file until the first one was fixed.
///
/// `versions` must be a whole path segment, so `delta::versions::v1` and
/// `super::versions::v2` are found while `my_versions::v1` is not.
fn code_references_to_version_module(code: &str) -> Vec<(usize, String)> {
    const VERSIONS: &[u8] = b"versions";
    let bytes = code.as_bytes();
    let mut found = Vec::new();
    let mut index = 0usize;

    while index + VERSIONS.len() + 2 <= bytes.len() {
        if bytes[index..].starts_with(VERSIONS)
            && (index == 0 || !is_identifier_byte(bytes[index - 1]))
        {
            let separator = index + VERSIONS.len();
            if bytes[separator..].starts_with(b"::")
                && let Some(end) = version_module_segment_end(bytes, separator + 2)
            {
                found.push((index, code[index..end].to_string()));
            }
        }
        index += 1;
    }

    found
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
/// Ensures non-version files never name a leaked versioned type.
///
/// The companion rule about naming a *version module* is enforced
/// workspace-wide by
/// [`no_file_outside_versions_references_a_version_module_path`]; this test
/// covers the separate rule that a wire type's name must not travel past the
/// boundary at all. The two rules share one definition of "outside a
/// `versions/` directory" ([`path_is_inside_versions_dir`]) so a file cannot be
/// in scope for one rule and out of scope for the other.
fn non_versions_files_never_name_leaked_versioned_types() {
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
            !has_known_versioned_type_leak(&content),
            "{} must not depend on leaked versioned type names; keep non-versions files on unversioned runtime models",
            path.display()
        );
    }
}

#[test]
/// Enforces, across every workspace member, that no file outside a
/// `versions/` directory references a version module path.
///
/// ## Why this test exists
///
/// The rule it covers is `versioning-and-migration.instructions.md`: "Files
/// outside `versions/` interact with versioned symbols through
/// `versions/mod.rs` only; do not import `versions::vX` directly." Until now
/// that rule was enforced only as a `git grep` a reviewer was expected to run.
/// A negative control on the conductor lane demonstrated why that is not
/// enough: seeding one direct `versions::v_latest` reference back into
/// `mediapm-conductor`'s `config/mod.rs` produced a clean build and a fully
/// green suite. Nothing in the toolchain is a backstop, because the `*Latest`
/// family is `pub(crate)` and the re-export that carried it made the path
/// resolvable. A future edit could therefore reopen every leak closed so far
/// and the suite would not notice.
///
/// ## What counts as a violation
///
/// A `versions::<version-module>` path in **code**: `versions::v1`,
/// `versions::v_latest`, `super::versions::v2`, and fully-qualified forms
/// such as `crate::config::versions::v_latest::Type`.
///
/// Prose is not a violation. Doc comments restating the rule are mandatory
/// (see [`VERSIONS_POLICY_GUARD_MARKER`]) and the policy files themselves
/// quote the forbidden syntax, so a matcher that could not tell a description
/// of the rule from an instance of it would either be useless or force a skip
/// list. [`code_only_projection`] is the whole distinction — comments and
/// string literals are blanked, everything else is scanned — so it needs no
/// file list and cannot rot when a new file explains the rule to its reader.
///
/// ## Why it spans the workspace
///
/// The scan root and member set come from [`find_workspace_root`] and
/// [`workspace_member_roots`], the same derivation the policy-guard scan
/// added in `eee523d4` uses, and [`workspace_member_set_matches_the_filesystem`]
/// independently checks that derivation against the filesystem. This test
/// additionally asserts that more than one member was reached, so a scan that
/// quietly degenerated to this crate alone would fail instead of passing
/// vacuously.
fn no_file_outside_versions_references_a_version_module_path() {
    let workspace_root = find_workspace_root();
    let members = workspace_member_roots(&workspace_root);
    let this_crate = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let mut violations = Vec::new();
    let mut scanned: Vec<PathBuf> = Vec::new();
    let mut covered_crates: Vec<PathBuf> = Vec::new();

    for member in &members {
        if !member.is_dir() {
            continue;
        }
        let mut rs_files = Vec::new();
        collect_rs_files_recursive_excluding(member, SCAN_EXCLUDED_DIRS, &mut rs_files);

        let before = scanned.len();
        scanned.extend(rs_files.iter().cloned());
        if scanned.len() > before {
            covered_crates.push(member.clone());
        }

        for path in rs_files {
            if path_is_inside_versions_dir(&path) {
                continue;
            }
            let content = fs::read_to_string(&path).unwrap_or_else(|err| {
                panic!("failed reading source file '{}': {err}", path.display())
            });
            let projection = code_only_projection(&content);
            for (offset, reference) in code_references_to_version_module(&projection) {
                let line = projection[..offset].matches('\n').count() + 1;
                violations.push(format!("{}:{}: {reference}", path.display(), line));
            }
        }
    }

    assert!(
        !scanned.is_empty(),
        "no Rust sources found under workspace root {}",
        workspace_root.display()
    );
    assert!(
        covered_crates.iter().filter(|member| *member != &this_crate).count() >= 2,
        "the version-path scan reached fewer than two members besides this crate \
         ({}); it must cover the whole workspace, not just the crate that \
         defines it. Covered: {covered_crates:#?}",
        this_crate.display()
    );

    assert!(
        violations.is_empty(),
        "these files reference a version module from outside a `versions/` \
         directory, which the versions boundary policy forbids; route through \
         the unversioned entry points in each `versions/mod.rs` instead:\n{}",
        violations.join("\n")
    );
}

#[cfg(test)]
mod code_projection_tests {
    //! Behavioural tests for the code-only projection and the path matcher.
    //!
    //! The workspace-wide test above can only be trusted if these two helpers
    //! behave, and several of their interesting cases cannot be reached from
    //! the real tree because the tree is currently clean: a matcher that
    //! stopped working would make the enforcement vacuous while the suite stayed
    //! green. Each case below names the property it protects.

    use super::{code_only_projection, code_references_to_version_module};

    /// Projects `source` and returns the single reference it should yield, or
    /// `None` when it should yield none.
    fn references(source: &str) -> Option<String> {
        let found = code_references_to_version_module(&code_only_projection(source));
        assert!(found.len() <= 1, "expected at most one reference in: {source}");
        found.into_iter().next().map(|(_, text)| text)
    }

    /// Prose describing the rule must not be reported: a doc comment that
    /// explains *why* a file avoids `versions::v_latest` is the required
    /// documentation style, not a violation.
    #[test]
    fn line_and_doc_comments_are_prose() {
        for source in [
            "// route through versions/mod.rs, never versions::v_latest\nfn f() {}\n",
            "//! A file must not name versions::v1 directly.\nfn f() {}\n",
            "/// Doc: versions::v_latest is forbidden outside versions/.\nfn f() {}\n",
        ] {
            assert_eq!(
                references(source),
                None,
                "comment text must not be scanned as code: {source}"
            );
        }
    }

    /// Block comments, including the doc forms and a nested one, are prose too.
    #[test]
    fn block_comments_are_prose() {
        for source in [
            "/* versions::v_latest */\nfn f() {}\n",
            "/** doc block: versions::v1 */\nfn f() {}\n",
            "/*! inner doc: versions::v_latest */\nfn f() {}\n",
            "/* outer /* inner versions::v1 */ still outer */\nfn f() {}\n",
        ] {
            assert_eq!(
                references(source),
                None,
                "block comment text must not be scanned as code: {source}"
            );
        }
    }

    /// A version path inside a string literal is data, not a reference, so
    /// assertion messages and golden strings must not trip the guard.
    #[test]
    fn string_literals_are_not_references() {
        let source = r#"fn f() { let s = "versions::v_latest must not appear"; }"#;
        assert_eq!(references(source), None);
    }

    /// A raw string may contain unescaped quotes; the projection must not stop
    /// at the first one and start scanning the rest as code.
    #[test]
    fn raw_string_with_quotes_is_one_literal() {
        let source = r##"fn f() { let s = r#"a " b versions::v1"#; }"##;
        assert_eq!(references(source), None);
    }

    /// A lifetime is not a character literal. If it were treated as one, the
    /// projection would consume everything up to the next `'` and the real
    /// violation after it would be invisible — a false negative in the one
    /// place the guard must not have one.
    #[test]
    fn lifetime_is_not_swallowed_as_a_character_literal() {
        let source = "fn f<'a>(x: &'a str) { let _ = crate::config::versions::v_latest::T; }";
        assert_eq!(
            references(source),
            Some("versions::v_latest".to_string()),
            "a real path after a lifetime must still be found"
        );
    }

    /// Character literals in the three unambiguous shapes are blanked, and
    /// they never hide a path because a char literal cannot hold one.
    #[test]
    fn character_literals_are_blanked() {
        for source in [
            "fn f() { let c = 'v'; }",
            "fn f() { let c = '\\n'; }",
            "fn f() { let c = '\\''; }",
            "fn f() { match c { 'a' => 1, _ => 2 }; }",
        ] {
            assert_eq!(
                references(source),
                None,
                "character literal must not be scanned as code: {source}"
            );
        }
    }

    /// Every spelling of a real violation in code is found, including the
    /// `super::` and fully-qualified forms the policy names.
    #[test]
    fn real_code_references_are_found() {
        for (source, expected) in [
            ("use crate::delta::versions::v1::Thing;", "versions::v1"),
            ("use super::versions::v2::Thing;", "versions::v2"),
            ("pub use versions::v_latest::{A, B};", "versions::v_latest"),
            ("let x: crate::config::versions::v_latest::T = q;", "versions::v_latest"),
        ] {
            assert_eq!(
                references(source).as_deref(),
                Some(expected),
                "expected `{expected}` to be reported in: {source}"
            );
        }
    }

    /// The matcher must not over-reach onto the unversioned siblings a
    /// `versions/` directory legitimately contains, or onto identifiers that
    /// merely start with `v`.
    #[test]
    fn unversioned_siblings_and_prefixes_are_not_reported() {
        for source in [
            "use crate::config::versions::mod;",
            "use crate::config::versions::merge::SourceDocument;",
            "use crate::config::versions::mod_policy_guard::Thing;",
            "use crate::config::versions::validate_v1_document;",
            "use my_versions::v1::Thing;",
            "use crate::config::versions::v1beta::Thing;",
        ] {
            assert_eq!(references(source), None, "must not be reported: {source}");
        }
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
/// Keeps the derived member set honest: every `Cargo.toml` on disk is a member,
/// and every declared member exists.
///
/// This is the anti-rot guard for the scan below. A hardcoded crate list would
/// silently drop a newly added crate from `every_versions_dir_keeps_policy_guard_docstring`,
/// which is the failure mode that let nine `versions/` files go unmarked; deriving
/// the set from the workspace manifest only helps if the derivation is itself
/// checked against the filesystem.
fn workspace_member_set_matches_the_filesystem() {
    let workspace_root = find_workspace_root();
    let members = workspace_member_roots(&workspace_root);

    for member in &members {
        assert!(
            member.join("Cargo.toml").is_file(),
            "member '{}' declared in '{}' has no Cargo.toml",
            member.display(),
            workspace_root.join("Cargo.toml").display()
        );
    }

    let on_disk = discovered_manifest_dirs(&workspace_root);
    let unclaimed: Vec<_> = on_disk.iter().filter(|dir| !members.contains(dir)).collect();
    assert!(
        unclaimed.is_empty(),
        "these crates sit inside the workspace but are not covered by \
         workspace_member_roots, so their versions/ files would escape the \
         policy-guard scan: {unclaimed:#?}"
    );
}

#[test]
/// Enforces the non-removable versions policy guard in every `versions/`
/// directory of every workspace member.
///
/// `versioned_files_keep_policy_guard_and_boundary_rules` pins the guard for
/// `src/delta/versions/` only, and the previous revision of this test reached no
/// further than `mediapm-cas/src/`, leaving all nine `versions/` files in
/// `mediapm` and `mediapm-conductor` unenforced: a marker could be deleted from
/// any of them with the suite green.
///
/// Two decisions make the wider scan trustworthy:
///
/// - **Scan root**: derived, not configured. [`find_workspace_root`] walks up to
///   the nearest `[workspace]` manifest and [`workspace_member_roots`] reads that
///   manifest's `members` list plus its path dependencies, so a new member is
///   covered the moment it is declared and nothing can rot silently.
///   `workspace_member_set_matches_the_filesystem` fails if a crate appears on
///   disk that the derivation does not claim.
/// - **Marker name**: one literal, [`VERSIONS_POLICY_GUARD_MARKER`], already
///   carried verbatim by every `versions/` file in the workspace. Accepting more
///   than one spelling would let a file satisfy the policy with the wrong marker
///   and would make the negative control ambiguous.
///
/// Coverage is cross-checked by a second, independent walk of the whole workspace
/// root: any `versions/` file the member-derived scan missed fails here, so a
/// derivation bug cannot masquerade as full coverage.
fn every_versions_dir_keeps_policy_guard_docstring() {
    let workspace_root = find_workspace_root();
    let members = workspace_member_roots(&workspace_root);
    let this_crate = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let mut guarded = Vec::new();
    let mut covered_crates: Vec<PathBuf> = Vec::new();
    for member in &members {
        let src_dir = member.join("src");
        if !src_dir.is_dir() {
            continue;
        }
        let mut rs_files = Vec::new();
        collect_rs_files_recursive(&src_dir, &mut rs_files);

        let before = guarded.len();
        guarded.extend(rs_files.into_iter().filter(|path| is_versions_dir_member(path)));
        if guarded.len() > before {
            covered_crates.push(member.clone());
        }
    }

    assert!(
        !guarded.is_empty(),
        "no versions/ files found under workspace root {}",
        workspace_root.display()
    );
    // A scan that silently degenerated to this crate alone would leave the nine
    // `mediapm` / `mediapm-conductor` files unchecked, which is exactly the
    // regression this test was widened to prevent.
    assert!(
        covered_crates.iter().any(|member| member != &this_crate),
        "the policy-guard scan covered only this crate ({}); it must reach every \
         workspace member",
        this_crate.display()
    );

    // Independent whole-root walk: nothing under any `versions/` directory may
    // escape the member-derived set.
    let mut root_walk = Vec::new();
    collect_rs_files_recursive_excluding(&workspace_root, SCAN_EXCLUDED_DIRS, &mut root_walk);
    let uncovered: Vec<_> = root_walk
        .into_iter()
        .filter(|path| is_versions_dir_member(path) && !guarded.contains(path))
        .collect();
    assert!(
        uncovered.is_empty(),
        "these versions/ files are inside the workspace but outside every \
         scanned member: {uncovered:#?}"
    );

    let mut missing = Vec::new();
    for path in guarded {
        let content = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed reading '{}': {err}", path.display()));
        // Search the production portion only: this test lives inside
        // `delta/versions/`, so scanning a whole file that restates the marker
        // would let the asserted literal satisfy itself and the guard could
        // never fail. A file with no `#[cfg(test)]` module is checked in full.
        let production = content
            .split_once("#[cfg(test)]")
            .map_or(content.as_str(), |(production, _tests)| production);
        if !production.contains(VERSIONS_POLICY_GUARD_MARKER) {
            missing.push(path);
        }
    }
    assert!(
        missing.is_empty(),
        "these files must include the non-removable versions policy guard \
         docstring ('{VERSIONS_POLICY_GUARD_MARKER}'): {missing:#?}"
    );
}
