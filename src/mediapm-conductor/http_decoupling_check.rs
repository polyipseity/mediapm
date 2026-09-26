// Single definition of the `src/http/` decoupling invariant.
//
// This file is **not** a Rust module and is never compiled as one. Nothing
// declares `mod http_decoupling_check;`. It is pulled into two independent
// compilation units with `include!`:
//
// - `build.rs`, where a violation aborts the build with `cargo:error=` lines, and
// - `tests/int/http_decoupling.rs`, where a violation fails a named test.
//
// Both consumers call `scan_http_decoupling_violations` below, so the guard
// and the test that guards the guard cannot drift into disagreeing about what
// the rule is.
//
// The invariant: for every `*.rs` file directly in `src/http/`, no scanned
// line contains `use crate::` and no scanned line contains `ConductorError`.
// Blank lines, line comments (`//`, `///`, `//!`) and attribute lines (`#[`)
// are excluded from the scan.
//
// The comments here are plain `//`, not `//!`, because `include!` splices this
// file into the middle of another file and E0753 forbids inner doc comments
// there.

/// Reported when a scanned line contains a `use crate::` import.
const REASON_CRATE_IMPORT: &str = "`use crate::` is forbidden in the HTTP module";

/// Reported when a scanned line names `ConductorError`.
const REASON_CONDUCTOR_ERROR: &str =
    "`ConductorError` is forbidden in the HTTP module; use `HttpClientError` instead";

/// A single line of an `src/http/*.rs` file that breaks the decoupling rule.
///
/// `Ord` is derived so that a `Vec` of violations sorts into a stable
/// path-then-line order. `std::fs::read_dir` yields directory entries in an
/// unspecified order, and both consumers need diagnostics that do not
/// reshuffle between runs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DecouplingViolation {
    /// The offending file, exactly as reached from the directory passed to
    /// [`scan_http_decoupling_violations`].
    pub path: String,
    /// 1-based line number within `path`.
    pub line: usize,
    /// Why this line is a violation.
    pub reason: &'static str,
}

/// Scans every `*.rs` file directly in `http_dir` and returns every decoupling
/// violation, sorted by path and then by line.
///
/// Non-`.rs` directory entries are skipped, as are the line kinds the rule
/// excludes: blank lines, lines whose trimmed form starts with `//` (covering
/// `//`, `///` and `//!`), and lines whose trimmed form starts with `#[`.
///
/// The scan is **textual**: it reports the forbidden substring wherever it
/// appears on a scanned line, regardless of what that text resolves to. That is
/// deliberate — the rule guards an architectural property of a directory that
/// must stay copy-paste extractable, so a bare mention is treated as a risk
/// even when the mention happens to be inert.
///
/// # Panics
///
/// Panics if `http_dir` cannot be read as a directory, or if a `.rs` file
/// inside it cannot be read as UTF-8. A build script cannot meaningfully
/// continue past an unreadable source tree, and a test must not silently pass
/// over one.
#[must_use]
pub fn scan_http_decoupling_violations(http_dir: &std::path::Path) -> Vec<DecouplingViolation> {
    let mut violations = Vec::new();
    for entry in std::fs::read_dir(http_dir)
        .unwrap_or_else(|err| panic!("cannot read HTTP module dir {}: {err}", http_dir.display()))
    {
        let path = entry
            .unwrap_or_else(|err| panic!("cannot read entry in {}: {err}", http_dir.display()))
            .path();
        if path.extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("cannot read {} as UTF-8: {err}", path.display()));
        let file = path.to_string_lossy().into_owned();
        for (index, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("#[") {
                continue;
            }
            if trimmed.contains("use crate::") {
                violations.push(DecouplingViolation {
                    path: file.clone(),
                    line: index + 1,
                    reason: REASON_CRATE_IMPORT,
                });
            }
            if trimmed.contains("ConductorError") {
                violations.push(DecouplingViolation {
                    path: file.clone(),
                    line: index + 1,
                    reason: REASON_CONDUCTOR_ERROR,
                });
            }
        }
    }
    violations.sort();
    violations
}
