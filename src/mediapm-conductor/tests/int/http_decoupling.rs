//! Guard test for the `src/http/` decoupling invariant.
//!
//! `build.rs` already fails the build when a file under `src/http/` gains a
//! `crate::` import or names `ConductorError`. That check is preventive, but
//! it only speaks as a build abort: a violation is reported as `cargo:error=`
//! noise during compilation, with no test name, no assertion, and nothing that
//! shows up in a test report or a CI test summary.
//!
//! This module asserts the same rule as a named test so a violation surfaces as
//! a readable failure that lists **every** offending file and line. The build
//! guard stays: it prevents, this diagnoses.
//!
//! The rule itself is not restated here. It is included from
//! `http_decoupling_check.rs`, the same file `build.rs` includes, so the two
//! consumers cannot disagree about what the rule is.

use std::path::{Path, PathBuf};

// `include!` is resolved relative to this file, so `../../` is the crate root.
include!("../../http_decoupling_check.rs");

/// Absolute path to the `src/http/` directory, resolved from the crate root at
/// compile time rather than from the process working directory.
///
/// A test that depends on the runner's working directory passes locally and
/// fails under a different runner, which is worse than having no test at all.
fn http_module_dir() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("http")
}

/// Renders violations as one `path:line: reason` line per entry.
///
/// Reports every violation rather than stopping at the first, matching what
/// `build.rs` accumulates before its single `assert!`. A test that reported
/// only the first hit would be less useful than the build abort it supplements.
fn render_violations(violations: &[DecouplingViolation]) -> String {
    violations
        .iter()
        .map(|violation| {
            let path = violation.path.as_str();
            let line = violation.line;
            let reason = violation.reason;
            format!("  {path}:{line}: {reason}")
        })
        .collect::<Vec<String>>()
        .join("\n")
}

/// Writes `contents` to `dir/name`, creating `dir` if needed.
///
/// Used only by the scanner-behaviour tests below, which need a directory the
/// build guard does not also scan.
fn write_probe(dir: &Path, name: &str, contents: &str) {
    std::fs::create_dir_all(dir).expect("create probe dir");
    std::fs::write(dir.join(name), contents).expect("write probe file");
}

/// Creates an owned scratch directory for scanner-behaviour tests.
///
/// Returns the [`tempfile::TempDir`] **owner**, not its path. Returning the
/// path would drop the owner at the end of this statement, deleting the
/// directory, and the caller would recreate it with `create_dir_all` and leave
/// it with no cleanup owner — a `mediapm-artifact-*` tree orphaned in `$TMPDIR`
/// on every run, which `scripts/run-all-tests.sh` fails the suite for.
///
/// The real `src/http/` tree cannot be used instead: the build guard in
/// `build.rs` scans the same tree and panics during compilation, so the test
/// binary never gets built and a violation there is reported as a *build*
/// failure rather than a test failure. A synthetic directory exercises the same
/// shared scanner with the guard out of the way.
fn probe_dir() -> tempfile::TempDir {
    mediapm_utils::temp::artifact_dir().expect("probe artifact dir")
}

/// The scanner must detect both forbidden patterns, name the file and line, and
/// report every violation rather than stopping at the first.
///
/// This is the red case for the real-tree test below, made observable: the
/// detector is proven to fire on both conditions, and to accumulate both
/// findings in one pass.
#[test]
fn scan_reports_both_forbidden_patterns_with_file_and_line() {
    let dir = probe_dir();
    write_probe(
        dir.path(),
        "a_crate_import.rs",
        "// header\nuse crate::http::client::shared_http_client;\n",
    );
    write_probe(
        dir.path(),
        "b_error_type.rs",
        "fn probe() -> u8 {\n    let _ = ConductorError;\n    0\n}\n",
    );

    let violations = scan_http_decoupling_violations(dir.path());
    let rendered = render_violations(&violations);

    assert_eq!(
        rendered,
        format!(
            "  {}:2: `use crate::` is forbidden in the HTTP module\n  {}:2: `ConductorError` is \
             forbidden in the HTTP module; use `HttpClientError` instead",
            dir.path().join("a_crate_import.rs").display(),
            dir.path().join("b_error_type.rs").display()
        ),
        "both conditions must be reported, sorted by path, with file and line"
    );
}

/// The rule classifies on `line.trim()`, so leading whitespace must change
/// nothing: an indented comment is still a comment, and an indented statement is
/// still a statement.
///
/// The real `src/http/` tree cannot exercise either direction. It contains no
/// scanned violations, so it cannot show that a real one is caught; and it
/// *does* contain `//`, `///` and `//!` lines mentioning `ConductorError` in its
/// own prose — `client.rs` and `mod.rs` both document the contract by saying the
/// module uses `HttpClientError` rather than `ConductorError` — so the real tree
/// only passes at all because the comment exclusion is load-bearing. Dropping
/// that exclusion makes the build guard reject the real tree, which is exactly
/// why the real tree cannot be the fixture for this test.
#[test]
fn scan_trims_lines_and_excludes_comments_but_catches_indented_code() {
    let dir = probe_dir();
    // Line 7 is the only violation; lines 1-6 are all excluded forms.
    let fixture = [
        "// use crate::foo",
        "/// names ConductorError in prose",
        "//! also mentions ConductorError",
        "",
        "#[cfg(feature = \"never\")]",
        "    // ConductorError, indented comment",
        "    use crate::x;",
    ]
    .join("\n");
    write_probe(dir.path(), "mentions.rs", &fixture);

    let violations = scan_http_decoupling_violations(dir.path());
    let rendered = render_violations(&violations);

    assert_eq!(
        rendered,
        format!(
            "  {}:7: `use crate::` is forbidden in the HTTP module",
            dir.path().join("mentions.rs").display()
        ),
        "only the indented statement on line 7 is a violation: every comment, blank and attribute \
         line is excluded, including the indented comment on line 6"
    );
}

/// The `src/http/` module must stay free of crate-internal references so it
/// remains extractable into a standalone crate with no code changes.
///
/// Asserting this as a test is what turns a build abort into a named,
/// diagnosable failure. On the current tree the module is clean of *scanned*
/// violations, so this test passes with no output; the red case is demonstrated
/// in `.superpowers/sdd/phase3-plan-2026-09-26-open-items/r3-report.md`.
#[test]
fn http_module_has_no_crate_internal_references() {
    let violations = scan_http_decoupling_violations(&http_module_dir());
    assert!(
        violations.is_empty(),
        "src/http/ must stay extractable to a standalone crate, but {} line(s) break the rule:\n{}",
        violations.len(),
        render_violations(&violations)
    );
}
