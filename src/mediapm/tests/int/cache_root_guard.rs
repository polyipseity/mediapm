//! A scan guard for the default-user cache root rule (spec S4).
//!
//! No test under `src/mediapm/tests/` may resolve the default user download
//! cache root, and an example may reach it only through its own
//! `example_cache_root` unset-env branch — the manual `cargo run --example`
//! path that deliberately shares the real cache with regular syncs. Before
//! this guard the rule was doc-only: a future test could reopen the default
//! root with nothing red (slice-28 final review, F-M5). This file walks the
//! package's `tests/` and `examples/` sources and fails on a reference
//! outside [`ALLOWED`], where each exception sits beside the reason it earns.

use std::io;
use std::path::{Path, PathBuf};

/// Source trees this guard walks, relative to the package root.
///
/// `tests/` is the "never" half of the rule and `examples/` the "only in
/// `example_cache_root`" half; both are scanned because a reference in
/// either can put a test process on the real user cache.
const SCAN_ROOTS: &[&str] = &["tests", "examples"];

/// References that resolve the default user cache root, in either spelling
/// the site may use: the conductor's accessor and the example-isolation
/// helper that resolves it when `MEDIAPM_EXAMPLE_CACHE_ROOT` is unset.
const BANNED_TOKENS: &[&str] =
    &["default_mediapm_user_download_cache_root", "user_level_cache_root"];

/// This file's own path relative to the package root, skipped because it
/// names both tokens as its scan patterns and cannot scan its own literals
/// without reporting them.
const SELF: &str = "tests/int/cache_root_guard.rs";

/// Exceptions as (file relative to the package root, trimmed line), so the
/// rule and its exceptions read together in one place.
///
/// The four examples resolve the real cache only inside their
/// `example_cache_root` function, whose unset-env branch is the documented
/// Level-3 manual-run path; their embedded tests set
/// `MEDIAPM_EXAMPLE_CACHE_ROOT` before any of this runs (see
/// `example-temp-isolation.instructions.md`). `tool_sync/basics.rs` reads
/// the real cache's `tools.json` modification time to prove the override
/// kept the sync away from it — a stat of a path on disk, never the cache
/// opened as a cache (the S4 isolation regression guard).
const ALLOWED: &[(&str, &str)] = &[
    ("examples/mediapm_demo.rs", "example_isolation::user_level_cache_root()"),
    ("examples/mediapm_cli_add_tools.rs", "example_isolation::user_level_cache_root()"),
    ("examples/mediapm_cli_add_hierarchy.rs", "example_isolation::user_level_cache_root()"),
    ("examples/mediapm_demo_online.rs", "example_isolation::user_level_cache_root()"),
    (
        "tests/int/tool_sync/basics.rs",
        "mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root()",
    ),
];

/// A test may not resolve the default user download cache root as its own
/// cache, and an example may do so only through the allowlisted
/// `example_cache_root` lines.
///
/// Guarantees spec S4's regression side: every future reference in the two
/// scanned trees either lands in [`ALLOWED`] with its reason or fails the
/// suite here, so the rule cannot go quiet again while the code moves. The
/// test reads sources only — it opens no cache and writes no file.
#[test]
fn no_test_or_example_resolves_the_default_user_cache_root() {
    let package_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let violations = violations(package_root)
        .expect("the package's tests and examples trees must be readable for the scan");
    assert!(
        violations.is_empty(),
        "the default-user-cache-root rule (spec S4) forbids these references; move the site \
         behind an ALLOWED entry that states why it does not open the real cache:\n{}",
        violations.join("\n")
    );
}

/// Collects every banned-token reference outside [`ALLOWED`] as
/// `path:line: text`, one entry per offending line.
///
/// Reads sources only; the error path means a tree could not be read, which
/// fails the scan rather than passing it on missing data.
fn violations(package_root: &Path) -> Result<Vec<String>, io::Error> {
    let mut files = Vec::new();
    for scan_root in SCAN_ROOTS {
        collect_rs_files(&package_root.join(scan_root), &mut files)?;
    }

    let mut found = Vec::new();
    for path in &files {
        scan_file(package_root, path, &mut found)?;
    }
    Ok(found)
}

/// Collects every `.rs` file under `dir`, following directory entries but
/// never symlinks, the way the tempdir gate walks its roots.
fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), io::Error> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_rs_files(&path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    Ok(())
}

/// Scans one file's lines for a banned token, skipping [`SELF`] and the
/// [`ALLOWED`] (file, line) pairs and pushing a `path:line: text` entry for
/// every reference left.
fn scan_file(package_root: &Path, path: &Path, found: &mut Vec<String>) -> Result<(), io::Error> {
    let relative = path.strip_prefix(package_root).map_err(|error| {
        io::Error::other(format!(
            "{} is not under the package root {}/: {error}",
            path.display(),
            package_root.display()
        ))
    })?;
    // Allowlist keys are written with `/` on every platform, so the
    // comparison form is normalized once per file rather than per line.
    let relative = relative.to_string_lossy().replace('\\', "/");
    let source = std::fs::read_to_string(path)?;
    for (index, line) in source.lines().enumerate() {
        let trimmed = line.trim();
        if !BANNED_TOKENS.iter().any(|token| trimmed.contains(token)) {
            continue;
        }
        if relative == SELF {
            continue;
        }
        if ALLOWED.iter().any(|(allowed_path, allowed_line)| {
            *allowed_path == relative && *allowed_line == trimmed
        }) {
            continue;
        }
        found.push(format!("{relative}:{}: {trimmed}", index + 1));
    }
    Ok(())
}
