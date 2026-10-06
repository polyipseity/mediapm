//! Unprefixed-tempdir invariant gate.
//!
//! Every mediapm-owned temp directory carries the `mediapm-` prefix so an
//! orphan is identifiable and the janitor can reclaim it. The role helpers
//! in `src/mediapm-utils/src/temp.rs` are the only place allowed to build
//! one, and this gate is what keeps that true.

use std::path::Path;

use anyhow::{Context, Result, bail};

/// The single file permitted to construct a temp directory.
const ALLOWED: &str = "src/mediapm-utils/src/temp.rs";

/// The two patterns that build a temp directory outside the role helpers.
const FORBIDDEN: [&str; 2] = ["tempfile::tempdir(", ".prefix("];

/// Roots the walk covers.
const SCAN_ROOTS: [&str; 2] = ["src", "tests"];

/// Fails when a temp directory is built outside the role helpers.
///
/// Each violating file is printed with its path, then one summary line is
/// returned as the error so the caller owns the `error: ` prefix.
///
/// # Errors
///
/// Returns an error listing the offending `file:line` pairs when any file
/// outside [`ALLOWED`] matches one of [`FORBIDDEN`], and an I/O error when
/// the tree cannot be walked or a file cannot be read. A `.rs` file that is
/// not UTF-8 aborts the whole gate rather than being skipped, because
/// `read_to_string` fails on it; the `grep -r` this gate replaces skipped
/// binary files silently, so the port is stricter here on purpose and a
/// non-UTF-8 source file has to be fixed rather than ignored.
pub fn enforce(root: &Path) -> Result<()> {
    let violations = violations_in(root)?;
    if violations.is_empty() {
        return Ok(());
    }
    for violation in &violations {
        eprintln!("{violation}");
    }
    bail!("unprefixed tempdir/prefix use outside {ALLOWED}");
}

/// Every `file:line` in the tree that builds a temp directory illegally.
fn violations_in(root: &Path) -> Result<Vec<String>> {
    let allowed = root.join(ALLOWED);
    let mut found = Vec::new();
    for dir in SCAN_ROOTS {
        let start = root.join(dir);
        if start.is_dir() {
            walk(root, &start, &allowed, &mut found)?;
        }
    }
    Ok(found)
}

/// Recurses into `dir`, recording every `*.rs` file that matches.
///
/// `root` is the workspace root, kept so a violation prints the same
/// repo-relative path a reader would type, not an absolute one.
///
/// Directory entries are classified by
/// [`std::fs::DirEntry::file_type`], which does not follow symlinks,
/// rather than by `Path::is_dir`, which does. That is
/// the `grep -r` semantics this gate ports: `grep -r` does not descend
/// through a symlink to a directory, so a link loop under `src/` or
/// `tests/` must terminate here too instead of recursing to a stack
/// overflow.
fn walk(root: &Path, dir: &Path, allowed: &Path, found: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry.with_context(|| format!("read entry in {}", dir.display()))?;
        let path = entry.path();
        let file_type =
            entry.file_type().with_context(|| format!("read the type of {}", path.display()))?;
        if file_type.is_dir() {
            walk(root, &path, allowed, found)?;
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") || path == allowed {
            continue;
        }
        let text =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        for (number, line) in text.lines().enumerate() {
            if FORBIDDEN.iter().any(|needle| line.contains(needle)) {
                let relative = path.strip_prefix(root).unwrap_or(&path).display();
                found.push(format!("{relative}:{}: {line}", number + 1));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{enforce, violations_in};
    use crate::gates::test_scratch::scratch;

    /// Guards the single allowed site: `temp.rs` may build temp
    /// directories by any means it likes, because it is where the prefix
    /// contract is defined and every other caller routes through it.
    #[test]
    fn temp_rs_is_the_only_allowed_site() {
        let root = scratch("allowed");
        fs::create_dir_all(root.join("src/mediapm-utils/src")).expect("mkdir");
        fs::write(
            root.join("src/mediapm-utils/src/temp.rs"),
            "let _ = tempfile::Builder::new().prefix(\"mediapm-\");\nlet _ = tempfile::tempdir();\n",
        )
        .expect("write");
        assert!(violations_in(&root).expect("scan").is_empty());
    }

    /// A bare `tempfile::tempdir()` anywhere else builds a directory the
    /// janitor's `mediapm-*` glob cannot match, so it would survive
    /// cleanup while looking like a leftover. It has to be reported.
    #[test]
    fn a_bare_tempdir_elsewhere_is_reported() {
        let root = scratch("bare");
        fs::create_dir_all(root.join("src/mediapm/src")).expect("mkdir");
        fs::write(root.join("src/mediapm/src/lib.rs"), "fn f() { let _ = tempfile::tempdir(); }\n")
            .expect("write");
        assert_eq!(violations_in(&root).expect("scan").len(), 1);
    }

    /// `.prefix(` is the other half of the same escape: a caller can pick
    /// its own prefix, which is neither the managed one nor checked, so an
    /// unprefixed-looking directory is the best it can do.
    #[test]
    fn a_prefix_call_elsewhere_is_reported() {
        let root = scratch("prefix");
        fs::create_dir_all(root.join("tests/src")).expect("mkdir");
        fs::write(
            root.join("tests/src/mod.rs"),
            "fn f() { let _ = tempfile::Builder::new().prefix(\"x\"); }\n",
        )
        .expect("write");
        assert_eq!(violations_in(&root).expect("scan").len(), 1);
    }

    /// A tree with neither construct must pass, or the gate would reject
    /// every file in the workspace.
    #[test]
    fn a_clean_tree_has_no_violations() {
        let root = scratch("clean");
        fs::create_dir_all(root.join("src/mediapm/src")).expect("mkdir");
        fs::write(root.join("src/mediapm/src/lib.rs"), "fn f() {}\n").expect("write");
        assert!(violations_in(&root).expect("scan").is_empty());
    }

    /// The gate's own verdict, not just its scan: a tree with a bare
    /// tempdir has to fail with the message the caller prints after the
    /// `error: ` prefix. The offending `file:line` goes to stderr
    /// separately, so this asserts the summary, not the echo.
    #[test]
    fn enforce_rejects_a_violating_tree() {
        let root = scratch("enforce-violating");
        fs::create_dir_all(root.join("src/mediapm/src")).expect("mkdir");
        fs::write(root.join("src/mediapm/src/lib.rs"), "fn f() { let _ = tempfile::tempdir(); }\n")
            .expect("write");
        let err = enforce(&root).expect_err("a bare tempdir must fail the gate");
        assert_eq!(
            err.to_string(),
            "unprefixed tempdir/prefix use outside src/mediapm-utils/src/temp.rs"
        );
    }

    /// The passing path of the same verdict, so a gate that rejected every
    /// tree would fail here rather than at the end of a real suite.
    #[test]
    fn enforce_accepts_a_clean_tree() {
        let root = scratch("enforce-clean");
        fs::create_dir_all(root.join("src/mediapm/src")).expect("mkdir");
        fs::write(root.join("src/mediapm/src/lib.rs"), "fn f() {}\n").expect("write");
        enforce(&root).expect("a clean tree must pass the gate");
    }
}
