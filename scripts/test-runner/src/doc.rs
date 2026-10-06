//! The `doc` subcommand: doctests, then the rustdoc link gate.
//!
//! Doctests and the rustdoc gate were separate concerns before this crate
//! existed, which is why prek ran a `test docs` hook and a `rustdoc` hook
//! while CI ran the same work once inside the shell runner and once again
//! in its build step. They are one concern here: the link gate only means
//! something if the docs it reads are the ones under test.
//!
//! nextest cannot execute doctests at all, which is the reason this step
//! exists as something separate from `test` rather than as a flag on it.

use std::ffi::OsString;
use std::path::Path;

use anyhow::{Context, Result};

use crate::cargo;
use crate::cli::Selection;

/// This crate's manifest directory, the anchor for every child cargo run.
///
/// Cargo walks up to the workspace root from here regardless of the
/// process working directory, so the runner never has to resolve a path
/// through git and works in a checkout without a `.git` directory.
const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");

/// Runs the doctests for `selection`, then the rustdoc link gate.
///
/// A non-zero doctest exit returns immediately rather than falling through
/// to the link gate: the gate reads the documentation of the very build
/// that just failed, so running it after a rejected build would report
/// against a tree whose docs were never the ones under test.
pub fn run(selection: &Selection) -> Result<i32> {
    let status = std::process::Command::new(cargo::cargo_binary())
        .current_dir(Path::new(MANIFEST_DIR))
        .args(doctest_argv(selection))
        .status()
        .context("run the doctests")?;
    let code = cargo::status_code(status, "the doctest run")?;
    if code != 0 {
        return Ok(code);
    }

    let status = std::process::Command::new(cargo::cargo_binary())
        .current_dir(Path::new(MANIFEST_DIR))
        .args(rustdoc_argv())
        .status()
        .context("run the rustdoc link gate")?;
    cargo::status_code(status, "the rustdoc link gate")
}

/// The argv for the doctest run.
///
/// `--workspace` rather than a package list, because a doctest is a
/// property of the documentation that shipped rather than of one target's
/// build, and a package list is a set someone has to keep updated. A `-p`
/// selection is still appended so the same flags narrow this subcommand
/// as they narrow `test`.
pub fn doctest_argv(selection: &Selection) -> Vec<OsString> {
    let mut argv =
        vec![OsString::from("test"), OsString::from("--doc"), OsString::from("--workspace")];
    if selection.wants_lock() {
        argv.push(OsString::from("--locked"));
    }
    argv.extend(selection.feature_flags());
    argv.extend(selection.package_flags());
    argv
}

/// The argv for the rustdoc link gate.
///
/// `--document-private-items` documents the private doc comments that hold
/// most cross-references in this workspace, and `--all-features` stops an
/// off-by-default module from hiding behind its feature flag. Without
/// both, a broken link here would pass.
///
/// The selection does not reach this step on purpose. It is the one
/// invocation whose whole value is being unconditional: a link gate
/// narrowable to a package or a feature set is a link gate that can be
/// pointed away from the module holding the broken link, so a caller who
/// selects one package for the doctests still gets the whole workspace
/// checked here. That is also why `--locked` is unconditional, for the
/// same reason `--all-features` is.
pub fn rustdoc_argv() -> Vec<OsString> {
    vec![
        OsString::from("doc"),
        OsString::from("--locked"),
        OsString::from("--no-deps"),
        OsString::from("--workspace"),
        OsString::from("--all-features"),
        OsString::from("--document-private-items"),
    ]
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use clap::Parser;

    use super::{doctest_argv, rustdoc_argv};
    use crate::cli::{Cli, Command};

    fn selection(options: &[&str]) -> crate::cli::Selection {
        let mut argv = vec!["test-runner", "doc"];
        argv.extend_from_slice(options);
        let Command::Doc(sel) = Cli::parse_from(argv).command else {
            panic!("expected the doc subcommand");
        };
        sel
    }

    #[test]
    fn doctest_argv_covers_the_workspace_and_locks() {
        assert_eq!(
            doctest_argv(&selection(&["--all-features"])),
            vec![
                OsString::from("test"),
                OsString::from("--doc"),
                OsString::from("--workspace"),
                OsString::from("--locked"),
                OsString::from("--all-features"),
            ]
        );
    }

    #[test]
    fn doctest_argv_omits_locked_when_opted_out() {
        let got = doctest_argv(&selection(&["--no-locked"]));
        assert!(!got.iter().any(|a| a == "--locked"));
    }

    #[test]
    fn rustdoc_argv_documents_private_items() {
        assert_eq!(
            rustdoc_argv(),
            vec![
                OsString::from("doc"),
                OsString::from("--locked"),
                OsString::from("--no-deps"),
                OsString::from("--workspace"),
                OsString::from("--all-features"),
                OsString::from("--document-private-items"),
            ]
        );
    }
}
