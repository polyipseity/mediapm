//! The nextest invocation.
//!
//! Nextest is reached through the `cargo-bin` crate rather than as an
//! ambient `cargo-nextest` on `PATH`. `cargo run --package cargo-bin --
//! cargo-nextest` is the route the `cargo-run-bin` alias installs on first
//! use, so the binary the suite runs is the one this workspace pinned.
//! Before this runner existed, `run-all-tests.sh` called `cargo nextest`
//! directly while nothing in `.github/` or `rust-toolchain.toml`
//! provisioned the binary, so CI succeeded only because the machine
//! happened to carry it.

use std::ffi::OsString;

use crate::cli::Selection;

/// The argv that runs the nextest suite for `selection`.
///
/// Nextest is reached as `cargo run --package cargo-bin -- cargo-nextest`,
/// which is the route `cargo-run-bin` installs through on first use. The
/// old shell runner called `cargo nextest` directly while nothing in
/// `.github/` or `rust-toolchain.toml` provisioned the binary, so CI
/// depended on it being ambient.
///
/// Both levels carry `--locked`: the outer `cargo run` guards the
/// `cargo-bin` build, the inner nextest run guards the workspace.
pub fn argv(selection: &Selection) -> Vec<OsString> {
    let mut argv: Vec<OsString> = vec![
        OsString::from("run"),
        OsString::from("--quiet"),
        OsString::from("--package"),
        OsString::from("cargo-bin"),
        OsString::from("--"),
        OsString::from("cargo-nextest"),
        OsString::from("run"),
    ];
    if selection.wants_lock() {
        argv.push(OsString::from("--locked"));
    }
    argv.push(OsString::from("--workspace"));
    argv.push(OsString::from("--all-targets"));
    argv.extend(selection.feature_flags());
    argv.extend(selection.package_flags());
    argv
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use clap::Parser;

    use super::argv;
    use crate::cli::{Cli, Command};

    fn selection(options: &[&str]) -> crate::cli::Selection {
        let mut argv = vec!["test-runner", "test"];
        argv.extend_from_slice(options);
        let Command::Test(sel) = Cli::parse_from(argv).command else {
            panic!("expected the test subcommand");
        };
        sel
    }

    #[test]
    fn argv_routes_through_cargo_bin_and_locks_both_levels() {
        assert_eq!(
            argv(&selection(&["--all-features"])),
            vec![
                OsString::from("run"),
                OsString::from("--quiet"),
                OsString::from("--package"),
                OsString::from("cargo-bin"),
                OsString::from("--"),
                OsString::from("cargo-nextest"),
                OsString::from("run"),
                OsString::from("--locked"),
                OsString::from("--workspace"),
                OsString::from("--all-targets"),
                OsString::from("--all-features"),
            ]
        );
    }

    #[test]
    fn argv_omits_locked_when_opted_out() {
        let got = argv(&selection(&["--no-locked"]));
        assert!(!got.iter().any(|a| a == "--locked"));
    }

    #[test]
    fn argv_appends_the_package_selector() {
        let got = argv(&selection(&["-p", "mediapm-utils"]));
        let idx = got.iter().position(|a| a == "-p").expect("-p present");
        assert_eq!(got[idx + 1], OsString::from("mediapm-utils"));
    }
}
