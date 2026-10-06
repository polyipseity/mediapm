//! The nextest invocation.
//!
//! Nextest is reached through the `cargo-bin` crate rather than as an
//! ambient `cargo-nextest` on `PATH`. `cargo run --package cargo-bin --
//! cargo-nextest` is the invocation the `bin` alias in
//! `.cargo/config.toml` wraps, so the binary the suite runs is the one
//! this workspace pinned. Before this runner existed,
//! `run-all-tests.sh` called `cargo nextest` directly while nothing in
//! `.github/` or `rust-toolchain.toml` provisioned the binary, so CI
//! succeeded only because the machine happened to carry it.

use std::ffi::OsString;

use crate::cli::Selection;

/// The argv that runs the nextest suite for `selection`.
///
/// Nextest is reached as `cargo run --package cargo-bin -- cargo-nextest`,
/// which is the invocation the `bin` alias wraps. The old shell runner
/// called `cargo nextest` directly while nothing in `.github/` or
/// `rust-toolchain.toml` provisioned the binary, so CI depended on it
/// being ambient.
///
/// Two `--locked` flags appear, one on each side of the `--` separator,
/// and they guard two different builds. After the separator the flag is a
/// nextest argument and guards the workspace resolution the suite itself
/// runs. Before it the flag is a cargo argument and guards the `cargo-bin`
/// build, which is a real resolution step rather than a formality: it
/// resolves `cargo-bin`'s own dependency graph and would otherwise be free
/// to rewrite `Cargo.lock` on the way to running the suite. `run-all-tests.sh`
/// passed `--locked` to cargo on every invocation, so dropping the outer
/// one here would reintroduce an unlocked resolution that the runner it
/// replaces did not have.
///
/// `--workspace` is emitted only when no `-p` was given. Cargo resolves
/// the two selectors in favour of `--workspace` rather than in favour of
/// `-p`, so a vector carrying both would run every member's tests and drop
/// the package the caller asked for, without an error or a warning to say
/// so. `doc::doctest_argv` narrows the same way, so the two subcommands
/// answer a `-p` selection identically.
pub fn argv(selection: &Selection) -> Vec<OsString> {
    let locked = selection.wants_lock();
    let mut argv: Vec<OsString> = vec![
        OsString::from("run"),
        OsString::from("--quiet"),
        OsString::from("--package"),
        OsString::from("cargo-bin"),
    ];
    if locked {
        argv.push(OsString::from("--locked"));
    }
    argv.push(OsString::from("--"));
    argv.push(OsString::from("cargo-nextest"));
    argv.push(OsString::from("run"));
    if locked {
        argv.push(OsString::from("--locked"));
    }
    if selection.package.is_none() {
        argv.push(OsString::from("--workspace"));
    }
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

    /// Both sides of the `--` separator carry `--locked`: the outer one
    /// is a cargo flag guarding the `cargo-bin` build, the inner one is a
    /// nextest flag guarding the workspace build.
    #[test]
    fn argv_routes_through_cargo_bin_and_locks_both_levels() {
        assert_eq!(
            argv(&selection(&["--all-features"])),
            vec![
                OsString::from("run"),
                OsString::from("--quiet"),
                OsString::from("--package"),
                OsString::from("cargo-bin"),
                OsString::from("--locked"),
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

    /// The escape hatch has to reach both levels, since one `wants_lock()`
    /// decision drives them. The second assertion is what makes that
    /// claim: it places the separator immediately after the fixed prefix,
    /// so an outer `--locked` left behind on the cargo side fails here
    /// even though a global absence check would also pass.
    #[test]
    fn argv_omits_locked_when_opted_out() {
        let got = argv(&selection(&["--no-locked"]));
        assert!(!got.iter().any(|a| a == "--locked"));
        let separator = got.iter().position(|a| a == "--").expect("-- present");
        assert_eq!(
            separator, 4,
            "nothing sits between the fixed prefix and the separator: {got:?}"
        );
    }

    /// A `-p` selection has to *replace* `--workspace`, not sit beside
    /// it, and this test carries both halves of that claim. Cargo resolves
    /// the two selectors in favour of `--workspace` without saying so, so a
    /// vector carrying both would run every member's tests and report the
    /// narrowed package as done. The absence check is what makes this a
    /// test of the composition: the `-p` pair alone would pass just as
    /// well with the trap still in place.
    #[test]
    fn argv_narrows_to_the_package_instead_of_the_workspace() {
        let got = argv(&selection(&["-p", "mediapm-utils"]));
        assert!(
            !got.iter().any(|a| a == "--workspace"),
            "a `-p` selection must not also carry --workspace: {got:?}"
        );
        let idx = got.iter().position(|a| a == "-p").expect("-p present");
        assert_eq!(got[idx + 1], OsString::from("mediapm-utils"));
    }
}
