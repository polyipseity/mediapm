//! The command-line surface of the test runner.
//!
//! This module owns everything the user types: the [`Cli`] parser, the
//! [`Command`] subcommand set, and the two argument groups. Nothing here
//! spawns a process or reads cargo state — the parsed values are plain data
//! that later tasks turn into invocations.
//!
//! Feature selection deliberately mirrors cargo's own three flags
//! (`--features`, `--all-features`, `--no-default-features`) and keeps the
//! same mutual exclusion, so a selection the runner accepts is a selection
//! cargo accepts.

use std::ffi::OsString;

use clap::{Args, Parser, Subcommand};

/// Parsed command line.
#[derive(Debug, Parser)]
#[command(
    name = "test-runner",
    version,
    about = "Central runner for workspace tests, doctests and the feature matrix."
)]
pub struct Cli {
    /// The subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The three things the runner can do.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the test suite, then the janitor and tempdir gates.
    Test(Selection),
    /// Run doctests, then the rustdoc link gate.
    Doc(Selection),
    /// Check every package and feature combination.
    FeatureMatrix(MatrixArgs),
}

/// Cargo-style package and feature selection.
///
/// The three feature flags are mutually exclusive and mirror cargo's own.
/// `--locked` is the default because CI runs this; `--no-locked` is the
/// local escape for a tree whose `Cargo.lock` still needs updating.
#[derive(Debug, Args, Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Selection mirrors cargo's own --all-features/--no-default-features/--locked/--no-locked flags one for one; collapsing them into enums would make the accepted command line diverge from cargo's and lose the clap derive"
)]
pub struct Selection {
    /// Space or comma separated list of features to activate.
    #[arg(
        long,
        value_name = "FEATURES",
        conflicts_with_all = ["all_features", "no_default_features"]
    )]
    pub features: Option<String>,
    /// Activate all available features.
    #[arg(long, conflicts_with = "no_default_features")]
    pub all_features: bool,
    /// Do not activate the `default` feature.
    #[arg(long)]
    pub no_default_features: bool,
    /// Package to check; defaults to the whole workspace.
    #[arg(short, long, value_name = "PKG")]
    pub package: Option<String>,
    /// Require an up-to-date `Cargo.lock`. This is the default. The cargo
    /// aliases hardcode `--locked`, so this flag has to parse when a caller
    /// appends it; `--no-locked` is the only one of the two that changes
    /// the answer.
    #[arg(long)]
    pub locked: bool,
    /// Permit `Cargo.lock` to be updated. Wins over `--locked`, so a caller
    /// that appended it to an alias carrying `--locked` still gets the
    /// escape hatch.
    #[arg(long)]
    pub no_locked: bool,
}

impl Selection {
    /// Whether child cargo invocations must pass `--locked`.
    ///
    /// Both flags are read so neither field is inert: the cargo aliases
    /// hardcode `--locked`, which is why the flag has to exist, and a
    /// caller that appends `--no-locked` to such an alias is still asking
    /// for the escape hatch. Every combination has one answer and none of
    /// them is wrong: neither flag means the default (`--locked`),
    /// `--locked` alone states the default explicitly, `--no-locked` alone
    /// opts out, and both together resolve to `--no-locked` so the escape
    /// hatch wins over the flag it is undoing.
    pub fn wants_lock(&self) -> bool {
        match (self.locked, self.no_locked) {
            (_, true) => false,
            (_, false) => true,
        }
    }

    /// The feature flags to append to a child cargo invocation.
    ///
    /// Order is fixed (`--features`, then `--all-features`, then
    /// `--no-default-features`) so two runs of the same selection produce
    /// byte-identical argv.
    pub fn feature_flags(&self) -> Vec<OsString> {
        let mut flags = Vec::new();
        if let Some(features) = &self.features {
            flags.push(OsString::from("--features"));
            flags.push(OsString::from(features));
        }
        if self.all_features {
            flags.push(OsString::from("--all-features"));
        }
        if self.no_default_features {
            flags.push(OsString::from("--no-default-features"));
        }
        flags
    }

    /// The `-p <PKG>` fragment, empty when the selection covers everything.
    pub fn package_flags(&self) -> Vec<OsString> {
        match &self.package {
            Some(pkg) => vec![OsString::from("-p"), OsString::from(pkg)],
            None => Vec::new(),
        }
    }
}

/// Arguments for the feature-matrix sweep.
#[derive(Debug, Args, Clone)]
pub struct MatrixArgs {
    /// Permit `Cargo.lock` to be updated. Wins over `--locked`.
    #[arg(long)]
    pub no_locked: bool,
    /// Require an up-to-date `Cargo.lock`. This is the default. The cargo
    /// aliases hardcode `--locked`, so this flag has to parse when a caller
    /// appends it; `--no-locked` is the only one of the two that changes
    /// the answer.
    #[arg(long)]
    pub locked: bool,
    /// Print every derived combination without running any of them.
    #[arg(long)]
    pub print: bool,
}

impl MatrixArgs {
    /// Whether each `cargo check` must pass `--locked`.
    ///
    /// Reads both flags for the same reason as [`Selection::wants_lock`]:
    /// the cargo aliases hardcode `--locked`, so the flag has to parse when
    /// a caller appends it, and `--no-locked` is the only one of the two
    /// that changes the answer. With neither flag the sweep is locked (the
    /// default), with `--locked` alone it is locked explicitly, with
    /// `--no-locked` alone it is unlocked, and with both it is unlocked, so
    /// the escape hatch always wins.
    pub fn wants_lock(&self) -> bool {
        match (self.locked, self.no_locked) {
            (_, true) => false,
            (_, false) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn locked_is_the_default() {
        let cli = Cli::parse_from(["test-runner", "test"]);
        let Command::Test(sel) = cli.command else {
            panic!("expected test");
        };
        assert!(sel.wants_lock());
    }

    #[test]
    fn locked_alone_keeps_the_default() {
        let cli = Cli::parse_from(["test-runner", "test", "--locked"]);
        let Command::Test(sel) = cli.command else {
            panic!("expected test");
        };
        assert!(sel.wants_lock());
    }

    #[test]
    fn no_locked_opts_out() {
        let cli = Cli::parse_from(["test-runner", "test", "--no-locked"]);
        let Command::Test(sel) = cli.command else {
            panic!("expected test");
        };
        assert!(!sel.wants_lock());
    }

    #[test]
    fn no_locked_wins_over_locked() {
        let cli = Cli::parse_from(["test-runner", "test", "--locked", "--no-locked"]);
        let Command::Test(sel) = cli.command else {
            panic!("expected test");
        };
        assert!(!sel.wants_lock());
    }

    #[test]
    fn feature_flags_emit_all_features() {
        let cli = Cli::parse_from(["test-runner", "test", "--all-features"]);
        let Command::Test(sel) = cli.command else {
            panic!("expected test");
        };
        assert_eq!(sel.feature_flags(), vec![std::ffi::OsString::from("--all-features")]);
    }

    #[test]
    fn feature_flags_are_empty_by_default() {
        let cli = Cli::parse_from(["test-runner", "test"]);
        let Command::Test(sel) = cli.command else {
            panic!("expected test");
        };
        assert!(sel.feature_flags().is_empty());
    }

    #[test]
    fn feature_flags_reject_no_default_features_with_all_features() {
        assert!(
            Cli::try_parse_from(["test-runner", "test", "--all-features", "--no-default-features"])
                .is_err()
        );
    }

    #[test]
    fn feature_flags_reject_features_with_all_features() {
        assert!(
            Cli::try_parse_from(["test-runner", "test", "--all-features", "--features", "cli"])
                .is_err()
        );
    }

    /// A selection with no `-p` covers the whole workspace, so the
    /// fragment has to be empty. A stray empty `-p` here would make cargo
    /// read the next flag as a package name.
    #[test]
    fn package_flags_are_empty_without_a_package() {
        let cli = Cli::parse_from(["test-runner", "test"]);
        let Command::Test(sel) = cli.command else {
            panic!("expected test");
        };
        assert_eq!(sel.package_flags(), Vec::<OsString>::new());
    }

    /// A `-p` selection has to reach the child as the pair `-p <PKG>`, in
    /// that order and with nothing between them, because cargo reads the
    /// value as the argument to `-p` rather than as a positional.
    #[test]
    fn package_flags_carry_the_package_selector() {
        let cli = Cli::parse_from(["test-runner", "test", "-p", "mediapm-utils"]);
        let Command::Test(sel) = cli.command else {
            panic!("expected test");
        };
        assert_eq!(
            sel.package_flags(),
            vec![OsString::from("-p"), OsString::from("mediapm-utils")]
        );
    }
}
