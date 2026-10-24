//! Clap command definition for the `mediapm-cas` binary.
//!
//! The definition lives in its own crate so that a build script can construct
//! the command tree. A build script cannot depend on the package it builds, so
//! a definition that lived in `mediapm-cas` would be unreachable from
//! `mediapm-cas`'s own build script. Nothing in this crate depends on
//! `mediapm-cas`, which is what keeps the tree reachable from there.
//!
//! The crate declares types and nothing else: no storage, no I/O, no dispatch.
//! `mediapm-cas` parses with these types and dispatches the result.
//!
//! Every doc comment on a clap type here is a user-facing string: clap renders
//! it into the help text, and a multi-paragraph one additionally becomes
//! `long_about`, which switches the whole command to long-help formatting. They
//! are kept as the definition carried them, and the reasoning that would
//! otherwise go in them belongs in this module's docs instead.

use clap::{Command, CommandFactory, Parser, Subcommand};

/// Top-level `mediapm-cas` CLI arguments.
#[derive(Debug, Parser)]
#[command(name = "mediapm-cas", author, version, about = "mediapm CAS CLI")]
pub struct Cli {
    /// Storage backend locator (e.g. "memory", "/path/to/store").
    #[arg(long, default_value = "memory")]
    pub storage: String,

    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: CasCommand,
}

/// CAS CLI subcommands.
#[derive(Debug, Subcommand)]
pub enum CasCommand {
    /// Store data from stdin and print the hash.
    Put,
    /// Retrieve data by hash and write to stdout.
    Get {
        /// BLAKE3 hash of the object (hex).
        hash: String,
    },
    /// Print metadata for an object.
    Stat {
        /// BLAKE3 hash of the object (hex).
        hash: String,
    },
    /// Delete an object by hash.
    Delete {
        /// BLAKE3 hash of the object (hex).
        hash: String,
    },
    /// Generate shell completions.
    Completions {
        /// Shell name (bash, elvish, fish, powershell, zsh).
        shell: String,
    },
}

/// Builds the command tree the binary parses with.
///
/// The binary and this function build from the same derive, so they cannot
/// disagree about which subcommands exist or what they are called. A build
/// script calls this to generate completions for the command the binary
/// actually accepts.
#[must_use]
pub fn command() -> Command {
    Cli::command()
}

#[cfg(test)]
mod tests {
    use super::{CasCommand, Cli, command};
    use clap::Parser;
    use clap::error::ErrorKind;

    /// The declared subcommand set is part of the CLI's observable interface,
    /// so the extraction must not have renamed, dropped, or added one. A
    /// missing name here means a caller's existing invocation stopped working.
    #[test]
    fn command_declares_the_expected_subcommands() {
        let declared: Vec<String> =
            command().get_subcommands().map(|sub| sub.get_name().to_owned()).collect();
        assert_eq!(declared, ["put", "get", "stat", "delete", "completions"]);
    }

    /// The name is the binary's rather than the defining crate's, because it
    /// reaches the help text and the generated completion script. This crate
    /// is named `mediapm-cas-cli`, so a missing `name` attribute would fail
    /// here instead of silently renaming the command.
    #[test]
    fn command_keeps_the_binary_name() {
        assert_eq!(command().get_name(), "mediapm-cas");
    }

    /// Every name the command tree declares is one the parser accepts. The two
    /// are built from one derive, and this is what would catch a future change
    /// that gave the binary a command tree of its own.
    ///
    /// Each parse is asked for help, so the answer is a help display rather
    /// than the error a subcommand's own missing arguments would raise.
    #[test]
    fn parser_accepts_every_declared_subcommand() {
        for name in ["put", "get", "stat", "delete", "completions"] {
            let error = Cli::try_parse_from(["mediapm-cas", name, "--help"])
                .expect_err("a help request is reported as an error by clap");
            assert_eq!(
                error.kind(),
                ErrorKind::DisplayHelp,
                "the parser does not know the subcommand {name}"
            );
        }
    }

    /// An unknown subcommand is still rejected, so the acceptance above is the
    /// parser knowing the name rather than clap accepting anything.
    #[test]
    fn parser_rejects_an_unknown_subcommand() {
        let error = Cli::try_parse_from(["mediapm-cas", "nonexistent"])
            .expect_err("an unknown subcommand must not parse");
        assert_eq!(error.kind(), ErrorKind::InvalidSubcommand);
    }

    /// The storage locator keeps its default, which is the value a caller gets
    /// when the flag is absent.
    #[test]
    fn storage_defaults_to_memory() {
        let cli = Cli::try_parse_from(["mediapm-cas", "put"]).expect("parse put");
        assert_eq!(cli.storage, "memory");
        assert!(matches!(cli.command, CasCommand::Put));
    }
}
