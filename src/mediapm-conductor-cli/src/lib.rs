//! Clap command definition for the `mediapm-conductor` binary.
//!
//! The definition lives in its own crate so that a build script can construct
//! the command tree. A build script cannot depend on the package it builds, so
//! a definition that lived in `mediapm-conductor` would be unreachable from
//! `mediapm-conductor`'s own build script. Nothing in this crate depends on
//! `mediapm-conductor`, which is what keeps the tree reachable from there.
//!
//! The crate declares types and nothing else: no orchestration, no I/O, no
//! dispatch. `mediapm-conductor` parses with these types and dispatches the
//! result.
//!
//! Every doc comment on a clap type here is a user-facing string: clap renders
//! it into the help text, and a multi-paragraph one additionally becomes
//! `long_about`, which switches the whole command to long-help formatting. They
//! are kept as the definition carried them, and the reasoning that would
//! otherwise go in them belongs in this module's docs instead.

use std::path::PathBuf;

use clap::{Command, CommandFactory, Parser, Subcommand};
use clap_complete::Shell;

/// Top-level `mediapm-conductor` CLI arguments.
#[derive(Parser)]
#[command(name = "conductor", version, about = "mediapm-conductor workflow orchestrator")]
pub struct Cli {
    /// Override the conductor runtime directory.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_DIR")]
    pub conductor_dir: Option<PathBuf>,
    /// Override the CAS store directory.
    #[arg(long, env = "MEDIAPM_CAS_STORE_DIR")]
    pub cas_store_dir: Option<PathBuf>,
    /// Override the schema export directory.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_SCHEMA_DIR")]
    pub conductor_schema_dir: Option<PathBuf>,
    /// Override the temporary directory.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_TMP_DIR")]
    pub conductor_tmp_dir: Option<PathBuf>,
    /// Override the tools materialization directory.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_TOOLS_DIR")]
    pub conductor_tools_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub command: CliCommand,
}

/// Conductor CLI subcommands.
#[derive(Subcommand)]
pub enum CliCommand {
    /// Run a workflow by name.
    Run {
        /// Name of the workflow to execute.
        workflow: String,
        /// Re-execute every step even when its instance is cached.
        ///
        /// Explicit invalidation: instances are never invalidated by the
        /// passage of time, so this flag (or changed step material) is the
        /// way to force a fresh run.
        #[arg(long)]
        force_rerun: bool,
    },
    /// Inspect or manipulate orchestration state.
    State(StateArgs),
    /// Import tools or data.
    Import(ImportArgs),
    /// Remove tools or data.
    Remove(RemoveArgs),
    /// Inspect or run tools.
    Tool(ToolArgs),
    /// Passthrough to the CAS CLI.
    Cas {
        /// Arguments forwarded to the CAS binary.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Export schemas to a directory.
    ExportSchemas {
        /// Output directory for schema files.
        #[arg(default_value = "schemas")]
        output: PathBuf,
    },
    /// Run garbage collection on the CAS store.
    Gc,
    /// Generate shell completions.
    Completions {
        /// Target shell.
        shell: Shell,
    },
}

/// Arguments for the `state` subcommand.
#[derive(clap::Args)]
pub struct StateArgs {
    #[command(subcommand)]
    pub command: Option<StateCommand>,
}

/// State subcommands.
#[derive(Subcommand)]
pub enum StateCommand {
    /// Compile and display the effective configuration.
    Compile,
    /// Export state to a JSON file.
    Export {
        /// Output file path.
        path: PathBuf,
    },
    /// Import state from a JSON file.
    Import {
        /// Input file path.
        path: PathBuf,
    },
    /// Show runtime diagnostics.
    Show,

    /// Mark a tool call instance as not to be reused.
    ///
    /// The cache probe answers "miss" for a marked instance, so the work behind
    /// it runs again on the next sync. Nothing is deleted: the record, its
    /// outputs, and the blobs they name stay until garbage collection finds them
    /// unreachable, and re-running the work clears the marker by recording a
    /// fresh instance. Undo with `state revalidate-tool-call`.
    InvalidateToolCall {
        /// Instance key to invalidate.
        key: String,
    },
    /// Clear a tool call instance's invalidation marker.
    ///
    /// Undoes [`StateCommand::InvalidateToolCall`]: the recorded instance
    /// becomes reusable again. A re-execution that landed on the same key has
    /// already cleared the marker by rewriting the record, so this reports that
    /// there was nothing to clear; one that landed on a new key leaves the old
    /// record marked, and clearing it is still meaningful because that record
    /// can still be projected by the materializer.
    RevalidateToolCall {
        /// Instance key to revalidate.
        key: String,
    },
}

/// Arguments for the `import` subcommand.
#[derive(clap::Args)]
pub struct ImportArgs {
    #[command(subcommand)]
    pub command: ImportCommand,
}

/// Import subcommands.
#[derive(Subcommand)]
pub enum ImportCommand {
    /// Import a tool (directory or file).
    Tool {
        /// Path to the tool directory or binary file.
        path: Option<PathBuf>,
        /// Tool name override.
        #[arg(long)]
        name: Option<String>,
        /// Process name override.
        #[arg(long)]
        process_name: Option<String>,
    },
}

/// Arguments for the `remove` subcommand.
#[derive(clap::Args)]
pub struct RemoveArgs {
    #[command(subcommand)]
    pub command: RemoveCommand,
}

/// Remove subcommands.
#[derive(Subcommand)]
pub enum RemoveCommand {
    /// Remove external data by hash.
    Data { hash: String },
    /// Remove a tool configuration.
    Tool {
        name: String,
        #[arg(long)]
        metadata: bool,
    },
}

/// Arguments for the `tool` subcommand.
#[derive(clap::Args)]
pub struct ToolArgs {
    #[command(subcommand)]
    pub command: ToolCommand,
}

/// Tool subcommands.
#[derive(Subcommand)]
pub enum ToolCommand {
    /// Run a tool with passthrough arguments.
    Run {
        /// Tool name to run.
        #[arg(long)]
        tool: String,
        /// Arguments forwarded to the tool.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// List configured tools with their binary presence status.
    List,
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
    use super::{Cli, command};
    use clap::Parser;
    use clap::error::ErrorKind;

    /// Collects a command's declared subcommand names in declaration order.
    fn subcommand_names(command: &clap::Command) -> Vec<String> {
        command.get_subcommands().map(|sub| sub.get_name().to_owned()).collect()
    }

    /// Parses `args`, expecting clap to reject them, and returns the error.
    ///
    /// `Cli` derives no `Debug`, matching the definition it replaced, so this
    /// matches on the result rather than calling `expect_err`, which would
    /// require it.
    fn parse_error(args: &[&str]) -> clap::Error {
        match Cli::try_parse_from(args) {
            Ok(_) => panic!("expected clap to reject {args:?}"),
            Err(error) => error,
        }
    }

    /// The declared subcommand set is part of the CLI's observable interface,
    /// so the extraction must not have renamed, dropped, or added one. A
    /// missing name here means a caller's existing invocation stopped working.
    #[test]
    fn command_declares_the_expected_subcommands() {
        assert_eq!(
            subcommand_names(&command()),
            [
                "run",
                "state",
                "import",
                "remove",
                "tool",
                "cas",
                "export-schemas",
                "gc",
                "completions"
            ]
        );
    }

    /// The nested sets are part of the same interface, and they are the ones a
    /// move is most likely to disturb because each lives in its own type.
    #[test]
    fn nested_commands_declare_the_expected_subcommands() {
        let root = command();
        for (parent, expected) in [
            (
                "state",
                vec![
                    "compile",
                    "export",
                    "import",
                    "show",
                    "invalidate-tool-call",
                    "revalidate-tool-call",
                ],
            ),
            ("import", vec!["tool"]),
            ("remove", vec!["data", "tool"]),
            ("tool", vec!["run", "list"]),
        ] {
            let child = root
                .find_subcommand(parent)
                .unwrap_or_else(|| panic!("the command tree has no {parent} subcommand"));
            assert_eq!(subcommand_names(child), expected, "wrong subcommands under {parent}");
        }
    }

    /// The name stays `conductor` rather than following the crate name, because
    /// it reaches the help text and the generated completion script.
    #[test]
    fn command_keeps_the_declared_name() {
        assert_eq!(command().get_name(), "conductor");
    }

    /// Every name the command tree declares is one the parser accepts. The two
    /// are built from one derive, and this is what would catch a future change
    /// that gave the binary a command tree of its own.
    ///
    /// Each parse is asked for help, so the answer is a help display rather
    /// than the error a subcommand's own missing arguments would raise.
    #[test]
    fn parser_accepts_every_declared_subcommand() {
        for name in [
            "run",
            "state",
            "import",
            "remove",
            "tool",
            "cas",
            "export-schemas",
            "gc",
            "completions",
        ] {
            let error = parse_error(&["conductor", name, "--help"]);
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
        let error = parse_error(&["conductor", "nonexistent"]);
        assert_eq!(error.kind(), ErrorKind::InvalidSubcommand);
    }

    /// Each path override reads its own environment variable, which is how a
    /// caller sets one without a flag. The variable name appears nowhere else
    /// in the crate, so a move that dropped the attribute would leave nothing
    /// to notice it.
    #[test]
    fn path_overrides_read_their_environment_variables() {
        let command = command();
        for (flag, variable) in [
            ("conductor_dir", "MEDIAPM_CONDUCTOR_DIR"),
            ("cas_store_dir", "MEDIAPM_CAS_STORE_DIR"),
            ("conductor_schema_dir", "MEDIAPM_CONDUCTOR_SCHEMA_DIR"),
            ("conductor_tmp_dir", "MEDIAPM_CONDUCTOR_TMP_DIR"),
            ("conductor_tools_dir", "MEDIAPM_CONDUCTOR_TOOLS_DIR"),
        ] {
            let arg = command
                .get_arguments()
                .find(|arg| arg.get_id().as_str() == flag)
                .unwrap_or_else(|| panic!("the command declares no {flag} argument"));
            assert_eq!(
                arg.get_env().and_then(std::ffi::OsStr::to_str),
                Some(variable),
                "the {flag} argument does not read {variable}"
            );
        }
    }

    /// The same override works as a flag, which is the form that needs no
    /// process environment.
    #[test]
    fn path_overrides_are_read_from_their_flags() {
        let cli = Cli::try_parse_from(["conductor", "gc"]).expect("parse gc");
        assert!(cli.conductor_dir.is_none());

        let cli = Cli::try_parse_from(["conductor", "--conductor-dir", "/tmp/x", "gc"])
            .expect("parse gc with an override");
        assert_eq!(cli.conductor_dir.as_deref(), Some(std::path::Path::new("/tmp/x")));
    }
}
