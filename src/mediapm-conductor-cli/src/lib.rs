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

pub mod env_registry;

use std::path::PathBuf;

use clap::{ArgAction, Command, CommandFactory, Parser, Subcommand};
use clap_complete::Shell;

/// Top-level `mediapm-conductor` CLI arguments.
#[derive(Parser)]
#[command(name = "conductor", version, about = "mediapm-conductor workflow orchestrator")]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the clap definition mirrors five independent operator switches whose names and defaults are the CLI contract"
)]
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
    /// Use a conductor-owned secret for instance-key derivation.
    #[arg(
        long,
        env = "MEDIAPM_CONDUCTOR_USE_INSTANCE_KEY_SECRET",
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = ArgAction::Set,
        conflicts_with = "no_use_instance_key_secret"
    )]
    pub use_instance_key_secret: bool,
    /// Disable conductor-owned secret use for instance-key derivation.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_NO_USE_INSTANCE_KEY_SECRET")]
    pub no_use_instance_key_secret: bool,
    /// Require the generated secret format and entropy sanity check.
    #[arg(
        long,
        env = "MEDIAPM_CONDUCTOR_REQUIRE_GENERATED_INSTANCE_KEY_SECRET_FORMAT",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub require_generated_instance_key_secret_format: bool,
    /// Generate a secret when state says no secret was used.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_FORCE_GENERATE_INSTANCE_KEY_SECRET")]
    pub force_generate_instance_key_secret: bool,
    /// Continue without a secret when state says one was used.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_FORCE_DISABLE_INSTANCE_KEY_SECRET")]
    pub force_disable_instance_key_secret: bool,

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

    /// Serializes process-environment mutations made by tests in this crate.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Restores one environment variable when a test scope ends.
    struct EnvValueGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvValueGuard {
        /// Installs `value` for `name`.
        ///
        /// # Safety
        ///
        /// The caller must hold [`ENV_LOCK`] for the guard's whole lifetime.
        unsafe fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            // SAFETY: the caller holds ENV_LOCK across the mutation and the guard.
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }
    }

    impl Drop for EnvValueGuard {
        fn drop(&mut self) {
            // SAFETY: the lock guard is declared first, so it is still held here.
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(self.name, value),
                    None => std::env::remove_var(self.name),
                }
            }
        }
    }

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
            ("state", vec!["compile", "show", "invalidate-tool-call", "revalidate-tool-call"]),
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

    /// The paired instance-key switches must be mutually exclusive: an
    /// operator who states both has contradictory intent, and clap must reject
    /// the invocation instead of letting one switch silently win.
    #[test]
    fn instance_key_secret_flags_follow_pair_convention() {
        let positive = Cli::try_parse_from(["conductor", "--use-instance-key-secret", "gc"])
            .expect("positive flag parses");
        let negative = Cli::try_parse_from(["conductor", "--no-use-instance-key-secret", "gc"])
            .expect("negative flag parses");
        assert!(!positive.no_use_instance_key_secret);
        assert!(negative.no_use_instance_key_secret);
        let both = Cli::try_parse_from([
            "conductor",
            "--use-instance-key-secret",
            "--no-use-instance-key-secret",
            "gc",
        ]);
        assert!(both.is_err(), "positive and negative switches conflict");
    }

    /// The positive switch accepts an explicit `false` at the command line:
    /// disabling secrets must not depend on the negative spelling being known.
    #[test]
    fn instance_key_secret_positive_switch_accepts_explicit_false() {
        let parsed = Cli::try_parse_from(["conductor", "--use-instance-key-secret=false", "gc"])
            .expect("explicit false parses");
        assert!(!parsed.use_instance_key_secret);
    }

    /// The positive switch's env var carries `false` into the parsed field.
    /// The mutex keeps this process-global mutation away from sibling tests.
    #[test]
    fn instance_key_secret_positive_env_false_disables_the_switch() {
        let name = "MEDIAPM_CONDUCTOR_USE_INSTANCE_KEY_SECRET";
        let lock = ENV_LOCK.lock().expect("environment lock");
        // SAFETY: this test holds ENV_LOCK for the whole mutation window.
        let value = unsafe { EnvValueGuard::set(name, "false") };

        let parsed = Cli::try_parse_from(["conductor", "gc"]).expect("parse gc under env false");

        drop(value);
        drop(lock);
        assert!(!parsed.use_instance_key_secret);
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

    /// Every instance-key switch reads its own environment variable, so a
    /// caller can set policy without adding flags to each invocation.
    #[test]
    fn instance_key_secret_switches_read_their_environment_variables() {
        let command = command();
        for (flag, variable) in [
            ("use_instance_key_secret", "MEDIAPM_CONDUCTOR_USE_INSTANCE_KEY_SECRET"),
            ("no_use_instance_key_secret", "MEDIAPM_CONDUCTOR_NO_USE_INSTANCE_KEY_SECRET"),
            (
                "require_generated_instance_key_secret_format",
                "MEDIAPM_CONDUCTOR_REQUIRE_GENERATED_INSTANCE_KEY_SECRET_FORMAT",
            ),
            (
                "force_generate_instance_key_secret",
                "MEDIAPM_CONDUCTOR_FORCE_GENERATE_INSTANCE_KEY_SECRET",
            ),
            (
                "force_disable_instance_key_secret",
                "MEDIAPM_CONDUCTOR_FORCE_DISABLE_INSTANCE_KEY_SECRET",
            ),
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

    /// The generated-format check defaults on but accepts an explicit value,
    /// while the two force switches are presence-only `SetTrue` flags.
    #[test]
    fn instance_key_secret_switch_values_follow_the_declared_actions() {
        let default = Cli::try_parse_from(["conductor", "gc"]).expect("parse gc");
        assert!(default.require_generated_instance_key_secret_format);
        assert!(!default.force_generate_instance_key_secret);
        assert!(!default.force_disable_instance_key_secret);

        let relaxed = Cli::try_parse_from([
            "conductor",
            "--require-generated-instance-key-secret-format",
            "false",
            "gc",
        ])
        .expect("parse relaxed generated-format check");
        assert!(!relaxed.require_generated_instance_key_secret_format);

        let forced = Cli::try_parse_from([
            "conductor",
            "--force-generate-instance-key-secret",
            "--force-disable-instance-key-secret",
            "gc",
        ])
        .expect("parse force switches");
        assert!(forced.force_generate_instance_key_secret);
        assert!(forced.force_disable_instance_key_secret);
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
