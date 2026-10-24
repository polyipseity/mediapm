//! The conductor binary's own entry point reaches its subcommands.
//!
//! The conductor has two entry points: [`mediapm_conductor::cli::run_from_args`]
//! takes an explicit argument vector, and `run_from_env` reads the process's
//! own. Only the first had coverage, and that gap is how `run_from_env` came to
//! drop the program name from the vector: clap reads position zero as the
//! program name, so a vector without it makes clap read the first real
//! argument as the program name and the second as the top-level subcommand.
//! Every subcommand a user typed was then swallowed, and the process printed
//! the root command's help instead.
//!
//! These tests spawn the built binary, because `run_from_env` reads
//! `std::env::args()`, which an in-process test cannot set. They are the only
//! coverage of the path a user actually takes.

use std::process::Command;

/// What one run of the built binary produced.
struct BinaryRun {
    /// Whether the process exited with a success status.
    success: bool,
    /// Everything the process wrote to standard output.
    stdout: String,
    /// Everything the process wrote to standard error.
    stderr: String,
}

/// Runs the built conductor binary with `args` and captures its result.
///
/// The binary is located through Cargo's `CARGO_BIN_EXE_mediapm-conductor`,
/// which Cargo sets for integration tests of the package that declares the
/// binary, so no target-directory path is guessed. A missing binary is a
/// build-configuration failure, and `.expect` reports it as one.
fn run_binary(args: &[&str]) -> BinaryRun {
    let output = Command::new(env!("CARGO_BIN_EXE_mediapm-conductor"))
        .args(args)
        .output()
        .expect("spawn the conductor binary");
    BinaryRun {
        success: output.status.success(),
        stdout: String::from_utf8(output.stdout).expect("conductor stdout is utf-8"),
        stderr: String::from_utf8(output.stderr).expect("conductor stderr is utf-8"),
    }
}

/// Returns the usage line of a clap help text, or `None` when it has none.
///
/// clap renders exactly one line beginning `Usage:`, and it is the line that
/// says which command the help describes, so it is the line these tests read.
fn usage_line(stdout: &str) -> Option<&str> {
    stdout.lines().find(|line| line.starts_with("Usage:"))
}

/// A subcommand's `--help` describes that subcommand, not the root command.
///
/// The usage line distinguishes the two without depending on wording. A leaf
/// command that takes no subcommand renders no `<COMMAND>` placeholder, while
/// the root command requires one, so a help text carrying the placeholder came
/// from the root command whatever else it says. That is exactly the shape the
/// regression produced: `gc --help` printed the root help under
/// `Usage: gc [OPTIONS] <COMMAND>`, with the subcommand name stolen into the
/// program-name position.
///
/// Two subcommands are probed because the defect swallowed every one of them,
/// so a test that probed only one could pass while the rest stayed broken.
#[test]
fn subcommand_help_describes_the_subcommand() {
    for subcommand in ["gc", "cas"] {
        let run = run_binary(&[subcommand, "--help"]);
        assert!(run.success, "`{subcommand} --help` exited with failure; stderr: {}", run.stderr);
        let usage = usage_line(&run.stdout).unwrap_or_else(|| {
            panic!("`{subcommand} --help` printed no usage line; stdout: {}", run.stdout)
        });
        assert!(
            usage.contains(subcommand),
            "`{subcommand} --help` printed the usage of another command: {usage}",
        );
        assert!(
            !usage.contains("<COMMAND>"),
            "`{subcommand} --help` printed the root command's help, whose usage line carries \
             the subcommand placeholder because the root command requires a subcommand: {usage}",
        );
    }
}

/// `completions <shell>` reaches its handler and emits a script for this CLI.
///
/// The handler is reached rather than merely parsed: a completion script is
/// only produced by running it. The assertion names a subcommand of this CLI
/// so the script cannot be an empty or generic one, and it reads the script
/// rather than only the exit status because a handler that printed nothing
/// would still exit successfully.
#[test]
fn completions_reaches_its_handler() {
    let run = run_binary(&["completions", "bash"]);
    assert!(run.success, "`completions bash` exited with failure; stderr: {}", run.stderr);
    assert!(
        run.stdout.contains("export-schemas"),
        "the completion script does not name this CLI's subcommands; stdout: {}",
        run.stdout,
    );
}

/// The root command's own `--help` still lists the subcommands.
///
/// This is the control for the two tests above: a change that made a
/// subcommand's help work by always printing something else, or that broke the
/// root command's help, would otherwise pass them.
#[test]
fn root_help_still_lists_subcommands() {
    let run = run_binary(&["--help"]);
    assert!(run.success, "`--help` exited with failure; stderr: {}", run.stderr);
    assert!(
        run.stdout.contains("Commands:"),
        "the root command's help no longer lists its subcommands; stdout: {}",
        run.stdout,
    );
    let usage = usage_line(&run.stdout)
        .unwrap_or_else(|| panic!("`--help` printed no usage line; stdout: {}", run.stdout));
    assert!(
        usage.contains("<COMMAND>"),
        "the root command requires a subcommand, so its usage line carries the placeholder: \
         {usage}",
    );
}
