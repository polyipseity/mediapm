//! The binary and the extracted definition describe the same commands.
//!
//! The clap types moved to `mediapm-cli` so a build script can reach them. What
//! that move can break is invisible to an in-process test: clap derives the
//! command name and the entire help text from the crate the types are declared
//! in, so a definition that renders correctly as a value can still be a
//! different command on the command line. These two tests compare the binary
//! against the definition.
//!
//! The subcommand walk is the shape that catches the defect this series opened
//! with, where the first argument was consumed as the program name and every
//! subcommand went unrouted while the library entry point stayed green.

use std::path::Path;
use std::process::Command;

/// Returns the top-level subcommand names the definition declares.
///
/// `help` is filtered out because clap synthesizes it rather than declaring it,
/// so it is not a name the binary has to route.
fn declared_subcommands() -> Vec<String> {
    mediapm_cli::command()
        .get_subcommands()
        .map(|subcommand| subcommand.get_name().to_string())
        .filter(|name| name != "help")
        .collect()
}

/// Returns the file name of the built binary.
///
/// This is ground truth the definition cannot influence, which is what makes the
/// usage-line assertions below non-circular.
fn binary_name() -> String {
    Path::new(env!("CARGO_BIN_EXE_mediapm"))
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .expect("the binary target has a UTF-8 file stem")
        .to_string()
}

/// Runs the binary and returns its captured output.
fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_mediapm"))
        .args(args)
        .output()
        .expect("the mediapm binary should run")
}

/// Runs the binary and returns its stdout and stderr as one string.
fn run_help(args: &[&str]) -> (std::process::ExitStatus, String) {
    let output = run(args);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status, text)
}

/// Every subcommand the definition declares is routed by the binary.
///
/// The check is the usage line rather than the exit status, because the two
/// failure shapes differ. A subcommand clap does not recognize is rejected with
/// the usage status. A subcommand whose position was consumed as the program
/// name is not rejected at all: clap finds the `--help` where it expected the
/// subcommand, prints the top-level help, and exits successfully. In that second
/// shape the printed usage line names the stolen argument, so it no longer
/// starts with the binary's own name.
#[test]
fn every_declared_subcommand_is_routed_by_the_binary() {
    let declared = declared_subcommands();
    assert!(!declared.is_empty(), "the definition must declare subcommands");
    let expected_usage = format!("Usage: {}", binary_name());

    for name in declared {
        let (status, help) = run_help(&[&name, "--help"]);
        assert_ne!(
            status.code(),
            Some(2),
            "`mediapm {name} --help` was rejected as a usage error:\n{help}"
        );
        assert!(
            help.contains(&expected_usage),
            "`mediapm {name} --help` did not render `{expected_usage}`, so the subcommand was \
             not routed:\n{help}"
        );
    }
}

/// The binary's `--help` is the definition's own rendering.
///
/// The comparison holds because both sides build from `mediapm_cli`, which is
/// what it is for: it fails when the binary parses a definition the build script
/// does not render, not when the definition's wording changes, since a doc
/// comment moves both sides together. The `Usage:` assertion below carries the
/// half the comparison cannot — the command must be named after the binary, not
/// after the crate the definition lives in.
///
/// The expected string comes from `render_help` rather than `render_long_help`
/// because that is what clap prints for a command where no argument carries a
/// `long_help`: `--help` falls back to the short renderer.
#[test]
fn the_binary_renders_the_definition_it_ships() {
    let expected = mediapm_cli::command().render_help().to_string();
    let (status, help) = run_help(&["--help"]);
    assert!(status.success(), "`mediapm --help` must succeed:\n{help}");
    assert_eq!(help, expected);
    assert!(
        help.contains(&format!("Usage: {} ", binary_name())),
        "the command must be named after the binary, not after the crate the definition lives \
         in:\n{help}"
    );
}
