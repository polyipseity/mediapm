//! Standalone runner for the `echo` builtin tool.
//!
//! This runner behaves like shell `echo`: positional text is written to one
//! selected stream (`stdout`, `stderr`, or both) and terminated with a newline.
//! Failures are surfaced as ordinary Rust errors.

use std::error::Error;

use clap::Parser;

/// Parses the simplified echo CLI and writes to stdout/stderr as requested.
///
/// The binary preserves ordinary Rust error propagation for failures so I/O,
/// argument parsing, and stream writes keep their original causes.
fn main() -> Result<(), Box<dyn Error>> {
    let cli = mediapm_conductor_builtin_echo::BuiltinCliArgs::parse();
    let mut stdout = std::io::stdout();
    let mut stderr = std::io::stderr();
    mediapm_conductor_builtin_echo::run_cli_command(&cli, &mut stdout, &mut stderr)
}
