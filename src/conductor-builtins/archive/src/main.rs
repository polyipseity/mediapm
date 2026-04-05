//! Standalone runner for the archive builtin crate.
//!
//! `archive` is pure and transforms input bytes into output bytes.
//! This crate currently requires explicit keyed flags and does not define a
//! default option key shorthand.

use std::error::Error;

use clap::Parser;

/// Parses standard builtin flags and runs the pure archive builtin.
///
/// The binary preserves ordinary Rust errors for invalid arguments,
/// filesystem issues, and serialization failures.
fn main() -> Result<(), Box<dyn Error>> {
    let cli = mediapm_conductor_builtin_archive::BuiltinCliArgs::parse();
    let mut stdout = std::io::stdout();
    mediapm_conductor_builtin_archive::run_cli_command(&cli, &mut stdout)
}
