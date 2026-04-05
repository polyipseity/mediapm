//! Standalone runner for the fs builtin crate.
//!
//! `fs` is impure, so CLI success is primarily the filesystem side effect.
//! Successful execution emits no payload bytes, while failures are surfaced via
//! ordinary Rust errors instead of being encoded as fake success maps.
//! This crate currently requires explicit keyed flags and does not define a
//! default option key shorthand.

use std::error::Error;

use clap::Parser;

/// Parses standard builtin flags and runs the impure filesystem builtin.
///
/// The binary preserves ordinary Rust error propagation for validation,
/// filesystem, and serialization failures.
fn main() -> Result<(), Box<dyn Error>> {
    let cli = mediapm_conductor_builtin_fs::BuiltinCliArgs::parse();
    let mut stdout = std::io::stdout();
    mediapm_conductor_builtin_fs::run_cli_command(&cli, &mut stdout)
}
