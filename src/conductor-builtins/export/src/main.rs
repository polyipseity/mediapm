//! Standalone runner for the export builtin crate.
//!
//! `export` is impure and writes file/folder payloads to host paths.
//! For `kind=file|folder`, `path_mode` defaults to `relative` and resolves
//! destination paths under the configured root directory.

use std::error::Error;

use clap::Parser;

/// Parses standard builtin flags and runs the export builtin.
fn main() -> Result<(), Box<dyn Error>> {
    let cli = mediapm_conductor_builtin_export::BuiltinCliArgs::parse();
    let mut stdout = std::io::stdout();
    mediapm_conductor_builtin_export::run_cli_command(&cli, &mut stdout)
}
