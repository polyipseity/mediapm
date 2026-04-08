//! Standalone runner for the import builtin crate.
//!
//! `import` is impure and reads external file/folder/fetch sources.
//! For `kind=file|folder`, `path_mode` defaults to `relative` and resolves
//! `path` under the configured root directory.
//! `kind=cas_hash` is intended for conductor runtime dispatch where the caller
//! can resolve hash bytes from CAS state.
//! Successful runs emit imported payload bytes directly, while failures are
//! surfaced via ordinary Rust errors.
//! This crate currently requires explicit keyed flags and does not define a
//! default option key shorthand.

use std::error::Error;

use clap::Parser;

/// Parses standard builtin flags and runs the impure import builtin.
///
/// The binary preserves ordinary Rust error propagation for invalid inputs and
/// I/O.
fn main() -> Result<(), Box<dyn Error>> {
    let cli = mediapm_conductor_builtin_import::BuiltinCliArgs::parse();
    let mut stdout = std::io::stdout();
    mediapm_conductor_builtin_import::run_cli_command(&cli, &mut stdout)
}
