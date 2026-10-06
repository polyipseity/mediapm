//! The `test` subcommand.
//!
//! The subcommand owns the suite run plus the two gates that guard it: the
//! temp-directory janitor and the unprefixed-tempdir invariant. Both gates
//! are placeholders here and gain their real bodies in Task 3; this task
//! only fixes the order they run in, which is the part nothing else
//! decides.

use std::path::Path;

use anyhow::{Context, Result};

use crate::cargo;
use crate::cli::Selection;

/// Runs the nextest suite for `selection`, then every gate that guards it.
///
/// A non-zero nextest exit returns immediately: running the temp-directory
/// gates after a failed suite would report leftovers belonging to a run
/// that had already been rejected.
pub fn run(selection: &Selection) -> Result<i32> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let metadata = cargo::metadata(manifest_dir)?;
    let root = metadata.workspace_root;

    let code = run_nextest(selection)?;
    if code != 0 {
        return Ok(code);
    }

    crate::gates::tempdir::enforce(&root)?;
    crate::gates::janitor::enforce(&root)?;
    Ok(0)
}

/// Spawns the nextest suite and returns its exit code.
fn run_nextest(selection: &Selection) -> Result<i32> {
    let status = std::process::Command::new(cargo::cargo_binary())
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args(crate::nextest::argv(selection))
        .status()
        .context("spawn the nextest suite")?;
    cargo::status_code(status, "the nextest suite")
}
