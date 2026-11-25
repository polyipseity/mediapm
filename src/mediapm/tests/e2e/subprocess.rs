//! The one place an end-to-end test spawns the real `mediapm` binary.
//!
//! Both subprocess suites in this directory go through [`run_sync`] or
//! [`run_tool_sync`], so the isolation a child process needs is stated once:
//! `--tool-cache-dir <cache_home>` puts the tool download cache inside the
//! test's own directory, and every ambient `MEDIAPM_*` variable is removed so a
//! `MEDIAPM_ROOT` or `MEDIAPM_PROGRESS_DEBUG` in the test process cannot steer
//! the child. `PATH` and the rest of the environment are left alone so the
//! binary resolves the same tools it normally would.

use std::path::Path;
use std::process::{Command, Output};

/// Runs `mediapm --root <workspace> sync`, optionally with `--no-progress`, and
/// returns the captured process output.
pub(crate) fn run_sync(workspace: &Path, cache_home: &Path, no_progress: bool) -> Output {
    run(&["sync".to_string()], workspace, cache_home, no_progress)
}

/// Runs `mediapm --root <workspace> tool sync`, optionally with `--no-progress`,
/// and returns the captured process output.
///
/// The same isolation as [`run_sync`]; only the subcommand differs.
pub(crate) fn run_tool_sync(workspace: &Path, cache_home: &Path, no_progress: bool) -> Output {
    run(&["tool".to_string(), "sync".to_string()], workspace, cache_home, no_progress)
}

/// Spawns the binary against `workspace` with `subcommand`, under the
/// environment isolation both subprocess suites depend on.
fn run(subcommand: &[String], workspace: &Path, cache_home: &Path, no_progress: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mediapm"));
    command.arg("--root").arg(workspace);
    command.arg("--tool-cache-dir").arg(cache_home);
    command.args(subcommand);
    if no_progress {
        command.arg("--no-progress");
    }
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("MEDIAPM_") {
            command.env_remove(&key);
        }
    }
    command.output().expect("mediapm binary should run")
}
