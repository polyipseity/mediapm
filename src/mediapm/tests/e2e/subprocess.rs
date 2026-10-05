//! The one place an end-to-end test spawns the real `mediapm` binary.
//!
//! Both subprocess suites in this directory go through [`run_sync`], so the
//! isolation a child process needs is stated once: a fresh `HOME` and no
//! inherited `XDG_CACHE_HOME`, so the tool download cache stays off the real
//! OS cache, and every ambient `MEDIAPM_*` variable removed, so a `MEDIAPM_ROOT`
//! or `MEDIAPM_PROGRESS_DEBUG` in the test process cannot steer the child.
//! `PATH` and the rest of the environment are left alone so the binary
//! resolves the same tools it normally would.

use std::path::Path;
use std::process::{Command, Output};

/// Runs `mediapm --root <workspace> sync`, optionally with `--no-progress`, and
/// returns the captured process output.
pub(crate) fn run_sync(workspace: &Path, cache_home: &Path, no_progress: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mediapm"));
    command.arg("--root").arg(workspace).arg("sync");
    if no_progress {
        command.arg("--no-progress");
    }
    command.env("HOME", cache_home).env_remove("XDG_CACHE_HOME");
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("MEDIAPM_") {
            command.env_remove(&key);
        }
    }
    command.output().expect("mediapm binary should run")
}
