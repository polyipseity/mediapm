//! End-to-end guardrails for the local `demo` example.
//!
//! Unlike `demo_online`, this example is expected to be runnable in automated
//! test runs because source ingest is local (`import-once`) and tests can force
//! config-only mode via `MEDIAPM_DEMO_RUN_SYNC=false`.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Resolves workspace root from crate-level `CARGO_MANIFEST_DIR`.
fn workspace_root() -> PathBuf {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_root
        .parent()
        .and_then(Path::parent)
        .expect("mediapm crate should live under <workspace>/src/mediapm")
        .to_path_buf()
}

/// Ensures the `demo` example is executed as part of `mediapm` test runs.
///
/// This invokes the real example entrypoint and forces configuration-only mode
/// to avoid network/tool-download dependencies inside CI.
#[test]
fn demo_example_runs_during_test_execution() {
    let output = Command::new("cargo")
        .arg("run")
        .arg("--package")
        .arg("mediapm")
        .arg("--example")
        .arg("demo")
        .env("MEDIAPM_DEMO_RUN_SYNC", "false")
        .current_dir(workspace_root())
        .output()
        .expect("running `cargo run --package mediapm --example demo`");

    assert!(
        output.status.success(),
        "demo example should run successfully during tests\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
