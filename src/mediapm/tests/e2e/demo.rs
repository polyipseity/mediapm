//! End-to-end guardrails for the local `demo` example.
//!
//! Unlike `demo_online`, this example is expected to be runnable in automated
//! test runs because source ingest is local (`import`) and tests can force
//! config-only mode via `MEDIAPM_DEMO_RUN_SYNC=false`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

/// Extracts `manifest: <path>` from demo example stdout.
fn manifest_path_from_stdout(stdout: &str) -> Option<PathBuf> {
    stdout.lines().find_map(|line| line.strip_prefix("manifest: ").map(PathBuf::from))
}

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
#[expect(
    clippy::cast_precision_loss,
    reason = "manifest ratio validation compares human-facing floating-point summary values"
)]
fn demo_example_runs_during_test_execution() {
    let workspace_root = workspace_root();
    let output = Command::new("cargo")
        .arg("run")
        .arg("--package")
        .arg("mediapm")
        .arg("--example")
        .arg("demo")
        .env("MEDIAPM_DEMO_RUN_SYNC", "false")
        .current_dir(&workspace_root)
        .output()
        .expect("running `cargo run --package mediapm --example demo`");

    assert!(
        output.status.success(),
        "demo example should run successfully during tests\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let manifest_path = manifest_path_from_stdout(&stdout).unwrap_or_else(|| {
        workspace_root.join("src/mediapm/examples/.artifacts/demo/manifest.json")
    });
    assert!(manifest_path.exists(), "demo manifest should be generated");

    let manifest_text = fs::read_to_string(&manifest_path).expect("read demo manifest");
    let manifest_json: Value = serde_json::from_str(&manifest_text).expect("parse demo manifest");

    assert_eq!(manifest_json.get("sync_executed").and_then(Value::as_bool), Some(false));
    assert_eq!(manifest_json.get("configured_tool_count").and_then(Value::as_u64), Some(5));
    assert_eq!(manifest_json.get("configured_step_count").and_then(Value::as_u64), Some(4));
    let without_delta = manifest_json
        .get("store_size_without_delta_bytes")
        .and_then(Value::as_u64)
        .expect("demo manifest should include store_size_without_delta_bytes");
    let with_delta = manifest_json
        .get("store_size_with_delta_bytes")
        .and_then(Value::as_u64)
        .expect("demo manifest should include store_size_with_delta_bytes");
    let ratio = manifest_json
        .get("store_size_ratio_with_delta_over_without")
        .and_then(Value::as_f64)
        .expect("demo manifest should include store_size_ratio_with_delta_over_without");
    let expected_ratio =
        if without_delta == 0 { 1.0 } else { with_delta as f64 / without_delta as f64 };
    assert!(
        (ratio - expected_ratio).abs() <= f64::EPSILON,
        "demo manifest ratio should match with/without store-size math"
    );
    assert!(
        manifest_json
            .get("auto_added_source_title")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "demo manifest should capture non-empty add_local_source title"
    );
    assert!(
        manifest_json
            .get("auto_added_source_description")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "demo manifest should capture non-empty add_local_source description"
    );
}
