//! End-to-end guardrails for offline CLI-focused examples.
//!
//! These tests execute the new examples so they run during automated test
//! passes and verify that each emits a manifest plus generated config files.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use mediapm_conductor::decode_machine_document;
use serde_json::Value;

/// Extracts `manifest: <path>` from example stdout.
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

/// Runs one mediapm example and returns combined process output.
fn run_example(example_name: &str) -> std::process::Output {
    Command::new("cargo")
        .arg("run")
        .arg("--package")
        .arg("mediapm")
        .arg("--example")
        .arg(example_name)
        .current_dir(workspace_root())
        .output()
        .expect("running mediapm CLI example should succeed")
}

/// Verifies source-add example runs and emits inspectable config artifacts.
#[test]
fn cli_add_sources_example_runs_and_writes_manifest() {
    let output = run_example("mediapm_cli_add_sources");
    assert!(
        output.status.success(),
        "example should run successfully\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let manifest_path = manifest_path_from_stdout(&stdout).unwrap_or_else(|| {
        workspace_root().join("src/mediapm/examples/.artifacts/cli-add-sources/manifest.json")
    });

    assert!(manifest_path.exists(), "example manifest should exist");

    let manifest_text = fs::read_to_string(&manifest_path).expect("read add-sources manifest");
    let manifest_json: Value = serde_json::from_str(&manifest_text).expect("parse manifest json");

    for key in ["mediapm_ncl", "conductor_user_ncl", "conductor_machine_ncl"] {
        let path = manifest_json
            .get(key)
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .expect("manifest should include config path");
        assert!(path.exists(), "manifest path '{key}' should exist");
    }

    assert!(
        manifest_json
            .get("local_media_id")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "manifest should include non-empty local media id"
    );
    assert!(
        manifest_json
            .get("remote_media_id")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "manifest should include non-empty remote media id"
    );

    let machine_path = manifest_json
        .get("conductor_machine_ncl")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .expect("manifest should include conductor machine path");
    let machine = decode_machine_document(&fs::read(machine_path).expect("read conductor machine"))
        .expect("decode conductor machine document");

    let local_media_id = manifest_json
        .get("local_media_id")
        .and_then(Value::as_str)
        .expect("manifest should include local media id");
    let remote_media_id = manifest_json
        .get("remote_media_id")
        .and_then(Value::as_str)
        .expect("manifest should include remote media id");
    for workflow_id in
        [format!("mediapm.media.{local_media_id}"), format!("mediapm.media.{remote_media_id}")]
    {
        assert!(
            machine.workflows.contains_key(&workflow_id),
            "conductor machine should contain managed workflow '{workflow_id}'"
        );
    }
}

// Verifies hierarchy example runs and emits inspectable config artifacts.
#[test]
fn cli_add_hierarchy_example_runs_and_writes_manifest() {
    let output = run_example("mediapm_cli_add_hierarchy");
    assert!(
        output.status.success(),
        "example should run successfully\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let manifest_path = manifest_path_from_stdout(&stdout).unwrap_or_else(|| {
        workspace_root().join("src/mediapm/examples/.artifacts/cli-add-hierarchy/manifest.json")
    });

    assert!(manifest_path.exists(), "example manifest should exist");

    let manifest_text = fs::read_to_string(&manifest_path).expect("read hierarchy manifest");
    let manifest_json: Value = serde_json::from_str(&manifest_text).expect("parse manifest json");

    for key in ["mediapm_ncl", "conductor_user_ncl", "conductor_machine_ncl"] {
        let path = manifest_json
            .get(key)
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .expect("manifest should include config path");
        assert!(path.exists(), "manifest path '{key}' should exist");
    }

    assert_eq!(
        manifest_json.get("hierarchy_node_count").and_then(Value::as_u64),
        Some(2),
        "hierarchy-presets example should add one node per media source"
    );

    for key in ["local_hierarchy_folder", "yt_dlp_hierarchy_folder"] {
        assert!(
            manifest_json
                .get(key)
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty()),
            "manifest should include non-empty '{key}'"
        );
    }

    let machine_path = manifest_json
        .get("conductor_machine_ncl")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .expect("manifest should include conductor machine path");
    let machine = decode_machine_document(&fs::read(machine_path).expect("read conductor machine"))
        .expect("decode conductor machine document");

    let local_media_id = manifest_json
        .get("local_media_id")
        .and_then(Value::as_str)
        .expect("manifest should include local media id");
    let remote_media_id = manifest_json
        .get("remote_media_id")
        .and_then(Value::as_str)
        .expect("manifest should include remote media id");
    for workflow_id in
        [format!("mediapm.media.{local_media_id}"), format!("mediapm.media.{remote_media_id}")]
    {
        assert!(
            machine.workflows.contains_key(&workflow_id),
            "conductor machine should contain managed workflow '{workflow_id}'"
        );
    }
}

// Verifies tools example runs and emits inspectable config artifacts.
#[test]
fn add_tools_example_runs_and_writes_manifest() {
    let output = run_example("mediapm_cli_add_tools");
    assert!(
        output.status.success(),
        "example should run successfully\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let manifest_path = manifest_path_from_stdout(&stdout).unwrap_or_else(|| {
        workspace_root().join("src/mediapm/examples/.artifacts/cli-add-tools/manifest.json")
    });

    assert!(manifest_path.exists(), "example manifest should exist");

    let manifest_text = fs::read_to_string(&manifest_path).expect("read add-tools manifest");
    let manifest_json: Value = serde_json::from_str(&manifest_text).expect("parse manifest json");

    for key in ["mediapm_ncl", "conductor_user_ncl", "conductor_machine_ncl"] {
        let path = manifest_json
            .get(key)
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .expect("manifest should include config path");
        assert!(path.exists(), "manifest path '{key}' should exist");
    }

    let logical_tool_names = manifest_json
        .get("logical_tool_names")
        .and_then(Value::as_array)
        .expect("manifest should include logical tool names");
    assert!(
        !logical_tool_names.is_empty(),
        "manifest should include at least one logical tool name"
    );

    let tool_ids = manifest_json
        .get("tool_ids")
        .and_then(Value::as_array)
        .expect("manifest should include tool ids");
    assert_eq!(tool_ids.len(), logical_tool_names.len());

    let machine_path = manifest_json
        .get("conductor_machine_ncl")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .expect("manifest should include conductor machine path");
    let machine = decode_machine_document(&fs::read(machine_path).expect("read conductor machine"))
        .expect("decode conductor machine document");

    let mediapm_path = manifest_json
        .get("mediapm_ncl")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .expect("manifest should include mediapm path");
    let document = mediapm::load_mediapm_document(&mediapm_path).expect("load mediapm document");
    assert!(document.media.is_empty(), "tools example should leave media empty");
    assert_eq!(document.tools.len(), tool_ids.len());

    for value in tool_ids {
        let tool_id = value.as_str().expect("tool id should be a string");
        assert!(
            machine.tools.contains_key(tool_id),
            "conductor machine should contain tool '{tool_id}'"
        );
        assert!(
            machine
                .tool_configs
                .get(tool_id)
                .and_then(|config| config.content_map.as_ref())
                .is_some_and(|content_map| !content_map.is_empty()),
            "conductor machine should contain dummy content-map entries for tool '{tool_id}'"
        );
    }
}
