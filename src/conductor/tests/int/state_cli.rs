//! Integration coverage for conductor state CLI parse + dispatch contracts.
//!
//! These tests validate externally visible clap/dispatch behavior through the
//! public `run_from_argv` entrypoint so parser contracts remain synchronized
//! with API-backed command wiring.

use std::collections::BTreeMap;

use mediapm_cas::Hash;
use mediapm_conductor::cli::run_from_argv;
use mediapm_conductor::{
    MachineNickelDocument, OutputCaptureSpec, ResolvedInputKey, ToolKindSpec, ToolOutputSpec,
    ToolSpec, UserNickelDocument, WorkflowSpec, WorkflowStepSpec, decode_state,
    encode_machine_document, encode_state, encode_user_document,
};
use tempfile::tempdir;

/// Writes one minimal builtin-echo user/machine config pair used by CLI tests.
fn write_minimal_configs(
    user_path: &std::path::Path,
    machine_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                is_impure: false,
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                outputs: BTreeMap::from([(
                    "result".to_string(),
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: false },
                )]),
                ..ToolSpec::default()
            },
        )]),
        workflows: BTreeMap::from([(
            "default".to_string(),
            WorkflowSpec {
                name: None,
                description: None,
                steps: vec![WorkflowStepSpec {
                    id: "s1".to_string(),
                    tool: "echo@1.0.0".to_string(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::new(),
                }],
            },
        )]),
        ..UserNickelDocument::default()
    };

    std::fs::write(user_path, encode_user_document(user)?)?;
    std::fs::write(machine_path, encode_machine_document(MachineNickelDocument::default())?)?;
    Ok(())
}

/// Returns one argv vector with deterministic config/runtime path flags.
fn cli_argv_with_paths(
    user_path: &std::path::Path,
    machine_path: &std::path::Path,
    conductor_dir: &std::path::Path,
    cas_store_dir: &std::path::Path,
    tail: &[&str],
) -> Vec<String> {
    let mut argv = vec![
        "conductor".to_string(),
        "--config".to_string(),
        user_path.display().to_string(),
        "--config-machine".to_string(),
        machine_path.display().to_string(),
        "--conductor-dir".to_string(),
        conductor_dir.display().to_string(),
        "--cas-store-dir".to_string(),
        cas_store_dir.display().to_string(),
    ];
    argv.extend(tail.iter().map(|value| (*value).to_string()));
    argv
}

/// Returns one platform-available editor command that always exits with
/// non-zero status for edit-flow failure assertions.
#[cfg(windows)]
const FAILING_EDITOR_COMMAND: &str = "cmd /C exit 1";

/// Returns one platform-available editor command that always exits with
/// non-zero status for edit-flow failure assertions.
#[cfg(not(windows))]
const FAILING_EDITOR_COMMAND: &str = "false";

/// Protects `state export` parser requirements by rejecting missing path args.
#[tokio::test]
async fn state_export_requires_path_argument() {
    let error = run_from_argv(["conductor", "state", "export"])
        .await
        .expect_err("state export without path should fail parsing");
    let rendered = error.to_string();
    assert!(
        rendered.contains("required arguments") || rendered.contains("Usage:"),
        "expected clap required-arg parse error, got: {rendered}"
    );
}

/// Protects `state import` parser requirements by rejecting missing path args.
#[tokio::test]
async fn state_import_requires_path_argument() {
    let error = run_from_argv(["conductor", "state", "import"])
        .await
        .expect_err("state import without path should fail parsing");
    let rendered = error.to_string();
    assert!(
        rendered.contains("required arguments") || rendered.contains("Usage:"),
        "expected clap required-arg parse error, got: {rendered}"
    );
}

/// Protects `state edit` parser requirements by rejecting missing editor value.
#[tokio::test]
async fn state_edit_editor_flag_requires_value() {
    let error = run_from_argv(["conductor", "state", "edit", "--editor"])
        .await
        .expect_err("state edit --editor without value should fail parsing");
    let rendered = error.to_string();
    assert!(
        rendered.contains("a value is required") || rendered.contains("Usage:"),
        "expected clap missing-value parse error, got: {rendered}"
    );
}

/// Protects CLI/API parity by ensuring `state export` writes decodable
/// persisted-state JSON for an API-produced workflow state.
#[tokio::test]
async fn state_export_writes_decodable_state_json() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let runtime_root = dir.path().join("runtime");
    let cas_store_dir = runtime_root.join("store");
    let export_path = dir.path().join("state-export.json");
    write_minimal_configs(&user_path, &machine_path).expect("write minimal conductor documents");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["run"],
    ))
    .await
    .expect("run should produce one state snapshot");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "export", export_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state export should succeed");

    let exported_bytes = std::fs::read(&export_path).expect("read exported state");
    let exported = decode_state(&exported_bytes).expect("decode exported state json");
    assert_eq!(exported.instances.len(), 1);
}

/// Protects `state import` validation wiring by rejecting state snapshots that
/// reference tools absent from merged user+machine config.
#[tokio::test]
async fn state_import_rejects_unknown_tool_reference() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let runtime_root = dir.path().join("runtime");
    let cas_store_dir = runtime_root.join("store");
    let state_path = dir.path().join("state-invalid.json");
    write_minimal_configs(&user_path, &machine_path).expect("write minimal conductor documents");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["run"],
    ))
    .await
    .expect("run should produce one state snapshot");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "export", state_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state export should succeed");

    let mut invalid_state = decode_state(&std::fs::read(&state_path).expect("read exported state"))
        .expect("decode exported state");
    let first_key = invalid_state
        .instances
        .keys()
        .next()
        .cloned()
        .expect("exported state should contain one instance");
    invalid_state.instances.get_mut(&first_key).expect("instance should exist").tool_name =
        "missing@1.0.0".to_string();
    std::fs::write(&state_path, encode_state(invalid_state).expect("encode invalid state"))
        .expect("write invalid state");

    let error = run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "import", state_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect_err("state import should reject unknown tool references");
    assert!(error.to_string().contains("references unknown tool"));
}

/// Protects successful import wiring by round-tripping one modified state
/// snapshot through `state import` and re-exporting it for structural checks.
#[tokio::test]
async fn state_import_accepts_valid_modified_state_snapshot() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let runtime_root = dir.path().join("runtime");
    let cas_store_dir = runtime_root.join("store");
    let export_path = dir.path().join("state-export.json");
    let import_path = dir.path().join("state-import.json");
    let reexport_path = dir.path().join("state-reexport.json");
    write_minimal_configs(&user_path, &machine_path).expect("write minimal conductor documents");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["run"],
    ))
    .await
    .expect("run should produce one state snapshot");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "export", export_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state export should succeed");

    let mut imported = decode_state(&std::fs::read(&export_path).expect("read exported state"))
        .expect("decode exported state");
    imported.instances.clear();
    std::fs::write(&import_path, encode_state(imported).expect("encode modified state"))
        .expect("write modified import state");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "import", import_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state import should accept valid modified state");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "export", reexport_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state re-export should succeed after import");

    let reexported = decode_state(&std::fs::read(&reexport_path).expect("read re-exported state"))
        .expect("decode re-exported state");
    assert!(reexported.instances.is_empty());
}

/// Protects decode-failure reporting by rejecting non-JSON import payloads.
#[tokio::test]
async fn state_import_rejects_non_json_payload() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let runtime_root = dir.path().join("runtime");
    let cas_store_dir = runtime_root.join("store");
    let state_path = dir.path().join("state-invalid.json");
    write_minimal_configs(&user_path, &machine_path).expect("write minimal conductor documents");
    std::fs::write(&state_path, b"not-json").expect("write invalid state payload");

    let error = run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "import", state_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect_err("state import should fail on non-JSON payload");
    let rendered = error.to_string();
    assert!(
        rendered.contains("expected") || rendered.contains("line") || rendered.contains("column"),
        "expected decode parse diagnostics, got: {rendered}"
    );
}

/// Protects edit-flow failure reporting when editor command exits non-zero.
#[tokio::test]
async fn state_edit_reports_editor_non_zero_exit() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let runtime_root = dir.path().join("runtime");
    let cas_store_dir = runtime_root.join("store");
    write_minimal_configs(&user_path, &machine_path).expect("write minimal conductor documents");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["run"],
    ))
    .await
    .expect("run should produce one state snapshot");

    let error = run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "edit", "--editor", FAILING_EDITOR_COMMAND],
    ))
    .await
    .expect_err("state edit should fail when editor exits non-zero");
    assert!(error.to_string().contains("non-zero status"));
}

/// Protects import validation for per-instance output names by rejecting
/// snapshots that reference output keys absent from tool spec.
#[tokio::test]
async fn state_import_rejects_unknown_output_reference() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let runtime_root = dir.path().join("runtime");
    let cas_store_dir = runtime_root.join("store");
    let state_path = dir.path().join("state-invalid-output.json");
    write_minimal_configs(&user_path, &machine_path).expect("write minimal conductor documents");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["run"],
    ))
    .await
    .expect("run should produce one state snapshot");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "export", state_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state export should succeed");

    let mut invalid_state = decode_state(&std::fs::read(&state_path).expect("read exported state"))
        .expect("decode exported state");
    let first_key = invalid_state
        .instances
        .keys()
        .next()
        .cloned()
        .expect("exported state should contain one instance");
    let instance = invalid_state.instances.get_mut(&first_key).expect("instance should exist");
    let first_output_hash = instance
        .outputs
        .values()
        .next()
        .map(|output| output.hash)
        .expect("instance should contain one output");
    instance.outputs.insert(
        "rogue_output".to_string(),
        mediapm_conductor::OutputRef {
            hash: first_output_hash,
            persistence: mediapm_conductor::PersistenceFlags::default(),
            allow_empty_capture: false,
        },
    );
    std::fs::write(&state_path, encode_state(invalid_state).expect("encode invalid state"))
        .expect("write invalid state");

    let error = run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "import", state_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect_err("state import should reject unknown output references");
    assert!(error.to_string().contains("references unknown output 'rogue_output'"));
}

/// Protects import validation for per-instance input names by rejecting
/// snapshots that reference undeclared/defaultless inputs.
#[tokio::test]
async fn state_import_rejects_unknown_input_reference() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let runtime_root = dir.path().join("runtime");
    let cas_store_dir = runtime_root.join("store");
    let state_path = dir.path().join("state-invalid-input.json");
    write_minimal_configs(&user_path, &machine_path).expect("write minimal conductor documents");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["run"],
    ))
    .await
    .expect("run should produce one state snapshot");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "export", state_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state export should succeed");

    let mut invalid_state = decode_state(&std::fs::read(&state_path).expect("read exported state"))
        .expect("decode exported state");
    let first_key = invalid_state
        .instances
        .keys()
        .next()
        .cloned()
        .expect("exported state should contain one instance");
    invalid_state.instances.get_mut(&first_key).expect("instance should exist").inputs.insert(
        "rogue_input".to_string(),
        ResolvedInputKey { hash: Hash::from_content(b"rogue-input") },
    );
    std::fs::write(&state_path, encode_state(invalid_state).expect("encode invalid state"))
        .expect("write invalid state");

    let error = run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "import", state_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect_err("state import should reject unknown input references");
    assert!(error.to_string().contains("references unknown input 'rogue_input'"));
}

/// Protects export-path behavior by ensuring nested parent directories are
/// created automatically for state export output files.
#[tokio::test]
async fn state_export_creates_nested_parent_directories() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let runtime_root = dir.path().join("runtime");
    let cas_store_dir = runtime_root.join("store");
    let export_path = dir.path().join("nested").join("deep").join("state-export.json");
    write_minimal_configs(&user_path, &machine_path).expect("write minimal conductor documents");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["run"],
    ))
    .await
    .expect("run should produce one state snapshot");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "export", export_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state export should create nested parent directories");
    assert!(export_path.exists(), "expected exported state file to exist");
}

/// Protects state invalidation wiring by removing exactly one existing
/// deterministic instance entry from persisted orchestration state.
#[tokio::test]
async fn state_invalidate_tool_call_removes_existing_instance() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let runtime_root = dir.path().join("runtime");
    let cas_store_dir = runtime_root.join("store");
    let export_before_path = dir.path().join("state-before.json");
    let export_after_path = dir.path().join("state-after.json");
    write_minimal_configs(&user_path, &machine_path).expect("write minimal conductor documents");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["run"],
    ))
    .await
    .expect("run should produce one state snapshot");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "export", export_before_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state export before invalidation should succeed");

    let exported_before =
        decode_state(&std::fs::read(&export_before_path).expect("read pre-invalidation export"))
            .expect("decode pre-invalidation export");
    let instance_id = exported_before
        .instances
        .keys()
        .next()
        .cloned()
        .expect("workflow run should produce one instance");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "invalidate-tool-call", &instance_id],
    ))
    .await
    .expect("state invalidate-tool-call should succeed for existing id");

    run_from_argv(cli_argv_with_paths(
        &user_path,
        &machine_path,
        &runtime_root,
        &cas_store_dir,
        &["state", "export", export_after_path.to_string_lossy().as_ref()],
    ))
    .await
    .expect("state export after invalidation should succeed");

    let exported_after =
        decode_state(&std::fs::read(&export_after_path).expect("read post-invalidation export"))
            .expect("decode post-invalidation export");
    assert!(
        !exported_after.instances.contains_key(&instance_id),
        "invalidated instance id should no longer exist in state"
    );
}
