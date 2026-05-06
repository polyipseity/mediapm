//! Integration coverage for conductor state CLI parse + dispatch contracts.
//!
//! These tests validate externally visible clap/dispatch behavior through the
//! public `run_from_argv` entrypoint so parser contracts remain synchronized
//! with API-backed command wiring.

use mediapm_conductor::cli::run_from_argv;

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
