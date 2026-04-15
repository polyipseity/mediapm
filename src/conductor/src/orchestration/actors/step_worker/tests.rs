//! Step-worker tests for `step_worker/mod.rs`.
//!
//! This module protects template semantics, builtin dispatch contracts, and
//! output-capture behavior for the actor-backed step worker.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mediapm_cas::{CasApi, InMemoryCas, empty_content_hash};

use crate::error::ConductorError;
use crate::model::config::{
    ImpureTimestamp, InputBinding, ProcessSpec, ToolInputKind, ToolInputSpec, ToolKindSpec,
    ToolOutputSpec, ToolSpec, WorkflowStepSpec,
};
use crate::model::state::{PersistenceFlags, ResolvedInput};
use crate::orchestration::protocol::{UnifiedNickelDocument, UnifiedToolSpec};

use super::{ResolvedOutputCapture, ResolvedOutputSpec, StepWorkerExecutor, ToolExecutionCapture};

/// Builds one ZIP payload for template-selector tests.
fn build_test_zip_payload(entry_relative_path: &str, entry_content: &[u8]) -> Vec<u8> {
    let temp = tempfile::tempdir().expect("tempdir");
    let source_dir = temp.path().join("source");
    let source_file = source_dir.join(entry_relative_path);
    if let Some(parent) = source_file.parent() {
        std::fs::create_dir_all(parent).expect("create zip source parent");
    }
    std::fs::write(&source_file, entry_content).expect("write zip source file");

    mediapm_conductor_builtin_archive::pack_directory_to_uncompressed_zip_bytes(&source_dir, false)
        .expect("build test zip payload")
}

/// Protects deterministic instance-key derivation across repeated calls.
#[test]
fn derived_keys_are_deterministic() {
    let metadata = ToolSpec {
        is_impure: false,
        kind: ToolKindSpec::Executable {
            command: vec!["bin/tool".to_string(), "a".to_string(), "1".to_string()],
            env_vars: BTreeMap::from([("RUST_LOG".to_string(), "info".to_string())]),
            success_codes: vec![0],
        },
        ..ToolSpec::default()
    };
    let inputs =
        BTreeMap::from([("input".to_string(), ResolvedInput::from_plain_content(b"abc".to_vec()))]);

    let impure_timestamp = Some(ImpureTimestamp { epoch_seconds: 12, subsec_nanos: 34 });
    let key_a = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
        "echo@1.0.0",
        &metadata,
        impure_timestamp,
        &inputs,
    )
    .expect("derive instance key");
    let key_b = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
        "echo@1.0.0",
        &metadata,
        impure_timestamp,
        &inputs,
    )
    .expect("derive instance key");
    assert_eq!(key_a, key_b);
}

/// Protects tool-name participation in deterministic instance-key derivation.
#[test]
fn derived_keys_include_tool_name() {
    let metadata = ToolSpec {
        kind: ToolKindSpec::Builtin { name: "echo".to_string(), version: "1.0.0".to_string() },
        ..ToolSpec::default()
    };
    let inputs =
        BTreeMap::from([("text".to_string(), ResolvedInput::from_plain_content(b"abc".to_vec()))]);

    let key_a = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
        "echo@1.0.0",
        &metadata,
        None,
        &inputs,
    )
    .expect("derive key for first tool name");
    let key_b = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
        "echo@2.0.0",
        &metadata,
        None,
        &inputs,
    )
    .expect("derive key for second tool name");

    assert_ne!(key_a, key_b);
}

/// Protects builtin identity-only metadata projection in instance-key derivation.
#[test]
fn derived_keys_for_builtin_ignore_non_identity_metadata_fields() {
    let builtin_minimal = ToolSpec {
        is_impure: false,
        inputs: BTreeMap::new(),
        kind: ToolKindSpec::Builtin { name: "echo".to_string(), version: "1.0.0".to_string() },
        outputs: BTreeMap::new(),
    };
    let builtin_verbose = ToolSpec {
        is_impure: true,
        inputs: BTreeMap::from([("text".to_string(), ToolInputSpec::default())]),
        kind: ToolKindSpec::Builtin { name: "echo".to_string(), version: "1.0.0".to_string() },
        outputs: BTreeMap::from([("result".to_string(), ToolOutputSpec::default())]),
    };
    let inputs =
        BTreeMap::from([("text".to_string(), ResolvedInput::from_plain_content(b"abc".to_vec()))]);

    let minimal_key = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
        "echo@1.0.0",
        &builtin_minimal,
        None,
        &inputs,
    )
    .expect("derive minimal builtin key");
    let verbose_key = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
        "echo@1.0.0",
        &builtin_verbose,
        None,
        &inputs,
    )
    .expect("derive verbose builtin key");

    assert_eq!(minimal_key, verbose_key);
}

/// Protects support for bare-identifier template interpolation.
#[test]
fn template_interpolation_supports_bare_identifier() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "subject".to_string(),
        ResolvedInput::from_plain_content(b"world".to_vec()),
    )]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value("hello ${subject}", &inputs, &mut pending_file_writes)
        .expect("bare identifier interpolation should resolve");

    assert_eq!(rendered, "hello world");
    assert!(pending_file_writes.is_empty());
}

/// Protects support for JavaScript-style bracket selector interpolation.
#[test]
fn template_interpolation_supports_inputs_bracket_notation() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "subject".to_string(),
        ResolvedInput::from_plain_content(b"world".to_vec()),
    )]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value("hello ${inputs[\"subject\"]}", &inputs, &mut pending_file_writes)
        .expect("bracket interpolation should resolve");

    assert_eq!(rendered, "hello world");
    assert!(pending_file_writes.is_empty());
}

/// Protects policy that context selectors are no longer supported.
#[test]
fn template_interpolation_rejects_context_selector() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::new();
    let mut pending_file_writes = Vec::new();

    let error = executor
        .render_template_value(
            "${context.config_dir}/notes/report.txt",
            &inputs,
            &mut pending_file_writes,
        )
        .expect_err("context selector should fail");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("unsupported template expression"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects explicit failure for removed `${context.config_dir}` input-binding syntax.
#[tokio::test]
async fn resolve_input_binding_rejects_context_config_dir() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let workflow_step = WorkflowStepSpec {
        id: "step".to_string(),
        tool: "echo@1.0.0".to_string(),
        inputs: BTreeMap::new(),
        depends_on: Vec::new(),
        outputs: BTreeMap::new(),
    };
    let unified = UnifiedNickelDocument {
        external_data: BTreeMap::new(),
        tools: BTreeMap::new(),
        workflows: BTreeMap::new(),
        tool_content_hashes: BTreeSet::new(),
    };
    let error = executor
        .resolve_input_binding(
            &unified,
            "wf",
            &workflow_step,
            "${context.config_dir}",
            &BTreeMap::new(),
        )
        .await
        .expect_err("context.config_dir binding should fail");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("unsupported input binding expression"));
            assert!(message.contains("context.config_dir"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects explicit failure on unsupported expression syntax.
#[test]
fn template_interpolation_rejects_unsupported_expression() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::new();
    let mut pending_file_writes = Vec::new();

    let error = executor
        .render_template_value("${foo.bar}", &inputs, &mut pending_file_writes)
        .expect_err("unsupported expression should fail");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("unsupported template expression"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects literal escaping of `\${...}` markers.
#[test]
fn template_interpolation_supports_js_escape_for_literal_start() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "subject".to_string(),
        ResolvedInput::from_plain_content(b"world".to_vec()),
    )]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value(
            r"show \${subject} and ${subject}",
            &inputs,
            &mut pending_file_writes,
        )
        .expect("escaped interpolation marker should render literally");

    assert_eq!(rendered, "show ${subject} and world");
    assert!(pending_file_writes.is_empty());
}

/// Protects `${<left> <op> <right>?<true>|<false>}` conditional semantics.
#[test]
fn template_interpolation_supports_comparison_conditional_expression() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::new();
    let mut pending_file_writes = Vec::new();

    let current = "${context.os == \"windows\" ? win | nonwin}";

    let rendered = executor
        .render_template_value(current, &inputs, &mut pending_file_writes)
        .expect("comparison conditional should render selected branch");

    let expected = if cfg!(windows) { "win" } else { "nonwin" };
    assert_eq!(rendered, expected);
    assert!(pending_file_writes.is_empty());
}

/// Protects truthiness conditional semantics for `${<operand>?<true>|<false>}`
/// template expressions.
#[test]
fn template_interpolation_supports_truthy_conditional_expression() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "subject".to_string(),
        ResolvedInput::from_plain_content(b"world".to_vec()),
    )]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value(
            "${inputs.subject ? has-subject | no-subject}",
            &inputs,
            &mut pending_file_writes,
        )
        .expect("truthy conditional should render selected branch");

    assert_eq!(rendered, "has-subject");
    assert!(pending_file_writes.is_empty());
}

/// Protects comparison conditional support for single-item list inputs used by
/// value-centric option transport.
#[test]
fn template_interpolation_supports_comparison_against_single_item_list_input() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "switch".to_string(),
        ResolvedInput::from_string_list(vec!["true".to_string()]).expect("build list input"),
    )]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value(
            "${inputs.switch == \"true\" ? enabled | disabled}",
            &inputs,
            &mut pending_file_writes,
        )
        .expect("single-item list comparison should render selected branch");

    assert_eq!(rendered, "enabled");
    assert!(pending_file_writes.is_empty());
}

/// Protects deterministic errors for ambiguous comparison operands sourced
/// from multi-item list inputs.
#[test]
fn template_interpolation_rejects_comparison_against_multi_item_list_input() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "switch".to_string(),
        ResolvedInput::from_string_list(vec!["true".to_string(), "false".to_string()])
            .expect("build list input"),
    )]);
    let mut pending_file_writes = Vec::new();

    let error = executor
        .render_template_value(
            "${inputs.switch == \"true\" ? enabled | disabled}",
            &inputs,
            &mut pending_file_writes,
        )
        .expect_err("multi-item list comparison should fail");

    assert!(
        format!("{error}").contains("comparisons support at most one list item"),
        "error should describe list-size comparison constraint"
    );
}

/// Protects recursive selector/materialization support inside comparison
/// values.
#[test]
fn template_interpolation_supports_special_forms_inside_conditional_branches() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "subject".to_string(),
        ResolvedInput::from_plain_content(b"world".to_vec()),
    )]);
    let mut pending_file_writes = Vec::new();

    let current = "${context.os == \"windows\" ? subject:file(runtime/windows-subject.txt) | subject:file(runtime/other-subject.txt)}";

    let rendered = executor
        .render_template_value(current, &inputs, &mut pending_file_writes)
        .expect("matching conditional should resolve special-form value recursively");

    let expected_path =
        if cfg!(windows) { "runtime/windows-subject.txt" } else { "runtime/other-subject.txt" };
    assert_eq!(rendered.replace('\\', "/"), expected_path);
    assert_eq!(pending_file_writes.len(), 1);
    assert_eq!(
        pending_file_writes[0].relative_path.to_string_lossy().replace('\\', "/"),
        expected_path
    );
    assert_eq!(pending_file_writes[0].plain_content, b"world".to_vec());
}

/// Protects omission of conditional branches that resolve to empty output.
#[test]
fn command_render_omits_conditionals_with_empty_false_branch() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::new();
    let mut pending_file_writes = Vec::new();

    let non_matching = if cfg!(windows) {
        "${context.os == \"linux\" ? --linux-only | ''}".to_string()
    } else {
        "${context.os == \"windows\" ? --windows-only | ''}".to_string()
    };
    let command = vec!["tool".to_string(), non_matching, "--always".to_string()];

    let rendered = executor
        .render_template_command(&command, &inputs, &mut pending_file_writes)
        .expect("command rendering should succeed");

    assert_eq!(rendered, vec!["tool".to_string(), "--always".to_string()]);
}

/// Protects standalone command unpack token expansion for list-of-strings
/// inputs.
#[test]
fn command_render_expands_standalone_unpack_token_for_list_input() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "argv".to_string(),
        ResolvedInput::from_string_list(vec!["--alpha".to_string(), "--beta".to_string()])
            .expect("build list input"),
    )]);
    let mut pending_file_writes = Vec::new();

    let command = vec!["tool".to_string(), "${*inputs.argv}".to_string(), "--tail".to_string()];
    let rendered = executor
        .render_template_command(&command, &inputs, &mut pending_file_writes)
        .expect("command unpack token should expand");

    assert_eq!(
        rendered,
        vec!["tool".to_string(), "--alpha".to_string(), "--beta".to_string(), "--tail".to_string(),]
    );
}

/// Protects scalar unpack behavior for standalone command tokens.
#[test]
fn command_render_expands_unpack_token_for_scalar_input() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "argv".to_string(),
        ResolvedInput::from_plain_content(b"--single".to_vec()),
    )]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_command(
            &["tool".to_string(), "${*inputs.argv}".to_string(), "--tail".to_string()],
            &inputs,
            &mut pending_file_writes,
        )
        .expect("scalar unpack token should expand to one argument");

    assert_eq!(rendered, vec!["tool".to_string(), "--single".to_string(), "--tail".to_string()]);
}

/// Protects conditional unpack support so command templates can include
/// optional key/value argv pairs without mediapm-specific preprocessing.
#[test]
fn command_render_supports_conditional_unpack_key_value_pair() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let mut pending_file_writes = Vec::new();

    let with_value_inputs = BTreeMap::from([(
        "test".to_string(),
        ResolvedInput::from_plain_content(b"alpha".to_vec()),
    )]);
    let command = vec![
        "tool".to_string(),
        "${*inputs.test ? --test | ''}".to_string(),
        "${*inputs.test}".to_string(),
    ];
    let rendered_with_value = executor
        .render_template_command(&command, &with_value_inputs, &mut pending_file_writes)
        .expect("conditional unpack command should render key/value pair when value is present");
    assert_eq!(
        rendered_with_value,
        vec!["tool".to_string(), "--test".to_string(), "alpha".to_string()]
    );

    let empty_value_inputs =
        BTreeMap::from([("test".to_string(), ResolvedInput::from_plain_content(Vec::new()))]);
    let rendered_without_value = executor
        .render_template_command(&command, &empty_value_inputs, &mut pending_file_writes)
        .expect("conditional unpack command should omit key/value pair when value is empty");
    assert_eq!(rendered_without_value, vec!["tool".to_string()]);
}

/// Protects standalone conditional-unpack semantics for list-valued inputs so
/// `${*...}` remains compatible with both scalar and list bindings.
#[test]
fn command_render_supports_conditional_unpack_with_list_input() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let mut pending_file_writes = Vec::new();
    let command = vec![
        "tool".to_string(),
        "${*inputs.argv ? --has-args | ''}".to_string(),
        "${*inputs.argv}".to_string(),
    ];

    let list_inputs = BTreeMap::from([(
        "argv".to_string(),
        ResolvedInput::from_string_list(vec!["--alpha".to_string(), "--beta".to_string()])
            .expect("build list input"),
    )]);
    let rendered_with_list = executor
        .render_template_command(&command, &list_inputs, &mut pending_file_writes)
        .expect("conditional unpack should render list flag and unpacked list values");
    assert_eq!(
        rendered_with_list,
        vec![
            "tool".to_string(),
            "--has-args".to_string(),
            "--alpha".to_string(),
            "--beta".to_string(),
        ]
    );

    let empty_list_inputs = BTreeMap::from([(
        "argv".to_string(),
        ResolvedInput::from_string_list(Vec::new()).expect("build empty list input"),
    )]);
    let rendered_without_list = executor
        .render_template_command(&command, &empty_list_inputs, &mut pending_file_writes)
        .expect("conditional unpack should omit list flag and values when list input is empty");
    assert_eq!(rendered_without_list, vec!["tool".to_string()]);
}

/// Protects plain `context.os` selector rendering.
#[test]
fn template_interpolation_supports_context_os_selector() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::new();
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value("${context.os}", &inputs, &mut pending_file_writes)
        .expect("context selector should render");

    let expected = if cfg!(windows) {
        "windows"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else {
        "linux"
    };
    assert_eq!(rendered, expected);
}

/// Protects syntax rule that `${*...}` unpack expressions must occupy the
/// entire command argument (standalone token only).
#[test]
fn command_render_rejects_non_standalone_unpack_expression() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "argv".to_string(),
        ResolvedInput::from_string_list(vec!["--alpha".to_string()]).expect("build list input"),
    )]);
    let mut pending_file_writes = Vec::new();

    let error = executor
        .render_template_command(
            &["tool".to_string(), "prefix-${*inputs.argv}".to_string()],
            &inputs,
            &mut pending_file_writes,
        )
        .expect_err("non-standalone unpack expression should fail");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("only valid as a standalone executable command argument"));
            assert!(message.contains("${*inputs.argv}"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects policy that list-typed inputs are invalid in normal `${...}`
/// interpolation and must use standalone unpack tokens.
#[test]
fn template_interpolation_rejects_list_input_outside_unpack_token() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "argv".to_string(),
        ResolvedInput::from_string_list(vec!["--alpha".to_string()]).expect("build list input"),
    )]);
    let mut pending_file_writes = Vec::new();

    let error = executor
        .render_template_value("prefix-${inputs.argv}", &inputs, &mut pending_file_writes)
        .expect_err("list interpolation outside unpack token should fail");

    match error {
        ConductorError::Workflow(message) => {
            assert!(
                message.contains("list inputs are only valid in standalone command unpack tokens")
            );
            assert!(message.contains("inputs.argv"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects runtime executable input-kind validation when programmatic callers
/// bypass config decoding helpers.
#[tokio::test]
async fn resolve_inputs_rejects_executable_input_kind_mismatch() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let unified = UnifiedNickelDocument {
        external_data: BTreeMap::new(),
        tools: BTreeMap::new(),
        workflows: BTreeMap::new(),
        tool_content_hashes: BTreeSet::new(),
    };
    let tool = UnifiedToolSpec {
        is_impure: false,
        max_concurrent_calls: -1,
        max_retries: 0,
        inputs: BTreeMap::from([(
            "argv".to_string(),
            ToolInputSpec { kind: ToolInputKind::StringList },
        )]),
        default_inputs: BTreeMap::new(),
        process: ProcessSpec::Executable {
            command: vec!["bin/tool".to_string(), "${*inputs.argv}".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        execution_env_vars: BTreeMap::new(),
        outputs: BTreeMap::from([("result".to_string(), ToolOutputSpec::default())]),
        tool_content_map: BTreeMap::new(),
    };
    let step = WorkflowStepSpec {
        id: "step".to_string(),
        tool: "tool_exec@1.0.0".to_string(),
        inputs: BTreeMap::from([(
            "argv".to_string(),
            InputBinding::String("--scalar-value".to_string()),
        )]),
        depends_on: Vec::new(),
        outputs: BTreeMap::new(),
    };

    let error = executor
        .resolve_inputs(&unified, &tool, "wf", &step, &BTreeMap::new())
        .await
        .expect_err("kind mismatch should fail");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("expects kind 'string_list'"));
            assert!(message.contains("received 'string'"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects JavaScript-style escape decoding in literal template spans.
#[test]
fn template_interpolation_applies_js_string_escapes() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "subject".to_string(),
        ResolvedInput::from_plain_content(b"world".to_vec()),
    )]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value(r"line1\nline2 ${subject}", &inputs, &mut pending_file_writes)
        .expect("js escape sequences should decode");

    assert_eq!(rendered, "line1\nline2 world");
    assert!(pending_file_writes.is_empty());
}

/// Protects explicit failure on unsupported escape sequences.
#[test]
fn template_interpolation_rejects_unsupported_escape_sequence() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::new();
    let mut pending_file_writes = Vec::new();

    let error = executor
        .render_template_value(r"bad\q", &inputs, &mut pending_file_writes)
        .expect_err("unsupported escape should fail");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("unsupported escape sequence"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects deferred file materialization during planning.
#[test]
fn template_file_materialization_is_deferred_until_execution() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let inputs = BTreeMap::from([(
        "subject".to_string(),
        ResolvedInput::from_plain_content(b"world".to_vec()),
    )]);
    let temp = tempfile::tempdir().expect("tempdir");
    let deferred_path = temp.path().join("runtime").join("subject.txt");
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value(
            "${subject:file(runtime/subject.txt)}",
            &inputs,
            &mut pending_file_writes,
        )
        .expect("file materialization token should resolve");

    assert_eq!(pending_file_writes.len(), 1);
    assert_eq!(
        pending_file_writes[0].relative_path,
        std::path::PathBuf::from("runtime").join("subject.txt")
    );
    assert_eq!(pending_file_writes[0].plain_content, b"world".to_vec());
    assert!(!deferred_path.exists(), "planning should not write files before execution");
    assert!(rendered.ends_with("subject.txt"));
}

/// Protects ZIP-entry selector extraction into inline template text.
#[test]
fn template_zip_selector_extracts_zip_entry_content() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let zip_bytes = build_test_zip_payload("nested/file.txt", b"hello-from-zip");
    let inputs =
        BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value(
            "${inputs.archive:zip(nested/file.txt)}",
            &inputs,
            &mut pending_file_writes,
        )
        .expect("zip selector should extract entry content");

    assert_eq!(rendered, "hello-from-zip");
    assert!(pending_file_writes.is_empty());
}

/// Protects ZIP-entry selector chaining with deferred file materialization.
#[test]
fn template_zip_selector_can_materialize_entry_to_file_path() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let zip_bytes = build_test_zip_payload("nested/file.txt", b"zip-file-content");
    let inputs =
        BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value(
            "${inputs.archive:zip(nested/file.txt):file(runtime/from_zip.txt)}",
            &inputs,
            &mut pending_file_writes,
        )
        .expect("zip selector + file materialization should resolve");

    assert_eq!(rendered.replace('\\', "/"), "runtime/from_zip.txt");
    assert_eq!(pending_file_writes.len(), 1);
    assert_eq!(
        pending_file_writes[0].relative_path,
        std::path::PathBuf::from("runtime").join("from_zip.txt")
    );
    assert_eq!(pending_file_writes[0].plain_content, b"zip-file-content".to_vec());
}

/// Protects explicit failure when a ZIP selector resolves to a directory
/// without opting into `:folder(...)` materialization.
#[test]
fn template_zip_selector_rejects_directory_without_folder_directive() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let zip_bytes = build_test_zip_payload("nested/file.txt", b"zip-dir-content");
    let inputs =
        BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
    let mut pending_file_writes = Vec::new();

    let error = executor
        .render_template_value("${inputs.archive:zip(nested)}", &inputs, &mut pending_file_writes)
        .expect_err("zip directory selector should require :folder(...) materialization");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("resolved 'nested' to a directory"));
            assert!(message.contains(":folder(<relative_path>)"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
    assert!(pending_file_writes.is_empty());
}

/// Protects deferred directory materialization when ZIP selection resolves
/// to one directory and the token opts into `:folder(...)`.
#[test]
fn template_zip_selector_can_materialize_directory_to_folder_path() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let zip_bytes = build_test_zip_payload("nested/file.txt", b"zip-dir-content");
    let inputs =
        BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
    let mut pending_file_writes = Vec::new();

    let rendered = executor
        .render_template_value(
            "${inputs.archive:zip(nested):folder(runtime/from_zip)}",
            &inputs,
            &mut pending_file_writes,
        )
        .expect("zip directory selector + folder materialization should resolve");

    assert_eq!(rendered.replace('\\', "/"), "runtime/from_zip");
    assert_eq!(pending_file_writes.len(), 1);
    assert_eq!(
        pending_file_writes[0].relative_path,
        std::path::PathBuf::from("runtime").join("from_zip").join("file.txt")
    );
    assert_eq!(pending_file_writes[0].plain_content, b"zip-dir-content".to_vec());
}

/// Protects explicit failure when `:folder(...)` is requested for a ZIP
/// selector that resolves to a regular file.
#[test]
fn template_zip_selector_folder_materialization_rejects_file_entries() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let zip_bytes = build_test_zip_payload("nested/file.txt", b"zip-file-content");
    let inputs =
        BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
    let mut pending_file_writes = Vec::new();

    let error = executor
        .render_template_value(
            "${inputs.archive:zip(nested/file.txt):folder(runtime/from_zip)}",
            &inputs,
            &mut pending_file_writes,
        )
        .expect_err("zip file selector should reject :folder(...) materialization");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("resolved 'nested/file.txt' to a file"));
            assert!(message.contains("expected a directory for :folder(...)"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
    assert!(pending_file_writes.is_empty());
}

/// Protects sandbox enforcement for tool-relative paths.
#[test]
fn tool_relative_paths_reject_absolute_and_escape_components() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };

    let absolute = if cfg!(windows) { r"C:\\escape.txt" } else { "/escape.txt" };
    let absolute_error = executor
        .normalized_relative_tool_path(absolute, "test")
        .expect_err("absolute tool path should fail");
    match absolute_error {
        ConductorError::Workflow(message) => assert!(message.contains("must be relative")),
        other => panic!("expected workflow error, got {other:?}"),
    }

    let escape_error = executor
        .normalized_relative_tool_path("../escape.txt", "test")
        .expect_err("parent traversal should fail");
    match escape_error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("must not escape the tool sandbox"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects regular file materialization for `content_map` entries whose keys
/// do not end with `/` or `\\`.
#[tokio::test]
async fn content_map_file_entry_materializes_plain_file_bytes() {
    let cas = Arc::new(InMemoryCas::new());
    let payload = b"#!/usr/bin/env sh\necho from-content-map\n".to_vec();
    let hash = cas.put(payload.clone()).await.expect("store payload in CAS");
    let executor = StepWorkerExecutor { cas };
    let temp = tempfile::tempdir().expect("tempdir");

    executor
        .materialize_tool_content_map(
            &BTreeMap::from([("bin/run.sh".to_string(), hash)]),
            temp.path(),
        )
        .await
        .expect("file-form content_map entry should materialize bytes");

    assert_eq!(
        std::fs::read(temp.path().join("bin").join("run.sh")).expect("read output"),
        payload
    );
}

/// Protects directory materialization semantics for trailing-slash
/// `content_map` keys where CAS payloads are ZIP archives.
#[tokio::test]
async fn content_map_directory_entry_unpacks_zip_payload() {
    let cas = Arc::new(InMemoryCas::new());
    let zip_payload = build_test_zip_payload("bin/run.sh", b"echo from zip\n");
    let hash = cas.put(zip_payload).await.expect("store zip payload in CAS");
    let executor = StepWorkerExecutor { cas };
    let temp = tempfile::tempdir().expect("tempdir");

    executor
        .materialize_tool_content_map(&BTreeMap::from([("tool/".to_string(), hash)]), temp.path())
        .await
        .expect("directory-form content_map entry should unpack ZIP");

    assert_eq!(
        std::fs::read_to_string(temp.path().join("tool").join("bin").join("run.sh"))
            .expect("read unpacked script"),
        "echo from zip\n"
    );
}

/// Protects support for `./` as a directory-form key that unpacks directly
/// into the execution sandbox root.
#[tokio::test]
async fn content_map_directory_entry_accepts_current_directory_root() {
    let cas = Arc::new(InMemoryCas::new());
    let zip_payload = build_test_zip_payload("bin/run.sh", b"echo from zip\n");
    let hash = cas.put(zip_payload).await.expect("store zip payload in CAS");
    let executor = StepWorkerExecutor { cas };
    let temp = tempfile::tempdir().expect("tempdir");

    executor
        .materialize_tool_content_map(&BTreeMap::from([("./".to_string(), hash)]), temp.path())
        .await
        .expect("'./' directory-form content_map entry should unpack ZIP at sandbox root");

    assert_eq!(
        std::fs::read_to_string(temp.path().join("bin").join("run.sh"))
            .expect("read unpacked root script"),
        "echo from zip\n"
    );
}

/// Protects ZIP validation for trailing-slash `content_map` directory keys.
#[tokio::test]
async fn content_map_directory_entry_rejects_non_zip_payload() {
    let cas = Arc::new(InMemoryCas::new());
    let hash = cas.put(b"not-a-zip".to_vec()).await.expect("store plain payload in CAS");
    let executor = StepWorkerExecutor { cas };
    let temp = tempfile::tempdir().expect("tempdir");

    let error = executor
        .materialize_tool_content_map(&BTreeMap::from([("tool/".to_string(), hash)]), temp.path())
        .await
        .expect_err("directory-form content_map entry should require ZIP payload");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("expects ZIP payload"));
            assert!(message.contains("tool/"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects explicit failure for trailing-slash directory keys that do not
/// include any concrete path component.
#[tokio::test]
async fn content_map_directory_entry_requires_non_empty_prefix() {
    let cas = Arc::new(InMemoryCas::new());
    let zip_payload = build_test_zip_payload("nested/file.txt", b"x");
    let hash = cas.put(zip_payload).await.expect("store zip payload in CAS");
    let executor = StepWorkerExecutor { cas };
    let temp = tempfile::tempdir().expect("tempdir");

    let error = executor
        .materialize_tool_content_map(&BTreeMap::from([("/".to_string(), hash)]), temp.path())
        .await
        .expect_err("root-only directory key should fail");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("must contain at least one path component"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects collision safety across separate `content_map` entries.
#[tokio::test]
async fn content_map_rejects_file_overwrite_between_entries() {
    let cas = Arc::new(InMemoryCas::new());
    let directory_zip = build_test_zip_payload("run.sh", b"echo from dir\n");
    let directory_hash = cas.put(directory_zip).await.expect("store dir zip payload");
    let file_hash =
        cas.put(b"#!/usr/bin/env sh\necho from file\n".to_vec()).await.expect("store file payload");
    let executor = StepWorkerExecutor { cas };
    let temp = tempfile::tempdir().expect("tempdir");

    let error = executor
        .materialize_tool_content_map(
            &BTreeMap::from([
                ("tool/".to_string(), directory_hash),
                ("tool/run.sh".to_string(), file_hash),
            ]),
            temp.path(),
        )
        .await
        .expect_err("conflicting content_map entries should fail before writes");

    match error {
        ConductorError::Workflow(message) => {
            assert!(message.contains("tool/"));
            assert!(message.contains("tool/run.sh"));
            assert!(message.contains("both materialize"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects merged directory behavior when entries target different files.
#[tokio::test]
async fn content_map_allows_distinct_paths_across_directory_entries() {
    let cas = Arc::new(InMemoryCas::new());
    let first_zip = build_test_zip_payload("a.txt", b"A");
    let first_hash = cas.put(first_zip).await.expect("store first zip payload");
    let second_zip = build_test_zip_payload("b.txt", b"B");
    let second_hash = cas.put(second_zip).await.expect("store second zip payload");
    let executor = StepWorkerExecutor { cas };
    let temp = tempfile::tempdir().expect("tempdir");

    executor
        .materialize_tool_content_map(
            &BTreeMap::from([
                ("tool/".to_string(), first_hash),
                ("tool/nested/".to_string(), second_hash),
            ]),
            temp.path(),
        )
        .await
        .expect("non-overlapping directory entries should merge successfully");

    assert_eq!(
        std::fs::read_to_string(temp.path().join("tool").join("a.txt")).expect("read first"),
        "A"
    );
    assert_eq!(
        std::fs::read_to_string(temp.path().join("tool").join("nested").join("b.txt"))
            .expect("read second"),
        "B"
    );
}

/// Protects process-code capture payload formatting.
#[test]
fn process_code_capture_serializes_exit_code_text() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let capture = ToolExecutionCapture { stdout: Vec::new(), stderr: Vec::new(), process_code: 27 };
    let output_spec = ResolvedOutputSpec {
        capture: ResolvedOutputCapture::ProcessCode,
        persistence: PersistenceFlags::default(),
    };
    let sandbox = tempfile::tempdir().expect("tempdir");

    let payload = executor
        .capture_output_payload(&output_spec, &capture, sandbox.path())
        .expect("process-code capture should serialize");

    assert_eq!(payload, b"27".to_vec());
}

/// Protects folder capture behavior that emits ZIP payload bytes.
#[test]
fn folder_capture_emits_zip_payload_with_optional_top_folder() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let sandbox = tempfile::tempdir().expect("tempdir");
    let folder = sandbox.path().join("bundle");
    std::fs::create_dir_all(folder.join("nested")).expect("create folder output");
    std::fs::write(folder.join("nested").join("a.txt"), b"A").expect("write folder output file");
    let capture = ToolExecutionCapture::default();

    let without_top = ResolvedOutputSpec {
        capture: ResolvedOutputCapture::FolderAsZip {
            relative_path: std::path::PathBuf::from("bundle"),
            include_topmost_folder: false,
        },
        persistence: PersistenceFlags::default(),
    };
    let payload_without_top = executor
        .capture_output_payload(&without_top, &capture, sandbox.path())
        .expect("folder capture without top-level folder should succeed");
    mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
        &payload_without_top,
        &sandbox.path().join("unzipped_without_top"),
    )
    .expect("unpack zip without top folder");
    assert!(sandbox.path().join("unzipped_without_top").join("nested").join("a.txt").exists());
    assert!(
        !sandbox
            .path()
            .join("unzipped_without_top")
            .join("bundle")
            .join("nested")
            .join("a.txt")
            .exists()
    );

    let with_top = ResolvedOutputSpec {
        capture: ResolvedOutputCapture::FolderAsZip {
            relative_path: std::path::PathBuf::from("bundle"),
            include_topmost_folder: true,
        },
        persistence: PersistenceFlags::default(),
    };
    let payload_with_top = executor
        .capture_output_payload(&with_top, &capture, sandbox.path())
        .expect("folder capture with top-level folder should succeed");
    mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
        &payload_with_top,
        &sandbox.path().join("unzipped_with_top"),
    )
    .expect("unpack zip with top folder");
    assert!(
        sandbox
            .path()
            .join("unzipped_with_top")
            .join("bundle")
            .join("nested")
            .join("a.txt")
            .exists()
    );
}

/// Protects executable success-code membership logic.
#[test]
fn success_code_membership_checks_configured_set() {
    let success_codes = BTreeSet::from([0_i32, 2_i32, 7_i32]);

    assert!(StepWorkerExecutor::<InMemoryCas>::is_success_exit_code(2, &success_codes));
    assert!(!StepWorkerExecutor::<InMemoryCas>::is_success_exit_code(1, &success_codes));
}

/// Protects reverse-diff hinting by skipping CAS constraint patches for the
/// empty-content root input hash.
#[tokio::test]
async fn reverse_diff_hints_skip_empty_content_root_input_hash() {
    let cas = Arc::new(InMemoryCas::new());
    let output_hash = cas.put(b"output".to_vec()).await.expect("put output payload");
    let executor = StepWorkerExecutor { cas: cas.clone() };

    let inputs =
        BTreeMap::from([("empty".to_string(), ResolvedInput::from_plain_content(Vec::new()))]);

    executor
        .apply_reverse_diff_hints(output_hash, &inputs)
        .await
        .expect("reverse-diff hinting should skip empty-content root input hash");

    assert!(
        cas.get_constraint(empty_content_hash()).await.expect("query empty constraint").is_none(),
        "empty-content root should remain unconstrained"
    );
}

/// Protects workflow-error diagnostics by preserving ANSI styling bytes.
#[test]
fn format_process_failure_stderr_preserves_ansi_sequences() {
    let raw = "\u{001b}[31merror\u{001b}[0m from tool \u{001b}]8;;https://example.com\u{0007}link\u{001b}]8;;\u{0007}";

    let formatted =
        StepWorkerExecutor::<InMemoryCas>::format_process_failure_stderr(raw.as_bytes());

    assert_eq!(formatted, raw);
}

/// Protects fallback message when stderr contains only whitespace.
#[test]
fn format_process_failure_stderr_uses_default_for_empty_text() {
    let raw = "\n\t\r";

    let formatted =
        StepWorkerExecutor::<InMemoryCas>::format_process_failure_stderr(raw.as_bytes());

    assert_eq!(formatted, "no stderr output");
}

/// Protects crate-owned builtin echo dispatch and stream payload shape.
#[tokio::test]
async fn builtin_echo_dispatch_uses_echo_crate_streams() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let temp = tempfile::tempdir().expect("tempdir");
    let args = BTreeMap::from([
        ("text".to_string(), "hello".to_string()),
        ("stream".to_string(), "both".to_string()),
    ]);

    let capture = executor
        .execute_builtin_tool(
            mediapm_conductor_builtin_echo::TOOL_NAME,
            mediapm_conductor_builtin_echo::TOOL_VERSION,
            &args,
            &BTreeMap::new(),
            temp.path(),
            temp.path(),
        )
        .await
        .expect("builtin echo dispatch should succeed");

    assert_eq!(capture.stdout, b"hello\n".to_vec());
    assert_eq!(capture.stderr, b"hello\n".to_vec());
    assert_eq!(capture.process_code, 0);
}

/// Protects builtin import dispatch for relative paths rooted in outermost config directory.
#[tokio::test]
async fn builtin_import_dispatch_supports_relative_local_path() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let temp = tempfile::tempdir().expect("tempdir");
    let tool_root = temp.path().join("tool-root");
    let config_root = temp.path().join("config-root");
    std::fs::create_dir_all(&tool_root).expect("create tool root");
    std::fs::create_dir_all(&config_root).expect("create config root");

    let source_path = config_root.join("input.txt");
    std::fs::write(&source_path, b"abc").expect("write source");

    let capture = executor
        .execute_builtin_tool(
            mediapm_conductor_builtin_import::TOOL_NAME,
            mediapm_conductor_builtin_import::TOOL_VERSION,
            &BTreeMap::from([
                ("kind".to_string(), "file".to_string()),
                ("path_mode".to_string(), "relative".to_string()),
                ("path".to_string(), "input.txt".to_string()),
            ]),
            &BTreeMap::new(),
            &tool_root,
            &config_root,
        )
        .await
        .expect("builtin import dispatch should succeed");

    assert_eq!(capture.stdout, b"abc");
    assert_eq!(capture.process_code, 0);
}

/// Protects builtin import folder dispatch that exports one ZIP payload.
#[tokio::test]
async fn builtin_import_dispatch_exports_folder_as_zip() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let temp = tempfile::tempdir().expect("tempdir");
    let source_dir = temp.path().join("fixtures").join("pack");
    std::fs::create_dir_all(&source_dir).expect("create source dir");
    std::fs::write(source_dir.join("a.txt"), b"z").expect("write source file");

    let capture = executor
        .execute_builtin_tool(
            mediapm_conductor_builtin_import::TOOL_NAME,
            mediapm_conductor_builtin_import::TOOL_VERSION,
            &BTreeMap::from([
                ("kind".to_string(), "folder".to_string()),
                ("path".to_string(), "fixtures/pack".to_string()),
            ]),
            &BTreeMap::new(),
            temp.path(),
            temp.path(),
        )
        .await
        .expect("builtin import folder dispatch should succeed");

    mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
        &capture.stdout,
        &temp.path().join("unzipped"),
    )
    .expect("unpack imported folder zip");

    assert!(temp.path().join("unzipped").join("a.txt").exists());
}

/// Protects builtin import `cas_hash` dispatch that reads payload bytes from CAS.
#[tokio::test]
async fn builtin_import_dispatch_supports_cas_hash_kind() {
    let cas = Arc::new(InMemoryCas::new());
    let hash = cas.put(b"from-cas".to_vec()).await.expect("seed CAS payload");
    let executor = StepWorkerExecutor { cas };
    let temp = tempfile::tempdir().expect("tempdir");

    let capture = executor
        .execute_builtin_tool(
            mediapm_conductor_builtin_import::TOOL_NAME,
            mediapm_conductor_builtin_import::TOOL_VERSION,
            &BTreeMap::from([
                ("kind".to_string(), "cas_hash".to_string()),
                ("hash".to_string(), hash.to_string()),
            ]),
            &BTreeMap::new(),
            temp.path(),
            temp.path(),
        )
        .await
        .expect("builtin import cas_hash dispatch should succeed");

    assert_eq!(capture.stdout, b"from-cas");
    assert_eq!(capture.process_code, 0);
}

/// Protects builtin fs dispatch and rooted file-write behavior.
#[tokio::test]
async fn builtin_fs_dispatch_writes_rooted_file() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let temp = tempfile::tempdir().expect("tempdir");
    let tool_root = temp.path().join("tool-root");
    let config_root = temp.path().join("config-root");
    std::fs::create_dir_all(&tool_root).expect("create tool root");
    std::fs::create_dir_all(&config_root).expect("create config root");
    let output_path = config_root.join("out").join("out.txt");

    let capture = executor
        .execute_builtin_tool(
            mediapm_conductor_builtin_fs::TOOL_NAME,
            mediapm_conductor_builtin_fs::TOOL_VERSION,
            &BTreeMap::from([
                ("op".to_string(), "write_text".to_string()),
                ("path_mode".to_string(), "relative".to_string()),
                ("path".to_string(), "out/out.txt".to_string()),
                ("content".to_string(), "payload".to_string()),
            ]),
            &BTreeMap::new(),
            &tool_root,
            &config_root,
        )
        .await
        .expect("builtin fs dispatch should succeed");

    assert!(capture.stdout.is_empty(), "fs builtin should not emit stdout payload");
    assert!(!tool_root.join("out").join("out.txt").exists());
    assert_eq!(std::fs::read_to_string(output_path).expect("read written file"), "payload");
    assert_eq!(capture.process_code, 0);
}

/// Protects builtin archive dispatch for pure file-content pack behavior.
#[tokio::test]
async fn builtin_archive_dispatch_packs_pure_file_content() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let temp = tempfile::tempdir().expect("tempdir");
    let input_bytes =
        BTreeMap::from([("content".to_string(), ResolvedInput::from_plain_content(b"z".to_vec()))]);

    let capture = executor
        .execute_builtin_tool(
            mediapm_conductor_builtin_archive::TOOL_NAME,
            mediapm_conductor_builtin_archive::TOOL_VERSION,
            &BTreeMap::from([
                ("action".to_string(), "pack".to_string()),
                ("kind".to_string(), "file".to_string()),
                ("entry_name".to_string(), "a.txt".to_string()),
            ]),
            &input_bytes,
            temp.path(),
            temp.path(),
        )
        .await
        .expect("builtin archive dispatch should succeed");

    let unpack_dir = temp.path().join("unpacked");
    mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(&capture.stdout, &unpack_dir)
        .expect("unpack archive payload");
    assert_eq!(std::fs::read(unpack_dir.join("a.txt")).ok(), Some(b"z".to_vec()));
    assert_eq!(capture.process_code, 0);
}

/// Protects builtin export dispatch by ensuring only `content` is forwarded as
/// binary input while structural keys remain args.
#[tokio::test]
async fn builtin_export_dispatch_filters_structural_binary_inputs() {
    let executor = StepWorkerExecutor { cas: Arc::new(InMemoryCas::new()) };
    let temp = tempfile::tempdir().expect("tempdir");
    let tool_root = temp.path().join("tool-root");
    let config_root = temp.path().join("config-root");
    std::fs::create_dir_all(&tool_root).expect("create tool root");
    std::fs::create_dir_all(&config_root).expect("create config root");
    let output_path = config_root.join("exports").join("payload.bin");
    let resolved_inputs = BTreeMap::from([
        ("kind".to_string(), ResolvedInput::from_plain_content(b"file".to_vec())),
        ("path".to_string(), ResolvedInput::from_plain_content(b"exports/payload.bin".to_vec())),
        ("path_mode".to_string(), ResolvedInput::from_plain_content(b"relative".to_vec())),
        ("content".to_string(), ResolvedInput::from_plain_content(b"hello-export".to_vec())),
    ]);
    let resolved_args = BTreeMap::from([
        ("kind".to_string(), "file".to_string()),
        ("path_mode".to_string(), "relative".to_string()),
        ("path".to_string(), "exports/payload.bin".to_string()),
        ("content".to_string(), "hello-export".to_string()),
    ]);

    let capture = executor
        .execute_builtin_tool(
            mediapm_conductor_builtin_export::TOOL_NAME,
            mediapm_conductor_builtin_export::TOOL_VERSION,
            &resolved_args,
            &resolved_inputs,
            &tool_root,
            &config_root,
        )
        .await
        .expect("builtin export dispatch should succeed");

    assert_eq!(std::fs::read(&output_path).ok(), Some(b"hello-export".to_vec()));
    assert!(!tool_root.join("exports").join("payload.bin").exists());
    let response: BTreeMap<String, String> =
        serde_json::from_slice(&capture.stdout).expect("export payload should deserialize");
    assert_eq!(response.get("status"), Some(&"ok".to_string()));
    assert_eq!(capture.process_code, 0);
}
