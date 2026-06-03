//! Unit and integration tests for the conductor CLI.
#[cfg(feature = "tool-presets")]
use super::CommonExecutableTool;
use super::tools::resolve_import_process_name;
use super::{
    Cli, CliCommand, ImportArgs, ImportCommand, StateArgs, StateCommand, ToolArgs, ToolCommand,
    extract_platform_conditional_paths, inject_cas_root_arg_if_missing,
    normalize_managed_tool_relative_command_path, parse_editor_command, passthrough_cas,
    persisted_state_json_pretty, register_or_merge_imported_tool,
};
use crate::model::config::{
    MachineNickelDocument, ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec,
};
use crate::model::state::{OrchestrationState, ToolCallInstance};
use clap::Parser;
use mediapm_cas::Hash;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[test]
fn parse_cas_passthrough_preserves_trailing_args() {
    let cli = Cli::parse_from(["conductor", "cas", "put", "example.bin", "--force"]);
    match cli.command {
        CliCommand::Cas(args) => {
            assert_eq!(args.args, vec!["put", "example.bin", "--force"]);
        }
        other => panic!("expected cas command, got {other:?}"),
    }
}

#[test]
fn parse_import_tool_command() {
    let cli = Cli::parse_from(["conductor", "import", "tool", "./tools/zip", "--name", "zip"]);

    match cli.command {
        CliCommand::Import(ImportArgs {
            command: ImportCommand::Tool { path, name, process_name, .. },
        }) => {
            assert_eq!(path, Some(PathBuf::from("./tools/zip")));
            assert_eq!(name, Some("zip".to_string()));
            assert!(process_name.is_none());
        }
        other => panic!("expected import tool command, got {other:?}"),
    }
}

#[cfg(feature = "tool-presets")]
#[test]
fn parse_import_tool_preset_command() {
    let cli = Cli::parse_from(["conductor", "import", "tool", "--preset", "sd"]);

    match cli.command {
        CliCommand::Import(ImportArgs {
            command: ImportCommand::Tool { path, preset, name, process_name },
        }) => {
            assert_eq!(preset, Some(CommonExecutableTool::Sd));
            assert!(path.is_none());
            assert!(name.is_none());
            assert!(process_name.is_none());
        }
        other => panic!("expected import tool --preset command, got {other:?}"),
    }
}

/// Protects in-process CAS passthrough routing with preserved trailing args.
#[tokio::test]
async fn passthrough_cas_reports_parse_errors_without_external_binary() {
    let error = passthrough_cas(
        &["bad-subcommand".to_string()],
        PathBuf::from(".conductor/store").as_path(),
    )
    .await
    .expect_err("invalid cas command should fail");
    assert!(
        error.to_string().contains("cas passthrough failed"),
        "error should be wrapped with passthrough context"
    );
}

#[test]
fn inject_cas_root_arg_if_missing_adds_resolved_conductor_root() {
    let injected = inject_cas_root_arg_if_missing(
        &["optimize".to_string()],
        PathBuf::from(".conductor/store").as_path(),
    );

    assert_eq!(
        injected,
        vec!["--root".to_string(), ".conductor/store".to_string(), "optimize".to_string(),]
    );
}

#[test]
fn inject_cas_root_arg_if_missing_respects_explicit_root() {
    let injected = inject_cas_root_arg_if_missing(
        &["--root".to_string(), "custom-store".to_string(), "optimize".to_string()],
        PathBuf::from(".conductor/store").as_path(),
    );

    assert_eq!(
        injected,
        vec!["--root".to_string(), "custom-store".to_string(), "optimize".to_string()]
    );
}

#[test]
fn parse_run_with_allow_tool_redefinition_flag() {
    let cli = Cli::parse_from(["conductor", "run", "--allow-tool-redefinition"]);
    match cli.command {
        CliCommand::Run { allow_tool_redefinition, .. } => {
            assert!(allow_tool_redefinition);
        }
        other => panic!("expected run command, got {other:?}"),
    }
}

#[test]
fn parse_tool_run_passthrough_args() {
    let cli = Cli::parse_from(["conductor", "tool", "run", "--tool", "yt-dlp", "--", "--version"]);

    match cli.command {
        CliCommand::Tool(ToolArgs { command: ToolCommand::Run { tool, args } }) => {
            assert_eq!(tool, "yt-dlp");
            assert_eq!(args, vec!["--version".to_string()]);
        }
        other => panic!("expected tool run command, got {other:?}"),
    }
}

#[test]
fn extract_platform_conditional_paths_reads_context_os_selector() {
    let selector = "${context.os == \"macos\" ? macos/yt-dlp | linux/yt-dlp}";
    let parsed =
        extract_platform_conditional_paths(selector).expect("context.os selector should parse");

    assert_eq!(parsed.get("macos").map(String::as_str), Some("macos/yt-dlp"));
}

#[test]
fn normalize_managed_tool_relative_command_path_rejects_parent_escape() {
    assert!(normalize_managed_tool_relative_command_path("../bin/tool").is_none());
    assert_eq!(
        normalize_managed_tool_relative_command_path("./macos/yt-dlp").as_deref(),
        Some("macos/yt-dlp")
    );
}

#[test]
fn parse_state_show_default_command() {
    let cli = Cli::parse_from(["conductor", "state"]);
    match cli.command {
        CliCommand::State(StateArgs { command }) => {
            assert!(command.is_none(), "state without subcommand should default to show");
        }
        other => panic!("expected state command, got {other:?}"),
    }
}

#[test]
fn parse_state_compile_command() {
    let cli = Cli::parse_from(["conductor", "state", "compile"]);
    match cli.command {
        CliCommand::State(StateArgs { command: Some(StateCommand::Compile) }) => {}
        other => panic!("expected state compile command, got {other:?}"),
    }
}

#[test]
fn parse_state_export_command() {
    let cli = Cli::parse_from(["conductor", "state", "export", "state.json"]);
    match cli.command {
        CliCommand::State(StateArgs { command: Some(StateCommand::Export { path }) }) => {
            assert_eq!(path, PathBuf::from("state.json"));
        }
        other => panic!("expected state export command, got {other:?}"),
    }
}

#[test]
fn parse_state_import_command() {
    let cli = Cli::parse_from(["conductor", "state", "import", "state.json"]);
    match cli.command {
        CliCommand::State(StateArgs { command: Some(StateCommand::Import { path }) }) => {
            assert_eq!(path, PathBuf::from("state.json"));
        }
        other => panic!("expected state import command, got {other:?}"),
    }
}

#[test]
fn parse_state_edit_with_editor_override() {
    let cli = Cli::parse_from(["conductor", "state", "edit", "--editor", "code --wait"]);
    match cli.command {
        CliCommand::State(StateArgs { command: Some(StateCommand::Edit { editor }) }) => {
            assert_eq!(editor.as_deref(), Some("code --wait"));
        }
        other => panic!("expected state edit command, got {other:?}"),
    }
}

#[test]
fn parse_state_invalidate_tool_call_command() {
    let cli = Cli::parse_from(["conductor", "state", "invalidate-tool-call", "instance-key-123"]);
    match cli.command {
        CliCommand::State(StateArgs {
            command: Some(StateCommand::InvalidateToolCall { instance_id }),
        }) => {
            assert_eq!(instance_id, "instance-key-123");
        }
        other => panic!("expected state invalidate-tool-call command, got {other:?}"),
    }
}

#[test]
fn parse_editor_command_supports_quoted_program_and_args() {
    let (program, args) = parse_editor_command("\"C:\\Program Files\\Editor\\editor.exe\" --wait")
        .expect("quoted editor command should parse");
    assert_eq!(program, "C:\\Program Files\\Editor\\editor.exe");
    assert_eq!(args, vec!["--wait".to_string()]);
}

#[test]
fn parse_editor_command_rejects_unterminated_quote() {
    let error = parse_editor_command("\"code --wait").expect_err("unterminated quote should fail");
    assert!(error.to_string().contains("unterminated"));
}

#[test]
fn parse_grouped_runtime_storage_path_options() {
    let cli = Cli::parse_from([
        "conductor",
        "--conductor-dir",
        "runtime/.conductor-custom",
        "--config-state",
        "runtime/state.custom.ncl",
        "--cas-store-dir",
        "runtime/cas-root",
        "--conductor-schema-dir",
        "runtime/config/custom-conductor-schemas",
        "run",
    ]);

    assert_eq!(cli.runtime_paths.conductor_dir, PathBuf::from("runtime/.conductor-custom"));
    assert_eq!(
        cli.runtime_paths.conductor_state_config,
        Some(PathBuf::from("runtime/state.custom.ncl"))
    );
    assert_eq!(cli.runtime_paths.cas_store_dir, Some("runtime/cas-root".to_string()));
    assert_eq!(
        cli.runtime_paths.conductor_schema_dir,
        Some(PathBuf::from("runtime/config/custom-conductor-schemas"))
    );
}

#[test]
fn register_or_merge_imported_tool_bootstraps_missing_tool_metadata_in_machine_document() {
    let mut machine = MachineNickelDocument::default();

    register_or_merge_imported_tool(
        &mut machine,
        "demo-tool@1.0.0",
        PathBuf::from("demo.exe").as_path(),
        Some("demo.exe"),
        BTreeMap::from([("demo.exe".to_string(), Hash::from_content(b"demo-a"))]),
        None,
    )
    .expect("import registration should bootstrap missing tool metadata");

    assert!(machine.tools.contains_key("demo-tool@1.0.0"));
    assert!(machine.tool_configs.contains_key("demo-tool@1.0.0"));

    let kind = &machine.tools.get("demo-tool@1.0.0").expect("tool metadata should exist").kind;
    let ToolKindSpec::Executable { command, .. } = kind else {
        panic!("bootstrapped tool should be executable");
    };
    assert_eq!(command, &vec!["demo.exe".to_string()]);
    assert!(machine.external_data.contains_key(&Hash::from_content(b"demo-a")));
}

#[test]
fn register_or_merge_imported_tool_merges_content_for_existing_executable() {
    let mut machine = MachineNickelDocument {
        tools: BTreeMap::from([(
            "demo-tool@1.0.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec!["demo.exe".to_string()],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            },
        )]),
        ..MachineNickelDocument::default()
    };

    register_or_merge_imported_tool(
        &mut machine,
        "demo-tool@1.0.0",
        PathBuf::from("demo.exe").as_path(),
        None,
        BTreeMap::from([("payload.txt".to_string(), Hash::from_content(b"demo-b"))]),
        None,
    )
    .expect("content-map merge should succeed for existing executable");

    let content_map = machine
        .tool_configs
        .get("demo-tool@1.0.0")
        .and_then(|config| config.content_map.as_ref())
        .expect("content_map should exist after merge");
    assert!(content_map.contains_key("payload.txt"));
    assert!(machine.external_data.contains_key(&Hash::from_content(b"demo-b")));
}

#[test]
fn resolve_import_process_name_requires_explicit_name_for_directory_bootstrap() {
    let error = resolve_import_process_name(
        PathBuf::from("tool-directory").as_path(),
        None,
        Some("bin/tool"),
    )
    .expect_err("directory bootstrap without explicit process name should fail");

    assert!(error.to_string().contains("--process-name"));
}

#[test]
fn persisted_state_json_pretty_normalizes_builtin_metadata() {
    let state = OrchestrationState {
        version: OrchestrationState::default().version,
        instances: BTreeMap::from([(
            "instance-a".to_string(),
            ToolCallInstance {
                tool_name: "echo@1.0.0".to_string(),
                metadata: ToolSpec {
                    is_impure: true,
                    inputs: BTreeMap::from([("text".to_string(), ToolInputSpec::default())]),
                    kind: ToolKindSpec::Builtin {
                        name: "echo".to_string(),
                        version: "1.0.0".to_string(),
                    },
                    outputs: BTreeMap::from([("result".to_string(), ToolOutputSpec::default())]),
                },
                impure_timestamp: None,
                last_used: None,
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
            },
        )]),
    };

    let rendered =
        persisted_state_json_pretty(&state).expect("state rendering should use persistence shape");
    let json: serde_json::Value =
        serde_json::from_str(&rendered).expect("rendered state should be valid JSON");

    assert_eq!(
        json["instances"]["instance-a"]["metadata"],
        serde_json::json!({
            "kind": "builtin",
            "name": "echo",
            "version": "1.0.0"
        })
    );
}
