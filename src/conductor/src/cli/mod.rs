//! Command-line interface for `mediapm-conductor`.
//!
//! This module exposes a conductor-oriented CLI surface:
//! - workflow execution/state inspection,
//! - program-edited Nickel maintenance through `conductor.machine.ncl`,
//! - direct passthrough command invocation for `cas`.
//!
//! Invariants:
//! - CLI automation mutates only `conductor.machine.ncl`.
//! - `conductor.ncl` remains user-edited input, but it shares the same schema.
//! - CAS mutations always go through configured CAS backends.
//! - passthrough commands reuse the CAS CLI parser/dispatcher in-process.

mod document_io;
#[cfg(test)]
mod tests {
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
        ExternalContentRef, MachineNickelDocument, StateNickelDocument, ToolConfigSpec,
        ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec, UserNickelDocument,
        encode_machine_document, encode_state_document, encode_user_document,
    };
    use crate::model::state::{OrchestrationState, ToolCallInstance, encode_state};
    use clap::Parser;
    use mediapm_cas::{ConfiguredCas, Hash, InMemoryCas};
    use std::collections::{BTreeMap, HashSet};
    use std::path::PathBuf;
    use tempfile::tempdir;

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
        let cli =
            Cli::parse_from(["conductor", "tool", "run", "--tool", "yt-dlp", "--", "--version"]);

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
        let cli =
            Cli::parse_from(["conductor", "state", "invalidate-tool-call", "instance-key-123"]);
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
        let (program, args) =
            parse_editor_command("\"C:\\Program Files\\Editor\\editor.exe\" --wait")
                .expect("quoted editor command should parse");
        assert_eq!(program, "C:\\Program Files\\Editor\\editor.exe");
        assert_eq!(args, vec!["--wait".to_string()]);
    }

    #[test]
    fn parse_editor_command_rejects_unterminated_quote() {
        let error =
            parse_editor_command("\"code --wait").expect_err("unterminated quote should fail");
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
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            ToolOutputSpec::default(),
                        )]),
                    },
                    impure_timestamp: None,
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                },
            )]),
            aux: BTreeMap::new(),
            referenced_instance_keys: HashSet::new(),
        };

        let rendered = persisted_state_json_pretty(&state)
            .expect("state rendering should use persistence shape");
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

    // ==== CLI `gc` command tests ====

    #[test]
    fn parse_gc_command() {
        let cli = Cli::parse_from(["conductor", "gc"]);
        match cli.command {
            CliCommand::Gc => {}
            other => panic!("expected Gc command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn run_gc_empty_docs_completes() {
        let dir = tempdir().expect("tempdir");
        let cas = ConfiguredCas::InMemory(InMemoryCas::new());

        let result = super::run_gc(
            cas,
            &dir.path().join("conductor.ncl"),
            &dir.path().join("conductor.machine.ncl"),
            &dir.path().join("state.ncl"),
        )
        .await;
        assert!(result.is_ok(), "run_gc with empty docs should succeed");
    }

    #[tokio::test]
    async fn run_gc_with_external_data_roots() {
        let dir = tempdir().expect("tempdir");
        let user_path = dir.path().join("conductor.ncl");
        let machine_path = dir.path().join("conductor.machine.ncl");
        let state_path = dir.path().join("state.ncl");

        let hash = Hash::from_content(b"gc-test-root");
        let user = UserNickelDocument {
            external_data: BTreeMap::from([(
                hash,
                ExternalContentRef { description: Some("gc test root".to_string()), save: None },
            )]),
            tool_configs: BTreeMap::from([(
                "test-tool".to_string(),
                ToolConfigSpec {
                    content_map: Some(BTreeMap::from([("payload.bin".to_string(), hash)])),
                    ..ToolConfigSpec::default()
                },
            )]),
            ..UserNickelDocument::default()
        };
        let machine = MachineNickelDocument {
            external_data: BTreeMap::from([(
                hash,
                ExternalContentRef {
                    description: Some("gc test machine root".to_string()),
                    save: None,
                },
            )]),
            ..MachineNickelDocument::default()
        };

        std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
            .expect("write user");
        std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
            .expect("write machine");

        let cas = ConfiguredCas::InMemory(InMemoryCas::new());
        let result = super::run_gc(cas, &user_path, &machine_path, &state_path).await;
        result.expect("run_gc with external data roots should succeed");
    }

    #[tokio::test]
    async fn run_gc_with_state_pointer_completes() {
        let dir = tempdir().expect("tempdir");
        let user_path = dir.path().join("conductor.ncl");
        let machine_path = dir.path().join("conductor.machine.ncl");
        let state_path = dir.path().join("state.ncl");

        let state = OrchestrationState {
            instances: BTreeMap::from([(
                "test-call".to_string(),
                ToolCallInstance {
                    tool_name: "echo".to_string(),
                    metadata: ToolSpec::default(),
                    impure_timestamp: None,
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                },
            )]),
            ..OrchestrationState::default()
        };

        let cas: ConfiguredCas = ConfiguredCas::InMemory(InMemoryCas::new());
        let state_hash = encode_state(&cas, state).await.expect("encode state");

        let state_doc = StateNickelDocument {
            state_pointer: Some(state_hash),
            ..StateNickelDocument::default()
        };

        std::fs::write(
            &user_path,
            encode_user_document(UserNickelDocument::default()).expect("encode"),
        )
        .expect("write user");
        std::fs::write(
            &machine_path,
            encode_machine_document(MachineNickelDocument::default()).expect("encode"),
        )
        .expect("write machine");
        std::fs::write(&state_path, encode_state_document(state_doc).expect("encode state doc"))
            .expect("write state doc");

        let result = super::run_gc(cas, &user_path, &machine_path, &state_path).await;
        assert!(result.is_ok(), "run_gc with state pointer should succeed");
    }
}
mod tools;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use clap::{Args, CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use mediapm_cas::{
    CasApi, CasConfig, CasError, CasLocatorParseOptions, CasMaintenanceApi, ConfiguredCas, Hash,
};

#[cfg(feature = "tool-presets")]
use crate::api::{CommonExecutableTool, fetch_common_executable_tool_payload};
use crate::api::{
    ConductorApi, RunWorkflowOptions, RuntimeStoragePaths, StateMutationOptions,
    default_state_paths, export_nickel_config_schemas, resolve_runtime_storage_paths,
};
use crate::error::ConductorError;
use crate::gc::compute_gc_roots;
use crate::model::config::{AddExternalDataOptions, ExternalContentRef};
use crate::model::state::{
    OrchestrationState, decode_state, decode_state_from_slice, persisted_state_json_pretty,
};
use crate::orchestration::SimpleConductor;
use crate::runtime_env::load_runtime_env_files;

use self::document_io::{
    compile_effective_configuration, load_machine_document, load_state_document,
    load_user_document, save_machine_document,
};
use self::tools::{
    collect_tool_files, handle_remove, inject_cas_root_arg_if_missing, normalized_relative_path,
    register_or_merge_imported_tool,
};

/// Default runtime storage root used by the conductor CLI.
const DEFAULT_CONDUCTOR_DIR: &str = ".conductor";

/// Grouped runtime storage path arguments.
#[derive(Debug, Clone, Args)]
struct RuntimePathArgs {
    /// Root directory for runtime-managed artifacts.
    ///
    /// Defaults to `.conductor` relative to the selected config-file parent.
    #[arg(long, global = true, default_value = DEFAULT_CONDUCTOR_DIR)]
    conductor_dir: PathBuf,

    /// Optional override path for the volatile state document.
    ///
    /// Defaults to `<conductor_dir>/state.ncl`.
    #[arg(long = "config-state", global = true)]
    conductor_state_config: Option<PathBuf>,

    /// CAS backend locator string or filesystem directory path.
    ///
    /// Accepts any CAS locator (plain filesystem path, URL, or other locator
    /// format supported by `mediapm-cas`). Defaults to `<conductor_dir>/store`.
    #[arg(long, global = true)]
    cas_store_dir: Option<String>,

    /// Optional override directory for exported conductor Nickel schemas.
    ///
    /// Defaults to `<conductor_dir>/config/conductor`.
    #[arg(long, global = true)]
    conductor_schema_dir: Option<PathBuf>,

    /// Optional override directory for the tool-content cache.
    ///
    /// The tool-content cache stores one ready-to-execute payload directory per
    /// tool id.  Entries are keyed on the full `content_map` and expire after
    /// 24 hours of non-use.  Defaults to `<conductor_dir>/tools`.
    #[arg(long, global = true)]
    conductor_tools_dir: Option<PathBuf>,

    /// Optional JSON profile artifact output path.
    ///
    /// When set, conductor writes one per-run profiler report at this path.
    #[arg(long = "profile-json", global = true)]
    profile_json: Option<PathBuf>,
}

/// Top-level conductor CLI parser.
#[derive(Debug, Parser)]
#[command(name = "conductor", about = "mediapm conductor CLI")]
pub struct Cli {
    /// Grouped runtime storage path arguments.
    #[command(flatten)]
    runtime_paths: RuntimePathArgs,

    /// Path to the user-edited configuration document (`conductor.ncl` by default).
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Path to the program-edited configuration document (`conductor.machine.ncl` by default).
    #[arg(long = "config-machine", global = true)]
    config_machine: Option<PathBuf>,

    /// Top-level CLI command.
    #[command(subcommand)]
    command: CliCommand,
}

/// Top-level conductor CLI commands.
#[derive(Debug, Subcommand)]
pub enum CliCommand {
    /// Executes workflows and updates orchestration state.
    Run {
        /// Allows conflicting tool redefinitions to override existing locked
        /// machine definitions for the same immutable tool name.
        #[arg(long, default_value_t = false)]
        allow_tool_redefinition: bool,
        /// Enables conductor profiling for this run.
        ///
        /// When set and no explicit `--profile-json` path is provided, the
        /// profile is written to `<conductor-dir>/profile.json`. The
        /// `MEDIAPM_CONDUCTOR_PROFILE_JSON` environment variable is consulted
        /// as an override first.
        #[arg(long, default_value_t = false)]
        enable_profiler: bool,
    },
    /// Prints a formatted profiler report from a conductor profile JSON file.
    Profiler {
        /// Path to the conductor profile JSON file to visualize.
        path: PathBuf,
    },
    /// State inspection and mutation operations.
    State(StateArgs),
    /// Imports tool/data content into CAS and Nickel docs.
    Import(ImportArgs),
    /// Managed tool execution helpers.
    Tool(ToolArgs),
    /// Removes tool/data references from Nickel docs.
    Remove(RemoveArgs),
    /// Runs root-based garbage collection in CAS.
    Gc,
    /// Passthrough to CAS CLI.
    Cas(PassthroughArgs),
    /// Generates shell completion scripts for the `mediapm-conductor` CLI.
    Completions {
        /// Target shell for completion script generation.
        shell: Shell,
    },
}

/// Managed-tool command group.
#[derive(Debug, Args)]
pub struct ToolArgs {
    /// Managed-tool operation variant.
    #[command(subcommand)]
    command: ToolCommand,
}

/// Managed-tool operation variants.
#[derive(Debug, Subcommand)]
pub enum ToolCommand {
    /// Resolves one managed executable tool, prepares cache payload, and runs it.
    Run {
        /// Immutable tool id or logical tool selector.
        #[arg(long)]
        tool: String,
        /// Trailing passthrough arguments for the managed executable.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}

/// State command group.
#[derive(Debug, Args)]
pub struct StateArgs {
    /// State operation variant.
    #[command(subcommand)]
    command: Option<StateCommand>,
}

/// State operation variants.
#[derive(Debug, Subcommand)]
pub enum StateCommand {
    /// Prints current migrated orchestration state.
    Show,
    /// Prints compiled merged configuration without mutating runtime state.
    Compile,
    /// Exports current migrated orchestration state to one JSON file.
    Export {
        /// Destination JSON file path.
        path: PathBuf,
    },
    /// Imports orchestration state from one JSON file.
    Import {
        /// Source JSON file path.
        path: PathBuf,
    },
    /// Opens current state in an editor and applies validated edits.
    Edit {
        /// Optional editor command override.
        ///
        /// When omitted, editor resolution follows git-style environment
        /// precedence: `GIT_EDITOR`, then `VISUAL`, then `EDITOR`, then
        /// platform fallback (`notepad` on Windows, `vi` elsewhere).
        #[arg(long)]
        editor: Option<String>,
    },
    /// Invalidates one completed tool call instance so it is re-run.
    InvalidateToolCall {
        /// Deterministic instance id (state `instances` map key).
        instance_id: String,
    },
}

/// Import command group.
#[derive(Debug, Args)]
pub struct ImportArgs {
    /// Import variant.
    #[command(subcommand)]
    command: ImportCommand,
}

/// Import variants.
#[derive(Debug, Subcommand)]
pub enum ImportCommand {
    /// Registers tool file(s) in CAS and updates machine tool metadata/config.
    Tool {
        /// Path to one tool file or tool directory.
        ///
        /// This is required unless `--preset` is used.
        #[cfg_attr(
            feature = "tool-presets",
            arg(required_unless_present = "preset", conflicts_with = "preset")
        )]
        #[cfg_attr(not(feature = "tool-presets"), arg(required = true))]
        path: Option<PathBuf>,
        #[cfg(feature = "tool-presets")]
        /// Optional source-install preset for common executable tools.
        ///
        /// When set, the tool binary is fetched from upstream source and
        /// imported directly into machine-managed runtime config.
        #[arg(long)]
        preset: Option<CommonExecutableTool>,
        /// Logical tool name.
        ///
        /// This is required for file/directory imports and optional for
        /// preset imports (defaults to the preset canonical logical name).
        #[cfg_attr(feature = "tool-presets", arg(long, required_unless_present = "preset"))]
        #[cfg_attr(not(feature = "tool-presets"), arg(long, required = true))]
        name: Option<String>,
        /// Optional executable process path recorded as
        /// `tools.<name>.command[0]`
        /// when this import must register new machine tool metadata.
        ///
        /// When omitted and `path` is one file, the default process path is
        /// that file's config-root-relative import key.
        ///
        /// When omitted and `path` is one directory, import fails with an
        /// explicit error because process entrypoint selection is ambiguous.
        #[arg(long)]
        process_name: Option<String>,
    },
    /// Registers external data in CAS and records the reference in
    /// `conductor.machine.ncl`.
    Data {
        /// Path to one data file.
        path: PathBuf,
        /// Optional description override. Defaults to file name.
        #[arg(long)]
        description: Option<String>,
    },
}

/// Remove command group.
#[derive(Debug, Args)]
pub struct RemoveArgs {
    /// Remove variant.
    #[command(subcommand)]
    command: RemoveCommand,
}

/// Remove variants.
#[derive(Debug, Subcommand)]
pub enum RemoveCommand {
    /// Removes one external-data reference from `conductor.machine.ncl`.
    Data {
        /// External data CAS hash key.
        hash: String,
    },
    /// Removes one tool content map from `conductor.machine.ncl`.
    Tool {
        /// Tool logical name.
        name: String,
        /// Also removes any same-named tool metadata stored in `conductor.machine.ncl`.
        #[arg(long)]
        metadata: bool,
    },
}

/// Generic passthrough-argument holder.
#[derive(Debug, Args)]
pub struct PassthroughArgs {
    /// Trailing passthrough arguments.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

/// Parses CLI from process arguments and executes it.
///
/// # Errors
///
/// Returns any workflow, I/O, CAS, or serialization error surfaced while
/// executing the parsed CLI command.
pub async fn run_from_env() -> Result<(), ConductorError> {
    let cli = Cli::parse();
    run(cli).await
}

/// Parses one explicit argv sequence and executes it.
///
/// Callers should include a program-name placeholder as argv[0].
///
/// # Errors
///
/// Returns any clap parsing error or command execution failure.
pub async fn run_from_argv<I, T>(argv: I) -> Result<(), ConductorError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(argv) {
        Ok(cli) => cli,
        Err(error) => return handle_clap_parse_error(&error),
    };
    run(cli).await
}

/// Parses trailing passthrough arguments and executes the conductor CLI.
///
/// This helper prepends an internal argv[0] binary-name placeholder so parent
/// CLIs can forward only trailing command arguments.
///
/// # Errors
///
/// Returns any clap parsing error or command execution failure.
pub async fn run_from_passthrough_args(args: &[String]) -> Result<(), ConductorError> {
    let passthrough_argv =
        std::iter::once("mediapm-conductor".to_string()).chain(args.iter().cloned());
    run_from_argv(passthrough_argv).await
}

/// Prints clap parse diagnostics with formatting preserved and maps outcomes.
fn handle_clap_parse_error(error: &clap::Error) -> Result<(), ConductorError> {
    use clap::error::ErrorKind;

    let is_help_or_version =
        matches!(error.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion);
    let rendered = error.to_string();
    error.print().map_err(|source| ConductorError::Io {
        operation: "writing conductor CLI parse diagnostics".to_string(),
        path: PathBuf::from("<stderr>"),
        source,
    })?;

    if is_help_or_version { Ok(()) } else { Err(ConductorError::Workflow(rendered)) }
}

/// Executes one parsed CLI command.
///
/// # Errors
///
/// Returns any workflow, I/O, CAS, or serialization error produced by the
/// selected subcommand.
pub async fn run(cli: Cli) -> Result<(), ConductorError> {
    let (default_user, default_machine) = default_state_paths();
    let user_ncl = cli.config.unwrap_or(default_user);
    let machine_ncl = cli.config_machine.unwrap_or(default_machine);

    let runtime_storage_paths = RuntimeStoragePaths {
        conductor_dir: cli.runtime_paths.conductor_dir,
        conductor_state_config: cli.runtime_paths.conductor_state_config,
        cas_store_dir: None,
        conductor_schema_dir: cli.runtime_paths.conductor_schema_dir,
        conductor_tools_dir: cli.runtime_paths.conductor_tools_dir,
    };
    let resolved_runtime_paths =
        resolve_runtime_storage_paths(&user_ncl, &machine_ncl, &runtime_storage_paths);

    let cas_locator = cli
        .runtime_paths
        .cas_store_dir
        .unwrap_or_else(|| resolved_runtime_paths.cas_store_dir.to_string_lossy().to_string());

    match cli.command {
        CliCommand::Cas(args) => {
            passthrough_cas(&args.args, &resolved_runtime_paths.cas_store_dir).await
        }
        CliCommand::Completions { shell } => {
            clap_complete::generate(
                shell,
                &mut Cli::command(),
                "mediapm-conductor",
                &mut std::io::stdout(),
            );
            Ok(())
        }
        CliCommand::Profiler { path } => {
            crate::orchestration::print_profile_timing(&path);
            Ok(())
        }
        other => {
            let runtime_env_var_names =
                load_runtime_env_files(&resolved_runtime_paths.conductor_dir)?;
            let schema_anchor = resolved_runtime_paths.conductor_schema_dir.as_path();
            export_nickel_config_schemas(schema_anchor)?;
            let cas = open_cas(&cas_locator).await?;
            match other {
                CliCommand::Run { allow_tool_redefinition, enable_profiler } => {
                    run_workflow(
                        cas,
                        &user_ncl,
                        &machine_ncl,
                        allow_tool_redefinition,
                        enable_profiler,
                        runtime_storage_paths,
                        runtime_env_var_names,
                        cli.runtime_paths.profile_json.clone(),
                    )
                    .await
                }
                CliCommand::State(args) => {
                    handle_state(
                        cas,
                        &user_ncl,
                        &machine_ncl,
                        runtime_storage_paths.clone(),
                        runtime_env_var_names,
                        args,
                    )
                    .await
                }
                CliCommand::Tool(args) => {
                    handle_tool(
                        cas,
                        &machine_ncl,
                        &resolved_runtime_paths.conductor_tools_dir,
                        args,
                    )
                    .await
                }
                CliCommand::Import(args) => handle_import(cas, &user_ncl, &machine_ncl, args).await,
                CliCommand::Remove(args) => handle_remove(&user_ncl, &machine_ncl, args),
                CliCommand::Gc => {
                    run_gc(
                        cas,
                        &user_ncl,
                        &machine_ncl,
                        &resolved_runtime_paths.conductor_state_config,
                    )
                    .await
                }
                CliCommand::Cas(_)
                | CliCommand::Completions { .. }
                | CliCommand::Profiler { .. } => {
                    unreachable!("passthrough/completions/profiler handled above")
                }
            }
        }
    }
}

/// Handles managed-tool command variants.
async fn handle_tool(
    cas: ConfiguredCas,
    machine_ncl: &Path,
    conductor_tools_dir: &Path,
    args: ToolArgs,
) -> Result<(), ConductorError> {
    match args.command {
        ToolCommand::Run { tool, args } => {
            run_managed_tool(cas, machine_ncl, conductor_tools_dir, &tool, &args).await
        }
    }
}

/// Runs one managed executable tool after preparing its payload cache entry.
async fn run_managed_tool(
    cas: ConfiguredCas,
    machine_ncl: &Path,
    conductor_tools_dir: &Path,
    selector: &str,
    args: &[String],
) -> Result<(), ConductorError> {
    let machine = load_machine_document(machine_ncl)?;
    let tool_id = resolve_managed_tool_id(&machine, selector)?;
    let tool_spec = machine.tools.get(&tool_id).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "managed tool '{tool_id}' is missing from conductor machine config"
        ))
    })?;
    let command_selector = match &tool_spec.kind {
        crate::model::config::ToolKindSpec::Executable { command, .. } => {
            command.first().map(String::as_str).ok_or_else(|| {
                ConductorError::Workflow(format!(
                    "managed tool '{tool_id}' has no executable command configured"
                ))
            })?
        }
        crate::model::config::ToolKindSpec::Builtin { .. } => {
            return Err(ConductorError::Workflow(format!(
                "tool selector '{selector}' resolved to builtin tool '{tool_id}', which cannot be executed via 'tool run'"
            )));
        }
    };
    let content_map = machine
        .tool_configs
        .get(&tool_id)
        .and_then(|config| config.content_map.as_ref())
        .filter(|map| !map.is_empty())
        .ok_or_else(|| {
            ConductorError::Workflow(format!(
                "managed tool '{tool_id}' has no tool_configs content_map; run sync/import first"
            ))
        })?;

    let cas = Arc::new(cas);
    let tool_cache =
        crate::tool_cache::ToolContentCache::new(conductor_tools_dir.to_path_buf(), cas, None);
    let cache_entry = tool_cache.materialize(&tool_id, content_map).await?;
    let payload_dir = cache_entry.payload_dir().to_path_buf();

    let host_relative = resolve_host_command_selector_path(command_selector)?.ok_or_else(|| {
        ConductorError::Workflow(format!(
            "managed tool '{tool_id}' command selector '{command_selector}' does not resolve to a host executable path for os '{}'",
            std::env::consts::OS
        ))
    })?;
    let relative_path = normalize_managed_tool_relative_command_path(&host_relative).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "managed tool '{tool_id}' command selector '{command_selector}' resolved to an invalid relative path"
        ))
    })?;

    let executable_path = payload_dir.join(relative_path);
    if !executable_path.is_file() {
        return Err(ConductorError::Workflow(format!(
            "managed tool '{tool_id}' executable is missing at '{}' after cache preparation",
            executable_path.display()
        )));
    }

    let status = Command::new(&executable_path).args(args).status().map_err(|source| {
        ConductorError::Io {
            operation: format!("executing managed tool '{tool_id}'"),
            path: executable_path.clone(),
            source,
        }
    })?;

    let Some(code) = status.code() else {
        return Err(ConductorError::Workflow(format!(
            "managed tool '{tool_id}' terminated without a numeric exit code"
        )));
    };
    if code != 0 {
        std::process::exit(code);
    }

    Ok(())
}

/// Resolves one immutable managed tool id from selector text.
fn resolve_managed_tool_id(
    machine: &crate::model::config::MachineNickelDocument,
    selector: &str,
) -> Result<String, ConductorError> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Err(ConductorError::Workflow(
            "managed tool selector must be non-empty".to_string(),
        ));
    }

    if let Some(exact) = machine.tools.keys().find(|tool_id| tool_id.eq_ignore_ascii_case(selector))
    {
        return Ok(exact.clone());
    }

    let mut matches = machine
        .tools
        .keys()
        .filter(|tool_id| logical_name_matches_tool_id(tool_id, selector))
        .cloned()
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();

    match matches.as_slice() {
        [only] => Ok(only.clone()),
        [] => Err(ConductorError::Workflow(format!(
            "tool selector '{selector}' did not match any managed tool id in conductor machine config"
        ))),
        _ => Err(ConductorError::Workflow(format!(
            "tool selector '{selector}' matched multiple managed tool ids ({}) ; pass --tool <immutable-id>",
            matches.join(", ")
        ))),
    }
}

/// Returns true when immutable tool id belongs to one logical tool name.
fn logical_name_matches_tool_id(tool_id: &str, logical_name: &str) -> bool {
    if tool_id.eq_ignore_ascii_case(logical_name) {
        return true;
    }

    let Some((prefix, _)) = tool_id.split_once('@') else {
        return false;
    };

    let marker = "mediapm.tools.";
    let canonical_prefix =
        if prefix.len() >= marker.len() && prefix[..marker.len()].eq_ignore_ascii_case(marker) {
            &prefix[marker.len()..]
        } else {
            prefix
        };
    let canonical_name =
        canonical_prefix.split_once('+').map_or(canonical_prefix, |(name, _)| name);

    canonical_name.trim().eq_ignore_ascii_case(logical_name)
}

/// Resolves one host command selector path for the active platform.
fn resolve_host_command_selector_path(
    command_selector: &str,
) -> Result<Option<String>, ConductorError> {
    if command_selector.contains("context.os") {
        let selectors = extract_platform_conditional_paths(command_selector)?;
        return Ok(selectors.get(std::env::consts::OS).cloned());
    }

    let trimmed = command_selector.trim();
    if trimmed.is_empty() { Ok(None) } else { Ok(Some(trimmed.to_string())) }
}

/// Parses `${context.os == "<target>" ? <path> | <fallback>}` selectors.
fn extract_platform_conditional_paths(
    template: &str,
) -> Result<BTreeMap<String, String>, ConductorError> {
    let mut result = BTreeMap::new();
    let mut cursor = 0usize;

    while let Some(start_rel) = template[cursor..].find("${") {
        let start = cursor + start_rel;
        let remainder = &template[start + 2..];
        let Some(end_rel) = remainder.find('}') else {
            return Err(ConductorError::Workflow(format!(
                "invalid command selector '{template}': missing closing '}}'"
            )));
        };
        let token = &remainder[..end_rel];

        if let Some((target, value)) = parse_platform_conditional_path_token(token)? {
            result.insert(target, value);
        }

        cursor = start + 2 + end_rel + 1;
    }

    if result.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool command '{template}' did not contain any context.os selectors"
        )));
    }

    Ok(result)
}

/// Parses one `${...}` token into a platform target/path selector.
fn parse_platform_conditional_path_token(
    token: &str,
) -> Result<Option<(String, String)>, ConductorError> {
    if !token.contains("context.os") {
        return Ok(None);
    }

    let Some((condition, branches)) = token.split_once('?') else {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; expected '?<true>|<false>'"
        )));
    };
    let Some((true_branch, _false_branch)) = branches.split_once('|') else {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; expected '<true>|<false>'"
        )));
    };

    let condition = condition.trim();
    let Some(remainder) = condition.strip_prefix("context.os") else {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; condition must start with 'context.os'"
        )));
    };
    let remainder = remainder.trim_start();
    let Some(remainder) = remainder.strip_prefix("==") else {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; condition must use '=='"
        )));
    };
    let target = parse_quoted_selector_value(remainder.trim()).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; target must be quoted"
        ))
    })?;

    let true_branch = true_branch.trim();
    let path = if let Some(decoded) = parse_quoted_selector_value(true_branch) {
        decoded
    } else {
        true_branch.to_string()
    };
    if path.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; true branch path is empty"
        )));
    }

    Ok(Some((target, path)))
}

/// Parses one single- or double-quoted selector value.
fn parse_quoted_selector_value(value: &str) -> Option<String> {
    if value.len() < 2 {
        return None;
    }
    let first = value.chars().next()?;
    let last = value.chars().last()?;
    if !((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
        return None;
    }

    Some(value[first.len_utf8()..value.len() - last.len_utf8()].to_string())
}

/// Normalizes one managed-tool relative command path for payload lookup.
fn normalize_managed_tool_relative_command_path(relative_command_path: &str) -> Option<String> {
    let normalized = relative_command_path
        .trim()
        .replace('\\', "/")
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string();

    if normalized.is_empty() {
        return None;
    }

    let path = Path::new(&normalized);
    if path.components().any(|component| matches!(component, std::path::Component::ParentDir)) {
        return None;
    }
    if path.is_absolute() {
        return None;
    }

    Some(
        Path::new(&normalized)
            .components()
            .map(|component| component.as_os_str().to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join("/"),
    )
}

/// Handles state command variants.
async fn handle_state(
    cas: ConfiguredCas,
    user_ncl: &Path,
    machine_ncl: &Path,
    runtime_storage_paths: RuntimeStoragePaths,
    runtime_inherited_env_vars: Vec<String>,
    args: StateArgs,
) -> Result<(), ConductorError> {
    let conductor = SimpleConductor::new(cas);
    let options = StateMutationOptions { runtime_storage_paths, runtime_inherited_env_vars };

    match args.command.unwrap_or(StateCommand::Show) {
        StateCommand::Show => {
            let state = conductor.load_resolved_state(user_ncl, machine_ncl, options).await?;
            let rendered = persisted_state_json_pretty(&state)?;
            println!("{rendered}");
            Ok(())
        }
        StateCommand::Compile => {
            let resolved = options.runtime_storage_paths.resolve_for(user_ncl, machine_ncl);
            let compiled = compile_effective_configuration(
                user_ncl,
                machine_ncl,
                &resolved.conductor_state_config,
            )?;
            let rendered = serde_json::to_string_pretty(&compiled).map_err(|source| {
                ConductorError::Serialization(format!(
                    "serializing compiled configuration JSON output failed: {source}"
                ))
            })?;
            println!("{rendered}");
            Ok(())
        }
        StateCommand::Export { path } => {
            let pointer =
                conductor.export_state_to_path(user_ncl, machine_ncl, options, &path).await?;
            println!("exported_state_path={}", path.display());
            println!("exported_state_hash={pointer}");
            Ok(())
        }
        StateCommand::Import { path } => {
            let pointer =
                conductor.import_state_from_path(user_ncl, machine_ncl, options, &path).await?;
            println!("imported_state_path={}", path.display());
            println!("imported_state_hash={pointer}");
            Ok(())
        }
        StateCommand::Edit { editor } => {
            edit_state_via_editor(&conductor, user_ncl, machine_ncl, options, editor.as_deref())
                .await
        }
        StateCommand::InvalidateToolCall { instance_id } => {
            invalidate_tool_call_by_instance_id(
                &conductor,
                user_ncl,
                machine_ncl,
                options,
                &instance_id,
            )
            .await
        }
    }
}

/// Invalidates one completed tool-call instance by deterministic instance id.
///
/// This helper removes the matching instance entry from orchestration state
/// and persists the updated state pointer through the regular state-replace
/// API so subsequent workflow runs recompute that call when needed.
async fn invalidate_tool_call_by_instance_id(
    conductor: &SimpleConductor<ConfiguredCas>,
    user_ncl: &Path,
    machine_ncl: &Path,
    options: StateMutationOptions,
    instance_id: &str,
) -> Result<(), ConductorError> {
    let normalized_instance_id = instance_id.trim();
    if normalized_instance_id.is_empty() {
        return Err(ConductorError::Workflow(
            "state invalidate-tool-call requires a non-empty instance id".to_string(),
        ));
    }

    let mut state = conductor.load_resolved_state(user_ncl, machine_ncl, options.clone()).await?;
    if state.instances.remove(normalized_instance_id).is_none() {
        return Err(ConductorError::Workflow(format!(
            "cannot invalidate tool call: instance id '{normalized_instance_id}' does not exist in orchestration state"
        )));
    }

    let pointer = conductor.replace_resolved_state(user_ncl, machine_ncl, state, options).await?;
    println!("invalidated_instance_id={normalized_instance_id}");
    println!("invalidated_state_hash={pointer}");
    Ok(())
}

/// Opens configured CAS backend from locator string.
async fn open_cas(locator: &str) -> Result<ConfiguredCas, ConductorError> {
    let config = CasConfig::from_locator_with_options(
        locator,
        CasLocatorParseOptions { allow_plain_filesystem_path: true },
    )
    .map_err(|err| ConductorError::Workflow(format!("invalid CAS locator '{locator}': {err}")))?;

    config
        .open()
        .await
        .map_err(|err| ConductorError::Workflow(format!("failed opening CAS backend: {err}")))
}

/// Executes workflow and prints run summary as pretty JSON.
#[allow(clippy::too_many_arguments)]
async fn run_workflow(
    cas: ConfiguredCas,
    user_ncl: &Path,
    machine_ncl: &Path,
    allow_tool_redefinition: bool,
    enable_profiler: bool,
    runtime_storage_paths: RuntimeStoragePaths,
    runtime_inherited_env_vars: Vec<String>,
    profile_output_path: Option<PathBuf>,
) -> Result<(), ConductorError> {
    let conductor = SimpleConductor::new(cas);
    let summary = conductor
        .run_workflow_with_options(
            user_ncl,
            machine_ncl,
            RunWorkflowOptions {
                allow_tool_redefinition,
                runtime_storage_paths,
                runtime_inherited_env_vars,
                profile_output_path,
                profiler_enabled: enable_profiler,
                progress_sender: None,
                cas_integrity_config: None,
            },
        )
        .await?;
    println!("executed_instances={}", summary.executed_instances);
    println!("cached_instances={}", summary.cached_instances);
    println!("rematerialized_instances={}", summary.rematerialized_instances);
    Ok(())
}

/// Opens current state in an editor, validates edits, and applies updates.
///
/// Edit-loop behavior:
/// - current resolved state is rendered to one temporary JSON file,
/// - editor is launched,
/// - decoded/validated state is applied,
/// - on decode/validation failure the user can iteratively re-edit.
async fn edit_state_via_editor(
    conductor: &SimpleConductor<ConfiguredCas>,
    user_ncl: &Path,
    machine_ncl: &Path,
    options: StateMutationOptions,
    editor_override: Option<&str>,
) -> Result<(), ConductorError> {
    let initial_state =
        conductor.load_resolved_state(user_ncl, machine_ncl, options.clone()).await?;
    let rendered = persisted_state_json_pretty(&initial_state)?;

    let temp = tempfile::Builder::new()
        .prefix("conductor-state-edit-")
        .suffix(".json")
        .tempfile()
        .map_err(|source| ConductorError::Io {
            operation: "creating temporary state edit file".to_string(),
            path: PathBuf::from("<tempfile>"),
            source,
        })?;
    let edit_path = temp.path().to_path_buf();
    std::fs::write(&edit_path, rendered.as_bytes()).map_err(|source| ConductorError::Io {
        operation: "writing initial state into temporary edit file".to_string(),
        path: edit_path.clone(),
        source,
    })?;

    loop {
        launch_editor(editor_override, &edit_path)?;

        let edited = std::fs::read(&edit_path).map_err(|source| ConductorError::Io {
            operation: "reading edited orchestration state".to_string(),
            path: edit_path.clone(),
            source,
        })?;

        match decode_state_from_slice(&edited) {
            Ok(state) => match conductor
                .replace_resolved_state(user_ncl, machine_ncl, state, options.clone())
                .await
            {
                Ok(pointer) => {
                    println!("edited_state_path={}", edit_path.display());
                    println!("edited_state_hash={pointer}");
                    return Ok(());
                }
                Err(error) => {
                    eprintln!("state edit validation failed: {error}");
                    if !should_retry_state_edit()? {
                        return Err(ConductorError::Workflow(
                            "state edit aborted after validation failure".to_string(),
                        ));
                    }
                }
            },
            Err(error) => {
                eprintln!("state edit decode failed: {error}");
                if !should_retry_state_edit()? {
                    return Err(ConductorError::Workflow(
                        "state edit aborted after decode failure".to_string(),
                    ));
                }
            }
        }
    }
}

/// Returns whether interactive state-edit flow should retry after a failure.
fn should_retry_state_edit() -> Result<bool, ConductorError> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Ok(false);
    }

    print!("Re-open editor to fix state and retry? [Y/n]: ");
    std::io::stdout().flush().map_err(|source| ConductorError::Io {
        operation: "flushing state edit retry prompt".to_string(),
        path: PathBuf::from("<stdout>"),
        source,
    })?;

    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|source| ConductorError::Io {
        operation: "reading state edit retry response".to_string(),
        path: PathBuf::from("<stdin>"),
        source,
    })?;

    let response = line.trim().to_ascii_lowercase();
    Ok(response.is_empty() || response == "y" || response == "yes")
}

/// Launches editor command against one state-edit file path.
fn launch_editor(editor_override: Option<&str>, edit_path: &Path) -> Result<(), ConductorError> {
    let editor = resolve_editor_command(editor_override);
    let (program, args) = parse_editor_command(&editor)?;
    let status = Command::new(&program).args(args).arg(edit_path).status().map_err(|source| {
        ConductorError::Io {
            operation: "launching state editor command".to_string(),
            path: PathBuf::from(program),
            source,
        }
    })?;

    if !status.success() {
        return Err(ConductorError::Workflow(format!(
            "state editor command '{editor}' exited with non-zero status {status}"
        )));
    }

    Ok(())
}

/// Resolves editor command string with git-style environment precedence.
#[must_use]
fn resolve_editor_command(editor_override: Option<&str>) -> String {
    if let Some(override_value) = editor_override {
        let trimmed = override_value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    for key in ["GIT_EDITOR", "VISUAL", "EDITOR"] {
        if let Some(value) = std::env::var_os(key) {
            let owned = value.to_string_lossy().to_string();
            let trimmed = owned.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }

    if cfg!(windows) { "notepad".to_string() } else { "vi".to_string() }
}

/// Parses one editor command line into executable path plus argument vector.
fn parse_editor_command(command: &str) -> Result<(String, Vec<String>), ConductorError> {
    let mut tokens = Vec::<String>::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;

    for ch in command.chars() {
        if quote.is_none() && ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            continue;
        }

        match quote {
            Some(active) if ch == active => {
                quote = None;
            }
            None if ch == '"' || ch == '\'' => {
                quote = Some(ch);
            }
            Some(_) | None => {
                current.push(ch);
            }
        }
    }

    if quote.is_some() {
        return Err(ConductorError::Workflow(format!(
            "invalid editor command '{command}': unterminated quote"
        )));
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    let Some(program) = tokens.first().cloned() else {
        return Err(ConductorError::Workflow("editor command must not be empty".to_string()));
    };
    let args = if tokens.len() > 1 { tokens[1..].to_vec() } else { Vec::new() };
    Ok((program, args))
}

/// Handles import command variants.
async fn handle_import(
    cas: ConfiguredCas,
    _user_ncl: &Path,
    machine_ncl: &Path,
    args: ImportArgs,
) -> Result<(), ConductorError> {
    match args.command {
        #[cfg(feature = "tool-presets")]
        ImportCommand::Tool { path, preset, name, process_name } => {
            if let Some(tool_preset) = preset {
                return import_common_tool(
                    cas,
                    machine_ncl,
                    tool_preset,
                    name.as_deref(),
                    process_name.as_deref(),
                )
                .await;
            }

            let import_path = path.as_deref().ok_or_else(|| {
                ConductorError::Workflow(
                    "import tool requires a path unless --preset is provided".to_string(),
                )
            })?;
            let tool_name = name.as_deref().ok_or_else(|| {
                ConductorError::Workflow(
                    "import tool requires --name when importing from path".to_string(),
                )
            })?;

            import_tool(cas, machine_ncl, import_path, tool_name, process_name.as_deref()).await
        }
        #[cfg(not(feature = "tool-presets"))]
        ImportCommand::Tool { path, name, process_name } => {
            let import_path = path.as_deref().ok_or_else(|| {
                ConductorError::Workflow("import tool requires a path".to_string())
            })?;
            let tool_name = name.as_deref().ok_or_else(|| {
                ConductorError::Workflow(
                    "import tool requires --name when importing from path".to_string(),
                )
            })?;

            import_tool(cas, machine_ncl, import_path, tool_name, process_name.as_deref()).await
        }
        ImportCommand::Data { path, description } => {
            import_data(cas, machine_ncl, &path, description.as_deref()).await
        }
    }
}

/// Installs one common upstream executable and imports it into machine config.
///
/// The installer fetches the executable bytes through conductor API helper
/// (release-asset download path), stores them in CAS, then wires
/// `tool_configs.<tool>.content_map` plus executable metadata for immediate
/// workflow use.
#[cfg(feature = "tool-presets")]
async fn import_common_tool(
    cas: ConfiguredCas,
    machine_ncl: &Path,
    tool: CommonExecutableTool,
    logical_name_override: Option<&str>,
    process_name_override: Option<&str>,
) -> Result<(), ConductorError> {
    let logical_tool_name = logical_name_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(tool.logical_tool_name())
        .to_string();

    let payload = fetch_common_executable_tool_payload(tool)?;
    let mut machine = load_machine_document(machine_ncl)?;
    let hash = cas.put(payload.executable_bytes).await?;
    let imported_content_map = BTreeMap::from([(payload.executable_file_name.clone(), hash)]);

    let resolved_process_name = process_name_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(payload.executable_file_name.as_str());
    let description_override = format!(
        "Installed by conductor CLI tool preset importer from upstream release assets for '{}'",
        tool.logical_tool_name()
    );

    register_or_merge_imported_tool(
        &mut machine,
        &logical_tool_name,
        Path::new(payload.executable_file_name.as_str()),
        Some(resolved_process_name),
        imported_content_map,
        Some(description_override.as_str()),
    )?;

    save_machine_document(machine_ncl, &machine)?;
    Ok(())
}

/// Imports one tool path into CAS and updates tool runtime content-map config in the
/// program-edited document.
async fn import_tool(
    cas: ConfiguredCas,
    machine_ncl: &Path,
    path: &Path,
    tool_name: &str,
    process_name: Option<&str>,
) -> Result<(), ConductorError> {
    if tool_name.trim().is_empty() {
        return Err(ConductorError::Workflow("tool name cannot be empty".to_string()));
    }

    let mut machine = load_machine_document(machine_ncl)?;

    let files = collect_tool_files(path)?;
    let base_dir = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf()
    };

    let mut imported_content_map = BTreeMap::new();

    for file in files {
        let content = std::fs::read(&file).map_err(|source| ConductorError::Io {
            operation: "reading tool file for import".to_string(),
            path: file.clone(),
            source,
        })?;
        let hash = cas.put(content).await?;
        let relative = normalized_relative_path(&base_dir, &file)?;
        imported_content_map.insert(relative, hash);
    }

    register_or_merge_imported_tool(
        &mut machine,
        tool_name,
        path,
        process_name,
        imported_content_map,
        None,
    )?;

    save_machine_document(machine_ncl, &machine)?;
    Ok(())
}

/// Imports one external data file into CAS and records it in the
/// program-edited document.
async fn import_data(
    cas: ConfiguredCas,
    machine_ncl: &Path,
    path: &Path,
    description: Option<&str>,
) -> Result<(), ConductorError> {
    let mut machine = load_machine_document(machine_ncl)?;
    let bytes = std::fs::read(path).map_err(|source| ConductorError::Io {
        operation: "reading external data for import".to_string(),
        path: path.to_path_buf(),
        source,
    })?;
    let hash = cas.put(bytes).await?;
    let default_description = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ConductorError::Workflow(format!(
                "external data path '{}' must end in a valid UTF-8 file name",
                path.display()
            ))
        })?
        .to_string();

    machine.add_external_data(
        hash,
        AddExternalDataOptions::new(ExternalContentRef {
            description: description
                .map(std::string::ToString::to_string)
                .or_else(|| Some(default_description.clone())),
            save: None,
        })
        .overwrite_existing(true),
    )?;
    save_machine_document(machine_ncl, &machine)?;
    Ok(())
}

/// Runs root-based GC using references from user/machine docs and state pointer.
async fn run_gc(
    cas: ConfiguredCas,
    user_ncl: &Path,
    machine_ncl: &Path,
    conductor_state_config: &Path,
) -> Result<(), ConductorError> {
    let user = load_user_document(user_ncl)?;
    let machine = load_machine_document(machine_ncl)?;
    let state_doc = load_state_document(conductor_state_config)?;
    let state_pointer = state_doc.state_pointer;

    // Load orchestration state from CAS if a pointer exists.
    let state = if let Some(sp) = &state_pointer {
        match decode_state(&cas, *sp).await {
            Ok(state) => state,
            Err(ConductorError::Cas(CasError::NotFound(_))) => OrchestrationState::default(),
            Err(e) => return Err(e),
        }
    } else {
        OrchestrationState::default()
    };

    let gc_roots =
        compute_gc_roots(&user.external_data, &machine.external_data, state_pointer, &state);

    let roots_vec: Vec<Hash> = gc_roots.iter().copied().collect();
    let optimize = cas.optimize_once(mediapm_cas::OptimizeOptions::default()).await?;
    let pruned = cas.prune_constraints().await?;
    let gc = cas.gc_sweep(&gc_roots).await?;

    println!("gc_roots_computed={}", roots_vec.len());
    println!("optimize_rewritten_objects={}", optimize.rewritten_objects);
    println!("constraints_removed_candidates={}", pruned.removed_candidates);
    println!("gc_sweep_deleted={}", gc.deleted_count);
    Ok(())
}

/// Executes passthrough to the CAS CLI in-process.
///
/// This path reuses `mediapm-cas` clap parsing and command dispatch directly,
/// so conductor does not require a sibling `mediapm-cas` executable.
async fn passthrough_cas(args: &[String], default_root: &Path) -> Result<(), ConductorError> {
    let injected = inject_cas_root_arg_if_missing(args, default_root);
    mediapm_cas::cli::run_from_passthrough_args(&injected)
        .await
        .map_err(|error| ConductorError::Workflow(format!("cas passthrough failed: {error}")))
}
