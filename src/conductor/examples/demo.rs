//! Persistent conductor demo exercising all builtin tools once.
//!
//! This example keeps host-side setup compact while demonstrating a complete
//! conductor run loop:
//! - clears `examples/.artifacts/demo` before each run,
//! - writes exactly two config documents (`conductor.ncl` and
//!   `conductor.machine.ncl`),
//! - runs one pipeline that uses all official builtins plus one custom
//!   executable support tool (`concat-tool`),
//! - keeps `import` as the first step and `export` as the final step,
//! - uses relative `path` values for `import`/`export` rooted at the outermost
//!   config directory,
//! - runs workflows twice to demonstrate cache reuse,
//! - writes one compact `manifest.json` snapshot for inspection.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{
    ConductorApi, ExternalContentRef, MachineNickelDocument, NickelDocumentMetadata,
    NickelIdentity, OrchestrationState, OutputPolicy, RunSummary, RuntimeDiagnostics,
    RuntimeStorageConfig, SimpleConductor, ToolConfigSpec, ToolInputSpec, ToolKindSpec, ToolSpec,
    UserNickelDocument, WorkflowSpec, WorkflowStepSpec, persisted_state_json_pretty,
};
use serde::Serialize;
use serde_json::{Value, json};

/// Default runtime conductor folder name used by conductor CLI/API.
const DEMO_DEFAULT_CONDUCTOR_DIR: &str = ".conductor";

/// Default volatile runtime state filename under `DEMO_DEFAULT_CONDUCTOR_DIR`.
const DEMO_DEFAULT_STATE_NCL_FILE: &str = "state.ncl";

/// Default CAS store folder name under `DEMO_DEFAULT_CONDUCTOR_DIR`.
const DEMO_DEFAULT_CAS_STORE_DIR: &str = "store";

/// Shared result type for this example binary.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Serializable snapshot of one workflow run summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct RunSummarySnapshot {
    /// Number of instances executed in this run.
    executed_instances: usize,
    /// Number of instances served from cache in this run.
    cached_instances: usize,
    /// Number of instances re-materialized in this run.
    rematerialized_instances: usize,
}

impl From<RunSummary> for RunSummarySnapshot {
    /// Converts a public runtime summary into a manifest-friendly snapshot.
    fn from(value: RunSummary) -> Self {
        Self {
            executed_instances: value.executed_instances,
            cached_instances: value.cached_instances,
            rematerialized_instances: value.rematerialized_instances,
        }
    }
}

/// Serializable snapshot of runtime diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DiagnosticsSnapshot {
    /// Number of active step workers.
    worker_pool_size: usize,
    /// Number of recent scheduler traces retained in memory.
    trace_event_count: usize,
    /// Total scheduler RPC fallback events observed.
    rpc_fallbacks_total: u64,
}

impl From<RuntimeDiagnostics> for DiagnosticsSnapshot {
    /// Converts runtime diagnostics into a compact serializable shape.
    fn from(value: RuntimeDiagnostics) -> Self {
        Self {
            worker_pool_size: value.worker_pool_size,
            trace_event_count: value.recent_traces.len(),
            rpc_fallbacks_total: value.scheduler.rpc_fallbacks_total,
        }
    }
}

/// JSON manifest persisted under `.artifacts/demo/manifest.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DemoManifest {
    /// Unix timestamp when the manifest was generated.
    generated_unix_epoch_seconds: u64,
    /// Artifact root used by this demo run.
    artifact_root: String,
    /// CAS root used by this demo run.
    cas_root: String,
    /// Path to generated user document.
    user_ncl_path: String,
    /// Path to generated machine document.
    machine_ncl_path: String,
    /// Workflow names included in the user document.
    workflow_names: Vec<String>,
    /// Absolute export destination used by `path_mode = absolute`.
    absolute_export_file_path: String,
    /// Absolute folder export destination used by `path_mode = absolute`.
    absolute_export_folder_path: String,
    /// Whether the relative export artifact exists.
    relative_export_exists: bool,
    /// Whether the absolute export artifact exists.
    absolute_export_exists: bool,
    /// Whether the absolute exported folder contains expected file.
    absolute_folder_export_contains_file: bool,
    /// Number of instances in final orchestration state.
    state_instance_count_after_second_run: usize,
    /// Tool names observed in the final orchestration state.
    tool_names_after_second_run: Vec<String>,
    /// First run summary.
    first_run: RunSummarySnapshot,
    /// Second run summary.
    second_run: RunSummarySnapshot,
    /// Diagnostics after first run.
    diagnostics_after_first_run: DiagnosticsSnapshot,
    /// Diagnostics after second run.
    diagnostics_after_second_run: DiagnosticsSnapshot,
}

/// Output paths printed to stdout when the demo finishes.
#[derive(Debug, Clone)]
struct DemoRunPaths {
    /// Artifact root containing config and manifest outputs.
    artifact_root: PathBuf,
    /// CAS root used by the conductor runtime.
    cas_root: PathBuf,
    /// Path to the persisted manifest.
    manifest_path: PathBuf,
    /// Path to pretty-formatted orchestration state snapshot JSON.
    orchestration_state_path: PathBuf,
}

/// Inputs used to build one feature-rich demo user document.
#[derive(Debug, Clone)]
struct DemoWorkflowBuildInputs {
    /// External-data hash used by `${external_data.demo_banner}` interpolation.
    banner_hash: Hash,
    /// Absolute machine-document path used for absolute import.
    absolute_machine_path: String,
    /// Absolute source path used by fs copy in absolute mode.
    absolute_fs_source_path: String,
    /// Absolute destination path used by fs copy in absolute mode.
    absolute_fs_copy_dest_path: String,
    /// Absolute file path used by export in absolute mode.
    absolute_export_file_path: String,
    /// Absolute folder path used by export kind='folder' absolute mode.
    absolute_export_folder_path: String,
    /// CAS hash for concat-tool executable bytes.
    concat_tool_binary_hash: Hash,
    /// CAS hash for concat-tool fixed resource text.
    concat_tool_resource_hash: Hash,
}

/// Returns deterministic artifact root for this persistent demo.
fn artifact_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples").join(".artifacts").join("demo")
}

/// Returns workspace root derived from `src/conductor` manifest location.
fn workspace_root() -> ExampleResult<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .ok_or_else(|| "failed to resolve workspace root from CARGO_MANIFEST_DIR".into())
}

/// Returns host-platform concat-tool executable filename.
fn concat_tool_executable_name() -> &'static str {
    if cfg!(windows) { "concat-tool.exe" } else { "concat-tool" }
}

/// Builds `support/concat-tool` and returns the host binary path under
/// workspace `target/debug`.
fn build_concat_tool_binary(workspace_root: &Path) -> ExampleResult<PathBuf> {
    let status = Command::new("cargo")
        .arg("build")
        .arg("-p")
        .arg("mediapm-conductor-examples-concat-tool")
        .arg("--bin")
        .arg("concat-tool")
        .current_dir(workspace_root)
        .status()?;

    if !status.success() {
        return Err(
            format!("building support concat-tool executable failed with status {status}").into()
        );
    }

    let binary_path =
        workspace_root.join("target").join("debug").join(concat_tool_executable_name());
    if !binary_path.exists() {
        return Err(format!(
            "expected concat-tool binary was not produced at '{}'",
            binary_path.display()
        )
        .into());
    }

    Ok(binary_path)
}

/// Recreates artifact root so every demo run starts from a clean slate.
fn reset_artifact_root() -> ExampleResult<PathBuf> {
    let root = artifact_root();
    if root.exists() {
        remove_dir_all_with_retry(&root)?;
    }
    fs::create_dir_all(&root)?;
    Ok(root)
}

/// Removes one directory with a short retry policy for Windows file locking.
fn remove_dir_all_with_retry(path: &Path) -> ExampleResult<()> {
    const ATTEMPTS: usize = 6;
    const BACKOFF_MS: u64 = 40;

    let mut last_error: Option<std::io::Error> = None;
    for attempt in 0..ATTEMPTS {
        match fs::remove_dir_all(path) {
            Ok(()) => return Ok(()),
            Err(error) => {
                let is_retryable = error.kind() == std::io::ErrorKind::PermissionDenied
                    || error.raw_os_error() == Some(32);
                if !is_retryable || attempt + 1 == ATTEMPTS {
                    last_error = Some(error);
                    break;
                }
                thread::sleep(Duration::from_millis(BACKOFF_MS));
                last_error = Some(error);
            }
        }
    }

    match last_error {
        Some(error) => Err(Box::new(error)),
        None => Ok(()),
    }
}

/// Writes UTF-8 text while creating parent directories as needed.
fn write_text_file(path: &Path, content: &str) -> ExampleResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)?;
    Ok(())
}

/// Writes one JSON value in pretty format with parent-directory creation.
fn write_json_file<T>(path: &Path, value: &T) -> ExampleResult<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

/// Converts one filesystem path into a normalized display string.
fn display_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

/// Returns current Unix timestamp in seconds.
fn unix_timestamp_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|value| value.as_secs()).unwrap_or(0)
}

/// Builds a user document that showcases broad conductor behavior.
fn build_user_document(inputs: &DemoWorkflowBuildInputs) -> UserNickelDocument {
    UserNickelDocument {
        metadata: NickelDocumentMetadata {
            id: "demo-conductor-feature-showcase".to_string(),
            identity: NickelIdentity { first: "demo".to_string(), last: "conductor".to_string() },
        },
        runtime_storage: RuntimeStorageConfig {
            conductor_dir: Some(DEMO_DEFAULT_CONDUCTOR_DIR.to_string()),
            state_ncl: Some(format!(
                "{}/{}",
                DEMO_DEFAULT_CONDUCTOR_DIR, DEMO_DEFAULT_STATE_NCL_FILE
            )),
            cas_store_dir: Some(format!(
                "{}/{}",
                DEMO_DEFAULT_CONDUCTOR_DIR, DEMO_DEFAULT_CAS_STORE_DIR
            )),
        },
        external_data: BTreeMap::from([(
            "demo_banner".to_string(),
            ExternalContentRef {
                hash: inputs.banner_hash,
                description: Some(
                    "banner payload used by ${external_data.demo_banner} bindings".to_string(),
                ),
            },
        )]),
        workflows: BTreeMap::from([
            (
                "01_feature_showcase".to_string(),
                WorkflowSpec {
                    steps: vec![
                        WorkflowStepSpec {
                            id: "import_user_relative".to_string(),
                            tool: "import@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("kind".to_string(), "file".to_string().into()),
                                ("path_mode".to_string(), "relative".to_string().into()),
                                ("path".to_string(), "conductor.ncl".to_string().into()),
                            ]),
                            depends_on: Vec::new(),
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "import_machine_absolute".to_string(),
                            tool: "import@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("kind".to_string(), "file".to_string().into()),
                                ("path_mode".to_string(), "absolute".to_string().into()),
                                ("path".to_string(), inputs.absolute_machine_path.clone().into()),
                            ]),
                            depends_on: Vec::new(),
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "fs_prepare_relative_dir".to_string(),
                            tool: "fs@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("op".to_string(), "ensure_dir".to_string().into()),
                                ("path_mode".to_string(), "relative".to_string().into()),
                                ("path".to_string(), "runtime/generated".to_string().into()),
                            ]),
                            depends_on: Vec::new(),
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "fs_write_relative_snapshot".to_string(),
                            tool: "fs@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("op".to_string(), "write_text".to_string().into()),
                                ("path_mode".to_string(), "relative".to_string().into()),
                                (
                                    "path".to_string(),
                                    "runtime/generated/user-plus-machine.txt".to_string().into(),
                                ),
                                (
                                    "content".to_string(),
                                    "${step_output.import_machine_absolute.result}".to_string().into(),
                                ),
                            ]),
                            depends_on: vec!["import_machine_absolute".to_string()],
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "fs_copy_absolute_side_effect".to_string(),
                            tool: "fs@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("op".to_string(), "copy".to_string().into()),
                                ("path_mode".to_string(), "absolute".to_string().into()),
                                ("path".to_string(), inputs.absolute_fs_source_path.clone().into()),
                                (
                                    "dest".to_string(),
                                    inputs.absolute_fs_copy_dest_path.clone().into(),
                                ),
                            ]),
                            depends_on: Vec::new(),
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "echo_with_external_data".to_string(),
                            tool: "echo@1.0.0".to_string(),
                            inputs: BTreeMap::from([(
                                "text".to_string(),
                                "banner=${external_data.demo_banner} | import=${step_output.import_user_relative.result}"
                                    .to_string()
                                    .into(),
                            )]),
                            depends_on: vec!["import_user_relative".to_string()],
                            outputs: BTreeMap::from([(
                                "result".to_string(),
                                OutputPolicy { save: Some(true), force_full: Some(true) },
                            )]),
                        },
                        WorkflowStepSpec {
                            id: "concat_with_fixed_resource".to_string(),
                            tool: "concat-tool@1.0.0".to_string(),
                            inputs: BTreeMap::from([(
                                "payload".to_string(),
                                "${step_output.echo_with_external_data.result}".to_string().into(),
                            )]),
                            depends_on: vec!["echo_with_external_data".to_string()],
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "archive_pack_banner".to_string(),
                            tool: "archive@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("action".to_string(), "pack".to_string().into()),
                                ("kind".to_string(), "file".to_string().into()),
                                ("entry_name".to_string(), "banner.txt".to_string().into()),
                                (
                                    "content".to_string(),
                                    "${step_output.concat_with_fixed_resource.result}".to_string().into(),
                                ),
                            ]),
                            depends_on: vec!["concat_with_fixed_resource".to_string()],
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "archive_repack_banner".to_string(),
                            tool: "archive@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("action".to_string(), "repack".to_string().into()),
                                (
                                    "archive".to_string(),
                                    "${step_output.archive_pack_banner.result}".to_string().into(),
                                ),
                            ]),
                            depends_on: vec!["archive_pack_banner".to_string()],
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "export_relative_archive".to_string(),
                            tool: "export@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("kind".to_string(), "file".to_string().into()),
                                ("path_mode".to_string(), "relative".to_string().into()),
                                (
                                    "path".to_string(),
                                    "exports/relative/showcase.zip".to_string().into(),
                                ),
                                (
                                    "content".to_string(),
                                    "${step_output.archive_repack_banner.result}".to_string().into(),
                                ),
                            ]),
                            depends_on: vec!["archive_repack_banner".to_string()],
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "archive_unpack_banner".to_string(),
                            tool: "archive@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("action".to_string(), "unpack".to_string().into()),
                                (
                                    "archive".to_string(),
                                    "${step_output.archive_repack_banner.result}".to_string().into(),
                                ),
                            ]),
                            depends_on: vec!["archive_repack_banner".to_string()],
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "export_absolute_folder".to_string(),
                            tool: "export@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("kind".to_string(), "folder".to_string().into()),
                                ("path_mode".to_string(), "absolute".to_string().into()),
                                (
                                    "path".to_string(),
                                    inputs.absolute_export_folder_path.clone().into(),
                                ),
                                (
                                    "content".to_string(),
                                    "${step_output.archive_unpack_banner.result}".to_string().into(),
                                ),
                            ]),
                            depends_on: vec!["archive_unpack_banner".to_string()],
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "export_absolute_file".to_string(),
                            tool: "export@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("kind".to_string(), "file".to_string().into()),
                                ("path_mode".to_string(), "absolute".to_string().into()),
                                (
                                    "path".to_string(),
                                    inputs.absolute_export_file_path.clone().into(),
                                ),
                                (
                                    "content".to_string(),
                                    "${step_output.echo_with_external_data.result}".to_string().into(),
                                ),
                            ]),
                            depends_on: vec![
                                "echo_with_external_data".to_string(),
                                "fs_copy_absolute_side_effect".to_string(),
                            ],
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "echo_side_effect_barrier".to_string(),
                            tool: "echo@1.0.0".to_string(),
                            inputs: BTreeMap::from([(
                                "text".to_string(),
                                "side effects completed".to_string().into(),
                            )]),
                            depends_on: vec![
                                "fs_copy_absolute_side_effect".to_string(),
                                "export_absolute_folder".to_string(),
                                "export_absolute_file".to_string(),
                            ],
                            outputs: BTreeMap::new(),
                        },
                    ],
                },
            ),
            (
                "02_cache_and_depends_on".to_string(),
                WorkflowSpec {
                    steps: vec![
                        WorkflowStepSpec {
                            id: "cached_source".to_string(),
                            tool: "echo@1.0.0".to_string(),
                            inputs: BTreeMap::from([(
                                "text".to_string(),
                                "${external_data.demo_banner}".to_string().into(),
                            )]),
                            depends_on: Vec::new(),
                            outputs: BTreeMap::from([(
                                "result".to_string(),
                                OutputPolicy { save: Some(false), force_full: Some(false) },
                            )]),
                        },
                        WorkflowStepSpec {
                            id: "fs_dependency_only".to_string(),
                            tool: "fs@1.0.0".to_string(),
                            inputs: BTreeMap::from([
                                ("op".to_string(), "write_text".to_string().into()),
                                ("path_mode".to_string(), "relative".to_string().into()),
                                (
                                    "path".to_string(),
                                    "runtime/cache/probe.txt".to_string().into(),
                                ),
                                ("content".to_string(), "cache-probe".to_string().into()),
                            ]),
                            depends_on: Vec::new(),
                            outputs: BTreeMap::new(),
                        },
                        WorkflowStepSpec {
                            id: "cached_consumer".to_string(),
                            tool: "echo@1.0.0".to_string(),
                            inputs: BTreeMap::from([(
                                "text".to_string(),
                                "consumer=${step_output.cached_source.result}".to_string().into(),
                            )]),
                            depends_on: vec![
                                "cached_source".to_string(),
                                "fs_dependency_only".to_string(),
                            ],
                            outputs: BTreeMap::from([(
                                "result".to_string(),
                                OutputPolicy { save: Some(true), force_full: Some(true) },
                            )]),
                        },
                    ],
                },
            ),
        ]),
        ..UserNickelDocument::default()
    }
}

/// Builds a machine document that registers required demo builtins and
/// per-tool runtime execution limits.
fn build_machine_document(inputs: &DemoWorkflowBuildInputs) -> MachineNickelDocument {
    let builtin_tool = |name: &str, version: &str, is_impure: bool| ToolSpec {
        is_impure,
        kind: ToolKindSpec::Builtin { name: name.to_string(), version: version.to_string() },
        ..ToolSpec::default()
    };

    let concat_tool = ToolSpec {
        is_impure: false,
        inputs: BTreeMap::from([("payload".to_string(), ToolInputSpec::default())]),
        kind: ToolKindSpec::Executable {
            command: vec![
                "${os.windows?concat-tool.exe}${os.linux?concat-tool}${os.macos?concat-tool}"
                    .to_string(),
                "--input-file".to_string(),
                "${inputs.payload:file(payload.txt)}".to_string(),
            ],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        ..ToolSpec::default()
    };

    MachineNickelDocument {
        metadata: NickelDocumentMetadata {
            id: "demo-machine".to_string(),
            identity: NickelIdentity { first: "demo".to_string(), last: "machine".to_string() },
        },
        tools: BTreeMap::from([
            ("echo@1.0.0".to_string(), builtin_tool("echo", "1.0.0", false)),
            ("fs@1.0.0".to_string(), builtin_tool("fs", "1.0.0", true)),
            ("archive@1.0.0".to_string(), builtin_tool("archive", "1.0.0", false)),
            ("import@1.0.0".to_string(), builtin_tool("import", "1.0.0", false)),
            ("export@1.0.0".to_string(), builtin_tool("export", "1.0.0", true)),
            ("concat-tool@1.0.0".to_string(), concat_tool),
        ]),
        tool_configs: BTreeMap::from([
            (
                "echo@1.0.0".to_string(),
                ToolConfigSpec { max_concurrent_calls: 1, content_map: None },
            ),
            ("fs@1.0.0".to_string(), ToolConfigSpec { max_concurrent_calls: 1, content_map: None }),
            (
                "archive@1.0.0".to_string(),
                ToolConfigSpec { max_concurrent_calls: 2, content_map: None },
            ),
            (
                "import@1.0.0".to_string(),
                ToolConfigSpec { max_concurrent_calls: 1, content_map: None },
            ),
            (
                "export@1.0.0".to_string(),
                ToolConfigSpec { max_concurrent_calls: 1, content_map: None },
            ),
            (
                "concat-tool@1.0.0".to_string(),
                ToolConfigSpec {
                    max_concurrent_calls: 1,
                    content_map: Some(BTreeMap::from([
                        ("concat-tool".to_string(), inputs.concat_tool_binary_hash),
                        ("concat-tool.exe".to_string(), inputs.concat_tool_binary_hash),
                        ("resource.txt".to_string(), inputs.concat_tool_resource_hash),
                    ])),
                },
            ),
        ]),
        ..MachineNickelDocument::default()
    }
}

/// Writes one user document as latest-schema Nickel source.
fn write_user_document(path: &Path, document: &UserNickelDocument) -> ExampleResult<()> {
    write_text_file(
        path,
        &format!("{}\n", render_nickel_value(&user_document_to_json(document), 0)),
    )
}

/// Writes one machine document as latest-schema Nickel source.
fn write_machine_document(path: &Path, document: &MachineNickelDocument) -> ExampleResult<()> {
    write_text_file(
        path,
        &format!("{}\n", render_nickel_value(&machine_document_to_json(document), 0)),
    )
}

/// Converts user document into persisted-envelope JSON shape.
fn user_document_to_json(document: &UserNickelDocument) -> Value {
    let mut object = serde_json::Map::new();
    object.insert("version".to_string(), json!(1));

    if !document.runtime_storage.is_empty() {
        object.insert(
            "runtime_storage".to_string(),
            json!({
                "conductor_dir": document.runtime_storage.conductor_dir.clone(),
                "state_ncl": document.runtime_storage.state_ncl.clone(),
                "cas_store_dir": document.runtime_storage.cas_store_dir.clone(),
            }),
        );
    }

    if !document.external_data.is_empty() {
        object.insert("external_data".to_string(), json!(document.external_data));
    }

    if !document.tools.is_empty() {
        object.insert("tools".to_string(), json!(tool_specs_to_wire_json(&document.tools)));
    }

    object.insert("workflows".to_string(), json!(document.workflows));

    if !document.tool_configs.is_empty() {
        object.insert("tool_configs".to_string(), json!(document.tool_configs));
    }

    if !document.impure_timestamps.is_empty() {
        object.insert("impure_timestamps".to_string(), json!(document.impure_timestamps));
    }

    if let Some(pointer) = document.state_pointer {
        object.insert("state_pointer".to_string(), json!(pointer));
    }

    Value::Object(object)
}

/// Converts machine document into persisted-envelope JSON shape.
fn machine_document_to_json(document: &MachineNickelDocument) -> Value {
    let mut object = serde_json::Map::new();
    object.insert("version".to_string(), json!(1));
    if !document.runtime_storage.is_empty() {
        object.insert(
            "runtime_storage".to_string(),
            json!({
                "conductor_dir": document.runtime_storage.conductor_dir.clone(),
                "state_ncl": document.runtime_storage.state_ncl.clone(),
                "cas_store_dir": document.runtime_storage.cas_store_dir.clone(),
            }),
        );
    }
    object.insert("external_data".to_string(), json!(document.external_data));
    object.insert("tools".to_string(), json!(tool_specs_to_wire_json(&document.tools)));
    object.insert("workflows".to_string(), json!(document.workflows));
    object.insert("tool_configs".to_string(), json!(document.tool_configs));
    object.insert("impure_timestamps".to_string(), json!(document.impure_timestamps));
    object.insert("state_pointer".to_string(), json!(document.state_pointer));
    Value::Object(object)
}

/// Converts runtime tool specs into strict persisted v1 wire-shape JSON.
fn tool_specs_to_wire_json(tools: &BTreeMap<String, ToolSpec>) -> BTreeMap<String, Value> {
    tools
        .iter()
        .map(|(tool_name, tool_spec)| (tool_name.clone(), tool_spec_to_wire_json(tool_spec)))
        .collect()
}

/// Converts one runtime `ToolSpec` into strict persisted orchestration-state
/// metadata JSON.
///
/// Builtin metadata is normalized to identity-only
/// (`kind`/`name`/`version`) while executable metadata keeps full `ToolSpec`
/// shape.
fn tool_spec_to_wire_json(tool_spec: &ToolSpec) -> Value {
    match &tool_spec.kind {
        ToolKindSpec::Builtin { name, version } => {
            json!({ "kind": "builtin", "name": name, "version": version })
        }
        ToolKindSpec::Executable { command, env_vars, success_codes } => json!({
            "kind": "executable",
            "is_impure": tool_spec.is_impure,
            "inputs": tool_spec.inputs,
            "command": command,
            "env_vars": env_vars,
            "success_codes": success_codes,
            "outputs": tool_spec.outputs,
        }),
    }
}

/// Returns whether one field key can be emitted bare in Nickel record syntax.
fn is_bare_nickel_identifier(key: &str) -> bool {
    let mut chars = key.chars().peekable();

    while matches!(chars.peek(), Some('_')) {
        let _ = chars.next();
    }

    let Some(head) = chars.next() else {
        return false;
    };

    if !head.is_ascii_alphabetic() {
        return false;
    }

    chars
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '\''))
}

/// Renders one record field name for Nickel output.
fn render_field_name(name: &str) -> String {
    if is_bare_nickel_identifier(name) {
        name.to_string()
    } else {
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""))
    }
}

/// Renders JSON values as deterministic Nickel source.
fn render_nickel_value(value: &Value, indent: usize) -> String {
    let pad = " ".repeat(indent);
    let next_pad = " ".repeat(indent + 2);

    match value {
        Value::Null => "null".to_string(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(items) => {
            if items.is_empty() {
                "[]".to_string()
            } else {
                let rendered = items
                    .iter()
                    .map(|item| format!("{next_pad}{},", render_nickel_value(item, indent + 2)))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("[\n{rendered}\n{pad}]")
            }
        }
        Value::Object(entries) => {
            if entries.is_empty() {
                "{}".to_string()
            } else {
                let mut ordered = entries.iter().collect::<Vec<_>>();
                ordered.sort_by(|(left, _), (right, _)| left.cmp(right));
                let rendered = ordered
                    .into_iter()
                    .map(|(key, entry)| {
                        format!(
                            "{next_pad}{} = {},",
                            render_field_name(key),
                            render_nickel_value(entry, indent + 2)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{{\n{rendered}\n{pad}}}")
            }
        }
    }
}

/// Returns sorted unique tool names present in orchestration state.
fn collect_tool_names(state: &OrchestrationState) -> Vec<String> {
    let mut names = state
        .instances
        .values()
        .map(|instance| instance.tool_name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    names.sort();
    names
}

/// Executes the demo workflows and writes persistent artifacts.
async fn generate_demo_artifacts() -> ExampleResult<DemoRunPaths> {
    let root = reset_artifact_root()?;
    let workspace_root = workspace_root()?;
    let conductor_dir = root.join(DEMO_DEFAULT_CONDUCTOR_DIR);
    let cas_root = conductor_dir.join(DEMO_DEFAULT_CAS_STORE_DIR);
    let user_path = root.join("conductor.ncl");
    let machine_path = root.join("conductor.machine.ncl");
    let relative_export_path = root.join("exports/relative/showcase.zip");
    let absolute_export_file_path = root.join("exports/absolute/echo.txt");
    let absolute_export_folder_path = root.join("exports/absolute-folder");
    let absolute_fs_source_path = root.join("imports/absolute-source.txt");
    let absolute_fs_copy_dest_path = root.join("runtime/absolute-copy/copied-source.txt");
    let concat_resource_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples/support/concat-tool/resource/fixed.txt");

    write_text_file(&absolute_fs_source_path, "source payload copied by fs path_mode='absolute'")?;

    let concat_binary_path = build_concat_tool_binary(&workspace_root)?;

    let cas = FileSystemCas::open(&cas_root).await?;
    let banner_hash = cas
        .put(
            "demo banner payload resolved through ${external_data.demo_banner}".as_bytes().to_vec(),
        )
        .await?;
    let concat_tool_binary_hash = cas.put(fs::read(&concat_binary_path)?).await?;
    let concat_tool_resource_hash = cas.put(fs::read(&concat_resource_path)?).await?;

    let build_inputs = DemoWorkflowBuildInputs {
        banner_hash,
        absolute_machine_path: display_path(&machine_path),
        absolute_fs_source_path: display_path(&absolute_fs_source_path),
        absolute_fs_copy_dest_path: display_path(&absolute_fs_copy_dest_path),
        absolute_export_file_path: display_path(&absolute_export_file_path),
        absolute_export_folder_path: display_path(&absolute_export_folder_path),
        concat_tool_binary_hash,
        concat_tool_resource_hash,
    };

    let user_document = build_user_document(&build_inputs);
    let machine_document = build_machine_document(&build_inputs);
    let workflow_names = user_document.workflows.keys().cloned().collect::<Vec<_>>();

    write_user_document(&user_path, &user_document)?;
    write_machine_document(&machine_path, &machine_document)?;

    let conductor = SimpleConductor::new(cas);

    let first_run: RunSummarySnapshot =
        conductor.run_workflow(&user_path, &machine_path).await?.into();
    let diagnostics_after_first_run: DiagnosticsSnapshot =
        conductor.get_runtime_diagnostics().await?.into();

    let second_run: RunSummarySnapshot =
        conductor.run_workflow(&user_path, &machine_path).await?.into();
    let diagnostics_after_second_run: DiagnosticsSnapshot =
        conductor.get_runtime_diagnostics().await?.into();
    let state_after_second_run = conductor.get_state().await?;
    let orchestration_state_pretty_json = persisted_state_json_pretty(&state_after_second_run)?;
    let tool_names_after_second_run = collect_tool_names(&state_after_second_run);
    let state_instance_count_after_second_run = state_after_second_run.instances.len();

    if second_run.cached_instances == 0 {
        return Err("expected second run to include cache hits".into());
    }

    let relative_export_exists = relative_export_path.exists();
    if !relative_export_exists {
        return Err("expected relative export artifact to exist after demo run".into());
    }

    let absolute_export_exists = absolute_export_file_path.exists();
    if !absolute_export_exists {
        return Err("expected absolute export file to exist after demo run".into());
    }

    let absolute_folder_export_contains_file =
        absolute_export_folder_path.join("banner.txt").exists();
    if !absolute_folder_export_contains_file {
        return Err("expected absolute folder export to contain unpacked 'banner.txt' entry".into());
    }

    if !absolute_fs_copy_dest_path.exists() {
        return Err("expected fs absolute copy destination to exist after demo run".into());
    }

    for required in [
        "echo@1.0.0",
        "fs@1.0.0",
        "archive@1.0.0",
        "import@1.0.0",
        "export@1.0.0",
        "concat-tool@1.0.0",
    ] {
        if !tool_names_after_second_run.iter().any(|name| name == required) {
            return Err(format!("expected tool '{required}' in orchestration state").into());
        }
    }

    let manifest = DemoManifest {
        generated_unix_epoch_seconds: unix_timestamp_seconds(),
        artifact_root: display_path(&root),
        cas_root: display_path(&cas_root),
        user_ncl_path: display_path(&user_path),
        machine_ncl_path: display_path(&machine_path),
        workflow_names,
        absolute_export_file_path: build_inputs.absolute_export_file_path,
        absolute_export_folder_path: build_inputs.absolute_export_folder_path,
        relative_export_exists,
        absolute_export_exists,
        absolute_folder_export_contains_file,
        state_instance_count_after_second_run,
        tool_names_after_second_run,
        first_run,
        second_run,
        diagnostics_after_first_run,
        diagnostics_after_second_run,
    };

    let manifest_path = root.join("manifest.json");
    write_json_file(&manifest_path, &manifest)?;

    let orchestration_state_path = root.join("orchestration-state.pretty.json");
    write_text_file(&orchestration_state_path, &orchestration_state_pretty_json)?;

    Ok(DemoRunPaths { artifact_root: root, cas_root, manifest_path, orchestration_state_path })
}

#[tokio::main]
/// Runs the persistent demo and prints generated artifact paths.
async fn main() -> ExampleResult<()> {
    let run_paths = generate_demo_artifacts().await?;
    println!("generated artifacts root: {}", run_paths.artifact_root.display());
    println!("generated cas root: {}", run_paths.cas_root.display());
    println!("manifest: {}", run_paths.manifest_path.display());
    println!("orchestration state: {}", run_paths.orchestration_state_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    /// Ensures artifact root remains under `examples/.artifacts/demo`.
    #[test]
    fn artifact_root_is_stable() {
        let text = super::display_path(&super::artifact_root());
        assert!(text.ends_with("src/conductor/examples/.artifacts/demo"));
    }

    /// Ensures the user document keeps all expected workflow names.
    #[test]
    fn user_document_workflow_names_are_stable() {
        let root = super::artifact_root();
        let inputs = super::DemoWorkflowBuildInputs {
            banner_hash: super::Hash::from_content(b"demo-banner"),
            absolute_machine_path: super::display_path(&root.join("conductor.machine.ncl")),
            absolute_fs_source_path: super::display_path(&root.join("source.txt")),
            absolute_fs_copy_dest_path: super::display_path(&root.join("copy.txt")),
            absolute_export_file_path: super::display_path(&root.join("export.txt")),
            absolute_export_folder_path: super::display_path(&root.join("export-folder")),
            concat_tool_binary_hash: super::Hash::from_content(b"concat-bin"),
            concat_tool_resource_hash: super::Hash::from_content(b"concat-resource"),
        };

        let document = super::build_user_document(&inputs);
        let mut names = document.workflows.keys().cloned().collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec!["01_feature_showcase".to_string(), "02_cache_and_depends_on".to_string()]
        );
    }
}
