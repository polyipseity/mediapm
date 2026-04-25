//! Versioned persistence envelopes for conductor Nickel documents.
//!
//! Runtime source-of-truth configuration structs live in
//! `model/config/mod.rs`. Version modules own persisted wire/document shapes
//! and Nickel migration/validation wrappers.
//!
//! The module path is `config` to communicate intent (runtime configuration),
//! while schema contracts remain Nickel-based.
//!
//! ## DO NOT REMOVE: version file correspondence guard
//!
//! - Every supported schema version must provide exactly one `vX.ncl` file.
//! - `latest` bindings in this module must point to the highest supported `vX.ncl`.
//! - Migration/validation dispatch in this module must remain latest-first.
//! - Keep historical version structs out of this file; only `v_latest.rs` may
//!   define the Rust persisted-schema bridge.
//! - These rules are mandatory and must not be removed.

mod v_latest;

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use nickel_lang_core::error::{Error as NickelError, NullReporter};
use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::program::Program;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::ConductorError;
use crate::model::config::{
    ExternalContentRef, ImpureTimestamp, InputBinding, MachineNickelDocument,
    NickelDocumentMetadata, OutputCaptureSpec, OutputPolicy, ParsedInputBindingSegment,
    StateNickelDocument, ToolConfigSpec, ToolInputKind, ToolInputSpec, ToolKindSpec,
    ToolOutputSpec, ToolSpec, UserNickelDocument, WorkflowSpec, WorkflowStepSpec,
    parse_input_binding,
};
use crate::model::state::OutputSaveMode;

/// Latest-version Nickel contract bindings.
///
/// Keep explicit latest pointers centralized for safe schema bumps.
// BEGIN latest-version bindings
mod latest {
    use fp_library::brands::RcBrand;
    use fp_library::types::optics::IsoPrime;

    use super::v_latest;

    /// Latest persisted Nickel schema marker.
    pub(super) const VERSION: u32 = v_latest::NICKEL_VERSION_LATEST;
    /// File name of the latest embedded Nickel contract.
    pub(super) const NCL_FILE_NAME: &str = "v1.ncl";
    /// Source of the latest embedded Nickel contract.
    pub(super) const NCL_SOURCE: &str = include_str!("v1.ncl");

    /// Rust envelope type for the latest schema bridge.
    pub(super) type Envelope = v_latest::NickelEnvelopeLatest;
    /// Rust shared-state type for the latest schema bridge.
    pub(super) type State = v_latest::NickelStateLatest;

    /// Returns whether `marker` equals the latest supported schema marker.
    #[must_use]
    pub(super) const fn is_version(marker: u32) -> bool {
        v_latest::is_nickel_version_latest(marker)
    }

    /// Isomorphism between the latest persisted document envelope and shared state.
    pub(super) fn version_iso() -> IsoPrime<'static, RcBrand, Envelope, State> {
        v_latest::nickel_latest_iso()
    }
}
// END latest-version bindings

/// Active version markers for both user and machine Nickel documents.
pub(crate) const USER_NICKEL_VERSION: u32 = latest::VERSION;
/// Active version markers for both user and machine Nickel documents.
pub(crate) const MACHINE_NICKEL_VERSION: u32 = latest::VERSION;

/// Fixed embedded migration helper module.
const MOD_NCL_SOURCE: &str = include_str!("mod.ncl");

/// Resolves one requested schema marker to the embedded Nickel contract file and source.
fn resolve_version_contract(
    requested_version: u32,
    document_kind: &str,
) -> Result<(&'static str, &'static str), ConductorError> {
    if latest::is_version(requested_version) {
        Ok((latest::NCL_FILE_NAME, latest::NCL_SOURCE))
    } else {
        Err(ConductorError::Workflow(format!(
            "unsupported {document_kind} schema version {requested_version}; expected {}",
            latest::VERSION
        )))
    }
}

/// Temporary Nickel workspace used to evaluate conductor-generated wrappers.
#[derive(Debug)]
struct TempNickelWorkspace {
    /// Root directory that hosts temporary `.ncl` files.
    path: PathBuf,
}

impl TempNickelWorkspace {
    /// Creates a unique temporary Nickel workspace root.
    fn new() -> Result<Self, ConductorError> {
        let pid = std::process::id();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        let path = std::env::temp_dir().join(format!("mediapm-conductor-nickel-{pid}-{nanos}"));

        fs::create_dir_all(&path).map_err(|source| ConductorError::Io {
            operation: "creating temporary Nickel workspace".to_string(),
            path: path.clone(),
            source,
        })?;

        Ok(Self { path })
    }

    /// Returns the workspace root path.
    fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl Drop for TempNickelWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Writes one Nickel source file into the temporary workspace.
fn write_nickel_file(path: &Path, source: &str, operation: &str) -> Result<(), ConductorError> {
    fs::write(path, source).map_err(|source_err| ConductorError::Io {
        operation: operation.to_string(),
        path: path.to_path_buf(),
        source: source_err,
    })
}

/// Creates a conductor error from one Nickel interpreter I/O setup failure.
fn nickel_io_error(err: io::Error, operation: &str, path: &Path) -> ConductorError {
    ConductorError::Io { operation: operation.to_string(), path: path.to_path_buf(), source: err }
}

/// Renders a Nickel interpreter error with file context for user-facing diagnostics.
fn nickel_eval_error(
    program: &Program<CacheImpl>,
    err: NickelError,
    context: &str,
) -> ConductorError {
    ConductorError::Workflow(format!(
        "{context}: {}",
        nickel_lang_core::error::report::report_as_str(
            &mut program.files(),
            err,
            nickel_lang_core::error::report::ColorOpt::Never,
        )
    ))
}

/// Evaluates one temporary Nickel main file and deserializes the fully exported result.
fn evaluate_main_file_as<T>(main_file: &Path, context: &str) -> Result<T, ConductorError>
where
    T: DeserializeOwned,
{
    let mut program = Program::<CacheImpl>::new_from_file(
        main_file.as_os_str(),
        std::io::sink(),
        NullReporter {},
    )
    .map_err(|err| nickel_io_error(err, "constructing Nickel program", main_file))?;

    let value =
        program.eval_full_for_export().map_err(|err| nickel_eval_error(&program, err, context))?;

    T::deserialize(value).map_err(|err| {
        ConductorError::Serialization(format!(
            "{context}: failed deserializing exported Nickel value: {err}"
        ))
    })
}

/// Evaluates one temporary Nickel main file purely for validation side effects.
fn validate_main_file(main_file: &Path, context: &str) -> Result<(), ConductorError> {
    let _: Value = evaluate_main_file_as(main_file, context)?;
    Ok(())
}

/// Evaluates one raw Nickel document source and returns its exported value.
///
/// This helper is intentionally schema-agnostic and is used for metadata
/// inspection tasks such as top-level field/key validation.
fn evaluate_document_source_value(
    source: &str,
    document_kind: &str,
) -> Result<Value, ConductorError> {
    let workspace = TempNickelWorkspace::new()?;
    write_nickel_file(
        &workspace.path().join("document_input.ncl"),
        source,
        "writing temporary Nickel input document for metadata inspection",
    )?;

    let wrapper_source = "import \"document_input.ncl\"\n";
    let wrapper_path = workspace.path().join("inspect_document.ncl");
    write_nickel_file(
        &wrapper_path,
        wrapper_source,
        "writing temporary Nickel metadata inspection wrapper",
    )?;

    evaluate_main_file_as(&wrapper_path, &format!("evaluating {document_kind} source metadata"))
}

/// Parses and validates the explicit top-level `version` marker from one
/// conductor Nickel source document.
///
/// All conductor document kinds (`conductor.ncl`, `conductor.machine.ncl`, and
/// `.conductor/state.ncl`) must carry an explicit numeric `version` field.
fn read_document_version_marker(source: &str, document_kind: &str) -> Result<u32, ConductorError> {
    let value = evaluate_document_source_value(source, document_kind)?;
    let object = value.as_object().ok_or_else(|| {
        ConductorError::Workflow(format!(
            "{document_kind} must evaluate to one record with a top-level 'version' field"
        ))
    })?;

    let version_value = object.get("version").ok_or_else(|| {
        ConductorError::Workflow(format!(
            "{document_kind} must define a top-level numeric 'version' field"
        ))
    })?;

    let marker_u64 = if let Some(version) = version_value.as_u64() {
        version
    } else if let Some(version) = version_value.as_f64() {
        if !version.is_finite() || version.fract() != 0.0 || version < 0.0 {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} top-level 'version' must be a non-negative integer"
            )));
        }

        format!("{version:.0}").parse::<u64>().map_err(|_| {
            ConductorError::Workflow(format!(
                "{document_kind} top-level 'version' value {version} exceeds supported range"
            ))
        })?
    } else {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} top-level 'version' must be numeric"
        )));
    };

    let marker = u32::try_from(marker_u64).map_err(|_| {
        ConductorError::Workflow(format!(
            "{document_kind} top-level 'version' value {marker_u64} exceeds supported range"
        ))
    })?;

    let _ = resolve_version_contract(marker, document_kind)?;
    Ok(marker)
}

/// Validates that `.conductor/state.ncl` defines only volatile state keys.
///
/// Allowed top-level keys are exactly:
/// - `version`
/// - `impure_timestamps`
/// - `state_pointer`
const ALLOWED_STATE_DOCUMENT_KEYS: [&str; 3] = ["version", "impure_timestamps", "state_pointer"];

fn validate_state_document_source_shape(source: &str) -> Result<(), ConductorError> {
    let value = evaluate_document_source_value(source, ".conductor/state.ncl")?;
    let object = value.as_object().ok_or_else(|| {
        ConductorError::Workflow(
            "state document '.conductor/state.ncl' must evaluate to one record".to_string(),
        )
    })?;

    for key in object.keys() {
        if !ALLOWED_STATE_DOCUMENT_KEYS.contains(&key.as_str()) {
            return Err(ConductorError::Workflow(format!(
                "state document '.conductor/state.ncl' may only define version, impure_timestamps, and state_pointer (found '{key}')"
            )));
        }
    }

    if !object.contains_key("version") {
        return Err(ConductorError::Workflow(
            "state document '.conductor/state.ncl' must define a top-level numeric 'version' field"
                .to_string(),
        ));
    }

    Ok(())
}

/// Determines one in-memory migration target by reading all three document
/// version markers and selecting the latest marker among them.
fn latest_version_among_sources(
    user_source: &str,
    machine_source: &str,
    state_source: &str,
) -> Result<u32, ConductorError> {
    let user_version = read_document_version_marker(user_source, "conductor.ncl")?;
    let machine_version = read_document_version_marker(machine_source, "conductor.machine.ncl")?;
    let state_version = read_document_version_marker(state_source, ".conductor/state.ncl")?;

    let target_version = user_version.max(machine_version).max(state_version);
    let _ = resolve_version_contract(target_version, "Nickel configuration")?;
    Ok(target_version)
}

/// Returns whether `key` can be emitted as a bare Nickel identifier.
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

    chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '\''))
}

/// Renders one field name in Nickel record syntax.
fn render_field_name(name: &str) -> String {
    if is_bare_nickel_identifier(name) {
        name.to_string()
    } else {
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""))
    }
}

/// Renders a serde JSON value as deterministic Nickel source.
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
                let rendered_items = items
                    .iter()
                    .map(|item| format!("{next_pad}{},", render_nickel_value(item, indent + 2)))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("[\n{rendered_items}\n{pad}]")
            }
        }
        Value::Object(entries) => {
            if entries.is_empty() {
                "{}".to_string()
            } else {
                let mut ordered_entries = entries.iter().collect::<Vec<_>>();
                ordered_entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                let rendered_entries = ordered_entries
                    .into_iter()
                    .map(|(key, entry_value)| {
                        format!(
                            "{next_pad}{} = {},",
                            render_field_name(key),
                            render_nickel_value(entry_value, indent + 2)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{{\n{rendered_entries}\n{pad}}}")
            }
        }
    }
}

/// Renders one serializable Rust structure as Nickel source.
fn render_document_as_nickel<T>(
    document: &T,
    document_kind: &str,
) -> Result<Vec<u8>, ConductorError>
where
    T: Serialize,
{
    let value = serde_json::to_value(document).map_err(|err| {
        ConductorError::Serialization(format!(
            "serializing {document_kind} to intermediate value: {err}"
        ))
    })?;
    let rendered = format!("{}\n", render_nickel_value(&value, 0));
    Ok(rendered.into_bytes())
}

/// Evaluates one document source through the embedded Nickel migration wrapper
/// into one requested persisted schema version.
pub(crate) fn migrate_document_source_to_version<T>(
    source: &str,
    requested_version: u32,
    document_kind: &str,
) -> Result<T, ConductorError>
where
    T: DeserializeOwned,
{
    let (version_file_name, version_contract_source) =
        resolve_version_contract(requested_version, document_kind)?;
    let validator_name = format!("validate_document_v{requested_version}");

    let workspace = TempNickelWorkspace::new()?;
    write_nickel_file(
        &workspace.path().join("mod.ncl"),
        MOD_NCL_SOURCE,
        "writing temporary Nickel migration helper",
    )?;
    write_nickel_file(
        &workspace.path().join(version_file_name),
        version_contract_source,
        &format!("writing temporary Nickel {version_file_name} helper"),
    )?;
    write_nickel_file(
        &workspace.path().join("document_input.ncl"),
        source,
        "writing temporary Nickel input document",
    )?;

    let wrapper_source = format!(
        r#"
let migration = import "mod.ncl" in
let version = import "{version_file_name}" in
let document = import "document_input.ncl" in
version.{validator_name} (migration.migrate_to {requested_version} document)
"#
    );
    let wrapper_path = workspace.path().join("decode_document.ncl");
    write_nickel_file(&wrapper_path, &wrapper_source, "writing temporary Nickel decode wrapper")?;

    evaluate_main_file_as(
        &wrapper_path,
        &format!("evaluating {document_kind} via Nickel migration wrapper"),
    )
}

/// Evaluates one document source through the embedded Nickel migration wrapper
/// into the latest supported schema version.
fn evaluate_document_source<T>(source: &str, document_kind: &str) -> Result<T, ConductorError>
where
    T: DeserializeOwned,
{
    migrate_document_source_to_version(source, latest::VERSION, document_kind)
}

/// Converts persisted latest-schema impure timestamps into runtime shape.
fn impure_timestamps_from_latest(
    latest_map: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, v_latest::ImpureTimestampLatest>,
    >,
) -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, ImpureTimestamp>> {
    latest_map
        .into_iter()
        .map(|(workflow_id, steps)| {
            let mapped_steps = steps
                .into_iter()
                .map(|(step_id, timestamp)| {
                    (
                        step_id,
                        ImpureTimestamp {
                            epoch_seconds: timestamp.epoch_seconds,
                            subsec_nanos: timestamp.subsec_nanos,
                        },
                    )
                })
                .collect();
            (workflow_id, mapped_steps)
        })
        .collect()
}

/// Converts runtime impure timestamps into persisted latest-schema shape.
fn impure_timestamps_to_latest(
    runtime_map: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, ImpureTimestamp>,
    >,
) -> std::collections::BTreeMap<
    String,
    std::collections::BTreeMap<String, v_latest::ImpureTimestampLatest>,
> {
    runtime_map
        .into_iter()
        .map(|(workflow_id, steps)| {
            let mapped_steps = steps
                .into_iter()
                .map(|(step_id, timestamp)| {
                    (
                        step_id,
                        v_latest::ImpureTimestampLatest {
                            epoch_seconds: timestamp.epoch_seconds,
                            subsec_nanos: timestamp.subsec_nanos,
                        },
                    )
                })
                .collect();
            (workflow_id, mapped_steps)
        })
        .collect()
}

/// Returns whether one builtin identity should be treated as impure by runtime
/// planning and deterministic cache invalidation.
fn builtin_is_impure(name: &str, version: &str) -> bool {
    matches!(
        (name, version),
        (
            mediapm_conductor_builtin_import::TOOL_NAME,
            mediapm_conductor_builtin_import::TOOL_VERSION
        ) | (mediapm_conductor_builtin_fs::TOOL_NAME, mediapm_conductor_builtin_fs::TOOL_VERSION)
            | (
                mediapm_conductor_builtin_export::TOOL_NAME,
                mediapm_conductor_builtin_export::TOOL_VERSION
            )
    )
}

/// Builds one runtime builtin tool spec from builtin identity only.
///
/// Persisted builtin definitions are intentionally minimal (`kind`, `name`,
/// `version`), so runtime-only defaults are derived here.
fn runtime_builtin_tool_spec(name: String, version: String) -> ToolSpec {
    ToolSpec {
        is_impure: builtin_is_impure(&name, &version),
        kind: ToolKindSpec::Builtin { name, version },
        ..ToolSpec::default()
    }
}

/// Evaluates fixed Nickel migrations/contracts plus user and machine configuration together.
pub(crate) fn evaluate_total_configuration_sources(
    user_source: &str,
    machine_source: &str,
    state_source: &str,
) -> Result<(), ConductorError> {
    validate_state_document_source_shape(state_source)?;

    let target_version = latest_version_among_sources(user_source, machine_source, state_source)?;
    let (target_file_name, target_contract_source) =
        resolve_version_contract(target_version, "Nickel configuration")?;
    let validator_name = format!("validate_document_v{target_version}");

    let workspace = TempNickelWorkspace::new()?;
    write_nickel_file(
        &workspace.path().join("mod.ncl"),
        MOD_NCL_SOURCE,
        "writing temporary Nickel migration helper",
    )?;
    write_nickel_file(
        &workspace.path().join(target_file_name),
        target_contract_source,
        &format!("writing temporary Nickel {target_file_name} helper"),
    )?;
    write_nickel_file(
        &workspace.path().join("user_input.ncl"),
        user_source,
        "writing temporary user Nickel input",
    )?;
    write_nickel_file(
        &workspace.path().join("machine_input.ncl"),
        machine_source,
        "writing temporary machine Nickel input",
    )?;
    write_nickel_file(
        &workspace.path().join("state_input.ncl"),
        state_source,
        "writing temporary state Nickel input",
    )?;

    let validate_source = format!(
        r#"
let migration = import "mod.ncl" in
let version = import "{target_file_name}" in
let user = version.{validator_name} (migration.migrate_to {target_version} (import "user_input.ncl")) in
let machine = version.{validator_name} (migration.migrate_to {target_version} (import "machine_input.ncl")) in
let state = version.{validator_name} (migration.migrate_to {target_version} (import "state_input.ncl")) in
{{
    validated_user = user,
    validated_machine = machine,
    validated_state = state,
    total = {{ include [user, machine, state] }},
}}
"#,
    );
    let validate_path = workspace.path().join("validate_total.ncl");
    write_nickel_file(
        &validate_path,
        &validate_source,
        "writing temporary total Nickel validation wrapper",
    )?;

    validate_main_file(&validate_path, "evaluating full Nickel configuration")
}

/// Optic bridge from latest persisted Nickel state to runtime user document.
#[allow(clippy::too_many_lines)]
fn user_runtime_iso() -> IsoPrime<'static, RcBrand, latest::State, UserNickelDocument> {
    IsoPrime::new(
        |state: latest::State| UserNickelDocument {
            metadata: NickelDocumentMetadata::default(),
            runtime: crate::model::config::RuntimeStorageConfig {
                conductor_dir: state.runtime.conductor_dir,
                state_config: state.runtime.state_config,
                cas_store_dir: state.runtime.cas_store_dir,
                inherited_env_vars: state.runtime.inherited_env_vars,
            },
            external_data: state
                .external_data
                .into_iter()
                .map(|(hash, reference)| {
                    (hash, ExternalContentRef { description: reference.description })
                })
                .collect(),
            tools: state
                .tools
                .into_iter()
                .map(|(tool_name, tool)| match tool {
                    v_latest::ToolSpecLatest::Executable {
                        is_impure,
                        inputs,
                        command,
                        env_vars,
                        success_codes,
                        outputs,
                    } => (
                        tool_name,
                        ToolSpec {
                            is_impure,
                            inputs: inputs
                                .into_iter()
                                .map(|(input_name, input_spec)| {
                                    (
                                        input_name,
                                        ToolInputSpec {
                                            kind: match input_spec.kind {
                                                v_latest::ToolInputKindLatest::String => {
                                                    ToolInputKind::String
                                                }
                                                v_latest::ToolInputKindLatest::StringList => {
                                                    ToolInputKind::StringList
                                                }
                                            },
                                        },
                                    )
                                })
                                .collect(),
                            kind: ToolKindSpec::Executable { command, env_vars, success_codes },
                            outputs: outputs
                                .into_iter()
                                .map(|(output_name, output_spec)| {
                                    (
                                        output_name,
                                        ToolOutputSpec {
                                            capture: match output_spec.capture {
                                                v_latest::OutputCaptureLatest::Stdout {} => {
                                                    OutputCaptureSpec::Stdout {}
                                                }
                                                v_latest::OutputCaptureLatest::Stderr {} => {
                                                    OutputCaptureSpec::Stderr {}
                                                }
                                                v_latest::OutputCaptureLatest::ProcessCode {} => {
                                                    OutputCaptureSpec::ProcessCode {}
                                                }
                                                v_latest::OutputCaptureLatest::File { path } => {
                                                    OutputCaptureSpec::File { path }
                                                }
                                                v_latest::OutputCaptureLatest::FileRegex {
                                                    path_regex,
                                                } => OutputCaptureSpec::FileRegex { path_regex },
                                                v_latest::OutputCaptureLatest::Folder {
                                                    path,
                                                    include_topmost_folder,
                                                } => OutputCaptureSpec::Folder {
                                                    path,
                                                    include_topmost_folder,
                                                },
                                                v_latest::OutputCaptureLatest::FolderRegex {
                                                    path_regex,
                                                } => OutputCaptureSpec::FolderRegex { path_regex },
                                            },
                                        },
                                    )
                                })
                                .collect(),
                        },
                    ),
                    v_latest::ToolSpecLatest::Builtin { name, version } => {
                        (tool_name, runtime_builtin_tool_spec(name, version))
                    }
                })
                .collect(),
            workflows: state
                .workflows
                .into_iter()
                .map(|(name, workflow)| {
                    (
                        name,
                        WorkflowSpec {
                            name: workflow.name,
                            description: workflow.description,
                            steps: workflow
                                .steps
                                .into_iter()
                                .map(|step| WorkflowStepSpec {
                                    id: step.id,
                                    tool: step.tool,
                                    inputs: step
                                        .inputs
                                        .into_iter()
                                        .map(|(input_name, binding)| {
                                            (
                                                input_name,
                                                match binding {
                                                    v_latest::InputBindingLatest::String(value) => {
                                                        InputBinding::String(value)
                                                    }
                                                    v_latest::InputBindingLatest::StringList(
                                                        values,
                                                    ) => InputBinding::StringList(values),
                                                },
                                            )
                                        })
                                        .collect(),
                                    depends_on: step.depends_on,
                                    outputs: step
                                        .outputs
                                        .into_iter()
                                        .map(|(output_name, policy)| {
                                            (
                                                output_name,
                                                OutputPolicy {
                                                    save: policy.save.map(|save| match save {
                                                        v_latest::OutputSaveLatest::Bool(false) => {
                                                            OutputSaveMode::Unsaved
                                                        }
                                                        v_latest::OutputSaveLatest::Bool(true) => {
                                                            OutputSaveMode::Saved
                                                        }
                                                        v_latest::OutputSaveLatest::Full => {
                                                            OutputSaveMode::Full
                                                        }
                                                    }),
                                                },
                                            )
                                        })
                                        .collect(),
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
            tool_configs: state
                .tool_configs
                .into_iter()
                .map(|(tool_name, config)| {
                    (
                        tool_name,
                        ToolConfigSpec {
                            max_concurrent_calls: config.max_concurrent_calls,
                            max_retries: config.max_retries,
                            description: config.description,
                            input_defaults: config
                                .input_defaults
                                .into_iter()
                                .map(|(input_name, binding)| {
                                    (
                                        input_name,
                                        match binding {
                                            v_latest::InputBindingLatest::String(value) => {
                                                InputBinding::String(value)
                                            }
                                            v_latest::InputBindingLatest::StringList(values) => {
                                                InputBinding::StringList(values)
                                            }
                                        },
                                    )
                                })
                                .collect(),
                            env_vars: config.env_vars,
                            content_map: config.content_map,
                        },
                    )
                })
                .collect(),
            impure_timestamps: impure_timestamps_from_latest(state.impure_timestamps),
            state_pointer: state.state_pointer,
        },
        |runtime: UserNickelDocument| latest::State {
            runtime: v_latest::RuntimeStorageLatest {
                conductor_dir: runtime.runtime.conductor_dir,
                state_config: runtime.runtime.state_config,
                cas_store_dir: runtime.runtime.cas_store_dir,
                inherited_env_vars: runtime.runtime.inherited_env_vars,
            },
            external_data: runtime
                .external_data
                .into_iter()
                .map(|(hash, reference)| {
                    (
                        hash,
                        v_latest::ExternalContentRefLatest { description: reference.description },
                    )
                })
                .collect(),
            tools: runtime
                .tools
                .into_iter()
                .map(|(tool_name, tool)| match tool.kind {
                    ToolKindSpec::Executable { command, env_vars, success_codes } => (
                        tool_name,
                        v_latest::ToolSpecLatest::Executable {
                            is_impure: tool.is_impure,
                            inputs: tool
                                .inputs
                                .into_iter()
                                .map(|(input_name, input_spec)| {
                                    (
                                        input_name,
                                        v_latest::ToolInputSpecLatest {
                                            kind: match input_spec.kind {
                                                ToolInputKind::String => {
                                                    v_latest::ToolInputKindLatest::String
                                                }
                                                ToolInputKind::StringList => {
                                                    v_latest::ToolInputKindLatest::StringList
                                                }
                                            },
                                        },
                                    )
                                })
                                .collect(),
                            command,
                            env_vars,
                            success_codes,
                            outputs: tool
                                .outputs
                                .into_iter()
                                .map(|(output_name, output_spec)| {
                                    (
                                        output_name,
                                        v_latest::ToolOutputSpecLatest {
                                            capture: match output_spec.capture {
                                                OutputCaptureSpec::Stdout {} => {
                                                    v_latest::OutputCaptureLatest::Stdout {}
                                                }
                                                OutputCaptureSpec::Stderr {} => {
                                                    v_latest::OutputCaptureLatest::Stderr {}
                                                }
                                                OutputCaptureSpec::ProcessCode {} => {
                                                    v_latest::OutputCaptureLatest::ProcessCode {}
                                                }
                                                OutputCaptureSpec::File { path } => {
                                                    v_latest::OutputCaptureLatest::File { path }
                                                }
                                                OutputCaptureSpec::FileRegex { path_regex } => {
                                                    v_latest::OutputCaptureLatest::FileRegex {
                                                        path_regex,
                                                    }
                                                }
                                                OutputCaptureSpec::Folder {
                                                    path,
                                                    include_topmost_folder,
                                                } => v_latest::OutputCaptureLatest::Folder {
                                                    path,
                                                    include_topmost_folder,
                                                },
                                                OutputCaptureSpec::FolderRegex { path_regex } => {
                                                    v_latest::OutputCaptureLatest::FolderRegex {
                                                        path_regex,
                                                    }
                                                }
                                            },
                                        },
                                    )
                                })
                                .collect(),
                        },
                    ),
                    ToolKindSpec::Builtin { name, version } => {
                        (tool_name, v_latest::ToolSpecLatest::Builtin { name, version })
                    }
                })
                .collect(),
            workflows: runtime
                .workflows
                .into_iter()
                .map(|(name, workflow)| {
                    (
                        name,
                        v_latest::WorkflowSpecLatest {
                            name: workflow.name,
                            description: workflow.description,
                            steps: workflow
                                .steps
                                .into_iter()
                                .map(|step| v_latest::WorkflowStepSpecLatest {
                                    id: step.id,
                                    tool: step.tool,
                                    inputs: step
                                        .inputs
                                        .into_iter()
                                        .map(|(input_name, binding)| {
                                            (
                                                input_name,
                                                match binding {
                                                    InputBinding::String(value) => {
                                                        v_latest::InputBindingLatest::String(value)
                                                    }
                                                    InputBinding::StringList(values) => {
                                                        v_latest::InputBindingLatest::StringList(
                                                            values,
                                                        )
                                                    }
                                                },
                                            )
                                        })
                                        .collect(),
                                    depends_on: step.depends_on,
                                    outputs: step
                                        .outputs
                                        .into_iter()
                                        .map(|(output_name, policy)| {
                                            (
                                                output_name,
                                                v_latest::OutputPolicyLatest {
                                                    save: policy.save.map(|save| match save {
                                                        OutputSaveMode::Unsaved => {
                                                            v_latest::OutputSaveLatest::Bool(false)
                                                        }
                                                        OutputSaveMode::Saved => {
                                                            v_latest::OutputSaveLatest::Bool(true)
                                                        }
                                                        OutputSaveMode::Full => {
                                                            v_latest::OutputSaveLatest::Full
                                                        }
                                                    }),
                                                },
                                            )
                                        })
                                        .collect(),
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
            tool_configs: runtime
                .tool_configs
                .into_iter()
                .map(|(tool_name, config)| {
                    (
                        tool_name,
                        v_latest::ToolConfigSpecLatest {
                            max_concurrent_calls: config.max_concurrent_calls,
                            max_retries: config.max_retries,
                            description: config.description,
                            input_defaults: config
                                .input_defaults
                                .into_iter()
                                .map(|(input_name, binding)| {
                                    (
                                        input_name,
                                        match binding {
                                            InputBinding::String(value) => {
                                                v_latest::InputBindingLatest::String(value)
                                            }
                                            InputBinding::StringList(values) => {
                                                v_latest::InputBindingLatest::StringList(values)
                                            }
                                        },
                                    )
                                })
                                .collect(),
                            env_vars: config.env_vars,
                            content_map: config.content_map,
                        },
                    )
                })
                .collect(),
            impure_timestamps: impure_timestamps_to_latest(runtime.impure_timestamps),
            state_pointer: runtime.state_pointer,
        },
    )
}

/// Optic bridge from latest persisted Nickel state to runtime machine document.
fn machine_runtime_iso() -> IsoPrime<'static, RcBrand, latest::State, MachineNickelDocument> {
    IsoPrime::new(
        |state: latest::State| {
            let runtime = user_runtime_iso().from(state);
            MachineNickelDocument {
                metadata: NickelDocumentMetadata::default(),
                runtime: runtime.runtime,
                external_data: runtime.external_data,
                tools: runtime.tools,
                workflows: runtime.workflows,
                tool_configs: runtime.tool_configs,
                impure_timestamps: runtime.impure_timestamps,
                state_pointer: runtime.state_pointer,
            }
        },
        |runtime: MachineNickelDocument| {
            user_runtime_iso().to(UserNickelDocument {
                metadata: NickelDocumentMetadata::default(),
                runtime: runtime.runtime,
                external_data: runtime.external_data,
                tools: runtime.tools,
                workflows: runtime.workflows,
                tool_configs: runtime.tool_configs,
                impure_timestamps: runtime.impure_timestamps,
                state_pointer: runtime.state_pointer,
            })
        },
    )
}

/// Optic bridge from latest persisted Nickel state to runtime volatile state
/// document.
fn state_runtime_iso() -> IsoPrime<'static, RcBrand, latest::State, StateNickelDocument> {
    IsoPrime::new(
        |state: latest::State| StateNickelDocument {
            impure_timestamps: impure_timestamps_from_latest(state.impure_timestamps),
            state_pointer: state.state_pointer,
        },
        |runtime: StateNickelDocument| latest::State {
            runtime: v_latest::RuntimeStorageLatest::default(),
            external_data: std::collections::BTreeMap::new(),
            tools: std::collections::BTreeMap::new(),
            workflows: std::collections::BTreeMap::new(),
            tool_configs: std::collections::BTreeMap::new(),
            impure_timestamps: impure_timestamps_to_latest(runtime.impure_timestamps),
            state_pointer: runtime.state_pointer,
        },
    )
}

/// Encodes `conductor.ncl` with the latest envelope.
pub(crate) fn encode_user_document(
    document: UserNickelDocument,
) -> Result<Vec<u8>, ConductorError> {
    let latest_state = user_runtime_iso().to(document);
    let envelope = latest::version_iso().to(latest_state);

    vet_latest_envelope(&envelope, "conductor.ncl")?;
    render_document_as_nickel(&envelope, "conductor.ncl")
}

/// Decodes `conductor.ncl` through the embedded migration wrapper.
pub(crate) fn decode_user_document(bytes: &[u8]) -> Result<UserNickelDocument, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!("conductor.ncl is not valid UTF-8: {err}"))
    })?;
    let envelope: latest::Envelope = evaluate_document_source(source, "conductor.ncl")?;
    let marker = envelope.version;
    if marker != USER_NICKEL_VERSION {
        return Err(ConductorError::Workflow(format!(
            "unsupported conductor.ncl schema version {marker}; expected {USER_NICKEL_VERSION}"
        )));
    }

    vet_latest_envelope(&envelope, "conductor.ncl")?;

    let mut runtime = user_runtime_iso().from(latest::version_iso().from(envelope));
    runtime.metadata = NickelDocumentMetadata::default();
    Ok(runtime)
}

/// Encodes `conductor.machine.ncl` with the latest envelope.
pub(crate) fn encode_machine_document(
    document: MachineNickelDocument,
) -> Result<Vec<u8>, ConductorError> {
    let latest_state = machine_runtime_iso().to(document);
    let envelope = latest::version_iso().to(latest_state);

    vet_latest_envelope(&envelope, "conductor.machine.ncl")?;
    render_document_as_nickel(&envelope, "conductor.machine.ncl")
}

/// Decodes `conductor.machine.ncl` through the embedded migration wrapper.
pub(crate) fn decode_machine_document(
    bytes: &[u8],
) -> Result<MachineNickelDocument, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!("conductor.machine.ncl is not valid UTF-8: {err}"))
    })?;
    let envelope: latest::Envelope = evaluate_document_source(source, "conductor.machine.ncl")?;
    let marker = envelope.version;
    if marker != MACHINE_NICKEL_VERSION {
        return Err(ConductorError::Workflow(format!(
            "unsupported conductor.machine.ncl schema version {marker}; expected {MACHINE_NICKEL_VERSION}"
        )));
    }

    vet_latest_envelope(&envelope, "conductor.machine.ncl")?;

    let mut runtime = machine_runtime_iso().from(latest::version_iso().from(envelope));
    runtime.metadata = NickelDocumentMetadata::default();
    Ok(runtime)
}

/// Encodes `.conductor/state.ncl` with the latest envelope.
pub(crate) fn encode_state_document(
    document: StateNickelDocument,
) -> Result<Vec<u8>, ConductorError> {
    /// Minimal persisted envelope emitted for `.conductor/state.ncl`.
    #[derive(Debug, Serialize)]
    struct StateEnvelope {
        /// Explicit schema marker shared with user/machine documents.
        version: u32,
        /// Impure timestamps map (`workflow_id -> step_id -> timestamp`).
        impure_timestamps: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, v_latest::ImpureTimestampLatest>,
        >,
        /// Optional orchestration-state pointer.
        state_pointer: Option<mediapm_cas::Hash>,
    }

    let latest_state = state_runtime_iso().to(document);
    let envelope = latest::version_iso().to(latest_state);

    vet_latest_envelope(&envelope, ".conductor/state.ncl")?;
    render_document_as_nickel(
        &StateEnvelope {
            version: envelope.version,
            impure_timestamps: envelope.impure_timestamps,
            state_pointer: envelope.state_pointer,
        },
        ".conductor/state.ncl",
    )
}

/// Decodes `.conductor/state.ncl` through the embedded migration wrapper.
pub(crate) fn decode_state_document(bytes: &[u8]) -> Result<StateNickelDocument, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!(".conductor/state.ncl is not valid UTF-8: {err}"))
    })?;

    validate_state_document_source_shape(source)?;
    let _ = read_document_version_marker(source, ".conductor/state.ncl")?;

    let envelope: latest::Envelope = evaluate_document_source(source, ".conductor/state.ncl")?;
    let marker = envelope.version;
    if marker != MACHINE_NICKEL_VERSION {
        return Err(ConductorError::Workflow(format!(
            "unsupported .conductor/state.ncl schema version {marker}; expected {MACHINE_NICKEL_VERSION}"
        )));
    }

    vet_latest_envelope(&envelope, ".conductor/state.ncl")?;

    Ok(state_runtime_iso().from(latest::version_iso().from(envelope)))
}

/// Performs structural invariant checks on one latest persisted Nickel envelope.
#[allow(clippy::too_many_lines)]
fn vet_latest_envelope(
    envelope: &latest::Envelope,
    document_kind: &str,
) -> Result<(), ConductorError> {
    if envelope.version != latest::VERSION {
        return Err(ConductorError::Workflow(format!(
            "expected {document_kind} version {} but found {}",
            latest::VERSION,
            envelope.version
        )));
    }

    if let Some(conductor_dir) = &envelope.runtime.conductor_dir
        && conductor_dir.trim().is_empty()
    {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} conductor_dir must be non-empty"
        )));
    }
    if let Some(state_config) = &envelope.runtime.state_config
        && state_config.trim().is_empty()
    {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} state_config must be non-empty when provided"
        )));
    }
    if let Some(cas_store_dir) = &envelope.runtime.cas_store_dir
        && cas_store_dir.trim().is_empty()
    {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} cas_store_dir must be non-empty when provided"
        )));
    }

    for (tool_name, tool) in &envelope.tools {
        if !tool_name.contains('@') {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool '{tool_name}' must include immutable version in its name (for example: compose@1.0.0)"
            )));
        }

        match tool {
            v_latest::ToolSpecLatest::Executable { command, success_codes, outputs, .. } => {
                let Some(executable) = command.first() else {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' executable command must contain at least one entry"
                    )));
                };
                if executable.trim().is_empty() {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' executable command[0] must be non-empty"
                    )));
                }
                if success_codes.is_empty() {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' executable process.success_codes must contain at least one exit code"
                    )));
                }
                if outputs.is_empty() {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' must declare at least one output capture"
                    )));
                }
            }
            v_latest::ToolSpecLatest::Builtin { name, version } => {
                if name.trim().is_empty() || version.trim().is_empty() {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' builtin process must provide non-empty name and version"
                    )));
                }
            }
        }
    }

    let external_hashes = envelope.external_data.keys().copied().collect::<BTreeSet<_>>();

    for (tool_name, tool_config) in &envelope.tool_configs {
        if tool_config.max_concurrent_calls == 0 || tool_config.max_concurrent_calls < -1 {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool_configs '{tool_name}' max_concurrent_calls must be -1 or a positive integer"
            )));
        }
        if tool_config.max_retries < -1 {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool_configs '{tool_name}' max_retries must be -1 or a non-negative integer"
            )));
        }
        if let Some(description) = &tool_config.description
            && description.trim().is_empty()
        {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool_configs '{tool_name}' description must be non-empty when provided"
            )));
        }

        if let Some(tool) = envelope.tools.get(tool_name)
            && tool_config.content_map.is_some()
            && matches!(tool, v_latest::ToolSpecLatest::Builtin { .. })
        {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool_configs '{tool_name}' content_map is invalid for builtin tools"
            )));
        }

        if let Some(tool) = envelope.tools.get(tool_name) {
            match tool {
                v_latest::ToolSpecLatest::Builtin { .. } => {
                    if !tool_config.input_defaults.is_empty() {
                        return Err(ConductorError::Workflow(format!(
                            "{document_kind} tool_configs '{tool_name}' input_defaults is invalid for builtin tools"
                        )));
                    }
                }
                v_latest::ToolSpecLatest::Executable { inputs, .. } => {
                    if let Some(content_map) = &tool_config.content_map {
                        for (relative_path, hash) in content_map {
                            if !external_hashes.contains(hash) {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} tool_configs '{tool_name}' content_map '{relative_path}' references hash '{hash}' that is missing from external_data"
                                )));
                            }
                        }
                    }

                    for (input_name, binding) in &tool_config.input_defaults {
                        let Some(input_spec) = inputs.get(input_name) else {
                            return Err(ConductorError::Workflow(format!(
                                "{document_kind} tool_configs '{tool_name}' input_defaults references undeclared tool input '{input_name}'"
                            )));
                        };
                        match (&input_spec.kind, binding) {
                            (
                                v_latest::ToolInputKindLatest::String,
                                v_latest::InputBindingLatest::String(_),
                            )
                            | (
                                v_latest::ToolInputKindLatest::StringList,
                                v_latest::InputBindingLatest::StringList(_),
                            ) => {}
                            (
                                v_latest::ToolInputKindLatest::String,
                                v_latest::InputBindingLatest::StringList(_),
                            ) => {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} tool_configs '{tool_name}' input_defaults['{input_name}'] expects kind 'string' but received 'string_list'"
                                )));
                            }
                            (
                                v_latest::ToolInputKindLatest::StringList,
                                v_latest::InputBindingLatest::String(_),
                            ) => {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} tool_configs '{tool_name}' input_defaults['{input_name}'] expects kind 'string_list' but received 'string'"
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    for (workflow_id, steps) in &envelope.impure_timestamps {
        for (step_id, timestamp) in steps {
            if timestamp.subsec_nanos >= 1_000_000_000 {
                return Err(ConductorError::Workflow(format!(
                    "{document_kind} impure_timestamps.{workflow_id}.{step_id}.subsec_nanos must be in range 0..999999999"
                )));
            }
        }
    }

    for (workflow_name, workflow) in &envelope.workflows {
        let step_tool_by_id = workflow
            .steps
            .iter()
            .map(|step| (step.id.as_str(), step.tool.as_str()))
            .collect::<std::collections::BTreeMap<_, _>>();

        for step in &workflow.steps {
            let mut explicit_dependencies = BTreeSet::new();
            for dependency_step_id in &step.depends_on {
                if !explicit_dependencies.insert(dependency_step_id.clone()) {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} workflow '{workflow_name}' step '{}' contains duplicate depends_on entry '{dependency_step_id}'",
                        step.id
                    )));
                }
                if dependency_step_id == &step.id {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} workflow '{workflow_name}' step '{}' must not depend on itself",
                        step.id
                    )));
                }
                if !step_tool_by_id.contains_key(dependency_step_id.as_str()) {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} workflow '{workflow_name}' step '{}' depends_on unknown step '{dependency_step_id}'",
                        step.id
                    )));
                }
            }

            if let Some(v_latest::ToolSpecLatest::Executable { inputs, .. }) =
                envelope.tools.get(&step.tool)
            {
                for input_name in step.inputs.keys() {
                    if !inputs.contains_key(input_name) {
                        return Err(ConductorError::Workflow(format!(
                            "{document_kind} workflow '{workflow_name}' step '{}' references undeclared input '{input_name}' for tool '{}'",
                            step.id, step.tool,
                        )));
                    }
                }

                for (input_name, input_spec) in inputs {
                    if let Some(binding) = step.inputs.get(input_name) {
                        match (&input_spec.kind, binding) {
                            (
                                v_latest::ToolInputKindLatest::String,
                                v_latest::InputBindingLatest::String(_),
                            )
                            | (
                                v_latest::ToolInputKindLatest::StringList,
                                v_latest::InputBindingLatest::StringList(_),
                            ) => {}
                            (
                                v_latest::ToolInputKindLatest::String,
                                v_latest::InputBindingLatest::StringList(_),
                            ) => {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' expects kind 'string' for tool '{}', but received 'string_list'",
                                    step.id, step.tool,
                                )));
                            }
                            (
                                v_latest::ToolInputKindLatest::StringList,
                                v_latest::InputBindingLatest::String(_),
                            ) => {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' expects kind 'string_list' for tool '{}', but received 'string'",
                                    step.id, step.tool,
                                )));
                            }
                        }
                    } else {
                        let tool_config_default = envelope
                            .tool_configs
                            .get(&step.tool)
                            .and_then(|tool_config| tool_config.input_defaults.get(input_name));

                        if tool_config_default.is_none() {
                            return Err(ConductorError::Workflow(format!(
                                "{document_kind} workflow '{workflow_name}' step '{}' is missing required input '{input_name}' for tool '{}'",
                                step.id, step.tool,
                            )));
                        }

                        if let Some(default_binding) = tool_config_default {
                            match (&input_spec.kind, default_binding) {
                                (
                                    v_latest::ToolInputKindLatest::String,
                                    v_latest::InputBindingLatest::String(_),
                                )
                                | (
                                    v_latest::ToolInputKindLatest::StringList,
                                    v_latest::InputBindingLatest::StringList(_),
                                ) => {}
                                (
                                    v_latest::ToolInputKindLatest::String,
                                    v_latest::InputBindingLatest::StringList(_),
                                ) => {
                                    return Err(ConductorError::Workflow(format!(
                                        "{document_kind} workflow '{workflow_name}' step '{}' uses tool_config input default '{input_name}' with kind 'string_list', but tool '{}' expects kind 'string'",
                                        step.id, step.tool,
                                    )));
                                }
                                (
                                    v_latest::ToolInputKindLatest::StringList,
                                    v_latest::InputBindingLatest::String(_),
                                ) => {
                                    return Err(ConductorError::Workflow(format!(
                                        "{document_kind} workflow '{workflow_name}' step '{}' uses tool_config input default '{input_name}' with kind 'string', but tool '{}' expects kind 'string_list'",
                                        step.id, step.tool,
                                    )));
                                }
                            }
                        }
                    }
                }
            }

            for (input_name, binding) in &step.inputs {
                let binding_items: Vec<(usize, &str)> = match binding {
                    v_latest::InputBindingLatest::String(value) => vec![(0, value.as_str())],
                    v_latest::InputBindingLatest::StringList(values) => {
                        values.iter().enumerate().map(|(idx, item)| (idx, item.as_str())).collect()
                    }
                };

                for (item_index, binding_item) in binding_items {
                    let parsed_segments = parse_input_binding(binding_item).map_err(|err| {
                        ConductorError::Workflow(format!(
                            "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' has invalid {}binding '{}': {err}",
                            step.id,
                            if matches!(binding, v_latest::InputBindingLatest::StringList(_)) {
                                format!("list item {item_index} ")
                            } else {
                                String::new()
                            },
                            binding_item,
                        ))
                    })?;

                    for segment in parsed_segments {
                        if let ParsedInputBindingSegment::StepOutput { step_id, output, .. } =
                            segment
                        {
                            if !explicit_dependencies.contains(step_id) {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' references '${{step_output.{step_id}.{output}}}' but step '{step_id}' is missing from depends_on",
                                    step.id
                                )));
                            }

                            let Some(producer_tool_name) = step_tool_by_id.get(step_id) else {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' references unknown dependency step '{step_id}'",
                                    step.id
                                )));
                            };

                            let Some(producer_tool) = envelope.tools.get(*producer_tool_name)
                            else {
                                continue;
                            };

                            let producer_outputs = match producer_tool {
                                v_latest::ToolSpecLatest::Executable { outputs, .. } => outputs,
                                v_latest::ToolSpecLatest::Builtin { .. } => continue,
                            };

                            if !producer_outputs.contains_key(output) {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' references missing output '{output}' on dependency step '{step_id}'",
                                    step.id
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    //! Tests for latest Nickel schema and Rust bridge compatibility.
    //!
    //! ## DO NOT REMOVE: latest schema compatibility guard
    //!
    //! These tests ensure the latest embedded Nickel contract (`vX.ncl`) stays
    //! wire-compatible with `v_latest.rs` Rust structs. When the schema evolves,
    //! update these tests alongside `v_latest.rs` and the latest `vX.ncl`.

    use super::render_document_as_nickel;
    use super::{ConductorError, MOD_NCL_SOURCE, TempNickelWorkspace};
    use super::{
        USER_NICKEL_VERSION, decode_machine_document, decode_user_document,
        encode_machine_document, encode_user_document, migrate_document_source_to_version,
    };
    use super::{evaluate_main_file_as, resolve_version_contract, write_nickel_file};
    use super::{latest, v_latest};
    use crate::model::config::{
        ImpureTimestamp, InputBinding, MachineNickelDocument, OutputPolicy, ToolInputKind,
        UserNickelDocument, WorkflowSpec, WorkflowStepSpec,
    };
    use crate::model::state::OutputSaveMode;
    use serde::Deserialize;

    /// One declared one-hop migration edge from Nickel migration metadata.
    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct AtomicMigrationPair {
        /// Source schema marker.
        from: u32,
        /// Destination schema marker.
        to: u32,
    }

    /// Exposed migration metadata from `mod.ncl` used by Rust invariants tests.
    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct MigrationMetadata {
        /// Latest schema marker exposed by Nickel migration module.
        current_version: u32,
        /// Supported schema markers exposed by Nickel migration module.
        supported_versions: Vec<u32>,
        /// Declared one-hop migration edges.
        atomic_migration_pairs: Vec<AtomicMigrationPair>,
    }

    /// Reads migration metadata from `mod.ncl` through Nickel evaluation.
    fn read_migration_metadata() -> Result<MigrationMetadata, ConductorError> {
        let workspace = TempNickelWorkspace::new()?;
        write_nickel_file(
            &workspace.path().join("mod.ncl"),
            MOD_NCL_SOURCE,
            "writing temporary Nickel migration helper for metadata test",
        )?;
        write_nickel_file(
            &workspace.path().join(latest::NCL_FILE_NAME),
            latest::NCL_SOURCE,
            "writing temporary latest Nickel contract for migration metadata test",
        )?;

        let wrapper_source = r#"
let migration = import "mod.ncl" in
{
  current_version = migration.current_version,
  supported_versions = migration.supported_versions,
  atomic_migration_pairs = migration.atomic_migration_pairs,
}
"#;
        let wrapper_path = workspace.path().join("migration_metadata.ncl");
        write_nickel_file(
            &wrapper_path,
            wrapper_source,
            "writing temporary Nickel migration metadata wrapper",
        )?;

        evaluate_main_file_as(&wrapper_path, "evaluating Nickel migration metadata")
    }

    /// Evaluates one document source through exactly one declared atomic
    /// migration hop (`from_version -> to_version`).
    fn migrate_document_source_atomic<T>(
        source: &str,
        from_version: u32,
        to_version: u32,
        document_kind: &str,
    ) -> Result<T, ConductorError>
    where
        T: serde::de::DeserializeOwned,
    {
        let (_, version_contract_source) = resolve_version_contract(to_version, document_kind)?;
        let validator_name = format!("validate_document_v{to_version}");
        let workspace = TempNickelWorkspace::new()?;

        write_nickel_file(
            &workspace.path().join("mod.ncl"),
            MOD_NCL_SOURCE,
            "writing temporary Nickel migration helper for atomic migration test",
        )?;
        write_nickel_file(
            &workspace.path().join(latest::NCL_FILE_NAME),
            version_contract_source,
            "writing temporary latest Nickel contract for atomic migration test",
        )?;
        write_nickel_file(
            &workspace.path().join("document_input.ncl"),
            source,
            "writing temporary Nickel input document for atomic migration test",
        )?;

        let wrapper_source = format!(
            r#"
let migration = import "mod.ncl" in
let version = import "{}" in
let document = import "document_input.ncl" in
version.{validator_name} (migration.migrate_atomic {} {} document)
"#,
            latest::NCL_FILE_NAME,
            from_version,
            to_version,
        );
        let wrapper_path = workspace.path().join("atomic_migrate_document.ncl");
        write_nickel_file(
            &wrapper_path,
            &wrapper_source,
            "writing temporary Nickel atomic migration wrapper",
        )?;

        evaluate_main_file_as(
            &wrapper_path,
            &format!(
                "evaluating atomic Nickel migration {from_version}->{to_version} for {document_kind}"
            ),
        )
    }

    /// Verifies that one Rust-authored latest envelope survives Nickel migration
    /// and validation unchanged.
    #[test]
    fn latest_schema_round_trips_latest_rust_bridge_envelope() {
        let envelope = v_latest::NickelEnvelopeLatest {
            version: latest::VERSION,
            ..v_latest::NickelEnvelopeLatest::default()
        };

        let source_bytes = render_document_as_nickel(&envelope, "compatibility-envelope")
            .expect("render latest envelope as Nickel source");
        let source = std::str::from_utf8(&source_bytes).expect("rendered envelope must be UTF-8");
        let decoded: v_latest::NickelEnvelopeLatest =
            migrate_document_source_to_version(source, latest::VERSION, "compatibility-envelope")
                .expect("decode latest envelope through Nickel migration wrapper");

        assert_eq!(decoded, envelope);
    }

    /// Verifies that the latest Nickel contract accepts a shape covering all
    /// struct fields in `v_latest.rs` that can be exercised without real hashes.
    #[test]
    fn latest_schema_deserializes_comprehensive_v_latest_shape() {
        let source = r#"
{
    version = 1,
    external_data = {},
    tools = {
        "tool_builtin@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
        "tool_exec@1.0.0" = {
            kind = "executable",
            is_impure = false,
            command = ["bin/tool", "--flag"],
            env_vars = {
                DEMO = "true",
            },
            success_codes = [0],
            inputs = {
                input_1 = {},
            },
            outputs = {
                out = {
                    capture = {
                        kind = "stdout",
                    },
                },
            },
        },
    },
    workflows = {
        wf = {
            name = "workflow label",
            description = "workflow description",
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = {
                        input_1 = "hello",
                    },
                    outputs = {
                        out = {
                            save = true,
                        },
                    },
                },
            ],
        },
    },
    tool_configs = {},
    impure_timestamps = {},
    state_pointer = null,
}
"#;

        let decoded: v_latest::NickelEnvelopeLatest =
            migrate_document_source_to_version(source, latest::VERSION, "compatibility-shape")
                .expect("decode comprehensive latest shape via Nickel");

        assert_eq!(decoded.version, latest::VERSION);
        assert_eq!(decoded.tools.len(), 2);
        assert_eq!(decoded.workflows.len(), 1);
        let workflow = decoded.workflows.get("wf").expect("workflow should exist");
        assert_eq!(workflow.name.as_deref(), Some("workflow label"));
        assert_eq!(workflow.description.as_deref(), Some("workflow description"));
    }

    /// Verifies that `save = false` round-trips through encode/decode without
    /// being coerced to default `save = true`.
    #[test]
    fn output_policy_unsaved_round_trips_through_latest_schema() {
        let document = UserNickelDocument {
            workflows: std::collections::BTreeMap::from([(
                "wf".to_string(),
                WorkflowSpec {
                    name: None,
                    description: None,
                    steps: vec![WorkflowStepSpec {
                        id: "step".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: std::collections::BTreeMap::new(),
                        depends_on: Vec::new(),
                        outputs: std::collections::BTreeMap::from([(
                            "result".to_string(),
                            OutputPolicy { save: Some(OutputSaveMode::Unsaved) },
                        )]),
                    }],
                },
            )]),
            ..UserNickelDocument::default()
        };

        let encoded = encode_user_document(document).expect("encode user document");
        let decoded = decode_user_document(&encoded).expect("decode user document");
        let save = decoded
            .workflows
            .get("wf")
            .and_then(|workflow| workflow.steps.first())
            .and_then(|step| step.outputs.get("result"))
            .and_then(|policy| policy.save);

        assert_eq!(save, Some(OutputSaveMode::Unsaved));
    }

    /// Verifies legacy builtin-only extras are rejected by the strict v1 shape.
    #[test]
    fn latest_schema_rejects_legacy_builtin_extra_fields() {
        let source = r#"
{
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
            is_impure = false,
        },
    },
}
"#;

        let err = migrate_document_source_to_version::<v_latest::NickelEnvelopeLatest>(
            source,
            latest::VERSION,
            "compatibility-shape",
        )
        .expect_err("legacy builtin extras must be rejected");
        assert!(err.to_string().contains("is_impure"));
    }

    /// Verifies encode helpers emit documents that can always deserialize into
    /// the latest Rust bridge envelope.
    #[test]
    fn encode_helpers_emit_latest_bridge_compatible_documents() {
        let user_bytes = encode_user_document(UserNickelDocument::default()).expect("encode user");
        let user_source = std::str::from_utf8(&user_bytes).expect("user bytes utf-8");
        let user_envelope: v_latest::NickelEnvelopeLatest =
            migrate_document_source_to_version(user_source, USER_NICKEL_VERSION, "conductor.ncl")
                .expect("decode encoded user envelope as latest bridge type");

        let machine_bytes =
            encode_machine_document(MachineNickelDocument::default()).expect("encode machine");
        let machine_source = std::str::from_utf8(&machine_bytes).expect("machine bytes utf-8");
        let machine_envelope: v_latest::NickelEnvelopeLatest = migrate_document_source_to_version(
            machine_source,
            USER_NICKEL_VERSION,
            "conductor.machine.ncl",
        )
        .expect("decode encoded machine envelope as latest bridge type");

        assert_eq!(user_envelope.version, latest::VERSION);
        assert_eq!(machine_envelope.version, latest::VERSION);
    }

    /// Verifies atomic migration edge declarations are symmetric and adjacent.
    #[test]
    fn atomic_migration_pairs_are_bidirectional_and_adjacent() {
        let metadata = read_migration_metadata().expect("read Nickel migration metadata");
        assert_eq!(metadata.current_version, latest::VERSION);

        for pair in &metadata.atomic_migration_pairs {
            let reverse_exists = metadata
                .atomic_migration_pairs
                .iter()
                .any(|candidate| candidate.from == pair.to && candidate.to == pair.from);
            assert!(
                reverse_exists,
                "missing reverse atomic migration edge for {} -> {}",
                pair.from, pair.to
            );

            let delta = pair.from.abs_diff(pair.to);
            assert_eq!(
                delta, 1,
                "atomic migration edge {} -> {} must be adjacent",
                pair.from, pair.to
            );
        }

        if metadata.supported_versions.len() <= 1 {
            assert!(
                metadata.atomic_migration_pairs.is_empty(),
                "single-version schema must not declare cross-version atomic migrations"
            );
        }
    }

    /// Verifies every declared atomic migration hop round-trips in both
    /// directions without document-shape drift.
    #[test]
    fn atomic_migrations_round_trip_both_directions() {
        let metadata = read_migration_metadata().expect("read Nickel migration metadata");

        if metadata.atomic_migration_pairs.is_empty() {
            assert!(
                metadata.supported_versions.len() <= 1,
                "empty atomic migration table is only valid for single-version schemas"
            );
            return;
        }

        let seed_latest = v_latest::NickelEnvelopeLatest {
            version: latest::VERSION,
            ..v_latest::NickelEnvelopeLatest::default()
        };
        let seed_latest_source = render_document_as_nickel(&seed_latest, "atomic-seed")
            .expect("render latest atomic seed envelope");
        let seed_latest_source = std::str::from_utf8(&seed_latest_source)
            .expect("rendered atomic seed envelope must be utf-8");

        for pair in &metadata.atomic_migration_pairs {
            let start: v_latest::NickelEnvelopeLatest =
                migrate_document_source_to_version(seed_latest_source, pair.from, "atomic-start")
                    .expect("materialize atomic start version from latest seed");
            let start_source = render_document_as_nickel(&start, "atomic-start")
                .expect("render atomic start envelope");
            let start_source =
                std::str::from_utf8(&start_source).expect("atomic start source must be utf-8");

            let forward: v_latest::NickelEnvelopeLatest =
                migrate_document_source_atomic(start_source, pair.from, pair.to, "atomic-forward")
                    .expect("evaluate atomic forward migration hop");

            let forward_source = render_document_as_nickel(&forward, "atomic-forward")
                .expect("render atomic forward envelope");
            let forward_source =
                std::str::from_utf8(&forward_source).expect("atomic forward source must be utf-8");

            let backward: v_latest::NickelEnvelopeLatest = migrate_document_source_atomic(
                forward_source,
                pair.to,
                pair.from,
                "atomic-backward",
            )
            .expect("evaluate atomic backward migration hop");

            assert_eq!(
                forward.version, pair.to,
                "atomic forward migration must end at requested target version"
            );
            assert_eq!(
                backward.version, pair.from,
                "atomic backward migration must end at requested source version"
            );
            assert_eq!(
                backward, start,
                "atomic migration {} -> {} -> {} must round-trip exactly",
                pair.from, pair.to, pair.from
            );
        }
    }

    /// Verifies that `conductor.machine.ncl` preserves the same full schema as
    /// `conductor.ncl`.
    #[test]
    fn decode_machine_document_accepts_full_schema_fields() {
        let source = r#"
{
    version = 1,
    runtime = {
        conductor_dir = ".runtime",
        state_config = ".runtime/state.ncl",
        cas_store_dir = ".runtime/store",
    },
    external_data = {
        "blake3:0000000000000000000000000000000000000000000000000000000000000000" = {
            description = "fixture root",
        },
    },
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
}
"#;

        let decoded =
            decode_machine_document(source.as_bytes()).expect("machine document should decode");
        assert_eq!(decoded.runtime.conductor_dir.as_deref(), Some(".runtime"));
        assert_eq!(decoded.runtime.state_config.as_deref(), Some(".runtime/state.ncl"));
        assert_eq!(decoded.runtime.cas_store_dir.as_deref(), Some(".runtime/store"));
        assert_eq!(decoded.external_data.len(), 1);
        assert_eq!(decoded.tools.len(), 1);
    }

    /// Verifies legacy `runtime_storage` key spelling is rejected.
    #[test]
    fn decode_machine_document_rejects_legacy_runtime_storage_key() {
        let source = r#"
{
    version = 1,
    runtime_storage = {
        conductor_dir = ".runtime",
    },
}
"#;

        let error = decode_machine_document(source.as_bytes())
            .expect_err("legacy runtime_storage key should be rejected");
        assert!(error.to_string().contains("runtime_storage"));
    }

    /// Verifies that `conductor.ncl` preserves the same full schema as
    /// `conductor.machine.ncl`.
    #[test]
    fn decode_user_document_accepts_full_schema_fields() {
        let source = r#"
{
    version = 1,
    runtime = {
        conductor_dir = ".runtime",
        state_config = ".runtime/state.ncl",
        cas_store_dir = ".runtime/store",
    },
    external_data = {
        "blake3:0000000000000000000000000000000000000000000000000000000000000000" = {
            description = "tool content root",
        },
    },
    tool_configs = {
        "tool_a@1.0.0" = {
            max_concurrent_calls = -1,
            max_retries = 1,
            input_defaults = {
                "args" = ["--flag", "value"],
            },
            content_map = {
                "bin/tool" = "blake3:0000000000000000000000000000000000000000000000000000000000000000",
            },
        },
    },
    impure_timestamps = {
        wf = {
            step = {
                epoch_seconds = 123,
                subsec_nanos = 456,
            },
        },
    },
    state_pointer = "blake3:1111111111111111111111111111111111111111111111111111111111111111",
}
"#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        assert_eq!(decoded.runtime.conductor_dir.as_deref(), Some(".runtime"));
        assert_eq!(decoded.runtime.state_config.as_deref(), Some(".runtime/state.ncl"));
        assert_eq!(decoded.runtime.cas_store_dir.as_deref(), Some(".runtime/store"));
        assert_eq!(decoded.external_data.len(), 1);
        assert_eq!(decoded.tool_configs.len(), 1);
        assert!(decoded.tool_configs.get("tool_a@1.0.0").is_some_and(|config| {
            config.input_defaults.contains_key("args") && config.max_retries == 1
        }));
        assert_eq!(
            decoded.impure_timestamps.get("wf").and_then(|steps| steps.get("step")).copied(),
            Some(ImpureTimestamp { epoch_seconds: 123, subsec_nanos: 456 })
        );
        assert!(decoded.state_pointer.is_some());
    }

    /// Verifies tool-config content-map hashes must be rooted in `external_data`.
    #[test]
    fn decode_user_document_rejects_content_map_hash_missing_external_data_root() {
        let source = r#"
{
    version = 1,
    tools = {
        "tool_a@1.0.0" = {
            kind = "executable",
            is_impure = false,
            command = ["bin/tool"],
            env_vars = {},
            success_codes = [0],
            inputs = {},
            outputs = {
                stdout = {
                    capture = {
                        kind = "stdout",
                    },
                },
            },
        },
    },
    tool_configs = {
        "tool_a@1.0.0" = {
            max_concurrent_calls = -1,
            content_map = {
                "bin/tool" = "blake3:0000000000000000000000000000000000000000000000000000000000000000",
            },
        },
    },
}
"#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("content_map hash without external_data root should be rejected");
        assert!(err.to_string().contains("missing from external_data"));
    }

    /// Verifies tool-config retry policy rejects values smaller than `-1`.
    #[test]
    fn decode_user_document_rejects_invalid_max_retries() {
        let source = r#"
{
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    tool_configs = {
        "echo@1.0.0" = {
            max_retries = -2,
        },
    },
}
"#;

        let err =
            decode_user_document(source.as_bytes()).expect_err("invalid max_retries should fail");
        assert!(err.to_string().contains("max_retries must be -1 or a non-negative integer"));
    }

    /// Verifies workflow-step string bindings accept `${external_data.<hash>}`.
    #[test]
    fn decode_user_document_accepts_external_data_input_binding() {
        let source = r#"
{
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        path = "${external_data.blake3:0000000000000000000000000000000000000000000000000000000000000000}",
                    },
                },
            ],
        },
    },
}
"#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let step = &decoded.workflows["wf"].steps[0];
        assert_eq!(
            step.inputs.get("path"),
            Some(&InputBinding::String(
                "${external_data.blake3:0000000000000000000000000000000000000000000000000000000000000000}"
                    .to_string()
            ))
        );
    }

    /// Verifies optional workflow metadata fields survive user-document decode.
    #[test]
    fn decode_user_document_preserves_workflow_metadata_fields() {
        let source = r#"
{
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            name = "friendly workflow",
            description = "informational metadata",
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        text = "hello",
                    },
                },
            ],
        },
    },
}
"#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let workflow = decoded.workflows.get("wf").expect("workflow should exist");
        assert_eq!(workflow.name.as_deref(), Some("friendly workflow"));
        assert_eq!(workflow.description.as_deref(), Some("informational metadata"));
    }

    /// Verifies workflow-step input bindings support mixed literal +
    /// interpolation segments.
    #[test]
    fn decode_user_document_accepts_interpolated_input_binding_segments() {
        let source = r#"
{
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        path = "prefix-${external_data.blake3:0000000000000000000000000000000000000000000000000000000000000000}/artifact.txt",
                    },
                },
            ],
        },
    },
}
"#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let step = &decoded.workflows["wf"].steps[0];
        assert_eq!(
            step.inputs.get("path"),
            Some(&InputBinding::String(
                "prefix-${external_data.blake3:0000000000000000000000000000000000000000000000000000000000000000}/artifact.txt"
                    .to_string()
            ))
        );
    }

    /// Verifies executable input declarations default to scalar `string` kind.
    #[test]
    fn decode_user_document_defaults_input_kind_to_string() {
        let source = r#"
{
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            command = ["bin/tool"],
            inputs = {
                text = {},
            },
            outputs = {
                out = { capture = { kind = "stdout" } },
            },
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = { text = "hello" },
                },
            ],
        },
    },
}
"#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let tool = decoded.tools.get("tool_exec@1.0.0").expect("tool should exist");
        let text_input = tool.inputs.get("text").expect("input should exist");
        assert_eq!(text_input.kind, ToolInputKind::String);
    }

    /// Verifies tool-level executable input defaults are rejected and callers
    /// must use `tool_configs.<tool>.input_defaults` instead.
    #[test]
    fn decode_user_document_rejects_tool_level_input_default_field() {
        let source = r#"
{
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            command = ["bin/tool"],
            inputs = {
                text = {
                    default = "fallback",
                },
            },
            outputs = {
                out = { capture = { kind = "stdout" } },
            },
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = { text = "hello" },
                },
            ],
        },
    },
}
"#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("tool-level input defaults should be rejected");
        assert!(err.to_string().contains("default"));
    }

    /// Verifies executable input declarations support explicit `string_list`
    /// kind and workflow steps can provide list-valued bindings.
    #[test]
    fn decode_user_document_accepts_string_list_input_declaration_and_binding() {
        let source = r#"
{
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            command = ["bin/tool", "${*inputs.args}"],
            inputs = {
                args = { kind = "string_list" },
            },
            outputs = {
                out = { capture = { kind = "stdout" } },
            },
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = {
                        args = ["--one", "--two"],
                    },
                },
            ],
        },
    },
}
"#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let tool = decoded.tools.get("tool_exec@1.0.0").expect("tool should exist");
        let args_input = tool.inputs.get("args").expect("args input should exist");
        assert_eq!(args_input.kind, ToolInputKind::StringList);

        let step = &decoded.workflows["wf"].steps[0];
        assert_eq!(
            step.inputs.get("args"),
            Some(&InputBinding::StringList(vec!["--one".to_string(), "--two".to_string()]))
        );
    }

    /// Verifies executable step input values must match the declared input
    /// kind.
    #[test]
    fn decode_user_document_rejects_executable_step_input_kind_mismatch() {
        let source = r#"
{
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            command = ["bin/tool", "${*inputs.args}"],
            inputs = {
                args = { kind = "string_list" },
            },
            outputs = {
                out = { capture = { kind = "stdout" } },
            },
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = {
                        args = "--not-a-list",
                    },
                },
            ],
        },
    },
}
"#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("mismatched executable input kind should be rejected");
        let message = err.to_string();
        assert!(message.contains("expects kind 'string_list'"));
        assert!(message.contains("received 'string'"));
    }

    /// Verifies unsupported `${...}` workflow-step input expressions fail fast.
    #[test]
    fn decode_user_document_rejects_unsupported_input_binding_expression() {
        let source = r#"
{
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        bad = "${unknown.binding}",
                    },
                },
            ],
        },
    },
}
"#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("unsupported input binding expression should be rejected");
        assert!(err.to_string().contains("unsupported input binding expression"));
    }

    /// Verifies workflow-step input bindings reject materialization directives.
    #[test]
    fn decode_user_document_rejects_materialization_directive_in_input_binding_expression() {
        let source = r#"
{
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        bad = "${step_output.seed.result:file(out.txt)}",
                    },
                },
            ],
        },
    },
}
"#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("materialization directive in input binding should be rejected");
        let message = err.to_string();
        assert!(message.contains(":file(...)"));
        assert!(message.contains(":folder(...)"));
    }

    /// Verifies output-capture kind variants reject undeclared fields.
    #[test]
    fn latest_schema_rejects_output_capture_extra_fields() {
        let source = r#"
{
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            is_impure = false,
            command = ["bin/tool"],
            env_vars = {},
            success_codes = [0],
            inputs = {},
            outputs = {
                out = {
                    capture = {
                        kind = "stdout",
                        path = "unexpected.txt",
                    },
                },
            },
        },
    },
}
"#;

        let err = migrate_document_source_to_version::<v_latest::NickelEnvelopeLatest>(
            source,
            latest::VERSION,
            "compatibility-shape",
        )
        .expect_err("output-capture extra field must be rejected");
        assert!(err.to_string().contains("path"));
    }

    /// Verifies impure timestamp nanosecond components stay within one second.
    #[test]
    fn decode_user_document_rejects_out_of_range_subsec_nanos() {
        let source = r"
{
    version = 1,
    impure_timestamps = {
        wf = {
            step = {
                epoch_seconds = 123,
                subsec_nanos = 1000000000,
            },
        },
    },
}
";

        let err = decode_user_document(source.as_bytes()).expect_err(
            "user document should reject impure timestamp subsec_nanos >= 1_000_000_000",
        );
        assert!(err.to_string().contains("subsec_nanos must be in range 0..999999999"));
    }

    /// Verifies state documents reject non-volatile top-level fields even when
    /// those fields are empty maps.
    #[test]
    fn decode_state_document_rejects_non_volatile_top_level_fields() {
        let source = r"
{
    version = 1,
    impure_timestamps = {},
    state_pointer = null,
    tools = {},
}
    ";

        let err = super::decode_state_document(source.as_bytes())
            .expect_err("state document with non-volatile fields should fail");
        assert!(
            err.to_string()
                .contains("may only define version, impure_timestamps, and state_pointer")
        );
    }

    /// Verifies encoded state documents emit only volatile keys plus explicit
    /// version marker.
    #[test]
    fn encode_state_document_emits_only_volatile_keys() {
        let encoded =
            super::encode_state_document(crate::model::config::StateNickelDocument::default())
                .expect("state encode should succeed");
        let rendered = std::str::from_utf8(&encoded).expect("state source must be utf-8");

        assert!(rendered.contains("version"));
        assert!(rendered.contains("impure_timestamps"));
        assert!(rendered.contains("state_pointer"));
        assert!(!rendered.contains("external_data"));
        assert!(!rendered.contains("tools"));
        assert!(!rendered.contains("workflows"));
        assert!(!rendered.contains("tool_configs"));
    }

    /// Verifies full configuration evaluation requires explicit top-level
    /// `version` markers in all three configuration documents.
    #[test]
    fn evaluate_total_configuration_sources_rejects_missing_version_marker() {
        let user = r"{ version = 1, workflows = {} }";
        let machine = r"{ version = 1, tools = {} }";
        let state = r"{ impure_timestamps = {}, state_pointer = null }";

        let err = super::evaluate_total_configuration_sources(user, machine, state)
            .expect_err("missing state version marker should fail");
        assert!(err.to_string().contains("top-level numeric 'version' field"));
    }
}
