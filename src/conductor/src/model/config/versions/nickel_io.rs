//! Low-level Nickel evaluation, rendering, and document workspace helpers.

use std::fs;
use std::io;
use std::path::Path;

use nickel_lang_core::error::{Error as NickelError, NullReporter};
use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::program::Program;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::ConductorError;

use super::{MOD_NCL_SOURCE, latest, resolve_version_contract};

/// Temporary Nickel workspace used to evaluate conductor-generated wrappers.
#[derive(Debug)]
pub(super) struct TempNickelWorkspace {
    /// Root directory that hosts temporary `.ncl` files.
    dir: tempfile::TempDir,
}

impl TempNickelWorkspace {
    /// Creates a unique temporary Nickel workspace root.
    pub(super) fn new() -> Result<Self, ConductorError> {
        let dir = tempfile::Builder::new().prefix("mediapm-conductor-nickel-").tempdir().map_err(
            |source| ConductorError::Io {
                operation: "creating temporary Nickel workspace".to_string(),
                path: std::env::temp_dir(),
                source,
            },
        )?;

        Ok(Self { dir })
    }

    /// Returns the workspace root path.
    pub(super) fn path(&self) -> &Path {
        self.dir.path()
    }
}

/// Writes one Nickel source file into the temporary workspace.
pub(super) fn write_nickel_file(
    path: &Path,
    source: &str,
    operation: &str,
) -> Result<(), ConductorError> {
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
pub(super) fn evaluate_main_file_as<T>(main_file: &Path, context: &str) -> Result<T, ConductorError>
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
pub(super) fn read_document_version_marker(
    source: &str,
    document_kind: &str,
) -> Result<u32, ConductorError> {
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

pub(super) fn validate_state_document_source_shape(source: &str) -> Result<(), ConductorError> {
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
pub(super) fn latest_version_among_sources(
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
/// Renders `text` as a Nickel multiline string literal using 2-percent delimiters.
///
/// Uses `m%%"..."%%` so content that contains the 1-percent closing sequence
/// `"%` is still safe. Literal `%{` interpolation markers are escaped to `%%{`.
fn render_nickel_multiline_string(text: &str) -> String {
    let escaped = text.replace("%{", "%%{");
    format!("m%%\"\n{escaped}\n\"%%")
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
                ordered_entries.sort_by_key(|(key, _)| *key);
                let rendered_entries = ordered_entries
                    .into_iter()
                    .map(|(key, entry_value)| {
                        let rendered_value = match (key.as_str(), entry_value) {
                            ("description", Value::String(text)) => {
                                render_nickel_multiline_string(text)
                            }
                            _ => render_nickel_value(entry_value, indent + 2),
                        };
                        format!("{next_pad}{} = {},", render_field_name(key), rendered_value)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{{\n{rendered_entries}\n{pad}}}")
            }
        }
    }
}

/// Renders one serializable Rust structure as Nickel source.
pub(super) fn render_document_as_nickel<T>(
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
pub(super) fn evaluate_document_source<T>(
    source: &str,
    document_kind: &str,
) -> Result<T, ConductorError>
where
    T: DeserializeOwned,
{
    migrate_document_source_to_version(source, latest::VERSION, document_kind)
}
