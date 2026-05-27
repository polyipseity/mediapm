//! Nickel document evaluation and Rust value rendering helpers.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use nickel_lang_core::error::{Error as NickelError, NullReporter};
use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::program::Program;
use serde::Deserialize;
use serde_json::Value;

use crate::error::MediaPmError;

/// Creates a temporary Nickel workspace that is cleaned up on drop.
#[derive(Debug)]
struct TempNickelWorkspace {
    /// Temporary workspace root.
    path: PathBuf,
}

impl TempNickelWorkspace {
    /// Allocates one unique temporary Nickel workspace directory.
    fn new() -> Result<Self, MediaPmError> {
        let pid = std::process::id();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        let path = std::env::temp_dir().join(format!("mediapm-nickel-{pid}-{nanos}"));

        fs::create_dir_all(&path).map_err(|source| MediaPmError::Io {
            operation: "creating temporary Nickel workspace".to_string(),
            path: path.clone(),
            source,
        })?;

        Ok(Self { path })
    }
}

impl Drop for TempNickelWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Evaluates one Nickel source string into exported JSON value.
pub(super) fn evaluate_nickel_source_to_json(
    path: &Path,
    source: &str,
) -> Result<Value, MediaPmError> {
    let workspace = TempNickelWorkspace::new()?;
    let source_path = workspace.path.join("mediapm.ncl");

    fs::write(&source_path, source).map_err(|source_err| MediaPmError::Io {
        operation: "writing temporary mediapm.ncl source".to_string(),
        path: source_path.clone(),
        source: source_err,
    })?;

    let mut program = Program::<CacheImpl>::new_from_file(
        source_path.as_os_str(),
        std::io::sink(),
        NullReporter {},
    )
    .map_err(|source_err| MediaPmError::Io {
        operation: "constructing Nickel program".to_string(),
        path: path.to_path_buf(),
        source: source_err,
    })?;

    let exported = program.eval_full_for_export().map_err(|err| {
        MediaPmError::Workflow(format!(
            "evaluating mediapm.ncl: {}",
            render_nickel_error(&mut program, err)
        ))
    })?;

    Value::deserialize(exported).map_err(|err| {
        MediaPmError::Serialization(format!("deserializing exported Nickel value: {err}"))
    })
}

/// Renders one Nickel interpreter error as user-facing text.
fn render_nickel_error(program: &mut Program<CacheImpl>, err: NickelError) -> String {
    nickel_lang_core::error::report::report_as_str(
        &mut program.files(),
        err,
        nickel_lang_core::error::report::ColorOpt::Never,
    )
}

/// Renders a field name in Nickel record syntax.
fn render_field_name(name: &str) -> String {
    if is_bare_identifier(name) {
        name.to_string()
    } else {
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""))
    }
}

/// Returns true when one record key can be emitted as a bare Nickel identifier.
fn is_bare_identifier(input: &str) -> bool {
    if is_nickel_reserved_identifier(input) {
        return false;
    }

    let mut chars = input.chars().peekable();

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

/// Returns true when one identifier token is reserved by Nickel syntax.
#[must_use]
fn is_nickel_reserved_identifier(input: &str) -> bool {
    matches!(
        input,
        "if" | "then"
            | "else"
            | "let"
            | "in"
            | "match"
            | "with"
            | "forall"
            | "fun"
            | "rec"
            | "import"
            | "as"
            | "null"
            | "true"
            | "false"
    )
}

/// Renders JSON as deterministic Nickel source with sorted object keys.
/// Renders `text` as a Nickel multiline string literal using 2-percent delimiters.
///
/// Uses `m%%"..."%%` so content that contains the 1-percent closing sequence
/// `"%` is still safe. Literal `%{` interpolation markers are escaped to `%%{`.
fn render_nickel_multiline_string(text: &str) -> String {
    let escaped = text.replace("%{", "%%{");
    format!("m%%\"\n{escaped}\n\"%%")
}

/// Renders JSON as deterministic Nickel source with sorted object keys.
pub(super) fn render_nickel_value(value: &Value, indent: usize) -> String {
    let pad = " ".repeat(indent);
    let next_pad = " ".repeat(indent + 2);

    match value {
        Value::Null => "null".to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => render_nickel_number(number),
        Value::String(text) => serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(items) => {
            if items.is_empty() {
                "[]".to_string()
            } else {
                let body = items
                    .iter()
                    .map(|item| format!("{next_pad}{},", render_nickel_value(item, indent + 2)))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("[\n{body}\n{pad}]")
            }
        }
        Value::Object(entries) => {
            if entries.is_empty() {
                "{}".to_string()
            } else {
                let mut ordered = entries.iter().collect::<Vec<_>>();
                ordered.sort_by_key(|(key, _)| *key);
                let body = ordered
                    .into_iter()
                    .map(|(key, item)| {
                        let rendered_value = match (key.as_str(), item) {
                            ("description", Value::String(text)) => {
                                render_nickel_multiline_string(text)
                            }
                            _ => render_nickel_value(item, indent + 2),
                        };
                        format!("{next_pad}{} = {},", render_field_name(key), rendered_value)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{{\n{body}\n{pad}}}")
            }
        }
    }
}

/// Renders one JSON number value into deterministic Nickel numeric syntax.
///
/// Integral values are emitted without trailing decimal points (`0` instead of
/// `0.0`) so generated Nickel remains stable for integer-typed fields such as
/// output-variant `idx`.
fn render_nickel_number(number: &serde_json::Number) -> String {
    if let Some(value) = number.as_i64() {
        return value.to_string();
    }

    if let Some(value) = number.as_u64() {
        return value.to_string();
    }

    if let Some(value) = number.as_f64()
        && value.is_finite()
        && value.fract() == 0.0
    {
        return format!("{value:.0}");
    }

    number.to_string()
}

/// Normalizes `version` field numbers exported by Nickel into integer JSON numbers.
pub(super) fn normalize_version_field_to_u64(
    value: &mut Value,
    document_name: &str,
) -> Result<(), MediaPmError> {
    let Some(object) = value.as_object_mut() else {
        return Err(MediaPmError::Workflow(format!(
            "{document_name} must evaluate to a top-level record"
        )));
    };

    let Some(version_value) = object.get("version").cloned() else {
        return Ok(());
    };

    let normalized = if let Some(raw) = version_value.as_u64() {
        raw
    } else if let Some(raw) = version_value.as_f64() {
        let Some(normalized) = parse_non_negative_integral_u64(raw) else {
            return Err(MediaPmError::Workflow(format!(
                "{document_name} version must be a non-negative integer"
            )));
        };
        normalized
    } else {
        return Err(MediaPmError::Workflow(format!("{document_name} version must be numeric")));
    };

    object.insert("version".to_string(), Value::from(normalized));
    Ok(())
}

/// Parses one non-negative integral `f64` into `u64` when lossless.
#[must_use]
pub(super) fn parse_non_negative_integral_u64(value: f64) -> Option<u64> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
        return None;
    }

    format!("{value:.0}").parse::<u64>().ok()
}

/// Parses one non-negative integral `f64` into `u32` when lossless.
#[must_use]
pub(super) fn parse_non_negative_integral_u32(value: f64) -> Option<u32> {
    parse_non_negative_integral_u64(value).and_then(|normalized| u32::try_from(normalized).ok())
}
