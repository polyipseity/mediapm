//! Export-operation builtin runtime crate.
//!
//! This crate is intentionally standalone:
//! - it exposes API contracts and execution helpers for conductor dispatch, and
//! - it can run independently via its binary target (`src/main.rs`).
//!
//! Stability contract (shared across all builtin crates):
//! - CLI uses standard Rust flag/option syntax while keeping all values as
//!   strings,
//! - API args are string maps (`BTreeMap<String, String>`),
//! - this impure builtin accepts input payload bytes from conductor runtime
//!   when called through API,
//! - undeclared args/inputs fail and required keys must be present.
//!
//! `export` is impure because it materializes payloads onto the host
//! filesystem.

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use clap::{ArgAction, Parser};

/// Stable builtin id used by topology registration.
pub const TOOL_ID: &str = "builtins.export@1.0.0";

/// Builtin process name used by conductor process dispatch.
pub const TOOL_NAME: &str = "export";

/// Canonical semantic version handled by this runtime.
pub const TOOL_VERSION: &str = "1.0.0";

/// Builtin purity marker.
pub const IS_IMPURE: bool = true;

/// Canonical string-map payload used by both API and CLI contracts.
pub type StringMap = BTreeMap<String, String>;

/// Canonical binary-input payload map used by API execution.
pub type BinaryInputMap = BTreeMap<String, Vec<u8>>;

/// Standard clap-based CLI accepted by every builtin crate.
#[derive(Debug, Clone, PartialEq, Eq, Parser)]
pub struct BuiltinCliArgs {
    /// Prints builtin descriptor metadata as JSON and exits.
    #[arg(long, default_value_t = false)]
    pub describe: bool,
    /// Optional execution root override.
    #[arg(long, default_value = ".")]
    pub root_dir: String,
    /// Builtin argument pairs as repeated `--arg KEY VALUE` options.
    #[arg(long = "arg", value_names = ["KEY", "VALUE"], num_args = 2, action = ArgAction::Append)]
    pub args: Vec<String>,
    /// Builtin input pairs as repeated `--input KEY VALUE` options.
    ///
    /// CLI transports inputs as UTF-8 strings. Conductor API calls may provide
    /// arbitrary bytes.
    #[arg(long = "input", value_names = ["KEY", "VALUE"], num_args = 2, action = ArgAction::Append)]
    pub inputs: Vec<String>,
}

/// Returns one deterministic descriptor map for this crate.
#[must_use]
pub fn describe() -> StringMap {
    StringMap::from([
        ("tool_id".to_string(), TOOL_ID.to_string()),
        ("tool_name".to_string(), TOOL_NAME.to_string()),
        ("tool_version".to_string(), TOOL_VERSION.to_string()),
        ("is_impure".to_string(), IS_IMPURE.to_string()),
        (
            "summary".to_string(),
            "export builtin runtime that writes file/folder payloads to host paths".to_string(),
        ),
    ])
}

/// Serializes [`describe`] for CLI output.
pub fn describe_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&describe())
}

/// Executes one export request using argument strings plus binary inputs.
///
/// Required args:
/// - `kind=file|folder`
/// - `path=<destination path>`
/// - optional `path_mode=relative|absolute`
///
/// Required input:
/// - `content=<bytes>`
///
/// Semantics:
/// - default `path_mode` is `relative`,
/// - `relative` resolves `path` under `export_root_dir` and rejects traversal
///   outside that root,
/// - `absolute` requires `path` to be absolute,
/// - `kind=file` writes `content` bytes to resolved destination.
/// - `kind=folder` interprets `content` as one ZIP payload and unpacks it into
///   resolved destination.
pub fn execute_string_map(
    export_root_dir: &Path,
    params: &StringMap,
    inputs: &BinaryInputMap,
) -> Result<StringMap, String> {
    validate_argument_contract(params, inputs)?;

    let kind =
        params.get("kind").ok_or_else(|| "export requires 'kind' (file|folder)".to_string())?;
    let destination = params.get("path").ok_or_else(|| "export requires 'path'".to_string())?;
    let mode = parse_path_mode(params)?;
    let destination = resolve_path_for_export_root(export_root_dir, kind, destination, mode)?;
    let content = payload_bytes_from_maps(inputs, params)
        .ok_or_else(|| "export requires input 'content'".to_string())?;

    match kind.as_str() {
        "file" => {
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent).map_err(|err| {
                    format!("creating export file parent '{}' failed: {err}", parent.display())
                })?;
            }
            std::fs::write(&destination, content).map_err(|err| {
                format!("writing export file '{}' failed: {err}", destination.display())
            })?;

            Ok(StringMap::from([
                ("status".to_string(), "ok".to_string()),
                ("kind".to_string(), "file".to_string()),
                ("path".to_string(), destination.to_string_lossy().to_string()),
                ("bytes_written".to_string(), content.len().to_string()),
            ]))
        }
        "folder" => {
            std::fs::create_dir_all(&destination).map_err(|err| {
                format!("creating export folder '{}' failed: {err}", destination.display())
            })?;

            let extracted_files = mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
                content,
                &destination,
            )?;

            Ok(StringMap::from([
                ("status".to_string(), "ok".to_string()),
                ("kind".to_string(), "folder".to_string()),
                ("path".to_string(), destination.to_string_lossy().to_string()),
                ("extracted_files".to_string(), extracted_files.to_string()),
            ]))
        }
        other => Err(format!("unsupported export kind '{other}'")),
    }
}

/// Destination-path resolution mode for export operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathMode {
    /// Resolve `path` under the export root directory.
    Relative,
    /// Treat `path` as an explicit absolute host path.
    Absolute,
}

/// Resolves export payload bytes from binary inputs or string params.
///
/// Binary inputs take precedence. String-param fallback supports UTF-8 payload
/// transport in environments that only pass args.
fn payload_bytes_from_maps<'a>(
    inputs: &'a BinaryInputMap,
    params: &'a StringMap,
) -> Option<&'a [u8]> {
    if let Some(content) = inputs.get("content") {
        return Some(content.as_slice());
    }
    params.get("content").map(String::as_bytes)
}

/// Runs the standalone CLI command using a normal clap-parsed option structure.
///
/// CLI `--input` values are UTF-8 and are converted to bytes before API
/// execution.
pub fn run_cli_command<W: Write>(
    cli: &BuiltinCliArgs,
    writer: &mut W,
) -> Result<(), Box<dyn Error>> {
    if cli.describe {
        let descriptor = describe_json()?;
        writer.write_all(descriptor.as_bytes())?;
        return Ok(());
    }

    let tool_cwd = PathBuf::from(&cli.root_dir);
    let params = parse_string_pairs(&cli.args, "args").map_err(std::io::Error::other)?;
    let input_strings = parse_string_pairs(&cli.inputs, "inputs").map_err(std::io::Error::other)?;
    let binary_inputs = input_strings
        .into_iter()
        .map(|(key, value)| (key, value.into_bytes()))
        .collect::<BinaryInputMap>();

    let response =
        execute_string_map(&tool_cwd, &params, &binary_inputs).map_err(std::io::Error::other)?;
    let payload = serde_json::to_vec(&response)?;
    writer.write_all(&payload)?;
    Ok(())
}

/// Converts repeated `--arg KEY VALUE` or `--input KEY VALUE` pairs into a map.
fn parse_string_pairs(pairs: &[String], label: &str) -> Result<StringMap, String> {
    let mut map = StringMap::new();
    let mut chunks = pairs.chunks_exact(2);
    for chunk in &mut chunks {
        let key = chunk[0].trim();
        let value = &chunk[1];
        if key.is_empty() {
            return Err(format!("invalid {label} entry; key must be non-empty"));
        }
        if map.insert(key.to_string(), value.to_string()).is_some() {
            return Err(format!("duplicate {label} entry for key '{key}'"));
        }
    }
    if !chunks.remainder().is_empty() {
        let option_name = if label == "args" { "arg" } else { "input" };
        return Err(format!(
            "invalid {label} entries; expected repeated '--{option_name} KEY VALUE' pairs"
        ));
    }
    Ok(map)
}

/// Validates export args/inputs for required and recognized keys.
fn validate_argument_contract(params: &StringMap, inputs: &BinaryInputMap) -> Result<(), String> {
    for key in params.keys() {
        if key != "kind" && key != "path" && key != "path_mode" && key != "content" {
            return Err(format!("export builtin does not accept arg '{key}'"));
        }
    }

    for key in inputs.keys() {
        if key != "content" {
            return Err(format!("export builtin does not accept input '{key}'"));
        }
    }

    let kind =
        params.get("kind").ok_or_else(|| "export requires 'kind' (file|folder)".to_string())?;
    if kind != "file" && kind != "folder" {
        return Err(format!("unsupported export kind '{kind}'"));
    }

    let path = params.get("path").ok_or_else(|| "export requires 'path'".to_string())?;
    if path.trim().is_empty() {
        return Err("export requires non-empty 'path'".to_string());
    }

    let _ = parse_path_mode(params)?;

    let Some(content) = payload_bytes_from_maps(inputs, params) else {
        return Err("export requires input 'content'".to_string());
    };
    if kind == "folder" && content.is_empty() {
        return Err("export kind='folder' requires non-empty input 'content' ZIP bytes".to_string());
    }

    Ok(())
}

/// Parses export destination path-mode selector.
fn parse_path_mode(params: &StringMap) -> Result<PathMode, String> {
    match params.get("path_mode").map(String::as_str).unwrap_or("relative") {
        "relative" => Ok(PathMode::Relative),
        "absolute" => Ok(PathMode::Absolute),
        other => Err(format!("export path_mode must be 'relative' or 'absolute', got '{other}'")),
    }
}

/// Resolves one candidate path against export root + path-mode semantics.
fn resolve_path_for_export_root(
    export_root_dir: &Path,
    kind: &str,
    candidate: &str,
    mode: PathMode,
) -> Result<PathBuf, String> {
    match mode {
        PathMode::Relative => {
            if Path::new(candidate).is_absolute() {
                return Err(format!(
                    "export kind='{kind}' with path_mode='relative' requires relative 'path'"
                ));
            }
            let root = absolute_root(export_root_dir)?;
            let normalized = normalize_relative_path(candidate, "export destination path")?;
            Ok(root.join(normalized))
        }
        PathMode::Absolute => {
            let parsed = Path::new(candidate);
            if !parsed.is_absolute() {
                return Err(format!(
                    "export kind='{kind}' with path_mode='absolute' requires absolute 'path'"
                ));
            }
            Ok(parsed.to_path_buf())
        }
    }
}

/// Resolves export root directory into absolute path.
fn absolute_root(root: &Path) -> Result<PathBuf, String> {
    if root.is_absolute() {
        return Ok(root.to_path_buf());
    }

    std::env::current_dir()
        .map(|cwd| cwd.join(root))
        .map_err(|err| format!("resolving current directory for export root failed: {err}"))
}

/// Normalizes one relative path and rejects escaping components.
fn normalize_relative_path(candidate: &str, context: &str) -> Result<PathBuf, String> {
    if candidate.trim().is_empty() {
        return Err(format!("{context} must be non-empty"));
    }

    let parsed = Path::new(candidate);
    if parsed.is_absolute() {
        return Err(format!("{context} must be relative"));
    }

    let mut normalized = PathBuf::new();
    for component in parsed.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!("{context} must stay under export root directory"));
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err(format!("{context} must contain at least one path component"));
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::{BinaryInputMap, StringMap, execute_string_map};

    /// Verifies file export writes input bytes directly to destination path.
    #[test]
    fn export_file_writes_bytes() {
        let temp = tempdir().expect("tempdir");
        let response = execute_string_map(
            temp.path(),
            &StringMap::from([
                ("kind".to_string(), "file".to_string()),
                ("path".to_string(), "out/payload.bin".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"hello".to_vec())]),
        )
        .expect("export file should succeed");

        assert_eq!(response.get("status"), Some(&"ok".to_string()));
        assert_eq!(response.get("kind"), Some(&"file".to_string()));
        assert_eq!(
            std::fs::read(temp.path().join("out").join("payload.bin")).ok(),
            Some(b"hello".to_vec())
        );
    }

    /// Verifies folder export unpacks ZIP bytes into destination directory.
    #[test]
    fn export_folder_unpacks_zip_bytes() {
        let temp = tempdir().expect("tempdir");
        let source = temp.path().join("source");
        std::fs::create_dir_all(source.join("nested")).expect("create source dir");
        std::fs::write(source.join("nested").join("a.txt"), b"A").expect("write source file");

        let zip_payload =
            mediapm_conductor_builtin_archive::pack_directory_to_uncompressed_zip_bytes(
                &source, false,
            )
            .expect("pack source folder to zip");

        execute_string_map(
            temp.path(),
            &StringMap::from([
                ("kind".to_string(), "folder".to_string()),
                ("path".to_string(), "exported".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), zip_payload)]),
        )
        .expect("export folder should succeed");

        assert!(temp.path().join("exported").join("nested").join("a.txt").exists());
    }

    /// Verifies unknown argument keys are rejected.
    #[test]
    fn export_rejects_unknown_arg() {
        let temp = tempdir().expect("tempdir");
        let error = execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "file".to_string()),
                ("path".to_string(), "x.txt".to_string()),
                ("unexpected".to_string(), "x".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"x".to_vec())]),
        )
        .expect_err("unknown arg should fail");

        assert!(error.contains("does not accept arg 'unexpected'"));
    }

    /// Verifies relative mode rejects absolute destination path values.
    #[test]
    fn export_relative_mode_rejects_absolute_path() {
        let temp = tempdir().expect("tempdir");
        let absolute_path = temp.path().join("out.bin");
        let error = execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "file".to_string()),
                ("path".to_string(), absolute_path.to_string_lossy().to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"x".to_vec())]),
        )
        .expect_err("relative mode should reject absolute path");

        assert!(error.contains("path_mode='relative'"));
    }

    /// Verifies relative mode rejects escaping parent traversal.
    #[test]
    fn export_relative_mode_rejects_parent_escape() {
        let temp = tempdir().expect("tempdir");
        let error = execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "file".to_string()),
                ("path".to_string(), "../escape.bin".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"x".to_vec())]),
        )
        .expect_err("relative mode should reject parent traversal");

        assert!(error.contains("must stay under export root directory"));
    }

    /// Verifies absolute mode allows explicit absolute destination paths.
    #[test]
    fn export_absolute_mode_accepts_absolute_path() {
        let temp = tempdir().expect("tempdir");
        let absolute_path = temp.path().join("abs").join("payload.bin");

        execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "file".to_string()),
                ("path_mode".to_string(), "absolute".to_string()),
                ("path".to_string(), absolute_path.to_string_lossy().to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"ok".to_vec())]),
        )
        .expect("absolute mode should accept absolute path");

        assert_eq!(std::fs::read(&absolute_path).ok(), Some(b"ok".to_vec()));
    }
}
