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

#[cfg(feature = "cli")]
use std::error::Error;
#[cfg(feature = "cli")]
use std::io::Write;
use std::path::Path;
#[cfg(feature = "cli")]
use std::path::PathBuf;

#[cfg(feature = "cli")]
pub use mediapm_utils::builtin::BuiltinCliArgs;
#[cfg(feature = "cli")]
use mediapm_utils::builtin::describe_json_compact_meta;
#[cfg(feature = "cli")]
use mediapm_utils::builtin::parse_string_pairs;
use mediapm_utils::{
    BinaryInputMap, StringMap,
    builtin::{
        BuiltinMeta, describe_meta, require_non_empty_param, require_param,
        validate_only_known_keys,
    },
};

/// Stable builtin id used by topology registration.
pub const TOOL_ID: &str = META.tool_id;

/// Versioned builtin identifier (e.g. "export@v1") used in config map keys and dispatch.
pub const TOOL_BUILTIN_ID: &str = "export@v1";

/// Builtin process name used by conductor process dispatch.
pub const TOOL_NAME: &str = META.tool_name;

/// Canonical semantic version handled by this runtime.
pub const TOOL_VERSION: &str = META.tool_version;

/// Builtin purity marker.
pub const IS_IMPURE: bool = META.is_impure;

/// Metadata for this builtin crate.
pub const META: BuiltinMeta = BuiltinMeta {
    tool_id: "builtins.export@v1",
    tool_name: "export",
    tool_version: "v1",
    is_impure: true,
    summary: "export builtin runtime that writes file/folder payloads to host paths",
};

/// Returns one deterministic descriptor map for this crate.
#[must_use]
pub fn describe() -> StringMap {
    describe_meta(&META)
}

/// Serializes [`describe`] for CLI output.
#[cfg(feature = "cli")]
#[must_use]
pub fn describe_json() -> String {
    describe_json_compact_meta(&META)
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
///
/// # Errors
///
/// Returns an error when arguments are invalid, path-mode/path-resolution
/// checks fail, folder ZIP payloads are invalid, destination directories/files
/// cannot be created, or write/unpack operations fail.
pub fn execute_string_map(
    export_root_dir: &Path,
    params: &StringMap,
    inputs: &BinaryInputMap,
) -> Result<StringMap, String> {
    validate_argument_contract(params, inputs)?;

    let kind =
        params.get("kind").ok_or_else(|| "export requires 'kind' (file|folder)".to_string())?;
    let destination = params.get("path").ok_or_else(|| "export requires 'path'".to_string())?;
    let mode = mediapm_utils::path::parse_path_mode(params, "export")?;
    let destination = mediapm_utils::path::resolve_path_for_root(
        export_root_dir,
        &format!("export kind='{kind}'"),
        "path",
        destination,
        mode,
    )?;
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
///
/// # Errors
///
/// Returns an error when CLI key/value pairs are malformed, export execution
/// fails, descriptor serialization fails, or writing output to the provided
/// writer fails.
#[cfg(feature = "cli")]
pub fn run_cli_command<W: Write>(
    cli: &BuiltinCliArgs,
    writer: &mut W,
) -> Result<(), Box<dyn Error>> {
    if cli.describe {
        let descriptor = describe_json();
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

/// Validates export args/inputs for required and recognized keys.
fn validate_argument_contract(params: &StringMap, inputs: &BinaryInputMap) -> Result<(), String> {
    validate_only_known_keys(params, &["kind", "path", "path_mode", "content"], "export")?;
    validate_only_known_keys(inputs, &["content"], "export")?;

    let kind = require_param(params, "kind", "export")?;
    if kind != "file" && kind != "folder" {
        return Err(format!("unsupported export kind '{kind}', expected 'file' or 'folder'"));
    }

    let _ = require_non_empty_param(params, "path", "export")?;

    let _ = mediapm_utils::path::parse_path_mode(params, "export")?;

    let Some(content) = payload_bytes_from_maps(inputs, params) else {
        return Err("export requires input 'content'".to_string());
    };
    if kind == "folder" && content.is_empty() {
        return Err("export kind='folder' requires non-empty input 'content' ZIP bytes".to_string());
    }

    Ok(())
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
}
