//! Archive-operations builtin runtime crate.
//!
//! This crate is intentionally standalone:
//! - it exposes API contracts and execution helpers for conductor dispatch, and
//! - it can run independently via its binary target (`src/main.rs`).
//!
//! `archive` is now a **pure** content transformer:
//! - it does not accept file paths,
//! - it does not write to file paths,
//! - it transforms input bytes into output bytes.
//!
//! Supported actions:
//! - `pack`: wrap `file` or `folder` content into one ZIP payload,
//! - `unpack`: convert archive bytes into folder payload bytes,
//! - `repack`: normalize archive bytes into canonical uncompressed ZIP bytes.
//!
//! Folder payloads are represented as uncompressed ZIP bytes (stored entries).

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser};

/// Stable builtin id used by topology registration.
pub const TOOL_ID: &str = "mediapm.builtin.archive@1.0.0";

/// Builtin process name used by conductor process dispatch.
pub const TOOL_NAME: &str = "archive";

/// Canonical semantic version handled by this runtime.
pub const TOOL_VERSION: &str = "1.0.0";

/// Builtin purity marker.
pub const IS_IMPURE: bool = false;

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
            "pure archive builtin runtime transforming bytes to bytes".to_string(),
        ),
    ])
}

/// Serializes [`describe`] for CLI output.
pub fn describe_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&describe())
}

/// Executes one archive request and returns transformed bytes.
///
/// Supported params:
/// - `action=pack` with `kind=file|folder` and corresponding input `content`,
/// - `action=unpack` with input `archive`,
/// - `action=repack` with input `archive`.
///
/// Optional params:
/// - `entry_name` when `action=pack` and `kind=file`.
pub fn execute_content_map(params: &StringMap, inputs: &BinaryInputMap) -> Result<Vec<u8>, String> {
    validate_argument_contract(params, inputs)?;

    let action = params
        .get("action")
        .ok_or_else(|| "archive builtin requires 'action' (pack|unpack|repack)".to_string())?;

    match action.as_str() {
        "pack" => {
            let kind = params
                .get("kind")
                .ok_or_else(|| "archive pack requires 'kind' (file|folder)".to_string())?;
            let payload = payload_bytes_from_maps(inputs, params, "content")
                .ok_or_else(|| "archive pack requires input 'content'".to_string())?;

            match kind.as_str() {
                "file" => {
                    let entry_name =
                        params.get("entry_name").map(String::as_str).unwrap_or("content.bin");
                    pack_single_file_to_uncompressed_zip_bytes(payload, entry_name)
                }
                "folder" => normalize_archive_zip_bytes_to_folder_zip_bytes(payload),
                other => {
                    Err(format!("archive pack kind must be 'file' or 'folder', got '{other}'"))
                }
            }
        }
        "unpack" => {
            let archive_payload = payload_bytes_from_maps(inputs, params, "archive")
                .ok_or_else(|| "archive unpack requires input 'archive'".to_string())?;
            normalize_archive_zip_bytes_to_folder_zip_bytes(archive_payload)
        }
        "repack" => {
            let archive_payload = payload_bytes_from_maps(inputs, params, "archive")
                .ok_or_else(|| "archive repack requires input 'archive'".to_string())?;
            normalize_archive_zip_bytes_to_folder_zip_bytes(archive_payload)
        }
        other => Err(format!("unsupported archive action '{other}'")),
    }
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

    let _tool_cwd = PathBuf::from(&cli.root_dir);
    let params = parse_string_pairs(&cli.args, "args").map_err(std::io::Error::other)?;
    let input_strings = parse_string_pairs(&cli.inputs, "inputs").map_err(std::io::Error::other)?;
    let binary_inputs = input_strings
        .into_iter()
        .map(|(key, value)| (key, value.into_bytes()))
        .collect::<BinaryInputMap>();

    let payload = execute_content_map(&params, &binary_inputs).map_err(std::io::Error::other)?;
    writer.write_all(&payload)?;
    Ok(())
}

/// Packs one directory tree into uncompressed ZIP bytes.
///
/// This helper exists for conductor-runtime internals that need deterministic
/// folder payload construction.
pub fn pack_directory_to_uncompressed_zip_bytes(
    source_dir: &Path,
    include_source_dir: bool,
) -> Result<Vec<u8>, String> {
    if !source_dir.exists() {
        return Err(format!("archive source directory '{}' does not exist", source_dir.display()));
    }
    if !source_dir.is_dir() {
        return Err(format!("archive source '{}' must be a directory", source_dir.display()));
    }

    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::<u8>::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o644);

    let source_dir_name = if include_source_dir {
        Some(
            source_dir
                .file_name()
                .ok_or_else(|| {
                    format!(
                        "archive source '{}' must end in a concrete directory name when include_source_dir=true",
                        source_dir.display()
                    )
                })?
                .to_string_lossy()
                .replace('\\', "/"),
        )
    } else {
        None
    };

    for entry in walkdir::WalkDir::new(source_dir) {
        let entry = entry.map_err(|err| format!("walkdir failed: {err}"))?;
        let path = entry.path();
        if path == source_dir {
            if let Some(root_name) = &source_dir_name {
                let mut root_entry = root_name.clone();
                if !root_entry.ends_with('/') {
                    root_entry.push('/');
                }
                writer
                    .add_directory(root_entry, options)
                    .map_err(|err| format!("adding root directory to zip failed: {err}"))?;
            }
            continue;
        }

        let relative =
            path.strip_prefix(source_dir).map_err(|err| format!("strip prefix failed: {err}"))?;
        let mut name = relative.to_string_lossy().replace('\\', "/");
        if let Some(root_name) = &source_dir_name {
            name = format!("{root_name}/{name}");
        }

        if entry.file_type().is_dir() {
            if !name.ends_with('/') {
                name.push('/');
            }
            writer
                .add_directory(name, options)
                .map_err(|err| format!("adding directory to zip failed: {err}"))?;
            continue;
        }

        writer
            .start_file(name, options)
            .map_err(|err| format!("starting zip file entry failed: {err}"))?;

        let mut source = std::fs::File::open(path)
            .map_err(|err| format!("opening source file '{}' failed: {err}", path.display()))?;
        std::io::copy(&mut source, &mut writer)
            .map_err(|err| format!("writing zip file entry failed: {err}"))?;
    }

    writer
        .finish()
        .map_err(|err| format!("finalizing zip payload failed: {err}"))
        .map(|cursor| cursor.into_inner())
}

/// Unpacks ZIP payload bytes into one destination directory.
///
/// The implementation rejects escaping ZIP entries (`../`) through
/// `enclosed_name` checks.
pub fn unpack_zip_bytes_to_directory(zip_bytes: &[u8], dest_dir: &Path) -> Result<usize, String> {
    std::fs::create_dir_all(dest_dir)
        .map_err(|err| format!("creating destination '{}' failed: {err}", dest_dir.display()))?;

    let cursor = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|err| format!("reading zip archive bytes failed: {err}"))?;

    let mut extracted_files = 0usize;
    for index in 0..archive.len() {
        let mut entry =
            archive.by_index(index).map_err(|err| format!("reading zip entry failed: {err}"))?;
        let enclosed = entry.enclosed_name().ok_or_else(|| {
            format!("unsafe zip entry name '{}', escaping destination", entry.name())
        })?;
        let out_path = dest_dir.join(enclosed);

        if entry.name().ends_with('/') {
            std::fs::create_dir_all(&out_path).map_err(|err| {
                format!("creating directory '{}' failed: {err}", out_path.display())
            })?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| format!("creating parent '{}' failed: {err}", parent.display()))?;
        }

        let mut outfile = std::fs::File::create(&out_path)
            .map_err(|err| format!("creating file '{}' failed: {err}", out_path.display()))?;
        std::io::copy(&mut entry, &mut outfile)
            .map_err(|err| format!("writing file '{}' failed: {err}", out_path.display()))?;
        extracted_files = extracted_files.saturating_add(1);
    }

    Ok(extracted_files)
}

/// Converts one archive ZIP payload into canonical folder ZIP payload bytes.
///
/// The resulting bytes always use uncompressed (stored) ZIP entries.
pub fn normalize_archive_zip_bytes_to_folder_zip_bytes(
    archive_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    let workspace = tempfile::tempdir().map_err(|err| {
        format!("creating temporary archive normalization workspace failed: {err}")
    })?;
    let unpack_dir = workspace.path().join("unpacked");

    unpack_zip_bytes_to_directory(archive_bytes, &unpack_dir)?;
    pack_directory_to_uncompressed_zip_bytes(&unpack_dir, false)
}

/// Packs one file payload into one uncompressed ZIP payload.
fn pack_single_file_to_uncompressed_zip_bytes(
    content: &[u8],
    entry_name: &str,
) -> Result<Vec<u8>, String> {
    let trimmed_entry_name = entry_name.trim();
    if trimmed_entry_name.is_empty() {
        return Err("archive pack requires non-empty 'entry_name' for kind='file'".to_string());
    }
    if trimmed_entry_name.contains('\\')
        || trimmed_entry_name.starts_with('/')
        || trimmed_entry_name.split('/').any(|segment| segment == "..")
    {
        return Err(format!(
            "archive pack received unsupported 'entry_name' '{trimmed_entry_name}'; expected one relative file path"
        ));
    }

    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o644);
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::<u8>::new()));
    writer
        .start_file(trimmed_entry_name, options)
        .map_err(|err| format!("starting zip entry '{trimmed_entry_name}' failed: {err}"))?;
    writer
        .write_all(content)
        .map_err(|err| format!("writing zip entry '{trimmed_entry_name}' failed: {err}"))?;
    writer
        .finish()
        .map_err(|err| format!("finalizing file-pack zip payload failed: {err}"))
        .map(|cursor| cursor.into_inner())
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

/// Resolves one payload byte value from binary inputs or string params.
///
/// Binary inputs take precedence. String-param fallback supports existing
/// builtin step input injection semantics for UTF-8 payloads.
fn payload_bytes_from_maps<'a>(
    inputs: &'a BinaryInputMap,
    params: &'a StringMap,
    key: &str,
) -> Option<&'a [u8]> {
    if let Some(bytes) = inputs.get(key) {
        return Some(bytes.as_slice());
    }
    params.get(key).map(String::as_bytes)
}

/// Validates archive args/inputs for required and recognized keys.
fn validate_argument_contract(params: &StringMap, inputs: &BinaryInputMap) -> Result<(), String> {
    for key in params.keys() {
        if key != "action"
            && key != "kind"
            && key != "entry_name"
            && key != "content"
            && key != "archive"
        {
            return Err(format!("archive builtin does not accept arg '{key}'"));
        }
    }

    let action = params
        .get("action")
        .ok_or_else(|| "archive builtin requires 'action' (pack|unpack|repack)".to_string())?;

    match action.as_str() {
        "pack" => {
            let kind = params
                .get("kind")
                .ok_or_else(|| "archive pack requires 'kind' (file|folder)".to_string())?;
            if kind != "file" && kind != "folder" {
                return Err(format!("archive pack kind must be 'file' or 'folder', got '{kind}'"));
            }

            for key in inputs.keys() {
                if key != "content" {
                    return Err(format!("archive pack does not accept input '{key}'"));
                }
            }
            if payload_bytes_from_maps(inputs, params, "content").is_none() {
                return Err("archive pack requires input 'content'".to_string());
            }
        }
        "unpack" | "repack" => {
            if params.contains_key("kind") {
                return Err(format!("archive action '{action}' does not accept arg 'kind'"));
            }
            if params.contains_key("entry_name") {
                return Err(format!("archive action '{action}' does not accept arg 'entry_name'"));
            }

            for key in inputs.keys() {
                if key != "archive" {
                    return Err(format!("archive action '{action}' does not accept input '{key}'"));
                }
            }
            if payload_bytes_from_maps(inputs, params, "archive").is_none() {
                return Err(format!("archive action '{action}' requires input 'archive'"));
            }
        }
        other => {
            return Err(format!(
                "archive action must be 'pack', 'unpack', or 'repack', got '{other}'"
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::{
        BinaryInputMap, describe_json, execute_content_map,
        pack_directory_to_uncompressed_zip_bytes, unpack_zip_bytes_to_directory,
    };

    /// Verifies file pack emits one ZIP payload that can be unpacked to a file.
    #[test]
    fn execute_pack_file_emits_zip_payload() {
        let payload = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "pack".to_string()),
                ("kind".to_string(), "file".to_string()),
                ("entry_name".to_string(), "payload.txt".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"hello".to_vec())]),
        )
        .expect("archive pack file should succeed");

        let temp = tempdir().expect("tempdir");
        unpack_zip_bytes_to_directory(&payload, temp.path()).expect("unpack payload");
        assert_eq!(std::fs::read(temp.path().join("payload.txt")).ok(), Some(b"hello".to_vec()));
    }

    /// Verifies folder pack accepts folder ZIP payload input and preserves files.
    #[test]
    fn execute_pack_folder_accepts_folder_zip_payload() {
        let temp = tempdir().expect("tempdir");
        let source = temp.path().join("folder");
        std::fs::create_dir_all(source.join("nested")).expect("create source folder");
        std::fs::write(source.join("nested").join("a.txt"), b"A").expect("write source file");

        let folder_payload =
            pack_directory_to_uncompressed_zip_bytes(&source, false).expect("pack source folder");

        let packed = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "pack".to_string()),
                ("kind".to_string(), "folder".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), folder_payload)]),
        )
        .expect("archive pack folder should succeed");

        let restored = temp.path().join("restored");
        unpack_zip_bytes_to_directory(&packed, &restored).expect("unpack packed folder payload");
        assert!(restored.join("nested").join("a.txt").exists());
    }

    /// Verifies unpack converts archive payload to folder ZIP payload bytes.
    #[test]
    fn execute_unpack_returns_folder_zip_payload() {
        let packed = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "pack".to_string()),
                ("kind".to_string(), "file".to_string()),
                ("entry_name".to_string(), "single.txt".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"content".to_vec())]),
        )
        .expect("archive pack file should succeed");

        let unpacked = execute_content_map(
            &BTreeMap::from([("action".to_string(), "unpack".to_string())]),
            &BinaryInputMap::from([("archive".to_string(), packed)]),
        )
        .expect("archive unpack should succeed");

        let temp = tempdir().expect("tempdir");
        unpack_zip_bytes_to_directory(&unpacked, temp.path()).expect("unpack folder payload");
        assert!(temp.path().join("single.txt").exists());
    }

    /// Verifies descriptor serialization keeps the stable builtin identifier.
    #[test]
    fn descriptor_json_contains_tool_id() {
        let json = describe_json().expect("descriptor serialization should succeed");
        assert!(json.contains("mediapm.builtin.archive@1.0.0"));
    }
}
