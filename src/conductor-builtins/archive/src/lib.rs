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

#[cfg(feature = "cli")]
use std::error::Error;
use std::io::Write;
use std::path::Path;
#[cfg(feature = "cli")]
use std::path::PathBuf;

use regex::{Regex as TextRegex, bytes::Regex as BytesRegex};

#[cfg(feature = "cli")]
pub use mediapm_utils::builtin::BuiltinCliArgs;
#[cfg(feature = "cli")]
use mediapm_utils::builtin::describe_json_compact_meta;
#[cfg(feature = "cli")]
use mediapm_utils::builtin::parse_string_pairs;
pub use mediapm_utils::{
    BinaryInputMap, StringMap,
    builtin::{BuiltinMeta, describe_meta, validate_only_known_keys},
};

/// Stable builtin id used by topology registration.
pub const TOOL_ID: &str = META.tool_id;

/// Builtin process name used by conductor process dispatch.
pub const TOOL_NAME: &str = META.tool_name;

/// Canonical semantic version handled by this runtime.
pub const TOOL_VERSION: &str = META.tool_version;

/// Builtin purity marker.
pub const IS_IMPURE: bool = META.is_impure;

/// Metadata for this builtin crate.
pub const META: BuiltinMeta = BuiltinMeta {
    tool_id: "builtins.archive@1.0.0",
    tool_name: "archive",
    tool_version: "1.0.0",
    is_impure: false,
    summary: "pure archive builtin runtime transforming bytes to bytes",
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

/// Executes one archive request and returns transformed bytes.
///
/// Supported params:
/// - `action=pack` with `kind=file|folder` and corresponding input `content`,
/// - `action=unpack` with input `archive`,
/// - `action=repack` with input `archive`,
/// - `action=transform` with input `content`.
///
/// Optional params:
/// - `entry_name` when `action=pack` and `kind=file`.
/// - `mode`, `filter`, `find_<N>`, `replace_<N>` when `action=transform`.
///
/// # Errors
///
/// Returns an error when required keys are missing, unknown keys are present,
/// action/kind values are invalid, or payload transformation fails.
pub fn execute_content_map(params: &StringMap, inputs: &BinaryInputMap) -> Result<Vec<u8>, String> {
    validate_argument_contract(params, inputs)?;

    let action = params.get("action").ok_or_else(|| {
        "archive builtin requires 'action' (pack|unpack|repack|transform)".to_string()
    })?;

    match action.as_str() {
        "pack" => {
            let kind = params
                .get("kind")
                .ok_or_else(|| "archive pack requires 'kind' (file|folder)".to_string())?;
            let payload = inputs
                .get("content")
                .ok_or_else(|| "archive pack requires input 'content'".to_string())?;

            match kind.as_str() {
                "file" => {
                    let entry_name = params.get("entry_name").map_or("content.bin", String::as_str);
                    pack_single_file_to_uncompressed_zip_bytes(payload, entry_name)
                }
                "folder" => normalize_archive_zip_bytes_to_folder_zip_bytes(payload),
                other => {
                    Err(format!("archive pack kind must be 'file' or 'folder', got '{other}'"))
                }
            }
        }
        "unpack" => {
            let archive_payload = inputs
                .get("archive")
                .ok_or_else(|| "archive unpack requires input 'archive'".to_string())?;
            normalize_archive_zip_bytes_to_folder_zip_bytes(archive_payload)
        }
        "repack" => {
            let archive_payload = inputs
                .get("archive")
                .ok_or_else(|| "archive repack requires input 'archive'".to_string())?;
            normalize_archive_zip_bytes_to_folder_zip_bytes(archive_payload)
        }
        "transform" => {
            let zip_payload = inputs
                .get("content")
                .ok_or_else(|| "archive transform requires input 'content'".to_string())?;
            transform_zip_bytes(zip_payload, params)
        }
        other => Err(format!("unsupported archive action '{other}'")),
    }
}

/// Runs the standalone CLI command using a normal clap-parsed option structure.
///
/// CLI `--input` values are UTF-8 and are converted to bytes before API
/// execution.
///
/// # Errors
///
/// Returns an error when CLI pair parsing fails, descriptor JSON writing fails,
/// archive execution fails, or output writing fails.
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
///
/// # Errors
///
/// Returns an error when the source directory is missing/invalid, directory
/// enumeration fails, ZIP entry writes fail, or ZIP finalization fails.
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

    if let Some(root_name) = &source_dir_name {
        let mut root_entry = root_name.clone();
        if !root_entry.ends_with('/') {
            root_entry.push('/');
        }
        writer
            .add_directory(root_entry, options)
            .map_err(|err| format!("adding root directory to zip failed: {err}"))?;
    }

    pack_directory_entries_recursively(
        &mut writer,
        source_dir,
        source_dir,
        source_dir_name.as_deref(),
        options,
    )?;

    writer
        .finish()
        .map_err(|err| format!("finalizing zip payload failed: {err}"))
        .map(std::io::Cursor::into_inner)
}

/// Packs one directory recursively into an existing ZIP writer.
///
/// Entries are traversed in sorted lexical order for deterministic output.
fn pack_directory_entries_recursively(
    writer: &mut zip::ZipWriter<std::io::Cursor<Vec<u8>>>,
    root_dir: &Path,
    current_dir: &Path,
    source_dir_name: Option<&str>,
    options: zip::write::SimpleFileOptions,
) -> Result<(), String> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(current_dir).map_err(|err| {
        format!("reading source directory '{}' failed: {err}", current_dir.display())
    })? {
        entries.push(entry.map_err(|err| format!("reading source directory entry failed: {err}"))?);
    }
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        let relative =
            path.strip_prefix(root_dir).map_err(|err| format!("strip prefix failed: {err}"))?;
        let mut name = relative.to_string_lossy().replace('\\', "/");
        if let Some(root_name) = source_dir_name {
            name = format!("{root_name}/{name}");
        }

        let file_type = entry
            .file_type()
            .map_err(|err| format!("reading file type for '{}' failed: {err}", path.display()))?;

        if file_type.is_dir() {
            if !name.ends_with('/') {
                name.push('/');
            }
            writer
                .add_directory(name, options)
                .map_err(|err| format!("adding directory to zip failed: {err}"))?;
            pack_directory_entries_recursively(writer, root_dir, &path, source_dir_name, options)?;
            continue;
        }

        writer
            .start_file(name, options)
            .map_err(|err| format!("starting zip file entry failed: {err}"))?;

        let mut source = std::fs::File::open(&path)
            .map_err(|err| format!("opening source file '{}' failed: {err}", path.display()))?;
        std::io::copy(&mut source, writer)
            .map_err(|err| format!("writing zip file entry failed: {err}"))?;
    }

    Ok(())
}

/// Unpacks ZIP payload bytes into one destination directory.
///
/// The implementation rejects escaping ZIP entries (`../`) through
/// `enclosed_name` checks.
/// Lists all file member paths inside one ZIP payload.
///
/// Only file entries are returned; directory entries are skipped.  Each
/// returned path is the portable (forward-slash) relative path of the member
/// as it appears inside the archive.
///
/// This is used by the conductor step worker to enumerate ZIP member paths
/// before extraction so it can perform collision detection across multiple
/// `tool_content_map` entries that target overlapping sandbox paths.
///
/// # Errors
///
/// Returns an error when ZIP decoding fails or a member has an unsafe
/// (path-escaping) name.
pub fn list_zip_member_file_paths(zip_bytes: &[u8]) -> Result<Vec<String>, String> {
    let cursor = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|err| format!("reading zip archive bytes failed: {err}"))?;
    let mut paths = Vec::new();
    for index in 0..archive.len() {
        let entry =
            archive.by_index(index).map_err(|err| format!("reading zip entry failed: {err}"))?;
        if entry.name().ends_with('/') {
            continue;
        }
        let enclosed = entry.enclosed_name().ok_or_else(|| {
            format!("unsafe zip entry name '{}', escaping destination", entry.name())
        })?;
        paths.push(enclosed.to_string_lossy().into_owned());
    }
    Ok(paths)
}

///
/// # Errors
///
/// Returns an error when destination creation fails, ZIP decoding fails,
/// entries attempt path escape, or extracted file writes fail.
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
///
/// # Errors
///
/// Returns an error when archive decoding fails, one entry escapes the
/// destination namespace, or canonical repacking fails.
pub fn normalize_archive_zip_bytes_to_folder_zip_bytes(
    archive_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    let reader = std::io::Cursor::new(archive_bytes);
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|err| format!("reading zip archive bytes failed: {err}"))?;
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::<u8>::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o644);

    for index in 0..archive.len() {
        let mut entry =
            archive.by_index(index).map_err(|err| format!("reading zip entry failed: {err}"))?;
        let enclosed = entry.enclosed_name().ok_or_else(|| {
            format!("unsafe zip entry name '{}', escaping destination", entry.name())
        })?;
        let mut normalized_name = enclosed.to_string_lossy().replace('\\', "/");

        if entry.name().ends_with('/') {
            if !normalized_name.ends_with('/') {
                normalized_name.push('/');
            }
            writer
                .add_directory(normalized_name, options)
                .map_err(|err| format!("adding directory to zip failed: {err}"))?;
            continue;
        }

        writer
            .start_file(normalized_name, options)
            .map_err(|err| format!("starting zip file entry failed: {err}"))?;
        std::io::copy(&mut entry, &mut writer)
            .map_err(|err| format!("writing zip file entry failed: {err}"))?;
    }

    writer
        .finish()
        .map_err(|err| format!("finalizing zip payload failed: {err}"))
        .map(std::io::Cursor::into_inner)
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
        .map(std::io::Cursor::into_inner)
}

/// Validates archive args/inputs for required and recognized keys.
fn validate_argument_contract(params: &StringMap, inputs: &BinaryInputMap) -> Result<(), String> {
    let action = params.get("action").ok_or_else(|| {
        "archive builtin requires 'action' (pack|unpack|repack|transform)".to_string()
    })?;

    // Numbered transform keys are only valid for transform.
    for key in params.keys() {
        if is_numbered_transform_key(key) && action != "transform" {
            return Err(format!("archive action '{action}' does not accept arg '{key}'"));
        }
    }

    match action.as_str() {
        "pack" => {
            validate_only_known_keys(params, &["action", "kind", "entry_name"], "archive pack")?;
            validate_only_known_keys(inputs, &["content"], "archive pack")?;

            if inputs.get("content").is_none() {
                return Err("archive pack requires input 'content'".to_string());
            }

            let kind = params
                .get("kind")
                .ok_or_else(|| "archive pack requires 'kind' (file|folder)".to_string())?;
            if kind != "file" && kind != "folder" {
                return Err(format!("archive pack kind must be 'file' or 'folder', got '{kind}'"));
            }

            Ok(())
        }
        "unpack" | "repack" => {
            validate_only_known_keys(params, &["action"], "archive unpack")?;
            validate_only_known_keys(inputs, &["archive"], "archive unpack")?;

            if inputs.get("archive").is_none() {
                return Err(format!("archive action '{action}' requires input 'archive'"));
            }

            Ok(())
        }
        "transform" => {
            // Check static params (numbered transform keys are checked per-action above).
            for key in params.keys() {
                if is_numbered_transform_key(key) {
                    continue;
                }
                if !["action", "mode", "filter"].contains(&key.as_str()) {
                    return Err(format!("archive transform does not accept arg '{key}'"));
                }
            }
            validate_only_known_keys(inputs, &["content"], "archive transform")?;

            if inputs.get("content").is_none() {
                return Err("archive transform requires input 'content'".to_string());
            }

            if let Some(mode) = params.get("mode") {
                if mode != "text" && mode != "binary" {
                    return Err(format!(
                        "archive transform mode must be 'text' or 'binary', got '{mode}'"
                    ));
                }
            }

            validate_numbered_transforms(params)?;

            Ok(())
        }
        other => Err(format!(
            "archive action must be 'pack', 'unpack', 'repack', or 'transform', got '{other}'"
        )),
    }
}

/// Checks whether a parameter key has the form `find_<N>`, `replace_<N>`,
/// `mode_<N>`, or `filter_<N>` where N is a non-negative integer.
fn is_numbered_transform_key(key: &str) -> bool {
    for prefix in ["find_", "replace_", "mode_", "filter_"] {
        if let Some(suffix) = key.strip_prefix(prefix) {
            return !suffix.is_empty() && suffix.parse::<u64>().is_ok();
        }
    }
    false
}

/// Validates numbered transform parameters: contiguous from 0, paired, valid modes.
fn validate_numbered_transforms(params: &StringMap) -> Result<(), String> {
    let mut max_n: Option<usize> = None;
    for key in params.keys() {
        for prefix in ["find_", "replace_", "mode_", "filter_"] {
            if let Some(suffix) = key.strip_prefix(prefix) {
                let n: usize = suffix.parse().map_err(|_| {
                    format!("invalid numbered transform key '{key}': suffix is not a valid integer")
                })?;
                max_n = Some(max_n.map_or(n, |m| m.max(n)));
                break;
            }
        }
    }

    let Some(max_n) = max_n else {
        return Ok(());
    };

    for n in 0..=max_n {
        let find_key = format!("find_{n}");
        let replace_key = format!("replace_{n}");
        let has_find = params.contains_key(&find_key);
        let has_replace = params.contains_key(&replace_key);

        if !has_find && !has_replace {
            return Err(format!(
                "transform indices must be contiguous; missing index {n} (found higher indices)"
            ));
        }
        if has_find && !has_replace {
            return Err(format!("transform {n} has '{find_key}' but no '{replace_key}'"));
        }
        if !has_find && has_replace {
            return Err(format!("transform {n} has '{replace_key}' but no '{find_key}'"));
        }

        if let Some(mode) = params.get(&format!("mode_{n}")) {
            if mode != "text" && mode != "binary" {
                return Err(format!("transform {n} mode must be 'text' or 'binary', got '{mode}'"));
            }
        }
    }

    Ok(())
}

/// A single numbered find/replace transform with optional per-transform overrides.
struct Transform {
    find: String,
    replace: String,
    mode: Option<String>,
    filter: Option<String>,
}

/// Parses numbered `find_<N>`/`replace_<N>` pairs from params in order.
fn parse_numbered_transforms(params: &StringMap) -> Result<Vec<Transform>, String> {
    let mut transforms = Vec::new();
    let mut n = 0usize;
    loop {
        let find_key = format!("find_{n}");
        let replace_key = format!("replace_{n}");

        match (params.get(&find_key), params.get(&replace_key)) {
            (Some(find), Some(replace)) => {
                transforms.push(Transform {
                    find: find.clone(),
                    replace: replace.clone(),
                    mode: params.get(&format!("mode_{n}")).cloned(),
                    filter: params.get(&format!("filter_{n}")).cloned(),
                });
            }
            (None, None) => break,
            (Some(_), None) => {
                return Err(format!("transform {n} has '{find_key}' but no '{replace_key}'"));
            }
            (None, Some(_)) => {
                return Err(format!("transform {n} has '{replace_key}' but no '{find_key}'"));
            }
        }
        n += 1;
    }
    Ok(transforms)
}

/// Converts a glob pattern (supporting `*` and `?` excluding `/`) to a regex.
fn glob_to_regex(pattern: &str) -> Result<TextRegex, String> {
    let mut regex_str = String::with_capacity(pattern.len() + 4);
    regex_str.push('^');
    for c in pattern.chars() {
        match c {
            '*' => regex_str.push_str("[^/]*"),
            '?' => regex_str.push_str("[^/]"),
            '.' | '+' | '^' | '$' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '\\' => {
                regex_str.push('\\');
                regex_str.push(c);
            }
            _ => regex_str.push(c),
        }
    }
    regex_str.push('$');
    TextRegex::new(&regex_str).map_err(|err| format!("invalid glob pattern '{pattern}': {err}"))
}

/// Applies all matching numbered transforms to entry content.
fn apply_transforms_to_content(
    content: &[u8],
    entry_name: &str,
    transforms: &[Transform],
    global_mode: &str,
    global_filter: &str,
) -> Result<Vec<u8>, String> {
    let mut result = content.to_vec();
    for (i, transform) in transforms.iter().enumerate() {
        let mode = transform.mode.as_deref().unwrap_or(global_mode);
        let filter = transform.filter.as_deref().unwrap_or(global_filter);

        let glob_re = glob_to_regex(filter)?;
        if !glob_re.is_match(entry_name) {
            continue;
        }

        match mode {
            "text" => {
                let text = std::str::from_utf8(&result).map_err(|err| {
                    format!(
                        "entry '{entry_name}' content is not valid UTF-8 for text mode transform {i}: {err}"
                    )
                })?;
                let re = TextRegex::new(&transform.find)
                    .map_err(|err| format!("invalid regex for transform {i}: {err}"))?;
                let replaced = re.replace_all(text, &transform.replace);
                result = replaced.as_bytes().to_vec();
            }
            "binary" => {
                let re = BytesRegex::new(&transform.find)
                    .map_err(|err| format!("invalid binary regex for transform {i}: {err}"))?;
                let replaced = re.replace_all(&result, transform.replace.as_bytes());
                result = replaced.to_vec();
            }
            other => {
                return Err(format!("transform mode must be 'text' or 'binary', got '{other}'"));
            }
        }
    }
    Ok(result)
}

/// Applies numbered find/replace transforms to ZIP entry content.
///
/// Returns a new uncompressed ZIP with transformed entry content.
/// Directory entries are preserved as-is.
fn transform_zip_bytes(zip_bytes: &[u8], params: &StringMap) -> Result<Vec<u8>, String> {
    let transforms = parse_numbered_transforms(params)?;
    let global_mode = params.get("mode").map_or("text", String::as_str);
    let global_filter = params.get("filter").map_or("*", String::as_str);

    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|err| format!("reading zip archive bytes failed: {err}"))?;
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::<u8>::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o644);

    for index in 0..archive.len() {
        let mut entry =
            archive.by_index(index).map_err(|err| format!("reading zip entry failed: {err}"))?;
        let enclosed = entry.enclosed_name().ok_or_else(|| {
            format!("unsafe zip entry name '{}', escaping destination", entry.name())
        })?;
        let normalized_name = enclosed.to_string_lossy().replace('\\', "/");

        if entry.name().ends_with('/') {
            let mut dir_name = normalized_name.clone();
            if !dir_name.ends_with('/') {
                dir_name.push('/');
            }
            writer
                .add_directory(dir_name, options)
                .map_err(|err| format!("adding directory to zip failed: {err}"))?;
            continue;
        }

        let mut content = Vec::new();
        std::io::copy(&mut entry, &mut content)
            .map_err(|err| format!("reading zip entry content failed: {err}"))?;

        let transformed = apply_transforms_to_content(
            &content,
            &normalized_name,
            &transforms,
            global_mode,
            global_filter,
        )?;

        writer
            .start_file(normalized_name, options)
            .map_err(|err| format!("starting zip file entry failed: {err}"))?;
        writer
            .write_all(&transformed)
            .map_err(|err| format!("writing zip file entry failed: {err}"))?;
    }

    writer
        .finish()
        .map_err(|err| format!("finalizing zip payload failed: {err}"))
        .map(std::io::Cursor::into_inner)
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
        let json = describe_json();
        assert!(json.contains("builtins.archive@1.0.0"));
    }

    /// Verifies text-mode transform replaces patterns in ZIP entries.
    #[test]
    fn execute_transform_strips_text() {
        let inner_payload = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "pack".to_string()),
                ("kind".to_string(), "file".to_string()),
                ("entry_name".to_string(), "test.txt".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"hello __mediapm__ world".to_vec())]),
        )
        .expect("pack inner should succeed");

        let result = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "transform".to_string()),
                ("find_0".to_string(), "__mediapm__".to_string()),
                ("replace_0".to_string(), "".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), inner_payload)]),
        )
        .expect("transform should succeed");

        let temp = tempdir().expect("tempdir");
        unpack_zip_bytes_to_directory(&result, temp.path()).expect("unpack");
        let content = std::fs::read_to_string(temp.path().join("test.txt")).expect("read");
        assert_eq!(content, "hello  world");
    }

    /// Verifies transform with no find/replace pairs returns ZIP unchanged.
    #[test]
    fn execute_transform_passthrough_no_transforms() {
        let inner_payload = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "pack".to_string()),
                ("kind".to_string(), "file".to_string()),
                ("entry_name".to_string(), "data.bin".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"original".to_vec())]),
        )
        .expect("pack inner should succeed");

        let result = execute_content_map(
            &BTreeMap::from([("action".to_string(), "transform".to_string())]),
            &BinaryInputMap::from([("content".to_string(), inner_payload)]),
        )
        .expect("transform with no transforms should succeed");

        let temp = tempdir().expect("tempdir");
        unpack_zip_bytes_to_directory(&result, temp.path()).expect("unpack");
        let content = std::fs::read_to_string(temp.path().join("data.bin")).expect("read");
        assert_eq!(content, "original");
    }

    /// Verifies filter pattern only transforms matching ZIP entries.
    #[test]
    fn execute_transform_filter_selects_entries() {
        let temp = tempdir().expect("tempdir");
        std::fs::write(temp.path().join("keep.txt"), b"hello __mediapm__ world")
            .expect("write keep");
        std::fs::write(temp.path().join("skip.dat"), b"other __mediapm__ data")
            .expect("write skip");

        let folder_payload =
            pack_directory_to_uncompressed_zip_bytes(temp.path(), false).expect("pack folder");

        let result = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "transform".to_string()),
                ("find_0".to_string(), "__mediapm__".to_string()),
                ("replace_0".to_string(), "".to_string()),
                ("filter".to_string(), "*.txt".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), folder_payload)]),
        )
        .expect("transform should succeed");

        let out = tempdir().expect("tempdir");
        unpack_zip_bytes_to_directory(&result, out.path()).expect("unpack");
        assert_eq!(
            std::fs::read_to_string(out.path().join("keep.txt")).ok(),
            Some("hello  world".to_string()),
        );
        assert_eq!(
            std::fs::read_to_string(out.path().join("skip.dat")).ok(),
            Some("other __mediapm__ data".to_string()),
        );
    }

    /// Verifies binary-mode transform replaces raw byte patterns.
    #[test]
    fn execute_transform_binary_mode() {
        let content = b"\x00\x01\x00\x02".to_vec();
        let folder_payload = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "pack".to_string()),
                ("kind".to_string(), "file".to_string()),
                ("entry_name".to_string(), "data.bin".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), content)]),
        )
        .expect("pack inner should succeed");

        let result = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "transform".to_string()),
                ("mode".to_string(), "binary".to_string()),
                ("find_0".to_string(), "\x00".to_string()),
                ("replace_0".to_string(), "\x01".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), folder_payload)]),
        )
        .expect("transform binary should succeed");

        let temp = tempdir().expect("tempdir");
        unpack_zip_bytes_to_directory(&result, temp.path()).expect("unpack");
        assert_eq!(
            std::fs::read(temp.path().join("data.bin")).ok(),
            Some(b"\x01\x01\x01\x02".to_vec()),
        );
    }

    /// Verifies non-contiguous transform numbering is rejected.
    #[test]
    fn execute_transform_rejects_non_contiguous() {
        let inner_payload = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "pack".to_string()),
                ("kind".to_string(), "file".to_string()),
                ("entry_name".to_string(), "dummy.txt".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"content".to_vec())]),
        )
        .expect("pack inner should succeed");

        let result = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "transform".to_string()),
                ("find_0".to_string(), "a".to_string()),
                ("replace_0".to_string(), "b".to_string()),
                ("find_2".to_string(), "c".to_string()),
                ("replace_2".to_string(), "d".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), inner_payload)]),
        );
        assert!(result.is_err(), "should reject non-contiguous numbering");
    }

    /// Verifies unpaired find/replace at same index is rejected.
    #[test]
    fn execute_transform_rejects_unpaired() {
        let inner_payload = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "pack".to_string()),
                ("kind".to_string(), "file".to_string()),
                ("entry_name".to_string(), "dummy.txt".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), b"content".to_vec())]),
        )
        .expect("pack inner should succeed");

        let result = execute_content_map(
            &BTreeMap::from([
                ("action".to_string(), "transform".to_string()),
                ("find_0".to_string(), "a".to_string()),
            ]),
            &BinaryInputMap::from([("content".to_string(), inner_payload)]),
        );
        assert!(result.is_err(), "should reject unpaired transform");
    }
}
