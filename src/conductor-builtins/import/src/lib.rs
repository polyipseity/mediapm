//! Import-operation builtin runtime crate.
//!
//! This crate is intentionally standalone:
//! - it exposes API contracts and execution helpers for conductor dispatch, and
//! - it can run independently via its binary target (`src/main.rs`).
//!
//! `import` is an impure source-ingestion builtin with one `kind` selector:
//! - `kind=file`: read one file and emit its bytes,
//! - `kind=folder`: read one directory and emit folder payload bytes as
//!   uncompressed ZIP,
//! - `kind=fetch`: download URL bytes with integrity pinning and emit bytes.
//! - `kind=cas_hash`: resolve one CAS hash and emit referenced bytes.
//!
//! For `kind=fetch`, destination file paths are not accepted; output bytes are
//! returned directly.

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Read;
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use clap::{ArgAction, Parser};
use ureq::Error as UreqError;

/// Stable builtin id used by topology registration.
pub const TOOL_ID: &str = "builtins.import@1.0.0";

/// Builtin process name used by conductor process dispatch.
pub const TOOL_NAME: &str = "import";

/// Canonical semantic version handled by this runtime.
pub const TOOL_VERSION: &str = "1.0.0";

/// Builtin purity marker.
pub const IS_IMPURE: bool = true;

/// Canonical string-map payload used by both API and CLI contracts.
pub type StringMap = BTreeMap<String, String>;

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
    /// `import` does not currently accept `inputs`; this transport is rejected.
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
            "import builtin that ingests file/folder/fetch/cas_hash sources into pure bytes"
                .to_string(),
        ),
    ])
}

/// Serializes [`describe`] for CLI output.
///
/// # Errors
///
/// Returns a serialization error when the descriptor map cannot be rendered
/// as valid JSON.
pub fn describe_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&describe())
}

/// Executes one import request and returns imported payload bytes.
///
/// Supported params:
/// - `kind=file` with `path=<source path>` and optional
///   `path_mode=relative|absolute`,
/// - `kind=folder` with `path=<source path>` and optional
///   `path_mode=relative|absolute`,
/// - `kind=fetch` with `url` + `expected_hash`.
/// - `kind=cas_hash` with `hash=<blake3:...>`.
///
/// Path-mode semantics for `kind=file|folder`:
/// - default `path_mode` is `relative`,
/// - `relative` resolves `path` under `import_root_dir` and rejects traversal
///   outside that root,
/// - `absolute` requires `path` to be absolute.
///
/// `kind=cas_hash` requires the caller to use
/// [`execute_content_map_with_hash_resolver`] so hash lookups can be provided
/// by runtime context (for example conductor CAS state).
///
/// # Errors
///
/// Returns an error when arguments are invalid, path-mode/path-resolution
/// checks fail, source payload loading fails, URL fetch/integrity checks fail,
/// or `cas_hash` is requested without a resolver.
pub fn execute_content_map(
    import_root_dir: &Path,
    params: &StringMap,
    inputs: &StringMap,
) -> Result<Vec<u8>, String> {
    execute_content_map_internal::<fn(&str) -> Result<Vec<u8>, String>>(
        import_root_dir,
        params,
        inputs,
        None,
    )
}

/// Executes one import request with a hash resolver for `kind=cas_hash`.
///
/// The resolver is called only when `kind=cas_hash` is selected and receives
/// the exact `hash` argument value.
///
/// # Errors
///
/// Returns an error when arguments are invalid, path-mode/path-resolution
/// checks fail, source payload loading fails, URL fetch/integrity checks fail,
/// or the resolver reports an unknown/invalid hash payload.
pub fn execute_content_map_with_hash_resolver<F>(
    import_root_dir: &Path,
    params: &StringMap,
    inputs: &StringMap,
    hash_resolver: F,
) -> Result<Vec<u8>, String>
where
    F: FnMut(&str) -> Result<Vec<u8>, String>,
{
    execute_content_map_internal(import_root_dir, params, inputs, Some(hash_resolver))
}

/// Executes one import request and optionally resolves CAS hashes.
fn execute_content_map_internal<F>(
    import_root_dir: &Path,
    params: &StringMap,
    inputs: &StringMap,
    mut hash_resolver: Option<F>,
) -> Result<Vec<u8>, String>
where
    F: FnMut(&str) -> Result<Vec<u8>, String>,
{
    validate_argument_contract(params, inputs)?;

    let kind = params.get("kind").ok_or_else(|| "import requires 'kind'".to_string())?;

    match kind.as_str() {
        "file" => {
            let source_path = resolve_file_or_folder_source(import_root_dir, params)?;
            if !source_path.is_file() {
                return Err(format!(
                    "import kind='file' source '{}' is not a regular file",
                    source_path.display()
                ));
            }
            std::fs::read(&source_path).map_err(|err| {
                format!("reading source file '{}' failed: {err}", source_path.display())
            })
        }
        "folder" => {
            let source_path = resolve_file_or_folder_source(import_root_dir, params)?;
            if !source_path.is_dir() {
                return Err(format!(
                    "import kind='folder' source '{}' is not a directory",
                    source_path.display()
                ));
            }
            mediapm_conductor_builtin_archive::pack_directory_to_uncompressed_zip_bytes(
                &source_path,
                false,
            )
        }
        "fetch" => execute_fetch(params),
        "cas_hash" => {
            let hash_text = params
                .get("hash")
                .ok_or_else(|| "import kind='cas_hash' requires 'hash'".to_string())?;
            let resolver = hash_resolver.as_mut().ok_or_else(|| {
                "import kind='cas_hash' requires caller-provided hash resolver support".to_string()
            })?;
            resolver(hash_text)
        }
        other => Err(format!("unsupported import kind '{other}'")),
    }
}

/// Path-resolution mode for `kind=file|folder` import operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathMode {
    /// Resolve `path` under the import root directory.
    Relative,
    /// Treat `path` as an explicit absolute host path.
    Absolute,
}

/// Runs the standalone CLI command using a normal clap-parsed option structure.
///
/// Successful execution writes imported payload bytes directly to stdout.
///
/// # Errors
///
/// Returns an error when CLI key/value pairs are malformed, import execution
/// fails, descriptor serialization fails, or writing output to the provided
/// writer fails.
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
    let inputs = parse_string_pairs(&cli.inputs, "inputs").map_err(std::io::Error::other)?;
    let payload =
        execute_content_map(&tool_cwd, &params, &inputs).map_err(std::io::Error::other)?;
    writer.write_all(&payload)?;
    Ok(())
}

/// Performs URL fetch with strict integrity pinning.
fn execute_fetch(params: &StringMap) -> Result<Vec<u8>, String> {
    let url = params.get("url").ok_or_else(|| "import kind='fetch' requires 'url'".to_string())?;
    let expected_hash = params
        .get("expected_hash")
        .ok_or_else(|| "import kind='fetch' requires 'expected_hash'".to_string())?;

    let parsed_url = url::Url::parse(url)
        .map_err(|err| format!("import fetch requires valid URL in 'url': {err}"))?;
    match parsed_url.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(format!(
                "import kind='fetch' only supports http/https URLs, got scheme '{scheme}'"
            ));
        }
    }

    if !is_valid_blake3_digest(expected_hash) {
        return Err(
            "import kind='fetch' requires 'expected_hash' in form 'blake3:<64 lowercase hex chars>'"
                .to_string(),
        );
    }

    let response = match ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .get(url)
        .call()
    {
        Ok(response) => response,
        Err(UreqError::Status(code, response)) => {
            return Err(format!(
                "import fetch got non-OK status: {code} {}",
                response.status_text()
            ));
        }
        Err(UreqError::Transport(error)) => {
            return Err(format!("import fetch request failed: {error}"));
        }
    };

    let mut reader = response.into_reader();
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|err| format!("reading import fetch response failed: {err}"))?;

    let actual_hash = blake3::hash(&bytes);
    let actual_digest = format!("blake3:{}", actual_hash.to_hex());
    if actual_digest != *expected_hash {
        return Err(format!(
            "hash mismatch for import fetch: expected {expected_hash}, got {actual_digest}"
        ));
    }

    Ok(bytes)
}

/// Resolves one file/folder import source path using configured path mode.
fn resolve_file_or_folder_source(
    import_root_dir: &Path,
    params: &StringMap,
) -> Result<PathBuf, String> {
    let kind = params.get("kind").map_or("file_or_folder", String::as_str);
    let path = params.get("path").ok_or_else(|| format!("import kind='{kind}' requires 'path'"))?;
    let mode = parse_path_mode(params, kind)?;
    resolve_path_for_import_root(import_root_dir, kind, path, mode)
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
        if map.insert(key.to_string(), value.clone()).is_some() {
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

/// Validates import args/inputs for required and recognized keys.
fn validate_argument_contract(params: &StringMap, inputs: &StringMap) -> Result<(), String> {
    if let Some(unexpected) = inputs.keys().next() {
        return Err(format!("import builtin does not accept input '{unexpected}'"));
    }

    let kind = params
        .get("kind")
        .ok_or_else(|| "import builtin requires 'kind' (file|folder|fetch|cas_hash)".to_string())?;

    match kind.as_str() {
        "file" | "folder" => {
            for key in params.keys() {
                if key != "kind" && key != "path" && key != "path_mode" {
                    return Err(format!("import kind='{kind}' does not accept arg '{key}'"));
                }
            }

            let path = params
                .get("path")
                .ok_or_else(|| format!("import kind='{kind}' requires 'path'"))?;
            if path.trim().is_empty() {
                return Err(format!("import kind='{kind}' requires non-empty 'path'"));
            }

            let _ = parse_path_mode(params, kind)?;
            Ok(())
        }
        "fetch" => {
            for key in params.keys() {
                if key != "kind" && key != "url" && key != "expected_hash" {
                    return Err(format!("import kind='fetch' does not accept arg '{key}'"));
                }
            }

            for required in ["url", "expected_hash"] {
                let Some(value) = params.get(required) else {
                    return Err(format!("import kind='fetch' requires '{required}'"));
                };
                if value.trim().is_empty() {
                    return Err(format!("import kind='fetch' requires non-empty '{required}'"));
                }
            }

            Ok(())
        }
        "cas_hash" => {
            for key in params.keys() {
                if key != "kind" && key != "hash" {
                    return Err(format!("import kind='cas_hash' does not accept arg '{key}'"));
                }
            }

            let Some(value) = params.get("hash") else {
                return Err("import kind='cas_hash' requires 'hash'".to_string());
            };
            if value.trim().is_empty() {
                return Err("import kind='cas_hash' requires non-empty 'hash'".to_string());
            }
            if !is_valid_blake3_digest(value) {
                return Err(
                    "import kind='cas_hash' requires 'hash' in form 'blake3:<64 lowercase hex chars>'"
                        .to_string(),
                );
            }

            Ok(())
        }
        other => {
            Err(format!("import builtin requires kind=file|folder|fetch|cas_hash, got '{other}'"))
        }
    }
}

/// Parses and validates path-mode selector for file/folder import kinds.
fn parse_path_mode(params: &StringMap, kind: &str) -> Result<PathMode, String> {
    match params.get("path_mode").map_or("relative", String::as_str) {
        "relative" => Ok(PathMode::Relative),
        "absolute" => Ok(PathMode::Absolute),
        other => Err(format!(
            "import kind='{kind}' path_mode must be 'relative' or 'absolute', got '{other}'"
        )),
    }
}

/// Resolves one source path using import root + path-mode semantics.
fn resolve_path_for_import_root(
    import_root_dir: &Path,
    kind: &str,
    candidate: &str,
    mode: PathMode,
) -> Result<PathBuf, String> {
    match mode {
        PathMode::Relative => {
            if Path::new(candidate).is_absolute() {
                return Err(format!(
                    "import kind='{kind}' with path_mode='relative' requires relative 'path'"
                ));
            }

            let import_root_absolute = absolute_root(import_root_dir)?;
            let normalized_relative = normalize_relative_path(candidate, "import source path")?;
            Ok(import_root_absolute.join(normalized_relative))
        }
        PathMode::Absolute => {
            let parsed = Path::new(candidate);
            if !parsed.is_absolute() {
                return Err(format!(
                    "import kind='{kind}' with path_mode='absolute' requires absolute 'path'"
                ));
            }
            Ok(parsed.to_path_buf())
        }
    }
}

/// Resolves one root directory into an absolute filesystem path.
fn absolute_root(root: &Path) -> Result<PathBuf, String> {
    if root.is_absolute() {
        return Ok(root.to_path_buf());
    }

    std::env::current_dir()
        .map(|cwd| cwd.join(root))
        .map_err(|err| format!("resolving current directory for import root failed: {err}"))
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
                return Err(format!("{context} must stay under import root directory"));
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err(format!("{context} must contain at least one path component"));
    }

    Ok(normalized)
}

/// Validates one expected BLAKE3 digest string shape.
fn is_valid_blake3_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("blake3:") else {
        return false;
    };
    hex.len() == 64
        && hex.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::{describe_json, execute_content_map, execute_content_map_with_hash_resolver};

    /// Verifies importing a local file by default relative mode returns bytes.
    #[test]
    fn execute_file_relative_path_returns_file_bytes() {
        let temp = tempdir().expect("tempdir");
        let source = temp.path().join("payload.txt");
        std::fs::write(&source, b"hello").expect("write payload");

        let request = BTreeMap::from([
            ("kind".to_string(), "file".to_string()),
            ("path".to_string(), "payload.txt".to_string()),
        ]);

        let response = execute_content_map(temp.path(), &request, &BTreeMap::new())
            .expect("import file should succeed");
        assert_eq!(response, b"hello");
    }

    /// Verifies importing a local folder returns uncompressed ZIP bytes.
    #[test]
    fn execute_folder_path_returns_zip_bytes() {
        let temp = tempdir().expect("tempdir");
        let source = temp.path().join("pack");
        std::fs::create_dir_all(source.join("nested")).expect("create source dir");
        std::fs::write(source.join("nested").join("a.txt"), b"abc").expect("write source file");

        let args = BTreeMap::from([
            ("kind".to_string(), "folder".to_string()),
            ("path".to_string(), "pack".to_string()),
        ]);
        let payload = execute_content_map(temp.path(), &args, &BTreeMap::new())
            .expect("directory import should succeed");

        let unpacked = temp.path().join("unzipped");
        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(&payload, &unpacked)
            .expect("unpack imported folder payload");
        assert!(unpacked.join("nested").join("a.txt").exists());
    }

    /// Verifies relative mode rejects absolute path values.
    #[test]
    fn execute_relative_mode_rejects_absolute_path() {
        let temp = tempdir().expect("tempdir");
        let absolute_path = temp.path().join("payload.txt");
        std::fs::write(&absolute_path, b"hello").expect("write payload");

        let err = execute_content_map(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "file".to_string()),
                ("path".to_string(), absolute_path.to_string_lossy().to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect_err("relative mode should reject absolute path");

        assert!(err.contains("path_mode='relative'"));
    }

    /// Verifies absolute mode accepts explicit absolute source paths.
    #[test]
    fn execute_absolute_mode_accepts_absolute_path() {
        let temp = tempdir().expect("tempdir");
        let absolute_path = temp.path().join("payload.txt");
        std::fs::write(&absolute_path, b"hello").expect("write payload");

        let payload = execute_content_map(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "file".to_string()),
                ("path_mode".to_string(), "absolute".to_string()),
                ("path".to_string(), absolute_path.to_string_lossy().to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect("absolute mode should accept absolute path");

        assert_eq!(payload, b"hello");
    }

    /// Verifies relative mode rejects escaping parent traversal.
    #[test]
    fn execute_relative_mode_rejects_parent_escape() {
        let temp = tempdir().expect("tempdir");

        let err = execute_content_map(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "file".to_string()),
                ("path".to_string(), "../outside.txt".to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect_err("relative mode should reject parent traversal");

        assert!(err.contains("must stay under import root directory"));
    }

    /// Verifies fetch args reject removed destination-path option.
    #[test]
    fn execute_fetch_rejects_dest_path_arg() {
        let temp = tempdir().expect("tempdir");
        let err = execute_content_map(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "fetch".to_string()),
                ("url".to_string(), "https://example.com/".to_string()),
                (
                    "expected_hash".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                ),
                ("dest_path".to_string(), "out.bin".to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect_err("dest_path should be rejected for fetch");

        assert!(err.contains("does not accept arg 'dest_path'"));
    }

    /// Verifies `cas_hash` mode resolves payload bytes via caller hash loader.
    #[test]
    fn execute_cas_hash_uses_hash_resolver_payload() {
        let temp = tempdir().expect("tempdir");
        let hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let payload = execute_content_map_with_hash_resolver(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "cas_hash".to_string()),
                ("hash".to_string(), hash.to_string()),
            ]),
            &BTreeMap::new(),
            |requested| {
                if requested == hash {
                    Ok(b"payload-from-cas".to_vec())
                } else {
                    Err("unexpected hash requested".to_string())
                }
            },
        )
        .expect("cas_hash import should succeed");

        assert_eq!(payload, b"payload-from-cas");
    }

    /// Verifies `cas_hash` mode fails fast when no hash resolver is available.
    #[test]
    fn execute_cas_hash_rejects_missing_hash_resolver() {
        let temp = tempdir().expect("tempdir");
        let hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let err = execute_content_map(
            temp.path(),
            &BTreeMap::from([
                ("kind".to_string(), "cas_hash".to_string()),
                ("hash".to_string(), hash.to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect_err("cas_hash mode should require runtime hash resolver");

        assert!(err.contains("requires caller-provided hash resolver support"));
    }

    /// Verifies descriptor serialization keeps the stable builtin identifier.
    #[test]
    fn descriptor_json_contains_tool_id() {
        let json = describe_json().expect("descriptor serialization should succeed");
        assert!(json.contains("builtins.import@1.0.0"));
    }
}
