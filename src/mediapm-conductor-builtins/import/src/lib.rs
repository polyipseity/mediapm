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

use mediapm_utils::StringMap;
#[cfg(feature = "cli")]
pub use mediapm_utils::builtin::BuiltinCliArgs;
#[cfg(feature = "cli")]
use mediapm_utils::builtin::describe_json_compact_meta;
#[cfg(feature = "cli")]
use mediapm_utils::builtin::parse_string_pairs;
use mediapm_utils::builtin::{
    BuiltinMeta, describe_meta, require_non_empty_param, validate_only_known_keys,
};
#[cfg(feature = "cli")]
use std::error::Error;
#[cfg(feature = "cli")]
use std::io::Write;
use std::path::{Path, PathBuf};

#[cfg(feature = "fetch")]
use reqwest::blocking::Client;

/// Stable builtin id used by topology registration.
pub const TOOL_ID: &str = META.tool_id;

/// Versioned builtin identifier (e.g. "import@1.0.0") used in config map keys and dispatch.
pub const TOOL_BUILTIN_ID: &str = "import@1.0.0";

/// Builtin process name used by conductor process dispatch.
pub const TOOL_NAME: &str = META.tool_name;

/// Canonical semantic version handled by this runtime.
pub const TOOL_VERSION: &str = META.tool_version;

/// Builtin purity marker.
pub const IS_IMPURE: bool = META.is_impure;

/// Metadata for this builtin crate.
pub const META: BuiltinMeta = BuiltinMeta {
    tool_id: "builtins.import@1.0.0",
    tool_name: "import",
    tool_version: "1.0.0",
    is_impure: true,
    summary: "import builtin that ingests file/folder/fetch/cas_hash sources into pure bytes",
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

/// Runs the standalone CLI command using a normal clap-parsed option structure.
///
/// Successful execution writes imported payload bytes directly to stdout.
///
/// # Errors
///
/// Returns an error when CLI key/value pairs are malformed, import execution
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
    let inputs = parse_string_pairs(&cli.inputs, "inputs").map_err(std::io::Error::other)?;
    let payload =
        execute_content_map(&tool_cwd, &params, &inputs).map_err(std::io::Error::other)?;
    writer.write_all(&payload)?;
    Ok(())
}

/// Performs URL fetch with strict integrity pinning.
#[cfg(feature = "fetch")]
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

    let client = Client::builder()
        .timeout(std::time::Duration::from_mins(1))
        .build()
        .map_err(|err| format!("building import fetch HTTP client failed: {err}"))?;

    let response =
        client.get(url).send().map_err(|err| format!("import fetch request failed: {err}"))?;
    if !response.status().is_success() {
        return Err(format!("import fetch got non-OK status: {}", response.status().as_u16()));
    }

    let bytes = response
        .bytes()
        .map_err(|err| format!("reading import fetch response failed: {err}"))?
        .to_vec();

    let actual_hash = blake3::hash(&bytes);
    let actual_digest = format!("blake3:{}", actual_hash.to_hex());
    if actual_digest != *expected_hash {
        return Err(format!(
            "hash mismatch for import fetch: expected {expected_hash}, got {actual_digest}"
        ));
    }

    Ok(bytes)
}

/// Fetch-path fallback when network feature is disabled.
#[cfg(not(feature = "fetch"))]
fn execute_fetch(_params: &StringMap) -> Result<Vec<u8>, String> {
    Err("import kind='fetch' requires enabling crate feature 'fetch'".to_string())
}

/// Resolves one file/folder import source path using configured path mode.
fn resolve_file_or_folder_source(
    import_root_dir: &Path,
    params: &StringMap,
) -> Result<PathBuf, String> {
    let kind = params.get("kind").map_or("file_or_folder", String::as_str);
    let path_value =
        params.get("path").ok_or_else(|| format!("import kind='{kind}' requires 'path'"))?;
    let mode = mediapm_utils::path::parse_path_mode(params, &format!("import kind='{kind}'"))?;
    mediapm_utils::path::resolve_path_for_root(
        import_root_dir,
        &format!("import kind='{kind}'"),
        "path",
        path_value,
        mode,
    )
}

/// Validates import args/inputs for required and recognized keys.
fn validate_argument_contract(params: &StringMap, inputs: &StringMap) -> Result<(), String> {
    validate_only_known_keys(inputs, &[], "import")?;

    let kind = params
        .get("kind")
        .ok_or_else(|| "import builtin requires 'kind' (file|folder|fetch|cas_hash)".to_string())?;

    match kind.as_str() {
        "file" | "folder" => {
            validate_only_known_keys(
                params,
                &["kind", "path", "path_mode"],
                &format!("import kind='{kind}'"),
            )?;

            let _ = require_non_empty_param(params, "path", &format!("import kind='{kind}'"))?;

            let _ = mediapm_utils::path::parse_path_mode(params, &format!("import kind='{kind}'"))?;
            Ok(())
        }
        "fetch" => {
            validate_only_known_keys(
                params,
                &["kind", "url", "expected_hash"],
                "import kind='fetch'",
            )?;

            for required in ["url", "expected_hash"] {
                let _ = require_non_empty_param(params, required, "import kind='fetch'")?;
            }

            Ok(())
        }
        "cas_hash" => {
            validate_only_known_keys(params, &["kind", "hash"], "import kind='cas_hash'")?;

            let value = require_non_empty_param(params, "hash", "import kind='cas_hash'")?;
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
        let json = describe_json();
        assert!(json.contains("builtins.import@1.0.0"));
    }
}
