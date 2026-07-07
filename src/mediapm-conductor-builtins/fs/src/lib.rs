//! Filesystem-operations builtin runtime crate.
//!
//! This crate is intentionally standalone:
//! - it exposes API contracts and execution helpers for conductor dispatch, and
//! - it can run independently via its binary target (`src/main.rs`).
//!
//! Stability contract (shared across all builtin crates):
//! - CLI uses standard Rust flag/option syntax while keeping all values as strings,
//! - API inputs are string maps (`BTreeMap<String, String>`),
//! - CLI may optionally define one default option key for one value shorthand,
//!   while explicit keyed input remains supported and maps to the same API key,
//! - pure successful outputs are string maps (`BTreeMap<String, String>`).
//!
//! `fs` is impure: its primary observable success result is the filesystem
//! side effect. Successful API calls therefore return no payload.
//!
//! API validation is strict: undeclared args/inputs fail, and required args
//! must be present for each operation.
//!
//! Error transport is intentionally separate from the success payload contract:
//! CLI entrypoints may return ordinary Rust errors rather than stuffing failure
//! details into the string-only success object.

#[cfg(feature = "cli")]
use std::error::Error;
#[cfg(feature = "cli")]
use std::io::Write;
use std::path::Path;
#[cfg(feature = "cli")]
use std::path::PathBuf;

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

/// Stable builtin id used by topology registration.
pub const TOOL_ID: &str = META.tool_id;

/// Versioned builtin identifier (e.g. "fs@v1") used in config map keys and dispatch.
pub const TOOL_BUILTIN_ID: &str = "fs@v1";

/// Builtin process name used by conductor process dispatch.
pub const TOOL_NAME: &str = META.tool_name;

/// Canonical semantic version handled by this runtime.
pub const TOOL_VERSION: &str = META.tool_version;

/// Builtin purity marker.
pub const IS_IMPURE: bool = META.is_impure;

/// Metadata for this builtin crate.
pub const META: BuiltinMeta = BuiltinMeta {
    tool_id: "builtins.fs@v1",
    tool_name: "fs",
    tool_version: "v1",
    is_impure: true,
    summary: "filesystem operation builtin runtime with impure side-effecting behavior",
};

#[must_use]
pub fn describe() -> StringMap {
    describe_meta(&META)
}

#[cfg(feature = "cli")]
#[must_use]
pub fn describe_json() -> String {
    describe_json_compact_meta(&META)
}

/// Executes one `fs` request using string-map arguments.
///
/// Supported params:
/// - `op=ensure_dir` + `path=<destination path>` + optional
///   `path_mode=relative|absolute`,
/// - `op=write_text` + `path=<destination path>` + optional `content` +
///   optional `path_mode=relative|absolute`,
/// - `op=copy` + `path=<source path>` + `dest=<destination path>` + optional
///   `path_mode=relative|absolute`.
///
/// Path-mode semantics:
/// - default `path_mode` is `relative`,
/// - `relative` resolves paths under `fs_root_dir` and rejects traversal
///   outside that root,
/// - `absolute` requires explicit absolute paths.
///
/// Successful execution returns no payload bytes because the primary result is
/// side effects on the filesystem.
///
/// # Errors
///
/// Returns an error when args/inputs violate the builtin contract, path-mode
/// validation fails, path resolution fails, or the requested filesystem
/// operation fails.
pub fn execute_string_map(
    fs_root_dir: &Path,
    params: &StringMap,
    inputs: &StringMap,
) -> Result<(), String> {
    validate_argument_contract(params, inputs)?;

    let op = params.get("op").ok_or_else(|| "fs requires 'op' argument".to_string())?.as_str();
    let path = params.get("path").ok_or_else(|| format!("fs op '{op}' requires 'path'"))?;
    let mode = mediapm_utils::path::parse_path_mode(params, &format!("fs op '{op}'"))?;
    let resolved = mediapm_utils::path::resolve_path_for_root(
        fs_root_dir,
        &format!("fs op '{op}'"),
        "path",
        path,
        mode,
    )?;

    match op {
        "ensure_dir" => {
            std::fs::create_dir_all(&resolved)
                .map_err(|err| format!("ensure_dir '{path}' failed: {err}"))?;
            Ok(())
        }
        "write_text" => {
            let content = params.get("content").cloned().unwrap_or_default();
            if let Some(parent) = resolved.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|err| format!("create_parent '{path}' failed: {err}"))?;
            }
            std::fs::write(&resolved, content.as_bytes())
                .map_err(|err| format!("write_text '{path}' failed: {err}"))?;
            Ok(())
        }
        "copy" => {
            let dest =
                params.get("dest").ok_or_else(|| "fs op 'copy' requires 'dest'".to_string())?;
            let resolved_dest = mediapm_utils::path::resolve_path_for_root(
                fs_root_dir,
                &format!("fs op '{op}'"),
                "dest",
                dest,
                mode,
            )?;
            if let Some(parent) = resolved_dest.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|err| format!("create_parent '{dest}' failed: {err}"))?;
            }
            std::fs::copy(&resolved, &resolved_dest)
                .map_err(|err| format!("copy '{path}' -> '{dest}' failed: {err}"))?;
            Ok(())
        }
        other => Err(format!("unsupported fs op '{other}'")),
    }
}

/// Runs the standalone CLI command using a normal clap-parsed option structure.
///
/// On success this command completes through filesystem side effects and emits
/// no payload bytes.
///
/// # Errors
///
/// Returns an error when descriptor JSON writing fails, key/value pair parsing
/// fails, or filesystem operation execution fails.
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

    let root_dir = PathBuf::from(&cli.root_dir);
    let params = parse_string_pairs(&cli.args, "args").map_err(std::io::Error::other)?;
    let inputs = parse_string_pairs(&cli.inputs, "inputs").map_err(std::io::Error::other)?;
    execute_string_map(&root_dir, &params, &inputs).map_err(std::io::Error::other)?;
    Ok(())
}

/// Validates `fs` args/inputs for required and recognized operation keys.
fn validate_argument_contract(params: &StringMap, inputs: &StringMap) -> Result<(), String> {
    let op = params.get("op").ok_or_else(|| "fs requires 'op' argument".to_string())?.as_str();

    let (allowed_params, allowed_inputs): (&[&str], &[&str]) = match op {
        "ensure_dir" => (&["op", "path", "path_mode"], &[]),
        "write_text" => (&["op", "path", "content", "path_mode"], &[]),
        "copy" => (&["op", "path", "dest", "path_mode"], &[]),
        other => return Err(format!("unsupported fs op '{other}'")),
    };

    validate_only_known_keys(params, allowed_params, &format!("fs op '{op}'"))?;
    validate_only_known_keys(inputs, allowed_inputs, &format!("fs op '{op}'"))?;

    let _ = require_non_empty_param(params, "path", &format!("fs op '{op}'"))?;
    if op == "copy" {
        let _ = require_non_empty_param(params, "dest", &format!("fs op '{op}'"))?;
    }

    let _ = mediapm_utils::path::parse_path_mode(params, &format!("fs op '{op}'"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    #[cfg(feature = "cli")]
    use super::{BuiltinCliArgs, run_cli_command};
    use super::{describe_json, execute_string_map};
    #[cfg(feature = "cli")]
    use clap::Parser;

    /// Verifies the library API can create directories and write text files.
    #[test]
    fn execute_string_map_writes_text_and_dirs() {
        let temp = tempdir().expect("tempdir");
        let dir_path = "a/b";
        let file_path = "a/b/file.txt";
        execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("op".to_string(), "ensure_dir".to_string()),
                ("path".to_string(), dir_path.to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect("ensure_dir should execute");

        execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("op".to_string(), "write_text".to_string()),
                ("path".to_string(), file_path.to_string()),
                ("content".to_string(), "hello".to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect("write_text should execute");

        assert_eq!(
            std::fs::read_to_string(temp.path().join(file_path)).ok(),
            Some("hello".to_string())
        );
    }

    /// Verifies copy op supports relative paths and destination creation.
    #[test]
    fn execute_copy_copies_file() {
        let temp = tempdir().expect("tempdir");
        let source_rel = "a.txt";
        let dest_rel = "out/b.txt";
        let source_path = temp.path().join(source_rel);
        std::fs::write(&source_path, b"copyme").expect("write source");
        let dest_path = temp.path().join(dest_rel);

        let args = BTreeMap::from([
            ("op".to_string(), "copy".to_string()),
            ("path".to_string(), source_rel.to_string()),
            ("dest".to_string(), dest_rel.to_string()),
        ]);

        execute_string_map(temp.path(), &args, &BTreeMap::new()).expect("copy should succeed");

        assert!(dest_path.exists());
    }

    /// Verifies relative paths resolve under the configured fs root directory.
    #[test]
    fn execute_relative_mode_resolves_under_fs_root() {
        let temp = tempdir().expect("tempdir");
        execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("op".to_string(), "write_text".to_string()),
                ("path".to_string(), "relative.txt".to_string()),
                ("content".to_string(), "x".to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect("relative path should succeed");

        assert!(temp.path().join("relative.txt").exists());
    }

    /// Verifies unknown args fail fast instead of being silently ignored.
    #[test]
    fn execute_rejects_unknown_arg() {
        let temp = tempdir().expect("tempdir");
        let error = execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("op".to_string(), "ensure_dir".to_string()),
                ("path".to_string(), "x".to_string()),
                ("unexpected".to_string(), "y".to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect_err("unknown arg should fail");
        assert!(error.contains("does not accept arg 'unexpected'"));
    }

    /// Verifies missing required operation args fail with explicit diagnostics.
    #[test]
    fn execute_rejects_missing_required_arg() {
        let temp = tempdir().expect("tempdir");
        let error = execute_string_map(
            temp.path(),
            &BTreeMap::from([("op".to_string(), "write_text".to_string())]),
            &BTreeMap::new(),
        )
        .expect_err("missing required arg should fail");
        assert!(error.contains("requires 'path'"));
    }

    #[cfg(feature = "cli")]
    /// Verifies successful CLI execution emits no output payload bytes.
    #[test]
    fn run_cli_executes_invocation() {
        let temp = tempdir().expect("tempdir");
        let cli = BuiltinCliArgs::parse_from([
            "fs",
            "--root-dir",
            &temp.path().to_string_lossy(),
            "--arg",
            "op",
            "write_text",
            "--arg",
            "path",
            "f.txt",
            "--arg",
            "content",
            "ok",
        ]);
        let mut writer = Vec::new();

        run_cli_command(&cli, &mut writer).expect("run_cli should succeed");
        assert!(writer.is_empty(), "successful fs CLI calls should not emit payload bytes");
        assert_eq!(std::fs::read_to_string(temp.path().join("f.txt")).ok(), Some("ok".to_string()));
    }

    /// Verifies descriptor serialization keeps the stable builtin identifier.
    #[test]
    fn descriptor_json_contains_tool_id() {
        let json = describe_json();
        assert!(json.contains("builtins.fs@v1"));
    }
}
