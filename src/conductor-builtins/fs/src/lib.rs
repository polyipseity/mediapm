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

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use clap::{ArgAction, Parser};

/// Stable builtin id used by topology registration.
pub const TOOL_ID: &str = "mediapm.builtin.fs@1.0.0";

/// Builtin process name used by conductor process dispatch.
pub const TOOL_NAME: &str = "fs";

/// Canonical semantic version handled by this runtime.
pub const TOOL_VERSION: &str = "1.0.0";

/// Builtin purity marker.
pub const IS_IMPURE: bool = true;

/// Canonical string-map payload used by both API and CLI contracts.
pub type StringMap = BTreeMap<String, String>;

/// Path-resolution mode for fs path arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathMode {
    /// Resolve paths under the configured fs root directory.
    Relative,
    /// Treat paths as explicit absolute host paths.
    Absolute,
}

/// Standard clap-based CLI accepted by every builtin crate.
///
/// This crate currently expects explicit `--arg KEY VALUE` and
/// `--input KEY VALUE` pairs and does not define a default option key.
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
            "filesystem operation builtin runtime with impure side-effecting behavior".to_string(),
        ),
    ])
}

/// Serializes [`describe`] for CLI output.
pub fn describe_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&describe())
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
pub fn execute_string_map(
    fs_root_dir: &Path,
    params: &StringMap,
    inputs: &StringMap,
) -> Result<(), String> {
    validate_argument_contract(params, inputs)?;

    let op = params.get("op").ok_or_else(|| "fs requires 'op' argument".to_string())?.as_str();
    let path = params.get("path").ok_or_else(|| format!("fs op '{op}' requires 'path'"))?;
    let mode = parse_path_mode(params, op)?;
    let resolved = resolve_path_for_fs_root(fs_root_dir, op, "path", path, mode)?;

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
            let resolved_dest = resolve_path_for_fs_root(fs_root_dir, op, "dest", dest, mode)?;
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
pub fn run_cli_command<W: Write>(
    cli: &BuiltinCliArgs,
    writer: &mut W,
) -> Result<(), Box<dyn Error>> {
    if cli.describe {
        let descriptor = describe_json()?;
        writer.write_all(descriptor.as_bytes())?;
        return Ok(());
    }

    let root_dir = PathBuf::from(&cli.root_dir);
    let params = parse_string_pairs(&cli.args, "args").map_err(std::io::Error::other)?;
    let inputs = parse_string_pairs(&cli.inputs, "inputs").map_err(std::io::Error::other)?;
    execute_string_map(&root_dir, &params, &inputs).map_err(std::io::Error::other)?;
    Ok(())
}

/// Converts repeated `--arg KEY VALUE` or `--input KEY VALUE` pairs into a map.
///
/// The helper rejects empty keys and incomplete pairs so builtin execution only
/// sees normalized map-shaped input.
///
/// When a builtin defines a default option key, that shorthand should be
/// normalized into this same key/value map before validation.
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

/// Validates `fs` args/inputs for required and recognized operation keys.
fn validate_argument_contract(params: &StringMap, inputs: &StringMap) -> Result<(), String> {
    let op = params.get("op").ok_or_else(|| "fs requires 'op' argument".to_string())?.as_str();

    let (allowed_params, allowed_inputs): (&[&str], &[&str]) = match op {
        "ensure_dir" => (&["op", "path", "path_mode"], &[]),
        "write_text" => (&["op", "path", "content", "path_mode"], &[]),
        "copy" => (&["op", "path", "dest", "path_mode"], &[]),
        other => return Err(format!("unsupported fs op '{other}'")),
    };

    for key in params.keys() {
        if !allowed_params.contains(&key.as_str()) {
            return Err(format!("fs op '{op}' does not accept arg '{key}'"));
        }
    }
    for key in inputs.keys() {
        if !allowed_inputs.contains(&key.as_str()) {
            return Err(format!("fs op '{op}' does not accept input '{key}'"));
        }
    }

    let Some(path) = params.get("path") else {
        return Err(format!("fs op '{op}' requires 'path'"));
    };
    if path.trim().is_empty() {
        return Err(format!("fs op '{op}' requires non-empty 'path'"));
    }
    if op == "copy" {
        let dest = params.get("dest").ok_or_else(|| "fs op 'copy' requires 'dest'".to_string())?;
        if dest.trim().is_empty() {
            return Err("fs op 'copy' requires non-empty 'dest'".to_string());
        }
    }

    let _ = parse_path_mode(params, op)?;

    Ok(())
}

/// Parses and validates path-mode selector for one fs operation.
fn parse_path_mode(params: &StringMap, op: &str) -> Result<PathMode, String> {
    match params.get("path_mode").map(String::as_str).unwrap_or("relative") {
        "relative" => Ok(PathMode::Relative),
        "absolute" => Ok(PathMode::Absolute),
        other => {
            Err(format!("fs op '{op}' path_mode must be 'relative' or 'absolute', got '{other}'"))
        }
    }
}

/// Resolves one fs path argument against root + path-mode semantics.
fn resolve_path_for_fs_root(
    fs_root_dir: &Path,
    op: &str,
    field: &str,
    candidate: &str,
    mode: PathMode,
) -> Result<PathBuf, String> {
    match mode {
        PathMode::Relative => {
            if Path::new(candidate).is_absolute() {
                return Err(format!(
                    "fs op '{op}' with path_mode='relative' requires relative '{field}'"
                ));
            }
            let root = absolute_root(fs_root_dir)?;
            let normalized = normalize_relative_path(candidate, "fs path")?;
            Ok(root.join(normalized))
        }
        PathMode::Absolute => {
            let parsed = Path::new(candidate);
            if !parsed.is_absolute() {
                return Err(format!(
                    "fs op '{op}' with path_mode='absolute' requires absolute '{field}'"
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
        .map_err(|err| format!("resolving current directory for fs root failed: {err}"))
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
                return Err(format!("{context} must stay under fs root directory"));
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

    use clap::Parser;
    use tempfile::tempdir;

    use super::{BuiltinCliArgs, describe_json, execute_string_map, run_cli_command};

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

    /// Verifies relative mode rejects absolute path values.
    #[test]
    fn execute_relative_mode_rejects_absolute_path() {
        let temp = tempdir().expect("tempdir");
        let absolute = temp.path().join("abs.txt");
        let error = execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("op".to_string(), "write_text".to_string()),
                ("path".to_string(), absolute.to_string_lossy().to_string()),
                ("content".to_string(), "x".to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect_err("relative mode should reject absolute path");
        assert!(error.contains("path_mode='relative'"));
    }

    /// Verifies absolute mode accepts explicit absolute paths.
    #[test]
    fn execute_absolute_mode_accepts_absolute_path() {
        let temp = tempdir().expect("tempdir");
        let absolute = temp.path().join("abs.txt");
        execute_string_map(
            temp.path(),
            &BTreeMap::from([
                ("op".to_string(), "write_text".to_string()),
                ("path_mode".to_string(), "absolute".to_string()),
                ("path".to_string(), absolute.to_string_lossy().to_string()),
                ("content".to_string(), "x".to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect("absolute mode should accept absolute path");
        assert_eq!(std::fs::read(&absolute).ok(), Some(b"x".to_vec()));
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
        let json = describe_json().expect("descriptor serialization should succeed");
        assert!(json.contains("mediapm.builtin.fs@1.0.0"));
    }
}
