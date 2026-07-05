//! Runtime implementation for the `echo` conductor builtin tool.
//!
//! This crate is intentionally standalone:
//! - it exposes one Rust API used by `mediapm-conductor`, and
//! - it can run independently as a CLI (see `main.rs`).
//!
//! `echo` is pure, so its non-error success payload follows the shared
//! string-only rule directly.
//!
//! Error transport is intentionally separate from the success payload contract:
//! CLI entrypoints may return ordinary Rust errors rather than encoding failure
//! details inside the string-only success object.

#[cfg(feature = "cli")]
use std::error::Error;
#[cfg(feature = "cli")]
use std::io::Write;

#[cfg(feature = "cli")]
use mediapm_utils::builtin::describe_json_compact_meta;
use mediapm_utils::builtin::{BuiltinMeta, describe_meta, validate_only_known_keys};

/// Re-export of [`mediapm_utils::StringMap`] for downstream convenience.
pub use mediapm_utils::StringMap;
#[cfg(feature = "cli")]
/// Re-export for the shared CLI runner macro.
pub use mediapm_utils::builtin::BuiltinCliArgs;

/// Builtin tool name handled by this crate.
pub const TOOL_NAME: &str = META.tool_name;

/// Stable builtin identifier registered in phase topology surfaces.
pub const TOOL_ID: &str = META.tool_id;

/// Versioned builtin identifier (e.g. "echo@1.0.0") used in config map keys and dispatch.
pub const TOOL_BUILTIN_ID: &str = "echo@1.0.0";

/// Canonical semantic version for this builtin implementation.
pub const TOOL_VERSION: &str = META.tool_version;

/// Metadata for this builtin crate.
pub const META: BuiltinMeta = BuiltinMeta {
    tool_id: "builtins.echo@1.0.0",
    tool_name: "echo",
    tool_version: "1.0.0",
    is_impure: false,
    summary: "echo-like builtin returning text as stdout/stderr string-map",
};

/// Executes the echo API and returns stdout/stderr payload strings.
///
/// API keys:
/// - `text` (optional): primary text payload,
/// - `stream` (optional): `stdout`, `stderr`, or `both`.
///
/// Returns `{"stdout": "...", "stderr": "..."}`.
///
/// # Errors
///
/// Returns an error when argument contract validation fails or when `stream`
/// is not one of `stdout`, `stderr`, or `both`.
pub fn execute(params: &StringMap, inputs: &StringMap) -> Result<StringMap, String> {
    validate_argument_contract(params, inputs)?;

    let stream =
        params.get("stream").or_else(|| inputs.get("stream")).map_or("stdout", String::as_str);
    let text = params.get("text").or_else(|| inputs.get("text")).cloned().unwrap_or_default();
    let rendered = format!("{text}\n");

    let (stdout, stderr): (String, String) = match stream {
        "stdout" => (rendered, String::new()),
        "stderr" => (String::new(), rendered),
        "both" => (rendered.clone(), rendered),
        other => {
            return Err(format!("invalid stream '{other}'; expected one of: stdout, stderr, both"));
        }
    };

    Ok(StringMap::from([("stdout".to_string(), stdout), ("stderr".to_string(), stderr)]))
}

/// Runs the standalone CLI command using the shared single-writer pattern.
///
/// Output is serialized JSON: `{"stdout": "...", "stderr": "..."}`.
///
/// # Errors
///
/// Returns an error when CLI key/value pairs are malformed, execution fails,
/// descriptor serialization fails, or writing output to the writer fails.
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

    let params = mediapm_utils::builtin::parse_string_pairs(&cli.args, "args")
        .map_err(std::io::Error::other)?;
    let inputs = mediapm_utils::builtin::parse_string_pairs(&cli.inputs, "inputs")
        .map_err(std::io::Error::other)?;
    let response = execute(&params, &inputs).map_err(std::io::Error::other)?;
    let payload = serde_json::to_vec(&response)?;
    writer.write_all(&payload)?;
    Ok(())
}

/// Returns one deterministic descriptor map for this builtin.
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

/// Validates echo API keys and duplicate-key-source ambiguity.
fn validate_argument_contract(params: &StringMap, inputs: &StringMap) -> Result<(), String> {
    validate_only_known_keys(params, &["text", "stream"], "echo")?;
    validate_only_known_keys(inputs, &["text", "stream"], "echo")?;

    if params.contains_key("text") && inputs.contains_key("text") {
        return Err("echo builtin received duplicate 'text' in args and inputs".to_string());
    }
    if params.contains_key("stream") && inputs.contains_key("stream") {
        return Err("echo builtin received duplicate 'stream' in args and inputs".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "cli")]
    use clap::Parser;
    #[cfg(feature = "cli")]
    use mediapm_utils::builtin::BuiltinCliArgs;

    use super::execute;
    #[cfg(feature = "cli")]
    use super::run_cli_command;
    use std::collections::BTreeMap;

    /// Verifies API defaults to stdout and appends newline like shell echo.
    #[test]
    fn execute_defaults_to_stdout() {
        let params = BTreeMap::from([("text".to_string(), "hello world".to_string())]);
        let result = execute(&params, &BTreeMap::new()).expect("api call should succeed");

        assert_eq!(result.get("stdout"), Some(&"hello world\n".to_string()));
        assert_eq!(result.get("stderr"), Some(&String::new()));
    }

    /// Verifies API supports stderr and both stream-routing modes.
    #[test]
    fn execute_supports_stream_routing() {
        let stderr_only = execute(
            &BTreeMap::from([
                ("text".to_string(), "warn".to_string()),
                ("stream".to_string(), "stderr".to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect("stderr mode should succeed");
        assert_eq!(stderr_only.get("stderr"), Some(&"warn\n".to_string()));
        assert_eq!(stderr_only.get("stdout"), Some(&String::new()));

        let both = execute(
            &BTreeMap::from([("stream".to_string(), "both".to_string())]),
            &BTreeMap::from([("text".to_string(), "echoed".to_string())]),
        )
        .expect("both mode should succeed");
        assert_eq!(both.get("stdout"), Some(&"echoed\n".to_string()));
        assert_eq!(both.get("stderr"), Some(&"echoed\n".to_string()));
    }

    /// Verifies unknown API keys are rejected with an actionable error.
    #[test]
    fn execute_rejects_unknown_keys() {
        let error =
            execute(&BTreeMap::from([("mode".to_string(), "demo".to_string())]), &BTreeMap::new())
                .expect_err("unknown keys should fail");
        assert!(error.contains("echo does not accept arg 'mode'"));
    }

    /// Verifies duplicate key sources across args/inputs are rejected.
    #[test]
    fn execute_rejects_duplicate_key_sources() {
        let error = execute(
            &BTreeMap::from([("text".to_string(), "a".to_string())]),
            &BTreeMap::from([("text".to_string(), "b".to_string())]),
        )
        .expect_err("duplicate text source should fail");
        assert!(error.contains("duplicate 'text'"));
    }

    #[cfg(feature = "cli")]
    /// Verifies CLI produces JSON string-map output.
    #[test]
    fn run_cli_produces_json_output() {
        let cli = BuiltinCliArgs::parse_from(["echo", "--arg", "text", "hello world"]);

        let mut output = Vec::new();

        run_cli_command(&cli, &mut output).expect("cli run should succeed");
        let json: serde_json::Value =
            serde_json::from_slice(&output).expect("output should be valid json");
        assert_eq!(json["stdout"], "hello world\n");
        assert_eq!(json["stderr"], "");
    }
}
