//! Runtime implementation for the `echo` conductor builtin tool.
//!
//! This crate is intentionally standalone:
//! - it exposes one Rust API used by `mediapm-conductor`, and
//! - it can run independently as a CLI (see `main.rs`).
//!
//! Stability contract (shared across all builtin crates):
//! - CLI uses standard Rust flags/options while all values remain strings,
//! - API input arguments are string maps (`BTreeMap<String, String>`),
//! - CLI may optionally define one default option key for one value shorthand,
//!   while explicit keyed input remains supported and maps to the same API key,
//! - pure successful outputs are string maps (`BTreeMap<String, String>`).
//!
//! This echo builtin intentionally stays simple and echo-like:
//! - CLI text is positional (no `--arg` / `--input` transport),
//! - one optional `--stream` flag chooses output stream,
//! - API uses map keys with equivalent semantics (`text` + optional `stream`).
//! - API rejects undeclared keys and ambiguous duplicate key sources.
//! - runtime semantics are stream payload pass-through: output bytes are
//!   returned to the caller; no host-console side effect is implied by API
//!   execution itself.
//!
//! `echo` is pure, so its non-error success payload follows the shared
//! string-only rule directly.
//!
//! Error transport is intentionally separate from the success payload contract:
//! CLI entrypoints may return ordinary Rust errors rather than encoding failure
//! details inside the string-only success object.

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;

use clap::{Parser, ValueEnum};

/// Builtin tool name handled by this crate.
pub const TOOL_NAME: &str = "echo";

/// Stable builtin identifier registered in phase topology surfaces.
pub const TOOL_ID: &str = "builtins.echo@1.0.0";

/// Canonical semantic version for this builtin implementation.
pub const TOOL_VERSION: &str = "1.0.0";

/// Canonical string-map payload used by both API and CLI contracts.
pub type StringMap = BTreeMap<String, String>;

/// Output stream selector used by both API and CLI entrypoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EchoStream {
    /// Emit text only to stdout.
    Stdout,
    /// Emit text only to stderr.
    Stderr,
    /// Emit text to both stdout and stderr.
    Both,
}

/// Standard clap-based CLI accepted by every builtin crate.
///
/// Contract notes:
/// - text is passed as positional arguments and echoed verbatim,
/// - one optional `--stream` flag chooses stdout/stderr/both,
/// - API equivalents are `text` and `stream` in [`StringMap`].
#[derive(Debug, Clone, PartialEq, Eq, Parser)]
pub struct BuiltinCliArgs {
    /// Output stream selection (`stdout`, `stderr`, or `both`).
    #[arg(long, value_enum, default_value_t = EchoStream::Stdout)]
    pub stream: EchoStream,
    /// Text tokens to echo, joined with single spaces.
    #[arg(value_name = "TEXT", allow_hyphen_values = true)]
    pub text: Vec<String>,
}

/// Pure echo execution result split by output stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchoEmission {
    /// Bytes that should be captured as stdout.
    pub stdout: String,
    /// Bytes that should be captured as stderr.
    pub stderr: String,
}

/// Executes the echo API and returns explicit stdout/stderr payload strings.
///
/// API keys:
/// - `text` (optional): primary text payload,
/// - `stream` (optional): `stdout`, `stderr`, or `both`.
///
/// Validation rules:
/// - unknown keys fail,
/// - if both `params` and `inputs` provide the same key, execution fails.
pub fn execute_echo(params: &StringMap, inputs: &StringMap) -> Result<EchoEmission, String> {
    validate_argument_contract(params, inputs)?;

    let stream = parse_stream(
        params
            .get("stream")
            .or_else(|| inputs.get("stream"))
            .map(|value| value.as_str())
            .unwrap_or("stdout"),
    )?;

    let text = params.get("text").or_else(|| inputs.get("text")).cloned().unwrap_or_default();

    let rendered = format!("{text}\n");
    let emission = match stream {
        EchoStream::Stdout => EchoEmission { stdout: rendered, stderr: String::new() },
        EchoStream::Stderr => EchoEmission { stdout: String::new(), stderr: rendered },
        EchoStream::Both => EchoEmission { stdout: rendered.clone(), stderr: rendered },
    };

    Ok(emission)
}

/// Compatibility API that serializes echo emission into a string-map payload.
///
/// Keys:
/// - `stdout`: emitted stdout text,
/// - `stderr`: emitted stderr text.
pub fn execute_string_map(params: &StringMap, inputs: &StringMap) -> Result<StringMap, String> {
    let emission = execute_echo(params, inputs)?;
    Ok(StringMap::from([
        ("stdout".to_string(), emission.stdout),
        ("stderr".to_string(), emission.stderr),
    ]))
}

/// Runs the standalone CLI command with one optional stream selector.
///
/// This behaves like shell `echo`: positional text is joined with spaces,
/// then terminated by a newline and written to the selected stream(s).
pub fn run_cli_command<WOut: Write, WErr: Write>(
    cli: &BuiltinCliArgs,
    stdout_writer: &mut WOut,
    stderr_writer: &mut WErr,
) -> Result<(), Box<dyn Error>> {
    let rendered = format!("{}\n", cli.text.join(" "));
    match cli.stream {
        EchoStream::Stdout => stdout_writer.write_all(rendered.as_bytes())?,
        EchoStream::Stderr => stderr_writer.write_all(rendered.as_bytes())?,
        EchoStream::Both => {
            stdout_writer.write_all(rendered.as_bytes())?;
            stderr_writer.write_all(rendered.as_bytes())?;
        }
    }
    Ok(())
}

/// Returns one deterministic descriptor map for this builtin.
#[must_use]
pub fn describe() -> StringMap {
    StringMap::from([
        ("tool_id".to_string(), TOOL_ID.to_string()),
        ("tool_name".to_string(), TOOL_NAME.to_string()),
        ("tool_version".to_string(), TOOL_VERSION.to_string()),
        ("is_impure".to_string(), "false".to_string()),
        (
            "summary".to_string(),
            "echo-like builtin with positional text and optional stream selection".to_string(),
        ),
    ])
}

/// Parses one API stream selector value into [`EchoStream`].
fn parse_stream(value: &str) -> Result<EchoStream, String> {
    match value {
        "stdout" => Ok(EchoStream::Stdout),
        "stderr" => Ok(EchoStream::Stderr),
        "both" => Ok(EchoStream::Both),
        other => Err(format!("invalid stream '{other}'; expected one of: stdout, stderr, both")),
    }
}

/// Validates echo API keys and duplicate-key-source ambiguity.
fn validate_argument_contract(params: &StringMap, inputs: &StringMap) -> Result<(), String> {
    for key in params.keys() {
        if key != "text" && key != "stream" {
            return Err(format!(
                "echo builtin does not accept arg '{key}'; allowed args are: text, stream"
            ));
        }
    }
    for key in inputs.keys() {
        if key != "text" && key != "stream" {
            return Err(format!(
                "echo builtin does not accept input '{key}'; allowed inputs are: text, stream"
            ));
        }
    }
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
    use clap::Parser;

    use super::{BuiltinCliArgs, execute_echo, execute_string_map, run_cli_command};
    use std::collections::BTreeMap;

    /// Verifies API defaults to stdout and appends newline like shell echo.
    #[test]
    fn execute_echo_defaults_to_stdout() {
        let params = BTreeMap::from([("text".to_string(), "hello world".to_string())]);
        let emission = execute_echo(&params, &BTreeMap::new()).expect("api call should succeed");

        assert_eq!(emission.stdout, "hello world\n");
        assert!(emission.stderr.is_empty());
    }

    /// Verifies API supports stderr and both stream-routing modes.
    #[test]
    fn execute_echo_supports_stream_routing() {
        let stderr_only = execute_echo(
            &BTreeMap::from([
                ("text".to_string(), "warn".to_string()),
                ("stream".to_string(), "stderr".to_string()),
            ]),
            &BTreeMap::new(),
        )
        .expect("stderr mode should succeed");
        assert_eq!(stderr_only.stderr, "warn\n");
        assert!(stderr_only.stdout.is_empty());

        let both = execute_echo(
            &BTreeMap::from([("stream".to_string(), "both".to_string())]),
            &BTreeMap::from([("text".to_string(), "echoed".to_string())]),
        )
        .expect("both mode should succeed");
        assert_eq!(both.stdout, "echoed\n");
        assert_eq!(both.stderr, "echoed\n");
    }

    /// Verifies unknown API keys are rejected with an actionable error.
    #[test]
    fn execute_echo_rejects_unknown_keys() {
        let error = execute_echo(
            &BTreeMap::from([("mode".to_string(), "demo".to_string())]),
            &BTreeMap::new(),
        )
        .expect_err("unknown keys should fail");
        assert!(error.contains("does not accept arg 'mode'"));
    }

    /// Verifies duplicate key sources across args/inputs are rejected.
    #[test]
    fn execute_echo_rejects_duplicate_key_sources() {
        let error = execute_echo(
            &BTreeMap::from([("text".to_string(), "a".to_string())]),
            &BTreeMap::from([("text".to_string(), "b".to_string())]),
        )
        .expect_err("duplicate text source should fail");
        assert!(error.contains("duplicate 'text'"));
    }

    /// Verifies compatibility API returns explicit stdout/stderr map fields.
    #[test]
    fn execute_string_map_returns_stream_fields() {
        let payload = execute_string_map(
            &BTreeMap::from([("text".to_string(), "hello".to_string())]),
            &BTreeMap::new(),
        )
        .expect("compatibility api should succeed");

        assert_eq!(payload.get("stdout"), Some(&"hello\n".to_string()));
        assert_eq!(payload.get("stderr"), Some(&"".to_string()));
    }

    /// Verifies CLI echoes positional text to selected streams.
    #[test]
    fn run_cli_emits_to_selected_stream() {
        let cli = BuiltinCliArgs::parse_from(["echo", "--stream", "both", "hello", "world"]);

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        run_cli_command(&cli, &mut stdout, &mut stderr).expect("cli run should succeed");
        assert_eq!(String::from_utf8(stdout).expect("stdout utf8"), "hello world\n");
        assert_eq!(String::from_utf8(stderr).expect("stderr utf8"), "hello world\n");
    }
}
