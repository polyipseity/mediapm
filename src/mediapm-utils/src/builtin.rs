//! Shared builtin descriptor and CLI helpers for conductor builtin crates.
//!
//! The [`describe`] and [`describe_json_compact`] helpers are always
//! available.  [`BuiltinCliArgs`] and [`parse_string_pairs`] require the `cli`
//! feature.

use crate::StringMap;

/// Returns one deterministic descriptor map for a builtin crate.
///
/// The returned map always contains `tool_id`, `tool_name`, `tool_version`,
/// `is_impure`, and `summary` keys.
#[must_use]
pub fn describe(
    tool_id: &str,
    tool_name: &str,
    tool_version: &str,
    is_impure: bool,
    summary: &str,
) -> StringMap {
    StringMap::from([
        ("tool_id".to_string(), tool_id.to_string()),
        ("tool_name".to_string(), tool_name.to_string()),
        ("tool_version".to_string(), tool_version.to_string()),
        ("is_impure".to_string(), is_impure.to_string()),
        ("summary".to_string(), summary.to_string()),
    ])
}

/// Returns a deterministic descriptor JSON string without serde dependencies.
///
/// The JSON is hand-formatted with 2-space indentation.  This matches the
/// output that [`serde_json::to_string_pretty`](https://docs.rs/serde_json)
/// would produce for the same key-value pairs, avoiding an extra dependency
/// for builtin crates that only need static description output.
#[must_use]
pub fn describe_json_compact(
    tool_id: &str,
    tool_name: &str,
    tool_version: &str,
    is_impure: bool,
    summary: &str,
) -> String {
    format!(
        "{{\n  \"is_impure\": \"{is_impure}\",\n  \"summary\": \"{summary}\",\n  \"tool_id\": \"{tool_id}\",\n  \"tool_name\": \"{tool_name}\",\n  \"tool_version\": \"{tool_version}\"\n}}"
    )
}

/// Metadata describing a builtin tool identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltinMeta {
    /// Stable qualified tool identifier (e.g. `builtins.echo@v1`).
    pub tool_id: &'static str,
    /// Short tool process name (e.g. `echo`).
    pub tool_name: &'static str,
    /// Canonical version string (e.g. `v1`).
    pub tool_version: &'static str,
    /// Whether this tool has side effects.
    pub is_impure: bool,
    /// Human-readable one-line tool summary.
    pub summary: &'static str,
}

/// Returns a descriptor map from a [`BuiltinMeta`].
#[must_use]
pub fn describe_meta(meta: &BuiltinMeta) -> StringMap {
    describe(meta.tool_id, meta.tool_name, meta.tool_version, meta.is_impure, meta.summary)
}

/// Returns a compact descriptor JSON string from a [`BuiltinMeta`].
#[must_use]
pub fn describe_json_compact_meta(meta: &BuiltinMeta) -> String {
    describe_json_compact(
        meta.tool_id,
        meta.tool_name,
        meta.tool_version,
        meta.is_impure,
        meta.summary,
    )
}

// ---------------------------------------------------------------------------
// CLI-specific helpers (behind `cli` feature)
// ---------------------------------------------------------------------------

/// Standard clap-based CLI accepted by every builtin crate.
///
/// Fields:
/// - `--describe`: prints descriptor JSON and exits,
/// - `--root-dir` (default `.`): optional execution root override,
/// - `--arg KEY VALUE`: repeated argument key-value pairs,
/// - `--input KEY VALUE`: repeated input key-value pairs.
///
/// Binary targets should parse with `BuiltinCliArgs::parse()` then
/// pass to the crate's `run_cli_command`.
#[cfg(feature = "cli")]
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
pub struct BuiltinCliArgs {
    /// Prints builtin descriptor metadata as JSON and exits.
    #[arg(long, default_value_t = false)]
    pub describe: bool,

    /// Optional execution root override.
    #[arg(long, default_value = ".")]
    pub root_dir: String,

    /// Builtin argument pairs as repeated `--arg KEY VALUE` options.
    #[arg(
        long = "arg",
        value_names = ["KEY", "VALUE"],
        num_args = 2,
        action = clap::ArgAction::Append
    )]
    pub args: Vec<String>,

    /// Builtin input pairs as repeated `--input KEY VALUE` options.
    #[arg(
        long = "input",
        value_names = ["KEY", "VALUE"],
        num_args = 2,
        action = clap::ArgAction::Append
    )]
    pub inputs: Vec<String>,
}

/// Converts repeated `--arg KEY VALUE` or `--input KEY VALUE` pairs into a
/// map.
///
/// The helper rejects empty keys and incomplete pairs so builtin execution
/// only sees normalized map-shaped input.
///
/// # Errors
///
/// Returns an error when a key is empty, a pair is incomplete, or a key
/// appears more than once.
#[cfg(feature = "cli")]
pub fn parse_string_pairs(pairs: &[String], label: &str) -> Result<StringMap, String> {
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

// ---------------------------------------------------------------------------
// Macro: builtin_main_single_writer!
// ---------------------------------------------------------------------------

/// Expands to a full `main()` function for builtin CLI binaries.
///
/// Suitable for archive, export, fs, and import (not echo, which is special).
///
/// # Arguments
///
/// * `$crate_id` — the crate identifier (e.g. `mediapm_conductor_builtin_archive`).
///
/// The expanded code includes `use clap::Parser;` so callers do not need to
/// pre-import it.
#[macro_export]
macro_rules! builtin_main_single_writer {
    ($crate_id:ident) => {
        fn main() -> Result<(), Box<dyn std::error::Error>> {
            use clap::Parser;
            let cli = $crate_id::BuiltinCliArgs::parse();
            let mut stdout = std::io::stdout();
            $crate_id::run_cli_command(&cli, &mut stdout)
        }
    };
}

// ---------------------------------------------------------------------------
// Helper: validate_only_known_keys
// ---------------------------------------------------------------------------

/// Checks that every key in `params` appears in `known`.
///
/// Returns `Err` with a descriptive message using `context` as the subject
/// (e.g. `"fs op 'copy'"`) when an unknown key is found.
///
/// # Errors
///
/// Returns `Err` if any key in `params` is not in `known`.
pub fn validate_only_known_keys<K: AsRef<str> + Ord, V>(
    params: &std::collections::BTreeMap<K, V>,
    known: &[&str],
    context: &str,
) -> Result<(), String> {
    for key in params.keys() {
        let key = key.as_ref();
        if !known.contains(&key) {
            return Err(format!("{context} does not accept arg '{key}'"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers: contract validation
// ---------------------------------------------------------------------------

/// Returns a required param value from a [`StringMap`], or an error.
///
/// # Errors
///
/// Returns `Err` if `key` is missing.
pub fn require_param<'a>(
    params: &'a StringMap,
    key: &str,
    context: &str,
) -> Result<&'a str, String> {
    params.get(key).map(String::as_str).ok_or_else(|| format!("{context} requires '{key}'"))
}

/// Returns a required non-empty param value from a [`StringMap`], or an error.
///
/// # Errors
///
/// Returns `Err` if `key` is missing or its value is empty/whitespace.
pub fn require_non_empty_param<'a>(
    params: &'a StringMap,
    key: &str,
    context: &str,
) -> Result<&'a str, String> {
    let value = require_param(params, key, context)?;
    if value.trim().is_empty() {
        return Err(format!("{context} requires non-empty '{key}'"));
    }
    Ok(value)
}

/// Returns a required binary input value, or an error.
///
/// # Errors
///
/// Returns `Err` if `key` is missing.
pub fn require_binary_input<'a>(
    inputs: &'a crate::BinaryInputMap,
    key: &str,
    context: &str,
) -> Result<&'a Vec<u8>, String> {
    inputs.get(key).ok_or_else(|| format!("{context} requires input '{key}'"))
}

#[cfg(test)]
mod tests {
    use super::{describe, describe_json_compact, validate_only_known_keys};
    use crate::StringMap;

    #[test]
    fn describe_contains_all_keys() {
        let result = describe("tool-1", "Tool One", "v1", false, "A test tool.");
        assert_eq!(result.len(), 5);
        assert!(result.contains_key("tool_id"));
        assert!(result.contains_key("tool_name"));
        assert!(result.contains_key("tool_version"));
        assert!(result.contains_key("is_impure"));
        assert!(result.contains_key("summary"));
    }

    #[test]
    fn describe_correct_values() {
        let result = describe("t1", "Test", "2.0", true, "Impure tool.");
        assert_eq!(result.get("tool_id"), Some(&"t1".to_string()));
        assert_eq!(result.get("tool_name"), Some(&"Test".to_string()));
        assert_eq!(result.get("tool_version"), Some(&"2.0".to_string()));
        assert_eq!(result.get("is_impure"), Some(&"true".to_string()));
        assert_eq!(result.get("summary"), Some(&"Impure tool.".to_string()));
    }

    #[test]
    fn describe_json_compact_contains_keys() {
        let json = describe_json_compact("echo", "Echo", "0.1.0", false, "Echoes input");
        assert!(json.contains(r#""tool_id": "echo""#));
        assert!(json.contains(r#""tool_name": "Echo""#));
        assert!(json.contains(r#""tool_version": "0.1.0""#));
        assert!(json.contains(r#""is_impure": "false""#));
        assert!(json.contains(r#""summary": "Echoes input""#));
    }

    #[test]
    fn describe_json_compact_indentation() {
        let json = describe_json_compact("x", "X", "1", false, ".");
        for line in json.lines().skip(1) {
            if line == "}" {
                continue;
            }
            assert!(line.starts_with("  "), "line should be indented: {line:?}");
        }
    }

    #[test]
    fn validate_only_known_keys_accepts_empty() {
        let params = StringMap::new();
        let known = &["foo", "bar"];
        assert!(validate_only_known_keys(&params, known, "test").is_ok());
    }

    #[test]
    fn validate_only_known_keys_accepts_known() {
        let params = StringMap::from([
            ("foo".to_string(), "1".to_string()),
            ("bar".to_string(), "2".to_string()),
        ]);
        let known = &["foo", "bar", "baz"];
        assert!(validate_only_known_keys(&params, known, "test").is_ok());
    }

    #[test]
    fn validate_only_known_keys_accepts_subset() {
        let params = StringMap::from([("foo".to_string(), "1".to_string())]);
        let known = &["foo", "bar"];
        assert!(validate_only_known_keys(&params, known, "test").is_ok());
    }

    #[test]
    fn validate_only_known_keys_rejects_unknown() {
        let params = StringMap::from([
            ("foo".to_string(), "1".to_string()),
            ("unknown".to_string(), "x".to_string()),
        ]);
        let known = &["foo", "bar"];
        let err = validate_only_known_keys(&params, known, "test")
            .expect_err("should reject unknown key");
        assert!(err.contains("unknown"), "error should mention unknown key: {err}");
    }

    #[test]
    fn validate_only_known_keys_includes_context() {
        let params = StringMap::from([("bad".to_string(), "x".to_string())]);
        let err =
            validate_only_known_keys(&params, &["good"], "my_operation").expect_err("should fail");
        assert!(err.contains("my_operation"), "error should include context: {err}");
    }
}
