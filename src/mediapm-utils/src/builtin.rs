//! Shared builtin descriptor and CLI helpers for conductor builtin crates.
//!
//! The [`describe`], [`describe_json_compact`], and [`describe_json_compat`]
//! helpers are always available.  [`BuiltinCliArgs`] and [`parse_string_pairs`]
//! require the `cli` feature.

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

/// Returns a deterministic descriptor JSON string, always available.
///
/// Unlike [`describe_json_compact`], this function is the canonical compat
/// entry point that builtins expose as a stable public API.  The return type
/// is [`String`] (infallible) across all builtins.
#[must_use]
pub fn describe_json_compat(
    tool_id: &str,
    tool_name: &str,
    tool_version: &str,
    is_impure: bool,
    summary: &str,
) -> String {
    describe_json_compact(tool_id, tool_name, tool_version, is_impure, summary)
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
pub fn validate_only_known_keys(
    params: &StringMap,
    known: &[&str],
    context: &str,
) -> Result<(), String> {
    for key in params.keys() {
        if !known.contains(&key.as_str()) {
            return Err(format!("{context} does not accept arg '{key}'"));
        }
    }
    Ok(())
}
