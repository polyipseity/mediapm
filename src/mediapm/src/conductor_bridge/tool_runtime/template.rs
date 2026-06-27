//! Template syntax constants and sandbox-path normalization for
//! managed-tool command templates.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;

use mediapm_conductor::ToolSpec;

use crate::error::MediaPmError;

// ── Template literal escaping ────────────────────────────────────────────

/// Escapes a literal string value for use inside conductor NCL templates.
#[must_use]
pub(super) fn escape_template_literal(value: &str) -> String {
    // NCL strings use `%` for template interpolation; escape literal `%` by
    // doubling it, and escape `\` and `"`.
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('%', "%%")
}

// ── Validation ───────────────────────────────────────────────────────────

/// Validates a built tool command template for correctness.
///
/// Checks that:
/// - The template references at least one sandbox input file.
/// - Required input bindings are satisfied.
/// - Template does not contain obviously invalid token sequences.
pub(crate) fn validate_tool_command(
    _tool_spec: &ToolSpec,
    _command_template: &str,
) -> Result<(), MediaPmError> {
    // Stub: passes all validation.
    Ok(())
}

// ── Platform-conditional path extraction ─────────────────────────────────

/// Extracts platform-conditional paths from a Nickel conditional expression.
///
/// Parses expressions of the form `${context.os == "linux" ? path1 : path2}`
/// into a map of platform → path.
#[must_use]
pub(super) fn extract_platform_conditional_paths(source: &str) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();

    for part in source.split("${") {
        if let Some(conditional) = part.split('}').next() {
            let trimmed = conditional.trim();
            if let Some(true_path) = trimmed.split("? ").nth(1) {
                if let Some((true_val, false_val)) = true_path.split_once(" : ") {
                    let os_name = if trimmed.contains("linux") {
                        "linux"
                    } else if trimmed.contains("macos") {
                        "macos"
                    } else if trimmed.contains("windows") {
                        "windows"
                    } else {
                        continue;
                    };
                    result
                        .insert(os_name.to_string(), true_val.trim().trim_matches('"').to_string());
                    // false path is the else branch
                    result.insert(
                        format!("!{os_name}"),
                        false_val.trim().trim_matches('"').to_string(),
                    );
                }
            }
        }
    }

    result
}

// ── Sandbox path normalization ───────────────────────────────────────────

/// Resolves a sandbox-relative path from a tool command's content_map entry.
#[must_use]
pub(super) fn resolve_sandbox_path(_content_map_key: &str) -> String {
    // Stub: returns an inputs-relative path.
    format!("inputs/{_content_map_key}")
}

/// Removes sandbox path prefix from a fully qualified sandbox path.
#[must_use]
pub(super) fn strip_sandbox_prefix(path: &str) -> &str {
    path.strip_prefix("inputs/").unwrap_or(path)
}
