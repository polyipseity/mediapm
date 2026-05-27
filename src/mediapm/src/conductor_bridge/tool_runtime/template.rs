//! Template literal escaping, command validation, and platform-conditional path helpers.

use std::collections::BTreeMap;
use std::path::Path;

use mediapm_cas::Hash;
use mediapm_conductor::{ToolKindSpec, ToolSpec};

use crate::error::MediaPmError;

use super::{
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV, MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV,
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV,
};

/// Escapes plain string literals for conductor template rendering.
#[must_use]
pub(super) fn escape_template_literal(value: &str) -> String {
    value.replace('\\', "\\\\")
}

/// Resolves the internal media-tagger launcher env var key for one host OS.
pub(in crate::conductor_bridge) fn media_tagger_launcher_mediapm_env_var_for_host()
-> Result<&'static str, MediaPmError> {
    media_tagger_launcher_mediapm_env_var_for_os(std::env::consts::OS)
}

/// Resolves the internal media-tagger launcher env var key for one target OS.
pub(super) fn media_tagger_launcher_mediapm_env_var_for_os(
    os: &str,
) -> Result<&'static str, MediaPmError> {
    match os {
        "windows" => Ok(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV),
        "linux" => Ok(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV),
        "macos" => Ok(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV),
        other => Err(MediaPmError::Workflow(format!(
            "unsupported host platform '{other}' for internal media-tagger launcher env mapping"
        ))),
    }
}

/// Validates executable command selectors against generated content-map keys.
///
/// For platform-conditional selectors, every declared platform branch must map
/// to at least one `content_map` key so planned tool payloads stay
/// cross-platform complete.
pub(in crate::conductor_bridge) fn validate_tool_command(
    tool_name: &str,
    command_vector: &[String],
    content_map: &BTreeMap<String, Hash>,
) -> Result<(), MediaPmError> {
    let Some(binary) = command_vector.first() else {
        return Err(MediaPmError::Workflow(format!("tool '{tool_name}' command is empty")));
    };

    if binary.contains("context.os") {
        let selectors = extract_platform_conditional_paths(binary)?;

        for (target, path) in selectors {
            if !content_map_contains_command_target(content_map, &path) {
                return Err(MediaPmError::Workflow(format!(
                    "tool '{tool_name}' command selector for '{target}' references '{path}', but content_map has no such key"
                )));
            }
        }
        return Ok(());
    }

    if !content_map_contains_command_target(content_map, binary) {
        return Err(MediaPmError::Workflow(format!(
            "tool '{tool_name}' command target '{binary}' is missing from content_map"
        )));
    }

    Ok(())
}

/// Returns true when one command target can be materialized by `content_map`.
///
/// Supported matches:
/// - direct file key equality (`target == key`),
/// - directory ZIP keys ending with `/` or `\\` where `target` is under that
///   directory,
/// - root ZIP keys (`./` or `.\\`) that materialize all relative paths.
pub(super) fn content_map_contains_command_target(
    content_map: &BTreeMap<String, Hash>,
    target: &str,
) -> bool {
    if content_map.contains_key(target) {
        return true;
    }

    let normalized_target = normalize_sandbox_relative_path(target);
    for key in content_map.keys() {
        let normalized_key = normalize_sandbox_relative_path(key);
        if normalized_key == "./" {
            return true;
        }

        if key.ends_with('/') || key.ends_with('\\') {
            let prefix = normalized_key.trim_start_matches("./");
            if prefix.is_empty() || normalized_target.starts_with(prefix) {
                return true;
            }
        }
    }

    false
}

/// Normalizes one sandbox-relative path key/value to slash-separated text.
pub(super) fn normalize_sandbox_relative_path(value: &str) -> String {
    value.replace('\\', "/")
}

/// Parses `${context.os == "<target>" ? <path> | <fallback>}` selector
/// paths from one command token.
pub(in crate::conductor_bridge) fn extract_platform_conditional_paths(
    template: &str,
) -> Result<BTreeMap<String, String>, MediaPmError> {
    let mut result = BTreeMap::new();
    let mut cursor = 0usize;

    while let Some(start_rel) = template[cursor..].find("${") {
        let start = cursor + start_rel;
        let remainder = &template[start + 2..];
        let Some(end_rel) = remainder.find('}') else {
            return Err(MediaPmError::Workflow(format!(
                "invalid command selector '{template}': missing closing '}}'"
            )));
        };
        let token = &remainder[..end_rel];

        if let Some((target, value)) = parse_platform_conditional_path_token(token)? {
            result.insert(target, value);
        }

        cursor = start + 2 + end_rel + 1;
    }

    if result.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "tool command '{template}' did not contain any context.os selectors"
        )));
    }

    Ok(result)
}

/// Parses one `${...}` token into a platform target/path selector when present.
pub(super) fn parse_platform_conditional_path_token(
    token: &str,
) -> Result<Option<(String, String)>, MediaPmError> {
    if !token.contains("context.os") {
        return Ok(None);
    }

    let Some((condition, branches)) = token.split_once('?') else {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; expected '?<true>|<false>'"
        )));
    };
    let Some((true_branch, _false_branch)) = branches.split_once('|') else {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; expected '<true>|<false>'"
        )));
    };

    let condition = condition.trim();
    let Some(remainder) = condition.strip_prefix("context.os") else {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; condition must start with 'context.os'"
        )));
    };
    let remainder = remainder.trim_start();
    let Some(remainder) = remainder.strip_prefix("==") else {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; condition must use '=='"
        )));
    };
    let target = parse_quoted_selector_value(remainder.trim()).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; target must be quoted"
        ))
    })?;

    let true_branch = true_branch.trim();
    let path = if let Some(decoded) = parse_quoted_selector_value(true_branch) {
        decoded
    } else {
        true_branch.to_string()
    };
    if path.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "invalid platform selector '${{{token}}}' for tool command; true branch path is empty"
        )));
    }

    Ok(Some((target, path)))
}

/// Parses one single- or double-quoted selector fragment.
#[must_use]
pub(super) fn parse_quoted_selector_value(value: &str) -> Option<String> {
    if value.len() < 2 {
        return None;
    }
    let first = value.chars().next()?;
    let last = value.chars().last()?;
    if !((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
        return None;
    }

    Some(value[first.len_utf8()..value.len() - last.len_utf8()].to_string())
}

/// Returns whether one stored tool specification currently points to a
/// workspace-local executable binary that exists on disk.
pub(in crate::conductor_bridge) fn tool_spec_has_binary(spec: &ToolSpec) -> bool {
    let ToolKindSpec::Executable { command, .. } = &spec.kind else {
        return false;
    };
    let Some(first) = command.first() else {
        return false;
    };
    Path::new(first).exists()
}
