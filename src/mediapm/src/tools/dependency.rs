//! Dependency type classification for managed-tool dependencies.
//!
//! `DependencyType` determines how a dependency is provisioned:
//! - `SameStep`: payload is inlined into the same conductor step (companion).
//! - `CrossStep`: dependency runs as a separate conductor workflow step.
//! - `Both`: functions as both.
//!
//! The registry function [`known_dependency_type`] provides per-tool lookup.
//! The [`validate_dependency_keys`] function checks all dependency keys for a
//! tool and produces `MPM-E001` errors with "did you mean" suggestions via
//! the [`similar`] crate.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_conductor::tools::provider::VersionSpec;
use similar::get_close_matches;

use crate::error::MediaPmError;

/// Dependency provisioning type for a managed-tool dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DependencyType {
    /// Dependency payload is inlined into the same conductor step as the
    /// primary tool (same-step companion).
    SameStep,
    /// Dependency runs as a separate conductor workflow step (cross-step).
    CrossStep,
    /// Functions as both same-step companion AND cross-step tool.
    #[allow(dead_code)]
    Both,
}

/// Looks up the known `DependencyType` for a dependency of a given tool.
///
/// Returns `None` if the tool or dependency is unknown.
#[must_use]
#[allow(dead_code)]
pub(crate) fn known_dependency_type(tool_id: &str, dep_id: &str) -> Option<DependencyType> {
    known_dependency_type_for_tool(tool_id).and_then(|types| types.get(dep_id).copied())
}

/// Returns the full dependency type map for a tool, or `None` if unknown.
#[must_use]
pub(crate) fn known_dependency_type_for_tool(
    tool_id: &str,
) -> Option<BTreeMap<&'static str, DependencyType>> {
    match tool_id {
        "yt-dlp" => Some(super::preset::yt_dlp::dependency_types()),
        "media-tagger" => Some(super::preset::media_tagger::dependency_types()),
        "rsgain" => Some(super::preset::rsgain::dependency_types()),
        _ => None,
    }
}

/// Find close matches for a dependency key against the given candidates.
///
/// First checks a `_version` suffix heuristic (e.g., `ffmpeg_version` → `ffmpeg`),
/// then falls back to [`similar::get_close_matches`] for general fuzzy matching.
fn find_closest_dep_key(dep_key: &str, valid_keys: &BTreeSet<String>) -> Vec<String> {
    // Always check `_version` suffix stripping as an explicit heuristic.
    // This handles the `ffmpeg_version` → `ffmpeg` case that edit distance
    // would place at the boundary of a 0.6 cutoff.
    if let Some(bare) = dep_key.strip_suffix("_version") {
        if valid_keys.contains(bare) {
            return vec![bare.to_string()];
        }
    }
    // Fall back to edit-distance-based fuzzy matching via similar crate.
    let candidates: Vec<&str> = valid_keys.iter().map(String::as_str).collect();
    get_close_matches(dep_key, &candidates, 3, 0.6).into_iter().map(|s| s.to_string()).collect()
}

/// Collect the set of valid dependency key strings for a given tool.
///
/// Valid keys are the union of:
/// - All tool IDs configured in the `desired_tools` map.
/// - Known dependency types registered via per-preset `dependency_types()`.
fn collect_valid_dep_keys(
    tool_id: &str,
    desired_tools: &BTreeMap<String, serde_json::Value>,
) -> BTreeSet<String> {
    let mut valid = BTreeSet::new();
    valid.extend(desired_tools.keys().cloned());
    if let Some(known) = known_dependency_type_for_tool(tool_id) {
        valid.extend(known.into_keys().map(String::from));
    }
    valid
}

/// Validate all dependency keys for a tool. Returns `MPM-E001` on unknown key
/// with "did you mean" suggestion.
///
/// # Errors
///
/// Returns [`MediaPmError::ConfigValidation`] with code `MPM-E001` if any
/// dependency key is not in the valid set.
pub(crate) fn validate_dependency_keys(
    tool_id: &str,
    dependencies: &BTreeMap<String, VersionSpec>,
    desired_tools: &BTreeMap<String, serde_json::Value>,
) -> Result<(), MediaPmError> {
    let valid = collect_valid_dep_keys(tool_id, desired_tools);
    for dep_key in dependencies.keys() {
        if valid.contains(dep_key.as_str()) {
            continue;
        }
        let close = find_closest_dep_key(dep_key, &valid);
        let suggestion = if close.is_empty() {
            format!("valid dependency keys for \"{tool_id}\": {}", sorted_join(&valid))
        } else {
            let close_str = close.join("\", \"");
            format!(
                "did you mean \"{close_str}\"? valid dependency keys for \"{tool_id}\": {}",
                sorted_join(&valid)
            )
        };
        return Err(MediaPmError::ConfigValidation {
            code: "MPM-E001",
            context: format!("tool \"{tool_id}\" has undefined dependency \"{dep_key}\""),
            detail: format!("\"{dep_key}\" is not a recognized dependency for \"{tool_id}\""),
            suggestion,
        });
    }
    Ok(())
}

/// Join a set of strings into a human-readable sorted list.
fn sorted_join(set: &BTreeSet<String>) -> String {
    let v: Vec<&str> = set.iter().map(String::as_str).collect();
    v.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_desired_tools(ids: &[&str]) -> BTreeMap<String, serde_json::Value> {
        let mut map = BTreeMap::new();
        for id in ids {
            map.insert(id.to_string(), serde_json::json!({}));
        }
        map
    }

    #[test]
    fn validate_unknown_dep_key_error() {
        let desired = make_desired_tools(&["ffmpeg"]);
        let mut deps = BTreeMap::new();
        deps.insert("nonexistent_dep".to_string(), VersionSpec::Latest);
        let result = validate_dependency_keys("yt-dlp", &deps, &desired);
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("yt-dlp"), "error should mention tool");
        assert!(msg.contains("nonexistent_dep"), "error should mention dep key");
        assert!(msg.contains("ffmpeg"), "error should suggest valid deps");
    }

    #[test]
    fn validate_version_suffix_suggests_bare_id() {
        let desired = make_desired_tools(&["ffmpeg"]);
        let mut deps = BTreeMap::new();
        deps.insert("ffmpeg_version".to_string(), VersionSpec::Latest);
        let result = validate_dependency_keys("yt-dlp", &deps, &desired);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("ffmpeg"), "should suggest correct key");
        assert!(msg.contains("ffmpeg_version"), "should reference used key");
        assert!(msg.contains("did you mean"), "should contain 'did you mean' hint");
    }

    #[test]
    fn validate_known_dep_key_passes() {
        let desired = make_desired_tools(&["ffmpeg", "deno"]);
        let mut deps = BTreeMap::new();
        deps.insert("ffmpeg".to_string(), VersionSpec::Latest);
        deps.insert("deno".to_string(), VersionSpec::Latest);
        let result = validate_dependency_keys("yt-dlp", &deps, &desired);
        assert!(result.is_ok(), "known dep keys should pass: {:?}", result.err());
    }

    #[test]
    fn validate_empty_deps_passes() {
        let desired = make_desired_tools(&[]);
        let deps = BTreeMap::new();
        let result = validate_dependency_keys("yt-dlp", &deps, &desired);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_close_match_via_similar() {
        // Test the get_close_matches integration with a mild typo.
        let desired = make_desired_tools(&["ffmpeg"]);
        let mut deps = BTreeMap::new();
        deps.insert("ffmepg".to_string(), VersionSpec::Latest);
        let result = validate_dependency_keys("yt-dlp", &deps, &desired);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("ffmepg"), "should reference used key");
        assert!(msg.contains("did you mean"), "should suggest correct key");
        assert!(msg.contains("ffmpeg"), "suggestion should include ffmpeg");
    }
}
