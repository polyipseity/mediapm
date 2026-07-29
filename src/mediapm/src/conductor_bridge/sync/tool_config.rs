//! Tool-config generation and companion-dependency binding.
//!
//! This module resolves companion tool selectors (ffmpeg, deno for yt-dlp),
//! and manages content-map prefixing for same-step companion dependencies.

use std::collections::BTreeMap;

use crate::config::ToolRequirement;
use crate::config::source_types;

/// Resolves the companion ffmpeg selection for a tool from its dependency config.
///
/// Searches all tool requirements for the first non-default `ffmpeg_version`
/// literal dependency value and returns it. Returns `None` when no override
/// is specified (the global default applies).
#[must_use]
pub(super) fn resolve_companion_ffmpeg_selection(
    requirements: &BTreeMap<String, serde_json::Value>,
) -> Option<String> {
    for value in requirements.values() {
        if let Ok(requirement) = serde_json::from_value::<ToolRequirement>(value.clone())
            && let source_types::MediaMetadataValue::Literal(v) =
                &requirement.dependencies.ffmpeg_version
            && !v.is_empty()
            && !v.eq_ignore_ascii_case("inherit")
        {
            return Some(v.clone());
        }
    }
    None
}

/// Resolves the companion deno selection for a tool from its dependency config.
///
/// Searches all tool requirements for the first non-default `deno_version`
/// literal dependency value and returns it. Returns `None` when no override
/// is specified (the global default applies).
#[must_use]
pub(super) fn resolve_companion_deno_selection(
    requirements: &BTreeMap<String, serde_json::Value>,
) -> Option<String> {
    for value in requirements.values() {
        if let Ok(requirement) = serde_json::from_value::<ToolRequirement>(value.clone())
            && let source_types::MediaMetadataValue::Literal(v) =
                &requirement.dependencies.deno_version
            && !v.is_empty()
            && !v.eq_ignore_ascii_case("inherit")
        {
            return Some(v.clone());
        }
    }
    None
}

/// Prefixes same-step companion content-map entries to distinguish them from
/// the requester's own payload entries.
#[allow(dead_code)]
#[must_use]
pub(super) fn prefix_same_step_companion_content_map(
    companion_prefix: &str,
    companion_content_map: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    companion_content_map
        .iter()
        .map(|(k, v)| (format!("{companion_prefix}{k}"), v.clone()))
        .collect()
}

/// Prefixes same-step companion content-map entries (value type is
/// [`mediapm_conductor::Hash`] string representation).
#[allow(dead_code)]
#[must_use]
pub(super) fn prefix_same_step_companion_content_entries(
    companion_prefix: &str,
    entries: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    prefix_same_step_companion_content_map(companion_prefix, entries)
}
