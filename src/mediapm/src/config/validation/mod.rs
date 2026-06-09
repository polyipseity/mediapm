//! Schema validation for mediapm configuration documents.

use std::path::Path;

use crate::error::MediaPmError;

use super::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS, DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, MediaPmDocument,
    MediaRuntimeStorage, MediaSourceSpec, MediaStepTool, collect_playlist_media_index,
    flatten_hierarchy_nodes_for_runtime, normalize_selector_compare_value,
    normalize_selector_value, step_option_scalar,
};

mod hierarchy;
mod sources;

use self::hierarchy::validate_hierarchy_entries;
use self::sources::validate_media_source;

pub(super) fn validate_mediapm_state_document_shape(
    state_path: &Path,
    document: &MediaPmDocument,
) -> Result<(), MediaPmError> {
    let has_non_state_fields = document.runtime != MediaRuntimeStorage::default()
        || !document.tools.is_empty()
        || !document.media.is_empty()
        || !document.hierarchy.is_empty();

    if has_non_state_fields {
        return Err(MediaPmError::Workflow(format!(
            "{} must contain only top-level 'version' and 'state' properties",
            state_path.display()
        )));
    }

    Ok(())
}

/// Validates media-source schema invariants that require cross-field checks.
pub(super) fn validate_media_document(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    validate_tool_requirements(document)?;
    validate_runtime_materialization_preference_order(document)?;

    for (media_id, source) in &document.media {
        validate_media_source(media_id, source)?;
    }

    let flattened_hierarchy = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;
    let playlist_media_index = collect_playlist_media_index(&flattened_hierarchy)?;
    validate_hierarchy_entries(document, &playlist_media_index)?;
    Ok(())
}

/// Validates runtime-configured materialization method ordering.
fn validate_runtime_materialization_preference_order(
    document: &MediaPmDocument,
) -> Result<(), MediaPmError> {
    let Some(order) = document.runtime.materialization_preference_order.as_ref() else {
        return Ok(());
    };

    if order.is_empty() {
        return Err(MediaPmError::Workflow(
            "runtime.materialization_preference_order must contain at least one method".to_string(),
        ));
    }

    let mut seen = std::collections::BTreeSet::new();
    for method in order {
        if !seen.insert(*method) {
            return Err(MediaPmError::Workflow(format!(
                "runtime.materialization_preference_order contains duplicate method '{}'",
                method.as_label()
            )));
        }
    }

    Ok(())
}

/// Validates desired tool requirement selector invariants.
#[expect(
    clippy::too_many_lines,
    reason = "validation keeps cross-tool dependency and selector invariants in one auditable flow"
)]
fn validate_tool_requirements(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    for (tool_name, requirement) in &document.tools {
        let version = requirement.normalized_version();
        let tag = requirement.normalized_tag();

        // Builtin source-ingest tools (import) are never
        // downloader-provisioned, so they are not required to carry a release
        // selector.
        let requires_selector = !MediaStepTool::is_builtin_source_ingest_name(tool_name.as_str());

        if requires_selector && version.is_none() && tag.is_none() {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must define at least one selector: version or tag"
            )));
        }

        if let (Some(version), Some(tag)) = (&version, &tag)
            && normalize_selector_compare_value(version) != normalize_selector_compare_value(tag)
        {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' defines mismatched version '{version}' and tag '{tag}'; when both are provided they must refer to the same release selector"
            )));
        }

        if requirement
            .dependencies
            .ffmpeg_version
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(MediaPmError::Workflow(format!(
                "tools.{tool_name}.dependencies.ffmpeg_version must be non-empty when provided"
            )));
        }

        if requirement
            .dependencies
            .deno_version
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(MediaPmError::Workflow(format!(
                "tools.{tool_name}.dependencies.deno_version must be non-empty when provided"
            )));
        }

        if requirement
            .dependencies
            .sd_version
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(MediaPmError::Workflow(format!(
                "tools.{tool_name}.dependencies.sd_version must be non-empty when provided"
            )));
        }

        let has_ffmpeg_dependency = requirement.dependencies.ffmpeg_version.is_some();
        let has_deno_dependency = requirement.dependencies.deno_version.is_some();
        let has_sd_dependency = requirement.dependencies.sd_version.is_some();
        let is_media_tagger = tool_name.eq_ignore_ascii_case("media-tagger");
        let is_yt_dlp = tool_name.eq_ignore_ascii_case("yt-dlp");
        let is_rsgain = tool_name.eq_ignore_ascii_case("rsgain");

        if is_media_tagger {
            if has_deno_dependency || has_sd_dependency {
                return Err(MediaPmError::Workflow(format!(
                    "tool '{tool_name}' must not define tools.{tool_name}.dependencies.deno_version or tools.{tool_name}.dependencies.sd_version; only tools.yt-dlp.dependencies.deno_version and tools.rsgain.dependencies.sd_version are supported"
                )));
            }

            ensure_inherit_dependency_target_is_configured(
                document,
                tool_name,
                "ffmpeg",
                requirement.dependencies.ffmpeg_version.as_deref(),
            )?;
        } else if is_yt_dlp {
            if has_sd_dependency {
                return Err(MediaPmError::Workflow(format!(
                    "tool '{tool_name}' must not define tools.{tool_name}.dependencies.sd_version; only tools.rsgain.dependencies.sd_version is supported"
                )));
            }

            ensure_inherit_dependency_target_is_configured(
                document,
                tool_name,
                "ffmpeg",
                requirement.dependencies.ffmpeg_version.as_deref(),
            )?;
            ensure_inherit_dependency_target_is_configured(
                document,
                tool_name,
                "deno",
                requirement.dependencies.deno_version.as_deref(),
            )?;
        } else if is_rsgain {
            // rsgain may define both ffmpeg and sd dependency selectors.
            if has_deno_dependency {
                return Err(MediaPmError::Workflow(format!(
                    "tool '{tool_name}' must not define tools.{tool_name}.dependencies.deno_version; only tools.yt-dlp.dependencies.deno_version is supported"
                )));
            }

            ensure_inherit_dependency_target_is_configured(
                document,
                tool_name,
                "ffmpeg",
                requirement.dependencies.ffmpeg_version.as_deref(),
            )?;
            ensure_inherit_dependency_target_is_configured(
                document,
                tool_name,
                "sd",
                requirement.dependencies.sd_version.as_deref(),
            )?;
        } else if has_ffmpeg_dependency || has_deno_dependency || has_sd_dependency {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must not define dependency selector overrides; only tools.yt-dlp.dependencies.ffmpeg_version, tools.yt-dlp.dependencies.deno_version, tools.media-tagger.dependencies.ffmpeg_version, tools.rsgain.dependencies.ffmpeg_version, and tools.rsgain.dependencies.sd_version are supported"
            )));
        }

        if tool_name.eq_ignore_ascii_case("ffmpeg") {
            if requirement.max_input_slots_or_default() == 0 {
                return Err(MediaPmError::Workflow(format!(
                    "tools.ffmpeg.max_input_slots must be at least 1 (default {DEFAULT_FFMPEG_MAX_INPUT_SLOTS})",
                )));
            }

            if requirement.max_output_slots_or_default() == 0 {
                return Err(MediaPmError::Workflow(format!(
                    "tools.ffmpeg.max_output_slots must be at least 1 (default {DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS})",
                )));
            }
        } else if requirement.max_input_slots.is_some() || requirement.max_output_slots.is_some() {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must not define ffmpeg slot settings; only tools.ffmpeg.max_input_slots and tools.ffmpeg.max_output_slots are supported"
            )));
        }
    }

    Ok(())
}

/// Enforces that inherit/global dependency selectors have a configured source
/// tool entry to inherit from.
fn ensure_inherit_dependency_target_is_configured(
    document: &MediaPmDocument,
    logical_tool_name: &str,
    dependency_tool_name: &str,
    selector: Option<&str>,
) -> Result<(), MediaPmError> {
    let selector = normalize_selector_value(selector).unwrap_or_else(|| "inherit".to_string());

    if !selector.eq_ignore_ascii_case("inherit") && !selector.eq_ignore_ascii_case("global") {
        return Ok(());
    }

    if document.tools.contains_key(dependency_tool_name) {
        return Ok(());
    }

    Err(MediaPmError::Workflow(format!(
        "tools.{logical_tool_name}.dependencies.{dependency_tool_name}_version='{selector}' requires tools.{dependency_tool_name} to be configured so there is a managed dependency to inherit"
    )))
}

/// Parses supported hierarchy placeholders from one hierarchy key.
///
/// Supported placeholders:
/// - `${media.id}`
/// - `${media.metadata.<key>}`
///
/// Returns each referenced `${media.metadata.<key>}` key in first-seen order.
pub(crate) fn hierarchy_metadata_placeholder_keys(
    hierarchy_path: &str,
) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut cursor = 0usize;

    while let Some(relative_start) = hierarchy_path[cursor..].find("${") {
        let placeholder_start = cursor + relative_start;
        let after_marker = &hierarchy_path[placeholder_start + 2..];
        let Some(relative_end) = after_marker.find('}') else {
            return Err("missing closing '}' for placeholder".to_string());
        };

        let expression = &after_marker[..relative_end];
        let expression = expression.trim();

        if expression == "media.id" {
            cursor = placeholder_start + 2 + relative_end + 1;
            continue;
        }

        let metadata_key = expression
            .strip_prefix("media.metadata.")
            .ok_or_else(|| {
                format!(
                    "unsupported placeholder '${{{expression}}}'; only '${{media.id}}' and '${{media.metadata.<key>}}' are supported"
                )
            })?
            .trim();

        if metadata_key.is_empty() {
            return Err(format!(
                "placeholder '${{{expression}}}' must reference a non-empty metadata key"
            ));
        }

        keys.push(metadata_key.to_string());
        cursor = placeholder_start + 2 + relative_end + 1;
    }

    Ok(keys)
}

/// Returns one source URI string for diagnostics/materialization bookkeeping.
#[must_use]
pub(crate) fn media_source_uri(media_id: &str, source: &MediaSourceSpec) -> String {
    source
        .steps
        .iter()
        .find_map(|step| {
            if step.tool.is_online_media_downloader() {
                step_option_scalar(step, "uri").map(ToString::to_string)
            } else {
                None
            }
        })
        .unwrap_or_else(|| format!("local:{media_id}"))
}
