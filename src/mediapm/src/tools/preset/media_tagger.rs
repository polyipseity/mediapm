//! Preset for the `media-tagger` managed tool.
//!
//! Delegates to [`crate::tools::workflows::media_tagger::build_media_tagger_spec`].

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::tools::workflows;

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `media-tagger`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    command_selector: &str,
) -> (ToolSpec, ToolRuntime) {
    workflows::media_tagger::build_media_tagger_spec(content_map, command_selector)
}
