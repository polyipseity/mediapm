//! Preset for the `media-tagger` managed tool.
//!
//! Delegates to [`crate::tools::workflows::media_tagger::build_media_tagger_spec`].

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::tools::dependency::DependencyTypes;
use crate::tools::workflows;

/// Returns the known `DependencyTypes` for each dependency of media-tagger.
#[must_use]
#[allow(dead_code)]
pub(crate) fn dependency_types() -> BTreeMap<&'static str, DependencyTypes> {
    BTreeMap::from([("ffmpeg", DependencyTypes::CROSS_STEP)])
}

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `media-tagger`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> (ToolSpec, ToolRuntime) {
    workflows::media_tagger::build_media_tagger_spec(content_map, os_exec_paths)
}
