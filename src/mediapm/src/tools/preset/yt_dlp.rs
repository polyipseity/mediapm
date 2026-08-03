//! Preset for the `yt-dlp` managed tool.
//!
//! Delegates to [`crate::tools::workflows::yt_dlp::build_yt_dlp_spec`].

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::tools::dependency::DependencyTypes;
use crate::tools::workflows;

/// Returns the known `DependencyTypes` for each dependency of yt-dlp.
#[must_use]
#[allow(dead_code)]
pub(crate) fn dependency_types() -> BTreeMap<&'static str, DependencyTypes> {
    BTreeMap::from([("ffmpeg", DependencyTypes::SAME_STEP), ("deno", DependencyTypes::SAME_STEP)])
}

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `yt-dlp`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> (ToolSpec, ToolRuntime) {
    workflows::yt_dlp::build_yt_dlp_spec(content_map, os_exec_paths)
}
