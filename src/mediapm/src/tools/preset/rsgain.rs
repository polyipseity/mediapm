//! Preset for the `rsgain` managed tool.
//!
//! Delegates to [`crate::tools::workflows::rsgain::build_rsgain_spec`].

use std::collections::BTreeMap;

use crate::tools::dependency::DependencyTypes;
use crate::tools::workflows;
use crate::tools::workflows::definition::ToolDefinition;

/// Returns the known `DependencyTypes` for each dependency of rsgain.
#[must_use]
pub(crate) fn dependency_types() -> BTreeMap<&'static str, DependencyTypes> {
    BTreeMap::from([("ffmpeg", DependencyTypes::CROSS_STEP), ("sd", DependencyTypes::CROSS_STEP)])
}

/// Builds the [`ToolDefinition`] for `rsgain`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> ToolDefinition {
    workflows::rsgain::build_rsgain_spec(content_map, os_exec_paths)
}
