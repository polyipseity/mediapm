//! Dependency declarations for the `media-tagger` managed tool.
//!
//! The tool has no preset of its own any more: it is a builtin mediapm's host
//! runs, so its [`ToolSpec`](mediapm_conductor::ToolSpec) is written by
//! [`crate::builtins::host_builtin_tool_spec`] rather than built from a fetched
//! content map. What stays here is the dependency declaration the sync gates on.

use std::collections::BTreeMap;

use crate::tools::dependency::DependencyTypes;

/// Returns the known `DependencyTypes` for each dependency of media-tagger.
#[must_use]
pub(crate) fn dependency_types() -> BTreeMap<&'static str, DependencyTypes> {
    BTreeMap::from([("ffmpeg", DependencyTypes::CROSS_STEP)])
}
