//! Preset for the `sd` managed tool.
//!
//! Delegates to [`crate::tools::workflows::sd::build_sd_spec`].

use std::collections::BTreeMap;

use crate::tools::workflows;
use crate::tools::workflows::definition::ToolDefinition;

/// Builds the [`ToolDefinition`] for `sd`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> ToolDefinition {
    workflows::sd::build_sd_spec(content_map, os_exec_paths)
}
