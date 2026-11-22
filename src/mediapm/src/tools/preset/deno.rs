//! Preset for the `deno` managed tool.
//!
//! Delegates to [`crate::tools::workflows::deno::build_deno_spec`].

use std::collections::BTreeMap;

use crate::tools::workflows;
use crate::tools::workflows::definition::ToolDefinition;

/// Builds the [`ToolDefinition`] for `deno`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> ToolDefinition {
    workflows::deno::build_deno_spec(content_map, os_exec_paths)
}
