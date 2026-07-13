//! Preset for the `deno` managed tool.
//!
//! Delegates to [`crate::tools::workflows::deno::build_deno_spec`].

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::tools::workflows;

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `deno`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> (ToolSpec, ToolRuntime) {
    workflows::deno::build_deno_spec(content_map, os_exec_paths)
}
