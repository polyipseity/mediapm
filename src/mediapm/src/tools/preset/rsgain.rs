//! Preset for the `rsgain` managed tool.
//!
//! Delegates to [`crate::tools::workflows::rsgain::build_rsgain_spec`].

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::tools::workflows;

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `rsgain`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> (ToolSpec, ToolRuntime) {
    workflows::rsgain::build_rsgain_spec(content_map, os_exec_paths)
}
