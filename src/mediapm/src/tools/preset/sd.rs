//! Preset for the `sd` managed tool.
//!
//! Delegates to [`crate::tools::workflows::sd::build_sd_spec`].

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::tools::workflows;

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `sd`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    command_selector: &str,
) -> (ToolSpec, ToolRuntime) {
    workflows::sd::build_sd_spec(content_map, command_selector)
}
