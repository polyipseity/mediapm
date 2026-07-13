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
    command_selector: &str,
) -> (ToolSpec, ToolRuntime) {
    workflows::rsgain::build_rsgain_spec(content_map, command_selector)
}
