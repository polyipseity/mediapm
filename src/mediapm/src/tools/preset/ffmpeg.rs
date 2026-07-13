//! Preset for the `ffmpeg` managed tool.
//!
//! Delegates to [`crate::tools::workflows::ffmpeg::build_ffmpeg_spec`].

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::conductor_bridge::tool_runtime::FfmpegSlotLimits;
use crate::tools::workflows;

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `ffmpeg`.
#[must_use]
pub(crate) fn apply(
    content_map: BTreeMap<String, String>,
    command_selector: &str,
    slot_limits: FfmpegSlotLimits,
) -> (ToolSpec, ToolRuntime) {
    workflows::ffmpeg::build_ffmpeg_spec(content_map, command_selector, slot_limits)
}
