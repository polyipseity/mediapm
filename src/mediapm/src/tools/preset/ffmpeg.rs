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
    os_exec_paths: &BTreeMap<String, String>,
    slot_limits: FfmpegSlotLimits,
) -> (ToolSpec, ToolRuntime) {
    workflows::ffmpeg::build_ffmpeg_spec(content_map, os_exec_paths, slot_limits)
}
