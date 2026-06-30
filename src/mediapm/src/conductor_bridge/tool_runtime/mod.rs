//! Managed-tool runtime contract dispatcher.
//!
//! This module dispatches [`ToolSpec`] and [`ToolRuntime`] construction to
//! per-tool workflow modules. It also defines the shared [`FfmpegSlotLimits`]
//! type used by the ffmpeg spec builder and the sync pipeline.
//!
//! Sub-modules:
//! - [`template`] — command-template validation and platform-path extraction

pub(crate) mod template;

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::{
    conductor_bridge::constants::{
        DEFAULT_FFMPEG_MAX_INPUT_SLOTS, DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS,
    },
    tools::workflows,
};

/// ffmpeg slot-limit configuration derived from tool requirements.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FfmpegSlotLimits {
    /// Maximum number of ffmpeg input content / cover-art slots.
    pub(crate) max_input_slots: u32,
    /// Maximum number of ffmpeg indexed output-file slots.
    pub(crate) max_output_slots: u32,
}

/// Resolves ffmpeg slot limits from config default or overrides.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn resolve_ffmpeg_slot_limits(
    max_input: Option<u32>,
    max_output: Option<u32>,
) -> FfmpegSlotLimits {
    FfmpegSlotLimits {
        max_input_slots: max_input.unwrap_or(DEFAULT_FFMPEG_MAX_INPUT_SLOTS as u32),
        max_output_slots: max_output.unwrap_or(DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS as u32),
    }
}

/// Builds a full [`ToolSpec`] and [`ToolRuntime`] for one managed tool by
/// delegating to the appropriate per-tool workflow module.
pub(crate) fn build_tool_spec(
    tool_name: &str,
    content_map: BTreeMap<String, String>,
    command_path: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> (ToolSpec, ToolRuntime) {
    match tool_name {
        n if n.eq_ignore_ascii_case("yt-dlp") => {
            workflows::yt_dlp::build_yt_dlp_spec(content_map, command_path)
        }
        n if n.eq_ignore_ascii_case("ffmpeg") => {
            workflows::ffmpeg::build_ffmpeg_spec(content_map, command_path, ffmpeg_slot_limits)
        }
        n if n.eq_ignore_ascii_case("rsgain") => {
            workflows::rsgain::build_rsgain_spec(content_map, command_path)
        }
        n if n.eq_ignore_ascii_case("media-tagger") => {
            workflows::media_tagger::build_media_tagger_spec(content_map, command_path)
        }
        n if n.eq_ignore_ascii_case("sd") => {
            workflows::sd::build_sd_spec(content_map, command_path)
        }
        _ => panic!("unknown managed tool: {tool_name}"),
    }
}
