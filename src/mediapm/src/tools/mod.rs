//! Managed-tool preset, provider, downloader, and workflow synthesis.
//!
//! This module groups tool provisioning (provider sources, preset
//! configuration) and per-tool workflow step synthesis under one folder
//! module so callers reason about them in one place.
//!
//! - **Preset** — per-tool [`ToolSpec`] / [`ToolRuntime`] builders
//! - **Provider** — per-tool source definitions for the 3-phase pipeline
//! - **Workflows** — step synthesis and shared spec helpers

pub(crate) mod downloader;
pub(crate) mod preset;
pub(crate) mod provider;
pub(crate) mod workflows;

/// Returns `true` when `tool_id` is a recognised managed tool.
#[must_use]
pub(crate) fn is_known_tool_id(tool_id: &str) -> bool {
    ["ffmpeg", "yt-dlp", "deno", "rsgain", "media-tagger", "sd"]
        .iter()
        .any(|&known| known.eq_ignore_ascii_case(tool_id))
}
