//! Managed-tool runtime contract dispatcher.
//!
//! This module dispatches [`ToolSpec`] and [`ToolRuntime`] construction to
//! per-tool workflow modules. It also defines the shared [`FfmpegSlotLimits`]
//! type used by the ffmpeg spec builder and the sync pipeline.

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::tools::preset;

/// ffmpeg slot-limit configuration derived from tool requirements.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FfmpegSlotLimits {
    /// Maximum number of ffmpeg input content / cover-art slots.
    pub(crate) max_input_slots: usize,
    /// Maximum number of ffmpeg indexed output-file slots.
    pub(crate) max_output_slots: usize,
}

/// Resolves ffmpeg slot limits from config default or overrides.
///
/// Config defaults and tool-requirement overrides carry `u32` counts
/// (`DEFAULT_FFMPEG_MAX_INPUT_SLOTS` / `DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS` in
/// `crate::config::defaults`), while spec builders and the workflow
/// synthesizer consume `usize`; the conversion happens here so callers never
/// cast.
#[must_use]
pub(crate) fn resolve_ffmpeg_slot_limits(max_input: u32, max_output: u32) -> FfmpegSlotLimits {
    FfmpegSlotLimits { max_input_slots: max_input as usize, max_output_slots: max_output as usize }
}

/// Builds a full [`ToolSpec`] and [`ToolRuntime`] for one managed tool by
/// delegating to the appropriate per-tool preset.
pub(crate) fn build_tool_spec(
    tool_name: &str,
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> (ToolSpec, ToolRuntime) {
    preset::apply_preset(tool_name, content_map, os_exec_paths, ffmpeg_slot_limits)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::conductor_bridge::constants::OUTPUT_SANDBOX_ARTIFACTS;

    /// Every managed tool name resolves to a spec through one of the two doors
    /// the sync uses: a fetched tool through the preset dispatcher, a host
    /// builtin through the definition the crate supplying its runner writes.
    ///
    /// media-tagger is the second kind, and it is named here, not dropped: a
    /// name in this list that nothing answers for is what this asserts.
    #[test]
    fn build_tool_spec_resolves_all_managed_tools() {
        let content_map = BTreeMap::new();
        let limits = FfmpegSlotLimits::default();

        for tool_name in &["deno", "yt-dlp", "ffmpeg", "rsgain", "sd"] {
            let os_exec_paths = BTreeMap::from([("linux".into(), (*tool_name).to_string())]);
            let (spec, runtime) =
                build_tool_spec(tool_name, content_map.clone(), &os_exec_paths, limits);
            assert_eq!(spec.name, *tool_name);
            assert_eq!(runtime.content_map, content_map);
        }

        let host_builtin = crate::builtins::host_builtin_tool_spec("media-tagger")
            .expect("the host builtin's definition must resolve for the tool a library declares");
        assert_eq!(host_builtin.name, "media-tagger");
        assert_eq!(
            host_builtin.kind,
            mediapm_conductor::ToolKindSpec::Builtin { builtin_id: "media-tagger".to_string() }
        );
    }

    /// Sandbox artifacts folder varies per tool: yt-dlp uses `downloads`,
    /// media-tagger uses `coverart`, others use `inputs`.
    #[test]
    fn sandbox_artifacts_folder_differs_by_tool() {
        let content_map = BTreeMap::new();
        let limits = FfmpegSlotLimits::default();

        let yt_os = BTreeMap::from([("linux".into(), "yt-dlp".into())]);
        let yt_dlp_outputs =
            build_tool_spec("yt-dlp", content_map.clone(), &yt_os, limits).0.outputs;
        assert_eq!(
            yt_dlp_outputs[OUTPUT_SANDBOX_ARTIFACTS].capture, "folder:downloads",
            "yt-dlp sandbox folder"
        );

        // A host builtin has no payload, so its outputs come from the
        // definition its runner answers for rather than from a preset.
        let media_tagger = crate::builtins::host_builtin_tool_spec("media-tagger")
            .expect("the host builtin's definition must resolve");
        assert_eq!(
            media_tagger.outputs[OUTPUT_SANDBOX_ARTIFACTS].capture, "folder:coverart",
            "media-tagger sandbox folder"
        );

        for tool_name in &["ffmpeg", "rsgain", "sd", "deno"] {
            let os_exec_paths = BTreeMap::from([("linux".into(), (*tool_name).to_string())]);
            let outputs =
                build_tool_spec(tool_name, content_map.clone(), &os_exec_paths, limits).0.outputs;
            assert_eq!(
                outputs[OUTPUT_SANDBOX_ARTIFACTS].capture, "folder:inputs",
                "{tool_name} sandbox folder"
            );
        }
    }
}
