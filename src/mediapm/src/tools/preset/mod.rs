//! Managed-tool preset dispatcher.
//!
//! Routes tool names to the appropriate per-tool spec builder, delegating
//! to [`crate::tools::workflows`] for the actual spec construction. A tool
//! mediapm's host runs as a builtin is not here: it has no fetched payload to
//! build a spec from, so its definition comes from
//! [`crate::builtins::host_builtin_tool_spec`] instead.

pub(crate) mod deno;
pub(crate) mod ffmpeg;
pub(crate) mod media_tagger;
pub(crate) mod rsgain;
pub(crate) mod sd;
pub(crate) mod yt_dlp;

use std::collections::BTreeMap;

use mediapm_conductor::{ToolRuntime, ToolSpec};

use crate::conductor_bridge::tool_runtime::FfmpegSlotLimits;

/// Builds a [`ToolSpec`] and [`ToolRuntime`] for the named managed tool.
///
/// # Panics
///
/// Panics if `tool_name` is not a recognized managed tool.
#[must_use]
pub(crate) fn apply_preset(
    tool_name: &str,
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
    slot_limits: FfmpegSlotLimits,
) -> (ToolSpec, ToolRuntime) {
    match tool_name {
        n if n.eq_ignore_ascii_case("ffmpeg") => {
            ffmpeg::apply(content_map, os_exec_paths, slot_limits)
        }
        n if n.eq_ignore_ascii_case("yt-dlp") => yt_dlp::apply(content_map, os_exec_paths),
        n if n.eq_ignore_ascii_case("deno") => deno::apply(content_map, os_exec_paths),
        n if n.eq_ignore_ascii_case("rsgain") => rsgain::apply(content_map, os_exec_paths),
        n if n.eq_ignore_ascii_case("sd") => sd::apply(content_map, os_exec_paths),
        _ => panic!("unknown managed tool: {tool_name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conductor_bridge::tool_runtime::FfmpegSlotLimits;

    #[test]
    fn apply_preset_routes_all_managed_tools() {
        let empty_map = BTreeMap::new();
        let empty_paths = BTreeMap::new();
        let defaults = FfmpegSlotLimits::default();
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "sd"] {
            let (spec, _runtime) = apply_preset(name, empty_map.clone(), &empty_paths, defaults);
            assert!(
                !spec.inputs.is_empty() || !spec.outputs.is_empty(),
                "tool {name}: should have at least one input or output"
            );
            // All managed tools are Executable kind.
            assert!(
                matches!(spec.kind, mediapm_conductor::ToolKindSpec::Executable { .. }),
                "tool {name}: expected Executable kind, got {kind:?}",
                kind = spec.kind
            );
        }
    }

    #[test]
    #[should_panic(expected = "unknown managed tool")]
    fn apply_preset_rejects_unknown_tool() {
        let defaults = FfmpegSlotLimits::default();
        let _ = apply_preset("nonexistent-tool", BTreeMap::new(), &BTreeMap::new(), defaults);
    }
}
