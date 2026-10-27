//! Parity between the media-step impurity classifier and the tool specs.
//!
//! A save policy resolved to `auto` asks two questions about the step that
//! produces a variant, and one of them is whether that step is impure. The
//! answer reaches the conductor through the [`ToolRuntime`] each builder
//! assembles, while mediapm reads it from [`MediaStepTool::is_impure`], so a
//! classifier that disagrees with a builder would resolve a policy the conductor
//! then contradicts: work the conductor reuses would be treated as unrepeatable,
//! or work it must repeat would be treated as reusable.
//!
//! Every media step tool is pinned here through the artefact the conductor sees.
//! The four managed tools are reached through their builders, and the `import`
//! builtin is reached through the generated document the bridge registers, which
//! is the only path its runtime takes — the builtin supplies its own purity from
//! its registration rather than from a builder in this crate.
//!
//! [`ToolRuntime`]: mediapm_conductor::ToolRuntime

use std::collections::BTreeMap;

use mediapm_conductor::NickelDocument;

use super::ffmpeg::build_ffmpeg_spec;
use super::media_tagger::build_media_tagger_spec;
use super::rsgain::build_rsgain_spec;
use super::yt_dlp::build_yt_dlp_spec;
use crate::conductor_bridge::documents::{
    apply_builtin_runtime_defaults, register_missing_builtin_tools,
};
use crate::conductor_bridge::tool_runtime::FfmpegSlotLimits;
use crate::config::MediaStepTool;

/// Builds the per-OS executable map a spec builder needs for one tool.
///
/// The builders read it only to compose the OS-conditional command selector, so
/// a single entry is enough: the purity assertion never looks at the command.
fn os_exec_paths(tool: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("linux".to_string(), tool.to_string())])
}

/// The impurity classifier and every tool spec agree about purity.
///
/// This protects the `auto` save policy's third rule: an output of an impure
/// step is saved. If a builder marked a tool pure while the classifier called it
/// impure, the resolver would save a variant mediapm considers unrepeatable
/// while the conductor reuses its recorded instance, and the reverse would drop
/// the output of a step that runs again on every sync.
#[test]
fn impure_classification_matches_every_tool_builder() {
    let (_spec, yt_dlp) = build_yt_dlp_spec(BTreeMap::new(), &os_exec_paths("yt-dlp"));
    let (_spec, media_tagger) =
        build_media_tagger_spec(BTreeMap::new(), &os_exec_paths("media-tagger"));
    let (_spec, rsgain) = build_rsgain_spec(BTreeMap::new(), &os_exec_paths("rsgain"));
    let (_spec, ffmpeg) =
        build_ffmpeg_spec(BTreeMap::new(), &os_exec_paths("ffmpeg"), FfmpegSlotLimits::default());

    for (tool, runtime) in [
        (MediaStepTool::YtDlp, yt_dlp),
        (MediaStepTool::Ffmpeg, ffmpeg),
        (MediaStepTool::Rsgain, rsgain),
        (MediaStepTool::MediaTagger, media_tagger),
    ] {
        assert_eq!(
            runtime.impure,
            tool.is_impure(),
            "the {} builder and the classifier disagree about purity",
            tool.as_str()
        );
    }

    // The import builtin carries no builder in this crate: its runtime is the
    // one the bridge copies from its registration onto the generated document.
    let mut generated = NickelDocument::default();
    register_missing_builtin_tools(&mut generated);
    apply_builtin_runtime_defaults(&mut generated);
    let registration = mediapm_conductor::tools::production_builtins()
        .find(|entry| entry.name == MediaStepTool::Import.as_str())
        .expect("the import builtin is a production entry");
    let spec = generated
        .tools
        .get(registration.builtin_id)
        .expect("registering the builtins inserts every production entry");
    assert_eq!(
        spec.runtime.impure,
        MediaStepTool::Import.is_impure(),
        "the import builtin registration and the classifier disagree about purity"
    );
}
