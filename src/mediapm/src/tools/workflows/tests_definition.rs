//! The invariant that a tool definition carries its policy once.
//!
//! A builder used to receive a `ToolSpec` and a separate `ToolRuntime`, both
//! mutable, where only the spec reached the generated document. Patching one and
//! not the other was a live defect, so this test asserts the two agree for every
//! managed tool: `into_parts()` derives the runtime from the spec, and nothing in
//! between can make them differ.

use super::definition::ToolDefinition;

/// Every managed tool's derived runtime must be exactly its spec's runtime.
///
/// The five fetched-tool presets and the host builtin are both covered: they
/// reached the document through different builders, and both are the kind of
/// builder that used to patch two values.
#[test]
fn every_managed_tool_derives_one_runtime() {
    let os_exec_paths = std::collections::BTreeMap::new();
    let content_map = std::collections::BTreeMap::new();

    let mut definitions: Vec<(&str, ToolDefinition)> = ["ffmpeg", "yt-dlp", "deno", "rsgain", "sd"]
        .into_iter()
        .map(|tool| {
            (
                tool,
                crate::tools::preset::apply_preset(
                    tool,
                    content_map.clone(),
                    &os_exec_paths,
                    crate::conductor_bridge::tool_runtime::FfmpegSlotLimits::default(),
                ),
            )
        })
        .collect();
    #[cfg(feature = "media-tagger")]
    definitions.push(("media-tagger", super::media_tagger::build_media_tagger_spec()));

    assert!(!definitions.is_empty(), "at least one managed tool must be present");
    for (tool, definition) in definitions {
        let spec_runtime = definition.spec().runtime.clone();
        let (spec, runtime) = definition.into_parts();
        assert_eq!(
            spec.runtime, runtime,
            "{tool}: the derived runtime must be the spec's runtime, or a builder can set a policy \
             the generated document never sees"
        );
        assert_eq!(spec_runtime, runtime, "{tool}: reading the spec must not change it");
    }
}

/// An empty definition is still internally consistent, which is what makes the
/// type usable by a builder that mutates before it derives.
#[test]
fn a_mutated_definition_still_derives_once() {
    let mut definition = ToolDefinition::new(mediapm_conductor::config::ToolSpec::default());
    definition.spec_mut().runtime.max_retries = 0;
    definition.spec_mut().runtime.max_concurrent_calls = Some(1);

    let (spec, runtime) = definition.into_parts();
    assert_eq!(spec.runtime.max_retries, 0);
    assert_eq!(runtime.max_retries, 0, "the derived runtime must see the mutation");
    assert_eq!(runtime.max_concurrent_calls, Some(1));
}
