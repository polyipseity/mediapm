//! Focused tests for [`find_active_tool_spec`]: a caller resolves the one
//! spec the last sync deployed, not a name-match by key order.

#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use infallible patterns for clarity"
)]

use std::collections::BTreeMap;

use mediapm_conductor::config::ToolKindSpec;
use mediapm_conductor::{NickelDocument, ToolRuntime, ToolSpec};
use mediapm_utils::Timestamp;

use super::deployments::ToolDeployments;
use super::{clear_superseded_content_maps, find_active_tool_spec};
use crate::config::ToolRegistryEntry;

fn spec(name: &str, content_map_entries: &[(&str, &str)]) -> ToolSpec {
    ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec![name.to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: name.to_string(),
        version: None,
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime {
            content_map: content_map_entries
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            ..ToolRuntime::default()
        },
    }
}

fn doc_with_tools(entries: Vec<(&str, ToolSpec)>) -> NickelDocument {
    let mut tools = BTreeMap::new();
    for (key, spec) in entries {
        tools.insert(key.to_string(), spec);
    }
    NickelDocument { tools, ..NickelDocument::default() }
}

fn record(tool_id: &str, content_map_hash: &str, deployed_at_nanos: u64) -> ToolRegistryEntry {
    ToolRegistryEntry {
        tool_id: tool_id.to_string(),
        version: String::new(),
        canonical_version: String::new(),
        content_map_hash: content_map_hash.to_string(),
        deployed_at: Timestamp::from_unix_nanos(deployed_at_nanos),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    }
}

#[test]
fn a_tool_with_no_deployment_record_falls_back_to_the_content_map() {
    let stale_key = "yt-dlp@old";
    let live_key = "yt-dlp@new";
    let doc = doc_with_tools(vec![
        (stale_key, spec("yt-dlp", &[])),
        (live_key, spec("yt-dlp", &[("bin/yt-dlp", "h2")])),
    ]);
    let (key, resolved) = find_active_tool_spec(&doc, &ToolDeployments::empty(), "yt-dlp")
        .expect("fallback spec must resolve");
    assert_eq!(key, live_key);
    assert_eq!(resolved.runtime.content_map.get("bin/yt-dlp"), Some(&"h2".to_string()));
}

#[test]
fn the_deployment_record_beats_a_first_key_with_a_live_map() {
    let stale_key = "yt-dlp@aaa";
    let live_key = "yt-dlp@bbb";
    let doc = doc_with_tools(vec![
        (stale_key, spec("yt-dlp", &[("bin/yt-dlp", "h-stale")])),
        (live_key, spec("yt-dlp", &[("bin/yt-dlp", "h-live")])),
    ]);
    let deployments =
        ToolDeployments::from_records(&[record("yt-dlp", "aaa", 1), record("yt-dlp", "bbb", 2)]);
    let (key, resolved) =
        find_active_tool_spec(&doc, &deployments, "yt-dlp").expect("active spec must resolve");
    assert_eq!(key, live_key);
    assert_eq!(resolved.runtime.content_map.get("bin/yt-dlp"), Some(&"h-live".to_string()));
}

#[test]
fn a_stale_sibling_key_is_cleared_but_the_live_one_is_kept() {
    let mut doc = doc_with_tools(vec![
        ("ffmpeg@old", spec("ffmpeg", &[("bin/ffmpeg", "old")])),
        ("ffmpeg@new", spec("ffmpeg", &[("bin/ffmpeg", "new")])),
        ("ffmpeg", spec("ffmpeg", &[("bin/ffmpeg", "bare")])),
        ("deno@keep", spec("deno", &[("bin/deno", "d")])),
    ]);
    let cleared = clear_superseded_content_maps(&mut doc, "ffmpeg", "ffmpeg@new");
    assert_eq!(cleared, 2);
    assert!(doc.tools["ffmpeg@old"].runtime.content_map.is_empty());
    assert!(doc.tools["ffmpeg"].runtime.content_map.is_empty());
    assert_eq!(
        doc.tools["ffmpeg@new"].runtime.content_map.get("bin/ffmpeg"),
        Some(&"new".to_string())
    );
    assert_eq!(doc.tools["deno@keep"].runtime.content_map.get("bin/deno"), Some(&"d".to_string()));
}

#[test]
fn a_deployment_naming_an_absent_key_falls_through() {
    let stale_key = "yt-dlp@old";
    let live_key = "yt-dlp@new";
    let doc = doc_with_tools(vec![
        (stale_key, spec("yt-dlp", &[])),
        (live_key, spec("yt-dlp", &[("bin/yt-dlp", "h2")])),
    ]);
    let deployments = ToolDeployments::from_records(&[record("yt-dlp", "missing", 5)]);

    let (key, resolved) =
        find_active_tool_spec(&doc, &deployments, "yt-dlp").expect("fallback spec must resolve");

    assert_eq!(key, live_key);
    assert_eq!(resolved.runtime.content_map.get("bin/yt-dlp"), Some(&"h2".to_string()));
}

#[test]
fn a_missing_tool_returns_none() {
    let doc = doc_with_tools(vec![("ffmpeg@a", spec("ffmpeg", &[("bin/ffmpeg", "h")]))]);
    assert!(find_active_tool_spec(&doc, &ToolDeployments::empty(), "yt-dlp").is_none());
}
