//! Which generated-document entry a tool name resolves to.

use crate::config::ToolRequirement;
use crate::output::ProgressScreen;
use mediapm_conductor::tools::provider::VersionSpecFields;
use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolSpec};
use std::collections::BTreeMap;

use super::*;

/// When multiple generated doc entries match a tool (a stale bare entry
/// with a cleared content map plus the active `{name}@{hash}` entry), the
/// skip path must prefer the entry with a non-empty content map.
#[tokio::test]
async fn reconcile_skip_prefers_entry_with_content_map() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Bare stale entry (cleared content map) sorts before the `@` key;
    // the active hashed entry carries the content map.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/yt-dlp".to_string(), "blake3:abc".to_string());
    let stale_spec = ToolSpec {
        name: "yt-dlp".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime::default(),
        ..Default::default()
    };
    let active_spec = ToolSpec {
        name: "yt-dlp".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime { content_map, ..Default::default() },
        ..Default::default()
    };
    let mut tools = BTreeMap::new();
    tools.insert("yt-dlp".to_string(), stale_spec);
    tools.insert("yt-dlp@blake3:abc".to_string(), active_spec);
    let doc = NickelDocument { tools, ..Default::default() };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "yt-dlp".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "yt-dlp-2024.01.01".to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: Some("2024.01.01".to_string()),
        resolved_vcs_hash: None,
    });

    let mut desired_tools = BTreeMap::new();
    let req = ToolRequirement {
        version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Exact(
            VersionSpecFields {
                version: Some("2024.01.01".to_string()),
                vcs_hash: None,
                tag: None,
            },
        ),
        ..Default::default()
    };
    desired_tools.insert("yt-dlp".to_string(), serde_json::to_value(req).unwrap());

    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &desired_tools,
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &ProgressScreen::disabled(),
        None,
    )
    .await;

    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

    // The active hashed entry must win: env paths carry the conductor id.
    let env_path = &paths.env_generated_file;
    let content = std::fs::read_to_string(env_path).expect("env file readable");
    assert!(
        content.contains("MEDIAPM_YT_DLP_LINUX="),
        "env file should have MEDIAPM_YT_DLP_LINUX\n--- content:\n{content}",
    );
    assert!(
        content.contains("/yt-dlp@blake3_abc/payload/linux/yt-dlp"),
        "skip path must prefer the entry with a content map\n--- content:\n{content}",
    );
}

/// A stale bare-name entry with a cleared content map plus an active
/// `{name}@{hash}` entry: the active entry (non-empty content map) wins
/// regardless of `BTreeMap` key order.
#[test]
fn find_active_tool_spec_prefers_non_empty_content_map() {
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/yt-dlp".to_string(), "blake3:abc".to_string());
    let mut tools = BTreeMap::new();
    tools.insert(
        "yt-dlp".to_string(),
        ToolSpec {
            version: None,
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime::default(),
            ..Default::default()
        },
    );
    tools.insert(
        "yt-dlp@blake3:abc".to_string(),
        ToolSpec {
            version: None,
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        },
    );
    let doc = NickelDocument { tools, ..Default::default() };

    let (key, spec) = find_active_tool_spec(&doc, "yt-dlp").expect("active spec must resolve");
    assert_eq!(key, "yt-dlp@blake3:abc");
    assert_eq!(spec.name, "yt-dlp");
    assert!(!spec.runtime.content_map.is_empty());
}

/// No spec carries a content map: resolution falls back to the first
/// name match in deterministic key order (bare key sorts first).
#[test]
fn find_active_tool_spec_falls_back_to_first_name_match() {
    let mut tools = BTreeMap::new();
    tools.insert(
        "yt-dlp@blake3:abc".to_string(),
        ToolSpec {
            version: None,
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime::default(),
            ..Default::default()
        },
    );
    tools.insert(
        "yt-dlp@blake3:def".to_string(),
        ToolSpec {
            version: None,
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime::default(),
            ..Default::default()
        },
    );
    let doc = NickelDocument { tools, ..Default::default() };

    let (key, spec) = find_active_tool_spec(&doc, "yt-dlp").expect("fallback spec must resolve");
    assert_eq!(key, "yt-dlp@blake3:abc");
    assert_eq!(spec.name, "yt-dlp");
}

/// No spec matches the logical name at all: `None`.
#[test]
fn find_active_tool_spec_none_when_name_missing() {
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/ffmpeg".to_string(), "blake3:abc".to_string());
    let mut tools = BTreeMap::new();
    tools.insert(
        "ffmpeg@blake3:abc".to_string(),
        ToolSpec {
            version: None,
            name: "ffmpeg".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        },
    );
    let doc = NickelDocument { tools, ..Default::default() };

    assert!(find_active_tool_spec(&doc, "yt-dlp").is_none());
}

/// Specs with other names never match the queried logical name.
#[test]
fn find_active_tool_spec_skips_other_names() {
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/ffmpeg".to_string(), "blake3:abc".to_string());
    let mut tools = BTreeMap::new();
    tools.insert(
        "ffmpeg@blake3:abc".to_string(),
        ToolSpec {
            name: "ffmpeg".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        },
    );
    let doc = NickelDocument { tools, ..Default::default() };

    assert!(find_active_tool_spec(&doc, "yt-dlp").is_none());
}
