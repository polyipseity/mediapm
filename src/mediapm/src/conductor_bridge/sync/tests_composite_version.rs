//! Composite canonical versions, built from a tool version and its same-step dependencies.

use crate::config::ToolRequirement;
use mediapm_conductor::tools::provider::VersionSpecFields;
use std::collections::BTreeMap;

use super::*;

#[test]
fn composite_canonical_version_no_deps() {
    assert_eq!(composite_canonical_version("v1", &[]), "v1");
}

#[test]
fn composite_canonical_version_single_dep() {
    let deps = [("ffmpeg", "ffmpeg-v7.1")];
    assert_eq!(composite_canonical_version("yt-dlp-v2", &deps), "yt-dlp-v2;ffmpeg:ffmpeg-v7.1");
}

#[test]
fn composite_canonical_version_multi_dep_alphabetical() {
    let deps = [("deno", "deno-v2.0"), ("ffmpeg", "ffmpeg-v7.1")];
    assert_eq!(
        composite_canonical_version("yt-dlp-v2", &deps),
        "yt-dlp-v2;deno:deno-v2.0;ffmpeg:ffmpeg-v7.1"
    );
}

#[test]
fn compute_composite_canonical_version_no_deps() {
    let req = ToolRequirement::default();
    let live_state = HashMap::new();
    let result = compute_composite_canonical_version("v1.0", "ffmpeg", &req, &live_state);
    assert_eq!(result, "v1.0");
}

#[test]
fn own_version_segment_bare_passthrough() {
    assert_eq!(own_version_segment("v1.2.3"), "v1.2.3");
}

#[test]
fn own_version_segment_strips_composite() {
    assert_eq!(own_version_segment("v1.2.3;ffmpeg:abc;deno:def"), "v1.2.3");
}

#[test]
fn own_version_segment_empty() {
    assert_eq!(own_version_segment(""), "");
}

#[test]
fn compute_composite_canonical_version_non_transitive() {
    // A dep that is itself an explicitly configured tool with its own
    // same-step deps carries a composite canonical_version in live_state.
    // The requester composite must reference the dep's OWN version
    // segment, never the dep's composite (no transitive nesting).
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let mut live_state: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::new();
    live_state.insert(
        "ffmpeg".to_string(),
        vec![ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: "ffmpeg-v7.1".to_string(),
            // ffmpeg itself has a same-step dep on "x" at "y" — its
            // canonical_version is a composite.
            canonical_version: "ffmpeg-v7.1;x:y".to_string(),
            content_map_hash: "blake3:abc".to_string(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: Some("v7.1".to_string()),
            resolved_version: Some("7.1".to_string()),
            resolved_vcs_hash: Some("abc123".to_string()),
        }],
    );
    let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &live_state);
    assert_eq!(
        result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1",
        "composite must use the dep's own version segment, never the dep's composite"
    );
    assert!(
        !result.contains(";x:y"),
        "no transitive nesting allowed — dep's deps must not leak: got {result}"
    );
}

#[test]
fn compute_composite_canonical_version_with_same_step_deps() {
    // Use VersionSpec::Exact so spec_matches_entry returns true.
    let deps = BTreeMap::from([(
        "ffmpeg".to_string(),
        ConfigVersionSpec::Exact(VersionSpecFields {
            tag: Some("v7.1".to_string()),
            version: None,
            vcs_hash: None,
        }),
    )]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let mut live_state: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::new();
    live_state.insert(
        "ffmpeg".to_string(),
        vec![ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: "v7.1".to_string(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: "blake3:abc".to_string(), // non-empty → matched
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: Some("v7.1".to_string()),
            resolved_version: Some("7.1".to_string()),
            resolved_vcs_hash: Some("abc123".to_string()),
        }],
    );
    let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &live_state);
    assert_eq!(result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1");
}

#[test]
fn compute_composite_canonical_version_with_same_step_deps_inherit() {
    // SameStep deps using Inherit — spec_matches_entry returns false for
    // Inherit, so the old code would fail to find the dep and return bare.
    // The fix: for Inherit/Latest, match any active entry.
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Inherit)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let mut live_state: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::new();
    live_state.insert(
        "ffmpeg".to_string(),
        vec![ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: "v7.1".to_string(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: "blake3:abc".to_string(), // non-empty → matched
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: Some("v7.1".to_string()),
            resolved_version: Some("7.1".to_string()),
            resolved_vcs_hash: Some("abc123".to_string()),
        }],
    );
    let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &live_state);
    assert_eq!(
        result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1",
        "Inherit dep specs must find active entry and include its version in composite"
    );
}

#[test]
fn compute_composite_canonical_version_with_latest_dep() {
    // Latest dep spec — same fix as Inherit: match any active entry.
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let mut live_state: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::new();
    live_state.insert(
        "ffmpeg".to_string(),
        vec![ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: "v7.1".to_string(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: "blake3:abc".to_string(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: Some("v7.1".to_string()),
            resolved_version: Some("7.1".to_string()),
            resolved_vcs_hash: Some("abc123".to_string()),
        }],
    );
    let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &live_state);
    assert_eq!(
        result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1",
        "Latest dep specs must find active entry and include its version in composite"
    );
}
