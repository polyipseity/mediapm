//! Composite canonical versions, built from a tool version and its same-step
//! dependency versions.
//!
//! [`compute_composite_canonical_version`] folds caller-supplied dep segments
//! into the composite; the segments themselves come from the provisioning
//! pool's gates at sync time or from stored state via
//! [`dep_segments_from_live_state`] during the post-sync check.

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
    let dep_segments = BTreeMap::new();
    let result = compute_composite_canonical_version("v1.0", "ffmpeg", &req, &dep_segments);
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
    // same-step deps carries a composite canonical version; the pool publishes
    // its bare segment, but defensively the composite must reference the dep's
    // OWN version segment even if a caller passes a composite, so a dep's deps
    // never leak transitively.
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    // The dep's own stored canonical version is a composite (`ffmpeg-v7.1;x:y`).
    let dep_segments = BTreeMap::from([("ffmpeg".to_string(), "ffmpeg-v7.1;x:y".to_string())]);
    let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &dep_segments);
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
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let dep_segments = BTreeMap::from([("ffmpeg".to_string(), "ffmpeg-v7.1".to_string())]);
    let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &dep_segments);
    assert_eq!(result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1");
}

fn ffmpeg_entry(tag: &str, canonical: &str, content_map_hash: &str) -> ToolRegistryEntry {
    ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: tag.to_string(),
        canonical_version: canonical.to_string(),
        content_map_hash: content_map_hash.to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: Some(tag.to_string()),
        resolved_version: Some(tag.trim_start_matches('v').to_string()),
        resolved_vcs_hash: Some("abc123".to_string()),
    }
}

#[test]
fn dep_segments_from_live_state_inherit_matches_any_active_entry() {
    // Inherit dep specs match any active entry regardless of its own spec
    // values — whatever version is active IS the version to include.
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Inherit)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let live_state: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::from([(
        "ffmpeg".to_string(),
        vec![ffmpeg_entry("v7.1", "ffmpeg-v7.1", "blake3:abc")],
    )]);
    let segments = dep_segments_from_live_state("yt-dlp", &req, &live_state);
    assert_eq!(
        segments.get("ffmpeg").map(String::as_str),
        Some("ffmpeg-v7.1"),
        "Inherit dep specs must find the active entry and include its bare version segment"
    );
}

#[test]
fn dep_segments_from_live_state_exact_requires_spec_match() {
    // Exact dep specs must match the stored entry's resolved fields; a
    // mismatching spec contributes no segment.
    let matching = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: BTreeMap::from([(
            "ffmpeg".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v7.1".to_string()),
                version: None,
                vcs_hash: None,
            }),
        )]),
        ..Default::default()
    };
    let mismatching = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: BTreeMap::from([(
            "ffmpeg".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v9.9".to_string()),
                version: None,
                vcs_hash: None,
            }),
        )]),
        ..Default::default()
    };
    let live_state: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::from([(
        "ffmpeg".to_string(),
        vec![ffmpeg_entry("v7.1", "ffmpeg-v7.1", "blake3:abc")],
    )]);
    assert_eq!(
        dep_segments_from_live_state("yt-dlp", &matching, &live_state).get("ffmpeg"),
        Some(&"ffmpeg-v7.1".to_string()),
        "a matching Exact spec must include the dep's bare version segment"
    );
    assert!(
        dep_segments_from_live_state("yt-dlp", &mismatching, &live_state).is_empty(),
        "a mismatching Exact spec must contribute no segment"
    );
}

#[test]
fn dep_segments_from_live_state_skips_empty_content_map() {
    // An entry whose content map is empty is not a provisioned payload, so it
    // must not contribute a segment.
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]),
        ..Default::default()
    };
    let live_state: HashMap<String, Vec<ToolRegistryEntry>> =
        HashMap::from([("ffmpeg".to_string(), vec![ffmpeg_entry("v7.1", "ffmpeg-v7.1", "")])]);
    assert!(
        dep_segments_from_live_state("yt-dlp", &req, &live_state).is_empty(),
        "a dep entry with an empty content map must contribute no segment"
    );
}
