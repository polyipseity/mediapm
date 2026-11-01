//! The two-level provisioning plan built from the desired tool set.

use crate::config::ToolRequirement;
use mediapm_conductor::tools::provider::VersionSpecFields;
use std::collections::BTreeMap;

use super::*;

/// The decoded requirements for a set of raw declarations: the shape
/// [`decode_tool_requirements`] gives `build_provisioning_entries`.
fn decoded_requirements(
    desired: &BTreeMap<String, serde_json::Value>,
) -> BTreeMap<String, ToolRequirement> {
    decode_tool_requirements(desired).unwrap()
}

#[test]
fn build_provisioning_entries_empty() {
    let entries = build_provisioning_entries(&decoded_requirements(&BTreeMap::new())).unwrap();
    assert!(entries.is_empty());
}

#[test]
fn build_provisioning_entries_single_no_deps() {
    let mut desired = BTreeMap::new();
    desired.insert(
        "ffmpeg".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            ..Default::default()
        })
        .unwrap(),
    );
    let entries = build_provisioning_entries(&decoded_requirements(&desired)).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].tool_id, "ffmpeg");
    assert!(matches!(entries[0].kind, EntryKind::Explicit));
}

#[test]
fn build_provisioning_entries_with_deps() {
    let mut desired = BTreeMap::new();
    let mut deps = BTreeMap::new();
    deps.insert(
        "ffmpeg".to_string(),
        ConfigVersionSpec::Exact(VersionSpecFields {
            tag: Some("v7.1".to_string()),
            version: None,
            vcs_hash: None,
        }),
    );
    desired.insert(
        "yt-dlp".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        })
        .unwrap(),
    );
    let entries = build_provisioning_entries(&decoded_requirements(&desired)).unwrap();
    assert_eq!(entries.len(), 2);
    // Dep entry should come first (dep-first sort)
    assert_eq!(entries[0].tool_id, "ffmpeg");
    assert!(matches!(entries[0].kind, EntryKind::Dep));
    assert_eq!(entries[1].tool_id, "yt-dlp");
    assert!(matches!(entries[1].kind, EntryKind::Explicit));
}

#[test]
fn build_provisioning_entries_dedup_same_spec() {
    let mut desired = BTreeMap::new();
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    desired.insert(
        "yt-dlp".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps.clone(),
            ..Default::default()
        })
        .unwrap(),
    );
    desired.insert(
        "rsgain".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        })
        .unwrap(),
    );
    let entries = build_provisioning_entries(&decoded_requirements(&desired)).unwrap();
    // Two same-spec ffmpeg dep entries dedup → total 3
    assert_eq!(entries.len(), 3);
    assert_eq!(entries.iter().filter(|e| e.tool_id == "ffmpeg").count(), 1);
}

#[test]
fn build_provisioning_entries_level_split_ordering() {
    let mut desired = BTreeMap::new();
    // yt-dlp depends on ffmpeg and deno (both same-step)
    let mut deps_yt = BTreeMap::new();
    deps_yt.insert(
        "ffmpeg".to_string(),
        ConfigVersionSpec::Exact(VersionSpecFields {
            tag: Some("v7.1".to_string()),
            version: None,
            vcs_hash: None,
        }),
    );
    deps_yt.insert(
        "deno".to_string(),
        ConfigVersionSpec::Exact(VersionSpecFields {
            tag: Some("v1.46".to_string()),
            version: None,
            vcs_hash: None,
        }),
    );
    desired.insert(
        "yt-dlp".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps_yt,
            ..Default::default()
        })
        .unwrap(),
    );
    // rsgain depends on ffmpeg with the same spec (shared dep)
    let deps_rsgain = BTreeMap::from([(
        "ffmpeg".to_string(),
        ConfigVersionSpec::Exact(VersionSpecFields {
            tag: Some("v7.1".to_string()),
            version: None,
            vcs_hash: None,
        }),
    )]);
    desired.insert(
        "rsgain".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps_rsgain,
            ..Default::default()
        })
        .unwrap(),
    );
    // media-tagger has no dependencies
    desired.insert(
        "media-tagger".to_string(),
        serde_json::to_value(ToolRequirement::default()).unwrap(),
    );

    let entries = build_provisioning_entries(&decoded_requirements(&desired)).unwrap();

    // ffmpeg deduped from two requesters (2→1), deno separate dep → total 5
    assert_eq!(entries.len(), 5, "expected 5 entries (ffmpeg deduped), got {}", entries.len());

    // All Dep entries precede all Explicit entries.
    let last_dep = entries
        .iter()
        .rposition(|e| matches!(e.kind, EntryKind::Dep))
        .expect("at least one Dep entry");
    let first_explicit = entries
        .iter()
        .position(|e| matches!(e.kind, EntryKind::Explicit))
        .expect("at least one Explicit entry");
    assert!(
        last_dep < first_explicit,
        "all Dep entries must precede all Explicit entries: last_dep={last_dep}, first_explicit={first_explicit}",
    );

    // Single ffmpeg dep, empty dependencies (non-transitive).
    let ffmpeg_deps: Vec<_> = entries.iter().filter(|e| e.tool_id == "ffmpeg").collect();
    assert_eq!(ffmpeg_deps.len(), 1, "exactly one ffmpeg dep entry");
    assert!(
        ffmpeg_deps[0].tool_requirement.dependencies.is_empty(),
        "dep entry must have empty dependencies (non-transitive)",
    );
    assert!(matches!(ffmpeg_deps[0].kind, EntryKind::Dep));

    // Three explicit entries.
    let explicit_ids: Vec<_> = entries
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::Explicit))
        .map(|e| e.tool_id.as_str())
        .collect();
    assert_eq!(explicit_ids.len(), 3);
    assert!(explicit_ids.contains(&"yt-dlp"));
    assert!(explicit_ids.contains(&"rsgain"));
    assert!(explicit_ids.contains(&"media-tagger"));
}

#[test]
fn build_provisioning_entries_different_spec_no_dedup() {
    let mut desired = BTreeMap::new();
    let mut deps_yt = BTreeMap::new();
    deps_yt.insert(
        "ffmpeg".to_string(),
        ConfigVersionSpec::Exact(VersionSpecFields {
            tag: Some("v7.1".to_string()),
            version: None,
            vcs_hash: None,
        }),
    );
    let mut deps_rsgain = BTreeMap::new();
    deps_rsgain.insert(
        "ffmpeg".to_string(),
        ConfigVersionSpec::Exact(VersionSpecFields {
            tag: Some("v6.0".to_string()),
            version: None,
            vcs_hash: None,
        }),
    );
    desired.insert(
        "yt-dlp".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps_yt,
            ..Default::default()
        })
        .unwrap(),
    );
    desired.insert(
        "rsgain".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps_rsgain,
            ..Default::default()
        })
        .unwrap(),
    );
    let entries = build_provisioning_entries(&decoded_requirements(&desired)).unwrap();
    // Two different ffmpeg specs → NO dedup, total = 4
    assert_eq!(entries.len(), 4);
    assert_eq!(entries.iter().filter(|e| e.tool_id == "ffmpeg").count(), 2);
}

/// A requirement that does not decode aborts the pass naming its tool, before
/// any neighbour's `"inherit"` dependency is resolved against the decoded map.
///
/// The decode runs once over the whole desired set, so a malformed requirement
/// is its own failure and never a hole in that map. The fixture puts the
/// inheriting tool first (`aaa-tool` sorts before `zzz-tool`), which is the
/// order in which a map that dropped the malformed value would answer the
/// dependency with `MPM-E002` ("not configured in the tools section") instead of
/// the parse failure. Collecting the map through `filter_map(... .ok())` again
/// fails this test.
#[test]
fn decode_tool_requirements_aborts_naming_a_malformed_requirement() {
    let mut desired = BTreeMap::new();
    desired.insert(
        "aaa-tool".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: BTreeMap::from([("zzz-tool".to_string(), ConfigVersionSpec::Inherit)]),
            ..Default::default()
        })
        .unwrap(),
    );
    // `"not-a-spec"` is neither `"latest"`, `"inherit"` nor an exact field
    // object, so `ConfigVersionSpec`'s visitor rejects it.
    desired.insert("zzz-tool".to_string(), serde_json::json!({ "version_spec": "not-a-spec" }));

    let error = decode_tool_requirements(&desired).expect_err("a malformed requirement aborts");
    assert_eq!(
        error.code(),
        "MPM-E004",
        "a value that failed to deserialize is a config parse failure"
    );
    let message = error.to_string();
    assert!(
        message.contains("zzz-tool"),
        "the abort must name the tool whose requirement does not decode: {message}"
    );
    assert!(
        message.contains("suggestion:"),
        "the catalogue's ConfigValidation carries a fix: {message}"
    );
    assert!(
        !message.contains("not configured in the tools section"),
        "the dependency must not be reported as unconfigured once its tool's requirement \
         failed to decode: {message}"
    );
}
