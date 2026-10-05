//! Dependency version specs on a `ToolRequirement`, on the wire and through resolution.

use crate::config::ToolRequirement;
use mediapm_conductor::tools::provider::VersionSpecFields;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::*;

/// `dependencies` is flat on the wire: each tool id is a direct key of the
/// map holding its version spec, with nothing wrapping the specs.
///
/// The shape is read out of the encoded document rather than compared as a
/// whole string, because a whole-string compare also passes when the map
/// grows a level of nesting that deserializes back to the same value. The
/// key set and the per-dependency reads below are what pin the flatness.
#[test]
fn tool_requirement_dependencies_round_trip_flat() {
    let mut dependencies = BTreeMap::new();
    dependencies.insert(
        "ffmpeg".to_string(),
        ConfigVersionSpec::Exact(VersionSpecFields {
            tag: Some("v7.1".to_string()),
            version: None,
            vcs_hash: None,
        }),
    );
    dependencies.insert("deno".to_string(), ConfigVersionSpec::Inherit);
    dependencies.insert("sd".to_string(), ConfigVersionSpec::Latest);
    let requirement = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: dependencies.clone(),
        ..Default::default()
    };

    let encoded = serde_json::to_value(&requirement).expect("a ToolRequirement serializes to JSON");
    let wire_dependencies = encoded
        .get("dependencies")
        .and_then(serde_json::Value::as_object)
        .expect("dependencies encodes as an object");

    assert_eq!(
        wire_dependencies.keys().cloned().collect::<BTreeSet<_>>(),
        BTreeSet::from(["deno".to_string(), "ffmpeg".to_string(), "sd".to_string()]),
        "each dependency tool id is a direct key of the map: {wire_dependencies:?}"
    );
    assert_eq!(
        wire_dependencies["deno"],
        serde_json::json!("inherit"),
        "a unit spec encodes as its bare string: {wire_dependencies:?}"
    );
    assert_eq!(
        wire_dependencies["sd"],
        serde_json::json!("latest"),
        "a unit spec encodes as its bare string: {wire_dependencies:?}"
    );
    assert_eq!(
        wire_dependencies["ffmpeg"]["tag"],
        serde_json::json!("v7.1"),
        "an exact spec encodes its fields at the top of the dependency value: {wire_dependencies:?}"
    );
    assert_eq!(
        wire_dependencies["ffmpeg"].as_object().map(serde_json::Map::len),
        Some(1),
        "an exact spec carries only the fields it set, so a wrapper level cannot hide here: {wire_dependencies:?}"
    );

    let decoded: ToolRequirement =
        serde_json::from_value(encoded.clone()).expect("the encoded requirement decodes");
    assert_eq!(decoded.dependencies, dependencies, "every dependency spec survives the round trip");
    assert_eq!(
        serde_json::to_value(&decoded).expect("the decoded requirement re-encodes"),
        encoded,
        "re-encoding reproduces the same document, so a config rewritten by a sync does not drift"
    );
}

/// A requirement that declares no dependency reads back as an empty map.
///
/// The field is `#[serde(default)]`, so the document a user writes without
/// a `dependencies` key must decode rather than fail, and must decode to
/// the same empty map a written-out empty map produces. The two encodings
/// are compared so a default that ever stopped matching the explicit form
/// would show up here.
#[test]
fn tool_requirement_without_dependencies_decodes_to_an_empty_map() {
    let omitted = serde_json::json!({ "version_spec": "latest" });
    let decoded: ToolRequirement =
        serde_json::from_value(omitted.clone()).expect("an omitted map falls back to the default");
    assert!(
        decoded.dependencies.is_empty(),
        "no dependencies declared means no dependencies: {:?}",
        decoded.dependencies
    );

    let explicit = serde_json::json!({ "version_spec": "latest", "dependencies": {} });
    let decoded_explicit: ToolRequirement =
        serde_json::from_value(explicit).expect("an empty map decodes");
    assert_eq!(
        decoded_explicit.dependencies, decoded.dependencies,
        "the omitted and the written-out empty map must land on the same value"
    );
}

#[test]
fn resolve_dep_version_spec_inherit_resolves() {
    let mut globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
    globals.insert(
        "ffmpeg".to_string(),
        ToolRequirement {
            version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Exact(
                VersionSpecFields { vcs_hash: Some("abc".into()), version: None, tag: None },
            ),
            ..Default::default()
        },
    );
    let result = resolve_dep_version_spec(
        &mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
        "ffmpeg",
        &globals,
        "test_parent",
    )
    .unwrap();
    assert_eq!(
        result,
        mediapm_conductor::tools::provider::VersionSpec::Exact(VersionSpecFields {
            vcs_hash: Some("abc".into()),
            version: None,
            tag: None,
        })
    );
}

#[test]
fn resolve_dep_version_spec_exact_passthrough() {
    let globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
    let spec = ConfigVersionSpec::Exact(VersionSpecFields {
        vcs_hash: None,
        version: Some("1.0".into()),
        tag: None,
    });
    let result = resolve_dep_version_spec(&spec, "any", &globals, "test_parent").unwrap();
    assert_eq!(
        result,
        VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: Some("1.0".into()),
            tag: None,
        })
    );
}

#[test]
fn resolve_dep_version_spec_latest_passthrough() {
    let globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
    let result = resolve_dep_version_spec(
        &mediapm_conductor::tools::provider::ConfigVersionSpec::Latest,
        "any",
        &globals,
        "test_parent",
    )
    .unwrap();
    assert_eq!(result, mediapm_conductor::tools::provider::VersionSpec::Latest);
}

#[test]
fn resolve_dep_version_spec_inherit_missing_tool_error() {
    let globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
    let result = resolve_dep_version_spec(
        &mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
        "missing",
        &globals,
        "test_parent",
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("MPM-E002"), "should contain MPM-E002 code");
    assert!(msg.contains("not configured"), "should mention not configured");
    assert!(msg.contains("inherit"), "should mention inherit");
}

#[test]
fn resolve_dep_version_spec_circular_inherit_error() {
    let mut globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
    globals.insert(
        "foo".to_string(),
        ToolRequirement {
            version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
            ..Default::default()
        },
    );
    let result = resolve_dep_version_spec(
        &mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
        "foo",
        &globals,
        "test_parent",
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("MPM-E003"), "should contain MPM-E003 code");
    assert!(msg.contains("circular"), "should mention circular");
    assert!(msg.contains("inherit"), "should mention inherit");
}
