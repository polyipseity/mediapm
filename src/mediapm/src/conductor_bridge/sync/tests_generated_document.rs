//! What the generated document records: external data, the no-payload arm, builtin registration.

use crate::output::ProgressScreen;
use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolSpec};
use std::collections::BTreeMap;

use super::*;

#[test]
fn external_data_rebuilt_independently_from_tool_specs() {
    // Create two tool specs with different content_map hashes using
    // Hash::from for deterministic test values.
    let hash_a = Hash::from([0u8; 32]);
    let hash_b = Hash::from([1u8; 32]);
    let hash_zero_hex = format!("blake3:{}", blake3::Hash::from([0u8; 32]).to_hex());
    let hash_one_hex = format!("blake3:{}", blake3::Hash::from([1u8; 32]).to_hex());

    let mut cm1 = BTreeMap::new();
    cm1.insert("linux/tool_a".to_string(), hash_zero_hex);
    let spec_a = ToolSpec {
        version: None,
        name: "tool_a".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime { content_map: cm1, ..Default::default() },
        ..Default::default()
    };
    let mut cm2 = BTreeMap::new();
    cm2.insert("macos/tool_b".to_string(), hash_one_hex);
    let spec_b = ToolSpec {
        version: None,
        name: "tool_b".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime { content_map: cm2, ..Default::default() },
        ..Default::default()
    };

    // Build external_data from both tool specs.
    let mut data_usage = self::external_data::DataUsageTracker::new();
    for spec in [&spec_a, &spec_b] {
        for hash_str in spec.runtime.content_map.values() {
            if let Ok(hash) = hash_str.parse::<Hash>() {
                data_usage.record(hash, format!("managed tool content root for {}", spec.name));
            }
        }
    }
    let external_data = data_usage.finalize();

    // Both hashes should be present.
    assert!(external_data.contains_key(&hash_a), "hash_a should be in external_data");
    assert!(external_data.contains_key(&hash_b), "hash_b should be in external_data");
    assert_eq!(external_data.len(), 2, "external_data should have exactly 2 entries");

    // Remove tool_a and verify its hash is excluded.
    let mut data_usage = self::external_data::DataUsageTracker::new();
    for hash_str in spec_b.runtime.content_map.values() {
        if let Ok(hash) = hash_str.parse::<Hash>() {
            data_usage.record(hash, format!("managed tool content root for {}", spec_b.name));
        }
    }
    let external_data_one = data_usage.finalize();

    assert!(!external_data_one.contains_key(&hash_a), "hash_a should be absent after removal");
    assert!(external_data_one.contains_key(&hash_b), "hash_b should remain");
    assert_eq!(external_data_one.len(), 1, "external_data should have 1 entry");
}

/// The arm that resolved a tool without fetching a payload inserts a
/// spec whose version is `None`, even though the provider resolved a tag,
/// a version, and a canonical version for it.
///
/// Those three values are real, but they describe what the provider would
/// have fetched, and nothing was fetched. A row that printed one of them
/// would name a release the workspace has no payload for. The registry
/// entry this arm also pushes does carry the composite, which is the audit
/// record; the document spec is what a step row reads, so it stays empty.
#[test]
fn fetched_none_registers_a_spec_without_a_version() {
    let screen = ProgressScreen::disabled();
    let bar = screen.add_bar(0, "fetched-none");
    let entry = ProvisionEntry {
        tool_id: "media-tagger".to_string(),
        tool_requirement: ToolRequirement::default(),
        kind: EntryKind::Explicit,
    };
    let mut generated_doc = NickelDocument::default();
    let mut tool_runtimes = BTreeMap::new();
    let mut provisioned_own_maps = BTreeMap::new();
    let mut report = ToolSyncReport::default();
    let mut live_state = std::collections::HashMap::new();
    let mut pruned_tools = 0usize;
    let inherited_env_vars = BTreeMap::new();

    apply_entry_outcome(
        &entry,
        EntryOutcome::FetchedNone {
            tool_id: "media-tagger".to_string(),
            is_builtin_code: false,
            already_exists: false,
            resolved_canonical_version: "mediapm-0123456789ab".to_string(),
            resolved_tag: Some("v2024.01.01".to_string()),
            resolved_version: Some("2024.01.01".to_string()),
            resolved_vcs_hash: Some("0123456789abcdef".to_string()),
        },
        &mut generated_doc,
        &mut tool_runtimes,
        &mut provisioned_own_maps,
        &mut report,
        &mut live_state,
        &mut pruned_tools,
        &inherited_env_vars,
        &bar,
    );

    let spec = generated_doc
        .tools
        .get("media-tagger")
        .expect("the no-payload arm registers the tool under its bare id");
    assert_eq!(
        spec.version, None,
        "no payload was fetched, so nothing claimed a release for this tool"
    );

    assert!(
        !generated_doc.tools.keys().any(|key| key.contains('@')),
        "an empty content map yields the bare tool id as the document key: {keys:?}",
        keys = generated_doc.tools.keys().collect::<Vec<_>>()
    );

    let record =
        report.tool_records.first().expect("the no-payload arm records the tool in the registry");
    assert_eq!(
        record.version,
        format!("{}+{}", env!("CARGO_PKG_VERSION"), crate::global::MEDIAPM_GIT_HASH),
        "the registry entry names the workspace build that will run the launcher, \
         which is a different claim from a release the provider resolved"
    );
}

/// Every builtin the coordinator registers carries no version, and its
/// document key is the bare registration id.
///
/// This is the tripwire for the convenience someone will eventually
/// propose. A builtin has no release, and both things a derivation could
/// reach for say something other than a release: `builtin_id` names a
/// registration and the key is that same id, so neither is a claim a user
/// wrote. Either derivation lands here as a `Some`, and a content-hash
/// suffix would show up as an `@` in the key.
///
/// The loop is over `ALL_BUILTINS` rather than one named builtin, so a
/// builtin added later is covered without editing this test.
#[test]
fn registered_builtins_carry_no_version_derived_from_id_or_key() {
    let mut generated_doc = NickelDocument::default();
    register_missing_builtin_tools(&mut generated_doc);

    assert!(
        !mediapm_conductor::tools::ALL_BUILTINS.is_empty(),
        "no builtins are registered, so this test would pass over an empty document"
    );

    for builtin in mediapm_conductor::tools::ALL_BUILTINS {
        let spec = generated_doc
            .tools
            .get(builtin.builtin_id)
            .unwrap_or_else(|| panic!("{} was not registered", builtin.builtin_id));
        assert_eq!(
            spec.version, None,
            "{} has no release to name, so its row must read {}",
            builtin.builtin_id, spec.name
        );
    }

    // Every key the registration wrote is a registration id and nothing
    // else. A key of the shape `{name}@{content-hash}` would name a
    // payload, which is exactly the string a derivation would have
    // reached for.
    let registered: Vec<&str> =
        mediapm_conductor::tools::ALL_BUILTINS.iter().map(|b| b.builtin_id).collect();
    for key in generated_doc.tools.keys() {
        assert!(
            registered.contains(&key.as_str()),
            "{key} is not a builtin id, so registration wrote something the builtin ids do not cover"
        );
        let spec = &generated_doc.tools[key];
        assert!(
            matches!(&spec.kind, ToolKindSpec::Builtin { builtin_id } if builtin_id == key),
            "{key} is registered under a key that is not its builtin id"
        );
    }
}
