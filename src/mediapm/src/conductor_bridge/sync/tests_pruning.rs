//! Stale tool keys and directories: pruning them, and telling an update from an addition.

use crate::config::ToolRequirement;
use crate::output::ProgressScreen;
use mediapm_conductor::tools::provider::VersionSpecFields;
use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolSpec};
use std::collections::BTreeMap;

use super::*;

/// The filesystem retain set uses conductor tool ids (the `tool_runtimes`
/// keys), matching the provision cache's
/// `<sanitize_tool_id(conductor_tool_id)>` directory layout. A
/// mediapm-id set would prune every provisioned directory.
#[tokio::test]
async fn reconcile_retain_active_set_uses_conductor_tool_ids() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Pre-seed the tools dir with a conductor-keyed active dir and a
    // stale dir that must be pruned. Retain-only only removes dirs that
    // carry a `.lock` file, so the stale dir gets one.
    std::fs::create_dir_all(paths.tools_dir.join("yt-dlp@blake3_abc")).expect("create active dir");
    std::fs::create_dir_all(paths.tools_dir.join("stale_dir")).expect("create stale dir");
    std::fs::write(paths.tools_dir.join("stale_dir").join(".lock"), b"")
        .expect("create stale lock file");

    // Generated doc with an active `{name}@{hash}` entry.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/yt-dlp".to_string(), "blake3:abc".to_string());
    let tool_spec = ToolSpec {
        name: "yt-dlp".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime { content_map, ..Default::default() },
        ..Default::default()
    };
    let mut tools = BTreeMap::new();
    tools.insert("yt-dlp@blake3:abc".to_string(), tool_spec);
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

    // The conductor-keyed dir survives; the stale dir is pruned.
    assert!(
        paths.tools_dir.join("yt-dlp@blake3_abc").exists(),
        "active conductor-keyed dir must survive retain-only",
    );
    assert!(
        !paths.tools_dir.join("stale_dir").exists(),
        "non-active dir must be pruned by retain-only",
    );
}

#[tokio::test]
async fn reconcile_prunes_old_tool_version_clears_content_map() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Pre-populate generated doc with an old version key that has a bogus
    // content hash suffix.  This simulates a stale entry from a previous
    // sync whose content_map should be cleared when a fresh key is computed.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/media-tagger".to_string(), "blake3:abc".to_string());
    let tool_spec = ToolSpec {
        name: "media-tagger".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime { content_map, ..Default::default() },
        ..Default::default()
    };
    let mut tools = BTreeMap::new();
    tools.insert("media-tagger@bogus_hash".to_string(), tool_spec);
    let doc = NickelDocument { tools, ..Default::default() };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    // State with a different canonical_version so the skip path does not
    // fire — forcing a fresh resolve and a new tool_key computation.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: "old-version".to_string(),
        canonical_version: "old-canonical".to_string(),
        content_map_hash: String::new(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });

    // Desired tools with media-tagger.
    let mut desired_tools = BTreeMap::new();
    let req = ToolRequirement::default();
    desired_tools.insert("media-tagger".to_string(), serde_json::to_value(req).unwrap());

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
    let report = result.unwrap();

    // The old bogus key should have been counted toward pruned_tools.
    assert!(
        report.pruned_tools >= 1,
        "expected at least 1 pruned tool, got {}",
        report.pruned_tools
    );

    // Reload generated doc: the old bogus key is removed once its
    // content_map was cleared — empty maps shadow active tool specs.
    let doc = load_conductor_generated_document(&paths).expect("load generated doc after sync");
    assert!(
        !doc.tools.contains_key("media-tagger@bogus_hash"),
        "old version key should be removed after sync, keys: {:?}",
        doc.tools.keys().collect::<Vec<_>>()
    );

    // The new key should exist with non-empty content_map.
    let has_new_key =
        doc.tools.keys().any(|k| k == "media-tagger" || k.starts_with("media-tagger@"));
    assert!(
        has_new_key,
        "new version key should exist after sync, keys: {:?}",
        doc.tools.keys().collect::<Vec<_>>()
    );
    let new_spec = doc.tools.values().find(|s| s.name == "media-tagger").unwrap();
    assert!(
        !new_spec.runtime.content_map.is_empty(),
        "new version key should have non-empty content_map"
    );
}

/// Runs one `media-tagger` reconcile pass and hands back its report.
///
/// media-tagger is the fixture tool for the stale-seed tests because its
/// provider is a builtin launcher: resolution needs neither network nor a
/// seeded download cache, yet the launcher it generates is a real payload.
/// The entry therefore reaches the `Fetched` outcome branch, which is the
/// branch where `already_exists` decides `tools_added` against
/// `tools_updated`. The no-payload branch makes a different decision, so a
/// fixture that landed there would prove nothing about the name-match.
async fn reconcile_media_tagger_latest(
    paths: &MediaPmPaths,
    state: &MediaPmState,
    cache_root: &std::path::Path,
) -> ToolSyncReport {
    let workspace_cas = super::open_workspace_cas_store(paths).await.expect("open workspace cas");
    let mut desired_tools = BTreeMap::new();
    desired_tools.insert(
        "media-tagger".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            ..Default::default()
        })
        .unwrap(),
    );
    reconcile_desired_tools(
        workspace_cas,
        paths,
        &desired_tools,
        &BTreeMap::new(),
        RecheckPolicy::default(),
        state,
        Some(cache_root),
        &ProgressScreen::disabled(),
        None,
    )
    .await
    .expect("reconcile_desired_tools must succeed for an offline builtin launcher")
}

/// A generated-doc entry whose `name` is the bare logical tool id makes the
/// pass an update, never an addition.
///
/// Both arms run the same fixture with one difference, the seeded entry, so
/// the assertion has nothing to read but that difference. The unseeded arm
/// is what makes the seeded one meaningful: a pass that ignored the
/// name-match would report `(1, 0)` in both arms, and a pass that counted
/// nothing at all would report `(0, 0)` in both.
#[tokio::test]
async fn reconcile_counts_a_name_matched_seed_as_an_update_not_an_addition() {
    let seeded_tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let seeded_cache = mediapm_utils::temp::cache_dir().unwrap();
    let seeded_paths = MediaPmPaths::from_root(seeded_tmp.path());
    let mut stale_content_map = BTreeMap::new();
    stale_content_map.insert("linux/media-tagger".to_string(), "blake3:stale".to_string());
    let seeded_doc = NickelDocument {
        tools: BTreeMap::from([(
            "media-tagger@blake3:stale".to_string(),
            ToolSpec {
                name: "media-tagger".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime { content_map: stale_content_map, ..Default::default() },
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    save_conductor_generated_document(&seeded_paths, &seeded_doc).expect("pre-save generated doc");

    let seeded =
        reconcile_media_tagger_latest(&seeded_paths, &MediaPmState::default(), seeded_cache.path())
            .await;

    let bare_tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let bare_cache = mediapm_utils::temp::cache_dir().unwrap();
    let bare_paths = MediaPmPaths::from_root(bare_tmp.path());
    let bare =
        reconcile_media_tagger_latest(&bare_paths, &MediaPmState::default(), bare_cache.path())
            .await;

    assert_eq!(
        (seeded.tools_added, seeded.tools_updated),
        (0, 1),
        "a generated-doc spec named `media-tagger` is an update, never an addition: {seeded:?}"
    );
    assert_eq!(
        (bare.tools_added, bare.tools_updated),
        (1, 0),
        "the same pass over an empty generated doc is an addition: {bare:?}"
    );
}

/// The install a real upgrade walks into: a generated-doc entry under a
/// stale `{name}@{hash}` key, a `managed_tools` record whose
/// `canonical_version` no longer matches what the provider resolves, and
/// the provisioned directory that stale hash owns.
///
/// One pass has to carry all three, so the assertions read the whole trail
/// in order rather than one field of it. The state record is seeded with a
/// non-empty `content_map_hash` on purpose: that is the half of the skip
/// check that lets a matching `canonical_version` skip, and pairing it with
/// a canonical version the provider cannot produce is what keeps this pass
/// on the reprovision path.
#[tokio::test]
async fn stale_seed_drives_name_match_reprovision_and_prune_in_one_pass() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());

    // The document half of the seed: a previous sync's payload, filed under
    // its own content hash.
    let mut stale_content_map = BTreeMap::new();
    stale_content_map.insert("linux/media-tagger".to_string(), "blake3:stale".to_string());
    let stale_doc = NickelDocument {
        tools: BTreeMap::from([(
            "media-tagger@blake3:stale".to_string(),
            ToolSpec {
                name: "media-tagger".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime { content_map: stale_content_map, ..Default::default() },
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    save_conductor_generated_document(&paths, &stale_doc).expect("pre-save generated doc");

    // The state half of the seed.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: "old-version".to_string(),
        canonical_version: "old".to_string(),
        content_map_hash: "blake3:stale".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });

    // The filesystem half of the seed. Retain-only removes a directory only
    // when it can take that directory's lock, so the stale one carries a
    // lock file the way a real provisioned entry does.
    let stale_dir = paths.tools_dir.join("media-tagger@blake3_stale");
    std::fs::create_dir_all(&stale_dir).expect("create stale tool dir");
    std::fs::write(stale_dir.join(".lock"), b"").expect("create stale lock file");

    let report = reconcile_media_tagger_latest(&paths, &state, cache_root.path()).await;

    // The name-match makes this an update, and the mismatched canonical
    // version keeps it off both skip paths.
    assert_eq!(
        report.tools_skipped, 0,
        "a canonical_version the provider cannot produce must reprovision: {report:?}"
    );
    assert_eq!(
        (report.tools_added, report.tools_updated),
        (0, 1),
        "the seeded install is an update, not an addition: {report:?}"
    );

    // The stale key loses its content map on the new entry's arrival and is
    // dropped by the rewrite; the fresh hash key is what survives.
    let after = load_conductor_generated_document(&paths).expect("load generated doc after sync");
    assert!(
        !after.tools.contains_key("media-tagger@blake3:stale"),
        "the stale key must be dropped once its content map is cleared, keys: {:?}",
        after.tools.keys().collect::<Vec<_>>()
    );
    let (active_key, active_spec) = find_active_tool_spec(&after, "media-tagger")
        .expect("the freshly provisioned tool must resolve as active");
    assert_ne!(
        active_key, "media-tagger@blake3:stale",
        "the active key must be the fresh content hash, got {active_key}"
    );
    assert!(
        !active_spec.runtime.content_map.is_empty(),
        "the fresh key must carry the reprovisioned payload"
    );
    assert!(
        report.pruned_tools >= 1,
        "clearing and dropping the stale key counts as a prune: {report:?}"
    );

    // The provisioned directory follows the same trail on disk.
    assert!(!stale_dir.exists(), "retain-only must remove the directory the stale hash owned");
    let provisioned: Vec<String> = std::fs::read_dir(&paths.tools_dir)
        .expect("read tools dir")
        .map(|entry| entry.expect("tools dir entry").file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("media-tagger@"))
        .collect();
    assert_eq!(
        provisioned.len(),
        1,
        "exactly one fresh directory is left for the tool: {provisioned:?}"
    );
}

#[tokio::test]
async fn reconcile_drops_manual_entries_from_generated_doc() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Pre-populate generated doc with a manual entry whose bare tool_id
    // is NOT in the desired set (e.g., "user_script"). Under condition 3
    // (generated-doc purity) the generated document is rewritten
    // wholesale on every sync, so such entries are dropped — manual
    // tools belong in the user-owned `mediapm.conductor.ncl` instead.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
    let tool_spec = ToolSpec {
        version: None,
        name: "user_script".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime { content_map, ..Default::default() },
        ..Default::default()
    };
    let mut tools = BTreeMap::new();
    tools.insert("user_script@somehash".to_string(), tool_spec);
    let doc = NickelDocument { tools, ..Default::default() };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    // Empty desired_tools — nothing is "used" so every non-managed entry
    // (the manual one) must be dropped on rewrite.
    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &BTreeMap::new(),
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &ProgressScreen::disabled(),
        None,
    )
    .await;

    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
    let report = result.unwrap();
    assert!(
        report.pruned_tools >= 1,
        "manual entry dropped on rewrite must be counted as pruned, got {}",
        report.pruned_tools
    );

    // Verify the manual entry is gone after the wholesale rewrite.
    let doc = load_conductor_generated_document(&paths).expect("load generated doc after sync");
    assert!(
        !doc.tools.contains_key("user_script@somehash"),
        "manual entry must be dropped on generated-doc rewrite",
    );
}
