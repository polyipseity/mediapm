use super::v2_state_json;
use mediapm::{
    MediaPmService, MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement,
};
use mediapm_conductor::cache::Cache;
use mediapm_conductor::cache::CacheDomainConfig;
use mediapm_conductor::cache_user_level::UserLevelCache;
use mediapm_conductor::tools::provider::VersionSpecFields;

use crate::common::stub_upstream::{STUB_MARTIN_RIEDL_BUILD_ID, mock_all_managed_tools};
use crate::common::{read_state, service_with_cache, test_sync_progress_overrides};

// ---------------------------------------------------------------------------
// Pure-function logic tests
// ---------------------------------------------------------------------------

/// `logical_tool_requires_sync` returns `true` for a tool absent from state.
#[tokio::test]
async fn sync_tool_requires_sync_when_missing() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let service = MediaPmService::new_fs_at(root.path()).await?;
    let state = MediaPmState::default();
    assert!(service.logical_tool_requires_sync("non-existent", &state).await?);
    Ok(())
}

/// `logical_tool_requires_sync` returns `false` for a tool that is present
/// in state with matching canonical version.
#[tokio::test]
async fn sync_tool_requires_sync_false_when_present() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    // media-tagger is a known tool whose provider resolves without network.
    let mut overrides = MediaRuntimeStorage::default();
    overrides.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), overrides).await?;
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: String::new(),
        canonical_version: mediapm::MEDIAPM_GIT_HASH.to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    assert!(!service.logical_tool_requires_sync("media-tagger", &state).await?);
    Ok(())
}

/// `ToolRegistryEntry.canonical_version` is populated with the canonical version
/// after sync, not an empty string or requirement version.
#[tokio::test]
async fn sync_tool_registry_entry_version_matches_canonical() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "media-tagger".to_string(),
        // `ToolRegistryEntry.canonical_version` uses the resolved canonical
        // version (git hash), not the requirement's version_spec.
        ToolRequirement::default(),
    );
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    let bytes = std::fs::read(&service.paths().mediapm_state_json)
        .expect("state.json should exist after sync");
    let state: MediaPmState =
        serde_json::from_slice(&bytes).expect("state.json should deserialize");

    let entry = state
        .managed_tools
        .iter()
        .find(|e| e.tool_id == "media-tagger")
        .expect("media-tagger should be registered after sync");

    // All desired tools are used seeds, so canonical_version is populated.
    assert!(
        !entry.canonical_version.is_empty(),
        "canonical_version must be non-empty for configured tools"
    );
    Ok(())
}

/// `collect_tools_requiring_sync` returns an empty vec when no tools are
/// desired.
#[tokio::test]
async fn sync_no_tools_need_sync_when_none_desired() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let service = MediaPmService::new_fs_at(root.path()).await?;
    let state = MediaPmState::default();
    let needing = service.collect_tools_requiring_sync(&state).await?;
    assert!(needing.is_empty(), "no desired tools → nothing needs sync");
    Ok(())
}

/// `collect_tools_requiring_sync` returns tool ids that are missing from
/// state.
#[tokio::test]
async fn sync_collects_missing_tool() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let mut overrides = MediaRuntimeStorage::default();
    overrides.tools.insert(
        "media-tagger".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                version: Some("2.0.0".to_string()),
                vcs_hash: None,
                tag: None,
            }),
            ..Default::default()
        },
    );
    let service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), overrides).await?;
    let state = MediaPmState::default();
    let needing = service.collect_tools_requiring_sync(&state).await?;
    assert_eq!(needing, vec!["media-tagger"]);
    Ok(())
}

/// Configured tools are never pruned when all desired tools are used as
/// seeds. Regression guard: the provision-cache retain set is the set of
/// active mediapm conductor tool ids (the `tool_runtimes` keys), which
/// always covers every provisioned tool — if a tool were missing from that
/// set, pruning would incorrectly remove its provisioned directory.
#[tokio::test]
async fn sync_no_pruning_for_configured_tools() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;
    let summary = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;
    assert_eq!(
        summary.pruned_tools, 0,
        "configured tools must not be pruned; pruned={}",
        summary.pruned_tools,
    );
    Ok(())
}

/// A v2 state.json on disk is automatically bridged to the current version on
/// load.
///
/// This tests the full version-dispatch path: JSON parsing → version field
/// extraction → v2→v3 advance → v4 migration → `MediaPmState` with
/// `Vec<ToolRegistryEntry>`.
#[tokio::test]
async fn state_v2_on_disk_bridges_to_current_version_on_load() -> Result<(), mediapm::MediaPmError>
{
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let state_path = root.path().join("state.json");

    std::fs::write(&state_path, serde_json::to_string_pretty(&v2_state_json(true)).unwrap())
        .expect("write v2 state.json");

    let state = mediapm::load_mediapm_state_document(&state_path).expect("load v2 state.json");

    assert_eq!(state.version, 5, "v2 state should bridge to the current version");
    assert_eq!(state.managed_tools.len(), 1, "v2 bridge should produce one tool entry");
    assert_eq!(state.managed_tools[0].tool_id, "ffmpeg");
    assert_eq!(state.managed_tools[0].canonical_version, "ffmpeg-v7.1");
    assert_eq!(state.managed_tools[0].content_map_hash, "blake3:abc123");
    Ok(())
}

/// Sync with a v2 state.json on disk automatically upgrades it to the current
/// format.
///
/// The service reads the v2 state via the bridge, reconciles with desired tools
/// (none), and writes the state back in the current version.
#[tokio::test]
async fn sync_upgrades_v2_state_to_current_format() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;

    // Seed a v2-format state.json; the service loads it through the ladder
    // during sync and writes the current format back.
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::write(&state_path, serde_json::to_string_pretty(&v2_state_json(false)).unwrap())
        .expect("write v2 state.json");
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    // After sync, state.json should be the current format (array instead of map).
    let content = std::fs::read_to_string(&state_path).expect("read state.json after sync");
    let value: serde_json::Value =
        serde_json::from_str(&content).expect("state.json after sync should be valid JSON");
    assert_eq!(
        value["version"], 5,
        "state.json must be the current version after sync with v2 input"
    );
    assert!(
        value["managed_tools"].is_array(),
        "state.json managed_tools must be an array after the upgrade"
    );
    assert_eq!(
        value["managed_tools"].as_array().map(std::vec::Vec::len),
        Some(1),
        "state.json should still have 1 tool entry after upgrade"
    );
    Ok(())
}

/// `load_mediapm_state_document` returns the default state for a non-existent
/// path (no crash on missing file).
#[tokio::test]
async fn state_default_on_missing_file() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let missing_path = root.path().join("does-not-exist.json");
    let state = mediapm::load_mediapm_state_document(&missing_path)?;
    assert_eq!(state.version, 5, "default state should be the current version");
    assert!(state.managed_tools.is_empty(), "default state should have no managed tools");
    Ok(())
}

/// Regression: the post-sync warning check (`logical_tool_requires_sync`)
/// must resolve the canonical version from the SAME cache root the sync
/// used, not the default user-level cache. Divergent caches produce a
/// spurious `canonical_version != expected_composite` mismatch and a false
/// "tools require sync" warning.
///
/// The sync cache root is pre-seeded with a `BtbN` autobuild tag the mocked
/// upstream does not serve, and state records the composite that tag yields,
/// so the assertion holds only while the warning check reads the sync cache: a
/// check that fell through to the default cache would resolve from the mock
/// and compare a different tag. ffmpeg is used (not media-tagger) because its
/// provider resolves from cache; media-tagger resolves to `MEDIAPM_GIT_HASH`
/// regardless of cache, so it cannot demonstrate the divergence.
#[tokio::test]
async fn regression_warning_check_uses_sync_cache() -> Result<(), mediapm::MediaPmError> {
    let (mock, _seams) = mock_all_managed_tools().await;
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("ffmpeg".to_string(), ToolRequirement::default());
    let (service, _root, cache_root) = service_with_cache(runtime).await?;

    // A tag no upstream serves, so only this cache can produce it. The macOS
    // leg is left unseeded, so it resolves from the mock and the two cache
    // roots differ in the tag alone.
    let seeded_tag = "autobuild-1970-01-01-00-00";
    let expected_composite = format!("{seeded_tag}+{STUB_MARTIN_RIEDL_BUILD_ID}");

    // Seed the override cache's `tool_metadata` domain with the exact values
    // the ffmpeg provider resolves to, so the warning check (using the sync
    // cache root) computes the same canonical version as sync did.
    let metadata_domain = CacheDomainConfig {
        domain: "tool_metadata".to_string(),
        index_file_name: "tool_metadata.json".to_string(),
        entry_ttl_seconds: 24 * 60 * 60,
    };
    {
        let cache =
            Cache::open(cache_root.path(), &[metadata_domain]).await.expect("open override cache");
        let user_cache = UserLevelCache::from_cache(cache);
        let api_base = mock.github_api_base();
        let releases_key = format!("{api_base}/repos/BtbN/FFmpeg-Builds/releases?per_page=10");
        user_cache.store_bytes("tool_metadata", &releases_key, seeded_tag.as_bytes()).await;
        // Drop the handle before the warning check opens the same root
        // (flock contention otherwise fails the cache open).
    }

    // Register ffmpeg in state with the matching canonical version and a
    // non-empty content_map_hash so the warning check reaches the
    // canonical-version comparison.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: String::new(),
        canonical_version: expected_composite.clone(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });

    // Without the fix, the warning check resolves from the default cache
    // (which holds no such tag) → divergent canonical version → returns `true`
    // (false positive). With the fix, it uses the override cache → matching
    // version → `false`.
    assert!(
        !service.logical_tool_requires_sync("ffmpeg", &state).await?,
        "warning check must resolve from the sync cache root and report no sync needed",
    );
    Ok(())
}

/// A completed sync records exactly the canonical version the post-sync
/// warning check computes, so a sync never reports a warning about itself.
///
/// This is the end-to-end form of `regression_warning_check_uses_sync_cache`:
/// instead of seeding state by hand it syncs yt-dlp, whose same-step deno
/// dependency makes its stored version a composite, and then asks the warning
/// check about both tools. A warning check that compared the composite against
/// the bare resolved version would report a mismatch here.
///
/// deno's recorded version is its own bare hash, because deno has no same-step
/// deps, so this does not exercise the own-segment rule for a dependency that
/// is itself composite. `inline_deps::sync_composite_non_transitive` covers
/// that case.
#[tokio::test]
async fn sync_leaves_no_tool_requiring_sync() -> Result<(), mediapm::MediaPmError> {
    let (_mock, _seams) = mock_all_managed_tools().await;
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            dependencies: std::collections::BTreeMap::from([(
                "deno".to_string(),
                mediapm::ConfigVersionSpec::Latest,
            )]),
            ..Default::default()
        },
    );
    // deno is declared as well as depended on, so the warning check has a
    // requirement to resolve it against rather than skipping it as undeclared.
    runtime.tools.insert("deno".to_string(), ToolRequirement::default());
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    let state = read_state(&service);
    for tool_id in ["yt-dlp", "deno"] {
        assert!(
            !service.logical_tool_requires_sync(tool_id, &state).await?,
            "{tool_id} was just synced, so the warning check must agree it is current",
        );
    }
    Ok(())
}
