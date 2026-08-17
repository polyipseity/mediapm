use super::v2_state_json;
use mediapm::{
    MediaPmService, MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement,
};
use mediapm_conductor::cache::Cache;
use mediapm_conductor::cache::CacheDomainConfig;
use mediapm_conductor::cache_user_level::UserLevelCache;
use mediapm_conductor::tools::provider::VersionSpecFields;

use crate::common::service_with_cache;

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
    service.sync_tools().await?;

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
    let summary = service.sync_tools().await?;
    assert_eq!(
        summary.pruned_tools, 0,
        "configured tools must not be pruned; pruned={}",
        summary.pruned_tools,
    );
    Ok(())
}

/// A v2 state.json on disk is automatically bridged to v3 on load.
///
/// This tests the full version-dispatch path: JSON parsing → version
/// field extraction → v2→v3 bridge → `MediaPmState` with
/// `Vec<ToolRegistryEntry>`.
#[tokio::test]
async fn state_v2_on_disk_bridges_to_v3_on_load() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let state_path = root.path().join("state.json");

    std::fs::write(&state_path, serde_json::to_string_pretty(&v2_state_json(true)).unwrap())
        .expect("write v2 state.json");

    let state = mediapm::load_mediapm_state_document(&state_path).expect("load v2 state.json");

    assert_eq!(state.version, 3, "v2 state should bridge to version 3");
    assert_eq!(state.managed_tools.len(), 1, "v2 bridge should produce one tool entry");
    assert_eq!(state.managed_tools[0].tool_id, "ffmpeg");
    assert_eq!(state.managed_tools[0].canonical_version, "ffmpeg-v7.1");
    assert_eq!(state.managed_tools[0].content_map_hash, "blake3:abc123");
    Ok(())
}

/// Sync with a v2 state.json on disk automatically upgrades it to v3.
///
/// The service reads the v2 state via the bridge, reconciles with desired
/// tools (none), and writes the state back as v3.
#[tokio::test]
async fn sync_upgrades_v2_state_to_v3_format() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;

    // Seed a v2-format state.json; the service loads it via the v2→v3 bridge
    // during sync and writes the upgraded v3 format back.
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::write(&state_path, serde_json::to_string_pretty(&v2_state_json(false)).unwrap())
        .expect("write v2 state.json");
    service.sync_tools().await?;

    // After sync, state.json should be v3 format (array instead of map).
    let content = std::fs::read_to_string(&state_path).expect("read state.json after sync");
    let value: serde_json::Value =
        serde_json::from_str(&content).expect("state.json after sync should be valid JSON");
    assert_eq!(value["version"], 3, "state.json must be version 3 after sync with v2 input");
    assert!(
        value["managed_tools"].is_array(),
        "state.json managed_tools must be an array after v2→v3 upgrade"
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
    assert_eq!(state.version, 3, "default state should be version 3");
    assert!(state.managed_tools.is_empty(), "default state should have no managed tools");
    Ok(())
}

/// Regression: the post-sync warning check (`logical_tool_requires_sync`)
/// must resolve the canonical version from the SAME cache root the sync
/// used, not the default user-level cache. Divergent caches produce a
/// spurious `canonical_version != expected_composite` mismatch and a false
/// "tools require sync" warning.
///
/// ffmpeg is used (not media-tagger) because its provider resolves from the
/// seeded override cache; media-tagger resolves to `MEDIAPM_GIT_HASH`
/// regardless of cache, so it cannot demonstrate the divergence.
#[tokio::test]
async fn regression_warning_check_uses_sync_cache() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("ffmpeg".to_string(), ToolRequirement::default());
    let (service, _root, cache_root) = service_with_cache(runtime).await?;

    // Seed the override cache's `tool_metadata` domain with the exact values
    // the ffmpeg provider resolves to, so the warning check (using the sync
    // cache root) computes the same canonical version as sync did.
    let metadata_domain = CacheDomainConfig {
        domain: "tool_metadata".to_string(),
        index_file_name: "tool_metadata.json".to_string(),
        entry_ttl_seconds: 24 * 60 * 60,
    };
    let cache =
        Cache::open(cache_root.path(), &[metadata_domain]).await.expect("open override cache");
    let user_cache = UserLevelCache::from_cache(cache);
    user_cache
        .store_bytes(
            "tool_metadata",
            "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
            b"autobuild-2025-07-15-12-00",
        )
        .await;
    user_cache
        .store_bytes("tool_metadata", "https://evermeet.cx/ffmpeg/getrelease/zip", b"8.1.2")
        .await;

    // Register ffmpeg in state with the matching canonical version and a
    // non-empty content_map_hash so the warning check reaches the
    // canonical-version comparison.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: String::new(),
        canonical_version: "autobuild-2025-07-15-12-00+evermeet-8.1.2".to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });

    // Without the fix, the warning check resolves from the default cache
    // (None) → divergent canonical version → returns `true` (false positive).
    // With the fix, it uses the override cache → matching version → `false`.
    assert!(
        !service.logical_tool_requires_sync("ffmpeg", &state).await?,
        "warning check must resolve from the sync cache root and report no sync needed",
    );
    Ok(())
}
