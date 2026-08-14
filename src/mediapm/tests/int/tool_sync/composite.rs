use mediapm::{MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement};

use crate::common::service_with_cache;

// ---------------------------------------------------------------------------
// Composite canonical_version integration tests
// ---------------------------------------------------------------------------
//
// These tests validate that the refactored code path
// (`compute_composite_canonical_version` + `index_managed_tools`) is wired
// correctly through `reconcile_desired_tools` and `logical_tool_requires_sync`.
//
// SameStep-specific composite inclusion (dep versions appended to bare) is
// covered by the unit tests in `sync/mod.rs`:
// `compute_composite_canonical_version_no_deps`
// `compute_composite_canonical_version_with_same_step_deps`
//
// Full end-to-end SameStep composite sync (e.g. yt-dlp + ffmpeg) requires
// hermetic HTTP mocking at the provider level, which is covered by:
// `fetch_and_import_ytdlp_full_pipeline` in `provision.rs`.

/// After sync, the stored `canonical_version` in `state.json` is populated
/// via `compute_composite_canonical_version`.
///
/// Uses media-tagger (resolves without network, `CrossStep` deps → composite
/// equals bare). Validates the refactored storage path is live.
#[tokio::test]
async fn sync_stores_composite_canonical_version() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;
    service.sync_tools().await?;

    let bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state.json after sync");
    let state: MediaPmState =
        serde_json::from_slice(&bytes).expect("state.json should deserialize");

    let entry = state
        .managed_tools
        .iter()
        .find(|e| e.tool_id == "media-tagger")
        .expect("media-tagger should be in state after sync");

    // canonical_version must be non-empty and equal to MEDIAPM_GIT_HASH
    // (the bare canonical version for builtin launcher tools).
    assert!(!entry.canonical_version.is_empty(), "stored canonical_version must not be empty");
    assert_eq!(
        entry.canonical_version,
        mediapm::MEDIAPM_GIT_HASH,
        "canonical_version should equal MEDIAPM_GIT_HASH for builtin launcher"
    );
    // For a tool with CrossStep-only deps, composite == bare.
    // If SameStep deps were present, the composite would include
    // ";dep_id:dep_version" suffixes — verified by unit tests.
    assert!(
        !entry.canonical_version.contains(';'),
        "CrossStep-only composite should have no ';' delimiter"
    );
    Ok(())
}

/// Re-syncing with unchanged tool state produces a matching sync report
/// (tools are skipped, not re-added).
///
/// Uses media-tagger as the configured tool. On first sync it is added;
/// on re-sync the skip check detects no change and does not re-provision.
/// The state.json content is identical after re-sync.
#[tokio::test]
async fn sync_skip_triggers_on_unchanged_composite() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;

    let state_path = service.paths().mediapm_state_json.clone();

    // First sync: media-tagger is provisioned
    service.sync_tools().await?;
    let state_after_first = std::fs::read(&state_path).expect("state.json after first sync");

    // Second sync: media-tagger should be skipped (composite matches)
    service.sync_tools().await?;
    let state_after_second = std::fs::read(&state_path).expect("state.json after second sync");

    assert_eq!(
        state_after_first, state_after_second,
        "state.json content must be identical after re-sync with unchanged tools"
    );
    Ok(())
}

/// `logical_tool_requires_sync` returns `false` when the stored composite
/// `canonical_version` matches the computed composite from the shared helper.
///
/// This validates that the refactored comparison in `service.rs` uses
/// `compute_composite_canonical_version` for apples-to-apples comparison.
#[tokio::test]
async fn sync_logical_requires_sync_composite_comparison() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    // Configure media-tagger so it's a desired tool with matching deps.
    runtime.tools.insert(
        "media-tagger".to_string(),
        ToolRequirement {
            dependencies: std::collections::BTreeMap::from([(
                "ffmpeg".to_string(),
                mediapm::ConfigVersionSpec::Latest,
            )]),
            ..Default::default()
        },
    );
    let (service, _root, _cache_root) = service_with_cache(runtime).await?;

    // Seed state with media-tagger at its expected composite canonical_version
    // (which equals bare MEDIAPM_GIT_HASH since ffmpeg is CrossStep).
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

    // composite should match → no sync needed
    assert!(
        !service.logical_tool_requires_sync("media-tagger", &state).await?,
        "logical_tool_requires_sync should return false when composite matches"
    );
    Ok(())
}

/// `logical_tool_requires_sync` returns `true` when the stored composite
/// `canonical_version` differs from the computed composite.
#[tokio::test]
async fn sync_logical_requires_sync_on_composite_mismatch() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let (service, _root, _cache_root) = service_with_cache(runtime).await?;

    // Seed state with media-tagger at a WRONG canonical_version.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: String::new(),
        canonical_version: "some-wrong-version".to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });

    // composite differs → needs sync
    assert!(
        service.logical_tool_requires_sync("media-tagger", &state).await?,
        "logical_tool_requires_sync should return true when composite mismatches"
    );
    Ok(())
}
