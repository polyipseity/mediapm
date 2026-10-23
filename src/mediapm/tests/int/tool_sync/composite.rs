use mediapm::{
    MediaPmService, MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement,
};

use crate::common::stub_upstream::{
    STUB_BTBN_TAG, STUB_COMMIT_SHA, STUB_DENO_COMMIT_SHA, STUB_MARTIN_RIEDL_BUILD_ID,
    mock_all_managed_tools,
};
use crate::common::{service_with_cache, test_sync_progress_overrides};

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
// The end-to-end SameStep composite needs providers that download, so it needs
// a mocked upstream; `sync_composite_carries_same_step_dep_segments` below
// supplies one.

/// Reads `state.json` after a sync.
///
/// # Panics
///
/// Panics when `state.json` is missing or does not deserialize — every caller
/// runs it on a service that just completed a sync.
fn read_state(service: &MediaPmService<mediapm_cas::FileSystemCas>) -> MediaPmState {
    let bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state.json after sync");
    serde_json::from_slice(&bytes).expect("state.json deserializes")
}

/// Returns the part of a canonical version before its first `;`.
///
/// A tool's stored `canonical_version` is its composite, so a tool carrying
/// `SameStep` dependencies stores more than its own version. This reads back the
/// own segment, which is what a dependency contributes to its requirer's
/// composite — a dependency's own composite is never nested into another's.
///
/// # Panics
///
/// Panics when `version` is empty, since `split` always yields at least one
/// segment and every caller passes a version the sync just recorded.
fn own_version_segment(version: &str) -> &str {
    version.split(';').next().expect("a version has a first segment")
}

/// Returns the registry entry for `tool_id` from a synced state document.
///
/// # Panics
///
/// Panics when the state holds no entry for `tool_id`.
fn tool_entry<'a>(state: &'a MediaPmState, tool_id: &str) -> &'a ToolRegistryEntry {
    state
        .managed_tools
        .iter()
        .find(|entry| entry.tool_id == tool_id)
        .unwrap_or_else(|| panic!("{tool_id} entry should exist after sync"))
}

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
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

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

/// A full sync stores a composite `canonical_version` carrying each `SameStep`
/// dependency's own version segment.
///
/// This is the end-to-end counterpart to the `compute_composite_canonical_version`
/// unit tests. Those assert the format against a hand-built requirement; this
/// one drives a real provisioning pass over three GitHub-release providers whose
/// declared edges are `SameStep`, so the string the pipeline *stores* is the one
/// it computed from the versions it had just resolved.
///
/// The media-tagger case above cannot reach it: media-tagger's only declared
/// dependency is `CrossStep`, so its composite equals its bare version by design
/// and the segment-appending path never runs.
///
/// The expected string is assembled from the dependency entries of the same
/// state rather than from the stub constants, so the assertion pins the
/// *relationship* — each segment is the dep's own version, ordered by dep id,
/// never its composite — without also encoding what each provider happened to
/// resolve. The three fixture values are asserted separately, so a composite
/// built from garbage cannot satisfy the relationship by accident.
#[tokio::test]
async fn sync_composite_carries_same_step_dep_segments() -> Result<(), mediapm::MediaPmError> {
    use std::collections::BTreeMap;

    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            dependencies: BTreeMap::from([
                ("ffmpeg".to_string(), mediapm::ConfigVersionSpec::Latest),
                ("deno".to_string(), mediapm::ConfigVersionSpec::Latest),
            ]),
            ..Default::default()
        },
    );
    runtime.tools.insert("ffmpeg".to_string(), ToolRequirement::default());
    runtime.tools.insert("deno".to_string(), ToolRequirement::default());

    let (_mock, _seams) = mock_all_managed_tools().await;
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    let state = read_state(&service);
    let yt_dlp = tool_entry(&state, "yt-dlp");
    let deno = tool_entry(&state, "deno");
    let ffmpeg = tool_entry(&state, "ffmpeg");

    // The fixture really resolved distinct versions, so the relationship
    // asserted below cannot be satisfied by three identical strings. Each
    // comparison reads the own segment, because the stored value of a tool with
    // SameStep deps is the composite rather than the bare version.
    assert_eq!(
        own_version_segment(&yt_dlp.canonical_version),
        STUB_COMMIT_SHA,
        "yt-dlp's own version is its commit hash: {}",
        yt_dlp.canonical_version,
    );
    assert_eq!(
        own_version_segment(&deno.canonical_version),
        STUB_DENO_COMMIT_SHA,
        "deno's own version is its commit hash: {}",
        deno.canonical_version,
    );
    assert!(
        own_version_segment(&ffmpeg.canonical_version).starts_with(STUB_BTBN_TAG),
        "ffmpeg's own version is its BtbN composite: {}",
        ffmpeg.canonical_version,
    );
    assert!(
        own_version_segment(&ffmpeg.canonical_version).contains(STUB_MARTIN_RIEDL_BUILD_ID),
        "ffmpeg's own version carries the macOS leg actually fetched: {}",
        ffmpeg.canonical_version,
    );

    let expected = format!(
        "{STUB_COMMIT_SHA};deno:{STUB_DENO_COMMIT_SHA};ffmpeg:{}",
        own_version_segment(&ffmpeg.canonical_version),
    );
    assert_eq!(
        yt_dlp.canonical_version, expected,
        "the stored composite must carry each SameStep dep's own version, ordered by dep id",
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
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;
    let state_after_first = std::fs::read(&state_path).expect("state.json after first sync");

    // Second sync: media-tagger should be skipped (composite matches)
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;
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
