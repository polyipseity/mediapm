//! # Tool-sync integration tests
//!
//! Tests for [`MediaPmService::sync_tools()`] — the managed-tool
//! reconciliation pipeline (download, register, provision, content-import,
//! lifecycle, env generation).
//!
//! **Do NOT add workflow-sync or state-sync tests here.** This file is
//! exclusively for the tool provisioning / syncing subset of the mediapm
//! sync pipeline. Other sync concerns (hierarchy, materialization,
//! conductor orchestration) belong in separate test modules.
//!
//! These tests focus on file-creation guarantees, document structure,
//! idempotency, and pure-function logic — not on counter values
//! (`added_tools`, `updated_tools`, etc.).

use mediapm::{
    MediaPmService, MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement,
};
use mediapm_conductor::tools::provider::VersionSpecFields;
use mediapm_conductor::{NickelDocument, ToolKindSpec, decode_document};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Dependency validation integration tests
// ---------------------------------------------------------------------------

/// Sync rejects a tool with an unknown dependency key (e.g. `ffmpeg_version`
/// instead of `ffmpeg`) and suggests the correct key via "did you mean".
///
/// Uses `runtime_storage_overrides.tools` to inject the bad dependency
/// (bypasses NCL evaluation, which expects Nickel syntax).
#[tokio::test]
async fn sync_rejects_bad_dependency_key() {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");

    use std::collections::BTreeMap;

    let bad_deps: BTreeMap<String, mediapm::ConfigVersionSpec> =
        [("ffmpeg_version".to_string(), mediapm::ConfigVersionSpec::Latest)].into();

    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement { dependencies: bad_deps, ..ToolRequirement::default() },
    );

    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime)
            .await
            .expect("service creation");

    // Sync should fail with MPM-E001 and a suggestion.
    let result = service.sync_tools().await;
    let err = match result {
        Ok(_) => panic!("sync should fail with bad dep key, but succeeded"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains("MPM-E001"), "error should contain MPM-E001 code, got: {msg}");
    assert!(msg.contains("ffmpeg_version"), "error should mention the bad key: {msg}");
    assert!(
        msg.contains("did you mean") || msg.contains("suggestion"),
        "error should suggest alternatives: {msg}"
    );
    assert!(msg.contains("ffmpeg"), "suggestion should mention 'ffmpeg': {msg}");
}

/// Sync rejects a dependency key that is not in the tool's registered
/// `dependency_types()`, even when that key is present in `desired_tools`
/// as a configured tool.
///
/// e.g. `sd` is registered as a dependency for `rsgain`, not for `yt-dlp`.
/// So yt-dlp with `dependencies.sd = "latest"` must be rejected.
#[tokio::test]
async fn sync_rejects_dep_key_not_in_known_types() {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");

    use std::collections::BTreeMap;

    let bad_deps: BTreeMap<String, mediapm::ConfigVersionSpec> =
        [("sd".to_string(), mediapm::ConfigVersionSpec::Latest)].into();

    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    // Configure yt-dlp with `sd` as a dependency — `sd` is in rsgain's
    // dependency_types but NOT in yt-dlp's.
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement { dependencies: bad_deps, ..ToolRequirement::default() },
    );
    // Also configure `sd` as a tool in desired_tools — should NOT make it
    // a valid dependency key for yt-dlp.
    runtime.tools.insert("sd".to_string(), ToolRequirement::default());

    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime)
            .await
            .expect("service creation");

    let result = service.sync_tools().await;
    let err = match result {
        Ok(_) => panic!("sync should fail with dep key not in known types, but succeeded"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains("MPM-E001"), "error should contain MPM-E001 code, got: {msg}");
    assert!(msg.contains("sd"), "error should mention the bad key: {msg}");
    assert!(
        msg.contains("did you mean") || msg.contains("suggestion"),
        "error should suggest alternatives: {msg}"
    );
    assert!(msg.contains("ffmpeg"), "suggestion should mention 'ffmpeg': {msg}");
    assert!(msg.contains("deno"), "suggestion should mention 'deno': {msg}");
}

// ---------------------------------------------------------------------------
// Structural side-effect tests (no counter assertions)
// ---------------------------------------------------------------------------

/// Sync on a completely empty workspace completes without error.
#[tokio::test]
async fn sync_empty_workspace_succeeds() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    let _summary = service.sync_tools().await?;
    Ok(())
}

/// Sync creates the expected runtime directories under `.mediapm/`.
#[tokio::test]
async fn sync_creates_runtime_directories() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;
    let paths = service.paths();
    assert!(paths.runtime_root.exists(), "runtime root .mediapm/ should exist");
    assert!(paths.tools_dir.exists(), "tools/ directory should exist");
    Ok(())
}

/// Sync creates `state.json` containing a version field.
#[tokio::test]
async fn sync_creates_state_document() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;
    let state_path = &service.paths().mediapm_state_json;
    assert!(state_path.exists(), "state.json should exist");
    let content = std::fs::read_to_string(state_path).expect("state.json should be readable");
    assert!(!content.is_empty(), "state.json must not be empty");
    assert!(content.contains("version"), "state.json must contain a version field");
    Ok(())
}

/// Sync creates `conductor.generated.ncl` with tools registered.
#[tokio::test]
async fn sync_creates_generated_document() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;
    let generated_path = &service.paths().conductor_generated_ncl;
    assert!(generated_path.exists(), "conductor.generated.ncl should exist");
    let bytes = std::fs::read(generated_path).expect("conductor.generated.ncl should be readable");
    let doc: NickelDocument = decode_document(&bytes).expect("valid Nickel document");
    assert!(!doc.tools.is_empty(), "generated doc must have tools");
    Ok(())
}

/// Sync creates `.env.generated` with a comment header.
#[tokio::test]
async fn sync_creates_env_generated() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;
    let env_path = &service.paths().env_generated_file;
    assert!(env_path.exists(), ".env.generated should exist");
    let content = std::fs::read_to_string(env_path).expect("env file should be readable");
    assert!(!content.is_empty(), "env file must not be empty");
    assert!(content.starts_with('#'), "env file must start with a comment header");
    Ok(())
}

/// Env var names in `.env.generated` must not contain the `@` character
/// (no content-addressed hash leakage into env var names).
///
/// This integration test runs through the full sync pipeline. Without
/// network-provisioned payload tools there will be no content-map entries,
/// but the assertion guards against regression where `@` could appear in
/// generated header lines or future entries.
#[tokio::test]
async fn sync_env_has_no_hash_in_names() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;
    let env_path = &service.paths().env_generated_file;
    let content = std::fs::read_to_string(env_path).expect("env file should be readable");
    for line in content.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(name) = line.split('=').next() {
            assert!(!name.contains('@'), "env var name must not contain @: '{name}' in env file");
        }
    }
    Ok(())
}

/// Env var values in `.env.generated` must contain the `/payload/` path
/// segment, matching the `ProvisionCache` layout.
///
/// Without network-provisioned payloads there will be no entries, so the
/// assertion only applies to non-comment, non-empty lines. The unit tests
/// in `tool_config.rs` verify the actual path construction with content
/// maps.
#[tokio::test]
async fn sync_env_paths_contain_payload_segment() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;
    let env_path = &service.paths().env_generated_file;
    let content = std::fs::read_to_string(env_path).expect("env file should be readable");
    for line in content.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        // Each non-comment line should be KEY=VALUE
        if let Some((_name, value)) = line.split_once('=') {
            assert!(
                value.contains("/payload/"),
                "env var value must contain /payload/ segment: {value} in env file"
            );
            let raw = value.trim_matches('"');
            assert!(
                raw.starts_with('/') || raw.starts_with("\\\\") || raw.chars().nth(1) == Some(':'),
                "env var value must be absolute: {raw}"
            );
        }
    }
    Ok(())
}

/// Sync registers all five built-in tools in the generated conductor
/// document.
#[tokio::test]
async fn sync_registers_builtins() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
    service.sync_tools().await?;
    let bytes = std::fs::read(&service.paths().conductor_generated_ncl)
        .expect("conductor.generated.ncl should be readable");
    let doc: NickelDocument = decode_document(&bytes).expect("valid Nickel document");
    for id in &["echo@v1", "fs@v1", "import@v1", "export@v1", "archive@v1"] {
        let tool =
            doc.tools.get(*id).unwrap_or_else(|| panic!("builtin {id} should be registered"));
        assert!(
            matches!(tool.kind, ToolKindSpec::Builtin { .. }),
            "builtin {id} must have kind=builtin"
        );
    }
    Ok(())
}

/// Skipped tools (already at canonical version) still get entries in
/// `.env.generated` after re-sync.
#[tokio::test]
async fn sync_twice_env_generated_persists() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;

    let env_path = service.paths().env_generated_file.clone();
    let content_after_first =
        std::fs::read_to_string(&env_path).expect("env file should be readable after first sync");

    // Second sync — media-tagger should be skipped (already at canonical version)
    service.sync_tools().await?;
    let content_after_second =
        std::fs::read_to_string(&env_path).expect("env file should be readable after second sync");

    // Env file persists across re-syncs with identical content.
    assert!(!content_after_second.is_empty(), "env file must not be empty after re-sync");
    assert!(
        content_after_second.starts_with('#'),
        "env file must start with a comment header after re-sync"
    );
    assert_eq!(
        content_after_first, content_after_second,
        "env content must be identical after re-sync"
    );
    Ok(())
}

/// Re-syncing produces an identical state document (idempotency).
#[tokio::test]
async fn sync_is_idempotent() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;
    let state_after_first =
        std::fs::read(&service.paths().mediapm_state_json).expect("state.json should exist");
    let _ = service.sync_tools().await?;
    let state_after_second =
        std::fs::read(&service.paths().mediapm_state_json).expect("state.json should exist");
    assert_eq!(state_after_first, state_after_second, "state.json must be identical after re-sync");
    Ok(())
}

// ---------------------------------------------------------------------------
// Pure-function logic tests
// ---------------------------------------------------------------------------

/// `logical_tool_requires_sync` returns `true` for a tool absent from state.
#[tokio::test]
async fn sync_tool_requires_sync_when_missing() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_fs_at(root.path()).await?;
    let state = MediaPmState::default();
    assert!(service.logical_tool_requires_sync("non-existent", &state).await?);
    Ok(())
}

/// `logical_tool_requires_sync` returns `false` for a tool that is present
/// in state with matching canonical version.
#[tokio::test]
async fn sync_tool_requires_sync_false_when_present() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
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
        deployed_at: 0,
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
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("tempdir for cache");
    let mut overrides = MediaRuntimeStorage::default();
    overrides.cache_root_override = Some(cache_root.path().to_path_buf());
    overrides.tools.insert(
        "media-tagger".to_string(),
        // `ToolRegistryEntry.canonical_version` uses the resolved canonical
        // version (git hash), not the requirement's version_spec.
        ToolRequirement::default(),
    );
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), overrides).await?;
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
    let root = tempdir().expect("tempdir");
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
    let root = tempdir().expect("tempdir");
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
/// seeds. Regression guard: if `compute_used_tool_ids` fails to include a
/// desired tool, pruning would incorrectly flag it as unused.
#[tokio::test]
async fn sync_no_pruning_for_configured_tools() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
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
    let root = tempdir().expect("tempdir");
    let state_path = root.path().join("state.json");

    let v2_json = serde_json::json!({
        "version": 2,
        "managed_files": {},
        "managed_tools": {
            "ffmpeg": {
                "version": "7.1",
                "canonical_version": "ffmpeg-v7.1",
                "content_map_hash": "blake3:abc123",
                "deployed_at": 1_700_000_000,
                "resolved_tag": "v7.1",
                "resolved_version": "7.1",
                "resolved_vcs_hash": "abc"
            }
        },
        "workflow_states": {}
    });

    std::fs::write(&state_path, serde_json::to_string_pretty(&v2_json).unwrap())
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
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let state_path = root.path().join(".mediapm").join("state.json");
    std::fs::create_dir_all(state_path.parent().unwrap()).expect("create .mediapm dir");

    let v2_json = serde_json::json!({
        "version": 2,
        "managed_files": {},
        "managed_tools": {
            "ffmpeg": {
                "version": "7.1",
                "canonical_version": "ffmpeg-v7.1",
                "content_map_hash": "blake3:abc123",
                "deployed_at": 1_700_000_000,
                "resolved_tag": null,
                "resolved_version": null,
                "resolved_vcs_hash": null
            }
        },
        "workflow_states": {}
    });

    std::fs::write(&state_path, serde_json::to_string_pretty(&v2_json).unwrap())
        .expect("write v2 state.json");

    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
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
        value["managed_tools"].as_array().map(|a| a.len()),
        Some(1),
        "state.json should still have 1 tool entry after upgrade"
    );
    Ok(())
}

/// `load_mediapm_state_document` returns the default state for a non-existent
/// path (no crash on missing file).
#[tokio::test]
async fn state_default_on_missing_file() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let missing_path = root.path().join("does-not-exist.json");
    let state = mediapm::load_mediapm_state_document(&missing_path)?;
    assert_eq!(state.version, 3, "default state should be version 3");
    assert!(state.managed_tools.is_empty(), "default state should have no managed tools");
    Ok(())
}

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
/// Uses media-tagger (resolves without network, CrossStep deps → composite
/// equals bare). Validates the refactored storage path is live.
#[tokio::test]
async fn sync_stores_composite_canonical_version() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
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
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

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
/// canonical_version matches the computed composite from the shared helper.
///
/// This validates that the refactored comparison in `service.rs` uses
/// `compute_composite_canonical_version` for apples-to-apples comparison.
#[tokio::test]
async fn sync_logical_requires_sync_composite_comparison() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
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
    let service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    // Seed state with media-tagger at its expected composite canonical_version
    // (which equals bare MEDIAPM_GIT_HASH since ffmpeg is CrossStep).
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: String::new(),
        canonical_version: mediapm::MEDIAPM_GIT_HASH.to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: 0,
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
/// canonical_version differs from the computed composite.
#[tokio::test]
async fn sync_logical_requires_sync_on_composite_mismatch() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    // Seed state with media-tagger at a WRONG canonical_version.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: String::new(),
        canonical_version: "some-wrong-version".to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: 0,
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
