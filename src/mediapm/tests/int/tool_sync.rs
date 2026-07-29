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
use mediapm_conductor::{NickelDocument, ToolKindSpec, decode_document};
use tempfile::tempdir;

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
    runtime.tools.insert(
        "media-tagger".to_string(),
        ToolRequirement {
            version: mediapm::MediaMetadataValue::Literal(String::new()),
            ..Default::default()
        },
    );
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

    assert!(
        content_after_second.contains("MEDIAPM_MEDIA_TAGGER"),
        "env must contain media-tagger entry after re-sync where it is skipped"
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
    overrides.tools.insert(
        "media-tagger".to_string(),
        ToolRequirement {
            version: mediapm::MediaMetadataValue::Literal(String::new()),
            ..Default::default()
        },
    );
    let service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), overrides).await?;
    let mut state = MediaPmState::default();
    state.managed_tools.insert(
        "media-tagger".to_string(),
        ToolRegistryEntry {
            version: String::new(),
            canonical_version: mediapm::MEDIAPM_GIT_HASH.to_string(),
            content_map_hash: Some("blake3:abc".to_string()),
            deployed_at: 0,
        },
    );
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
        ToolRequirement {
            // Explicitly set a non-empty requirement version to prove that
            // `ToolRegistryEntry.canonical_version` uses the resolved canonical
            // version, not this requirement value.
            version: mediapm::MediaMetadataValue::Literal("2.0.0".to_string()),
            ..Default::default()
        },
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
        .get("media-tagger")
        .expect("media-tagger should be registered after sync");

    let expected_canonical = mediapm::MEDIAPM_GIT_HASH;
    assert_eq!(
        entry.canonical_version, expected_canonical,
        "canonical_version should be the git hash"
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
            version: mediapm::MediaMetadataValue::Literal("2.0.0".to_string()),
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
