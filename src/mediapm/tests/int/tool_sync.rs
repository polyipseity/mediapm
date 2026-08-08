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
use mediapm_conductor::cache::{Cache, CacheDomainConfig};
use mediapm_conductor::tools::provider::VersionSpecFields;
use mediapm_conductor::{
    NickelDocument, ToolKindSpec, ToolRuntime, ToolSpec, decode_document, encode_document,
};
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

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
    use std::collections::BTreeMap;

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");

    let bad_deps: BTreeMap<String, mediapm::ConfigVersionSpec> =
        [("ffmpeg_version".to_string(), mediapm::ConfigVersionSpec::Latest)].into();

    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let Err(err) = result else {
        panic!("sync should fail with bad dep key, but succeeded");
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
    use std::collections::BTreeMap;

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");

    let bad_deps: BTreeMap<String, mediapm::ConfigVersionSpec> =
        [("sd".to_string(), mediapm::ConfigVersionSpec::Latest)].into();

    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let Err(err) = result else {
        panic!("sync should fail with dep key not in known types, but succeeded");
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    let _summary = service.sync_tools().await?;
    Ok(())
}

/// Sync creates the expected runtime directories under `.mediapm/`.
#[tokio::test]
async fn sync_creates_runtime_directories() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
/// Names derive from the plain mediapm tool id (`yt-dlp`, `ffmpeg`, ...)
/// after stripping the `@hash` suffix of the mediapm conductor tool id
/// (the generated doc `tools` map key). The `@hash` suffix lives only in
/// the *path values* (the sanitized provision-cache directory segment).
///
/// This integration test runs through the full sync pipeline. Without
/// network-provisioned payload tools there will be no content-map entries,
/// but the assertion guards against regression where `@` could appear in
/// generated header lines or future entries.
#[tokio::test]
async fn sync_env_has_no_hash_in_names() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
/// Path values point at
/// `<tools_dir>/<sanitize_tool_id(conductor_tool_id)>/payload/<key>` — the
/// path segment is the *mediapm conductor tool id* (the generated doc
/// `tools` map key, `{name}@{hash}`), sanitized for the filesystem, never
/// the bare mediapm tool id. See the dedicated
/// `sync_env_paths_use_conductor_tool_id` test for the full keyed-by-id
/// regression coverage.
///
/// Without network-provisioned payloads there will be no entries, so the
/// assertion only applies to non-comment, non-empty lines. The unit tests
/// in `tool_config.rs` verify the actual path construction with content
/// maps.
#[tokio::test]
async fn sync_env_paths_contain_payload_segment() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
///
/// Uses an explicit `cache_root_override` so the sync never touches the real
/// OS user cache, and asserts the real cache mtime is unchanged (hermetic
/// isolation regression guard).
#[tokio::test]
async fn sync_registers_builtins() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };

    // Record real cache state before the sync.
    let real_cache_mtime =
        mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root()
            .and_then(|p| std::fs::metadata(p.join("tools.json")).ok())
            .and_then(|m| m.modified().ok());

    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
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

    // Verify the override path was used (cache files initialized there).
    assert!(
        cache_root.path().join("tools.json").exists() || cache_root.path().join("store").exists(),
        "override cache dir should have been initialized",
    );

    // Verify the real cache was not modified by the sync (mtime unchanged).
    let real_cache_mtime_after =
        mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root()
            .and_then(|p| std::fs::metadata(p.join("tools.json")).ok())
            .and_then(|m| m.modified().ok());
    assert_eq!(
        real_cache_mtime, real_cache_mtime_after,
        "real cache directory must not be modified when cache_root_override is set",
    );
    Ok(())
}

/// Skipped tools (already at canonical version) still get entries in
/// `.env.generated` after re-sync.
#[tokio::test]
async fn sync_twice_env_generated_persists() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("tempdir for cache");
    let mut overrides = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
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

    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
/// `canonical_version` matches the computed composite from the shared helper.
///
/// This validates that the refactored comparison in `service.rs` uses
/// `compute_composite_canonical_version` for apples-to-apples comparison.
#[tokio::test]
async fn sync_logical_requires_sync_composite_comparison() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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

// ---------------------------------------------------------------------------
// Resolved-field population (resolved_tag / resolved_version / resolved_vcs_hash)
// ---------------------------------------------------------------------------
//
// These tests validate the Phase 3 wiring: the provider's resolved metadata
// is persisted into `state.json` on provision, backfilled in place for
// skipped tools, and matched by exact version specs. Media-tagger is used
// throughout because it resolves without network.

/// Sync persists provider-resolved fields into the managed-tool registry.
///
/// Uses media-tagger (builtin launcher, no network). Expected per the
/// provider matrix: `resolved_tag` stays `None` (no upstream tag —
/// why-empty invariant), `resolved_version` is the mediapm crate version,
/// and `resolved_vcs_hash` is the mediapm git hash.
#[tokio::test]
async fn sync_populates_resolved_fields_in_state() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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
        .expect("media-tagger entry should exist after sync");

    // WHY: media-tagger is a builtin launcher shipped inside mediapm; there
    // is no upstream tag that identifies the artifact set.
    assert_eq!(entry.resolved_tag, None, "media-tagger has no upstream tag");
    assert_eq!(
        entry.resolved_version.as_deref(),
        Some(env!("CARGO_PKG_VERSION")),
        "resolved_version should be the mediapm crate version"
    );
    assert_eq!(
        entry.resolved_vcs_hash.as_deref(),
        Some(mediapm::MEDIAPM_GIT_HASH),
        "resolved_vcs_hash should be the mediapm git hash"
    );
    assert_eq!(
        entry.canonical_version,
        mediapm::MEDIAPM_GIT_HASH,
        "canonical_version should equal MEDIAPM_GIT_HASH for builtin launcher"
    );
    Ok(())
}

/// Skipped tools get `None` resolved fields backfilled in place from fresh
/// provider metadata, while identity fields are preserved.
///
/// Seeds state.json with a media-tagger entry whose resolved fields are all
/// `None` plus a non-empty `content_map_hash` (so the skip check fires). After
/// re-sync the resolved fields are filled, but `content_map_hash`,
/// `deployed_at`, and `version` are untouched — proving no re-provision.
#[tokio::test]
async fn sync_skip_backfills_resolved_fields() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    // First sync provisions media-tagger, populates workspace CAS, and writes
    // conductor.generated.ncl so the skip path can verify content-map bytes.
    service.sync_tools().await?;

    let state_path = service.paths().mediapm_state_json.clone();
    let bytes = std::fs::read(&state_path).expect("state.json after provision");
    let provisioned_state: MediaPmState =
        serde_json::from_slice(&bytes).expect("state.json should deserialize");
    let provisioned = provisioned_state
        .managed_tools
        .iter()
        .find(|entry| entry.tool_id == "media-tagger")
        .expect("media-tagger entry should exist after provision");

    // Reset resolved fields while preserving identity fields that trigger skip.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: provisioned.canonical_version.clone(),
        content_map_hash: provisioned.content_map_hash.clone(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    let bytes = serde_json::to_vec(&state).expect("state serializes");
    std::fs::write(&state_path, bytes).expect("write seeded state");

    service.sync_tools().await?;

    let bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state.json after sync");
    let state: MediaPmState =
        serde_json::from_slice(&bytes).expect("state.json should deserialize");
    let entry = state
        .managed_tools
        .iter()
        .find(|e| e.tool_id == "media-tagger")
        .expect("media-tagger entry should exist after sync");

    // Backfilled resolved fields (why-empty preserved for tag).
    assert_eq!(entry.resolved_tag, None, "why-empty tag must stay None");
    assert_eq!(
        entry.resolved_version.as_deref(),
        Some(env!("CARGO_PKG_VERSION")),
        "backfilled resolved_version should be the mediapm crate version"
    );
    assert_eq!(
        entry.resolved_vcs_hash.as_deref(),
        Some(mediapm::MEDIAPM_GIT_HASH),
        "backfilled resolved_vcs_hash should be the mediapm git hash"
    );

    // Identity fields preserved — skip path never re-provisions.
    assert_eq!(
        entry.content_map_hash, provisioned.content_map_hash,
        "content_map_hash must be preserved"
    );
    assert_eq!(
        entry.deployed_at,
        mediapm_utils::Timestamp::from_unix_secs(42),
        "deployed_at must be preserved"
    );
    assert_eq!(entry.version, "seeded-version", "version must be preserved");
    assert_eq!(
        entry.canonical_version,
        mediapm::MEDIAPM_GIT_HASH,
        "canonical_version must be preserved"
    );
    Ok(())
}

/// An exact version spec whose fields match stored resolved fields skips the
/// tool instead of re-provisioning (regression: `spec_matches_entry` with
/// `None` stored fields never matched, forcing re-provision).
///
/// Seeds a media-tagger entry with `Some` resolved fields matching an
/// `Exact { version, vcs_hash }` spec. Sync must skip: `added_tools == 0`,
/// and `deployed_at`/`version` stay at seeded values (no new record).
#[tokio::test]
async fn sync_exact_version_spec_skips_when_stored_fields_match()
-> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    runtime.tools.insert(
        "media-tagger".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                vcs_hash: Some(mediapm::MEDIAPM_GIT_HASH.to_string()),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
                tag: None,
            }),
            ..Default::default()
        },
    );
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    // Seed state.json with matching resolved fields.
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::create_dir_all(state_path.parent().expect("state parent dir"))
        .expect("create state parent dir");
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: mediapm::MEDIAPM_GIT_HASH.to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        resolved_vcs_hash: Some(mediapm::MEDIAPM_GIT_HASH.to_string()),
    });
    let bytes = serde_json::to_vec(&state).expect("state serializes");
    std::fs::write(&state_path, bytes).expect("write seeded state");

    let summary = service.sync_tools().await?;

    assert_eq!(
        summary.added_tools, 0,
        "exact spec matching stored fields must skip, not re-provision"
    );
    let bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state.json after sync");
    let state: MediaPmState =
        serde_json::from_slice(&bytes).expect("state.json should deserialize");
    let entry = state
        .managed_tools
        .iter()
        .find(|e| e.tool_id == "media-tagger")
        .expect("media-tagger entry should exist after sync");

    // No re-provision: identity and resolved fields unchanged.
    assert_eq!(
        entry.deployed_at,
        mediapm_utils::Timestamp::from_unix_secs(42),
        "deployed_at must be preserved on skip"
    );
    assert_eq!(entry.version, "seeded-version", "version must be preserved on skip");
    assert_eq!(
        entry.resolved_version.as_deref(),
        Some(env!("CARGO_PKG_VERSION")),
        "stored resolved_version must be preserved on skip"
    );
    assert_eq!(
        entry.resolved_vcs_hash.as_deref(),
        Some(mediapm::MEDIAPM_GIT_HASH),
        "stored resolved_vcs_hash must be preserved on skip"
    );
    Ok(())
}

/// Env payload paths in `.env.generated` are keyed by the **mediapm
/// conductor tool id** (the generated doc `tools` map key,
/// `{name}@{content_map_hash}`), matching the `ProvisionCache` deployment
/// layout (`tools_dir/<sanitize_tool_id(conductor_tool_id)>/payload/`).
/// The bare mediapm tool id (`ffmpeg`) must never appear as the path
/// segment.
///
/// Hermetic: seeds a matching state entry and a conductor-keyed generated
/// doc entry, then runs sync with an exact version spec so the spec-based
/// skip fires (no network). The skip path reconstructs the runtime from
/// the generated doc under its conductor key, and env generation must emit
/// paths under the sanitized conductor id.
#[tokio::test]
async fn sync_env_paths_use_conductor_tool_id() -> Result<(), mediapm::MediaPmError> {
    use std::collections::BTreeMap;

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    runtime.tools.insert(
        "ffmpeg".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                version: Some("7.1".to_string()),
                vcs_hash: None,
                tag: None,
            }),
            ..Default::default()
        },
    );
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    // Seed state.json with a matching ffmpeg entry (exact version "7.1").
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::create_dir_all(state_path.parent().expect("state parent dir"))
        .expect("create state parent dir");
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "ffmpeg-v7.1".to_string(),
        content_map_hash: "blake3:abc123".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("7.1".to_string()),
        resolved_vcs_hash: None,
    });
    std::fs::write(&state_path, serde_json::to_vec(&state).expect("state serializes"))
        .expect("write seeded state");

    // Seed the generated doc with a conductor-keyed ffmpeg entry. Content
    // map values are non-hash placeholders (external_data invariant skips
    // values that do not parse as `Hash`).
    let mut doc = NickelDocument::default();
    doc.tools.insert(
        "ffmpeg@blake3:abc123".to_string(),
        ToolSpec {
            name: "ffmpeg".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime {
                content_map: BTreeMap::from([
                    ("linux/ffmpeg".to_string(), "provisioned".to_string()),
                    ("linux/".to_string(), "provisioned".to_string()),
                ]),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let generated_path = service.paths().conductor_generated_ncl.clone();
    let bytes = encode_document(doc).expect("seeded doc encodes");
    std::fs::write(&generated_path, bytes).expect("write seeded generated doc");

    service.sync_tools().await?;

    // The exact spec matches the seeded resolved version → skip fires.
    let content = std::fs::read_to_string(&service.paths().env_generated_file)
        .expect("env file should be readable");

    // Binary entry: payload path keyed by the sanitized conductor id.
    let binary_line = content
        .lines()
        .find(|line| line.starts_with("MEDIAPM_FFMPEG_LINUX="))
        .unwrap_or_else(|| panic!("missing MEDIAPM_FFMPEG_LINUX in env file:\n{content}"));
    assert!(
        binary_line.contains("/ffmpeg@blake3_abc123/payload/linux/ffmpeg"),
        "binary env path must use the sanitized conductor tool id: {binary_line}"
    );

    // Dir entry: payload dir path keyed by the sanitized conductor id.
    let dir_line = content
        .lines()
        .find(|line| line.starts_with("MEDIAPM_FFMPEG_LINUX_DIR="))
        .unwrap_or_else(|| panic!("missing MEDIAPM_FFMPEG_LINUX_DIR in env file:\n{content}"));
    assert!(
        dir_line.contains("/ffmpeg@blake3_abc123/payload/linux/"),
        "dir env path must use the sanitized conductor tool id: {dir_line}"
    );

    // Regression: the bare mediapm tool id must never be the path segment.
    assert!(
        !content.contains("/ffmpeg/payload/"),
        "env paths must not use the bare mediapm tool id:\n{content}"
    );

    // Env var names stay hash-free (plain mediapm id stem).
    assert!(
        content.lines().all(|line| {
            line.starts_with('#') || line.split('=').next().is_none_or(|name| !name.contains('@'))
        }),
        "env var names must not contain @:\n{content}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Same-step dependency payload inlining (deps/<mediapm tool id>/)
// ---------------------------------------------------------------------------
//
// The inlining itself (`inline_same_step_deps`) is covered by the unit tests
// in `conductor_bridge/sync/mod.rs`. The integration tests below are hermetic
// (no network) and cover the observable pipeline contracts around it:
//
// - `sync_skip_preserves_inlined_deps` — a generated-doc runtime that already
//   carries `deps/` keys survives a spec-based skip intact; the reserved
//   prefix is never stripped, renamed, or re-inlined on the skip path.
// - `sync_env_has_no_deps_garbage` — inlined `deps/` keys never leak into
//   `.env.generated` (no `_DEPS` / `_DIR` / `_COMPANIONS_` lines).
// - `sync_composite_non_transitive` — a dep whose stored `canonical_version`
//   is itself composite (realistic post-network-sync state) never triggers a
//   false reprovision of the requester; composite segments reference each
//   dep's OWN version segment only (Phase 1 wiring through `service.rs`).
//
// - `sync_inlines_same_step_deps_into_content_map` — a fresh sync of a
//   requester (yt-dlp) with a same-step dep (deno) inlines the dep's payload
//   under `deps/deno/<key>` into the generated-doc runtime, mirroring the
//   dep's own content-map values exactly, with no recursion and no
//   `companions/` residue.
// - `sync_dep_version_change_reprovisions_requester` — bumping a dep's
//   resolved version changes the requester's composite `canonical_version`
//   and forces a reprovision with freshly inlined payloads, while the
//   requester's own payload keys stay byte-identical.
//
// The provisioning-path tests above are hermetic via download-cache
// pre-seeding (metadata + payload domains), mirroring
// `fetch_and_import_ytdlp_full_pipeline` in
// `conductor_bridge/sync/provision.rs`. yt-dlp + deno are the only
// network-resolved managed tools whose payloads are pure downloads with
// tooling mediapm already has (`zip`); ffmpeg is excluded because its linux
// payload is tar.xz (mediapm has no tar/xz decoder). One tolerated network
// touch remains: `prefetch_expected_sizes` fires tolerant HEAD probes at the
// rewritten GitHub URLs before fetch; failures are silently ignored, so
// offline runs only pay a fast DNS failure.

/// Hermetic: seeds a generated doc whose `yt-dlp` runtime already carries
/// inlined `deps/ffmpeg/...` and `deps/deno/...` keys (as a previous network
/// sync would have produced) plus matching state entries, then runs sync with
/// exact version specs so all three tools spec-skip (no network).
///
/// Asserts the inlined structure survives the skip path: the generated doc
/// keeps the `deps/` keys alongside the requester's own keys, the state entry
/// keeps its non-transitive composite `canonical_version`, and no
/// `companions/` prefix ever appears.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn sync_skip_preserves_inlined_deps() -> Result<(), mediapm::MediaPmError> {
    use std::collections::BTreeMap;

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                version: Some("v2024.01.01".to_string()),
                vcs_hash: None,
                tag: None,
            }),
            dependencies: BTreeMap::from([
                (
                    "ffmpeg".to_string(),
                    mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                        version: Some("v7.1".to_string()),
                        vcs_hash: None,
                        tag: None,
                    }),
                ),
                (
                    "deno".to_string(),
                    mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                        version: Some("v1.46.0".to_string()),
                        vcs_hash: None,
                        tag: None,
                    }),
                ),
            ]),
            ..Default::default()
        },
    );
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    // Seed state.json with matching entries for all three tools so the
    // spec-based skip fires without network.
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::create_dir_all(state_path.parent().expect("state parent dir"))
        .expect("create state parent dir");
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "yt-dlp".to_string(),
        version: "seeded-version".to_string(),
        // Non-transitive composite: segments reference each dep's OWN
        // version segment (sorted by dep_id); composite-bearing dep entries
        // never nest into the requester's composite.
        canonical_version: "yt-dlp-v2024.01.01;deno:deno-v1.46.0;ffmpeg:ffmpeg-v7.1".to_string(),
        content_map_hash: "blake3:abc123".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("v2024.01.01".to_string()),
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "ffmpeg-v7.1".to_string(),
        content_map_hash: "blake3:ffmpeg1".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("v7.1".to_string()),
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "deno".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "deno-v1.46.0".to_string(),
        content_map_hash: "blake3:deno1".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("v1.46.0".to_string()),
        resolved_vcs_hash: None,
    });
    std::fs::write(&state_path, serde_json::to_vec(&state).expect("state serializes"))
        .expect("write seeded state");

    // Seed the generated doc with a yt-dlp entry whose runtime already
    // carries inlined same-step dep keys. Content map values are non-hash
    // placeholders (external_data invariant skips them).
    let mut doc = NickelDocument::default();
    doc.tools.insert(
        "yt-dlp@blake3:abc123".to_string(),
        ToolSpec {
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime {
                content_map: BTreeMap::from([
                    ("linux/yt-dlp".to_string(), "provisioned".to_string()),
                    ("linux/".to_string(), "provisioned".to_string()),
                    ("deps/ffmpeg/linux/ffmpeg".to_string(), "provisioned".to_string()),
                    ("deps/deno/linux/deno".to_string(), "provisioned".to_string()),
                ]),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let generated_path = service.paths().conductor_generated_ncl.clone();
    let bytes = encode_document(doc).expect("seeded doc encodes");
    std::fs::write(&generated_path, bytes).expect("write seeded generated doc");

    service.sync_tools().await?;

    // The exact specs match the seeded resolved versions → all three tools
    // spec-skip; the inlined structure must survive intact.
    let doc_bytes =
        std::fs::read(&service.paths().conductor_generated_ncl).expect("generated doc readable");
    let doc: NickelDocument = decode_document(&doc_bytes).expect("valid Nickel document");
    let spec = doc
        .tools
        .values()
        .find(|s| s.name == "yt-dlp")
        .expect("yt-dlp entry must survive the skip path");
    let content_map = &spec.runtime.content_map;
    assert!(
        content_map.contains_key("deps/ffmpeg/linux/ffmpeg"),
        "inlined ffmpeg key must be preserved: {content_map:?}"
    );
    assert!(
        content_map.contains_key("deps/deno/linux/deno"),
        "inlined deno key must be preserved: {content_map:?}"
    );
    assert!(
        content_map.contains_key("linux/yt-dlp"),
        "requester's own key must be preserved: {content_map:?}"
    );
    assert!(
        content_map.keys().all(|k| !k.contains("companions")),
        "no companions/ prefix may appear: {content_map:?}"
    );

    // State entries are preserved unchanged on the skip path.
    let state_bytes =
        std::fs::read(&service.paths().mediapm_state_json).expect("state.json readable");
    let state: MediaPmState =
        serde_json::from_slice(&state_bytes).expect("state.json should deserialize");
    let entry =
        state.managed_tools.iter().find(|e| e.tool_id == "yt-dlp").expect("yt-dlp entry in state");
    assert_eq!(
        entry.canonical_version, "yt-dlp-v2024.01.01;deno:deno-v1.46.0;ffmpeg:ffmpeg-v7.1",
        "stored composite must stay non-transitive and unchanged"
    );
    assert_eq!(
        entry.content_map_hash, "blake3:abc123",
        "content_map_hash must be preserved on skip"
    );
    Ok(())
}

/// Hermetic: same seeding as `sync_skip_preserves_inlined_deps`; asserts
/// `.env.generated` never leaks inlined `deps/` keys.
///
/// Without the Phase 3 skip, `content_key_to_env_name` would split
/// `deps/ffmpeg/linux/ffmpeg` on the first `/` and emit `MEDIAPM_YT_DLP_DEPS_*`
/// garbage pointing at `.../payload/deps/...`. Inlined companions are
/// referenced via the predictable `deps/<tool_id>/` path, never env vars.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn sync_env_has_no_deps_garbage() -> Result<(), mediapm::MediaPmError> {
    use std::collections::BTreeMap;

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                version: Some("v2024.01.01".to_string()),
                vcs_hash: None,
                tag: None,
            }),
            dependencies: BTreeMap::from([
                (
                    "ffmpeg".to_string(),
                    mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                        version: Some("v7.1".to_string()),
                        vcs_hash: None,
                        tag: None,
                    }),
                ),
                (
                    "deno".to_string(),
                    mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                        version: Some("v1.46.0".to_string()),
                        vcs_hash: None,
                        tag: None,
                    }),
                ),
            ]),
            ..Default::default()
        },
    );
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    // Seed state.json with matching entries for all three tools.
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::create_dir_all(state_path.parent().expect("state parent dir"))
        .expect("create state parent dir");
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "yt-dlp".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "yt-dlp-v2024.01.01;deno:deno-v1.46.0;ffmpeg:ffmpeg-v7.1".to_string(),
        content_map_hash: "blake3:abc123".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("v2024.01.01".to_string()),
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "ffmpeg-v7.1".to_string(),
        content_map_hash: "blake3:ffmpeg1".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("v7.1".to_string()),
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "deno".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "deno-v1.46.0".to_string(),
        content_map_hash: "blake3:deno1".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("v1.46.0".to_string()),
        resolved_vcs_hash: None,
    });
    std::fs::write(&state_path, serde_json::to_vec(&state).expect("state serializes"))
        .expect("write seeded state");

    // Seed the generated doc with a yt-dlp entry carrying inlined deps keys.
    let mut doc = NickelDocument::default();
    doc.tools.insert(
        "yt-dlp@blake3:abc123".to_string(),
        ToolSpec {
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime {
                content_map: BTreeMap::from([
                    ("linux/yt-dlp".to_string(), "provisioned".to_string()),
                    ("linux/".to_string(), "provisioned".to_string()),
                    ("deps/ffmpeg/linux/ffmpeg".to_string(), "provisioned".to_string()),
                    ("deps/deno/linux/deno".to_string(), "provisioned".to_string()),
                ]),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let generated_path = service.paths().conductor_generated_ncl.clone();
    let bytes = encode_document(doc).expect("seeded doc encodes");
    std::fs::write(&generated_path, bytes).expect("write seeded generated doc");

    service.sync_tools().await?;

    let content = std::fs::read_to_string(&service.paths().env_generated_file)
        .expect("env file should be readable");

    // The requester's own keys still emit (binary + dir entries).
    assert!(
        content.lines().any(|line| line.starts_with("MEDIAPM_YT_DLP_LINUX=")),
        "own binary var missing:\n{content}"
    );
    assert!(
        content.lines().any(|line| line.starts_with("MEDIAPM_YT_DLP_LINUX_DIR=")),
        "own dir var missing:\n{content}"
    );

    // Inlined deps keys never leak: no `deps/` path segments, no `_DEPS_*`
    // or `_COMPANIONS_*` var names.
    for line in content.lines() {
        assert!(!line.contains("deps/"), "env must not reference deps/ paths: {line}\n{content}");
        assert!(
            !line.contains("DEPS") && !line.contains("COMPANIONS"),
            "env must not leak companion vars: {line}\n{content}"
        );
    }
    Ok(())
}

/// `logical_tool_requires_sync` stays stable when a dep's stored
/// `canonical_version` is itself composite (realistic state after a network
/// sync in which the dep had its own same-step deps).
///
/// Guards the Phase 1 wiring through `service.rs`: composite segments
/// reference each dep's OWN version segment (`own_version_segment`), so a
/// composite-bearing dep entry never changes the requester's computed
/// composite and never triggers a false reprovision. (The same-step
/// version-segment math itself is unit-tested in
/// `compute_composite_canonical_version_non_transitive`; no same-step tool
/// resolves without network, so the integration check uses media-tagger's
/// `CrossStep` dep to assert composite-dep tolerance.)
#[tokio::test]
async fn sync_composite_non_transitive() -> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
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

    let mut state = MediaPmState::default();
    // ffmpeg stored with a COMPOSITE canonical_version — as if it had been
    // network-synced with its own same-step dep (`deno`) in a prior pass.
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: String::new(),
        canonical_version: "ffmpeg-v7.1;deno:deno-v1.46.0".to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
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

    assert!(
        !service.logical_tool_requires_sync("media-tagger", &state).await?,
        "composite-bearing dep entries must not trigger reprovision of the requester"
    );
    Ok(())
}

/// Opens a user-level download cache with the two domains the sync pipeline
/// uses (`tools` payload domain + `tool_metadata` tag domain), mirroring the
/// `CacheDomainConfig`s from `reconcile_desired_tools`.
async fn open_test_cache(root: &std::path::Path) -> Cache {
    Cache::open(
        root,
        &[
            CacheDomainConfig {
                domain: "tools".to_string(),
                index_file_name: "tools.json".to_string(),
                entry_ttl_seconds: 30 * 24 * 60 * 60,
            },
            CacheDomainConfig {
                domain: "tool_metadata".to_string(),
                index_file_name: "tool_metadata.json".to_string(),
                entry_ttl_seconds: 24 * 60 * 60,
            },
        ],
    )
    .await
    .expect("test cache opens")
}

/// Builds a single-entry Stored-compression zip named after the executable
/// (`deno`), matching the archive shape `find_os_executable` expects after
/// extraction (`<os_dir>/deno`).
fn make_deno_zip(name: &str, content: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut writer = ZipWriter::new(std::io::Cursor::new(Vec::<u8>::new()));
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file(name, options).expect("start zip entry");
    writer.write_all(content).expect("write zip entry");
    writer.finish().expect("finish zip").into_inner()
}

/// Hermetic fresh-sync of the provisioning path: yt-dlp (requester) with a
/// same-step dep (deno) is provisioned entirely from a pre-seeded
/// user-level download cache (no network). Asserts the generated-doc
/// runtime carries the dep's payload inlined under `deps/deno/<key>` with
/// values mirroring the dep's own content map exactly, no recursion into
/// `deps/deno/deps/`, no `companions/` residue, and a composite
/// `canonical_version` in state.
///
/// Hermeticity recipe (mirrors `fetch_and_import_ytdlp_full_pipeline` in
/// `conductor_bridge/sync/provision.rs`): pre-seed the `tool_metadata`
/// domain with `"{tag}\n{hash}"` entries (tag resolution never touches the
/// GitHub API), pre-seed the `tools` domain under the REWRITTEN download
/// URLs (`/download/{tag}/...` — `resolve_tool_fetch` substitutes the
/// `latest/download/` placeholder), then drop the cache handle BEFORE sync
/// (two open `Cache` handles at the same root contend for the directory
/// lock). ffmpeg is excluded because its linux payload is tar.xz (mediapm
/// has no tar/xz decoder); deno payloads are zips built with the `zip`
/// crate. The only tolerated network touch is `prefetch_expected_sizes`
/// firing tolerant HEAD probes at the rewritten URLs; failures are silently
/// ignored.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn sync_inlines_same_step_deps_into_content_map() -> Result<(), mediapm::MediaPmError> {
    use std::collections::BTreeMap;

    let yt_dlp_tag = "2025.07.15";
    let yt_dlp_hash = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
    let deno_tag = "1.46.0";
    let deno_hash = "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1";
    let yt_dlp_payloads = [
        ("yt-dlp.exe", &b"fake yt-dlp windows binary"[..]),
        ("yt-dlp_macos", &b"fake yt-dlp macos binary"[..]),
        ("yt-dlp_linux", &b"fake yt-dlp linux binary"[..]),
    ];
    let deno_zip_payloads = [
        ("windows", &b"fake deno 1.46.0 windows binary"[..]),
        ("macos", &b"fake deno 1.46.0 macos binary"[..]),
        ("linux", &b"fake deno 1.46.0 linux binary"[..]),
    ];

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let cache = open_test_cache(cache_root.path()).await;
    // Metadata cache: tag resolution is served from cache (no GitHub API).
    let yt_dlp_metadata_key = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";
    cache
        .store_bytes(
            "tool_metadata",
            yt_dlp_metadata_key,
            format!("{yt_dlp_tag}\n{yt_dlp_hash}").as_bytes(),
        )
        .await;
    let deno_metadata_key = "https://api.github.com/repos/denoland/deno/releases/latest";
    cache
        .store_bytes(
            "tool_metadata",
            deno_metadata_key,
            format!("{deno_tag}\n{deno_hash}").as_bytes(),
        )
        .await;
    // Payload cache: REWRITTEN download URLs (`/download/{tag}/`).
    for (filename, payload) in &yt_dlp_payloads {
        let url =
            format!("https://github.com/yt-dlp/yt-dlp/releases/download/{yt_dlp_tag}/{filename}");
        cache.store_bytes("tools", &url, payload).await;
    }
    for (os, content) in &deno_zip_payloads {
        let zip_name = match *os {
            "windows" => "deno-x86_64-pc-windows-msvc.zip",
            "macos" => "deno-aarch64-apple-darwin.zip",
            _ => "deno-aarch64-unknown-linux-gnu.zip",
        };
        let zip_bytes = make_deno_zip("deno", content);
        let url =
            format!("https://github.com/denoland/deno/releases/download/{deno_tag}/{zip_name}");
        cache.store_bytes("tools", &url, &zip_bytes).await;
    }
    // Drop BEFORE sync: a second open `Cache` at the same root would contend
    // for the directory lock.
    drop(cache);

    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Latest,
            dependencies: BTreeMap::from([(
                "deno".to_string(),
                mediapm::ConfigVersionSpec::Latest,
            )]),
            ..Default::default()
        },
    );
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;

    // Generated doc: the requester's ACTIVE spec (name match + non-empty
    // content map — pruned stale keys keep the name with a cleared map).
    let doc_bytes =
        std::fs::read(&service.paths().conductor_generated_ncl).expect("generated doc readable");
    let doc: NickelDocument = decode_document(&doc_bytes).expect("valid Nickel document");
    let yt_dlp_spec = doc
        .tools
        .values()
        .find(|s| s.name == "yt-dlp" && !s.runtime.content_map.is_empty())
        .expect("active yt-dlp spec");
    let deno_spec = doc
        .tools
        .values()
        .find(|s| s.name == "deno" && !s.runtime.content_map.is_empty())
        .expect("active deno spec");

    // deno's own map: exactly the three `{os}/` dir keys.
    assert_eq!(
        deno_spec.runtime.content_map.len(),
        3,
        "deno own map must have exactly 3 OS entries"
    );
    for os in ["windows", "macos", "linux"] {
        let key = format!("{os}/");
        assert!(deno_spec.runtime.content_map.contains_key(&key), "deno missing own key {key}");
        // The requester inlines the dep payload under `deps/deno/{os}/` with
        // values mirroring the dep's own content map exactly.
        let inlined_key = format!("deps/deno/{os}/");
        assert_eq!(
            yt_dlp_spec.runtime.content_map.get(&inlined_key),
            deno_spec.runtime.content_map.get(&key),
            "inlined {inlined_key} must mirror deno's own {key}"
        );
    }
    // The requester keeps its own binary keys.
    for (filename, _) in &yt_dlp_payloads {
        let os = match *filename {
            "yt-dlp.exe" => "windows",
            "yt-dlp_macos" => "macos",
            _ => "linux",
        };
        let key = format!("{os}/{filename}");
        assert!(yt_dlp_spec.runtime.content_map.contains_key(&key), "yt-dlp missing own key {key}");
    }
    // Non-transitive: no `deps/deno/deps/` recursion; no `companions/`.
    for key in yt_dlp_spec.runtime.content_map.keys() {
        assert!(!key.starts_with("deps/deno/deps/"), "inlining must not recurse: {key}");
        assert!(!key.contains("companions/"), "no companions residue: {key}");
    }

    // State: yt-dlp carries the composite canonical version referencing the
    // dep's OWN version segment; deno carries its bare hash.
    let state_bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state readable");
    let state: MediaPmState = serde_json::from_slice(&state_bytes).expect("valid state json");
    let expected_composite = format!("{yt_dlp_hash};deno:{deno_hash}");
    assert!(
        state
            .managed_tools
            .iter()
            .any(|e| e.tool_id == "yt-dlp" && e.canonical_version == expected_composite),
        "yt-dlp must carry composite canonical {expected_composite}"
    );
    assert!(
        state.managed_tools.iter().any(|e| e.tool_id == "deno" && e.canonical_version == deno_hash),
        "deno must carry bare canonical {deno_hash}"
    );
    Ok(())
}

/// Hermetic dep-version-change reprovision: bumps deno's resolved tag/hash
/// (1.46.0 → 1.47.0) while yt-dlp's payload stays identical, then syncs a
/// workspace whose `state.json` was seeded with the PREVIOUS deployment
/// records. Asserts yt-dlp reprovisions (composite canonical becomes
/// `{yt_dlp_hash};deno:{new_deno_hash}`) with freshly inlined deno payloads,
/// while its own binary keys keep byte-identical hashes (same cached
/// payloads → same blake3 content addresses).
///
/// The seeded state drives the composite skip check:
/// - deno's entry carries an EMPTY `content_map_hash`. The composite
///   computation (`compute_composite_canonical_version`) matches only
///   non-empty-hash entries via `find()`, so the stale deno record is
///   ignored — this dodges the find-first hazard where an accumulated stale
///   entry with a non-empty hash would be matched first and suppress
///   yt-dlp's reprovision — and deno itself cannot skip either.
/// - yt-dlp's entry carries the OLD composite `{yt_dlp_hash};deno:{old_deno_hash}`
///   with a non-empty placeholder hash: the composite skip check compares it
///   against the freshly computed `{yt_dlp_hash};deno:{new_deno_hash}` and
///   falls through to reprovision.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn sync_dep_version_change_reprovisions_requester() -> Result<(), mediapm::MediaPmError> {
    use std::collections::BTreeMap;

    let yt_dlp_tag = "2025.07.15";
    let yt_dlp_hash = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
    let old_deno_hash = "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1";
    let new_deno_tag = "1.47.0";
    let new_deno_hash = "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2";
    let yt_dlp_payloads = [
        ("yt-dlp.exe", &b"fake yt-dlp windows binary"[..]),
        ("yt-dlp_macos", &b"fake yt-dlp macos binary"[..]),
        ("yt-dlp_linux", &b"fake yt-dlp linux binary"[..]),
    ];
    let deno_zip_payloads = [
        ("windows", &b"fake deno 1.47.0 windows binary"[..]),
        ("macos", &b"fake deno 1.47.0 macos binary"[..]),
        ("linux", &b"fake deno 1.47.0 linux binary"[..]),
    ];

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let cache = open_test_cache(cache_root.path()).await;
    // Metadata cache: deno resolves to the NEW tag/hash; yt-dlp stays put.
    let yt_dlp_metadata_key = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";
    cache
        .store_bytes(
            "tool_metadata",
            yt_dlp_metadata_key,
            format!("{yt_dlp_tag}\n{yt_dlp_hash}").as_bytes(),
        )
        .await;
    let deno_metadata_key = "https://api.github.com/repos/denoland/deno/releases/latest";
    cache
        .store_bytes(
            "tool_metadata",
            deno_metadata_key,
            format!("{new_deno_tag}\n{new_deno_hash}").as_bytes(),
        )
        .await;
    // Payload cache: REWRITTEN download URLs (`/download/{tag}/`).
    for (filename, payload) in &yt_dlp_payloads {
        let url =
            format!("https://github.com/yt-dlp/yt-dlp/releases/download/{yt_dlp_tag}/{filename}");
        cache.store_bytes("tools", &url, payload).await;
    }
    for (os, content) in &deno_zip_payloads {
        let zip_name = match *os {
            "windows" => "deno-x86_64-pc-windows-msvc.zip",
            "macos" => "deno-aarch64-apple-darwin.zip",
            _ => "deno-aarch64-unknown-linux-gnu.zip",
        };
        let zip_bytes = make_deno_zip("deno", content);
        let url =
            format!("https://github.com/denoland/deno/releases/download/{new_deno_tag}/{zip_name}");
        cache.store_bytes("tools", &url, &zip_bytes).await;
    }
    drop(cache);

    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Latest,
            dependencies: BTreeMap::from([(
                "deno".to_string(),
                mediapm::ConfigVersionSpec::Latest,
            )]),
            ..Default::default()
        },
    );
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    // Seed the PREVIOUS deployment records (as a prior network sync would
    // have left them). The generated doc is left empty — this sync pass
    // rebuilds it from scratch.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "deno".to_string(),
        version: String::new(),
        canonical_version: old_deno_hash.to_string(),
        content_map_hash: String::new(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "yt-dlp".to_string(),
        version: String::new(),
        canonical_version: format!("{yt_dlp_hash};deno:{old_deno_hash}"),
        content_map_hash: "blake3:old".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::create_dir_all(state_path.parent().expect("state parent dir"))
        .expect("create state dir");
    std::fs::write(&state_path, serde_json::to_vec(&state).expect("state serializes"))
        .expect("write seeded state");

    service.sync_tools().await?;

    // Generated doc: yt-dlp's ACTIVE spec carries freshly inlined deno
    // payloads; deno's own map reflects the NEW zip contents.
    let doc_bytes =
        std::fs::read(&service.paths().conductor_generated_ncl).expect("generated doc readable");
    let doc: NickelDocument = decode_document(&doc_bytes).expect("valid Nickel document");
    let yt_dlp_spec = doc
        .tools
        .values()
        .find(|s| s.name == "yt-dlp" && !s.runtime.content_map.is_empty())
        .expect("active yt-dlp spec");
    let deno_spec = doc
        .tools
        .values()
        .find(|s| s.name == "deno" && !s.runtime.content_map.is_empty())
        .expect("active deno spec");

    // yt-dlp's own keys are UNCHANGED: identical cached payloads → identical
    // blake3 content addresses (binary format imports the raw bytes as-is).
    for (filename, payload) in &yt_dlp_payloads {
        let os = match *filename {
            "yt-dlp.exe" => "windows",
            "yt-dlp_macos" => "macos",
            _ => "linux",
        };
        let key = format!("{os}/{filename}");
        let expected_hash = blake3::hash(payload).to_hex().to_string();
        assert_eq!(
            yt_dlp_spec.runtime.content_map.get(&key),
            Some(&expected_hash),
            "yt-dlp own key {key} must be byte-stable after dep bump"
        );
    }
    // Inlined dep payloads mirror deno's NEW own map.
    for os in ["windows", "macos", "linux"] {
        let key = format!("{os}/");
        let inlined_key = format!("deps/deno/{os}/");
        assert_eq!(
            yt_dlp_spec.runtime.content_map.get(&inlined_key),
            deno_spec.runtime.content_map.get(&key),
            "inlined {inlined_key} must mirror deno's new own {key}"
        );
    }
    // Non-transitive: no recursion; no companions residue.
    for key in yt_dlp_spec.runtime.content_map.keys() {
        assert!(!key.starts_with("deps/deno/deps/"), "inlining must not recurse: {key}");
        assert!(!key.contains("companions/"), "no companions residue: {key}");
    }

    // State: yt-dlp reprovisioned with the NEW composite; deno carries the
    // NEW bare hash.
    let state_bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state readable");
    let state: MediaPmState = serde_json::from_slice(&state_bytes).expect("valid state json");
    let expected_composite = format!("{yt_dlp_hash};deno:{new_deno_hash}");
    assert!(
        state
            .managed_tools
            .iter()
            .any(|e| e.tool_id == "yt-dlp" && e.canonical_version == expected_composite),
        "yt-dlp must reprovision to composite canonical {expected_composite}"
    );
    assert!(
        state
            .managed_tools
            .iter()
            .any(|e| e.tool_id == "deno" && e.canonical_version == new_deno_hash),
        "deno must reprovision to bare canonical {new_deno_hash}"
    );
    Ok(())
}
