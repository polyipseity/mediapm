use mediapm::{MediaRuntimeStorage, ToolRequirement};
use mediapm_conductor::ToolKindSpec;

use crate::common::{read_generated_doc, service_with_cache};

// ---------------------------------------------------------------------------
// Structural side-effect tests (no counter assertions)
// ---------------------------------------------------------------------------

/// Sync on a completely empty workspace completes without error.
#[tokio::test]
async fn sync_empty_workspace_succeeds() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;
    let _summary = service.sync_tools().await?;
    Ok(())
}

/// Sync creates the expected runtime directories under `.mediapm/`.
#[tokio::test]
async fn sync_creates_runtime_directories() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;
    service.sync_tools().await?;
    let paths = service.paths();
    assert!(paths.runtime_root.exists(), "runtime root .mediapm/ should exist");
    assert!(paths.tools_dir.exists(), "tools/ directory should exist");
    Ok(())
}

/// Sync creates `state.json` containing a version field.
#[tokio::test]
async fn sync_creates_state_document() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;
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
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;
    service.sync_tools().await?;
    let generated_path = &service.paths().conductor_generated_ncl;
    assert!(generated_path.exists(), "conductor.generated.ncl should exist");
    let doc = read_generated_doc(&service);
    assert!(!doc.tools.is_empty(), "generated doc must have tools");
    Ok(())
}

/// Sync creates `.env.generated` with a comment header.
#[tokio::test]
async fn sync_creates_env_generated() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;
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
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;
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
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;
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
    let (mut service, _root, cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;

    // Record real cache state before the sync.
    let real_cache_mtime =
        mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root()
            .and_then(|p| std::fs::metadata(p.join("tools.json")).ok())
            .and_then(|m| m.modified().ok());

    service.sync_tools().await?;
    let doc = read_generated_doc(&service);
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
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;
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
    let (mut service, _root, _cache_root) =
        service_with_cache(MediaRuntimeStorage::default()).await?;
    service.sync_tools().await?;
    let state_after_first =
        std::fs::read(&service.paths().mediapm_state_json).expect("state.json should exist");
    let _ = service.sync_tools().await?;
    let state_after_second =
        std::fs::read(&service.paths().mediapm_state_json).expect("state.json should exist");
    assert_eq!(state_after_first, state_after_second, "state.json must be identical after re-sync");
    Ok(())
}
