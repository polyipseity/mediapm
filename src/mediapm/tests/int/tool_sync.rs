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

use mediapm::{MediaPmService, MediaPmState, MediaRuntimeStorage, ToolRequirement};
use mediapm_conductor::{NickelDocument, ToolKindSpec, decode_document};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Structural side-effect tests (no counter assertions)
// ---------------------------------------------------------------------------

/// Sync on a completely empty workspace completes without error.
#[tokio::test]
async fn sync_empty_workspace_succeeds() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
    let _summary = service.sync_tools().await?;
    Ok(())
}

/// Sync creates the expected runtime directories under `.mediapm/`.
#[tokio::test]
async fn sync_creates_runtime_directories() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
    service.sync_tools().await?;
    let paths = service.paths();
    assert!(paths.runtime_root.exists(), "runtime root .mediapm/ should exist");
    assert!(paths.tools_dir.exists(), "tools/ directory should exist");
    Ok(())
}

/// Sync creates `state.ncl` containing a version field.
#[tokio::test]
async fn sync_creates_state_document() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
    service.sync_tools().await?;
    let state_path = &service.paths().mediapm_state_ncl;
    assert!(state_path.exists(), "state.ncl should exist");
    let content = std::fs::read_to_string(state_path).expect("state.ncl should be readable");
    assert!(!content.is_empty(), "state.ncl must not be empty");
    assert!(content.contains("version"), "state.ncl must contain a version field");
    Ok(())
}

/// Sync creates `conductor.generated.ncl` with tools registered.
#[tokio::test]
async fn sync_creates_generated_document() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
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
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
    service.sync_tools().await?;
    let env_path = &service.paths().env_generated_file;
    assert!(env_path.exists(), ".env.generated should exist");
    let content = std::fs::read_to_string(env_path).expect("env file should be readable");
    assert!(!content.is_empty(), "env file must not be empty");
    assert!(content.starts_with('#'), "env file must start with a comment header");
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

/// Re-syncing produces an identical state document (idempotency).
#[tokio::test]
async fn sync_is_idempotent() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
    service.sync_tools().await?;
    let state_after_first =
        std::fs::read(&service.paths().mediapm_state_ncl).expect("state.ncl should exist");
    let _ = service.sync_tools().await?;
    let state_after_second =
        std::fs::read(&service.paths().mediapm_state_ncl).expect("state.ncl should exist");
    assert_eq!(state_after_first, state_after_second, "state.ncl must be identical after re-sync");
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
    assert!(service.logical_tool_requires_sync("non-existent", &state)?);
    Ok(())
}

/// `logical_tool_requires_sync` returns `false` for a tool that is present
/// in state with matching version.
#[tokio::test]
async fn sync_tool_requires_sync_false_when_present() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut overrides = MediaRuntimeStorage::default();
    overrides.tools.insert(
        "demo-tool".to_string(),
        ToolRequirement {
            version: mediapm::MediaMetadataValue::Literal("1.0".to_string()),
            ..Default::default()
        },
    );
    let service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), overrides).await?;
    let mut state = MediaPmState::default();
    state.tools.insert(
        "demo-tool".to_string(),
        ToolRequirement {
            version: mediapm::MediaMetadataValue::Literal("1.0".to_string()),
            ..Default::default()
        },
    );
    assert!(!service.logical_tool_requires_sync("demo-tool", &state)?);
    Ok(())
}

/// `collect_tools_requiring_sync` returns an empty vec when no tools are
/// desired.
#[tokio::test]
async fn sync_no_tools_need_sync_when_none_desired() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_fs_at(root.path()).await?;
    let state = MediaPmState::default();
    let needing = service.collect_tools_requiring_sync(&state)?;
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
    let needing = service.collect_tools_requiring_sync(&state)?;
    assert_eq!(needing, vec!["media-tagger"]);
    Ok(())
}
