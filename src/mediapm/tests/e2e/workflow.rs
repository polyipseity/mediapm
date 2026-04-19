//! End-to-end workflow tests for mediapm phase composition.

use mediapm::{
    MediaPmApi, MediaPmDocument, MediaPmService, MediaRuntimeStorage, MediaStepTool,
    TransformInputValue, load_mediapm_document, save_mediapm_document,
};
use mediapm_conductor::default_runtime_inherited_env_vars_for_host;
use mediapm_conductor::{decode_machine_document, decode_user_document};
use tempfile::tempdir;
use url::Url;

/// Protects bootstrap behavior for freshly initialized Phase 3 workspaces.
#[tokio::test]
async fn sync_library_bootstraps_phase3_state_files() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let _summary = service.sync_library().await.expect("sync should succeed");

    assert!(service.paths().mediapm_ncl.exists());
    assert!(service.paths().conductor_user_ncl.exists());
    assert!(service.paths().conductor_machine_ncl.exists());
    assert!(service.paths().lock_jsonc.exists());
    assert!(service.paths().library_dir == root.path());
    assert!(!root.path().join("library").exists());
}

/// Protects source URI scheme restrictions for unsupported protocols.
#[tokio::test]
async fn source_scheme_validation_is_enforced() {
    let service = MediaPmService::new_in_memory();
    let invalid = Url::parse("ftp://example.com/file.mp4").expect("url must parse");

    let result = service.process_source(invalid, true).await;

    assert!(result.is_err());
}

/// Protects local-source URI acceptance used by `media add-local` flows.
#[tokio::test]
async fn local_scheme_is_accepted() {
    let service = MediaPmService::new_in_memory();
    let local = Url::parse("local:media-123").expect("url must parse");

    let result = service.process_source(local, false).await;

    assert!(result.is_ok());
}

/// Protects conductor runtime-storage defaults written by Phase 3 bootstrap.
#[tokio::test]
async fn sync_bootstrap_sets_mediapm_conductor_runtime_defaults() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let _summary = service.sync_library().await.expect("sync should succeed");

    let user_bytes =
        std::fs::read(&service.paths().conductor_user_ncl).expect("read conductor user document");
    let user = decode_user_document(&user_bytes).expect("decode user document");
    assert_eq!(user.runtime.conductor_dir.as_deref(), Some(".mediapm"));
    assert_eq!(user.runtime.state_config.as_deref(), Some(".mediapm/state.ncl"));
    assert_eq!(user.runtime.cas_store_dir.as_deref(), Some(".mediapm/store"));
    assert!(
        user.runtime.inherited_env_vars.is_none(),
        "user runtime defaults should omit inherited_env_vars"
    );

    let expected_inherited = default_runtime_inherited_env_vars_for_host();

    let machine_bytes = std::fs::read(&service.paths().conductor_machine_ncl)
        .expect("read conductor machine document");
    let machine = decode_machine_document(&machine_bytes).expect("decode machine document");

    assert_eq!(machine.runtime.conductor_dir.as_deref(), Some(".mediapm"));
    assert_eq!(machine.runtime.state_config.as_deref(), Some(".mediapm/state.ncl"));
    assert_eq!(machine.runtime.cas_store_dir.as_deref(), Some(".mediapm/store"));
    if expected_inherited.is_empty() {
        assert!(machine.runtime.inherited_env_vars.is_none());
    } else {
        assert_eq!(machine.runtime.inherited_env_vars, Some(expected_inherited));
    }
    assert!(root.path().join(".mediapm").join("state.ncl").exists());
    assert!(!root.path().join(".conductor").join("state.ncl").exists());
}

/// Protects split-root runtime-storage resolution for `mediapm` path settings.
#[tokio::test]
async fn sync_uses_split_runtime_storage_resolution_roots() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let document = MediaPmDocument {
        runtime: MediaRuntimeStorage {
            mediapm_dir: Some(".mediapm-custom".to_string()),
            library_dir: Some("library-custom".to_string()),
            tmp_dir: Some("tmp-custom".to_string()),
            conductor_config: None,
            conductor_machine_config: None,
            conductor_state: None,
            inherited_env_vars: None,
            lockfile: None,
            env_file: None,
            schema_config_dir: None,
            use_user_download_cache: None,
        },
        ..MediaPmDocument::default()
    };
    save_mediapm_document(&service.paths().mediapm_ncl, &document)
        .expect("save mediapm.ncl with runtime_storage overrides");

    let _summary = service.sync_library().await.expect("sync should succeed");

    assert!(root.path().join(".mediapm-custom").join("lock.jsonc").exists());
    assert!(root.path().join(".mediapm-custom").join("state.ncl").exists());
    assert!(root.path().join("library-custom").exists());
    assert!(root.path().join(".mediapm-custom").join("tmp-custom").exists());
    assert!(!root.path().join(".mediapm").join("lock.jsonc").exists());
    assert!(!root.path().join(".conductor").join("state.ncl").exists());
}

/// Protects explicit `conductor_state` overrides from falling back to
/// `.conductor/state.ncl` during workflow execution.
#[tokio::test]
async fn sync_honors_explicit_conductor_state_override() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let document = MediaPmDocument {
        runtime: MediaRuntimeStorage {
            mediapm_dir: None,
            library_dir: None,
            tmp_dir: None,
            conductor_config: None,
            conductor_machine_config: None,
            conductor_state: Some("state/custom.state.ncl".to_string()),
            inherited_env_vars: None,
            lockfile: None,
            env_file: None,
            schema_config_dir: None,
            use_user_download_cache: None,
        },
        ..MediaPmDocument::default()
    };
    save_mediapm_document(&service.paths().mediapm_ncl, &document)
        .expect("save mediapm.ncl with explicit conductor_state override");

    let _summary = service.sync_library().await.expect("sync should succeed");

    assert!(root.path().join("state").join("custom.state.ncl").exists());
    assert!(!root.path().join(".mediapm").join("state.ncl").exists());
    assert!(!root.path().join(".conductor").join("state.ncl").exists());
}

/// Protects defaults applied when adding one remote media source.
#[tokio::test]
async fn add_media_source_sets_remote_download_defaults() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let remote = Url::parse("https://example.com/video.mkv").expect("url must parse");
    let media_id = service.add_media_source(remote).expect("add media source");
    let document = load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm doc");
    let source = document.media.get(&media_id).expect("source exists");

    assert_eq!(source.steps.len(), 1);
    let step = &source.steps[0];
    assert_eq!(step.tool, MediaStepTool::YtDlp);
    assert_eq!(
        step.options.get("uri"),
        Some(&TransformInputValue::String("https://example.com/video.mkv".to_string())),
    );
    assert_eq!(
        step.options.get("write_description"),
        Some(&TransformInputValue::String("true".to_string())),
    );
    assert_eq!(
        step.options.get("write_info_json"),
        Some(&TransformInputValue::String("true".to_string())),
    );
    assert_eq!(
        step.output_variants.get("default"),
        Some(&serde_json::json!({
            "kind": "primary",
            "save": true,
            "save_full": false,
        })),
    );
    let description = source.description.as_deref().expect("description should be set");
    assert!(description.contains("title:"));
    assert!(description.contains("author:"));
}

/// Protects local-source registration as an import-once CAS ingest workflow.
#[tokio::test]
async fn add_local_source_sets_import_step_and_description() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());
    let local_file = root.path().join("sample-media.txt");
    std::fs::write(&local_file, b"sample-bytes").expect("write local file");

    let media_id = service.add_local_source(&local_file).await.expect("add local source");
    let document = load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm doc");
    let source = document.media.get(&media_id).expect("source exists");

    assert!(source.variant_hashes.is_empty());
    assert_eq!(source.steps.len(), 1);
    let step = &source.steps[0];
    assert_eq!(step.tool, MediaStepTool::ImportOnce);
    assert!(step.input_variants.is_empty());
    assert_eq!(
        step.options.get("kind"),
        Some(&TransformInputValue::String("cas_hash".to_string())),
    );
    assert!(matches!(
        step.options.get("hash"),
        Some(TransformInputValue::String(value)) if !value.trim().is_empty()
    ));
    assert_eq!(
        step.output_variants.get("default"),
        Some(&serde_json::json!({
            "kind": "output_content",
            "save": true,
            "save_full": false,
        })),
    );

    let description = source.description.as_deref().expect("description should be set");
    assert!(description.contains("file:"));
    assert!(description.contains("sample-media.txt"));
}
