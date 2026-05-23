//! End-to-end workflow tests for mediapm phase composition.

use mediapm::{
    MediaMetadataValue, MediaMetadataValueCandidate, MediaPmApi, MediaPmDocument, MediaPmService,
    MediaRuntimeStorage, MediaStepTool, TransformInputValue, load_mediapm_document,
    save_mediapm_document,
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
    assert!(service.paths().mediapm_state_ncl.exists());
    assert!(service.paths().hierarchy_root_dir == root.path());
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

/// Protects local-source URI acceptance used by `media add --preset local` flows.
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
    assert_eq!(
        user.runtime.conductor_state_config.as_deref(),
        Some(".mediapm/state.conductor.ncl")
    );
    assert_eq!(user.runtime.cas_store_dir.as_deref(), Some(".mediapm/store"));
    assert_eq!(user.runtime.conductor_tmp_dir.as_deref(), Some(".mediapm/tmp"));
    assert_eq!(user.runtime.conductor_schema_dir.as_deref(), Some(".mediapm/config/conductor"));
    assert!(
        user.runtime.inherited_env_vars.is_none(),
        "user runtime defaults should omit inherited_env_vars"
    );

    let expected_inherited = default_runtime_inherited_env_vars_for_host();

    let machine_bytes = std::fs::read(&service.paths().conductor_machine_ncl)
        .expect("read conductor machine document");
    let machine = decode_machine_document(&machine_bytes).expect("decode machine document");

    assert_eq!(machine.runtime.conductor_dir.as_deref(), Some(".mediapm"));
    assert_eq!(
        machine.runtime.conductor_state_config.as_deref(),
        Some(".mediapm/state.conductor.ncl")
    );
    assert_eq!(machine.runtime.cas_store_dir.as_deref(), Some(".mediapm/store"));
    assert_eq!(machine.runtime.conductor_tmp_dir.as_deref(), Some(".mediapm/tmp"));
    assert_eq!(machine.runtime.conductor_schema_dir.as_deref(), Some(".mediapm/config/conductor"));
    if expected_inherited.is_empty() {
        assert!(machine.runtime.inherited_env_vars.is_none());
    } else {
        assert_eq!(machine.runtime.inherited_env_vars, Some(expected_inherited));
    }
    assert!(root.path().join(".mediapm").join("state.conductor.ncl").exists());
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
            hierarchy_root_dir: Some("library-custom".to_string()),
            mediapm_tmp_dir: Some("tmp-custom".to_string()),
            materialization_preference_order: None,
            conductor_config: None,
            conductor_machine_config: None,
            conductor_state_config: None,
            conductor_tmp_dir: None,
            conductor_schema_dir: None,
            inherited_env_vars: None,
            media_state_config: None,
            env_file: None,
            mediapm_schema_dir: None,
            use_user_tool_cache: None,
        },
        ..MediaPmDocument::default()
    };
    save_mediapm_document(&service.paths().mediapm_ncl, &document)
        .expect("save mediapm.ncl with runtime_storage overrides");

    let _summary = service.sync_library().await.expect("sync should succeed");

    assert!(root.path().join(".mediapm-custom").join("state.ncl").exists());
    assert!(root.path().join(".mediapm-custom").join("state.conductor.ncl").exists());
    assert!(root.path().join("library-custom").exists());
    assert!(root.path().join(".mediapm-custom").join("tmp-custom").exists());
    assert!(!root.path().join(".mediapm").join("state.ncl").exists());
    assert!(!root.path().join(".conductor").join("state.ncl").exists());
}

/// Protects explicit `conductor_state_config` overrides from falling back to
/// `.conductor/state.ncl` during workflow execution.
#[tokio::test]
async fn sync_honors_explicit_conductor_state_override() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let document = MediaPmDocument {
        runtime: MediaRuntimeStorage {
            mediapm_dir: None,
            hierarchy_root_dir: None,
            mediapm_tmp_dir: None,
            materialization_preference_order: None,
            conductor_config: None,
            conductor_machine_config: None,
            conductor_state_config: Some("state/custom.state.ncl".to_string()),
            conductor_tmp_dir: None,
            conductor_schema_dir: None,
            inherited_env_vars: None,
            media_state_config: None,
            env_file: None,
            mediapm_schema_dir: None,
            use_user_tool_cache: None,
        },
        ..MediaPmDocument::default()
    };
    save_mediapm_document(&service.paths().mediapm_ncl, &document)
        .expect("save mediapm.ncl with explicit conductor_state_config override");

    let _summary = service.sync_library().await.expect("sync should succeed");

    assert!(root.path().join("state").join("custom.state.ncl").exists());
    assert!(root.path().join(".mediapm").join("state.ncl").exists());
    assert!(!root.path().join(".mediapm").join("state.conductor.ncl").exists());
    assert!(!root.path().join(".conductor").join("state.ncl").exists());
}

/// Protects remote add-flow defaults for managed `yt-dlp -> ffmpeg -> media-tagger -> rsgain` synthesis.
#[expect(
    clippy::too_many_lines,
    reason = "this test intentionally asserts full preset shape in one place so regressions remain easy to diagnose"
)]
#[tokio::test]
async fn add_media_source_sets_remote_download_defaults() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let remote = Url::parse("https://example.com/video.mkv").expect("url must parse");
    let media_id = service.add_media_source(&remote, None).await.expect("add media source");
    let document = load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm doc");
    let source = document.media.get(&media_id).expect("source exists");

    assert_eq!(source.steps.len(), 4);
    let yt_dlp_step = &source.steps[0];
    let ffmpeg_step = &source.steps[1];
    let media_tagger_step = &source.steps[2];
    let rsgain_step = &source.steps[3];

    assert_eq!(yt_dlp_step.tool, MediaStepTool::YtDlp);
    assert_eq!(
        yt_dlp_step.options.get("uri"),
        Some(&TransformInputValue::String("https://example.com/video.mkv".to_string())),
    );
    assert_eq!(
        yt_dlp_step.options.len(),
        1,
        "add_media_source should only store the uri option (format and sub_langs are auto-selected by yt-dlp)"
    );
    assert_eq!(
        yt_dlp_step.output_variants.get("video"),
        Some(&serde_json::json!({
            "kind": "primary",
        })),
    );
    assert_eq!(
        yt_dlp_step.output_variants.get("subtitles"),
        Some(&serde_json::json!({
            "kind": "subtitles",
        })),
    );
    assert_eq!(
        yt_dlp_step.output_variants.get("thumbnails"),
        Some(&serde_json::json!({
            "kind": "thumbnails",
        })),
    );
    assert_eq!(
        yt_dlp_step.output_variants.get("description"),
        Some(&serde_json::json!({
            "kind": "description",
        })),
    );
    assert_eq!(
        yt_dlp_step.output_variants.get("infojson"),
        Some(&serde_json::json!({
            "kind": "infojson",
        })),
    );
    assert_eq!(
        yt_dlp_step.output_variants.get("links"),
        Some(&serde_json::json!({
            "kind": "links",
        })),
    );
    assert_eq!(
        yt_dlp_step.output_variants.get("archive"),
        Some(&serde_json::json!({
            "kind": "archive",
        })),
    );
    assert_eq!(yt_dlp_step.output_variants.len(), 7);

    assert_eq!(ffmpeg_step.tool, MediaStepTool::Ffmpeg);
    assert_eq!(ffmpeg_step.input_variants, vec!["video".to_string()]);
    assert_eq!(
        ffmpeg_step.output_variants.get("video"),
        Some(&serde_json::json!({
            "kind": "primary",
            "idx": 0.0,
            "extension": "mkv",
        })),
    );
    assert_eq!(
        ffmpeg_step.options.get("codec_copy"),
        Some(&TransformInputValue::String("true".to_string())),
    );
    assert_eq!(
        ffmpeg_step.options.get("container"),
        Some(&TransformInputValue::String("matroska".to_string())),
    );

    assert_eq!(media_tagger_step.tool, MediaStepTool::MediaTagger);
    assert_eq!(media_tagger_step.input_variants, vec!["video".to_string()]);
    assert_eq!(
        media_tagger_step.output_variants.get("video"),
        Some(&serde_json::json!({
            "kind": "primary",
            "extension": "mkv",
        })),
    );

    assert_eq!(rsgain_step.tool, MediaStepTool::Rsgain);
    assert_eq!(rsgain_step.input_variants, vec!["video".to_string()]);
    assert_eq!(
        rsgain_step.output_variants.get("video"),
        Some(&serde_json::json!({
            "kind": "primary",
            "extension": "mkv",
        })),
    );

    let metadata = source.metadata.as_ref().expect("metadata should be set for remote add");
    assert!(matches!(
        metadata.get("title"),
        Some(MediaMetadataValue::Fallback(candidates))
            if matches!(
                candidates.as_slice(),
                [
                    MediaMetadataValueCandidate::Variant(first),
                    MediaMetadataValueCandidate::Variant(second),
                    MediaMetadataValueCandidate::Variant(third),
                    MediaMetadataValueCandidate::Literal(_),
                ]
                if first.variant == "video"
                    && first.metadata_key == "title"
                    && second.variant == "video"
                    && second.metadata_key == "track"
                    && third.variant == "infojson"
                    && third.metadata_key == "title"
            )
    ));
    assert!(matches!(
        metadata.get("artist"),
        Some(MediaMetadataValue::Fallback(candidates))
            if matches!(
                candidates.as_slice(),
                [
                    MediaMetadataValueCandidate::Variant(first),
                    MediaMetadataValueCandidate::Variant(second),
                    MediaMetadataValueCandidate::Variant(third),
                    MediaMetadataValueCandidate::Literal(literal),
                ]
                if first.variant == "video"
                    && first.metadata_key == "artist"
                    && second.variant == "video"
                    && second.metadata_key == "album_artist"
                    && third.variant == "infojson"
                    && third.metadata_key == "uploader"
                    && literal == "unknown"
            )
    ));
    assert!(
        matches!(metadata.get("video_id"), Some(MediaMetadataValue::Variant(binding)) if binding.variant == "infojson" && binding.metadata_key == "id")
    );
    assert!(matches!(
        metadata.get("video_ext"),
        Some(MediaMetadataValue::Variant(binding))
            if binding.variant == "video"
                && binding.metadata_key == "format_name"
                && binding
                    .transform
                    .as_ref()
                    .is_some_and(|transform| transform.pattern == "(?i)matroska(?:,.*)?" && transform.replacement == ".mkv")
    ));

    assert!(source.title.as_deref().is_some_and(|title| !title.trim().is_empty()));
    let description = source.description.as_deref().expect("description should be set");
    assert!(!description.trim().is_empty());
}

/// Protects local add-flow registration as `import -> media-tagger -> rsgain` CAS ingest.
#[tokio::test]
async fn add_local_source_sets_import_step_and_description() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());
    let local_file = root.path().join("sample-media.txt");
    std::fs::write(&local_file, b"sample-bytes").expect("write local file");

    let media_id = service.add_local_source(&local_file, None).await.expect("add local source");
    let document = load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm doc");
    let source = document.media.get(&media_id).expect("source exists");

    assert!(source.variant_hashes.is_empty());
    assert_eq!(source.steps.len(), 3);
    let import_step = &source.steps[0];
    let media_tagger_step = &source.steps[1];
    let rsgain_step = &source.steps[2];

    assert_eq!(import_step.tool, MediaStepTool::Import);
    assert!(import_step.input_variants.is_empty());
    assert_eq!(
        import_step.options.get("kind"),
        Some(&TransformInputValue::String("cas_hash".to_string())),
    );
    assert!(matches!(
        import_step.options.get("hash"),
        Some(TransformInputValue::String(value)) if !value.trim().is_empty()
    ));
    assert_eq!(
        import_step.output_variants.get("media"),
        Some(&serde_json::json!({
            "kind": "primary",
        })),
    );

    assert_eq!(media_tagger_step.tool, MediaStepTool::MediaTagger);
    assert_eq!(media_tagger_step.input_variants, vec!["media".to_string()]);
    assert_eq!(
        media_tagger_step.output_variants.get("media"),
        Some(&serde_json::json!({
            "kind": "primary",
        })),
    );

    assert_eq!(rsgain_step.tool, MediaStepTool::Rsgain);
    assert_eq!(rsgain_step.input_variants, vec!["media".to_string()]);
    assert_eq!(
        rsgain_step.output_variants.get("media"),
        Some(&serde_json::json!({
            "kind": "primary",
        })),
    );

    let metadata = source.metadata.as_ref().expect("metadata should be set for local add");
    assert!(matches!(
        metadata.get("title"),
        Some(MediaMetadataValue::Fallback(candidates))
            if matches!(
                candidates.as_slice(),
                [
                    MediaMetadataValueCandidate::Variant(first),
                    MediaMetadataValueCandidate::Variant(second),
                    MediaMetadataValueCandidate::Literal(literal),
                ]
                if first.variant == "media"
                    && first.metadata_key == "title"
                    && second.variant == "media"
                    && second.metadata_key == "track"
                    && literal == "sample-media.txt"
            )
    ));
    assert!(matches!(
        metadata.get("artist"),
        Some(MediaMetadataValue::Fallback(candidates))
            if matches!(
                candidates.as_slice(),
                [
                    MediaMetadataValueCandidate::Variant(first),
                    MediaMetadataValueCandidate::Variant(second),
                    MediaMetadataValueCandidate::Literal(literal),
                ]
                if first.variant == "media"
                    && first.metadata_key == "artist"
                    && second.variant == "media"
                    && second.metadata_key == "album_artist"
                    && literal == "unknown"
            )
    ));
    assert_eq!(metadata.get("video_ext"), Some(&MediaMetadataValue::Literal(".txt".to_string())));

    assert_eq!(source.title.as_deref(), Some("sample-media.txt"));

    let description = source.description.as_deref().expect("description should be set");
    assert!(description.contains("file:"));
    assert!(description.contains("sample-media.txt"));
    assert!(description.contains("artist:"));
}
