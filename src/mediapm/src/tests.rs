// Tests for the mediapm crate.
#![allow(clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::fs;

use mediapm_cas::Hash;
use mediapm_conductor::{
    ExternalContentRef, MachineNickelDocument, ToolConfigSpec, ToolKindSpec, ToolSpec,
    encode_machine_document,
};
use serde_json::json;

use super::{
    AddInsertPosition, HierarchyNode, HierarchyNodeKind, LocalSourceMetadata, MediaHierarchyPreset,
    MediaPmApi, MediaPmDocument, MediaPmService, MediaRuntimeStorage, OnlineSourceMetadata,
    SanitizeNamesConfig, ToolRequirement, ToolRequirementDependencies, load_mediapm_document,
    merge_runtime_storage, parse_local_source_metadata_from_ffprobe_json,
    parse_online_source_metadata, save_mediapm_document, should_prefer_filesystem_workflow_runner,
    validate_source_uri,
};
use crate::config::load_mediapm_document_without_validation;
use crate::lockfile::{MediaLockFile, ToolRegistryRecord, ToolRegistryStatus, save_lockfile};
use crate::source_metadata::resolve_online_source_metadata_for_add;
use tempfile::tempdir;
use url::Url;

/// Ensures scheme validation allows online and local URI inputs.
#[test]
fn source_scheme_validation_matches_phase3_policy() {
    let http = Url::parse("https://example.com/video.mkv").expect("url");
    let local = Url::parse("local:media-id").expect("url");

    assert!(validate_source_uri(&http).is_ok());
    assert!(validate_source_uri(&local).is_ok());
}

/// Ensures unsupported schemes are rejected.
#[test]
fn source_scheme_validation_rejects_unsupported_schemes() {
    let ftp = Url::parse("ftp://example.com/video.mkv").expect("url");
    assert!(validate_source_uri(&ftp).is_err());
}

/// Ensures sync bootstraps default docs and state on a fresh workspace.
#[tokio::test]
async fn sync_library_bootstraps_default_phase3_state_files() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let _ = service.sync_library().await.expect("sync");

    assert!(service.paths().mediapm_ncl.exists());
    assert!(service.paths().conductor_user_ncl.exists());
    assert!(service.paths().conductor_machine_ncl.exists());
    assert!(service.paths().mediapm_state_ncl.exists());
    assert!(service.paths().runtime_root.join(".env").exists());
    assert!(service.paths().runtime_root.join(".env.generated").exists());
    assert!(service.paths().runtime_root.join(".gitignore").exists());

    let dotenv_text =
        fs::read_to_string(service.paths().runtime_root.join(".env")).expect("read .env");
    assert!(dotenv_text.contains("# conductor runtime environment variables"));
    assert!(dotenv_text.contains("# MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS="));
    assert!(dotenv_text.contains("# MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS="));
    assert!(dotenv_text.contains("# ACOUSTID_API_KEY="));
    assert!(dotenv_text.contains("# MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN="));

    let dotenv_generated_text =
        fs::read_to_string(service.paths().runtime_root.join(".env.generated"))
            .expect("read .env.generated");
    let _ = dotenv_generated_text;

    let gitignore_text = fs::read_to_string(service.paths().runtime_root.join(".gitignore"))
        .expect("read runtime .gitignore");
    assert!(gitignore_text.contains("/.env"));
    assert!(gitignore_text.contains("/.env.generated"));

    let schema_dir = service.paths().schema_export_dir.as_ref().expect("default schema export dir");
    assert!(schema_dir.join("mod.ncl").exists());
    assert!(schema_dir.join("v1.ncl").exists());

    let conductor_schema_dir = service.paths().conductor_schema_dir.clone();
    assert!(conductor_schema_dir.join("mod.ncl").exists());
    assert!(conductor_schema_dir.join("v1.ncl").exists());
}

/// Ensures tools-only sync bootstraps documents without running workflows.
#[tokio::test]
async fn sync_tools_bootstraps_default_state_files() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let summary = service.sync_tools().await.expect("tool sync");

    assert_eq!(summary.added_tools, 0);
    assert_eq!(summary.updated_tools, 0);
    assert_eq!(summary.unchanged_tools, 0);
    assert!(service.paths().mediapm_ncl.exists());
    assert!(service.paths().conductor_user_ncl.exists());
    assert!(service.paths().conductor_machine_ncl.exists());
    assert!(service.paths().mediapm_state_ncl.exists());
    assert!(service.paths().runtime_root.join(".env").exists());
    assert!(service.paths().runtime_root.join(".env.generated").exists());
    assert!(service.paths().runtime_root.join(".gitignore").exists());

    let dotenv_text =
        fs::read_to_string(service.paths().runtime_root.join(".env")).expect("read .env");
    assert!(dotenv_text.contains("# conductor runtime environment variables"));
    assert!(dotenv_text.contains("# MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS="));
    assert!(dotenv_text.contains("# MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS="));
    assert!(dotenv_text.contains("# ACOUSTID_API_KEY="));
    assert!(dotenv_text.contains("# MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN="));

    let dotenv_generated_text =
        fs::read_to_string(service.paths().runtime_root.join(".env.generated"))
            .expect("read .env.generated");
    let _ = dotenv_generated_text;

    let gitignore_text = fs::read_to_string(service.paths().runtime_root.join(".gitignore"))
        .expect("read runtime .gitignore");
    assert!(gitignore_text.contains("/.env"));
    assert!(gitignore_text.contains("/.env.generated"));

    let schema_dir = service.paths().schema_export_dir.as_ref().expect("default schema export dir");
    assert!(schema_dir.join("mod.ncl").exists());
    assert!(schema_dir.join("v1.ncl").exists());

    let conductor_schema_dir = service.paths().conductor_schema_dir.clone();
    assert!(conductor_schema_dir.join("mod.ncl").exists());
    assert!(conductor_schema_dir.join("v1.ncl").exists());
}

/// Ensures `tool add` can bootstrap a missing managed dependency target even
/// when another tool currently uses `inherit` for that dependency selector.
#[test]
fn add_tool_requirement_skips_cross_field_validation_during_bootstrap() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let mut document = MediaPmDocument::default();
    document.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: ToolRequirementDependencies {
                ffmpeg_version: Some("inherit".to_string()),
                deno_version: None,
                sd_version: None,
            },
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        },
    );
    save_mediapm_document(&service.paths().mediapm_ncl, &document).expect("seed mediapm.ncl");

    let added = service.add_tool_requirement("ffmpeg").expect("add ffmpeg");

    assert!(added, "ffmpeg should be added even when yt-dlp depends on inherit");

    let loaded = load_mediapm_document_without_validation(&service.paths().mediapm_ncl)
        .expect("load mediapm.ncl without validation");
    assert!(loaded.tools.contains_key("ffmpeg"));
    assert!(loaded.tools.contains_key("yt-dlp"));
}

/// Ensures `sync` does not emit stale tool-sync warnings when a tag-based
/// tool requirement (`tag = "latest"`) already has an active, materialized
/// runtime registration from a previous tool-sync pass.
#[tokio::test]
async fn sync_library_does_not_warn_for_latest_tag_with_current_runtime_state() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let mut document = MediaPmDocument::default();
    document.tools.insert(
        "ffmpeg".to_string(),
        ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: ToolRequirementDependencies::default(),
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        },
    );
    save_mediapm_document(&service.paths().mediapm_ncl, &document).expect("seed mediapm.ncl");

    let tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string();
    let registry_hash = Hash::from_content(b"registry-row");

    let mut machine = MachineNickelDocument::default();
    machine.external_data.insert(
        registry_hash,
        ExternalContentRef {
            description: Some("test content-map payload".to_string()),
            save: None,
        },
    );
    machine.tools.insert(
        tool_id.clone(),
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["./ffmpeg".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        },
    );
    machine.tool_configs.insert(
        tool_id.clone(),
        ToolConfigSpec {
            content_map: Some(BTreeMap::from([("./".to_string(), registry_hash)])),
            ..ToolConfigSpec::default()
        },
    );
    fs::create_dir_all(
        service.paths().conductor_machine_ncl.parent().expect("machine parent directory"),
    )
    .expect("create machine parent directory");
    fs::write(
        &service.paths().conductor_machine_ncl,
        encode_machine_document(machine).expect("encode machine"),
    )
    .expect("write machine doc");

    let mut lock = MediaLockFile::default();
    lock.active_tools.insert("ffmpeg".to_string(), tool_id.clone());
    lock.tool_registry.insert(
        tool_id,
        ToolRegistryRecord {
            name: "ffmpeg".to_string(),
            version: "2026.05.31".to_string(),
            source: "github-releases:btbn/ffmpeg-builds".to_string(),
            registry_multihash: registry_hash.to_string(),
            last_transition_unix_seconds: 1,
            status: ToolRegistryStatus::Active,
        },
    );
    save_lockfile(&service.paths().mediapm_state_ncl, &lock).expect("write lockfile");

    let summary = service.sync_library().await.expect("sync library");
    let warning = "tool state appears outdated for [";
    assert!(
        summary.warnings.iter().all(|entry| !entry.contains(warning)),
        "sync should not emit outdated-tool warning when active lock+machine state is current: {:?}",
        summary.warnings
    );
}

/// Ensures explicit `runtime.mediapm_schema_dir = null` disables schema
/// file export during sync.
#[tokio::test]
async fn sync_tools_skips_schema_export_when_runtime_schema_dir_is_null() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at_with_runtime_storage_overrides(
        root.path(),
        MediaRuntimeStorage { mediapm_schema_dir: Some(None), ..MediaRuntimeStorage::default() },
    );

    let summary = service.sync_tools().await.expect("tool sync");

    assert_eq!(summary.added_tools, 0);
    assert_eq!(summary.updated_tools, 0);
    assert_eq!(summary.unchanged_tools, 0);
    assert!(!root.path().join(".mediapm").join("config").join("mediapm").exists());
    let conductor_schema_dir = root.path().join(".mediapm").join("config").join("conductor");
    assert!(conductor_schema_dir.join("mod.ncl").exists());
    assert!(conductor_schema_dir.join("v1.ncl").exists());
}

/// Ensures local hierarchy preset insertion is idempotent for one
/// `(media, folder)` target and emits the expected folder tree.
#[tokio::test]
async fn add_local_hierarchy_preset_is_idempotent_for_existing_media() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());
    let local_file = root.path().join("local-source.txt");
    fs::write(&local_file, b"local-bytes").expect("write local source");
    let folder = "music videos";

    let media_id = service.add_local_source(&local_file, None).await.expect("add local source");

    service
        .add_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
        .expect("first hierarchy preset insertion should succeed");
    service
        .add_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
        .expect("second hierarchy preset insertion should remain idempotent");

    let document =
        load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm document");

    let matching_nodes: Vec<_> = document
        .hierarchy
        .iter()
        .filter(|node| {
            node.kind == HierarchyNodeKind::Folder
                && node.path == folder
                && node.media_id.is_none()
                && node.children.len() == 1
        })
        .collect();

    assert_eq!(
        matching_nodes.len(),
        1,
        "local hierarchy preset should exist exactly once for one media id/folder"
    );
    assert!(matching_nodes[0].id.is_none(), "outer hierarchy folder should not carry an id");
    let media_root = &matching_nodes[0].children[0];
    assert_eq!(
        media_root.id.as_deref(),
        Some(media_id.as_str()),
        "inner media-root folder should use the media id"
    );
    assert_eq!(
        media_root.path, "${media.metadata.title} [${media.id}]",
        "local hierarchy preset should keep stable media-root template"
    );
    let variants: Vec<_> =
        media_root.children.iter().map(|node| node.variant.as_deref().unwrap_or("")).collect();
    assert_eq!(variants, vec!["media"]);
    assert_eq!(media_root.children[0].id.as_deref(), Some(format!("{media_id}.media").as_str()));
}

/// Ensures yt-dlp hierarchy preset adds infojson projection while keeping
/// the same media-root style as the online demo (without sidecars folder).
#[tokio::test]
async fn add_yt_dlp_hierarchy_preset_includes_infojson_projection() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());
    let media_id = service
        .add_media_source(&Url::parse("https://example.com/video").expect("url"), None)
        .await
        .expect("add remote source");

    service
        .add_media_hierarchy_preset(MediaHierarchyPreset::YtDlp, &media_id, "music videos")
        .expect("add yt-dlp hierarchy preset");

    let document =
        load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm document");
    let media_root = document
        .hierarchy
        .iter()
        .find(|node| {
            node.kind == HierarchyNodeKind::Folder
                && node.path == "music videos"
                && node.media_id.is_none()
        })
        .and_then(|node| node.children.first())
        .expect("yt-dlp preset should create media-root child folder");

    let variants: std::collections::BTreeSet<_> = media_root
        .children
        .iter()
        .flat_map(|node| {
            let mut values = Vec::new();
            if let Some(variant) = node.variant.as_deref() {
                values.push(variant.to_string());
            }
            values.extend(node.variants.iter().cloned());
            values
        })
        .collect();
    assert_eq!(
        variants,
        std::collections::BTreeSet::from([
            "archive".to_string(),
            "description".to_string(),
            "infojson".to_string(),
            "links".to_string(),
            "subtitles".to_string(),
            "thumbnails".to_string(),
            "video".to_string(),
        ])
    );

    let variant_ids: std::collections::BTreeSet<_> = media_root
        .children
        .iter()
        .map(|node| node.id.as_deref().unwrap_or("").to_string())
        .collect();
    assert_eq!(
        variant_ids,
        std::collections::BTreeSet::from([
            format!("{media_id}.archive"),
            format!("{media_id}.description"),
            format!("{media_id}.infojson"),
            format!("{media_id}.links"),
            format!("{media_id}.subtitles"),
            format!("{media_id}.thumbnails"),
            format!("{media_id}.thumbnails.folder"),
            format!("{media_id}.video"),
        ])
    );
}

/// Ensures adding a hierarchy preset merges into an existing nameless
/// container folder at the same path instead of creating a duplicate
/// sibling.
#[test]
fn add_hierarchy_preset_merges_into_existing_nameless_container() {
    let folder = "music videos";
    let mut hierarchy = vec![HierarchyNode {
        path: folder.to_string(),
        kind: HierarchyNodeKind::Folder,
        id: None,
        media_id: None,
        variant: None,
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: super::PlaylistFormat::default(),
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::Inherit,
        children: vec![HierarchyNode {
            path: "existing-media-root".to_string(),
            kind: HierarchyNodeKind::Folder,
            id: Some("existing-id".to_string()),
            media_id: Some("existing-media".to_string()),
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: super::PlaylistFormat::default(),
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
            children: Vec::new(),
        }],
    }];

    let inserted = super::build_hierarchy_preset_node(
        MediaHierarchyPreset::Local,
        "new-media",
        folder,
        "new-media".to_string(),
    );
    super::insert_hierarchy_preset_node(
        &mut hierarchy,
        inserted,
        folder,
        AddInsertPosition::End,
        false,
    );

    // Verify no duplicate folder: exactly one node at the target path.
    let matching: Vec<_> = hierarchy
        .iter()
        .filter(|node| node.kind == HierarchyNodeKind::Folder && node.path == folder)
        .collect();
    assert_eq!(matching.len(), 1, "should not create a duplicate container folder");

    // Verify both media roots are present as children of the same folder.
    let container = &matching[0];
    let child_ids: Vec<Option<&str>> =
        container.children.iter().map(|child| child.id.as_deref()).collect();
    assert!(
        child_ids.contains(&Some("existing-id")),
        "existing media root should still be present"
    );
    assert!(child_ids.contains(&Some("new-media")), "new media root should be present");
}

/// Ensures sorted hierarchy insertion places missing ids first, then empty
/// ids, then lexicographically ordered non-empty ids within one root
/// folder.
#[test]
fn add_hierarchy_preset_sorted_order_uses_missing_empty_then_id() {
    let root_folder = "music videos/online";
    let mut hierarchy = vec![
        HierarchyNode {
            path: root_folder.to_string(),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: super::PlaylistFormat::default(),
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
            children: vec![HierarchyNode {
                path: "missing-id".to_string(),
                kind: HierarchyNodeKind::Folder,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: super::PlaylistFormat::default(),
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: Vec::new(),
            }],
        },
        HierarchyNode {
            path: root_folder.to_string(),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: super::PlaylistFormat::default(),
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
            children: vec![HierarchyNode {
                path: "empty-id".to_string(),
                kind: HierarchyNodeKind::Folder,
                id: Some(String::new()),
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: super::PlaylistFormat::default(),
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: Vec::new(),
            }],
        },
        HierarchyNode {
            path: root_folder.to_string(),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: super::PlaylistFormat::default(),
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
            children: vec![HierarchyNode {
                path: "zzz-id".to_string(),
                kind: HierarchyNodeKind::Folder,
                id: Some("zzz".to_string()),
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: super::PlaylistFormat::default(),
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: Vec::new(),
            }],
        },
    ];

    let inserted = super::build_hierarchy_preset_node(
        MediaHierarchyPreset::YtDlp,
        "aaa",
        root_folder,
        "aaa".to_string(),
    );
    super::insert_hierarchy_preset_node(
        &mut hierarchy,
        inserted,
        root_folder,
        AddInsertPosition::Sorted,
        false,
    );

    let observed_ids: Vec<Option<String>> = hierarchy
        .iter()
        .filter(|node| node.path == root_folder)
        .map(|node| node.children.first().and_then(|child| child.id.clone()))
        .collect();

    assert_eq!(
        observed_ids,
        vec![None, Some(String::new()), Some("aaa".to_string()), Some("zzz".to_string())]
    );
}

/// Ensures hierarchy add defaults to preset-specific root folder when no
/// folder is provided.
#[tokio::test]
async fn add_hierarchy_preset_uses_default_root_folder_when_omitted() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());
    let media_id = service
        .add_media_source(
            &Url::parse("https://www.youtube.com/watch?v=default-root").expect("url"),
            None,
        )
        .await
        .expect("add media source");

    service
        .add_media_hierarchy_preset_with_position(
            MediaHierarchyPreset::YtDlp,
            &media_id,
            None,
            AddInsertPosition::Sorted,
            false,
        )
        .expect("add hierarchy preset with default folder");

    let document =
        load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm document");
    assert!(
        document.hierarchy.iter().any(|node| node.path == "music videos/online"),
        "yt-dlp hierarchy preset should default to music videos/online root"
    );
}

/// Ensures hierarchy preset insertion fails for unknown media ids.
#[test]
fn add_hierarchy_preset_rejects_unknown_media_id() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let error = service
        .add_media_hierarchy_preset(MediaHierarchyPreset::Local, "missing-media", "music videos")
        .expect_err("unknown media id should be rejected");

    assert!(
        error
            .to_string()
            .contains("cannot add local hierarchy preset: media id 'missing-media' does not exist"),
        "error should explain missing media id"
    );
}

/// Ensures hierarchy preset removal is idempotent for one media/folder.
#[tokio::test]
async fn remove_hierarchy_preset_is_idempotent() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());
    let local_file = root.path().join("local-source.txt");
    fs::write(&local_file, b"local-bytes").expect("write local source");
    let folder = "music videos";

    let media_id = service.add_local_source(&local_file, None).await.expect("add local source");
    service
        .add_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
        .expect("add hierarchy preset");

    let removed_first = service
        .remove_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
        .expect("first hierarchy-preset removal should succeed");
    let removed_second = service
        .remove_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
        .expect("second hierarchy-preset removal should remain idempotent");

    assert_eq!(removed_first, 1, "first removal should remove one node");
    assert_eq!(removed_second, 0, "second removal should remove zero nodes");
}

/// Ensures media-source removal drops matching hierarchy nodes.
#[tokio::test]
async fn remove_media_source_removes_matching_hierarchy_nodes() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());
    let local_file = root.path().join("local-source.txt");
    fs::write(&local_file, b"local-bytes").expect("write local source");

    let media_id = service.add_local_source(&local_file, None).await.expect("add local source");
    service
        .add_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, "music videos")
        .expect("add hierarchy preset");

    let removed_hierarchy_nodes =
        service.remove_media_source(&media_id).expect("remove media source");
    assert_eq!(
        removed_hierarchy_nodes, 1,
        "media-source removal should cascade one matching hierarchy node"
    );

    let document =
        load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm document");
    assert!(!document.media.contains_key(&media_id), "removed media id should no longer exist");
    assert!(
        document.hierarchy.iter().all(|node| node.media_id.as_deref() != Some(&media_id)),
        "matching hierarchy nodes should also be removed"
    );
}

/// Ensures media-source removal rejects unknown media ids.
#[test]
fn remove_media_source_rejects_unknown_media_id() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());

    let error = service
        .remove_media_source("missing-media")
        .expect_err("unknown media id should be rejected");

    assert!(
        error
            .to_string()
            .contains("cannot remove media source: media id 'missing-media' does not exist"),
        "error should explain missing media id"
    );
}

/// Ensures local preset media-tagger defaults explicitly include both
/// optional `MusicBrainz` identifier fields as empty placeholders.
#[test]
fn local_preset_media_tagger_defaults_include_empty_mbids() {
    let steps = super::local_source_default_steps("blake3:deadbeef");
    let media_tagger_step = steps
        .iter()
        .find(|step| step.tool == super::MediaStepTool::MediaTagger)
        .expect("local preset should include media-tagger step");

    assert_eq!(
        media_tagger_step.options.get("recording_mbid"),
        Some(&super::TransformInputValue::String(String::new()))
    );
    assert_eq!(
        media_tagger_step.options.get("release_mbid"),
        Some(&super::TransformInputValue::String(String::new()))
    );
}

/// Ensures yt-dlp preset media-tagger defaults explicitly include both
/// optional `MusicBrainz` identifier fields as empty placeholders.
#[tokio::test]
async fn yt_dlp_preset_media_tagger_defaults_include_empty_mbids() {
    let root = tempdir().expect("tempdir");
    let service = MediaPmService::new_in_memory_at(root.path());
    let media_id = service
        .add_media_source(
            &Url::parse("https://www.youtube.com/watch?v=mbid-defaults").expect("url"),
            None,
        )
        .await
        .expect("add online media source");

    let document =
        load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm document");
    let media = document.media.get(&media_id).expect("media source should exist");
    let media_tagger_step = media
        .steps
        .iter()
        .find(|step| step.tool == super::MediaStepTool::MediaTagger)
        .expect("yt-dlp preset should include media-tagger step");

    assert_eq!(
        media_tagger_step.options.get("recording_mbid"),
        Some(&super::TransformInputValue::String(String::new()))
    );
    assert_eq!(
        media_tagger_step.options.get("release_mbid"),
        Some(&super::TransformInputValue::String(String::new()))
    );
}

/// Ensures service-level runtime overrides keep precedence for retained
/// runtime-storage fields.
#[test]
fn merge_runtime_storage_prefers_override_fields() {
    let config = MediaRuntimeStorage {
        env_file: Some("config.env".to_string()),
        env_generated_file: None,
        inherited_env_vars: Some(BTreeMap::from([(
            "windows".to_string(),
            vec!["SYSTEMROOT".to_string(), "PATH".to_string()],
        )])),
        ..MediaRuntimeStorage::default()
    };
    let override_value = MediaRuntimeStorage {
        env_file: Some("override.env".to_string()),
        env_generated_file: None,
        inherited_env_vars: Some(BTreeMap::from([
            ("WINDOWS".to_string(), vec!["path".to_string(), "TMPDIR".to_string()]),
            ("linux".to_string(), vec!["LD_LIBRARY_PATH".to_string()]),
        ])),
        ..MediaRuntimeStorage::default()
    };

    let merged = merge_runtime_storage(&config, &override_value);

    assert_eq!(merged.env_file.as_deref(), Some("override.env"));
    assert_eq!(
        merged.inherited_env_vars,
        Some(BTreeMap::from([
            ("linux".to_string(), vec!["LD_LIBRARY_PATH".to_string()],),
            (
                "windows".to_string(),
                vec!["SYSTEMROOT".to_string(), "PATH".to_string(), "TMPDIR".to_string(),],
            ),
        ]))
    );
}

/// Ensures online metadata parsing extracts title/artist/description when
/// downloader JSON includes those fields.
#[test]
fn parse_online_metadata_reads_title_artist_and_description() {
    let payload = json!({
        "fulltitle": "Demo Song",
        "uploader": "Demo Artist",
        "description": "A short description"
    });

    let metadata = parse_online_source_metadata(&payload);
    assert_eq!(
        metadata,
        OnlineSourceMetadata {
            title: Some("Demo Song".to_string()),
            artist: Some("Demo Artist".to_string()),
            description: Some("A short description".to_string()),
        }
    );
}

/// Ensures remote add-flow metadata falls back to built-in defaults and emits
/// a warning when yt-dlp is not configured.
#[test]
fn resolve_online_metadata_for_add_warns_when_yt_dlp_is_missing() {
    let url = Url::parse("https://example.com/demo-video").expect("url");

    let warning = "yt-dlp managed tool is not configured; cannot fetch title, description, or artist metadata for remote source 'https://example.com/demo-video'".to_string();
    let resolved = resolve_online_source_metadata_for_add(&url, None, Some(warning.clone()));

    assert_eq!(resolved.title, "demo-video");
    assert_eq!(resolved.description, "title: demo-video\nartist: unknown");
    assert_eq!(resolved.artist, None);
    assert_eq!(resolved.warning.as_deref(), Some(warning.as_str()));
}

/// Ensures remote add-flow metadata prefers yt-dlp-fetched title, artist, and
/// description values when the tool is configured.
#[test]
fn resolve_online_metadata_for_add_prefers_yt_dlp_values_when_configured() {
    let url = Url::parse("https://example.com/demo-video").expect("url");
    let fetched = OnlineSourceMetadata {
        title: Some("Fetched Title".to_string()),
        artist: Some("Fetched Artist".to_string()),
        description: Some("Fetched Description".to_string()),
    };

    let resolved = resolve_online_source_metadata_for_add(&url, Some(fetched), None);

    assert_eq!(resolved.title, "Fetched Title");
    assert_eq!(resolved.description, "Fetched Description");
    assert_eq!(resolved.artist, Some("Fetched Artist".to_string()));
    assert!(resolved.warning.is_none());
}

/// Ensures short `YouTube` links are normalized to the canonical watch URL.
#[test]
fn normalize_source_uri_expands_short_youtube_links() {
    let short = Url::parse("https://youtu.be/dQw4w9WgXcQ?t=43").expect("url");

    let normalized = crate::normalize_source_uri(&short);

    assert_eq!(normalized.as_str(), "https://www.youtube.com/watch?v=dQw4w9WgXcQ");
}

/// Ensures local metadata parsing extracts title/description from ffprobe
/// `format.tags` payloads with case-insensitive key matching.
#[test]
fn parse_local_metadata_reads_ffprobe_tags_case_insensitively() {
    let payload = json!({
        "format": {
            "tags": {
                "TITLE": "Local Demo",
                "Comment": "Local description"
            }
        }
    });

    let metadata = parse_local_source_metadata_from_ffprobe_json(&payload);
    assert_eq!(
        metadata,
        LocalSourceMetadata {
            title: Some("Local Demo".to_string()),
            description: Some("Local description".to_string()),
        }
    );
}

/// Ensures workflow execution prefers filesystem CAS when managed runtime
/// tool configs reference persisted payload hashes.
#[test]
fn prefer_filesystem_workflow_runner_when_content_map_hashes_exist() {
    let machine = MachineNickelDocument {
        tool_configs: BTreeMap::from([(
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([(
                    "./".to_string(),
                    Hash::from_content(b"payload"),
                )])),
                ..ToolConfigSpec::default()
            },
        )]),
        ..MachineNickelDocument::default()
    };

    assert!(should_prefer_filesystem_workflow_runner(&machine));
}

/// Ensures workflow execution keeps existing conductor backend when no
/// managed runtime payload hashes are configured.
#[test]
fn prefer_filesystem_workflow_runner_is_false_without_content_map_hashes() {
    let machine = MachineNickelDocument {
        tool_configs: BTreeMap::from([(
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ToolConfigSpec::default(),
        )]),
        ..MachineNickelDocument::default()
    };

    assert!(!should_prefer_filesystem_workflow_runner(&machine));
}
