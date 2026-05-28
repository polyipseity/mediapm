use std::collections::BTreeMap;

use mediapm_cas::{FileSystemCas, Hash};
use mediapm_conductor::{
    InputBinding, MachineNickelDocument, ToolConfigSpec, ToolKindSpec, ToolSpec,
};

use crate::config::ToolRequirement;
use crate::lockfile::{MediaLockFile, ToolRegistryRecord, ToolRegistryStatus};
use crate::paths::MediaPmPaths;
use crate::tools::catalog::{
    DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor, current_tool_os,
    tool_catalog_entry,
};
use crate::tools::downloader::{
    ContentMapSource, DownloadProgressSnapshot, ProvisionedToolPayload,
};

use super::super::tool_runtime::{
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV, MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV,
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV,
};
use super::content_import::{ContentMapSourceCacheKey, import_tool_content_source_into_cas};
use super::lifecycle::{
    ensure_internal_launcher_content_entries_exist, is_builtin_source_ingest_requirement,
    lock_registry_version, should_skip_tag_update_check,
};
use super::provision::{
    TOOL_PROGRESS_BAR_SCALE, ToolDownloadProgressState, format_overall_tool_download_message,
    format_tool_download_completion_message, format_tool_download_message,
    normalize_download_progress_snapshot, tool_progress_position,
};
use super::tool_config::{
    augment_media_tagger_tool_id_with_ffmpeg_selector, ffmpeg_selector_from_registry_or_tool_id,
    media_tagger_ffmpeg_content_key, remove_redundant_inherited_env_vars_from_tool_config,
    resolve_companion_ffmpeg_selection, resolve_host_command_selector_path,
    resolve_managed_tool_command_absolute_path, resolve_yt_dlp_js_runtime_path,
    should_set_yt_dlp_ffmpeg_location,
};

fn catalog_entry_fixture(download: ToolDownloadDescriptor) -> ToolCatalogEntry {
    ToolCatalogEntry {
        name: "fixture",
        description: "fixture",
        registry_track: "latest",
        source_label: PlatformValue { windows: "fixture", linux: "fixture", macos: "fixture" },
        source_identifier: PlatformValue { windows: "fixture", linux: "fixture", macos: "fixture" },
        executable_name: PlatformValue {
            windows: "fixture.exe",
            linux: "fixture",
            macos: "fixture",
        },
        download,
        additional_download_sources: &[],
    }
}

fn provisioned_fixture(
    identity: crate::tools::downloader::ResolvedToolIdentity,
) -> ProvisionedToolPayload {
    ProvisionedToolPayload {
        tool_id: "mediapm.tools.fixture+fixture@latest".to_string(),
        command_selector: "fixture".to_string(),
        content_entries: BTreeMap::new(),
        identity,
        source_label: "fixture".to_string(),
        source_identifier: "fixture".to_string(),
        catalog: catalog_entry_fixture(ToolDownloadDescriptor::StaticUrls {
            modes: PlatformValue {
                windows: DownloadPayloadMode::DirectBinary,
                linux: DownloadPayloadMode::DirectBinary,
                macos: DownloadPayloadMode::DirectBinary,
            },
            urls: PlatformValue {
                windows: &["https://example.invalid/windows"],
                linux: &["https://example.invalid/linux"],
                macos: &["https://example.invalid/macos"],
            },
            release_repo: None,
        }),
        warnings: Vec::new(),
    }
}

/// Protects percentage scaling so per-tool bars map byte snapshots to the
/// fixed shared progress range used by `MultiProgress` rows.
#[test]
fn tool_progress_position_scales_known_totals() {
    let snapshot = DownloadProgressSnapshot { downloaded_bytes: 50, total_bytes: Some(200) };

    assert_eq!(tool_progress_position(snapshot), TOOL_PROGRESS_BAR_SCALE / 4);
}

/// Keeps companion ffmpeg auto-wiring active when yt-dlp defaults omit
/// `ffmpeg_location` entirely.
#[test]
fn should_set_yt_dlp_ffmpeg_location_when_missing() {
    let input_defaults = BTreeMap::new();
    assert!(should_set_yt_dlp_ffmpeg_location(&input_defaults));
}

/// Preserves explicit non-fallback ffmpeg paths instead of overwriting
/// user-provided yt-dlp defaults.
#[test]
fn should_not_set_yt_dlp_ffmpeg_location_for_explicit_value() {
    let input_defaults = BTreeMap::from([(
        "ffmpeg_location".to_string(),
        InputBinding::String("/custom/ffmpeg/bin".to_string()),
    )]);
    assert!(!should_set_yt_dlp_ffmpeg_location(&input_defaults));
}

/// Protects message contract by preserving compact known-size transfer
/// text during active downloads.
#[test]
fn format_tool_download_message_reports_known_totals() {
    let message = format_tool_download_message(
        "ffmpeg",
        DownloadProgressSnapshot { downloaded_bytes: 1_024, total_bytes: Some(2_048) },
    );

    assert!(message.contains("ffmpeg:"));
    assert!(message.contains("1.0 KiB / 2.0 KiB — downloading"));
}

/// Protects unknown-size transfer messaging so rows stay compact and avoid
/// redundant wording.
#[test]
fn format_tool_download_message_handles_unknown_totals() {
    let message = format_tool_download_message(
        "yt-dlp",
        DownloadProgressSnapshot { downloaded_bytes: 512, total_bytes: None },
    );

    assert_eq!(message, "yt-dlp: 512 B — downloading");
}

/// Protects transfer rendering from zero-size `Content-Length` headers by
/// treating them as unknown totals instead of forcing `0 B / 0 B` labels.
#[test]
fn normalize_download_progress_snapshot_treats_zero_total_as_unknown() {
    let normalized = normalize_download_progress_snapshot(DownloadProgressSnapshot {
        downloaded_bytes: 16 * 1024,
        total_bytes: Some(0),
    });

    assert_eq!(normalized.downloaded_bytes, 16 * 1024);
    assert_eq!(normalized.total_bytes, None);
}

/// Protects aggregate status labels so active downloads report compact
/// completed/total counts.
#[test]
fn format_overall_tool_download_message_reports_known_totals() {
    let states = BTreeMap::from([
        (
            "ffmpeg".to_string(),
            ToolDownloadProgressState {
                last_snapshot: Some(DownloadProgressSnapshot {
                    downloaded_bytes: 1_024,
                    total_bytes: Some(2_048),
                }),
                completed: true,
            },
        ),
        (
            "yt-dlp".to_string(),
            ToolDownloadProgressState {
                last_snapshot: Some(DownloadProgressSnapshot {
                    downloaded_bytes: 512,
                    total_bytes: Some(1_024),
                }),
                completed: false,
            },
        ),
    ]);

    let message = format_overall_tool_download_message(2, &states);
    assert_eq!(message, "tool downloads: 1/2 — downloading",);
}

/// Protects completion-row labels so successful tools collapse to one
/// downloaded-size value with stable status text.
#[test]
fn format_tool_download_completion_message_appends_status() {
    let message = format_tool_download_completion_message(
        "media-tagger",
        DownloadProgressSnapshot { downloaded_bytes: 2_048, total_bytes: Some(4_096) },
        "ready",
    );

    assert_eq!(message, "media-tagger: 2.0 KiB — ready");
}

/// Protects aggregate pre-download labels so the top row stays minimal
/// while workers are still resolving releases.
#[test]
fn format_overall_tool_download_message_reports_resolving_phase() {
    let states = BTreeMap::from([
        ("ffmpeg".to_string(), ToolDownloadProgressState::default()),
        ("yt-dlp".to_string(), ToolDownloadProgressState::default()),
    ]);

    let message = format_overall_tool_download_message(2, &states);
    assert_eq!(message, "tool downloads: resolving");
}

/// Protects aggregate completion labels so ready state reports only the
/// completed tool count and terminal status.
#[test]
fn format_overall_tool_download_message_reports_ready_phase() {
    let states = BTreeMap::from([
        (
            "ffmpeg".to_string(),
            ToolDownloadProgressState {
                last_snapshot: Some(DownloadProgressSnapshot {
                    downloaded_bytes: 1_024,
                    total_bytes: Some(1_024),
                }),
                completed: true,
            },
        ),
        (
            "yt-dlp".to_string(),
            ToolDownloadProgressState {
                last_snapshot: Some(DownloadProgressSnapshot {
                    downloaded_bytes: 2_048,
                    total_bytes: None,
                }),
                completed: true,
            },
        ),
    ]);

    let message = format_overall_tool_download_message(2, &states);
    assert_eq!(message, "tool downloads: 2 — ready");
}

/// Verifies lock registry version uses immutable identity precedence and
/// fails when all identity selectors are absent.
#[test]
fn lock_registry_version_uses_identity_precedence() {
    let with_hash = provisioned_fixture(crate::tools::downloader::ResolvedToolIdentity {
        git_hash: Some("abc123".to_string()),
        version: Some("1.2.3".to_string()),
        tag: Some("v1.2.3".to_string()),
        release_description: None,
    });
    assert_eq!(lock_registry_version(&with_hash).expect("hash wins"), "abc123");

    let with_version = provisioned_fixture(crate::tools::downloader::ResolvedToolIdentity {
        git_hash: None,
        version: Some("1.2.3".to_string()),
        tag: Some("v1.2.3".to_string()),
        release_description: None,
    });
    assert_eq!(lock_registry_version(&with_version).expect("version wins"), "1.2.3");

    let with_tag = provisioned_fixture(crate::tools::downloader::ResolvedToolIdentity {
        git_hash: None,
        version: None,
        tag: Some("v1.2.3".to_string()),
        release_description: None,
    });
    assert_eq!(lock_registry_version(&with_tag).expect("tag wins"), "v1.2.3");

    let missing = provisioned_fixture(crate::tools::downloader::ResolvedToolIdentity {
        git_hash: None,
        version: None,
        tag: None,
        release_description: None,
    });
    assert!(lock_registry_version(&missing).is_err());
}

/// Verifies reconciliation drops redundant inherited env-vars from
/// generated tool config rows while preserving tool-specific entries.
#[test]
fn inherited_env_vars_are_not_duplicated_into_tool_config_env_vars() {
    let mut config = mediapm_conductor::ToolConfigSpec {
        env_vars: BTreeMap::from([
            ("SYSTEMROOT".to_string(), "C:/Windows".to_string()),
            ("Temp".to_string(), "C:/Temp".to_string()),
            (
                MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV.to_string(),
                "C:/tools/mediapm.exe".to_string(),
            ),
            ("CUSTOM_TOOL_FLAG".to_string(), "enabled".to_string()),
        ]),
        ..mediapm_conductor::ToolConfigSpec::default()
    };

    remove_redundant_inherited_env_vars_from_tool_config(
        &mut config,
        &["systemroot".to_string(), "TEMP".to_string()],
    );

    assert!(!config.env_vars.contains_key("SYSTEMROOT"));
    assert!(!config.env_vars.contains_key("Temp"));
    assert!(config.env_vars.contains_key(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV));
    assert_eq!(config.env_vars.get("CUSTOM_TOOL_FLAG").map(String::as_str), Some("enabled"));
}

/// Verifies internal launchers do not use tag-only skip mode so stale
/// launcher content maps can be refreshed on sync.
#[test]
fn should_not_skip_tag_updates_for_internal_launcher() {
    let requirement = ToolRequirement {
        version: None,
        tag: Some("latest".to_string()),
        dependencies: crate::config::ToolRequirementDependencies::default(),
        recheck_seconds: None,
        max_input_slots: None,
        max_output_slots: None,
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "media-tagger".to_string(),
            "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };

    let machine = MachineNickelDocument {
        tools: BTreeMap::from([(
            "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
            mediapm_conductor::ToolSpec::default(),
        )]),
        ..MachineNickelDocument::default()
    };

    assert!(!should_skip_tag_update_check(&requirement, "media-tagger", &lock, &machine, false,));
}

/// Verifies tag-only skip mode is disabled when the active executable tool
/// row is missing non-host platform payload keys.
#[test]
fn should_not_skip_tag_updates_when_platform_selector_content_is_incomplete() {
    let requirement = ToolRequirement {
        version: None,
        tag: Some("latest".to_string()),
        dependencies: crate::config::ToolRequirementDependencies::default(),
        recheck_seconds: None,
        max_input_slots: None,
        max_output_slots: None,
    };
    let active_tool_id =
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string();
    let command_selector = "${context.os == \"windows\" ? windows/ffmpeg.exe | ''}${context.os == \"linux\" ? linux/ffmpeg | ''}${context.os == \"macos\" ? macos/ffmpeg | ''}".to_string();

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([("ffmpeg".to_string(), active_tool_id.clone())]),
        ..MediaLockFile::default()
    };

    let machine = MachineNickelDocument {
        tools: BTreeMap::from([(
            active_tool_id.clone(),
            ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec![command_selector],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            },
        )]),
        tool_configs: BTreeMap::from([(
            active_tool_id,
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([(
                    "windows/ffmpeg.exe".to_string(),
                    Hash::from_content(b"windows"),
                )])),
                ..ToolConfigSpec::default()
            },
        )]),
        ..MachineNickelDocument::default()
    };

    assert!(!should_skip_tag_update_check(&requirement, "ffmpeg", &lock, &machine, false,));
}

/// Verifies tag-only skip mode remains enabled when active executable
/// content maps include every platform selector branch target.
#[test]
fn should_skip_tag_updates_when_platform_selector_content_is_complete() {
    let requirement = ToolRequirement {
        version: None,
        tag: Some("latest".to_string()),
        dependencies: crate::config::ToolRequirementDependencies::default(),
        recheck_seconds: None,
        max_input_slots: None,
        max_output_slots: None,
    };
    let active_tool_id =
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string();
    let command_selector = "${context.os == \"windows\" ? windows/ffmpeg.exe | ''}${context.os == \"linux\" ? linux/ffmpeg | ''}${context.os == \"macos\" ? macos/ffmpeg | ''}".to_string();

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([("ffmpeg".to_string(), active_tool_id.clone())]),
        ..MediaLockFile::default()
    };

    let machine = MachineNickelDocument {
        tools: BTreeMap::from([(
            active_tool_id.clone(),
            ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec![command_selector],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            },
        )]),
        tool_configs: BTreeMap::from([(
            active_tool_id,
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([
                    ("windows/ffmpeg.exe".to_string(), Hash::from_content(b"windows")),
                    ("linux/ffmpeg".to_string(), Hash::from_content(b"linux")),
                    ("macos/ffmpeg".to_string(), Hash::from_content(b"macos")),
                ])),
                ..ToolConfigSpec::default()
            },
        )]),
        ..MachineNickelDocument::default()
    };

    assert!(should_skip_tag_update_check(&requirement, "ffmpeg", &lock, &machine, false,));
}

/// Verifies host-specific managed executable path resolution from
/// platform-conditional command selector templates.
#[test]
fn resolve_host_command_selector_path_prefers_host_selector_branch() {
    let selector = "${context.os == \"windows\" ? windows/tool.exe | ''}${context.os == \"linux\" ? linux/tool | ''}${context.os == \"macos\" ? macos/tool | ''}";
    let resolved = resolve_host_command_selector_path(selector).expect("path");
    let expected = if cfg!(windows) {
        "windows/tool.exe"
    } else if cfg!(target_os = "macos") {
        "macos/tool"
    } else {
        "linux/tool"
    };

    assert_eq!(resolved, expected);
}

/// Verifies command selector resolution returns direct path values when
/// selector is already host-specific text.
#[test]
fn resolve_host_command_selector_path_accepts_direct_path() {
    let resolved = resolve_host_command_selector_path("windows/ffmpeg-master/bin/ffmpeg.exe")
        .expect("direct path");

    assert_eq!(resolved, "windows/ffmpeg-master/bin/ffmpeg.exe");
}

/// Verifies media-tagger managed ids include selected ffmpeg selector
/// identity to invalidate stale launcher rows when ffmpeg changes.
#[test]
fn media_tagger_tool_id_includes_ffmpeg_selector_fragment() {
    let base_tool_id = "mediapm.tools.media-tagger+mediapm-internal@latest";
    let augmented =
        augment_media_tagger_tool_id_with_ffmpeg_selector(base_tool_id, "blake3:ABC_def");

    assert_eq!(
        augmented,
        "mediapm.tools.media-tagger+mediapm-internal+ffmpeg-blake3-abc-def@latest"
    );
}

/// Verifies ffmpeg selector derivation prefers lock registry versions and
/// falls back to immutable tool-id suffixes when registry rows are absent.
#[test]
fn ffmpeg_selector_resolution_uses_registry_then_tool_id_suffix() {
    let mut lock = MediaLockFile::default();
    lock.tool_registry.insert(
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1".to_string(),
        ToolRegistryRecord {
            name: "ffmpeg".to_string(),
            version: "v7.1".to_string(),
            source: "GitHub BTBN".to_string(),
            registry_multihash: "blake3:fixture".to_string(),
            last_transition_unix_seconds: 0,
            status: ToolRegistryStatus::Active,
        },
    );

    let from_registry = ffmpeg_selector_from_registry_or_tool_id(
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1",
        &lock,
    );
    assert_eq!(from_registry.as_deref(), Some("v7.1"));

    let from_suffix = ffmpeg_selector_from_registry_or_tool_id(
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@blake3-abcdef1234",
        &MediaLockFile::default(),
    );
    assert_eq!(from_suffix.as_deref(), Some("blake3-abcdef1234"));
}

/// Verifies media-tagger ffmpeg content entries are mounted under a stable
/// namespaced prefix to avoid collisions with launcher paths.
#[test]
fn media_tagger_ffmpeg_content_keys_are_namespaced() {
    assert_eq!(
        media_tagger_ffmpeg_content_key("windows/ffmpeg/bin/ffmpeg.exe"),
        "ffmpeg/windows/ffmpeg/bin/ffmpeg.exe"
    );
    assert_eq!(media_tagger_ffmpeg_content_key("ffmpeg/linux/ffmpeg"), "ffmpeg/linux/ffmpeg");
}

/// Verifies media-tagger ffmpeg env path resolves payload-layout binary paths
/// for installed managed tools.
#[test]
fn resolve_managed_tool_command_absolute_path_prefers_payload_layout() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1";
    let relative = "windows/bin/ffmpeg.exe";

    let absolute = paths.tools_dir.join(tool_id).join("payload").join(relative);
    std::fs::create_dir_all(absolute.parent().expect("parent dir")).expect("mkdirs");
    std::fs::write(&absolute, b"ffmpeg").expect("write fake ffmpeg binary");

    let resolved = resolve_managed_tool_command_absolute_path(&paths, Some(tool_id), relative)
        .expect("absolute path");

    assert_eq!(resolved, absolute.to_string_lossy().replace('\\', "/"));
}

/// Verifies media-tagger namespaced ffmpeg selectors resolve to the same
/// payload-layout managed-tool binary path.
#[test]
fn resolve_managed_tool_command_absolute_path_accepts_media_tagger_namespaced_paths() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1";
    let installed_relative = "windows/bin/ffmpeg.exe";
    let namespaced_relative = "ffmpeg/windows/bin/ffmpeg.exe";

    let absolute = paths.tools_dir.join(tool_id).join("payload").join(installed_relative);
    std::fs::create_dir_all(absolute.parent().expect("parent dir")).expect("mkdirs");
    std::fs::write(&absolute, b"ffmpeg").expect("write fake ffmpeg binary");

    let resolved =
        resolve_managed_tool_command_absolute_path(&paths, Some(tool_id), namespaced_relative)
            .expect("absolute path");

    assert_eq!(resolved, absolute.to_string_lossy().replace('\\', "/"));
}

/// Verifies missing internal media-tagger launcher files are
/// deterministically regenerated before CAS import.
#[test]
fn internal_media_tagger_launcher_entries_are_regenerated_when_missing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let install_root = temp.path().join("mediapm.tools.media-tagger+mediapm-internal@0.0.0");
    let windows_path = install_root.join("windows").join("media-tagger.cmd");
    let linux_path = install_root.join("linux").join("media-tagger");
    let macos_path = install_root.join("macos").join("media-tagger");

    let content_entries = BTreeMap::from([
        ("windows/media-tagger.cmd".to_string(), ContentMapSource::FilePath(windows_path.clone())),
        ("linux/media-tagger".to_string(), ContentMapSource::FilePath(linux_path.clone())),
        ("macos/media-tagger".to_string(), ContentMapSource::FilePath(macos_path.clone())),
    ]);

    let provisioned = ProvisionedToolPayload {
        tool_id: "mediapm.tools.media-tagger+mediapm-internal@0.0.0".to_string(),
        command_selector: "windows/media-tagger.cmd".to_string(),
        content_entries: content_entries.clone(),
        identity: crate::tools::downloader::ResolvedToolIdentity::default(),
        source_label: "mediapm internal launcher".to_string(),
        source_identifier: "mediapm-internal".to_string(),
        catalog: tool_catalog_entry("media-tagger").expect("catalog entry"),
        warnings: Vec::new(),
    };

    ensure_internal_launcher_content_entries_exist(&provisioned, &content_entries)
        .expect("regenerate missing launcher files");

    assert!(windows_path.is_file());
    assert!(linux_path.is_file());
    assert!(macos_path.is_file());

    let windows_script =
        std::fs::read_to_string(&windows_path).expect("read regenerated windows launcher");
    assert!(windows_script.contains(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV));

    let linux_script =
        std::fs::read_to_string(&linux_path).expect("read regenerated linux launcher");
    assert!(linux_script.contains(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV));

    let macos_script =
        std::fs::read_to_string(&macos_path).expect("read regenerated macos launcher");
    assert!(macos_script.contains(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV));
}

/// Verifies per-pass content-source caching reuses file-path imports.
#[test]
fn import_tool_content_source_into_cas_reuses_cached_file_path_hash() {
    let temp = tempfile::tempdir().expect("tempdir");
    let cas_root = temp.path().join("cas");
    let payload_path = temp.path().join("payload.bin");
    std::fs::write(&payload_path, b"fixture-payload").expect("write payload file");

    let runtime =
        tokio::runtime::Builder::new_current_thread().enable_all().build().expect("build runtime");

    runtime.block_on(async {
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let source = ContentMapSource::FilePath(payload_path.clone());
        let mut cache = BTreeMap::<ContentMapSourceCacheKey, Hash>::new();

        let first =
            import_tool_content_source_into_cas(&cas, "windows/tool.exe", &source, &mut cache)
                .await
                .expect("first import");

        std::fs::remove_file(&payload_path).expect("remove source payload file");

        let second =
            import_tool_content_source_into_cas(&cas, "windows/tool-copy.exe", &source, &mut cache)
                .await
                .expect("cached import");

        assert_eq!(first, second);
    });
}

/// Verifies per-pass content-source caching reuses directory-ZIP imports.
#[test]
fn import_tool_content_source_into_cas_reuses_cached_directory_zip_hash() {
    let temp = tempfile::tempdir().expect("tempdir");
    let cas_root = temp.path().join("cas");
    let directory_root = temp.path().join("tool-dir");
    std::fs::create_dir_all(&directory_root).expect("create tool directory");
    std::fs::write(directory_root.join("tool.txt"), b"tool-bytes")
        .expect("write directory payload");

    let runtime =
        tokio::runtime::Builder::new_current_thread().enable_all().build().expect("build runtime");

    runtime.block_on(async {
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let source = ContentMapSource::DirectoryZip { root_dir: directory_root.clone() };
        let mut cache = BTreeMap::<ContentMapSourceCacheKey, Hash>::new();

        let first = import_tool_content_source_into_cas(&cas, "windows/", &source, &mut cache)
            .await
            .expect("first directory import");

        std::fs::remove_dir_all(&directory_root).expect("remove source directory");

        let second =
            import_tool_content_source_into_cas(&cas, "windows-copy/", &source, &mut cache)
                .await
                .expect("cached directory import");

        assert_eq!(first, second);
    });
}

/// Verifies companion ffmpeg selector resolution for yt-dlp can pin to an
/// already-registered managed ffmpeg tool without requiring reprovision.
#[test]
fn companion_ffmpeg_selection_matches_registered_ffmpeg_tool() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let requirement = ToolRequirement {
        version: None,
        tag: Some("latest".to_string()),
        dependencies: crate::config::ToolRequirementDependencies {
            ffmpeg_version: Some("v7.1".to_string()),
            sd_version: None,
        },
        recheck_seconds: None,
        max_input_slots: None,
        max_output_slots: None,
    };

    let mut lock = MediaLockFile::default();
    lock.tool_registry.insert(
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1".to_string(),
        ToolRegistryRecord {
            name: "ffmpeg".to_string(),
            version: "v7.1".to_string(),
            source: "GitHub BTBN".to_string(),
            registry_multihash: "blake3:fixture".to_string(),
            last_transition_unix_seconds: 0,
            status: ToolRegistryStatus::Active,
        },
    );

    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1".to_string(),
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["windows/ffmpeg/bin/ffmpeg.exe".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        },
    );
    machine.tool_configs.insert(
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1".to_string(),
        ToolConfigSpec {
            content_map: Some(BTreeMap::from([(
                "windows/ffmpeg/bin/ffmpeg.exe".to_string(),
                Hash::from_content(b"ffmpeg-v7.1"),
            )])),
            ..ToolConfigSpec::default()
        },
    );
    let selection = resolve_companion_ffmpeg_selection(
        &paths,
        "yt-dlp",
        &requirement,
        &BTreeMap::new(),
        &lock,
        &machine,
    )
    .expect("companion selection should succeed");

    assert!(selection.provisioned_content_entries.is_empty());
    assert!(selection.existing_content_map.contains_key("windows/ffmpeg/bin/ffmpeg.exe"));
    assert_eq!(selection.host_command_path.as_deref(), Some("windows/ffmpeg/bin/ffmpeg.exe"));
}

/// Verifies companion ffmpeg linkage resolves payload-layout paths for
/// installed managed tools.
#[test]
fn companion_ffmpeg_selection_uses_payload_layout() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1";
    let host_os = current_tool_os().as_str();
    let ffmpeg_file_name = if cfg!(windows) { "ffmpeg.exe" } else { "ffmpeg" };

    let payload_dir = paths.tools_dir.join(tool_id).join("payload").join(host_os);
    std::fs::create_dir_all(&payload_dir).expect("create tool payload dir");
    std::fs::write(payload_dir.join(ffmpeg_file_name), b"ffmpeg").expect("write ffmpeg");

    let requirement = ToolRequirement {
        version: None,
        tag: Some("latest".to_string()),
        dependencies: crate::config::ToolRequirementDependencies {
            ffmpeg_version: Some("inherit".to_string()),
            sd_version: None,
        },
        recheck_seconds: None,
        max_input_slots: None,
        max_output_slots: None,
    };

    let mut lock = MediaLockFile::default();
    lock.active_tools.insert("ffmpeg".to_string(), tool_id.to_string());

    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(
        tool_id.to_string(),
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec![format!("{host_os}/{ffmpeg_file_name}")],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        },
    );

    let selection = resolve_companion_ffmpeg_selection(
        &paths,
        "yt-dlp",
        &requirement,
        &BTreeMap::new(),
        &lock,
        &machine,
    )
    .expect("companion selection should succeed");

    assert!(selection.existing_content_map.is_empty());
    assert_eq!(
        selection.host_command_path.as_deref(),
        Some(format!("{host_os}/{ffmpeg_file_name}").as_str())
    );
}

/// Verifies yt-dlp js runtime resolution resolves payload-layout paths only.
#[test]
fn resolve_yt_dlp_js_runtime_path_uses_payload_layout() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let tool_id = "mediapm.tools.yt-dlp+github-releases-yt-dlp@latest";
    let host_os = current_tool_os().as_str();
    let runtime_file_name = if cfg!(windows) { "deno.exe" } else { "deno" };

    let runtime_path =
        paths.tools_dir.join(tool_id).join("payload").join(host_os).join(runtime_file_name);
    std::fs::create_dir_all(runtime_path.parent().expect("runtime parent")).expect("mkdir");
    std::fs::write(&runtime_path, b"deno").expect("write runtime");

    let resolved = resolve_yt_dlp_js_runtime_path(&paths, tool_id).expect("resolved path");

    assert_eq!(resolved, runtime_path.to_string_lossy());
}

/// Verifies explicit yt-dlp companion ffmpeg selectors fail fast when no
/// managed ffmpeg identity matches the requested selector.
#[test]
fn companion_ffmpeg_selection_rejects_unknown_selector() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let requirement = ToolRequirement {
        version: None,
        tag: Some("latest".to_string()),
        dependencies: crate::config::ToolRequirementDependencies {
            ffmpeg_version: Some("v9.9".to_string()),
            sd_version: None,
        },
        recheck_seconds: None,
        max_input_slots: None,
        max_output_slots: None,
    };

    let error = resolve_companion_ffmpeg_selection(
        &paths,
        "yt-dlp",
        &requirement,
        &BTreeMap::new(),
        &MediaLockFile::default(),
        &MachineNickelDocument::default(),
    )
    .expect_err("unknown selector should fail");

    assert!(
        error.to_string().contains(
            "tools.yt-dlp.dependencies.ffmpeg_version 'v9.9' did not match any managed ffmpeg tool"
        ),
        "unexpected error: {error}"
    );
}

/// Verifies builtin source-ingest logical tool requirements are skipped
/// from downloader provisioning.
#[test]
fn builtin_source_ingest_tool_requirements_are_skipped_from_provisioning() {
    assert!(is_builtin_source_ingest_requirement("import"));
    assert!(!is_builtin_source_ingest_requirement("ffmpeg"));
    assert!(!is_builtin_source_ingest_requirement("yt-dlp"));
}
