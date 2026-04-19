//! Unit tests for downloader helper behavior.

use std::collections::BTreeMap;
use std::future::Future;

use crate::config::ToolRequirement;
use crate::tools::catalog::ToolOs;
use crate::tools::catalog::{
    DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor,
};

use super::ResolvedToolIdentity;
use super::materialize::build_command_selector;
use super::resolve::{
    logical_name_matches_tool_id, resolve_download_plan, tool_id_suffix_from_identity,
};

/// Runs one async downloader helper future on a single-thread runtime.
fn run_async<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime")
        .block_on(future)
}

/// Verifies immutable id suffix precedence is hash -> version -> tag.
#[test]
fn tool_id_suffix_prefers_git_hash_then_version_then_tag() {
    let hash = tool_id_suffix_from_identity(&ResolvedToolIdentity {
        git_hash: Some("abcdef123456".to_string()),
        version: Some("2.0.0".to_string()),
        tag: Some("v2.0.0".to_string()),
        release_description: None,
    })
    .expect("git hash should win");
    assert_eq!(hash, "abcdef123456");

    let version = tool_id_suffix_from_identity(&ResolvedToolIdentity {
        git_hash: None,
        version: Some("2.0.0".to_string()),
        tag: Some("v2.0.0".to_string()),
        release_description: None,
    })
    .expect("version should be used when hash missing");
    assert_eq!(version, "2.0.0");

    let tag = tool_id_suffix_from_identity(&ResolvedToolIdentity {
        git_hash: None,
        version: None,
        tag: Some("v2.0.0".to_string()),
        release_description: None,
    })
    .expect("tag should be used when hash/version missing");
    assert_eq!(tag, "v2.0.0");
}

/// Verifies command selector renders platform-conditional path expression.
#[test]
fn build_command_selector_renders_platform_conditionals() {
    let selector = build_command_selector(&BTreeMap::from([
        (ToolOs::Windows, "windows/ffmpeg.exe".to_string()),
        (ToolOs::Linux, "linux/ffmpeg".to_string()),
        (ToolOs::Macos, "macos/ffmpeg".to_string()),
    ]))
    .expect("selector build should succeed");

    assert_eq!(
        selector,
        "${context.os == \"windows\" ? windows/ffmpeg.exe | ''}${context.os == \"linux\" ? linux/ffmpeg | ''}${context.os == \"macos\" ? macos/ffmpeg | ''}"
    );
}

/// Verifies host-only path sets produce direct selectors without conditionals.
#[test]
fn build_command_selector_accepts_single_os_path() {
    let selector = build_command_selector(&BTreeMap::from([(
        ToolOs::Windows,
        "windows/media-tagger.cmd".to_string(),
    )]))
    .expect("selector build should succeed");

    assert_eq!(selector, "windows/media-tagger.cmd");
}

/// Verifies logical-name matching accepts source-qualified immutable ids.
#[test]
fn logical_name_matching_accepts_source_qualified_ids() {
    assert!(logical_name_matches_tool_id(
        "mediapm.tools.yt-dlp+github-releases@abcdef12",
        "yt-dlp"
    ));
    assert!(logical_name_matches_tool_id("mediapm.tools.ffmpeg+github-btbn@latest", "ffmpeg"));
    assert!(!logical_name_matches_tool_id("mediapm.tools.rsgain+github-releases@latest", "yt-dlp"));
}

/// Verifies static catalog planning emits one action per supported OS target.
#[test]
fn resolve_download_plan_emits_cross_platform_actions() {
    let entry = ToolCatalogEntry {
        name: "fixture-tool",
        description: "fixture static downloader",
        registry_track: "latest",
        source_label: PlatformValue { windows: "Fixture", linux: "Fixture", macos: "Fixture" },
        source_identifier: PlatformValue { windows: "fixture", linux: "fixture", macos: "fixture" },
        executable_name: PlatformValue {
            windows: "fixture.exe",
            linux: "fixture",
            macos: "fixture",
        },
        download: ToolDownloadDescriptor::StaticUrls {
            modes: PlatformValue {
                windows: DownloadPayloadMode::DirectBinary,
                linux: DownloadPayloadMode::DirectBinary,
                macos: DownloadPayloadMode::DirectBinary,
            },
            urls: PlatformValue {
                windows: &["https://example.invalid/windows.exe"],
                linux: &["https://example.invalid/linux"],
                macos: &["https://example.invalid/macos"],
            },
            release_repo: None,
        },
    };

    let requirement = ToolRequirement {
        version: Some("1.2.3".to_string()),
        tag: None,
        recheck_seconds: None,
        max_input_slots: None,
        max_output_slots: None,
    };
    let plan = run_async(resolve_download_plan(entry, &requirement, None))
        .expect("static plan should resolve");

    assert_eq!(plan.per_os_actions.len(), 3);
    assert!(plan.per_os_actions.contains_key(&ToolOs::Windows));
    assert!(plan.per_os_actions.contains_key(&ToolOs::Linux));
    assert!(plan.per_os_actions.contains_key(&ToolOs::Macos));
    assert!(!plan.shared_package);
    assert!(!plan.internal_launcher);
}

/// Verifies static catalog planning marks shared payloads when URLs match.
#[test]
fn resolve_download_plan_marks_shared_package_when_urls_match() {
    let entry = ToolCatalogEntry {
        name: "fixture-tool",
        description: "fixture static downloader",
        registry_track: "latest",
        source_label: PlatformValue { windows: "Fixture", linux: "Fixture", macos: "Fixture" },
        source_identifier: PlatformValue { windows: "fixture", linux: "fixture", macos: "fixture" },
        executable_name: PlatformValue {
            windows: "fixture.exe",
            linux: "fixture",
            macos: "fixture",
        },
        download: ToolDownloadDescriptor::StaticUrls {
            modes: PlatformValue {
                windows: DownloadPayloadMode::ZipArchive,
                linux: DownloadPayloadMode::ZipArchive,
                macos: DownloadPayloadMode::ZipArchive,
            },
            urls: PlatformValue {
                windows: &["https://example.invalid/shared.zip"],
                linux: &["https://example.invalid/shared.zip"],
                macos: &["https://example.invalid/shared.zip"],
            },
            release_repo: None,
        },
    };

    let requirement = ToolRequirement {
        version: Some("1.2.3".to_string()),
        tag: None,
        recheck_seconds: None,
        max_input_slots: None,
        max_output_slots: None,
    };
    let plan = run_async(resolve_download_plan(entry, &requirement, None))
        .expect("static plan should resolve");

    assert!(plan.shared_package);
    assert!(!plan.internal_launcher);
}
