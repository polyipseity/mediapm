//! Workspace-local download and extraction helpers for managed media tools.
//!
//! This folder-module keeps provisioning logic focused by splitting catalog
//! resolution, release metadata querying, transfer behavior, and payload
//! materialization into dedicated submodules.

mod cache;
mod github;
mod http;
mod materialize;
mod models;
mod resolve;

#[cfg(test)]
mod tests {
    //! Unit tests for downloader helper behavior.

    use std::collections::BTreeMap;

    use crate::config::{ToolRequirement, ToolRequirementDependencies};
    use crate::tools::catalog::ToolOs;
    use crate::tools::catalog::{
        DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor,
    };

    use crate::paths::MediaPmPaths;
    use crate::test_util::run_async;
    use crate::tools::downloader::ResolvedToolIdentity;
    use crate::tools::downloader::materialize::build_command_selector;
    use crate::tools::downloader::resolve::{
        logical_name_matches_tool_id, resolve_download_plan, tool_id_suffix_from_identity,
    };
    use crate::tools::downloader::resolve_provision_staging_base_dir;

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
            "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@abcdef12",
            "yt-dlp"
        ));
        assert!(logical_name_matches_tool_id(
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest",
            "ffmpeg"
        ));
        assert!(!logical_name_matches_tool_id(
            "mediapm.tools.rsgain+github-releases-complexlogic-rsgain@latest",
            "yt-dlp"
        ));
    }

    /// Verifies static catalog planning emits one action per supported OS target.
    #[test]
    fn resolve_download_plan_emits_cross_platform_actions() {
        let entry = ToolCatalogEntry {
            name: "fixture-tool",
            description: "fixture static downloader",
            registry_track: "latest",
            source_label: PlatformValue { windows: "Fixture", linux: "Fixture", macos: "Fixture" },
            source_identifier: PlatformValue {
                windows: "fixture",
                linux: "fixture",
                macos: "fixture",
            },
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
            additional_download_sources: &[],
        };

        let requirement = ToolRequirement {
            version: Some("1.2.3".to_string()),
            tag: None,
            dependencies: ToolRequirementDependencies::default(),
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };
        let plan = run_async(resolve_download_plan(&entry, &requirement, None))
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
            source_identifier: PlatformValue {
                windows: "fixture",
                linux: "fixture",
                macos: "fixture",
            },
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
            additional_download_sources: &[],
        };

        let requirement = ToolRequirement {
            version: Some("1.2.3".to_string()),
            tag: None,
            dependencies: ToolRequirementDependencies::default(),
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };
        let plan = run_async(resolve_download_plan(&entry, &requirement, None))
            .expect("static plan should resolve");

        assert!(plan.shared_package);
        assert!(!plan.internal_launcher);
    }

    /// Verifies provisioning staging prefers a user-scoped cache root when one is
    /// available so workspace-local tmp is not required for tool sync staging.
    #[test]
    fn resolve_provision_staging_base_dir_prefers_user_scoped_root() {
        let paths = MediaPmPaths::from_root("/workspace/project");
        let user_scoped_root = PathBuf::from("/Users/demo/Library/Caches/mediapm/cache/tmp");

        let resolved = resolve_provision_staging_base_dir(&paths, Some(user_scoped_root.clone()));

        assert_eq!(resolved, user_scoped_root);
    }

    /// Verifies provisioning staging falls back to workspace runtime tmp when no
    /// user-scoped cache root can be resolved on the current host.
    #[test]
    fn resolve_provision_staging_base_dir_falls_back_to_workspace_tmp() {
        let paths = MediaPmPaths::from_root("/workspace/project");

        let resolved = resolve_provision_staging_base_dir(&paths, None);

        assert_eq!(resolved, paths.mediapm_tmp_dir);
    }
}

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::ToolRequirement;
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::tools::catalog::tool_catalog_entry;

pub(crate) use cache::{ToolCachePruneReport, ToolDownloadCache, default_global_tool_cache_root};
pub(crate) use models::{ContentMapSource, ProvisionedToolPayload, ResolvedToolIdentity};

/// Byte-level transfer snapshot emitted while one tool payload downloads.
///
/// The downloader reports progress for the currently active URL candidate
/// only. If fallback moves to a new candidate, progress may restart from
/// `0 / total` for that candidate instead of accumulating bytes from failed
/// attempts.
///
/// `total_bytes` is the active candidate `Content-Length` when provided by
/// the server. When unknown, `total_bytes` is `None` and callers should treat
/// progress as indeterminate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DownloadProgressSnapshot {
    /// Cumulative bytes read so far across all attempted URL candidates.
    pub downloaded_bytes: u64,
    /// Total payload bytes expected across attempted + active candidates,
    /// when known.
    pub total_bytes: Option<u64>,
}

/// Callback invoked as downloader transfer progress advances.
///
/// Callers may use this to update per-tool progress UI while preserving
/// download concurrency.
pub(crate) type DownloadProgressCallback = Arc<dyn Fn(DownloadProgressSnapshot) + Send + Sync>;

/// Stable prefix for `mediapm`-managed immutable tool ids.
const MANAGED_TOOL_ID_PREFIX: &str = "mediapm.tools.";

/// Ensures one managed tool payload is provisioned into user-scoped staging
/// storage and converted into conductor-ready command/content-map metadata.
///
/// Staging remains necessary because downloader materialization may need to:
/// - expand archives,
/// - discover executable paths,
/// - collect deterministic content-map entries before CAS import.
///
/// When available, staging is rooted under user cache (`<os-cache>/mediapm`) so
/// repeated workspace runs do not churn one workspace-local tmp tree.
/// Workspace tmp remains the fallback when no user cache root is available.
pub(crate) async fn provision_tool_payload(
    paths: &MediaPmPaths,
    tool_name: &str,
    requirement: &ToolRequirement,
    download_progress: Option<DownloadProgressCallback>,
    download_cache: Option<Arc<ToolDownloadCache>>,
) -> Result<ProvisionedToolPayload, MediaPmError> {
    let entry = tool_catalog_entry(tool_name)?;
    let resolved =
        resolve::resolve_download_plan(&entry, requirement, download_cache.clone()).await?;
    let suffix = resolve::tool_id_suffix_from_identity(&resolved.identity)?;
    let tool_name_id = resolve::sanitize_tool_id_fragment(entry.name);
    let source_id = resolve::sanitize_tool_id_fragment(&resolved.source_identifier);
    let tool_id = format!(
        "{MANAGED_TOOL_ID_PREFIX}{}+{}@{}",
        tool_name_id,
        source_id,
        resolve::sanitize_tool_id_fragment(&suffix)
    );
    let install_root = create_provision_staging_dir(paths, &tool_id)?;

    materialize::materialize_download_plan(
        &entry,
        &resolved,
        &install_root,
        download_progress,
        download_cache,
    )
    .await?;

    let executable_paths = materialize::resolve_executable_paths(&entry, &resolved, &install_root)?;
    let command_selector = materialize::build_command_selector(&executable_paths)?;
    let content_entries =
        materialize::collect_materialized_content_entries(&resolved, &install_root)?;
    if content_entries.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "tool '{tool_id}' provisioning produced no content-map payload entries"
        )));
    }

    Ok(ProvisionedToolPayload {
        tool_id,
        command_selector,
        content_entries,
        identity: resolved.identity,
        source_label: resolved.source_label,
        source_identifier: resolved.source_identifier,
        catalog: entry,
        warnings: resolved.warnings,
    })
}

/// Creates one unique staging directory for a single provisioning run.
///
/// A per-run temp directory avoids cross-process races where concurrent
/// provisioning attempts for the same immutable tool id could otherwise delete
/// each other's in-flight staged payloads.
fn create_provision_staging_dir(
    paths: &MediaPmPaths,
    tool_id: &str,
) -> Result<PathBuf, MediaPmError> {
    let user_scoped_root =
        default_global_tool_cache_root().map(|cache_root| cache_root.join("tmp"));
    let staging_base_dir = resolve_provision_staging_base_dir(paths, user_scoped_root);
    fs::create_dir_all(&staging_base_dir).map_err(|source| MediaPmError::Io {
        operation: format!("creating provisioning staging base directory for '{tool_id}'"),
        path: staging_base_dir.clone(),
        source,
    })?;

    let staging_dir = tempfile::Builder::new()
        .prefix("tool-sync-provision-")
        .tempdir_in(&staging_base_dir)
        .map_err(|source| MediaPmError::Io {
            operation: format!("creating staged tool install directory for '{tool_id}'"),
            path: staging_base_dir,
            source,
        })?;

    Ok(staging_dir.keep())
}

/// Resolves the staging base directory for one tool payload provisioning run.
#[must_use]
pub(super) fn resolve_provision_staging_base_dir(
    paths: &MediaPmPaths,
    user_scoped_root: Option<PathBuf>,
) -> PathBuf {
    user_scoped_root.unwrap_or_else(|| paths.mediapm_tmp_dir.clone())
}
