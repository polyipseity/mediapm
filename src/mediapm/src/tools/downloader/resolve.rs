//! Download-plan and release-identity resolution helpers.
//!
//! This module converts catalog recipes plus user selectors into concrete
//! cross-platform download actions and immutable identity metadata used for
//! tool-id creation.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{ToolRequirement, normalize_selector_compare_value};
use crate::error::MediaPmError;
use crate::tools::catalog::{
    DownloadPayloadMode, ToolCatalogEntry, ToolDownloadDescriptor, ToolOs, current_tool_os,
};

use super::ToolDownloadCache;
use super::github::{
    github_latest_release_json, github_release_asset_url_by_markers_from_release,
    github_release_by_tag_json, github_release_description, github_release_list_json,
    github_release_resolved_commit_hash, github_release_zip_asset_url_from_release,
};
use super::models::{OsDownloadAction, ResolvedDownloadPlan, ResolvedToolIdentity};

/// Running `mediapm` package version used for internal-launcher identity.
const CURRENT_MEDIAPM_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default release-metadata cache refresh interval when tool config omits
/// `recheck_seconds`.
const DEFAULT_RELEASE_METADATA_RECHECK_SECONDS: u64 = 24 * 60 * 60;

/// Prefix used for `mediapm`-managed immutable tool ids in test helpers.
#[cfg(test)]
const MANAGED_TOOL_ID_PREFIX: &str = "mediapm.tools.";

/// Resolves one download plan from a catalog entry and a declared requirement.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
pub(super) async fn resolve_download_plan(
    entry: &ToolCatalogEntry,
    requirement: &ToolRequirement,
    download_cache: Option<Arc<ToolDownloadCache>>,
) -> Result<ResolvedDownloadPlan, MediaPmError> {
    let requested_version = requirement.normalized_version();
    let requested_tag = requirement.normalized_tag();

    if requested_version.is_none() && requested_tag.is_none() {
        return Err(MediaPmError::Workflow(format!(
            "tool '{}' must define version or tag before reconciliation",
            entry.name
        )));
    }

    if let (Some(version), Some(tag)) = (&requested_version, &requested_tag)
        && normalize_selector_compare_value(version) != normalize_selector_compare_value(tag)
    {
        return Err(MediaPmError::Workflow(format!(
            "tool '{}' has mismatched version '{version}' and tag '{tag}'; selectors must match",
            entry.name
        )));
    }

    let host_os = current_tool_os();
    let mut warnings = Vec::new();

    match entry.download {
        ToolDownloadDescriptor::StaticUrls { modes, urls, release_repo } => {
            let mut per_os_actions = ToolOs::all()
                .into_iter()
                .map(|os| {
                    (
                        os,
                        OsDownloadAction {
                            os,
                            urls: urls.for_os(os).iter().map(|url| (*url).to_string()).collect(),
                            mode: modes.for_os(os),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let shared_package = all_os_first_urls_equal(&per_os_actions);

            let identity = if let Some(repo) = release_repo {
                let resolved = resolve_github_release(
                    repo,
                    entry.name,
                    requested_version.as_deref(),
                    requested_tag.as_deref(),
                    requirement.metadata_recheck_seconds(),
                    download_cache.clone(),
                )
                .await?;
                warnings.extend(resolved.warnings);
                let release = resolved.release;
                remap_latest_download_urls_from_release(&mut per_os_actions, &release);
                let tag = release
                    .get("tag_name")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .or(requested_tag.clone());
                let version = requested_version.clone().or_else(|| {
                    tag.as_deref()
                        .map(normalize_selector_compare_value)
                        .filter(|value| !value.is_empty())
                });

                validate_resolved_release_selectors(
                    entry.name,
                    requested_version.as_deref(),
                    requested_tag.as_deref(),
                    version.as_deref(),
                    tag.as_deref(),
                )?;

                ResolvedToolIdentity {
                    git_hash: github_release_resolved_commit_hash(repo, &release).await,
                    version,
                    tag,
                    release_description: github_release_description(&release)
                        .or_else(|| Some(entry.description.to_string())),
                }
            } else {
                ResolvedToolIdentity {
                    git_hash: None,
                    version: requested_version,
                    tag: requested_tag,
                    release_description: Some(entry.description.to_string()),
                }
            };

            Ok(ResolvedDownloadPlan {
                per_os_actions,
                shared_package,
                internal_launcher: false,
                common_executable_tool: None,
                identity,
                source_label: entry.source_label_for_os(host_os).to_string(),
                source_identifier: entry.source_identifier_for_os(host_os).to_string(),
                warnings,
            })
        }
        ToolDownloadDescriptor::GitHubLatestZipAsset { repo, markers } => {
            let resolved = resolve_github_release(
                repo,
                entry.name,
                requested_version.as_deref(),
                requested_tag.as_deref(),
                requirement.metadata_recheck_seconds(),
                download_cache,
            )
            .await?;
            warnings.extend(resolved.warnings);
            let release = resolved.release;
            let tag = release
                .get("tag_name")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or(requested_tag.clone());
            let version = requested_version.clone().or_else(|| {
                tag.as_deref()
                    .map(normalize_selector_compare_value)
                    .filter(|value| !value.is_empty())
            });
            validate_resolved_release_selectors(
                entry.name,
                requested_version.as_deref(),
                requested_tag.as_deref(),
                version.as_deref(),
                tag.as_deref(),
            )?;

            let per_os_actions = ToolOs::all()
                .into_iter()
                .map(|os| {
                    let marker_set = markers.for_os(os);
                    let url = github_release_zip_asset_url_from_release(&release, marker_set)?;
                    Ok((
                        os,
                        OsDownloadAction {
                            os,
                            urls: vec![url],
                            mode: DownloadPayloadMode::ZipArchive,
                        },
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, MediaPmError>>()?;

            Ok(ResolvedDownloadPlan {
                shared_package: all_os_first_urls_equal(&per_os_actions),
                per_os_actions,
                internal_launcher: false,
                common_executable_tool: None,
                identity: ResolvedToolIdentity {
                    git_hash: github_release_resolved_commit_hash(repo, &release).await,
                    version,
                    tag,
                    release_description: github_release_description(&release)
                        .or_else(|| Some(entry.description.to_string())),
                },
                source_label: entry.source_label_for_os(host_os).to_string(),
                source_identifier: entry.source_identifier_for_os(host_os).to_string(),
                warnings,
            })
        }
        ToolDownloadDescriptor::InternalLauncher => {
            let per_os_actions = ToolOs::all()
                .into_iter()
                .map(|os| {
                    (
                        os,
                        OsDownloadAction {
                            os,
                            urls: Vec::new(),
                            mode: DownloadPayloadMode::DirectBinary,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();

            let resolved_version = Some(CURRENT_MEDIAPM_VERSION.to_string());
            let resolved_tag = Some(format!("v{CURRENT_MEDIAPM_VERSION}"));

            if let Some(version) = requested_version.as_deref()
                && !is_latest_selector(version)
                && normalize_selector_compare_value(version)
                    != normalize_selector_compare_value(CURRENT_MEDIAPM_VERSION)
            {
                warnings.push(format!(
                    "tool '{}' requested internal launcher version '{}', but mediapm builtins always resolve to running mediapm version '{}'",
                    entry.name,
                    version,
                    CURRENT_MEDIAPM_VERSION
                ));
            }

            if let Some(tag) = requested_tag.as_deref()
                && !is_latest_selector(tag)
                && normalize_selector_compare_value(tag)
                    != normalize_selector_compare_value(CURRENT_MEDIAPM_VERSION)
            {
                warnings.push(format!(
                    "tool '{}' requested internal launcher tag '{}', but mediapm builtins always resolve to running mediapm version '{}'",
                    entry.name,
                    tag,
                    CURRENT_MEDIAPM_VERSION
                ));
            }

            Ok(ResolvedDownloadPlan {
                per_os_actions,
                shared_package: false,
                internal_launcher: true,
                common_executable_tool: None,
                identity: ResolvedToolIdentity {
                    git_hash: None,
                    version: resolved_version,
                    tag: resolved_tag,
                    release_description: Some(entry.description.to_string()),
                },
                source_label: entry.source_label_for_os(host_os).to_string(),
                source_identifier: entry.source_identifier_for_os(host_os).to_string(),
                warnings,
            })
        }
    }
}

/// Resolved release metadata plus non-fatal warnings.
struct ResolvedGitHubRelease {
    /// Release payload consumed by identity and URL-selection helpers.
    release: Value,
    /// Non-fatal warning messages emitted during resolution.
    warnings: Vec<String>,
}

/// Cached release-metadata payload envelope stored in shared tool cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CachedReleaseMetadata {
    /// Unix timestamp when metadata was fetched from remote API.
    fetched_unix_seconds: u64,
    /// Raw GitHub release payload.
    release: Value,
}

/// Validates requested selector constraints against resolved release metadata.
fn validate_resolved_release_selectors(
    tool_name: &str,
    requested_version: Option<&str>,
    requested_tag: Option<&str>,
    resolved_version: Option<&str>,
    resolved_tag: Option<&str>,
) -> Result<(), MediaPmError> {
    if let Some(version) = requested_version
        && !is_latest_selector(version)
    {
        let resolved = resolved_version.ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "tool '{tool_name}' requested version '{version}' but resolved release has no version metadata"
            ))
        })?;
        if normalize_selector_compare_value(version) != normalize_selector_compare_value(resolved) {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' requested version '{version}' but downloader resolved version '{resolved}'"
            )));
        }
    }

    if let Some(tag) = requested_tag
        && !is_latest_selector(tag)
    {
        let resolved = resolved_tag.ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "tool '{tool_name}' requested tag '{tag}' but resolved release has no tag metadata"
            ))
        })?;
        if normalize_selector_compare_value(tag) != normalize_selector_compare_value(resolved) {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' requested tag '{tag}' but downloader resolved tag '{resolved}'"
            )));
        }
    }

    Ok(())
}

/// Returns true when all per-OS actions resolve to the same primary URL.
fn all_os_first_urls_equal(actions: &BTreeMap<ToolOs, OsDownloadAction>) -> bool {
    let mut iter = actions.values();
    let Some(first) = iter.next() else {
        return false;
    };
    let Some(first_url) = first.urls.first() else {
        return false;
    };

    iter.all(|action| action.urls.first() == Some(first_url))
}

/// Rewrites stale `/releases/latest/download/...` candidates to concrete asset
/// URLs discovered in resolved GitHub release metadata.
fn remap_latest_download_urls_from_release(
    actions: &mut BTreeMap<ToolOs, OsDownloadAction>,
    release: &Value,
) {
    for action in actions.values_mut() {
        let mut resolved_urls = Vec::new();
        for url in &action.urls {
            let mapped =
                remap_latest_download_url(release, action.mode, url).unwrap_or_else(|| url.clone());
            if !resolved_urls.contains(&mapped) {
                resolved_urls.push(mapped);
            }
        }
        action.urls = resolved_urls;
    }
}

/// Resolves one static latest-download URL to the matching concrete release
/// asset URL when marker extraction succeeds.
fn remap_latest_download_url(
    release: &Value,
    mode: DownloadPayloadMode,
    url: &str,
) -> Option<String> {
    let filename = url.split("/releases/latest/download/").nth(1)?;
    let markers = latest_download_asset_markers(filename)?;
    let marker_refs = markers.iter().map(String::as_str).collect::<Vec<_>>();
    let require_zip = matches!(mode, DownloadPayloadMode::ZipArchive);
    github_release_asset_url_by_markers_from_release(release, &marker_refs, require_zip).ok()
}

/// Extracts marker tokens used to locate one concrete release asset.
fn latest_download_asset_markers(filename: &str) -> Option<Vec<String>> {
    let filename = filename.to_ascii_lowercase();
    let suffix = filename.as_str();

    let (stem, archive_suffix) = if let Some(stem) = suffix.strip_suffix(".tar.xz") {
        (stem, "tar.xz")
    } else if let Some(stem) = suffix.strip_suffix(".tar.gz") {
        (stem, "tar.gz")
    } else if let Some(stem) = suffix.strip_suffix(".zip") {
        (stem, "zip")
    } else {
        return None;
    };

    let mut markers = Vec::new();
    markers.push(stem.to_string());
    markers.push(archive_suffix.to_string());

    let stem_parts = stem
        .split('-')
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if let Some(last) = stem_parts.last() {
        markers.push(format!("{last}.{archive_suffix}"));
    }

    markers.extend(stem_parts);
    markers.sort();
    markers.dedup();
    Some(markers)
}

/// Resolves one GitHub release payload from tool selectors.
async fn resolve_github_release(
    repo: &str,
    tool_name: &str,
    requested_version: Option<&str>,
    requested_tag: Option<&str>,
    metadata_recheck_seconds: Option<u64>,
    download_cache: Option<Arc<ToolDownloadCache>>,
) -> Result<ResolvedGitHubRelease, MediaPmError> {
    let cache_key = release_metadata_cache_key(repo, requested_version, requested_tag);
    let cached = load_cached_release_metadata(download_cache.as_ref(), &cache_key).await;
    let now = now_unix_seconds();

    if !should_refresh_release_metadata(cached.as_ref(), metadata_recheck_seconds, now)
        && let Some(cached) = cached
    {
        return Ok(ResolvedGitHubRelease { release: cached.release, warnings: Vec::new() });
    }

    match fetch_github_release(repo, tool_name, requested_version, requested_tag).await {
        Ok(release) => {
            store_cached_release_metadata(download_cache.as_ref(), &cache_key, &release, now).await;
            Ok(ResolvedGitHubRelease { release, warnings: Vec::new() })
        }
        Err(error) => {
            if let Some(cached) = cached {
                let age_seconds = now.saturating_sub(cached.fetched_unix_seconds);
                return Ok(ResolvedGitHubRelease {
                    release: cached.release,
                    warnings: vec![format!(
                        "tool '{tool_name}' release metadata refresh for '{repo}' failed ({error}); using cached metadata from {age_seconds} seconds ago"
                    )],
                });
            }

            Err(error)
        }
    }
}

/// Fetches one GitHub release payload from requested version/tag selectors.
async fn fetch_github_release(
    repo: &str,
    tool_name: &str,
    requested_version: Option<&str>,
    requested_tag: Option<&str>,
) -> Result<Value, MediaPmError> {
    if let Some(tag) = requested_tag {
        if is_latest_selector(tag) {
            return github_latest_release_json(repo).await;
        }
        return github_release_by_tag_json(repo, tag).await;
    }

    let Some(version) = requested_version else {
        return Err(MediaPmError::Workflow(format!(
            "tool '{tool_name}' must define version or tag for GitHub release selection"
        )));
    };

    if is_latest_selector(version) {
        return github_latest_release_json(repo).await;
    }

    let preferred_tags = [version.to_string(), format!("v{version}")];
    for tag in preferred_tags {
        if let Ok(release) = github_release_by_tag_json(repo, &tag).await {
            return Ok(release);
        }
    }

    let releases = github_release_list_json(repo).await?;
    let release = releases
        .iter()
        .find(|release| {
            release.get("tag_name").and_then(Value::as_str).is_some_and(|tag| {
                normalize_selector_compare_value(tag) == normalize_selector_compare_value(version)
            })
        })
        .cloned()
        .ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "tool '{tool_name}' could not find GitHub release in '{repo}' matching version '{version}'"
            ))
        })?;

    Ok(release)
}

/// Builds one logical cache key for GitHub release metadata.
#[must_use]
fn release_metadata_cache_key(
    repo: &str,
    requested_version: Option<&str>,
    requested_tag: Option<&str>,
) -> String {
    let selector = requested_tag
        .map(|tag| format!("tag:{}", normalize_selector_compare_value(tag)))
        .or_else(|| {
            requested_version
                .map(|version| format!("version:{}", normalize_selector_compare_value(version)))
        })
        .unwrap_or_else(|| "unknown".to_string());

    format!("metadata=github-release|repo={repo}|selector={selector}")
}

/// Loads cached GitHub release metadata when available and decodable.
async fn load_cached_release_metadata(
    download_cache: Option<&Arc<ToolDownloadCache>>,
    cache_key: &str,
) -> Option<CachedReleaseMetadata> {
    let cache = download_cache?;
    let bytes = cache.lookup_bytes(cache_key).await?;
    serde_json::from_slice::<CachedReleaseMetadata>(&bytes).ok()
}

/// Stores one GitHub release payload in shared cache with refresh timestamp.
async fn store_cached_release_metadata(
    download_cache: Option<&Arc<ToolDownloadCache>>,
    cache_key: &str,
    release: &Value,
    fetched_unix_seconds: u64,
) {
    let Some(cache) = download_cache else {
        return;
    };

    let envelope = CachedReleaseMetadata { fetched_unix_seconds, release: release.clone() };

    if let Ok(bytes) = serde_json::to_vec(&envelope) {
        cache.store_bytes(cache_key, &bytes).await;
    }
}

/// Returns whether release metadata should be refreshed from remote API.
#[must_use]
fn should_refresh_release_metadata(
    cached: Option<&CachedReleaseMetadata>,
    metadata_recheck_seconds: Option<u64>,
    now_unix_seconds: u64,
) -> bool {
    let Some(cached) = cached else {
        return true;
    };

    let recheck_seconds =
        metadata_recheck_seconds.unwrap_or(DEFAULT_RELEASE_METADATA_RECHECK_SECONDS);
    now_unix_seconds.saturating_sub(cached.fetched_unix_seconds) >= recheck_seconds
}

/// Returns true when selector explicitly requests moving latest release.
fn is_latest_selector(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("latest")
}

/// Returns current Unix timestamp in seconds.
#[must_use]
fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Builds immutable id suffix from hash/version/tag precedence.
pub(super) fn tool_id_suffix_from_identity(
    identity: &ResolvedToolIdentity,
) -> Result<String, MediaPmError> {
    if let Some(hash) =
        identity.git_hash.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(hash.to_string());
    }
    if let Some(version) =
        identity.version.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(version.to_string());
    }
    if let Some(tag) = identity.tag.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(tag.to_string());
    }

    Err(MediaPmError::Workflow(
        "unable to derive immutable tool id: release metadata exposed no git hash, version, or tag"
            .to_string(),
    ))
}

/// Sanitizes one tool-id fragment for filesystem-safe identity keys.
pub(super) fn sanitize_tool_id_fragment(raw: &str) -> String {
    raw.trim()
        .trim_start_matches('@')
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '+') { ch } else { '-' }
        })
        .collect::<String>()
}

/// Returns true when immutable tool id belongs to one logical tool name.
#[cfg(test)]
pub(super) fn logical_name_matches_tool_id(tool_id: &str, logical_name: &str) -> bool {
    if tool_id.eq_ignore_ascii_case(logical_name) {
        return true;
    }

    let Some((prefix, _)) = tool_id.split_once('@') else {
        return false;
    };

    let prefixed = strip_managed_tool_id_prefix(prefix);
    let canonical_name = prefixed.split_once('+').map_or(prefixed, |(name, _)| name).trim();

    canonical_name.eq_ignore_ascii_case(logical_name)
}

/// Removes the optional `mediapm` managed-tool prefix from one id head.
#[cfg(test)]
fn strip_managed_tool_id_prefix(prefix: &str) -> &str {
    if prefix.len() >= MANAGED_TOOL_ID_PREFIX.len()
        && prefix[..MANAGED_TOOL_ID_PREFIX.len()].eq_ignore_ascii_case(MANAGED_TOOL_ID_PREFIX)
    {
        &prefix[MANAGED_TOOL_ID_PREFIX.len()..]
    } else {
        prefix
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;

    use serde_json::json;

    use crate::config::ToolRequirement;
    use crate::tools::catalog::{
        DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor, ToolOs,
    };

    use super::{
        CURRENT_MEDIAPM_VERSION, CachedReleaseMetadata, OsDownloadAction,
        latest_download_asset_markers, release_metadata_cache_key,
        remap_latest_download_urls_from_release, resolve_download_plan,
        should_refresh_release_metadata,
    };

    fn run_async<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime")
            .block_on(future)
    }

    /// Verifies release-metadata cache keys normalize selectors for stable reuse.
    #[test]
    fn release_metadata_cache_key_normalizes_version_selectors() {
        let key = release_metadata_cache_key("yt-dlp/yt-dlp", Some("v2026.04.01"), None);

        assert_eq!(key, "metadata=github-release|repo=yt-dlp/yt-dlp|selector=version:2026.04.01");
    }

    /// Verifies missing cache rows always force remote refresh attempts.
    #[test]
    fn should_refresh_release_metadata_without_cache() {
        assert!(should_refresh_release_metadata(None, Some(3600), 5_000));
    }

    /// Verifies recheck interval prevents premature metadata refreshes.
    #[test]
    fn should_refresh_release_metadata_respects_recheck_interval() {
        let cached = CachedReleaseMetadata {
            fetched_unix_seconds: 10_000,
            release: json!({ "tag_name": "v1.0.0" }),
        };

        assert!(!should_refresh_release_metadata(Some(&cached), Some(300), 10_299));
        assert!(should_refresh_release_metadata(Some(&cached), Some(300), 10_300));
    }

    /// Verifies omitted `recheck_seconds` uses one-day metadata cache reuse.
    #[test]
    fn should_refresh_release_metadata_defaults_to_daily_recheck_when_absent() {
        let cached = CachedReleaseMetadata {
            fetched_unix_seconds: 10_000,
            release: json!({ "tag_name": "v1.0.0" }),
        };

        assert!(!should_refresh_release_metadata(Some(&cached), None, 10_000 + (24 * 60 * 60) - 1));
        assert!(should_refresh_release_metadata(Some(&cached), None, 10_000 + (24 * 60 * 60)));
    }

    /// Verifies internal launcher identity is pinned to the running mediapm
    /// version regardless of moving-selector requests.
    #[test]
    fn internal_launcher_identity_uses_running_mediapm_version() {
        let entry = ToolCatalogEntry {
            name: "media-tagger",
            description: "fixture",
            registry_track: "latest",
            source_label: PlatformValue {
                windows: "internal",
                linux: "internal",
                macos: "internal",
            },
            source_identifier: PlatformValue {
                windows: "mediapm-internal",
                linux: "mediapm-internal",
                macos: "mediapm-internal",
            },
            executable_name: PlatformValue {
                windows: "media-tagger.cmd",
                linux: "media-tagger",
                macos: "media-tagger",
            },
            download: ToolDownloadDescriptor::InternalLauncher,
        };

        let requirement = ToolRequirement {
            version: Some("latest".to_string()),
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies::default(),
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let plan =
            run_async(resolve_download_plan(&entry, &requirement, None)).expect("resolve plan");

        let expected_tag = format!("v{CURRENT_MEDIAPM_VERSION}");
        assert_eq!(plan.identity.version.as_deref(), Some(CURRENT_MEDIAPM_VERSION));
        assert_eq!(plan.identity.tag.as_deref(), Some(expected_tag.as_str()));
    }

    /// Verifies marker extraction keeps platform, flavor, and archive suffix
    /// hints used to remap latest-download URLs to concrete release assets.
    #[test]
    fn latest_download_marker_extraction_is_stable() {
        let markers =
            latest_download_asset_markers("ffmpeg-master-latest-linux64-gpl-shared.tar.xz")
                .expect("markers");

        assert!(markers.contains(&"linux64".to_string()));
        assert!(markers.contains(&"gpl".to_string()));
        assert!(markers.contains(&"shared.tar.xz".to_string()));
        assert!(markers.contains(&"tar.xz".to_string()));
    }

    /// Verifies static latest-download URLs are rewritten to concrete
    /// per-release assets when release metadata provides matching files.
    #[test]
    fn remap_latest_download_urls_rewrites_matching_assets() {
        let mut actions = BTreeMap::from([
            (
                ToolOs::Linux,
                OsDownloadAction {
                    os: ToolOs::Linux,
                    urls: vec![
                        "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl-shared.tar.xz".to_string(),
                    ],
                    mode: DownloadPayloadMode::TarXzArchive,
                },
            ),
            (
                ToolOs::Windows,
                OsDownloadAction {
                    os: ToolOs::Windows,
                    urls: vec![
                        "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl-shared.zip".to_string(),
                    ],
                    mode: DownloadPayloadMode::ZipArchive,
                },
            ),
        ]);

        let release = json!({
            "assets": [
                {
                    "name": "ffmpeg-N-1-linux64-gpl-shared.tar.xz",
                    "browser_download_url": "https://example.test/linux.tar.xz"
                },
                {
                    "name": "ffmpeg-N-1-win64-gpl-shared.zip",
                    "browser_download_url": "https://example.test/win.zip"
                }
            ]
        });

        remap_latest_download_urls_from_release(&mut actions, &release);

        assert_eq!(
            actions.get(&ToolOs::Linux).and_then(|action| action.urls.first()).map(String::as_str),
            Some("https://example.test/linux.tar.xz")
        );
        assert_eq!(
            actions
                .get(&ToolOs::Windows)
                .and_then(|action| action.urls.first())
                .map(String::as_str),
            Some("https://example.test/win.zip")
        );
    }
}
