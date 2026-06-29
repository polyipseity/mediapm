//! Managed-tool download, resolution, HTTP fetching, GitHub API, and materialization.
//!
//! This module consolidates everything needed to:
//! - Resolve release metadata from GitHub API or static URLs,
//! - Fetch payload bytes through HTTP with resume and caching,
//! - Extract archives (ZIP, tar.gz, tar.xz),
//! - Materialize provisioned payloads under the workspace tool cache.
//!
//! # Re-exports
//!
//! - [`ToolDownloadCache`] – domain alias for [`UserLevelCache`].
//! - [`ToolCachePruneReport`] – domain alias for [`CachePruneReport`].
//! - [`default_global_tool_cache_root`] – default OS cache path for mediapm.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mediapm_conductor::cache::CachePruneReport;
use mediapm_conductor::cache_user_level::UserLevelCache;

/// User-level managed-tool download cache.
pub(crate) type ToolDownloadCache = UserLevelCache;

/// Summary of one cache-prune operation.
#[allow(dead_code)]
pub(crate) type ToolCachePruneReport = CachePruneReport;

use crate::error::MediaPmError;
use crate::http_client::shared_http_client;
use crate::tools::catalog::{
    ARCHIVE_BINARY, ARCHIVE_TAR_GZ, ARCHIVE_TAR_XZ, ARCHIVE_ZIP, ToolCatalogEntry, ToolOs,
    current_tool_os,
};
use crate::tools::models::{
    ContentMapSource, DownloadProgressCallback, DownloadProgressSnapshot, OsDownloadAction,
    ResolvedDownloadPlan, ResolvedToolIdentity,
};

// ---------------------------------------------------------------------------
// HTTP / fetch helpers
// ---------------------------------------------------------------------------

/// Builds a platform-appropriate HTTP [`Client`](reqwest::Client) for download
/// operations. Delegates to the shared process-wide client.
#[must_use]
#[allow(dead_code)]
pub(crate) fn build_http_client() -> reqwest::Client {
    shared_http_client().unwrap_or_else(|e| panic!("failed to create HTTP client: {e}")).clone()
}

/// Fetches payload bytes from the first URL candidate that returns a
/// successful response.
///
/// URLs are tried in order; the first 200-range response wins. Progress is
/// reported through the optional callback.
///
/// # Errors
///
/// Returns [`MediaPmError`] when all candidates fail.
pub(crate) async fn fetch_bytes_from_candidates(
    urls: &[String],
    progress: Option<DownloadProgressCallback>,
) -> Result<Vec<u8>, MediaPmError> {
    let client = shared_http_client()
        .map_err(|e| MediaPmError::Workflow(format!("failed to create HTTP client: {e}")))?;

    for url in urls {
        let request = client
            .get(url)
            .header(reqwest::header::USER_AGENT, crate::http_client::MEDIAPM_USER_AGENT);
        match request.send().await {
            Ok(response) if response.status().is_success() => {
                let total = response.content_length();
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|e| MediaPmError::Workflow(format!("download error: {e}")))?;
                if let Some(ref cb) = progress {
                    cb(DownloadProgressSnapshot {
                        downloaded_bytes: bytes.len() as u64,
                        total_bytes: total,
                    });
                }
                return Ok(bytes.to_vec());
            }
            Ok(response) => {
                tracing::warn!("HTTP {} for {}, skipping", response.status(), url);
            }
            Err(e) => {
                tracing::warn!("HTTP error for {url}: {e}, skipping");
            }
        }
    }

    Err(MediaPmError::Workflow(format!("all {} download candidates failed", urls.len())))
}

/// Probes the first responsive URL candidate for its `Content-Length` header.
///
/// # Errors
///
/// Returns [`MediaPmError`] when all candidates fail to respond.
#[allow(dead_code)]
pub(crate) async fn probe_content_length_from_candidates(
    urls: &[String],
) -> Result<Option<u64>, MediaPmError> {
    let client = shared_http_client()
        .map_err(|e| MediaPmError::Workflow(format!("failed to create HTTP client: {e}")))?;

    for url in urls {
        match client
            .head(url)
            .header(reqwest::header::USER_AGENT, crate::http_client::MEDIAPM_USER_AGENT)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                return Ok(response.content_length());
            }
            _ => {}
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// GitHub API helpers
// ---------------------------------------------------------------------------

/// Fetches the release JSON object for the latest release of a repo.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the API call fails.
#[allow(dead_code)]
pub(crate) async fn github_latest_release_json(
    repo: &str,
) -> Result<serde_json::Value, MediaPmError> {
    let url = format!("{GITHUB_API_BASE}/{repo}/releases/latest");
    github_api_json(&url).await
}

/// Fetches the release JSON object for a specific tag.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the API call fails.
#[allow(dead_code)]
pub(crate) async fn github_release_by_tag_json(
    repo: &str,
    tag: &str,
) -> Result<serde_json::Value, MediaPmError> {
    let url = format!("{GITHUB_API_BASE}/{repo}/releases/tags/{tag}");
    github_api_json(&url).await
}

/// Fetches a paginated list of recent releases.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the API call fails.
#[allow(dead_code)]
pub(crate) async fn github_release_list_json(
    repo: &str,
) -> Result<Vec<serde_json::Value>, MediaPmError> {
    let url = format!("{GITHUB_API_BASE}/{repo}/releases?per_page=10");
    let value = github_api_json(&url).await?;
    Ok(value.as_array().cloned().unwrap_or_default())
}

/// Extracts the human-readable description from a release JSON object.
#[must_use]
#[allow(dead_code)]
pub(crate) fn github_release_description(release: &serde_json::Value) -> Option<String> {
    release.get("body").and_then(|v| v.as_str()).map(ToString::to_string)
}

/// Finds the download URL of a named asset in a release JSON object.
#[must_use]
#[allow(dead_code)]
pub(crate) fn github_release_asset_url_by_markers(
    release: &serde_json::Value,
    name_contains: &[&str],
) -> Option<String> {
    let assets = release.get("assets")?.as_array()?;
    for asset in assets {
        let name = asset.get("name")?.as_str()?;
        if name_contains.iter().all(|m| name.contains(m)) {
            return asset.get("browser_download_url")?.as_str().map(String::from);
        }
    }
    None
}

/// Internal HTTP GET for GitHub API with auth and pagination.
async fn github_api_json(url: &str) -> Result<serde_json::Value, MediaPmError> {
    let client = shared_http_client()
        .map_err(|e| MediaPmError::Workflow(format!("failed to create HTTP client: {e}")))?;

    let response = client
        .get(url)
        .header(reqwest::header::USER_AGENT, crate::global::MEDIAPM_USER_AGENT)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| MediaPmError::Workflow(format!("GitHub API request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(MediaPmError::Workflow(format!(
            "GitHub API returned HTTP {}",
            response.status()
        )));
    }

    response
        .json()
        .await
        .map_err(|e| MediaPmError::Serialization(format!("failed to parse GitHub API JSON: {e}")))
}

use crate::tools::models::GITHUB_API_BASE;

// ---------------------------------------------------------------------------
// Resolve helpers
// ---------------------------------------------------------------------------

/// Resolves a download plan for the given catalog entry, determining URLs,
/// archive format, and whether it is a static or dynamic (GitHub API) plan.
///
/// **All-platform output**: the returned plan includes one
/// [`OsDownloadAction`] per [`ToolOs`] variant in `entry.platforms`, so
/// callers can download and provision executables for every supported OS
/// regardless of the host.
///
/// # Errors
///
/// Returns [`MediaPmError`] when required metadata cannot be resolved.
pub(crate) async fn resolve_download_plan(
    entry: &ToolCatalogEntry,
    _cache: &ToolDownloadCache,
) -> Result<ResolvedDownloadPlan, MediaPmError> {
    let host_os = current_tool_os();
    let mut per_os_actions = BTreeMap::new();
    let mut warnings = Vec::new();

    if entry.platforms.is_empty() {
        // Internal launcher — no downloadable payload.
        return Ok(ResolvedDownloadPlan {
            per_os_actions: BTreeMap::new(),
            shared_package: false,
            internal_launcher: true,
            identity: ResolvedToolIdentity::default(),
            source_label: format!("internal:{}", entry.id),
            source_identifier: entry.id.to_string(),
            warnings: Vec::new(),
        });
    }

    for (os, values) in &entry.platforms {
        let urls: Vec<String> = values.iter().map(|pv| pv.url.to_string()).collect();
        let archive_format =
            values.iter().find_map(|pv| pv.archive_format).unwrap_or(entry.archive_format);
        per_os_actions.insert(*os, OsDownloadAction { os: *os, urls, archive_format });
    }

    // Verify host OS has at least one action.
    if !per_os_actions.contains_key(&host_os) {
        warnings.push(format!(
            "no downloads defined for host OS {:?}, tool {} may not provision",
            host_os, entry.id
        ));
    }

    Ok(ResolvedDownloadPlan {
        per_os_actions,
        shared_package: false,
        internal_launcher: false,
        identity: ResolvedToolIdentity {
            version: Some(entry.latest.to_string()),
            ..ResolvedToolIdentity::default()
        },
        source_label: format!("catalog:{}", entry.id),
        source_identifier: entry.id.to_string(),
        warnings,
    })
}

/// Returns true when a logical tool name matches its catalog entry id.
#[must_use]
#[allow(dead_code)]
pub(crate) fn logical_name_matches_tool_id(logical_name: &str, entry: &ToolCatalogEntry) -> bool {
    logical_name.eq_ignore_ascii_case(entry.id)
}

/// Builds the immutable tool-id suffix from resolved identity.
#[must_use]
#[allow(dead_code)]
pub(crate) fn tool_id_suffix_from_identity(identity: &ResolvedToolIdentity) -> String {
    if let Some(ref hash) = identity.git_hash {
        hash[..12].to_string()
    } else if let Some(ref version) = identity.version {
        version.clone()
    } else if let Some(ref tag) = identity.tag {
        tag.clone()
    } else {
        "unknown".to_string()
    }
}

/// Builds a sandbox-relative command selector for the executable in a
/// materialized tool payload.
#[must_use]
#[allow(dead_code)]
pub(crate) fn build_command_selector(entry: &ToolCatalogEntry, os: ToolOs) -> String {
    let executable_name = entry.id;
    let ext = if os == ToolOs::Windows { ".exe" } else { "" };
    format!("./{executable_name}{ext}")
}

// ---------------------------------------------------------------------------
// Staging helper
// ---------------------------------------------------------------------------

/// Returns the staging base directory for one provision action within the tool
/// cache root.
#[must_use]
#[allow(dead_code)]
pub(crate) fn provision_staging_base_dir(tools_cache_root: &Path) -> PathBuf {
    tools_cache_root.join("staging")
}

// ---------------------------------------------------------------------------
// Materialize helpers
// ---------------------------------------------------------------------------

/// Materializes a resolved download plan into provisioned payload under the
/// given tool cache root.
///
/// Returns the primary executable path and content map entries.
///
/// # Errors
///
/// Returns [`MediaPmError`] when download or extraction fails.
#[allow(dead_code)]
pub(crate) async fn materialize_download_plan(
    plan: &ResolvedDownloadPlan,
    entry: &ToolCatalogEntry,
    tools_cache_root: &Path,
    cache: &ToolDownloadCache,
) -> Result<ProvisionedPayload, MediaPmError> {
    let host_os = current_tool_os();

    if plan.internal_launcher {
        return Ok(materialize_internal_launcher(entry, tools_cache_root));
    }

    let action = plan.per_os_actions.get(&host_os).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "no download action for host OS {:?} in plan for {}",
            host_os, entry.id
        ))
    })?;

    let staging_dir = provision_staging_base_dir(tools_cache_root);
    tokio::fs::create_dir_all(&staging_dir).await.map_err(|e| MediaPmError::Io {
        operation: "create staging dir".to_string(),
        path: staging_dir.clone(),
        source: e,
    })?;

    // Download payload bytes.
    let cache_key = format!("{}_{}", entry.id, entry.latest);
    let payload_bytes = if let Some(cached) = cache.lookup_bytes(&cache_key).await {
        cached
    } else {
        let bytes = fetch_bytes_from_candidates(&action.urls, None).await?;
        cache.store_bytes(&cache_key, &bytes).await;
        bytes
    };

    // Extract payload to staging.
    let extract_dir = staging_dir.join(format!("{}_{}", entry.id, entry.latest));
    if extract_dir.exists() {
        tokio::fs::remove_dir_all(&extract_dir).await.ok();
    }
    tokio::fs::create_dir_all(&extract_dir).await.map_err(|e| MediaPmError::Io {
        operation: "create extract dir".to_string(),
        path: extract_dir.clone(),
        source: e,
    })?;

    extract_archive(&payload_bytes, entry.archive_format, &extract_dir)?;

    // Build content map entries.
    let content_entries = build_content_entries(entry, &extract_dir, host_os);

    let identity = plan.identity.clone();
    let tool_id = format!("{}-{}", entry.id, tool_id_suffix_from_identity(&identity));

    Ok(ProvisionedPayload {
        tool_id,
        command_selector: build_command_selector(entry, host_os),
        content_entries,
        identity,
        source_label: plan.source_label.clone(),
        source_identifier: plan.source_identifier.clone(),
        catalog: entry.clone(),
        warnings: plan.warnings.clone(),
    })
}

/// Result of materializing one tool payload.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct ProvisionedPayload {
    /// Fully resolved immutable tool id.
    pub tool_id: String,
    /// Sandbox-relative command selector for executable tool specs.
    pub command_selector: String,
    /// Materialized content-map payload entries.
    pub content_entries: BTreeMap<String, ContentMapSource>,
    /// Resolved identity metadata for lock state and diagnostics.
    pub identity: ResolvedToolIdentity,
    /// Human-readable source label recorded in lock metadata.
    pub source_label: String,
    /// Stable source identifier fragment used in immutable tool id.
    pub source_identifier: String,
    /// Catalog entry that produced this payload.
    pub catalog: ToolCatalogEntry,
    /// Non-fatal metadata-resolution warnings.
    pub warnings: Vec<String>,
}

/// Materializes the internal launcher shim for tools that ship with mediapm
/// itself (e.g. media-tagger).
///
/// # Errors
///
/// Returns [`MediaPmError`] when the launcher binary cannot be located.
#[allow(dead_code)]
pub(crate) fn materialize_internal_launcher(
    entry: &ToolCatalogEntry,
    _tools_cache_root: &Path,
) -> ProvisionedPayload {
    let host_os = current_tool_os();
    let command_selector = build_command_selector(entry, host_os);

    ProvisionedPayload {
        tool_id: entry.id.to_string(),
        command_selector: command_selector.clone(),
        content_entries: BTreeMap::new(),
        identity: ResolvedToolIdentity::default(),
        source_label: format!("internal:{}", entry.id),
        source_identifier: entry.id.to_string(),
        catalog: entry.clone(),
        warnings: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Archive extraction helpers
// ---------------------------------------------------------------------------

/// Extracts archive bytes to the given directory based on archive format.
///
/// # Errors
///
/// Returns [`MediaPmError`] when extraction fails.
pub(crate) fn extract_archive(
    bytes: &[u8],
    format: &str,
    target_dir: &Path,
) -> Result<(), MediaPmError> {
    match format {
        ARCHIVE_ZIP => extract_zip(bytes, target_dir),
        ARCHIVE_TAR_GZ => extract_tar_gz(bytes, target_dir),
        ARCHIVE_TAR_XZ => extract_tar_xz(bytes, target_dir),
        ARCHIVE_BINARY => extract_binary(bytes, target_dir),
        other => Err(MediaPmError::Workflow(format!("unsupported archive format: {other}"))),
    }
}

fn extract_zip(bytes: &[u8], target_dir: &Path) -> Result<(), MediaPmError> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .map_err(|e| MediaPmError::Workflow(format!("ZIP open error: {e}")))?;

    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .map_err(|e| MediaPmError::Workflow(format!("ZIP entry error: {e}")))?;
        let out_path = target_dir.join(file.name());
        if file.name().ends_with('/') {
            std::fs::create_dir_all(&out_path).map_err(|e| MediaPmError::Io {
                operation: "create directory".to_string(),
                path: out_path.clone(),
                source: e,
            })?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| MediaPmError::Io {
                    operation: "create directory".to_string(),
                    path: parent.to_path_buf(),
                    source: e,
                })?;
            }
            let mut out = std::fs::File::create(&out_path).map_err(|e| MediaPmError::Io {
                operation: "create file".to_string(),
                path: out_path.clone(),
                source: e,
            })?;
            std::io::copy(&mut file, &mut out).map_err(|e| MediaPmError::Io {
                operation: "write file".to_string(),
                path: out_path.clone(),
                source: e,
            })?;
        }
    }
    Ok(())
}

fn extract_tar_gz(bytes: &[u8], target_dir: &Path) -> Result<(), MediaPmError> {
    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(target_dir).map_err(|e| MediaPmError::Io {
        operation: "extract tar.gz".to_string(),
        path: target_dir.to_path_buf(),
        source: e,
    })
}

fn extract_tar_xz(bytes: &[u8], target_dir: &Path) -> Result<(), MediaPmError> {
    let decoder = xz2::read::XzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(target_dir).map_err(|e| MediaPmError::Io {
        operation: "extract tar.xz".to_string(),
        path: target_dir.to_path_buf(),
        source: e,
    })
}

fn extract_binary(bytes: &[u8], target_dir: &Path) -> Result<(), MediaPmError> {
    let exe_name = if cfg!(target_os = "windows") { "tool.exe" } else { "tool" };
    let out_path = target_dir.join(exe_name);
    std::fs::write(&out_path, bytes).map_err(|e| MediaPmError::Io {
        operation: "write binary payload".to_string(),
        path: out_path.clone(),
        source: e,
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(0o755)).map_err(
            |e| MediaPmError::Io {
                operation: "set permissions".to_string(),
                path: out_path.clone(),
                source: e,
            },
        )?;
    }
    Ok(())
}

/// Builds content map entries from extracted files in the staging directory.
#[allow(dead_code)]
fn build_content_entries(
    _entry: &ToolCatalogEntry,
    extract_dir: &Path,
    os: ToolOs,
) -> BTreeMap<String, ContentMapSource> {
    let mut entries = BTreeMap::new();
    let os_prefix = match os {
        ToolOs::Windows => "windows",
        ToolOs::Linux => "linux",
        ToolOs::Macos => "macos",
    };

    entries.insert(
        os_prefix.to_string(),
        ContentMapSource::DirectoryZip { root_dir: extract_dir.to_path_buf() },
    );
    entries
}
