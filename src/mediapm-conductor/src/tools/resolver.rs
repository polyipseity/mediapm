//! Download resolution, HTTP fetching, GitHub API, and archive extraction.
//!
//! Feature-gated behind `tool-presets`.
//!
//! This module provides the core functions needed to resolve download plans
//! from catalog entries, fetch payload bytes, and extract archives.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[cfg(feature = "tool-presets")]
use futures_util::StreamExt;

#[cfg(feature = "tool-presets")]
use crate::cache_user_level::UserLevelCache;
#[cfg(feature = "tool-presets")]
use crate::error::ConductorError;
#[cfg(feature = "tool-presets")]
use crate::tools::catalog::{
    ARCHIVE_BINARY, ARCHIVE_TAR_GZ, ARCHIVE_TAR_XZ, ARCHIVE_ZIP, ToolCatalogEntry, ToolOs,
    current_tool_os,
};
#[cfg(feature = "tool-presets")]
use crate::tools::model::{
    ContentMapSource, DownloadProgressCallback, DownloadProgressSnapshot, OsDownloadAction,
    ResolvedDownloadPlan, ResolvedToolIdentity,
};

/// User-agent string for conductor-initiated HTTP requests.
#[cfg(feature = "tool-presets")]
const CONDUCTOR_USER_AGENT: &str = concat!("mediapm-conductor/", env!("CARGO_PKG_VERSION"));

/// User-level managed-tool download cache used by the resolver.
#[cfg(feature = "tool-presets")]
pub(crate) type ToolDownloadCache = UserLevelCache;

// ---------------------------------------------------------------------------
// HTTP / fetch helpers
// ---------------------------------------------------------------------------

/// Fetches payload bytes from the first URL candidate that returns a
/// successful response.
///
/// URLs are tried in order; the first 200-range response wins. Progress is
/// reported through the optional callback.
///
/// # Errors
///
/// Returns [`ConductorError`] when all candidates fail.
#[cfg(feature = "tool-presets")]
pub(crate) async fn fetch_bytes_from_candidates(
    urls: &[String],
    progress: Option<DownloadProgressCallback>,
) -> Result<Vec<u8>, ConductorError> {
    let client = reqwest::Client::builder()
        .user_agent(CONDUCTOR_USER_AGENT)
        .build()
        .map_err(|e| ConductorError::Workflow(format!("building HTTP client failed: {e}")))?;

    for url in urls {
        let request = client.get(url);
        match request.send().await {
            Ok(response) if response.status().is_success() => {
                let total = response.content_length();
                let mut downloaded = 0u64;
                let mut buffer = Vec::new();
                let mut stream = response.bytes_stream();
                while let Some(chunk_result) = stream.next().await {
                    let chunk = chunk_result
                        .map_err(|e| ConductorError::Workflow(format!("download error: {e}")))?;
                    downloaded += chunk.len() as u64;
                    buffer.extend_from_slice(&chunk);
                    if let Some(ref cb) = progress {
                        cb(DownloadProgressSnapshot {
                            downloaded_bytes: downloaded,
                            total_bytes: total,
                        });
                    }
                }
                return Ok(buffer);
            }
            Ok(response) => {
                tracing::warn!("HTTP {} for {}, skipping", response.status(), url);
            }
            Err(e) => {
                tracing::warn!("HTTP error for {url}: {e}, skipping");
            }
        }
    }

    Err(ConductorError::Workflow(format!("all {} download candidates failed", urls.len())))
}

/// Probes the first responsive URL candidate for its `Content-Length` header.
///
/// # Errors
///
/// Returns [`ConductorError`] when all candidates fail to respond.
#[cfg(feature = "tool-presets")]
#[allow(dead_code)]
pub(crate) async fn probe_content_length_from_candidates(
    urls: &[String],
) -> Result<Option<u64>, ConductorError> {
    let client = reqwest::Client::builder()
        .user_agent(CONDUCTOR_USER_AGENT)
        .build()
        .map_err(|e| ConductorError::Workflow(format!("building HTTP client failed: {e}")))?;

    for url in urls {
        match client.head(url).send().await {
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

use crate::tools::model::GITHUB_API_BASE;

/// Fetches the release JSON object for the latest release of a repo.
///
/// # Errors
///
/// Returns [`ConductorError`] when the API call fails.
#[cfg(feature = "tool-presets")]
#[allow(dead_code)]
pub(crate) async fn github_latest_release_json(
    repo: &str,
) -> Result<serde_json::Value, ConductorError> {
    let url = format!("{GITHUB_API_BASE}/{repo}/releases/latest");
    github_api_json(&url).await
}

/// Fetches the release JSON object for a specific tag.
///
/// # Errors
///
/// Returns [`ConductorError`] when the API call fails.
#[cfg(feature = "tool-presets")]
#[allow(dead_code)]
pub(crate) async fn github_release_by_tag_json(
    repo: &str,
    tag: &str,
) -> Result<serde_json::Value, ConductorError> {
    let url = format!("{GITHUB_API_BASE}/{repo}/releases/tags/{tag}");
    github_api_json(&url).await
}

/// Fetches a paginated list of recent releases.
///
/// # Errors
///
/// Returns [`ConductorError`] when the API call fails.
#[cfg(feature = "tool-presets")]
#[allow(dead_code)]
pub(crate) async fn github_release_list_json(
    repo: &str,
) -> Result<Vec<serde_json::Value>, ConductorError> {
    let url = format!("{GITHUB_API_BASE}/{repo}/releases?per_page=10");
    let value = github_api_json(&url).await?;
    Ok(value.as_array().cloned().unwrap_or_default())
}

/// Extracts the human-readable description from a release JSON object.
#[cfg(feature = "tool-presets")]
#[must_use]
#[allow(dead_code)]
pub(crate) fn github_release_description(release: &serde_json::Value) -> Option<String> {
    release.get("body").and_then(|v| v.as_str()).map(ToString::to_string)
}

/// Finds the download URL of a named asset in a release JSON object.
#[cfg(feature = "tool-presets")]
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
#[cfg(feature = "tool-presets")]
async fn github_api_json(url: &str) -> Result<serde_json::Value, ConductorError> {
    let client = reqwest::Client::builder()
        .user_agent(CONDUCTOR_USER_AGENT)
        .build()
        .map_err(|e| ConductorError::Workflow(format!("building HTTP client failed: {e}")))?;

    let response = client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| ConductorError::Workflow(format!("GitHub API request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(ConductorError::Workflow(format!(
            "GitHub API returned HTTP {}",
            response.status()
        )));
    }

    response
        .json()
        .await
        .map_err(|e| ConductorError::Serialization(format!("failed to parse GitHub API JSON: {e}")))
}

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
/// Returns [`ConductorError`] when required metadata cannot be resolved.
#[cfg(feature = "tool-presets")]
pub(crate) async fn resolve_download_plan(
    entry: &ToolCatalogEntry,
    _cache: &ToolDownloadCache,
) -> Result<ResolvedDownloadPlan, ConductorError> {
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
        let archive_format = values[0].archive_format;
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
#[cfg(feature = "tool-presets")]
#[must_use]
#[allow(dead_code)]
pub(crate) fn logical_name_matches_tool_id(logical_name: &str, entry: &ToolCatalogEntry) -> bool {
    logical_name.eq_ignore_ascii_case(entry.id)
}

/// Builds a sandbox-relative command selector for the executable in a
/// materialized tool payload.
#[cfg(feature = "tool-presets")]
#[must_use]
#[allow(dead_code)]
pub(crate) fn build_command_selector(entry: &ToolCatalogEntry, os: ToolOs) -> String {
    let executable_name = entry.id;
    let ext = if os == ToolOs::Windows { ".exe" } else { "" };
    format!("./{executable_name}{ext}")
}

/// Returns the staging base directory for one provision action within the tool
/// cache root.
#[cfg(feature = "tool-presets")]
#[must_use]
#[allow(dead_code)]
pub(crate) fn provision_staging_base_dir(tools_cache_root: &Path) -> PathBuf {
    tools_cache_root.join("staging")
}

/// Builds content map entries from extracted files in the staging directory.
#[cfg(feature = "tool-presets")]
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

// ---------------------------------------------------------------------------
// Archive extraction helpers
// ---------------------------------------------------------------------------

/// Extracts archive bytes to the given directory based on archive format.
///
/// # Errors
///
/// Returns [`ConductorError`] when extraction fails.
#[cfg(feature = "tool-presets")]
pub(crate) fn extract_archive(
    bytes: &[u8],
    format: &str,
    target_dir: &Path,
) -> Result<(), ConductorError> {
    match format {
        ARCHIVE_ZIP => extract_zip(bytes, target_dir),
        ARCHIVE_TAR_GZ => extract_tar_gz(bytes, target_dir),
        ARCHIVE_TAR_XZ => extract_tar_xz(bytes, target_dir),
        ARCHIVE_BINARY => extract_binary(bytes, target_dir),
        other => Err(ConductorError::Workflow(format!("unsupported archive format: {other}"))),
    }
}

#[cfg(feature = "tool-presets")]
fn extract_zip(bytes: &[u8], target_dir: &Path) -> Result<(), ConductorError> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .map_err(|e| ConductorError::Workflow(format!("ZIP open error: {e}")))?;

    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .map_err(|e| ConductorError::Workflow(format!("ZIP entry error: {e}")))?;
        let out_path = target_dir.join(file.name());
        if file.name().ends_with('/') {
            std::fs::create_dir_all(&out_path)
                .map_err(|e| ConductorError::io("create directory", &out_path, e))?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| ConductorError::io("create directory", parent, e))?;
            }
            let mut out = std::fs::File::create(&out_path)
                .map_err(|e| ConductorError::io("create file", &out_path, e))?;
            std::io::copy(&mut file, &mut out)
                .map_err(|e| ConductorError::io("write file", &out_path, e))?;
        }
    }
    Ok(())
}

#[cfg(feature = "tool-presets")]
fn extract_tar_gz(bytes: &[u8], target_dir: &Path) -> Result<(), ConductorError> {
    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(target_dir).map_err(|e| ConductorError::io("extract tar.gz", target_dir, e))
}

#[cfg(feature = "tool-presets")]
fn extract_tar_xz(bytes: &[u8], target_dir: &Path) -> Result<(), ConductorError> {
    let decoder = xz2::read::XzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(target_dir).map_err(|e| ConductorError::io("extract tar.xz", target_dir, e))
}

#[cfg(feature = "tool-presets")]
fn extract_binary(bytes: &[u8], target_dir: &Path) -> Result<(), ConductorError> {
    let exe_name = if cfg!(target_os = "windows") { "tool.exe" } else { "tool" };
    let out_path = target_dir.join(exe_name);
    std::fs::write(&out_path, bytes)
        .map_err(|e| ConductorError::io("write binary payload", &out_path, e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| ConductorError::io("set permissions", &out_path, e))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Materialize helpers
// ---------------------------------------------------------------------------

/// Builds the immutable tool-id suffix from resolved identity.
#[cfg(feature = "tool-presets")]
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

/// Result of materializing one tool payload.
#[cfg(feature = "tool-presets")]
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

/// Materializes a resolved download plan into provisioned payload under the
/// given tool cache root.
///
/// Returns the primary executable path and content map entries.
///
/// # Errors
///
/// Returns [`ConductorError`] when download or extraction fails.
#[cfg(feature = "tool-presets")]
#[allow(dead_code)]
pub(crate) async fn materialize_download_plan(
    plan: &ResolvedDownloadPlan,
    entry: &ToolCatalogEntry,
    tools_cache_root: &Path,
    cache: &ToolDownloadCache,
) -> Result<ProvisionedPayload, ConductorError> {
    let host_os = current_tool_os();

    if plan.internal_launcher {
        return Ok(materialize_internal_launcher(entry, tools_cache_root));
    }

    let action = plan.per_os_actions.get(&host_os).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "no download action for host OS {:?} in plan for {}",
            host_os, entry.id
        ))
    })?;

    let staging_dir = provision_staging_base_dir(tools_cache_root);
    std::fs::create_dir_all(&staging_dir)
        .map_err(|e| ConductorError::io("create staging dir", &staging_dir, e))?;

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
        std::fs::remove_dir_all(&extract_dir).ok();
    }
    std::fs::create_dir_all(&extract_dir)
        .map_err(|e| ConductorError::io("create extract dir", &extract_dir, e))?;

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

/// Materializes the internal launcher shim for tools that ship with mediapm
/// itself (e.g. media-tagger).
#[cfg(feature = "tool-presets")]
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

#[cfg(test)]
#[cfg(feature = "tool-presets")]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::tools::catalog::PlatformValue;
    use crate::tools::catalog::current_tool_os;

    // ── Test fixture helpers ──────────────────────────────────────────

    fn entry_all_platforms() -> ToolCatalogEntry {
        ToolCatalogEntry {
            id: "test-tool",
            description: "Test tool with all three platforms",
            homepage: "https://example.com",
            latest: "1.0.0",
            platforms: vec![
                (
                    ToolOs::Linux,
                    vec![PlatformValue {
                        url: "https://example.com/linux.zip",
                        arch: "x86_64",
                        checksum_sha256: None,
                        archive_format: ARCHIVE_ZIP,
                    }],
                ),
                (
                    ToolOs::Macos,
                    vec![PlatformValue {
                        url: "https://example.com/macos.tar.gz",
                        arch: "x86_64",
                        checksum_sha256: None,
                        archive_format: ARCHIVE_TAR_GZ,
                    }],
                ),
                (
                    ToolOs::Windows,
                    vec![PlatformValue {
                        url: "https://example.com/windows.zip",
                        arch: "x86_64",
                        checksum_sha256: None,
                        archive_format: ARCHIVE_ZIP,
                    }],
                ),
            ],
            archive_format: ARCHIVE_BINARY,
        }
    }

    fn entry_internal_launcher() -> ToolCatalogEntry {
        ToolCatalogEntry {
            id: "internal-test",
            description: "Internal launcher entry",
            homepage: "https://example.com",
            latest: "1.0.0",
            platforms: vec![],
            archive_format: ARCHIVE_BINARY,
        }
    }

    fn entry_partial() -> ToolCatalogEntry {
        ToolCatalogEntry {
            id: "partial-tool",
            description: "Tool with linux+macos only (no windows)",
            homepage: "https://example.com",
            latest: "2.0.0",
            platforms: vec![
                (
                    ToolOs::Linux,
                    vec![PlatformValue {
                        url: "https://example.com/linux.tar.xz",
                        arch: "x86_64",
                        checksum_sha256: None,
                        archive_format: ARCHIVE_TAR_XZ,
                    }],
                ),
                (
                    ToolOs::Macos,
                    vec![PlatformValue {
                        url: "https://example.com/macos.tar.xz",
                        arch: "x86_64",
                        checksum_sha256: None,
                        archive_format: ARCHIVE_TAR_XZ,
                    }],
                ),
            ],
            archive_format: ARCHIVE_TAR_XZ,
        }
    }

    async fn create_cache() -> (TempDir, ToolDownloadCache) {
        let dir = TempDir::new().expect("tempdir");
        let cache = ToolDownloadCache::open(dir.path()).await.expect("open cache");
        (dir, cache)
    }

    // ── resolve_download_plan ─────────────────────────────────────────

    #[tokio::test]
    async fn plan_includes_all_platforms() {
        let (_dir, cache) = create_cache().await;
        let plan = resolve_download_plan(&entry_all_platforms(), &cache).await.unwrap();

        assert!(plan.per_os_actions.contains_key(&ToolOs::Linux));
        assert!(plan.per_os_actions.contains_key(&ToolOs::Macos));
        assert!(plan.per_os_actions.contains_key(&ToolOs::Windows));
        assert_eq!(plan.per_os_actions.len(), 3);
        assert!(!plan.internal_launcher);
    }

    #[tokio::test]
    async fn plan_includes_host_os() {
        let (_dir, cache) = create_cache().await;
        let plan = resolve_download_plan(&entry_all_platforms(), &cache).await.unwrap();

        assert!(
            plan.per_os_actions.contains_key(&current_tool_os()),
            "host OS should be in the plan",
        );
    }

    #[tokio::test]
    async fn plan_respects_per_platform_archive_format_override() {
        let (_dir, cache) = create_cache().await;
        let plan = resolve_download_plan(&entry_all_platforms(), &cache).await.unwrap();

        assert_eq!(plan.per_os_actions[&ToolOs::Linux].archive_format, ARCHIVE_ZIP);
        assert_eq!(plan.per_os_actions[&ToolOs::Macos].archive_format, ARCHIVE_TAR_GZ);
        assert_eq!(plan.per_os_actions[&ToolOs::Windows].archive_format, ARCHIVE_ZIP);
    }

    #[tokio::test]
    async fn plan_internal_launcher_is_empty() {
        let (_dir, cache) = create_cache().await;
        let plan = resolve_download_plan(&entry_internal_launcher(), &cache).await.unwrap();

        assert!(plan.per_os_actions.is_empty());
        assert!(plan.internal_launcher);
        assert!(plan.warnings.is_empty());
    }

    #[tokio::test]
    async fn plan_preserves_urls() {
        let (_dir, cache) = create_cache().await;
        let plan = resolve_download_plan(&entry_all_platforms(), &cache).await.unwrap();

        assert_eq!(plan.per_os_actions[&ToolOs::Linux].urls, vec!["https://example.com/linux.zip"],);
        assert_eq!(
            plan.per_os_actions[&ToolOs::Macos].urls,
            vec!["https://example.com/macos.tar.gz"],
        );
        assert_eq!(
            plan.per_os_actions[&ToolOs::Windows].urls,
            vec!["https://example.com/windows.zip"],
        );
    }

    #[tokio::test]
    async fn plan_missing_host_os_produces_warning() {
        let (_dir, cache) = create_cache().await;
        let plan = resolve_download_plan(&entry_partial(), &cache).await.unwrap();

        assert_eq!(plan.per_os_actions.len(), 2);
        let host = current_tool_os();
        if host == ToolOs::Windows {
            assert!(!plan.warnings.is_empty(), "windows is missing → should warn");
        }
    }

    // ── extract_archive format-mismatch errors ────────────────────────

    #[test]
    fn extract_zip_rejects_tar_xz_bytes() {
        let dir = TempDir::new().expect("tempdir");
        let bad_bytes = b"this is not a zip, it looks more like tar.xz junk\x00\x01\x02";
        let result = extract_archive(bad_bytes, ARCHIVE_ZIP, dir.path());
        assert!(result.is_err(), "tar.xz bytes should fail ZIP extraction");
    }

    #[test]
    fn extract_zip_rejects_empty_bytes() {
        let dir = TempDir::new().expect("tempdir");
        let result = extract_archive(b"", ARCHIVE_ZIP, dir.path());
        assert!(result.is_err(), "empty bytes should fail ZIP extraction");
    }

    #[test]
    fn extract_tar_gz_rejects_zip_bytes() {
        let dir = TempDir::new().expect("tempdir");
        // Minimal ZIP bytes (empty zip archive)
        let empty_zip =
            b"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let result = extract_archive(empty_zip, ARCHIVE_TAR_GZ, dir.path());
        assert!(result.is_err(), "ZIP bytes should fail tar.gz extraction");
    }

    #[test]
    fn extract_tar_xz_rejects_zip_bytes() {
        let dir = TempDir::new().expect("tempdir");
        let empty_zip =
            b"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let result = extract_archive(empty_zip, ARCHIVE_TAR_XZ, dir.path());
        assert!(result.is_err(), "ZIP bytes should fail tar.xz extraction");
    }

    #[test]
    fn extract_binary_writes_bytes_as_file() {
        let dir = TempDir::new().expect("tempdir");
        let payload = b"#!/bin/sh\necho hello\n";
        extract_archive(payload, ARCHIVE_BINARY, dir.path()).expect("binary extraction");
        let file_path = dir.path().join("tool");
        assert!(file_path.exists(), "binary payload should be written as a file");
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "#!/bin/sh\necho hello\n");
    }

    #[test]
    fn extract_unknown_format_returns_error() {
        let dir = TempDir::new().expect("tempdir");
        let result = extract_archive(b"some bytes", "nonexistent-format", dir.path());
        assert!(result.is_err(), "unknown format should return error");
    }
}
