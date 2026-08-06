//! Managed-tool provider source definitions.
//!
//! Each per-tool module defines a `sources()` function returning
//! [`ResolvedToolFetch`] describing where and how to fetch the tool
//! binary for each target platform.
//!
//! The dispatcher [`resolve_tool_fetch`] routes tool names to the
//! appropriate per-tool module.

pub(crate) mod deno;
pub(crate) mod ffmpeg;
pub(crate) mod import;
pub(crate) mod media_tagger;
pub(crate) mod rsgain;
pub(crate) mod sd;
pub(crate) mod yt_dlp;

use std::sync::atomic::{AtomicU32, Ordering};

use mediapm_conductor::cache::Cache;
use mediapm_conductor::tools::provider::{ResolvedToolFetch, SourceProducer};

#[cfg(test)]
use crate::tools::downloader::ToolDownloadCache;

/// Whether to force re-resolve or use cached metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecheckPolicy {
    /// Force re-resolve — ignore cached metadata, always fetch from source.
    ForceReResolve,
    /// Use cached metadata if available.
    #[default]
    UseCached,
}

/// Wraps a [`Cache`] reference and domain, counting every `lookup_bytes` call.
///
/// This enables `resolve_tool_fetch` to auto-derive `metadata_fetch_count` from
/// the actual number of metadata cache lookups performed, rather than requiring
/// a manually maintained per-tool count that drifts when resolvers are added or
/// removed.
pub(crate) struct MetadataCacheTracker<'a> {
    inner: &'a Cache,
    domain: String,
    count: AtomicU32,
    policy: RecheckPolicy,
}

impl<'a> MetadataCacheTracker<'a> {
    /// Creates a new tracker wrapping the given cache and domain.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn new(inner: &'a Cache, domain: &str) -> Self {
        Self {
            inner,
            domain: domain.to_string(),
            count: AtomicU32::new(0),
            policy: RecheckPolicy::UseCached,
        }
    }

    /// Creates a new tracker with the given recheck policy.
    #[must_use]
    pub(crate) fn with_policy(inner: &'a Cache, domain: &str, policy: RecheckPolicy) -> Self {
        Self { inner, domain: domain.to_string(), count: AtomicU32::new(0), policy }
    }

    /// Delegates to [`Cache::lookup_bytes`] for the tracked domain and
    /// increments the internal lookup counter.
    ///
    /// When `policy` is [`RecheckPolicy::ForceReResolve`], always returns
    /// `None` (skipping cache) but still counts the lookup for telemetry.
    pub(crate) async fn lookup_bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.count.fetch_add(1, Ordering::Relaxed);
        if self.policy == RecheckPolicy::ForceReResolve {
            return None;
        }
        self.inner.lookup_bytes(&self.domain, key).await
    }

    /// Delegates to [`Cache::store_bytes`] for the tracked domain (not
    /// counted).
    pub(crate) async fn store_bytes(&self, key: &str, payload: &[u8]) {
        self.inner.store_bytes(&self.domain, key, payload).await;
    }

    /// Returns the number of `lookup_bytes` calls made so far.
    #[must_use]
    pub(crate) fn lookup_count(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }
}

/// Resolved tool metadata produced by [`resolve_tool_fetch`].
///
/// Carries the human-readable and canonical versions, metadata-cache
/// telemetry, and the three resolved provenance fields
/// (`resolved_tag` / `resolved_version` / `resolved_vcs_hash`) as
/// `Option<String>` — `None` (JSON `null`) when the provider has no value;
/// empty strings never occur. Any `None` field must carry a documented
/// why-empty reason in the provider dispatch arm, the provider module doc,
/// and `provider-dispatch.instructions.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedToolMetadata {
    /// Human-readable version string (informational only — no semantic use in
    /// state logic; skip-if-up-to-date uses `canonical_version`).
    pub(crate) human_readable_version: String,
    /// Canonical version identifier for skip-if-up-to-date logic. Always
    /// populated.
    pub(crate) canonical_version: String,
    /// `true` when all version/tag lookups were served from the metadata cache.
    pub(crate) metadata_cached: bool,
    /// Number of individual version/tag lookups performed, auto-derived from
    /// [`MetadataCacheTracker::lookup_bytes`] calls (e.g., ffmpeg = 2, all
    /// others = 1).
    pub(crate) metadata_fetch_count: u32,
    /// Resolved upstream git tag, or `None` when the provider has no tag.
    pub(crate) resolved_tag: Option<String>,
    /// Resolved upstream version string, or `None` when the provider has no
    /// version.
    pub(crate) resolved_version: Option<String>,
    /// Resolved upstream VCS commit hash, or `None` when the provider has no
    /// hash.
    pub(crate) resolved_vcs_hash: Option<String>,
}

/// Resolves the latest GitHub release tag and its commit hash for `owner/repo`.
///
/// Returns `(tag, commit_hash, metadata_cached)` where `tag` is the version
/// string for URL substitution and `commit_hash` is the git commit SHA for
/// canonical version tracking. `metadata_cached` is `true` when the result
/// was served from the metadata cache.
///
/// Uses the metadata cache to avoid repeated GitHub API calls. The cache key
/// is the GitHub API endpoint URL itself, stored as `"{tag}\n{commit_hash}"`.
/// The caller must NOT call `touch()` on the metadata cache — the 1-day TTL is
/// anchored to creation time, not last use.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from [`crate::http_client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
///
/// # Errors
///
/// Returns [`mediapm_conductor::ConductorError`] when the HTTP request or
/// cache I/O fails.
pub(crate) async fn resolve_latest_github_tag(
    owner: &str,
    repo: &str,
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
) -> Result<(String, String, bool), mediapm_conductor::ConductorError> {
    let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases/latest");

    // Try metadata cache first. If the cached entry is non-UTF-8 or malformed,
    // fall through to re-fetch.
    // Try cache first. A fully valid cached entry (UTF-8, non-empty
    // tag+hash pair) short-circuits the HTTP fetch below.
    if let Some(cache) = metadata_cache
        && let Some(bytes) = cache.lookup_bytes(&api_url).await
        && let Ok(s) = String::from_utf8(bytes.clone())
        && let Some((tag, hash)) = s.split_once('\n')
        && !tag.is_empty()
        && !hash.is_empty()
    {
        return Ok((tag.to_string(), hash.to_string(), true));
    }
    // Invalid cache entry — fall through to re-fetch.

    // Fetch from GitHub API.
    let http_client = mediapm_conductor::http::client::shared_http_client().map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!("HTTP client unavailable: {e}"))
    })?;

    let response = http_client.get(&api_url).send().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!("GitHub API request failed: {e}"))
    })?;

    let release: serde_json::Value = response.json().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!(
            "GitHub API response parse failed: {e}"
        ))
    })?;

    let tag = release["tag_name"].as_str().ok_or_else(|| {
        mediapm_conductor::ConductorError::Workflow(
            "GitHub API response missing tag_name".to_string(),
        )
    })?;
    let tag = tag.to_string();

    // Resolve git commit hash from the tag ref.
    let ref_url = format!("https://api.github.com/repos/{owner}/{repo}/git/refs/tags/{tag}");
    let ref_response = http_client.get(&ref_url).send().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!("GitHub API ref request failed: {e}"))
    })?;
    let ref_value: serde_json::Value = ref_response.json().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!(
            "GitHub API ref response parse failed: {e}"
        ))
    })?;

    let commit_sha = match ref_value["object"]["type"].as_str() {
        Some("commit") => ref_value["object"]["sha"].as_str().unwrap_or(&tag).to_string(),
        Some("tag") => {
            // Annotated tag — dereference one more level.
            let tag_sha = ref_value["object"]["sha"].as_str().unwrap_or(&tag);
            let tag_url = format!("https://api.github.com/repos/{owner}/{repo}/git/tags/{tag_sha}");
            let tag_response = http_client.get(&tag_url).send().await.map_err(|e| {
                mediapm_conductor::ConductorError::Workflow(format!(
                    "GitHub API annotated tag request failed: {e}"
                ))
            })?;
            let tag_value: serde_json::Value = tag_response.json().await.map_err(|e| {
                mediapm_conductor::ConductorError::Workflow(format!(
                    "GitHub API annotated tag response parse failed: {e}"
                ))
            })?;
            tag_value["object"]["sha"].as_str().unwrap_or(tag_sha).to_string()
        }
        _ => tag.clone(), // fallback: use tag as-is
    };

    // Store in metadata cache. Do NOT call touch() — TTL is creation-time-based.
    if let Some(cache) = metadata_cache {
        cache.store_bytes(&api_url, format!("{tag}\n{commit_sha}").as_bytes()).await;
    }

    Ok((tag, commit_sha, false))
}

/// Resolves the latest `autobuild-*` tag for `owner/repo` by listing
/// recent releases.
///
/// Some GitHub repos (e.g. BtbN/FFmpeg-Builds) return `"tag_name":"latest"`
/// from the `/releases/latest` endpoint. This function uses the releases list
/// endpoint (`/releases?per_page=10`) and picks the first non-placeholder release
/// matching the `autobuild-*` pattern.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from [`crate::http_client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
///
/// # Errors
///
/// Returns [`mediapm_conductor::ConductorError`] when the HTTP request, cache
/// I/O, or tag extraction fails.
pub(crate) async fn resolve_latest_autobuild_tag(
    owner: &str,
    repo: &str,
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
) -> Result<(String, bool), mediapm_conductor::ConductorError> {
    let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases?per_page=10");

    // Try cache first.
    if let Some(cache) = metadata_cache
        && let Some(bytes) = cache.lookup_bytes(&api_url).await
        && let Ok(tag) = String::from_utf8(bytes.clone())
        && tag.starts_with("autobuild-")
    {
        return Ok((tag, true));
    }
    // Invalid/non-UTF-8 cache entry — fall through.

    // Fetch releases list from GitHub API.
    let http_client = mediapm_conductor::http::client::shared_http_client().map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!("HTTP client unavailable: {e}"))
    })?;
    let response = http_client.get(&api_url).send().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!(
            "GitHub API releases list request failed: {e}"
        ))
    })?;
    let releases: serde_json::Value = response.json().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!(
            "GitHub API releases list response parse failed: {e}"
        ))
    })?;
    let releases = releases.as_array().ok_or_else(|| {
        mediapm_conductor::ConductorError::Workflow(
            "GitHub API releases list response is not an array".to_string(),
        )
    })?;

    // Find first non-placeholder autobuild tag.
    let tag = releases
        .iter()
        .filter_map(|r| r["tag_name"].as_str())
        .find(|t| t.starts_with("autobuild-"))
        .ok_or_else(|| {
            mediapm_conductor::ConductorError::Workflow(format!(
                "no autobuild-* tag found for {owner}/{repo}"
            ))
        })?;
    let tag = tag.to_string();

    // Cache it.
    if let Some(cache) = metadata_cache {
        cache.store_bytes(&api_url, tag.as_bytes()).await;
    }

    Ok((tag, false))
}

/// Resolves source descriptors and canonical version for the named managed tool.
///
/// Returns a tuple of `(ResolvedToolFetch, ResolvedToolMetadata)` where the
/// second element carries:
/// - `human_readable_version`: informational only, no semantic use — version
///   comparison, skip-if-up-to-date, and update decisions all use
///   `canonical_version`. Provider-defined format; no prefix stripping or
///   normalization is performed.
/// - `canonical_version`: canonical version identifier for skip-if-up-to-date
///   logic (always populated — `String`, not `Option<String>`).
/// - `metadata_cached`: `true` when all version/tag lookups were served from
///   the metadata cache.
/// - `metadata_fetch_count`: number of individual version/tag lookups
///   performed, auto-derived by counting [`MetadataCacheTracker::lookup_bytes`]
///   calls (e.g., ffmpeg = 2, all others = 1).
/// - `resolved_tag` / `resolved_version` / `resolved_vcs_hash`: resolved
///   provenance fields, each `Option<String>` — `None` when the provider has
///   no value (empty strings never occur). A `None` field must carry a
///   documented why-empty reason.
///
/// The semantic kind (VCS hash, version, or tag) is fixed at code-writing time
/// per tool.
///
/// When `metadata_cache` is provided, tools with dynamic version resolution
/// (e.g., yt-dlp "latest" tag) use it to cache version/tag lookup results.
/// The consumer must NOT call `touch()` on the metadata cache — its TTL is
/// creation-time-based.
///
/// # Errors
///
/// Returns an error when the tool name is not recognised.
#[expect(
    clippy::too_many_lines,
    reason = "resolve dispatch covers six per-tool providers with distinct URL rewrite and metadata handling"
)]
pub(crate) async fn resolve_tool_fetch(
    tool_name: &str,
    metadata_cache: Option<(&Cache, &str)>,
    recheck_policy: RecheckPolicy,
) -> Result<(ResolvedToolFetch, ResolvedToolMetadata), mediapm_conductor::ConductorError> {
    let tracker = metadata_cache
        .map(|(cache, domain)| MetadataCacheTracker::with_policy(cache, domain, recheck_policy));
    let tracker_ref = tracker.as_ref();

    let (fetch, mut metadata) = match tool_name {
        n if n.eq_ignore_ascii_case("yt-dlp") => {
            let (tag, commit_hash, mc) = yt_dlp::resolve_latest_tag(tracker_ref).await?;
            let mut fetch = yt_dlp::sources();
            for source in &mut fetch.sources {
                if let SourceProducer::Fetch { urls } = &mut source.producer {
                    for url in urls.iter_mut() {
                        *url = url.replace("/latest/download/", &format!("/download/{tag}/"));
                    }
                }
            }
            (
                fetch,
                ResolvedToolMetadata {
                    human_readable_version: tag.clone(),
                    canonical_version: commit_hash.clone(),
                    metadata_cached: mc,
                    metadata_fetch_count: 0,
                    resolved_tag: Some(tag.clone()),
                    resolved_version: Some(tag),
                    resolved_vcs_hash: Some(commit_hash),
                },
            )
        }
        n if n.eq_ignore_ascii_case("ffmpeg") => {
            let (autobuild_tag, btbn_cached) = ffmpeg::resolve_btbn_tag(tracker_ref).await?;
            let (evermeet_version, evermeet_cached) =
                ffmpeg::resolve_evermeet_version(tracker_ref).await?;
            let canonical_version = format!("{autobuild_tag}+evermeet-{evermeet_version}");
            let fetch = ffmpeg::sources();
            (
                fetch,
                ResolvedToolMetadata {
                    human_readable_version: canonical_version.clone(),
                    canonical_version,
                    metadata_cached: btbn_cached || evermeet_cached,
                    metadata_fetch_count: 0,
                    resolved_tag: Some(autobuild_tag),
                    // WHY: the artifact set mixes BtbN autobuilds and
                    // evermeet.cx zips — no single version identifies it;
                    // evermeet's semver identifies only the macOS build.
                    resolved_version: None,
                    // WHY: deref'ing a BtbN build tag yields the build-script
                    // repo commit (not the upstream ffmpeg source commit), and
                    // evermeet.cx zips carry no git provenance. Resolvable != meaningful.
                    resolved_vcs_hash: None,
                },
            )
        }
        n if n.eq_ignore_ascii_case("deno") => {
            let (tag, commit_hash, mc) = deno::resolve_tag(tracker_ref).await?;
            let mut fetch = deno::sources();
            for source in &mut fetch.sources {
                if let SourceProducer::Fetch { urls } = &mut source.producer {
                    for url in urls.iter_mut() {
                        *url = url.replace("/latest/download/", &format!("/download/{tag}/"));
                    }
                }
            }
            (
                fetch,
                ResolvedToolMetadata {
                    human_readable_version: tag.clone(),
                    canonical_version: commit_hash.clone(),
                    metadata_cached: mc,
                    metadata_fetch_count: 0,
                    resolved_tag: Some(tag.clone()),
                    resolved_version: Some(tag),
                    resolved_vcs_hash: Some(commit_hash),
                },
            )
        }
        n if n.eq_ignore_ascii_case("rsgain") => {
            let (tag, commit_hash, mc) = rsgain::resolve_tag(tracker_ref).await?;
            let version = tag.strip_prefix('v').unwrap_or(&tag).to_string();
            let mut fetch = rsgain::sources();
            for source in &mut fetch.sources {
                if let SourceProducer::Fetch { urls } = &mut source.producer {
                    for url in urls.iter_mut() {
                        *url = url
                            .replace("/latest/download/", &format!("/download/{tag}/"))
                            .replace("rsgain-latest", &format!("rsgain-{version}"));
                    }
                }
            }
            (
                fetch,
                ResolvedToolMetadata {
                    human_readable_version: tag.clone(),
                    canonical_version: commit_hash.clone(),
                    metadata_cached: mc,
                    metadata_fetch_count: 0,
                    resolved_tag: Some(tag.clone()),
                    resolved_version: Some(tag),
                    resolved_vcs_hash: Some(commit_hash),
                },
            )
        }
        n if n.eq_ignore_ascii_case("media-tagger") => {
            let canonical = crate::global::MEDIAPM_GIT_HASH.to_string();
            let hr = format!("{}+{}", env!("CARGO_PKG_VERSION"), canonical);
            (
                media_tagger::sources(),
                ResolvedToolMetadata {
                    human_readable_version: hr,
                    canonical_version: canonical.clone(),
                    metadata_cached: false,
                    metadata_fetch_count: 0,
                    // WHY: media-tagger is a builtin launcher shipped inside
                    // mediapm; there is no upstream tagged release to resolve.
                    resolved_tag: None,
                    resolved_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    resolved_vcs_hash: Some(canonical),
                },
            )
        }
        n if n.eq_ignore_ascii_case("import") => {
            let canonical = crate::global::MEDIAPM_GIT_HASH.to_string();
            let hr = format!("{}+{}", env!("CARGO_PKG_VERSION"), canonical);
            (
                import::sources(),
                ResolvedToolMetadata {
                    human_readable_version: hr,
                    canonical_version: canonical.clone(),
                    metadata_cached: false,
                    metadata_fetch_count: 0,
                    // WHY: import is a conductor builtin launcher; no upstream release to resolve.
                    resolved_tag: None,
                    resolved_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    resolved_vcs_hash: Some(canonical),
                },
            )
        }
        n if n.eq_ignore_ascii_case("sd") => {
            let (tag, commit_hash, mc) = sd::resolve_tag(tracker_ref).await?;
            let mut fetch = sd::sources();
            for source in &mut fetch.sources {
                if let SourceProducer::Fetch { urls } = &mut source.producer {
                    for url in urls.iter_mut() {
                        *url = url
                            .replace("/latest/download/", &format!("/download/{tag}/"))
                            .replace("sd-latest", &format!("sd-{tag}"));
                    }
                }
            }
            (
                fetch,
                ResolvedToolMetadata {
                    human_readable_version: tag.clone(),
                    canonical_version: commit_hash.clone(),
                    metadata_cached: mc,
                    metadata_fetch_count: 0,
                    resolved_tag: Some(tag.clone()),
                    resolved_version: Some(tag),
                    resolved_vcs_hash: Some(commit_hash),
                },
            )
        }
        _ => {
            return Err(mediapm_conductor::ConductorError::Workflow(format!(
                "tool {tool_name}: no provider registered for resolution"
            )));
        }
    };
    metadata.metadata_fetch_count = tracker_ref.map_or(0, MetadataCacheTracker::lookup_count);
    Ok((fetch, metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "integration-style test seeds six tools' metadata caches and asserts each route"
    )]
    async fn resolve_tool_fetch_routes_all_tools() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache for network-backed tools to avoid real API calls.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag (yt-dlp, deno, rsgain, sd)
        // and plain "{tag}" for resolve_latest_autobuild_tag (ffmpeg BtbN) or evermeet version.
        for (api_url, tag, hash) in &[
            (
                "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest",
                "2025.07.15",
                "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
            ),
            (
                "https://api.github.com/repos/denoland/deno/releases/latest",
                "v2.2.12",
                "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1",
            ),
            (
                "https://api.github.com/repos/complexlogic/rsgain/releases/latest",
                "v3.7",
                "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
            ),
            (
                "https://api.github.com/repos/chmln/sd/releases/latest",
                "v1.1.0",
                "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3",
            ),
        ] {
            cache.store_bytes("default", api_url, format!("{tag}\n{hash}").as_bytes()).await;
        }
        // ffmpeg: autobuild tag + evermeet version
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache.store_bytes("default", "https://evermeet.cx/ffmpeg/getrelease/zip", b"8.1.2").await;

        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "media-tagger", "import", "sd"] {
            let result =
                resolve_tool_fetch(name, Some((&*cache, "default")), RecheckPolicy::default())
                    .await;
            assert!(result.is_ok(), "tool {name}: resolve should succeed");
            let (fetch, metadata) = result.unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(fetch.tool_id, *name, "tool_id should match input name");
            match *name {
                "yt-dlp" => {
                    assert_eq!(canonical, "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0");
                    assert_eq!(metadata.resolved_tag.as_deref(), Some("2025.07.15"));
                    assert_eq!(metadata.resolved_version.as_deref(), Some("2025.07.15"));
                    assert_eq!(
                        metadata.resolved_vcs_hash.as_deref(),
                        Some("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0")
                    );
                }
                "ffmpeg" => {
                    assert_eq!(canonical, "autobuild-2025-07-15-12-00+evermeet-8.1.2");
                    assert_eq!(
                        metadata.resolved_tag.as_deref(),
                        Some("autobuild-2025-07-15-12-00")
                    );
                    // WHY: no single version identifies the mixed BtbN+evermeet
                    // artifact set, and the BtbN build-repo hash is not the
                    // upstream ffmpeg source hash.
                    assert_eq!(metadata.resolved_version, None);
                    assert_eq!(metadata.resolved_vcs_hash, None);
                }
                "deno" => {
                    assert_eq!(canonical, "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1");
                    assert_eq!(metadata.resolved_tag.as_deref(), Some("v2.2.12"));
                    assert_eq!(metadata.resolved_version.as_deref(), Some("v2.2.12"));
                    assert_eq!(
                        metadata.resolved_vcs_hash.as_deref(),
                        Some("b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1")
                    );
                }
                "rsgain" => {
                    assert_eq!(canonical, "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2");
                    assert_eq!(metadata.resolved_tag.as_deref(), Some("v3.7"));
                    assert_eq!(metadata.resolved_version.as_deref(), Some("v3.7"));
                    assert_eq!(
                        metadata.resolved_vcs_hash.as_deref(),
                        Some("c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2")
                    );
                }
                "sd" => {
                    assert_eq!(canonical, "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3");
                    assert_eq!(metadata.resolved_tag.as_deref(), Some("v1.1.0"));
                    assert_eq!(metadata.resolved_version.as_deref(), Some("v1.1.0"));
                    assert_eq!(
                        metadata.resolved_vcs_hash.as_deref(),
                        Some("d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3")
                    );
                }
                "media-tagger" | "import" => {
                    assert!(!canonical.is_empty(), "{name} canonical_version should not be empty");
                    assert_eq!(metadata.resolved_tag, None);
                    assert_eq!(
                        metadata.resolved_version.as_deref(),
                        Some(env!("CARGO_PKG_VERSION"))
                    );
                    assert_eq!(
                        metadata.resolved_vcs_hash.as_deref(),
                        Some(crate::global::MEDIAPM_GIT_HASH)
                    );
                }
                _ => unreachable!(),
            }
            if *name == "media-tagger" || *name == "import" {
                // media-tagger is a builtin launcher with 3 GenerateLauncher sources.
                assert_eq!(fetch.sources.len(), 3, "tool {name}: should have 3 sources");
                for source in &fetch.sources {
                    assert!(
                        matches!(source.producer, SourceProducer::GenerateLauncher { .. }),
                        "tool {name}: source should be GenerateLauncher"
                    );
                }
            } else {
                assert!(!fetch.sources.is_empty(), "tool {name}: should have at least one source");
            }
        }
    }

    #[tokio::test]
    async fn resolve_tool_fetch_rejects_unknown() {
        let result = resolve_tool_fetch("no-such-tool", None, RecheckPolicy::default()).await;
        assert!(result.is_err(), "unknown tool should return error");
    }

    /// Seeded metadata cache for a single tool's resolved fields, avoiding real
    /// API calls.
    async fn seed_metadata_cache(tool_name: &str) -> ToolDownloadCache {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();
        let (urls, values): (Vec<&str>, Vec<&[u8]>) = match tool_name {
            "ffmpeg" => (
                vec![
                    "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                    "https://evermeet.cx/ffmpeg/getrelease/zip",
                ],
                vec![b"autobuild-2025-07-15-12-00", b"8.1.2"],
            ),
            _ => (vec![], vec![]),
        };
        for (url, value) in urls.into_iter().zip(values) {
            cache.store_bytes("default", url, value).await;
        }
        cache
    }

    #[tokio::test]
    async fn resolve_tool_fetch_ffmpeg_leaves_version_and_vcs_hash_none() {
        // Regression: ffmpeg's resolved_version and resolved_vcs_hash must stay
        // None by design (mixed BtbN + evermeet sources; no single version or
        // upstream VCS hash identifies the artifact set). See the why-empty
        // rationale in the ffmpeg provider module doc.
        let cache = seed_metadata_cache("ffmpeg").await;
        let (_, metadata) =
            resolve_tool_fetch("ffmpeg", Some((&*cache, "default")), RecheckPolicy::default())
                .await
                .expect("ffmpeg resolve should succeed");
        assert_eq!(metadata.resolved_tag.as_deref(), Some("autobuild-2025-07-15-12-00"));
        // WHY: no single version identifies the mixed BtbN+evermeet artifact set.
        assert_eq!(metadata.resolved_version, None);
        // WHY: the BtbN build-repo hash is not the upstream ffmpeg source hash.
        assert_eq!(metadata.resolved_vcs_hash, None);
    }

    #[tokio::test]
    async fn resolve_tool_fetch_media_tagger_leaves_tag_none() {
        // Regression: media-tagger's resolved_tag must stay None by design
        // (builtin launcher shipped inside mediapm; no upstream tag exists).
        // resolved_version and resolved_vcs_hash identify the mediapm build.
        let cache = seed_metadata_cache("media-tagger").await;
        let (_, metadata) = resolve_tool_fetch(
            "media-tagger",
            Some((&*cache, "default")),
            RecheckPolicy::default(),
        )
        .await
        .expect("media-tagger resolve should succeed");
        // WHY: media-tagger is a builtin launcher; there is no upstream tag.
        assert_eq!(metadata.resolved_tag, None);
        assert_eq!(metadata.resolved_version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
        assert_eq!(metadata.resolved_vcs_hash.as_deref(), Some(crate::global::MEDIAPM_GIT_HASH));
    }

    #[tokio::test]
    async fn resolve_tool_fetch_each_fetched_tool_has_three_os_entries() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache to avoid real API calls.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag and plain "{tag}" for autobuild/evermeet.
        for (api_url, tag, hash) in &[
            (
                "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest",
                "2025.07.15",
                "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
            ),
            (
                "https://api.github.com/repos/denoland/deno/releases/latest",
                "v2.2.12",
                "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1",
            ),
            (
                "https://api.github.com/repos/complexlogic/rsgain/releases/latest",
                "v3.7",
                "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
            ),
            (
                "https://api.github.com/repos/chmln/sd/releases/latest",
                "v1.1.0",
                "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3",
            ),
        ] {
            cache.store_bytes("default", api_url, format!("{tag}\n{hash}").as_bytes()).await;
        }
        // ffmpeg: autobuild tag + evermeet version
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache.store_bytes("default", "https://evermeet.cx/ffmpeg/getrelease/zip", b"8.1.2").await;

        // media-tagger is an internal launcher — no external sources.
        let expected_oses = ["windows", "linux", "macos"];
        let expected_canonicals: [(&str, &str); 5] = [
            ("ffmpeg", "autobuild-2025-07-15-12-00+evermeet-8.1.2"),
            ("yt-dlp", "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"),
            ("deno", "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1"),
            ("rsgain", "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2"),
            ("sd", "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3"),
        ];
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "sd"] {
            let (fetch, metadata) =
                resolve_tool_fetch(name, Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            let canonical = metadata.canonical_version;
            let expected_canonical = expected_canonicals
                .iter()
                .find(|(n, _)| *n == *name)
                .map(|(_, c)| *c)
                .expect("canonical mapping exists");
            assert_eq!(canonical, expected_canonical, "tool {name}: canonical version mismatch");
            let oses: Vec<&str> = fetch.sources.iter().map(|s| s.os.as_str()).collect();
            for expected_os in &expected_oses {
                assert!(
                    oses.contains(expected_os),
                    "tool {name}: missing source for OS {expected_os}; found OSes: {oses:?}"
                );
            }
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "integration-style test seeds metadata caches and asserts concrete URLs for all tools"
    )]
    async fn resolve_tool_fetch_with_metadata_cache_produces_concrete_urls() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache with tag values for each tool.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag and plain "{tag}" for autobuild/evermeet.
        let test_data: Vec<(&str, &str, &str, &str)> = vec![
            (
                "yt-dlp",
                "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest",
                "2025.07.15",
                "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
            ),
            (
                "deno",
                "https://api.github.com/repos/denoland/deno/releases/latest",
                "v2.2.12",
                "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1",
            ),
            (
                "rsgain",
                "https://api.github.com/repos/complexlogic/rsgain/releases/latest",
                "v3.7",
                "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
            ),
            (
                "sd",
                "https://api.github.com/repos/chmln/sd/releases/latest",
                "v1.1.0",
                "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3",
            ),
        ];

        for (_, api_url, tag, hash) in &test_data {
            cache.store_bytes("default", api_url, format!("{tag}\n{hash}").as_bytes()).await;
        }
        // ffmpeg: autobuild tag + evermeet version
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache.store_bytes("default", "https://evermeet.cx/ffmpeg/getrelease/zip", b"8.1.2").await;

        for (tool_name, _, tag, _hash) in &test_data {
            let (fetch, metadata) =
                resolve_tool_fetch(tool_name, Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(fetch.tool_id, *tool_name, "tool_id should match input name");
            // Canonical version is the git hash for GitHub-sourced tools.
            assert_ne!(
                canonical, *tag,
                "tool {tool_name}: canonical version should NOT be the tag",
            );
            assert!(!fetch.sources.is_empty(), "tool {tool_name}: should have at least one source");
            for source in &fetch.sources {
                if let SourceProducer::Fetch { urls } = &source.producer {
                    for url in urls {
                        // No URL should still contain the /latest/download/ placeholder,
                        // EXCEPT ffmpeg's BtbN URLs which intentionally keep /latest/download/.
                        if *tool_name == "ffmpeg" && url.contains("BtbN") {
                            // ffmpeg BtbN URLs intentionally keep /latest/download/.
                            assert!(
                                url.contains("/latest/download/"),
                                "tool {tool_name}: ffmpeg BtbN URL should keep /latest/download/",
                            );
                        } else {
                            assert!(
                                !url.contains("/latest/download/"),
                                "tool {tool_name}: URL {url} still contains placeholder /latest/download/",
                            );
                        }
                        // For all tools other than ffmpeg, the URL should contain the resolved tag.
                        if *tool_name != "ffmpeg" {
                            assert!(
                                url.contains(tag),
                                "tool {tool_name}: URL {url} does not contain resolved tag '{tag}'",
                            );
                        }
                    }
                }
            }
        }

        // Also verify ffmpeg separately (different cache structure, composite canonical).
        {
            let (fetch, metadata) =
                resolve_tool_fetch("ffmpeg", Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(fetch.tool_id, "ffmpeg");
            assert_eq!(
                canonical, "autobuild-2025-07-15-12-00+evermeet-8.1.2",
                "ffmpeg canonical version mismatch"
            );
            assert!(!fetch.sources.is_empty(), "ffmpeg should have sources");
            // BtbN sources keep /latest/download/; evermeet source unchanged
            for source in &fetch.sources {
                if let SourceProducer::Fetch { urls } = &source.producer {
                    for url in urls {
                        if url.contains("BtbN") {
                            assert!(
                                url.contains("/latest/download/"),
                                "ffmpeg BtbN URL should keep /latest/download/: {url}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn resolve_latest_github_tag_round_trip() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();
        let tracker = MetadataCacheTracker::new(&cache, "default");

        let owner = "testowner";
        let repo = "testrepo";
        let expected_tag = "v1.0.0";
        let expected_hash = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
        let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases/latest");

        // Pre-seed metadata cache with "{tag}\n{hash}" format.
        cache
            .store_bytes("default", &api_url, format!("{expected_tag}\n{expected_hash}").as_bytes())
            .await;

        let (tag, commit_hash, _metadata_cached) =
            resolve_latest_github_tag(owner, repo, Some(&tracker))
                .await
                .expect("resolve_latest_github_tag should succeed with cached data");

        assert_eq!(tag, expected_tag, "cached tag should be returned without HTTP call");
        assert_eq!(commit_hash, expected_hash, "cached hash should be returned without HTTP call");
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "integration-style test resolves and asserts exact URLs for all managed tools"
    )]
    async fn resolve_tool_fetch_exact_urls_after_resolution() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache with known tags/hashes for all tools.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag and plain "{tag}" for autobuild/evermeet.
        for (api_url, tag, hash) in &[
            (
                "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest",
                "2025.07.15",
                "y1y2y3y4y5y6y7y8y9y0y1y2y3y4y5y6y7y8y9y0y1y2y3",
            ),
            (
                "https://api.github.com/repos/denoland/deno/releases/latest",
                "v2.2.12",
                "d1d2d3d4d5d6d7d8d9d0d1d2d3d4d5d6d7d8d9d0d1d2d3",
            ),
            (
                "https://api.github.com/repos/complexlogic/rsgain/releases/latest",
                "v3.7",
                "r1r2r3r4r5r6r7r8r9r0r1r2r3r4r5r6r7r8r9r0r1r2r3",
            ),
            (
                "https://api.github.com/repos/chmln/sd/releases/latest",
                "v1.1.0",
                "s1s2s3s4s5s6s7s8s9s0s1s2s3s4s5s6s7s8s9s0s1s2s3",
            ),
        ] {
            cache.store_bytes("default", api_url, format!("{tag}\n{hash}").as_bytes()).await;
        }
        // ffmpeg: autobuild tag + evermeet version
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache.store_bytes("default", "https://evermeet.cx/ffmpeg/getrelease/zip", b"8.1.2").await;

        // — yt-dlp (tag "2025.07.15", no v-prefix, no filename rewrite) —
        {
            let (fetch, metadata) =
                resolve_tool_fetch("yt-dlp", Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "y1y2y3y4y5y6y7y8y9y0y1y2y3y4y5y6y7y8y9y0y1y2y3",
                "yt-dlp canonical version"
            );
            assert_eq!(fetch.sources.len(), 3, "yt-dlp: expected 3 OS sources");
            // Assert exact URLs per OS source.
            if let SourceProducer::Fetch { urls } = &fetch.sources[0].producer {
                assert_eq!(fetch.sources[0].os, "windows");
                assert_eq!(
                    urls,
                    &["https://github.com/yt-dlp/yt-dlp/releases/download/2025.07.15/yt-dlp.exe"]
                );
            }
            if let SourceProducer::Fetch { urls } = &fetch.sources[1].producer {
                assert_eq!(fetch.sources[1].os, "macos");
                assert_eq!(
                    urls,
                    &["https://github.com/yt-dlp/yt-dlp/releases/download/2025.07.15/yt-dlp_macos"]
                );
            }
            if let SourceProducer::Fetch { urls } = &fetch.sources[2].producer {
                assert_eq!(fetch.sources[2].os, "linux");
                assert_eq!(
                    urls,
                    &["https://github.com/yt-dlp/yt-dlp/releases/download/2025.07.15/yt-dlp_linux"]
                );
            }
        }

        // — ffmpeg (composite canonical, BtbN URLs keep /latest/download/, Evermeet unchanged) —
        {
            let (fetch, metadata) =
                resolve_tool_fetch("ffmpeg", Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "autobuild-2025-07-15-12-00+evermeet-8.1.2",
                "ffmpeg canonical version"
            );
            assert_eq!(fetch.sources.len(), 3, "ffmpeg: expected 3 OS sources");
            // windows: BtbN URLs keep /latest/download/ (no substitution)
            if let SourceProducer::Fetch { urls } = &fetch.sources[0].producer {
                assert_eq!(fetch.sources[0].os, "windows");
                assert_eq!(urls.len(), 2);
                assert!(
                    urls[0].contains("/releases/latest/download/"),
                    "ffmpeg windows primary URL should use /latest/download/"
                );
                assert!(
                    urls[0].contains("ffmpeg-master-latest"),
                    "ffmpeg windows primary URL should use master-latest naming"
                );
            }
            // macos: Evermeet, completely unchanged
            if let SourceProducer::Fetch { urls } = &fetch.sources[1].producer {
                assert_eq!(fetch.sources[1].os, "macos");
                assert_eq!(urls, &["https://evermeet.cx/ffmpeg/getrelease/zip"]);
            }
            // linux: BtbN URLs keep /latest/download/
            if let SourceProducer::Fetch { urls } = &fetch.sources[2].producer {
                assert_eq!(fetch.sources[2].os, "linux");
                assert_eq!(urls.len(), 2);
                assert!(
                    urls[0].contains("/releases/latest/download/"),
                    "ffmpeg linux primary URL should use /latest/download/"
                );
            }
        }

        // — deno (tag "v2.2.12", v-prefixed, no filename rewrite) —
        {
            let (fetch, metadata) =
                resolve_tool_fetch("deno", Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "d1d2d3d4d5d6d7d8d9d0d1d2d3d4d5d6d7d8d9d0d1d2d3",
                "deno canonical version"
            );
            assert_eq!(fetch.sources.len(), 3, "deno: expected 3 OS sources");
            if let SourceProducer::Fetch { urls } = &fetch.sources[0].producer {
                assert_eq!(fetch.sources[0].os, "windows");
                assert!(urls[0].contains("v2.2.12"), "deno windows URL should contain tag");
                assert!(
                    urls[0].ends_with("deno-x86_64-pc-windows-msvc.zip"),
                    "deno windows URL filename mismatch",
                );
            }
            if let SourceProducer::Fetch { urls } = &fetch.sources[1].producer {
                assert_eq!(fetch.sources[1].os, "macos");
                assert!(urls[0].contains("v2.2.12"), "deno macos URL should contain tag");
                assert!(
                    urls[0].ends_with("deno-aarch64-apple-darwin.zip"),
                    "deno macos primary URL filename mismatch",
                );
                assert_eq!(urls.len(), 2, "deno macos should have 2 URLs");
            }
            if let SourceProducer::Fetch { urls } = &fetch.sources[2].producer {
                assert_eq!(fetch.sources[2].os, "linux");
                assert!(urls[0].contains("v2.2.12"), "deno linux URL should contain tag");
                assert_eq!(urls.len(), 2, "deno linux should have 2 URLs");
            }
        }

        // — rsgain (tag "v3.7", path + filename rewrite: rsgain-latest → rsgain-3.7) —
        {
            let (fetch, metadata) =
                resolve_tool_fetch("rsgain", Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "r1r2r3r4r5r6r7r8r9r0r1r2r3r4r5r6r7r8r9r0r1r2r3",
                "rsgain canonical version"
            );
            assert_eq!(fetch.sources.len(), 3, "rsgain: expected 3 OS sources");
            if let SourceProducer::Fetch { urls } = &fetch.sources[0].producer {
                assert_eq!(fetch.sources[0].os, "windows");
                assert_eq!(
                    urls,
                    &[
                        "https://github.com/complexlogic/rsgain/releases/download/v3.7/rsgain-3.7-win64.zip"
                    ]
                );
            }
            if let SourceProducer::Fetch { urls } = &fetch.sources[1].producer {
                assert_eq!(fetch.sources[1].os, "macos");
                assert_eq!(
                    urls,
                    &[
                        "https://github.com/complexlogic/rsgain/releases/download/v3.7/rsgain-3.7-macOS-x86_64.zip"
                    ]
                );
            }
            if let SourceProducer::Fetch { urls } = &fetch.sources[2].producer {
                assert_eq!(fetch.sources[2].os, "linux");
                assert_eq!(
                    urls,
                    &[
                        "https://github.com/complexlogic/rsgain/releases/download/v3.7/rsgain-3.7-Linux.tar.xz"
                    ]
                );
            }
        }

        // — sd (tag "v1.1.0", path + filename rewrite: sd-latest → sd-v1.1.0) —
        {
            let (fetch, metadata) =
                resolve_tool_fetch("sd", Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "s1s2s3s4s5s6s7s8s9s0s1s2s3s4s5s6s7s8s9s0s1s2s3",
                "sd canonical version"
            );
            assert_eq!(fetch.sources.len(), 3, "sd: expected 3 OS sources");
            if let SourceProducer::Fetch { urls } = &fetch.sources[0].producer {
                assert_eq!(fetch.sources[0].os, "windows");
                assert_eq!(
                    urls,
                    &[
                        "https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-x86_64-pc-windows-msvc.zip"
                    ]
                );
            }
            if let SourceProducer::Fetch { urls } = &fetch.sources[1].producer {
                assert_eq!(fetch.sources[1].os, "macos");
                assert_eq!(
                    urls,
                    &[
                        "https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-aarch64-apple-darwin.tar.gz",
                        "https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-x86_64-apple-darwin.tar.gz",
                    ]
                );
            }
            if let SourceProducer::Fetch { urls } = &fetch.sources[2].producer {
                assert_eq!(fetch.sources[2].os, "linux");
                assert_eq!(
                    urls,
                    &[
                        "https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-x86_64-unknown-linux-gnu.tar.gz"
                    ]
                );
            }
        }
    }

    #[tokio::test]
    async fn resolve_tool_fetch_media_tagger_canonical_is_git_hash() {
        let (_, metadata) =
            resolve_tool_fetch("media-tagger", None, RecheckPolicy::default()).await.unwrap();
        let canonical = metadata.canonical_version;
        // MEDIAPM_GIT_HASH is the compile-time constant — it may be empty in some
        // test environments without .git, but it must not panic.
        if !canonical.is_empty() {
            assert!(canonical.len() >= 7, "git hash should be at least 7 chars");
        }
        // WHY: media-tagger is a builtin launcher — no upstream tag; identity is
        // the mediapm build itself.
        assert_eq!(metadata.resolved_tag, None);
        assert_eq!(metadata.resolved_version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
        assert_eq!(metadata.resolved_vcs_hash.as_deref(), Some(crate::global::MEDIAPM_GIT_HASH));
    }

    #[tokio::test]
    async fn resolve_canonical_version_is_deterministic() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = ToolDownloadCache::open(temp_dir.path(), "metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache with known tags and hashes.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag and plain "{tag}" for autobuild/evermeet.
        let seeds: &[(&str, &str)] = &[
            (
                "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest",
                "2025.07.15\na1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
            ),
            (
                "https://api.github.com/repos/denoland/deno/releases/latest",
                "v2.2.12\nb2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1",
            ),
            (
                "https://api.github.com/repos/complexlogic/rsgain/releases/latest",
                "v3.7\nc3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
            ),
            (
                "https://api.github.com/repos/chmln/sd/releases/latest",
                "v1.1.0\nd4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3",
            ),
        ];
        for (url, tag_hash) in seeds {
            cache.store_bytes("default", url, tag_hash.as_bytes()).await;
        }
        // ffmpeg: autobuild tag + evermeet version
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache.store_bytes("default", "https://evermeet.cx/ffmpeg/getrelease/zip", b"8.1.2").await;

        for tool in &["ffmpeg", "yt-dlp", "deno", "rsgain", "sd", "media-tagger"] {
            let (_, metadata1) =
                resolve_tool_fetch(tool, Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            let (_, metadata2) =
                resolve_tool_fetch(tool, Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap();
            assert_eq!(
                metadata1.canonical_version, metadata2.canonical_version,
                "canonical_version for {tool} must be deterministic"
            );
        }
    }

    #[tokio::test]
    async fn all_fetch_providers_have_size_hint_bytes() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache to avoid real API calls.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag and plain "{tag}" for autobuild/evermeet.
        for (api_url, tag, hash) in &[
            (
                "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest",
                "2025.07.15",
                "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
            ),
            (
                "https://api.github.com/repos/denoland/deno/releases/latest",
                "v2.2.12",
                "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1",
            ),
            (
                "https://api.github.com/repos/complexlogic/rsgain/releases/latest",
                "v3.7",
                "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
            ),
        ] {
            cache.store_bytes("default", api_url, format!("{tag}\n{hash}").as_bytes()).await;
        }
        // ffmpeg: autobuild tag + evermeet version
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache.store_bytes("default", "https://evermeet.cx/ffmpeg/getrelease/zip", b"8.1.2").await;

        // All managed tools whose provider type is Fetch must have size_hint_bytes.
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain"] {
            let (fetch, _metadata) =
                resolve_tool_fetch(name, Some((&*cache, "default")), RecheckPolicy::default())
                    .await
                    .unwrap_or_else(|e| panic!("resolve {name}: {e}"));
            for source in &fetch.sources {
                assert!(
                    matches!(source.producer, super::SourceProducer::Fetch { .. }),
                    "{name}: expected Fetch source, got {:?}",
                    source.producer
                );
                assert!(
                    source.size_hint_bytes.is_some(),
                    "{name} source for {}: size_hint_bytes should be Some",
                    source.os
                );
            }
        }

        // media-tagger is a builtin launcher — all sources are GenerateLauncher.
        {
            let (fetch, _metadata) = resolve_tool_fetch(
                "media-tagger",
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap();
            for source in &fetch.sources {
                assert!(
                    matches!(source.producer, super::SourceProducer::GenerateLauncher { .. }),
                    "media-tagger should only have GenerateLauncher sources"
                );
            }
        }
    }

    #[tokio::test]
    async fn resolve_latest_github_tag_fallthrough_on_stale_latest() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();
        let tracker = MetadataCacheTracker::new(&cache, "default");

        // Pre-seed cache with non-UTF-8 bytes — String::from_utf8 conversion
        // fails, triggering fallthrough to the HTTP fetch. Without a real GitHub
        // API endpoint, the HTTP call fails with a transport error.
        let owner = "testowner";
        let repo = "testrepo";
        let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases/latest");
        cache.store_bytes("default", &api_url, b"\xff\xfe\x00latest").await;

        let err = resolve_latest_github_tag(owner, repo, Some(&tracker)).await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("GitHub API request failed")
                || msg.contains("HTTP client unavailable")
                || msg.contains("GitHub API response"),
            "stale-cache fallthrough should produce an HTTP/transport error, not 'placeholder', got: {msg}"
        );
    }

    #[tokio::test]
    async fn resolve_latest_autobuild_tag_returns_cached() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();
        let tracker = MetadataCacheTracker::new(&cache, "default");
        let api_url = "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10";
        cache.store_bytes("default", api_url, b"autobuild-2025-07-15-12-00").await;

        let (tag, _metadata_cached) =
            resolve_latest_autobuild_tag("BtbN", "FFmpeg-Builds", Some(&tracker))
                .await
                .expect("should return cached autobuild tag");
        assert_eq!(tag, "autobuild-2025-07-15-12-00");
    }

    #[tokio::test]
    async fn resolve_evermeet_version_returns_cached() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();
        let tracker = MetadataCacheTracker::new(&cache, "default");
        cache.store_bytes("default", "https://evermeet.cx/ffmpeg/getrelease/zip", b"8.1.2").await;

        let (version, cached) = ffmpeg::resolve_evermeet_version(Some(&tracker))
            .await
            .expect("should return cached evermeet version");
        assert_eq!(version, "8.1.2");
        assert!(cached, "should indicate metadata was cached");
    }

    #[tokio::test]
    async fn resolve_latest_github_tag_returns_tag_and_hash() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();
        let tracker = MetadataCacheTracker::new(&cache, "default");
        let api_url = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";
        // Cache seeded with "{tag}\n{hash}" format.
        cache
            .store_bytes(
                "default",
                api_url,
                b"2025.07.15\na1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
            )
            .await;

        let (tag, commit_hash, _metadata_cached) =
            resolve_latest_github_tag("yt-dlp", "yt-dlp", Some(&tracker))
                .await
                .expect("should return cached (tag, hash)");
        assert_eq!(tag, "2025.07.15");
        assert_eq!(commit_hash, "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0");
    }
}
