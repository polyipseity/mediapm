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
use mediapm_conductor::tools::provider::{
    DownloadedSource, ResolvedToolFetch, SourceProducer, VersionSpec,
};

#[cfg(test)]
use crate::tools::downloader::ToolDownloadCache;
#[cfg(test)]
use mediapm_conductor::tools::provider::host_arch_label;

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
    ///
    /// Compiled only under `cfg(test)`: the production call site uses
    /// [`Self::with_policy`] to pick up a recheck policy, so this
    /// always-`UseCached` shortcut has no non-test caller.
    #[must_use]
    #[cfg(test)]
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

/// Production GitHub REST API base URL.
const GITHUB_API_BASE_DEFAULT: &str = "https://api.github.com";

/// Production base URL for GitHub release-asset downloads.
///
/// A tag-pinned asset is served at
/// `{base}/{owner}/{repo}/releases/download/{tag}/{asset}`.
const GITHUB_DOWNLOAD_BASE_DEFAULT: &str = "https://github.com";

/// Environment variable overriding the GitHub REST API base URL.
///
/// Compiled only into a `test-seams` build: a test points it at a local
/// `wiremock` server to exercise tag-pinned resolution without real network
/// access. A build without the feature has no such variable.
#[cfg(feature = "test-seams")]
const GITHUB_API_BASE_ENV: &str = "MEDIAPM_GITHUB_API_BASE";

/// Environment variable overriding the GitHub release-download base URL.
///
/// Compiled only into a `test-seams` build, same rule as
/// [`GITHUB_API_BASE_ENV`].
#[cfg(feature = "test-seams")]
const GITHUB_DOWNLOAD_BASE_ENV: &str = "MEDIAPM_GITHUB_DOWNLOAD_BASE";

/// Returns the GitHub REST API base URL.
///
/// In a build with the `test-seams` feature the value is the
/// `MEDIAPM_GITHUB_API_BASE` override when set, read per call (never cached in
/// a `OnceLock`) so per-test `wiremock` servers can each take a turn under the
/// process env lock. Every other build returns [`GITHUB_API_BASE_DEFAULT`], so
/// the environment cannot redirect a shipped binary.
fn github_api_base() -> String {
    #[cfg(feature = "test-seams")]
    {
        std::env::var(GITHUB_API_BASE_ENV).unwrap_or_else(|_| GITHUB_API_BASE_DEFAULT.to_string())
    }
    #[cfg(not(feature = "test-seams"))]
    {
        GITHUB_API_BASE_DEFAULT.to_string()
    }
}

/// Returns the base URL GitHub release assets are downloaded from.
///
/// Same feature rule as [`github_api_base`]: the
/// `MEDIAPM_GITHUB_DOWNLOAD_BASE` override is read per call in a `test-seams`
/// build, and every other build returns [`GITHUB_DOWNLOAD_BASE_DEFAULT`].
///
/// Every GitHub payload URL is built from here rather than from a literal
/// host: [`github_release_asset_url`] for a tag-pinned asset, and the per-tool
/// provider modules for the `latest/download` templates a `Latest` spec
/// resolves — [`yt_dlp`], [`deno`], [`rsgain`], [`sd`], and the `BtbN` legs of
/// [`ffmpeg`]. A template that hardcoded its host would compile, resolve, and
/// fetch from the real GitHub even in a `test-seams` build, so
/// `every_latest_payload_url_is_built_from_the_download_seam` walks every
/// managed tool's resolved sources and fails on any URL that still names
/// `github.com`.
pub(crate) fn github_download_base() -> String {
    #[cfg(feature = "test-seams")]
    {
        std::env::var(GITHUB_DOWNLOAD_BASE_ENV)
            .unwrap_or_else(|_| GITHUB_DOWNLOAD_BASE_DEFAULT.to_string())
    }
    #[cfg(not(feature = "test-seams"))]
    {
        GITHUB_DOWNLOAD_BASE_DEFAULT.to_string()
    }
}

/// Resolves the commit SHA a tag points at, dereferencing annotated tags.
///
/// Shared by [`resolve_latest_github_tag`] (whose callers also pin by tag)
/// and [`resolve_github_tag_release`] so both paths dereference identically.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`](mediapm_conductor::http::client::shared_http_client).
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
///
/// # Errors
///
/// Returns [`mediapm_conductor::ConductorError`] when the HTTP request or
/// response parsing fails.
async fn resolve_github_tag_commit_hash(
    http_client: &reqwest::Client,
    owner: &str,
    repo: &str,
    tag: &str,
) -> Result<String, mediapm_conductor::ConductorError> {
    // Resolve git commit hash from the tag ref.
    let ref_url = format!("{}/repos/{owner}/{repo}/git/refs/tags/{tag}", github_api_base());
    let ref_response = http_client.get(&ref_url).send().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!("GitHub API ref request failed: {e}"))
    })?;

    let ref_value: serde_json::Value = ref_response.json().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!(
            "GitHub API ref response parse failed: {e}"
        ))
    })?;

    let commit_sha = match ref_value["object"]["type"].as_str() {
        Some("commit") => ref_value["object"]["sha"].as_str().unwrap_or(tag).to_string(),
        Some("tag") => {
            // Annotated tag — dereference one more level.
            let tag_sha = ref_value["object"]["sha"].as_str().unwrap_or(tag);
            let tag_url = format!("{}/repos/{owner}/{repo}/git/tags/{tag_sha}", github_api_base());
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
        // A ref that is neither a commit nor an annotated tag carries no
        // dereferenceable SHA; the tag name itself is the best identity.
        _ => tag.to_string(),
    };

    Ok(commit_sha)
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
/// Uses the process-wide shared client from
/// [`shared_http_client`](mediapm_conductor::http::client::shared_http_client).
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
    let api_url = format!("{}/repos/{owner}/{repo}/releases/latest", github_api_base());

    // Try cache first. A fully valid cached entry (UTF-8, non-empty
    // tag+hash pair) short-circuits the HTTP fetch below. A malformed or
    // non-UTF-8 entry falls through to re-fetch.
    if let Some(cache) = metadata_cache
        && let Some(bytes) = cache.lookup_bytes(&api_url).await
        && let Ok(s) = String::from_utf8(bytes.clone())
        && let Some((tag, hash)) = s.split_once('\n')
        && !tag.is_empty()
        && !hash.is_empty()
    {
        return Ok((tag.to_string(), hash.to_string(), true));
    }

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

    let commit_sha = resolve_github_tag_commit_hash(http_client, owner, repo, &tag).await?;

    // Store in metadata cache. Do NOT call touch() — TTL is creation-time-based.
    if let Some(cache) = metadata_cache {
        cache.store_bytes(&api_url, format!("{tag}\n{commit_sha}").as_bytes()).await;
    }

    Ok((tag, commit_sha, false))
}

/// Resolves a tag-pinned GitHub release: its commit hash and asset names.
///
/// Returns `(commit_hash, asset_names, metadata_cached)`. The metadata cache
/// key is the release-by-tag API endpoint; the cached value is
/// `"{commit_hash}\n{asset_name}\n{asset_name}..."` — first line the commit
/// hash, remaining lines the release asset names. A non-UTF-8 or
/// hash-without-assets entry falls through to re-fetch. The caller must NOT
/// call `touch()` on the metadata cache — the 1-day TTL is anchored to
/// creation time, not last use.
///
/// The commit hash is resolved through the same tag-ref dereference as
/// [`resolve_latest_github_tag`], so a pinned tool's `canonical_version` has
/// the same semantic kind (resolved commit hash) as an unpinned one.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`](mediapm_conductor::http::client::shared_http_client).
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
///
/// # Errors
///
/// Returns [`mediapm_conductor::ConductorError`] when the HTTP request,
/// response parsing, or tag verification fails.
pub(crate) async fn resolve_github_tag_release(
    owner: &str,
    repo: &str,
    tag: &str,
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
) -> Result<(String, Vec<String>, bool), mediapm_conductor::ConductorError> {
    let api_url = format!("{}/repos/{owner}/{repo}/releases/tags/{tag}", github_api_base());

    // Try cache first. A fully valid entry (UTF-8, non-empty hash plus at
    // least one asset name) short-circuits the HTTP fetch below. An entry
    // whose "hash" is the tag itself is the old non-dereferencing fallback
    // recorded as a commit — it falls through to the re-resolve, which now
    // refuses such a ref loudly.
    if let Some(cache) = metadata_cache
        && let Some(bytes) = cache.lookup_bytes(&api_url).await
        && let Ok(s) = String::from_utf8(bytes)
        && let Some((hash, rest)) = s.split_once('\n')
        && !hash.is_empty()
        && hash != tag
    {
        let assets: Vec<String> =
            rest.split('\n').filter(|n| !n.is_empty()).map(str::to_string).collect();
        if !assets.is_empty() {
            return Ok((hash.to_string(), assets, true));
        }
    }

    // Fetch the release-by-tag document from GitHub API.
    let http_client = mediapm_conductor::http::client::shared_http_client().map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!("HTTP client unavailable: {e}"))
    })?;

    let response = http_client.get(&api_url).send().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!(
            "GitHub API release-by-tag request failed: {e}"
        ))
    })?;

    let release: serde_json::Value = response.json().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!(
            "GitHub API release-by-tag response parse failed: {e}"
        ))
    })?;

    let returned_tag = release["tag_name"].as_str().ok_or_else(|| {
        mediapm_conductor::ConductorError::Workflow(
            "GitHub API release-by-tag response missing tag_name".to_string(),
        )
    })?;
    if returned_tag != tag {
        return Err(mediapm_conductor::ConductorError::Workflow(format!(
            "GitHub API release-by-tag returned tag {returned_tag} for requested {tag}"
        )));
    }
    let assets = release["assets"].as_array().ok_or_else(|| {
        mediapm_conductor::ConductorError::Workflow(
            "GitHub API release-by-tag response missing assets array".to_string(),
        )
    })?;
    let asset_names: Vec<String> =
        assets.iter().filter_map(|a| a["name"].as_str()).map(str::to_string).collect();
    // A release with no named assets is not refused here: the per-tool asset
    // match reports which platform slot found nothing, with tool context.

    let commit_hash = resolve_github_tag_commit_hash(http_client, owner, repo, tag).await?;

    // A pinned canonical version must be the tag's commit, not the tag name
    // echoed back: `resolve_github_tag_commit_hash` falls back to the tag
    // string when the ref carries no dereferenceable SHA, which is tolerable
    // for a `Latest` resolve (comparison re-resolves) but silently wrong for
    // a pin — the stored "commit" would never match a real one, forcing a
    // re-provision on every sync while never proving what was deployed.
    if commit_hash == tag {
        return Err(mediapm_conductor::ConductorError::Workflow(format!(
            "{owner}/{repo}: ref for tag {tag} did not dereference to a commit"
        )));
    }

    // Store in metadata cache. Do NOT call touch() — TTL is creation-time-based.
    if let Some(cache) = metadata_cache {
        let mut payload = commit_hash.clone();
        for name in &asset_names {
            payload.push('\n');
            payload.push_str(name);
        }
        cache.store_bytes(&api_url, payload.as_bytes()).await;
    }

    Ok((commit_hash, asset_names, false))
}

/// Returns the tag the version target pins, if the target pins one.
///
/// [`VersionSpec::Exact`] carrying only `vcs_hash`/`version` returns `None`:
/// a bare hash or version string cannot address a release asset list, so
/// those flavors keep resolving latest and leave comparison to the caller's
/// validation gate.
fn pinned_tag(target: &VersionSpec) -> Option<&str> {
    match target {
        VersionSpec::Exact(fields) => fields.tag.as_deref(),
        VersionSpec::Latest => None,
    }
}

/// Builds the clear resolve-time refusal for a pinned release that carries
/// no asset matching a platform slot's pattern.
fn missing_release_asset_error(
    tool_id: &str,
    tag: &str,
    os: &str,
    pattern: &str,
) -> mediapm_conductor::ConductorError {
    mediapm_conductor::ConductorError::Workflow(format!(
        "tool {tool_id}: release tag {tag} has no asset matching '{pattern}' for os {os}"
    ))
}

/// Builds the download URL for one asset of a tag-pinned GitHub release.
///
/// GitHub serves tag-pinned assets at `releases/download/{tag}/{asset}`,
/// mirroring the `releases/latest/download/{asset}` template the `Latest`
/// path substitutes. The host comes from [`github_download_base`], so a test
/// can point the whole payload path at a local server.
#[must_use]
pub(crate) fn github_release_asset_url(
    owner: &str,
    repo: &str,
    tag: &str,
    asset_name: &str,
) -> String {
    format!("{}/{owner}/{repo}/releases/download/{tag}/{asset_name}", github_download_base())
}

/// Resolves a tag-pinned GitHub tool into its fetch and metadata.
///
/// `sources_for_release` maps the release's asset names onto per-OS sources,
/// refusing (with tool, tag, and pattern context) when a platform slot has
/// no matching asset. All four GitHub tools share this path so the pinned
/// metadata shape — canonical = the tag's commit hash, provenance fields
/// carrying the pinned tag — stays identical across tools.
async fn resolve_pinned_github_tool(
    owner: &str,
    repo: &str,
    tag: &str,
    tracker: Option<&MetadataCacheTracker<'_>>,
    sources_for_release: fn(
        &str,
        &[String],
    ) -> Result<ResolvedToolFetch, mediapm_conductor::ConductorError>,
) -> Result<(ResolvedToolFetch, ResolvedToolMetadata), mediapm_conductor::ConductorError> {
    let (commit_hash, assets, metadata_cached) =
        resolve_github_tag_release(owner, repo, tag, tracker).await?;
    let fetch = sources_for_release(tag, &assets)?;
    Ok((
        fetch,
        ResolvedToolMetadata {
            human_readable_version: tag.to_string(),
            canonical_version: commit_hash.clone(),
            metadata_cached,
            metadata_fetch_count: 0,
            resolved_tag: Some(tag.to_string()),
            resolved_version: Some(tag.to_string()),
            resolved_vcs_hash: Some(commit_hash),
        },
    ))
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
/// Uses the process-wide shared client from
/// [`shared_http_client`](mediapm_conductor::http::client::shared_http_client).
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
    let api_url = format!("{}/repos/{owner}/{repo}/releases?per_page=10", github_api_base());

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

/// Corrects a tool's canonical version from the sources that actually
/// served its fetch (fetch feedback).
///
/// The resolve phase cannot know which chain member will answer: a
/// multi-candidate chain falls through on HTTP failures and `.sha256`
/// mismatches, and the ffmpeg macOS chain's resolve-time probe may not
/// reflect what the fetch phase downloads (a permalink cached yesterday can
/// redirect somewhere new). This function re-derives the canonical version
/// from each source's actual URL and must run after fetch, before the
/// canonical version is recorded.
///
/// Only ffmpeg uses it today: its canonical version is
/// `{btbn_tag}+{macos_leg}`, where the leg is the martin-riedl build id or
/// jellyfin-ffmpeg version parsed from the URL that actually served the
/// macOS bytes. The rewrite replaces the segment after the first `+` of the
/// bare version and preserves any `;dep:ver` composite segments, so a
/// SameStep-dependency composite stays intact. Tools with no rule (or no
/// parsable actual URL) return their canonical version unchanged.
#[must_use]
pub(crate) fn canonical_version_after_fetch(
    tool_name: &str,
    canonical_version: &str,
    entries: &[DownloadedSource],
) -> String {
    if !tool_name.eq_ignore_ascii_case("ffmpeg") {
        return canonical_version.to_string();
    }
    // The macOS chain owns the canonical leg: whichever macOS entry carries
    // an actual URL first is the one whose bytes were imported.
    let Some(macos_entry) = entries.iter().find(|entry| entry.os == "macos") else {
        return canonical_version.to_string();
    };
    let Some(actual_url) = macos_entry.actual_url.as_deref() else {
        return canonical_version.to_string();
    };
    let Some(leg) = ffmpeg::macos_leg_from_url(actual_url) else {
        return canonical_version.to_string();
    };

    // Preserve SameStep dependency segments: `{bare}+{leg};dep:ver;…`.
    let (bare, deps) = match canonical_version.split_once(';') {
        Some((bare, rest)) => (bare, format!(";{rest}")),
        None => (canonical_version, String::new()),
    };
    let prefix = bare.split_once('+').map_or(bare, |(prefix, _old_leg)| prefix);
    format!("{prefix}+{leg}{deps}")
}

/// Resolves source descriptors and canonical version for the named managed tool.
///
/// `version_target` is the already-resolved [`VersionSpec`] for the tool.
/// [`VersionSpec::Exact`] with a `tag` makes the GitHub-release tools
/// (yt-dlp, deno, rsgain, sd) resolve that release through the
/// release-by-tag endpoint and build fetch URLs from its asset list
/// ([`resolve_github_tag_release`]); `Exact` with only `vcs_hash`/`version`
/// keeps resolving latest and leaves comparison to the caller's validation
/// gate. `Exact` of any flavor is refused for ffmpeg (`BtbN` retains no
/// historical builds). [`media-tagger`](crate::tools::provider::media_tagger)
/// and [`import`] are builtin launchers with
/// no upstream release and ignore the target.
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
    version_target: &VersionSpec,
    metadata_cache: Option<(&Cache, &str)>,
    recheck_policy: RecheckPolicy,
) -> Result<(ResolvedToolFetch, ResolvedToolMetadata), mediapm_conductor::ConductorError> {
    let tracker = metadata_cache
        .map(|(cache, domain)| MetadataCacheTracker::with_policy(cache, domain, recheck_policy));
    let tracker_ref = tracker.as_ref();

    let (fetch, mut metadata) = match tool_name {
        n if n.eq_ignore_ascii_case("yt-dlp") => {
            if let Some(tag) = pinned_tag(version_target) {
                resolve_pinned_github_tool(
                    "yt-dlp",
                    "yt-dlp",
                    tag,
                    tracker_ref,
                    yt_dlp::sources_for_release,
                )
                .await?
            } else {
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
        }
        n if n.eq_ignore_ascii_case("ffmpeg") => {
            if matches!(version_target, VersionSpec::Exact(_)) {
                // BtbN replaces its rolling "latest" assets on every build and
                // retains no historical archives; a pin resolved against
                // latest would silently fetch the wrong bytes, so the pin is
                // refused with the reason instead.
                return Err(mediapm_conductor::ConductorError::Workflow(
                    "tool ffmpeg: BtbN does not retain historical builds; pinning ffmpeg's BtbN legs is impossible"
                        .to_string(),
                ));
            }
            let (autobuild_tag, btbn_cached) = ffmpeg::resolve_btbn_tag(tracker_ref).await?;
            // The jellyfin fallback is resolved before the leg because the
            // leg records the version the fallback would serve when the
            // permalink probe cannot identify a build. The leg is advisory
            // either way: `canonical_version_after_fetch` rewrites the
            // composite from the URL that actually served the fetch.
            let (jellyfin, jellyfin_cached) =
                ffmpeg::resolve_jellyfin_macos_fallback(tracker_ref).await;
            let (macos_leg, leg_cached) = ffmpeg::resolve_macos_leg(
                tracker_ref,
                jellyfin.as_ref().map(|fallback| fallback.version.as_str()),
            )
            .await;
            let canonical_version = format!("{autobuild_tag}+{macos_leg}");
            let fetch =
                ffmpeg::sources(version_target, jellyfin.as_ref(), &macos_leg, &autobuild_tag);
            (
                fetch,
                ResolvedToolMetadata {
                    human_readable_version: canonical_version.clone(),
                    canonical_version,
                    metadata_cached: btbn_cached || leg_cached || jellyfin_cached,
                    metadata_fetch_count: 0,
                    resolved_tag: Some(autobuild_tag),
                    // WHY: the artifact set mixes BtbN autobuilds,
                    // martin-riedl snapshots, and jellyfin-ffmpeg builds —
                    // no single version identifies it; the macOS leg
                    // identifies only that leg.
                    resolved_version: None,
                    // WHY: deref'ing a BtbN build tag yields the build-script
                    // repo commit (not the upstream ffmpeg source commit),
                    // and neither martin-riedl build ids nor jellyfin-ffmpeg
                    // versions identify the whole artifact set. Resolvable != meaningful.
                    resolved_vcs_hash: None,
                },
            )
        }
        n if n.eq_ignore_ascii_case("deno") => {
            if let Some(tag) = pinned_tag(version_target) {
                resolve_pinned_github_tool(
                    "denoland",
                    "deno",
                    tag,
                    tracker_ref,
                    deno::sources_for_release,
                )
                .await?
            } else {
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
        }
        n if n.eq_ignore_ascii_case("rsgain") => {
            if let Some(tag) = pinned_tag(version_target) {
                resolve_pinned_github_tool(
                    "complexlogic",
                    "rsgain",
                    tag,
                    tracker_ref,
                    rsgain::sources_for_release,
                )
                .await?
            } else {
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
            if let Some(tag) = pinned_tag(version_target) {
                resolve_pinned_github_tool("chmln", "sd", tag, tracker_ref, sd::sources_for_release)
                    .await?
            } else {
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
mod tests_version_targeting;

/// Provider tests, including the ones that drive a `MEDIAPM_*` seam.
///
/// A test that sets a seam variable carries `#[cfg(feature = "test-seams")]`,
/// because a build without the feature ignores that variable: the test would
/// reach the real network instead of a local server. Every workspace gate
/// passes `--all-features`, so those tests run wherever the suite runs.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::example_isolation::lock_process_env;

    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() and github_download_base() reads against the MEDIAPM_GITHUB_API_BASE and MEDIAPM_GITHUB_DOWNLOAD_BASE seam mutation scopes"
    )]
    async fn resolve_tool_fetch_routes_all_tools() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        seed_all_tools_metadata(&cache, "default").await;

        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "media-tagger", "import", "sd"] {
            let result = resolve_tool_fetch(
                name,
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
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
                    assert_eq!(
                        canonical,
                        "autobuild-2025-07-15-12-00+1759500000_N-127260-g7bc3576910"
                    );
                    assert_eq!(
                        metadata.resolved_tag.as_deref(),
                        Some("autobuild-2025-07-15-12-00")
                    );
                    // WHY: no single version identifies the mixed
                    // BtbN+martin-riedl+jellyfin artifact set, and the BtbN
                    // build-repo hash is not the upstream ffmpeg source hash.
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
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test against sibling tests that mutate the seam variables; its unknown-tool path returns before reading one"
    )]
    async fn resolve_tool_fetch_rejects_unknown() {
        let _env = lock_process_env();
        let result = resolve_tool_fetch(
            "no-such-tool",
            &VersionSpec::Latest,
            None,
            RecheckPolicy::default(),
        )
        .await;
        assert!(result.is_err(), "unknown tool should return error");
    }

    /// Seeds the jellyfin macOS repository lookups, so a test that resolves
    /// ffmpeg never reaches the live repository: the directory index, which
    /// names the newest build line, and both architectures' pointer files,
    /// which name the archives.
    async fn seed_jellyfin_metadata(cache: &ToolDownloadCache, domain: &str) {
        cache
            .store_bytes(
                domain,
                "https://repo.jellyfin.org/files/ffmpeg/macos/",
                b"<a href=\"latest-7.x/\">latest-7.x/</a><a href=\"latest-8.x/\">latest-8.x/</a>",
            )
            .await;
        cache
            .store_bytes(
                domain,
                "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/arm64/macarm64-gpl.txt",
                b"jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz\n",
            )
            .await;
        cache
            .store_bytes(
                domain,
                "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/x86_64/mac64-gpl.txt",
                b"jellyfin-ffmpeg_8.1.3-1_portable_mac64-gpl.tar.xz\n",
            )
            .await;
    }

    /// Seeds the metadata cache every network-backed managed tool resolves
    /// through, so a test can walk all of them without reaching the network:
    /// the four GitHub release lookups the tag-resolving tools read, the
    /// `BtbN` autobuild listing and the martin-riedl leg the ffmpeg chain
    /// probes, and the jellyfin repository lookups behind
    /// [`seed_jellyfin_metadata`].
    ///
    /// `{tag}\n{hash}` is the shape `resolve_latest_github_tag` stores, so one
    /// entry carries both the tag and the commit hash it dereferences to; the
    /// autobuild and martin-riedl lookups store a bare tag.
    async fn seed_all_tools_metadata(cache: &ToolDownloadCache, domain: &str) {
        for (api_url, tag, hash) in [
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
            cache.store_bytes(domain, api_url, format!("{tag}\n{hash}").as_bytes()).await;
        }
        cache
            .store_bytes(
                domain,
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache
            .store_bytes(
                domain,
                "https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip",
                b"1759500000_N-127260-g7bc3576910",
            )
            .await;
        seed_jellyfin_metadata(cache, domain).await;
    }

    /// Seeded metadata cache for a single tool's resolved fields, avoiding real
    /// API calls.
    async fn seed_metadata_cache(tool_name: &str) -> (ToolDownloadCache, tempfile::TempDir) {
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();
        let (urls, values): (Vec<&str>, Vec<&[u8]>) = match tool_name {
            "ffmpeg" => (
                vec![
                    "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                    "https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip",
                    "https://repo.jellyfin.org/files/ffmpeg/macos/",
                    "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/arm64/macarm64-gpl.txt",
                    "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/x86_64/mac64-gpl.txt",
                ],
                vec![
                    b"autobuild-2025-07-15-12-00",
                    b"1759500000_N-127260-g7bc3576910",
                    b"<a href=\"latest-8.x/\">latest-8.x/</a>",
                    b"jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz\n",
                    b"jellyfin-ffmpeg_8.1.3-1_portable_mac64-gpl.tar.xz\n",
                ],
            ),
            _ => (vec![], vec![]),
        };
        for (url, value) in urls.into_iter().zip(values) {
            cache.store_bytes("default", url, value).await;
        }
        (cache, temp_dir)
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() and github_download_base() reads against the MEDIAPM_GITHUB_API_BASE and MEDIAPM_GITHUB_DOWNLOAD_BASE seam mutation scopes"
    )]
    async fn resolve_tool_fetch_ffmpeg_leaves_version_and_vcs_hash_none() {
        let _env = lock_process_env();
        // Regression: ffmpeg's resolved_version and resolved_vcs_hash must stay
        // None by design (mixed BtbN + martin-riedl + jellyfin sources; no single
        // version or upstream VCS hash identifies the artifact set). See the
        // why-empty rationale in the ffmpeg provider module doc.
        let (cache, _tmp) = seed_metadata_cache("ffmpeg").await;
        let (_, metadata) = resolve_tool_fetch(
            "ffmpeg",
            &VersionSpec::Latest,
            Some((&*cache, "default")),
            RecheckPolicy::default(),
        )
        .await
        .expect("ffmpeg resolve should succeed");
        assert_eq!(metadata.resolved_tag.as_deref(), Some("autobuild-2025-07-15-12-00"));
        assert!(
            metadata.metadata_cached,
            "the seeded cache must serve every ffmpeg lookup; a false here means a test \
             reached the network"
        );
        // The resolve bar's item total, and the number the progress-output and
        // tool-sync-3-phase-provisioning instructions both name: the BtbN tag, the
        // martin-riedl build probe, the jellyfin index, and one pointer file per
        // macOS arch. A new lookup must fail here, not silently make those docs
        // wrong.
        assert_eq!(
            metadata.metadata_fetch_count, 5,
            "ffmpeg resolves 5 metadata lookups (BtbN tag + martin-riedl probe + jellyfin \
             index + one pointer file per macOS arch)"
        );
        // WHY: no single version identifies the mixed BtbN+martin-riedl+jellyfin artifact set.
        assert_eq!(metadata.resolved_version, None);
        // WHY: the BtbN build-repo hash is not the upstream ffmpeg source hash.
        assert_eq!(metadata.resolved_vcs_hash, None);
    }

    /// The resolved macOS leg is threaded from the resolve into the sources:
    /// both macOS permalink sources carry it as their content-cache identity,
    /// so a permalink whose build rotated is a cache miss rather than the
    /// older build's payload served under the new build's name. Seeded
    /// metadata only — the leg comes from the seeded permalink entry, not the
    /// network.
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() and github_download_base() reads against the MEDIAPM_GITHUB_API_BASE and MEDIAPM_GITHUB_DOWNLOAD_BASE seam mutation scopes"
    )]
    async fn resolve_tool_fetch_ffmpeg_threads_the_leg_into_the_permalink_identities() {
        let _env = lock_process_env();
        let (cache, _tmp) = seed_metadata_cache("ffmpeg").await;
        let (fetch, metadata) = resolve_tool_fetch(
            "ffmpeg",
            &VersionSpec::Latest,
            Some((&*cache, "default")),
            RecheckPolicy::default(),
        )
        .await
        .expect("ffmpeg resolve should succeed");

        let leg = "1759500000_N-127260-g7bc3576910";
        assert!(
            metadata.canonical_version.ends_with(&format!("+{leg}")),
            "the seeded permalink entry is the leg this test keys against: {}",
            metadata.canonical_version
        );
        for source in fetch.sources.iter().filter(|source| source.os == "macos") {
            assert_eq!(
                source.identity.as_deref(),
                Some(leg),
                "each macOS permalink source records the resolved leg as its cache identity"
            );
        }
    }

    /// The resolved `BtbN` autobuild tag is threaded from the resolve into the
    /// Windows and Linux sources: both fetch a `releases/latest/download`
    /// alias whose asset is replaced when upstream rotates, so each carries the
    /// tag as its content-cache identity, and a rotated tag is a cache miss
    /// rather than the previous build's bytes served under the new tag's name.
    /// Seeded metadata only — the tag comes from the seeded `BtbN` entry, not
    /// the network.
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() and github_download_base() reads against the MEDIAPM_GITHUB_API_BASE and MEDIAPM_GITHUB_DOWNLOAD_BASE seam mutation scopes"
    )]
    async fn resolve_tool_fetch_ffmpeg_threads_the_autobuild_tag_into_the_alias_identities() {
        let _env = lock_process_env();
        let (cache, _tmp) = seed_metadata_cache("ffmpeg").await;
        let (fetch, metadata) = resolve_tool_fetch(
            "ffmpeg",
            &VersionSpec::Latest,
            Some((&*cache, "default")),
            RecheckPolicy::default(),
        )
        .await
        .expect("ffmpeg resolve should succeed");

        let tag = metadata.resolved_tag.as_deref().expect("the seeded BtbN tag is resolved");
        assert_eq!(tag, "autobuild-2025-07-15-12-00", "the seeded tag is what the aliases key");
        for source in fetch.sources.iter().filter(|source| source.os != "macos") {
            assert_eq!(
                source.identity.as_deref(),
                Some(tag),
                "each BtbN alias source records the resolved autobuild tag as its cache identity"
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test against sibling tests that mutate the seam variables; the media-tagger arm resolves from build-time constants and reads none"
    )]
    async fn resolve_tool_fetch_media_tagger_leaves_tag_none() {
        let _env = lock_process_env();
        // Regression: media-tagger's resolved_tag must stay None by design
        // (builtin launcher shipped inside mediapm; no upstream tag exists).
        // resolved_version and resolved_vcs_hash identify the mediapm build.
        let (cache, _tmp) = seed_metadata_cache("media-tagger").await;
        let (_, metadata) = resolve_tool_fetch(
            "media-tagger",
            &VersionSpec::Latest,
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
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() and github_download_base() reads against the MEDIAPM_GITHUB_API_BASE and MEDIAPM_GITHUB_DOWNLOAD_BASE seam mutation scopes"
    )]
    async fn resolve_tool_fetch_each_fetched_tool_covers_three_oses() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache to avoid real API calls.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag and plain "{tag}" for autobuild/ martin-riedl build id.
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
        // ffmpeg: autobuild tag + martin-riedl macOS-leg build id
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache
            .store_bytes(
                "default",
                "https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip",
                b"1759500000_N-127260-g7bc3576910",
            )
            .await;

        seed_jellyfin_metadata(&cache, "default").await;

        // media-tagger is an internal launcher — no external sources.
        let expected_oses = ["windows", "linux", "macos"];
        let expected_canonicals: [(&str, &str); 5] = [
            ("ffmpeg", "autobuild-2025-07-15-12-00+1759500000_N-127260-g7bc3576910"),
            ("yt-dlp", "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"),
            ("deno", "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1"),
            ("rsgain", "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2"),
            ("sd", "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3"),
        ];
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "sd"] {
            let (fetch, metadata) = resolve_tool_fetch(
                name,
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
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
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() and github_download_base() reads against the MEDIAPM_GITHUB_API_BASE and MEDIAPM_GITHUB_DOWNLOAD_BASE seam mutation scopes"
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "integration-style test seeds metadata caches and asserts concrete URLs for all tools"
    )]
    async fn resolve_tool_fetch_with_metadata_cache_produces_concrete_urls() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache with tag values for each tool.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag and plain "{tag}" for autobuild/ martin-riedl build id.
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
        // ffmpeg: autobuild tag + martin-riedl macOS-leg build id
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache
            .store_bytes(
                "default",
                "https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip",
                b"1759500000_N-127260-g7bc3576910",
            )
            .await;

        seed_jellyfin_metadata(&cache, "default").await;

        for (tool_name, _, tag, _hash) in &test_data {
            let (fetch, metadata) = resolve_tool_fetch(
                tool_name,
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
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
            let (fetch, metadata) = resolve_tool_fetch(
                "ffmpeg",
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(fetch.tool_id, "ffmpeg");
            assert_eq!(
                canonical, "autobuild-2025-07-15-12-00+1759500000_N-127260-g7bc3576910",
                "ffmpeg canonical version mismatch"
            );
            assert!(!fetch.sources.is_empty(), "ffmpeg should have sources");
            // BtbN sources keep /latest/download/; the martin-riedl permalink
            // and jellyfin fallback carry no /latest/download/ placeholder.
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
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() reads against the MEDIAPM_GITHUB_API_BASE seam mutation scope"
    )]
    async fn resolve_latest_github_tag_round_trip() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
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
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() and github_download_base() reads against the MEDIAPM_GITHUB_API_BASE and MEDIAPM_GITHUB_DOWNLOAD_BASE seam mutation scopes"
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "integration-style test resolves and asserts exact URLs for all managed tools"
    )]
    async fn resolve_tool_fetch_exact_urls_after_resolution() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        seed_all_tools_metadata(&cache, "default").await;

        // — yt-dlp (tag "2025.07.15", no v-prefix, no filename rewrite) —
        {
            let (fetch, metadata) = resolve_tool_fetch(
                "yt-dlp",
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
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

        // — ffmpeg (composite canonical, BtbN URLs keep /latest/download/, macOS is
        //    a martin-riedl permalink + jellyfin fallback chain per arch) —
        {
            let (fetch, metadata) = resolve_tool_fetch(
                "ffmpeg",
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "autobuild-2025-07-15-12-00+1759500000_N-127260-g7bc3576910",
                "ffmpeg canonical version"
            );
            assert_eq!(fetch.sources.len(), 4, "ffmpeg: windows + 2 macOS arches + linux");
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
            // macos: one source per arch — snapshot permalink, release
            // permalink, then the jellyfin fallback
            for source in &fetch.sources[1..3] {
                assert_eq!(source.os, "macos");
                let SourceProducer::Fetch { urls } = &source.producer else {
                    panic!("ffmpeg macOS sources must be Fetch producers");
                };
                assert_eq!(
                    urls.len(),
                    3,
                    "macOS chain holds snapshot + release permalinks + jellyfin fallback"
                );
                assert!(
                    urls[0].contains("/redirect/latest/macos/") && urls[0].contains("/snapshot/"),
                    "macOS primary is the martin-riedl snapshot permalink: {}",
                    urls[0]
                );
                assert!(
                    urls[1].contains("/redirect/latest/macos/") && urls[1].contains("/release/"),
                    "macOS second candidate is the martin-riedl release permalink: {}",
                    urls[1]
                );
                assert!(
                    urls[2].contains("jellyfin-ffmpeg_"),
                    "macOS fallback is a jellyfin archive: {}",
                    urls[2]
                );
            }
            assert_eq!(fetch.sources[1].arch, "aarch64");
            assert_eq!(fetch.sources[2].arch, "x86_64");
            // linux: BtbN URLs keep /latest/download/
            if let SourceProducer::Fetch { urls } = &fetch.sources[3].producer {
                assert_eq!(fetch.sources[3].os, "linux");
                assert_eq!(urls.len(), 2);
                assert!(
                    urls[0].contains("/releases/latest/download/"),
                    "ffmpeg linux primary URL should use /latest/download/"
                );
            }
        }

        // — deno (tag "v2.2.12", v-prefixed, no filename rewrite) —
        {
            let (fetch, metadata) = resolve_tool_fetch(
                "deno",
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1",
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
                    urls[0].ends_with(&format!("deno-{}-apple-darwin.zip", host_arch_label())),
                    "deno macos URL should name the host-arch asset, got {}",
                    urls[0],
                );
                assert_eq!(
                    urls.len(),
                    1,
                    "deno macos should have exactly one host-arch URL, not a cross-arch list"
                );
            }
            if let SourceProducer::Fetch { urls } = &fetch.sources[2].producer {
                assert_eq!(fetch.sources[2].os, "linux");
                assert!(urls[0].contains("v2.2.12"), "deno linux URL should contain tag");
                assert_eq!(
                    urls.len(),
                    1,
                    "deno linux should have exactly one host-arch URL, not a cross-arch list"
                );
            }
        }

        // — rsgain (tag "v3.7", path + filename rewrite: rsgain-latest → rsgain-3.7) —
        {
            let (fetch, metadata) = resolve_tool_fetch(
                "rsgain",
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
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
            let (fetch, metadata) = resolve_tool_fetch(
                "sd",
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap();
            let canonical = metadata.canonical_version;
            assert_eq!(
                canonical, "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3",
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
                let expected = format!(
                    "https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-{}-apple-darwin.tar.gz",
                    host_arch_label()
                );
                assert_eq!(
                    urls,
                    &[expected],
                    "sd macos should have exactly one host-arch URL, not a cross-arch list"
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
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test against sibling tests that mutate the seam variables; the media-tagger arm resolves from build-time constants and reads none"
    )]
    async fn resolve_tool_fetch_media_tagger_canonical_is_git_hash() {
        let _env = lock_process_env();
        let (_, metadata) = resolve_tool_fetch(
            "media-tagger",
            &VersionSpec::Latest,
            None,
            RecheckPolicy::default(),
        )
        .await
        .unwrap();
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
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() and github_download_base() reads against the MEDIAPM_GITHUB_API_BASE and MEDIAPM_GITHUB_DOWNLOAD_BASE seam mutation scopes"
    )]
    async fn resolve_canonical_version_is_deterministic() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
        let cache = ToolDownloadCache::open(temp_dir.path(), "metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache with known tags and hashes.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag and plain "{tag}" for autobuild/ martin-riedl build id.
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
        // ffmpeg: autobuild tag + martin-riedl macOS-leg build id
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache
            .store_bytes(
                "default",
                "https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip",
                b"1759500000_N-127260-g7bc3576910",
            )
            .await;

        seed_jellyfin_metadata(&cache, "default").await;

        for tool in &["ffmpeg", "yt-dlp", "deno", "rsgain", "sd", "media-tagger"] {
            let (_, metadata1) = resolve_tool_fetch(
                tool,
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap();
            let (_, metadata2) = resolve_tool_fetch(
                tool,
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                metadata1.canonical_version, metadata2.canonical_version,
                "canonical_version for {tool} must be deterministic"
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() and github_download_base() reads against the MEDIAPM_GITHUB_API_BASE and MEDIAPM_GITHUB_DOWNLOAD_BASE seam mutation scopes"
    )]
    async fn all_fetch_providers_have_size_hint_bytes() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache to avoid real API calls.
        // Cache format is "{tag}\n{hash}" for resolve_latest_github_tag and plain "{tag}" for autobuild/ martin-riedl build id.
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
        // ffmpeg: autobuild tag + martin-riedl macOS-leg build id
        cache
            .store_bytes(
                "default",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
                b"autobuild-2025-07-15-12-00",
            )
            .await;
        cache
            .store_bytes(
                "default",
                "https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip",
                b"1759500000_N-127260-g7bc3576910",
            )
            .await;

        seed_jellyfin_metadata(&cache, "default").await;

        // All managed tools whose provider type is Fetch must have size_hint_bytes.
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain"] {
            let (fetch, _metadata) = resolve_tool_fetch(
                name,
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
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
                &VersionSpec::Latest,
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
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() reads against the MEDIAPM_GITHUB_API_BASE seam mutation scope"
    )]
    async fn resolve_latest_github_tag_fallthrough_on_stale_latest() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
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
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
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

    /// The releases-list lookup routes through the `MEDIAPM_GITHUB_API_BASE`
    /// seam (the folded review fix): a wiremock server standing in for the
    /// GitHub API answers the list endpoint and the first `autobuild-*` tag
    /// is picked from it, proving the URL is built from `github_api_base()`
    /// and not a hardcoded host.
    #[cfg(feature = "test-seams")]
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve of github_api_base(); the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
    )]
    async fn resolve_latest_autobuild_tag_routes_through_the_github_api_base_seam() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/BtbN/FFmpeg-Builds/releases"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                { "tag_name": "latest" },
                { "tag_name": "autobuild-2025-07-15-12-00" },
            ])))
            .mount(&server)
            .await;

        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();
        let tracker = MetadataCacheTracker::new(&cache, "default");

        let _env = lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(GITHUB_API_BASE_ENV, server.uri());
        }

        let (tag, metadata_cached) =
            resolve_latest_autobuild_tag("BtbN", "FFmpeg-Builds", Some(&tracker))
                .await
                .expect("seam-routed releases list should resolve");
        assert_eq!(tag, "autobuild-2025-07-15-12-00", "first autobuild-* tag wins");
        assert!(!metadata_cached, "the list was fetched, not cached");

        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(GITHUB_API_BASE_ENV);
        }
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock serializes this test's github_api_base() reads against the MEDIAPM_GITHUB_API_BASE seam mutation scope"
    )]
    async fn resolve_latest_github_tag_returns_tag_and_hash() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
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

    // ── canonical_version_after_fetch (fetch feedback) ───────────────

    /// Builds one downloaded macOS entry carrying an `actual_url`, for the
    /// fetch-feedback tests.
    fn macos_entry_with_actual_url(url: &str) -> DownloadedSource {
        DownloadedSource {
            os: "macos".to_string(),
            arch: "aarch64".to_string(),
            producer: SourceProducer::Fetch { urls: vec![url.to_string()] },
            bytes: Vec::new(),
            expected_size: None,
            actual_url: Some(url.to_string()),
        }
    }

    /// When the martin-riedl primary served, the composite's leg becomes the
    /// build id from the actual versioned download URL — whatever the
    /// resolve-time probe predicted.
    #[test]
    fn canonical_after_fetch_rewrites_the_leg_from_a_martin_riedl_url() {
        let entries = [macos_entry_with_actual_url(
            "https://ffmpeg.martin-riedl.de/download/macos/aarch64/1759600000_N-127999-gdeadbeef12/ffmpeg.zip",
        )];
        assert_eq!(
            canonical_version_after_fetch(
                "ffmpeg",
                "autobuild-2025-07-15-12-00+1759500000_N-127260-g7bc3576910",
                &entries,
            ),
            "autobuild-2025-07-15-12-00+1759600000_N-127999-gdeadbeef12",
            "the leg is replaced by the build id the fetch actually served"
        );
    }

    /// A fallback (jellyfin) fetch rewrites the leg to the archive's
    /// version, and `SameStep` dependency segments (`;dep:ver`) survive the
    /// rewrite untouched.
    #[test]
    fn canonical_after_fetch_rewrites_the_leg_from_a_jellyfin_url_and_keeps_dep_segments() {
        let entries = [macos_entry_with_actual_url(
            "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/arm64/jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz",
        )];
        assert_eq!(
            canonical_version_after_fetch(
                "ffmpeg",
                "autobuild-2025-07-15-12-00+1759500000_N-127260-g7bc3576910;deno:abc123",
                &entries,
            ),
            "autobuild-2025-07-15-12-00+8.1.3-1;deno:abc123",
            "the fallback leg replaces the composite segment; dep segments stay"
        );
    }

    /// No macOS entry, no actual URL, an unparsable URL, or a different tool
    /// all leave the canonical version untouched — feedback corrects only
    /// what it can actually read.
    #[test]
    fn canonical_after_fetch_leaves_uncorrectable_cases_unchanged() {
        let windows_only = [DownloadedSource {
            os: "windows".to_string(),
            arch: "x86_64".to_string(),
            producer: SourceProducer::Fetch { urls: vec!["https://example.com/x.zip".to_string()] },
            bytes: Vec::new(),
            expected_size: None,
            actual_url: Some("https://example.com/x.zip".to_string()),
        }];
        assert_eq!(
            canonical_version_after_fetch("ffmpeg", "autobuild-x+leg", &windows_only),
            "autobuild-x+leg",
            "no macOS entry means no leg to re-derive"
        );

        let no_actual_url = [DownloadedSource {
            actual_url: None,
            ..macos_entry_with_actual_url("https://example.com/y.zip")
        }];
        assert_eq!(
            canonical_version_after_fetch("ffmpeg", "autobuild-x+leg", &no_actual_url),
            "autobuild-x+leg",
            "a launcher-style entry carries no URL feedback"
        );

        let unparsable = [macos_entry_with_actual_url("https://example.com/not-a-chain-url.zip")];
        assert_eq!(
            canonical_version_after_fetch("ffmpeg", "autobuild-x+leg", &unparsable),
            "autobuild-x+leg",
            "an unparsable actual URL is no basis for a rewrite"
        );

        let yt_dlp_entry = [DownloadedSource {
            os: "linux".to_string(),
            ..macos_entry_with_actual_url(
                "https://github.com/yt-dlp/yt-dlp/releases/download/2025.07.15/yt-dlp_linux",
            )
        }];
        assert_eq!(
            canonical_version_after_fetch("yt-dlp", "a1b2c3d4", &yt_dlp_entry),
            "a1b2c3d4",
            "only ffmpeg's canonical carries a macOS leg to correct"
        );
    }

    /// The API seam answers the `MEDIAPM_GITHUB_API_BASE` override in a
    /// `test-seams` build, which is what lets a test point the provider at a
    /// local server instead of the real GitHub API.
    #[cfg(feature = "test-seams")]
    #[test]
    fn github_api_base_returns_the_override_when_the_seam_is_set() {
        let _env = lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(GITHUB_API_BASE_ENV, "http://127.0.0.1:9/github-api");
        }
        assert_eq!(github_api_base(), "http://127.0.0.1:9/github-api");
        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(GITHUB_API_BASE_ENV);
        }
    }

    /// The download seam answers the `MEDIAPM_GITHUB_DOWNLOAD_BASE` override
    /// in a `test-seams` build, which is what lets a test serve every managed
    /// tool's payload URLs from a local server.
    #[cfg(feature = "test-seams")]
    #[test]
    fn github_download_base_returns_the_override_when_the_seam_is_set() {
        let _env = lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(GITHUB_DOWNLOAD_BASE_ENV, "http://127.0.0.1:9/assets");
        }
        assert_eq!(github_download_base(), "http://127.0.0.1:9/assets");
        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(GITHUB_DOWNLOAD_BASE_ENV);
        }
    }

    /// The release-asset URL is composed from the download seam rather than
    /// from a literal host, so a test can redirect the whole payload path and
    /// a future edit cannot quietly reintroduce `github.com`.
    #[cfg(feature = "test-seams")]
    #[test]
    fn github_release_asset_url_is_composed_from_the_download_seam() {
        let _env = lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(GITHUB_DOWNLOAD_BASE_ENV, "http://127.0.0.1:9/assets");
        }
        assert_eq!(
            github_release_asset_url("yt-dlp", "yt-dlp", "2025.07.15", "yt-dlp_linux"),
            "http://127.0.0.1:9/assets/yt-dlp/yt-dlp/releases/download/2025.07.15/yt-dlp_linux",
            "only the host comes from the seam; the path shape is unchanged"
        );
        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(GITHUB_DOWNLOAD_BASE_ENV);
        }
    }

    /// Every payload URL a `Latest` spec resolves is built from
    /// [`github_download_base`], so pointing that seam at a local server moves
    /// the default spec — the one `mediapm.ncl` and the all-platform test use
    /// — off the real GitHub.
    ///
    /// The walk is the point. The `latest/download` templates live in five
    /// provider modules, and a template that hardcoded its host would compile,
    /// resolve, and fetch from `github.com` while every per-tool assertion
    /// still passed: the resolve phase only rewrites the path segment, never
    /// the host. So this resolves every managed tool and fails on the first
    /// URL that still names `github.com`, naming the tool and the URL.
    #[cfg(feature = "test-seams")]
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the process env lock scopes this test's own MEDIAPM_GITHUB_DOWNLOAD_BASE mutation and serializes its github_api_base() and github_download_base() reads against sibling mutations"
    )]
    async fn every_latest_payload_url_is_built_from_the_download_seam() {
        let _env = lock_process_env();
        let temp_dir = mediapm_utils::temp::cache_dir().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();
        seed_all_tools_metadata(&cache, "default").await;

        let seam = "http://127.0.0.1:9/assets";
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(GITHUB_DOWNLOAD_BASE_ENV, seam);
        }

        for name in ["ffmpeg", "yt-dlp", "deno", "rsgain", "sd", "media-tagger", "import"] {
            let (fetch, _) = resolve_tool_fetch(
                name,
                &VersionSpec::Latest,
                Some((&*cache, "default")),
                RecheckPolicy::default(),
            )
            .await
            .unwrap_or_else(|error| panic!("tool {name}: resolve should succeed, got {error:?}"));

            let mut payload_urls = 0_usize;
            for source in &fetch.sources {
                let SourceProducer::Fetch { urls } = &source.producer else {
                    continue;
                };
                for url in urls {
                    payload_urls += 1;
                    assert!(
                        !url.contains("github.com"),
                        "tool {name} (os {}): the Latest payload URL {url} still names github.com, \
                         so MEDIAPM_GITHUB_DOWNLOAD_BASE cannot move it",
                        source.os
                    );
                }
            }
            if matches!(name, "media-tagger" | "import") {
                assert_eq!(payload_urls, 0, "tool {name}: a launcher fetches no payload");
            } else {
                assert!(
                    payload_urls > 0,
                    "tool {name}: no payload URL to check, so this walk would pass vacuously"
                );
            }
        }

        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(GITHUB_DOWNLOAD_BASE_ENV);
        }
    }

    /// Without the `test-seams` feature the environment cannot redirect the
    /// provider: both bases stay at their production values, and the asset URL
    /// keeps the production host, even with the variables set.
    ///
    /// Run with
    /// `cargo test -p mediapm --no-default-features --features cli,media-tagger`.
    /// The plain `--no-default-features` form cannot compile in this workspace:
    /// with `tool-presets` off, `mediapm-conductor` fails on its optional `zip`
    /// dependency in `step_worker/capture.rs`. This test cannot run under
    /// `--all-features` either, which compiles the seam in and would take the
    /// override branch instead.
    #[cfg(not(feature = "test-seams"))]
    #[test]
    fn github_bases_ignore_the_environment_without_the_seam_feature() {
        let _env = lock_process_env();
        // SAFETY: the env lock the guard holds scopes these process-global
        // variables to this test, per the documented MEDIAPM_* mutation-scope
        // rule. The names are spelled here rather than through the constants
        // because this build compiles those constants out.
        unsafe {
            std::env::set_var("MEDIAPM_GITHUB_API_BASE", "http://127.0.0.1:9/github-api");
            std::env::set_var("MEDIAPM_GITHUB_DOWNLOAD_BASE", "http://127.0.0.1:9/assets");
        }
        assert_eq!(github_api_base(), GITHUB_API_BASE_DEFAULT);
        assert_eq!(github_download_base(), GITHUB_DOWNLOAD_BASE_DEFAULT);
        assert_eq!(
            github_release_asset_url("yt-dlp", "yt-dlp", "2025.07.15", "yt-dlp_linux"),
            "https://github.com/yt-dlp/yt-dlp/releases/download/2025.07.15/yt-dlp_linux"
        );
        // SAFETY: same env-lock scope as the sets above.
        unsafe {
            std::env::remove_var("MEDIAPM_GITHUB_API_BASE");
            std::env::remove_var("MEDIAPM_GITHUB_DOWNLOAD_BASE");
        }
    }
}
