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
pub(crate) mod media_tagger;
pub(crate) mod rsgain;
pub(crate) mod sd;
pub(crate) mod yt_dlp;

use mediapm_conductor::tools::provider::{ResolvedToolFetch, SourceProducer};

use crate::tools::downloader::ToolDownloadCache;

/// Resolves the latest GitHub release tag for `owner/repo`.
///
/// Uses the metadata cache to avoid repeated GitHub API calls. The
/// cache key is the GitHub API endpoint URL itself, so every unique
/// owner/repo pair gets its own cache entry. The caller must NOT call
/// `touch()` on the metadata cache — the 1-day TTL is anchored to
/// creation time, not last use.
///
/// # Errors
///
/// Returns [`mediapm_conductor::ConductorError`] when the HTTP request
/// or cache I/O fails.
/// Validates that a tag is a concrete version, not a placeholder.
fn validate_tag(tag: &str) -> Result<(), mediapm_conductor::ConductorError> {
    if tag.eq_ignore_ascii_case("latest") {
        return Err(mediapm_conductor::ConductorError::Workflow(format!(
            "resolved tag is '{tag}' which is a placeholder, not a concrete version"
        )));
    }
    Ok(())
}

pub(crate) async fn resolve_latest_github_tag(
    owner: &str,
    repo: &str,
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<String, mediapm_conductor::ConductorError> {
    let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases/latest");

    // Try metadata cache first. If the cached tag is invalid (e.g. stale
    // "latest" placeholder) or not UTF-8, fall through to re-fetch from
    // the GitHub API — treating it as a cache miss.
    if let Some(cache) = metadata_cache {
        if let Some(bytes) = cache.lookup_bytes(&api_url).await {
            if let Ok(tag) = String::from_utf8(bytes.to_vec()) {
                if validate_tag(&tag).is_ok() {
                    return Ok(tag);
                }
            }
            // Cached tag is invalid or not UTF-8 — fall through to re-fetch.
        }
    }

    // Fetch from GitHub API.
    let http_client = crate::http_client::shared_http_client().map_err(|e| {
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
    validate_tag(&tag)?;

    // Store in metadata cache. Do NOT call touch() — TTL is creation-time-based.
    if let Some(cache) = metadata_cache {
        cache.store_bytes(&api_url, tag.as_bytes()).await;
    }

    Ok(tag)
}

/// Resolves source descriptors and canonical version for the named managed tool.
///
/// Returns a tuple of `(ResolvedToolFetch, String)` where the second element
/// is the canonical version identifier for skip-if-up-to-date logic. Always
/// populated — the type is `String`, not `Option<String>`. The semantic kind
/// (VCS hash, version, or tag) is fixed at code-writing time per tool.
///
/// When `metadata_cache` is provided, tools with dynamic version resolution
/// (e.g., yt-dlp "latest" tag) use it to cache version/tag lookup results.
/// The consumer must NOT call `touch()` on the metadata cache — its TTL is
/// creation-time-based.
///
/// # Errors
///
/// Returns an error when the tool name is not recognised.
pub(crate) async fn resolve_tool_fetch(
    tool_name: &str,
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<(ResolvedToolFetch, String), mediapm_conductor::ConductorError> {
    match tool_name {
        n if n.eq_ignore_ascii_case("yt-dlp") => {
            let tag = yt_dlp::resolve_latest_tag(metadata_cache).await?;
            let mut fetch = yt_dlp::sources();
            for source in &mut fetch.sources {
                if let SourceProducer::Fetch { urls } = &mut source.producer {
                    for url in urls.iter_mut() {
                        *url = url.replace("/latest/download/", &format!("/download/{tag}/"));
                    }
                }
            }
            Ok((fetch, tag))
        }
        n if n.eq_ignore_ascii_case("ffmpeg") => {
            let tag = ffmpeg::resolve_tag(metadata_cache).await?;
            let mut fetch = ffmpeg::sources();
            for source in &mut fetch.sources {
                if let SourceProducer::Fetch { urls } = &mut source.producer {
                    for url in urls.iter_mut() {
                        // Only substitute BtbN URLs; Evermeet macOS URL stays floating.
                        if url.contains("BtbN") {
                            *url = url.replace("/latest/download/", &format!("/download/{tag}/"));
                        }
                    }
                }
            }
            Ok((fetch, tag))
        }
        n if n.eq_ignore_ascii_case("deno") => {
            let tag = deno::resolve_tag(metadata_cache).await?;
            let mut fetch = deno::sources();
            for source in &mut fetch.sources {
                if let SourceProducer::Fetch { urls } = &mut source.producer {
                    for url in urls.iter_mut() {
                        *url = url.replace("/latest/download/", &format!("/download/{tag}/"));
                    }
                }
            }
            Ok((fetch, tag))
        }
        n if n.eq_ignore_ascii_case("rsgain") => {
            let tag = rsgain::resolve_tag(metadata_cache).await?;
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
            Ok((fetch, tag))
        }
        n if n.eq_ignore_ascii_case("media-tagger") => {
            let canonical = crate::global::MEDIAPM_GIT_HASH.to_string();
            Ok((media_tagger::sources(), canonical))
        }
        n if n.eq_ignore_ascii_case("sd") => {
            let tag = sd::resolve_tag(metadata_cache).await?;
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
            Ok((fetch, tag))
        }
        _ => Err(mediapm_conductor::ConductorError::Workflow(format!(
            "tool {tool_name}: no provider registered for resolution"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_tool_fetch_routes_all_tools() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache for network-backed tools to avoid real API calls.
        for (api_url, tag) in &[
            ("https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest", "2025.07.15"),
            ("https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/latest", "L2025-07-15"),
            ("https://api.github.com/repos/denoland/deno/releases/latest", "v2.2.12"),
            ("https://api.github.com/repos/complexlogic/rsgain/releases/latest", "v3.7"),
            ("https://api.github.com/repos/chmln/sd/releases/latest", "v1.1.0"),
        ] {
            cache.store_bytes(api_url, tag.as_bytes()).await;
        }

        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "media-tagger", "sd"] {
            let result = resolve_tool_fetch(name, Some(&cache)).await;
            assert!(result.is_ok(), "tool {name}: resolve should succeed");
            let (fetch, canonical) = result.unwrap();
            assert_eq!(fetch.tool_id, *name, "tool_id should match input name");
            match *name {
                "yt-dlp" => assert_eq!(canonical, "2025.07.15"),
                "ffmpeg" => assert_eq!(canonical, "L2025-07-15"),
                "deno" => assert_eq!(canonical, "v2.2.12"),
                "rsgain" => assert_eq!(canonical, "v3.7"),
                "sd" => assert_eq!(canonical, "v1.1.0"),
                "media-tagger" => assert!(
                    !canonical.is_empty(),
                    "media-tagger canonical_version should not be empty"
                ),
                _ => unreachable!(),
            }
            if *name == "media-tagger" {
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
        let result = resolve_tool_fetch("no-such-tool", None).await;
        assert!(result.is_err(), "unknown tool should return error");
    }

    #[tokio::test]
    async fn resolve_tool_fetch_each_fetched_tool_has_three_os_entries() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache to avoid real API calls.
        for (api_url, tag) in &[
            ("https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest", "2025.07.15"),
            ("https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/latest", "L2025-07-15"),
            ("https://api.github.com/repos/denoland/deno/releases/latest", "v2.2.12"),
            ("https://api.github.com/repos/complexlogic/rsgain/releases/latest", "v3.7"),
            ("https://api.github.com/repos/chmln/sd/releases/latest", "v1.1.0"),
        ] {
            cache.store_bytes(api_url, tag.as_bytes()).await;
        }

        // media-tagger is an internal launcher — no external sources.
        let expected_oses = ["windows", "linux", "macos"];
        let expected_canonicals: [(&str, &str); 5] = [
            ("ffmpeg", "L2025-07-15"),
            ("yt-dlp", "2025.07.15"),
            ("deno", "v2.2.12"),
            ("rsgain", "v3.7"),
            ("sd", "v1.1.0"),
        ];
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "sd"] {
            let (fetch, canonical) = resolve_tool_fetch(name, Some(&cache)).await.unwrap();
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
    async fn resolve_tool_fetch_with_metadata_cache_produces_concrete_urls() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache with tag values for each tool.
        // The cache keys are the actual API endpoint URLs (URL-based convention).
        let test_data: Vec<(&str, &str, &str)> = vec![
            ("yt-dlp", "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest", "2025.07.15"),
            (
                "ffmpeg",
                "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/latest",
                "L2025-07-15",
            ),
            ("deno", "https://api.github.com/repos/denoland/deno/releases/latest", "v2.2.12"),
            ("rsgain", "https://api.github.com/repos/complexlogic/rsgain/releases/latest", "v3.7"),
            ("sd", "https://api.github.com/repos/chmln/sd/releases/latest", "v1.1.0"),
        ];

        for (_, api_url, tag) in &test_data {
            cache.store_bytes(api_url, tag.as_bytes()).await;
        }

        for (tool_name, _, tag) in &test_data {
            let (fetch, canonical) = resolve_tool_fetch(tool_name, Some(&cache)).await.unwrap();
            assert_eq!(fetch.tool_id, *tool_name, "tool_id should match input name",);
            assert_eq!(canonical, *tag, "tool {tool_name}: canonical version mismatch",);
            assert!(!fetch.sources.is_empty(), "tool {tool_name}: should have at least one source",);
            for source in &fetch.sources {
                if let SourceProducer::Fetch { urls } = &source.producer {
                    for url in urls {
                        // No URL should still contain the /latest/download/ placeholder.
                        assert!(
                            !url.contains("/latest/download/"),
                            "tool {tool_name}: URL {url} still contains placeholder /latest/download/",
                        );
                        // For non-ffmpeg tools, and for ffmpeg's BtbN sources (not Evermeet),
                        // the URL should contain the resolved tag.
                        if *tool_name != "ffmpeg"
                            || !url.contains("evermeet") && !url.contains("getrelease")
                        {
                            assert!(
                                url.contains(tag),
                                "tool {tool_name}: URL {url} does not contain resolved tag '{tag}'",
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

        let owner = "testowner";
        let repo = "testrepo";
        let expected_tag = "v1.0.0";
        let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases/latest");

        // Pre-seed metadata cache so the helper returns cached data without network.
        cache.store_bytes(&api_url, expected_tag.as_bytes()).await;

        let tag = resolve_latest_github_tag(owner, repo, Some(&cache))
            .await
            .expect("resolve_latest_github_tag should succeed with cached data");

        assert_eq!(tag, expected_tag, "cached tag should be returned without HTTP call",);
    }

    #[tokio::test]
    async fn resolve_tool_fetch_exact_urls_after_resolution() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache with known tags for all tools.
        for (api_url, tag) in &[
            ("https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest", "2025.07.15"),
            ("https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/latest", "L2025-07-15"),
            ("https://api.github.com/repos/denoland/deno/releases/latest", "v2.2.12"),
            ("https://api.github.com/repos/complexlogic/rsgain/releases/latest", "v3.7"),
            ("https://api.github.com/repos/chmln/sd/releases/latest", "v1.1.0"),
        ] {
            cache.store_bytes(api_url, tag.as_bytes()).await;
        }

        // — yt-dlp (tag "2025.07.15", no v-prefix, no filename rewrite) —
        {
            let (fetch, canonical) = resolve_tool_fetch("yt-dlp", Some(&cache)).await.unwrap();
            assert_eq!(canonical, "2025.07.15", "yt-dlp canonical version");
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

        // — ffmpeg (tag "L2025-07-15", BtbN substituted, Evermeet untouched) —
        {
            let (fetch, canonical) = resolve_tool_fetch("ffmpeg", Some(&cache)).await.unwrap();
            assert_eq!(canonical, "L2025-07-15", "ffmpeg canonical version");
            assert_eq!(fetch.sources.len(), 3, "ffmpeg: expected 3 OS sources");
            // windows: BtbN with tag, 2 fallback URLs
            if let SourceProducer::Fetch { urls } = &fetch.sources[0].producer {
                assert_eq!(fetch.sources[0].os, "windows");
                assert_eq!(urls.len(), 2);
                assert!(
                    urls[0].contains("L2025-07-15"),
                    "ffmpeg windows primary URL should contain tag"
                );
                assert!(urls[0].contains("BtbN"), "ffmpeg windows primary URL should be BtbN");
                assert!(
                    urls[1].contains("L2025-07-15"),
                    "ffmpeg windows fallback URL should contain tag"
                );
            }
            // macos: Evermeet, completely unchanged
            if let SourceProducer::Fetch { urls } = &fetch.sources[1].producer {
                assert_eq!(fetch.sources[1].os, "macos");
                assert_eq!(urls, &["https://evermeet.cx/ffmpeg/getrelease/zip"]);
            }
            // linux: BtbN with tag, 2 fallback URLs
            if let SourceProducer::Fetch { urls } = &fetch.sources[2].producer {
                assert_eq!(fetch.sources[2].os, "linux");
                assert_eq!(urls.len(), 2);
                assert!(
                    urls[0].contains("L2025-07-15"),
                    "ffmpeg linux primary URL should contain tag"
                );
                assert!(urls[0].contains("BtbN"), "ffmpeg linux primary URL should be BtbN");
            }
        }

        // — deno (tag "v2.2.12", v-prefixed, no filename rewrite) —
        {
            let (fetch, canonical) = resolve_tool_fetch("deno", Some(&cache)).await.unwrap();
            assert_eq!(canonical, "v2.2.12", "deno canonical version");
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
            let (fetch, canonical) = resolve_tool_fetch("rsgain", Some(&cache)).await.unwrap();
            assert_eq!(canonical, "v3.7", "rsgain canonical version");
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
            let (fetch, canonical) = resolve_tool_fetch("sd", Some(&cache)).await.unwrap();
            assert_eq!(canonical, "v1.1.0", "sd canonical version");
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
        let (_, canonical) = resolve_tool_fetch("media-tagger", None).await.unwrap();
        // MEDIAPM_GIT_HASH is the compile-time constant — it may be empty in some
        // test environments without .git, but it must not panic.
        if !canonical.is_empty() {
            assert!(canonical.len() >= 7, "git hash should be at least 7 chars");
        }
    }

    #[tokio::test]
    async fn resolve_canonical_version_is_deterministic() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = ToolDownloadCache::open(temp_dir.path(), "metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache with known tags.
        let seeds: &[(&str, &str)] = &[
            ("https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest", "2025.07.15"),
            ("https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/latest", "L2025-07-15"),
            ("https://api.github.com/repos/denoland/deno/releases/latest", "v2.2.12"),
            ("https://api.github.com/repos/complexlogic/rsgain/releases/latest", "v3.7"),
            ("https://api.github.com/repos/chmln/sd/releases/latest", "v1.1.0"),
        ];
        for (url, tag) in seeds {
            cache.store_bytes(url, tag.as_bytes()).await;
        }

        for tool in &["ffmpeg", "yt-dlp", "deno", "rsgain", "sd", "media-tagger"] {
            let (_, cv1) = resolve_tool_fetch(tool, Some(&cache)).await.unwrap();
            let (_, cv2) = resolve_tool_fetch(tool, Some(&cache)).await.unwrap();
            assert_eq!(cv1, cv2, "canonical_version for {tool} must be deterministic");
        }
    }

    #[tokio::test]
    async fn all_fetch_providers_have_size_hint_bytes() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed metadata cache to avoid real API calls.
        for (api_url, tag) in &[
            ("https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest", "2025.07.15"),
            ("https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/latest", "L2025-07-15"),
            ("https://api.github.com/repos/denoland/deno/releases/latest", "v2.2.12"),
            ("https://api.github.com/repos/complexlogic/rsgain/releases/latest", "v3.7"),
        ] {
            cache.store_bytes(api_url, tag.as_bytes()).await;
        }

        // All managed tools whose provider type is Fetch must have size_hint_bytes.
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain"] {
            let (fetch, _canonical) = resolve_tool_fetch(name, Some(&cache))
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
            let (fetch, _) = resolve_tool_fetch("media-tagger", Some(&cache)).await.unwrap();
            for source in &fetch.sources {
                assert!(
                    matches!(source.producer, super::SourceProducer::GenerateLauncher { .. }),
                    "media-tagger should only have GenerateLauncher sources"
                );
            }
        }
    }

    // ── validate_tag ────────────────────────────────────────────────

    #[tokio::test]
    async fn validate_tag_rejects_latest() {
        let err = validate_tag("latest").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("placeholder"),
            "error for 'latest' should mention 'placeholder', got: {msg}"
        );
    }

    #[tokio::test]
    async fn validate_tag_rejects_latest_case_insensitive() {
        let err = validate_tag("LATEST").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("placeholder"),
            "error for 'LATEST' should mention 'placeholder', got: {msg}"
        );
    }

    #[tokio::test]
    async fn validate_tag_accepts_concrete_version() {
        validate_tag("v1.0.0").expect("concrete version should be accepted");
        validate_tag("2025.07.15").expect("date version should be accepted");
        validate_tag("L2025-07-15").expect("ffmpeg-style version should be accepted");
        validate_tag("abc123").expect("hash should be accepted");
    }

    #[tokio::test]
    async fn resolve_latest_github_tag_fallthrough_on_stale_latest() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache =
            ToolDownloadCache::open(temp_dir.path(), "test_metadata.json", 3600).await.unwrap();

        // Pre-seed cache with "latest" placeholder — this simulates stale state.
        let owner = "testowner";
        let repo = "testrepo";
        let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases/latest");
        cache.store_bytes(&api_url, b"latest").await;

        // The cached "latest" is rejected by validate_tag, so the function
        // falls through to the HTTP fetch.  Without a real GitHub API, the
        // HTTP call fails with a transport error — NOT a "placeholder" error.
        let err = resolve_latest_github_tag(owner, repo, Some(&cache)).await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            !msg.contains("placeholder"),
            "stale-cache fallthrough should produce a transport error, not 'placeholder', got: {msg}"
        );
        assert!(
            msg.contains("GitHub API request failed")
                || msg.contains("HTTP client unavailable")
                || msg.contains("GitHub API response"),
            "stale-cache fallthrough should produce an HTTP/transport error, not 'placeholder', got: {msg}"
        );
    }
}
