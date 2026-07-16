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
pub(crate) async fn resolve_latest_github_tag(
    owner: &str,
    repo: &str,
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<String, mediapm_conductor::ConductorError> {
    let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases/latest");

    // Try metadata cache first.
    if let Some(cache) = metadata_cache {
        if let Some(bytes) = cache.lookup_bytes(&api_url).await {
            let tag = String::from_utf8(bytes.to_vec()).map_err(|_| {
                mediapm_conductor::ConductorError::Workflow(
                    "cached tag is not valid UTF-8".to_string(),
                )
            })?;
            return Ok(tag);
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

    // Store in metadata cache. Do NOT call touch() — TTL is creation-time-based.
    if let Some(cache) = metadata_cache {
        cache.store_bytes(&api_url, tag.as_bytes()).await;
    }

    Ok(tag)
}

/// Resolves source descriptors for the named managed tool.
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
) -> Result<ResolvedToolFetch, mediapm_conductor::ConductorError> {
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
            Ok(fetch)
        }
        n if n.eq_ignore_ascii_case("ffmpeg") => Ok(ffmpeg::sources()),
        n if n.eq_ignore_ascii_case("deno") => Ok(deno::sources()),
        n if n.eq_ignore_ascii_case("rsgain") => Ok(rsgain::sources()),
        n if n.eq_ignore_ascii_case("media-tagger") => Ok(media_tagger::sources()),
        n if n.eq_ignore_ascii_case("sd") => Ok(sd::sources()),
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
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "media-tagger", "sd"] {
            let result = resolve_tool_fetch(name, None).await;
            assert!(result.is_ok(), "tool {name}: resolve should succeed");
            let fetch = result.unwrap();
            assert_eq!(fetch.tool_id, *name, "tool_id should match input name");
            if *name == "media-tagger" {
                // media-tagger is an internal launcher with no external sources.
                assert!(fetch.sources.is_empty(), "tool {name}: should have zero sources");
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
        // media-tagger is an internal launcher — no external sources.
        let expected_oses = ["windows", "linux", "macos"];
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "sd"] {
            let fetch = resolve_tool_fetch(name, None).await.unwrap();
            let oses: Vec<&str> = fetch.sources.iter().map(|s| s.os.as_str()).collect();
            for expected_os in &expected_oses {
                assert!(
                    oses.contains(expected_os),
                    "tool {name}: missing source for OS {expected_os}; found OSes: {oses:?}"
                );
            }
        }
    }
}
