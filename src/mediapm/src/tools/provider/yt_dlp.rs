//! Provider source definitions for `yt-dlp`.
//!
//! Sources are fetched from the yt-dlp GitHub releases page as
//! standalone binaries (no archive extraction needed). The "latest"
//! tag is resolved via the GitHub API and cached in the metadata cache.

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

use crate::tools::downloader::ToolDownloadCache;

/// Resolves the "latest" tag for yt-dlp from GitHub releases.
///
/// Uses the metadata cache to avoid repeated GitHub API calls. The
/// caller must NOT call `touch()` on the metadata cache — the 1-day TTL
/// is anchored to creation time, not last use.
///
/// Cache key: `"yt-dlp:latest-tag"`.
///
/// # Errors
///
/// Returns [`mediapm_conductor::ConductorError`] when the HTTP request
/// or cache I/O fails.
pub(crate) async fn resolve_latest_tag(
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<String, mediapm_conductor::ConductorError> {
    let cache_key = "yt-dlp:latest-tag";

    // Try metadata cache first.
    if let Some(cache) = metadata_cache {
        if let Some(bytes) = cache.lookup_bytes(cache_key).await {
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

    let response = http_client
        .get("https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest")
        .send()
        .await
        .map_err(|e| {
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
        cache.store_bytes(cache_key, tag.as_bytes()).await;
    }

    Ok(tag)
}

/// Returns the resolved sources for `yt-dlp`.
///
/// The URLs contain a `latest` placeholder that is replaced with a
/// concrete tag by [`resolve_latest_tag`] in [`super::resolve_tool_fetch`].
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "yt-dlp".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_macos"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
        ],
        total_items: 3,
    }
}
