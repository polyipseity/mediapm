//! Provider source definitions for `ffmpeg`.
//!
//! Sources are fetched from BtbN/FFmpeg-Builds (Windows, Linux) and
//! evermeet.cx (macOS). Each platform has two URL candidates tried in
//! order.
//!
//! Canonical version: composite of `autobuild-{tag}` from BtbN and
//! `evermeet-{semver}` from evermeet.cx (e.g. "autobuild-2026-07-22-13-36+evermeet-8.1.2").

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

use crate::tools::downloader::ToolDownloadCache;

/// Resolves the latest `autobuild-*` tag for BtbN/FFmpeg-Builds.
///
/// BtbN's `/releases/latest` endpoint returns `"tag_name":"latest"`, which
/// is useless. This function lists recent releases and picks the first
/// `autobuild-*` tag.
pub(crate) async fn resolve_btbn_tag(
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<String, mediapm_conductor::ConductorError> {
    super::resolve_latest_autobuild_tag("BtbN", "FFmpeg-Builds", metadata_cache).await
}

/// Resolves the current ffmpeg version from evermeet.cx via HEAD redirect.
///
/// Makes a `HEAD` request to `https://evermeet.cx/ffmpeg/getrelease/zip` and
/// extracts the semver from the `Location` header (e.g. `.../ffmpeg-8.1.2.zip`).
///
/// # Errors
///
/// Returns [`mediapm_conductor::ConductorError`] when the HTTP request or
/// header parsing fails.
pub(crate) async fn resolve_evermeet_version(
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<String, mediapm_conductor::ConductorError> {
    let url = "https://evermeet.cx/ffmpeg/getrelease/zip";

    // Cache lookup.
    if let Some(cache) = metadata_cache {
        if let Some(bytes) = cache.lookup_bytes(url).await {
            if let Ok(version) = String::from_utf8(bytes.to_vec()) {
                if !version.is_empty() {
                    return Ok(version);
                }
            }
        }
    }

    // HEAD request — no body download.
    let http_client = crate::http_client::shared_http_client().map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!("HTTP client unavailable: {e}"))
    })?;
    let response = http_client.head(url).send().await.map_err(|e| {
        mediapm_conductor::ConductorError::Workflow(format!("evermeet HEAD request failed: {e}"))
    })?;

    // Extract from Location header: "https://.../ffmpeg-{version}.zip"
    let location =
        response.headers().get("location").and_then(|v| v.to_str().ok()).ok_or_else(|| {
            mediapm_conductor::ConductorError::Workflow(
                "evermeet response missing Location header".to_string(),
            )
        })?;
    let version = location
        .rsplit('/')
        .next()
        .and_then(|filename| filename.strip_prefix("ffmpeg-"))
        .and_then(|filename| filename.strip_suffix(".zip"))
        .ok_or_else(|| {
            mediapm_conductor::ConductorError::Workflow(format!(
                "could not parse version from Location: {location}"
            ))
        })?;
    let version = version.to_string();

    // Cache.
    if let Some(cache) = metadata_cache {
        cache.store_bytes(url, version.as_bytes()).await;
    }

    Ok(version)
}

/// Returns the resolved sources for `ffmpeg`.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "ffmpeg".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl-shared.zip".to_string(),
                        "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl.zip".to_string(),
                    ],
                },
                expected_size: None,
                size_hint_bytes: Some(120_000_000),
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://evermeet.cx/ffmpeg/getrelease/zip".to_string(),
                    ],
                },
                expected_size: None,
                size_hint_bytes: Some(45_000_000),
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl-shared.tar.xz".to_string(),
                        "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl.tar.xz".to_string(),
                    ],
                },
                expected_size: None,
                size_hint_bytes: Some(80_000_000),
            },
        ],
    }
}
