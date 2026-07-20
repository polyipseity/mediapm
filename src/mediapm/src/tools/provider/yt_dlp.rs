//! Provider source definitions for `yt-dlp`.
//!
//! Sources are fetched from the yt-dlp GitHub releases page as
//! standalone binaries (no archive extraction needed). The "latest"
//! tag is resolved via the GitHub API and cached in the metadata cache.
//!
//! Canonical version: the resolved tag verbatim (e.g. "2025.07.15").

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

use crate::tools::downloader::ToolDownloadCache;

/// Resolves the "latest" tag for yt-dlp from GitHub releases.
///
/// Delegates to [`super::resolve_latest_github_tag`]. The metadata cache
/// key is the GitHub API endpoint URL — see the shared helper for details.
/// The caller must NOT call `touch()` on the metadata cache.
pub(crate) async fn resolve_latest_tag(
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<String, mediapm_conductor::ConductorError> {
    super::resolve_latest_github_tag("yt-dlp", "yt-dlp", metadata_cache).await
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
    }
}
