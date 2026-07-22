//! Provider source definitions for `ffmpeg`.
//!
//! Sources are fetched from BtbN/FFmpeg-Builds (Windows, Linux) and
//! evermeet.cx (macOS). Each platform has two URL candidates tried in
//! order.
//!
//! Canonical version: the resolved tag verbatim (e.g. "L2025-07-15").

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

use crate::tools::downloader::ToolDownloadCache;

/// Resolves the latest tag for BtbN/FFmpeg-Builds.
///
/// Windows and Linux sources use BtbN GitHub releases; macOS uses
/// evermeet.cx (no resolution needed — the `getrelease/zip` endpoint
/// always serves the latest build).
pub(crate) async fn resolve_tag(
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<String, mediapm_conductor::ConductorError> {
    super::resolve_latest_github_tag("BtbN", "FFmpeg-Builds", metadata_cache).await
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
