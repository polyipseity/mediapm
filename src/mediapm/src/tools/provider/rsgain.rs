//! Provider source definitions for `rsgain`.
//!
//! Sources are fetched from the rsgain GitHub releases page as
//! zip (Windows, macOS) and tar.xz (Linux) archives. The latest
//! tag is resolved via the GitHub API.
//!
//! Canonical version: the resolved tag verbatim (e.g. "v3.7").

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

use crate::tools::downloader::ToolDownloadCache;

/// Resolves the latest tag for rsgain from GitHub releases.
pub(crate) async fn resolve_tag(
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<String, mediapm_conductor::ConductorError> {
    super::resolve_latest_github_tag("complexlogic", "rsgain", metadata_cache).await
}

/// Returns the resolved sources for `rsgain`.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "rsgain".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/complexlogic/rsgain/releases/latest/download/rsgain-latest-win64.zip"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/complexlogic/rsgain/releases/latest/download/rsgain-latest-macOS-x86_64.zip"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/complexlogic/rsgain/releases/latest/download/rsgain-latest-Linux.tar.xz"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
        ],
    }
}
