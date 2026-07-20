//! Provider source definitions for `deno`.
//!
//! Sources are fetched from the deno GitHub releases page as zip archives.
//!
//! Canonical version: the resolved tag verbatim (e.g. "v2.2.12").

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

use crate::tools::downloader::ToolDownloadCache;

/// Resolves the latest tag for deno from GitHub releases.
pub(crate) async fn resolve_tag(
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<String, mediapm_conductor::ConductorError> {
    super::resolve_latest_github_tag("denoland", "deno", metadata_cache).await
}

/// Returns the resolved sources for `deno`.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "deno".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-pc-windows-msvc.zip"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/denoland/deno/releases/latest/download/deno-aarch64-apple-darwin.zip"
                            .to_string(),
                        "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-apple-darwin.zip"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/denoland/deno/releases/latest/download/deno-aarch64-unknown-linux-gnu.zip"
                            .to_string(),
                        "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
        ],
    }
}
