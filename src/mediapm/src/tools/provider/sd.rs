//! Provider source definitions for `sd`.
//!
//! Sources are fetched from the sd GitHub releases page as zip/tar.gz archives.
//! The latest tag is resolved via the GitHub API.
//!
//! Canonical version: the resolved commit hash (from GitHub API).

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

use crate::tools::downloader::ToolDownloadCache;

/// Resolves the latest tag for sd from GitHub releases.
///
/// Returns `(tag, commit_hash)` where `tag` is used for URL substitution
/// and `commit_hash` is the canonical version identifier.
pub(crate) async fn resolve_tag(
    metadata_cache: Option<&ToolDownloadCache>,
) -> Result<(String, String), mediapm_conductor::ConductorError> {
    super::resolve_latest_github_tag("chmln", "sd", metadata_cache).await
}

/// Returns the resolved sources for `sd`.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "sd".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/chmln/sd/releases/latest/download/sd-latest-x86_64-pc-windows-msvc.zip"
                            .to_string(),
                    ],
                },
                expected_size: None,
                size_hint_bytes: Some(358_000),
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/chmln/sd/releases/latest/download/sd-latest-aarch64-apple-darwin.tar.gz"
                            .to_string(),
                        "https://github.com/chmln/sd/releases/latest/download/sd-latest-x86_64-apple-darwin.tar.gz"
                            .to_string(),
                    ],
                },
                expected_size: None,
                size_hint_bytes: Some(490_000),
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/chmln/sd/releases/latest/download/sd-latest-x86_64-unknown-linux-gnu.tar.gz"
                            .to_string(),
                    ],
                },
                expected_size: None,
                size_hint_bytes: Some(480_000),
            },
        ],
    }
}
