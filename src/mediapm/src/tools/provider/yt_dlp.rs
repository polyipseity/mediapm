//! Provider source definitions for `yt-dlp`.
//!
//! Sources are fetched from the yt-dlp GitHub releases page as
//! standalone binaries (no archive extraction needed).

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

/// Returns the resolved sources for `yt-dlp`.
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
