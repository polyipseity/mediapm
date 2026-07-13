//! Provider source definitions for `rsgain`.
//!
//! Sources are fetched from the rsgain GitHub releases page as
//! zip (Windows, macOS) and tar.xz (Linux) archives.

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

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
                        "https://github.com/complexlogic/rsgain/releases/download/v3.7/rsgain-3.7-win64.zip"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/complexlogic/rsgain/releases/download/v3.7/rsgain-3.7-macOS-x86_64.zip"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![
                        "https://github.com/complexlogic/rsgain/releases/download/v3.7/rsgain-3.7-Linux.tar.xz"
                            .to_string(),
                    ],
                },
                expected_size: None,
            },
        ],
        total_items: 3,
    }
}
