//! Provider for the `sd` managed tool.

use super::{ResolvedSource, ResolvedToolFetch, SourceProducer};

/// Returns the resolved tool fetch for `sd`.
#[must_use]
pub fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "sd".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec!["https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-x86_64-pc-windows-msvc.zip".to_string()],
                },
                expected_size: None,
                size_hint_bytes: Some(358_000),
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec!["https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-aarch64-apple-darwin.tar.gz".to_string()],
                },
                expected_size: None,
                size_hint_bytes: Some(490_000),
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec!["https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-x86_64-unknown-linux-gnu.tar.gz".to_string()],
                },
                expected_size: None,
                size_hint_bytes: Some(480_000),
            },
        ],
    }
}
