//! Provider for the `export` builtin tool.

use super::{ResolvedSource, ResolvedToolFetch, SourceProducer};

/// Returns the resolved tool fetch for `export`.
#[must_use]
pub fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "export".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "export@v1".to_string() },
                expected_size: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "export@v1".to_string() },
                expected_size: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "export@v1".to_string() },
                expected_size: None,
            },
        ],
        total_items: 3,
    }
}
