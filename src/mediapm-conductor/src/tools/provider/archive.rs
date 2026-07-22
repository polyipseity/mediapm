//! Provider for the `archive` builtin tool.

use super::{ResolvedSource, ResolvedToolFetch, SourceProducer};

/// Returns the resolved tool fetch for `archive`.
#[must_use]
pub fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "archive".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "archive@v1".to_string() },
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "archive@v1".to_string() },
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "archive@v1".to_string() },
                expected_size: None,
                size_hint_bytes: None,
            },
        ],
    }
}
