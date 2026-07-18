//! Provider for the `echo` builtin tool.

use super::{ResolvedSource, ResolvedToolFetch, SourceProducer};

/// Returns the resolved tool fetch for `echo`.
#[must_use]
pub fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "echo".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "echo@v1".to_string() },
                expected_size: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "echo@v1".to_string() },
                expected_size: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "echo@v1".to_string() },
                expected_size: None,
            },
        ],
    }
}
