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
                producer: SourceProducer::launcher("echo@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::launcher("echo@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::launcher("echo@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
        ],
    }
}
