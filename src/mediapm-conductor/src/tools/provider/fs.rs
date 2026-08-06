//! Provider for the `fs` builtin tool.

use super::{ResolvedSource, ResolvedToolFetch, SourceProducer};

/// Returns the resolved tool fetch for `fs`.
#[must_use]
pub fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "fs".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::launcher("fs@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::launcher("fs@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::launcher("fs@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
        ],
    }
}
