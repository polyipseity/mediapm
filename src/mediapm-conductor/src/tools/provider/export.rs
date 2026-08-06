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
                producer: SourceProducer::launcher("export@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::launcher("export@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::launcher("export@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
        ],
    }
}
