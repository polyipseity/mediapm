//! Provider for the conductor `import` builtin tool.

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

/// Returns the resolved tool fetch for `import`.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "import".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::launcher("import@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::launcher("import@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::launcher("import@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
        ],
    }
}
