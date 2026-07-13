//! Provider for the `import` builtin tool.

use super::{ResolvedSource, ResolvedToolFetch, SourceProducer};

/// Returns the resolved tool fetch for `import`.
#[must_use]
pub fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "import".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "import@v1".to_string() },
                expected_size: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "import@v1".to_string() },
                expected_size: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "import@v1".to_string() },
                expected_size: None,
            },
        ],
        total_items: 3,
    }
}
