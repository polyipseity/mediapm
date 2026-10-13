//! Provider for the `import` builtin tool.

use super::{ResolvedSource, ResolvedToolFetch, SourceProducer, host_arch_label};

/// Returns the resolved tool fetch for `import`.
#[must_use]
pub fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "import".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                // Launcher scripts are arch-independent; they label the host arch.
                arch: host_arch_label(),
                producer: SourceProducer::launcher("import@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                // Launcher scripts are arch-independent; they label the host arch.
                arch: host_arch_label(),
                producer: SourceProducer::launcher("import@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                // Launcher scripts are arch-independent; they label the host arch.
                arch: host_arch_label(),
                producer: SourceProducer::launcher("import@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
        ],
    }
}
