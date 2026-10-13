//! Provider for the `echo` builtin tool.

use super::{ResolvedSource, ResolvedToolFetch, SourceProducer, host_arch_label};

/// Returns the resolved tool fetch for `echo`.
#[must_use]
pub fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "echo".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                // Launcher scripts are arch-independent; they label the host arch.
                arch: host_arch_label(),
                producer: SourceProducer::launcher("echo@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                // Launcher scripts are arch-independent; they label the host arch.
                arch: host_arch_label(),
                producer: SourceProducer::launcher("echo@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                // Launcher scripts are arch-independent; they label the host arch.
                arch: host_arch_label(),
                producer: SourceProducer::launcher("echo@v1"),
                expected_size: None,
                size_hint_bytes: None,
            },
        ],
    }
}
