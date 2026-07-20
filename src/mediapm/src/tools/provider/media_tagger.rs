//! Provider source definitions for `media-tagger`.
//!
//! `media-tagger` is a builtin launcher tool shipped with mediapm itself.
//! It uses `GenerateLauncher` sources (like the echo/fs/archive builtins)
//! so the standard 3-phase provisioning pipeline generates the launcher
//! script via the conductor's `generate_launcher_script`.
//!
//! No canonical version (no external sources).

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

/// Returns per-OS `GenerateLauncher` sources for the media-tagger builtin.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "media-tagger".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "media-tagger".into() },
                expected_size: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "media-tagger".into() },
                expected_size: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                producer: SourceProducer::GenerateLauncher { builtin_id: "media-tagger".into() },
                expected_size: None,
            },
        ],
    }
}
