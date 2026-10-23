//! Provider source definitions for `yt-dlp`.
//!
//! Sources are fetched from the yt-dlp GitHub releases page as
//! standalone binaries (no archive extraction needed). The "latest"
//! tag is resolved via the GitHub API and cached in the metadata cache.
//!
//! Canonical version: the resolved commit hash (from GitHub API).

use mediapm_conductor::tools::provider::{
    ResolvedSource, ResolvedToolFetch, SourceProducer, host_arch_label,
};

use super::MetadataCacheTracker;

/// Resolves the "latest" tag for yt-dlp from GitHub releases.
///
/// Delegates to [`super::resolve_latest_github_tag`]. The metadata cache
/// key is the GitHub API endpoint URL — see the shared helper for details.
/// The caller must NOT call `touch()` on the metadata cache.
///
/// Returns `(tag, commit_hash, metadata_cached)` where `tag` is used for URL
/// substitution and `commit_hash` is the canonical version identifier.
/// `metadata_cached` is `true` when the result was served from the metadata
/// cache.
pub(crate) async fn resolve_latest_tag(
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
) -> Result<(String, String, bool), mediapm_conductor::ConductorError> {
    super::resolve_latest_github_tag("yt-dlp", "yt-dlp", metadata_cache).await
}

/// Returns the arch label for one OS slot.
///
/// yt-dlp's `yt-dlp_macos` binary is a universal2 build covering both
/// architectures, so it labels the host arch; the windows and linux
/// binaries are x86_64-only builds.
fn arch_for_os(os: &str) -> String {
    match os {
        "macos" => host_arch_label(),
        _ => "x86_64".to_string(),
    }
}

/// Returns the resolved sources for `yt-dlp`.
///
/// The URLs contain a `latest` placeholder that is replaced with a
/// concrete tag by [`resolve_latest_tag`] in [`super::resolve_tool_fetch`].
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "yt-dlp".to_string(),
        sources: vec![
            ResolvedSource {
                os: "windows".to_string(),
                arch: arch_for_os("windows"),
                producer: SourceProducer::Fetch {
                    urls: vec![format!(
                        "{}/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe",
                        super::github_download_base()
                    )],
                },
                expected_size: None,
                size_hint_bytes: Some(25_000_000),
                identity: None,
            },
            ResolvedSource {
                os: "macos".to_string(),
                arch: arch_for_os("macos"),
                producer: SourceProducer::Fetch {
                    urls: vec![format!(
                        "{}/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_macos",
                        super::github_download_base()
                    )],
                },
                expected_size: None,
                size_hint_bytes: Some(25_000_000),
                identity: None,
            },
            ResolvedSource {
                os: "linux".to_string(),
                arch: arch_for_os("linux"),
                producer: SourceProducer::Fetch {
                    urls: vec![format!(
                        "{}/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux",
                        super::github_download_base()
                    )],
                },
                expected_size: None,
                size_hint_bytes: Some(25_000_000),
                identity: None,
            },
        ],
    }
}

/// Builds the per-OS sources for one tag-pinned yt-dlp release.
///
/// yt-dlp asset names carry no version or arch token, so each OS slot
/// matches its exact asset name from the release's asset list. Refuses when
/// any platform slot has no matching asset — a pin that cannot be served is
/// a resolve error, not a silent fallback to latest.
///
/// # Errors
///
/// Returns [`mediapm_conductor::ConductorError`] when the pinned release
/// lacks the asset a platform slot needs.
pub(crate) fn sources_for_release(
    tag: &str,
    assets: &[String],
) -> Result<ResolvedToolFetch, mediapm_conductor::ConductorError> {
    let mut sources = Vec::with_capacity(3);
    for (os, pattern) in
        [("windows", "yt-dlp.exe"), ("macos", "yt-dlp_macos"), ("linux", "yt-dlp_linux")]
    {
        let name = assets
            .iter()
            .find(|a| a.as_str() == pattern)
            .ok_or_else(|| super::missing_release_asset_error("yt-dlp", tag, os, pattern))?;
        sources.push(ResolvedSource {
            os: os.to_string(),
            arch: arch_for_os(os),
            producer: SourceProducer::Fetch {
                urls: vec![super::github_release_asset_url("yt-dlp", "yt-dlp", tag, name)],
            },
            expected_size: None,
            size_hint_bytes: Some(25_000_000),
            identity: None,
        });
    }
    Ok(ResolvedToolFetch { tool_id: "yt-dlp".to_string(), sources })
}
