//! Provider source definitions for `rsgain`.
//!
//! Sources are fetched from the rsgain GitHub releases page as
//! zip (Windows, macOS) and tar.xz (Linux) archives. The latest
//! tag is resolved via the GitHub API.
//!
//! Canonical version: the resolved commit hash (from GitHub API).

use mediapm_conductor::tools::provider::{ResolvedSource, ResolvedToolFetch, SourceProducer};

use super::MetadataCacheTracker;

/// Resolves the latest tag for rsgain from GitHub releases.
///
/// Returns `(tag, commit_hash, metadata_cached)` where `tag` is used for URL
/// substitution and `commit_hash` is the canonical version identifier.
/// `metadata_cached` is `true` when the result was served from the metadata
/// cache.
pub(crate) async fn resolve_tag(
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
) -> Result<(String, String, bool), mediapm_conductor::ConductorError> {
    super::resolve_latest_github_tag("complexlogic", "rsgain", metadata_cache).await
}

/// Returns the per-OS asset slots as `(os, asset-name suffix, size hint)`.
///
/// rsgain publishes one artifact per OS and all three are `x86_64` builds, so
/// every source labels `x86_64`. Asset names embed the version
/// (`rsgain-3.7-win64.zip`), so pinned matching is by suffix.
fn os_slots() -> [(String, String, u64); 3] {
    [
        ("windows".to_string(), "-win64.zip".to_string(), 5_000_000),
        ("macos".to_string(), "-macOS-x86_64.zip".to_string(), 5_000_000),
        ("linux".to_string(), "-Linux.tar.xz".to_string(), 5_000_000),
    ]
}

/// Returns the resolved sources for `rsgain`.
///
/// The URLs contain an `rsgain-latest` token that is replaced with the
/// concrete version by [`resolve_tag`] in [`super::resolve_tool_fetch`].
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "rsgain".to_string(),
        sources: os_slots()
            .into_iter()
            .map(|(os, suffix, hint)| ResolvedSource {
                os,
                // WHY: rsgain ships a single x86_64 artifact per OS; there is
                // no aarch64 build to label.
                arch: "x86_64".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![format!(
                        "https://github.com/complexlogic/rsgain/releases/latest/download/rsgain-latest{suffix}"
                    )],
                },
                expected_size: None,
                size_hint_bytes: Some(hint),
                identity: None,
            })
            .collect(),
    }
}

/// Builds the per-OS sources for one tag-pinned rsgain release.
///
/// Matches the release's asset list with the same per-platform name patterns
/// the [`Latest`](super::VersionSpec::Latest) templates use (asset names
/// embed the version, so matching is by `rsgain-*{suffix}`); the matched
/// asset name carries the real versioned filename into the download URL.
/// Refuses when any platform slot has no matching asset — a pin that cannot
/// be served is a resolve error, not a silent fallback to latest.
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
    for (os, suffix, hint) in os_slots() {
        let name = assets
            .iter()
            .find(|a| a.starts_with("rsgain-") && a.ends_with(suffix.as_str()))
            .ok_or_else(|| {
                super::missing_release_asset_error("rsgain", tag, &os, &format!("rsgain-*{suffix}"))
            })?;
        sources.push(ResolvedSource {
            os,
            // WHY: rsgain ships a single x86_64 artifact per OS; there is no
            // aarch64 build to label.
            arch: "x86_64".to_string(),
            producer: SourceProducer::Fetch {
                urls: vec![super::github_release_asset_url("complexlogic", "rsgain", tag, name)],
            },
            expected_size: None,
            size_hint_bytes: Some(hint),
            identity: None,
        });
    }
    Ok(ResolvedToolFetch { tool_id: "rsgain".to_string(), sources })
}
