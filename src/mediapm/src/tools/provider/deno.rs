//! Provider source definitions for `deno`.
//!
//! Sources are fetched from the deno GitHub releases page as zip archives.
//!
//! Canonical version: the resolved commit hash (from GitHub API).

use mediapm_conductor::tools::provider::{
    ResolvedSource, ResolvedToolFetch, SourceProducer, host_arch_label,
};

use super::MetadataCacheTracker;

/// Resolves the latest tag for deno from GitHub releases.
///
/// Returns `(tag, commit_hash, metadata_cached)` where `tag` is used for URL
/// substitution and `commit_hash` is the canonical version identifier.
/// `metadata_cached` is `true` when the result was served from the metadata
/// cache.
pub(crate) async fn resolve_tag(
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
) -> Result<(String, String, bool), mediapm_conductor::ConductorError> {
    super::resolve_latest_github_tag("denoland", "deno", metadata_cache).await
}

/// Returns the per-OS asset slots as `(os, asset-name pattern, arch, size hint)`.
///
/// deno publishes `x86_64`-only Windows builds and both `x86_64` and `aarch64`
/// macOS/Linux builds. The macOS and Linux slots follow the host
/// architecture — exactly one candidate per OS, never a cross-arch
/// fallback list, so a fetch can never land on the wrong architecture.
fn os_slots() -> [(String, String, String, u64); 3] {
    let arch = host_arch_label();
    [
        (
            "windows".to_string(),
            "deno-x86_64-pc-windows-msvc.zip".to_string(),
            "x86_64".to_string(),
            40_000_000,
        ),
        ("macos".to_string(), format!("deno-{arch}-apple-darwin.zip"), arch.clone(), 40_000_000),
        ("linux".to_string(), format!("deno-{arch}-unknown-linux-gnu.zip"), arch, 40_000_000),
    ]
}

/// Returns the resolved sources for `deno`.
///
/// The URLs contain a `latest` placeholder that is replaced with a concrete
/// tag by [`resolve_tag`] in [`super::resolve_tool_fetch`].
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "deno".to_string(),
        sources: os_slots()
            .into_iter()
            .map(|(os, name, arch, hint)| ResolvedSource {
                os,
                arch,
                producer: SourceProducer::Fetch {
                    urls: vec![format!(
                        "https://github.com/denoland/deno/releases/latest/download/{name}"
                    )],
                },
                expected_size: None,
                size_hint_bytes: Some(hint),
            })
            .collect(),
    }
}

/// Builds the per-OS sources for one tag-pinned deno release.
///
/// Matches the release's asset list with the same per-platform name patterns
/// the [`Latest`](super::VersionSpec::Latest) templates use. Refuses when
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
    for (os, pattern, arch, hint) in os_slots() {
        let name = assets
            .iter()
            .find(|a| a.as_str() == pattern)
            .ok_or_else(|| super::missing_release_asset_error("deno", tag, &os, &pattern))?;
        sources.push(ResolvedSource {
            os,
            arch,
            producer: SourceProducer::Fetch {
                urls: vec![super::github_release_asset_url("denoland", "deno", tag, name)],
            },
            expected_size: None,
            size_hint_bytes: Some(hint),
        });
    }
    Ok(ResolvedToolFetch { tool_id: "deno".to_string(), sources })
}
