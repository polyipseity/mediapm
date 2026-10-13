//! Provider source definitions for `sd`.
//!
//! Sources are fetched from the sd GitHub releases page as zip/tar.gz archives.
//! The latest tag is resolved via the GitHub API.
//!
//! Canonical version: the resolved commit hash (from GitHub API).

use mediapm_conductor::tools::provider::{
    ResolvedSource, ResolvedToolFetch, SourceProducer, host_arch_label,
};

use super::MetadataCacheTracker;

/// Resolves the latest tag for sd from GitHub releases.
///
/// Returns `(tag, commit_hash, metadata_cached)` where `tag` is used for URL
/// substitution and `commit_hash` is the canonical version identifier.
/// `metadata_cached` is `true` when the result was served from the metadata
/// cache.
pub(crate) async fn resolve_tag(
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
) -> Result<(String, String, bool), mediapm_conductor::ConductorError> {
    super::resolve_latest_github_tag("chmln", "sd", metadata_cache).await
}

/// Returns the per-OS asset slots as `(os, asset-name suffix, arch, size hint)`.
///
/// sd publishes `x86_64`-only Windows and Linux builds and both `x86_64` and
/// `aarch64` macOS builds. The macOS slot follows the host architecture —
/// exactly one candidate per OS, never a cross-arch fallback list, so a
/// fetch can never land on the wrong architecture. Asset names embed the
/// version (`sd-v1.1.0-...`), so pinned matching is by suffix.
fn os_slots() -> [(String, String, String, u64); 3] {
    let arch = host_arch_label();
    [
        (
            "windows".to_string(),
            "-x86_64-pc-windows-msvc.zip".to_string(),
            "x86_64".to_string(),
            358_000,
        ),
        ("macos".to_string(), format!("-{arch}-apple-darwin.tar.gz"), arch, 490_000),
        (
            "linux".to_string(),
            "-x86_64-unknown-linux-gnu.tar.gz".to_string(),
            "x86_64".to_string(),
            480_000,
        ),
    ]
}

/// Returns the resolved sources for `sd`.
///
/// The URLs contain an `sd-latest` token that is replaced with the concrete
/// tag by [`resolve_tag`] in [`super::resolve_tool_fetch`].
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch {
        tool_id: "sd".to_string(),
        sources: os_slots()
            .into_iter()
            .map(|(os, suffix, arch, hint)| ResolvedSource {
                os,
                arch,
                producer: SourceProducer::Fetch {
                    urls: vec![format!(
                        "https://github.com/chmln/sd/releases/latest/download/sd-latest{suffix}"
                    )],
                },
                expected_size: None,
                size_hint_bytes: Some(hint),
            })
            .collect(),
    }
}

/// Builds the per-OS sources for one tag-pinned sd release.
///
/// Matches the release's asset list with the same per-platform name patterns
/// the [`Latest`](super::VersionSpec::Latest) templates use (asset names
/// embed the version, so matching is by `sd-*{suffix}`); the matched asset
/// name carries the real versioned filename into the download URL. Refuses
/// when any platform slot has no matching asset — a pin that cannot be
/// served is a resolve error, not a silent fallback to latest.
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
    for (os, suffix, arch, hint) in os_slots() {
        let name = assets
            .iter()
            .find(|a| a.starts_with("sd-") && a.ends_with(suffix.as_str()))
            .ok_or_else(|| {
                super::missing_release_asset_error("sd", tag, &os, &format!("sd-*{suffix}"))
            })?;
        sources.push(ResolvedSource {
            os,
            arch,
            producer: SourceProducer::Fetch {
                urls: vec![super::github_release_asset_url("chmln", "sd", tag, name)],
            },
            expected_size: None,
            size_hint_bytes: Some(hint),
        });
    }
    Ok(ResolvedToolFetch { tool_id: "sd".to_string(), sources })
}
