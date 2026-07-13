//! Managed-tool provider source definitions.
//!
//! Each per-tool module defines a `sources()` function returning
//! [`ResolvedToolFetch`] describing where and how to fetch the tool
//! binary for each target platform.
//!
//! The dispatcher [`resolve_tool_fetch`] routes tool names to the
//! appropriate per-tool module.

pub(crate) mod deno;
pub(crate) mod ffmpeg;
pub(crate) mod media_tagger;
pub(crate) mod rsgain;
pub(crate) mod sd;
pub(crate) mod yt_dlp;

use mediapm_conductor::tools::provider::{ResolvedToolFetch, VersionSpec};
use mediapm_utils::progress::ProviderProgressCallback;

/// Resolves source descriptors for the named managed tool.
///
/// # Errors
///
/// Returns an error when the tool name is not recognised.
pub(crate) async fn resolve_tool_fetch(
    tool_name: &str,
    _version: Option<VersionSpec>,
    _progress_cb: Option<ProviderProgressCallback>,
) -> Result<ResolvedToolFetch, mediapm_conductor::ConductorError> {
    match tool_name {
        n if n.eq_ignore_ascii_case("ffmpeg") => Ok(ffmpeg::sources()),
        n if n.eq_ignore_ascii_case("yt-dlp") => Ok(yt_dlp::sources()),
        n if n.eq_ignore_ascii_case("deno") => Ok(deno::sources()),
        n if n.eq_ignore_ascii_case("rsgain") => Ok(rsgain::sources()),
        n if n.eq_ignore_ascii_case("media-tagger") => Ok(media_tagger::sources()),
        n if n.eq_ignore_ascii_case("sd") => Ok(sd::sources()),
        _ => Err(mediapm_conductor::ConductorError::Workflow(format!(
            "tool {tool_name}: no provider registered for resolution"
        ))),
    }
}
