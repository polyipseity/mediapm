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

use mediapm_conductor::tools::provider::ResolvedToolFetch;

/// Resolves source descriptors for the named managed tool.
///
/// # Errors
///
/// Returns an error when the tool name is not recognised.
pub(crate) async fn resolve_tool_fetch(
    tool_name: &str,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_tool_fetch_routes_all_tools() {
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "media-tagger", "sd"] {
            let result = resolve_tool_fetch(name).await;
            assert!(result.is_ok(), "tool {name}: resolve should succeed");
            let fetch = result.unwrap();
            assert_eq!(fetch.tool_id, *name, "tool_id should match input name");
            if *name == "media-tagger" {
                // media-tagger is an internal launcher with no external sources.
                assert!(fetch.sources.is_empty(), "tool {name}: should have zero sources");
            } else {
                assert!(!fetch.sources.is_empty(), "tool {name}: should have at least one source");
            }
        }
    }

    #[tokio::test]
    async fn resolve_tool_fetch_rejects_unknown() {
        let result = resolve_tool_fetch("no-such-tool").await;
        assert!(result.is_err(), "unknown tool should return error");
    }

    #[tokio::test]
    async fn resolve_tool_fetch_each_fetched_tool_has_three_os_entries() {
        // media-tagger is an internal launcher — no external sources.
        let expected_oses = ["windows", "linux", "macos"];
        for name in &["ffmpeg", "yt-dlp", "deno", "rsgain", "sd"] {
            let fetch = resolve_tool_fetch(name).await.unwrap();
            let oses: Vec<&str> = fetch.sources.iter().map(|s| s.os.as_str()).collect();
            for expected_os in &expected_oses {
                assert!(
                    oses.contains(expected_os),
                    "tool {name}: missing source for OS {expected_os}; found OSes: {oses:?}"
                );
            }
        }
    }
}
