//! Playlist generation for all supported formats.
//!
//! Supports M3U8 (extended M3U), PLS, XSPF (XML Shareable Playlist Format),
//! WPL (Windows Media Player), and ASX (Advanced Stream Redirector).

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use crate::config::hierarchy_types::PlaylistFormat;
use crate::error::MediaPmError;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// One rendered playlist entry.
#[derive(Debug, Clone)]
pub(super) struct RenderedPlaylistEntry {
    /// Human-readable track or media id.
    pub(super) id: String,
    /// Relative or absolute path to the media file.
    pub(super) path: String,
}

/// Controls whether playlist entries use relative or absolute paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PlaylistEntryPathMode {
    /// Relative path mode (default).
    Relative,
    /// Absolute path mode.
    #[allow(dead_code)]
    Absolute,
}

// ---------------------------------------------------------------------------
// Playlist generation
// ---------------------------------------------------------------------------

/// Generates playlist file bytes in the requested format.
pub(super) fn generate_playlist_bytes(
    entries: &[RenderedPlaylistEntry],
    format: PlaylistFormat,
) -> Result<Vec<u8>, MediaPmError> {
    match format {
        PlaylistFormat::M3u8 => render_m3u8(entries),
        PlaylistFormat::Pls => render_pls(entries),
        PlaylistFormat::Xspf => render_xspf(entries),
        PlaylistFormat::Wpl => render_wpl(entries),
        PlaylistFormat::Asx => render_asx(entries),
    }
}

/// Resolves the relative path from a playlist file location to the media
/// entry target.
///
/// In relative mode, computes a `../`-based path from the playlist's parent
/// directory to the entry. In absolute mode, returns the entry path as-is.
#[must_use]
pub(super) fn resolve_playlist_target_relative_path(
    playlist_path: &str,
    entry_path: &str,
    path_mode: PlaylistEntryPathMode,
) -> PathBuf {
    match path_mode {
        PlaylistEntryPathMode::Absolute => PathBuf::from(entry_path),
        PlaylistEntryPathMode::Relative => {
            let playlist = Path::new(playlist_path);
            let parent = playlist.parent().unwrap_or(playlist);
            let entry = Path::new(entry_path);

            let parent_components: Vec<std::path::Component<'_>> = parent.components().collect();
            let entry_components: Vec<std::path::Component<'_>> = entry.components().collect();

            // Find the shared prefix length.
            let shared_prefix = parent_components
                .iter()
                .zip(entry_components.iter())
                .take_while(|(a, b)| a == b)
                .count();

            // Build relative path: "../" for each remaining parent component,
            // then the remaining entry components.
            let mut relative = PathBuf::new();
            for _ in shared_prefix..parent_components.len() {
                relative.push("..");
            }
            for component in &entry_components[shared_prefix..] {
                relative.push(component.as_os_str());
            }

            relative
        }
    }
}

// ---------------------------------------------------------------------------
// Format label
// ---------------------------------------------------------------------------

/// Returns a human-readable label for one playlist format.
#[must_use]
#[allow(dead_code)]
pub(super) fn playlist_format_label(format: PlaylistFormat) -> &'static str {
    match format {
        PlaylistFormat::M3u8 => "M3U8",
        PlaylistFormat::Pls => "PLS",
        PlaylistFormat::Xspf => "XSPF",
        PlaylistFormat::Wpl => "WPL",
        PlaylistFormat::Asx => "ASX",
    }
}

// ---------------------------------------------------------------------------
// XML escaping
// ---------------------------------------------------------------------------

/// Escapes special XML characters in a string.
fn escape_xml(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => result.push_str("&amp;"),
            '<' => result.push_str("&lt;"),
            '>' => result.push_str("&gt;"),
            '"' => result.push_str("&quot;"),
            '\'' => result.push_str("&apos;"),
            other => result.push(other),
        }
    }
    result
}

// ---------------------------------------------------------------------------
// M3U8 / M3U rendering
// ---------------------------------------------------------------------------

fn render_m3u8(entries: &[RenderedPlaylistEntry]) -> Result<Vec<u8>, MediaPmError> {
    let mut output = String::from("#EXTM3U\n");
    for entry in entries {
        let _ = writeln!(output, "#EXTINF:-1,{}\n{}", entry.id, entry.path);
    }
    Ok(output.into_bytes())
}

// ---------------------------------------------------------------------------
// PLS rendering
// ---------------------------------------------------------------------------

fn render_pls(entries: &[RenderedPlaylistEntry]) -> Result<Vec<u8>, MediaPmError> {
    let mut output = String::from("[playlist]\n");
    let _ = writeln!(output, "NumberOfEntries={}", entries.len());
    for (i, entry) in entries.iter().enumerate() {
        let num = i + 1;
        let _ = writeln!(output, "File{num}={}", entry.path);
        let _ = writeln!(output, "Title{num}={}", entry.id);
        let _ = writeln!(output, "Length{num}=-1");
    }
    output.push_str("Version=2\n");
    Ok(output.into_bytes())
}

// ---------------------------------------------------------------------------
// XSPF rendering
// ---------------------------------------------------------------------------

fn render_xspf(entries: &[RenderedPlaylistEntry]) -> Result<Vec<u8>, MediaPmError> {
    let mut output = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <playlist version=\"1\" xmlns=\"http://xspf.org/ns/0/\">\n\
         \x20 <trackList>\n",
    );
    for entry in entries {
        let _ = writeln!(
            output,
            "\x20\x20\x20<track>\n\
             \x20\x20\x20\x20<location>{}</location>\n\
             \x20\x20\x20\x20<title>{}</title>\n\
             \x20\x20\x20</track>",
            escape_xml(&entry.path),
            escape_xml(&entry.id),
        );
    }
    output.push_str("\x20 </trackList>\n</playlist>\n");
    Ok(output.into_bytes())
}

// ---------------------------------------------------------------------------
// WPL rendering
// ---------------------------------------------------------------------------

fn render_wpl(entries: &[RenderedPlaylistEntry]) -> Result<Vec<u8>, MediaPmError> {
    let mut output = String::from(
        "<?wpl version=\"1.0\"?>\n\
         <smil>\n\
         \x20 <head>\n\
         \x20\x20\x20<meta name=\"Generator\" content=\"mediapm\"/>\n\
         \x20 </head>\n\
         \x20 <body>\n\
         \x20\x20\x20<seq>\n",
    );
    for entry in entries {
        let _ = writeln!(output, "\x20\x20\x20\x20<media src=\"{}\"/>", escape_xml(&entry.path),);
    }
    output.push_str(
        "\x20\x20\x20</seq>\n\
         \x20 </body>\n\
         </smil>\n",
    );
    Ok(output.into_bytes())
}

// ---------------------------------------------------------------------------
// ASX rendering
// ---------------------------------------------------------------------------

fn render_asx(entries: &[RenderedPlaylistEntry]) -> Result<Vec<u8>, MediaPmError> {
    let mut output = String::from("<asx version=\"3.0\">\n");
    for entry in entries {
        let _ = writeln!(
            output,
            "\x20<entry>\n\
             \x20\x20<ref href=\"{}\"/>\n\
             \x20\x20<title>{}</title>\n\
             \x20</entry>",
            escape_xml(&entry.path),
            escape_xml(&entry.id),
        );
    }
    output.push_str("</asx>\n");
    Ok(output.into_bytes())
}
