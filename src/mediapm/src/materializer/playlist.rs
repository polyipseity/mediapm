//! Playlist generation for all supported formats.
//!
//! Supports M3U8 (extended M3U), PLS, XSPF (XML Shareable Playlist Format),
//! WPL (Windows Media Player), and ASX (Advanced Stream Redirector).

use std::fmt::Write as _;

use crate::config::hierarchy_types::PlaylistFormat;

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
    Absolute,
}

/// Generates playlist file bytes in the requested format.
pub(super) fn generate_playlist_bytes(
    entries: &[RenderedPlaylistEntry],
    format: PlaylistFormat,
) -> Vec<u8> {
    match format {
        PlaylistFormat::M3u8 => render_m3u8(entries),
        PlaylistFormat::Pls => render_pls(entries),
        PlaylistFormat::Xspf => render_xspf(entries),
        PlaylistFormat::Wpl => render_wpl(entries),
        PlaylistFormat::Asx => render_asx(entries),
    }
}

/// Resolves the path a playlist entry records, always separated by `/`.
///
/// In relative mode, computes a `../`-based path from the playlist's parent
/// directory to the entry. In absolute mode, returns the entry path as-is.
///
/// The result is a string rather than a `PathBuf` because three of the five
/// formats carry URI references — XSPF `<location>`, WPL `<media src>`, ASX
/// `<ref href>` — where `\` is not a separator. M3U8 and PLS readers accept `/`
/// on Windows too, so one rendering is correct in all five formats and the
/// written bytes no longer depend on the host.
#[must_use]
pub(super) fn resolve_playlist_target_relative_path(
    playlist_path: &str,
    entry_path: &str,
    path_mode: PlaylistEntryPathMode,
) -> String {
    match path_mode {
        PlaylistEntryPathMode::Absolute => entry_path.to_string(),
        PlaylistEntryPathMode::Relative => {
            let playlist_components: Vec<&str> = playlist_path.split(['/', '\\']).collect();
            let entry_components: Vec<&str> = entry_path.split(['/', '\\']).collect();
            // The file name is not a directory: relative paths start from the
            // playlist's parent. A playlist named by one component has no
            // parent components, and neither does an empty name, so the slice
            // is taken only when there is a file name to drop.
            let parent_components = if playlist_components.len() > 1 {
                &playlist_components[..playlist_components.len() - 1]
            } else {
                &[][..]
            };

            let shared_prefix = parent_components
                .iter()
                .zip(entry_components.iter())
                .take_while(|(a, b)| a == b)
                .count();

            let mut segments: Vec<&str> = Vec::new();
            segments.extend(std::iter::repeat_n("..", parent_components.len() - shared_prefix));
            segments.extend(entry_components[shared_prefix..].iter().copied());
            segments.join("/")
        }
    }
}

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

fn render_m3u8(entries: &[RenderedPlaylistEntry]) -> Vec<u8> {
    let mut output = String::from("#EXTM3U\n");
    for entry in entries {
        let _ = writeln!(output, "#EXTINF:-1,{}\n{}", entry.id, entry.path);
    }
    output.into_bytes()
}

fn render_pls(entries: &[RenderedPlaylistEntry]) -> Vec<u8> {
    let mut output = String::from("[playlist]\n");
    let _ = writeln!(output, "NumberOfEntries={}", entries.len());
    for (i, entry) in entries.iter().enumerate() {
        let num = i + 1;
        let _ = writeln!(output, "File{num}={}", entry.path);
        let _ = writeln!(output, "Title{num}={}", entry.id);
        let _ = writeln!(output, "Length{num}=-1");
    }
    output.push_str("Version=2\n");
    output.into_bytes()
}

fn render_xspf(entries: &[RenderedPlaylistEntry]) -> Vec<u8> {
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
    output.into_bytes()
}

fn render_wpl(entries: &[RenderedPlaylistEntry]) -> Vec<u8> {
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
        let _ = writeln!(output, "\x20\x20\x20\x20<media src=\"{}\"/>", escape_xml(&entry.path));
    }
    output.push_str(
        "\x20\x20\x20</seq>\n\
         \x20 </body>\n\
         </smil>\n",
    );
    output.into_bytes()
}

fn render_asx(entries: &[RenderedPlaylistEntry]) -> Vec<u8> {
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
    output.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single_entry() -> RenderedPlaylistEntry {
        RenderedPlaylistEntry { id: "track1".to_string(), path: "dir/track1.mp3".to_string() }
    }

    #[test]
    fn m3u8_renders() {
        let entries = &[single_entry()];
        let output = generate_playlist_bytes(entries, PlaylistFormat::M3u8);
        let s = String::from_utf8(output).unwrap();
        assert!(s.contains("#EXTM3U"));
        assert!(s.contains("#EXTINF:-1,track1"));
        assert!(s.contains("dir/track1.mp3"));
    }

    #[test]
    fn pls_renders() {
        let entries = &[single_entry()];
        let output = generate_playlist_bytes(entries, PlaylistFormat::Pls);
        let s = String::from_utf8(output).unwrap();
        assert!(s.contains("[playlist]"));
        assert!(s.contains("NumberOfEntries=1"));
        assert!(s.contains("File1=dir/track1.mp3"));
        assert!(s.contains("Title1=track1"));
        assert!(s.contains("Version=2"));
    }

    #[test]
    fn xspf_renders() {
        let entries = &[single_entry()];
        let output = generate_playlist_bytes(entries, PlaylistFormat::Xspf);
        let s = String::from_utf8(output).unwrap();
        assert!(s.contains("<?xml"));
        assert!(s.contains("<playlist"));
        assert!(s.contains("<trackList>"));
        assert!(s.contains("<location>dir/track1.mp3</location>"));
        assert!(s.contains("<title>track1</title>"));
    }

    #[test]
    fn wpl_renders() {
        let entries = &[single_entry()];
        let output = generate_playlist_bytes(entries, PlaylistFormat::Wpl);
        let s = String::from_utf8(output).unwrap();
        assert!(s.contains("<?wpl"));
        assert!(s.contains("<smil>"));
        assert!(s.contains("<seq>"));
        assert!(s.contains("<media src=\"dir/track1.mp3\"/>"));
    }

    #[test]
    fn asx_renders() {
        let entries = &[single_entry()];
        let output = generate_playlist_bytes(entries, PlaylistFormat::Asx);
        let s = String::from_utf8(output).unwrap();
        assert!(s.contains("<asx"));
        assert!(s.contains("<entry>"));
        assert!(s.contains("<ref href=\"dir/track1.mp3\"/>"));
    }

    #[test]
    fn empty_entries() {
        let output = generate_playlist_bytes(&[], PlaylistFormat::M3u8);
        let s = String::from_utf8(output).unwrap();
        assert!(s.contains("#EXTM3U"));
        assert!(!s.contains("#EXTINF"));
    }

    #[test]
    fn multiple_entries() {
        let entries = &[
            RenderedPlaylistEntry { id: "a".to_string(), path: "p1.mp3".to_string() },
            RenderedPlaylistEntry { id: "b".to_string(), path: "p2.mp3".to_string() },
            RenderedPlaylistEntry { id: "c".to_string(), path: "p3.mp3".to_string() },
        ];
        let output = generate_playlist_bytes(entries, PlaylistFormat::Pls);
        let s = String::from_utf8(output).unwrap();
        assert!(s.contains("NumberOfEntries=3"));
        assert!(s.contains("File1=p1.mp3"));
        assert!(s.contains("File2=p2.mp3"));
        assert!(s.contains("File3=p3.mp3"));
    }
}
