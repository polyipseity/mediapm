//! Committed media fixture support shared by the demo examples and integration tests.
//!
//! The offline demo and the `YouTube` regression run against a small Matroska file
//! committed at `src/mediapm/tests/fixtures/media/`, so their codec and tag
//! assertions need no network and no external probe. `ffmetadata` parses
//! ffmpeg's `;FFMETADATA1` text format, not Matroska, so the tags and stream
//! facts those assertions read come from the bounded EBML walk in this module
//! instead.
//!
//! This module lives in the test-support crate rather than in `mediapm` because
//! the fixture is test support: the shipped library needs no EBML reader, and
//! both consumers (the example and the integration tests) already link this
//! crate. The fixture itself stays under `mediapm`'s own tree, so
//! [`media_fixture_path`] resolves it against the **caller's** manifest dir.
//!
//! `src/mediapm/tests/fixtures/media/README.md` records the command that
//! regenerated the fixture, the codec settings it used, and the ceiling it must
//! stay under.

use std::path::{Path, PathBuf};

/// Fixture path relative to the consuming package's manifest dir.
pub const MEDIA_FIXTURE_RELATIVE_PATH: &str = "tests/fixtures/media/demo.mkv";

/// File name of the committed media fixture (the last component of
/// [`MEDIA_FIXTURE_RELATIVE_PATH`]).
pub const MEDIA_FIXTURE_FILE_NAME: &str = "demo.mkv";

/// Duration in seconds the fixture's generation command requests.
///
/// The muxed container runs a few milliseconds past this value, so an assertion
/// bounds the measured duration instead of pinning the millisecond.
pub const MEDIA_FIXTURE_DURATION_SECONDS: f64 = 10.0;

/// Encoded video width in pixels.
pub const MEDIA_FIXTURE_WIDTH: u64 = 320;

/// Encoded video height in pixels.
pub const MEDIA_FIXTURE_HEIGHT: u64 = 180;

/// Matroska `CodecID` of the fixture's video track (`libx264`).
pub const MEDIA_FIXTURE_VIDEO_CODEC_ID: &str = "V_MPEG4/ISO/AVC";

/// Matroska `CodecID` of the fixture's audio track (`aac`).
pub const MEDIA_FIXTURE_AUDIO_CODEC_ID: &str = "A_AAC";

/// Upper size bound in bytes the committed fixture must stay under (1 MiB).
pub const MEDIA_FIXTURE_MAX_BYTES: u64 = 1_048_576;

/// Maximum EBML nesting the walk follows before it refuses the file.
const MAX_EBML_DEPTH: usize = 8;

/// Maximum number of bytes the walk reads from one file.
const MAX_EBML_BYTES: usize = 16 * 1024 * 1024;

/// Matroska's default `TimestampScale` in nanoseconds (one millisecond).
const DEFAULT_TIMESTAMP_SCALE_NS: u64 = 1_000_000;

/// Nanoseconds in one second, the denominator of the `Duration` conversion.
const NANOS_PER_SECOND: f64 = 1e9;

const EBML_ID_SEGMENT: u64 = 0x1853_8067;
const EBML_ID_INFO: u64 = 0x1549_A966;
const EBML_ID_TIMESTAMP_SCALE: u64 = 0x2A_D7B1;
const EBML_ID_DURATION: u64 = 0x4489;
const EBML_ID_TITLE: u64 = 0x7BA9;
const EBML_ID_TRACKS: u64 = 0x1654_AE6B;
const EBML_ID_TRACK_ENTRY: u64 = 0xAE;
const EBML_ID_TRACK_TYPE: u64 = 0x83;
const EBML_ID_CODEC_ID: u64 = 0x86;
const EBML_ID_VIDEO: u64 = 0xE0;
const EBML_ID_PIXEL_WIDTH: u64 = 0xB0;
const EBML_ID_PIXEL_HEIGHT: u64 = 0xBA;
const EBML_ID_TAGS: u64 = 0x1254_C367;
const EBML_ID_TAG: u64 = 0x7373;
const EBML_ID_SIMPLE_TAG: u64 = 0x67C8;
const EBML_ID_TAG_NAME: u64 = 0x45A3;
const EBML_ID_TAG_STRING: u64 = 0x4487;

const TRACK_TYPE_VIDEO: u64 = 1;
const TRACK_TYPE_AUDIO: u64 = 2;

/// Resolves the committed media fixture against the caller's manifest dir.
///
/// `manifest_dir` is the **caller's** `CARGO_MANIFEST_DIR` — the package that
/// owns the fixture, not this support crate — because the fixture stays under
/// `mediapm`'s tree. Pass `Path::new(env!("CARGO_MANIFEST_DIR"))`.
#[must_use]
pub fn media_fixture_path(manifest_dir: &Path) -> PathBuf {
    manifest_dir.join(MEDIA_FIXTURE_RELATIVE_PATH)
}

/// The bounded set of Matroska facts the demo assertions read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MatroskaFacts {
    /// Segment title (`Info`/`Title`), when the file carries one.
    pub title: Option<String>,
    /// `ARTIST` simple tag, when the file carries one.
    pub artist: Option<String>,
    /// `CodecID` of the first video track.
    pub video_codec_id: Option<String>,
    /// `CodecID` of the first audio track.
    pub audio_codec_id: Option<String>,
    /// `PixelWidth` of the first video track.
    pub width: Option<u64>,
    /// `PixelHeight` of the first video track.
    pub height: Option<u64>,
    /// Segment duration in seconds, derived from `Duration` and `TimestampScale`.
    pub duration_seconds: Option<f64>,
    /// `TimestampScale` in nanoseconds, the unit `Duration` is expressed in.
    timestamp_scale_ns: u64,
    /// Raw `Duration` value in `TimestampScale` units.
    duration_units: Option<f64>,
}

/// Reads the bounded Matroska facts the demo assertions need from `path`.
///
/// The walk descends only the elements that carry those facts (`Segment`,
/// `Info`, `Tracks`, `Tags`) and skips every other element by its declared size,
/// so the cluster payload a fixture carries is never decoded. Nesting is capped
/// at [`MAX_EBML_DEPTH`] and the file at [`MAX_EBML_BYTES`], so a malformed file
/// fails instead of looping.
///
/// # Errors
///
/// Returns a message when the file cannot be read, exceeds [`MAX_EBML_BYTES`],
/// or holds EBML the walk cannot follow: a zero or over-long length marker, an
/// element whose declared size overruns its parent, nesting past the depth cap,
/// or a text element that is not UTF-8.
#[expect(
    clippy::cast_precision_loss,
    reason = "a Matroska TimestampScale is a small nanosecond count that f64 represents exactly"
)]
pub fn read_matroska_facts(path: &Path) -> Result<MatroskaFacts, String> {
    let bytes =
        std::fs::read(path).map_err(|error| format!("read media '{}': {error}", path.display()))?;
    if bytes.len() > MAX_EBML_BYTES {
        return Err(format!(
            "media '{}' is {} bytes, above the {MAX_EBML_BYTES}-byte EBML read cap",
            path.display(),
            bytes.len()
        ));
    }

    let mut facts = MatroskaFacts {
        timestamp_scale_ns: DEFAULT_TIMESTAMP_SCALE_NS,
        ..MatroskaFacts::default()
    };
    walk(&bytes, 0, bytes.len(), 0, &mut facts)?;
    facts.duration_seconds = facts
        .duration_units
        .map(|units| units * facts.timestamp_scale_ns as f64 / NANOS_PER_SECOND);
    Ok(facts)
}

/// One EBML element located within its parent.
#[derive(Debug)]
struct Element {
    /// The element's ID, marker bit included.
    id: u64,
    /// First byte of the element's payload.
    payload_start: usize,
    /// One past the last byte of the element's payload.
    payload_end: usize,
}

/// The `TrackEntry` fields the assertions need.
#[derive(Default)]
struct TrackEntry {
    /// `TrackType`: 1 is video, 2 is audio.
    track_type: Option<u64>,
    /// `CodecID`, read before `TrackType` in the file.
    codec_id: Option<String>,
    /// `PixelWidth` of a video track.
    width: Option<u64>,
    /// `PixelHeight` of a video track.
    height: Option<u64>,
}

impl MatroskaFacts {
    /// Records one track's codec and, for video, its dimensions.
    fn merge_track(&mut self, track: TrackEntry) {
        match track.track_type {
            Some(TRACK_TYPE_VIDEO) => {
                self.video_codec_id = track.codec_id;
                self.width = track.width;
                self.height = track.height;
            }
            Some(TRACK_TYPE_AUDIO) => self.audio_codec_id = track.codec_id,
            _ => {}
        }
    }
}

/// Walks the elements between `start` and `end`, descending the containers the
/// assertions read and skipping every other element by its declared size.
fn walk(
    bytes: &[u8],
    start: usize,
    end: usize,
    depth: usize,
    facts: &mut MatroskaFacts,
) -> Result<(), String> {
    if depth > MAX_EBML_DEPTH {
        return Err(format!("EBML nesting is deeper than the {MAX_EBML_DEPTH}-level cap"));
    }
    let mut pos = start;
    while pos < end {
        let element = next_element(bytes, pos, end)?;
        match element.id {
            EBML_ID_SEGMENT | EBML_ID_INFO | EBML_ID_TRACKS | EBML_ID_TAGS | EBML_ID_TAG => {
                walk(bytes, element.payload_start, element.payload_end, depth + 1, facts)?;
            }
            EBML_ID_TRACK_ENTRY => {
                let track = read_track_entry(bytes, element.payload_start, element.payload_end)?;
                facts.merge_track(track);
            }
            EBML_ID_SIMPLE_TAG => {
                let (name, value) =
                    read_simple_tag(bytes, element.payload_start, element.payload_end)?;
                if name.as_deref() == Some("ARTIST")
                    && let Some(value) = value
                {
                    facts.artist = Some(value);
                }
            }
            EBML_ID_TIMESTAMP_SCALE => {
                facts.timestamp_scale_ns =
                    read_uint(bytes, element.payload_start, element.payload_end);
            }
            EBML_ID_DURATION => {
                facts.duration_units =
                    Some(read_float(bytes, element.payload_start, element.payload_end)?);
            }
            EBML_ID_TITLE => {
                facts.title = Some(read_text(bytes, element.payload_start, element.payload_end)?);
            }
            _ => {}
        }
        pos = element.payload_end;
    }
    Ok(())
}

/// Reads one `TrackEntry`: its type, codec, and (for video) its dimensions.
fn read_track_entry(bytes: &[u8], start: usize, end: usize) -> Result<TrackEntry, String> {
    let mut track = TrackEntry::default();
    let mut pos = start;
    while pos < end {
        let element = next_element(bytes, pos, end)?;
        match element.id {
            EBML_ID_TRACK_TYPE => {
                track.track_type =
                    Some(read_uint(bytes, element.payload_start, element.payload_end));
            }
            EBML_ID_CODEC_ID => {
                track.codec_id =
                    Some(read_text(bytes, element.payload_start, element.payload_end)?);
            }
            EBML_ID_VIDEO => {
                let mut inner = element.payload_start;
                while inner < element.payload_end {
                    let leaf = next_element(bytes, inner, element.payload_end)?;
                    match leaf.id {
                        EBML_ID_PIXEL_WIDTH => {
                            track.width =
                                Some(read_uint(bytes, leaf.payload_start, leaf.payload_end));
                        }
                        EBML_ID_PIXEL_HEIGHT => {
                            track.height =
                                Some(read_uint(bytes, leaf.payload_start, leaf.payload_end));
                        }
                        _ => {}
                    }
                    inner = leaf.payload_end;
                }
            }
            _ => {}
        }
        pos = element.payload_end;
    }
    Ok(track)
}

/// Reads one `SimpleTag`'s name and string value.
fn read_simple_tag(
    bytes: &[u8],
    start: usize,
    end: usize,
) -> Result<(Option<String>, Option<String>), String> {
    let mut name = None;
    let mut value = None;
    let mut pos = start;
    while pos < end {
        let element = next_element(bytes, pos, end)?;
        match element.id {
            EBML_ID_TAG_NAME => {
                name = Some(read_text(bytes, element.payload_start, element.payload_end)?);
            }
            EBML_ID_TAG_STRING => {
                value = Some(read_text(bytes, element.payload_start, element.payload_end)?);
            }
            _ => {}
        }
        pos = element.payload_end;
    }
    Ok((name, value))
}

/// Locates the element at `pos`, whose declared size must fit inside `parent_end`.
fn next_element(bytes: &[u8], pos: usize, parent_end: usize) -> Result<Element, String> {
    let (id, id_len) = read_vint(bytes, pos, true)?;
    if id_len > 4 {
        return Err("EBML element id is longer than four bytes".to_string());
    }
    let (size, size_len) = read_vint(bytes, pos + id_len, false)?;
    let payload_start = pos + id_len + size_len;
    let payload_end = if size == (1u64 << (7 * size_len)) - 1 {
        parent_end
    } else {
        let size = usize::try_from(size).map_err(|_| {
            format!("EBML element 0x{id:X} declares a size that does not fit memory")
        })?;
        payload_start
            .checked_add(size)
            .filter(|end| *end <= parent_end)
            .ok_or_else(|| format!("EBML element 0x{id:X} overruns its parent"))?
    };
    Ok(Element { id, payload_start, payload_end })
}

/// Reads one EBML variable-length integer: `keep_marker` keeps the marker bit in
/// the value (element IDs) or strips it (element sizes).
fn read_vint(bytes: &[u8], pos: usize, keep_marker: bool) -> Result<(u64, usize), String> {
    let first = *bytes
        .get(pos)
        .ok_or_else(|| "EBML variable-length integer runs past the end of the data".to_string())?;
    if first == 0 {
        return Err("EBML variable-length integer has no length marker".to_string());
    }
    let mut length = 1usize;
    let mut marker = 0x80u8;
    while (first & marker) == 0 {
        marker >>= 1;
        length += 1;
    }
    if pos + length > bytes.len() {
        return Err("EBML variable-length integer runs past the end of the data".to_string());
    }
    let mut value = if keep_marker { 0u64 } else { u64::from(first & (marker - 1)) };
    let start = if keep_marker { pos } else { pos + 1 };
    for &byte in &bytes[start..pos + length] {
        value = (value << 8) | u64::from(byte);
    }
    Ok((value, length))
}

/// Reads an unsigned big-endian integer of any width.
fn read_uint(bytes: &[u8], start: usize, end: usize) -> u64 {
    let mut value = 0u64;
    for &byte in &bytes[start..end] {
        value = (value << 8) | u64::from(byte);
    }
    value
}

/// Reads a 4- or 8-byte big-endian IEEE float, the widths Matroska uses.
fn read_float(bytes: &[u8], start: usize, end: usize) -> Result<f64, String> {
    match end - start {
        4 => {
            let mut raw = [0u8; 4];
            raw.copy_from_slice(&bytes[start..end]);
            Ok(f64::from(f32::from_be_bytes(raw)))
        }
        8 => {
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&bytes[start..end]);
            Ok(f64::from_be_bytes(raw))
        }
        length => Err(format!("Matroska float element has unsupported length {length}")),
    }
}

/// Reads a UTF-8 text element.
fn read_text(bytes: &[u8], start: usize, end: usize) -> Result<String, String> {
    std::str::from_utf8(&bytes[start..end])
        .map(str::to_owned)
        .map_err(|error| format!("Matroska text element is not UTF-8: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The walk refuses a size that overruns its parent instead of looping.
    #[test]
    fn next_element_rejects_a_size_past_its_parent() {
        // Segment (0x18538067) declaring 0x10 bytes inside a 4-byte parent.
        let bytes = [0x18, 0x53, 0x80, 0x67, 0x90];
        let error = next_element(&bytes, 0, bytes.len()).expect_err("an overrun must fail");
        assert!(error.contains("overruns its parent"), "unexpected error: {error}");
    }

    /// A zero length marker is a malformed VINT, not a one-byte element.
    #[test]
    fn read_vint_rejects_a_zero_marker() {
        let error = read_vint(&[0x00], 0, true).expect_err("a zero marker must fail");
        assert!(error.contains("no length marker"), "unexpected error: {error}");
    }
}
