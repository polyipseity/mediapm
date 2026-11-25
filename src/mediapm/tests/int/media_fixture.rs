//! The committed media fixture the online demo's offline route asserts against.

use std::path::Path;

use mediapm_test_support::media_fixture::{
    MEDIA_FIXTURE_AUDIO_CODEC_ID, MEDIA_FIXTURE_DURATION_SECONDS, MEDIA_FIXTURE_FILE_NAME,
    MEDIA_FIXTURE_HEIGHT, MEDIA_FIXTURE_MAX_BYTES, MEDIA_FIXTURE_VIDEO_CODEC_ID,
    MEDIA_FIXTURE_WIDTH, media_fixture_path, read_matroska_facts,
};

/// The committed fixture states the codec and tag facts the demo's offline route asserts.
///
/// The online demo's offline route reads the file this test pins, so a
/// fixture that drifts in size, container, codec, or tags must fail here rather
/// than silently weaken its assertions. The tags and stream facts come from
/// the in-process EBML walk in `mediapm_test_support::media_fixture`;
/// `ffmetadata` parses ffmpeg's ffmetadata text format, not Matroska, so it
/// cannot read this file.
#[test]
fn media_fixture_states_what_the_demo_asserts() {
    let path = media_fixture_path(Path::new(env!("CARGO_MANIFEST_DIR")));
    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some(MEDIA_FIXTURE_FILE_NAME),
        "the fixture path must end in the pinned file name"
    );
    assert!(path.is_file(), "the fixture must be committed at '{}'", path.display());

    let bytes = std::fs::read(&path).expect("read the committed media fixture");
    let ceiling = usize::try_from(MEDIA_FIXTURE_MAX_BYTES).expect("fixture ceiling fits usize");
    assert!(
        bytes.len() < ceiling,
        "the fixture is {} bytes, not under the {ceiling}-byte ceiling",
        bytes.len()
    );
    assert!(
        bytes.starts_with(&[0x1A, 0x45, 0xDF, 0xA3]),
        "the fixture must begin with the EBML magic 1a 45 df a3"
    );

    let facts = read_matroska_facts(&path).expect("read the fixture's Matroska facts");
    assert_eq!(facts.title.as_deref(), Some("mediapm fixture"));
    assert_eq!(facts.artist.as_deref(), Some("mediapm"));
    assert_eq!(facts.video_codec_id.as_deref(), Some(MEDIA_FIXTURE_VIDEO_CODEC_ID));
    assert_eq!(facts.audio_codec_id.as_deref(), Some(MEDIA_FIXTURE_AUDIO_CODEC_ID));
    assert_eq!(facts.width, Some(MEDIA_FIXTURE_WIDTH));
    assert_eq!(facts.height, Some(MEDIA_FIXTURE_HEIGHT));
    let duration = facts.duration_seconds.expect("the fixture states its duration");
    assert!(
        (duration - MEDIA_FIXTURE_DURATION_SECONDS).abs() < 0.5,
        "the fixture runs {duration} s, not the {MEDIA_FIXTURE_DURATION_SECONDS} s the demo expects"
    );
}

/// The committed sample AV fixture carries the markers its recipe produces.
///
/// `src/mediapm/tests/fixtures/media/README.md` pins ffmpeg 8.1.1 and the exact
/// command that produced these bytes, and a re-run of that command reproduces
/// them byte for byte. The markers below are what that recipe writes and a
/// different recipe does not: the MP4 brand, the compatible-brand list the
/// muxer emits, both sample entry formats, the libavformat muxer string, and
/// the documented `comment` tag. Asserting them keeps the pin honest without
/// spawning ffmpeg, which CI does not guarantee at this version.
#[test]
fn the_sample_av_fixture_carries_its_recipe_markers() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/media/sample-av.mp4");
    let bytes = std::fs::read(&path).expect("read the committed sample AV fixture");
    let text = String::from_utf8_lossy(&bytes);

    // The first box is `ftyp` with the isom major brand and the compatible
    // brands the recipe's muxer writes; the sample entries below are what the
    // `-c:v libx264 -c:a aac` arguments produce.
    assert!(
        bytes.starts_with(&[0x00, 0x00, 0x00, 0x20, b'f', b't', b'y', b'p'])
            && text.contains("ftypisom"),
        "the fixture must open with a 32-byte ftyp box declaring the isom brand"
    );
    for marker in ["isomiso2avc1mp41", "avc1", "mp4a"] {
        assert!(
            text.contains(marker),
            "the fixture must carry '{marker}', which the documented recipe writes"
        );
    }
    // The library strings are the pins that a different ffmpeg cannot satisfy
    // silently: the muxer (`Lavf`) and the x264 encoder (`Lavc`, `x264 - core`)
    // versions are the ones the pinned 8.1.1 build ships.
    for marker in ["Lavf62.12.101", "Lavc62.28.101", "x264 - core"] {
        assert!(
            text.contains(marker),
            "the fixture must carry '{marker}'; a regeneration on another ffmpeg build \
             changes it, which is what makes the README's pin checkable"
        );
    }
    assert!(
        text.contains("mediapm sample fixture"),
        "the fixture must carry the documented `-metadata comment` value"
    );
}
