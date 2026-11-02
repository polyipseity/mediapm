# Committed media fixture

`demo.mkv` is the small Matroska file the offline demo and the YouTube
regression run against, so their codec and tag assertions need no network and no
external probe. The pinned facts live in
`src/mediapm-test-support/src/media_fixture.rs`, which also holds the bounded
EBML reader the assertions use; `ffmetadata` parses ffmpeg's ffmetadata text
format, not Matroska, so it cannot read this file.

## Regeneration

The fixture was produced with this exact command (ffmpeg 8.1.1), from the
repository root:

```sh
ffmpeg -f lavfi -i testsrc2=size=320x180:rate=15:duration=10 -f lavfi -i anullsrc=r=44100:cl=mono -shortest -c:v libx264 -pix_fmt yuv420p -preset veryslow -crf 40 -c:a aac -b:a 24k -metadata title="mediapm fixture" -metadata artist="mediapm" -y src/mediapm/tests/fixtures/media/demo.mkv
```

`-crf 40` is the value the fixture was committed with. If a regeneration ever
lands at or above the ceiling below, raise `-crf` and record the value used in
this file.

## Size

The committed file is 61 330 bytes. It must stay under 1 048 576 bytes (1 MiB),
the value `MEDIA_FIXTURE_MAX_BYTES` pins and
`src/mediapm/tests/int/media_fixture.rs` asserts.

## Contents

- 10 s nominal duration (the muxed container runs a few milliseconds past it)
- 320x180 video, Matroska `CodecID` `V_MPEG4/ISO/AVC` (`libx264`)
- mono 44.1 kHz audio, Matroska `CodecID` `A_AAC`
- `title` = `mediapm fixture`, `ARTIST` = `mediapm`

## Consumers

- `src/mediapm/tests/int/media_fixture.rs` pins these facts.
- The offline demo and the YouTube regression read the same file through
  `mediapm_test_support::media_fixture::media_fixture_path`.
