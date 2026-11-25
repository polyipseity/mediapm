# Committed media fixtures

Small committed media files used by examples and tests. Keep each fixture
small, document its pinned facts, and record the exact command that produces
the committed bytes.

## `demo.mkv`

`demo.mkv` is the small Matroska file the online demo's offline (stub-chain)
route runs against, so its codec and tag assertions need no network and no
external probe. The pinned facts live in
`src/mediapm-test-support/src/media_fixture.rs`, which also holds the bounded
EBML reader the assertions use; `ffmetadata` parses ffmpeg's ffmetadata text
format, not Matroska, so it cannot read this file.

### Regeneration

The fixture was produced with this exact command (ffmpeg 8.1.1), from the
repository root:

```sh
ffmpeg -f lavfi -i testsrc2=size=320x180:rate=15:duration=10 -f lavfi -i anullsrc=r=44100:cl=mono -shortest -c:v libx264 -pix_fmt yuv420p -preset veryslow -crf 40 -c:a aac -b:a 24k -metadata title="mediapm fixture" -metadata artist="mediapm" -y src/mediapm/tests/fixtures/media/demo.mkv
```

`-crf 40` is the value the fixture was committed with. If a regeneration ever
lands at or above the ceiling below, raise `-crf` and record the value used in
this file.

### Size

The committed file is 61 330 bytes. It must stay under 1 048 576 bytes (1 MiB),
the value `MEDIA_FIXTURE_MAX_BYTES` pins and
`src/mediapm/tests/int/media_fixture.rs` asserts.

### Contents

- 10 s nominal duration (the muxed container runs a few milliseconds past it)
- 320x180 video, Matroska `CodecID` `V_MPEG4/ISO/AVC` (`libx264`)
- mono 44.1 kHz audio, Matroska `CodecID` `A_AAC`
- `title` = `mediapm fixture`, `ARTIST` = `mediapm`

### Consumers

- `src/mediapm/tests/int/media_fixture.rs` pins these facts.
- The online demo's offline (stub-chain) route reads the same file through
  `mediapm_test_support::media_fixture::media_fixture_path`.

## `sample-av.mp4`

`sample-av.mp4` is the tiny embedded MP4 payload used by local-ingest examples.
It keeps the examples self-contained: each consumer embeds the bytes with
`include_bytes!` and writes them into an isolated workspace at runtime.

### Regeneration

The fixture was produced with this exact command on `ffmpeg` 8.1.1, from the
repository root:

```sh
ffmpeg -y -f lavfi -i testsrc=duration=1:size=160x120:rate=24 -f lavfi -i sine=frequency=440:duration=1 -c:v libx264 -pix_fmt yuv420p -c:a aac -metadata comment="mediapm sample fixture" -shortest src/mediapm/tests/fixtures/media/sample-av.mp4
```

### Contents

- ~1 s duration
- 160x120 video, H.264 (`libx264`), 24 fps, `yuv420p`
- AAC mono audio, 44.1 kHz

### Consumers

- `src/mediapm/examples/mediapm_demo.rs`
- `src/mediapm/examples/mediapm_cli_add_sources.rs`
- `src/mediapm/examples/mediapm_cli_add_hierarchy.rs`
