---
description: "Use when editing demo hierarchy golden fixtures, demo_hierarchy_spec helpers, mediapm_demo_online post-sync content assertions, or online hierarchy e2e seeds. Documents the two online link filename formats (yt-dlp sidecar vs mediapm root projection), the artist-prefixed canonical naming family and where the artist comes from, which basenames are exact and which match the sidecar title by normalized prefix, the per-artifact tag split, and the content verification strategy (exact bytes in e2e, resilient structural checks in live demo)."
name: "Demo Hierarchy Golden and Content Verification Contract"
applyTo: "src/mediapm/src/demo_hierarchy_spec.rs,src/mediapm/tests/fixtures/demo_hierarchy_golden.json,src/mediapm/tests/int/demo_hierarchy_golden.rs,src/mediapm/tests/int/online_sync_post_sync_dump.rs,src/mediapm/tests/e2e/demo_online_hierarchy_materialization.rs,src/mediapm/examples/mediapm_demo_online.rs,src/mediapm/examples/support/materialization.rs"
---

# Demo hierarchy golden contract

The online demo (`mediapm_demo_online`) materializes a large hierarchy under `<artifact_root>/media/`. Tests encode that tree in `tests/fixtures/demo_hierarchy_golden.json` and `demo_hierarchy_spec.rs`. Link files appear in **two places** with **two naming formats**, and their titles have **two provenances**.

## Two link filename formats

| Location | Format family | Title provenance | Bracket id | Example basename |
| -------- | ------------- | ---------------- | ---------- | ---------------- |
| `sidecars/links/` | yt-dlp output template (`%(title)s [%(id)s].{ext}` after materializer strip) | LIVE upstream provider title, which embeds the artist | Raw video id `dQw4w9WgXcQ` | `Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].url` |
| Media folder root | mediapm hierarchy `rename_files` on links `MediaFolder` | Artist prefix from `${media.metadata.artist}` plus the canonical metadata title | Media id `youtube.dQw4w9WgXcQ` | `Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.url` |

Sidecar links are yt-dlp-native (`tools/workflows/yt_dlp.rs`). Root projections are mediapm-config-owned (`${media.metadata.artist} - ${media.metadata.title} [${media.id}].link.$1`). Every name mediapm renders draws its title half from `${media.metadata.title}` and its artist half from `${media.metadata.artist}`: the canonical recording title carries no artist, so the artist has to be its own metadata entry, and a template that relies on the title to carry it would drop the artist from every root name. The materializer strips `downloads/` and `__mediapm__` from folder-variant ZIP members but does **not** rewrite yt-dlp basenames into root projection shape.

## One naming family, one title

Every name mediapm renders — the media root folder, the video leaves, the subtitle/description/info leaves, the thumbnail projections, the root link projections — renders `${media.metadata.artist} - ${media.metadata.title}`, and `title` resolves to the tagged `video` variant first. media-tagger writes MusicBrainz's recording title, so the canonical materialized name is `Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]`. Only the yt-dlp-named sidecar links keep the upstream title, which happens to embed the artist too but appends its own suffixes.

A run that never reached the tagging steps resolves the upstream title instead, which appends `(Official Video)` and `(4K Remaster)` after a second copy of the artist. That run is a failed run: `assert_metadata_files_present` compares the media root against the canonical basename (`expected_demo_media_root_name`), which no pre-tagging name can match, so the tagging step's absence fails the assertion rather than being absorbed.

## Assertions (which basenames are exact)

Hermetic e2e and golden assertions must use **exact** basenames from `demo_hierarchy_spec` helpers, because the mock controls the title:

- Sidecar: `online_demo_sidecar_link_filename(ext)` → yt-dlp format
- Root: `online_demo_root_link_filename(ext)` → mediapm projection format

The live `mediapm_demo_online` post-sync check cannot pin the sidecar title, because that text is upstream's and upstream appends to it. It asserts the family exactly — the raw video id and the extension — and matches the title half by normalized prefix (`assert_sidecar_links_directory_matches_the_yt_dlp_family`). Its root-link check asserts the `[{media_id}].link.{ext}` suffix and leaves the title text unasserted, for the same reason. Only the media root basename is pinned exactly, by `expected_demo_media_root_name`, because its name is the canonical artist-prefixed one.

All three extensions (`url`, `webloc`, `desktop`) are required in both locations. Do not replace explicit golden `required_files` with `*.link.*` globs.

## Reinforcement rules

1. Do not conflate the two formats — sidecar bracket id is raw `dQw4w9WgXcQ` and its title is the upstream provider title; root projection bracket id is `youtube.dQw4w9WgXcQ` with `.link.` before the extension and its title is the artist-prefixed canonical one.
2. Keep the live sidecar check on the family: the raw video id and the extension are exact, and the title half is matched by normalized prefix. Do not reduce it to a `*.link.*` glob or a suffix-only assert — the family is what tells a yt-dlp sidecar link apart from a root projection renamed into `sidecars/links/`.
3. Do not add materializer logic to rename sidecar link basenames into root projection shape.
4. Assert each artifact's tags separately: the untagged file carries upstream's own tags, the tagged file carries the canonical ones. A tag assertion that applies to both artifacts is wrong for one of them.
5. A root template renders the artist and the title from their own metadata entries. Do not drop `${media.metadata.artist}` from a root name, and do not add it to a yt-dlp sidecar name — the sidecar family is upstream's, and mediapm never renames it.
6. When extending link coverage: update helpers in `demo_hierarchy_spec.rs`, golden JSON, `golden_fixture_link_paths_match_helpers`, then the live check — exact helper basenames in the hermetic tests, family-exact plus normalized-prefix title in the live demo.
7. Do not modify hermetic golden fixtures (`tests/fixtures/demo_hierarchy_golden.json`, `demo_hierarchy_spec.rs`) casually — they are the offline contract for the materialized tree; update fixtures and the helper-linkage test in the same commit as a materialization change.

## Content verification assertions

Every materialized file is verified for content correctness. Two strategies:

**E2e hermetic** (`demo_online_hierarchy_materialization.rs`): exact byte comparison against pre-seeded CAS content via `assert_materialized_content_matches_seeds()`, which walks every file and asserts `assert_eq!(actual_bytes, expected_seed_bytes)`. Covers MKV (MKV_HEADER), VTT (WEBVTT prefix), description, info.json, archive, thumbnails (jpg/webp), links (url/webloc/desktop), and folder projections.

**Live online demo** (`mediapm_demo_online.rs`): resilient structural checks tolerating upstream variation while catching regressions, in three tiers:

| Tier | Checks | Example |
|------|--------|---------|
| **Magic bytes** | File header matches format | JPEG `FF D8 FF`, WebP `RIFF+WEBP`, MKV `1A 45 DF A3` |
| **Content format** | Structure + deterministic URL content | `.url` = `[InternetShortcut]` + YouTube URL; `.webloc` = plist XML |
| **ffprobe metadata** | Exact codec/container/tag values, asserted per artifact | h264, aac, matroska, 256x144, bt709; tagged: ARTIST=Rick Astley, ISRC=GBUM71029573; untagged: upstream title, no MusicBrainz or replaygain tags |

Shared constants/helpers in `demo_hierarchy_spec.rs`: `ONLINE_DEMO_YOUTUBE_URL`, `ONLINE_DEMO_YT_DLP_VIDEO_ID`, `assert_starts_with_webvtt()`, `assert_valid_image_magic_bytes()`, `assert_content_contains_youtube_url()`, `assert_ffprobe_has_video_and_audio()`.

Coverage matrix: `sdd-tdd-workflow.instructions.md` → "Demo online content verification".

## Key symbols

- `online_demo_yt_dlp_provider_title()` — `{artist} - {title}` stand-in for the upstream yt-dlp `%(title)s`
- `online_demo_media_folder_relative()` — canonical media folder path (artist-prefixed canonical title)
- `online_demo_sidecar_link_filename(ext)` — yt-dlp-format basename under `sidecars/links/` (upstream title)
- `online_demo_root_link_filename(ext)` — mediapm root projection basename (artist-prefixed canonical title)
- `online_demo_sidecar_link_relative_path` / `online_demo_root_link_relative_path` — golden-relative paths
- `golden_fixture_link_paths_match_helpers` — golden ↔ helper linkage test
- `assert_untagged_format_tags()` / `assert_format_tags_structural()` — the per-artifact tag assertions
- `expected_demo_media_root_name()` — the live demo's pinned canonical media root basename
