---
description: "Use for examples, demos and golden fixture verification."
name: "Examples and demos"
applyTo: "src/**/examples/**/*.rs, src/mediapm/src/demo_hierarchy_spec.rs, src/mediapm/tests/fixtures/demo_hierarchy_golden.json, src/mediapm/tests/int/demo_hierarchy_golden.rs, src/mediapm/tests/int/media_fixture.rs, src/mediapm/tests/e2e/demo_online_hierarchy_materialization.rs, src/mediapm/examples/support/materialization.rs"
---

# Examples and demos

## Example execution policy

Every `src/*/examples/` example has `#[cfg(test)]` calling `super::main()`; `#[tokio::main]` uses plain `#[test]` (`#[tokio::test]` nests runtimes). Nextest runs embedded tests with `--all-targets`, never `main()` itself.

- Deterministic: full path in tests, no CI detection/reduced mode. Nondeterministic: route from argv parsed once in `main`, `--online` = real upstream; no env route, no CI detection inside `main`.
- `mediapm_demo_online`: offline/hermetic stub chain default (mocked upstream/MusicBrainz); `--online` real providers/binaries/MusicBrainz, no gate reaches it. Release refuses offline stub before watchdog/tool resolve, naming `--release`.
- Offline deselects `include_user_genres`, `include_user_ratings`, `include_user_collections`, `include_user_tags`: gates need no credentials. `--online` leaves defaults, owner/manual: user data needs `mediapm auth musicbrainz login`, never a CI claim.
- Assertions by route: real ffprobe streams, untagged/tagged tags, ReplayGain, media-root name online only; offline reads materialized file via `mediapm_test_support::read_matroska_facts`.

## Network-bound tests

Mocked upstream over skip: one shared assertion function, one hermetic mock in CI/`test-all`/pre-push; real host only manual `cargo run --package mediapm --example mediapm_demo_online -- --online` (or `online` workflow), no ignored twin. `mediapm-test-support` (`src/mediapm-test-support/`) owns mocks, dev-dep of `mediapm`/`mediapm-conductor`, never depends back.

## Debug-only upstream seams

`MEDIAPM_GITHUB_API_BASE`, `MEDIAPM_GITHUB_DOWNLOAD_BASE`, `MEDIAPM_MARTIN_RIEDL_BASE`, `MEDIAPM_JELLYFIN_BASE`, `MEDIAPM_ACOUSTID_BASE`: read per call, never cached, honored only under `cfg!(debug_assertions)` in the reader body; release ignores, no seam feature. Tests setting seams hold the process env lock (`temp-and-paths.instructions.md`).

## Demo hierarchy golden

`mediapm_demo_online` materializes `<output_root>/media/`; encoded in `tests/fixtures/demo_hierarchy_golden.json` and `src/mediapm/src/demo_hierarchy_spec.rs`.

| Location | Format | Bracket id | Title provenance |
| --- | --- | --- | --- |
| `sidecars/links/` | `%(title)s [%(id)s].{ext}` (yt-dlp) | `dQw4w9WgXcQ` | upstream provider title |
| media folder root | `${media.metadata.artist} - ${media.metadata.title} [${media.id}].link.{ext}` | `youtube.dQw4w9WgXcQ` | artist + canonical metadata title |

Mediapm names use `${media.metadata.artist} - ${media.metadata.title}`, title from tagged `video` first; only yt-dlp sidecars keep upstream title. Materializer strips `downloads/`/`__mediapm__` from folder-variant ZIP members, never rewrites sidecar basenames. Offline playlist carries shorthand/absolute/relative references.

- Basenames from `demo_hierarchy_spec`: `online_demo_sidecar_link_filename(ext)`/`online_demo_root_link_filename(ext)`. Live: sidecar raw id + extension + normalized-prefix title, root `[{media_id}].link.{ext}` suffix, basename `expected_demo_media_root_name()`. `url`/`webloc`/`desktop` required both locations; no `*.link.*` globs for `required_files`; tags asserted separately.
- Hermetic content exact bytes vs seeded CAS (`assert_materialized_content_matches_seeds()`); live magic/format both routes, ffprobe online, offline `assert_fixture_media_facts` (`read_matroska_facts`) vs fixture codec/size/duration.
- No sidecar/root conflation or renaming, no dropping `${media.metadata.artist}` from root or adding to sidecars. Coverage changes: `demo_hierarchy_spec.rs`, golden JSON, `golden_fixture_link_paths_match_helpers`, live check; fixtures + linkage test in same commit. One unlinked variant `save = false`; `assert_demo_saves_what_the_hierarchy_materializes` pins counts.

## Manifest rule

A manifest records what no other artifact records for that run (counters, sizes, warnings, versions, policy) + one path anchor, never a path derivable from its own directory or another file's map.

## Pointers

`src/mediapm/src/example_isolation.rs`, `src/mediapm/src/demo_hierarchy_spec.rs`, `src/mediapm/tests/fixtures/demo_hierarchy_golden.json`, `src/mediapm/tests/int/demo_hierarchy_golden.rs`, `src/mediapm/tests/e2e/demo_online_hierarchy_materialization.rs`, `src/mediapm/examples/mediapm_demo_online.rs`.
