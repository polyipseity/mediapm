---
description: "Use when editing examples, demo fixtures, or example tests. Covers example execution policy, online/offline demo routes, mock seams, and hierarchy golden/content verification."
name: "Examples and demos"
applyTo: "src/**/examples/**/*.rs, src/mediapm/src/demo_hierarchy_spec.rs, src/mediapm/tests/fixtures/demo_hierarchy_golden.json, src/mediapm/tests/int/demo_hierarchy_golden.rs, src/mediapm/tests/int/media_fixture.rs, src/mediapm/tests/e2e/demo_online_hierarchy_materialization.rs, src/mediapm/examples/support/materialization.rs"
---

# Examples and demos

## Example execution policy

Every example under `src/*/examples/` has a `#[cfg(test)]` test that calls `super::main()`. A `#[tokio::main]` example uses a plain `#[test]`; `#[tokio::test]` would nest runtimes. Nextest compiles examples and runs their embedded tests with `--all-targets`, but never executes `main()` itself.

- **Deterministic examples**: full path in tests, no CI detection, no reduced mode.
- **Nondeterministic examples**: route selected by argv parsed once in `main`; `--online` names the real upstream. No env var selects route. No example detects CI inside `main()`.
- **`mediapm_demo_online`**: default is offline/hermetic stub chain (mocked upstream payloads, mocked MusicBrainz). `--online` selects real providers, real tool binaries, and real MusicBrainz; no gate reaches it. The offline route runs in every gate. A release build refuses the offline stub route before watchdog/tool resolve and names `--release`.
- Assertions follow route: real-tool ffprobe stream properties, untagged/tagged tag sets, ReplayGain tags, and canonical media-root name run only online. Offline reads the materialized file in process and asserts fixture facts via `mediapm_test_support::read_matroska_facts`.

## Network-bound tests

Prefer a mocked upstream over a skip:

- One shared assertion function holds the contract.
- One hermetic mock test always runs in CI, `test-all`, and pre-push.
- Real-host validation is the manual `cargo run --package mediapm --example mediapm_demo_online -- --online` run (or `online` workflow dispatch). No ignored real-host twin.

`mediapm-test-support` (`src/mediapm-test-support/`) owns mocks. It is a dev-dependency of `mediapm` and `mediapm-conductor` and must never depend back on either.

## Debug-only upstream seams

Four base URL env vars are read per call and never cached: `MEDIAPM_GITHUB_API_BASE`, `MEDIAPM_GITHUB_DOWNLOAD_BASE`, `MEDIAPM_MARTIN_RIEDL_BASE`, `MEDIAPM_JELLYFIN_BASE`. Each reader honors its variable only when `cfg!(debug_assertions)` holds, gated on the reader body. Debug builds read env; release builds ignore these vars. No seam feature. Tests that set seams must hold the process env lock (see `temp-and-paths.instructions.md`, "Env overrides and isolation").

## Demo hierarchy golden

`mediapm_demo_online` materializes a hierarchy under `<artifact_root>/media/`. `tests/fixtures/demo_hierarchy_golden.json` and `src/mediapm/src/demo_hierarchy_spec.rs` encode the tree.

### Two link formats

| Location | Format | Bracket id | Title provenance |
| --- | --- | --- | --- |
| `sidecars/links/` | `%(title)s [%(id)s].{ext}` (yt-dlp) | raw video id `dQw4w9WgXcQ` | upstream provider title |
| media folder root | `${media.metadata.artist} - ${media.metadata.title} [${media.id}].link.{ext}` | media id `youtube.dQw4w9WgXcQ` | artist + canonical metadata title |

Every mediapm-rendered name uses `${media.metadata.artist} - ${media.metadata.title}`; title resolves to the tagged `video` variant first. Only yt-dlp sidecar links keep the upstream title. The materializer strips `downloads/` and `__mediapm__` from folder-variant ZIP members but does not rewrite yt-dlp basenames into root projection shape.

The offline demo playlist deliberately carries all three reference forms (shorthand, explicit absolute, explicit relative) so both path modes are exercised.

### Assertions

- Hermetic e2e/golden use exact basenames from `demo_hierarchy_spec` helpers. Sidecar: `online_demo_sidecar_link_filename(ext)`. Root: `online_demo_root_link_filename(ext)`.
- Live demo cannot pin sidecar title: assert sidecar family by exact raw id + extension and normalized-prefix title match. Assert root by `[{media_id}].link.{ext}` suffix. Pin media root basename exactly with `expected_demo_media_root_name()`.
- All three extensions (`url`, `webloc`, `desktop`) are required in both locations. Do not replace explicit golden `required_files` with `*.link.*` globs.
- Assert each artifact's tags separately: untagged carries upstream tags, tagged carries canonical tags.

### Content verification

- **E2e hermetic**: exact byte comparison against pre-seeded CAS content (`assert_materialized_content_matches_seeds()`).
- **Live demo**: magic bytes and content format checks run on both routes. ffprobe metadata tier runs online only. Offline replaces ffprobe tier with `assert_fixture_media_facts` (in-process `read_matroska_facts`) against committed fixture codec/size/duration.

### Reinforcement rules

- Do not conflate sidecar and root formats, ids, or title provenance.
- Do not add materializer logic to rename sidecar link basenames into root projection shape.
- Do not drop `${media.metadata.artist}` from a root name; do not add it to yt-dlp sidecar names.
- When extending link coverage: update `demo_hierarchy_spec.rs`, golden JSON, `golden_fixture_link_paths_match_helpers`, then live check.
- Do not modify hermetic golden fixtures casually; update fixtures and helper-linkage test in the same commit as a materialization change.
- Each demo declares one captured variant that no hierarchy entry links, with `save = false`; `assert_demo_saves_what_the_hierarchy_materializes` pins counts.

## Pointers

- `src/mediapm/src/example_isolation.rs`
- `src/mediapm/src/demo_hierarchy_spec.rs`
- `src/mediapm/tests/fixtures/demo_hierarchy_golden.json`
- `src/mediapm/tests/int/demo_hierarchy_golden.rs`
- `src/mediapm/tests/e2e/demo_online_hierarchy_materialization.rs`
- `src/mediapm/examples/mediapm_demo_online.rs`
