---
description: "Use when editing demo hierarchy golden fixtures, demo_hierarchy_spec helpers, mediapm_demo_online post-sync asserts, or online hierarchy e2e seeds. Documents the two online link filename formats (yt-dlp sidecar vs mediapm root projection)."
name: "Demo Hierarchy Golden Contract"
applyTo: "src/mediapm/src/demo_hierarchy_spec.rs,src/mediapm/tests/fixtures/demo_hierarchy_golden.json,src/mediapm/tests/int/demo_hierarchy_golden.rs,src/mediapm/tests/e2e/demo_online_hierarchy_materialization.rs,src/mediapm/examples/mediapm_demo_online.rs"
---

# Demo hierarchy golden contract

## Purpose

The online demo (`mediapm_demo_online`) materializes a large hierarchy under `<artifact_root>/media/`. Tests encode that tree in `tests/fixtures/demo_hierarchy_golden.json` and `demo_hierarchy_spec.rs`. Link files appear in **two places** with **two different naming formats** — not title drift, not loosened suffix checks.

## Two link filename formats

| Location | Format family | Bracket id | Example basename |
| -------- | ------------- | ---------- | ---------------- |
| `sidecars/links/` | **yt-dlp output template** (`%(title)s [%(id)s].{ext}` after materializer strip) | Raw video id `dQw4w9WgXcQ` | `Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].url` |
| Media folder root | **mediapm hierarchy `rename_files`** on links `MediaFolder` | Media id `youtube.dQw4w9WgXcQ` | `Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.url` |

Sidecar links are yt-dlp-native filenames (managed output in `tools/workflows/yt_dlp.rs`). Root link projections are mediapm-config-owned (`${media.metadata.artist} - ${media.metadata.title} [${media.id}].link.$1`).

Materializer strips `downloads/` and `__mediapm__` from folder-variant ZIP members but does **not** rewrite yt-dlp basenames into mediapm root projection shape.

## Assertions (exact filenames)

Golden, hermetic e2e, and live `mediapm_demo_online` post-sync checks must use **exact** expected basenames from `demo_hierarchy_spec` helpers:

- Sidecar: `online_demo_sidecar_link_filename(ext)` → yt-dlp format
- Root: `online_demo_root_link_filename(ext)` → mediapm projection format

All three extensions (`url`, `webloc`, `desktop`) are required in both locations. Do not replace explicit golden `required_files` with `*.link.*` globs.

## Reinforcement rules (do not regress)

1. **Do not** conflate the two formats — sidecar bracket id is raw `dQw4w9WgXcQ`; root projection bracket id is `youtube.dQw4w9WgXcQ` with `.link.` before the extension.
2. **Do not** use suffix-only link asserts in live demo when exact helper filenames apply.
3. **Do not** add materializer logic to rename sidecar link basenames into mediapm root projection shape.
4. When extending link coverage: update helpers in `demo_hierarchy_spec.rs`, golden JSON, `golden_fixture_link_paths_match_helpers`, then live exact asserts.

## Key symbols

- `online_demo_yt_dlp_provider_title()` — `{artist} - {title}` stand-in for yt-dlp `%(title)s` in tests
- `online_demo_sidecar_link_filename(ext)` — yt-dlp-format basename under `sidecars/links/`
- `online_demo_root_link_filename(ext)` — mediapm root projection basename
- `online_demo_sidecar_link_relative_path` / `online_demo_root_link_relative_path` — golden-relative paths
- `golden_fixture_link_paths_match_helpers` — golden ↔ helper linkage test
