---
description: "Use when editing demo hierarchy golden fixtures, demo_hierarchy_spec helpers, mediapm_demo_online post-sync asserts, or online hierarchy e2e seeds. Documents canonical vs live-tolerant link naming and the two link filename pipelines."
name: "Demo Hierarchy Golden Contract"
applyTo: "src/mediapm/src/demo_hierarchy_spec.rs,src/mediapm/tests/fixtures/demo_hierarchy_golden.json,src/mediapm/tests/int/hierarchy_golden.rs,src/mediapm/tests/e2e/online_hierarchy_materialization.rs,src/mediapm/examples/mediapm_demo_online.rs"
---

# Demo hierarchy golden contract

## Purpose

The online demo (`mediapm_demo_online`) materializes a large hierarchy under `<artifact_root>/media/`. Tests encode that tree in `tests/fixtures/demo_hierarchy_golden.json` and `demo_hierarchy_spec.rs`. Live sync and hermetic tests intentionally use **different strictness** for link file title prefixes. This is by design — do not collapse the layers without an explicit product decision.

## Two link filename pipelines

Link artifacts appear in two places with different naming sources:

| Location | Naming source | Title prefix comes from |
| -------- | ------------- | ----------------------- |
| `sidecars/links/` | yt-dlp capture + materializer strip | yt-dlp `%(title)s` in the managed output template (`tools/workflows/yt_dlp.rs`) |
| Media folder root (`…link.url`, etc.) | `MediaFolder` `rename_files` | mediapm metadata templates `${media.metadata.artist}` / `${media.metadata.title}` / `${media.id}` |

Materializer strips `downloads/` and `__mediapm__` from folder-variant ZIP members but **does not rewrite** the yt-dlp title prefix in `sidecars/links/`.

## Canonical vs live-tolerant assertions

| Layer | Title prefix | Link verification | Why |
| ----- | ------------ | ----------------- | --- |
| **Golden JSON** + `online_demo_*_link_*` helpers | Fixed canonical `Rick Astley - Never Gonna Give You Up` | Exact full paths in `required_files` | Deterministic regression contract for `assert_tree_under` |
| **Hermetic e2e seeds** | Same canonical prefix in ZIP members | Materialized tree must match golden | No network; yt-dlp never runs |
| **Live `mediapm_demo_online`** | Whatever yt-dlp returns for `%(title)s` | Suffix checks: `[dQw4w9WgXcQ].{url,webloc,desktop}` for sidecars; `[youtube.dQw4w9WgXcQ].link.{ext}` for root projections | Provider title can drift (suffix qualifiers, punctuation, etc.) without breaking sync |

Example: live sidecar link `Rick Astley - Never Gonna Give You Up (Official Video) [dQw4w9WgXcQ].url` **passes** live asserts but **does not** match golden `required_files`. Both outcomes are correct for their layer.

## Reinforcement rules (do not regress)

1. **Do not** require exact yt-dlp title strings in live `mediapm_demo_online` post-sync asserts for link files — keep suffix-based checks on video id and extension (all three formats required).
2. **Do not** replace golden explicit link `required_files` with a loose `*.link.*` glob — use the six explicit paths (three root + three sidecar) tied to helpers.
3. **Do not** change golden or e2e seeds to match a one-off live yt-dlp title without updating `online_demo_yt_dlp_provider_title()` and documenting the new canonical form.
4. **Do not** add materializer logic to rewrite yt-dlp link basenames to mediapm metadata titles — sidecar folder names stay yt-dlp-native.
5. When adding new link-sidecar coverage, extend helpers in `demo_hierarchy_spec.rs` first, then golden JSON, then `golden_fixture_link_paths_match_helpers` (or equivalent), then live suffix asserts if applicable.

## Key symbols

- `online_demo_yt_dlp_provider_title()` — canonical `%(title)s` stand-in for tests
- `online_demo_sidecar_link_filename(ext)` — public basename under `sidecars/links/`
- `online_demo_root_link_filename(ext)` — public root projection basename
- `online_demo_sidecar_link_relative_path` / `online_demo_root_link_relative_path` — golden-relative paths
- `assert_sidecar_links_directory_has_all_public_formats` — live sidecar suffix gate
- `golden_fixture_link_paths_match_helpers` — golden ↔ helper linkage test
