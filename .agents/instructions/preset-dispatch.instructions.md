---
description: "Use when editing tool preset definitions in src/mediapm/src/tools/preset/ and workflows/. Covers apply_preset dispatch, per-tool spec builders, workflow step synthesis, and sandbox artifact conventions."
name: "Preset Dispatch"
applyTo: "src/mediapm/src/tools/preset/**/*.rs, src/mediapm/src/tools/workflows/**/*.rs"
---

# Preset dispatch

## Purpose

- Route tool names to per-tool spec builders via `apply_preset()`.
- Construct `ToolSpec` (command template, inputs/outputs, content_map) and `ToolRuntime` (impure flag, concurrency, retry).

## `apply_preset(tool_name, content_map, os_exec_paths, slot_limits)` dispatch

Case-insensitive routing to per-tool `apply()` functions:

| Tool           | Module            | Key responsibilities                                            |
| -------------- | ----------------- | --------------------------------------------------------------- |
| `ffmpeg`       | `ffmpeg.rs`       | Command template, slot limit config, input/output streams       |
| `yt-dlp`       | `yt_dlp.rs`       | URL-based command, subtitle/thumbnail outputs, format selectors |
| `deno`         | `deno.rs`         | Deno runtime command with permissions                           |
| `rsgain`       | `rsgain.rs`       | ReplayGain analysis command, single-track mode                  |
| `media-tagger` | `media_tagger.rs` | Picard-based tagging command, CAA provider config               |
| `sd`           | `sd.rs`           | Stable Diffusion command, model paths                           |

Each preset builds:

- `command`: template with `{tool_dir}` and `{os}` placeholders.
- `inputs`: declared input keys with types and defaults.
- `outputs`: declared output variants with capture kind and file patterns.
- `content_map`: sandbox-relative path → CAS hash entries from provisioning.
- `runtime`: impure flag (impure for tools with side-effects), concurrency limits, retry policy.

## Workflow step synthesis (`tools/workflows/`)

Per-tool `build_<tool>_spec()` functions wrap presets to produce final `(ToolSpec, ToolRuntime)`:

- Apply preset → add ffmpeg slot limits → set inherited env vars → return.
- Each workflow module also defines step-specific constants (input keys, output variant definitions).

## Sandbox artifacts folder convention

| Directory    | Purpose                                             |
| ------------ | --------------------------------------------------- |
| `downloads/` | Downloaded source files (yt-dlp outputs, raw media) |
| `coverart/`  | Extracted cover art images                          |
| `inputs/`    | Step input files staged by the conductor            |

## Per-preset dependency type registry

Each preset module with dependencies should provide a `dependency_types()`
function returning `BTreeMap<&'static str, DependencyType>`. This maps
companion tool IDs to their companion relationship type (`SameStep`,
`CrossStep`, `Both`), overriding the default `SameStep`.

The function is used by `known_dependency_type()` in
`src/mediapm/src/tools/dependency.rs` to resolve a dependency's relationship
without involving user config.

Currently classified edges:

| Requirer       | Dependency    | Type        |
| -------------- | ------------- | ----------- |
| `yt-dlp`       | `ffmpeg`      | `SameStep`  |
| `yt-dlp`       | `media-tagger` | `CrossStep` |
| `media-tagger` | `ffmpeg`      | `SameStep`  |
| `media-tagger` | `rsgain`      | `CrossStep` |
| `rsgain`       | `ffmpeg`      | `CrossStep` |

## Key invariants

- Unknown tool name → **panics** with `"unknown managed tool: {tool_name}"` (programming error, not user error).
- `apply_preset` is called only after successful provisioning — content_map and os_exec_paths are always populated for known managed tools.
- `runtime.impure` must be `true` for tools with side effects (yt-dlp: network fetches, media-tagger: network lookups).
- Concurrency defaults: 1 active concurrent call; retry: 1 outer retry for network-dependent tools.
- `DependencyType` is defined in `src/mediapm/src/tools/dependency.rs` (internal, no serde derives, not user-configurable).
